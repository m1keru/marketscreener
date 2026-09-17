// Package store is the SQLite layer: the EDGAR fact cache, the quote cache and
// the history of runs. It is the only place that touches the database.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite" // pure-Go driver: keeps the binary cgo-free

	"github.com/m1ke/marketscreener/internal/model"
)

const schema = `
CREATE TABLE IF NOT EXISTS companies (
  cik TEXT PRIMARY KEY, ticker TEXT, title TEXT,
  sic INTEGER, sic_desc TEXT, updated_at INTEGER
);
-- period_start belongs in the key: one filing reports both the full year and
-- its fourth quarter with the same period_end and accession number, and a key
-- without the start silently keeps whichever row was written last.
CREATE TABLE IF NOT EXISTS facts (
  cik TEXT, tag TEXT, unit TEXT, period_end TEXT,
  period_start TEXT, fy INTEGER, fp TEXT, form TEXT, val REAL, accn TEXT, filed TEXT,
  fetched_at INTEGER,
  PRIMARY KEY (cik, tag, unit, period_end, period_start, accn)
);
CREATE TABLE IF NOT EXISTS prices (
  ticker TEXT PRIMARY KEY, price REAL, as_of TEXT, source TEXT, fetched_at INTEGER
);
CREATE TABLE IF NOT EXISTS runs (
  id TEXT PRIMARY KEY, started_at INTEGER, finished_at INTEGER,
  profile TEXT, params_json TEXT, universe_size INTEGER, passed_count INTEGER,
  funnel_json TEXT
);
CREATE TABLE IF NOT EXISTS run_results (
  run_id TEXT, ticker TEXT, verdict TEXT, score REAL,
  metrics_json TEXT, criteria_json TEXT,
  PRIMARY KEY (run_id, ticker)
);
-- Bookkeeping for the caches above: which bulk fetch happened when. Without it
-- a company that legitimately has no facts would be re-fetched on every run.
CREATE TABLE IF NOT EXISTS fetches (key TEXT PRIMARY KEY, fetched_at INTEGER);
CREATE INDEX IF NOT EXISTS idx_facts_lookup ON facts(cik, tag, period_end);
CREATE INDEX IF NOT EXISTS idx_run_results_ticker ON run_results(ticker, verdict);
`

// Store wraps the database handle.
type Store struct {
	db *sql.DB
}

// Open creates the database file and schema if they do not exist. Pass ":memory:"
// for tests.
func Open(path string) (*Store, error) {
	if path != ":memory:" {
		if dir := filepath.Dir(path); dir != "" && dir != "." {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, fmt.Errorf("create db dir %s: %w", dir, err)
			}
		}
	}
	dsn := path
	if path == ":memory:" {
		dsn = "file::memory:?cache=shared"
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	// One writer at a time; modernc/sqlite serialises anyway and this avoids
	// SQLITE_BUSY under the concurrent fetchers.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA busy_timeout=5000;`); err != nil {
		return nil, fmt.Errorf("pragmas: %w", err)
	}
	if err := migrate(db); err != nil {
		db.Close()
		return nil, err
	}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("schema: %w", err)
	}
	return &Store{db: db}, nil
}

// schemaVersion is bumped whenever a cached table has to be rebuilt. The
// caches are disposable; the run history is not, and migrations must preserve
// it.
const schemaVersion = 2

// migrate brings an existing database up to schemaVersion. Version 2 rebuilds
// the facts cache, whose primary key used to collide full-year facts with the
// fourth-quarter facts of the same filing.
func migrate(db *sql.DB) error {
	var version int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}
	if version >= schemaVersion {
		return nil
	}
	var factsExist int
	if err := db.QueryRow(
		`SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'facts'`).
		Scan(&factsExist); err != nil {
		return fmt.Errorf("inspect schema: %w", err)
	}
	if factsExist > 0 && version < 2 {
		// Drop the fact cache and the markers that say it was fetched, so the
		// next run refills it under the corrected key. Runs are untouched.
		if _, err := db.Exec(`DROP TABLE facts;
			DELETE FROM fetches WHERE key LIKE 'facts:%' OR key LIKE 'frame:%';`); err != nil {
			return fmt.Errorf("rebuild fact cache: %w", err)
		}
	}
	if _, err := db.Exec(fmt.Sprintf(`PRAGMA user_version = %d`, schemaVersion)); err != nil {
		return fmt.Errorf("write schema version: %w", err)
	}
	return nil
}

// Close releases the handle.
func (s *Store) Close() error { return s.db.Close() }

// DB exposes the handle for tests.
func (s *Store) DB() *sql.DB { return s.db }

// --- fetch bookkeeping ---------------------------------------------------

// Fresh reports whether the named fetch happened within ttl.
func (s *Store) Fresh(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	var at int64
	err := s.db.QueryRowContext(ctx, `SELECT fetched_at FROM fetches WHERE key = ?`, key).Scan(&at)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return time.Since(time.Unix(at, 0)) < ttl, nil
}

// MarkFetched records that the named fetch just completed.
func (s *Store) MarkFetched(ctx context.Context, key string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO fetches (key, fetched_at) VALUES (?, ?)
		 ON CONFLICT(key) DO UPDATE SET fetched_at = excluded.fetched_at`,
		key, time.Now().Unix())
	return err
}

// --- companies -----------------------------------------------------------

// SaveCompanies upserts the registrant rows.
func (s *Store) SaveCompanies(ctx context.Context, cs []model.Company) error {
	return s.tx(ctx, func(tx *sql.Tx) error {
		stmt, err := tx.PrepareContext(ctx,
			`INSERT INTO companies (cik, ticker, title, sic, sic_desc, updated_at)
			 VALUES (?, ?, ?, ?, ?, ?)
			 ON CONFLICT(cik) DO UPDATE SET ticker=excluded.ticker, title=excluded.title,
			   sic=excluded.sic, sic_desc=excluded.sic_desc, updated_at=excluded.updated_at`)
		if err != nil {
			return err
		}
		defer stmt.Close()
		now := time.Now().Unix()
		for _, c := range cs {
			if _, err := stmt.ExecContext(ctx, c.CIK, c.Ticker, c.Title, c.SIC, c.SICDesc, now); err != nil {
				return err
			}
		}
		return nil
	})
}

// Company returns a cached registrant if it was refreshed within ttl.
func (s *Store) Company(ctx context.Context, cik string, ttl time.Duration) (*model.Company, error) {
	var c model.Company
	var updated int64
	err := s.db.QueryRowContext(ctx,
		`SELECT cik, ticker, title, sic, sic_desc, updated_at FROM companies WHERE cik = ?`, cik).
		Scan(&c.CIK, &c.Ticker, &c.Title, &c.SIC, &c.SICDesc, &updated)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if time.Since(time.Unix(updated, 0)) >= ttl {
		return nil, nil
	}
	return &c, nil
}

// AllCompanies returns every cached registrant.
func (s *Store) AllCompanies(ctx context.Context) ([]model.Company, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT cik, ticker, title, sic, sic_desc FROM companies ORDER BY cik, ticker`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Company
	for rows.Next() {
		var c model.Company
		if err := rows.Scan(&c.CIK, &c.Ticker, &c.Title, &c.SIC, &c.SICDesc); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// LatestByTag returns, for one tag, the freshest fact of every company. It is
// how a warm run reads back what the frames stage cached.
func (s *Store) LatestByTag(ctx context.Context, tag string) ([]model.Fact, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT f.cik, f.tag, f.unit, f.period_end, f.period_start, f.fy, f.fp, f.form, f.val, f.accn, f.filed
		 FROM facts f
		 JOIN (SELECT cik, MAX(period_end) AS pe FROM facts WHERE tag = ? GROUP BY cik) m
		   ON m.cik = f.cik AND m.pe = f.period_end
		 WHERE f.tag = ?`, tag, tag)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Fact
	for rows.Next() {
		var f model.Fact
		if err := rows.Scan(&f.CIK, &f.Tag, &f.Unit, &f.End, &f.Start, &f.FY, &f.FP,
			&f.Form, &f.Val, &f.Accn, &f.Filed); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

// AnnualByTag returns every annual-form fact of one tag across all companies,
// used by the frames stage when the annual EPS frames are already cached.
func (s *Store) AnnualByTag(ctx context.Context, tag string) ([]model.Fact, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT cik, tag, unit, period_end, period_start, fy, fp, form, val, accn, filed
		 FROM facts WHERE tag = ? AND period_start <> ''`, tag)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Fact
	for rows.Next() {
		var f model.Fact
		if err := rows.Scan(&f.CIK, &f.Tag, &f.Unit, &f.End, &f.Start, &f.FY, &f.FP,
			&f.Form, &f.Val, &f.Accn, &f.Filed); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

// CompanyByTicker looks a registrant up by ticker regardless of age.
func (s *Store) CompanyByTicker(ctx context.Context, ticker string) (*model.Company, error) {
	var c model.Company
	err := s.db.QueryRowContext(ctx,
		`SELECT cik, ticker, title, sic, sic_desc FROM companies WHERE ticker = ?`, ticker).
		Scan(&c.CIK, &c.Ticker, &c.Title, &c.SIC, &c.SICDesc)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &c, nil
}

// --- facts ---------------------------------------------------------------

// SaveFacts upserts XBRL facts.
func (s *Store) SaveFacts(ctx context.Context, facts []model.Fact) error {
	if len(facts) == 0 {
		return nil
	}
	return s.tx(ctx, func(tx *sql.Tx) error {
		stmt, err := tx.PrepareContext(ctx,
			`INSERT INTO facts (cik, tag, unit, period_end, period_start, fy, fp, form, val, accn, filed, fetched_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			 ON CONFLICT(cik, tag, unit, period_end, period_start, accn) DO UPDATE SET
			   fy=excluded.fy, fp=excluded.fp, form=excluded.form, val=excluded.val,
			   filed=excluded.filed, fetched_at=excluded.fetched_at`)
		if err != nil {
			return err
		}
		defer stmt.Close()
		now := time.Now().Unix()
		for _, f := range facts {
			if _, err := stmt.ExecContext(ctx, f.CIK, f.Tag, f.Unit, f.End, f.Start,
				f.FY, f.FP, f.Form, f.Val, f.Accn, f.Filed, now); err != nil {
				return err
			}
		}
		return nil
	})
}

// Facts returns every cached fact of one company.
func (s *Store) Facts(ctx context.Context, cik string) ([]model.Fact, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT cik, tag, unit, period_end, period_start, fy, fp, form, val, accn, filed
		 FROM facts WHERE cik = ? ORDER BY tag, period_end, accn`, cik)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Fact
	for rows.Next() {
		var f model.Fact
		if err := rows.Scan(&f.CIK, &f.Tag, &f.Unit, &f.End, &f.Start, &f.FY, &f.FP,
			&f.Form, &f.Val, &f.Accn, &f.Filed); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

// FactsByTag returns cached facts of one company for a single tag.
func (s *Store) FactsByTag(ctx context.Context, cik, tag string) ([]model.Fact, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT cik, tag, unit, period_end, period_start, fy, fp, form, val, accn, filed
		 FROM facts WHERE cik = ? AND tag = ? ORDER BY period_end, accn`, cik, tag)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Fact
	for rows.Next() {
		var f model.Fact
		if err := rows.Scan(&f.CIK, &f.Tag, &f.Unit, &f.End, &f.Start, &f.FY, &f.FP,
			&f.Form, &f.Val, &f.Accn, &f.Filed); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

// --- prices --------------------------------------------------------------

// Price returns a cached quote if it is younger than ttl.
func (s *Store) Price(ctx context.Context, ticker string, ttl time.Duration) (*model.Price, error) {
	var p model.Price
	var fetched int64
	err := s.db.QueryRowContext(ctx,
		`SELECT ticker, price, as_of, source, fetched_at FROM prices WHERE ticker = ?`, ticker).
		Scan(&p.Ticker, &p.Price, &p.AsOf, &p.Source, &fetched)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if time.Since(time.Unix(fetched, 0)) >= ttl {
		return nil, nil
	}
	return &p, nil
}

// SavePrices upserts quotes.
func (s *Store) SavePrices(ctx context.Context, ps []model.Price) error {
	if len(ps) == 0 {
		return nil
	}
	return s.tx(ctx, func(tx *sql.Tx) error {
		stmt, err := tx.PrepareContext(ctx,
			`INSERT INTO prices (ticker, price, as_of, source, fetched_at) VALUES (?, ?, ?, ?, ?)
			 ON CONFLICT(ticker) DO UPDATE SET price=excluded.price, as_of=excluded.as_of,
			   source=excluded.source, fetched_at=excluded.fetched_at`)
		if err != nil {
			return err
		}
		defer stmt.Close()
		now := time.Now().Unix()
		for _, p := range ps {
			if _, err := stmt.ExecContext(ctx, p.Ticker, p.Price, p.AsOf, p.Source, now); err != nil {
				return err
			}
		}
		return nil
	})
}

// --- runs ----------------------------------------------------------------

// RunResult is one company's stored outcome inside a run.
type RunResult struct {
	Ticker   string                  `json:"ticker"`
	Verdict  model.Verdict           `json:"verdict"`
	Score    float64                 `json:"score"`
	Metrics  model.Metrics           `json:"metrics"`
	Criteria []model.CriterionResult `json:"criteria"`
}

// SaveRun stores the run header together with its results, atomically.
func (s *Store) SaveRun(ctx context.Context, run model.Run, results []RunResult) error {
	return s.tx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO runs (id, started_at, finished_at, profile, params_json,
			  universe_size, passed_count, funnel_json)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?)
			 ON CONFLICT(id) DO UPDATE SET finished_at=excluded.finished_at,
			  passed_count=excluded.passed_count, funnel_json=excluded.funnel_json`,
			run.ID, run.StartedAt.Unix(), run.FinishedAt.Unix(), run.Profile, run.ParamsJSON,
			run.UniverseSize, run.PassedCount, run.FunnelJSON); err != nil {
			return err
		}
		stmt, err := tx.PrepareContext(ctx,
			`INSERT INTO run_results (run_id, ticker, verdict, score, metrics_json, criteria_json)
			 VALUES (?, ?, ?, ?, ?, ?)
			 ON CONFLICT(run_id, ticker) DO UPDATE SET verdict=excluded.verdict,
			   score=excluded.score, metrics_json=excluded.metrics_json,
			   criteria_json=excluded.criteria_json`)
		if err != nil {
			return err
		}
		defer stmt.Close()
		for _, r := range results {
			mj, err := json.Marshal(r.Metrics)
			if err != nil {
				return err
			}
			cj, err := json.Marshal(r.Criteria)
			if err != nil {
				return err
			}
			if _, err := stmt.ExecContext(ctx, run.ID, r.Ticker, string(r.Verdict), r.Score,
				string(mj), string(cj)); err != nil {
				return err
			}
		}
		return nil
	})
}

// Runs lists stored runs, newest first.
func (s *Store) Runs(ctx context.Context, limit int) ([]model.Run, error) {
	if limit <= 0 {
		limit = 10
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, started_at, finished_at, profile, params_json, universe_size, passed_count, funnel_json
		 FROM runs ORDER BY started_at DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Run
	for rows.Next() {
		var r model.Run
		var started, finished int64
		var params, funnel sql.NullString
		if err := rows.Scan(&r.ID, &started, &finished, &r.Profile, &params,
			&r.UniverseSize, &r.PassedCount, &funnel); err != nil {
			return nil, err
		}
		r.StartedAt = time.Unix(started, 0).UTC()
		r.FinishedAt = time.Unix(finished, 0).UTC()
		r.ParamsJSON = params.String
		r.FunnelJSON = funnel.String
		out = append(out, r)
	}
	return out, rows.Err()
}

// Run returns one run header by id.
func (s *Store) Run(ctx context.Context, id string) (*model.Run, error) {
	var r model.Run
	var started, finished int64
	var params, funnel sql.NullString
	err := s.db.QueryRowContext(ctx,
		`SELECT id, started_at, finished_at, profile, params_json, universe_size, passed_count, funnel_json
		 FROM runs WHERE id = ?`, id).
		Scan(&r.ID, &started, &finished, &r.Profile, &params, &r.UniverseSize, &r.PassedCount, &funnel)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	r.StartedAt = time.Unix(started, 0).UTC()
	r.FinishedAt = time.Unix(finished, 0).UTC()
	r.ParamsJSON = params.String
	r.FunnelJSON = funnel.String
	return &r, nil
}

// LastRunBefore returns the newest run started strictly before the given time —
// the baseline the current run diffs against.
func (s *Store) LastRunBefore(ctx context.Context, t time.Time) (*model.Run, error) {
	var id string
	err := s.db.QueryRowContext(ctx,
		`SELECT id FROM runs WHERE started_at < ? ORDER BY started_at DESC LIMIT 1`, t.Unix()).Scan(&id)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return s.Run(ctx, id)
}

// RunResults returns every stored result of one run, keyed by ticker.
func (s *Store) RunResults(ctx context.Context, runID string) (map[string]RunResult, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT ticker, verdict, score, metrics_json, criteria_json FROM run_results WHERE run_id = ?`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]RunResult)
	for rows.Next() {
		var r RunResult
		var verdict, mj, cj string
		if err := rows.Scan(&r.Ticker, &verdict, &r.Score, &mj, &cj); err != nil {
			return nil, err
		}
		r.Verdict = model.Verdict(verdict)
		_ = json.Unmarshal([]byte(mj), &r.Metrics)
		_ = json.Unmarshal([]byte(cj), &r.Criteria)
		out[r.Ticker] = r
	}
	return out, rows.Err()
}

// FirstSeen returns the start date of the earliest run in which the ticker
// passed, formatted as YYYY-MM-DD.
func (s *Store) FirstSeen(ctx context.Context, ticker string) (string, error) {
	var started int64
	err := s.db.QueryRowContext(ctx,
		`SELECT MIN(r.started_at) FROM run_results rr JOIN runs r ON r.id = rr.run_id
		 WHERE rr.ticker = ? AND rr.verdict = ?`, ticker, string(model.VerdictPass)).Scan(&started)
	if err != nil || started == 0 {
		return "", nil
	}
	return time.Unix(started, 0).UTC().Format("2006-01-02"), nil
}

// LastSeen returns the start date of the latest run in which the ticker passed.
func (s *Store) LastSeen(ctx context.Context, ticker string, before time.Time) (string, error) {
	var started int64
	err := s.db.QueryRowContext(ctx,
		`SELECT MAX(r.started_at) FROM run_results rr JOIN runs r ON r.id = rr.run_id
		 WHERE rr.ticker = ? AND rr.verdict = ? AND r.started_at < ?`,
		ticker, string(model.VerdictPass), before.Unix()).Scan(&started)
	if err != nil || started == 0 {
		return "", nil
	}
	return time.Unix(started, 0).UTC().Format("2006-01-02"), nil
}

func (s *Store) tx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}
