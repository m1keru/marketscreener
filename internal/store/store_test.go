package store_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/m1ke/marketscreener/internal/model"
	"github.com/m1ke/marketscreener/internal/store"
)

func open(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "msc.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func TestFactCacheAndTTL(t *testing.T) {
	ctx := context.Background()
	st := open(t)

	facts := []model.Fact{
		{CIK: "0000000101", Tag: "EarningsPerShareBasic", Unit: "USD/shares",
			Start: "2024-01-01", End: "2024-12-31", FY: 2024, FP: "FY", Form: "10-K", Val: 3.6, Accn: "a1"},
		{CIK: "0000000101", Tag: "EarningsPerShareBasic", Unit: "USD/shares",
			Start: "2023-01-01", End: "2023-12-31", FY: 2023, FP: "FY", Form: "10-K", Val: 3.4, Accn: "a0"},
	}
	if err := st.SaveFacts(ctx, facts); err != nil {
		t.Fatalf("save facts: %v", err)
	}
	// Re-saving the same primary key must update, not duplicate.
	if err := st.SaveFacts(ctx, facts); err != nil {
		t.Fatalf("resave facts: %v", err)
	}
	got, err := st.Facts(ctx, "0000000101")
	if err != nil {
		t.Fatalf("read facts: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d facts, want 2 (upsert, not insert)", len(got))
	}

	latest, err := st.LatestByTag(ctx, "EarningsPerShareBasic")
	if err != nil {
		t.Fatalf("latest by tag: %v", err)
	}
	if len(latest) != 1 || latest[0].End != "2024-12-31" {
		t.Errorf("latest by tag = %+v, want only the 2024 fact", latest)
	}

	if fresh, _ := st.Fresh(ctx, "facts:0000000101", time.Hour); fresh {
		t.Error("nothing was marked fetched yet")
	}
	if err := st.MarkFetched(ctx, "facts:0000000101"); err != nil {
		t.Fatalf("mark fetched: %v", err)
	}
	if fresh, _ := st.Fresh(ctx, "facts:0000000101", time.Hour); !fresh {
		t.Error("entry should be fresh inside the TTL")
	}
	if fresh, _ := st.Fresh(ctx, "facts:0000000101", time.Nanosecond); fresh {
		t.Error("entry should be stale outside the TTL")
	}
}

func TestPriceTTL(t *testing.T) {
	ctx := context.Background()
	st := open(t)
	if err := st.SavePrices(ctx, []model.Price{{Ticker: "GOOD", Price: 40, AsOf: "2025-09-15", Source: "stooq"}}); err != nil {
		t.Fatalf("save prices: %v", err)
	}
	p, err := st.Price(ctx, "GOOD", time.Hour)
	if err != nil || p == nil {
		t.Fatalf("price = %v, %v; want a cached quote", p, err)
	}
	if p.Price != 40 {
		t.Errorf("price = %v, want 40", p.Price)
	}
	stale, err := st.Price(ctx, "GOOD", time.Nanosecond)
	if err != nil {
		t.Fatalf("price: %v", err)
	}
	if stale != nil {
		t.Error("a quote older than the TTL must be reported as absent")
	}
}

func TestRunsRoundTrip(t *testing.T) {
	ctx := context.Background()
	st := open(t)
	start := time.Now().Add(-time.Hour).Truncate(time.Second)

	run := model.Run{
		ID: "run-1", StartedAt: start, FinishedAt: start.Add(time.Minute),
		Profile: "graham_defensive", UniverseSize: 4712, PassedCount: 1,
		FunnelJSON: `[{"stage":"1","criterion":"graham.1.size","dropped":3,"remaining":1}]`,
	}
	pe := 11.7
	results := []store.RunResult{{
		Ticker: "GOOD", Verdict: model.VerdictPass, Score: 0.54,
		Metrics:  model.Metrics{PE3YAvg: &pe},
		Criteria: []model.CriterionResult{{ID: "graham.6.pe", Status: model.Pass, Value: &pe}},
	}}
	if err := st.SaveRun(ctx, run, results); err != nil {
		t.Fatalf("save run: %v", err)
	}

	runs, err := st.Runs(ctx, 10)
	if err != nil || len(runs) != 1 {
		t.Fatalf("runs = %v, %v; want one row", runs, err)
	}
	if runs[0].FunnelJSON == "" {
		t.Error("funnel must survive the round trip")
	}

	rows, err := st.RunResults(ctx, "run-1")
	if err != nil {
		t.Fatalf("run results: %v", err)
	}
	row, ok := rows["GOOD"]
	if !ok {
		t.Fatal("GOOD missing from the stored results")
	}
	if row.Metrics.PE3YAvg == nil || *row.Metrics.PE3YAvg != pe {
		t.Errorf("metrics did not survive the round trip: %+v", row.Metrics)
	}
	if len(row.Criteria) != 1 || row.Criteria[0].ID != "graham.6.pe" {
		t.Errorf("criteria did not survive the round trip: %+v", row.Criteria)
	}

	first, err := st.FirstSeen(ctx, "GOOD")
	if err != nil {
		t.Fatalf("first seen: %v", err)
	}
	if first != start.UTC().Format("2006-01-02") {
		t.Errorf("first seen = %q, want %q", first, start.UTC().Format("2006-01-02"))
	}

	prev, err := st.LastRunBefore(ctx, time.Now())
	if err != nil || prev == nil || prev.ID != "run-1" {
		t.Errorf("last run before now = %v, %v", prev, err)
	}
}

// TestFullYearAndFourthQuarterCoexist is the regression test for a key that
// lost data: a 10-K reports both the full year and its fourth quarter with the
// same period end and accession number. A key without the period start keeps
// only whichever row was written last, and which one that is depends on map
// ordering — so the same input produced different P/E values between runs.
func TestFullYearAndFourthQuarterCoexist(t *testing.T) {
	ctx := context.Background()
	st := open(t)

	const cik, tag, accn = "0000912767", "EarningsPerShareBasic", "0001104659-26-019567"
	facts := []model.Fact{
		{CIK: cik, Tag: tag, Unit: "USD/shares", Start: "2024-12-29", End: "2025-12-27",
			Val: 5.00, Form: "10-K", Accn: accn}, // full year
		{CIK: cik, Tag: tag, Unit: "USD/shares", Start: "2025-09-28", End: "2025-12-27",
			Val: 0.70, Form: "10-K", Accn: accn}, // fourth quarter
	}
	if err := st.SaveFacts(ctx, facts); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err := st.FactsByTag(ctx, cik, tag)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d facts, want both the year and the quarter: %+v", len(got), got)
	}
	var sawYear bool
	for _, f := range got {
		if f.Start == "2024-12-29" && f.Val == 5.00 {
			sawYear = true
		}
	}
	if !sawYear {
		t.Error("the full-year fact was overwritten by the quarterly one")
	}
}

// TestMigrationRebuildsFactCacheKeepingRuns: an existing database from before
// the key fix must lose its fact cache and keep its run history.
func TestMigrationRebuildsFactCacheKeepingRuns(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "old.db")

	old, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open raw: %v", err)
	}
	if _, err := old.Exec(`
		CREATE TABLE facts (cik TEXT, tag TEXT, unit TEXT, period_end TEXT,
		  period_start TEXT, fy INTEGER, fp TEXT, form TEXT, val REAL, accn TEXT, filed TEXT,
		  fetched_at INTEGER, PRIMARY KEY (cik, tag, unit, period_end, accn));
		CREATE TABLE fetches (key TEXT PRIMARY KEY, fetched_at INTEGER);
		CREATE TABLE runs (id TEXT PRIMARY KEY, started_at INTEGER, finished_at INTEGER,
		  profile TEXT, params_json TEXT, universe_size INTEGER, passed_count INTEGER, funnel_json TEXT);
		INSERT INTO facts VALUES ('1','EarningsPerShareBasic','USD/shares','2025-12-27','2024-12-29',2025,'FY','10-K',5.0,'a','2026-02-01',1);
		INSERT INTO fetches VALUES ('facts:1', 1), ('tickers', 1);
		INSERT INTO runs VALUES ('run-1', 1, 2, 'graham_defensive', '{}', 10, 1, '[]');`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	old.Close()

	st, err := store.Open(path)
	if err != nil {
		t.Fatalf("open migrated: %v", err)
	}
	defer st.Close()

	runs, err := st.Runs(ctx, 10)
	if err != nil || len(runs) != 1 {
		t.Fatalf("runs = %v, %v; the history must survive the migration", runs, err)
	}
	facts, err := st.Facts(ctx, "1")
	if err != nil {
		t.Fatalf("facts: %v", err)
	}
	if len(facts) != 0 {
		t.Errorf("the fact cache should have been dropped, got %d rows", len(facts))
	}
	fresh, err := st.Fresh(ctx, "facts:1", time.Hour)
	if err != nil {
		t.Fatalf("fresh: %v", err)
	}
	if fresh {
		t.Error("the fetch marker must be cleared, or the emptied cache is never refilled")
	}
}
