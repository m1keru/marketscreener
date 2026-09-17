package screen_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/m1ke/marketscreener/internal/config"
	"github.com/m1ke/marketscreener/internal/edgar"
	"github.com/m1ke/marketscreener/internal/metrics"
	"github.com/m1ke/marketscreener/internal/model"
	"github.com/m1ke/marketscreener/internal/prices"
	"github.com/m1ke/marketscreener/internal/screen"
	"github.com/m1ke/marketscreener/internal/store"
)

// fakeCompany ties a fixture file to the identity EDGAR would report for it.
type fakeCompany struct {
	CIK     string
	Ticker  string
	Title   string
	SIC     int
	SICDesc string
	Fixture string
}

var fakeUniverse = []fakeCompany{
	{"0000000101", "GOOD", "GOODCO INC", 3559, "Special Industry Machinery", "pass_all.json"},
	{"0000000102", "CYC", "CYCLICO INC", 3559, "Special Industry Machinery", "negative_eps.json"},
	{"0000000103", "INS", "INSURECO INC", 6311, "Life Insurance", "financial_no_current.json"},
	{"0000000104", "FBK", "FALLBACKCO INC", 3559, "Special Industry Machinery", "fallback_tags.json"},
	{"0000000105", "JUN", "JUNECO INC", 3559, "Special Industry Machinery", "fiscal_year_june.json"},
	{"0000000106", "GAP", "GAPCO INC", 3559, "Special Industry Machinery", "dividend_gap.json"},
	// A warrant that must never reach the pipeline.
	{"0000000107", "GOOD-WS", "GOODCO INC WARRANT", 3559, "Special Industry Machinery", "pass_all.json"},
}

// fakeSEC serves company_tickers, submissions, companyfacts and frames built
// from the same fixtures, so the pipeline is exercised end to end offline.
type fakeSEC struct {
	t        *testing.T
	byCIK    map[string]fakeCompany
	facts    map[string][]model.Fact
	raw      map[string][]byte
	Requests int
}

func newFakeSEC(t *testing.T) *fakeSEC {
	f := &fakeSEC{t: t, byCIK: map[string]fakeCompany{}, facts: map[string][]model.Fact{}, raw: map[string][]byte{}}
	for _, c := range fakeUniverse {
		f.byCIK[c.CIK] = c
		data, err := os.ReadFile(filepath.Join("..", "..", "testdata", c.Fixture))
		if err != nil {
			t.Fatalf("read fixture %s: %v", c.Fixture, err)
		}
		var cf edgar.CompanyFacts
		if err := json.Unmarshal(data, &cf); err != nil {
			t.Fatalf("parse fixture %s: %v", c.Fixture, err)
		}
		f.raw[c.CIK] = data
		f.facts[c.CIK] = cf.Flatten(c.CIK)
	}
	return f
}

func (f *fakeSEC) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.Requests++
		p := r.URL.Path
		switch {
		case p == "/files/company_tickers.json":
			out := map[string]any{}
			for i, c := range fakeUniverse {
				out[fmt.Sprint(i)] = map[string]any{
					"cik_str": jsonNumber(c.CIK), "ticker": c.Ticker, "title": c.Title,
				}
			}
			writeJSON(w, out)
		case strings.HasPrefix(p, "/submissions/CIK"):
			cik := strings.TrimSuffix(strings.TrimPrefix(p, "/submissions/CIK"), ".json")
			c, ok := f.byCIK[cik]
			if !ok {
				http.NotFound(w, r)
				return
			}
			writeJSON(w, map[string]any{
				"cik": cik, "name": c.Title, "sic": fmt.Sprint(c.SIC), "sicDescription": c.SICDesc,
			})
		case strings.HasPrefix(p, "/api/xbrl/companyfacts/CIK"):
			cik := strings.TrimSuffix(strings.TrimPrefix(p, "/api/xbrl/companyfacts/CIK"), ".json")
			body, ok := f.raw[cik]
			if !ok {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.Write(body)
		case strings.HasPrefix(p, "/api/xbrl/frames/"):
			f.serveFrame(w, r)
		default:
			http.NotFound(w, r)
		}
	})
}

// serveFrame answers /api/xbrl/frames/{taxonomy}/{tag}/{unit}/{frame}.json by
// selecting the facts of every fixture that fall in that frame — the same way
// SEC does, including leaving off-calendar filers out of calendar frames.
func (f *fakeSEC) serveFrame(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(strings.TrimSuffix(r.URL.Path, ".json"), "/api/xbrl/frames/"), "/")
	if len(parts) != 4 {
		http.NotFound(w, r)
		return
	}
	taxonomy, tag, _, frame := parts[0], parts[1], parts[2], parts[3]
	qualified := tag
	if taxonomy != "us-gaap" {
		qualified = taxonomy + ":" + tag
	}
	instant := strings.HasSuffix(frame, "I")
	var year int
	var quarter int
	if instant {
		fmt.Sscanf(frame, "CY%dQ%dI", &year, &quarter)
	} else if strings.Contains(frame, "Q") {
		fmt.Sscanf(frame, "CY%dQ%d", &year, &quarter)
	} else {
		fmt.Sscanf(frame, "CY%d", &year)
	}

	var data []map[string]any
	for cik, facts := range f.facts {
		for _, fact := range facts {
			if fact.Tag != qualified {
				continue
			}
			match := false
			if instant {
				match = fact.Instant() && fact.End == quarterEnd(year, quarter)
			} else if quarter == 0 {
				match = !fact.Instant() &&
					fact.Start == fmt.Sprintf("%d-01-01", year) &&
					fact.End == fmt.Sprintf("%d-12-31", year)
			}
			if !match {
				continue
			}
			data = append(data, map[string]any{
				"cik": jsonNumber(cik), "end": fact.End, "start": fact.Start, "val": fact.Val,
				"accn": fact.Accn, "fy": fact.FY, "fp": fact.FP, "form": fact.Form, "filed": fact.Filed,
			})
		}
	}
	if len(data) == 0 {
		http.NotFound(w, r) // frame exists but holds nothing we know about
		return
	}
	writeJSON(w, map[string]any{"taxonomy": taxonomy, "tag": tag, "ccp": frame, "data": data})
}

func quarterEnd(year, quarter int) string {
	switch quarter {
	case 1:
		return fmt.Sprintf("%d-03-31", year)
	case 2:
		return fmt.Sprintf("%d-06-30", year)
	case 3:
		return fmt.Sprintf("%d-09-30", year)
	default:
		return fmt.Sprintf("%d-12-31", year)
	}
}

func jsonNumber(cik string) int {
	n := 0
	fmt.Sscanf(strings.TrimLeft(cik, "0"), "%d", &n)
	return n
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// fakeQuotes serves a Stooq-shaped CSV for every requested symbol.
func fakeQuotes(t *testing.T, price func(ticker string) float64) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		symbols := strings.Split(r.URL.Query().Get("s"), ",")
		w.Header().Set("Content-Type", "text/csv")
		fmt.Fprintln(w, "Symbol,Date,Time,Open,High,Low,Close,Volume")
		for _, s := range symbols {
			ticker := strings.ToUpper(strings.TrimSuffix(s, ".us"))
			p := price(ticker)
			if p <= 0 {
				fmt.Fprintf(w, "%s,N/D,N/D,N/D,N/D,N/D,N/D,N/D\n", strings.ToUpper(s))
				continue
			}
			fmt.Fprintf(w, "%s,2025-09-15,22:00:00,%.2f,%.2f,%.2f,%.2f,1000000\n",
				strings.ToUpper(s), p, p, p, p)
		}
	}))
}

func newScreener(t *testing.T, sec *httptest.Server, quotes *httptest.Server, now time.Time) *screen.Screener {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "msc.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })

	cfg := config.Default()
	cfg.UserAgentContact = "tester@example.com"
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	ec := edgar.New(edgar.Options{
		UserAgent: cfg.UserAgent(), RequestsPerSecond: 10, MaxRetries: 2,
		Timeout: 5 * time.Second, Logger: log, DataHost: sec.URL, WWWHost: sec.URL,
	})
	pp := prices.New(prices.Options{
		Store: st, TTL: time.Hour, Logger: log,
		Sources:   []string{prices.SourceStooq},
		StooqBase: quotes.URL + "/q/l/",
		// Unreachable on purpose: the fake market speaks the Stooq CSV dialect,
		// and no test may fall through to a real quote source.
		SparkBase:         "http://127.0.0.1:1/spark",
		YahooBase:         "http://127.0.0.1:1/chart/",
		CboeBase:          "http://127.0.0.1:1/cboe/",
		RequestsPerSecond: 1000,
	})
	return &screen.Screener{
		Cfg: cfg, Edgar: ec, Prices: pp, Store: st, Log: log, Workers: 4,
		Now: func() time.Time { return now },
	}
}

const goldenFile = "screen_golden.json"

// TestScreenContract pins the shape of the screen response. The agent is built
// against this JSON, so any change to it must break this test on purpose.
func TestScreenContract(t *testing.T) {
	sec := httptest.NewServer(newFakeSEC(t).handler())
	defer sec.Close()
	quotes := fakeQuotes(t, func(string) float64 { return 40 })
	defer quotes.Close()

	now := time.Date(2025, 9, 15, 12, 0, 0, 0, time.UTC)
	s := newScreener(t, sec, quotes, now)

	res, err := s.Screen(context.Background(), screen.Params{})
	if err != nil {
		t.Fatalf("screen: %v", err)
	}
	normalise(res)

	got, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	path := filepath.Join("..", "..", "testdata", goldenFile)
	if os.Getenv("UPDATE_GOLDEN") == "1" {
		if err := os.WriteFile(path, append(got, '\n'), 0o644); err != nil {
			t.Fatalf("write golden: %v", err)
		}
		t.Log("golden file updated")
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden (run with UPDATE_GOLDEN=1 to create it): %v", err)
	}
	if strings.TrimSpace(string(want)) != strings.TrimSpace(string(got)) {
		t.Errorf("screen response changed.\n--- want ---\n%s\n--- got ---\n%s", want, got)
	}
}

// normalise replaces the fields that legitimately change between runs.
func normalise(r *screen.Result) {
	r.RunID = "RUN_ID"
	r.AsOf = "AS_OF"
	r.DataQuality.DurationMs = 0
	r.DataQuality.SECRequests = 0
	r.DataQuality.SECRetries = 0
	r.DataQuality.PriceRequests = 0
	r.DataQuality.CacheHits = 0
}

func TestScreenVerdicts(t *testing.T) {
	sec := httptest.NewServer(newFakeSEC(t).handler())
	defer sec.Close()
	quotes := fakeQuotes(t, func(string) float64 { return 40 })
	defer quotes.Close()

	s := newScreener(t, sec, quotes, time.Date(2025, 9, 15, 12, 0, 0, 0, time.UTC))
	res, err := s.Screen(context.Background(), screen.Params{})
	if err != nil {
		t.Fatalf("screen: %v", err)
	}

	passed := map[string]bool{}
	for _, c := range res.Candidates {
		passed[c.Ticker] = true
	}
	for _, want := range []string{"GOOD", "FBK", "JUN"} {
		if !passed[want] {
			t.Errorf("%s should have passed every criterion; candidates: %v", want, tickers(res))
		}
	}
	for _, unwanted := range []string{"CYC", "GAP", "INS", "GOOD-WS"} {
		if passed[unwanted] {
			t.Errorf("%s must not be a candidate", unwanted)
		}
	}
	// The warrant must never even enter the universe.
	if res.UniverseSize != len(fakeUniverse)-1 {
		t.Errorf("universe = %d, want %d (the -WS ticker is filtered out)", res.UniverseSize, len(fakeUniverse)-1)
	}
	// The insurer failed nothing: it was excluded by sector, not by a metric.
	for _, i := range res.Incomplete {
		if i.Ticker == "INS" {
			t.Error("INS is excluded by SIC, it should not appear in incomplete[]")
		}
	}
}

// TestFunnelAndDiff covers the second run: the reason a ticker dropped out is
// computed from this run's numbers, never narrated.
func TestFunnelAndDiff(t *testing.T) {
	sec := httptest.NewServer(newFakeSEC(t).handler())
	defer sec.Close()

	price := 40.0
	quotes := fakeQuotes(t, func(ticker string) float64 {
		if ticker == "FBK" {
			return price
		}
		return 40
	})
	defer quotes.Close()

	first := time.Date(2025, 9, 15, 12, 0, 0, 0, time.UTC)
	s := newScreener(t, sec, quotes, first)
	if _, err := s.Screen(context.Background(), screen.Params{}); err != nil {
		t.Fatalf("first screen: %v", err)
	}

	// FBK triples in price: its P/E leaves the threshold behind.
	price = 300
	s.Now = func() time.Time { return first.Add(24 * time.Hour) }
	res, err := s.Screen(context.Background(), screen.Params{Refresh: true})
	if err != nil {
		t.Fatalf("second screen: %v", err)
	}

	var dropped *screen.DroppedEntry
	for i := range res.Diff.Dropped {
		if res.Diff.Dropped[i].Ticker == "FBK" {
			dropped = &res.Diff.Dropped[i]
		}
	}
	if dropped == nil {
		t.Fatalf("FBK should be reported as dropped, diff = %+v", res.Diff)
	}
	if dropped.Criterion != "graham.6.pe" {
		t.Errorf("drop criterion = %q, want graham.6.pe", dropped.Criterion)
	}
	if dropped.Was == nil || dropped.Now == nil {
		t.Fatalf("drop must carry both values, got was=%v now=%v", dropped.Was, dropped.Now)
	}
	if *dropped.Now <= *dropped.Was {
		t.Errorf("P/E should have risen: was %.2f, now %.2f", *dropped.Was, *dropped.Now)
	}
	if dropped.LastSeen == "" {
		t.Error("drop should say when the ticker was last a candidate")
	}

	f, err := s.Funnel(context.Background(), "")
	if err != nil {
		t.Fatalf("funnel: %v", err)
	}
	if len(f.Steps) == 0 {
		t.Fatal("funnel has no steps")
	}
	var sawFinancial bool
	for _, st := range f.Steps {
		if st.Criterion == "universe.financial" && st.Dropped == 1 {
			sawFinancial = true
		}
	}
	if !sawFinancial {
		t.Errorf("funnel should record the insurer excluded by sector: %+v", f.Steps)
	}
}

func TestExplainAndHistory(t *testing.T) {
	sec := httptest.NewServer(newFakeSEC(t).handler())
	defer sec.Close()
	quotes := fakeQuotes(t, func(string) float64 { return 40 })
	defer quotes.Close()

	s := newScreener(t, sec, quotes, time.Date(2025, 9, 15, 12, 0, 0, 0, time.UTC))

	ex, err := s.Explain(context.Background(), "jun", 10, false)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	if ex.FiscalYearEndMonth != 6 {
		t.Errorf("fiscal year end month = %d, want 6", ex.FiscalYearEndMonth)
	}
	if len(ex.Series.EPS) != 10 {
		t.Errorf("EPS series has %d years, want 10", len(ex.Series.EPS))
	}
	if ex.Verdict != model.VerdictPass {
		t.Errorf("verdict = %s, want pass", ex.Verdict)
	}
	for _, c := range ex.Criteria {
		if c.Status == model.Unknown && c.Reason == "" {
			t.Errorf("criterion %s unknown without a reason", c.ID)
		}
	}

	h, err := s.History(context.Background(), "GOOD", "dividends", 10, false)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(h.Values) != 10 {
		t.Errorf("dividend series has %d years, want 10", len(h.Values))
	}
	if h.Tag != "CommonStockDividendsPerShareDeclared" {
		t.Errorf("tag = %q", h.Tag)
	}
	if _, err := s.History(context.Background(), "GOOD", "ebitda", 10, false); err == nil {
		t.Error("an unknown metric name should be rejected")
	}
}

func tickers(r *screen.Result) []string {
	var out []string
	for _, c := range r.Candidates {
		out = append(out, c.Ticker)
	}
	return out
}

// TestOnlyUsefulFactsAreCached: companyfacts documents carry tens of thousands
// of facts per registrant. Caching the ones no criterion reads turned the
// database into gigabytes within a single run.
func TestOnlyUsefulFactsAreCached(t *testing.T) {
	sec := httptest.NewServer(newFakeSEC(t).handler())
	defer sec.Close()
	quotes := fakeQuotes(t, func(string) float64 { return 40 })
	defer quotes.Close()

	s := newScreener(t, sec, quotes, time.Date(2025, 9, 15, 12, 0, 0, 0, time.UTC))
	if _, err := s.Explain(context.Background(), "GOOD", 10, false); err != nil {
		t.Fatalf("explain: %v", err)
	}

	rows, err := s.Store.DB().QueryContext(context.Background(),
		`SELECT DISTINCT tag FROM facts`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	known := metrics.KnownTags()
	var cached int
	for rows.Next() {
		var tag string
		if err := rows.Scan(&tag); err != nil {
			t.Fatalf("scan: %v", err)
		}
		cached++
		if !known[tag] {
			t.Errorf("tag %q was cached but no criterion can read it", tag)
		}
	}
	if cached == 0 {
		t.Fatal("nothing was cached at all")
	}
}
