package prices_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m1ke/marketscreener/internal/prices"
	"github.com/m1ke/marketscreener/internal/store"
)

func newStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "msc.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

// fakeMarket serves all four endpoints. Every test points the provider at it,
// so no test can reach a real quote source.
type fakeMarket struct {
	Spark, Chart, Cboe, Stooq atomic.Int64
	LargestSparkBatch         atomic.Int64
	// known prices; a ticker absent here is unknown to every source
	prices map[string]float64
	// sparkKnows limits which tickers the batch endpoint resolves
	sparkKnows map[string]bool
	// cboeOnly are tickers no Yahoo endpoint knows about
	cboeOnly   map[string]bool
	sparkFails bool
}

func (m *fakeMarket) server(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/spark"):
			m.Spark.Add(1)
			if m.sparkFails {
				http.Error(w, "boom", http.StatusInternalServerError)
				return
			}
			syms := strings.Split(r.URL.Query().Get("symbols"), ",")
			if int64(len(syms)) > m.LargestSparkBatch.Load() {
				m.LargestSparkBatch.Store(int64(len(syms)))
			}
			if len(syms) > prices.YahooSparkBatch {
				http.Error(w, `{"spark":{"result":null,"error":{"code":"Bad Request"}}}`, http.StatusBadRequest)
				return
			}
			var result []map[string]any
			for _, s := range syms {
				p, ok := m.prices[s]
				if !ok || (m.sparkKnows != nil && !m.sparkKnows[s]) {
					continue // spark silently omits what it cannot resolve
				}
				result = append(result, map[string]any{
					"symbol": s,
					"response": []map[string]any{{"meta": map[string]any{
						"symbol": s, "currency": "USD",
						"regularMarketPrice": p, "regularMarketTime": 1757980800,
					}}},
				})
			}
			if len(result) == 0 {
				http.Error(w, `{"spark":{"result":null}}`, http.StatusNotFound)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"spark": map[string]any{"result": result}})

		case strings.HasPrefix(r.URL.Path, "/chart/"):
			m.Chart.Add(1)
			sym := strings.TrimPrefix(r.URL.Path, "/chart/")
			p, ok := m.prices[sym]
			if ok && m.cboeOnly[sym] {
				ok = false
			}
			if !ok {
				_ = json.NewEncoder(w).Encode(map[string]any{"chart": map[string]any{"result": nil}})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"chart": map[string]any{
				"result": []map[string]any{{"meta": map[string]any{
					"regularMarketPrice": p, "regularMarketTime": 1757980800, "currency": "USD",
				}}}}})

		case strings.HasPrefix(r.URL.Path, "/cboe/"):
			m.Cboe.Add(1)
			sym := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/cboe/"), ".json")
			p, ok := m.prices[sym]
			if !ok {
				http.NotFound(w, r)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"timestamp": "2025-09-15 21:16:59",
				"data":      map[string]any{"symbol": sym, "current_price": p},
			})

		case strings.HasPrefix(r.URL.Path, "/stooq"):
			m.Stooq.Add(1)
			fmt.Fprintln(w, "Symbol,Date,Time,Open,High,Low,Close,Volume")
			for _, s := range strings.Split(r.URL.Query().Get("s"), ",") {
				ticker := strings.ToUpper(strings.TrimSuffix(s, ".us"))
				p, ok := m.prices[ticker]
				if !ok {
					fmt.Fprintf(w, "%s,N/D,N/D,N/D,N/D,N/D,N/D,N/D\n", strings.ToUpper(s))
					continue
				}
				fmt.Fprintf(w, "%s,2025-09-15,22:00:00,%.2f,%.2f,%.2f,%.2f,1000\n",
					strings.ToUpper(s), p, p, p, p)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newProvider(t *testing.T, srv *httptest.Server, st *store.Store, sources ...string) *prices.Provider {
	t.Helper()
	return prices.New(prices.Options{
		Store: st, TTL: time.Hour, Sources: sources,
		SparkBase:         srv.URL + "/spark",
		YahooBase:         srv.URL + "/chart/",
		CboeBase:          srv.URL + "/cboe/",
		StooqBase:         srv.URL + "/stooq",
		RequestsPerSecond: 1000,
	})
}

// TestYahooBatchThenFallbacks: the batch source answers for what it knows, the
// per-ticker endpoints pick up the rest, and the next source in the list gets
// what is still missing.
func TestYahooBatchThenFallbacks(t *testing.T) {
	// ODD is known to the chart endpoint but not to spark; CBOEONLY only to Cboe.
	m := &fakeMarket{
		prices:     map[string]float64{"KO": 88.08, "PEP": 140.10, "ODD": 12.34, "CBOEONLY": 5.55},
		sparkKnows: map[string]bool{"KO": true, "PEP": true},
		cboeOnly:   map[string]bool{"CBOEONLY": true},
	}
	srv := m.server(t)

	p := newProvider(t, srv, newStore(t), prices.SourceYahoo, prices.SourceCboe)
	got := p.Quotes(context.Background(), []string{"KO", "PEP", "ODD", "CBOEONLY", "GHOST"}, false)

	if len(got) != 4 {
		t.Fatalf("got %d quotes, want 4: %+v", len(got), got)
	}
	if got["KO"].Price != 88.08 || got["KO"].Source != prices.SourceYahoo {
		t.Errorf("KO = %+v, want the spark batch price", got["KO"])
	}
	if got["ODD"].Price != 12.34 {
		t.Errorf("ODD = %+v, want the chart fallback price", got["ODD"])
	}
	if got["CBOEONLY"].Source != prices.SourceCboe {
		t.Errorf("CBOEONLY = %+v, want the Cboe fallback", got["CBOEONLY"])
	}
	if _, ok := got["GHOST"]; ok {
		t.Error("a ticker nobody quotes must be absent, not zero")
	}
	if p.Stats.Missing.Load() != 1 {
		t.Errorf("missing counter = %d, want 1", p.Stats.Missing.Load())
	}
	if m.Spark.Load() != 1 {
		t.Errorf("spark requests = %d, want 1 batch for 5 tickers", m.Spark.Load())
	}
}

// TestSparkBatchSizeIsRespected: more than 20 symbols per spark request is a
// 400 from Yahoo, so the provider must chunk.
func TestSparkBatchSizeIsRespected(t *testing.T) {
	m := &fakeMarket{prices: map[string]float64{}}
	var tickers []string
	for i := 0; i < 45; i++ {
		tk := fmt.Sprintf("T%02d", i)
		tickers = append(tickers, tk)
		m.prices[tk] = float64(i) + 1
	}
	srv := m.server(t)
	p := newProvider(t, srv, newStore(t), prices.SourceYahoo)

	got := p.Quotes(context.Background(), tickers, false)
	if len(got) != len(tickers) {
		t.Fatalf("got %d quotes, want %d", len(got), len(tickers))
	}
	if n := m.LargestSparkBatch.Load(); n > int64(prices.YahooSparkBatch) {
		t.Errorf("largest batch was %d symbols, the endpoint caps at %d", n, prices.YahooSparkBatch)
	}
	if m.Spark.Load() != 3 {
		t.Errorf("spark requests = %d, want 3 batches for 45 tickers", m.Spark.Load())
	}
	if m.Chart.Load() != 0 {
		t.Errorf("chart requests = %d, want none when the batch answered everything", m.Chart.Load())
	}
}

// TestDegradedIsReported: when the batching source fails outright, the run must
// say so rather than present thin coverage as a market fact.
func TestDegradedIsReported(t *testing.T) {
	m := &fakeMarket{prices: map[string]float64{"KO": 88.08}, sparkFails: true}
	srv := m.server(t)
	p := newProvider(t, srv, newStore(t), prices.SourceYahoo, prices.SourceCboe)

	got := p.Quotes(context.Background(), []string{"KO"}, false)
	if !p.Degraded() {
		t.Error("a total batch failure must set the degraded flag")
	}
	if got["KO"].Price != 88.08 {
		t.Errorf("the fallbacks should still have found KO, got %+v", got["KO"])
	}
}

func TestCacheAndRefresh(t *testing.T) {
	m := &fakeMarket{prices: map[string]float64{"KO": 88.08}, sparkKnows: map[string]bool{"KO": true}}
	srv := m.server(t)
	st := newStore(t)
	p := newProvider(t, srv, st, prices.SourceYahoo)

	p.Quotes(context.Background(), []string{"KO"}, false)
	before := m.Spark.Load()
	p.Quotes(context.Background(), []string{"KO"}, false)
	if m.Spark.Load() != before {
		t.Error("the second read inside the TTL must come from the cache")
	}
	if p.Stats.Cached.Load() == 0 {
		t.Error("cache hits are not counted")
	}
	p.Quotes(context.Background(), []string{"KO"}, true)
	if m.Spark.Load() != before+1 {
		t.Error("refresh must bypass the cache")
	}
}

// TestStooqStillWorksWhenEnabled keeps the original source usable for networks
// where its CSV endpoint still answers.
func TestStooqStillWorksWhenEnabled(t *testing.T) {
	m := &fakeMarket{prices: map[string]float64{"KO": 88.08}}
	srv := m.server(t)
	p := newProvider(t, srv, newStore(t), prices.SourceStooq)

	got := p.Quotes(context.Background(), []string{"KO", "GHOST"}, false)
	if got["KO"].Source != prices.SourceStooq || got["KO"].Price != 88.08 {
		t.Errorf("KO = %+v, want the stooq price", got["KO"])
	}
	if _, ok := got["GHOST"]; ok {
		t.Error("N/D must not become a quote")
	}
	if m.Spark.Load() != 0 || m.Cboe.Load() != 0 {
		t.Error("only the configured source may be contacted")
	}
}
