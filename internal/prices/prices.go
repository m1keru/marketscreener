// Package prices fetches quotes. EDGAR has no prices, so market cap, P/E and
// P/B all depend on this package; a missing quote makes those criteria unknown
// rather than dropping the company.
//
// Sources are tried in the configured order. They fall into two kinds:
// batching sources, which answer for many tickers in one request, and
// per-ticker sources, used to fill the gaps a batch left behind.
package prices

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"github.com/m1ke/marketscreener/internal/model"
	"github.com/m1ke/marketscreener/internal/store"
)

// Source names accepted in the configuration.
const (
	// SourceYahoo batches 20 tickers per request through the spark endpoint
	// and falls back to the chart endpoint for anything left over. Neither
	// needs a key or a crumb.
	SourceYahoo = "yahoo"
	// SourceCboe reads Cboe's delayed quote CDN, one request per ticker.
	// Exchange-operated and keyless, it is the backstop when Yahoo throttles.
	SourceCboe = "cboe"
	// SourceStooq reads Stooq's CSV list endpoint. As of 2026-09 that endpoint
	// answers 404 for every symbol and the daily CSV sits behind a JavaScript
	// challenge, so it is off by default and kept only because it may still
	// work from other networks.
	SourceStooq = "stooq"
)

// Batch sizes, imposed by the providers.
const (
	// YahooSparkBatch is Yahoo's documented cap: more than 20 symbols in one
	// spark request returns 400.
	YahooSparkBatch = 20
	// StooqBatch is how many symbols Stooq's CSV endpoint accepts.
	StooqBatch = 50
)

// Stats counts quote coverage for the data_quality block.
type Stats struct {
	Requests atomic.Int64
	Cached   atomic.Int64
	Missing  atomic.Int64
	BySource sync.Map // source name -> *atomic.Int64
}

func (s *Stats) count(source string) {
	v, _ := s.BySource.LoadOrStore(source, &atomic.Int64{})
	v.(*atomic.Int64).Add(1)
}

// Source reports how many quotes came from one source.
func (s *Stats) Source(name string) int64 {
	if v, ok := s.BySource.Load(name); ok {
		return v.(*atomic.Int64).Load()
	}
	return 0
}

// Provider fetches quotes through a cache.
type Provider struct {
	httpc       *http.Client
	store       *store.Store
	ttl         time.Duration
	limiter     *rate.Limiter
	log         *slog.Logger
	Stats       *Stats
	Concurrency int

	sources   []string
	yahooBase string // chart endpoint, per ticker
	sparkBase string // spark endpoint, batched
	stooqBase string
	cboeBase  string

	degraded atomic.Bool
}

// Options configures a Provider.
type Options struct {
	Store       *store.Store
	TTL         time.Duration
	Logger      *slog.Logger
	HTTPClient  *http.Client
	Sources     []string
	Concurrency int
	// Endpoint overrides; tests point these at a local server.
	YahooBase string
	SparkBase string
	StooqBase string
	CboeBase  string
	// RequestsPerSecond bounds the whole provider. The quote sources publish
	// no limit; this keeps a full run polite.
	RequestsPerSecond float64
}

// New builds a Provider.
func New(o Options) *Provider {
	if o.TTL <= 0 {
		o.TTL = time.Hour
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.HTTPClient == nil {
		o.HTTPClient = &http.Client{Timeout: 30 * time.Second}
	}
	if o.YahooBase == "" {
		o.YahooBase = "https://query1.finance.yahoo.com/v8/finance/chart/"
	}
	if o.SparkBase == "" {
		o.SparkBase = "https://query1.finance.yahoo.com/v7/finance/spark"
	}
	if o.StooqBase == "" {
		o.StooqBase = "https://stooq.com/q/l/"
	}
	if o.CboeBase == "" {
		o.CboeBase = "https://cdn.cboe.com/api/global/delayed_quotes/quotes/"
	}
	if o.Concurrency <= 0 {
		o.Concurrency = 4
	}
	if len(o.Sources) == 0 {
		o.Sources = []string{SourceYahoo, SourceCboe}
	}
	if o.RequestsPerSecond <= 0 {
		o.RequestsPerSecond = 8
	}
	return &Provider{
		httpc:       o.HTTPClient,
		store:       o.Store,
		ttl:         o.TTL,
		limiter:     rate.NewLimiter(rate.Limit(o.RequestsPerSecond), 1),
		log:         o.Logger,
		Stats:       &Stats{},
		Concurrency: o.Concurrency,
		sources:     o.Sources,
		yahooBase:   o.YahooBase,
		sparkBase:   o.SparkBase,
		stooqBase:   o.StooqBase,
		cboeBase:    o.CboeBase,
	}
}

// Degraded reports that a batching source failed during the last call, so a
// coverage gap is an infrastructure problem and not a list of unlisted tickers.
func (p *Provider) Degraded() bool { return p.degraded.Load() }

func (p *Provider) uses(name string) bool {
	for _, s := range p.sources {
		if s == name {
			return true
		}
	}
	return false
}

// Quotes returns a quote per ticker. Tickers no source answers for are simply
// absent from the map; the caller turns that into an unknown criterion.
func (p *Provider) Quotes(ctx context.Context, tickers []string, refresh bool) map[string]model.Price {
	out := make(map[string]model.Price, len(tickers))
	var pending []string
	for _, t := range tickers {
		if !refresh && p.store != nil {
			if cached, err := p.store.Price(ctx, t, p.ttl); err == nil && cached != nil {
				out[t] = *cached
				p.Stats.Cached.Add(1)
				continue
			}
		}
		pending = append(pending, t)
	}

	var fresh []model.Price
	collect := func(got map[string]model.Price) {
		for t, q := range got {
			if _, seen := out[t]; seen {
				continue
			}
			out[t] = q
			fresh = append(fresh, q)
			p.Stats.count(q.Source)
		}
	}

	for _, source := range p.sources {
		missing := remaining(pending, out)
		if len(missing) == 0 || ctx.Err() != nil {
			break
		}
		switch source {
		case SourceYahoo:
			collect(p.batched(ctx, missing, YahooSparkBatch, p.yahooSpark, SourceYahoo))
			// Spark omits symbols it cannot resolve; the chart endpoint knows
			// a few of those, so it gets one pass over what is left.
			collect(p.perTicker(ctx, remaining(pending, out), p.yahooChart))
		case SourceStooq:
			collect(p.batched(ctx, missing, StooqBatch, p.stooqBatch, SourceStooq))
		case SourceCboe:
			collect(p.perTicker(ctx, missing, p.cboeOne))
		default:
			p.log.Warn("unknown quote source, skipped", "source", source)
		}
	}

	p.Stats.Missing.Add(int64(len(remaining(pending, out))))
	if p.store != nil && len(fresh) > 0 {
		if err := p.store.SavePrices(ctx, fresh); err != nil {
			p.log.Warn("cache prices", "err", err)
		}
	}
	return out
}

func remaining(pending []string, have map[string]model.Price) []string {
	var out []string
	for _, t := range pending {
		if _, ok := have[t]; !ok {
			out = append(out, t)
		}
	}
	return out
}

// batched runs a batching source over the ticker list in chunks.
func (p *Provider) batched(ctx context.Context, tickers []string, size int,
	fn func(context.Context, []string) (map[string]model.Price, error), source string) map[string]model.Price {

	out := make(map[string]model.Price, len(tickers))
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, p.Concurrency)
	failures := &atomic.Int64{}
	batches := 0

	for i := 0; i < len(tickers); i += size {
		if ctx.Err() != nil {
			break
		}
		batch := tickers[i:min(i+size, len(tickers))]
		batches++
		wg.Add(1)
		sem <- struct{}{}
		go func(batch []string) {
			defer wg.Done()
			defer func() { <-sem }()
			got, err := fn(ctx, batch)
			if err != nil {
				failures.Add(1)
				p.log.Debug("quote batch failed", "source", source, "n", len(batch), "err", err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			for t, q := range got {
				out[t] = q
			}
		}(batch)
	}
	wg.Wait()

	if batches > 0 && failures.Load() == int64(batches) {
		if !p.degraded.Swap(true) {
			p.log.Warn("every batch request to this quote source failed; coverage will depend on "+
				"the per-ticker fallbacks", "source", source, "batches", batches)
		}
	}
	return out
}

// perTicker runs a one-request-per-ticker source.
func (p *Provider) perTicker(ctx context.Context, tickers []string,
	fn func(context.Context, string) (*model.Price, error)) map[string]model.Price {

	out := make(map[string]model.Price, len(tickers))
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, p.Concurrency)
	for _, t := range tickers {
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(ticker string) {
			defer wg.Done()
			defer func() { <-sem }()
			q, err := fn(ctx, ticker)
			if err != nil || q == nil {
				return
			}
			mu.Lock()
			out[ticker] = *q
			mu.Unlock()
		}(t)
	}
	wg.Wait()
	return out
}

// --- Yahoo ---------------------------------------------------------------

// yahooSymbol maps an EDGAR ticker to Yahoo's symbol space: class shares are
// written with a dash in both (BRK-B).
func yahooSymbol(ticker string) string { return strings.ReplaceAll(ticker, ".", "-") }

type yahooMeta struct {
	Currency           string  `json:"currency"`
	Symbol             string  `json:"symbol"`
	RegularMarketPrice float64 `json:"regularMarketPrice"`
	RegularMarketTime  int64   `json:"regularMarketTime"`
}

// yahooSpark asks for up to 20 symbols in one request. Symbols it cannot
// resolve are omitted from the response rather than reported as an error.
func (p *Provider) yahooSpark(ctx context.Context, tickers []string) (map[string]model.Price, error) {
	syms := make([]string, 0, len(tickers))
	back := make(map[string]string, len(tickers))
	for _, t := range tickers {
		s := yahooSymbol(t)
		syms = append(syms, s)
		back[strings.ToUpper(s)] = t
	}
	url := fmt.Sprintf("%s?symbols=%s&range=1d&interval=1d", p.sparkBase, strings.Join(syms, ","))
	body, err := p.fetch(ctx, url)
	if err != nil {
		if strings.Contains(err.Error(), "HTTP 404") {
			return nil, nil // none of these symbols exist at Yahoo
		}
		return nil, err
	}
	defer body.Close()

	var resp struct {
		Spark struct {
			Result []struct {
				Symbol   string `json:"symbol"`
				Response []struct {
					Meta yahooMeta `json:"meta"`
				} `json:"response"`
			} `json:"result"`
		} `json:"spark"`
	}
	if err := json.NewDecoder(body).Decode(&resp); err != nil {
		return nil, err
	}
	out := make(map[string]model.Price, len(resp.Spark.Result))
	for _, r := range resp.Spark.Result {
		if len(r.Response) == 0 {
			continue
		}
		m := r.Response[0].Meta
		ticker, ok := back[strings.ToUpper(r.Symbol)]
		if !ok || m.RegularMarketPrice <= 0 {
			continue
		}
		out[ticker] = model.Price{
			Ticker: ticker, Price: m.RegularMarketPrice,
			AsOf: asOf(m.RegularMarketTime), Source: SourceYahoo,
		}
	}
	return out, nil
}

func (p *Provider) yahooChart(ctx context.Context, ticker string) (*model.Price, error) {
	url := fmt.Sprintf("%s%s?range=1d&interval=1d", p.yahooBase, yahooSymbol(ticker))
	body, err := p.fetchRetrying(ctx, url)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	var resp struct {
		Chart struct {
			Result []struct {
				Meta yahooMeta `json:"meta"`
			} `json:"result"`
		} `json:"chart"`
	}
	if err := json.NewDecoder(body).Decode(&resp); err != nil {
		return nil, err
	}
	if len(resp.Chart.Result) == 0 || resp.Chart.Result[0].Meta.RegularMarketPrice <= 0 {
		return nil, errors.New("yahoo: no quote")
	}
	m := resp.Chart.Result[0].Meta
	return &model.Price{Ticker: ticker, Price: m.RegularMarketPrice,
		AsOf: asOf(m.RegularMarketTime), Source: SourceYahoo}, nil
}

// --- Cboe ----------------------------------------------------------------

// cboeOne reads Cboe's delayed quote feed: an exchange-operated source that
// needs no key. Quotes are delayed about fifteen minutes, which is irrelevant
// for a screen whose horizon is years.
func (p *Provider) cboeOne(ctx context.Context, ticker string) (*model.Price, error) {
	url := p.cboeBase + strings.ReplaceAll(ticker, ".", "-") + ".json"
	body, err := p.fetch(ctx, url)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	var resp struct {
		Timestamp string `json:"timestamp"`
		Data      struct {
			Symbol       string  `json:"symbol"`
			CurrentPrice float64 `json:"current_price"`
			PrevDayClose float64 `json:"prev_day_close_price"`
		} `json:"data"`
	}
	if err := json.NewDecoder(body).Decode(&resp); err != nil {
		return nil, err
	}
	price := resp.Data.CurrentPrice
	if price <= 0 {
		price = resp.Data.PrevDayClose
	}
	if price <= 0 {
		return nil, errors.New("cboe: no quote")
	}
	asOfDate := resp.Timestamp
	if len(asOfDate) >= 10 {
		asOfDate = asOfDate[:10]
	}
	return &model.Price{Ticker: ticker, Price: price, AsOf: asOfDate, Source: SourceCboe}, nil
}

// --- Stooq ---------------------------------------------------------------

func stooqSymbol(ticker string) string {
	return strings.ToLower(strings.ReplaceAll(ticker, ".", "-")) + ".us"
}

func (p *Provider) stooqBatch(ctx context.Context, tickers []string) (map[string]model.Price, error) {
	syms := make([]string, 0, len(tickers))
	back := make(map[string]string, len(tickers))
	for _, t := range tickers {
		s := stooqSymbol(t)
		syms = append(syms, s)
		back[strings.ToUpper(s)] = t
	}
	url := fmt.Sprintf("%s?s=%s&f=sd2t2ohlcv&h&e=csv", p.stooqBase, strings.Join(syms, ","))
	body, err := p.fetch(ctx, url)
	if err != nil {
		return nil, err
	}
	defer body.Close()

	r := csv.NewReader(body)
	r.FieldsPerRecord = -1
	rows, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("stooq csv: %w", err)
	}
	out := make(map[string]model.Price)
	for i, row := range rows {
		if i == 0 || len(row) < 7 {
			continue // header
		}
		ticker, ok := back[strings.ToUpper(strings.TrimSpace(row[0]))]
		if !ok {
			continue
		}
		price, err := strconv.ParseFloat(strings.TrimSpace(row[6]), 64)
		if err != nil || price <= 0 {
			continue // "N/D": Stooq does not know this symbol
		}
		out[ticker] = model.Price{Ticker: ticker, Price: price,
			AsOf: strings.TrimSpace(row[1]), Source: SourceStooq}
	}
	return out, nil
}

// --- transport -----------------------------------------------------------

func asOf(unix int64) string {
	if unix <= 0 {
		return ""
	}
	return time.Unix(unix, 0).UTC().Format("2006-01-02")
}

func (p *Provider) fetch(ctx context.Context, url string) (io.ReadCloser, error) {
	if err := p.limiter.Wait(ctx); err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "marketscreener/2.0")
	req.Header.Set("Accept", "application/json,text/csv,*/*")
	p.Stats.Requests.Add(1)
	resp, err := p.httpc.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("%s: HTTP %d", url, resp.StatusCode)
	}
	return resp.Body, nil
}

// fetchRetrying backs off on 429, which the per-ticker endpoints return
// readily when they are carrying a whole run.
func (p *Provider) fetchRetrying(ctx context.Context, url string) (io.ReadCloser, error) {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		body, err := p.fetch(ctx, url)
		if err == nil {
			return body, nil
		}
		lastErr = err
		if !strings.Contains(err.Error(), "HTTP 429") {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Duration(1<<attempt) * time.Second):
		}
	}
	return nil, lastErr
}
