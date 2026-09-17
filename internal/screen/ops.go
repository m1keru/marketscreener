package screen

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/m1ke/marketscreener/internal/criteria"
	"github.com/m1ke/marketscreener/internal/metrics"
	"github.com/m1ke/marketscreener/internal/model"
)

// Resolve maps a ticker to a registrant, fetching the ticker file if the cache
// does not know it yet.
func (s *Screener) Resolve(ctx context.Context, ticker string) (model.Company, error) {
	ticker = strings.ToUpper(strings.TrimSpace(ticker))
	if ticker == "" {
		return model.Company{}, fmt.Errorf("ticker is required")
	}
	if c, err := s.Store.CompanyByTicker(ctx, ticker); err == nil && c != nil {
		return *c, nil
	}
	// The screening universe keeps one ticker per registrant, so a second share
	// class (BRK-B against BRK-A) is missing from it. Explain and history work
	// on any ticker EDGAR knows, so look the raw file up directly.
	entries, err := s.Edgar.CompanyTickers(ctx)
	if err != nil {
		return model.Company{}, err
	}
	for _, e := range entries {
		if e.Ticker == ticker {
			return model.Company{CIK: e.CIK, Ticker: e.Ticker, Title: e.Title, SIC: SICUnknown}, nil
		}
	}
	return model.Company{}, fmt.Errorf("ticker %s is not in EDGAR's company_tickers.json "+
		"(EDGAR covers US registrants only)", ticker)
}

// Explain evaluates one ticker in full and returns every number behind the
// verdict, including the tags and accession numbers, so any figure can be
// checked against the filing by hand.
func (s *Screener) Explain(ctx context.Context, ticker string, years int, refresh bool) (*ExplainResult, error) {
	c, err := s.Resolve(ctx, ticker)
	if err != nil {
		return nil, err
	}
	cfg := s.Cfg
	if years > 0 {
		cfg.Thresholds.HistoryYears = years
	}
	if c.SIC == SICUnknown || c.SIC == 0 {
		if sub, err := s.Edgar.Submissions(ctx, c.CIK); err == nil {
			c.SIC, c.SICDesc = sub.SICInt(), sub.SICDescription
			if sub.Name != "" {
				c.Title = sub.Name
			}
			if err := s.Store.SaveCompanies(ctx, []model.Company{c}); err != nil {
				s.log().Warn("cache company", "cik", c.CIK, "err", err)
			}
		} else {
			s.log().Warn("submissions", "cik", c.CIK, "err", err)
		}
	}
	facts, err := s.companyFacts(ctx, c.CIK, refresh)
	if err != nil {
		return nil, fmt.Errorf("companyfacts for %s: %w", ticker, err)
	}
	quotes := s.Prices.Quotes(ctx, []string{c.Ticker}, refresh)
	var price *model.Price
	if q, ok := quotes[c.Ticker]; ok {
		price = &q
	}
	m, series := metrics.Compute(metrics.Input{Facts: facts, Price: price, HistoryYears: cfg.Thresholds.HistoryYears})
	rs := criteria.Evaluate(c, m, series, cfg)
	return &ExplainResult{
		Ticker: c.Ticker, CIK: c.CIK, Name: c.Title, SIC: c.SIC, Sector: c.SICDesc,
		AsOf:               s.now().Format(time.RFC3339),
		Verdict:            criteria.Verdict(rs),
		Metrics:            m,
		Criteria:           rs,
		Series:             series,
		FiscalYearEndMonth: int(metrics.NewExtractor(facts).FiscalYearEndMonth()),
	}, nil
}

// Metric names accepted by History.
var historyFamilies = map[string]metrics.Family{
	"eps":       metrics.EPS,
	"dividends": metrics.DividendsPerShare,
	"revenue":   metrics.Revenue,
	"equity":    metrics.Equity,
	"debt":      metrics.LongTermDebt,
	"assets":    metrics.Assets,
}

// HistoryMetrics lists the accepted metric names, sorted.
func HistoryMetrics() []string {
	out := make([]string, 0, len(historyFamilies))
	for k := range historyFamilies {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// History returns one raw annual series for one ticker.
func (s *Screener) History(ctx context.Context, ticker, metric string, years int, refresh bool) (*HistoryResult, error) {
	fam, ok := historyFamilies[strings.ToLower(strings.TrimSpace(metric))]
	if !ok {
		return nil, fmt.Errorf("unknown metric %q: expected one of %s", metric, strings.Join(HistoryMetrics(), ", "))
	}
	c, err := s.Resolve(ctx, ticker)
	if err != nil {
		return nil, err
	}
	facts, err := s.companyFacts(ctx, c.CIK, refresh)
	if err != nil {
		return nil, fmt.Errorf("companyfacts for %s: %w", ticker, err)
	}
	e := metrics.NewExtractor(facts)
	series, tag := e.AnnualSeries(fam)
	if len(series) == 0 && fam.Name == metrics.DividendsPerShare.Name {
		series, tag = e.AnnualSeries(metrics.DividendsPaid)
		fam = metrics.DividendsPaid
	}
	if years <= 0 {
		years = s.Cfg.Thresholds.HistoryYears
	}
	if len(series) > years {
		series = series[len(series)-years:]
	}
	if series == nil {
		series = []model.YearValue{}
	}
	return &HistoryResult{
		Ticker: c.Ticker, CIK: c.CIK, Name: c.Title, Metric: metric,
		Tag: tag, Unit: fam.Unit, Values: series,
	}, nil
}

// Runs lists stored runs, newest first, and optionally diffs two of them.
func (s *Screener) Runs(ctx context.Context, limit int, compare []string) (*RunsResult, error) {
	runs, err := s.Store.Runs(ctx, limit)
	if err != nil {
		return nil, err
	}
	out := &RunsResult{Runs: []RunSummary{}}
	for _, r := range runs {
		out.Runs = append(out.Runs, RunSummary{
			RunID: r.ID, StartedAt: r.StartedAt.Format(time.RFC3339),
			FinishedAt: r.FinishedAt.Format(time.RFC3339), Profile: r.Profile,
			UniverseSize: r.UniverseSize, Passed: r.PassedCount, Params: r.ParamsJSON,
		})
	}
	if len(compare) == 2 {
		cmp, err := s.Compare(ctx, compare[0], compare[1])
		if err != nil {
			return nil, err
		}
		out.Compare = cmp
	} else if len(compare) != 0 {
		return nil, fmt.Errorf("compare takes exactly two run ids, got %d", len(compare))
	}
	return out, nil
}

// Funnel returns how many companies each criterion removed in a run. Without
// it, tuning a threshold is guesswork.
func (s *Screener) Funnel(ctx context.Context, runID string) (*FunnelResult, error) {
	var run *model.Run
	var err error
	if runID == "" {
		runs, err := s.Store.Runs(ctx, 1)
		if err != nil {
			return nil, err
		}
		if len(runs) == 0 {
			return nil, fmt.Errorf("no runs stored yet: run screen first")
		}
		run = &runs[0]
	} else {
		run, err = s.Store.Run(ctx, runID)
		if err != nil {
			return nil, err
		}
		if run == nil {
			return nil, fmt.Errorf("run %s not found", runID)
		}
	}
	steps := []FunnelStep{}
	if run.FunnelJSON != "" {
		if err := json.Unmarshal([]byte(run.FunnelJSON), &steps); err != nil {
			return nil, fmt.Errorf("stored funnel of run %s is unreadable: %w", run.ID, err)
		}
	}
	return &FunnelResult{
		RunID: run.ID, Profile: run.Profile, UniverseSize: run.UniverseSize,
		Passed: run.PassedCount, Steps: steps,
	}, nil
}
