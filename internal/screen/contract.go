// Package screen runs the three-stage pipeline and owns the shapes the MCP
// tools and the CLI both return. Changing a field here changes the contract the
// agent depends on.
package screen

import "github.com/m1ke/marketscreener/internal/model"

// Params are the inputs of the screen operation.
type Params struct {
	Profile           string   `json:"profile,omitempty"`
	MinMarketCapUSD   *float64 `json:"min_market_cap_usd,omitempty"`
	IncludeFinancials *bool    `json:"include_financials,omitempty"`
	MaxResults        int      `json:"max_results,omitempty"`
	Refresh           bool     `json:"refresh,omitempty"`
	// NoStore runs the pipeline without recording the run. It exists for cache
	// warming: a stored warm-up run would become the baseline of the next
	// diff, and the day-over-day comparison would silently go empty.
	NoStore bool `json:"-"`
}

// Candidate is one company that passed every criterion.
type Candidate struct {
	Ticker string `json:"ticker"`
	CIK    string `json:"cik"`
	Name   string `json:"name"`
	SIC    int    `json:"sic"`
	Sector string `json:"sector"`

	Price       *float64 `json:"price,omitempty"`
	PriceAsOf   string   `json:"price_as_of,omitempty"`
	PriceSource string   `json:"price_source,omitempty"`
	MarketCap   *float64 `json:"market_cap,omitempty"`

	PE3YAvg        *float64 `json:"pe_3y_avg,omitempty"`
	PB             *float64 `json:"pb,omitempty"`
	GrahamNumber   *float64 `json:"graham_number,omitempty"`
	MarginOfSafety *float64 `json:"margin_of_safety,omitempty"`

	CurrentRatio    *float64 `json:"current_ratio,omitempty"`
	LTDToWorkingCap *float64 `json:"ltd_to_working_capital,omitempty"`
	LTDToEquity     *float64 `json:"ltd_to_equity,omitempty"`

	EPSPositiveYears *int     `json:"eps_positive_years,omitempty"`
	DividendYears    *int     `json:"dividend_years,omitempty"`
	EPSGrowth10YPct  *float64 `json:"eps_growth_10y_pct,omitempty"`

	Criteria []model.CriterionResult `json:"criteria"`
}

// Incomplete is a company that failed nothing but could not be decided.
// It is reported, never dropped: the agent can finish the job by hand.
type Incomplete struct {
	Ticker   string   `json:"ticker"`
	Name     string   `json:"name"`
	Sector   string   `json:"sector,omitempty"`
	Blocking []string `json:"blocking"`
	Reason   string   `json:"reason"`
}

// NewEntry is a ticker that was not in the previous run's candidates.
type NewEntry struct {
	Ticker    string `json:"ticker"`
	Name      string `json:"name,omitempty"`
	FirstSeen string `json:"first_seen,omitempty"`
}

// DroppedEntry explains why a previous candidate is gone. The criterion and the
// values are computed from this run's data, never narrated by a model.
type DroppedEntry struct {
	Ticker    string   `json:"ticker"`
	Criterion string   `json:"criterion,omitempty"`
	Status    string   `json:"status,omitempty"`
	Was       *float64 `json:"was,omitempty"`
	Now       *float64 `json:"now,omitempty"`
	Reason    string   `json:"reason"`
	LastSeen  string   `json:"last_seen,omitempty"`
}

// Diff compares this run's candidates with the previous run's.
type Diff struct {
	PreviousRunID string         `json:"previous_run_id,omitempty"`
	New           []NewEntry     `json:"new"`
	Dropped       []DroppedEntry `json:"dropped"`
}

// FunnelStep is one rejection bucket: how many companies a criterion removed.
type FunnelStep struct {
	Stage     string `json:"stage"`
	Criterion string `json:"criterion"`
	Label     string `json:"label,omitempty"`
	Dropped   int    `json:"dropped"`
	Remaining int    `json:"remaining"`
}

// DataQuality is the cost and the coverage of the run.
type DataQuality struct {
	SECRequests   int64 `json:"sec_requests"`
	SEC429        int64 `json:"sec_429"`
	SECRetries    int64 `json:"sec_retries"`
	CacheHits     int64 `json:"cache_hits"`
	PriceRequests int64 `json:"price_requests"`
	PriceMissing  int64 `json:"price_missing"`
	FactsMissing  int64 `json:"facts_missing"`
	DurationMs    int64 `json:"duration_ms"`
}

// Result is the screen tool's response.
type Result struct {
	RunID        string `json:"run_id"`
	AsOf         string `json:"as_of"`
	Profile      string `json:"profile"`
	UniverseSize int    `json:"universe_size"`
	Evaluated    int    `json:"evaluated"`
	Passed       int    `json:"passed"`

	Candidates []Candidate `json:"candidates"`
	// IncompleteTotal is the full count; Incomplete carries at most
	// max_results of them, closest to a verdict first, so the response stays
	// small enough for an agent's context.
	Incomplete      []Incomplete `json:"incomplete"`
	IncompleteTotal int          `json:"incomplete_total"`

	Diff        Diff        `json:"diff"`
	DataQuality DataQuality `json:"data_quality"`
	Notes       []string    `json:"notes,omitempty"`
}

// ExplainResult is the full breakdown of one ticker.
type ExplainResult struct {
	Ticker   string                  `json:"ticker"`
	CIK      string                  `json:"cik"`
	Name     string                  `json:"name"`
	SIC      int                     `json:"sic"`
	Sector   string                  `json:"sector"`
	AsOf     string                  `json:"as_of"`
	Verdict  model.Verdict           `json:"verdict"`
	Metrics  model.Metrics           `json:"metrics"`
	Criteria []model.CriterionResult `json:"criteria"`
	Series   model.Series            `json:"series"`
	// FiscalYearEndMonth is 1-12; it explains why a calendar-quarter frame can
	// miss this company.
	FiscalYearEndMonth int `json:"fiscal_year_end_month"`
}

// HistoryResult is one raw annual series.
type HistoryResult struct {
	Ticker string            `json:"ticker"`
	CIK    string            `json:"cik"`
	Name   string            `json:"name"`
	Metric string            `json:"metric"`
	Tag    string            `json:"tag,omitempty"`
	Unit   string            `json:"unit,omitempty"`
	Values []model.YearValue `json:"values"`
}

// RunSummary is one row of the runs tool.
type RunSummary struct {
	RunID        string `json:"run_id"`
	StartedAt    string `json:"started_at"`
	FinishedAt   string `json:"finished_at"`
	Profile      string `json:"profile"`
	UniverseSize int    `json:"universe_size"`
	Passed       int    `json:"passed"`
	Params       string `json:"params,omitempty"`
}

// RunsResult is the runs tool's response.
type RunsResult struct {
	Runs    []RunSummary   `json:"runs"`
	Compare *CompareResult `json:"compare,omitempty"`
}

// CompareResult diffs two stored runs.
type CompareResult struct {
	RunA    string         `json:"run_a"`
	RunB    string         `json:"run_b"`
	New     []NewEntry     `json:"new"`
	Dropped []DroppedEntry `json:"dropped"`
	Kept    []string       `json:"kept"`
}

// FunnelResult is the rejection breakdown of one run.
type FunnelResult struct {
	RunID        string       `json:"run_id"`
	Profile      string       `json:"profile"`
	UniverseSize int          `json:"universe_size"`
	Passed       int          `json:"passed"`
	Steps        []FunnelStep `json:"steps"`
}
