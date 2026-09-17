// Package model holds the types shared by every stage of the screener.
package model

import "time"

// Status is the ternary verdict of a single criterion. Unknown means "the data
// required to decide is missing" — it is never silently folded into Fail.
type Status string

const (
	Pass    Status = "pass"
	Fail    Status = "fail"
	Unknown Status = "unknown"
)

// CriterionResult is the trace of one criterion applied to one company.
type CriterionResult struct {
	ID     string   `json:"id"`
	Status Status   `json:"status"`
	Value  *float64 `json:"value"`
	Target string   `json:"target"`
	Reason string   `json:"reason,omitempty"`
	Tag    string   `json:"tag,omitempty"`
	Accn   []string `json:"accn,omitempty"`
}

// Company is the EDGAR registrant identity.
type Company struct {
	CIK     string `json:"cik"`
	Ticker  string `json:"ticker"`
	Title   string `json:"name"`
	SIC     int    `json:"sic"`
	SICDesc string `json:"sector"`
}

// IsFinancial reports SIC 6000-6799: banks, insurers, REITs. Their balance
// sheets are not split into current/non-current, so criteria 2 and 2b are
// undefined for them.
func (c Company) IsFinancial() bool { return c.SIC >= 6000 && c.SIC <= 6799 }

// IsUtility reports SIC 4900-4949, where the liquidity test is replaced by a
// debt-to-equity test.
func (c Company) IsUtility() bool { return c.SIC >= 4900 && c.SIC <= 4949 }

// Fact is one XBRL data point as reported in a filing.
type Fact struct {
	CIK   string  `json:"cik"`
	Tag   string  `json:"tag"`
	Unit  string  `json:"unit"`
	Start string  `json:"start,omitempty"` // empty for instant (balance sheet) facts
	End   string  `json:"end"`
	FY    int     `json:"fy"`
	FP    string  `json:"fp"`
	Form  string  `json:"form"`
	Val   float64 `json:"val"`
	Accn  string  `json:"accn"`
	Filed string  `json:"filed"`
}

// Instant reports whether the fact is a point-in-time (balance) value.
func (f Fact) Instant() bool { return f.Start == "" }

// Price is a quote from one of the quote providers.
type Price struct {
	Ticker string  `json:"ticker"`
	Price  float64 `json:"price"`
	AsOf   string  `json:"as_of"`
	Source string  `json:"source"`
}

// YearValue is one fiscal year of an annual series.
type YearValue struct {
	FY   int     `json:"fy"`
	End  string  `json:"end"`
	Val  float64 `json:"val"`
	Tag  string  `json:"tag"`
	Accn string  `json:"accn"`
	Form string  `json:"form"`
}

// Metrics is everything computable from facts plus a quote. Pointer fields are
// nil when the underlying facts were not found; that nil travels into the
// criteria as Unknown rather than as a zero.
type Metrics struct {
	Price       *float64 `json:"price,omitempty"`
	PriceAsOf   string   `json:"price_as_of,omitempty"`
	PriceSource string   `json:"price_source,omitempty"`

	SharesOutstanding *float64 `json:"shares_outstanding,omitempty"`
	// ShareClasses is how many share-class facts were summed into the count
	// above. More than one means market cap assumes every class trades near
	// the quoted price, which is not true for every dual-class issuer.
	ShareClasses int `json:"share_classes,omitempty"`
	// SharesAsOf is the date of the share count, which is not always as recent
	// as the rest of the balance sheet.
	SharesAsOf string   `json:"shares_as_of,omitempty"`
	MarketCap  *float64 `json:"market_cap,omitempty"`

	AssetsCurrent      *float64 `json:"assets_current,omitempty"`
	LiabilitiesCurrent *float64 `json:"liabilities_current,omitempty"`
	CurrentRatio       *float64 `json:"current_ratio,omitempty"`
	WorkingCapital     *float64 `json:"working_capital,omitempty"`
	LongTermDebt       *float64 `json:"long_term_debt,omitempty"`
	LTDToWorkingCap    *float64 `json:"ltd_to_working_capital,omitempty"`
	LTDToEquity        *float64 `json:"ltd_to_equity,omitempty"`
	Equity             *float64 `json:"equity,omitempty"`
	BookValuePerShare  *float64 `json:"book_value_per_share,omitempty"`

	EPSLatest    *float64 `json:"eps_latest,omitempty"`
	EPSAvg3Y     *float64 `json:"eps_avg_3y,omitempty"`
	PE3YAvg      *float64 `json:"pe_3y_avg,omitempty"`
	PB           *float64 `json:"pb,omitempty"`
	GrahamNumber *float64 `json:"graham_number,omitempty"`
	// MarginOfSafety is graham_number/price - 1: how far below the Graham
	// number the stock trades. Used to rank candidates.
	MarginOfSafety *float64 `json:"margin_of_safety,omitempty"`

	EPSPositiveYears  *int     `json:"eps_positive_years,omitempty"`
	EPSYearsAvailable int      `json:"eps_years_available"`
	DividendYears     *int     `json:"dividend_years,omitempty"`
	DivYearsAvailable int      `json:"dividend_years_available"`
	EPSGrowth10YPct   *float64 `json:"eps_growth_10y_pct,omitempty"`

	// Tags records which XBRL tag actually supplied each metric family.
	Tags map[string]string `json:"tags,omitempty"`
	// Accn records accession numbers backing each metric family.
	Accn map[string][]string `json:"accn,omitempty"`
}

// Series bundles the annual rows behind the metrics, for explain/history.
type Series struct {
	EPS       []YearValue `json:"eps,omitempty"`
	Dividends []YearValue `json:"dividends,omitempty"`
	Revenue   []YearValue `json:"revenue,omitempty"`
	Equity    []YearValue `json:"equity,omitempty"`
	Debt      []YearValue `json:"debt,omitempty"`
	Assets    []YearValue `json:"assets,omitempty"`
}

// Verdict is the company-level outcome of the criteria set.
type Verdict string

const (
	VerdictPass       Verdict = "pass"
	VerdictFail       Verdict = "fail"
	VerdictIncomplete Verdict = "incomplete"
)

// Evaluation is one company scored against the full criteria set.
type Evaluation struct {
	Company  Company           `json:"company"`
	Metrics  Metrics           `json:"metrics"`
	Criteria []CriterionResult `json:"criteria"`
	Verdict  Verdict           `json:"verdict"`
}

// FirstFailure returns the first criterion with the given status, or nil.
func (e Evaluation) First(status Status) *CriterionResult {
	for i := range e.Criteria {
		if e.Criteria[i].Status == status {
			return &e.Criteria[i]
		}
	}
	return nil
}

// Blocking lists the IDs of criteria that could not be decided.
func (e Evaluation) Blocking() []string {
	var out []string
	for _, c := range e.Criteria {
		if c.Status == Unknown {
			out = append(out, c.ID)
		}
	}
	return out
}

// Run is a stored screening run.
type Run struct {
	ID           string    `json:"run_id"`
	StartedAt    time.Time `json:"started_at"`
	FinishedAt   time.Time `json:"finished_at"`
	Profile      string    `json:"profile"`
	ParamsJSON   string    `json:"params,omitempty"`
	UniverseSize int       `json:"universe_size"`
	PassedCount  int       `json:"passed"`
	FunnelJSON   string    `json:"-"`
}
