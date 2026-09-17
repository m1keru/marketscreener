// Package i18n localises the human-readable part of the output: criterion
// names, thresholds, reasons and the markdown report.
//
// JSON field names, criterion ids and statuses are never translated — the agent
// and the golden contract test depend on them.
package i18n

import "fmt"

// Lang is a supported output language.
type Lang string

const (
	EN Lang = "en"
	RU Lang = "ru"
)

// Parse maps a config value to a language, falling back to English.
func Parse(s string) Lang {
	switch Lang(s) {
	case RU:
		return RU
	default:
		return EN
	}
}

// Supported lists the languages the binary can render.
func Supported() []string { return []string{string(EN), string(RU)} }

// Message keys. Every key must exist in every catalog; a test enforces it.
const (
	LabelSize              = "label.size"
	LabelCurrentRatio      = "label.current_ratio"
	LabelDebt              = "label.debt"
	LabelUtilityLeverage   = "label.utility_leverage"
	LabelEarningsStability = "label.earnings_stability"
	LabelDividends         = "label.dividends"
	LabelEPSGrowth         = "label.eps_growth"
	LabelPE                = "label.pe"
	LabelPriceToBook       = "label.price_to_book"

	TargetSize         = "target.size"
	TargetCurrentRatio = "target.current_ratio"
	TargetDebt         = "target.debt"
	TargetUtility      = "target.utility"
	TargetStability    = "target.stability"
	TargetDividends    = "target.dividends"
	TargetGrowth       = "target.growth"
	TargetPE           = "target.pe"
	TargetPB           = "target.pb"
	TargetPBWithActual = "target.pb_actual"

	ReasonFinancialSector   = "reason.financial_sector"
	ReasonNoPriceNoShares   = "reason.no_price_no_shares"
	ReasonNoPrice           = "reason.no_price"
	ReasonNoShareCount      = "reason.no_share_count"
	ReasonNoCurrentRatio    = "reason.no_current_ratio"
	ReasonZeroLiabilities   = "reason.zero_liabilities"
	ReasonNoWorkingCapital  = "reason.no_working_capital"
	ReasonNoLongTermDebt    = "reason.no_long_term_debt"
	ReasonNegWorkingCapital = "reason.negative_working_capital"
	ReasonNoDebtToEquity    = "reason.no_debt_to_equity"
	ReasonNoEquityTag       = "reason.no_equity_tag"
	ReasonEquityNotPositive = "reason.equity_not_positive"
	ReasonNoAnnualEPS       = "reason.no_annual_eps"
	ReasonEPSNotPositive    = "reason.eps_not_positive"
	ReasonEPSYearsMissing   = "reason.eps_years_missing"
	ReasonNoDividendWindow  = "reason.no_dividend_window"
	ReasonNoDividendFacts   = "reason.no_dividend_facts"
	ReasonDividendGap       = "reason.dividend_gap"
	ReasonDividendCoverage  = "reason.dividend_coverage"
	ReasonGrowthNeedsYears  = "reason.growth_needs_years"
	ReasonAvgEPSNotPositive = "reason.avg_eps_not_positive"
	ReasonFewerThanThree    = "reason.fewer_than_three_eps"
	ReasonBVPSNotPositive   = "reason.bvps_not_positive"
	ReasonPBAboveNoPE       = "reason.pb_above_no_pe"
	ReasonShareClasses      = "reason.share_classes"
	ReasonFactsFetchFailed  = "reason.facts_fetch_failed"

	DropFramesStage     = "drop.frames_stage"
	DropFinancial       = "drop.financial"
	DropNotUtility      = "drop.not_utility"
	DropOutsideUniverse = "drop.outside_universe"
	DropUndecided       = "drop.undecided"
	DropValueTarget     = "drop.value_target"
	DropFailedTarget    = "drop.failed_target"

	NoteIncompleteTruncated = "note.incomplete_truncated"
	NoteQuoteCoverage       = "note.quote_coverage"
	NoteQuoteSourceDown     = "note.quote_source_down"
	NoteNotStored           = "note.not_stored"
	NoteRunNotPersisted     = "note.run_not_persisted"

	FunnelNoXBRL        = "funnel.no_xbrl"
	FunnelSectorFinance = "funnel.sector_finance"

	RenderScreenTitle     = "render.screen_title"
	RenderScreenHeader    = "render.screen_header"
	RenderNoCandidates    = "render.no_candidates"
	RenderCandidates      = "render.candidates"
	RenderTableHead       = "render.table_head"
	RenderIncomplete      = "render.incomplete"
	RenderBlocking        = "render.blocking"
	RenderChangeSince     = "render.change_since"
	RenderNew             = "render.new"
	RenderDropped         = "render.dropped"
	RenderDataQuality     = "render.data_quality"
	RenderDataQualityLine = "render.data_quality_line"
	RenderCriteria        = "render.criteria"
	RenderCriteriaHead    = "render.criteria_head"
	RenderExplainHeader   = "render.explain_header"
	RenderExplainPrices   = "render.explain_prices"
	RenderSources         = "render.sources"
	RenderSourceLine      = "render.source_line"
	RenderSeriesEPS       = "render.series_eps"
	RenderSeriesDiv       = "render.series_dividends"
	RenderSeriesRevenue   = "render.series_revenue"
	RenderSeriesEquity    = "render.series_equity"
	RenderSeriesDebt      = "render.series_debt"
	RenderNoValues        = "render.no_values"
	RenderRuns            = "render.runs"
	RenderRunsHead        = "render.runs_head"
	RenderKept            = "render.kept"
	RenderFunnelTitle     = "render.funnel_title"
	RenderFunnelSummary   = "render.funnel_summary"
	RenderFunnelHead      = "render.funnel_head"
	RenderHistoryTitle    = "render.history_title"

	StatusPass    = "status.pass"
	StatusFail    = "status.fail"
	StatusUnknown = "status.unknown"
)

// Catalog holds the format strings of one language.
type Catalog struct {
	lang Lang
	m    map[string]string
}

// Lang reports which language this catalog renders.
func (c *Catalog) Lang() Lang { return c.lang }

// T formats the message for key. An unknown key falls back to English, and a
// key missing everywhere renders as the key itself rather than an empty string,
// so a gap is visible instead of silent.
func (c *Catalog) T(key string, args ...any) string {
	pattern, ok := c.m[key]
	if !ok {
		if pattern, ok = catalogs[EN].m[key]; !ok {
			return key
		}
	}
	if len(args) == 0 {
		return pattern
	}
	return fmt.Sprintf(pattern, args...)
}

var catalogs = map[Lang]*Catalog{
	EN: {lang: EN, m: en},
	RU: {lang: RU, m: ru},
}

// For returns the catalog of a language.
func For(l Lang) *Catalog {
	if c, ok := catalogs[l]; ok {
		return c
	}
	return catalogs[EN]
}

// Keys lists every key of the English catalog, which is the reference set.
func Keys() []string {
	out := make([]string, 0, len(en))
	for k := range en {
		out = append(out, k)
	}
	return out
}
