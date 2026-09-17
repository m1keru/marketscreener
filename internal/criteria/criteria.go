// Package criteria applies the Graham defensive tests to computed metrics.
//
// Every test returns pass, fail or unknown. Unknown means the data needed to
// decide is missing — it is never folded into fail, because a silently dropped
// company is exactly the defect this version exists to remove.
package criteria

import (
	"sort"

	"github.com/m1ke/marketscreener/internal/config"
	"github.com/m1ke/marketscreener/internal/i18n"
	"github.com/m1ke/marketscreener/internal/metrics"
	"github.com/m1ke/marketscreener/internal/model"
)

// Criterion IDs. They are part of the MCP contract: the agent quotes them back
// when it explains why a ticker dropped out.
const (
	IDSize              = "graham.1.size"
	IDCurrentRatio      = "graham.2.current_ratio"
	IDDebt              = "graham.2b.debt"
	IDUtilityLeverage   = "graham.2u.debt_to_equity"
	IDEarningsStability = "graham.3.earnings_stability"
	IDDividends         = "graham.4.dividends"
	IDEPSGrowth         = "graham.5.eps_growth"
	IDPE                = "graham.6.pe"
	IDPriceToBook       = "graham.7.price_to_book"
)

// labelKeys maps criterion ids to catalog keys.
var labelKeys = map[string]string{
	IDSize:              i18n.LabelSize,
	IDCurrentRatio:      i18n.LabelCurrentRatio,
	IDDebt:              i18n.LabelDebt,
	IDUtilityLeverage:   i18n.LabelUtilityLeverage,
	IDEarningsStability: i18n.LabelEarningsStability,
	IDDividends:         i18n.LabelDividends,
	IDEPSGrowth:         i18n.LabelEPSGrowth,
	IDPE:                i18n.LabelPE,
	IDPriceToBook:       i18n.LabelPriceToBook,
}

// LabelIn returns a short human-readable name for a criterion id in the given
// language. An id with no entry renders as itself, which is what a new
// criterion should look like until it is named.
func LabelIn(c *i18n.Catalog, id string) string {
	if key, ok := labelKeys[id]; ok {
		return c.T(key)
	}
	return id
}

// Label names a criterion in English. Callers that have a language use LabelIn.
func Label(id string) string { return LabelIn(i18n.For(i18n.EN), id) }

func f64(v float64) *float64 { return &v }

// Evaluate applies the whole profile to one company. Values, ids and statuses
// are language independent; targets and reasons are rendered in cfg.Language.
func Evaluate(c model.Company, m model.Metrics, s model.Series, cfg config.Config) []model.CriterionResult {
	t := cfg.Thresholds
	lc := i18n.For(i18n.Parse(cfg.Language))
	out := []model.CriterionResult{size(lc, m, t)}

	switch {
	case c.IsFinancial():
		// SIC 6000-6799 have no current/non-current split. Reporting these as
		// pass is how the old version let a REIT through on a current ratio
		// of 37: the number exists but means nothing.
		reason := lc.T(i18n.ReasonFinancialSector, c.SIC, c.SICDesc)
		out = append(out,
			model.CriterionResult{ID: IDCurrentRatio, Status: model.Unknown,
				Target: lc.T(i18n.TargetCurrentRatio, t.CurrentRatioMin), Reason: reason},
			model.CriterionResult{ID: IDDebt, Status: model.Unknown,
				Target: lc.T(i18n.TargetDebt), Reason: reason})
	case c.IsUtility():
		// SIC 4900-4949: regulated utilities run thin working capital by
		// design, so leverage against equity replaces both tests.
		out = append(out, utilityLeverage(lc, c, m, t))
	default:
		out = append(out, currentRatio(lc, m, t), debtVsWorkingCapital(lc, m))
	}

	out = append(out,
		earningsStability(lc, m, s, t),
		dividends(lc, m, s, t),
		epsGrowth(lc, m, t),
		priceEarnings(lc, m, t),
		priceToBook(lc, m, t),
	)
	return out
}

// Verdict folds the criteria into a company-level outcome.
func Verdict(rs []model.CriterionResult) model.Verdict {
	v := model.VerdictPass
	for _, r := range rs {
		switch r.Status {
		case model.Fail:
			return model.VerdictFail
		case model.Unknown:
			v = model.VerdictIncomplete
		}
	}
	return v
}

func size(lc *i18n.Catalog, m model.Metrics, t config.Thresholds) model.CriterionResult {
	r := model.CriterionResult{
		ID:     IDSize,
		Target: lc.T(i18n.TargetSize, t.MinMarketCapUSD/1e6),
		Tag:    m.Tags["shares_outstanding"],
		Accn:   m.Accn["shares_outstanding"],
	}
	if m.MarketCap == nil {
		r.Status = model.Unknown
		switch {
		case m.Price == nil && m.SharesOutstanding == nil:
			r.Reason = lc.T(i18n.ReasonNoPriceNoShares)
		case m.Price == nil:
			r.Reason = lc.T(i18n.ReasonNoPrice)
		default:
			r.Reason = lc.T(i18n.ReasonNoShareCount, tagList(metrics.SharesOutstanding))
		}
		return r
	}
	r.Value = m.MarketCap
	if m.ShareClasses > 1 {
		r.Reason = lc.T(i18n.ReasonShareClasses, m.ShareClasses)
	}
	if *m.MarketCap >= t.MinMarketCapUSD {
		r.Status = model.Pass
	} else {
		r.Status = model.Fail
	}
	return r
}

func currentRatio(lc *i18n.Catalog, m model.Metrics, t config.Thresholds) model.CriterionResult {
	r := model.CriterionResult{
		ID:     IDCurrentRatio,
		Target: lc.T(i18n.TargetCurrentRatio, t.CurrentRatioMin),
		Tag:    m.Tags["assets_current"],
		Accn:   append(append([]string{}, m.Accn["assets_current"]...), m.Accn["liabilities_current"]...),
	}
	if m.CurrentRatio == nil {
		r.Status = model.Unknown
		var missing []string
		if m.AssetsCurrent == nil {
			missing = append(missing, "AssetsCurrent")
		}
		if m.LiabilitiesCurrent == nil {
			missing = append(missing, "LiabilitiesCurrent")
		} else if *m.LiabilitiesCurrent <= 0 {
			missing = append(missing, lc.T(i18n.ReasonZeroLiabilities))
		}
		r.Reason = lc.T(i18n.ReasonNoCurrentRatio, join(missing))
		return r
	}
	r.Value = m.CurrentRatio
	if *m.CurrentRatio >= t.CurrentRatioMin {
		r.Status = model.Pass
	} else {
		r.Status = model.Fail
	}
	return r
}

// debtVsWorkingCapital is Graham's own leverage test: long-term debt must not
// exceed working capital. The old version compared debt to assets against 1.0,
// a bound only a company with negative equity could break.
func debtVsWorkingCapital(lc *i18n.Catalog, m model.Metrics) model.CriterionResult {
	r := model.CriterionResult{
		ID:     IDDebt,
		Target: lc.T(i18n.TargetDebt),
		Tag:    m.Tags["long_term_debt"],
		Accn:   m.Accn["long_term_debt"],
	}
	if m.WorkingCapital == nil {
		r.Status = model.Unknown
		r.Reason = lc.T(i18n.ReasonNoWorkingCapital)
		return r
	}
	if m.LongTermDebt == nil {
		// No long-term debt tag at all: treat as no long-term debt only when
		// the company reports a balance sheet; otherwise it is unknown.
		r.Status = model.Unknown
		r.Reason = lc.T(i18n.ReasonNoLongTermDebt, tagList(metrics.LongTermDebt))
		return r
	}
	if *m.WorkingCapital <= 0 {
		r.Status = model.Fail
		r.Value = m.WorkingCapital
		r.Reason = lc.T(i18n.ReasonNegWorkingCapital, *m.WorkingCapital)
		return r
	}
	r.Value = m.LTDToWorkingCap
	if *m.LongTermDebt <= *m.WorkingCapital {
		r.Status = model.Pass
	} else {
		r.Status = model.Fail
	}
	return r
}

func utilityLeverage(lc *i18n.Catalog, c model.Company, m model.Metrics, t config.Thresholds) model.CriterionResult {
	r := model.CriterionResult{
		ID:     IDUtilityLeverage,
		Target: lc.T(i18n.TargetUtility, t.UtilityDebtToEquityMax, c.SIC),
		Tag:    m.Tags["long_term_debt"],
		Accn:   append(append([]string{}, m.Accn["long_term_debt"]...), m.Accn["equity"]...),
	}
	if m.LTDToEquity == nil {
		r.Status = model.Unknown
		var missing []string
		if m.LongTermDebt == nil {
			missing = append(missing, lc.T(i18n.ReasonNoLongTermDebt, tagList(metrics.LongTermDebt)))
		}
		if m.Equity == nil {
			missing = append(missing, lc.T(i18n.ReasonNoEquityTag, tagList(metrics.Equity)))
		} else if *m.Equity <= 0 {
			missing = append(missing, lc.T(i18n.ReasonEquityNotPositive))
		}
		r.Reason = lc.T(i18n.ReasonNoDebtToEquity, join(missing))
		return r
	}
	r.Value = m.LTDToEquity
	if *m.LTDToEquity <= t.UtilityDebtToEquityMax {
		r.Status = model.Pass
	} else {
		r.Status = model.Fail
	}
	return r
}

func earningsStability(lc *i18n.Catalog, m model.Metrics, s model.Series, t config.Thresholds) model.CriterionResult {
	years := t.HistoryYears
	r := model.CriterionResult{
		ID:     IDEarningsStability,
		Target: lc.T(i18n.TargetStability, years, t.EPSPositiveYears, years),
		Tag:    m.Tags["eps"],
		Accn:   m.Accn["eps"],
	}
	if len(s.EPS) == 0 || m.EPSPositiveYears == nil {
		r.Status = model.Unknown
		r.Reason = lc.T(i18n.ReasonNoAnnualEPS, tagList(metrics.EPS))
		return r
	}
	r.Value = f64(float64(*m.EPSPositiveYears))
	// A known negative year fails regardless of what else is missing.
	var negatives []int
	for _, v := range s.EPS {
		if v.Val <= 0 {
			negatives = append(negatives, v.FY)
		}
	}
	if len(negatives) > 0 {
		r.Status = model.Fail
		r.Reason = lc.T(i18n.ReasonEPSNotPositive, metrics.FormatYears(negatives))
		return r
	}
	if missing := metrics.MissingYears(s.EPS, years); len(missing) > 0 {
		r.Status = model.Unknown
		r.Reason = lc.T(i18n.ReasonEPSYearsMissing, metrics.FormatYears(missing), m.EPSYearsAvailable, years)
		return r
	}
	if *m.EPSPositiveYears >= t.EPSPositiveYears {
		r.Status = model.Pass
	} else {
		r.Status = model.Fail
	}
	return r
}

// dividends distinguishes "no dividend was paid" from "the payment was not
// tagged". A gap between two tagged years is evidence of a suspension and
// fails; missing years at the start of the window are an EDGAR coverage
// problem and stay unknown.
func dividends(lc *i18n.Catalog, m model.Metrics, s model.Series, t config.Thresholds) model.CriterionResult {
	years := t.HistoryYears
	r := model.CriterionResult{
		ID:     IDDividends,
		Target: lc.T(i18n.TargetDividends, years, t.DividendYears, years),
		Tag:    m.Tags["dividends"],
	}
	if m.DividendYears == nil {
		r.Status = model.Unknown
		r.Reason = lc.T(i18n.ReasonNoDividendWindow)
		return r
	}
	r.Value = f64(float64(*m.DividendYears))
	if m.DivYearsAvailable == 0 {
		r.Status = model.Fail
		r.Reason = lc.T(i18n.ReasonNoDividendFacts,
			tagList(metrics.DividendsPerShare)+", "+tagList(metrics.DividendsPaid))
		return r
	}
	if *m.DividendYears >= t.DividendYears {
		r.Status = model.Pass
		return r
	}
	// Which years are missing, and are they inside the tagged range?
	lastFY := s.EPS[len(s.EPS)-1].FY
	first := lastFY - years + 1
	var gaps []int
	for fy := first; fy <= lastFY; fy++ {
		if !hasDividend(s.Dividends, fy) {
			gaps = append(gaps, fy)
		}
	}
	firstTagged, lastTagged := taggedRange(s.Dividends)
	interior := false
	var leading []int
	for _, fy := range gaps {
		if fy > firstTagged && fy < lastTagged {
			interior = true
		} else {
			leading = append(leading, fy)
		}
	}
	if interior {
		r.Status = model.Fail
		r.Reason = lc.T(i18n.ReasonDividendGap, metrics.FormatYears(gaps), *m.DividendYears, years)
		return r
	}
	r.Status = model.Unknown
	r.Reason = lc.T(i18n.ReasonDividendCoverage,
		metrics.FormatYears(leading), firstTagged, *m.DividendYears, years)
	return r
}

func hasDividend(vs []model.YearValue, fy int) bool {
	for _, v := range vs {
		if v.FY == fy && v.Val > 0 {
			return true
		}
	}
	return false
}

func taggedRange(vs []model.YearValue) (first, last int) {
	if len(vs) == 0 {
		return 0, 0
	}
	years := make([]int, 0, len(vs))
	for _, v := range vs {
		if v.Val > 0 {
			years = append(years, v.FY)
		}
	}
	if len(years) == 0 {
		return 0, 0
	}
	sort.Ints(years)
	return years[0], years[len(years)-1]
}

func epsGrowth(lc *i18n.Catalog, m model.Metrics, t config.Thresholds) model.CriterionResult {
	r := model.CriterionResult{
		ID:     IDEPSGrowth,
		Target: lc.T(i18n.TargetGrowth, t.EPSGrowthMinPct, t.HistoryYears),
		Tag:    m.Tags["eps"],
	}
	if m.EPSGrowth10YPct == nil {
		r.Status = model.Unknown
		r.Reason = lc.T(i18n.ReasonGrowthNeedsYears, t.HistoryYears, m.EPSYearsAvailable)
		return r
	}
	r.Value = m.EPSGrowth10YPct
	if *m.EPSGrowth10YPct >= t.EPSGrowthMinPct {
		r.Status = model.Pass
	} else {
		r.Status = model.Fail
	}
	return r
}

// priceEarnings uses the three-year average EPS on purpose: a trailing P/E at
// the top of a cycle is the classic value trap.
func priceEarnings(lc *i18n.Catalog, m model.Metrics, t config.Thresholds) model.CriterionResult {
	r := model.CriterionResult{
		ID:     IDPE,
		Target: lc.T(i18n.TargetPE, t.PEMax),
		Tag:    m.Tags["eps"],
	}
	if m.EPSAvg3Y != nil && *m.EPSAvg3Y <= 0 {
		r.Status = model.Fail
		r.Value = m.EPSAvg3Y
		r.Reason = lc.T(i18n.ReasonAvgEPSNotPositive, *m.EPSAvg3Y)
		return r
	}
	if m.PE3YAvg == nil {
		r.Status = model.Unknown
		switch {
		case m.EPSAvg3Y == nil:
			r.Reason = lc.T(i18n.ReasonFewerThanThree)
		default:
			r.Reason = lc.T(i18n.ReasonNoPrice)
		}
		return r
	}
	r.Value = m.PE3YAvg
	if *m.PE3YAvg <= t.PEMax {
		r.Status = model.Pass
	} else {
		r.Status = model.Fail
	}
	return r
}

func priceToBook(lc *i18n.Catalog, m model.Metrics, t config.Thresholds) model.CriterionResult {
	r := model.CriterionResult{
		ID:     IDPriceToBook,
		Target: lc.T(i18n.TargetPB, t.PBMax, t.GrahamProductMax),
		Tag:    m.Tags["equity"],
		Accn:   m.Accn["equity"],
	}
	if m.PB == nil {
		r.Status = model.Unknown
		switch {
		case m.Equity == nil:
			r.Reason = lc.T(i18n.ReasonNoEquityTag, tagList(metrics.Equity))
		case m.SharesOutstanding == nil:
			r.Reason = lc.T(i18n.ReasonNoShareCount, tagList(metrics.SharesOutstanding))
		case m.BookValuePerShare != nil && *m.BookValuePerShare <= 0:
			r.Status = model.Fail
			r.Reason = lc.T(i18n.ReasonBVPSNotPositive)
			r.Value = m.BookValuePerShare
			return r
		default:
			r.Reason = lc.T(i18n.ReasonNoPrice)
		}
		return r
	}
	r.Value = m.PB
	if *m.PB <= t.PBMax {
		r.Status = model.Pass
		return r
	}
	if m.PE3YAvg == nil {
		r.Status = model.Unknown
		r.Reason = lc.T(i18n.ReasonPBAboveNoPE, *m.PB, t.PBMax)
		return r
	}
	product := *m.PE3YAvg * *m.PB
	r.Value = f64(product)
	r.Target = lc.T(i18n.TargetPBWithActual, t.PBMax, *m.PB, t.GrahamProductMax)
	if product <= t.GrahamProductMax {
		r.Status = model.Pass
	} else {
		r.Status = model.Fail
	}
	return r
}

func tagList(f metrics.Family) string { return join(f.Tags) }

func join(ss []string) string {
	out := ""
	for i, s := range ss {
		if i > 0 {
			out += ", "
		}
		out += s
	}
	if out == "" {
		return "(none)"
	}
	return out
}
