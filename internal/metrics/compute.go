package metrics

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/m1ke/marketscreener/internal/model"
)

// Extractor indexes a company's facts by tag and answers the two questions the
// metrics need: "what is the latest value" and "what is the annual series".
type Extractor struct {
	byTag    map[string][]model.Fact
	fyeMonth time.Month
}

// NewExtractor indexes facts and infers the company's fiscal year end.
func NewExtractor(facts []model.Fact) *Extractor {
	e := &Extractor{byTag: make(map[string][]model.Fact)}
	for _, f := range facts {
		e.byTag[f.Tag] = append(e.byTag[f.Tag], f)
	}
	e.fyeMonth = inferFiscalYearEnd(facts)
	return e
}

// FiscalYearEndMonth reports the inferred fiscal year end, December by default.
func (e *Extractor) FiscalYearEndMonth() time.Month { return e.fyeMonth }

// inferFiscalYearEnd takes the most common end month of annual periods reported
// in 10-K filings. Companies whose year ends outside December are exactly the
// ones a single-frame query would lose, so this is load-bearing.
func inferFiscalYearEnd(facts []model.Fact) time.Month {
	counts := map[time.Month]int{}
	for _, f := range facts {
		if f.Instant() || !isAnnualForm(f.Form) {
			continue
		}
		d, ok := durationDays(f)
		if !ok || d < 300 || d > 400 {
			continue
		}
		end, err := parseDate(f.End)
		if err != nil {
			continue
		}
		counts[end.Month()]++
	}
	best, bestN := time.December, 0
	for m, n := range counts {
		if n > bestN || (n == bestN && m < best) {
			best, bestN = m, n
		}
	}
	return best
}

func isAnnualForm(form string) bool { return strings.HasPrefix(form, "10-K") }

func parseDate(s string) (time.Time, error) { return time.Parse("2006-01-02", s) }

func durationDays(f model.Fact) (int, bool) {
	s, err := parseDate(f.Start)
	if err != nil {
		return 0, false
	}
	e, err := parseDate(f.End)
	if err != nil {
		return 0, false
	}
	return int(e.Sub(s).Hours() / 24), true
}

// FiscalYear labels a period by the calendar year its end falls in, shifting
// years that end in the first half back by one. Two consecutive fiscal years
// therefore always get two different labels, whatever the year end month.
func FiscalYear(end string) (int, bool) {
	t, err := parseDate(end)
	if err != nil {
		return 0, false
	}
	if t.Month() >= time.June {
		return t.Year(), true
	}
	return t.Year() - 1, true
}

// Latest returns the freshest fact of a family: the largest period end, ties
// broken by filing date and accession number. It accepts facts from any form,
// so a quarterly balance sheet can supply a fresher value than the last 10-K.
func (e *Extractor) Latest(f Family) (*model.Fact, string) {
	for _, tag := range f.Tags {
		var best *model.Fact
		for i := range e.byTag[tag] {
			c := e.byTag[tag][i]
			if f.Instant && !c.Instant() {
				continue
			}
			if !f.Instant && c.Instant() {
				continue
			}
			if !f.Instant {
				if d, ok := durationDays(c); !ok || d < 300 || d > 400 {
					continue // annual periods only
				}
			}
			if best == nil || newer(c, *best) {
				cp := c
				best = &cp
			}
		}
		if best != nil {
			return best, tag
		}
	}
	return nil, ""
}

func newer(a, b model.Fact) bool {
	if a.End != b.End {
		return a.End > b.End
	}
	if a.Filed != b.Filed {
		return a.Filed > b.Filed
	}
	return a.Accn > b.Accn
}

// LatestSum is Latest for share counts: a registrant with several share classes
// reports one cover-page fact per class at the same instant, and the total is
// their sum. Taking a single fact there would silently report one class as the
// whole company — for a dual-class issuer that is wrong by orders of magnitude.
// The number of components summed is returned so the trace can say so.
// notBefore, when set, rejects a tag whose freshest fact is older than that
// date: a share count from an abandoned tag is not data about today.
func (e *Extractor) LatestSum(f Family, notBefore string) (val float64, tag string, end string, accn []string, components int) {
	for _, t := range f.Tags {
		var maxEnd, maxFiled string
		for _, c := range e.byTag[t] {
			if f.Instant != c.Instant() {
				continue
			}
			if c.End > maxEnd || (c.End == maxEnd && c.Filed > maxFiled) {
				maxEnd, maxFiled = c.End, c.Filed
			}
		}
		if maxEnd == "" || (notBefore != "" && maxEnd < notBefore) {
			continue // the tag was abandoned; try the next one in the family
		}
		seen := map[float64]bool{}
		accns := map[string]bool{}
		sum := 0.0
		for _, c := range e.byTag[t] {
			if f.Instant != c.Instant() || c.End != maxEnd || c.Filed != maxFiled || c.Val <= 0 {
				continue
			}
			// Distinct values at one instant are distinct share classes; the
			// same value repeated is the same class restated.
			if seen[c.Val] {
				continue
			}
			seen[c.Val] = true
			sum += c.Val
			accns[c.Accn] = true
		}
		if sum <= 0 {
			continue
		}
		var as []string
		for a := range accns {
			as = append(as, a)
		}
		sort.Strings(as)
		return sum, t, maxEnd, as, len(seen)
	}
	return 0, "", "", nil, 0
}

// AnnualSeries returns one value per fiscal year, oldest first, taken from
// annual filings. Tags are walked in priority order and later tags only fill
// years the earlier ones left empty: a registrant that stops tagging
// EarningsPerShareBasic and reports only the diluted figure keeps an unbroken
// series instead of acquiring a hole. Each year records the tag it came from.
func (e *Extractor) AnnualSeries(f Family) ([]model.YearValue, string) {
	byYear := map[int]model.Fact{}
	primary := ""
	for _, tag := range f.Tags {
		fromTag := e.annualByTag(f, tag)
		if len(fromTag) == 0 {
			continue
		}
		if primary == "" {
			primary = tag
		}
		for fy, c := range fromTag {
			if _, taken := byYear[fy]; !taken {
				byYear[fy] = c
			}
		}
	}
	if len(byYear) == 0 {
		return nil, ""
	}
	out := make([]model.YearValue, 0, len(byYear))
	for fy, c := range byYear {
		out = append(out, model.YearValue{FY: fy, End: c.End, Val: c.Val, Tag: c.Tag, Accn: c.Accn, Form: c.Form})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].FY < out[j].FY })
	return out, primary
}

// annualByTag collects one value per fiscal year for a single tag.
func (e *Extractor) annualByTag(f Family, tag string) map[int]model.Fact {
	byYear := map[int]model.Fact{}
	for _, c := range e.byTag[tag] {
		if !isAnnualForm(c.Form) {
			continue
		}
		if f.Instant {
			end, err := parseDate(c.End)
			if err != nil || end.Month() != e.fyeMonth {
				continue // not a fiscal year end balance
			}
		} else {
			d, ok := durationDays(c)
			if !ok || d < 300 || d > 400 {
				continue
			}
		}
		fy, ok := FiscalYear(c.End)
		if !ok {
			continue
		}
		// Restatements: the latest filing wins.
		if prev, seen := byYear[fy]; !seen || newer(c, prev) {
			byYear[fy] = c
		}
	}
	return byYear
}

// AnyPositiveInYear reports whether the family has any positive fact whose
// period ends inside the given fiscal year. Criterion 4 only asks whether a
// dividend was paid, so a quarterly per-share fact is evidence enough even when
// no annual total was tagged.
func (e *Extractor) AnyPositiveInYear(f Family, fy int) bool {
	for _, tag := range f.Tags {
		for _, c := range e.byTag[tag] {
			if c.Val <= 0 {
				continue
			}
			if y, ok := FiscalYear(c.End); ok && y == fy {
				return true
			}
		}
	}
	return false
}

// Input is everything Compute needs about one company.
type Input struct {
	Facts        []model.Fact
	Price        *model.Price
	HistoryYears int
}

// Compute derives every metric and the annual series behind them. Anything that
// cannot be derived stays nil and travels into the criteria as Unknown.
func Compute(in Input) (model.Metrics, model.Series) {
	years := in.HistoryYears
	if years <= 0 {
		years = 10
	}
	e := NewExtractor(in.Facts)
	m := model.Metrics{Tags: map[string]string{}, Accn: map[string][]string{}}
	var s model.Series

	note := func(family string, f *model.Fact, tag string) {
		if f == nil {
			return
		}
		m.Tags[family] = tag
		m.Accn[family] = append(m.Accn[family], f.Accn)
	}

	if in.Price != nil {
		p := in.Price.Price
		m.Price = &p
		m.PriceAsOf = in.Price.AsOf
		m.PriceSource = in.Price.Source
	}

	// --- balance sheet ---
	// The share count must be roughly as recent as the balance sheet it is
	// paired with. Registrants do abandon tags: Berkshire last tagged
	// EntityCommonStockSharesOutstanding in 2011, and multiplying a 2011 count
	// by today's price produces a market cap that is wrong by orders of
	// magnitude while looking entirely plausible.
	reference := latestInstantEnd(e)
	if v, tag, end, accns, classes := e.LatestSum(SharesOutstanding, shiftMonths(reference, -18)); v > 0 {
		m.SharesOutstanding = &v
		m.ShareClasses = classes
		m.SharesAsOf = end
		m.Tags["shares_outstanding"] = tag
		m.Accn["shares_outstanding"] = accns
	}
	if f, tag := e.Latest(AssetsCurrent); f != nil {
		v := f.Val
		m.AssetsCurrent = &v
		note("assets_current", f, tag)
	}
	if f, tag := e.Latest(LiabilitiesCurrent); f != nil {
		v := f.Val
		m.LiabilitiesCurrent = &v
		note("liabilities_current", f, tag)
	}
	if f, tag := e.Latest(LongTermDebt); f != nil {
		v := f.Val
		m.LongTermDebt = &v
		note("long_term_debt", f, tag)
	}
	if f, tag := e.Latest(Equity); f != nil {
		v := f.Val
		m.Equity = &v
		note("equity", f, tag)
	}

	if m.AssetsCurrent != nil && m.LiabilitiesCurrent != nil && *m.LiabilitiesCurrent > 0 {
		v := *m.AssetsCurrent / *m.LiabilitiesCurrent
		m.CurrentRatio = &v
	}
	if m.AssetsCurrent != nil && m.LiabilitiesCurrent != nil {
		v := *m.AssetsCurrent - *m.LiabilitiesCurrent
		m.WorkingCapital = &v
		if m.LongTermDebt != nil && v > 0 {
			r := *m.LongTermDebt / v
			m.LTDToWorkingCap = &r
		}
	}
	if m.LongTermDebt != nil && m.Equity != nil && *m.Equity > 0 {
		v := *m.LongTermDebt / *m.Equity
		m.LTDToEquity = &v
	}
	if m.Price != nil && m.SharesOutstanding != nil {
		v := *m.Price * *m.SharesOutstanding
		m.MarketCap = &v
	}
	if m.Equity != nil && m.SharesOutstanding != nil && *m.SharesOutstanding > 0 {
		v := *m.Equity / *m.SharesOutstanding
		m.BookValuePerShare = &v
		if m.Price != nil && v > 0 {
			pb := *m.Price / v
			m.PB = &pb
		}
	}

	// --- annual series ---
	epsSeries, epsTag := e.AnnualSeries(EPS)
	s.EPS = lastN(epsSeries, years)
	if epsTag != "" {
		m.Tags["eps"] = epsTag
		m.Accn["eps"] = accnsOf(s.EPS)
	}
	divSeries, divTag := e.AnnualSeries(DividendsPerShare)
	if len(divSeries) == 0 {
		divSeries, divTag = e.AnnualSeries(DividendsPaid)
	}
	s.Dividends = lastN(divSeries, years)
	if divTag != "" {
		m.Tags["dividends"] = divTag
	}
	revSeries, revTag := e.AnnualSeries(Revenue)
	s.Revenue = lastN(revSeries, years)
	if revTag != "" {
		m.Tags["revenue"] = revTag
	}
	eqSeries, _ := e.AnnualSeries(Equity)
	s.Equity = lastN(eqSeries, years)
	debtSeries, _ := e.AnnualSeries(LongTermDebt)
	s.Debt = lastN(debtSeries, years)
	asSeries, _ := e.AnnualSeries(Assets)
	s.Assets = lastN(asSeries, years)

	// Latest annual EPS, used by the cheap pre-screen.
	if len(s.EPS) > 0 {
		v := s.EPS[len(s.EPS)-1].Val
		m.EPSLatest = &v
	}

	// Window: the `years` fiscal years ending with the most recent one on file.
	if len(s.EPS) > 0 {
		lastFY := s.EPS[len(s.EPS)-1].FY
		first := lastFY - years + 1
		byYear := map[int]float64{}
		for _, v := range s.EPS {
			if v.FY >= first {
				byYear[v.FY] = v.Val
			}
		}
		positives, available := 0, 0
		for fy := first; fy <= lastFY; fy++ {
			v, ok := byYear[fy]
			if !ok {
				continue
			}
			available++
			if v > 0 {
				positives++
			}
		}
		m.EPSPositiveYears = &positives
		m.EPSYearsAvailable = available

		// Dividend evidence, per year of the same window.
		divYears, divAvailable := 0, 0
		for fy := first; fy <= lastFY; fy++ {
			paid := e.AnyPositiveInYear(DividendsPerShare, fy) || e.AnyPositiveInYear(DividendsPaid, fy)
			hasData := paid || hasYear(divSeries, fy)
			if hasData {
				divAvailable++
			}
			if paid {
				divYears++
			}
		}
		m.DividendYears = &divYears
		m.DivYearsAvailable = divAvailable
	}

	// avg EPS over the three most recent fiscal years
	if avg, ok := avgLast(s.EPS, 3); ok {
		m.EPSAvg3Y = &avg
		if m.Price != nil && avg > 0 {
			pe := *m.Price / avg
			m.PE3YAvg = &pe
		}
		if m.BookValuePerShare != nil && avg > 0 && *m.BookValuePerShare > 0 {
			g := math.Sqrt(22.5 * avg * *m.BookValuePerShare)
			m.GrahamNumber = &g
			if m.Price != nil && *m.Price > 0 {
				mos := g / *m.Price - 1
				m.MarginOfSafety = &mos
			}
		}
	}

	// growth: avg of the last three years against avg of the three years that
	// open the ten-year window
	if len(s.EPS) >= years {
		w := s.EPS[len(s.EPS)-years:]
		recent, okR := avgLast(w, 3)
		base, okB := avgFirst(w, 3)
		if okR && okB && base > 0 {
			g := (recent/base - 1) * 100
			m.EPSGrowth10YPct = &g
		}
	}

	return m, s
}

// latestInstantEnd is the freshest balance sheet date on file: the point in
// time the share count should be close to.
func latestInstantEnd(e *Extractor) string {
	out := ""
	for _, fam := range []Family{Assets, Equity, AssetsCurrent, Liabilities} {
		if f, _ := e.Latest(fam); f != nil && f.End > out {
			out = f.End
		}
	}
	return out
}

// shiftMonths moves a YYYY-MM-DD date by n months; an unparseable date yields
// an empty cutoff, which disables the freshness check.
func shiftMonths(date string, n int) string {
	t, err := parseDate(date)
	if err != nil {
		return ""
	}
	return t.AddDate(0, n, 0).Format("2006-01-02")
}

func hasYear(vs []model.YearValue, fy int) bool {
	for _, v := range vs {
		if v.FY == fy {
			return true
		}
	}
	return false
}

func lastN(vs []model.YearValue, n int) []model.YearValue {
	if n <= 0 || len(vs) <= n {
		return vs
	}
	return vs[len(vs)-n:]
}

func accnsOf(vs []model.YearValue) []string {
	seen := map[string]bool{}
	var out []string
	for _, v := range vs {
		if v.Accn != "" && !seen[v.Accn] {
			seen[v.Accn] = true
			out = append(out, v.Accn)
		}
	}
	return out
}

func avgLast(vs []model.YearValue, n int) (float64, bool) {
	if len(vs) < n {
		return 0, false
	}
	sum := 0.0
	for _, v := range vs[len(vs)-n:] {
		sum += v.Val
	}
	return sum / float64(n), true
}

func avgFirst(vs []model.YearValue, n int) (float64, bool) {
	if len(vs) < n {
		return 0, false
	}
	sum := 0.0
	for _, v := range vs[:n] {
		sum += v.Val
	}
	return sum / float64(n), true
}

// MissingYears lists the fiscal years of the window that have no value, for the
// Unknown reason text.
func MissingYears(vs []model.YearValue, window int) []int {
	if len(vs) == 0 {
		return nil
	}
	last := vs[len(vs)-1].FY
	first := last - window + 1
	have := map[int]bool{}
	for _, v := range vs {
		have[v.FY] = true
	}
	var out []int
	for fy := first; fy <= last; fy++ {
		if !have[fy] {
			out = append(out, fy)
		}
	}
	return out
}

// FormatYears renders a year list as "FY2017, FY2018".
func FormatYears(ys []int) string {
	parts := make([]string, 0, len(ys))
	for _, y := range ys {
		parts = append(parts, fmt.Sprintf("FY%d", y))
	}
	return strings.Join(parts, ", ")
}
