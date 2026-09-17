// Package metrics turns raw XBRL facts into the numbers the criteria compare.
package metrics

// Family names a metric and the ordered list of XBRL tags that may carry it.
// The first tag with usable data wins and is recorded in the trace, so every
// number in a report can be traced back to the tag it came from.
type Family struct {
	Name     string
	Taxonomy string
	Unit     string
	Instant  bool
	Tags     []string
}

// The tag table from the design document. Order is priority.
var (
	EPS = Family{Name: "EPS", Taxonomy: "us-gaap", Unit: "USD/shares", Tags: []string{
		"EarningsPerShareBasic",
		"EarningsPerShareDiluted",
	}}
	NetIncome = Family{Name: "NetIncome", Taxonomy: "us-gaap", Unit: "USD", Tags: []string{
		"NetIncomeLoss",
		"ProfitLoss",
	}}
	Assets = Family{Name: "Assets", Taxonomy: "us-gaap", Unit: "USD", Instant: true, Tags: []string{
		"Assets",
	}}
	Liabilities = Family{Name: "Liabilities", Taxonomy: "us-gaap", Unit: "USD", Instant: true, Tags: []string{
		"Liabilities",
	}}
	AssetsCurrent = Family{Name: "AssetsCurrent", Taxonomy: "us-gaap", Unit: "USD", Instant: true, Tags: []string{
		"AssetsCurrent",
	}}
	LiabilitiesCurrent = Family{Name: "LiabilitiesCurrent", Taxonomy: "us-gaap", Unit: "USD", Instant: true, Tags: []string{
		"LiabilitiesCurrent",
	}}
	Equity = Family{Name: "Equity", Taxonomy: "us-gaap", Unit: "USD", Instant: true, Tags: []string{
		"StockholdersEquity",
		"StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
	}}
	LongTermDebt = Family{Name: "LongTermDebt", Taxonomy: "us-gaap", Unit: "USD", Instant: true, Tags: []string{
		"LongTermDebtNoncurrent",
		"LongTermDebt",
		"LongTermDebtAndCapitalLeaseObligations",
	}}
	DividendsPerShare = Family{Name: "DividendsPerShare", Taxonomy: "us-gaap", Unit: "USD/shares", Tags: []string{
		"CommonStockDividendsPerShareDeclared",
		"CommonStockDividendsPerShareCashPaid",
	}}
	DividendsPaid = Family{Name: "DividendsPaid", Taxonomy: "us-gaap", Unit: "USD", Tags: []string{
		"PaymentsOfDividendsCommonStock",
		"PaymentsOfDividends",
	}}
	Revenue = Family{Name: "Revenue", Taxonomy: "us-gaap", Unit: "USD", Tags: []string{
		"RevenueFromContractWithCustomerExcludingAssessedTax",
		"Revenues",
		"SalesRevenueNet",
	}}
	// SharesOutstanding starts in the dei namespace, not us-gaap.
	SharesOutstanding = Family{Name: "SharesOutstanding", Taxonomy: "dei", Unit: "shares", Instant: true, Tags: []string{
		"dei:EntityCommonStockSharesOutstanding",
		"CommonStockSharesOutstanding",
		"WeightedAverageNumberOfSharesOutstandingBasic",
	}}
)

// All is every family, in the order the frames stage fetches them.
var All = []Family{
	AssetsCurrent, LiabilitiesCurrent, Assets, Liabilities, Equity,
	LongTermDebt, SharesOutstanding, EPS, DividendsPerShare, DividendsPaid, Revenue, NetIncome,
}

// KnownTags is every tag the screener can actually read, in the qualified form
// facts are stored under. Caching anything else is dead weight: a single
// registrant's companyfacts document runs to tens of thousands of facts, of
// which the criteria touch a few hundred.
func KnownTags() map[string]bool {
	out := make(map[string]bool)
	for _, f := range All {
		for _, tag := range f.Tags {
			out[tag] = true
		}
	}
	return out
}

// FrameSpec describes how one tag is queried from the frames API. The dei tag
// and the us-gaap fallbacks of the same family live in different taxonomies, so
// the taxonomy travels per tag rather than per family.
type FrameSpec struct {
	Family   Family
	Tag      string
	Taxonomy string
	Unit     string
	Instant  bool
}

// FrameSpecs expands a family into per-tag frame queries.
func FrameSpecs(f Family) []FrameSpec {
	out := make([]FrameSpec, 0, len(f.Tags))
	for _, tag := range f.Tags {
		taxonomy, unit, instant := f.Taxonomy, f.Unit, f.Instant
		if len(tag) > 4 && tag[:4] == "dei:" {
			taxonomy, tag = "dei", tag[4:]
		} else {
			taxonomy = "us-gaap"
		}
		// WeightedAverageNumberOfSharesOutstandingBasic is a duration concept
		// even though the rest of its family is instant.
		if tag == "WeightedAverageNumberOfSharesOutstandingBasic" {
			instant = false
		}
		out = append(out, FrameSpec{Family: f, Tag: tag, Taxonomy: taxonomy, Unit: unit, Instant: instant})
	}
	return out
}

// QualifiedTag renders the tag the way facts are stored: the dei namespace is
// kept, us-gaap is implicit.
func (s FrameSpec) QualifiedTag() string {
	if s.Taxonomy == "us-gaap" {
		return s.Tag
	}
	return s.Taxonomy + ":" + s.Tag
}
