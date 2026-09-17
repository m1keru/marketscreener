package criteria_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/m1ke/marketscreener/internal/config"
	"github.com/m1ke/marketscreener/internal/criteria"
	"github.com/m1ke/marketscreener/internal/edgar"
	"github.com/m1ke/marketscreener/internal/i18n"
	"github.com/m1ke/marketscreener/internal/metrics"
	"github.com/m1ke/marketscreener/internal/model"
)

// loadFixture reads a trimmed companyfacts document from testdata and flattens
// it the same way the live client does.
func loadFixture(t *testing.T, name, cik string) []model.Fact {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", name))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var cf edgar.CompanyFacts
	if err := json.Unmarshal(data, &cf); err != nil {
		t.Fatalf("parse fixture %s: %v", name, err)
	}
	facts := cf.Flatten(cik)
	if len(facts) == 0 {
		t.Fatalf("fixture %s produced no facts", name)
	}
	return facts
}

func evaluate(t *testing.T, fixture, cik string, c model.Company, price float64) ([]model.CriterionResult, model.Metrics) {
	t.Helper()
	cfg := config.Default()
	facts := loadFixture(t, fixture, cik)
	var p *model.Price
	if price > 0 {
		p = &model.Price{Ticker: c.Ticker, Price: price, AsOf: "2025-09-15", Source: "test"}
	}
	m, s := metrics.Compute(metrics.Input{Facts: facts, Price: p, HistoryYears: cfg.Thresholds.HistoryYears})
	return criteria.Evaluate(c, m, s, cfg), m
}

func statusOf(rs []model.CriterionResult, id string) (model.Status, string, bool) {
	for _, r := range rs {
		if r.ID == id {
			return r.Status, r.Reason, true
		}
	}
	return "", "", false
}

func TestCriteria(t *testing.T) {
	industrial := model.Company{CIK: "0000000101", Ticker: "GOOD", Title: "GOODCO INC", SIC: 3559, SICDesc: "Special Industry Machinery"}

	tests := []struct {
		name     string
		fixture  string
		cik      string
		company  model.Company
		price    float64
		want     map[string]model.Status
		verdict  model.Verdict
		checkFor func(t *testing.T, rs []model.CriterionResult, m model.Metrics)
	}{
		{
			name: "passes every criterion", fixture: "pass_all.json", cik: "0000000101",
			company: industrial, price: 40,
			want: map[string]model.Status{
				criteria.IDSize: model.Pass, criteria.IDCurrentRatio: model.Pass,
				criteria.IDDebt: model.Pass, criteria.IDEarningsStability: model.Pass,
				criteria.IDDividends: model.Pass, criteria.IDEPSGrowth: model.Pass,
				criteria.IDPE: model.Pass, criteria.IDPriceToBook: model.Pass,
			},
			verdict: model.VerdictPass,
			checkFor: func(t *testing.T, _ []model.CriterionResult, m model.Metrics) {
				if m.EPSAvg3Y == nil || round(*m.EPSAvg3Y, 2) != 3.40 {
					t.Errorf("3-year average EPS = %v, want 3.40", m.EPSAvg3Y)
				}
				if m.PE3YAvg == nil || round(*m.PE3YAvg, 2) != 11.76 {
					t.Errorf("P/E on 3-year EPS = %v, want 11.76", m.PE3YAvg)
				}
				if m.PB == nil || round(*m.PB, 2) != 0.80 {
					t.Errorf("P/B = %v, want 0.80", m.PB)
				}
				if m.MarketCap == nil || *m.MarketCap != 4e9 {
					t.Errorf("market cap = %v, want 4e9", m.MarketCap)
				}
				// (3.20+3.40+3.60)/3 over (2.00+2.10+2.20)/3 - 1 = 61.9%
				if m.EPSGrowth10YPct == nil || round(*m.EPSGrowth10YPct, 1) != 61.9 {
					t.Errorf("EPS growth = %v, want 61.9", m.EPSGrowth10YPct)
				}
			},
		},
		{
			name:    "a loss year in the middle of the decade fails stability",
			fixture: "negative_eps.json", cik: "0000000102",
			company: model.Company{CIK: "0000000102", Ticker: "CYC", SIC: 3559}, price: 40,
			want:    map[string]model.Status{criteria.IDEarningsStability: model.Fail},
			verdict: model.VerdictFail,
			checkFor: func(t *testing.T, rs []model.CriterionResult, _ model.Metrics) {
				_, reason, _ := statusOf(rs, criteria.IDEarningsStability)
				if want := "FY2019"; !contains(reason, want) {
					t.Errorf("reason %q does not name %s", reason, want)
				}
			},
		},
		{
			name:    "an insurer cannot be judged on liquidity",
			fixture: "financial_no_current.json", cik: "0000000103",
			company: model.Company{CIK: "0000000103", Ticker: "INS", SIC: 6311, SICDesc: "Life Insurance"},
			price:   40,
			want: map[string]model.Status{
				criteria.IDCurrentRatio: model.Unknown, criteria.IDDebt: model.Unknown,
				criteria.IDEarningsStability: model.Pass, criteria.IDPE: model.Pass,
			},
			verdict: model.VerdictIncomplete,
		},
		{
			name:    "fallback tags are used when the primary ones are absent",
			fixture: "fallback_tags.json", cik: "0000000104",
			company: model.Company{CIK: "0000000104", Ticker: "FBK", SIC: 3559}, price: 40,
			want: map[string]model.Status{
				criteria.IDEarningsStability: model.Pass, criteria.IDDividends: model.Pass,
				criteria.IDPriceToBook: model.Pass, criteria.IDDebt: model.Pass,
			},
			verdict: model.VerdictPass,
			checkFor: func(t *testing.T, _ []model.CriterionResult, m model.Metrics) {
				want := map[string]string{
					"eps":                "EarningsPerShareDiluted",
					"equity":             "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
					"long_term_debt":     "LongTermDebt",
					"dividends":          "CommonStockDividendsPerShareCashPaid",
					"shares_outstanding": "CommonStockSharesOutstanding",
				}
				for family, tag := range want {
					if m.Tags[family] != tag {
						t.Errorf("tag for %s = %q, want %q", family, m.Tags[family], tag)
					}
				}
			},
		},
		{
			name:    "a June fiscal year is handled like any other",
			fixture: "fiscal_year_june.json", cik: "0000000105",
			company: model.Company{CIK: "0000000105", Ticker: "JUN", SIC: 3559}, price: 40,
			want: map[string]model.Status{
				criteria.IDEarningsStability: model.Pass, criteria.IDDividends: model.Pass,
				criteria.IDEPSGrowth: model.Pass, criteria.IDCurrentRatio: model.Pass,
			},
			verdict: model.VerdictPass,
			checkFor: func(t *testing.T, _ []model.CriterionResult, m model.Metrics) {
				if m.EPSYearsAvailable != 10 {
					t.Errorf("EPS years available = %d, want 10", m.EPSYearsAvailable)
				}
			},
		},
		{
			name:    "a one-year dividend gap fails the dividend record",
			fixture: "dividend_gap.json", cik: "0000000106",
			company: model.Company{CIK: "0000000106", Ticker: "GAP", SIC: 3559}, price: 40,
			want:    map[string]model.Status{criteria.IDDividends: model.Fail, criteria.IDEarningsStability: model.Pass},
			verdict: model.VerdictFail,
			checkFor: func(t *testing.T, rs []model.CriterionResult, m model.Metrics) {
				_, reason, _ := statusOf(rs, criteria.IDDividends)
				if !contains(reason, "FY2020") {
					t.Errorf("reason %q does not name the missing year FY2020", reason)
				}
				if m.DividendYears == nil || *m.DividendYears != 9 {
					t.Errorf("dividend years = %v, want 9", m.DividendYears)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rs, m := evaluate(t, tc.fixture, tc.cik, tc.company, tc.price)
			for id, want := range tc.want {
				got, reason, ok := statusOf(rs, id)
				if !ok {
					t.Fatalf("criterion %s missing from the result set", id)
				}
				if got != want {
					t.Errorf("%s = %s (%s), want %s", id, got, reason, want)
				}
			}
			if v := criteria.Verdict(rs); v != tc.verdict {
				t.Errorf("verdict = %s, want %s", v, tc.verdict)
			}
			if tc.checkFor != nil {
				tc.checkFor(t, rs, m)
			}
		})
	}
}

// TestUnknownIsNeverSilent is the regression test for the defect that motivated
// the rewrite: a missing metric must surface as unknown with a reason, never as
// a quiet rejection.
func TestUnknownIsNeverSilent(t *testing.T) {
	rs, _ := evaluate(t, "financial_no_current.json", "0000000103",
		model.Company{CIK: "0000000103", Ticker: "INS", SIC: 6311, SICDesc: "Life Insurance"}, 40)
	for _, r := range rs {
		if r.Status == model.Unknown && r.Reason == "" {
			t.Errorf("criterion %s is unknown without a reason", r.ID)
		}
	}
}

// TestNoPriceLeavesCompanyUndecided: without a quote the price-based criteria
// are unknown, and the company lands in incomplete[] rather than being dropped.
func TestNoPriceLeavesCompanyUndecided(t *testing.T) {
	rs, _ := evaluate(t, "pass_all.json", "0000000101",
		model.Company{CIK: "0000000101", Ticker: "GOOD", SIC: 3559}, 0)
	for _, id := range []string{criteria.IDSize, criteria.IDPE, criteria.IDPriceToBook} {
		st, reason, _ := statusOf(rs, id)
		if st != model.Unknown {
			t.Errorf("%s = %s without a price, want unknown", id, st)
		}
		if !contains(reason, "price") {
			t.Errorf("%s reason %q should mention the missing price", id, reason)
		}
	}
	if v := criteria.Verdict(rs); v != model.VerdictIncomplete {
		t.Errorf("verdict = %s, want %s", v, model.VerdictIncomplete)
	}
}

func round(v float64, places int) float64 {
	p := 1.0
	for i := 0; i < places; i++ {
		p *= 10
	}
	return float64(int64(v*p+0.5)) / p
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && (len(needle) == 0 || indexOf(haystack, needle) >= 0)
}

func indexOf(h, n string) int {
	for i := 0; i+len(n) <= len(h); i++ {
		if h[i:i+len(n)] == n {
			return i
		}
	}
	return -1
}

// TestReasonsAreLocalised: the reason text is what the agent quotes into the
// report, so it has to follow the configured language — while ids, statuses and
// XBRL tags stay untranslated, because they are identifiers.
func TestReasonsAreLocalised(t *testing.T) {
	cfg := config.Default()
	cfg.Language = "ru"
	facts := loadFixture(t, "dividend_gap.json", "0000000106")
	price := &model.Price{Ticker: "GAP", Price: 40, AsOf: "2025-09-15", Source: "test"}
	m, s := metrics.Compute(metrics.Input{Facts: facts, Price: price, HistoryYears: cfg.Thresholds.HistoryYears})
	rs := criteria.Evaluate(model.Company{Ticker: "GAP", SIC: 3559}, m, s, cfg)

	byID := map[string]model.CriterionResult{}
	for _, r := range rs {
		byID[r.ID] = r
	}
	div := byID[criteria.IDDividends]
	if div.Status != model.Fail {
		t.Fatalf("dividend record = %s, want fail", div.Status)
	}
	if !contains(div.Reason, "дивиденд") {
		t.Errorf("reason is not in Russian: %q", div.Reason)
	}
	if !contains(div.Reason, "FY2020") {
		t.Errorf("the missing year must survive translation: %q", div.Reason)
	}
	if !contains(byID[criteria.IDPE].Target, "средняя EPS") {
		t.Errorf("target is not in Russian: %q", byID[criteria.IDPE].Target)
	}
	// Identifiers stay as they are.
	if byID[criteria.IDEarningsStability].Tag != "EarningsPerShareBasic" {
		t.Errorf("tag was altered: %q", byID[criteria.IDEarningsStability].Tag)
	}
	if criteria.LabelIn(i18n.For(i18n.RU), criteria.IDPE) == criteria.Label(criteria.IDPE) {
		t.Error("criterion labels should differ between languages")
	}
}
