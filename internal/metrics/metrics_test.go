package metrics_test

import (
	"testing"
	"time"

	"github.com/m1ke/marketscreener/internal/metrics"
	"github.com/m1ke/marketscreener/internal/model"
)

func TestFiscalYearLabelling(t *testing.T) {
	cases := map[string]int{
		"2024-12-31": 2024, // calendar year
		"2024-06-30": 2024, // June year end
		"2024-01-31": 2023, // retailer year ending in January belongs to 2023
		"2025-01-31": 2024, // and the next one to 2024: consecutive years never collide
	}
	for end, want := range cases {
		got, ok := metrics.FiscalYear(end)
		if !ok {
			t.Fatalf("FiscalYear(%q) failed to parse", end)
		}
		if got != want {
			t.Errorf("FiscalYear(%q) = %d, want %d", end, got, want)
		}
	}
}

// TestRestatementPrefersTheLatestFiling: when the same fiscal year is reported
// twice, the newer filing wins.
func TestRestatementPrefersTheLatestFiling(t *testing.T) {
	facts := []model.Fact{
		{CIK: "1", Tag: "EarningsPerShareBasic", Unit: "USD/shares", Start: "2024-01-01", End: "2024-12-31",
			Val: 3.00, Form: "10-K", Accn: "a-1", Filed: "2025-02-15"},
		{CIK: "1", Tag: "EarningsPerShareBasic", Unit: "USD/shares", Start: "2024-01-01", End: "2024-12-31",
			Val: 2.80, Form: "10-K", Accn: "a-2", Filed: "2026-02-15"}, // restated a year later
	}
	series, tag := metrics.NewExtractor(facts).AnnualSeries(metrics.EPS)
	if tag != "EarningsPerShareBasic" {
		t.Fatalf("tag = %q", tag)
	}
	if len(series) != 1 {
		t.Fatalf("got %d years, want 1", len(series))
	}
	if series[0].Val != 2.80 {
		t.Errorf("EPS = %v, want the restated 2.80", series[0].Val)
	}
}

// TestLatestInstantMayComeFromAQuarterly: a fresher balance sheet from a 10-Q
// beats the last annual one for point-in-time metrics.
func TestLatestInstantMayComeFromAQuarterly(t *testing.T) {
	facts := []model.Fact{
		{CIK: "1", Tag: "AssetsCurrent", Unit: "USD", End: "2024-12-31", Val: 100, Form: "10-K", Accn: "a-1"},
		{CIK: "1", Tag: "AssetsCurrent", Unit: "USD", End: "2025-06-30", Val: 130, Form: "10-Q", Accn: "a-2"},
	}
	e := metrics.NewExtractor(facts)
	f, tag := e.Latest(metrics.AssetsCurrent)
	if f == nil {
		t.Fatal("no latest fact")
	}
	if f.Val != 130 || tag != "AssetsCurrent" {
		t.Errorf("latest = %+v (tag %s), want the 130 from the 10-Q", f, tag)
	}
	// The annual series, on the other hand, only takes annual filings.
	series, _ := e.AnnualSeries(metrics.AssetsCurrent)
	if len(series) != 1 || series[0].Val != 100 {
		t.Errorf("annual series = %+v, want only the 10-K value", series)
	}
}

func TestQuarterlyDurationsAreNotTreatedAsAnnual(t *testing.T) {
	facts := []model.Fact{
		{CIK: "1", Tag: "EarningsPerShareBasic", Unit: "USD/shares", Start: "2024-10-01", End: "2024-12-31",
			Val: 0.9, Form: "10-K", Accn: "q4"},
		{CIK: "1", Tag: "EarningsPerShareBasic", Unit: "USD/shares", Start: "2024-01-01", End: "2024-12-31",
			Val: 3.6, Form: "10-K", Accn: "fy"},
	}
	series, _ := metrics.NewExtractor(facts).AnnualSeries(metrics.EPS)
	if len(series) != 1 || series[0].Val != 3.6 {
		t.Errorf("series = %+v, want only the full-year 3.6", series)
	}
}

func TestFiscalYearEndInference(t *testing.T) {
	facts := []model.Fact{
		{CIK: "1", Tag: "Revenues", Unit: "USD", Start: "2023-07-01", End: "2024-06-30", Val: 10, Form: "10-K", Accn: "a"},
		{CIK: "1", Tag: "Revenues", Unit: "USD", Start: "2022-07-01", End: "2023-06-30", Val: 9, Form: "10-K", Accn: "b"},
	}
	if got := metrics.NewExtractor(facts).FiscalYearEndMonth(); got != time.June {
		t.Errorf("fiscal year end = %v, want June", got)
	}
	// No annual facts at all: December is the safe default.
	if got := metrics.NewExtractor(nil).FiscalYearEndMonth(); got != time.December {
		t.Errorf("default fiscal year end = %v, want December", got)
	}
}

func TestMissingYearsAreListed(t *testing.T) {
	series := []model.YearValue{{FY: 2018, Val: 1}, {FY: 2019, Val: 1}, {FY: 2022, Val: 1}}
	got := metrics.MissingYears(series, 5) // window 2018..2022
	if len(got) != 2 || got[0] != 2020 || got[1] != 2021 {
		t.Errorf("missing years = %v, want [2020 2021]", got)
	}
	if s := metrics.FormatYears(got); s != "FY2020, FY2021" {
		t.Errorf("formatted = %q", s)
	}
}

// TestSeriesFillsGapsFromFallbackTags: a registrant that stops tagging the
// primary concept and reports only the fallback must keep an unbroken series,
// with each year labelled by the tag it came from.
func TestSeriesFillsGapsFromFallbackTags(t *testing.T) {
	facts := []model.Fact{
		{CIK: "1", Tag: "EarningsPerShareBasic", Unit: "USD/shares", Start: "2023-01-01", End: "2023-12-31",
			Val: 3.4, Form: "10-K", Accn: "a-1"},
		{CIK: "1", Tag: "EarningsPerShareDiluted", Unit: "USD/shares", Start: "2023-01-01", End: "2023-12-31",
			Val: 3.3, Form: "10-K", Accn: "a-1"},
		// 2024: only the diluted figure was tagged.
		{CIK: "1", Tag: "EarningsPerShareDiluted", Unit: "USD/shares", Start: "2024-01-01", End: "2024-12-31",
			Val: 3.6, Form: "10-K", Accn: "a-2"},
	}
	series, tag := metrics.NewExtractor(facts).AnnualSeries(metrics.EPS)
	if tag != "EarningsPerShareBasic" {
		t.Errorf("primary tag = %q, want EarningsPerShareBasic", tag)
	}
	if len(series) != 2 {
		t.Fatalf("series = %+v, want both years", series)
	}
	if series[0].Val != 3.4 || series[0].Tag != "EarningsPerShareBasic" {
		t.Errorf("2023 = %+v, want the basic figure", series[0])
	}
	if series[1].Val != 3.6 || series[1].Tag != "EarningsPerShareDiluted" {
		t.Errorf("2024 = %+v, want the diluted figure filling the gap", series[1])
	}
}
