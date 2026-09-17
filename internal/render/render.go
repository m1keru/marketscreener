// Package render prints results for humans (markdown) and machines (JSON).
// The MCP server uses the same renderers, so the CLI and the agent always see
// the same numbers.
package render

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/m1ke/marketscreener/internal/criteria"
	"github.com/m1ke/marketscreener/internal/i18n"
	"github.com/m1ke/marketscreener/internal/model"
	"github.com/m1ke/marketscreener/internal/screen"
)

// JSON writes v as indented JSON.
func JSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	return enc.Encode(v)
}

func num(p *float64, format string) string {
	if p == nil {
		return "—"
	}
	return fmt.Sprintf(format, *p)
}

func inum(p *int) string {
	if p == nil {
		return "—"
	}
	return fmt.Sprintf("%d", *p)
}

func money(p *float64) string {
	if p == nil {
		return "—"
	}
	v := *p
	switch {
	case v >= 1e12:
		return fmt.Sprintf("$%.2fT", v/1e12)
	case v >= 1e9:
		return fmt.Sprintf("$%.2fB", v/1e9)
	case v >= 1e6:
		return fmt.Sprintf("$%.0fM", v/1e6)
	default:
		return fmt.Sprintf("$%.0f", v)
	}
}

func pct(p *float64) string {
	if p == nil {
		return "—"
	}
	return fmt.Sprintf("%+.1f%%", *p)
}

// Screen renders a screening result as markdown in the given language.
func Screen(w io.Writer, r *screen.Result, lc *i18n.Catalog) error {
	b := &strings.Builder{}
	fmt.Fprint(b, lc.T(i18n.RenderScreenTitle, r.RunID))
	fmt.Fprint(b, lc.T(i18n.RenderScreenHeader,
		r.Profile, r.UniverseSize, r.Evaluated, r.Passed, float64(r.DataQuality.DurationMs)/1000))

	if len(r.Candidates) == 0 {
		fmt.Fprint(b, lc.T(i18n.RenderNoCandidates))
	} else {
		fmt.Fprint(b, lc.T(i18n.RenderCandidates, r.Passed))
		fmt.Fprint(b, lc.T(i18n.RenderTableHead))
		for _, c := range r.Candidates {
			fmt.Fprintf(b, "| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n",
				c.Ticker, trim(c.Name, 28), num(c.Price, "%.2f"), money(c.MarketCap),
				num(c.PE3YAvg, "%.1f"), num(c.PB, "%.2f"), num(c.GrahamNumber, "%.2f"),
				pct(mul100(c.MarginOfSafety)), num(c.CurrentRatio, "%.2f"),
				inum(c.EPSPositiveYears), inum(c.DividendYears), pct(c.EPSGrowth10YPct))
		}
		fmt.Fprintln(b)
	}

	if r.IncompleteTotal > 0 {
		fmt.Fprint(b, lc.T(i18n.RenderIncomplete, r.IncompleteTotal))
		for _, i := range r.Incomplete {
			fmt.Fprintf(b, "- **%s** %s — %s: %s\n  %s\n",
				i.Ticker, trim(i.Name, 40), lc.T(i18n.RenderBlocking),
				strings.Join(i.Blocking, ", "), i.Reason)
		}
		fmt.Fprintln(b)
	}

	if len(r.Diff.New) > 0 || len(r.Diff.Dropped) > 0 {
		fmt.Fprint(b, lc.T(i18n.RenderChangeSince, r.Diff.PreviousRunID))
		for _, n := range r.Diff.New {
			fmt.Fprintf(b, "- %s: **%s** %s (%s)\n",
				lc.T(i18n.RenderNew), n.Ticker, trim(n.Name, 40), n.FirstSeen)
		}
		for _, d := range r.Diff.Dropped {
			fmt.Fprintf(b, "- %s: **%s** — %s (%s: %s → %s)\n",
				lc.T(i18n.RenderDropped), d.Ticker, d.Reason, d.Criterion,
				num(d.Was, "%.2f"), num(d.Now, "%.2f"))
		}
		fmt.Fprintln(b)
	}

	q := r.DataQuality
	fmt.Fprint(b, lc.T(i18n.RenderDataQuality))
	fmt.Fprint(b, lc.T(i18n.RenderDataQualityLine,
		q.SECRequests, q.SEC429, q.SECRetries, q.CacheHits, q.PriceRequests, q.PriceMissing, q.FactsMissing))
	for _, n := range r.Notes {
		fmt.Fprintf(b, "\n> %s\n", n)
	}
	_, err := io.WriteString(w, b.String())
	return err
}

func mul100(p *float64) *float64 {
	if p == nil {
		return nil
	}
	v := *p * 100
	return &v
}

// Explain renders the full breakdown of one ticker.
func Explain(w io.Writer, r *screen.ExplainResult, lc *i18n.Catalog) error {
	b := &strings.Builder{}
	fmt.Fprintf(b, "# %s — %s\n\n", r.Ticker, r.Name)
	fmt.Fprint(b, lc.T(i18n.RenderExplainHeader,
		r.CIK, r.SIC, r.Sector, r.FiscalYearEndMonth, r.Verdict))

	m := r.Metrics
	fmt.Fprint(b, lc.T(i18n.RenderExplainPrices,
		num(m.Price, "%.2f"), m.PriceAsOf, m.PriceSource, money(m.MarketCap),
		num(m.PE3YAvg, "%.2f"), num(m.PB, "%.2f"), num(m.GrahamNumber, "%.2f")))

	fmt.Fprint(b, lc.T(i18n.RenderCriteria))
	fmt.Fprint(b, lc.T(i18n.RenderCriteriaHead))
	for _, c := range r.Criteria {
		fmt.Fprintf(b, "| %s | %s | %s | %s | %s | %s | %s |\n",
			c.ID, criteria.LabelIn(lc, c.ID), statusMark(lc, c.Status), num(c.Value, "%.2f"),
			c.Target, c.Tag, trim(c.Reason, 90))
	}
	fmt.Fprintln(b)

	writeSeries(b, lc.T(i18n.RenderSeriesEPS), r.Series.EPS, "%.2f")
	writeSeries(b, lc.T(i18n.RenderSeriesDiv), r.Series.Dividends, "%.4f")
	writeSeries(b, lc.T(i18n.RenderSeriesRevenue), r.Series.Revenue, "%.0f")
	writeSeries(b, lc.T(i18n.RenderSeriesEquity), r.Series.Equity, "%.0f")
	writeSeries(b, lc.T(i18n.RenderSeriesDebt), r.Series.Debt, "%.0f")

	if len(m.Accn) > 0 {
		fmt.Fprint(b, lc.T(i18n.RenderSources))
		for _, k := range sortedMapKeys(m.Accn) {
			fmt.Fprint(b, lc.T(i18n.RenderSourceLine, k, m.Tags[k], strings.Join(m.Accn[k], ", ")))
		}
	}
	_, err := io.WriteString(w, b.String())
	return err
}

func writeSeries(b *strings.Builder, title string, vs []model.YearValue, format string) {
	if len(vs) == 0 {
		return
	}
	fmt.Fprintf(b, "## %s\n\n", title)
	for _, v := range vs {
		fmt.Fprintf(b, "- FY%d (%s): "+format+"  `%s`\n", v.FY, v.End, v.Val, v.Tag)
	}
	fmt.Fprintln(b)
}

func statusMark(lc *i18n.Catalog, s model.Status) string {
	switch s {
	case model.Pass:
		return lc.T(i18n.StatusPass)
	case model.Fail:
		return lc.T(i18n.StatusFail)
	default:
		return lc.T(i18n.StatusUnknown)
	}
}

// History renders one raw series.
func History(w io.Writer, r *screen.HistoryResult, lc *i18n.Catalog) error {
	b := &strings.Builder{}
	fmt.Fprint(b, lc.T(i18n.RenderHistoryTitle, r.Ticker, r.Metric, r.Tag, r.Unit))
	if len(r.Values) == 0 {
		fmt.Fprint(b, lc.T(i18n.RenderNoValues))
	}
	for _, v := range r.Values {
		fmt.Fprintf(b, "- FY%d (%s): %g  `%s` %s\n", v.FY, v.End, v.Val, v.Tag, v.Accn)
	}
	_, err := io.WriteString(w, b.String())
	return err
}

// Runs renders the run list and an optional comparison.
func Runs(w io.Writer, r *screen.RunsResult, lc *i18n.Catalog) error {
	b := &strings.Builder{}
	fmt.Fprint(b, lc.T(i18n.RenderRuns))
	fmt.Fprint(b, lc.T(i18n.RenderRunsHead))
	for _, run := range r.Runs {
		fmt.Fprintf(b, "| %s | %s | %d | %d | %s |\n",
			run.RunID, run.StartedAt, run.UniverseSize, run.Passed, run.Profile)
	}
	if r.Compare != nil {
		c := r.Compare
		fmt.Fprintf(b, "\n## %s → %s\n\n", c.RunA, c.RunB)
		fmt.Fprintf(b, "%s: %s\n\n", lc.T(i18n.RenderKept), strings.Join(c.Kept, ", "))
		for _, n := range c.New {
			fmt.Fprintf(b, "- %s: %s\n", lc.T(i18n.RenderNew), n.Ticker)
		}
		for _, d := range c.Dropped {
			fmt.Fprintf(b, "- %s: %s — %s (%s: %s → %s)\n",
				lc.T(i18n.RenderDropped), d.Ticker, d.Reason, d.Criterion,
				num(d.Was, "%.2f"), num(d.Now, "%.2f"))
		}
	}
	_, err := io.WriteString(w, b.String())
	return err
}

// Funnel renders the rejection breakdown.
func Funnel(w io.Writer, r *screen.FunnelResult, lc *i18n.Catalog) error {
	b := &strings.Builder{}
	fmt.Fprint(b, lc.T(i18n.RenderFunnelTitle, r.RunID))
	fmt.Fprint(b, lc.T(i18n.RenderFunnelSummary, r.UniverseSize, r.Passed))
	fmt.Fprint(b, lc.T(i18n.RenderFunnelHead))
	for _, s := range r.Steps {
		label := s.Label
		if label == "" {
			label = s.Criterion
		}
		fmt.Fprintf(b, "| %s | %s (%s) | %d | %d |\n", s.Stage, label, s.Criterion, s.Dropped, s.Remaining)
	}
	_, err := io.WriteString(w, b.String())
	return err
}

func trim(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}

func sortedMapKeys(m map[string][]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	for i := range out {
		for j := i + 1; j < len(out); j++ {
			if out[j] < out[i] {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}
