package edgar

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/m1ke/marketscreener/internal/model"
)

// FlexNumber accepts a JSON value that EDGAR serves inconsistently as either a
// number or a zero-padded string: companyfacts does both for "cik", depending
// on the registrant.
type FlexNumber string

// UnmarshalJSON accepts numbers, strings and null.
func (f *FlexNumber) UnmarshalJSON(b []byte) error {
	s := strings.TrimSpace(string(b))
	if s == "null" {
		*f = ""
		return nil
	}
	s = strings.Trim(s, `"`)
	*f = FlexNumber(s)
	return nil
}

// String renders the raw value.
func (f FlexNumber) String() string { return string(f) }

// Int parses the value, returning zero when it is not a number.
func (f FlexNumber) Int() int {
	n, err := strconv.Atoi(strings.TrimLeft(string(f), "0"))
	if err != nil {
		return 0
	}
	return n
}

// TickerEntry is one row of company_tickers.json.
type TickerEntry struct {
	CIK    string
	Ticker string
	Title  string
}

// CompanyTickers fetches the ticker -> CIK map (~12k rows). The file is a JSON
// object keyed by row index, not an array.
func (c *Client) CompanyTickers(ctx context.Context) ([]TickerEntry, error) {
	var raw map[string]struct {
		CIKStr FlexNumber `json:"cik_str"`
		Ticker string     `json:"ticker"`
		Title  string     `json:"title"`
	}
	url := c.wwwHost + "/files/company_tickers.json"
	if err := c.get(ctx, url, &raw); err != nil {
		return nil, err
	}
	out := make([]TickerEntry, 0, len(raw))
	for _, v := range raw {
		if v.Ticker == "" {
			continue
		}
		out = append(out, TickerEntry{
			CIK:    PadCIK(v.CIKStr.String()),
			Ticker: strings.ToUpper(strings.TrimSpace(v.Ticker)),
			Title:  v.Title,
		})
	}
	// company_tickers.json is a map, so iteration order is random; sort to
	// keep the universe deterministic between runs.
	sort.Slice(out, func(i, j int) bool {
		if out[i].CIK != out[j].CIK {
			return out[i].CIK < out[j].CIK
		}
		return out[i].Ticker < out[j].Ticker
	})
	return out, nil
}

// Submission is the slice of submissions/CIK{cik}.json we need: identity and SIC.
type Submission struct {
	CIK            string `json:"cik"`
	Name           string `json:"name"`
	SIC            string `json:"sic"`
	SICDescription string `json:"sicDescription"`
	Exchanges      []string
}

// Submissions returns company metadata, most importantly the SIC code that
// drives the sector branches of the criteria.
func (c *Client) Submissions(ctx context.Context, cik string) (*Submission, error) {
	var s Submission
	url := fmt.Sprintf("%s/submissions/CIK%s.json", c.dataHost, PadCIK(cik))
	if err := c.get(ctx, url, &s); err != nil {
		return nil, err
	}
	return &s, nil
}

// SICInt parses the SIC code, which EDGAR serves as a string.
func (s *Submission) SICInt() int {
	n, err := strconv.Atoi(strings.TrimSpace(s.SIC))
	if err != nil {
		return 0
	}
	return n
}

// FrameFact is one company's value inside a frame.
type FrameFact struct {
	CIK      FlexNumber `json:"cik"`
	Entity   string     `json:"entityName"`
	Start    string     `json:"start"`
	End      string     `json:"end"`
	Val      float64    `json:"val"`
	Accn     string     `json:"accn"`
	FY       int        `json:"fy"`
	FP       string     `json:"fp"`
	Form     string     `json:"form"`
	Filed    string     `json:"filed"`
	Frame    string     `json:"frame"`
	UOM      string     `json:"uom"`
	Taxonomy string     `json:"taxonomy"`
}

// FrameResponse is the body of the frames endpoint.
type FrameResponse struct {
	Taxonomy string      `json:"taxonomy"`
	Tag      string      `json:"tag"`
	CCP      string      `json:"ccp"`
	UOM      string      `json:"uom"`
	Label    string      `json:"label"`
	Pts      int         `json:"pts"`
	Data     []FrameFact `json:"data"`
}

// FrameID names one quarterly frame. Instant frames (balance sheet items)
// carry the trailing "I".
type FrameID struct {
	Year    int
	Quarter int
	Instant bool
}

// String renders the frame id. Quarter 0 means the annual frame, which is what
// full-year duration concepts such as annual EPS live in.
func (f FrameID) String() string {
	if f.Quarter == 0 {
		return fmt.Sprintf("CY%d", f.Year)
	}
	s := fmt.Sprintf("CY%dQ%d", f.Year, f.Quarter)
	if f.Instant {
		s += "I"
	}
	return s
}

// AnnualFrames returns the n most recent completed calendar-year frames.
// The current year is never complete, so it is skipped.
func AnnualFrames(now time.Time, n int) []FrameID {
	out := make([]FrameID, 0, n)
	for i := 1; i <= n; i++ {
		out = append(out, FrameID{Year: now.Year() - i})
	}
	return out
}

// RecentFrames returns the n most recent quarterly frames at or before now,
// most recent first. SEC publishes a frame some weeks after the quarter ends,
// so the newest one is often still empty; callers query several.
func RecentFrames(now time.Time, n int, instant bool) []FrameID {
	q := (int(now.Month())-1)/3 + 1
	y := now.Year()
	out := make([]FrameID, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, FrameID{Year: y, Quarter: q, Instant: instant})
		q--
		if q == 0 {
			q = 4
			y--
		}
	}
	return out
}

// Frame fetches one frame. A missing frame is reported as ErrNotFound, which
// callers treat as "nothing here yet" rather than as a failure.
func (c *Client) Frame(ctx context.Context, taxonomy, tag, unit string, id FrameID) (*FrameResponse, error) {
	var fr FrameResponse
	url := fmt.Sprintf("%s/api/xbrl/frames/%s/%s/%s/%s.json", c.dataHost, taxonomy, tag, unit, id.String())
	if err := c.get(ctx, url, &fr); err != nil {
		return nil, err
	}
	return &fr, nil
}

// LatestFromFrames queries the given frames for one tag and keeps, per CIK, the
// single freshest fact: the largest `end`, ties broken by accession number.
//
// Querying one frame is not enough and this is a correctness matter, not an
// optimisation: companies whose fiscal year does not end in December land in
// neighbouring frames, and a single-frame query silently drops them.
func (c *Client) LatestFromFrames(ctx context.Context, taxonomy, tag, unit string, frames []FrameID) (map[string]model.Fact, error) {
	best := make(map[string]model.Fact)
	var firstErr error
	found := 0
	for _, id := range frames {
		fr, err := c.Frame(ctx, taxonomy, tag, unit, id)
		if err != nil {
			var nf *ErrNotFound
			if errors.As(err, &nf) {
				continue // frame not published yet
			}
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		found++
		for _, d := range fr.Data {
			cik := PadCIK(d.CIK.String())
			f := model.Fact{
				CIK: cik, Tag: tag, Unit: unit,
				Start: d.Start, End: d.End, FY: d.FY, FP: d.FP,
				Form: d.Form, Val: d.Val, Accn: d.Accn, Filed: d.Filed,
			}
			if cur, ok := best[cik]; !ok || fresher(f, cur) {
				best[cik] = f
			}
		}
	}
	if found == 0 && firstErr != nil {
		return nil, firstErr
	}
	return best, nil
}

// fresher reports whether a should replace b: later period end wins, then the
// higher accession number (later filing).
func fresher(a, b model.Fact) bool {
	if a.End != b.End {
		return a.End > b.End
	}
	return a.Accn > b.Accn
}

// CompanyFacts is the whole XBRL history of one registrant.
type CompanyFacts struct {
	CIK        FlexNumber `json:"cik"`
	EntityName string     `json:"entityName"`
	Facts      map[string]map[string]struct {
		Label string `json:"label"`
		Units map[string][]struct {
			Start string     `json:"start"`
			End   string     `json:"end"`
			Val   float64    `json:"val"`
			Accn  string     `json:"accn"`
			FY    FlexNumber `json:"fy"`
			FP    string     `json:"fp"`
			Form  string     `json:"form"`
			Filed string     `json:"filed"`
			Frame string     `json:"frame"`
		} `json:"units"`
	} `json:"facts"`
}

// CompanyFacts fetches every fact of one company in a single request.
func (c *Client) CompanyFacts(ctx context.Context, cik string) (*CompanyFacts, error) {
	var cf CompanyFacts
	url := fmt.Sprintf("%s/api/xbrl/companyfacts/CIK%s.json", c.dataHost, PadCIK(cik))
	if err := c.get(ctx, url, &cf); err != nil {
		return nil, err
	}
	return &cf, nil
}

// Flatten converts the nested companyfacts document into flat facts.
func (cf *CompanyFacts) Flatten(cik string) []model.Fact {
	cik = PadCIK(cik)
	var out []model.Fact
	for taxonomy, tags := range cf.Facts {
		for tag, body := range tags {
			name := tag
			if taxonomy != "us-gaap" {
				name = taxonomy + ":" + tag
			}
			for unit, points := range body.Units {
				for _, p := range points {
					out = append(out, model.Fact{
						CIK: cik, Tag: name, Unit: unit,
						Start: p.Start, End: p.End, FY: p.FY.Int(), FP: p.FP,
						Form: p.Form, Val: p.Val, Accn: p.Accn, Filed: p.Filed,
					})
				}
			}
		}
	}
	// Fully ordered, including the period start: two facts of one filing can
	// share tag, end and accession and differ only in the period they cover.
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		switch {
		case a.Tag != b.Tag:
			return a.Tag < b.Tag
		case a.End != b.End:
			return a.End < b.End
		case a.Start != b.Start:
			return a.Start < b.Start
		default:
			return a.Accn < b.Accn
		}
	})
	return out
}

// CompanyConcept fetches the full history of a single tag for one company.
// Used for targeted top-ups when companyfacts is unavailable.
func (c *Client) CompanyConcept(ctx context.Context, cik, taxonomy, tag string) ([]model.Fact, error) {
	var resp struct {
		CIK   FlexNumber `json:"cik"`
		Tag   string     `json:"tag"`
		Units map[string][]struct {
			Start string     `json:"start"`
			End   string     `json:"end"`
			Val   float64    `json:"val"`
			Accn  string     `json:"accn"`
			FY    FlexNumber `json:"fy"`
			FP    string     `json:"fp"`
			Form  string     `json:"form"`
			Filed string     `json:"filed"`
		} `json:"units"`
	}
	url := fmt.Sprintf("%s/api/xbrl/companyconcept/CIK%s/%s/%s.json", c.dataHost, PadCIK(cik), taxonomy, tag)
	if err := c.get(ctx, url, &resp); err != nil {
		return nil, err
	}
	name := tag
	if taxonomy != "us-gaap" {
		name = taxonomy + ":" + tag
	}
	var out []model.Fact
	for unit, points := range resp.Units {
		for _, p := range points {
			out = append(out, model.Fact{
				CIK: PadCIK(cik), Tag: name, Unit: unit,
				Start: p.Start, End: p.End, FY: p.FY.Int(), FP: p.FP,
				Form: p.Form, Val: p.Val, Accn: p.Accn, Filed: p.Filed,
			})
		}
	}
	return out, nil
}
