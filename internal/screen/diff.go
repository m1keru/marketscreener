package screen

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/m1ke/marketscreener/internal/criteria"
	"github.com/m1ke/marketscreener/internal/i18n"
	"github.com/m1ke/marketscreener/internal/model"
	"github.com/m1ke/marketscreener/internal/store"
)

// diff compares this run's candidates with the previous run's. Every dropped
// ticker carries a criterion and the two values behind the change: the reason
// is computed here, never left to a model to narrate.
func (s *Screener) diff(ctx context.Context, lc *i18n.Catalog, prev *model.Run, evals []model.Evaluation,
	drops map[string]dropRecord, now time.Time) Diff {

	d := Diff{PreviousRunID: prev.ID, New: []NewEntry{}, Dropped: []DroppedEntry{}}
	prevRows, err := s.Store.RunResults(ctx, prev.ID)
	if err != nil {
		s.log().Warn("previous run results", "run", prev.ID, "err", err)
		return d
	}
	current := make(map[string]model.Evaluation, len(evals))
	for _, ev := range evals {
		current[ev.Company.Ticker] = ev
	}

	for ticker, ev := range current {
		if ev.Verdict != model.VerdictPass {
			continue
		}
		if r, ok := prevRows[ticker]; ok && r.Verdict == model.VerdictPass {
			continue
		}
		first, _ := s.Store.FirstSeen(ctx, ticker)
		if first == "" {
			first = now.Format("2006-01-02")
		}
		d.New = append(d.New, NewEntry{Ticker: ticker, Name: ev.Company.Title, FirstSeen: first})
	}
	sort.Slice(d.New, func(i, j int) bool { return d.New[i].Ticker < d.New[j].Ticker })

	for ticker, r := range prevRows {
		if r.Verdict != model.VerdictPass {
			continue
		}
		if ev, ok := current[ticker]; ok && ev.Verdict == model.VerdictPass {
			continue
		}
		lastSeen, _ := s.Store.LastSeen(ctx, ticker, now)
		d.Dropped = append(d.Dropped, s.explainDrop(lc, ticker, r, current, drops, lastSeen))
	}
	sort.Slice(d.Dropped, func(i, j int) bool { return d.Dropped[i].Ticker < d.Dropped[j].Ticker })
	return d
}

func (s *Screener) explainDrop(lc *i18n.Catalog, ticker string, prev store.RunResult,
	current map[string]model.Evaluation, drops map[string]dropRecord, lastSeen string) DroppedEntry {

	e := DroppedEntry{Ticker: ticker, LastSeen: lastSeen}
	was := func(id string) *float64 {
		for _, c := range prev.Criteria {
			if c.ID == id {
				return c.Value
			}
		}
		return nil
	}

	if ev, ok := current[ticker]; ok {
		// The company was evaluated in full this time: name the criterion that
		// changed status, failures before unknowns.
		if c := ev.First(model.Fail); c != nil {
			e.Criterion, e.Status, e.Now, e.Was = c.ID, string(model.Fail), c.Value, was(c.ID)
			e.Reason = failReason(lc, *c)
			return e
		}
		if c := ev.First(model.Unknown); c != nil {
			e.Criterion, e.Status, e.Now, e.Was = c.ID, string(model.Unknown), c.Value, was(c.ID)
			e.Reason = lc.T(i18n.DropUndecided, criteria.LabelIn(lc, c.ID), c.Reason)
			return e
		}
	}
	if d, ok := drops[ticker]; ok {
		e.Criterion, e.Status, e.Now, e.Was = d.criterion, string(d.status), d.value, was(d.criterion)
		e.Reason = d.reason
		return e
	}
	e.Status = string(model.Unknown)
	e.Reason = lc.T(i18n.DropOutsideUniverse)
	return e
}

// Compare diffs two stored runs by id, for the runs tool.
func (s *Screener) Compare(ctx context.Context, runA, runB string) (*CompareResult, error) {
	a, err := s.Store.RunResults(ctx, runA)
	if err != nil {
		return nil, err
	}
	b, err := s.Store.RunResults(ctx, runB)
	if err != nil {
		return nil, err
	}
	if len(a) == 0 {
		return nil, fmt.Errorf("run %s has no stored results", runA)
	}
	if len(b) == 0 {
		return nil, fmt.Errorf("run %s has no stored results", runB)
	}
	lc := s.Cfg.Catalog()
	out := &CompareResult{RunA: runA, RunB: runB, New: []NewEntry{}, Dropped: []DroppedEntry{}, Kept: []string{}}
	for ticker, rb := range b {
		if rb.Verdict != model.VerdictPass {
			continue
		}
		if ra, ok := a[ticker]; ok && ra.Verdict == model.VerdictPass {
			out.Kept = append(out.Kept, ticker)
			continue
		}
		out.New = append(out.New, NewEntry{Ticker: ticker})
	}
	for ticker, ra := range a {
		if ra.Verdict != model.VerdictPass {
			continue
		}
		if rb, ok := b[ticker]; ok && rb.Verdict == model.VerdictPass {
			continue
		}
		e := DroppedEntry{Ticker: ticker}
		if rb, ok := b[ticker]; ok {
			if c := firstOf(rb.Criteria, model.Fail); c != nil {
				e.Criterion, e.Status, e.Now = c.ID, string(model.Fail), c.Value
				e.Reason = failReason(lc, *c)
			} else if c := firstOf(rb.Criteria, model.Unknown); c != nil {
				e.Criterion, e.Status, e.Now = c.ID, string(model.Unknown), c.Value
				e.Reason = lc.T(i18n.DropUndecided, criteria.LabelIn(lc, c.ID), c.Reason)
			}
			for _, c := range ra.Criteria {
				if c.ID == e.Criterion {
					e.Was = c.Value
				}
			}
		} else {
			e.Reason = "not present in run " + runB
		}
		out.Dropped = append(out.Dropped, e)
	}
	sort.Slice(out.New, func(i, j int) bool { return out.New[i].Ticker < out.New[j].Ticker })
	sort.Slice(out.Dropped, func(i, j int) bool { return out.Dropped[i].Ticker < out.Dropped[j].Ticker })
	sort.Strings(out.Kept)
	return out, nil
}

func firstOf(cs []model.CriterionResult, st model.Status) *model.CriterionResult {
	for i := range cs {
		if cs[i].Status == st {
			return &cs[i]
		}
	}
	return nil
}
