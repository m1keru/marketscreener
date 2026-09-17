package screen

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/m1ke/marketscreener/internal/config"
	"github.com/m1ke/marketscreener/internal/criteria"
	"github.com/m1ke/marketscreener/internal/edgar"
	"github.com/m1ke/marketscreener/internal/i18n"
	"github.com/m1ke/marketscreener/internal/metrics"
	"github.com/m1ke/marketscreener/internal/model"
	"github.com/m1ke/marketscreener/internal/prices"
	"github.com/m1ke/marketscreener/internal/store"
)

// SICUnknown marks a registrant whose SIC has not been fetched yet. It is not
// zero, because zero is a SIC code EDGAR really uses for blank filers.
const SICUnknown = -1

// Screener owns the three-stage pipeline.
type Screener struct {
	Cfg     config.Config
	Edgar   *edgar.Client
	Prices  *prices.Provider
	Store   *store.Store
	Log     *slog.Logger
	Now     func() time.Time
	Workers int
}

func (s *Screener) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now().UTC()
}

func (s *Screener) workers() int {
	if s.Workers > 0 {
		return s.Workers
	}
	return 8
}

func (s *Screener) log() *slog.Logger {
	if s.Log != nil {
		return s.Log
	}
	return slog.Default()
}

// derivativeSuffix matches warrants, units, rights and preferred series. Class
// shares such as BRK-B are deliberately not matched.
var derivativeSuffix = regexp.MustCompile(`-(W|WS|WT|WD|U|UN|R|RT|P[A-Z]?)$`)

// Universe builds stage 0: the deduplicated list of operating registrants.
func (s *Screener) Universe(ctx context.Context, refresh bool) ([]model.Company, error) {
	ttl := s.Cfg.FactsTTL()
	if !refresh {
		fresh, err := s.Store.Fresh(ctx, "tickers", ttl)
		if err == nil && fresh {
			cs, err := s.Store.AllCompanies(ctx)
			if err == nil && len(cs) > 0 {
				s.Edgar.Stats.CacheHits.Add(1)
				return cs, nil
			}
		}
	}
	entries, err := s.Edgar.CompanyTickers(ctx)
	if err != nil {
		return nil, fmt.Errorf("company_tickers.json: %w", err)
	}
	byCIK := make(map[string]model.Company, len(entries))
	for _, e := range entries {
		if derivativeSuffix.MatchString(e.Ticker) {
			continue
		}
		// Several tickers map to one CIK: share classes, and preferred or
		// legacy lines. Keep the shortest, which is the common share in
		// practice (MSA over MNESP, GOOG over GOOGL), and break ties
		// lexicographically so the universe is identical between runs.
		if cur, ok := byCIK[e.CIK]; ok && preferredTicker(cur.Ticker, e.Ticker) == cur.Ticker {
			continue
		}
		byCIK[e.CIK] = model.Company{CIK: e.CIK, Ticker: e.Ticker, Title: e.Title, SIC: SICUnknown}
	}
	out := make([]model.Company, 0, len(byCIK))
	for _, c := range byCIK {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CIK < out[j].CIK })

	// Preserve SIC codes already known from earlier runs.
	if known, err := s.Store.AllCompanies(ctx); err == nil {
		sic := make(map[string]model.Company, len(known))
		for _, c := range known {
			sic[c.CIK] = c
		}
		for i := range out {
			if k, ok := sic[out[i].CIK]; ok && k.SIC != SICUnknown {
				out[i].SIC, out[i].SICDesc = k.SIC, k.SICDesc
			}
		}
	}
	if err := s.Store.SaveCompanies(ctx, out); err != nil {
		s.log().Warn("cache companies", "err", err)
	}
	if err := s.Store.MarkFetched(ctx, "tickers"); err != nil {
		s.log().Warn("mark tickers fetched", "err", err)
	}
	return out, nil
}

// preferredTicker picks which of two tickers of one registrant represents the
// company: the shorter one, then the lexicographically smaller.
func preferredTicker(a, b string) string {
	if len(a) != len(b) {
		if len(a) < len(b) {
			return a
		}
		return b
	}
	if a <= b {
		return a
	}
	return b
}

// stage1Instant are the balance sheet families read from instant frames.
var stage1Instant = []metrics.Family{
	metrics.AssetsCurrent, metrics.LiabilitiesCurrent, metrics.Equity,
	metrics.LongTermDebt, metrics.SharesOutstanding,
}

// FrameQuarters is how many quarterly frames are queried per instant tag.
// One is not enough: companies whose fiscal year ends off-calendar report into
// neighbouring frames and would vanish from a single-frame query.
const FrameQuarters = 5

// AnnualFrameYears is how many completed calendar years of EPS frames the cheap
// stage reads, enough for the three-year average when the fiscal year is
// calendar-aligned.
const AnnualFrameYears = 4

// frameFacts fetches (or reads from cache) one tag across the given frames and
// returns the freshest fact per CIK.
func (s *Screener) frameFacts(ctx context.Context, spec metrics.FrameSpec, frames []edgar.FrameID, refresh bool) ([]model.Fact, error) {
	ttl := s.Cfg.FactsTTL()
	allFresh := !refresh
	keys := make([]string, 0, len(frames))
	for _, f := range frames {
		key := fmt.Sprintf("frame:%s:%s:%s:%s", spec.Taxonomy, spec.Tag, spec.Unit, f.String())
		keys = append(keys, key)
		if allFresh {
			fresh, err := s.Store.Fresh(ctx, key, ttl)
			if err != nil || !fresh {
				allFresh = false
			}
		}
	}
	if allFresh {
		s.Edgar.Stats.CacheHits.Add(1)
		if spec.Instant {
			return s.Store.LatestByTag(ctx, spec.QualifiedTag())
		}
		return s.Store.AnnualByTag(ctx, spec.QualifiedTag())
	}

	var out []model.Fact
	if spec.Instant {
		best, err := s.Edgar.LatestFromFrames(ctx, spec.Taxonomy, spec.Tag, spec.Unit, frames)
		if err != nil {
			return nil, err
		}
		for _, f := range best {
			f.Tag = spec.QualifiedTag()
			out = append(out, f)
		}
	} else {
		// Annual frames: every year is a separate data point, so they are all
		// kept rather than reduced to the freshest one.
		for _, id := range frames {
			fr, err := s.Edgar.Frame(ctx, spec.Taxonomy, spec.Tag, spec.Unit, id)
			if err != nil {
				var nf *edgar.ErrNotFound
				if errors.As(err, &nf) {
					continue
				}
				return nil, err
			}
			for _, d := range fr.Data {
				out = append(out, model.Fact{
					CIK: edgar.PadCIK(d.CIK.String()), Tag: spec.QualifiedTag(), Unit: spec.Unit,
					Start: d.Start, End: d.End, FY: d.FY, FP: d.FP, Form: d.Form,
					Val: d.Val, Accn: d.Accn, Filed: d.Filed,
				})
			}
		}
	}
	if err := s.Store.SaveFacts(ctx, out); err != nil {
		s.log().Warn("cache frame facts", "tag", spec.Tag, "err", err)
	}
	for _, k := range keys {
		if err := s.Store.MarkFetched(ctx, k); err != nil {
			s.log().Warn("mark frame fetched", "key", k, "err", err)
		}
	}
	return out, nil
}

// preVerdict is the outcome of the cheap stage.
type preVerdict int

const (
	preKeep    preVerdict = iota // survives to the deep stage
	preDrop                      // definitely fails a sector-independent test
	preSuspect                   // fails liquidity, which only matters once the
	// SIC is known: utilities are judged on leverage instead
)

type preResult struct {
	verdict   preVerdict
	criterion string
	value     *float64
}

// prescreen applies the tests that are cheap and sector-independent. It drops a
// company only on a definite fail; anything unknown survives, which is what
// keeps missing data from turning into a silent rejection.
func prescreen(m model.Metrics, cfg config.Config, includeFinancials bool) preResult {
	t := cfg.Thresholds
	if m.MarketCap != nil && *m.MarketCap < t.MinMarketCapUSD {
		return preResult{preDrop, criteria.IDSize, m.MarketCap}
	}
	if m.EPSLatest != nil && *m.EPSLatest <= 0 {
		return preResult{preDrop, criteria.IDEarningsStability, m.EPSLatest}
	}
	if m.PE3YAvg != nil && *m.PE3YAvg > t.PEMax {
		return preResult{preDrop, criteria.IDPE, m.PE3YAvg}
	}
	if m.PB != nil && *m.PB > t.PBMax && m.PE3YAvg != nil {
		product := *m.PB * *m.PE3YAvg
		if product > t.GrahamProductMax {
			return preResult{preDrop, criteria.IDPriceToBook, &product}
		}
	}
	if m.BookValuePerShare != nil && *m.BookValuePerShare <= 0 {
		return preResult{preDrop, criteria.IDPriceToBook, m.BookValuePerShare}
	}
	// Liquidity is sector-dependent, and the SIC is not known yet at this
	// stage. A company that fails it is only dropped when it would also fail
	// the utility branch; otherwise it is held back as a suspect.
	if !includeFinancials && m.CurrentRatio != nil && *m.CurrentRatio < t.CurrentRatioMin {
		if m.LTDToEquity != nil && *m.LTDToEquity > t.UtilityDebtToEquityMax {
			return preResult{preDrop, criteria.IDCurrentRatio, m.CurrentRatio}
		}
		return preResult{preSuspect, criteria.IDCurrentRatio, m.CurrentRatio}
	}
	return preResult{preKeep, "", nil}
}

// dropRecord is why one company left the pipeline, kept in memory so the diff
// against the previous run states a computed reason instead of guessing one.
type dropRecord struct {
	criterion string
	status    model.Status
	value     *float64
	reason    string
}

// Screen runs the whole pipeline.
func (s *Screener) Screen(ctx context.Context, p Params) (*Result, error) {
	started := s.now()
	cfg := s.Cfg
	if p.MinMarketCapUSD != nil {
		cfg.Thresholds.MinMarketCapUSD = *p.MinMarketCapUSD
	}
	includeFinancials := cfg.Universe.IncludeFinancials
	if p.IncludeFinancials != nil {
		includeFinancials = *p.IncludeFinancials
	}
	cfg.Universe.IncludeFinancials = includeFinancials
	maxResults := p.MaxResults
	if maxResults <= 0 {
		maxResults = cfg.MaxResults
	}
	if p.Profile != "" && p.Profile != cfg.Profile {
		return nil, fmt.Errorf("unknown profile %q: only %q is implemented", p.Profile, cfg.Profile)
	}

	lc := cfg.Catalog()
	res := &Result{
		RunID:   runID(started),
		AsOf:    started.Format(time.RFC3339),
		Profile: cfg.Profile,
	}
	var funnel []FunnelStep
	drops := map[string]dropRecord{}

	// --- stage 0 ---------------------------------------------------------
	universe, err := s.Universe(ctx, p.Refresh)
	if err != nil {
		return nil, err
	}
	res.UniverseSize = len(universe)
	s.log().Info("stage 0: universe", "companies", len(universe))
	byCIK := make(map[string]model.Company, len(universe))
	for _, c := range universe {
		byCIK[c.CIK] = c
	}

	// --- stage 1: frames -------------------------------------------------
	factsByCIK := make(map[string][]model.Fact, len(universe))
	addFacts := func(fs []model.Fact) {
		for _, f := range fs {
			if _, ok := byCIK[f.CIK]; !ok {
				continue
			}
			factsByCIK[f.CIK] = append(factsByCIK[f.CIK], f)
		}
	}
	instantFrames := edgar.RecentFrames(started, FrameQuarters, true)
	for _, fam := range stage1Instant {
		for _, spec := range metrics.FrameSpecs(fam) {
			fs, err := s.frameFacts(ctx, spec, instantFrames, p.Refresh)
			if err != nil {
				s.log().Warn("frames", "tag", spec.Tag, "err", err)
				continue
			}
			addFacts(fs)
		}
	}
	annualFrames := edgar.AnnualFrames(started, AnnualFrameYears)
	for _, spec := range metrics.FrameSpecs(metrics.EPS) {
		fs, err := s.frameFacts(ctx, spec, annualFrames, p.Refresh)
		if err != nil {
			s.log().Warn("annual frames", "tag", spec.Tag, "err", err)
			continue
		}
		addFacts(fs)
	}

	withFacts := make([]model.Company, 0, len(factsByCIK))
	for _, c := range universe {
		if len(factsByCIK[c.CIK]) > 0 {
			withFacts = append(withFacts, c)
		}
	}
	res.Evaluated = len(withFacts)
	noFacts := len(universe) - len(withFacts)
	funnel = append(funnel, FunnelStep{Stage: "1", Criterion: "data.no_xbrl",
		Label: lc.T(i18n.FunnelNoXBRL), Dropped: noFacts, Remaining: len(withFacts)})
	s.log().Info("stage 1: frames", "with_facts", len(withFacts), "without", noFacts)

	// Quotes for everything that has fundamentals: the cheap stage needs them
	// for size, P/E and P/B, which are the filters that do most of the work.
	tickers := make([]string, 0, len(withFacts))
	for _, c := range withFacts {
		tickers = append(tickers, c.Ticker)
	}
	quotes := s.Prices.Quotes(ctx, tickers, p.Refresh)
	s.log().Info("stage 1: quotes", "have", len(quotes), "want", len(tickers))
	if len(tickers) > 0 && len(quotes)*2 < len(tickers) {
		// Without a price there is no market cap, no P/E and no P/B, so those
		// criteria come back unknown for everyone. Say so instead of returning
		// a plausible-looking empty candidate list.
		note := lc.T(i18n.NoteQuoteCoverage, len(quotes), len(tickers))
		if s.Prices.Degraded() {
			note += lc.T(i18n.NoteQuoteSourceDown)
		}
		res.Notes = append(res.Notes, note)
		s.log().Warn("quote coverage is degraded", "have", len(quotes), "want", len(tickers))
	}

	type survivor struct {
		company model.Company
		pre     preResult
	}
	var survivors []survivor
	dropCounts := map[string]int{}
	for _, c := range withFacts {
		var price *model.Price
		if q, ok := quotes[c.Ticker]; ok {
			price = &q
		}
		m, _ := metrics.Compute(metrics.Input{
			Facts: factsByCIK[c.CIK], Price: price, HistoryYears: cfg.Thresholds.HistoryYears,
		})
		pr := prescreen(m, cfg, includeFinancials)
		if pr.verdict == preDrop {
			dropCounts[pr.criterion]++
			drops[c.Ticker] = dropRecord{criterion: pr.criterion, status: model.Fail, value: pr.value,
				reason: lc.T(i18n.DropFramesStage, criteria.LabelIn(lc, pr.criterion))}
			continue
		}
		survivors = append(survivors, survivor{company: c, pre: pr})
	}
	remaining := len(survivors)
	for _, id := range sortedKeys(dropCounts) {
		funnel = append(funnel, FunnelStep{Stage: "1", Criterion: id, Label: criteria.LabelIn(lc, id),
			Dropped: dropCounts[id], Remaining: remaining})
	}
	s.log().Info("stage 1: prescreen", "survivors", len(survivors))

	// --- stage 2: SIC, then full history ---------------------------------
	sectorDropped, suspectDropped := 0, 0
	type deep struct {
		company model.Company
		pre     preResult
	}
	var deepList []deep
	survivorCompanies := make([]model.Company, 0, len(survivors))
	for _, sv := range survivors {
		survivorCompanies = append(survivorCompanies, sv.company)
	}
	sics := s.fetchSICs(ctx, survivorCompanies, p.Refresh)
	for _, sv := range survivors {
		c := sv.company
		if sic, ok := sics[c.CIK]; ok {
			c.SIC, c.SICDesc = sic.SIC, sic.SICDesc
		}
		if c.SIC != SICUnknown && c.IsFinancial() && !includeFinancials {
			sectorDropped++
			drops[c.Ticker] = dropRecord{criterion: "universe.financial", status: model.Fail,
				reason: lc.T(i18n.DropFinancial, c.SIC, c.SICDesc)}
			continue
		}
		if sv.pre.verdict == preSuspect && !(c.IsUtility() || (c.IsFinancial() && includeFinancials)) {
			suspectDropped++
			drops[c.Ticker] = dropRecord{criterion: sv.pre.criterion, status: model.Fail, value: sv.pre.value,
				reason: lc.T(i18n.DropNotUtility, criteria.LabelIn(lc, sv.pre.criterion), c.SIC)}
			continue
		}
		deepList = append(deepList, deep{company: c, pre: sv.pre})
	}
	if sectorDropped > 0 {
		funnel = append(funnel, FunnelStep{Stage: "2", Criterion: "universe.financial",
			Label: lc.T(i18n.FunnelSectorFinance), Dropped: sectorDropped, Remaining: len(deepList)})
	}
	if suspectDropped > 0 {
		funnel = append(funnel, FunnelStep{Stage: "2", Criterion: criteria.IDCurrentRatio,
			Label: criteria.LabelIn(lc, criteria.IDCurrentRatio), Dropped: suspectDropped, Remaining: len(deepList)})
	}
	s.log().Info("stage 2: deep", "companies", len(deepList))

	deepCompanies := make([]model.Company, 0, len(deepList))
	for _, d := range deepList {
		deepCompanies = append(deepCompanies, d.company)
	}
	evals := s.evaluateDeep(ctx, deepCompanies, quotes, cfg, p.Refresh)

	// --- stage 3: verdicts, diff, persistence ----------------------------
	var candidates []Candidate
	var incomplete []Incomplete
	deepDrops := map[string]int{}
	factsMissing := 0
	for _, ev := range evals {
		switch ev.Verdict {
		case model.VerdictPass:
			candidates = append(candidates, toCandidate(ev))
		case model.VerdictIncomplete:
			incomplete = append(incomplete, toIncomplete(lc, ev))
			if len(ev.Metrics.Tags) == 0 {
				factsMissing++
			}
		default:
			if f := ev.First(model.Fail); f != nil {
				deepDrops[f.ID]++
				drops[ev.Company.Ticker] = dropRecord{criterion: f.ID, status: model.Fail,
					value: f.Value, reason: failReason(lc, *f)}
			}
		}
	}
	remaining = len(candidates) + len(incomplete)
	for _, id := range sortedKeys(deepDrops) {
		funnel = append(funnel, FunnelStep{Stage: "2", Criterion: id, Label: criteria.LabelIn(lc, id),
			Dropped: deepDrops[id], Remaining: remaining})
	}

	// Cheapest relative to the Graham number first: the larger the margin of
	// safety, the further the price sits below what the numbers support.
	sort.Slice(candidates, func(i, j int) bool {
		a, b := candidates[i], candidates[j]
		av, bv := valueOr(a.MarginOfSafety, -1e18), valueOr(b.MarginOfSafety, -1e18)
		if av != bv {
			return av > bv
		}
		return a.Ticker < b.Ticker
	})
	res.Passed = len(candidates)
	if len(candidates) > maxResults {
		candidates = candidates[:maxResults]
	}
	if candidates == nil {
		candidates = []Candidate{}
	}
	res.Candidates = candidates

	// Incomplete: fewest blocking criteria first, so the agent sees the names
	// that are one fact away from a verdict.
	sort.Slice(incomplete, func(i, j int) bool {
		if len(incomplete[i].Blocking) != len(incomplete[j].Blocking) {
			return len(incomplete[i].Blocking) < len(incomplete[j].Blocking)
		}
		return incomplete[i].Ticker < incomplete[j].Ticker
	})
	res.IncompleteTotal = len(incomplete)
	if len(incomplete) > maxResults {
		incomplete = incomplete[:maxResults]
		res.Notes = append(res.Notes, lc.T(i18n.NoteIncompleteTruncated, maxResults, res.IncompleteTotal))
	}
	if incomplete == nil {
		incomplete = []Incomplete{}
	}
	res.Incomplete = incomplete

	finished := s.now()
	res.DataQuality = DataQuality{
		SECRequests:   s.Edgar.Stats.Requests.Load(),
		SEC429:        s.Edgar.Stats.TooMany.Load(),
		SECRetries:    s.Edgar.Stats.Retries.Load(),
		CacheHits:     s.Edgar.Stats.CacheHits.Load(),
		PriceRequests: s.Prices.Stats.Requests.Load(),
		PriceMissing:  int64(len(tickers) - len(quotes)),
		FactsMissing:  int64(factsMissing + noFacts),
		DurationMs:    finished.Sub(started).Milliseconds(),
	}

	// diff against the previous run, computed from this run's numbers
	prev, err := s.Store.LastRunBefore(ctx, started)
	if err != nil {
		s.log().Warn("previous run", "err", err)
	}
	if prev != nil {
		res.Diff = s.diff(ctx, lc, prev, evals, drops, started)
	} else {
		res.Diff = Diff{New: []NewEntry{}, Dropped: []DroppedEntry{}}
	}

	if p.NoStore {
		res.Notes = append(res.Notes, lc.T(i18n.NoteNotStored))
		return res, nil
	}

	run := model.Run{
		ID: res.RunID, StartedAt: started, FinishedAt: finished, Profile: cfg.Profile,
		UniverseSize: res.UniverseSize, PassedCount: res.Passed,
	}
	if b, err := json.Marshal(p); err == nil {
		run.ParamsJSON = string(b)
	}
	if b, err := json.Marshal(funnel); err == nil {
		run.FunnelJSON = string(b)
	}
	if err := s.Store.SaveRun(ctx, run, s.runResults(ctx, evals, prev, drops)); err != nil {
		s.log().Warn("save run", "err", err)
		res.Notes = append(res.Notes, lc.T(i18n.NoteRunNotPersisted, err.Error()))
	}
	return res, nil
}

// fetchSICs fills in the SIC of each company from the submissions endpoint,
// using the cached value when it is fresh enough.
func (s *Screener) fetchSICs(ctx context.Context, cs []model.Company, refresh bool) map[string]model.Company {
	ttl := s.Cfg.CompaniesTTL()
	out := make(map[string]model.Company, len(cs))
	var mu sync.Mutex
	var updated []model.Company

	work := make(chan model.Company)
	var wg sync.WaitGroup
	for i := 0; i < s.workers(); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for c := range work {
				if !refresh {
					if cached, err := s.Store.Company(ctx, c.CIK, ttl); err == nil && cached != nil && cached.SIC != SICUnknown {
						mu.Lock()
						out[c.CIK] = *cached
						mu.Unlock()
						s.Edgar.Stats.CacheHits.Add(1)
						continue
					}
				}
				sub, err := s.Edgar.Submissions(ctx, c.CIK)
				if err != nil {
					s.log().Warn("submissions", "cik", c.CIK, "err", err)
					continue
				}
				c.SIC = sub.SICInt()
				c.SICDesc = sub.SICDescription
				if sub.Name != "" {
					c.Title = sub.Name
				}
				mu.Lock()
				out[c.CIK] = c
				updated = append(updated, c)
				mu.Unlock()
			}
		}()
	}
	for _, c := range cs {
		if ctx.Err() != nil {
			break
		}
		work <- c
	}
	close(work)
	wg.Wait()

	if len(updated) > 0 {
		if err := s.Store.SaveCompanies(ctx, updated); err != nil {
			s.log().Warn("cache companies", "err", err)
		}
	}
	return out
}

// companyFacts returns the full XBRL history of one company, from the cache
// when it is fresh.
func (s *Screener) companyFacts(ctx context.Context, cik string, refresh bool) ([]model.Fact, error) {
	ttl := s.Cfg.FactsTTL()
	key := "facts:" + cik
	if !refresh {
		if fresh, err := s.Store.Fresh(ctx, key, ttl); err == nil && fresh {
			s.Edgar.Stats.CacheHits.Add(1)
			return s.Store.Facts(ctx, cik)
		}
	}
	cf, err := s.Edgar.CompanyFacts(ctx, cik)
	if err != nil {
		return nil, err
	}
	facts := s.usefulFacts(cf.Flatten(cik))
	if err := s.Store.SaveFacts(ctx, facts); err != nil {
		s.log().Warn("cache facts", "cik", cik, "err", err)
	}
	if err := s.Store.MarkFetched(ctx, key); err != nil {
		s.log().Warn("mark facts fetched", "cik", cik, "err", err)
	}
	return facts, nil
}

// usefulFacts keeps only what the criteria can read: the tags of the metric
// families, within the history window plus a margin. Everything else would be
// cached forever and never looked at — an unfiltered companyfacts cache of a
// few thousand companies reaches several gigabytes.
func (s *Screener) usefulFacts(facts []model.Fact) []model.Fact {
	known := metrics.KnownTags()
	years := s.Cfg.Thresholds.HistoryYears + 3
	cutoff := s.now().AddDate(-years, 0, 0).Format("2006-01-02")
	out := facts[:0:0]
	for _, f := range facts {
		if !known[f.Tag] || f.End < cutoff {
			continue
		}
		out = append(out, f)
	}
	return out
}

// evaluateDeep runs stage 2 for every survivor: full history, metrics, criteria.
// A company whose facts cannot be fetched is not dropped — it comes back with
// unknown criteria and a reason.
func (s *Screener) evaluateDeep(ctx context.Context, cs []model.Company, quotes map[string]model.Price,
	cfg config.Config, refresh bool) []model.Evaluation {

	out := make([]model.Evaluation, 0, len(cs))
	var mu sync.Mutex
	work := make(chan model.Company)
	var wg sync.WaitGroup
	for i := 0; i < s.workers(); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for c := range work {
				ev := s.evaluateOne(ctx, c, quotes[c.Ticker], cfg, refresh)
				mu.Lock()
				out = append(out, ev)
				mu.Unlock()
			}
		}()
	}
	for _, c := range cs {
		if ctx.Err() != nil {
			break
		}
		work <- c
	}
	close(work)
	wg.Wait()
	sort.Slice(out, func(i, j int) bool { return out[i].Company.Ticker < out[j].Company.Ticker })
	return out
}

func (s *Screener) evaluateOne(ctx context.Context, c model.Company, q model.Price,
	cfg config.Config, refresh bool) model.Evaluation {

	var price *model.Price
	if q.Price > 0 {
		price = &q
	}
	facts, err := s.companyFacts(ctx, c.CIK, refresh)
	if err != nil {
		s.log().Warn("companyfacts", "cik", c.CIK, "ticker", c.Ticker, "err", err)
	}
	m, series := metrics.Compute(metrics.Input{
		Facts: facts, Price: price, HistoryYears: cfg.Thresholds.HistoryYears,
	})
	rs := criteria.Evaluate(c, m, series, cfg)
	if err != nil {
		// Mark every undecided criterion with the transport failure, so the
		// agent can tell "EDGAR was unreachable" from "EDGAR has no such tag".
		lc := cfg.Catalog()
		for i := range rs {
			if rs[i].Status == model.Unknown {
				rs[i].Reason = lc.T(i18n.ReasonFactsFetchFailed, rs[i].Reason, err)
			}
		}
	}
	return model.Evaluation{Company: c, Metrics: m, Criteria: rs, Verdict: criteria.Verdict(rs)}
}

func toCandidate(ev model.Evaluation) Candidate {
	m := ev.Metrics
	return Candidate{
		Ticker: ev.Company.Ticker, CIK: ev.Company.CIK, Name: ev.Company.Title,
		SIC: ev.Company.SIC, Sector: ev.Company.SICDesc,
		Price: m.Price, PriceAsOf: m.PriceAsOf, PriceSource: m.PriceSource,
		MarketCap: m.MarketCap, PE3YAvg: m.PE3YAvg, PB: m.PB,
		GrahamNumber: m.GrahamNumber, MarginOfSafety: m.MarginOfSafety,
		CurrentRatio: m.CurrentRatio, LTDToWorkingCap: m.LTDToWorkingCap, LTDToEquity: m.LTDToEquity,
		EPSPositiveYears: m.EPSPositiveYears, DividendYears: m.DividendYears,
		EPSGrowth10YPct: m.EPSGrowth10YPct,
		Criteria:        ev.Criteria,
	}
}

func toIncomplete(lc *i18n.Catalog, ev model.Evaluation) Incomplete {
	var reasons []string
	for _, c := range ev.Criteria {
		if c.Status == model.Unknown && c.Reason != "" {
			reasons = append(reasons, criteria.LabelIn(lc, c.ID)+": "+c.Reason)
		}
	}
	return Incomplete{
		Ticker: ev.Company.Ticker, Name: ev.Company.Title, Sector: ev.Company.SICDesc,
		Blocking: ev.Blocking(), Reason: strings.Join(reasons, "; "),
	}
}

func failReason(lc *i18n.Catalog, c model.CriterionResult) string {
	if c.Reason != "" {
		return c.Reason
	}
	if c.Value != nil {
		return lc.T(i18n.DropValueTarget, criteria.LabelIn(lc, c.ID), *c.Value, c.Target)
	}
	return lc.T(i18n.DropFailedTarget, criteria.LabelIn(lc, c.ID), c.Target)
}

// runResults picks what to persist: every candidate and every incomplete name,
// plus any ticker that passed in the previous run, so the next diff can always
// state what changed.
func (s *Screener) runResults(ctx context.Context, evals []model.Evaluation,
	prev *model.Run, drops map[string]dropRecord) []store.RunResult {

	prevPassed := map[string]bool{}
	if prev != nil {
		if rows, err := s.Store.RunResults(ctx, prev.ID); err == nil {
			for t, r := range rows {
				if r.Verdict == model.VerdictPass {
					prevPassed[t] = true
				}
			}
		}
	}
	var out []store.RunResult
	for _, ev := range evals {
		switch {
		case ev.Verdict == model.VerdictPass, ev.Verdict == model.VerdictIncomplete, prevPassed[ev.Company.Ticker]:
			score := 0.0
			if ev.Metrics.MarginOfSafety != nil {
				score = *ev.Metrics.MarginOfSafety
			}
			out = append(out, store.RunResult{
				Ticker: ev.Company.Ticker, Verdict: ev.Verdict, Score: score,
				Metrics: ev.Metrics, Criteria: ev.Criteria,
			})
		}
	}
	// Previous candidates that never reached the deep stage: keep the computed
	// drop reason so it survives into later comparisons.
	seen := map[string]bool{}
	for _, r := range out {
		seen[r.Ticker] = true
	}
	for ticker := range prevPassed {
		if seen[ticker] {
			continue
		}
		d, ok := drops[ticker]
		if !ok {
			continue
		}
		out = append(out, store.RunResult{
			Ticker: ticker, Verdict: model.VerdictFail,
			Criteria: []model.CriterionResult{{
				ID: d.criterion, Status: d.status, Value: d.value, Reason: d.reason,
			}},
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Ticker < out[j].Ticker })
	return out
}

func runID(t time.Time) string {
	return fmt.Sprintf("%s-%06x", t.UTC().Format("2006-01-02T15:04:05Z"), t.UnixNano()%0xffffff)
}

func sortedKeys(m map[string]int) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func valueOr(p *float64, def float64) float64 {
	if p == nil {
		return def
	}
	return *p
}
