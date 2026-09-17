# Deviations from the design document

The design is
[`superpowers/specs/2026-09-16-marketscreener-go-mcp-design.md`](superpowers/specs/2026-09-16-marketscreener-go-mcp-design.md).
Everything below is a place where the implementation departs from it, and why.
The criteria, the ternary logic and the MCP contract are unchanged.

## 1. Quote sources: Stooq is out, Yahoo and Cboe are in

The design named Stooq as the primary source (50 tickers per request) and Yahoo
as the fallback. Appendix claim 2 asked for this to be confirmed. It does not
hold: as of 2026-09 `https://stooq.com/q/l/?s=…&e=csv` answers **HTTP 404 for
every symbol**, and the daily CSV endpoint returns a JavaScript browser
challenge. The live integration test `TestLiveStooqBatch` fails on purpose and
documents it.

What replaced it:

- **Yahoo spark** (`/v7/finance/spark?symbols=…`) batches **20** tickers per
  request, needs no key and no crumb, and omits symbols it cannot resolve. In a
  live run it answered for 6131 of 6185 companies in under a minute.
- **Yahoo chart** (`/v8/finance/chart/{ticker}`) picks up what spark missed.
- **Cboe delayed quotes** (`cdn.cboe.com/api/global/delayed_quotes/quotes/…`) is
  an exchange-operated, keyless backstop, one request per ticker.

Sources live in `prices.sources` and are tried in order, so a node where Stooq
works can simply put it back at the front.

## 2. SIC is fetched in stage 2, not stage 0

The design filters SIC 6000–6799 out of the universe in stage 0. Nothing in
EDGAR hands out SIC codes in bulk: `company_tickers.json` has none, and
`submissions/CIK…json` is one request per company — 8000 requests before the
first metric. The sector filter therefore runs at the start of stage 2, on the
few thousand companies that survived the cheap stage, and the design's "~600 SEC
requests" is optimistic: a measured cold run is 5139 requests and 577 seconds. Registrant identity is cached for 30 days
(`cache.companies_ttl_hours`), because a SIC code changes at most once in a
company's life; the second run of the week pays only for facts and quotes.
A fully warm run takes under a minute.

The consequence for correctness is nil: a financial that reaches stage 2 is
excluded there, and its liquidity criteria are `unknown` either way.

## 3. Liquidity cannot drop a company before its SIC is known

Criterion 2 is replaced by a leverage test for utilities, which means a low
current ratio must not remove a company until we know whether it is one. The
cheap stage therefore treats a failing current ratio as *suspect* rather than
*failed*, unless the company would also fail the utility test. Suspects are
resolved in stage 2 once the SIC is in hand.

## 4. Candidates are ranked by margin of safety

The design says "sorted by margin of safety (Graham number ascending)". Those
are two different orders, and ascending Graham number ranks the companies with
the smallest earnings and book value first, which is not a measure of safety.
Candidates are sorted by `margin_of_safety` (`graham_number / price - 1`)
descending: the further the price sits below the Graham number, the higher the
company ranks. The field is in the response, so any other order can be applied
by the caller.

## 5. One ticker per registrant: the shortest, not the lexicographically first

The design keeps the lexicographically first ticker of each CIK. On live data
that picked `MNESP` (a preferred line) over `MSA` for MSA Safety, and paired the
preferred quote with the company's fundamentals — the result was a bogus P/E of
7.1 and a false candidate. The rule is now "shortest ticker, ties broken
lexicographically", which is still fully deterministic and picks MSA over
MNESP, GOOG over GOOGL, BRK-A over BRK-B.

## 6. Fallback tags fill individual years, not whole series

The design picks the first tag in a family that has data. Applied to a whole
series, that punches holes in it: UFP Industries stopped tagging
`EarningsPerShareBasic` in FY2024 and reported only the diluted figure, so the
company was reported as missing a year and landed in `incomplete[]`. Tags are
now walked in priority order per year, and each year in the series records the
tag that supplied it.

## 7. Share counts are summed across classes and must be recent

Two corrections that the design does not mention, both found on live data:

- A registrant with several share classes reports one cover-page fact per class
  at the same instant. These are summed, and `share_classes` in the response
  says how many were added; the size criterion notes that market cap assumes
  every class trades near the quoted price.
- A share count older than 18 months relative to the balance sheet is rejected
  and the next tag is tried. Berkshire last tagged
  `EntityCommonStockSharesOutstanding` in 2011; multiplying that count by today's
  price produced a market cap of $489M for a company worth a thousand times more.
  It is now `unknown`, with the reason stated.

## 8. SQLite schema additions

The design's schema is kept, plus what the caches need: a `fetches` table
(which bulk fetch happened when, so an empty result is not refetched every run),
`runs.funnel_json` (the funnel has to survive to be queried later), and
`period_start`/`filed` on `facts` (annual periods cannot be told from quarterly
ones without the start date, and restatements cannot be ordered without the
filing date).

## 9. Only the tags the criteria read are cached

A companyfacts document runs to tens of thousands of facts per registrant —
Microsoft alone returns 32671. Caching them whole produced a 4.9 GB database in
one run. Facts are filtered to the tags of the metric families and to the
history window plus three years before they are stored, which brings a full run
to about 350 MB with no change to any verdict.

## 10. `incomplete[]` is truncated in the response

A live run produces several hundred companies blocked by missing data. The full
count is in `incomplete_total`, the list itself is capped at `max_results`
(fewest blocking criteria first) with a note saying so, because the design also
requires the `screen` response to fit in a few thousand tokens. `explain` gives
the full trace for any single ticker.
