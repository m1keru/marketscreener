# marketscreener

A single Go binary, `msc`, that screens US equities against Benjamin Graham's
defensive criteria using SEC EDGAR filings as the only source of fundamentals.
It runs as an MCP server for an agent, or as a plain CLI.

The binary decides what is *numerically true*. It makes no LLM calls, holds no
API keys, writes no report files, and does no technical analysis. Judgement —
why a company is cheap, whether the cheapness is a trap, what to actually do —
belongs to the agent that calls it.

## What it checks

Profile `graham_defensive`, thresholds in [`config/default.yaml`](config/default.yaml):

| # | Criterion | Test | Default |
|---|---|---|---|
| 1 | Size | `price × shares outstanding` | ≥ $300M |
| 2 | Liquidity | `AssetsCurrent / LiabilitiesCurrent` | ≥ 2.0 |
| 2b | Leverage | long-term debt ≤ working capital | — |
| 3 | Earnings stability | EPS > 0 every year | 10 of 10 |
| 4 | Dividend record | a dividend every year | 10 of 10 |
| 5 | Earnings growth | avg EPS of last 3 years vs the first 3 of the window | ≥ +33% |
| 6 | Price to earnings | `price / avg EPS of 3 years` | ≤ 15 |
| 7 | Price to book | `P/B ≤ 1.5` or `P/E × P/B ≤ 22.5` | Graham number |

Sector branches: SIC 6000–6799 (banks, insurers, REITs) have no current/non-current
split, so criteria 2 and 2b return **unknown** for them and they are excluded from the
universe unless `include_financials` is set. SIC 4900–4949 (utilities) are judged on
long-term debt / equity ≤ 2.0 instead.

Every criterion returns **pass**, **fail** or **unknown**, with the value, the
threshold, the XBRL tag it used and the accession numbers behind it. A company
with missing data lands in `incomplete[]` with the list of missing facts — it is
never dropped silently. The reason a ticker left the previous run is computed
from this run's numbers and returned in `diff.dropped[]`.

## Build

Go ≥ 1.24, no cgo:

```shell
go build -o msc ./cmd/msc
```

## Language

Criterion names, thresholds, reasons and the markdown report render in the
language set by `language` in the config (`en` or `ru`), overridable per run
with `--lang`. JSON field names, criterion ids, statuses and XBRL tags are
identifiers and are never translated, so the contract and the golden test hold
in either language.

```shell
msc screen --lang ru
```

## Configure

SEC rejects anonymous traffic with 403, so a real contact address is required
before the first request:

```shell
export MSC_CONTACT=you@example.com        # or set user_agent_contact in the config
```

Config lookup order: `--config`, `$XDG_CONFIG_HOME/marketscreener/config.yaml`,
`./config/default.yaml`. The database defaults to
`$XDG_DATA_HOME/marketscreener/msc.db` and holds the EDGAR fact cache (24h),
the quote cache (1h) and the history of runs.

## Use it from the command line

```shell
msc screen                          # full run, markdown to stdout
msc screen --format json            # the same result as JSON
msc screen --min-market-cap 1e9 --max-results 10
msc screen --no-store               # warm the caches without recording a run
msc explain KO                      # every criterion, value and source for one ticker
msc history KO --metric dividends   # one raw annual series
msc runs --limit 10                 # past runs
msc runs --compare run-a,run-b      # diff two runs
msc funnel                          # what each criterion removed in the last run
```

A cold run takes about ten minutes and ~5100 SEC requests at the mandated
10 requests/second, and leaves a ~350 MB cache. Registrant identity is kept for
30 days and facts for 24 hours, so later runs are far cheaper — a fully warm run
finishes in under a minute.

### Quotes

EDGAR carries no prices, and market cap, P/E and P/B all need one. Sources are
configured in `prices.sources` and tried in order:

| Source | Shape | Notes |
|---|---|---|
| `yahoo` | 20 tickers per request (spark), then one per ticker (chart) | no key, no crumb; default |
| `cboe` | one request per ticker | the exchange's own delayed feed; keyless backstop |
| `stooq` | 50 tickers per request | **off by default**: as of 2026-09 its CSV endpoint answers 404 for every symbol and the daily CSV sits behind a JavaScript challenge |

A ticker no source answers for is reported as `unknown` on the price-based
criteria and appears in `incomplete[]` — never dropped. If coverage collapses,
`notes[]` in the response says so instead of presenting an empty candidate list
as a market fact.

## Use it from an agent (MCP)

```shell
msc mcp        # speaks MCP over stdio
```

Tools: `screen`, `explain`, `history`, `runs`, `funnel` — the same five
operations as the CLI, calling the same functions. In this mode stdout carries
the protocol and every log line goes to stderr; `os.Stdout` is redirected into
stderr for the lifetime of the session so a stray write cannot corrupt the
stream.

Registration with OpenClaw, the agent skill and the schedule are described in
[docs/openclaw.md](docs/openclaw.md).

## Tests

```shell
go test ./...                                                   # offline, no network
MSC_CONTACT=you@example.com go test -tags integration ./...      # live SEC and Stooq
UPDATE_GOLDEN=1 go test ./internal/screen -run TestScreenContract # re-pin the contract
```

Unit tests never touch the network. `testdata/*.json` are trimmed
companyfacts fixtures generated by `testdata/gen.py`;
`testdata/screen_golden.json` pins the shape of the `screen` response, which the
agent depends on.

## Layout

```
cmd/msc/          subcommands and flags
internal/model/   shared types: Company, Fact, Metrics, CriterionResult, Run
internal/edgar/   SEC client: rate limit, gzip, retries, frames, companyfacts
internal/prices/  quote sources: Yahoo batches, Cboe fallback
internal/metrics/ XBRL facts -> annual series and ratios
internal/criteria/the Graham tests, ternary logic, sector branches
internal/screen/  the three-stage pipeline, diff, funnel
internal/store/   SQLite: fact cache, quote cache, run history
internal/mcpsrv/  MCP server and tool definitions
internal/render/  markdown and JSON output
```

The design document is [docs/superpowers/specs/2026-09-16-marketscreener-go-mcp-design.md](docs/superpowers/specs/2026-09-16-marketscreener-go-mcp-design.md);
[docs/deviations.md](docs/deviations.md) records where the implementation
departs from it and why.
