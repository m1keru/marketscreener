# Running marketscreener under OpenClaw

Verified against OpenClaw 2026.9.4. The flags below differ from the design
document in three places, noted inline; the design was written against an
older CLI.

## How the agent reaches the binary

OpenClaw spawns `msc mcp` as a child process and speaks JSON-RPC over its
stdin/stdout — there is no port, no URL and no auth. The binary advertises five
tools (`screen`, `explain`, `history`, `runs`, `funnel`) and the agent calls
them like any other tool. Every log line goes to stderr, where OpenClaw collects
it; `os.Stdout` is redirected into stderr for the lifetime of the session so a
stray write cannot corrupt the protocol stream.

```
openclaw agent ──spawn──▶ msc mcp ──▶ data.sec.gov (10 req/s, cached in SQLite)
        ▲   JSON-RPC over stdio    └──▶ quote sources
        └── tools: screen, explain, history, runs, funnel
```

## 1. Build on the node

```shell
git clone <repo> /srv/marketscreener
cd /srv/marketscreener
go build -o /srv/marketscreener/msc ./cmd/msc
```

Set the SEC contact address in `/srv/marketscreener/config/default.yaml`
(`user_agent_contact:`) or pass it as an environment variable, below. SEC
answers 403 to anonymous traffic and `msc` refuses to start without it.

Check the whole chain — EDGAR, the quote sources and the criteria — with one
call before wiring anything up:

```shell
MSC_CONTACT=you@example.com ./msc explain KO
```

## 2. Register the MCP server

```shell
openclaw mcp add marketscreener \
  --command /srv/marketscreener/msc \
  --arg mcp \
  --cwd /srv/marketscreener \
  --env MSC_CONTACT=you@example.com \
  --connect-timeout 15 \
  --timeout 900
```

`--timeout` is **in seconds** in this CLI, not the `requestTimeoutMs`
milliseconds field the design document quotes. 900 s is not padding: a measured
cold run took 577 s and 5139 SEC requests. The default of 30 s would abort every
first run of the day.

`openclaw mcp add` probes the server before saving, so a successful run of the
command above is already proof that the handshake works. Afterwards:

```shell
openclaw mcp probe marketscreener    # list the tools it advertises
openclaw mcp doctor                  # static configuration check
openclaw mcp reload                  # after rebuilding the binary
```

## 3. Install the skill

```shell
openclaw skills install /srv/marketscreener/docs/skills/marketscreener --global
openclaw skills check
```

The skill tells the agent when to run, in what order to work, and — most
importantly — that the reason a ticker dropped out is already computed in
`diff.dropped[]` and must not be embellished.

## 4. Schedule the daily run

Two jobs. The second one is optional but recommended.

**The agent session**, after the US close:

```shell
openclaw cron add \
  --name marketscreener-daily \
  --cron "5 22 * * 1-5" --tz UTC \
  --agent main \
  --message "Run the marketscreener skill: screen, check each candidate against recent news, explain anything that looks odd, then report to Telegram." \
  --model opus --thinking xhigh \
  --session isolated \
  --announce --channel telegram --to <chatId>
```

`--thinking xhigh` is accepted per job in this version, so the design's
workaround (setting it in `agents.defaults` because the agent-config whitelist
rejected it) is unnecessary. `--model opus` uses the node's own alias — check
`openclaw models` for what is actually allowed; on the machine this was written
on the allowed list is `anthropic/claude-opus-4-8` and `anthropic/claude-sonnet-5`,
and the `anthropic/claude-opus-5` identifier from the design document does not
exist there.

**Cache warming**, half an hour earlier, so the agent's `screen` call returns in
under a minute instead of holding the session for ten:

```shell
openclaw cron add \
  --name marketscreener-warm \
  --cron "35 21 * * 1-5" --tz UTC \
  --command "/srv/marketscreener/msc screen --no-store --format json > /dev/null" \
  --command-cwd /srv/marketscreener \
  --command-env MSC_CONTACT=you@example.com \
  --no-output-timeout-seconds 900
```

`--no-store` matters. Without it the warm-up would be recorded as a run, become
the baseline of the agent's diff half an hour later, and `diff.new` and
`diff.dropped` would be empty every single day. With it, the caches are filled
and yesterday's run stays the baseline.

Check both:

```shell
openclaw cron list
openclaw cron run marketscreener-daily     # run it now
openclaw cron runs                         # history
```

## Cache lifetimes and what they cost

| Data | TTL | Cold cost |
|---|---|---|
| Registrant identity and SIC | 30 days | one request per company |
| XBRL facts | 24 hours | ~40 frame requests + one per surviving company |
| Quotes | 1 hour | one request per 20 tickers |

A daily schedule therefore pays the 24-hour fact expiry every day — a measured
cold run is 577 s and 5139 requests, which is exactly what the warm-up job
absorbs — and the 30-day registrant expiry once a month. A fully warm run takes
under a minute.

The database holds only the tags the criteria read, within the history window:
about 350 MB after a full run. Caching whole companyfacts documents instead
reached 4.9 GB in a single run.

## Operational notes

- The database defaults to `$XDG_DATA_HOME/marketscreener/msc.db`; pass `--db`
  to move it. It must be on persistent storage: `diff` between runs comes from
  there, and losing it means losing the history the agent reports on.
- Both cron jobs and the MCP server must see the same database. The `--cwd` and
  `--db` above keep them pointed at one file.
- `msc` never writes report files. Output goes to stdout (CLI) or through the
  agent (MCP).
- After rebuilding the binary, `openclaw mcp reload` so the next turn spawns the
  new one.
