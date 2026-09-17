---
name: marketscreener
description: Screen US equities against Graham's defensive criteria via the marketscreener MCP server, check the survivors against the news, and report to Telegram. Use daily after the US close and whenever asked about screening candidates.
---

# marketscreener

The `marketscreener` MCP server does the arithmetic; you do the judgement. It
returns pass/fail/unknown per criterion with the XBRL tag and accession number
behind every number. It never guesses, and neither do you.

## When to run

- Once a day, after the US market close (22:00 UTC, 21:00 UTC during daylight saving).
- Whenever asked directly about candidates, a specific ticker, or a threshold.

## Order of work

1. **`screen`**. A cold run takes 2–3 minutes. Do not re-run it to "check" a
   number: everything is already in the response.
2. **For each name in `candidates[]`**, search the web for what has happened
   recently: the last earnings report, guidance, litigation, a sector-wide
   repricing. The screen is built on filings that can be up to a year old, so
   ask what the filings cannot tell you — *why is it this cheap?*
3. **`explain TICKER`** when a number looks odd or a thesis needs the series
   behind it. `history TICKER metric` for one raw series.
4. **Drop the value traps.** A stock can pass all seven criteria and still be a
   melting ice cube. Say so explicitly when you think it is.
5. **Work through `incomplete[]`.** These companies failed nothing — EDGAR was
   missing a fact. The `reason` field names exactly which fact and which years.
   Some of them can be finished with a web search; do that for names that look
   interesting and say what you found and where.
6. **Explain `diff.dropped[]` using the `reason`, `criterion`, `was` and `now`
   fields that are already in the response.** This reason is computed. Do not
   invent, embellish or supplement it — no stop levels, no target prices, no
   "profit taking". If the response says the P/E went from 14.2 to 18.7, that is
   the whole story you are allowed to tell.
7. **Report to Telegram.**

## Hard rules

- Never state a number the tools did not return. Every figure in your message
  must be traceable to `screen`, `explain` or `history`.
- Never explain a drop-out with anything other than the computed reason.
- Never treat `unknown` as `fail`. Unknown means missing data, and the company
  is still a candidate for your attention.
- No technical analysis, no price targets, no stop levels. The horizon of this
  screen is years; a daily chart says nothing about it.
- The universe is US-listed only (EDGAR covers nothing else). If asked about a
  foreign listing, say so instead of improvising.

## Language

Write the Telegram message in Russian. The server already returns criterion
names, thresholds and reasons in the language set by `language` in its config,
so quote them as they come back rather than translating them yourself — a
re-translated reason is no longer the computed one.

Criterion ids (`graham.6.pe`), XBRL tags (`EarningsPerShareBasic`) and accession
numbers are identifiers: keep them verbatim.

## Telegram message format

At most ~15 lines.

```
Отбор <дата> — <N> кандидатов (просеяно <вселенная>)

<ТИКЕР> <название> — P/E <x> (среднее за 3 года), P/B <x>, дивиденды <N> лет
  <одна-две строки: чем занимается, почему дёшево>
  Сомнение: <то единственное, что настораживает>

Выбыли с <дата прошлого прогона>:
  <ТИКЕР> — <причина дословно из diff.dropped[].reason>

Не хватило данных: <ТИКЕР> (<какого факта>), ...
```

If nothing passes, say so in one line and name the criterion that removed the
most companies (from `funnel`). An empty result is a valid answer, not a failure.

## Tuning thresholds

`funnel` shows how many companies each criterion removed. Read it before
suggesting any threshold change, and say which step you are reacting to.
