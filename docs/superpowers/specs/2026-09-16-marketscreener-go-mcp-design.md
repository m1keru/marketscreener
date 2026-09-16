# marketscreener v2: Go + MCP + OpenClaw — дизайн

**Дата:** 2026-09-16
**Статус:** утверждён к реализации
**Предшественник:** Python-версия (`main.py`, `screener.py`, `technicals.py`, `reporter.py`) — выводится из эксплуатации целиком.

---

## 1. Зачем переписываем

Текущая Python-версия не решает заявленную задачу («искать надёжные инвестиции»). Найденные дефекты, каждый из которых новый дизайн обязан закрыть:

| # | Дефект текущей версии | Где | Как чиним |
|---|---|---|---|
| D1 | Фильтр по номинальной цене акции $10–100 — бессмысленен, номинал не говорит о дороговизне | `config.py:20-21` | Убран. Вместо него — порог капитализации |
| D2 | `DEBT_TO_ASSETS_MAX = 1.0` не отсекает ничего (>1 = отрицательный капитал) | `config.py:24` | Заменён на критерий Грэма: долгосрочный долг ≤ оборотного капитала |
| D3 | `current_ratio ≥ 1.5` применяется к страховщикам и REIT, где показатель не определён. VICI прошёл с current ratio 37.05 | `screener.py:120` | Исключение SIC 6000–6799; отдельная ветка для утилит |
| D4 | Отсутствие метрики → компания молча выбрасывается (`None` → `False`) | `screener.py:115-125` | Троичная логика `pass/fail/unknown`, список `incomplete[]` с причинами |
| D5 | Trailing P/E — классическая value trap на пике цикла | `screener.py:180` | P/E считается по среднему EPS за 3 года |
| D6 | Нет стабильности прибыли, дивидендной истории, динамики EPS | — | Критерии 3–5, данные из EDGAR за 10 лет |
| D7 | Дневной рейтинг TradingView подмешан в тезис на годы; в отчёте 2026-01-30 два кандидата с `STRONG_SELL` остались в списке | `technicals.py` | Технический анализ удалён целиком |
| D8 | LLM додумывает: в отчёте 2026-01-30 выпадение BG и LKQ «объяснено» стоп-лоссами и целевыми ценами, которых не существовало | `reporter.py:44` | Go отдаёт факты и трассировку критериев; выдумывать нечего. Причина выпадения вычисляется, а не сочиняется |
| D9 | `_find_available_model()` берёт произвольную первую модель из списка | `reporter.py:76` | LLM-вызовов в Go нет вообще; модель задаёт OpenClaw |
| D10 | Нет ни бэктеста, ни проверки, что отбор работает | — | Схема `runs` сохраняет каждый прогон; бэктест — вне первой версии, но место оставлено |

## 2. Что строим

Один Go-бинарь `msc`, работающий в двух режимах:

- **`msc mcp`** — MCP-сервер поверх stdio. Основной режим, подключается к OpenClaw. Агент (Claude Opus, thinking `xhigh`) вызывает инструменты, сам решает, куда углубиться, и пишет результат в Telegram.
- **`msc screen` / `msc explain` / `msc history` / `msc runs` / `msc funnel`** — ровно те же пять операций, что и инструменты MCP (§7), как обычный CLI с JSON или markdown в stdout. Нужны для разработки, отладки порогов и как запасной путь через cron, если MCP недоступен. Подкоманда и инструмент MCP с одним именем обязаны вызывать одну и ту же функцию — расхождение поведения между режимами недопустимо.

**Границы ответственности.** Go отвечает за «что численно правда»: детерминированный, воспроизводимый расчёт метрик из первоисточника. Агент отвечает за «что это значит и чего в цифрах не видно»: свежие новости, причина дешевизны, отсев value traps, формулировка тезиса и рисков. Агент **не** решает, кто прошёл фильтр — он объясняет, почему прошедшему всё равно можно не верить.

**Чего в Go нет и не будет:** LLM-вызовов, API-ключей, генерации отчётов в файлы, технического анализа, не-US бумаг (EDGAR их не покрывает).

## 3. Критерии отбора

Профиль `graham_defensive` (единственный в v1; `graham_enterprising` — задел на будущее, в v1 не реализуется):

| # | Критерий | Формула | Порог по умолчанию |
|---|---|---|---|
| 1 | Размер | `market_cap = price × shares_outstanding` | ≥ $300M |
| 2 | Финансовая устойчивость | `AssetsCurrent / LiabilitiesCurrent` | ≥ 2.0 |
| 2b | Долговая нагрузка | `LongTermDebtNoncurrent ≤ (AssetsCurrent − LiabilitiesCurrent)` | — |
| 3 | Стабильность прибыли | `EPS_basic > 0` **каждый** год за 10 лет | 10 из 10 |
| 4 | Дивиденды | Ненулевые выплаты каждый год за 10 лет | 10 из 10 (настраивается) |
| 5 | Рост прибыли | `avg(EPS[-3:]) / avg(EPS[-10:-7]) − 1` | ≥ +33% |
| 6 | Цена к прибыли | `price / avg(EPS[-3:])` | ≤ 15 |
| 7 | Цена к капиталу | `P/B ≤ 1.5` **или** `(P/E × P/B) ≤ 22.5` | число Грэма |

Все пороги живут в YAML-конфиге (`--config`), значения выше — дефолты. Изменение порога не требует пересборки.

**Секторные исключения.** Критерии 2 и 2b не определены для компаний без классического деления баланса на оборотные/необоротные статьи:

- **SIC 6000–6799** (финансы, страхование, недвижимость) — исключаются из вселенной по умолчанию. Флаг `include_financials: true` включает их, но критерии 2/2b для них возвращают `unknown`, а не `pass`. Именно этот дефект пропустил VICI (D3).
- **SIC 4900–4949** (утилиты) — остаются, но критерии 2/2b заменяются на `LongTermDebtNoncurrent / StockholdersEquity ≤ 2.0`.

## 4. Источники данных

### 4.1 SEC EDGAR — фундаментал

База: `https://data.sec.gov` и `https://www.sec.gov`.

**Обязательные требования SEC, нарушение = 403:**
- Заголовок `User-Agent` с реальным контактом: `marketscreener/2.0 (<email>)`. Берётся из конфига, при отсутствии — фатальная ошибка на старте с внятным текстом, а не 403 в середине прогона.
- Не более **10 запросов/сек**. Реализуется через `golang.org/x/time/rate`, лимитер общий на весь процесс.
- `Accept-Encoding: gzip`.

**Эндпоинты:**

| Назначение | URL | Примечание |
|---|---|---|
| Тикер → CIK | `https://www.sec.gov/files/company_tickers.json` | ~12k записей, TTL кэша 24 ч |
| Метаданные, SIC | `https://data.sec.gov/submissions/CIK{cik10}.json` | `sic`, `sicDescription`, `name` |
| Срез по всему рынку | `https://data.sec.gov/api/xbrl/frames/us-gaap/{tag}/{unit}/CY{YYYY}Q{N}[I].json` | суффикс `I` — моментальные (баланс), без него — периодные (отчёт о прибылях) |
| Полная история компании | `https://data.sec.gov/api/xbrl/companyfacts/CIK{cik10}.json` | один запрос = вся отчётность |
| Один показатель компании | `https://data.sec.gov/api/xbrl/companyconcept/CIK{cik10}/us-gaap/{tag}.json` | точечные добивки |

CIK паддится нулями до 10 знаков: `320193` → `CIK0000320193`.

**US-GAAP теги с фолбэками** (порядок = приоритет; первый найденный выигрывает, использованный тег пишется в трассировку):

```
EPS                 EarningsPerShareBasic
                  → EarningsPerShareDiluted
NetIncome           NetIncomeLoss
                  → ProfitLoss
Assets              Assets
Liabilities         Liabilities
AssetsCurrent       AssetsCurrent
LiabilitiesCurrent  LiabilitiesCurrent
Equity              StockholdersEquity
                  → StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest
LongTermDebt        LongTermDebtNoncurrent
                  → LongTermDebt
                  → LongTermDebtAndCapitalLeaseObligations
DividendsPerShare   CommonStockDividendsPerShareDeclared
                  → CommonStockDividendsPerShareCashPaid
DividendsPaid       PaymentsOfDividendsCommonStock
                  → PaymentsOfDividends
Revenue             RevenueFromContractWithCustomerExcludingAssessedTax
                  → Revenues
                  → SalesRevenueNet
SharesOutstanding   dei:EntityCommonStockSharesOutstanding   (неймспейс dei, не us-gaap)
                  → CommonStockSharesOutstanding
                  → WeightedAverageNumberOfSharesOutstandingBasic
```

**Известная ловушка frames API:** фрейм `CY2026Q2I` содержит не все компании — те, у кого нестандартный фискальный год, попадают в соседние фреймы. Реализация обязана опрашивать **последние 5 квартальных фреймов** на каждый тег и брать по каждому CIK самый свежий факт (по `end`, при равенстве — по `accn`). Одного фрейма недостаточно; это не оптимизация, а корректность.

### 4.2 Котировки

EDGAR цен не содержит. Основной источник — **Stooq** (без ключа, поддерживает батчи):

```
https://stooq.com/q/l/?s=aapl.us,msft.us,...&f=sd2t2ohlcv&h&e=csv
```

Батч до 50 тикеров на запрос. Фолбэк на тикер, которого нет в Stooq, — Yahoo chart (авторизация не нужна, crumb не требуется):

```
https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1d&interval=1d
```

Если оба источника молчат — цена `unknown`, компания попадает в `incomplete[]`, а не выбрасывается. TTL кэша цен — 1 час.

**Капитализация считается как `price × shares_outstanding`** из dei, а не берётся готовой из Yahoo — чтобы у всех чисел был один воспроизводимый источник.

## 5. Конвейер отбора

Трёхступенчатый. Смысл ступеней — не делать 3000 запросов `companyfacts` там, где хватает 40 запросов `frames`.

**Ступень 0 — вселенная.**
`company_tickers.json` → ~12k записей. Отбрасываем: тикеры с суффиксами деривативов (`-WS`, `-U`, `-R`, `-P`), дубли по CIK (несколько классов акций → оставляем лексикографически первый тикер; объём торгов на этой ступени ещё не известен, а детерминированность списка важнее «правильного» класса), SIC 6000–6799 при `include_financials: false`. Остаётся ~4–5k.

**Ступень 1 — широкий срез через frames.**
~8 тегов × 5 квартальных фреймов ≈ 40 запросов. Плюс котировки батчами (~100 запросов на 5k тикеров). Считаем дешёвые метрики и применяем критерии 1, 2, 2b, 7 и «последний EPS > 0».
Ожидаемый выход: 100–400 компаний. Время: ~1–2 мин с холодным кэшем.

**Ступень 2 — глубина через companyfacts.**
По выжившим — 1 запрос на компанию. При 10 req/s это 10–40 сек. Из полной истории считаем критерии 3, 4, 5 и средний EPS за 3 года для критерия 6.

**Ступень 3 — вердикт.**
Финальная сортировка по запасу прочности (число Грэма по возрастанию), diff с предыдущим прогоном из SQLite, формирование ответа.

**Итог холодного прогона:** ~2–3 минуты, ~600 запросов к SEC. Тёплого (кэш 24 ч) — секунды, только котировки.

## 6. Троичная логика и трассировка

Центральное отличие от старой версии (чинит D4 и D8). Каждый критерий по каждой компании возвращает:

```go
type CriterionResult struct {
    ID      string   // "graham.3.earnings_stability"
    Status  Status   // Pass | Fail | Unknown
    Value   *float64 // фактическое значение, nil при Unknown
    Target  string   // "> 0 каждый год, 10 лет"
    Reason  string   // при Unknown: "нет EarningsPerShareBasic за FY2019, FY2020"
    Tag     string   // какой XBRL-тег фактически использован
    Accn    []string // accession numbers фактов — можно проверить руками
}
```

Правила:
- Компания с хотя бы одним `Fail` → отсеяна, причина известна.
- Компания без `Fail`, но с `Unknown` → **не отсеяна молча**, а попадает в `incomplete[]` с перечнем недостающих фактов. Агент видит её и может добить веб-поиском.
- Компания со всеми `Pass` → в `candidates[]`.

Причина выпадения тикера из прошлого прогона **вычисляется** (какой критерий сменил статус с `Pass` на `Fail`/`Unknown` и с каким значением), а не отдаётся модели на додумывание.

## 7. MCP-контракт

Транспорт — stdio. **Критично:** в режиме `mcp` stdout занят протоколом. Любой `fmt.Print*` в stdout ломает сессию. Все логи, прогресс и диагностика — строго в stderr. Это должно быть зафиксировано линтером или обёрткой логгера, а не дисциплиной.

### Инструменты

**`screen`** — прогон отбора.
```jsonc
// input
{
  "profile": "graham_defensive",      // опц., дефолт из конфига
  "min_market_cap_usd": 300000000,    // опц.
  "include_financials": false,        // опц.
  "max_results": 25,                  // опц., дефолт 25
  "refresh": false                    // опц., true = игнорировать кэш EDGAR
}
```
```jsonc
// output
{
  "run_id": "2026-09-16T09:03:11Z-a1b2c3",
  "as_of": "2026-09-16T09:03:11Z",
  "profile": "graham_defensive",
  "universe_size": 4712,
  "evaluated": 4712,
  "passed": 11,
  "candidates": [
    {
      "ticker": "XXXX", "cik": "0000123456", "name": "...",
      "sic": 3559, "sector": "Special Industry Machinery",
      "price": 41.22, "price_as_of": "2026-09-15", "price_source": "stooq",
      "market_cap": 1840000000,
      "pe_3y_avg": 11.4, "pb": 1.12, "graham_number": 12.77,
      "current_ratio": 2.61, "ltd_to_working_capital": 0.43,
      "eps_positive_years": 10, "dividend_years": 10,
      "eps_growth_10y_pct": 58.2,
      "criteria": [ /* CriterionResult[], см. §6 */ ]
    }
  ],
  "incomplete": [
    { "ticker": "YYYY", "name": "...", "blocking": ["graham.4.dividends"],
      "reason": "нет CommonStockDividendsPerShareDeclared за FY2017-FY2019" }
  ],
  "diff": {
    "new": [ { "ticker": "ZZZZ", "first_seen": "2026-09-16" } ],
    "dropped": [ { "ticker": "BG", "criterion": "graham.6.pe",
                   "was": 14.2, "now": 18.7, "last_seen": "2026-08-12" } ]
  },
  "data_quality": {
    "sec_requests": 612, "sec_429": 0, "cache_hits": 0,
    "price_missing": 37, "facts_missing": 104
  }
}
```

**Ряды за 10 лет в `screen` не возвращаются.** Это сознательное решение против раздувания контекста агента: ответ `screen` должен помещаться в несколько тысяч токенов. За историей агент идёт в `history`/`explain` точечно, по интересным именам.

**`explain`** — полный разбор одного тикера: все 7 критериев с числами, ряды EPS / дивидендов / выручки / капитала за N лет, использованные теги и accession numbers.
`{ "ticker": "XXXX", "years": 10 }`

**`history`** — сырой ряд одного показателя.
`{ "ticker": "XXXX", "metric": "eps|dividends|revenue|equity|debt|assets", "years": 10 }`

**`runs`** — список прошлых прогонов; с `compare: ["run_id_a","run_id_b"]` возвращает diff между любыми двумя.
`{ "limit": 10, "compare": null }`

**`funnel`** — воронка отсева последнего (или указанного) прогона: сколько компаний срезал каждый критерий. Инструмент отладки порогов; без него подбор порогов вслепую.
`{ "run_id": null }`

## 8. Структура проекта

```
cmd/msc/main.go            — подкоманды, разбор флагов
internal/model/            — Company, Fact, Metrics, CriterionResult, Run
internal/edgar/            — SEC-клиент: rate limit, gzip, retry, frames, companyfacts, submissions
internal/prices/           — Stooq + Yahoo fallback, батчинг
internal/metrics/          — расчёт метрик из фактов XBRL
internal/criteria/         — критерии Грэма, троичная логика, секторные ветки
internal/screen/           — трёхступенчатый конвейер
internal/store/            — SQLite: кэш фактов, котировок, истории прогонов
internal/mcpsrv/           — MCP-сервер, определения инструментов
internal/render/           — JSON и markdown для CLI-режима
config/default.yaml        — пороги
testdata/                  — обрезанные companyfacts-фикстуры
```

**Зависимости** (минимум, всё pure-Go, чтобы бинарь кросс-компилировался без cgo):

| Назначение | Пакет |
|---|---|
| MCP | `github.com/modelcontextprotocol/go-sdk` — официальный SDK; если на момент реализации он не готов, `github.com/mark3labs/mcp-go` |
| SQLite | `modernc.org/sqlite` — **обязательно pure-Go**, не `mattn/go-sqlite3` (cgo ломает кросс-компиляцию) |
| Rate limit | `golang.org/x/time/rate` |
| CLI | `github.com/spf13/cobra` |
| YAML | `gopkg.in/yaml.v3` |

Go ≥ 1.24.

### Схема SQLite

```sql
CREATE TABLE companies (
  cik TEXT PRIMARY KEY, ticker TEXT, title TEXT,
  sic INTEGER, sic_desc TEXT, updated_at INTEGER
);
CREATE TABLE facts (
  cik TEXT, tag TEXT, unit TEXT, period_end TEXT,
  fy INTEGER, fp TEXT, form TEXT, val REAL, accn TEXT, fetched_at INTEGER,
  PRIMARY KEY (cik, tag, unit, period_end, accn)
);
CREATE TABLE prices (
  ticker TEXT PRIMARY KEY, price REAL, as_of TEXT, source TEXT, fetched_at INTEGER
);
CREATE TABLE runs (
  id TEXT PRIMARY KEY, started_at INTEGER, finished_at INTEGER,
  profile TEXT, params_json TEXT, universe_size INTEGER, passed_count INTEGER
);
CREATE TABLE run_results (
  run_id TEXT, ticker TEXT, verdict TEXT, score REAL,
  metrics_json TEXT, criteria_json TEXT,
  PRIMARY KEY (run_id, ticker)
);
CREATE INDEX idx_facts_lookup ON facts(cik, tag, period_end);
```

TTL: `facts` и `companies` — 24 ч, `prices` — 1 ч. Путь к БД задаётся флагом `--db`, по умолчанию `$XDG_DATA_HOME/marketscreener/msc.db` (при отсутствии переменной — `~/.local/share/marketscreener/msc.db`). Файлов отчётов бинарь не создаёт никогда.

## 9. Обработка ошибок

| Ситуация | Поведение |
|---|---|
| Нет `User-Agent` с контактом в конфиге | Фатально на старте, до первого запроса, с текстом требования SEC |
| SEC 403 | Фатально, с подсказкой про User-Agent |
| SEC 429 | Экспоненциальный backoff (1s → 2s → 4s → 8s), до 5 попыток; после — компания помечается `unknown`, прогон продолжается |
| Таймаут/сеть на одной компании | `unknown` по затронутым критериям, прогон продолжается |
| Обе котировочные площадки молчат | Цена `unknown` → компания в `incomplete[]` |
| Нет XBRL-тега и всех его фолбэков | `unknown` с перечнем испробованных тегов в `Reason` |
| Ступень 1 вернула пустой список | Не ошибка. Пустой `candidates[]` с заполненным `funnel` — валидный и осмысленный ответ |

Принцип: **частичный отказ не роняет прогон и не превращается в молчаливое исключение компании.** Ровно это отличает новую версию от старой.

## 10. Тестирование

- **`internal/criteria`, `internal/metrics`** — табличные тесты на фикстурах реальных, обрезанных `companyfacts` в `testdata/`. Обязательный набор случаев: (1) компания, проходящая все 7; (2) отрицательный EPS в середине десятилетия; (3) финансовая компания без `AssetsCurrent`; (4) пропущенный основной тег, срабатывает фолбэк; (5) нестандартный фискальный год; (6) разрыв в дивидендах на один год.
- **`internal/edgar`** — `httptest`: соблюдение rate limit, retry на 429, распаковка gzip, склейка нескольких фреймов и выбор самого свежего факта на CIK.
- **Контракт `screen`** — golden-файл JSON. Изменение формы ответа должно ломать тест, потому что на неё завязан агент.
- **Сеть в юнит-тестах запрещена.** Интеграционные тесты с живым SEC — за build tag `integration`, в CI не гоняются.

## 11. Интеграция с OpenClaw

Разработка и запуск — на хосте с OpenClaw. Бинарь собирается там же: `go build -o /srv/marketscreener/msc ./cmd/msc`.

### Регистрация MCP-сервера

```bash
openclaw mcp add marketscreener \
  --command /srv/marketscreener/msc \
  --arg mcp \
  --cwd /srv/marketscreener

openclaw mcp doctor marketscreener --probe
```

Эквивалент в `openclaw.json`:

```json5
{
  mcp: {
    servers: {
      "marketscreener": {
        command: "/srv/marketscreener/msc",
        args: ["mcp"],
        cwd: "/srv/marketscreener",
        transport: "stdio",
        enabled: true,
        connectionTimeoutMs: 10000,
        requestTimeoutMs: 300000   // холодный прогон screen занимает 2-3 мин
      }
    }
  }
}
```

`requestTimeoutMs` — не деталь: дефолтные 20 сек гарантированно обрывают `screen` на холодном кэше.

Имя сервера `__proto__` зарезервировано (упомянуто в документации); `marketscreener` конфликтов не имеет.

### Модель и уровень рассуждения

```json5
{
  agents: {
    defaults: {
      model: { primary: "anthropic/claude-opus-5" },
      thinkingDefault: "xhigh"
    }
  }
}
```

Два момента, которые **надо проверить на ноде**, а не принимать на веру из этой спеки:

1. **Точный идентификатор модели.** В документации OpenClaw примеры используют схему вида `anthropic/claude-sonnet-4-6`. Актуальный идентификатор для Opus нужно взять из `openclaw models` или конфига ноды, а не копировать отсюда.
2. **`thinkingDefault` на уровне агента.** «Extra high» / «extra effort» отображается в уровень `xhigh` — это то, что просил заказчик. Но в трекере OpenClaw есть открытый issue: whitelist полей конфига агента не включает `thinking`/`thinkingDefault`, и попытка задать уровень **на конкретном агенте** отвергается валидацией. Поэтому уровень ставится в `agents.defaults`, а при проблемах — через `/think xhigh` в сессии. Если к моменту реализации issue закрыт, можно ставить на агенте.

### Skill для агента

Кладётся в директорию скиллов OpenClaw как `marketscreener/SKILL.md` и подключается в `agents.defaults.skills`. Содержание:

- Когда запускать: по расписанию раз в сутки после закрытия рынка США и по явной просьбе.
- Порядок работы: `screen` → по каждому кандидату из `candidates[]` проверить свежие новости и причину дешевизны веб-поиском → при необходимости `explain` по конкретному тикеру → отсеять value traps → написать сообщение в Telegram.
- Обязательно разобрать `incomplete[]`: это компании, которым не хватило данных в EDGAR, а не отвергнутые. Часть из них добивается веб-поиском.
- Обязательно объяснить `diff.dropped[]`, используя **вычисленную** причину из ответа, и ничего не добавлять от себя. Прямой запрет на домысливание причин выпадения — это регресс, который уже случался в Python-версии.
- Формат сообщения в Telegram: не более ~15 строк, тезис по каждому имени в одну-две строки, отдельной строкой — что именно вызывает сомнение.

### Расписание

```json5
{ cron: { enabled: true, sessionRetention: "24h" } }
```

Задача — ежедневный запуск скилла после закрытия рынка США (22:00 UTC / 21:00 UTC в летнее время).

## 12. Что осознанно не делаем в v1

- **Бэктест.** Таблицы `runs`/`run_results` копят данные для него с первого дня, но сам бэктест — отдельная задача.
- **Профиль `graham_enterprising`.** Поле `profile` в контракте есть, значение одно.
- **Не-US бумаги.** EDGAR их не покрывает; поддержка потребует второго источника и другой архитектуры.
- **Технический анализ в любом виде.** См. D7.
- **Веб-UI, графики, PDF.** Вывод — только stdout и Telegram через агента.

## 13. Этапы

| Этап | Содержание | Готовность = |
|---|---|---|
| M1 | `internal/edgar` + `internal/store` | Тесты на `httptest` зелёные, кэш работает, rate limit соблюдён |
| M2 | `internal/metrics` + `internal/criteria` | Все 6 фикстур из §10 дают ожидаемые вердикты, включая `Unknown` |
| M3 | `internal/prices` + `internal/screen` ступени 0–2 | `msc screen --format json` даёт непустой осмысленный результат на живых данных |
| M4 | `runs`, diff, `funnel` | Второй прогон корректно показывает `new`/`dropped` с вычисленной причиной |
| M5 | `internal/mcpsrv` | `openclaw mcp doctor marketscreener --probe` проходит, все 5 инструментов отвечают |
| M6 | Skill + cron + первое сообщение в Telegram | Агент прошёл полный цикл без ручного вмешательства |

M1–M2 можно вести параллельно с M3 только после фиксации типов в `internal/model`.

---

## Приложение: проверяемые утверждения

Пункты, которые реализующая сторона должна подтвердить на живых данных до того, как закладываться на них:

1. Frames API действительно требует опроса нескольких соседних фреймов для полного покрытия (§4.1) — проверить на компании с фискальным годом, заканчивающимся не в декабре.
2. Stooq отдаёт батч до 50 тикеров одним запросом без ключа и без блокировки (§4.2).
3. Актуальный идентификатор модели Opus в OpenClaw и работоспособность `thinkingDefault: "xhigh"` (§11).
4. Официальный Go SDK для MCP готов к использованию; иначе — `mark3labs/mcp-go` (§8).
