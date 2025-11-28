from __future__ import annotations

import asyncio
import csv
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from typing import Any, Dict, Iterable, List, Optional

from bs4 import BeautifulSoup
import requests
import yfinance as yf
from rich.console import Console
from rich.progress import track
from tenacity import retry, stop_after_attempt, wait_exponential

from config import (
    CURRENT_RATIO_MIN,
    DEBT_TO_ASSETS_MAX,
    DEBUG_MODE,
    PB_MAX,
    PE_MAX,
    PRICE_MAX,
    PRICE_MIN,
)


console = Console()
MAX_WORKERS = 8
BATCH_SIZE = 25
SP500_DATAHUB_SOURCE = "https://datahub.io/core/s-and-p-500-companies/r/constituents.csv"
SP500_WIKI_SOURCE = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; TradingConsultant/1.0; +https://github.com/ranaroussi/yfinance)"
}


@dataclass(slots=True)
class ScreenedStock:
    ticker: str
    name: Optional[str]
    sector: Optional[str]
    price: Optional[float]
    trailing_pe: Optional[float]
    price_to_book: Optional[float]
    current_ratio: Optional[float]
    debt_to_assets: Optional[float]
    market_cap: Optional[float]
    beta: Optional[float]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _split_batches(items: Iterable[str], size: int) -> List[List[str]]:
    batch: List[str] = []
    batches: List[List[str]] = []
    for symbol in items:
        batch.append(symbol)
        if len(batch) >= size:
            batches.append(batch[:])
            batch.clear()
    if batch:
        batches.append(batch)
    return batches


def _compute_current_ratio(ticker_obj: yf.Ticker, fallback: Optional[float]) -> Optional[float]:
    if fallback:
        return fallback

    sheet = ticker_obj.balance_sheet
    if sheet is None or sheet.empty:
        return None

    def _get_value(label: str) -> Optional[float]:
        try:
            series = sheet.loc[label]
        except KeyError:
            return None
        return float(series.iloc[0]) if not series.empty else None

    current_assets = _get_value("Total Current Assets")
    current_liabilities = _get_value("Total Current Liabilities")
    if current_assets and current_liabilities:
        if current_liabilities == 0:
            return None
        return current_assets / current_liabilities
    return None


def _compute_debt_to_assets(info: Dict[str, Any], ticker_obj: yf.Ticker) -> Optional[float]:
    total_debt = info.get("totalDebt")
    total_assets = info.get("totalAssets")

    if not total_assets:
        sheet = ticker_obj.balance_sheet
        if sheet is not None and not sheet.empty:
            try:
                total_assets = float(sheet.loc["Total Assets"].iloc[0])
            except (KeyError, IndexError, ValueError):
                total_assets = None

    if not total_debt:
        sheet = ticker_obj.balance_sheet
        if sheet is not None and not sheet.empty:
            try:
                total_debt = float(sheet.loc["Total Debt"].iloc[0])
            except (KeyError, IndexError, ValueError):
                total_debt = None

    if total_debt is None or total_assets in (None, 0):
        return None
    return total_debt / total_assets


def _price_from_info(info: Dict[str, Any]) -> Optional[float]:
    for key in ("currentPrice", "regularMarketPrice", "previousClose"):
        value = info.get(key)
        if value:
            return float(value)
    return None


def _passes_filters(stock: ScreenedStock) -> bool:
    if DEBUG_MODE:
        _debug_stock_eval(stock)
    if stock.trailing_pe is None or not (0 < stock.trailing_pe <= PE_MAX):
        return False
    if stock.price_to_book is None or not (0 < stock.price_to_book <= PB_MAX):
        return False
    if stock.current_ratio is None or stock.current_ratio < CURRENT_RATIO_MIN:
        return False
    if stock.debt_to_assets is None or stock.debt_to_assets > DEBT_TO_ASSETS_MAX:
        return False
    if stock.price is None or not (PRICE_MIN <= stock.price <= PRICE_MAX):
        return False
    return True


def _debug_stock_eval(stock: ScreenedStock) -> None:
    checks = [
        (
            "price",
            stock.price,
            f"{PRICE_MIN}-{PRICE_MAX}",
            stock.price is not None and PRICE_MIN <= stock.price <= PRICE_MAX,
        ),
        (
            "P/E",
            stock.trailing_pe,
            f"0-{PE_MAX}",
            stock.trailing_pe is not None and 0 < stock.trailing_pe <= PE_MAX,
        ),
        (
            "P/B",
            stock.price_to_book,
            f"0-{PB_MAX}",
            stock.price_to_book is not None and 0 < stock.price_to_book <= PB_MAX,
        ),
        (
            "Current ratio",
            stock.current_ratio,
            f">= {CURRENT_RATIO_MIN}",
            stock.current_ratio is not None and stock.current_ratio >= CURRENT_RATIO_MIN,
        ),
        (
            "Debt/Assets",
            stock.debt_to_assets,
            f"<= {DEBT_TO_ASSETS_MAX}",
            stock.debt_to_assets is not None and stock.debt_to_assets <= DEBT_TO_ASSETS_MAX,
        ),
    ]
    console.log(f"[magenta]{stock.ticker}[/] fundamentals snapshot:")
    for label, value, target, ok in checks:
        console.log(
            f"    {label:<14} = {value!r:<15} | target {target:<10} -> {'OK' if ok else 'FAIL'}"
        )


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
def _fetch_single(symbol: str) -> Optional[ScreenedStock]:
    ticker_obj = yf.Ticker(symbol)
    info = ticker_obj.info or {}
    price = _price_from_info(info)
    trailing_pe = info.get("trailingPE")
    price_to_book = info.get("priceToBook")
    current_ratio = _compute_current_ratio(ticker_obj, info.get("currentRatio"))
    debt_to_assets = _compute_debt_to_assets(info, ticker_obj)

    stock = ScreenedStock(
        ticker=symbol,
        name=info.get("shortName") or info.get("longName"),
        sector=info.get("sector"),
        price=price,
        trailing_pe=float(trailing_pe) if trailing_pe else None,
        price_to_book=float(price_to_book) if price_to_book else None,
        current_ratio=current_ratio,
        debt_to_assets=debt_to_assets,
        market_cap=info.get("marketCap"),
        beta=info.get("beta"),
    )

    if _passes_filters(stock):
        return stock
    return None


def _fetch_batch(symbols: List[str]) -> List[ScreenedStock]:
    results: List[ScreenedStock] = []
    for symbol in symbols:
        try:
            stock = _fetch_single(symbol)
            if stock:
                results.append(stock)
        except Exception as exc:  # noqa: BLE001 - we want to continue
            console.log(f"[yellow]Skipping {symbol} due to error: {exc}")
    return results


def _load_tickers_from_wikipedia() -> List[str]:
    try:
        resp = requests.get(SP500_WIKI_SOURCE, headers=DEFAULT_HEADERS, timeout=20)
        resp.raise_for_status()
    except requests.RequestException as exc:
        console.log(f"[yellow]Failed to pull Wikipedia constituents: {exc}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table", {"id": "constituents"})
    if table is None:
        table = soup.select_one("table.wikitable.sortable")
    if table is None:
        console.log("[yellow]Wikipedia response didn't contain the constituents table.")
        return []

    tickers: List[str] = []
    for row in table.find_all("tr"):
        cells = row.find_all("td")
        if not cells:
            continue
        symbol = cells[0].get_text(strip=True)
        if symbol:
            tickers.append(symbol.replace(".", "-"))
    return tickers


def _load_tickers_from_datahub() -> List[str]:
    try:
        resp = requests.get(SP500_DATAHUB_SOURCE, timeout=15)
        resp.raise_for_status()
    except requests.RequestException as exc:
        console.log(f"[red]Failed to download S&P 500 constituents CSV: {exc}")
        return []

    tickers: List[str] = []
    reader = csv.DictReader(resp.text.splitlines())
    for row in reader:
        symbol = row.get("Symbol")
        if symbol:
            tickers.append(symbol.strip().upper())
    return tickers


def _load_sp500_tickers() -> List[str]:
    tickers = _load_tickers_from_wikipedia()
    if tickers:
        return tickers

    console.log("[yellow]Falling back to DataHub constituents feed.")
    return _load_tickers_from_datahub()


async def screen_stocks(limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Screen SP500 tickers and return a list of dicts for downstream processing.
    """
    tickers = _load_sp500_tickers()
    if not tickers:
        raise RuntimeError("Failed to load S&P 500 tickers.")

    async def _screen(symbols: List[str]) -> List[ScreenedStock]:
        batches = _split_batches(symbols, BATCH_SIZE)

        loop = asyncio.get_running_loop()
        screened: List[ScreenedStock] = []

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            tasks = [
                loop.run_in_executor(executor, _fetch_batch, batch) for batch in batches
            ]
            for task in track(
                asyncio.as_completed(tasks),
                total=len(tasks),
                description="Screening fundamentals",
            ):
                batch_result = await task
                screened.extend(batch_result)
        return screened

    subset = tickers[:limit] if limit else tickers
    screened = await _screen(subset)

    if not screened and limit:
        console.log(
            f"[yellow]No matches inside the first {limit} tickers, retrying full universe..."
        )
        screened = await _screen(tickers)

    console.log(
        "[blue]Fundamental candidates:",
        ", ".join(stock.ticker for stock in screened) or "none",
    )

    screened.sort(key=lambda s: (s.price or 0), reverse=True)
    return [item.to_dict() for item in screened]

