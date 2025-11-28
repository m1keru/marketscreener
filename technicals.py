from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional

from rich.console import Console
from tradingview_ta import Interval, TA_Handler


console = Console()
MAX_WORKERS = 8
EXCHANGES = ("NASDAQ", "NYSE", "AMEX")


def _fetch_from_tradingview(symbol: str) -> Optional[Dict[str, Any]]:
    for exchange in EXCHANGES:
        handler = TA_Handler(
            symbol=symbol,
            screener="america",
            exchange=exchange,
            interval=Interval.INTERVAL_1_DAY,
        )
        try:
            analysis = handler.get_analysis()
            indicators = analysis.indicators or {}
            summary = analysis.summary or {}
            return {
                "ticker": symbol,
                "rating": summary.get("RECOMMENDATION"),
                "oscillators": summary.get("OSCILLATORS"),
                "moving_averages": summary.get("MOVING_AVERAGES"),
                "rsi": indicators.get("RSI"),
                "ema20": indicators.get("EMA20"),
                "ema50": indicators.get("EMA50"),
                "macd": indicators.get("MACD.macd"),
            }
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            continue

    console.log(f"[yellow]TradingView data unavailable for {symbol}: {last_exc}")
    return None


async def enrich_with_technicals(
    fundamentals: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    symbols = [item["ticker"] for item in fundamentals]

    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        tasks = [loop.run_in_executor(executor, _fetch_from_tradingview, sym) for sym in symbols]
        tech_results = await asyncio.gather(*tasks, return_exceptions=True)

    technical_map: Dict[str, Dict[str, Any]] = {}
    for result in tech_results:
        if isinstance(result, dict):
            technical_map[result["ticker"]] = result

    enriched = []
    for stock in fundamentals:
        merged = stock.copy()
        merged["technicals"] = technical_map.get(stock["ticker"], {})
        enriched.append(merged)
    return enriched


