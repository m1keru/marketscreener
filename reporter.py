from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List

import yfinance as yf
from openai import OpenAI
from rich.console import Console

from config import get_settings


console = Console()
_client: OpenAI | None = None


def _get_client() -> OpenAI:
    global _client
    if _client is None:
        settings = get_settings()
        _client = OpenAI(api_key=settings.openai_api_key)
    return _client


def fetch_market_context() -> Dict[str, Any]:
    ticker = yf.Ticker("^GSPC")
    hist = ticker.history(period="5d")
    change_pct = None
    if not hist.empty:
        last_close = hist["Close"].iloc[-1]
        prev_close = hist["Close"].iloc[0]
        change_pct = ((last_close - prev_close) / prev_close) * 100

    return {
        "index": "^GSPC",
        "change_pct_5d": change_pct,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def _build_prompt(
    stocks: List[Dict[str, Any]],
    market_context: Dict[str, Any],
    new_symbols: List[str],
    dropped_symbols: List[str],
) -> List[Dict[str, str]]:
    payload = {
        "market_context": market_context,
        "screened_stocks": stocks,
        "new_symbols": new_symbols,
        "dropped_symbols": dropped_symbols,
    }

    user_prompt = f"""
Ты опытный инвестиционный аналитик. Используй предоставленные данные (JSON ниже), чтобы подготовить структурированный отчёт на русском языке.

Требования к ответу (Markdown):
1. Краткое состояние рынка (1 абзац, укажи динамику S&P500).
2. Таблица результатов скринера (тикер, сектор, цена, P/E, P/B, Current Ratio, Debt/Assets, технический рейтинг TradingView, RSI, EMA20).
3. Отдельный блок про новые идеи (new_symbols) с более глубоким разбором каждой компании.
4. Блок рисков (общесекторные + индивидуальные).
5. Если есть dropped_symbols — упомяни, почему они могли выпасть.

Данные:
{json.dumps(payload, ensure_ascii=False, indent=2)}
"""
    return [
        {
            "role": "system",
            "content": "Ты строгий финансовый аналитик, придерживаешься регламентов compliance и объясняешь выводы без эмоций.",
        },
        {"role": "user", "content": user_prompt},
    ]


def generate_report(
    stocks: List[Dict[str, Any]],
    market_context: Dict[str, Any],
    new_symbols: List[str],
    dropped_symbols: List[str],
    model: str = "gpt-4o-mini",
) -> str:
    client = _get_client()
    messages = _build_prompt(stocks, market_context, new_symbols, dropped_symbols)
    console.log("[green]Requesting report from OpenAI...")
    response = client.responses.create(
        model=model,
        input=messages,
        temperature=0.2,
    )

    for item in response.output or []:
        for chunk in item.content:
            if chunk.type == "output_text":
                return chunk.text

    raise RuntimeError("OpenAI response did not contain text output.")


