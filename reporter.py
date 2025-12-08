from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List

import google.generativeai as genai
import yfinance as yf
from rich.console import Console

from config import get_settings


console = Console()
_configured: bool = False


def _configure_gemini() -> None:
    global _configured
    if not _configured:
        settings = get_settings()
        genai.configure(api_key=settings.gemini_api_key)
        _configured = True


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
) -> str:
    payload = {
        "market_context": market_context,
        "screened_stocks": stocks,
        "new_symbols": new_symbols,
        "dropped_symbols": dropped_symbols,
    }

    prompt = f"""Ты строгий финансовый аналитик, придерживаешься регламентов compliance и объясняешь выводы без эмоций.

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
    return prompt


def _find_available_model() -> str:
    """Find an available Gemini model."""
    _configure_gemini()
    try:
        models = genai.list_models()
        available = [m.name for m in models if "generateContent" in m.supported_generation_methods]
        if available:
            # Extract model name without 'models/' prefix if present
            model_name = available[0].replace("models/", "")
            console.log(f"[blue]Found available model: {model_name}")
            return model_name
    except Exception as e:
        console.log(f"[yellow]Could not list models: {e}")
    
    # Fallback to common model names
    return "gemini-pro"


def generate_report(
    stocks: List[Dict[str, Any]],
    market_context: Dict[str, Any],
    new_symbols: List[str],
    dropped_symbols: List[str],
    model: str | None = None,
) -> str:
    _configure_gemini()
    prompt = _build_prompt(stocks, market_context, new_symbols, dropped_symbols)
    
    # Use provided model or find an available one
    if model is None:
        model = _find_available_model()
    
    console.log(f"[green]Requesting report from Gemini ({model})...")
    
    # Try different model name formats
    model_variants = [model, f"models/{model}"]
    if model.startswith("models/"):
        model_variants = [model, model.replace("models/", "")]
    
    last_error = None
    for model_name in model_variants:
        try:
            genai_model = genai.GenerativeModel(model_name=model_name)
            generation_config = genai.types.GenerationConfig(
                temperature=0.2,
            )
            response = genai_model.generate_content(
                prompt,
                generation_config=generation_config,
            )
            
            if response.text:
                return response.text
            raise RuntimeError("Gemini response did not contain text output.")
        except Exception as e:
            last_error = e
            continue
    
    raise RuntimeError(f"Failed to generate report with Gemini. Tried models: {model_variants}. Last error: {last_error}") from last_error


