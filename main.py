from __future__ import annotations

import argparse
import asyncio
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import schedule
from rich.console import Console

from config import REPORTS_DIR, STATE_FILE, ensure_storage
from reporter import fetch_market_context, generate_report
from screener import screen_stocks
from technicals import enrich_with_technicals


console = Console()


def load_history() -> List[str]:
    if not STATE_FILE.exists():
        return []
    try:
        data = json.loads(STATE_FILE.read_text())
        if isinstance(data, list):
            return [str(item) for item in data]
    except json.JSONDecodeError:
        console.log("[yellow]history.json corrupted, resetting.")
    return []


def save_history(symbols: List[str]) -> None:
    STATE_FILE.write_text(json.dumps(sorted(set(symbols)), indent=2))


def diff_symbols(previous: List[str], current: List[str]) -> tuple[List[str], List[str]]:
    prev_set = set(previous)
    current_set = set(current)
    new_symbols = sorted(current_set - prev_set)
    dropped_symbols = sorted(prev_set - current_set)
    return new_symbols, dropped_symbols


async def run_analysis_cycle(limit: int | None = None) -> Path:
    console.rule("[bold blue]Stock Analysis Cycle")
    ensure_storage()

    fundamentals = await screen_stocks(limit=limit)
    console.log(f"[cyan]Fundamental screener produced {len(fundamentals)} symbols.")
    if not fundamentals:
        raise RuntimeError("Screener returned no candidates.")

    enriched = await enrich_with_technicals(fundamentals)
    tickers = [item["ticker"] for item in enriched]

    prev = load_history()
    new_symbols, dropped_symbols = diff_symbols(prev, tickers)
    save_history(tickers)

    market_context = fetch_market_context()
    report_markdown = await asyncio.to_thread(
        generate_report, enriched, market_context, new_symbols, dropped_symbols
    )

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    report_path = REPORTS_DIR / f"{today}.md"
    report_path.write_text(report_markdown)
    console.log(f"[green]Report saved to {report_path}")
    return report_path


def _scheduled_job(limit: int | None = None) -> None:
    asyncio.run(run_analysis_cycle(limit=limit))


def main() -> None:
    parser = argparse.ArgumentParser(description="Daily stock analysis daemon.")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single analysis cycle and exit (useful for tests).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of tickers for quick dry runs.",
    )
    args = parser.parse_args()

    if args.once:
        asyncio.run(run_analysis_cycle(limit=args.limit))
        return

    console.log("[green]Starting scheduler (09:00 UTC daily).")
    schedule.every().day.at("09:00").do(_scheduled_job, limit=args.limit)
    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    main()


