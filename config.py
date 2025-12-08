from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent
STATE_FILE = BASE_DIR / "history.json"
REPORTS_DIR = BASE_DIR / "reports"


# Load the .env file if present so local development works without exporting vars
load_dotenv()


# Screening constraints
PRICE_MIN = 10
PRICE_MAX = 100
PE_MAX = 15
PB_MAX = 2
CURRENT_RATIO_MIN = 1.5
DEBT_TO_ASSETS_MAX = 1.0
DEBUG_MODE = os.getenv("DEBUG_MODE", "0").lower() in {"1", "true", "yes", "on"}


@dataclass(slots=True)
class Settings:
    gemini_api_key: str
    telegram_bot_token: Optional[str]
    chat_id: Optional[str]


_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """
    Lazily build the Settings object, ensuring required env vars exist.
    """
    global _settings
    if _settings is None:
        gemini_api_key = os.getenv("GEMINI_API_KEY")
        if not gemini_api_key:
            raise RuntimeError("GEMINI_API_KEY is required but missing.")

        _settings = Settings(
            gemini_api_key=gemini_api_key,
            telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN"),
            chat_id=os.getenv("CHAT_ID"),
        )

    return _settings


def ensure_storage() -> None:
    """
    Make sure local directories/files exist so the daemon does not crash
    on first boot.
    """
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    if not STATE_FILE.exists():
        STATE_FILE.write_text("[]")


