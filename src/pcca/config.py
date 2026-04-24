from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value


@dataclass
class Settings:
    timezone: str
    nightly_cron: str
    morning_cron: str
    data_dir: Path
    db_path: Path
    browser_profiles_dir: Path
    browser_headless: bool
    ollama_enabled: bool
    ollama_base_url: str
    ollama_model: str
    telegram_bot_token: str | None

    @classmethod
    def from_env(cls) -> "Settings":
        data_dir = Path(_env("PCCA_DATA_DIR", ".pcca") or ".pcca")
        db_path = Path(_env("PCCA_DB_PATH", str(data_dir / "pcca.db")) or str(data_dir / "pcca.db"))
        browser_profiles_dir = Path(
            _env("PCCA_BROWSER_PROFILES_DIR", str(data_dir / "browser_profiles")) or str(data_dir / "browser_profiles")
        )
        browser_headless_raw = (_env("PCCA_BROWSER_HEADLESS", "true") or "true").strip().lower()
        browser_headless = browser_headless_raw in {"1", "true", "yes", "on"}
        ollama_enabled_raw = (_env("PCCA_OLLAMA_ENABLED", "false") or "false").strip().lower()
        ollama_enabled = ollama_enabled_raw in {"1", "true", "yes", "on"}
        return cls(
            timezone=_env("PCCA_TIMEZONE", "Asia/Makassar") or "Asia/Makassar",
            nightly_cron=_env("PCCA_NIGHTLY_CRON", "0 1 * * *") or "0 1 * * *",
            morning_cron=_env("PCCA_MORNING_CRON", "30 8 * * *") or "30 8 * * *",
            data_dir=data_dir,
            db_path=db_path,
            browser_profiles_dir=browser_profiles_dir,
            browser_headless=browser_headless,
            ollama_enabled=ollama_enabled,
            ollama_base_url=_env("PCCA_OLLAMA_BASE_URL", "http://localhost:11434") or "http://localhost:11434",
            ollama_model=_env("PCCA_OLLAMA_MODEL", "qwen2.5:7b") or "qwen2.5:7b",
            telegram_bot_token=_env("PCCA_TELEGRAM_BOT_TOKEN"),
        )

    def ensure_dirs(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.browser_profiles_dir.mkdir(parents=True, exist_ok=True)
