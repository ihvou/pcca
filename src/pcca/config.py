from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _load_dotenv(path: Path = Path(".env")) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key or key in os.environ:
            continue
        value = value.strip().strip("\"'")
        os.environ[key] = value


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
    browser_headful_platforms: set[str]
    browser_channel: str | None
    ollama_enabled: bool
    ollama_base_url: str
    ollama_model: str
    telegram_bot_token: str | None

    @classmethod
    def from_env(cls) -> "Settings":
        _load_dotenv()
        data_dir = Path(_env("PCCA_DATA_DIR", ".pcca") or ".pcca")
        db_path = Path(_env("PCCA_DB_PATH", str(data_dir / "pcca.db")) or str(data_dir / "pcca.db"))
        browser_profiles_dir = Path(
            _env("PCCA_BROWSER_PROFILES_DIR", str(data_dir / "browser_profiles")) or str(data_dir / "browser_profiles")
        )
        browser_headless_raw = (_env("PCCA_BROWSER_HEADLESS", "true") or "true").strip().lower()
        browser_headless = browser_headless_raw in {"1", "true", "yes", "on"}
        headful_platforms_raw = _env("PCCA_BROWSER_HEADFUL_PLATFORMS", "x,linkedin") or "x,linkedin"
        browser_headful_platforms = {
            p.strip().lower() for p in headful_platforms_raw.split(",") if p.strip()
        }
        browser_channel = _env("PCCA_BROWSER_CHANNEL", "chrome")
        if browser_channel is not None:
            browser_channel = browser_channel.strip().lower() or None
        ollama_enabled_raw = (_env("PCCA_OLLAMA_ENABLED", "false") or "false").strip().lower()
        ollama_enabled = ollama_enabled_raw in {"1", "true", "yes", "on"}
        return cls(
            timezone=_env("PCCA_TIMEZONE", "UTC") or "UTC",
            nightly_cron=_env("PCCA_NIGHTLY_CRON", "0 1 * * *") or "0 1 * * *",
            morning_cron=_env("PCCA_MORNING_CRON", "30 8 * * *") or "30 8 * * *",
            data_dir=data_dir,
            db_path=db_path,
            browser_profiles_dir=browser_profiles_dir,
            browser_headless=browser_headless,
            browser_headful_platforms=browser_headful_platforms,
            browser_channel=browser_channel,
            ollama_enabled=ollama_enabled,
            ollama_base_url=_env("PCCA_OLLAMA_BASE_URL", "http://localhost:11434") or "http://localhost:11434",
            ollama_model=_env("PCCA_OLLAMA_MODEL", "qwen2.5:7b") or "qwen2.5:7b",
            telegram_bot_token=_env("PCCA_TELEGRAM_BOT_TOKEN"),
        )

    def ensure_dirs(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.browser_profiles_dir.mkdir(parents=True, exist_ok=True)
