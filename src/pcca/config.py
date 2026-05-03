from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _load_dotenv(path: Path = Path(".env")) -> None:
    """Populate `os.environ` from `.env` for keys that aren't already set.

    "Already set" means the env var is in `os.environ` AND has a non-empty
    value. An existing empty placeholder (e.g., `PCCA_TELEGRAM_BOT_TOKEN=`
    from a previous load against a not-yet-edited `.env`) is treated as
    unset so the file is allowed to override it on the next load. Non-empty
    real environment exports still take precedence over `.env`.
    """
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        existing = os.environ.get(key)
        if existing is not None and existing != "":
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
    digest_auto_send: bool
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
    model_router_timeout_seconds: int = 180
    session_refresh_enabled: bool = True
    session_refresh_cooldown_seconds: int = 1800
    session_refresh_browser: str | None = None
    platform_circuit_threshold: int = 5
    platform_empty_threshold: int = 25
    scorer: str = "both"
    embedding_model: str = "nomic-embed-text:v1.5"
    embedding_timeout_seconds: int = 30
    embedding_max_chars: int = 7500
    embedding_backfill_concurrency: int = 2
    auto_backfill_embeddings: bool = True
    youtube_transcript_backfill_concurrency: int = 2

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
        # Default OFF: in v1 daily Brief delivery is on-demand via the Telegram bot's
        # "Get Briefs" button. Set PCCA_DIGEST_AUTO_SEND=true to re-enable the
        # morning_cron auto-send for users who have a stable nightly+morning routine.
        digest_auto_send_raw = (_env("PCCA_DIGEST_AUTO_SEND", "false") or "false").strip().lower()
        digest_auto_send = digest_auto_send_raw in {"1", "true", "yes", "on"}
        try:
            model_router_timeout_seconds = int(_env("PCCA_MODEL_ROUTER_TIMEOUT_SECONDS", "180") or "180")
        except ValueError:
            model_router_timeout_seconds = 180
        session_refresh_enabled_raw = (_env("PCCA_SESSION_REFRESH_ENABLED", "true") or "true").strip().lower()
        session_refresh_enabled = session_refresh_enabled_raw in {"1", "true", "yes", "on"}
        try:
            session_refresh_cooldown_seconds = int(_env("PCCA_SESSION_REFRESH_COOLDOWN_SECONDS", "1800") or "1800")
        except ValueError:
            session_refresh_cooldown_seconds = 1800
        session_refresh_browser = _env("PCCA_SESSION_REFRESH_BROWSER", None)
        if session_refresh_browser is not None:
            session_refresh_browser = session_refresh_browser.strip().lower() or None
        try:
            platform_circuit_threshold = int(_env("PCCA_PLATFORM_CIRCUIT_THRESHOLD", "5") or "5")
        except ValueError:
            platform_circuit_threshold = 5
        try:
            platform_empty_threshold = int(_env("PCCA_PLATFORM_EMPTY_THRESHOLD", "25") or "25")
        except ValueError:
            platform_empty_threshold = 25
        scorer = (_env("PCCA_SCORER", "both") or "both").strip().lower()
        if scorer not in {"keyword", "embedding", "both"}:
            scorer = "both"
        try:
            embedding_timeout_seconds = int(_env("PCCA_EMBEDDING_TIMEOUT_SECONDS", "30") or "30")
        except ValueError:
            embedding_timeout_seconds = 30
        try:
            embedding_max_chars = int(_env("PCCA_EMBEDDING_MAX_CHARS", "7500") or "7500")
        except ValueError:
            embedding_max_chars = 7500
        # T-100: Concurrency for embedding-backfill calls to Ollama. Lower
        # values produce a cooler chip at the cost of slightly longer
        # backfill duration. Default 2 (was 4) for thermal safety on
        # laptops; tune via PCCA_EMBEDDING_BACKFILL_CONCURRENCY.
        try:
            embedding_backfill_concurrency = int(
                _env("PCCA_EMBEDDING_BACKFILL_CONCURRENCY", "2") or "2"
            )
        except ValueError:
            embedding_backfill_concurrency = 2
        auto_backfill_raw = (_env("PCCA_AUTO_BACKFILL", "true") or "true").strip().lower()
        auto_backfill_embeddings = auto_backfill_raw in {"1", "true", "yes", "on"}
        try:
            youtube_transcript_backfill_concurrency = int(
                _env("PCCA_YOUTUBE_TRANSCRIPT_BACKFILL_CONCURRENCY", "2") or "2"
            )
        except ValueError:
            youtube_transcript_backfill_concurrency = 2
        return cls(
            timezone=_env("PCCA_TIMEZONE", "UTC") or "UTC",
            nightly_cron=_env("PCCA_NIGHTLY_CRON", "0 1 * * *") or "0 1 * * *",
            morning_cron=_env("PCCA_MORNING_CRON", "30 8 * * *") or "30 8 * * *",
            digest_auto_send=digest_auto_send,
            data_dir=data_dir,
            db_path=db_path,
            browser_profiles_dir=browser_profiles_dir,
            browser_headless=browser_headless,
            browser_headful_platforms=browser_headful_platforms,
            browser_channel=browser_channel,
            ollama_enabled=ollama_enabled,
            ollama_base_url=_env("PCCA_OLLAMA_BASE_URL", "http://localhost:11434") or "http://localhost:11434",
            ollama_model=_env("PCCA_OLLAMA_MODEL", "qwen2.5:7b") or "qwen2.5:7b",
            model_router_timeout_seconds=max(1, model_router_timeout_seconds),
            telegram_bot_token=_env("PCCA_TELEGRAM_BOT_TOKEN"),
            session_refresh_enabled=session_refresh_enabled,
            session_refresh_cooldown_seconds=max(0, session_refresh_cooldown_seconds),
            session_refresh_browser=session_refresh_browser,
            platform_circuit_threshold=max(1, platform_circuit_threshold),
            platform_empty_threshold=max(1, platform_empty_threshold),
            scorer=scorer,
            embedding_model=_env("PCCA_EMBEDDING_MODEL", "nomic-embed-text:v1.5") or "nomic-embed-text:v1.5",
            embedding_timeout_seconds=max(1, embedding_timeout_seconds),
            embedding_max_chars=max(500, embedding_max_chars),
            embedding_backfill_concurrency=max(1, embedding_backfill_concurrency),
            auto_backfill_embeddings=auto_backfill_embeddings,
            youtube_transcript_backfill_concurrency=max(1, youtube_transcript_backfill_concurrency),
        )

    def ensure_dirs(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.browser_profiles_dir.mkdir(parents=True, exist_ok=True)
