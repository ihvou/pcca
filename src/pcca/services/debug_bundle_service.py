from __future__ import annotations

import os
import json
import sqlite3
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pcca.config import Settings
from pcca.observability import safe_value


def create_debug_bundle(settings: Settings, output: Path | None = None) -> Path:
    """Create a local redacted debug bundle.

    The bundle intentionally includes screenshots/metadata under `.pcca/debug`
    because they are the fastest way to debug browser extraction failures.
    It does not include raw browser profile stores or raw item text.
    """
    settings.ensure_dirs()
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    bundle_path = output or (settings.data_dir / "debug" / f"pcca-debug-{timestamp}.zip")
    bundle_path.parent.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(bundle_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        _add_runtime_summary(zf, settings)
        _add_db_summary(zf, settings.db_path)
        _add_log_files(zf, settings.data_dir)
        _add_debug_artifacts(zf, settings.data_dir)

    return bundle_path


def _add_runtime_summary(zf: zipfile.ZipFile, settings: Settings) -> None:
    payload = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "cwd": str(Path.cwd()),
        "settings": {
            "timezone": settings.timezone,
            "nightly_cron": settings.nightly_cron,
            "morning_cron": settings.morning_cron,
            "data_dir": str(settings.data_dir),
            "db_path": str(settings.db_path),
            "browser_profiles_dir": str(settings.browser_profiles_dir),
            "browser_headless": settings.browser_headless,
            "browser_headful_platforms": sorted(settings.browser_headful_platforms),
            "browser_channel": settings.browser_channel,
            "session_refresh_enabled": settings.session_refresh_enabled,
            "session_refresh_cooldown_seconds": settings.session_refresh_cooldown_seconds,
            "session_refresh_browser": settings.session_refresh_browser,
            "platform_circuit_threshold": settings.platform_circuit_threshold,
            "platform_empty_threshold": settings.platform_empty_threshold,
            "scorer": settings.scorer,
            "embedding_model": settings.embedding_model,
            "embedding_timeout_seconds": settings.embedding_timeout_seconds,
            "ollama_enabled": settings.ollama_enabled,
            "ollama_base_url": settings.ollama_base_url,
            "ollama_model": settings.ollama_model,
            "telegram_bot_token": "<configured>" if settings.telegram_bot_token else None,
        },
        "env": {
            key: safe_value(value, key=key)
            for key, value in sorted(os.environ.items())
            if key.startswith("PCCA_")
        },
    }
    zf.writestr("runtime_summary.json", _json_bytes(payload))


def _add_db_summary(zf: zipfile.ZipFile, db_path: Path) -> None:
    if not db_path.exists():
        zf.writestr("db_summary.json", _json_bytes({"exists": False, "path": str(db_path)}))
        return
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        payload = {
            "exists": True,
            "path": str(db_path),
            "run_logs": _query_rows(
                conn,
                "SELECT id, run_type, started_at, ended_at, status, stats_json, metadata_json FROM run_logs ORDER BY id DESC LIMIT 50",
            ),
            "sources": _query_rows(
                conn,
                "SELECT id, platform, account_or_channel_id, display_name, follow_state, last_crawled_at FROM sources ORDER BY id",
            ),
            "onboarding_imported_sources": _query_rows(
                conn,
                "SELECT id, platform, account_or_channel_id, display_name, status FROM onboarding_imported_sources ORDER BY id DESC LIMIT 100",
            ),
            "onboarding_state": _query_rows(
                conn,
                "SELECT current_step, timezone, digest_time, telegram_verified, subject_name, completed_at FROM onboarding_state",
            ),
            "counts": {
                table: _count(conn, table)
                for table in [
                    "subjects",
                    "sources",
                    "items",
                    "item_scores",
                    "digests",
                    "digest_deliveries",
                    "feedback_events",
                ]
            },
        }
    finally:
        conn.close()
    zf.writestr("db_summary.json", _json_bytes(payload))


def _add_log_files(zf: zipfile.ZipFile, data_dir: Path) -> None:
    logs_dir = data_dir / "logs"
    if not logs_dir.exists():
        return
    for path in sorted(logs_dir.glob("pcca.log*")):
        if path.is_file():
            content = path.read_text(encoding="utf-8", errors="replace")
            zf.writestr(f"logs/{path.name}", str(safe_value(content, max_chars=2_000_000)))


def _add_debug_artifacts(zf: zipfile.ZipFile, data_dir: Path) -> None:
    debug_dir = data_dir / "debug"
    if not debug_dir.exists():
        return
    for path in sorted(debug_dir.rglob("*")):
        if path.is_file() and path.suffix.lower() in {".json", ".png", ".txt", ".log"}:
            zf.write(path, f"debug/{path.relative_to(debug_dir)}")


def _query_rows(conn: sqlite3.Connection, sql: str) -> list[dict[str, Any]]:
    try:
        rows = conn.execute(sql).fetchall()
    except sqlite3.Error as exc:
        return [{"error": str(exc), "sql": sql}]
    return [dict(row) for row in rows]


def _count(conn: sqlite3.Connection, table: str) -> int | str:
    try:
        row = conn.execute(f"SELECT COUNT(*) AS count FROM {table}").fetchone()
        return int(row["count"])
    except sqlite3.Error as exc:
        return f"error: {exc}"


def _json_bytes(payload: dict[str, Any]) -> str:
    return json.dumps(safe_value(payload), indent=2, sort_keys=True, default=str)
