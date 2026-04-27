from __future__ import annotations

import json
import re
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pcca.logging_utils import _redact_secrets


SECRET_KEY_RE = re.compile(r"(token|cookie|secret|password|auth|authorization)", re.IGNORECASE)
QUERY_SECRET_RE = re.compile(r"([?&](?:token|auth|key|code|state|session|access_token)=)[^&]+", re.IGNORECASE)
SAFE_MAX_CHARS = 500


def utc_compact_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def new_action_id(prefix: str) -> str:
    normalized = slugify(prefix) or "action"
    return f"{normalized}-{utc_compact_timestamp()}-{uuid.uuid4().hex[:8]}"


def slugify(value: str, *, max_len: int = 64) -> str:
    out = re.sub(r"[^a-zA-Z0-9_.-]+", "-", value.strip().lower()).strip("-")
    return out[:max_len] or "debug"


def sanitize_url(url: str | None) -> str | None:
    if url is None:
        return None
    return QUERY_SECRET_RE.sub(r"\1<redacted>", _truncate(str(url), SAFE_MAX_CHARS))


def safe_value(value: Any, *, key: str | None = None, max_chars: int = SAFE_MAX_CHARS) -> Any:
    if key and SECRET_KEY_RE.search(key):
        return "<redacted>" if value not in (None, "") else value
    if isinstance(value, str):
        return _truncate(_redact_secrets(QUERY_SECRET_RE.sub(r"\1<redacted>", value)), max_chars)
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    if isinstance(value, dict):
        return {str(k): safe_value(v, key=str(k), max_chars=max_chars) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        rows = list(value)
        summarized = [safe_value(v, max_chars=max_chars) for v in rows[:10]]
        if len(rows) > 10:
            summarized.append(f"... {len(rows) - 10} more")
        return summarized
    return _truncate(_redact_secrets(str(value)), max_chars)


def summarize_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    if not payload:
        return {}
    return {str(key): safe_value(value, key=str(key), max_chars=200) for key, value in payload.items()}


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")


def monotonic_ms_since(started_at: float) -> int:
    return int((time.monotonic() - started_at) * 1000)


@asynccontextmanager
async def log_timed(logger, message: str, **fields):
    started_at = time.monotonic()
    logger.info("%s started %s", message, _format_fields(fields))
    try:
        yield
    except Exception:
        logger.exception("%s failed duration_ms=%d %s", message, monotonic_ms_since(started_at), _format_fields(fields))
        raise
    else:
        logger.info("%s finished duration_ms=%d %s", message, monotonic_ms_since(started_at), _format_fields(fields))


def _format_fields(fields: dict[str, Any]) -> str:
    safe = summarize_payload(fields)
    return " ".join(f"{key}={value!r}" for key, value in safe.items())


def _truncate(value: str, max_chars: int) -> str:
    if len(value) <= max_chars:
        return value
    return value[: max_chars - 20] + "...<truncated>"
