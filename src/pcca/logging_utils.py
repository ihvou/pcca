from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import re


TOKEN_RE = re.compile(r"bot\d+:[A-Za-z0-9_-]+")


def _redact_secrets(value):
    if isinstance(value, str):
        return TOKEN_RE.sub("bot<redacted>", value)
    return value


class SecretRedactionFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = _redact_secrets(record.msg)
        if isinstance(record.args, tuple):
            record.args = tuple(_redact_secrets(str(arg)) if "bot" in str(arg) else arg for arg in record.args)
        elif isinstance(record.args, dict):
            record.args = {
                key: _redact_secrets(str(value)) if "bot" in str(value) else value
                for key, value in record.args.items()
            }
        return True


def _load_dotenv_for_logging(path: Path = Path(".env")) -> None:
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
        os.environ[key] = value.strip().strip("\"'")


def _configured_log_file() -> Path | None:
    raw_log_file = os.getenv("PCCA_LOG_FILE")
    if raw_log_file and raw_log_file.strip().lower() in {"0", "false", "off", "none"}:
        return None
    if raw_log_file and raw_log_file.strip():
        return Path(raw_log_file.strip())
    data_dir = Path(os.getenv("PCCA_DATA_DIR") or ".pcca")
    return data_dir / "logs" / "pcca.log"


def _has_filter(target: logging.Filterer, filter_type: type[logging.Filter]) -> bool:
    return any(isinstance(existing, filter_type) for existing in target.filters)


def configure_logging(level: int = logging.INFO) -> None:
    _load_dotenv_for_logging()
    raw_level = os.getenv("PCCA_LOG_LEVEL")
    if raw_level:
        level = getattr(logging, raw_level.strip().upper(), level)
    formatter = logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
    root = logging.getLogger()
    root.setLevel(level)

    if not root.handlers:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        root.addHandler(stream_handler)

    redaction_filter = SecretRedactionFilter()
    if not _has_filter(root, SecretRedactionFilter):
        root.addFilter(redaction_filter)
    for handler in root.handlers:
        if handler.formatter is None:
            handler.setFormatter(formatter)
        if not _has_filter(handler, SecretRedactionFilter):
            handler.addFilter(redaction_filter)

    log_file = _configured_log_file()
    if log_file is None:
        return
    log_file.parent.mkdir(parents=True, exist_ok=True)
    resolved_log_file = str(log_file.resolve())
    for handler in root.handlers:
        if getattr(handler, "_pcca_log_file", None) == resolved_log_file:
            handler.setLevel(level)
            return

    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=1_000_000,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    file_handler.addFilter(redaction_filter)
    file_handler._pcca_log_file = resolved_log_file  # type: ignore[attr-defined]
    root.addHandler(file_handler)
