from __future__ import annotations

import logging
import os
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


def configure_logging(level: int = logging.INFO) -> None:
    raw_level = os.getenv("PCCA_LOG_LEVEL")
    if raw_level:
        level = getattr(logging, raw_level.strip().upper(), level)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    redaction_filter = SecretRedactionFilter()
    root = logging.getLogger()
    root.addFilter(redaction_filter)
    for handler in root.handlers:
        handler.addFilter(redaction_filter)
