import logging

from pcca.logging_utils import SecretRedactionFilter, _redact_secrets


def test_redacts_telegram_bot_tokens() -> None:
    raw = "https://api.telegram.org/bot123456:ABC_def-ghi/getUpdates"

    assert _redact_secrets(raw) == "https://api.telegram.org/bot<redacted>/getUpdates"


def test_redaction_filter_redacts_record_args() -> None:
    record = logging.LogRecord(
        name="httpx",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="HTTP Request: %s",
        args=("https://api.telegram.org/bot123456:ABC_def-ghi/getUpdates",),
        exc_info=None,
    )

    assert SecretRedactionFilter().filter(record) is True
    assert record.getMessage() == "HTTP Request: https://api.telegram.org/bot<redacted>/getUpdates"
