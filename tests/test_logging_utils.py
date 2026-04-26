import logging

from pcca.logging_utils import SecretRedactionFilter, _redact_secrets, configure_logging


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


def test_configure_logging_writes_default_debug_file(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text(
        "PCCA_DATA_DIR=.runtime\nPCCA_LOG_LEVEL=DEBUG\n",
        encoding="utf-8",
    )

    configure_logging()
    logging.getLogger("pcca.test").debug("secret %s", "bot123456:ABC_def-ghi")
    for handler in logging.getLogger().handlers:
        handler.flush()

    content = (tmp_path / ".runtime" / "logs" / "pcca.log").read_text(encoding="utf-8")
    assert "secret bot<redacted>" in content
