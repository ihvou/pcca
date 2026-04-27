from __future__ import annotations

import zipfile
from pathlib import Path

import pytest

from pcca.config import Settings
from pcca.db import Database
from pcca.repositories.run_logs import RunLogRepository
from pcca.services.debug_bundle_service import create_debug_bundle


def make_settings(tmp_path: Path) -> Settings:
    data_dir = tmp_path / ".pcca"
    return Settings(
        timezone="UTC",
        nightly_cron="0 1 * * *",
        morning_cron="30 8 * * *",
        digest_auto_send=False,
        data_dir=data_dir,
        db_path=data_dir / "pcca.db",
        browser_profiles_dir=data_dir / "browser_profiles",
        browser_headless=True,
        browser_headful_platforms={"x", "linkedin"},
        browser_channel="chrome",
        ollama_enabled=False,
        ollama_base_url="http://localhost:11434",
        ollama_model="qwen2.5:7b",
        telegram_bot_token="bot123:super-secret",
    )


@pytest.mark.asyncio
async def test_debug_bundle_contains_redacted_runtime_and_db_summary(tmp_path: Path, monkeypatch) -> None:
    settings = make_settings(tmp_path)
    monkeypatch.setenv("PCCA_TELEGRAM_BOT_TOKEN", "bot123:super-secret")
    settings.ensure_dirs()
    db = Database(path=settings.db_path)
    await db.connect()
    await db.initialize()
    assert db.conn is not None
    try:
        repo = RunLogRepository(conn=db.conn)
        run_id = await repo.start_run("nightly_collection")
        await repo.finish_run(run_id, "success", {"items_collected": 1})
    finally:
        await db.close()

    (settings.data_dir / "logs").mkdir(parents=True)
    (settings.data_dir / "logs" / "pcca.log").write_text("hello bot123:super-secret\n", encoding="utf-8")

    bundle = create_debug_bundle(settings)

    assert bundle.exists()
    with zipfile.ZipFile(bundle) as zf:
        names = set(zf.namelist())
        assert "runtime_summary.json" in names
        assert "db_summary.json" in names
        assert "logs/pcca.log" in names
        runtime_summary = zf.read("runtime_summary.json").decode("utf-8")
        assert "super-secret" not in runtime_summary
        bundled_log = zf.read("logs/pcca.log").decode("utf-8")
        assert "super-secret" not in bundled_log
        assert "bot<redacted>" in bundled_log
        db_summary = zf.read("db_summary.json").decode("utf-8")
        assert "nightly_collection" in db_summary
