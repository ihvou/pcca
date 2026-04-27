from __future__ import annotations

from pathlib import Path

import pytest

from pcca.config import Settings
from pcca.db import Database
from pcca.repositories.onboarding import OnboardingRepository
from pcca.repositories.routing import RoutingRepository
from pcca.repositories.subjects import SubjectRepository
from pcca.services.desktop_command_service import (
    DesktopCommandService,
    cron_to_digest_time,
    digest_time_to_cron,
    evaluate_smoke_result,
)
from pcca.services.routing_service import RoutingService


def make_settings(tmp_path: Path, token: str | None = None) -> Settings:
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
        telegram_bot_token=token,
    )


def test_digest_time_cron_roundtrip() -> None:
    assert digest_time_to_cron("09:05") == "5 9 * * *"
    assert cron_to_digest_time("5 9 * * *") == "09:05"
    assert cron_to_digest_time("bad") == "08:30"


def test_smoke_evaluation_requires_items_and_delivery() -> None:
    no_items = evaluate_smoke_result({"items_collected": 0}, {"deliveries_sent": 1})
    assert no_items.ok is False
    assert "0 items" in no_items.message

    no_delivery = evaluate_smoke_result({"items_collected": 2}, {"deliveries_sent": 0})
    assert no_delivery.ok is False
    assert "not delivered" in no_delivery.message

    ok = evaluate_smoke_result({"items_collected": 2}, {"deliveries_sent": 1})
    assert ok.ok is True
    assert ok.items_collected == 2
    assert ok.deliveries_sent == 1


@pytest.mark.asyncio
async def test_desktop_service_hides_token_and_persists_runtime_settings(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    service = DesktopCommandService()

    result = await service.save_runtime_settings(
        token="bot123:super-secret-token",
        timezone="Europe/Kyiv",
        digest_time="07:45",
    )

    assert result.ok is True
    assert "super-secret-token" not in result.message
    env_text = (tmp_path / ".env").read_text(encoding="utf-8")
    assert "PCCA_TELEGRAM_BOT_TOKEN=bot123:super-secret-token" in env_text
    state = await service.get_state()
    assert state["settings"]["telegram_token_configured"] is True
    assert "super-secret-token" not in str(state)
    assert state["onboarding"]["current_step"] == "runtime_configured"
    assert state["onboarding"]["timezone"] == "Europe/Kyiv"


@pytest.mark.asyncio
async def test_desktop_service_confirms_staged_sources_without_marking_complete(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    service = DesktopCommandService(settings_factory=lambda: settings)
    await service.init_db()

    db = Database(path=settings.db_path)
    await db.connect()
    await db.initialize()
    assert db.conn is not None
    try:
        await RoutingService(
            routing_repo=RoutingRepository(conn=db.conn),
            subject_repo=SubjectRepository(conn=db.conn),
        ).register_chat(chat_id=123, title="Personal")
        await OnboardingRepository(conn=db.conn).stage_source(
            platform="youtube",
            account_or_channel_id="@openai",
            display_name="OpenAI",
            raw_source="@openai",
        )
    finally:
        await db.close()

    result = await service.confirm_staged_sources(
        subject="Vibe Coding",
        include_terms=["claude code"],
        exclude_terms=["biography"],
        high_quality_examples="practical releases",
    )

    assert result.ok is True
    assert result.data["confirmed_sources"] == 1
    assert result.data["new_routes"] == 1

    db = Database(path=settings.db_path)
    await db.connect()
    await db.initialize()
    assert db.conn is not None
    try:
        state = await OnboardingRepository(conn=db.conn).get_state()
        staged = await OnboardingRepository(conn=db.conn).list_sources(status=None)
        subject_row = await (
            await db.conn.execute("SELECT name FROM subjects WHERE name = 'Vibe Coding'")
        ).fetchone()
    finally:
        await db.close()

    assert state.current_step == "subject_confirmed"
    assert state.completed_at is None
    assert [row.status for row in staged] == ["confirmed"]
    assert subject_row is not None
