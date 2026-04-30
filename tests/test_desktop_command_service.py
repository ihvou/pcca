from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from pcca.config import Settings
from pcca.db import Database
from pcca.repositories.onboarding import OnboardingRepository
from pcca.repositories.routing import RoutingRepository
from pcca.repositories.sources import SourceRepository
from pcca.repositories.subject_drafts import SubjectDraftRepository
from pcca.repositories.subjects import SubjectRepository
from pcca.services.desktop_command_service import (
    DesktopCommandService,
    cron_to_digest_time,
    digest_time_to_cron,
    evaluate_smoke_result,
)
from pcca.services.routing_service import RoutingService
from pcca.services.source_service import SourceService


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
async def test_desktop_service_blank_token_preserves_existing_env_token(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text(
        "PCCA_TELEGRAM_BOT_TOKEN=bot123:existing-token\nPCCA_TIMEZONE=UTC\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("PCCA_TELEGRAM_BOT_TOKEN", raising=False)
    service = DesktopCommandService()

    result = await service.save_runtime_settings(token="", timezone="Europe/Kyiv", digest_time="06:15")

    assert result.ok is True
    env_text = (tmp_path / ".env").read_text(encoding="utf-8")
    assert "PCCA_TELEGRAM_BOT_TOKEN=bot123:existing-token" in env_text
    assert "PCCA_MORNING_CRON=15 6 * * *" in env_text
    state = await service.get_state()
    assert state["settings"]["telegram_token_configured"] is True


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
    assert result.data["monitored_sources"] == 1
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
        source_row = await (
            await db.conn.execute("SELECT is_monitored FROM sources WHERE account_or_channel_id = '@openai'")
        ).fetchone()
    finally:
        await db.close()

    assert state.current_step == "subject_confirmed"
    assert state.completed_at is None
    assert [row.status for row in staged] == ["confirmed"]
    assert subject_row is not None
    assert source_row is not None
    assert int(source_row["is_monitored"]) == 1


@pytest.mark.asyncio
async def test_desktop_service_monitors_staged_sources_without_subject(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    service = DesktopCommandService(settings_factory=lambda: settings)
    await service.init_db()

    db = Database(path=settings.db_path)
    await db.connect()
    await db.initialize()
    assert db.conn is not None
    try:
        await OnboardingRepository(conn=db.conn).stage_source(
            platform="youtube",
            account_or_channel_id="@openai",
            display_name="OpenAI",
            raw_source="@openai",
        )
    finally:
        await db.close()

    result = await service.monitor_staged_sources()

    assert result.ok is True
    assert result.data["monitored_sources"] == 1
    state = await service.get_state()
    assert state["pending_staged_count"] == 0
    assert state["staged_counts"] == {}

    db = Database(path=settings.db_path)
    await db.connect()
    await db.initialize()
    assert db.conn is not None
    try:
        source_row = await (
            await db.conn.execute("SELECT is_monitored FROM sources WHERE account_or_channel_id = '@openai'")
        ).fetchone()
    finally:
        await db.close()

    assert source_row is not None
    assert int(source_row["is_monitored"]) == 1


@pytest.mark.asyncio
async def test_desktop_subject_draft_requires_actionable_rules(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    service = DesktopCommandService(settings_factory=lambda: settings)
    await service.init_db()

    thin = await service.draft_subject(text="Create subject: AI jobs")
    assert thin.ok is True
    assert thin.data["actionable"] is False

    with pytest.raises(ValueError):
        await service.confirm_subject_draft()

    rich = await service.draft_subject(
        text=(
            "Track AI impact on IT jobs. Include credible labor market data, concrete company hiring changes, "
            "developer productivity evidence. Avoid generic AI hype and unrelated finance news."
        )
    )
    assert rich.data["actionable"] is True

    saved = await service.confirm_subject_draft()
    assert saved.ok is True

    state = await service.get_state()
    assert state["subjects"][0]["name"] == "AI jobs"
    pref = state["subject_preferences"][str(state["subjects"][0]["id"])]
    assert pref["include_terms"]


@pytest.mark.asyncio
async def test_desktop_subject_refinement_updates_existing_subject(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    service = DesktopCommandService(settings_factory=lambda: settings)
    await service.init_db()

    db = Database(path=settings.db_path)
    await db.connect()
    await db.initialize()
    assert db.conn is not None
    try:
        subject_repo = SubjectRepository(conn=db.conn)
        subject = await SubjectRepository(conn=db.conn).create("Vibe Coding", include_terms=["claude code"])
    finally:
        await db.close()

    draft = await service.draft_subject(
        subject_id=subject.id,
        text="Include practical release notes and avoid generic motivation.",
    )
    assert draft.data["actionable"] is True
    saved = await service.confirm_subject_draft()
    assert saved.ok is True

    state = await service.get_state()
    assert len(state["subjects"]) == 1
    pref = state["subject_preferences"][str(subject.id)]
    assert "claude code" in pref["include_terms"]
    assert "practical release notes" in pref["include_terms"]
    assert "generic motivation" in pref["exclude_terms"]


@pytest.mark.asyncio
async def test_desktop_rebuild_subject_rules_replaces_preferences(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    service = DesktopCommandService(settings_factory=lambda: settings)
    await service.init_db()

    db = Database(path=settings.db_path)
    await db.connect()
    await db.initialize()
    assert db.conn is not None
    try:
        subject = await SubjectRepository(conn=db.conn).create(
            "Ukraine War News",
            include_terms=["reputable sources", "high quality analytics"],
        )
    finally:
        await db.close()

    result = await service.rebuild_subject_rules(
        subject_id=subject.id,
        text="Subject: Ukraine War News. Include ukraine, russia, kyiv, war. Avoid propaganda.",
    )

    assert result.ok is True
    assert result.data["version"] == 2
    include_terms = result.data["include_terms"]
    assert "ukraine" in include_terms
    assert "russia" in include_terms
    assert "reputable sources" not in include_terms


@pytest.mark.asyncio
async def test_desktop_can_confirm_telegram_draft_and_assign_route(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    service = DesktopCommandService(settings_factory=lambda: settings)
    await service.init_db()

    db = Database(path=settings.db_path)
    await db.connect()
    await db.initialize()
    assert db.conn is not None
    try:
        subject_repo = SubjectRepository(conn=db.conn)
        routing = RoutingService(routing_repo=RoutingRepository(conn=db.conn), subject_repo=subject_repo)
        await routing.register_chat(chat_id=555, title="Telegram Group")
        await SubjectDraftRepository(conn=db.conn).upsert(
            chat_id=555,
            title="Agentic PM",
            description_text="Track agentic PM workflows. Include practical product operations. Avoid generic PM slogans.",
            include_terms=["agentic pm workflows", "product operations"],
            exclude_terms=["generic pm slogans"],
            quality_notes=None,
            last_user_message="Create subject",
        )
    finally:
        await db.close()

    state = await service.get_state()
    assert state["subject_drafts"][0]["chat_id"] == 555

    saved = await service.confirm_subject_draft(chat_id=555)
    assert saved.ok is True
    state = await service.get_state()
    assert state["subjects"][0]["name"] == "Agentic PM"
    assert state["routes"][0]["chat_id"] == 555

    db = Database(path=settings.db_path)
    await db.connect()
    await db.initialize()
    assert db.conn is not None
    try:
        routing = RoutingService(
            routing_repo=RoutingRepository(conn=db.conn),
            subject_repo=SubjectRepository(conn=db.conn),
        )
        await routing.register_chat(chat_id=777, title="Second Group")
    finally:
        await db.close()

    reassigned = await service.reassign_subject_route(subject_id=state["subjects"][0]["id"], chat_id=777)
    assert reassigned.ok is True
    state = await service.get_state()
    assert state["routes"][0]["chat_id"] == 777


@pytest.mark.asyncio
async def test_desktop_state_surfaces_subject_source_overrides(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    service = DesktopCommandService(settings_factory=lambda: settings)
    await service.init_db()

    db = Database(path=settings.db_path)
    await db.connect()
    await db.initialize()
    assert db.conn is not None
    try:
        subject_repo = SubjectRepository(conn=db.conn)
        subject = await subject_repo.create("Vibe Coding", include_terms=["vibe coding"])
        source_service = SourceService(
            source_repo=SourceRepository(conn=db.conn),
            subject_repo=subject_repo,
        )
        await source_service.add_source_to_subject(
            subject_name=subject.name,
            platform="youtube",
            account_or_channel_id="@noisy",
            display_name="Noisy Channel",
        )
        await source_service.remove_source_from_subject(
            subject_name=subject.name,
            platform="youtube",
            account_or_channel_id="@noisy",
        )
    finally:
        await db.close()

    state = await service.get_state()
    overrides = state["subject_source_overrides"][str(subject.id)]
    assert overrides[0]["display_name"] == "Noisy Channel"
    assert overrides[0]["status"] == "inactive"


@pytest.mark.asyncio
async def test_desktop_state_surfaces_sources_needing_reauth(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    service = DesktopCommandService(settings_factory=lambda: settings)
    await service.init_db()

    db = Database(path=settings.db_path)
    await db.connect()
    await db.initialize()
    assert db.conn is not None
    try:
        subject_repo = SubjectRepository(conn=db.conn)
        source_service = SourceService(
            source_repo=SourceRepository(conn=db.conn),
            subject_repo=subject_repo,
        )
        await SubjectRepository(conn=db.conn).create("Vibe Coding", include_terms=["vibe coding"])
        await source_service.add_source_to_subject(
            subject_name="Vibe Coding",
            platform="youtube",
            account_or_channel_id="@openai",
            display_name="OpenAI",
        )
        source = await SourceRepository(conn=db.conn).get_by_identity(
            platform="youtube",
            account_or_channel_id="@openai",
        )
        assert source is not None
        await SourceRepository(conn=db.conn).mark_needs_reauth(source.id)
    finally:
        await db.close()

    state = await service.get_state()

    assert state["reauth_sources"][0]["platform"] == "youtube"
    assert state["reauth_sources"][0]["account_or_channel_id"] == "@openai"


@pytest.mark.asyncio
async def test_desktop_read_content_passes_platform_scope_to_running_agent(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    service = DesktopCommandService(settings_factory=lambda: settings)

    class FakeOrchestrator:
        def __init__(self) -> None:
            self.platforms: list[str | None] = []

        async def run_nightly_collection(self, *, platform: str | None = None):
            self.platforms.append(platform)
            return {"platform_filter": platform, "items_collected": 1}

    fake_orchestrator = FakeOrchestrator()
    service._agent_app = type("FakeApp", (), {"pipeline_orchestrator": fake_orchestrator})()  # type: ignore[assignment]
    service._agent_task = asyncio.create_task(asyncio.sleep(60))
    try:
        scoped = await service.read_content(platform="youtube")
        unscoped = await service.read_content()
    finally:
        service._agent_task.cancel()
        try:
            await service._agent_task
        except asyncio.CancelledError:
            pass

    assert scoped.ok is True
    assert scoped.data["platform"] == "youtube"
    assert unscoped.data["platform"] is None
    assert fake_orchestrator.platforms == ["youtube", None]


@pytest.mark.asyncio
async def test_desktop_read_content_guard_rejects_concurrent_runs(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    service = DesktopCommandService(settings_factory=lambda: settings)
    entered = asyncio.Event()
    release = asyncio.Event()

    class SlowOrchestrator:
        async def run_nightly_collection(self, *, platform: str | None = None):
            entered.set()
            await release.wait()
            return {"platform_filter": platform, "items_collected": 1}

    service._agent_app = type("FakeApp", (), {"pipeline_orchestrator": SlowOrchestrator()})()  # type: ignore[assignment]
    service._agent_task = asyncio.create_task(asyncio.sleep(60))
    first = asyncio.create_task(service.read_content(platform="youtube"))
    try:
        await asyncio.wait_for(entered.wait(), timeout=1)
        second = await service.read_content(platform="youtube")
        assert second.ok is False
        assert second.data["already_running"] is True
        assert service.inflight_actions()[0]["key"] == "read_content"
        release.set()
        first_result = await first
        assert first_result.ok is True
        assert service.inflight_actions() == []
    finally:
        release.set()
        if not first.done():
            first.cancel()
        service._agent_task.cancel()
        try:
            await service._agent_task
        except asyncio.CancelledError:
            pass
