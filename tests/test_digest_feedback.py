from __future__ import annotations

import json
from pathlib import Path

import pytest

from pcca.collectors.base import CollectedItem
from pcca.db import Database
from pcca.digest_renderer import BriefButtonPayload, BriefPayload, DeliveryPayload
from pcca.repositories.digests import DigestRepository
from pcca.repositories.feedback import FeedbackRepository
from pcca.repositories.item_scores import ItemScoreRepository
from pcca.repositories.items import ItemRepository
from pcca.repositories.routing import RoutingRepository
from pcca.repositories.run_logs import RunLogRepository
from pcca.repositories.subjects import SubjectRepository
from pcca.scheduler import AgentScheduler, JobRunner
from pcca.services.feedback_service import FeedbackService
from pcca.services.routing_service import RoutingService
from pcca.services.subject_service import SubjectService


class FakeTelegramService:
    application = object()

    def __init__(self) -> None:
        self.sent_messages: list[dict] = []

    async def send_brief_message(
        self,
        *,
        chat_id: int,
        subject_name: str,
        brief: BriefPayload,
        footer: str | None = None,
        thread_id: int | None = None,
    ) -> int:
        self.sent_messages.append(
            {
                "chat_id": chat_id,
                "subject_name": subject_name,
                "brief": brief,
                "footer": footer,
                "thread_id": thread_id,
            }
        )
        return len(self.sent_messages)

    async def send_no_briefs_message(
        self,
        *,
        chat_id: int,
        subject_name: str,
        footer: str | None = None,
        thread_id: int | None = None,
    ) -> int:
        self.sent_messages.append(
            {
                "chat_id": chat_id,
                "subject_name": subject_name,
                "brief": None,
                "footer": footer,
                "thread_id": thread_id,
            }
        )
        return len(self.sent_messages)


class HeadlineOnlyRenderer:
    name = "headline_only"

    async def render(self, *, subject, ranked_items, context) -> DeliveryPayload:
        if not ranked_items:
            return DeliveryPayload(renderer_name=self.name, briefs=[])
        candidate = ranked_items[0]
        token = await context.create_button_token(
            candidate,
            "more like this",
            label="More like this",
            kind="feedback",
        )
        brief = BriefPayload(
            item_id=candidate.item_id,
            rank=1,
            reason_selected="headline renderer",
            short_text=f"{subject.name}: {candidate.title_or_text.splitlines()[0]}",
            full_text=f"{subject.name}: {candidate.title_or_text}",
            buttons=[
                BriefButtonPayload(
                    label="More like this",
                    token=token,
                    text_macro="more like this",
                )
            ],
        )
        return DeliveryPayload(renderer_name=self.name, briefs=[brief])


class FakePipelineOrchestrator:
    def __init__(self) -> None:
        self.rescore_calls = 0
        self.rescore_subject_ids: list[set[int] | None] = []

    async def rescore_existing_items(self, *, limit=None, subject_ids=None, progress_callback=None):
        _ = progress_callback
        self.rescore_calls += 1
        self.rescore_subject_ids.append(subject_ids)
        assert limit is None
        return {"items_scored": 1}


class FakeSubjectService:
    async def list_subjects(self):
        return []


async def _seed_subject_with_route(db: Database) -> tuple[SubjectService, RoutingService, object]:
    assert db.conn is not None
    subject_repo = SubjectRepository(conn=db.conn)
    subject_service = SubjectService(repository=subject_repo)
    subject = await subject_service.create_subject("Agentic PM", include_terms=["agentic pm"])
    routing_service = RoutingService(routing_repo=RoutingRepository(conn=db.conn), subject_repo=subject_repo)
    await routing_service.register_chat(chat_id=123, title="Test")
    await routing_service.link_subject(subject_name="Agentic PM", chat_id=123)
    return subject_service, routing_service, subject


async def _seed_item_score(
    db: Database,
    *,
    subject_id: int,
    external_id: str,
    text: str,
    url: str,
    score: float,
    rationale: str,
) -> int:
    assert db.conn is not None
    item_repo = ItemRepository(conn=db.conn)
    item_stats = await item_repo.upsert_many(
        [
            CollectedItem(
                platform="rss",
                external_id=external_id,
                author="Boris Cherny",
                url=url,
                text=text,
                transcript_text=None,
                published_at="2026-04-24T09:00:00",
                metadata={},
            )
        ]
    )
    item_id = item_stats["item_ids"][0]
    await ItemScoreRepository(conn=db.conn).upsert_score(
        item_id=item_id,
        subject_id=subject_id,
        pass1_score=score,
        pass2_score=score,
        practicality_score=score,
        novelty_score=score,
        trust_score=score,
        noise_penalty=0.0,
        final_score=score,
        rationale=rationale,
    )
    return item_id


@pytest.mark.asyncio
async def test_t154_update_briefs_skips_collection_after_recent_success(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    run_log_repo = RunLogRepository(conn=db.conn)
    run_id = await run_log_repo.start_run("nightly_collection")
    await run_log_repo.finish_run(
        run_id,
        "success",
        {"items_collected": 1700, "items_inserted": 87, "items_updated": 1613},
    )
    runner = JobRunner(
        subject_service=FakeSubjectService(),  # type: ignore[arg-type]
        run_log_repo=run_log_repo,
    )

    async def forbidden_collection(*, score=True, progress_callback=None):
        _ = score, progress_callback
        raise AssertionError("recent successful nightly should skip duplicate collection")

    async def fast_briefs(*, subject_ids=None, progress_callback=None):
        _ = subject_ids, progress_callback
        return {"briefs_sent": 3, "subjects_with_routes": 2}

    runner.run_nightly_collection = forbidden_collection  # type: ignore[method-assign]
    runner.run_smart_briefs = fast_briefs  # type: ignore[method-assign]

    stats = await runner.update_briefs()

    assert stats["collection"]["collection_skipped_recent"] is True
    assert stats["collection"]["recent_nightly_collection"]["id"] == run_id
    assert stats["briefs"]["briefs_sent"] == 3

    await db.close()


@pytest.mark.asyncio
async def test_t155_scheduled_nightly_completion_sends_summary() -> None:
    class FakePipeline:
        async def run_nightly_collection(self, *, score=True, progress_callback=None):
            _ = score, progress_callback
            return {
                "items_collected": 12,
                "items_inserted": 4,
                "items_updated": 8,
                "subjects_active": 2,
                "items_scored": 6,
            }

    class FakeTelegramSummary:
        def __init__(self) -> None:
            self.calls: list[dict] = []

        async def send_nightly_completion_summary(self, *, stats, status="success", error_text=None):
            self.calls.append({"stats": stats, "status": status, "error_text": error_text})

    telegram = FakeTelegramSummary()
    runner = JobRunner(
        subject_service=FakeSubjectService(),  # type: ignore[arg-type]
        pipeline_orchestrator=FakePipeline(),  # type: ignore[arg-type]
        telegram_service=telegram,  # type: ignore[arg-type]
    )
    scheduler = AgentScheduler(
        nightly_cron="0 1 * * *",
        morning_cron="30 8 * * *",
        timezone="UTC",
        job_runner=runner,
    )

    stats = await scheduler._run_scheduled_nightly_collection()

    assert stats["items_inserted"] == 4
    assert telegram.calls == [
        {
            "stats": stats,
            "status": "success",
            "error_text": None,
        }
    ]


@pytest.mark.asyncio
async def test_digest_run_sends_one_message_per_brief_and_feedback_buttons_map_to_items(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_service, routing_service, subject = await _seed_subject_with_route(db)
    item_id = await _seed_item_score(
        db,
        subject_id=subject.id,
        external_id="item-1",
        text="Claude Code release details with practical agent workflow",
        url="https://example.com/post",
        score=0.92,
        rationale="practical release details",
    )

    digest_repo = DigestRepository(conn=db.conn)
    fake_telegram = FakeTelegramService()
    runner = JobRunner(
        subject_service=subject_service,
        routing_service=routing_service,
        item_score_repo=ItemScoreRepository(conn=db.conn),
        digest_repo=digest_repo,
        run_log_repo=RunLogRepository(conn=db.conn),
        telegram_service=fake_telegram,  # type: ignore[arg-type]
    )

    await runner.run_morning_digest()
    await runner.run_morning_digest()

    digest_count = await (await db.conn.execute("SELECT COUNT(*) AS c FROM digests")).fetchone()
    digest_item_count = await (await db.conn.execute("SELECT COUNT(*) AS c FROM digest_items")).fetchone()
    delivery_count = await (await db.conn.execute("SELECT COUNT(*) AS c FROM digest_deliveries")).fetchone()
    item_delivery_count = await (await db.conn.execute("SELECT COUNT(*) AS c FROM digest_item_deliveries")).fetchone()
    button_count = await (await db.conn.execute("SELECT COUNT(*) AS c FROM digest_buttons")).fetchone()
    assert int(digest_count["c"]) == 1
    assert int(digest_item_count["c"]) == 1
    assert int(delivery_count["c"]) == 1
    assert int(item_delivery_count["c"]) == 2
    assert int(button_count["c"]) == 5
    assert len(fake_telegram.sent_messages) == 2
    assert "#AgenticPM" in fake_telegram.sent_messages[0]["brief"].short_text
    assert "Why this matched:" not in fake_telegram.sent_messages[0]["brief"].short_text

    token_row = await (
        await db.conn.execute("SELECT token FROM digest_buttons WHERE action = 'more like this' LIMIT 1")
    ).fetchone()
    feedback_service = FeedbackService(
        feedback_repo=FeedbackRepository(conn=db.conn),
        subject_repo=SubjectRepository(conn=db.conn),
        digest_repo=digest_repo,
    )
    button = await feedback_service.get_digest_button(token_row["token"])
    assert button is not None
    assert button.item_id == item_id
    assert button.subject_id == subject.id
    assert button.label == "👍"
    assert button.kind == "feedback"

    await feedback_service.add_feedback_by_subject_id(
        subject_id=button.subject_id,
        item_id=button.item_id,
        feedback_type="button_macro",
        comment_text=button.action,
    )
    await feedback_service.add_feedback_by_subject_id(
        subject_id=button.subject_id,
        item_id=button.item_id,
        feedback_type="button_macro",
        comment_text=button.action,
    )
    feedback_count = await (await db.conn.execute("SELECT COUNT(*) AS c FROM feedback_events")).fetchone()
    assert int(feedback_count["c"]) == 2

    delivery = await feedback_service.find_digest_item_by_message(chat_id=123, message_id=1)
    assert delivery is not None
    assert delivery.item_id == item_id
    assert delivery.subject_id == subject.id

    await db.close()


@pytest.mark.asyncio
async def test_digest_run_skips_paused_subjects(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_service, routing_service, active = await _seed_subject_with_route(db)
    paused = await subject_service.create_subject("Paused AI", include_terms=["claude code"])
    await subject_service.set_subject_status(paused.id, "paused")
    await routing_service.link_subject(subject_name=paused.name, chat_id=123)
    await _seed_item_score(
        db,
        subject_id=active.id,
        external_id="active-item",
        text="Claude Code release details for active subject",
        url="https://example.com/active",
        score=0.92,
        rationale="active match",
    )
    await _seed_item_score(
        db,
        subject_id=paused.id,
        external_id="paused-item",
        text="Claude Code release details for paused subject",
        url="https://example.com/paused",
        score=0.95,
        rationale="paused match",
    )

    fake_telegram = FakeTelegramService()
    runner = JobRunner(
        subject_service=subject_service,
        routing_service=routing_service,
        item_score_repo=ItemScoreRepository(conn=db.conn),
        digest_repo=DigestRepository(conn=db.conn),
        run_log_repo=RunLogRepository(conn=db.conn),
        telegram_service=fake_telegram,  # type: ignore[arg-type]
    )

    stats = await runner.run_morning_digest()
    paused_digest_items = await (
        await db.conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM digest_items di
            JOIN digests d ON d.id = di.digest_id
            WHERE d.subject_id = ?
            """,
            (paused.id,),
        )
    ).fetchone()

    assert stats["subjects_seen"] == 1
    assert stats["briefs_sent"] == 1
    assert [message["subject_name"] for message in fake_telegram.sent_messages] == [active.name]
    assert int(paused_digest_items["c"]) == 0

    await db.close()


@pytest.mark.asyncio
async def test_digest_sends_no_briefs_notice_when_top_score_below_relevance_floor(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_service, routing_service, subject = await _seed_subject_with_route(db)
    await _seed_item_score(
        db,
        subject_id=subject.id,
        external_id="weak-item",
        text="Generic AI podcast chatter with no useful subject match",
        url="https://example.com/weak",
        score=0.3,
        rationale="weak match",
    )

    fake_telegram = FakeTelegramService()
    runner = JobRunner(
        subject_service=subject_service,
        routing_service=routing_service,
        item_score_repo=ItemScoreRepository(conn=db.conn),
        digest_repo=DigestRepository(conn=db.conn),
        run_log_repo=RunLogRepository(conn=db.conn),
        telegram_service=fake_telegram,  # type: ignore[arg-type]
        min_brief_relevance_score=0.55,
    )

    stats = await runner.run_morning_digest()
    digest_item_count = await (await db.conn.execute("SELECT COUNT(*) AS c FROM digest_items")).fetchone()

    assert stats["subjects_below_relevance_threshold"] == [subject.id]
    assert stats["placeholder_briefs_sent"] == 1
    assert stats["briefs_sent"] == 1
    assert int(digest_item_count["c"]) == 0
    assert fake_telegram.sent_messages[-1]["brief"] is None
    assert "Most-relevant candidates scored 0.30" in fake_telegram.sent_messages[-1]["footer"]
    assert "below threshold 0.55" in fake_telegram.sent_messages[-1]["footer"]

    await db.close()


@pytest.mark.asyncio
async def test_digest_relevance_floor_does_not_filter_lower_ranked_items_when_top_score_passes(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_service, routing_service, subject = await _seed_subject_with_route(db)
    for idx, score in enumerate([0.9, 0.8, 0.7, 0.3, 0.2], start=1):
        await _seed_item_score(
            db,
            subject_id=subject.id,
            external_id=f"mixed-{idx}",
            text=f"Claude Code workflow detail {idx}",
            url=f"https://example.com/mixed-{idx}",
            score=score,
            rationale=f"score {score}",
        )

    fake_telegram = FakeTelegramService()
    runner = JobRunner(
        subject_service=subject_service,
        routing_service=routing_service,
        item_score_repo=ItemScoreRepository(conn=db.conn),
        digest_repo=DigestRepository(conn=db.conn),
        run_log_repo=RunLogRepository(conn=db.conn),
        telegram_service=fake_telegram,  # type: ignore[arg-type]
        min_brief_relevance_score=0.55,
    )

    stats = await runner.run_morning_digest()

    assert stats["subjects_below_relevance_threshold"] == []
    assert stats["briefs_sent"] == 5
    assert len(fake_telegram.sent_messages) == 5
    assert all(message["brief"] is not None for message in fake_telegram.sent_messages)

    await db.close()


@pytest.mark.asyncio
async def test_smart_briefs_rebuild_when_preferences_changed_without_new_items(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_service, routing_service, subject = await _seed_subject_with_route(db)
    first_item_id = await _seed_item_score(
        db,
        subject_id=subject.id,
        external_id="item-1",
        text="Old top item",
        url="https://example.com/old",
        score=0.95,
        rationale="old top",
    )
    second_item_id = await _seed_item_score(
        db,
        subject_id=subject.id,
        external_id="item-2",
        text="New preference winner",
        url="https://example.com/new",
        score=0.30,
        rationale="new winner after refinement",
    )

    item_score_repo = ItemScoreRepository(conn=db.conn)
    digest_repo = DigestRepository(conn=db.conn)
    fake_telegram = FakeTelegramService()
    runner = JobRunner(
        subject_service=subject_service,
        routing_service=routing_service,
        item_score_repo=item_score_repo,
        digest_repo=digest_repo,
        run_log_repo=RunLogRepository(conn=db.conn),
        telegram_service=fake_telegram,  # type: ignore[arg-type]
    )

    await runner.run_morning_digest()
    assert fake_telegram.sent_messages[0]["brief"].item_id == first_item_id

    await item_score_repo.upsert_score(
        item_id=first_item_id,
        subject_id=subject.id,
        pass1_score=0.2,
        pass2_score=0.2,
        practicality_score=0.2,
        novelty_score=0.2,
        trust_score=0.2,
        noise_penalty=0.0,
        final_score=0.2,
        rationale="downgraded",
    )
    await item_score_repo.upsert_score(
        item_id=second_item_id,
        subject_id=subject.id,
        pass1_score=0.99,
        pass2_score=0.99,
        practicality_score=0.99,
        novelty_score=0.99,
        trust_score=0.99,
        noise_penalty=0.0,
        final_score=0.99,
        rationale="now best",
    )
    await db.conn.execute(
        """
        UPDATE subject_preferences
        SET updated_at = datetime('now', '+1 minute')
        WHERE subject_id = ?
        """,
        (subject.id,),
    )
    await db.conn.commit()

    stats = await runner.run_smart_briefs()

    assert stats["digests_rebuilt"] == 1
    assert fake_telegram.sent_messages[-2]["brief"].item_id == second_item_id
    assert "Preferences changed" in fake_telegram.sent_messages[-1]["footer"]

    await db.close()


@pytest.mark.asyncio
async def test_digest_skips_legacy_empty_rule_subjects_with_warning(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_repo = SubjectRepository(conn=db.conn)
    # Bypass SubjectService to simulate a legacy row created before the
    # non-empty-preferences invariant existed.
    subject = await subject_repo.create("Legacy Empty Subject", include_terms=[], exclude_terms=[])
    subject_service = SubjectService(repository=subject_repo)
    routing_service = RoutingService(routing_repo=RoutingRepository(conn=db.conn), subject_repo=subject_repo)
    await routing_service.register_chat(chat_id=123, title="Test")
    await routing_service.link_subject(subject_name="Legacy Empty Subject", chat_id=123)
    await _seed_item_score(
        db,
        subject_id=subject.id,
        external_id="item-1",
        text="Unrelated finance item that should not be sent from baseline scoring",
        url="https://example.com/finance",
        score=0.99,
        rationale="legacy noisy baseline",
    )

    fake_telegram = FakeTelegramService()
    runner = JobRunner(
        subject_service=subject_service,
        routing_service=routing_service,
        item_score_repo=ItemScoreRepository(conn=db.conn),
        digest_repo=DigestRepository(conn=db.conn),
        run_log_repo=RunLogRepository(conn=db.conn),
        telegram_service=fake_telegram,  # type: ignore[arg-type]
    )

    stats = await runner.run_smart_briefs()

    assert stats["subjects_skipped_empty_preferences"] == 1
    assert fake_telegram.sent_messages[-1]["brief"] is None
    assert "no preference rules" in fake_telegram.sent_messages[-1]["footer"]

    await db.close()


@pytest.mark.asyncio
async def test_smart_briefs_combines_fresh_and_preference_change_footer(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_service, routing_service, subject = await _seed_subject_with_route(db)
    await _seed_item_score(
        db,
        subject_id=subject.id,
        external_id="item-1",
        text="Older Claude Code workflow detail",
        url="https://example.com/old",
        score=0.72,
        rationale="older but useful",
    )

    digest_repo = DigestRepository(conn=db.conn)
    fake_telegram = FakeTelegramService()
    runner = JobRunner(
        subject_service=subject_service,
        routing_service=routing_service,
        item_score_repo=ItemScoreRepository(conn=db.conn),
        digest_repo=digest_repo,
        run_log_repo=RunLogRepository(conn=db.conn),
        telegram_service=fake_telegram,  # type: ignore[arg-type]
    )

    await runner.run_smart_briefs()
    await _seed_item_score(
        db,
        subject_id=subject.id,
        external_id="item-2",
        text="Fresh Claude Code release with concrete implementation steps",
        url="https://example.com/new",
        score=0.97,
        rationale="fresh release details",
    )
    await db.conn.execute(
        """
        UPDATE subject_preferences
        SET updated_at = datetime('now', '+1 minute')
        WHERE subject_id = ?
        """,
        (subject.id,),
    )
    await db.conn.commit()

    stats = await runner.run_smart_briefs()

    assert stats["digests_rebuilt"] == 1
    footer = fake_telegram.sent_messages[-1]["footer"]
    assert footer is not None
    assert "1 new brief since" in footer
    assert "Preferences changed - re-ranked from current scores" in footer

    await db.close()


@pytest.mark.asyncio
async def test_digest_runner_uses_injected_renderer(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_service, routing_service, subject = await _seed_subject_with_route(db)
    item_id = await _seed_item_score(
        db,
        subject_id=subject.id,
        external_id="item-1",
        text="Claude Code release details with practical agent workflow",
        url="https://example.com/post",
        score=0.92,
        rationale="practical release details",
    )

    digest_repo = DigestRepository(conn=db.conn)
    fake_telegram = FakeTelegramService()
    runner = JobRunner(
        subject_service=subject_service,
        routing_service=routing_service,
        item_score_repo=ItemScoreRepository(conn=db.conn),
        digest_repo=digest_repo,
        run_log_repo=RunLogRepository(conn=db.conn),
        telegram_service=fake_telegram,  # type: ignore[arg-type]
        digest_renderer=HeadlineOnlyRenderer(),
    )

    stats = await runner.run_morning_digest()

    assert stats["renderers_used"] == {"headline_only": 1}
    assert fake_telegram.sent_messages[0]["brief"].short_text == (
        "Agentic PM: Claude Code release details with practical agent workflow"
    )
    digest_item = await (
        await db.conn.execute("SELECT reason_selected, short_text FROM digest_items WHERE item_id = ?", (item_id,))
    ).fetchone()
    assert digest_item["reason_selected"] == "headline renderer"
    assert digest_item["short_text"].startswith("Agentic PM:")
    latest_run = await (
        await db.conn.execute("SELECT stats_json FROM run_logs ORDER BY id DESC LIMIT 1")
    ).fetchone()
    assert json.loads(latest_run["stats_json"])["renderers_used"] == {"headline_only": 1}

    await db.close()


@pytest.mark.asyncio
async def test_scheduled_morning_digest_runs_rescore_before_sending(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_service, routing_service, subject = await _seed_subject_with_route(db)
    await _seed_item_score(
        db,
        subject_id=subject.id,
        external_id="item-1",
        text="Claude Code release details with practical agent workflow",
        url="https://example.com/post",
        score=0.92,
        rationale="practical release details",
    )

    fake_pipeline = FakePipelineOrchestrator()
    runner = JobRunner(
        subject_service=subject_service,
        routing_service=routing_service,
        item_score_repo=ItemScoreRepository(conn=db.conn),
        digest_repo=DigestRepository(conn=db.conn),
        run_log_repo=RunLogRepository(conn=db.conn),
        pipeline_orchestrator=fake_pipeline,  # type: ignore[arg-type]
        telegram_service=FakeTelegramService(),  # type: ignore[arg-type]
        digest_renderer=HeadlineOnlyRenderer(),
    )

    stats = await runner.run_morning_digest()

    assert fake_pipeline.rescore_calls == 1
    assert fake_pipeline.rescore_subject_ids == [None]
    assert stats["pre_send_rescore"] == {"items_scored": 1}
    assert stats["deliveries_sent"] == 1

    await db.close()


@pytest.mark.asyncio
async def test_smart_briefs_delivers_existing_scores_without_rescore(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_service, routing_service, subject = await _seed_subject_with_route(db)
    await _seed_item_score(
        db,
        subject_id=subject.id,
        external_id="item-1",
        text="Claude Code release details with practical agent workflow",
        url="https://example.com/post",
        score=0.92,
        rationale="practical release details",
    )

    fake_pipeline = FakePipelineOrchestrator()
    runner = JobRunner(
        subject_service=subject_service,
        routing_service=routing_service,
        item_score_repo=ItemScoreRepository(conn=db.conn),
        digest_repo=DigestRepository(conn=db.conn),
        run_log_repo=RunLogRepository(conn=db.conn),
        pipeline_orchestrator=fake_pipeline,  # type: ignore[arg-type]
        telegram_service=FakeTelegramService(),  # type: ignore[arg-type]
        digest_renderer=HeadlineOnlyRenderer(),
    )

    stats = await runner.run_smart_briefs(subject_ids={subject.id})

    assert fake_pipeline.rescore_calls == 0
    assert "pre_send_rescore" not in stats
    assert stats["deliveries_sent"] == 1

    await db.close()


@pytest.mark.asyncio
async def test_smart_briefs_resends_when_no_new_items_and_rebuilds_when_fresh_items_exist(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_service, routing_service, subject = await _seed_subject_with_route(db)
    await _seed_item_score(
        db,
        subject_id=subject.id,
        external_id="item-1",
        text="Older Claude Code workflow detail",
        url="https://example.com/old",
        score=0.72,
        rationale="older but useful",
    )

    digest_repo = DigestRepository(conn=db.conn)
    fake_telegram = FakeTelegramService()
    runner = JobRunner(
        subject_service=subject_service,
        routing_service=routing_service,
        item_score_repo=ItemScoreRepository(conn=db.conn),
        digest_repo=digest_repo,
        run_log_repo=RunLogRepository(conn=db.conn),
        telegram_service=fake_telegram,  # type: ignore[arg-type]
    )

    initial_stats = await runner.run_smart_briefs()
    resend_stats = await runner.run_smart_briefs()

    assert initial_stats["deliveries_sent"] == 1
    assert resend_stats["smart_resends"] == 1
    assert "Older Claude Code workflow detail" in fake_telegram.sent_messages[-1]["brief"].short_text
    assert fake_telegram.sent_messages[-1]["footer"].startswith("No new briefs since ")

    await _seed_item_score(
        db,
        subject_id=subject.id,
        external_id="item-2",
        text="Fresh Claude Code release with concrete implementation steps",
        url="https://example.com/new",
        score=0.97,
        rationale="fresh release details",
    )

    fresh_stats = await runner.run_smart_briefs()

    assert fresh_stats["digests_rebuilt"] == 1
    assert fresh_stats["deliveries_sent"] == 1
    assert "Fresh Claude Code release" in fake_telegram.sent_messages[-1]["brief"].short_text
    assert fake_telegram.sent_messages[-1]["footer"].startswith("1 new brief since ")

    digest_count = await (await db.conn.execute("SELECT COUNT(*) AS c FROM digests")).fetchone()
    delivery_count = await (await db.conn.execute("SELECT COUNT(*) AS c FROM digest_deliveries")).fetchone()
    latest_run = await (
        await db.conn.execute("SELECT run_type, status FROM run_logs ORDER BY id DESC LIMIT 1")
    ).fetchone()
    assert int(digest_count["c"]) == 1
    assert int(delivery_count["c"]) == 1
    assert latest_run["run_type"] == "briefs"
    assert latest_run["status"] == "success"

    await db.close()


@pytest.mark.asyncio
async def test_digest_rebuild_deletes_today_and_recomposes_from_current_scores(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_service, routing_service, subject = await _seed_subject_with_route(db)
    item_id = await _seed_item_score(
        db,
        subject_id=subject.id,
        external_id="item-1",
        text="Older Claude Code workflow detail",
        url="https://example.com/old",
        score=0.72,
        rationale="older but useful",
    )

    digest_repo = DigestRepository(conn=db.conn)
    fake_telegram = FakeTelegramService()
    runner = JobRunner(
        subject_service=subject_service,
        routing_service=routing_service,
        item_score_repo=ItemScoreRepository(conn=db.conn),
        digest_repo=digest_repo,
        run_log_repo=RunLogRepository(conn=db.conn),
        telegram_service=fake_telegram,  # type: ignore[arg-type]
    )

    initial_stats = await runner.run_morning_digest()
    assert initial_stats["deliveries_sent"] == 1
    assert "Older Claude Code workflow detail" in fake_telegram.sent_messages[-1]["brief"].short_text
    old_more_button = await (
        await db.conn.execute(
            "SELECT token, digest_id FROM digest_buttons WHERE item_id = ? AND kind = 'expand'",
            (item_id,),
        )
    ).fetchone()
    assert old_more_button is not None

    await _seed_item_score(
        db,
        subject_id=subject.id,
        external_id="item-2",
        text="Fresh Claude Code release with concrete implementation steps",
        url="https://example.com/new",
        score=0.97,
        rationale="fresh release details",
    )

    rebuild_stats = await runner.rebuild_todays_digest()

    assert rebuild_stats["digests_rebuilt"] == 1
    assert rebuild_stats["deliveries_sent"] == 1
    assert len(fake_telegram.sent_messages) == 3
    rebuilt_texts = [message["brief"].short_text for message in fake_telegram.sent_messages[-2:]]
    assert any("Fresh Claude Code release" in text for text in rebuilt_texts)
    preserved_button = await digest_repo.get_button(old_more_button["token"])
    assert preserved_button is not None
    preserved_view = await digest_repo.get_brief_view(
        digest_id=preserved_button.digest_id,
        item_id=preserved_button.item_id,
    )
    assert preserved_view is not None
    assert preserved_view.item_id == item_id

    digest_count = await (await db.conn.execute("SELECT COUNT(*) AS c FROM digests")).fetchone()
    delivery_count = await (await db.conn.execute("SELECT COUNT(*) AS c FROM digest_deliveries")).fetchone()
    latest_run = await (
        await db.conn.execute("SELECT run_type, status FROM run_logs ORDER BY id DESC LIMIT 1")
    ).fetchone()
    assert int(digest_count["c"]) == 1
    assert int(delivery_count["c"]) == 1
    assert latest_run["run_type"] == "digest_rebuild"
    assert latest_run["status"] == "success"

    await db.close()
