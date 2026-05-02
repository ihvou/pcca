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
from pcca.scheduler import JobRunner
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
async def test_smart_briefs_rescores_before_sending(tmp_path: Path) -> None:
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

    assert fake_pipeline.rescore_calls == 1
    assert fake_pipeline.rescore_subject_ids == [{subject.id}]
    assert stats["pre_send_rescore"] == {"items_scored": 1}
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

    initial_stats = await runner.run_morning_digest()
    assert initial_stats["deliveries_sent"] == 1
    assert "Older Claude Code workflow detail" in fake_telegram.sent_messages[-1]["brief"].short_text

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
