from __future__ import annotations

from pathlib import Path

import pytest

from pcca.collectors.base import CollectedItem
from pcca.db import Database
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

    async def send_digest_message(
        self,
        *,
        chat_id: int,
        subject_name: str,
        items: list[str],
        item_actions: list[dict] | None = None,
        thread_id: int | None = None,
    ) -> int:
        self.sent_messages.append(
            {
                "chat_id": chat_id,
                "subject_name": subject_name,
                "items": items,
                "item_actions": item_actions or [],
                "thread_id": thread_id,
            }
        )
        return len(self.sent_messages)


@pytest.mark.asyncio
async def test_digest_run_is_idempotent_and_feedback_buttons_map_to_items(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_repo = SubjectRepository(conn=db.conn)
    subject_service = SubjectService(repository=subject_repo)
    subject = await subject_service.create_subject("Agentic PM")

    routing_service = RoutingService(routing_repo=RoutingRepository(conn=db.conn), subject_repo=subject_repo)
    await routing_service.register_chat(chat_id=123, title="Test")
    await routing_service.link_subject(subject_name="Agentic PM", chat_id=123)

    item_repo = ItemRepository(conn=db.conn)
    item_stats = await item_repo.upsert_many(
        [
            CollectedItem(
                platform="rss",
                external_id="item-1",
                author="Boris Cherny",
                url="https://example.com/post",
                text="Claude Code release details with practical agent workflow",
                transcript_text=None,
                published_at="2026-04-24T09:00:00",
                metadata={},
            )
        ]
    )
    item_id = item_stats["item_ids"][0]

    item_score_repo = ItemScoreRepository(conn=db.conn)
    await item_score_repo.upsert_score(
        item_id=item_id,
        subject_id=subject.id,
        pass1_score=0.9,
        pass2_score=0.9,
        practicality_score=0.9,
        novelty_score=0.8,
        trust_score=0.8,
        noise_penalty=0.0,
        final_score=0.92,
        rationale="practical release details",
    )

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
    await runner.run_morning_digest()

    digest_count = await (await db.conn.execute("SELECT COUNT(*) AS c FROM digests")).fetchone()
    digest_item_count = await (await db.conn.execute("SELECT COUNT(*) AS c FROM digest_items")).fetchone()
    delivery_count = await (await db.conn.execute("SELECT COUNT(*) AS c FROM digest_deliveries")).fetchone()
    button_count = await (await db.conn.execute("SELECT COUNT(*) AS c FROM digest_buttons")).fetchone()
    assert int(digest_count["c"]) == 1
    assert int(digest_item_count["c"]) == 1
    assert int(delivery_count["c"]) == 1
    assert int(button_count["c"]) == 3
    assert len(fake_telegram.sent_messages) == 2
    assert "published: 2026-04-24T09:00:00" in fake_telegram.sent_messages[0]["items"][0]

    token_row = await (
        await db.conn.execute("SELECT token FROM digest_buttons WHERE action = 'up' LIMIT 1")
    ).fetchone()
    feedback_service = FeedbackService(
        feedback_repo=FeedbackRepository(conn=db.conn),
        subject_repo=subject_repo,
        digest_repo=digest_repo,
    )
    button = await feedback_service.get_digest_button(token_row["token"])
    assert button is not None
    assert button.item_id == item_id
    assert button.subject_id == subject.id

    await feedback_service.add_feedback_by_subject_id(
        subject_id=button.subject_id,
        item_id=button.item_id,
        feedback_type=f"button_{button.action}",
    )
    await feedback_service.add_feedback_by_subject_id(
        subject_id=button.subject_id,
        item_id=button.item_id,
        feedback_type=f"button_{button.action}",
    )
    feedback_count = await (await db.conn.execute("SELECT COUNT(*) AS c FROM feedback_events")).fetchone()
    assert int(feedback_count["c"]) == 1

    await db.close()
