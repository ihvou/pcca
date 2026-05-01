from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from pcca.collectors.base import CollectedItem
from pcca.db import Database
from pcca.repositories.digests import DigestRepository
from pcca.repositories.item_scores import ItemScoreRepository
from pcca.repositories.items import ItemRepository
from pcca.repositories.subjects import SubjectRepository
from pcca.services.subject_service import SubjectService


@pytest.mark.asyncio
async def test_top_unsent_candidates_filters_only_current_subject_digest_history(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_service = SubjectService(repository=SubjectRepository(conn=db.conn))
    subject_a = await subject_service.create_subject("Agentic PM", include_terms=["agentic pm"])
    subject_b = await subject_service.create_subject("Vibe Coding", include_terms=["vibe coding"])

    item_repo = ItemRepository(conn=db.conn)
    item_stats = await item_repo.upsert_many(
        [
            CollectedItem(
                platform="rss",
                external_id="shared-item",
                author="OpenAI",
                url="https://example.com/shared",
                text="Practical Claude Code workflow notes",
                transcript_text=None,
                published_at="2026-04-24T09:00:00",
                metadata={},
            )
        ]
    )
    item_id = item_stats["item_ids"][0]

    score_repo = ItemScoreRepository(conn=db.conn)
    for subject in (subject_a, subject_b):
        await score_repo.upsert_score(
            item_id=item_id,
            subject_id=subject.id,
            pass1_score=0.9,
            pass2_score=0.9,
            practicality_score=0.9,
            novelty_score=0.8,
            trust_score=0.8,
            noise_penalty=0.0,
            final_score=0.91,
            rationale="useful details",
        )

    digest_repo = DigestRepository(conn=db.conn)
    digest_a = await digest_repo.get_or_create_digest(subject_id=subject_a.id, run_date=date.today())
    await digest_repo.add_digest_item(
        digest_id=digest_a.id,
        item_id=item_id,
        rank=1,
        reason_selected="already sent to subject A",
    )

    assert await score_repo.top_unsent_candidates(subject_id=subject_a.id, limit=5) == []
    subject_b_candidates = await score_repo.top_unsent_candidates(subject_id=subject_b.id, limit=5)
    assert [candidate.item_id for candidate in subject_b_candidates] == [item_id]

    await db.close()


@pytest.mark.asyncio
async def test_candidate_preserves_key_message_and_metadata(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_service = SubjectService(repository=SubjectRepository(conn=db.conn))
    subject = await subject_service.create_subject("Agentic PM", include_terms=["agentic pm"])
    assert subject.telegram_hashtag == "#AgenticPM"

    item_repo = ItemRepository(conn=db.conn)
    item_stats = await item_repo.upsert_many(
        [
            CollectedItem(
                platform="youtube",
                external_id="video-1",
                author="Creator",
                url="https://youtube.com/watch?v=abc",
                text="Original title",
                transcript_text=None,
                published_at="2026-04-24T09:00:00",
                metadata={"duration_seconds": 1234},
            )
        ]
    )
    item_id = item_stats["item_ids"][0]

    score_repo = ItemScoreRepository(conn=db.conn)
    await score_repo.upsert_score(
        item_id=item_id,
        subject_id=subject.id,
        pass1_score=0.9,
        pass2_score=0.9,
        practicality_score=0.9,
        novelty_score=0.8,
        trust_score=0.8,
        noise_penalty=0.0,
        final_score=0.91,
        rationale="useful details",
        key_message="The useful core idea.",
    )

    candidate = (await score_repo.top_candidates(subject_id=subject.id, limit=1))[0]

    assert candidate.key_message == "The useful core idea."
    assert candidate.metadata == {"duration_seconds": 1234}

    await db.close()
