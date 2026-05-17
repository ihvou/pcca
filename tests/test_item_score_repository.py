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
            key_message=f"Useful summary for {subject.name}.",
            refined_segment=f"Detailed summary for {subject.name}.",
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
        refined_segment="A cleaned-up version of the relevant segment.",
    )

    candidate = (await score_repo.top_candidates(subject_id=subject.id, limit=1))[0]

    assert candidate.key_message == "The useful core idea."
    assert candidate.refined_segment == "A cleaned-up version of the relevant segment."
    assert candidate.metadata == {"duration_seconds": 1234}

    await db.close()


@pytest.mark.asyncio
async def test_t159_top_candidates_excludes_unprocessed_items(tmp_path: Path) -> None:
    """T-159: digest selector must require unified Pass-2 summaries.

    A higher-scoring item without a generated brief_summary/detailed_summary
    must not enter the digest at all, because the renderer no longer falls
    back to raw source or transcript text.
    """
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_service = SubjectService(repository=SubjectRepository(conn=db.conn))
    subject = await subject_service.create_subject("AI Tools", include_terms=["ai"])

    item_repo = ItemRepository(conn=db.conn)
    item_stats = await item_repo.upsert_many(
        [
            CollectedItem(
                platform="linkedin",
                external_id="unreranked-promo",
                author=None,
                url="https://linkedin.com/feed/update/raw",
                text="Hello community! I'd love to invite you all to our upcoming webinar...",
                transcript_text=None,
                published_at="2026-05-14T09:00:00",
                metadata={},
            ),
            CollectedItem(
                platform="youtube",
                external_id="reranked-useful",
                author="Karpathy",
                url="https://youtube.com/watch?v=useful",
                text="Practical Claude Code workflow notes",
                transcript_text=None,
                published_at="2026-05-14T09:00:00",
                metadata={},
            ),
        ]
    )
    unreranked_id, reranked_id = item_stats["item_ids"]

    score_repo = ItemScoreRepository(conn=db.conn)
    # Unreranked item: higher final_score but no key_message — the exact
    # shape that produced the "Hello community!" brief at rank #3.
    await score_repo.upsert_score(
        item_id=unreranked_id,
        subject_id=subject.id,
        pass1_score=0.7,
        pass2_score=None,
        practicality_score=0.5,
        novelty_score=0.5,
        trust_score=0.5,
        noise_penalty=0.0,
        final_score=0.70,
        rationale="embedding only, no Pass-2",
        key_message=None,
        refined_segment=None,
    )
    # Reranked item: slightly lower final_score but has a clean key_message.
    await score_repo.upsert_score(
        item_id=reranked_id,
        subject_id=subject.id,
        pass1_score=0.65,
        pass2_score=0.65,
        practicality_score=0.7,
        novelty_score=0.7,
        trust_score=0.7,
        noise_penalty=0.0,
        final_score=0.65,
        rationale="reranked by Pass-2",
        key_message="Karpathy explains a concrete Claude Code workflow.",
        refined_segment="Concise paraphrase of the segment.",
    )

    top = await score_repo.top_candidates(subject_id=subject.id, limit=5)
    assert [c.item_id for c in top] == [reranked_id]
    assert top[0].key_message == "Karpathy explains a concrete Claude Code workflow."

    unsent = await score_repo.top_unsent_candidates(subject_id=subject.id, limit=5)
    assert [c.item_id for c in unsent] == [reranked_id]

    await db.close()


@pytest.mark.asyncio
async def test_t152_top_candidates_ties_broken_by_final_score_within_reranked(
    tmp_path: Path,
) -> None:
    """Among reranked items, ordering is still by final_score."""
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_service = SubjectService(repository=SubjectRepository(conn=db.conn))
    subject = await subject_service.create_subject("AI Tools", include_terms=["ai"])

    item_repo = ItemRepository(conn=db.conn)
    item_stats = await item_repo.upsert_many(
        [
            CollectedItem(
                platform="youtube",
                external_id=f"reranked-{i}",
                author="Author",
                url=f"https://youtube.com/watch?v={i}",
                text=f"Item {i}",
                transcript_text=None,
                published_at="2026-05-14T09:00:00",
                metadata={},
            )
            for i in range(3)
        ]
    )
    score_repo = ItemScoreRepository(conn=db.conn)
    for item_id, score in zip(item_stats["item_ids"], [0.70, 0.85, 0.78]):
        await score_repo.upsert_score(
            item_id=item_id,
            subject_id=subject.id,
            pass1_score=score,
            pass2_score=score,
            practicality_score=0.7,
            novelty_score=0.7,
            trust_score=0.7,
            noise_penalty=0.0,
            final_score=score,
            rationale="reranked by Pass-2",
            key_message=f"Curated message {item_id}.",
            refined_segment=f"Detailed summary for item {item_id}.",
        )

    top = await score_repo.top_candidates(subject_id=subject.id, limit=5)
    # All have key_message, so order is purely by final_score DESC.
    scores = [c.final_score for c in top]
    assert scores == sorted(scores, reverse=True)
    assert scores == [0.85, 0.78, 0.70]

    await db.close()
