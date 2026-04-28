from __future__ import annotations

from pathlib import Path

import pytest

from pcca.db import Database
from pcca.repositories.subject_drafts import SubjectDraftRepository
from pcca.services.preference_extraction_service import PreferenceExtractionService


@pytest.mark.asyncio
async def test_preference_extraction_heuristic_creates_subject_draft(tmp_path: Path) -> None:
    service = PreferenceExtractionService(model_router=None)

    draft = await service.extract(
        "I want practical vibe coding news: include Claude Code releases, agent workflows; "
        "avoid biography and generic AI hype"
    )

    assert "Vibe" in draft.title
    assert "claude code releases" in draft.include_terms
    assert "agent workflows" in draft.include_terms
    assert "biography" in draft.exclude_terms
    assert "generic ai hype" in draft.exclude_terms

    unnamed = await service.extract("I want practical AI-in-HR case studies, no hype")
    assert unnamed.title == "Practical AI HR Case Studies"
    assert unnamed.exclude_terms == ["hype"]


@pytest.mark.asyncio
async def test_subject_draft_repository_roundtrips_rules(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    repo = SubjectDraftRepository(conn=db.conn)
    await repo.upsert(
        chat_id=123,
        title="Vibe Coding",
        description_text="practical vibe coding",
        include_terms=["claude code"],
        exclude_terms=["biography"],
        quality_notes="only concrete workflows",
        last_user_message="create subject",
    )

    draft = await repo.get(123)

    assert draft is not None
    assert draft.title == "Vibe Coding"
    assert draft.include_terms == ["claude code"]
    assert draft.exclude_terms == ["biography"]
    assert draft.quality_notes == "only concrete workflows"

    await repo.delete(123)
    assert await repo.get(123) is None

    await db.close()
