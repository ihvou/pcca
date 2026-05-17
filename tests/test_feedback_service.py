from __future__ import annotations

from pathlib import Path

import pytest

from pcca.db import Database
from pcca.repositories.feedback import FeedbackRepository
from pcca.repositories.preferences import SubjectPreferenceRepository
from pcca.repositories.subjects import SubjectRepository
from pcca.services.feedback_service import FeedbackService
from pcca.services.subject_service import SubjectService


@pytest.mark.asyncio
async def test_feedback_versions_preferences_without_mutating_description(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_repo = SubjectRepository(conn=db.conn)
    pref_repo = SubjectPreferenceRepository(conn=db.conn)
    subject = await SubjectService(repository=subject_repo).create_subject(
        "AI Tools",
        include_terms=["ai tools"],
        description_text="Track practical AI tool updates.",
    )
    before = await pref_repo.get_latest(subject.id)
    assert before is not None

    service = FeedbackService(
        feedback_repo=FeedbackRepository(conn=db.conn),
        subject_repo=subject_repo,
        preference_repo=pref_repo,
    )
    await service.add_feedback_by_subject_id(
        subject_id=subject.id,
        feedback_type="reply_text",
        comment_text="less hype like this",
    )

    description = await subject_repo.get_description_text(subject.id)
    after = await pref_repo.get_latest(subject.id)
    assert description == "Track practical AI tool updates."
    assert after is not None
    assert after.version == before.version + 1
    assert after.quality_rules["notes"] == "User feedback (reply_text): less hype like this"

    await db.close()


@pytest.mark.asyncio
async def test_t157_feedback_macros_cannot_overwrite_description(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_repo = SubjectRepository(conn=db.conn)
    subject = await SubjectService(repository=subject_repo).create_subject(
        "AI PM Success Stories",
        include_terms=["ai pm"],
        description_text="Track concrete AI PM success stories.",
    )

    with pytest.raises(ValueError):
        await subject_repo.update_description(subject.id, "User feedback (button_macro): less like this")

    assert await subject_repo.get_description_text(subject.id) == "Track concrete AI PM success stories."
    await db.close()
