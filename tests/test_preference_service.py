from __future__ import annotations

from pathlib import Path

import pytest

from pcca.db import Database
from pcca.repositories.preferences import SubjectPreferenceRepository
from pcca.repositories.subjects import SubjectRepository
from pcca.services.preference_service import PreferenceService
from pcca.services.subject_service import SubjectService


@pytest.mark.asyncio
async def test_refine_preferences_appends_terms(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_repo = SubjectRepository(conn=db.conn)
    subject_service = SubjectService(repository=subject_repo)
    pref_service = PreferenceService(
        preference_repo=SubjectPreferenceRepository(conn=db.conn),
        subject_repo=subject_repo,
    )

    subject = await subject_service.create_subject("Agentic PM", include_terms=["agentic pm"])
    before = await pref_service.get_preferences_for_subject("Agentic PM")
    updated = await pref_service.refine_subject_rules(
        subject_name="Agentic PM",
        include_terms=["claude code", "releases"],
        exclude_terms=["biography"],
        description_append="More concrete Claude Code release details.",
    )
    assert updated.version == before.version + 1
    assert "claude code" in updated.include_rules.get("topics", [])
    assert "releases" in updated.include_rules.get("topics", [])
    assert "biography" in updated.exclude_rules.get("topics", [])
    description = await subject_repo.get_description_text(subject.id)
    assert description == "More concrete Claude Code release details."

    await db.close()
