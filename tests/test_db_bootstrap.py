from __future__ import annotations

from pathlib import Path

import pytest

from pcca.db import Database
from pcca.repositories.subjects import SubjectRepository
from pcca.services.subject_service import SubjectService


@pytest.mark.asyncio
async def test_db_init_and_subject_create(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    # Re-run to ensure idempotent migrations (v1 + v2).
    await db.initialize()
    assert db.conn is not None

    service = SubjectService(repository=SubjectRepository(conn=db.conn))
    created = await service.create_subject("Vibe Coding")
    assert created.name == "Vibe Coding"

    listed = await service.list_subjects()
    assert len(listed) == 1
    assert listed[0].name == "Vibe Coding"

    await db.close()
