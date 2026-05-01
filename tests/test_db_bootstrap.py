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
    created = await service.create_subject("Vibe Coding", include_terms=["vibe coding"])
    assert created.name == "Vibe Coding"

    listed = await service.list_subjects()
    assert len(listed) == 1
    assert listed[0].name == "Vibe Coding"
    repo = SubjectRepository(conn=db.conn)
    await repo.save_description_embedding(created.id, model="fake", embedding=[1.0, 0.0])
    assert await repo.get_description_embedding(created.id, model="fake") == [1.0, 0.0]
    await repo.update_description(created.id, "Updated full subject description")
    assert await repo.get_description_embedding(created.id, model="fake") is None

    await db.close()


@pytest.mark.asyncio
async def test_subject_service_rejects_empty_preferences(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    service = SubjectService(repository=SubjectRepository(conn=db.conn))
    with pytest.raises(ValueError):
        await service.create_subject("Thin Subject")

    await db.close()
