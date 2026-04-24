from __future__ import annotations

from pathlib import Path

import pytest

from pcca.db import Database
from pcca.repositories.sources import SourceRepository
from pcca.repositories.subjects import SubjectRepository
from pcca.services.source_service import SourceService
from pcca.services.subject_service import SubjectService


@pytest.mark.asyncio
async def test_add_and_list_sources_for_subject(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_service = SubjectService(repository=SubjectRepository(conn=db.conn))
    source_service = SourceService(
        source_repo=SourceRepository(conn=db.conn),
        subject_repo=SubjectRepository(conn=db.conn),
    )

    await subject_service.create_subject("Vibe Coding")
    await source_service.add_source_to_subject(
        subject_name="Vibe Coding",
        platform="x",
        account_or_channel_id="borischerny",
        display_name="Boris Cherny",
        priority=2,
    )
    sources = await source_service.list_sources_for_subject("Vibe Coding")
    assert len(sources) == 1
    assert sources[0].platform == "x"
    assert sources[0].account_or_channel_id == "borischerny"
    assert sources[0].priority == 2

    await db.close()

