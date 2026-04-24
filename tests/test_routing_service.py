from __future__ import annotations

from pathlib import Path

import pytest

from pcca.db import Database
from pcca.repositories.routing import RoutingRepository
from pcca.repositories.subjects import SubjectRepository
from pcca.services.routing_service import RoutingService
from pcca.services.subject_service import SubjectService


@pytest.mark.asyncio
async def test_routing_service_links_subject_to_chat(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_repo = SubjectRepository(conn=db.conn)
    subject_service = SubjectService(repository=subject_repo)
    routing_service = RoutingService(
        routing_repo=RoutingRepository(conn=db.conn),
        subject_repo=subject_repo,
    )

    subject = await subject_service.create_subject("Vibe Coding")
    await routing_service.register_chat(chat_id=123, title="Test")
    await routing_service.link_subject("Vibe Coding", chat_id=123, thread_id=None)
    routes = await routing_service.list_routes_for_subject(subject.id)
    assert len(routes) == 1
    assert routes[0].chat_id == 123
    resolved = await routing_service.resolve_subject_for_chat(chat_id=123, thread_id=None)
    assert resolved is not None
    assert resolved.id == subject.id

    await db.close()
