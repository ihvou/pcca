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
    await routing_service.link_subject("Vibe Coding", chat_id=123, thread_id=None)
    routes = await routing_service.list_routes_for_subject(subject.id)
    assert len(routes) == 1
    assert routes[0].chat_id == 123
    assert routes[0].thread_id is None
    resolved = await routing_service.resolve_subject_for_chat(chat_id=123, thread_id=None)
    assert resolved is not None
    assert resolved.id == subject.id

    await db.close()


@pytest.mark.asyncio
async def test_ensure_routes_for_chat_links_to_existing_subjects(tmp_path: Path) -> None:
    """Legacy helper can still link a chat to every existing subject."""
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

    subject_a = await subject_service.create_subject("Agentic PM")
    subject_b = await subject_service.create_subject("Vibe Coding")

    new_routes = await routing_service.ensure_routes_for_chat(chat_id=999, title="Test")
    assert new_routes == 2

    routes_a = await routing_service.list_routes_for_subject(subject_a.id)
    routes_b = await routing_service.list_routes_for_subject(subject_b.id)
    assert [r.chat_id for r in routes_a] == [999]
    assert [r.chat_id for r in routes_b] == [999]

    # Re-running /start must be a no-op.
    again = await routing_service.ensure_routes_for_chat(chat_id=999, title="Test")
    assert again == 0

    await db.close()


@pytest.mark.asyncio
async def test_register_chat_does_not_link_subjects_until_picker_selection(tmp_path: Path) -> None:
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

    subject_a = await subject_service.create_subject("Agentic PM")
    subject_b = await subject_service.create_subject("Vibe Coding")
    await routing_service.register_chat(chat_id=999, title="Subject Group")

    assert await routing_service.list_routes_for_chat(chat_id=999) == []

    linked = await routing_service.link_subject_id(subject_id=subject_b.id, chat_id=999)
    assert linked.id == subject_b.id

    routes = await routing_service.list_routes_for_chat(chat_id=999)
    assert len(routes) == 1
    assert routes[0].subject_id == subject_b.id
    assert routes[0].subject_name == "Vibe Coding"
    assert routes[0].chat_title == "Subject Group"
    assert await routing_service.list_routes_for_subject(subject_a.id) == []

    await db.close()


@pytest.mark.asyncio
async def test_route_can_be_moved_and_unlinked(tmp_path: Path) -> None:
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
    await routing_service.register_chat(chat_id=111, title="Old Group")
    await routing_service.register_chat(chat_id=222, title="New Group")
    await routing_service.link_subject_id(subject_id=subject.id, chat_id=111)

    chats = await routing_service.list_registered_chats()
    assert {chat.chat_id for chat in chats} == {111, 222}

    moved = await routing_service.move_subject_route(
        subject_id=subject.id,
        from_chat_id=111,
        from_thread_id=None,
        to_chat_id=222,
    )
    assert moved is True
    assert await routing_service.list_routes_for_chat(chat_id=111) == []
    moved_routes = await routing_service.list_routes_for_chat(chat_id=222)
    assert [route.subject_name for route in moved_routes] == ["Vibe Coding"]

    removed = await routing_service.unlink_subject_route(subject_id=subject.id, chat_id=222)
    assert removed is True
    assert await routing_service.list_routes_for_subject(subject.id) == []

    moved_missing = await routing_service.move_subject_route(
        subject_id=subject.id,
        from_chat_id=222,
        from_thread_id=None,
        to_chat_id=111,
    )
    assert moved_missing is False
    assert await routing_service.list_routes_for_subject(subject.id) == []

    await db.close()


@pytest.mark.asyncio
async def test_ensure_routes_for_subject_links_to_single_existing_chat(tmp_path: Path) -> None:
    """User sends /start first, then creates a subject via desktop wizard.

    Subject creation via the wizard/CLI can auto-link only when there is one
    registered chat, so the first-run smoke-crawl digest can deliver without
    creating cross-talk between groups.
    """
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

    await routing_service.register_chat(chat_id=111, title="Personal")

    subject = await subject_service.create_subject("Vibe Coding")
    new_routes = await routing_service.ensure_routes_for_subject(subject_name="Vibe Coding")
    assert new_routes == 1

    routes = sorted(
        (route.chat_id for route in await routing_service.list_routes_for_subject(subject.id))
    )
    assert routes == [111]

    # Re-running ensure on the same subject is a no-op.
    again = await routing_service.ensure_routes_for_subject(subject_name="Vibe Coding")
    assert again == 0

    await db.close()


@pytest.mark.asyncio
async def test_ensure_routes_for_subject_does_not_auto_link_multiple_chats(tmp_path: Path) -> None:
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

    await routing_service.register_chat(chat_id=111, title="Agentic PM Group")
    await routing_service.register_chat(chat_id=222, title="Vibe Coding Group")
    subject = await subject_service.create_subject("Vibe Coding")

    new_routes = await routing_service.ensure_routes_for_subject(subject_name="Vibe Coding")

    assert new_routes == 0
    assert await routing_service.list_routes_for_subject(subject.id) == []

    await db.close()


@pytest.mark.asyncio
async def test_ensure_routes_for_subject_does_not_auto_link_second_subject(tmp_path: Path) -> None:
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

    await routing_service.register_chat(chat_id=111, title="Only Registered Group")
    first_subject = await subject_service.create_subject("Agentic PM")
    second_subject = await subject_service.create_subject("Vibe Coding")

    new_routes = await routing_service.ensure_routes_for_subject(subject_name="Vibe Coding")

    assert new_routes == 0
    assert await routing_service.list_routes_for_subject(first_subject.id) == []
    assert await routing_service.list_routes_for_subject(second_subject.id) == []

    await db.close()


@pytest.mark.asyncio
async def test_ensure_routes_preserves_thread_specific_routes(tmp_path: Path) -> None:
    """Thread-specific routes (added via link_subject) coexist with default-thread routes."""
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
    await routing_service.register_chat(chat_id=42, title="Group")
    await routing_service.link_subject("Vibe Coding", chat_id=42, thread_id="7")
    new_routes = await routing_service.ensure_routes_for_subject(subject_name="Vibe Coding")
    # Default-thread route added on top of the existing thread-specific route.
    assert new_routes == 1
    routes = sorted(
        (route.thread_id or "", route.chat_id)
        for route in await routing_service.list_routes_for_subject(subject.id)
    )
    assert routes == [("", 42), ("7", 42)]
    fallback_routes = await routing_service.list_routes_for_chat(chat_id=42, thread_id="99")
    assert [(route.subject_name, route.thread_id) for route in fallback_routes] == [("Vibe Coding", None)]

    await db.close()
