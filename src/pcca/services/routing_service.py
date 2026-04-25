from __future__ import annotations

from dataclasses import dataclass

from pcca.models import Subject
from pcca.repositories.routing import RoutingRepository, SubjectRoute
from pcca.repositories.subjects import SubjectRepository


@dataclass
class RoutingService:
    routing_repo: RoutingRepository
    subject_repo: SubjectRepository

    async def register_chat(self, chat_id: int, title: str | None = None) -> None:
        await self.routing_repo.register_chat(chat_id=chat_id, title=title)

    async def ensure_routes_for_chat(self, *, chat_id: int, title: str | None = None) -> int:
        """Register a chat (idempotent) and link it to every active subject not already routed.

        Returns the number of new default-thread routes created.

        This is the canonical entry point on `/start` and any other inbound Telegram
        message: in single-user mode, every subject should be deliverable to every
        chat the user has interacted with.
        """
        await self.routing_repo.register_chat(chat_id=chat_id, title=title)
        return await self.routing_repo.link_chat_to_all_subjects(chat_id=chat_id)

    async def ensure_routes_for_subject(self, *, subject_name: str) -> int:
        """Link a subject to every registered chat (default thread) not already routed.

        Returns the number of new routes created. Use after subject creation so that
        a subject created via the desktop wizard / CLI is immediately deliverable
        without requiring the user to invoke `/start` afterwards.
        """
        subject = await self.subject_repo.get_by_name(subject_name)
        if subject is None:
            raise ValueError(f"Subject not found: {subject_name}")
        return await self.routing_repo.link_subject_to_all_chats(subject_id=subject.id)

    async def link_subject(self, subject_name: str, chat_id: int, thread_id: str | None = None) -> None:
        subject = await self.subject_repo.get_by_name(subject_name)
        if subject is None:
            raise ValueError(f"Subject not found: {subject_name}")
        await self.routing_repo.link_subject_route(subject_id=subject.id, chat_id=chat_id, thread_id=thread_id)

    async def list_routes_for_subject(self, subject_id: int) -> list[SubjectRoute]:
        return await self.routing_repo.list_routes_for_subject(subject_id=subject_id)

    async def resolve_subject_for_chat(self, *, chat_id: int, thread_id: str | None) -> Subject | None:
        return await self.routing_repo.resolve_subject_for_chat(chat_id=chat_id, thread_id=thread_id)
