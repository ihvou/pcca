from __future__ import annotations

from dataclasses import dataclass

from pcca.models import Subject
from pcca.repositories.routing import RoutingRepository, SubjectChatRoute, SubjectRoute, TelegramChat
from pcca.repositories.subjects import SubjectRepository


@dataclass
class RoutingService:
    routing_repo: RoutingRepository
    subject_repo: SubjectRepository

    async def register_chat(self, chat_id: int, title: str | None = None) -> None:
        await self.routing_repo.register_chat(chat_id=chat_id, title=title)

    async def list_registered_chats(self) -> list[TelegramChat]:
        return await self.routing_repo.list_registered_chats()

    async def ensure_routes_for_chat(self, *, chat_id: int, title: str | None = None) -> int:
        """Register a chat (idempotent) and link it to every active subject not already routed.

        Returns the number of new default-thread routes created.

        This remains available for legacy CLI/wizard flows. Telegram `/start` uses
        explicit subject selection so one subject can map cleanly to one group.
        """
        await self.routing_repo.register_chat(chat_id=chat_id, title=title)
        return await self.routing_repo.link_chat_to_all_subjects(chat_id=chat_id)

    async def ensure_routes_for_subject(self, *, subject_name: str) -> int:
        """Link a new subject only when routing is unambiguous.

        The first-run happy path is still low friction: first subject plus one
        registered Telegram chat gets linked automatically. If multiple chats are
        registered, or if this is not the first subject, the user must choose the
        destination via `/start` or the wizard route table to avoid cross-talk.
        """
        subject = await self.subject_repo.get_by_name(subject_name)
        if subject is None:
            raise ValueError(f"Subject not found: {subject_name}")
        subjects = [row for row in await self.subject_repo.list_all() if row.status == "active"]
        chats = await self.routing_repo.list_registered_chats()
        if len(subjects) != 1 or len(chats) != 1:
            return 0
        routes_before = await self.routing_repo.list_routes_for_subject(subject_id=subject.id)
        await self.routing_repo.link_subject_route(subject_id=subject.id, chat_id=chats[0].chat_id)
        routes_after = await self.routing_repo.list_routes_for_subject(subject_id=subject.id)
        return max(0, len(routes_after) - len(routes_before))

    async def link_subject(self, subject_name: str, chat_id: int, thread_id: str | None = None) -> None:
        subject = await self.subject_repo.get_by_name(subject_name)
        if subject is None:
            raise ValueError(f"Subject not found: {subject_name}")
        await self.routing_repo.link_subject_route(subject_id=subject.id, chat_id=chat_id, thread_id=thread_id)

    async def link_subject_id(self, subject_id: int, chat_id: int, thread_id: str | None = None) -> Subject:
        subject = await self.subject_repo.get_by_id(subject_id)
        await self.routing_repo.link_subject_route(subject_id=subject.id, chat_id=chat_id, thread_id=thread_id)
        return subject

    async def list_routes_for_subject(self, subject_id: int) -> list[SubjectRoute]:
        return await self.routing_repo.list_routes_for_subject(subject_id=subject_id)

    async def list_routes_for_chat(self, *, chat_id: int, thread_id: str | None = None) -> list[SubjectChatRoute]:
        return await self.routing_repo.list_routes_for_chat(chat_id=chat_id, thread_id=thread_id)

    async def list_all_routes(self) -> list[SubjectChatRoute]:
        return await self.routing_repo.list_all_routes()

    async def unlink_subject_route(self, *, subject_id: int, chat_id: int, thread_id: str | None = None) -> bool:
        return await self.routing_repo.unlink_subject_route(
            subject_id=subject_id,
            chat_id=chat_id,
            thread_id=thread_id,
        )

    async def move_subject_route(
        self,
        *,
        subject_id: int,
        from_chat_id: int,
        from_thread_id: str | None,
        to_chat_id: int,
        to_thread_id: str | None = None,
    ) -> bool:
        return await self.routing_repo.move_subject_route(
            subject_id=subject_id,
            from_chat_id=from_chat_id,
            from_thread_id=from_thread_id,
            to_chat_id=to_chat_id,
            to_thread_id=to_thread_id,
        )

    async def resolve_subject_for_chat(self, *, chat_id: int, thread_id: str | None) -> Subject | None:
        return await self.routing_repo.resolve_subject_for_chat(chat_id=chat_id, thread_id=thread_id)
