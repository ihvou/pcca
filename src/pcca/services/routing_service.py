from __future__ import annotations

from dataclasses import dataclass

from pcca.repositories.routing import RoutingRepository, SubjectRoute
from pcca.repositories.subjects import SubjectRepository


@dataclass
class RoutingService:
    routing_repo: RoutingRepository
    subject_repo: SubjectRepository

    async def register_chat(self, chat_id: int, title: str | None = None) -> None:
        await self.routing_repo.register_chat(chat_id=chat_id, title=title)

    async def link_subject(self, subject_name: str, chat_id: int, thread_id: str | None = None) -> None:
        subject = await self.subject_repo.get_by_name(subject_name)
        if subject is None:
            raise ValueError(f"Subject not found: {subject_name}")
        await self.routing_repo.link_subject_route(subject_id=subject.id, chat_id=chat_id, thread_id=thread_id)

    async def list_routes_for_subject(self, subject_id: int) -> list[SubjectRoute]:
        return await self.routing_repo.list_routes_for_subject(subject_id=subject_id)

