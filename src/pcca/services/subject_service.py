from __future__ import annotations

from dataclasses import dataclass

from pcca.models import Subject
from pcca.repositories.subjects import SubjectRepository


@dataclass
class SubjectService:
    repository: SubjectRepository

    async def create_subject(self, name: str, telegram_thread_id: str | None = None) -> Subject:
        normalized_name = " ".join(name.split()).strip()
        if not normalized_name:
            raise ValueError("Subject name cannot be empty.")

        existing = await self.repository.get_by_name(normalized_name)
        if existing is not None:
            return existing
        return await self.repository.create(normalized_name, telegram_thread_id=telegram_thread_id)

    async def list_subjects(self) -> list[Subject]:
        return await self.repository.list_all()
