from __future__ import annotations

from dataclasses import dataclass

from pcca.repositories.sources import SourceRepository, SubjectSourceRow
from pcca.repositories.subjects import SubjectRepository


@dataclass
class SourceService:
    source_repo: SourceRepository
    subject_repo: SubjectRepository

    async def add_source_to_subject(
        self,
        subject_name: str,
        platform: str,
        account_or_channel_id: str,
        display_name: str | None = None,
        priority: int = 0,
    ) -> None:
        subject = await self.subject_repo.get_by_name(subject_name)
        if subject is None:
            raise ValueError(f"Subject not found: {subject_name}")

        source = await self.source_repo.create_or_get(
            platform=platform.strip().lower(),
            account_or_channel_id=account_or_channel_id.strip(),
            display_name=(display_name or account_or_channel_id).strip(),
        )
        await self.source_repo.link_to_subject(subject_id=subject.id, source_id=source.id, priority=priority)

    async def list_sources_for_subject(self, subject_name: str) -> list[SubjectSourceRow]:
        subject = await self.subject_repo.get_by_name(subject_name)
        if subject is None:
            raise ValueError(f"Subject not found: {subject_name}")
        return await self.source_repo.list_for_subject(subject.id)

