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
            is_monitored=True,
        )
        await self.source_repo.link_to_subject(subject_id=subject.id, source_id=source.id, priority=priority)

    async def monitor_source(
        self,
        *,
        platform: str,
        account_or_channel_id: str,
        display_name: str | None = None,
    ) -> None:
        await self.source_repo.create_or_get(
            platform=platform.strip().lower(),
            account_or_channel_id=account_or_channel_id.strip(),
            display_name=(display_name or account_or_channel_id).strip(),
            is_monitored=True,
        )

    async def list_monitored_sources(self) -> list[SubjectSourceRow]:
        return await self.source_repo.list_monitored(active_only=True)

    async def list_inactive_source_ids_for_subject(self, subject_id: int) -> set[int]:
        return await self.source_repo.list_inactive_source_ids_for_subject(subject_id)

    async def list_sources_for_subject(self, subject_name: str) -> list[SubjectSourceRow]:
        subject = await self.subject_repo.get_by_name(subject_name)
        if subject is None:
            raise ValueError(f"Subject not found: {subject_name}")
        return await self.source_repo.list_for_subject(subject.id)

    async def list_source_overrides_for_subject(self, subject_id: int) -> list[SubjectSourceRow]:
        return await self.source_repo.list_overrides_for_subject(subject_id)

    async def remove_source_from_subject(
        self,
        *,
        subject_name: str,
        platform: str,
        account_or_channel_id: str,
    ) -> bool:
        subject = await self.subject_repo.get_by_name(subject_name)
        if subject is None:
            raise ValueError(f"Subject not found: {subject_name}")
        source = await self.source_repo.get_by_identity(
            platform=platform.strip().lower(),
            account_or_channel_id=account_or_channel_id.strip(),
        )
        if source is None:
            return False
        return await self.source_repo.unlink_from_subject(subject.id, source.id)

    async def mark_platform_active_after_login(self, platform: str) -> int:
        return await self.source_repo.mark_platform_active(platform.strip().lower())

    async def list_sources_needing_reauth(self):
        return await self.source_repo.list_needs_reauth()

    async def mark_source_crawl_success(self, source_id: int) -> None:
        await self.source_repo.mark_crawl_success(source_id)

    async def mark_source_needs_reauth(self, source_id: int) -> None:
        await self.source_repo.mark_needs_reauth(source_id)
