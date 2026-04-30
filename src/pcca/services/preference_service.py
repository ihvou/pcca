from __future__ import annotations

from dataclasses import dataclass

from pcca.repositories.preferences import SubjectPreference, SubjectPreferenceRepository
from pcca.repositories.subjects import SubjectRepository


@dataclass
class PreferenceService:
    preference_repo: SubjectPreferenceRepository
    subject_repo: SubjectRepository

    async def get_preferences_for_subject(self, subject_name: str) -> SubjectPreference:
        subject = await self.subject_repo.get_by_name(subject_name)
        if subject is None:
            raise ValueError(f"Subject not found: {subject_name}")
        return await self.get_preferences_by_subject_id(subject.id)

    async def get_preferences_by_subject_id(self, subject_id: int) -> SubjectPreference:
        pref = await self.preference_repo.get_latest(subject_id)
        if pref is None:
            pref = await self.preference_repo.append_rules(subject_id=subject_id)
        return pref

    async def refine_subject_rules(
        self,
        *,
        subject_name: str,
        include_terms: list[str] | None = None,
        exclude_terms: list[str] | None = None,
        quality_notes: str | None = None,
    ) -> SubjectPreference:
        subject = await self.subject_repo.get_by_name(subject_name)
        if subject is None:
            raise ValueError(f"Subject not found: {subject_name}")
        return await self.preference_repo.append_rules(
            subject_id=subject.id,
            include_terms=include_terms or [],
            exclude_terms=exclude_terms or [],
            quality_notes=quality_notes,
        )

    async def replace_subject_rules(
        self,
        *,
        subject_id: int,
        include_terms: list[str] | None = None,
        exclude_terms: list[str] | None = None,
        quality_notes: str | None = None,
    ) -> SubjectPreference:
        await self.subject_repo.get_by_id(subject_id)
        return await self.preference_repo.replace_rules(
            subject_id=subject_id,
            include_terms=include_terms or [],
            exclude_terms=exclude_terms or [],
            quality_notes=quality_notes,
        )
