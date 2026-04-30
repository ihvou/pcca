from __future__ import annotations

from dataclasses import dataclass

from pcca.models import Subject
from pcca.repositories.subjects import SubjectRepository


@dataclass
class SubjectService:
    repository: SubjectRepository

    async def create_subject(
        self,
        name: str,
        telegram_thread_id: str | None = None,
        *,
        include_terms: list[str] | None = None,
        exclude_terms: list[str] | None = None,
        quality_notes: str | None = None,
        description_text: str | None = None,
        brief_full_text_chars: int = 1800,
        allow_empty_preferences: bool = False,
    ) -> Subject:
        normalized_name = " ".join(name.split()).strip()
        if not normalized_name:
            raise ValueError("Subject name cannot be empty.")
        cleaned_include = self._clean_terms(include_terms or [])
        cleaned_exclude = self._clean_terms(exclude_terms or [])
        if not allow_empty_preferences and not cleaned_include and not cleaned_exclude:
            raise ValueError(
                "Subject preferences cannot be empty. Describe what to include, what to exclude, "
                "or provide an example of high-quality content before saving the subject."
            )

        existing = await self.repository.get_by_name(normalized_name)
        if existing is not None:
            return existing
        return await self.repository.create(
            normalized_name,
            telegram_thread_id=telegram_thread_id,
            include_terms=cleaned_include,
            exclude_terms=cleaned_exclude,
            quality_notes=quality_notes,
            description_text=description_text,
            brief_full_text_chars=brief_full_text_chars,
        )

    async def list_subjects(self) -> list[Subject]:
        return await self.repository.list_all()

    @staticmethod
    def _clean_terms(terms: list[str]) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        for term in terms:
            normalized = " ".join(str(term).lower().split()).strip(" ,.;:")
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            out.append(normalized)
        return out
