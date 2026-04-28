from __future__ import annotations

from dataclasses import dataclass

from pcca.repositories.digests import DigestBriefViewRow, DigestButtonRow, DigestItemDeliveryRow, DigestRepository
from pcca.repositories.feedback import FeedbackRepository
from pcca.repositories.subjects import SubjectRepository


@dataclass
class FeedbackService:
    feedback_repo: FeedbackRepository
    subject_repo: SubjectRepository
    digest_repo: DigestRepository | None = None

    async def add_feedback_by_subject_name(
        self,
        *,
        subject_name: str,
        feedback_type: str,
        comment_text: str | None = None,
        item_id: int | None = None,
    ) -> None:
        subject = await self.subject_repo.get_by_name(subject_name)
        if subject is None:
            raise ValueError(f"Subject not found: {subject_name}")
        await self.feedback_repo.add_feedback(
            subject_id=subject.id,
            feedback_type=feedback_type,
            comment_text=comment_text,
            item_id=item_id,
        )

    async def add_feedback_by_subject_id(
        self,
        *,
        subject_id: int,
        feedback_type: str,
        comment_text: str | None = None,
        item_id: int | None = None,
    ) -> None:
        await self.feedback_repo.add_feedback(
            subject_id=subject_id,
            feedback_type=feedback_type,
            comment_text=comment_text,
            item_id=item_id,
        )

    async def get_digest_button(self, token: str) -> DigestButtonRow | None:
        if self.digest_repo is None:
            return None
        return await self.digest_repo.get_button(token)

    async def list_digest_buttons_for_item(self, *, digest_id: int, item_id: int) -> list[DigestButtonRow]:
        if self.digest_repo is None:
            return []
        return await self.digest_repo.list_buttons_for_item(digest_id=digest_id, item_id=item_id)

    async def get_digest_brief_view(self, *, digest_id: int, item_id: int) -> DigestBriefViewRow | None:
        if self.digest_repo is None:
            return None
        return await self.digest_repo.get_brief_view(digest_id=digest_id, item_id=item_id)

    async def find_digest_item_by_message(self, *, chat_id: int, message_id: int) -> DigestItemDeliveryRow | None:
        if self.digest_repo is None:
            return None
        return await self.digest_repo.find_item_delivery_by_message(chat_id=chat_id, message_id=message_id)
