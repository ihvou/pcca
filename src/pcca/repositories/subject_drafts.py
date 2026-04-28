from __future__ import annotations

import json
from dataclasses import dataclass

import aiosqlite


@dataclass
class SubjectDraft:
    chat_id: int
    title: str
    description_text: str
    include_terms: list[str]
    exclude_terms: list[str]
    quality_notes: str | None
    last_user_message: str
    updated_at: str

    @property
    def rules_json(self) -> dict:
        return {
            "include_terms": self.include_terms,
            "exclude_terms": self.exclude_terms,
            "quality_notes": self.quality_notes,
        }


@dataclass
class SubjectDraftRepository:
    conn: aiosqlite.Connection

    async def get(self, chat_id: int) -> SubjectDraft | None:
        row = await (
            await self.conn.execute(
                """
                SELECT chat_id, title, description_text, extracted_rules_json, last_user_message, updated_at
                FROM pending_subject_drafts
                WHERE chat_id = ?
                  AND updated_at >= datetime('now', '-1 hour')
                """,
                (chat_id,),
            )
        ).fetchone()
        if row is None:
            await self.conn.execute(
                """
                DELETE FROM pending_subject_drafts
                WHERE chat_id = ?
                  AND updated_at < datetime('now', '-1 hour')
                """,
                (chat_id,),
            )
            await self.conn.commit()
            return None
        return self._row_to_draft(row)

    async def upsert(
        self,
        *,
        chat_id: int,
        title: str,
        description_text: str,
        include_terms: list[str],
        exclude_terms: list[str],
        quality_notes: str | None,
        last_user_message: str,
    ) -> SubjectDraft:
        rules = {
            "include_terms": include_terms,
            "exclude_terms": exclude_terms,
            "quality_notes": quality_notes,
        }
        await self.conn.execute(
            """
            INSERT INTO pending_subject_drafts(
              chat_id, title, description_text, extracted_rules_json, last_user_message, updated_at
            ) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(chat_id)
            DO UPDATE SET
              title = excluded.title,
              description_text = excluded.description_text,
              extracted_rules_json = excluded.extracted_rules_json,
              last_user_message = excluded.last_user_message,
              updated_at = CURRENT_TIMESTAMP
            """,
            (
                chat_id,
                title,
                description_text,
                json.dumps(rules),
                last_user_message,
            ),
        )
        await self.conn.commit()
        draft = await self.get(chat_id)
        if draft is None:
            raise RuntimeError("Failed to persist subject draft.")
        return draft

    async def delete(self, chat_id: int) -> None:
        await self.conn.execute("DELETE FROM pending_subject_drafts WHERE chat_id = ?", (chat_id,))
        await self.conn.commit()

    def _row_to_draft(self, row) -> SubjectDraft:
        try:
            rules = json.loads(row["extracted_rules_json"] or "{}")
        except json.JSONDecodeError:
            rules = {}
        include_terms = rules.get("include_terms") if isinstance(rules, dict) else []
        exclude_terms = rules.get("exclude_terms") if isinstance(rules, dict) else []
        quality_notes = rules.get("quality_notes") if isinstance(rules, dict) else None
        return SubjectDraft(
            chat_id=int(row["chat_id"]),
            title=str(row["title"]),
            description_text=str(row["description_text"]),
            include_terms=[str(t) for t in include_terms or [] if str(t).strip()],
            exclude_terms=[str(t) for t in exclude_terms or [] if str(t).strip()],
            quality_notes=str(quality_notes).strip() if quality_notes else None,
            last_user_message=str(row["last_user_message"]),
            updated_at=str(row["updated_at"]),
        )
