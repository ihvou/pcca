from __future__ import annotations

from dataclasses import dataclass

import aiosqlite


@dataclass
class FeedbackRepository:
    conn: aiosqlite.Connection

    async def add_feedback(
        self,
        *,
        subject_id: int,
        feedback_type: str,
        comment_text: str | None = None,
        item_id: int | None = None,
    ) -> None:
        await self.conn.execute(
            """
            INSERT INTO feedback_events(subject_id, item_id, feedback_type, comment_text)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(subject_id, item_id, feedback_type)
            WHERE item_id IS NOT NULL
            DO NOTHING
            """,
            (subject_id, item_id, feedback_type, comment_text),
        )
        await self.conn.commit()
