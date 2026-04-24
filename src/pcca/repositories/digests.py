from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import aiosqlite


@dataclass
class DigestRepository:
    conn: aiosqlite.Connection

    async def create_digest(self, *, subject_id: int, run_date: date) -> int:
        cursor = await self.conn.execute(
            """
            INSERT INTO digests(subject_id, run_date, status)
            VALUES (?, ?, 'pending')
            """,
            (subject_id, run_date.isoformat()),
        )
        await self.conn.commit()
        return int(cursor.lastrowid)

    async def add_digest_item(self, *, digest_id: int, item_id: int, rank: int, reason_selected: str) -> None:
        await self.conn.execute(
            """
            INSERT INTO digest_items(digest_id, item_id, rank, reason_selected)
            VALUES (?, ?, ?, ?)
            """,
            (digest_id, item_id, rank, reason_selected),
        )
        await self.conn.commit()

    async def mark_sent(self, *, digest_id: int) -> None:
        await self.conn.execute(
            """
            UPDATE digests
            SET status = 'sent', sent_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (digest_id,),
        )
        await self.conn.commit()

