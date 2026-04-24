from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from secrets import token_urlsafe

import aiosqlite


@dataclass
class DigestRow:
    id: int
    subject_id: int
    run_date: str
    sent_at: str | None
    status: str


@dataclass
class DigestItemRow:
    digest_id: int
    item_id: int
    rank: int
    reason_selected: str


@dataclass
class DigestButtonRow:
    token: str
    digest_id: int
    item_id: int
    subject_id: int
    action: str


@dataclass
class DigestRepository:
    conn: aiosqlite.Connection

    async def get_or_create_digest(self, *, subject_id: int, run_date: date) -> DigestRow:
        await self.conn.execute(
            """
            INSERT INTO digests(subject_id, run_date, status)
            VALUES (?, ?, 'pending')
            ON CONFLICT(subject_id, run_date)
            DO NOTHING
            """,
            (subject_id, run_date.isoformat()),
        )
        await self.conn.commit()
        row = await (
            await self.conn.execute(
                """
                SELECT id, subject_id, run_date, sent_at, status
                FROM digests
                WHERE subject_id = ? AND run_date = ?
                """,
                (subject_id, run_date.isoformat()),
            )
        ).fetchone()
        if row is None:
            raise RuntimeError("Digest upsert failed.")
        return DigestRow(
            id=row["id"],
            subject_id=row["subject_id"],
            run_date=row["run_date"],
            sent_at=row["sent_at"],
            status=row["status"],
        )

    async def list_digest_items(self, *, digest_id: int) -> list[DigestItemRow]:
        rows = await (
            await self.conn.execute(
                """
                SELECT digest_id, item_id, rank, reason_selected
                FROM digest_items
                WHERE digest_id = ?
                ORDER BY rank ASC
                """,
                (digest_id,),
            )
        ).fetchall()
        return [
            DigestItemRow(
                digest_id=row["digest_id"],
                item_id=row["item_id"],
                rank=row["rank"],
                reason_selected=row["reason_selected"],
            )
            for row in rows
        ]

    async def add_digest_item(self, *, digest_id: int, item_id: int, rank: int, reason_selected: str) -> None:
        await self.conn.execute(
            """
            INSERT INTO digest_items(digest_id, item_id, rank, reason_selected)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(digest_id, item_id)
            DO UPDATE SET rank = excluded.rank, reason_selected = excluded.reason_selected
            """,
            (digest_id, item_id, rank, reason_selected),
        )
        await self.conn.commit()

    async def create_button_token(self, *, digest_id: int, item_id: int, subject_id: int, action: str) -> str:
        existing = await (
            await self.conn.execute(
                """
                SELECT token
                FROM digest_buttons
                WHERE digest_id = ? AND item_id = ? AND subject_id = ? AND action = ?
                LIMIT 1
                """,
                (digest_id, item_id, subject_id, action),
            )
        ).fetchone()
        if existing is not None:
            return str(existing["token"])

        token = token_urlsafe(8)
        await self.conn.execute(
            """
            INSERT INTO digest_buttons(token, digest_id, item_id, subject_id, action)
            VALUES (?, ?, ?, ?, ?)
            """,
            (token, digest_id, item_id, subject_id, action),
        )
        await self.conn.commit()
        return token

    async def get_button(self, token: str) -> DigestButtonRow | None:
        row = await (
            await self.conn.execute(
                """
                SELECT token, digest_id, item_id, subject_id, action
                FROM digest_buttons
                WHERE token = ?
                """,
                (token,),
            )
        ).fetchone()
        if row is None:
            return None
        return DigestButtonRow(
            token=row["token"],
            digest_id=row["digest_id"],
            item_id=row["item_id"],
            subject_id=row["subject_id"],
            action=row["action"],
        )

    async def mark_sent(self, *, digest_id: int) -> None:
        await self.conn.execute(
            """
            UPDATE digests
            SET status = 'sent', sent_at = COALESCE(sent_at, CURRENT_TIMESTAMP)
            WHERE id = ?
            """,
            (digest_id,),
        )
        await self.conn.commit()

    async def record_delivery(
        self,
        *,
        digest_id: int,
        chat_id: int,
        thread_id: str | None,
        status: str,
        message_id: int | None = None,
        error_text: str | None = None,
    ) -> None:
        thread_key = thread_id or ""
        await self.conn.execute(
            """
            INSERT INTO digest_deliveries(digest_id, chat_id, thread_id, message_id, sent_at, status, error_text)
            VALUES (?, ?, ?, ?, CASE WHEN ? = 'sent' THEN CURRENT_TIMESTAMP ELSE NULL END, ?, ?)
            ON CONFLICT(digest_id, chat_id, thread_id)
            DO UPDATE SET
              message_id = excluded.message_id,
              sent_at = excluded.sent_at,
              status = excluded.status,
              error_text = excluded.error_text
            """,
            (digest_id, chat_id, thread_key, message_id, status, status, error_text),
        )
        await self.conn.commit()
