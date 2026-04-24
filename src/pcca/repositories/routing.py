from __future__ import annotations

from dataclasses import dataclass

import aiosqlite

from pcca.models import Subject


@dataclass
class SubjectRoute:
    subject_id: int
    chat_id: int
    thread_id: str | None


@dataclass
class RoutingRepository:
    conn: aiosqlite.Connection

    async def register_chat(self, chat_id: int, title: str | None = None) -> None:
        await self.conn.execute(
            """
            INSERT INTO telegram_chats(chat_id, title)
            VALUES (?, ?)
            ON CONFLICT(chat_id)
            DO UPDATE SET title = excluded.title, last_seen_at = CURRENT_TIMESTAMP
            """,
            (chat_id, title),
        )
        await self.conn.commit()

    async def link_subject_route(self, subject_id: int, chat_id: int, thread_id: str | None = None) -> None:
        thread_key = thread_id or ""
        await self.conn.execute(
            """
            INSERT INTO subject_routes(subject_id, chat_id, thread_id)
            VALUES (?, ?, ?)
            ON CONFLICT(subject_id, chat_id, thread_id)
            DO NOTHING
            """,
            (subject_id, chat_id, thread_key),
        )
        await self.conn.commit()

    async def list_routes_for_subject(self, subject_id: int) -> list[SubjectRoute]:
        rows = await (
            await self.conn.execute(
                """
                SELECT subject_id, chat_id, thread_id
                FROM subject_routes
                WHERE subject_id = ?
                """,
                (subject_id,),
            )
        ).fetchall()
        return [
            SubjectRoute(
                subject_id=row["subject_id"],
                chat_id=row["chat_id"],
                thread_id=row["thread_id"] or None,
            )
            for row in rows
        ]

    async def resolve_subject_for_chat(
        self,
        *,
        chat_id: int,
        thread_id: str | None,
    ) -> Subject | None:
        thread_key = thread_id or ""
        if thread_id is not None:
            row = await (
                await self.conn.execute(
                    """
                    SELECT s.id, s.name, s.telegram_thread_id, s.status, s.created_at
                    FROM subject_routes sr
                    JOIN subjects s ON s.id = sr.subject_id
                    WHERE sr.chat_id = ? AND sr.thread_id = ?
                    ORDER BY sr.id DESC
                    LIMIT 1
                    """,
                    (chat_id, thread_key),
                )
            ).fetchone()
            if row is not None:
                return Subject(
                    id=row["id"],
                    name=row["name"],
                    telegram_thread_id=row["telegram_thread_id"],
                    status=row["status"],
                    created_at=row["created_at"],
                )

        # Fallback to latest route for this chat without thread pin.
        row = await (
            await self.conn.execute(
                """
                SELECT s.id, s.name, s.telegram_thread_id, s.status, s.created_at
                FROM subject_routes sr
                JOIN subjects s ON s.id = sr.subject_id
                WHERE sr.chat_id = ? AND sr.thread_id = ''
                ORDER BY sr.id DESC
                LIMIT 1
                """,
                (chat_id,),
            )
        ).fetchone()
        if row is None:
            return None
        return Subject(
            id=row["id"],
            name=row["name"],
            telegram_thread_id=row["telegram_thread_id"],
            status=row["status"],
            created_at=row["created_at"],
        )
