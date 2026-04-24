from __future__ import annotations

from dataclasses import dataclass

import aiosqlite


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
        await self.conn.execute(
            """
            INSERT INTO subject_routes(subject_id, chat_id, thread_id)
            VALUES (?, ?, ?)
            ON CONFLICT(subject_id, chat_id, thread_id)
            DO NOTHING
            """,
            (subject_id, chat_id, thread_id),
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
        return [SubjectRoute(subject_id=row["subject_id"], chat_id=row["chat_id"], thread_id=row["thread_id"]) for row in rows]

