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
class SubjectChatRoute:
    subject_id: int
    subject_name: str
    chat_id: int
    chat_title: str | None
    thread_id: str | None
    created_at: str


@dataclass
class TelegramChat:
    chat_id: int
    title: str | None
    last_seen_at: str


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

    async def list_registered_chats(self) -> list[TelegramChat]:
        rows = await (
            await self.conn.execute(
                """
                SELECT chat_id, title, last_seen_at
                FROM telegram_chats
                ORDER BY last_seen_at DESC, chat_id ASC
                """
            )
        ).fetchall()
        return [
            TelegramChat(
                chat_id=row["chat_id"],
                title=row["title"],
                last_seen_at=row["last_seen_at"],
            )
            for row in rows
        ]

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

    async def list_routes_for_chat(self, chat_id: int, thread_id: str | None = None) -> list[SubjectChatRoute]:
        thread_key = thread_id or ""
        rows = await (
            await self.conn.execute(
                """
                SELECT sr.subject_id,
                       s.name AS subject_name,
                       sr.chat_id,
                       t.title AS chat_title,
                       sr.thread_id,
                       sr.created_at
                FROM subject_routes sr
                JOIN subjects s ON s.id = sr.subject_id
                LEFT JOIN telegram_chats t ON t.chat_id = sr.chat_id
                WHERE sr.chat_id = ?
                  AND s.status = 'active'
                  AND sr.thread_id = ?
                ORDER BY s.name ASC, sr.id ASC
                """,
                (chat_id, thread_key),
            )
        ).fetchall()
        if not rows and thread_id is not None:
            rows = await (
                await self.conn.execute(
                    """
                    SELECT sr.subject_id,
                           s.name AS subject_name,
                           sr.chat_id,
                           t.title AS chat_title,
                           sr.thread_id,
                           sr.created_at
                    FROM subject_routes sr
                    JOIN subjects s ON s.id = sr.subject_id
                    LEFT JOIN telegram_chats t ON t.chat_id = sr.chat_id
                    WHERE sr.chat_id = ?
                      AND s.status = 'active'
                      AND sr.thread_id = ''
                    ORDER BY s.name ASC, sr.id ASC
                    """,
                    (chat_id,),
                )
            ).fetchall()
        return [self._subject_chat_route_from_row(row) for row in rows]

    async def list_all_routes(self) -> list[SubjectChatRoute]:
        rows = await (
            await self.conn.execute(
                """
                SELECT sr.subject_id,
                       s.name AS subject_name,
                       sr.chat_id,
                       t.title AS chat_title,
                       sr.thread_id,
                       sr.created_at
                FROM subject_routes sr
                JOIN subjects s ON s.id = sr.subject_id
                LEFT JOIN telegram_chats t ON t.chat_id = sr.chat_id
                WHERE s.status = 'active'
                ORDER BY s.name ASC, sr.chat_id ASC, sr.thread_id ASC
                """
            )
        ).fetchall()
        return [self._subject_chat_route_from_row(row) for row in rows]

    async def unlink_subject_route(self, subject_id: int, chat_id: int, thread_id: str | None = None) -> bool:
        thread_key = thread_id or ""
        cursor = await self.conn.execute(
            """
            DELETE FROM subject_routes
            WHERE subject_id = ?
              AND chat_id = ?
              AND thread_id = ?
            """,
            (subject_id, chat_id, thread_key),
        )
        await self.conn.commit()
        return bool(cursor.rowcount)

    async def move_subject_route(
        self,
        *,
        subject_id: int,
        from_chat_id: int,
        from_thread_id: str | None,
        to_chat_id: int,
        to_thread_id: str | None = None,
    ) -> bool:
        if from_chat_id == to_chat_id and (from_thread_id or "") == (to_thread_id or ""):
            return False
        old_thread_key = from_thread_id or ""
        existing = await (
            await self.conn.execute(
                """
                SELECT 1
                FROM subject_routes
                WHERE subject_id = ?
                  AND chat_id = ?
                  AND thread_id = ?
                LIMIT 1
                """,
                (subject_id, from_chat_id, old_thread_key),
            )
        ).fetchone()
        if existing is None:
            return False
        await self.link_subject_route(subject_id=subject_id, chat_id=to_chat_id, thread_id=to_thread_id)
        return await self.unlink_subject_route(
            subject_id=subject_id,
            chat_id=from_chat_id,
            thread_id=from_thread_id,
        )

    async def link_chat_to_all_subjects(self, chat_id: int) -> int:
        """Insert default-thread routes for every active subject not already routed to this chat.

        Returns the number of new routes created.
        """
        cursor = await self.conn.execute(
            """
            INSERT INTO subject_routes(subject_id, chat_id, thread_id)
            SELECT s.id, ?, ''
            FROM subjects s
            WHERE s.status = 'active'
              AND NOT EXISTS (
                SELECT 1 FROM subject_routes sr
                WHERE sr.subject_id = s.id
                  AND sr.chat_id = ?
                  AND sr.thread_id = ''
              )
            """,
            (chat_id, chat_id),
        )
        await self.conn.commit()
        return int(cursor.rowcount or 0)

    async def link_subject_to_all_chats(self, subject_id: int) -> int:
        """Insert default-thread routes for every registered chat not already routed to this subject.

        Returns the number of new routes created.
        """
        cursor = await self.conn.execute(
            """
            INSERT INTO subject_routes(subject_id, chat_id, thread_id)
            SELECT ?, t.chat_id, ''
            FROM telegram_chats t
            WHERE NOT EXISTS (
                SELECT 1 FROM subject_routes sr
                WHERE sr.subject_id = ?
                  AND sr.chat_id = t.chat_id
                  AND sr.thread_id = ''
              )
            """,
            (subject_id, subject_id),
        )
        await self.conn.commit()
        return int(cursor.rowcount or 0)

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

    @staticmethod
    def _subject_chat_route_from_row(row) -> SubjectChatRoute:
        return SubjectChatRoute(
            subject_id=row["subject_id"],
            subject_name=row["subject_name"],
            chat_id=row["chat_id"],
            chat_title=row["chat_title"],
            thread_id=row["thread_id"] or None,
            created_at=row["created_at"],
        )
