from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

import aiosqlite


@dataclass
class SourceRow:
    id: int
    platform: str
    account_or_channel_id: str
    display_name: str
    follow_state: str
    last_crawled_at: str | None


@dataclass
class SubjectSourceRow:
    source_id: int
    platform: str
    account_or_channel_id: str
    display_name: str
    priority: int
    status: str


@dataclass
class SourceRepository:
    conn: aiosqlite.Connection

    async def get_by_identity(self, *, platform: str, account_or_channel_id: str) -> SourceRow | None:
        row = await (
            await self.conn.execute(
                """
                SELECT id, platform, account_or_channel_id, display_name, follow_state, last_crawled_at
                FROM sources
                WHERE platform = ? AND account_or_channel_id = ?
                """,
                (platform, account_or_channel_id),
            )
        ).fetchone()
        if row is None:
            return None
        return SourceRow(
            id=row["id"],
            platform=row["platform"],
            account_or_channel_id=row["account_or_channel_id"],
            display_name=row["display_name"],
            follow_state=row["follow_state"],
            last_crawled_at=row["last_crawled_at"],
        )

    async def create_or_get(self, platform: str, account_or_channel_id: str, display_name: str) -> SourceRow:
        existing = await (
            await self.conn.execute(
                """
                SELECT id, platform, account_or_channel_id, display_name, follow_state, last_crawled_at
                FROM sources
                WHERE platform = ? AND account_or_channel_id = ?
                """,
                (platform, account_or_channel_id),
            )
        ).fetchone()
        if existing:
            return SourceRow(
                id=existing["id"],
                platform=existing["platform"],
                account_or_channel_id=existing["account_or_channel_id"],
                display_name=existing["display_name"],
                follow_state=existing["follow_state"],
                last_crawled_at=existing["last_crawled_at"],
            )

        cursor = await self.conn.execute(
            """
            INSERT INTO sources(platform, account_or_channel_id, display_name, follow_state)
            VALUES (?, ?, ?, 'active')
            """,
            (platform, account_or_channel_id, display_name),
        )
        await self.conn.commit()
        source_id = cursor.lastrowid
        created = await (
            await self.conn.execute(
                """
                SELECT id, platform, account_or_channel_id, display_name, follow_state, last_crawled_at
                FROM sources
                WHERE id = ?
                """,
                (source_id,),
            )
        ).fetchone()
        return SourceRow(
            id=created["id"],
            platform=created["platform"],
            account_or_channel_id=created["account_or_channel_id"],
            display_name=created["display_name"],
            follow_state=created["follow_state"],
            last_crawled_at=created["last_crawled_at"],
        )

    async def link_to_subject(self, subject_id: int, source_id: int, priority: int = 0) -> None:
        await self.conn.execute(
            """
            INSERT INTO subject_sources(subject_id, source_id, priority, status)
            VALUES (?, ?, ?, 'active')
            ON CONFLICT(subject_id, source_id)
            DO UPDATE SET priority = excluded.priority, status = 'active'
            """,
            (subject_id, source_id, priority),
        )
        await self.conn.commit()

    async def unlink_from_subject(self, subject_id: int, source_id: int) -> bool:
        cursor = await self.conn.execute(
            """
            UPDATE subject_sources
            SET status = 'inactive'
            WHERE subject_id = ? AND source_id = ? AND status = 'active'
            """,
            (subject_id, source_id),
        )
        await self.conn.commit()
        return (cursor.rowcount or 0) > 0

    async def mark_crawl_success(self, source_id: int) -> None:
        await self.conn.execute(
            """
            UPDATE sources
            SET follow_state = 'active', last_crawled_at = ?
            WHERE id = ?
            """,
            (datetime.now(timezone.utc).isoformat(), source_id),
        )
        await self.conn.commit()

    async def mark_needs_reauth(self, source_id: int) -> None:
        await self.conn.execute(
            """
            UPDATE sources
            SET follow_state = 'needs_reauth'
            WHERE id = ?
            """,
            (source_id,),
        )
        await self.conn.commit()

    async def mark_platform_active(self, platform: str) -> int:
        cursor = await self.conn.execute(
            """
            UPDATE sources
            SET follow_state = 'active'
            WHERE platform = ? AND follow_state = 'needs_reauth'
            """,
            (platform,),
        )
        await self.conn.commit()
        return int(cursor.rowcount or 0)

    async def list_needs_reauth(self) -> list[SourceRow]:
        rows = await (
            await self.conn.execute(
                """
                SELECT id, platform, account_or_channel_id, display_name, follow_state, last_crawled_at
                FROM sources
                WHERE follow_state = 'needs_reauth'
                ORDER BY platform, display_name
                """
            )
        ).fetchall()
        return [
            SourceRow(
                id=row["id"],
                platform=row["platform"],
                account_or_channel_id=row["account_or_channel_id"],
                display_name=row["display_name"],
                follow_state=row["follow_state"],
                last_crawled_at=row["last_crawled_at"],
            )
            for row in rows
        ]

    async def list_for_subject(self, subject_id: int) -> list[SubjectSourceRow]:
        rows = await (
            await self.conn.execute(
                """
                SELECT
                  s.id AS source_id,
                  s.platform,
                  s.account_or_channel_id,
                  s.display_name,
                  ss.priority,
                  ss.status
                FROM subject_sources ss
                JOIN sources s ON s.id = ss.source_id
                WHERE ss.subject_id = ?
                  AND ss.status = 'active'
                  AND s.follow_state = 'active'
                ORDER BY ss.priority DESC, s.display_name ASC
                """,
                (subject_id,),
            )
        ).fetchall()
        return [
            SubjectSourceRow(
                source_id=row["source_id"],
                platform=row["platform"],
                account_or_channel_id=row["account_or_channel_id"],
                display_name=row["display_name"],
                priority=row["priority"],
                status=row["status"],
            )
            for row in rows
        ]
