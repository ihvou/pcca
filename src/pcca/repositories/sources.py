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
    is_monitored: bool = True


@dataclass
class SubjectSourceRow:
    source_id: int
    platform: str
    account_or_channel_id: str
    display_name: str
    priority: int
    status: str
    last_crawled_at: str | None
    follow_state: str = "active"
    is_monitored: bool = True


@dataclass
class SourceRepository:
    conn: aiosqlite.Connection

    def _source_row(self, row) -> SourceRow:
        return SourceRow(
            id=row["id"],
            platform=row["platform"],
            account_or_channel_id=row["account_or_channel_id"],
            display_name=row["display_name"],
            follow_state=row["follow_state"],
            last_crawled_at=row["last_crawled_at"],
            is_monitored=bool(row["is_monitored"]),
        )

    def _subject_source_row(self, row) -> SubjectSourceRow:
        return SubjectSourceRow(
            source_id=row["source_id"],
            platform=row["platform"],
            account_or_channel_id=row["account_or_channel_id"],
            display_name=row["display_name"],
            priority=int(row["priority"] or 0),
            status=row["status"],
            last_crawled_at=row["last_crawled_at"],
            follow_state=row["follow_state"],
            is_monitored=bool(row["is_monitored"]),
        )

    async def get_by_identity(self, *, platform: str, account_or_channel_id: str) -> SourceRow | None:
        row = await (
            await self.conn.execute(
                """
                SELECT id, platform, account_or_channel_id, display_name, follow_state, last_crawled_at, is_monitored
                FROM sources
                WHERE platform = ? AND account_or_channel_id = ?
                """,
                (platform, account_or_channel_id),
            )
        ).fetchone()
        if row is None:
            return None
        return self._source_row(row)

    async def create_or_get(
        self,
        platform: str,
        account_or_channel_id: str,
        display_name: str,
        *,
        is_monitored: bool = True,
    ) -> SourceRow:
        existing = await (
            await self.conn.execute(
                """
                SELECT id, platform, account_or_channel_id, display_name, follow_state, last_crawled_at, is_monitored
                FROM sources
                WHERE platform = ? AND account_or_channel_id = ?
                """,
                (platform, account_or_channel_id),
            )
        ).fetchone()
        if existing:
            await self.conn.execute(
                """
                UPDATE sources
                SET display_name = COALESCE(NULLIF(?, ''), display_name),
                    is_monitored = CASE WHEN ? THEN 1 ELSE is_monitored END
                WHERE id = ?
                """,
                (display_name, int(is_monitored), existing["id"]),
            )
            await self.conn.commit()
            refreshed = await (
                await self.conn.execute(
                    """
                    SELECT id, platform, account_or_channel_id, display_name, follow_state, last_crawled_at, is_monitored
                    FROM sources
                    WHERE id = ?
                    """,
                    (existing["id"],),
                )
            ).fetchone()
            return self._source_row(refreshed)

        cursor = await self.conn.execute(
            """
            INSERT INTO sources(platform, account_or_channel_id, display_name, follow_state, is_monitored)
            VALUES (?, ?, ?, 'active', ?)
            """,
            (platform, account_or_channel_id, display_name, int(is_monitored)),
        )
        await self.conn.commit()
        source_id = cursor.lastrowid
        created = await (
            await self.conn.execute(
                """
                SELECT id, platform, account_or_channel_id, display_name, follow_state, last_crawled_at, is_monitored
                FROM sources
                WHERE id = ?
                """,
                (source_id,),
            )
        ).fetchone()
        return self._source_row(created)

    async def mark_monitored(self, source_id: int, monitored: bool = True) -> None:
        await self.conn.execute(
            """
            UPDATE sources
            SET is_monitored = ?
            WHERE id = ?
            """,
            (int(monitored), source_id),
        )
        await self.conn.commit()

    async def link_to_subject(self, subject_id: int, source_id: int, priority: int = 0) -> None:
        await self.conn.execute(
            """
            INSERT INTO subject_sources(subject_id, source_id, priority, status, updated_at)
            VALUES (?, ?, ?, 'active', CURRENT_TIMESTAMP)
            ON CONFLICT(subject_id, source_id)
            DO UPDATE SET
              priority = excluded.priority,
              status = 'active',
              updated_at = CURRENT_TIMESTAMP
            """,
            (subject_id, source_id, priority),
        )
        await self.conn.commit()

    async def unlink_from_subject(self, subject_id: int, source_id: int) -> bool:
        cursor = await self.conn.execute(
            """
            UPDATE subject_sources
            SET status = 'inactive',
                updated_at = CURRENT_TIMESTAMP
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
                SELECT id, platform, account_or_channel_id, display_name, follow_state, last_crawled_at, is_monitored
                FROM sources
                WHERE follow_state = 'needs_reauth'
                ORDER BY platform, display_name
                """
            )
        ).fetchall()
        return [
            self._source_row(row)
            for row in rows
        ]

    async def list_monitored(self, *, active_only: bool = True) -> list[SubjectSourceRow]:
        state_filter = "AND s.follow_state = 'active'" if active_only else ""
        rows = await (
            await self.conn.execute(
                f"""
                SELECT
                  s.id AS source_id,
                  s.platform,
                  s.account_or_channel_id,
                  s.display_name,
                  0 AS priority,
                  'active' AS status,
                  s.last_crawled_at,
                  s.follow_state,
                  s.is_monitored
                FROM sources s
                WHERE s.is_monitored = 1
                  {state_filter}
                ORDER BY s.platform ASC, s.display_name ASC
                """
            )
        ).fetchall()
        return [self._subject_source_row(row) for row in rows]

    async def list_for_subject(self, subject_id: int) -> list[SubjectSourceRow]:
        rows = await (
            await self.conn.execute(
                """
                SELECT
                  s.id AS source_id,
                  s.platform,
                  s.account_or_channel_id,
                  s.display_name,
                  COALESCE(ss.priority, 0) AS priority,
                  COALESCE(ss.status, 'active') AS status,
                  s.last_crawled_at,
                  s.follow_state,
                  s.is_monitored
                FROM sources s
                LEFT JOIN subject_sources ss
                  ON ss.source_id = s.id
                 AND ss.subject_id = ?
                WHERE s.is_monitored = 1
                  AND s.follow_state = 'active'
                  AND COALESCE(ss.status, 'active') = 'active'
                ORDER BY COALESCE(ss.priority, 0) DESC, s.display_name ASC
                """,
                (subject_id,),
            )
        ).fetchall()
        return [self._subject_source_row(row) for row in rows]

    async def list_overrides_for_subject(self, subject_id: int) -> list[SubjectSourceRow]:
        rows = await (
            await self.conn.execute(
                """
                SELECT
                  s.id AS source_id,
                  s.platform,
                  s.account_or_channel_id,
                  s.display_name,
                  ss.priority AS priority,
                  ss.status AS status,
                  s.last_crawled_at,
                  s.follow_state,
                  s.is_monitored
                FROM subject_sources ss
                JOIN sources s ON s.id = ss.source_id
                WHERE ss.subject_id = ?
                ORDER BY ss.status ASC, s.platform ASC, s.display_name ASC
                """,
                (subject_id,),
            )
        ).fetchall()
        return [self._subject_source_row(row) for row in rows]

    async def list_inactive_source_ids_for_subject(self, subject_id: int) -> set[int]:
        rows = await (
            await self.conn.execute(
                """
                SELECT source_id
                FROM subject_sources
                WHERE subject_id = ?
                  AND status <> 'active'
                """,
                (subject_id,),
            )
        ).fetchall()
        return {int(row["source_id"]) for row in rows}
