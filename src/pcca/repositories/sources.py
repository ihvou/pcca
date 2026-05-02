from __future__ import annotations

import json
from dataclasses import dataclass, field
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
    metadata: dict = field(default_factory=dict)


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
    metadata: dict = field(default_factory=dict)


@dataclass
class SourceRepository:
    conn: aiosqlite.Connection

    def _metadata(self, row) -> dict:
        try:
            raw = row["metadata_json"] if "metadata_json" in row.keys() else "{}"
        except Exception:
            raw = "{}"
        try:
            metadata = json.loads(raw or "{}")
        except json.JSONDecodeError:
            metadata = {}
        return metadata if isinstance(metadata, dict) else {}

    def _source_row(self, row) -> SourceRow:
        return SourceRow(
            id=row["id"],
            platform=row["platform"],
            account_or_channel_id=row["account_or_channel_id"],
            display_name=row["display_name"],
            follow_state=row["follow_state"],
            last_crawled_at=row["last_crawled_at"],
            is_monitored=bool(row["is_monitored"]),
            metadata=self._metadata(row),
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
            metadata=self._metadata(row),
        )

    async def get_by_identity(self, *, platform: str, account_or_channel_id: str) -> SourceRow | None:
        row = await (
            await self.conn.execute(
                """
                SELECT id, platform, account_or_channel_id, display_name, follow_state,
                       last_crawled_at, is_monitored, metadata_json
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
                SELECT id, platform, account_or_channel_id, display_name, follow_state,
                       last_crawled_at, is_monitored, metadata_json
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
                    SELECT id, platform, account_or_channel_id, display_name, follow_state,
                           last_crawled_at, is_monitored, metadata_json
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
                SELECT id, platform, account_or_channel_id, display_name, follow_state,
                       last_crawled_at, is_monitored, metadata_json
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

    async def update_identity(
        self,
        *,
        source_id: int,
        account_or_channel_id: str,
        display_name: str | None = None,
    ) -> SourceRow | None:
        current = await (
            await self.conn.execute(
                """
                SELECT id, platform, account_or_channel_id, display_name, follow_state,
                       last_crawled_at, is_monitored, metadata_json
                FROM sources
                WHERE id = ?
                """,
                (source_id,),
            )
        ).fetchone()
        if current is None:
            return None
        platform = current["platform"]
        existing = await self.get_by_identity(platform=platform, account_or_channel_id=account_or_channel_id)
        if existing is not None and existing.id != source_id:
            await self.conn.execute(
                """
                INSERT OR IGNORE INTO subject_sources(subject_id, source_id, priority, status, updated_at)
                SELECT subject_id, ?, priority, status, CURRENT_TIMESTAMP
                FROM subject_sources
                WHERE source_id = ?
                """,
                (existing.id, source_id),
            )
            await self.conn.execute("DELETE FROM subject_sources WHERE source_id = ?", (source_id,))
            await self.conn.execute(
                """
                UPDATE sources
                SET is_monitored = 0
                WHERE id = ?
                """,
                (source_id,),
            )
            await self.conn.commit()
            return existing

        await self.conn.execute(
            """
            UPDATE sources
            SET account_or_channel_id = ?,
                display_name = COALESCE(NULLIF(?, ''), display_name)
            WHERE id = ?
            """,
            (account_or_channel_id, display_name or "", source_id),
        )
        await self.conn.commit()
        row = await (
            await self.conn.execute(
                """
                SELECT id, platform, account_or_channel_id, display_name, follow_state,
                       last_crawled_at, is_monitored, metadata_json
                FROM sources
                WHERE id = ?
                """,
                (source_id,),
            )
        ).fetchone()
        return self._source_row(row) if row is not None else None

    async def merge_metadata(self, source_id: int, values: dict) -> dict:
        row = await (
            await self.conn.execute(
                "SELECT metadata_json FROM sources WHERE id = ?",
                (source_id,),
            )
        ).fetchone()
        if row is None:
            return {}
        try:
            metadata = json.loads(row["metadata_json"] or "{}")
        except json.JSONDecodeError:
            metadata = {}
        if not isinstance(metadata, dict):
            metadata = {}
        metadata.update(values)
        await self.conn.execute(
            """
            UPDATE sources
            SET metadata_json = ?
            WHERE id = ?
            """,
            (json.dumps(metadata, sort_keys=True), source_id),
        )
        await self.conn.commit()
        return metadata

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

    async def mark_inactive(self, source_id: int, *, reason: str, details: dict | None = None) -> None:
        metadata = await self.merge_metadata(
            source_id,
            {
                "inactive_reason": reason,
                "inactive_details": details or {},
                "inactive_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        _ = metadata
        await self.conn.execute(
            """
            UPDATE sources
            SET follow_state = 'inactive'
            WHERE id = ?
            """,
            (source_id,),
        )
        await self.conn.commit()

    async def list_all(self) -> list[SourceRow]:
        rows = await (
            await self.conn.execute(
                """
                SELECT id, platform, account_or_channel_id, display_name, follow_state,
                       last_crawled_at, is_monitored, metadata_json
                FROM sources
                ORDER BY platform ASC, display_name ASC
                """
            )
        ).fetchall()
        return [self._source_row(row) for row in rows]

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
                  s.is_monitored,
                  s.metadata_json
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
                  s.is_monitored,
                  s.metadata_json
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
                  s.is_monitored,
                  s.metadata_json
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
