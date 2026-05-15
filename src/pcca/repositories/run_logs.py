from __future__ import annotations

import json
from dataclasses import dataclass

import aiosqlite


@dataclass
class RunLogRepository:
    conn: aiosqlite.Connection

    async def start_run(self, run_type: str) -> int:
        cursor = await self.conn.execute(
            """
            INSERT INTO run_logs(run_type, status, stats_json)
            VALUES (?, 'running', '{}')
            """,
            (run_type,),
        )
        await self.conn.commit()
        return int(cursor.lastrowid)

    async def finish_run(self, run_id: int, status: str, stats: dict, metadata: dict | None = None) -> None:
        await self.conn.execute(
            """
            UPDATE run_logs
            SET ended_at = CURRENT_TIMESTAMP, status = ?, stats_json = ?, metadata_json = ?
            WHERE id = ?
            """,
            (status, json.dumps(stats), json.dumps(metadata or {}), run_id),
        )
        await self.conn.commit()

    async def latest_run_metadata(self, *, run_type: str) -> dict:
        row = await (
            await self.conn.execute(
                """
                SELECT metadata_json
                FROM run_logs
                WHERE run_type = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (run_type,),
            )
        ).fetchone()
        if row is None:
            return {}
        try:
            payload = json.loads(row["metadata_json"] or "{}")
        except json.JSONDecodeError:
            return {}
        return payload if isinstance(payload, dict) else {}

    async def latest_successful_run_started_within(self, *, run_type: str, seconds: int) -> dict | None:
        modifier = f"-{max(1, int(seconds))} seconds"
        row = await (
            await self.conn.execute(
                """
                SELECT id, run_type, started_at, ended_at, status, stats_json, metadata_json
                FROM run_logs
                WHERE run_type = ?
                  AND status = 'success'
                  AND started_at >= datetime('now', ?)
                ORDER BY id DESC
                LIMIT 1
                """,
                (run_type, modifier),
            )
        ).fetchone()
        if row is None:
            return None
        try:
            stats = json.loads(row["stats_json"] or "{}")
        except json.JSONDecodeError:
            stats = {}
        try:
            metadata = json.loads(row["metadata_json"] or "{}")
        except json.JSONDecodeError:
            metadata = {}
        return {
            "id": int(row["id"]),
            "run_type": row["run_type"],
            "started_at": row["started_at"],
            "ended_at": row["ended_at"],
            "status": row["status"],
            "stats": stats if isinstance(stats, dict) else {},
            "metadata": metadata if isinstance(metadata, dict) else {},
        }
