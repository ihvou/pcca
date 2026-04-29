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
