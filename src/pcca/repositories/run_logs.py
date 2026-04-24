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

    async def finish_run(self, run_id: int, status: str, stats: dict) -> None:
        await self.conn.execute(
            """
            UPDATE run_logs
            SET ended_at = CURRENT_TIMESTAMP, status = ?, stats_json = ?
            WHERE id = ?
            """,
            (status, json.dumps(stats), run_id),
        )
        await self.conn.commit()

