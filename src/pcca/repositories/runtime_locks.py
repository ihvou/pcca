from __future__ import annotations

from dataclasses import dataclass

import aiosqlite


@dataclass
class RuntimeLockRepository:
    conn: aiosqlite.Connection

    async def acquire(self, *, lock_name: str, owner_id: str, ttl_seconds: int) -> bool:
        await self.conn.execute(
            """
            DELETE FROM runtime_locks
            WHERE lock_name = ? AND expires_at <= CURRENT_TIMESTAMP
            """,
            (lock_name,),
        )
        cursor = await self.conn.execute(
            """
            INSERT OR IGNORE INTO runtime_locks(lock_name, owner_id, expires_at)
            VALUES (?, ?, datetime('now', ?))
            """,
            (lock_name, owner_id, f"+{max(1, int(ttl_seconds))} seconds"),
        )
        await self.conn.commit()
        return cursor.rowcount == 1

    async def release(self, *, lock_name: str, owner_id: str) -> None:
        await self.conn.execute(
            """
            DELETE FROM runtime_locks
            WHERE lock_name = ? AND owner_id = ?
            """,
            (lock_name, owner_id),
        )
        await self.conn.commit()

    async def get(self, *, lock_name: str) -> dict | None:
        row = await (
            await self.conn.execute(
                """
                SELECT lock_name, owner_id, acquired_at, expires_at
                FROM runtime_locks
                WHERE lock_name = ?
                """,
                (lock_name,),
            )
        ).fetchone()
        return dict(row) if row is not None else None
