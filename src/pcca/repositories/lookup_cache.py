from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import aiosqlite


@dataclass
class LookupCacheRepository:
    conn: aiosqlite.Connection

    async def get_json(self, key: str, *, ttl_days: int = 30) -> dict[str, Any] | None:
        row = await (
            await self.conn.execute(
                """
                SELECT value_json, updated_at
                FROM source_lookup_cache
                WHERE cache_key = ?
                """,
                (key,),
            )
        ).fetchone()
        if row is None:
            return None

        updated_at = _parse_sqlite_timestamp(str(row["updated_at"]))
        if updated_at is None or datetime.now(timezone.utc) - updated_at > timedelta(days=ttl_days):
            return None

        try:
            payload = json.loads(row["value_json"] or "{}")
        except json.JSONDecodeError:
            return None
        return payload if isinstance(payload, dict) else None

    async def set_json(self, key: str, value: dict[str, Any]) -> None:
        await self.conn.execute(
            """
            INSERT INTO source_lookup_cache(cache_key, value_json, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(cache_key)
            DO UPDATE SET
              value_json = excluded.value_json,
              updated_at = CURRENT_TIMESTAMP
            """,
            (key, json.dumps(value, sort_keys=True)),
        )
        await self.conn.commit()


def _parse_sqlite_timestamp(value: str) -> datetime | None:
    text = value.strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f%z"):
        try:
            parsed = datetime.strptime(text, fmt)
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except ValueError:
            continue
    return None
