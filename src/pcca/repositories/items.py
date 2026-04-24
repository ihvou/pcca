from __future__ import annotations

import json
from dataclasses import dataclass

import aiosqlite

from pcca.collectors.base import CollectedItem


@dataclass
class ItemRepository:
    conn: aiosqlite.Connection

    async def upsert_many(self, items: list[CollectedItem]) -> dict:
        inserted = 0
        updated = 0
        item_ids: list[int] = []
        for item in items:
            exists = await (
                await self.conn.execute(
                    "SELECT id FROM items WHERE platform = ? AND external_id = ?",
                    (item.platform, item.external_id),
                )
            ).fetchone()

            if exists is None:
                cursor = await self.conn.execute(
                    """
                    INSERT INTO items(
                      platform, external_id, canonical_url, author, published_at, raw_text, transcript_text, metadata_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        item.platform,
                        item.external_id,
                        item.url,
                        item.author,
                        item.published_at,
                        item.text,
                        item.transcript_text,
                        json.dumps(item.metadata),
                    ),
                )
                inserted += 1
                item_ids.append(int(cursor.lastrowid))
            else:
                await self.conn.execute(
                    """
                    UPDATE items
                    SET canonical_url = ?, author = ?, published_at = ?, raw_text = ?, transcript_text = ?, metadata_json = ?
                    WHERE platform = ? AND external_id = ?
                    """,
                    (
                        item.url,
                        item.author,
                        item.published_at,
                        item.text,
                        item.transcript_text,
                        json.dumps(item.metadata),
                        item.platform,
                        item.external_id,
                    ),
                )
                updated += 1
                item_ids.append(int(exists["id"]))

        await self.conn.commit()
        return {"inserted": inserted, "updated": updated, "item_ids": item_ids}
