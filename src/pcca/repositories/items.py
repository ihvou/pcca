from __future__ import annotations

import json
import hashlib
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
        changed_item_ids: list[int] = []
        for item in items:
            content_hash = self._content_hash(item)
            exists = await (
                await self.conn.execute(
                    """
                    SELECT id, canonical_url, raw_text, transcript_text, content_hash
                    FROM items
                    WHERE platform = ? AND external_id = ?
                    """,
                    (item.platform, item.external_id),
                )
            ).fetchone()

            if exists is None:
                cursor = await self.conn.execute(
                    """
                    INSERT INTO items(
                      platform, external_id, canonical_url, author, published_at, raw_text,
                      transcript_text, metadata_json, content_hash, ingested_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
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
                        content_hash,
                    ),
                )
                inserted += 1
                item_id = int(cursor.lastrowid)
                item_ids.append(item_id)
                changed_item_ids.append(item_id)
            else:
                effective_text = item.text if item.text and item.text.strip() else exists["raw_text"]
                effective_transcript = (
                    item.transcript_text
                    if item.transcript_text and item.transcript_text.strip()
                    else exists["transcript_text"]
                )
                effective_url = item.url or exists["canonical_url"]
                effective_hash = self._content_hash_values(
                    url=effective_url,
                    text=effective_text,
                    transcript_text=effective_transcript,
                )
                if exists["content_hash"] != effective_hash:
                    await self.conn.execute(
                        """
                        UPDATE items
                        SET canonical_url = COALESCE(?, canonical_url),
                            author = COALESCE(?, author),
                            published_at = COALESCE(?, published_at),
                            raw_text = CASE
                              WHEN ? IS NOT NULL AND LENGTH(TRIM(?)) > 0 THEN ?
                              ELSE raw_text
                            END,
                            transcript_text = CASE
                              WHEN ? IS NOT NULL AND LENGTH(TRIM(?)) > 0 THEN ?
                              ELSE transcript_text
                            END,
                            metadata_json = ?,
                            content_hash = ?,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE platform = ? AND external_id = ?
                        """,
                        (
                            item.url,
                            item.author,
                            item.published_at,
                            item.text,
                            item.text,
                            item.text,
                            item.transcript_text,
                            item.transcript_text,
                            item.transcript_text,
                            json.dumps(item.metadata),
                            effective_hash,
                            item.platform,
                            item.external_id,
                        ),
                    )
                    updated += 1
                    changed_item_ids.append(int(exists["id"]))
                item_ids.append(int(exists["id"]))

        await self.conn.commit()
        return {
            "inserted": inserted,
            "updated": updated,
            "item_ids": item_ids,
            "changed_item_ids": changed_item_ids,
        }

    async def list_unscored_for_subject(self, *, subject_id: int) -> list[tuple[int, CollectedItem]]:
        rows = await (
            await self.conn.execute(
                """
                SELECT
                  i.id,
                  i.platform,
                  i.external_id,
                  i.canonical_url,
                  i.author,
                  i.published_at,
                  i.raw_text,
                  i.transcript_text,
                  i.metadata_json
                FROM items i
                LEFT JOIN item_scores s
                  ON s.item_id = i.id
                 AND s.subject_id = ?
                WHERE s.id IS NULL
                ORDER BY COALESCE(i.updated_at, i.ingested_at, '') DESC, i.id DESC
                """,
                (subject_id,),
            )
        ).fetchall()
        out: list[tuple[int, CollectedItem]] = []
        for row in rows:
            try:
                metadata = json.loads(row["metadata_json"] or "{}")
            except json.JSONDecodeError:
                metadata = {}
            out.append(
                (
                    int(row["id"]),
                    CollectedItem(
                        platform=row["platform"],
                        external_id=row["external_id"],
                        author=row["author"],
                        url=row["canonical_url"],
                        text=row["raw_text"],
                        transcript_text=row["transcript_text"],
                        published_at=row["published_at"],
                        metadata=metadata if isinstance(metadata, dict) else {},
                    ),
                )
            )
        return out

    def _content_hash(self, item: CollectedItem) -> str:
        return self._content_hash_values(
            url=item.url,
            text=item.text,
            transcript_text=item.transcript_text,
        )

    def _content_hash_values(self, *, url: str | None, text: str | None, transcript_text: str | None) -> str:
        payload = "\n".join(
            [
                url or "",
                text or "",
                transcript_text or "",
            ]
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()
