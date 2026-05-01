from __future__ import annotations

import hashlib
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
        changed_item_ids: list[int] = []
        for item in items:
            content_hash = self._content_hash(item)
            exists = await (
                await self.conn.execute(
                    """
                    SELECT id, canonical_url, raw_text, transcript_text, metadata_json, content_hash
                    FROM items
                    WHERE platform = ? AND external_id = ?
                    """,
                    (item.platform, item.external_id),
                )
            ).fetchone()

            if exists is None:
                metadata_json = json.dumps(item.metadata)
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
                        metadata_json,
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
                metadata_json = json.dumps(item.metadata)
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
                            updated_at = CURRENT_TIMESTAMP,
                            content_embedding_json = NULL,
                            content_embedding_model = NULL,
                            content_embedding_text_hash = NULL,
                            content_embedding_updated_at = NULL
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
                            metadata_json,
                            effective_hash,
                            item.platform,
                            item.external_id,
                        ),
                    )
                    updated += 1
                    changed_item_ids.append(int(exists["id"]))
                elif self._has_new_transcript_rows(
                    existing_metadata_json=exists["metadata_json"],
                    incoming_metadata=item.metadata,
                ):
                    await self.conn.execute(
                        """
                        UPDATE items
                        SET metadata_json = ?,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE platform = ? AND external_id = ?
                        """,
                        (
                            metadata_json,
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

    async def list_missing_segment_scores_for_subject(
        self,
        *,
        subject_id: int,
        limit: int | None = None,
    ) -> list[tuple[int, CollectedItem]]:
        limit_clause = "LIMIT ?" if limit is not None and limit > 0 else ""
        params: tuple[int, ...] = (int(subject_id), int(limit)) if limit_clause else (int(subject_id),)
        rows = await (
            await self.conn.execute(
                f"""
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
                WHERE (COALESCE(i.raw_text, '') <> '' OR COALESCE(i.transcript_text, '') <> '')
                  AND NOT EXISTS (
                    SELECT 1
                    FROM item_segment_scores iss
                    WHERE iss.item_id = i.id
                      AND iss.subject_id = ?
                  )
                ORDER BY COALESCE(i.updated_at, i.ingested_at, '') DESC, i.id DESC
                {limit_clause}
                """,
                params,
            )
        ).fetchall()
        return [self._row_to_collected_item(row) for row in rows]

    async def list_all_for_scoring(self, *, limit: int | None = None) -> list[tuple[int, CollectedItem]]:
        limit_clause = "LIMIT ?" if limit is not None and limit > 0 else ""
        params: tuple[int, ...] = (int(limit),) if limit_clause else ()
        rows = await (
            await self.conn.execute(
                f"""
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
                ORDER BY COALESCE(i.updated_at, i.ingested_at, '') DESC, i.id DESC
                {limit_clause}
                """,
                params,
            )
        ).fetchall()
        return [self._row_to_collected_item(row) for row in rows]

    async def count_missing_embeddings(self, *, model: str) -> int:
        row = await (
            await self.conn.execute(
                """
                SELECT COUNT(*) AS c
                FROM items
                WHERE content_embedding_json IS NULL
                   OR content_embedding_model IS NULL
                   OR content_embedding_model != ?
                   OR content_embedding_text_hash IS NULL
                """,
                (model,),
            )
        ).fetchone()
        return int(row["c"] or 0)

    async def list_missing_embeddings(
        self,
        *,
        model: str,
        limit: int,
    ) -> list[tuple[int, CollectedItem]]:
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
                WHERE content_embedding_json IS NULL
                   OR content_embedding_model IS NULL
                   OR content_embedding_model != ?
                   OR content_embedding_text_hash IS NULL
                ORDER BY COALESCE(i.updated_at, i.ingested_at, '') DESC, i.id DESC
                LIMIT ?
                """,
                (model, max(1, int(limit))),
            )
        ).fetchall()
        return [self._row_to_collected_item(row) for row in rows]

    async def get_content_embedding(self, item_id: int, *, model: str) -> list[float] | None:
        return await self.get_content_embedding_for_text(item_id, model=model)

    async def get_content_embedding_for_text(
        self,
        item_id: int,
        *,
        model: str,
        text_hash: str | None = None,
    ) -> list[float] | None:
        row = await (
            await self.conn.execute(
                """
                SELECT content_embedding_json, content_embedding_model, content_embedding_text_hash
                FROM items
                WHERE id = ?
                """,
                (item_id,),
            )
        ).fetchone()
        if row is None or row["content_embedding_model"] != model or not row["content_embedding_json"]:
            return None
        if text_hash is not None and row["content_embedding_text_hash"] != text_hash:
            return None
        try:
            payload = json.loads(row["content_embedding_json"])
        except json.JSONDecodeError:
            return None
        if not isinstance(payload, list):
            return None
        try:
            return [float(value) for value in payload]
        except (TypeError, ValueError):
            return None

    async def save_content_embedding(
        self,
        item_id: int,
        *,
        model: str,
        embedding: list[float],
        text_hash: str | None = None,
    ) -> None:
        await self.conn.execute(
            """
            UPDATE items
            SET content_embedding_json = ?,
                content_embedding_model = ?,
                content_embedding_text_hash = ?,
                content_embedding_updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (json.dumps(embedding), model, text_hash, item_id),
        )
        await self.conn.commit()

    @staticmethod
    def embedding_text(item: CollectedItem) -> str:
        return "\n".join(
            part.strip()
            for part in (
                item.author or "",
                item.text or "",
                item.transcript_text or "",
            )
            if part and part.strip()
        )[:8000]

    @staticmethod
    def embedding_text_hash(text: str) -> str:
        normalized = " ".join((text or "").split()).strip()
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()

    @staticmethod
    def _row_to_collected_item(row) -> tuple[int, CollectedItem]:
        try:
            metadata = json.loads(row["metadata_json"] or "{}")
        except json.JSONDecodeError:
            metadata = {}
        return (
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

    @staticmethod
    def _has_new_transcript_rows(*, existing_metadata_json: str | None, incoming_metadata: dict) -> bool:
        incoming_rows = incoming_metadata.get("transcript_rows") if isinstance(incoming_metadata, dict) else None
        if not isinstance(incoming_rows, list) or not incoming_rows:
            return False
        try:
            existing_metadata = json.loads(existing_metadata_json or "{}")
        except json.JSONDecodeError:
            existing_metadata = {}
        existing_rows = existing_metadata.get("transcript_rows") if isinstance(existing_metadata, dict) else None
        return existing_rows != incoming_rows
