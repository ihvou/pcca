from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

import aiosqlite

from pcca.collectors.base import CollectedItem
from pcca.pipeline.segmenter import SegmentDraft, segment_item


@dataclass
class ItemSegment:
    id: int
    item_id: int
    start_offset: int
    end_offset: int
    text: str
    segment_type: str
    start_offset_seconds: float | None = None
    end_offset_seconds: float | None = None


@dataclass
class ItemSegmentRepository:
    conn: aiosqlite.Connection

    async def ensure_segments(self, *, item_id: int, item: CollectedItem, replace: bool = False) -> list[ItemSegment]:
        if replace:
            await self.delete_segments_for_item(item_id=item_id)
        existing = await self.list_for_item(item_id=item_id)
        if existing:
            return existing
        return await self.replace_segments_for_item(item_id=item_id, item=item)

    async def replace_segments_for_item(self, *, item_id: int, item: CollectedItem) -> list[ItemSegment]:
        await self.delete_segments_for_item(item_id=item_id)
        drafts = segment_item(item)
        for draft in drafts:
            await self._insert_segment(item_id=item_id, draft=draft)
        await self.conn.commit()
        return await self.list_for_item(item_id=item_id)

    async def delete_segments_for_item(self, *, item_id: int) -> None:
        segment_rows = await (
            await self.conn.execute("SELECT id FROM item_segments WHERE item_id = ?", (item_id,))
        ).fetchall()
        segment_ids = [int(row["id"]) for row in segment_rows]
        if segment_ids:
            placeholders = ",".join("?" for _ in segment_ids)
            await self.conn.execute(
                f"DELETE FROM item_segment_scores WHERE segment_id IN ({placeholders})",
                tuple(segment_ids),
            )
        await self.conn.execute("DELETE FROM item_segments WHERE item_id = ?", (item_id,))
        await self.conn.commit()

    async def list_for_item(self, *, item_id: int) -> list[ItemSegment]:
        rows = await (
            await self.conn.execute(
                """
                SELECT id, item_id, start_offset, end_offset, segment_text, segment_type,
                       start_offset_seconds, end_offset_seconds
                FROM item_segments
                WHERE item_id = ?
                ORDER BY id ASC
                """,
                (item_id,),
            )
        ).fetchall()
        return [self._row_to_segment(row) for row in rows]

    async def count_missing_embeddings(self, *, model: str) -> int:
        row = await (
            await self.conn.execute(
                """
                SELECT COUNT(*) AS c
                FROM item_segments
                WHERE embedding_json IS NULL
                   OR embedding_model IS NULL
                   OR embedding_model != ?
                   OR embedding_text_hash IS NULL
                """,
                (model,),
            )
        ).fetchone()
        return int(row["c"] or 0)

    async def list_missing_embeddings(self, *, model: str, limit: int) -> list[ItemSegment]:
        rows = await (
            await self.conn.execute(
                """
                SELECT id, item_id, start_offset, end_offset, segment_text, segment_type,
                       start_offset_seconds, end_offset_seconds
                FROM item_segments
                WHERE embedding_json IS NULL
                   OR embedding_model IS NULL
                   OR embedding_model != ?
                   OR embedding_text_hash IS NULL
                ORDER BY item_id ASC, id ASC
                LIMIT ?
                """,
                (model, max(1, int(limit))),
            )
        ).fetchall()
        return [self._row_to_segment(row) for row in rows]

    async def get_embedding_for_text(self, segment_id: int, *, model: str, text_hash: str | None = None) -> list[float] | None:
        row = await (
            await self.conn.execute(
                """
                SELECT embedding_json, embedding_model, embedding_text_hash
                FROM item_segments
                WHERE id = ?
                """,
                (segment_id,),
            )
        ).fetchone()
        if row is None or row["embedding_model"] != model or not row["embedding_json"]:
            return None
        if text_hash is not None and row["embedding_text_hash"] != text_hash:
            return None
        try:
            payload = json.loads(row["embedding_json"])
        except json.JSONDecodeError:
            return None
        if not isinstance(payload, list):
            return None
        try:
            return [float(value) for value in payload]
        except (TypeError, ValueError):
            return None

    async def save_embedding(
        self,
        segment_id: int,
        *,
        model: str,
        embedding: list[float],
        text_hash: str | None = None,
    ) -> None:
        await self.conn.execute(
            """
            UPDATE item_segments
            SET embedding_json = ?,
                embedding_model = ?,
                embedding_text_hash = ?,
                embedding_updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (json.dumps(embedding), model, text_hash, segment_id),
        )
        await self.conn.commit()

    async def _insert_segment(self, *, item_id: int, draft: SegmentDraft) -> None:
        await self.conn.execute(
            """
            INSERT INTO item_segments(
              item_id, start_offset, end_offset, segment_text, segment_type,
              start_offset_seconds, end_offset_seconds
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                item_id,
                draft.start_offset,
                draft.end_offset,
                draft.text,
                draft.segment_type,
                draft.start_offset_seconds,
                draft.end_offset_seconds,
            ),
        )

    @staticmethod
    def embedding_text(segment: ItemSegment) -> str:
        return segment.text

    @staticmethod
    def embedding_text_hash(text: str) -> str:
        normalized = " ".join((text or "").split()).strip()
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()

    @staticmethod
    def _row_to_segment(row) -> ItemSegment:
        return ItemSegment(
            id=int(row["id"]),
            item_id=int(row["item_id"]),
            start_offset=int(row["start_offset"] or 0),
            end_offset=int(row["end_offset"] or 0),
            text=str(row["segment_text"] or ""),
            segment_type=str(row["segment_type"] or "text_window"),
            start_offset_seconds=float(row["start_offset_seconds"]) if row["start_offset_seconds"] is not None else None,
            end_offset_seconds=float(row["end_offset_seconds"]) if row["end_offset_seconds"] is not None else None,
        )
