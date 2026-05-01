from __future__ import annotations

import json
from dataclasses import dataclass

import aiosqlite


@dataclass
class CandidateItem:
    item_id: int
    title_or_text: str
    url: str | None
    author: str | None
    published_at: str | None
    final_score: float
    rationale: str
    platform: str | None = None
    segment_id: int | None = None
    segment_text: str | None = None
    segment_start_seconds: float | None = None
    segment_end_seconds: float | None = None
    metadata: dict | None = None
    key_message: str | None = None


@dataclass
class ItemScoreRepository:
    conn: aiosqlite.Connection

    async def upsert_score(
        self,
        *,
        item_id: int,
        subject_id: int,
        pass1_score: float,
        pass2_score: float,
        practicality_score: float,
        novelty_score: float,
        trust_score: float,
        noise_penalty: float,
        final_score: float,
        rationale: str,
        key_message: str | None = None,
    ) -> None:
        await self.conn.execute(
            """
            INSERT INTO item_scores(
              item_id, subject_id, pass1_score, pass2_score, practicality_score, novelty_score, trust_score, noise_penalty, final_score, rationale_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(item_id, subject_id)
            DO UPDATE SET
              pass1_score = excluded.pass1_score,
              pass2_score = excluded.pass2_score,
              practicality_score = excluded.practicality_score,
              novelty_score = excluded.novelty_score,
              trust_score = excluded.trust_score,
              noise_penalty = excluded.noise_penalty,
              final_score = excluded.final_score,
              rationale_json = excluded.rationale_json
            """,
            (
                item_id,
                subject_id,
                pass1_score,
                pass2_score,
                practicality_score,
                novelty_score,
                trust_score,
                noise_penalty,
                final_score,
                json.dumps({"reason": rationale, "key_message": key_message}),
            ),
        )
        await self.conn.commit()

    async def upsert_segment_score(
        self,
        *,
        segment_id: int,
        item_id: int,
        subject_id: int,
        pass1_score: float,
        pass2_score: float,
        practicality_score: float,
        novelty_score: float,
        trust_score: float,
        noise_penalty: float,
        final_score: float,
        rationale: str,
        key_message: str | None = None,
    ) -> None:
        await self.conn.execute(
            """
            INSERT INTO item_segment_scores(
              segment_id, item_id, subject_id, pass1_score, pass2_score, practicality_score, novelty_score, trust_score,
              noise_penalty, final_score, rationale_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(segment_id, subject_id)
            DO UPDATE SET
              item_id = excluded.item_id,
              pass1_score = excluded.pass1_score,
              pass2_score = excluded.pass2_score,
              practicality_score = excluded.practicality_score,
              novelty_score = excluded.novelty_score,
              trust_score = excluded.trust_score,
              noise_penalty = excluded.noise_penalty,
              final_score = excluded.final_score,
              rationale_json = excluded.rationale_json,
              updated_at = CURRENT_TIMESTAMP
            """,
            (
                segment_id,
                item_id,
                subject_id,
                pass1_score,
                pass2_score,
                practicality_score,
                novelty_score,
                trust_score,
                noise_penalty,
                final_score,
                json.dumps({"reason": rationale, "key_message": key_message}),
            ),
        )
        await self.conn.commit()

    async def top_unsent_candidates(self, *, subject_id: int, limit: int = 5) -> list[CandidateItem]:
        rows = await (
            await self.conn.execute(
                """
                SELECT
                  i.id AS item_id,
                  i.platform AS platform,
                  COALESCE(i.raw_text, '') AS title_or_text,
                  i.canonical_url AS url,
                  i.author AS author,
                  i.published_at AS published_at,
                  i.metadata_json AS metadata_json,
                  s.final_score AS final_score,
                  json_extract(s.rationale_json, '$.reason') AS rationale,
                  COALESCE(json_extract(iss_best.rationale_json, '$.key_message'), json_extract(s.rationale_json, '$.key_message')) AS key_message,
                  seg.id AS segment_id,
                  seg.segment_text AS segment_text,
                  seg.start_offset_seconds AS segment_start_seconds,
                  seg.end_offset_seconds AS segment_end_seconds
                FROM item_scores s
                JOIN items i ON i.id = s.item_id
                LEFT JOIN item_segment_scores iss_best
                  ON iss_best.id = (
                    SELECT iss.id
                    FROM item_segment_scores iss
                    WHERE iss.item_id = i.id
                      AND iss.subject_id = s.subject_id
                    ORDER BY iss.final_score DESC, iss.id ASC
                    LIMIT 1
                  )
                LEFT JOIN item_segments seg ON seg.id = iss_best.segment_id
                LEFT JOIN (
                  SELECT di.item_id
                  FROM digest_items di
                  JOIN digests d ON d.id = di.digest_id
                  WHERE d.subject_id = ?
                ) sent ON sent.item_id = i.id
                WHERE s.subject_id = ?
                  AND sent.item_id IS NULL
                ORDER BY s.final_score DESC
                LIMIT ?
                """,
                (subject_id, subject_id, limit),
            )
        ).fetchall()
        return [self._candidate_from_row(row) for row in rows]

    async def top_candidates(self, *, subject_id: int, limit: int = 5) -> list[CandidateItem]:
        rows = await (
            await self.conn.execute(
                """
                SELECT
                  i.id AS item_id,
                  i.platform AS platform,
                  COALESCE(i.raw_text, '') AS title_or_text,
                  i.canonical_url AS url,
                  i.author AS author,
                  i.published_at AS published_at,
                  i.metadata_json AS metadata_json,
                  s.final_score AS final_score,
                  json_extract(s.rationale_json, '$.reason') AS rationale,
                  COALESCE(json_extract(iss_best.rationale_json, '$.key_message'), json_extract(s.rationale_json, '$.key_message')) AS key_message,
                  seg.id AS segment_id,
                  seg.segment_text AS segment_text,
                  seg.start_offset_seconds AS segment_start_seconds,
                  seg.end_offset_seconds AS segment_end_seconds
                FROM item_scores s
                JOIN items i ON i.id = s.item_id
                LEFT JOIN item_segment_scores iss_best
                  ON iss_best.id = (
                    SELECT iss.id
                    FROM item_segment_scores iss
                    WHERE iss.item_id = i.id
                      AND iss.subject_id = s.subject_id
                    ORDER BY iss.final_score DESC, iss.id ASC
                    LIMIT 1
                  )
                LEFT JOIN item_segments seg ON seg.id = iss_best.segment_id
                WHERE s.subject_id = ?
                ORDER BY s.final_score DESC
                LIMIT ?
                """,
                (subject_id, limit),
            )
        ).fetchall()
        return [self._candidate_from_row(row) for row in rows]

    async def candidates_by_item_ids(self, *, subject_id: int, item_ids: list[int]) -> list[CandidateItem]:
        if not item_ids:
            return []
        placeholders = ",".join("?" for _ in item_ids)
        rows = await (
            await self.conn.execute(
                f"""
                SELECT
                  i.id AS item_id,
                  i.platform AS platform,
                  COALESCE(i.raw_text, '') AS title_or_text,
                  i.canonical_url AS url,
                  i.author AS author,
                  i.published_at AS published_at,
                  i.metadata_json AS metadata_json,
                  s.final_score AS final_score,
                  json_extract(s.rationale_json, '$.reason') AS rationale,
                  COALESCE(json_extract(iss_best.rationale_json, '$.key_message'), json_extract(s.rationale_json, '$.key_message')) AS key_message,
                  seg.id AS segment_id,
                  seg.segment_text AS segment_text,
                  seg.start_offset_seconds AS segment_start_seconds,
                  seg.end_offset_seconds AS segment_end_seconds
                FROM item_scores s
                JOIN items i ON i.id = s.item_id
                LEFT JOIN item_segment_scores iss_best
                  ON iss_best.id = (
                    SELECT iss.id
                    FROM item_segment_scores iss
                    WHERE iss.item_id = i.id
                      AND iss.subject_id = s.subject_id
                    ORDER BY iss.final_score DESC, iss.id ASC
                    LIMIT 1
                  )
                LEFT JOIN item_segments seg ON seg.id = iss_best.segment_id
                WHERE s.subject_id = ?
                  AND i.id IN ({placeholders})
                """,
                (subject_id, *item_ids),
            )
        ).fetchall()
        by_id = {
            int(row["item_id"]): self._candidate_from_row(row)
            for row in rows
        }
        return [by_id[item_id] for item_id in item_ids if item_id in by_id]

    @staticmethod
    def _candidate_from_row(row) -> CandidateItem:
        try:
            metadata = json.loads(row["metadata_json"] or "{}")
        except json.JSONDecodeError:
            metadata = {}
        return CandidateItem(
            item_id=int(row["item_id"]),
            title_or_text=row["title_or_text"],
            url=row["url"],
            author=row["author"],
            published_at=row["published_at"],
            final_score=float(row["final_score"] or 0.0),
            rationale=str(row["rationale"] or ""),
            platform=row["platform"],
            segment_id=int(row["segment_id"]) if row["segment_id"] is not None else None,
            segment_text=str(row["segment_text"]) if row["segment_text"] else None,
            segment_start_seconds=float(row["segment_start_seconds"]) if row["segment_start_seconds"] is not None else None,
            segment_end_seconds=float(row["segment_end_seconds"]) if row["segment_end_seconds"] is not None else None,
            metadata=metadata if isinstance(metadata, dict) else {},
            key_message=str(row["key_message"]).strip() if row["key_message"] else None,
        )
