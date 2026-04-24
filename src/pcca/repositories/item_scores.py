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
                json.dumps({"reason": rationale}),
            ),
        )
        await self.conn.commit()

    async def top_unsent_candidates(self, *, subject_id: int, limit: int = 5) -> list[CandidateItem]:
        rows = await (
            await self.conn.execute(
                """
                SELECT
                  i.id AS item_id,
                  COALESCE(i.raw_text, '') AS title_or_text,
                  i.canonical_url AS url,
                  i.author AS author,
                  i.published_at AS published_at,
                  s.final_score AS final_score,
                  json_extract(s.rationale_json, '$.reason') AS rationale
                FROM item_scores s
                JOIN items i ON i.id = s.item_id
                WHERE s.subject_id = ?
                  AND i.id NOT IN (
                    SELECT di.item_id
                    FROM digest_items di
                    JOIN digests d ON d.id = di.digest_id
                    WHERE d.subject_id = ?
                  )
                ORDER BY s.final_score DESC
                LIMIT ?
                """,
                (subject_id, subject_id, limit),
            )
        ).fetchall()
        return [
            CandidateItem(
                item_id=row["item_id"],
                title_or_text=row["title_or_text"],
                url=row["url"],
                author=row["author"],
                published_at=row["published_at"],
                final_score=float(row["final_score"] or 0.0),
                rationale=str(row["rationale"] or ""),
            )
            for row in rows
        ]

    async def candidates_by_item_ids(self, *, subject_id: int, item_ids: list[int]) -> list[CandidateItem]:
        if not item_ids:
            return []
        placeholders = ",".join("?" for _ in item_ids)
        rows = await (
            await self.conn.execute(
                f"""
                SELECT
                  i.id AS item_id,
                  COALESCE(i.raw_text, '') AS title_or_text,
                  i.canonical_url AS url,
                  i.author AS author,
                  i.published_at AS published_at,
                  s.final_score AS final_score,
                  json_extract(s.rationale_json, '$.reason') AS rationale
                FROM item_scores s
                JOIN items i ON i.id = s.item_id
                WHERE s.subject_id = ?
                  AND i.id IN ({placeholders})
                """,
                (subject_id, *item_ids),
            )
        ).fetchall()
        by_id = {
            int(row["item_id"]): CandidateItem(
                item_id=row["item_id"],
                title_or_text=row["title_or_text"],
                url=row["url"],
                author=row["author"],
                published_at=row["published_at"],
                final_score=float(row["final_score"] or 0.0),
                rationale=str(row["rationale"] or ""),
            )
            for row in rows
        }
        return [by_id[item_id] for item_id in item_ids if item_id in by_id]
