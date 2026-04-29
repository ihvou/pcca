from __future__ import annotations

import json
from dataclasses import dataclass

import aiosqlite

from pcca.models import Subject


@dataclass
class SubjectRepository:
    conn: aiosqlite.Connection

    async def create(
        self,
        name: str,
        telegram_thread_id: str | None = None,
        *,
        include_terms: list[str] | None = None,
        exclude_terms: list[str] | None = None,
        brief_full_text_chars: int = 1800,
    ) -> Subject:
        cursor = await self.conn.execute(
            """
            INSERT INTO subjects(name, telegram_thread_id, status, brief_full_text_chars)
            VALUES (?, ?, 'active', ?)
            """,
            (name, telegram_thread_id, brief_full_text_chars),
        )
        await self.conn.commit()
        created_id = cursor.lastrowid

        # Seed initial preference version.
        include_topics = self._clean_terms(include_terms or [])
        exclude_topics = self._clean_terms(exclude_terms or [])
        await self.conn.execute(
            """
            INSERT INTO subject_preferences(
              subject_id, version, include_rules_json, exclude_rules_json, source_weights_json, quality_rules_json
            ) VALUES (?, 1, ?, ?, ?, ?)
            """,
            (
                created_id,
                json.dumps({"topics": include_topics, "formats": []}),
                json.dumps({"topics": exclude_topics, "sources": []}),
                json.dumps({}),
                json.dumps({"min_practicality": 0.5, "max_items": 5}),
            ),
        )
        await self.conn.commit()
        return await self.get_by_id(created_id)

    async def list_all(self) -> list[Subject]:
        rows = await (
            await self.conn.execute(
                """
                SELECT id, name, telegram_thread_id, status, created_at, brief_full_text_chars
                FROM subjects
                ORDER BY created_at ASC
                """
            )
        ).fetchall()
        return [
            Subject(
                id=row["id"],
                name=row["name"],
                telegram_thread_id=row["telegram_thread_id"],
                status=row["status"],
                created_at=row["created_at"],
                brief_full_text_chars=int(row["brief_full_text_chars"] or 1800),
            )
            for row in rows
        ]

    async def get_by_id(self, subject_id: int) -> Subject:
        row = await (
            await self.conn.execute(
                """
                SELECT id, name, telegram_thread_id, status, created_at, brief_full_text_chars
                FROM subjects
                WHERE id = ?
                """,
                (subject_id,),
            )
        ).fetchone()
        if row is None:
            raise ValueError(f"Subject {subject_id} not found.")
        return Subject(
            id=row["id"],
            name=row["name"],
            telegram_thread_id=row["telegram_thread_id"],
            status=row["status"],
            created_at=row["created_at"],
            brief_full_text_chars=int(row["brief_full_text_chars"] or 1800),
        )

    async def get_by_name(self, name: str) -> Subject | None:
        row = await (
            await self.conn.execute(
                """
                SELECT id, name, telegram_thread_id, status, created_at, brief_full_text_chars
                FROM subjects
                WHERE LOWER(name) = LOWER(?)
                """,
                (name,),
            )
        ).fetchone()
        if row is None:
            return None
        return Subject(
            id=row["id"],
            name=row["name"],
            telegram_thread_id=row["telegram_thread_id"],
            status=row["status"],
            created_at=row["created_at"],
            brief_full_text_chars=int(row["brief_full_text_chars"] or 1800),
        )

    @staticmethod
    def _clean_terms(terms: list[str]) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        for term in terms:
            normalized = " ".join(str(term).lower().split()).strip(" ,.;:")
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            out.append(normalized)
        return out
