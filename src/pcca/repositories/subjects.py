from __future__ import annotations

import hashlib
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
        quality_notes: str | None = None,
        description_text: str | None = None,
        brief_full_text_chars: int = 1800,
    ) -> Subject:
        cursor = await self.conn.execute(
            """
            INSERT INTO subjects(name, telegram_thread_id, status, brief_full_text_chars, description_text)
            VALUES (?, ?, 'active', ?, ?)
            """,
            (name, telegram_thread_id, brief_full_text_chars, description_text),
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
                json.dumps(
                    {
                        "min_practicality": 0.5,
                        "max_items": 5,
                        **({"notes": quality_notes.strip()} if quality_notes and quality_notes.strip() else {}),
                    }
                ),
            ),
        )
        await self.conn.commit()
        return await self.get_by_id(created_id)

    async def update_description(self, subject_id: int, description_text: str | None) -> None:
        await self.conn.execute(
            """
            UPDATE subjects
            SET description_text = COALESCE(NULLIF(?, ''), description_text),
                description_embedding_json = NULL,
                description_embedding_model = NULL,
                description_embedding_updated_at = NULL
            WHERE id = ?
            """,
            ((description_text or "").strip(), subject_id),
        )
        await self.conn.commit()

    async def get_description_text(self, subject_id: int) -> str | None:
        row = await (
            await self.conn.execute(
                "SELECT description_text FROM subjects WHERE id = ?",
                (subject_id,),
            )
        ).fetchone()
        if row is None:
            return None
        value = row["description_text"]
        return str(value).strip() if value else None

    async def get_description_embedding(self, subject_id: int, *, model: str) -> list[float] | None:
        return await self.get_description_embedding_for_text(subject_id, model=model)

    async def get_description_embedding_for_text(
        self,
        subject_id: int,
        *,
        model: str,
        text_hash: str | None = None,
    ) -> list[float] | None:
        row = await (
            await self.conn.execute(
                """
                SELECT description_embedding_json, description_embedding_model, description_embedding_text_hash
                FROM subjects
                WHERE id = ?
                """,
                (subject_id,),
            )
        ).fetchone()
        if row is None or row["description_embedding_model"] != model or not row["description_embedding_json"]:
            return None
        if text_hash is not None and row["description_embedding_text_hash"] != text_hash:
            return None
        try:
            payload = json.loads(row["description_embedding_json"])
        except json.JSONDecodeError:
            return None
        if not isinstance(payload, list):
            return None
        try:
            return [float(value) for value in payload]
        except (TypeError, ValueError):
            return None

    async def save_description_embedding(
        self,
        subject_id: int,
        *,
        model: str,
        embedding: list[float],
        text_hash: str | None = None,
    ) -> None:
        await self.conn.execute(
            """
            UPDATE subjects
            SET description_embedding_json = ?,
                description_embedding_model = ?,
                description_embedding_text_hash = ?,
                description_embedding_updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (json.dumps(embedding), model, text_hash, subject_id),
        )
        await self.conn.commit()

    @staticmethod
    def embedding_text_hash(text: str) -> str:
        normalized = " ".join((text or "").split()).strip()
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()

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
