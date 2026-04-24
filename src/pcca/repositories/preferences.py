from __future__ import annotations

import json
from dataclasses import dataclass

import aiosqlite


@dataclass
class SubjectPreference:
    subject_id: int
    version: int
    include_rules: dict
    exclude_rules: dict
    source_weights: dict
    quality_rules: dict
    updated_at: str


@dataclass
class SubjectPreferenceRepository:
    conn: aiosqlite.Connection

    async def get_latest(self, subject_id: int) -> SubjectPreference | None:
        row = await (
            await self.conn.execute(
                """
                SELECT subject_id, version, include_rules_json, exclude_rules_json, source_weights_json, quality_rules_json, updated_at
                FROM subject_preferences
                WHERE subject_id = ?
                ORDER BY version DESC
                LIMIT 1
                """,
                (subject_id,),
            )
        ).fetchone()
        if row is None:
            return None
        return SubjectPreference(
            subject_id=row["subject_id"],
            version=row["version"],
            include_rules=json.loads(row["include_rules_json"] or "{}"),
            exclude_rules=json.loads(row["exclude_rules_json"] or "{}"),
            source_weights=json.loads(row["source_weights_json"] or "{}"),
            quality_rules=json.loads(row["quality_rules_json"] or "{}"),
            updated_at=row["updated_at"],
        )

    async def append_rules(
        self,
        *,
        subject_id: int,
        include_terms: list[str] | None = None,
        exclude_terms: list[str] | None = None,
    ) -> SubjectPreference:
        current = await self.get_latest(subject_id)
        if current is None:
            include_rules = {"topics": []}
            exclude_rules = {"topics": []}
            source_weights = {}
            quality_rules = {"min_practicality": 0.5, "max_items": 5}
            version = 1
        else:
            include_rules = dict(current.include_rules)
            exclude_rules = dict(current.exclude_rules)
            source_weights = dict(current.source_weights)
            quality_rules = dict(current.quality_rules)
            version = current.version + 1

        merged_include = self._merge_terms(include_rules.get("topics"), include_terms or [])
        merged_exclude = self._merge_terms(exclude_rules.get("topics"), exclude_terms or [])
        include_rules["topics"] = merged_include
        exclude_rules["topics"] = merged_exclude

        await self.conn.execute(
            """
            INSERT INTO subject_preferences(
              subject_id, version, include_rules_json, exclude_rules_json, source_weights_json, quality_rules_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                subject_id,
                version,
                json.dumps(include_rules),
                json.dumps(exclude_rules),
                json.dumps(source_weights),
                json.dumps(quality_rules),
            ),
        )
        await self.conn.commit()
        latest = await self.get_latest(subject_id)
        if latest is None:
            raise RuntimeError("Failed to persist subject preferences.")
        return latest

    @staticmethod
    def _merge_terms(existing: object, new_terms: list[str]) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()

        if isinstance(existing, list):
            for term in existing:
                if not isinstance(term, str):
                    continue
                normalized = term.strip().lower()
                if not normalized or normalized in seen:
                    continue
                seen.add(normalized)
                out.append(normalized)
        for term in new_terms:
            normalized = term.strip().lower()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            out.append(normalized)
        return out
