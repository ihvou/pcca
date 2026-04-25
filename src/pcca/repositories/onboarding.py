from __future__ import annotations

import json
from dataclasses import dataclass

import aiosqlite


@dataclass
class OnboardingImportedSource:
    id: int
    platform: str
    account_or_channel_id: str
    display_name: str
    raw_source: str
    status: str


@dataclass
class OnboardingState:
    current_step: str
    timezone: str | None
    digest_time: str | None
    telegram_verified: bool
    subject_name: str | None
    include_terms: list[str]
    exclude_terms: list[str]
    high_quality_examples: str | None
    completed_at: str | None


@dataclass
class OnboardingRepository:
    conn: aiosqlite.Connection

    async def get_state(self) -> OnboardingState:
        await self.conn.execute("INSERT INTO onboarding_state(id) VALUES (1) ON CONFLICT(id) DO NOTHING")
        await self.conn.commit()
        row = await (
            await self.conn.execute(
                """
                SELECT current_step, timezone, digest_time, telegram_verified, subject_name,
                       include_terms_json, exclude_terms_json, high_quality_examples, completed_at
                FROM onboarding_state
                WHERE id = 1
                """
            )
        ).fetchone()
        if row is None:
            raise RuntimeError("Onboarding state row is unavailable.")
        return OnboardingState(
            current_step=row["current_step"],
            timezone=row["timezone"],
            digest_time=row["digest_time"],
            telegram_verified=bool(row["telegram_verified"]),
            subject_name=row["subject_name"],
            include_terms=json.loads(row["include_terms_json"] or "[]"),
            exclude_terms=json.loads(row["exclude_terms_json"] or "[]"),
            high_quality_examples=row["high_quality_examples"],
            completed_at=row["completed_at"],
        )

    async def update_state(
        self,
        *,
        current_step: str | None = None,
        timezone: str | None = None,
        digest_time: str | None = None,
        telegram_verified: bool | None = None,
        subject_name: str | None = None,
        include_terms: list[str] | None = None,
        exclude_terms: list[str] | None = None,
        high_quality_examples: str | None = None,
        completed: bool = False,
    ) -> None:
        existing = await self.get_state()
        await self.conn.execute(
            """
            UPDATE onboarding_state
            SET current_step = ?,
                timezone = ?,
                digest_time = ?,
                telegram_verified = ?,
                subject_name = ?,
                include_terms_json = ?,
                exclude_terms_json = ?,
                high_quality_examples = ?,
                completed_at = CASE WHEN ? THEN CURRENT_TIMESTAMP ELSE completed_at END,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = 1
            """,
            (
                current_step or existing.current_step,
                timezone if timezone is not None else existing.timezone,
                digest_time if digest_time is not None else existing.digest_time,
                int(telegram_verified if telegram_verified is not None else existing.telegram_verified),
                subject_name if subject_name is not None else existing.subject_name,
                json.dumps(include_terms if include_terms is not None else existing.include_terms),
                json.dumps(exclude_terms if exclude_terms is not None else existing.exclude_terms),
                high_quality_examples if high_quality_examples is not None else existing.high_quality_examples,
                int(completed),
            ),
        )
        await self.conn.commit()

    async def stage_source(
        self,
        *,
        platform: str,
        account_or_channel_id: str,
        display_name: str,
        raw_source: str,
    ) -> None:
        await self.conn.execute(
            """
            INSERT INTO onboarding_imported_sources(platform, account_or_channel_id, display_name, raw_source, status)
            VALUES (?, ?, ?, ?, 'pending')
            ON CONFLICT(platform, account_or_channel_id)
            DO UPDATE SET
              display_name = excluded.display_name,
              raw_source = excluded.raw_source,
              status = 'pending'
            """,
            (platform, account_or_channel_id, display_name, raw_source),
        )
        await self.conn.commit()

    async def list_sources(self, *, status: str | None = "pending") -> list[OnboardingImportedSource]:
        if status is None:
            rows = await (
                await self.conn.execute(
                    """
                    SELECT id, platform, account_or_channel_id, display_name, raw_source, status
                    FROM onboarding_imported_sources
                    ORDER BY platform, display_name
                    """
                )
            ).fetchall()
        else:
            rows = await (
                await self.conn.execute(
                    """
                    SELECT id, platform, account_or_channel_id, display_name, raw_source, status
                    FROM onboarding_imported_sources
                    WHERE status = ?
                    ORDER BY platform, display_name
                    """,
                    (status,),
                )
            ).fetchall()
        return [
            OnboardingImportedSource(
                id=row["id"],
                platform=row["platform"],
                account_or_channel_id=row["account_or_channel_id"],
                display_name=row["display_name"],
                raw_source=row["raw_source"],
                status=row["status"],
            )
            for row in rows
        ]

    async def mark_removed(self, source_id: int) -> bool:
        cursor = await self.conn.execute(
            """
            UPDATE onboarding_imported_sources
            SET status = 'removed'
            WHERE id = ? AND status = 'pending'
            """,
            (source_id,),
        )
        await self.conn.commit()
        return int(cursor.rowcount or 0) > 0

    async def mark_confirmed(self, source_ids: list[int]) -> None:
        if not source_ids:
            return
        placeholders = ",".join("?" for _ in source_ids)
        await self.conn.execute(
            f"""
            UPDATE onboarding_imported_sources
            SET status = 'confirmed'
            WHERE id IN ({placeholders})
            """,
            tuple(source_ids),
        )
        await self.conn.commit()
