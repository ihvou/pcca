from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from secrets import token_urlsafe

import aiosqlite


@dataclass
class DigestRow:
    id: int
    subject_id: int
    run_date: str
    sent_at: str | None
    status: str


@dataclass
class DigestItemRow:
    digest_id: int
    item_id: int
    rank: int
    reason_selected: str
    short_text: str | None = None
    full_text: str | None = None


@dataclass
class DigestButtonRow:
    token: str
    digest_id: int
    item_id: int
    subject_id: int
    action: str
    label: str
    kind: str


@dataclass
class DigestBriefViewRow:
    digest_id: int
    item_id: int
    short_text: str
    full_text: str


@dataclass
class DigestItemDeliveryRow:
    digest_id: int
    item_id: int
    subject_id: int
    chat_id: int
    thread_id: str
    message_id: int


@dataclass
class DigestRepository:
    conn: aiosqlite.Connection

    async def get_or_create_digest(self, *, subject_id: int, run_date: date) -> DigestRow:
        await self.conn.execute(
            """
            INSERT INTO digests(subject_id, run_date, status)
            VALUES (?, ?, 'pending')
            ON CONFLICT(subject_id, run_date)
            DO NOTHING
            """,
            (subject_id, run_date.isoformat()),
        )
        await self.conn.commit()
        row = await (
            await self.conn.execute(
                """
                SELECT id, subject_id, run_date, sent_at, status
                FROM digests
                WHERE subject_id = ? AND run_date = ?
                """,
                (subject_id, run_date.isoformat()),
            )
        ).fetchone()
        if row is None:
            raise RuntimeError("Digest upsert failed.")
        return DigestRow(
            id=row["id"],
            subject_id=row["subject_id"],
            run_date=row["run_date"],
            sent_at=row["sent_at"],
            status=row["status"],
        )

    async def delete_digests_for_date(self, *, run_date: date, subject_ids: set[int] | None = None) -> int:
        params: list[object] = [run_date.isoformat()]
        subject_filter = ""
        if subject_ids:
            placeholders = ",".join("?" for _ in subject_ids)
            subject_filter = f" AND subject_id IN ({placeholders})"
            params.extend(sorted(subject_ids))

        rows = await (
            await self.conn.execute(
                f"""
                SELECT id
                FROM digests
                WHERE run_date = ?{subject_filter}
                """,
                tuple(params),
            )
        ).fetchall()
        digest_ids = [int(row["id"]) for row in rows]
        if not digest_ids:
            return 0

        placeholders = ",".join("?" for _ in digest_ids)
        digest_params = tuple(digest_ids)
        # Current schema does not use ON DELETE CASCADE, so dependency rows must
        # be removed explicitly before deleting the digest root rows.
        await self.conn.execute(
            f"DELETE FROM digest_item_deliveries WHERE digest_id IN ({placeholders})",
            digest_params,
        )
        await self.conn.execute(f"DELETE FROM digest_deliveries WHERE digest_id IN ({placeholders})", digest_params)
        await self.conn.execute(f"DELETE FROM digest_buttons WHERE digest_id IN ({placeholders})", digest_params)
        await self.conn.execute(f"DELETE FROM digest_items WHERE digest_id IN ({placeholders})", digest_params)
        await self.conn.execute(f"DELETE FROM digests WHERE id IN ({placeholders})", digest_params)
        await self.conn.commit()
        return len(digest_ids)

    async def get_button_shortcuts_json(self, *, subject_id: int) -> str | None:
        row = await (
            await self.conn.execute(
                """
                SELECT button_shortcuts_json
                FROM subjects
                WHERE id = ?
                """,
                (subject_id,),
            )
        ).fetchone()
        return str(row["button_shortcuts_json"]) if row is not None and row["button_shortcuts_json"] else None

    async def latest_subject_config_updated_at(self, *, subject_id: int) -> str | None:
        pref_row = await (
            await self.conn.execute(
                """
                SELECT MAX(updated_at) AS updated_at
                FROM subject_preferences
                WHERE subject_id = ?
                """,
                (subject_id,),
            )
        ).fetchone()
        source_row = await (
            await self.conn.execute(
                """
                SELECT MAX(updated_at) AS updated_at
                FROM subject_sources
                WHERE subject_id = ?
                """,
                (subject_id,),
            )
        ).fetchone()
        candidates = [
            str(row["updated_at"])
            for row in (pref_row, source_row)
            if row is not None and row["updated_at"]
        ]
        return max(candidates) if candidates else None

    async def subject_preferences_are_empty(self, *, subject_id: int) -> bool:
        row = await (
            await self.conn.execute(
                """
                SELECT include_rules_json, exclude_rules_json
                FROM subject_preferences
                WHERE subject_id = ?
                ORDER BY version DESC
                LIMIT 1
                """,
                (subject_id,),
            )
        ).fetchone()
        if row is None:
            return True
        include_rules = _json_object(row["include_rules_json"])
        exclude_rules = _json_object(row["exclude_rules_json"])
        return not _has_rule_content(include_rules) and not _has_rule_content(exclude_rules)

    async def list_digest_items(self, *, digest_id: int) -> list[DigestItemRow]:
        rows = await (
            await self.conn.execute(
                """
                SELECT digest_id, item_id, rank, reason_selected, short_text, full_text
                FROM digest_items
                WHERE digest_id = ?
                ORDER BY rank ASC
                """,
                (digest_id,),
            )
        ).fetchall()
        return [
            DigestItemRow(
                digest_id=row["digest_id"],
                item_id=row["item_id"],
                rank=row["rank"],
                reason_selected=row["reason_selected"],
                short_text=row["short_text"],
                full_text=row["full_text"],
            )
            for row in rows
        ]

    async def add_digest_item(
        self,
        *,
        digest_id: int,
        item_id: int,
        rank: int,
        reason_selected: str,
        short_text: str | None = None,
        full_text: str | None = None,
    ) -> None:
        await self.conn.execute(
            """
            INSERT INTO digest_items(digest_id, item_id, rank, reason_selected, short_text, full_text)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(digest_id, item_id)
            DO UPDATE SET
              rank = excluded.rank,
              reason_selected = excluded.reason_selected,
              short_text = excluded.short_text,
              full_text = excluded.full_text
            """,
            (digest_id, item_id, rank, reason_selected, short_text, full_text),
        )
        await self.conn.commit()

    async def create_button_token(
        self,
        *,
        digest_id: int,
        item_id: int,
        subject_id: int,
        action: str,
        label: str | None = None,
        kind: str = "feedback",
    ) -> str:
        existing = await (
            await self.conn.execute(
                """
                SELECT token
                FROM digest_buttons
                WHERE digest_id = ? AND item_id = ? AND subject_id = ? AND action = ?
                LIMIT 1
                """,
                (digest_id, item_id, subject_id, action),
            )
        ).fetchone()
        if existing is not None:
            await self.conn.execute(
                """
                UPDATE digest_buttons
                SET label = COALESCE(?, label),
                    kind = COALESCE(?, kind)
                WHERE token = ?
                """,
                (label, kind, existing["token"]),
            )
            await self.conn.commit()
            return str(existing["token"])

        token = token_urlsafe(8)
        await self.conn.execute(
            """
            INSERT INTO digest_buttons(token, digest_id, item_id, subject_id, action, label, kind)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (token, digest_id, item_id, subject_id, action, label or action, kind),
        )
        await self.conn.commit()
        return token

    async def get_button(self, token: str) -> DigestButtonRow | None:
        row = await (
            await self.conn.execute(
                """
                SELECT token, digest_id, item_id, subject_id, action, label, kind
                FROM digest_buttons
                WHERE token = ?
                """,
                (token,),
            )
        ).fetchone()
        if row is None:
            return None
        return DigestButtonRow(
            token=row["token"],
            digest_id=row["digest_id"],
            item_id=row["item_id"],
            subject_id=row["subject_id"],
            action=row["action"],
            label=row["label"] or row["action"],
            kind=row["kind"] or "feedback",
        )

    async def list_buttons_for_item(self, *, digest_id: int, item_id: int) -> list[DigestButtonRow]:
        rows = await (
            await self.conn.execute(
                """
                SELECT token, digest_id, item_id, subject_id, action, label, kind
                FROM digest_buttons
                WHERE digest_id = ? AND item_id = ?
                ORDER BY created_at ASC
                """,
                (digest_id, item_id),
            )
        ).fetchall()
        return [
            DigestButtonRow(
                token=row["token"],
                digest_id=row["digest_id"],
                item_id=row["item_id"],
                subject_id=row["subject_id"],
                action=row["action"],
                label=row["label"] or row["action"],
                kind=row["kind"] or "feedback",
            )
            for row in rows
        ]

    async def get_brief_view(self, *, digest_id: int, item_id: int) -> DigestBriefViewRow | None:
        row = await (
            await self.conn.execute(
                """
                SELECT digest_id, item_id, short_text, full_text
                FROM digest_items
                WHERE digest_id = ? AND item_id = ?
                """,
                (digest_id, item_id),
            )
        ).fetchone()
        if row is None:
            return None
        return DigestBriefViewRow(
            digest_id=row["digest_id"],
            item_id=row["item_id"],
            short_text=row["short_text"] or "",
            full_text=row["full_text"] or row["short_text"] or "",
        )

    async def mark_sent(self, *, digest_id: int) -> None:
        await self.conn.execute(
            """
            UPDATE digests
            SET status = 'sent', sent_at = COALESCE(sent_at, CURRENT_TIMESTAMP)
            WHERE id = ?
            """,
            (digest_id,),
        )
        await self.conn.commit()

    async def record_delivery(
        self,
        *,
        digest_id: int,
        chat_id: int,
        thread_id: str | None,
        status: str,
        message_id: int | None = None,
        error_text: str | None = None,
    ) -> None:
        thread_key = thread_id or ""
        await self.conn.execute(
            """
            INSERT INTO digest_deliveries(digest_id, chat_id, thread_id, message_id, sent_at, status, error_text)
            VALUES (?, ?, ?, ?, CASE WHEN ? = 'sent' THEN CURRENT_TIMESTAMP ELSE NULL END, ?, ?)
            ON CONFLICT(digest_id, chat_id, thread_id)
            DO UPDATE SET
              message_id = excluded.message_id,
              sent_at = excluded.sent_at,
              status = excluded.status,
              error_text = excluded.error_text
            """,
            (digest_id, chat_id, thread_key, message_id, status, status, error_text),
        )
        await self.conn.commit()

    async def record_item_delivery(
        self,
        *,
        digest_id: int,
        item_id: int,
        chat_id: int,
        thread_id: str | None,
        status: str,
        message_id: int | None = None,
        error_text: str | None = None,
    ) -> None:
        thread_key = thread_id or ""
        await self.conn.execute(
            """
            INSERT INTO digest_item_deliveries(
              digest_id, item_id, chat_id, thread_id, message_id, sent_at, status, error_text
            )
            VALUES (?, ?, ?, ?, ?, CASE WHEN ? = 'sent' THEN CURRENT_TIMESTAMP ELSE NULL END, ?, ?)
            """,
            (digest_id, item_id, chat_id, thread_key, message_id, status, status, error_text),
        )
        await self.conn.commit()

    async def find_item_delivery_by_message(
        self,
        *,
        chat_id: int,
        message_id: int,
    ) -> DigestItemDeliveryRow | None:
        row = await (
            await self.conn.execute(
                """
                SELECT
                  did.digest_id,
                  did.item_id,
                  d.subject_id,
                  did.chat_id,
                  did.thread_id,
                  did.message_id
                FROM digest_item_deliveries did
                JOIN digests d ON d.id = did.digest_id
                WHERE did.chat_id = ?
                  AND did.message_id = ?
                  AND did.status = 'sent'
                ORDER BY did.sent_at DESC
                LIMIT 1
                """,
                (chat_id, message_id),
            )
        ).fetchone()
        if row is None:
            return None
        return DigestItemDeliveryRow(
            digest_id=row["digest_id"],
            item_id=row["item_id"],
            subject_id=row["subject_id"],
            chat_id=row["chat_id"],
            thread_id=row["thread_id"],
            message_id=row["message_id"],
        )


def _json_object(raw: str | None) -> dict:
    try:
        payload = json.loads(raw or "{}")
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _has_rule_content(rules: dict) -> bool:
    for value in rules.values():
        if isinstance(value, str) and value.strip():
            return True
        if isinstance(value, list) and any(str(item).strip() for item in value):
            return True
        if isinstance(value, dict) and _has_rule_content(value):
            return True
    return False
