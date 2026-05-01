from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import date
from typing import Protocol
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from pcca.models import Subject
from pcca.repositories.item_scores import CandidateItem

EXPAND_BRIEF_ACTION = "__expand_brief__"
DEFAULT_FULL_TEXT_CHARS = 1800
MIN_FULL_TEXT_CHARS = 200
MAX_FULL_TEXT_CHARS = 4000

logger = logging.getLogger(__name__)


class ButtonTokenFactory(Protocol):
    async def __call__(
        self,
        candidate: CandidateItem,
        action: str,
        *,
        label: str | None = None,
        kind: str = "feedback",
    ) -> str:
        ...


@dataclass(frozen=True)
class ButtonShortcut:
    label: str
    text_macro: str
    kind: str = "feedback"


@dataclass(frozen=True)
class BriefButtonPayload:
    label: str
    token: str
    text_macro: str
    kind: str = "feedback"


@dataclass
class BriefPayload:
    item_id: int
    rank: int
    reason_selected: str
    short_text: str
    full_text: str
    buttons: list[BriefButtonPayload] = field(default_factory=list)


@dataclass
class DeliveryPayload:
    renderer_name: str
    briefs: list[BriefPayload]
    footer: str | None = None


@dataclass
class DigestRenderContext:
    digest_id: int
    run_date: date
    create_button_token: ButtonTokenFactory
    button_shortcuts: list[ButtonShortcut] = field(default_factory=list)
    full_text_chars: int = DEFAULT_FULL_TEXT_CHARS

    def resolved_button_shortcuts(self) -> list[ButtonShortcut]:
        return self.button_shortcuts or default_button_shortcuts()


class DigestRenderer(Protocol):
    name: str

    async def render(
        self,
        *,
        subject: Subject,
        ranked_items: list[CandidateItem],
        context: DigestRenderContext,
    ) -> DeliveryPayload:
        ...


def default_button_shortcuts() -> list[ButtonShortcut]:
    return [
        ButtonShortcut(label="👍", text_macro="more like this"),
        ButtonShortcut(label="👎", text_macro="less like this"),
        ButtonShortcut(label="🔖", text_macro="save this brief"),
        ButtonShortcut(label="🚫", text_macro="this is spam or off-topic"),
        ButtonShortcut(label="📖 More", text_macro=EXPAND_BRIEF_ACTION, kind="expand"),
    ]


def parse_button_shortcuts(raw_json: str | None) -> list[ButtonShortcut]:
    if not raw_json:
        return default_button_shortcuts()
    try:
        raw_items = json.loads(raw_json)
    except json.JSONDecodeError:
        return default_button_shortcuts()
    if not isinstance(raw_items, list):
        return default_button_shortcuts()

    shortcuts: list[ButtonShortcut] = []
    for raw in raw_items:
        if not isinstance(raw, dict):
            continue
        label = str(raw.get("label") or "").strip()
        text_macro = str(raw.get("text_macro") or "").strip()
        kind = str(raw.get("kind") or "feedback").strip().lower()
        if not label:
            continue
        if kind in {"more", "expand"}:
            shortcuts.append(ButtonShortcut(label=label, text_macro=EXPAND_BRIEF_ACTION, kind="expand"))
            continue
        if not text_macro:
            continue
        shortcuts.append(ButtonShortcut(label=label, text_macro=text_macro, kind="feedback"))

    if not any(shortcut.kind == "expand" for shortcut in shortcuts):
        shortcuts.append(ButtonShortcut(label="📖 More", text_macro=EXPAND_BRIEF_ACTION, kind="expand"))
    return shortcuts or default_button_shortcuts()


@dataclass
class TelegramDigestRenderer:
    name: str = "telegram_brief_default"

    async def render(
        self,
        *,
        subject: Subject,
        ranked_items: list[CandidateItem],
        context: DigestRenderContext,
    ) -> DeliveryPayload:
        _ = subject, context.run_date
        briefs: list[BriefPayload] = []
        shortcuts = context.resolved_button_shortcuts()
        full_text_chars = clamp_full_text_chars(context.full_text_chars)
        for idx, candidate in enumerate(ranked_items, start=1):
            title = _first_line(candidate.title_or_text)
            reason = candidate.rationale or f"score={candidate.final_score:.2f}"
            metadata_line = _metadata_line(candidate)
            brief_url = _brief_url(candidate)
            url_line = f"\n{brief_url}" if brief_url else ""
            matched_segment = _segment_body(candidate, limit=700)
            short_text = (
                f"Brief {idx}: {title}\n"
                f"{metadata_line}{url_line}\n\n"
                f"Why this matched: {reason}\n\n"
                f"Matched segment:\n{matched_segment}"
            )
            full_text = (
                f"Brief {idx}: {title}\n"
                f"{metadata_line}{url_line}\n\n"
                f"Why this matched: {reason}\n\n"
                f"{_segment_body(candidate, limit=full_text_chars)}"
            )
            buttons: list[BriefButtonPayload] = []
            for shortcut in shortcuts:
                token = await context.create_button_token(
                    candidate,
                    shortcut.text_macro,
                    label=shortcut.label,
                    kind=shortcut.kind,
                )
                buttons.append(
                    BriefButtonPayload(
                        label=shortcut.label,
                        token=token,
                        text_macro=shortcut.text_macro,
                        kind=shortcut.kind,
                    )
                )
            briefs.append(
                BriefPayload(
                    item_id=candidate.item_id,
                    rank=idx,
                    reason_selected=reason,
                    short_text=short_text,
                    full_text=full_text,
                    buttons=buttons,
                )
            )
        return DeliveryPayload(renderer_name=self.name, briefs=briefs)


def _first_line(text: str) -> str:
    first = (text or "").splitlines()[0].strip() if text else ""
    return first[:180] if first else "(no title)"


def clamp_full_text_chars(value: int | None) -> int:
    if value is None:
        return DEFAULT_FULL_TEXT_CHARS
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        logger.warning("Invalid brief_full_text_chars=%r; using default=%d", value, DEFAULT_FULL_TEXT_CHARS)
        return DEFAULT_FULL_TEXT_CHARS
    clamped = max(MIN_FULL_TEXT_CHARS, min(MAX_FULL_TEXT_CHARS, parsed))
    if clamped != parsed:
        logger.warning(
            "Clamped brief_full_text_chars from %d to %d (allowed range %d-%d).",
            parsed,
            clamped,
            MIN_FULL_TEXT_CHARS,
            MAX_FULL_TEXT_CHARS,
        )
    return clamped


def _full_body(text: str, *, limit: int = DEFAULT_FULL_TEXT_CHARS) -> str:
    normalized = "\n".join(line.strip() for line in (text or "").splitlines() if line.strip())
    if not normalized:
        return "(No additional text available.)"
    limit = clamp_full_text_chars(limit)
    if len(normalized) <= limit:
        return normalized
    return normalized[:limit].rstrip() + "\n\n..."


def _metadata_line(candidate: CandidateItem) -> str:
    author = candidate.author or "unknown"
    published = candidate.published_at or "unknown"
    return f"via {author} - published {published}"


def _segment_body(candidate: CandidateItem, *, limit: int) -> str:
    body = candidate.segment_text or candidate.title_or_text
    prefix = _timestamp_prefix(candidate)
    rendered = f"{prefix} {body}".strip() if prefix else body
    return _full_body(rendered, limit=limit)


def _timestamp_prefix(candidate: CandidateItem) -> str | None:
    if candidate.segment_start_seconds is None:
        return None
    start = _format_offset(candidate.segment_start_seconds)
    end = _format_offset(candidate.segment_end_seconds) if candidate.segment_end_seconds is not None else None
    return f"[{start}-{end}]" if end else f"[{start}]"


def _brief_url(candidate: CandidateItem) -> str | None:
    if not candidate.url:
        return None
    if candidate.segment_start_seconds is None:
        return candidate.url
    start = max(0, int(candidate.segment_start_seconds))
    if candidate.platform == "youtube":
        return _url_with_query(candidate.url, {"t": f"{start}s"})
    if candidate.platform == "spotify":
        return _url_with_query(candidate.url, {"t": _format_offset(start)})
    return candidate.url


def _url_with_query(url: str, params: dict[str, str]) -> str:
    parts = urlsplit(url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query.update(params)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def _format_offset(seconds: float | int) -> str:
    total = max(0, int(seconds))
    hours, remainder = divmod(total, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"
