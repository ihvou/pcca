from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Awaitable, Callable, Protocol

from pcca.models import Subject
from pcca.repositories.item_scores import CandidateItem


ButtonTokenFactory = Callable[[CandidateItem, str], Awaitable[str]]


@dataclass
class RenderedDigestItem:
    item_id: int
    rank: int
    reason_selected: str


@dataclass
class DeliveryPayload:
    renderer_name: str
    items: list[str]
    item_actions: list[dict] = field(default_factory=list)
    rendered_items: list[RenderedDigestItem] = field(default_factory=list)


@dataclass
class DigestRenderContext:
    digest_id: int
    run_date: date
    create_button_token: ButtonTokenFactory


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


@dataclass
class TelegramDigestRenderer:
    name: str = "telegram_default"

    async def render(
        self,
        *,
        subject: Subject,
        ranked_items: list[CandidateItem],
        context: DigestRenderContext,
    ) -> DeliveryPayload:
        _ = subject, context.run_date
        lines: list[str] = []
        item_actions: list[dict] = []
        rendered_items: list[RenderedDigestItem] = []
        for idx, candidate in enumerate(ranked_items, start=1):
            title = candidate.title_or_text.splitlines()[0][:180] if candidate.title_or_text else "(no title)"
            reason = candidate.rationale or f"score={candidate.final_score:.2f}"
            lines.append(
                f"{idx}. {title}\n"
                f"   via {candidate.author or 'unknown'}\n"
                f"   published: {candidate.published_at or 'unknown'}\n"
                f"   {candidate.url or ''}\n"
                f"   why: {reason}"
            )
            tokens = {
                action: await context.create_button_token(candidate, action)
                for action in ("up", "down", "save")
            }
            item_actions.append({"rank": idx, "tokens": tokens})
            rendered_items.append(
                RenderedDigestItem(
                    item_id=candidate.item_id,
                    rank=idx,
                    reason_selected=reason,
                )
            )
        return DeliveryPayload(
            renderer_name=self.name,
            items=lines,
            item_actions=item_actions,
            rendered_items=rendered_items,
        )
