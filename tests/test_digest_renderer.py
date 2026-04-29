from __future__ import annotations

import pytest

from pcca.digest_renderer import DigestRenderContext, TelegramDigestRenderer
from pcca.models import Subject
from pcca.repositories.item_scores import CandidateItem


async def _token_factory(candidate, action, *, label=None, kind="feedback") -> str:
    _ = candidate, action, label, kind
    return "token"


@pytest.mark.asyncio
async def test_telegram_renderer_uses_per_subject_full_text_cap() -> None:
    subject = Subject(
        id=1,
        name="Long Form",
        telegram_thread_id=None,
        status="active",
        created_at="2026-04-29 00:00:00",
        brief_full_text_chars=3500,
    )
    candidate = CandidateItem(
        item_id=1,
        title_or_text="A" * 3200,
        url=None,
        author="Author",
        published_at="2026-04-29",
        final_score=0.9,
        rationale="matched",
    )

    payload = await TelegramDigestRenderer().render(
        subject=subject,
        ranked_items=[candidate],
        context=DigestRenderContext(
            digest_id=1,
            run_date=__import__("datetime").date.today(),
            create_button_token=_token_factory,
            full_text_chars=subject.brief_full_text_chars,
        ),
    )

    assert "A" * 3000 in payload.briefs[0].full_text
    assert "\n\n..." not in payload.briefs[0].full_text


@pytest.mark.asyncio
async def test_telegram_renderer_clamps_full_text_cap() -> None:
    subject = Subject(
        id=1,
        name="Tiny",
        telegram_thread_id=None,
        status="active",
        created_at="2026-04-29 00:00:00",
        brief_full_text_chars=20,
    )
    candidate = CandidateItem(
        item_id=1,
        title_or_text="B" * 600,
        url=None,
        author="Author",
        published_at="2026-04-29",
        final_score=0.9,
        rationale="matched",
    )

    payload = await TelegramDigestRenderer().render(
        subject=subject,
        ranked_items=[candidate],
        context=DigestRenderContext(
            digest_id=1,
            run_date=__import__("datetime").date.today(),
            create_button_token=_token_factory,
            full_text_chars=subject.brief_full_text_chars,
        ),
    )

    assert "B" * 200 in payload.briefs[0].full_text
    assert "B" * 250 not in payload.briefs[0].full_text
    assert "\n\n..." in payload.briefs[0].full_text
