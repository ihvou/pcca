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


@pytest.mark.asyncio
async def test_telegram_renderer_deep_links_youtube_matched_segment() -> None:
    subject = Subject(
        id=1,
        name="AI Tools",
        telegram_thread_id=None,
        status="active",
        created_at="2026-04-29 00:00:00",
    )
    candidate = CandidateItem(
        item_id=1,
        title_or_text="Anthropic release interview\nWhole episode description",
        url="https://www.youtube.com/watch?v=abc123",
        author="Anthropic",
        published_at="2026-04-29",
        final_score=0.9,
        rationale="matched segment",
        platform="youtube",
        segment_id=10,
        segment_text="Claude Code now supports a practical workflow for agent handoffs.",
        segment_start_seconds=92.0,
        segment_end_seconds=173.0,
    )

    payload = await TelegramDigestRenderer().render(
        subject=subject,
        ranked_items=[candidate],
        context=DigestRenderContext(
            digest_id=1,
            run_date=__import__("datetime").date.today(),
            create_button_token=_token_factory,
        ),
    )

    brief = payload.briefs[0]
    assert "https://www.youtube.com/watch?v=abc123&t=92s" in brief.short_text
    assert "Matched segment:" in brief.short_text
    assert "[01:32-02:53] Claude Code now supports" in brief.short_text
    assert "Whole episode description" not in brief.full_text


@pytest.mark.asyncio
async def test_telegram_renderer_prefixes_apple_podcasts_timestamp_without_deeplink() -> None:
    subject = Subject(
        id=1,
        name="Podcasts",
        telegram_thread_id=None,
        status="active",
        created_at="2026-04-29 00:00:00",
    )
    candidate = CandidateItem(
        item_id=1,
        title_or_text="Bill Gurley episode",
        url="https://podcasts.apple.com/us/podcast/demo/id123?i=456",
        author="Podcast",
        published_at="2026-04-29",
        final_score=0.9,
        rationale="matched segment",
        platform="apple_podcasts",
        segment_id=11,
        segment_text="The useful part explains how the team evaluates agent changes.",
        segment_start_seconds=872.0,
        segment_end_seconds=1028.0,
    )

    payload = await TelegramDigestRenderer().render(
        subject=subject,
        ranked_items=[candidate],
        context=DigestRenderContext(
            digest_id=1,
            run_date=__import__("datetime").date.today(),
            create_button_token=_token_factory,
        ),
    )

    brief = payload.briefs[0]
    assert "https://podcasts.apple.com/us/podcast/demo/id123?i=456" in brief.short_text
    assert "?t=" not in brief.short_text
    assert "[14:32-17:08] The useful part explains" in brief.short_text
