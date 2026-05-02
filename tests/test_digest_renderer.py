from __future__ import annotations

import pytest

from pcca.digest_renderer import DigestRenderContext, TelegramDigestRenderer, _platform_icon
from pcca.models import Subject
from pcca.repositories.item_scores import CandidateItem


async def _token_factory(candidate, action, *, label=None, kind="feedback") -> str:
    _ = candidate, action, label, kind
    return "token"


@pytest.mark.parametrize(
    ("platform", "icon"),
    [
        ("apple_podcasts", "🎙"),
        ("spotify", "🎙"),
        ("youtube", "📺"),
        ("x", "𝕏"),
        ("linkedin", "💼"),
        ("substack", "📰"),
        ("medium", "📰"),
        ("reddit", "🔗"),
        ("rss", "🔗"),
    ],
)
def test_platform_icon_mapping(platform: str, icon: str) -> None:
    assert _platform_icon(platform) == icon


@pytest.mark.asyncio
async def test_telegram_renderer_escapes_hashtag_for_markdown_v2() -> None:
    """Regression: hashtag must be MarkdownV2-escaped or every Brief send returns
    HTTP 400 'character #' is reserved'. T-107 — see live failure logged
    2026-05-02 12:09:56 against subject AI PM Success Stories item 18."""
    subject = Subject(
        id=1,
        name="AI PM Success Stories",
        telegram_thread_id=None,
        status="active",
        created_at="2026-04-29 00:00:00",
    )
    candidate = CandidateItem(
        item_id=1,
        title_or_text="Cat Wu - How Anthropic moves faster",
        url="https://www.youtube.com/watch?v=abc123",
        author="Lenny's Podcast",
        published_at="2026-04-30",
        final_score=0.88,
        rationale="matched segment",
        platform="youtube",
        segment_id=10,
        segment_text="Anthropic's PM team uses Claude on every workflow.",
        segment_start_seconds=42.0,
        segment_end_seconds=120.0,
        metadata={"duration_seconds": 1800},
        key_message="Anthropic PMs use Claude for scoping and review on every task.",
    )

    payload = await TelegramDigestRenderer().render(
        subject=subject,
        ranked_items=[candidate],
        context=DigestRenderContext(
            digest_id=1,
            run_date=__import__("datetime").date(2026, 5, 2),
            create_button_token=_token_factory,
        ),
    )

    brief = payload.briefs[0]
    # Hashtag must appear escaped: \#AIPMSuccessStories — the backslash before
    # # is what makes the message MarkdownV2-valid. Telegram's hashtag detector
    # still recognizes \#word as a clickable hashtag after MarkdownV2 parsing.
    assert "\\#AIPMSuccessStories" in brief.short_text
    # No unescaped `#` should ever appear (except inside escaped sequences).
    # Strip the literal `\#` occurrences and assert no bare `#` remains.
    cleaned = brief.short_text.replace("\\#", "")
    assert "#" not in cleaned, (
        f"Unescaped # in short_text would break MarkdownV2 send: {brief.short_text!r}"
    )
    # Same check for full_text.
    full_cleaned = brief.full_text.replace("\\#", "")
    assert "#" not in full_cleaned, (
        f"Unescaped # in full_text would break MarkdownV2 send: {brief.full_text!r}"
    )


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
        title_or_text="Short title",
        url=None,
        author="Author",
        published_at="2026-04-29",
        final_score=0.9,
        rationale="matched",
        segment_text="B" * 600,
        key_message="Short summary.",
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
    assert "\n\n\\.\\.\\." in payload.briefs[0].full_text


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
        title_or_text="Anthropic_release *interview*\nWhole episode description",
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
        metadata={"duration_seconds": 3600},
        key_message="Claude Code adds a practical handoff workflow that teams can apply directly.",
    )

    payload = await TelegramDigestRenderer().render(
        subject=subject,
        ranked_items=[candidate],
        context=DigestRenderContext(
            digest_id=1,
            run_date=__import__("datetime").date(2026, 5, 2),
            create_button_token=_token_factory,
        ),
    )

    brief = payload.briefs[0]
    assert "https://www.youtube.com/watch?v=abc123&t=92s" in brief.short_text
    assert "Claude Code adds a practical handoff workflow" in brief.short_text
    assert "Why this matched:" not in brief.short_text
    assert "Matched segment:" not in brief.short_text
    assert "📺 Anthropic — _Anthropic\\_release \\*interview\\*_" in brief.short_text
    assert "\\[01:32\\-02:53\\] · 1:00:00 · 3 days ago" in brief.short_text
    assert "#AITools" in brief.short_text
    assert "Full segment:" in brief.full_text
    assert "Why this matched:" in brief.full_text
    assert "Whole episode description" not in brief.full_text


@pytest.mark.asyncio
async def test_telegram_renderer_youtube_timestamp_regression_1122_seconds() -> None:
    subject = Subject(
        id=1,
        name="AI PM Success Stories",
        telegram_thread_id=None,
        status="active",
        created_at="2026-04-29 00:00:00",
    )
    candidate = CandidateItem(
        item_id=1,
        title_or_text="Demo",
        url="https://www.youtube.com/watch?v=XXX",
        author="Creator",
        published_at="2026-05-01",
        final_score=0.9,
        rationale="matched segment",
        platform="youtube",
        segment_text="Important practical point.",
        segment_start_seconds=1122.0,
    )

    payload = await TelegramDigestRenderer().render(
        subject=subject,
        ranked_items=[candidate],
        context=DigestRenderContext(
            digest_id=1,
            run_date=__import__("datetime").date(2026, 5, 2),
            create_button_token=_token_factory,
        ),
    )

    assert "https://www.youtube.com/watch?v=XXX&t=1122s" in payload.briefs[0].short_text
    assert "#AIPMSuccessStories" in payload.briefs[0].short_text


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
    assert "\\[14:32\\-17:08\\]" in brief.short_text
    assert "The useful part explains" in brief.short_text
