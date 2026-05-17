"""Tests for LinkedIn collector — focused on T-138 bot-shaped detection.

These tests exercise the empty-result code path and verify that anti-bot
page signals (React error #418 hydration crash, captcha/checkpoint
redirects) escalate into `BotShapedError` so the orchestrator can apply
the fast-trip circuit breaker AND mark the source for re-auth instead of
silently accumulating empty_legitimate signals."""

from __future__ import annotations

from typing import Any

import pytest

from pcca.collectors.errors import BotShapedError
from pcca.collectors.linkedin_collector import (
    LinkedInCollector,
    LinkedInTimelineCollector,
    _detect_empty_page_reason,
    _detect_bot_shaped_signal,
    _linkedin_collection_urls,
)
from pcca.collectors.linkedin_utils import LINKEDIN_TIMELINE_SOURCE_ID, normalize_linkedin_source_id


class _FakePage:
    """Minimal stand-in for Playwright Page exposing the attributes
    `_detect_bot_shaped_signal` reads. Real Page has many more methods;
    we only model what the function under test touches."""

    def __init__(self, *, url: str = "", debug_events: list[dict] | None = None) -> None:
        self.url = url
        self._pcca_debug_events = debug_events or []


def test_detect_bot_shaped_signal_returns_none_for_clean_page() -> None:
    page = _FakePage(
        url="https://www.linkedin.com/in/melissacote1/recent-activity/all/",
        debug_events=[],
    )
    assert _detect_bot_shaped_signal(page) is None


def test_detect_bot_shaped_signal_picks_react_418_hydration_error() -> None:
    """T-138: this is the exact signature LinkedIn served during run_id=80.
    Live log: `error=Minified React error #418; visit https://react.dev/...`.
    """
    page = _FakePage(
        url="https://www.linkedin.com/in/melissacote1/recent-activity/all/",
        debug_events=[
            {
                "event": "pageerror",
                "platform": "linkedin",
                "error": (
                    "Minified React error #418; visit https://react.dev/"
                    "errors/418?args[]= for the full message or use the "
                    "non-minified dev environment for full errors and "
                    "additional helpful warnings."
                ),
            }
        ],
    )
    signal = _detect_bot_shaped_signal(page)
    assert signal is not None
    assert "418" in signal


def test_detect_bot_shaped_signal_picks_checkpoint_redirect() -> None:
    page = _FakePage(
        url="https://www.linkedin.com/checkpoint/challenge/AgFFFqXX",
        debug_events=[],
    )
    assert _detect_bot_shaped_signal(page) == "/checkpoint/"


def test_detect_bot_shaped_signal_ignores_non_pageerror_events() -> None:
    """Console warnings and requestfailed events are not bot signals
    (LinkedIn pages emit dozens of those during normal operation)."""
    page = _FakePage(
        url="https://www.linkedin.com/in/test/recent-activity/all/",
        debug_events=[
            {"event": "console", "type": "warning", "text": "React error #418 mention from a third-party script"},
            {"event": "requestfailed", "url": "chrome-extension://invalid/"},
            {"event": "response", "url": "https://www.linkedin.com/...", "status": 502},
        ],
    )
    assert _detect_bot_shaped_signal(page) is None


@pytest.mark.asyncio
async def test_detect_empty_page_reason_identifies_follow_interstitial() -> None:
    class _FollowPage:
        async def evaluate(self, _js: str) -> str:
            return "Follow Lenny Rachitsky to see their posts and activity."

    assert await _detect_empty_page_reason(_FollowPage()) == "follow_interstitial"


def test_linkedin_collection_urls_include_creator_posts_fallbacks() -> None:
    urls = _linkedin_collection_urls("in/lennyrachitsky")

    assert urls == [
        "https://www.linkedin.com/in/lennyrachitsky/recent-activity/all/",
        "https://www.linkedin.com/in/lennyrachitsky/posts/",
        "https://www.linkedin.com/in/lennyrachitsky/detail/recent-activity/shares/",
    ]


def test_t156_linkedin_timeline_alias_normalizes_to_my_timeline() -> None:
    assert normalize_linkedin_source_id("linkedin:my-timeline") == LINKEDIN_TIMELINE_SOURCE_ID
    assert normalize_linkedin_source_id("https://www.linkedin.com/feed/") == LINKEDIN_TIMELINE_SOURCE_ID


@pytest.mark.asyncio
async def test_linkedin_collector_raises_bot_shaped_on_react_418(monkeypatch: pytest.MonkeyPatch) -> None:
    """End-to-end shape of T-138: when the page renders no feed AND the
    pageerror event log contains React #418, the collector raises
    BotShapedError instead of returning [].

    Replaces orchestrator-side classification of "empty_legitimate"
    (threshold 25, slow trip) with "bot_shaped" (threshold 5, fast trip).
    Without this, 33 silent-empty LinkedIn sources accumulate without
    surfacing the failure to the user.
    """

    class _SessionManagerSpy:
        async def new_page(self, _platform: str) -> Any:
            return _FakePage(
                url="https://www.linkedin.com/in/melissacote1/recent-activity/all/",
                debug_events=[
                    {
                        "event": "pageerror",
                        "platform": "linkedin",
                        "error": "Minified React error #418; visit https://react.dev/errors/418?args[]=",
                    }
                ],
            )

        async def capture_empty_result_snapshot(self, *_args: Any, **_kwargs: Any) -> None:
            return None

        async def capture_debug_snapshot(self, *_args: Any, **_kwargs: Any) -> None:
            return None

    async def _fake_goto(_url: str, **_kwargs: Any) -> None:
        return None

    async def _fake_wait_for_timeout(_ms: int) -> None:
        return None

    async def _fake_evaluate(_js: str, _max_items: int) -> list:
        return []  # LinkedIn returned no feed items (page was the React-#418 error shell)

    async def _fake_close() -> None:
        return None

    # Build the page returned by session_manager.new_page and patch the
    # methods the collector calls on it.
    spy = _SessionManagerSpy()
    page = await spy.new_page("linkedin")
    page.goto = _fake_goto  # type: ignore[attr-defined]
    page.wait_for_timeout = _fake_wait_for_timeout  # type: ignore[attr-defined]
    page.evaluate = _fake_evaluate  # type: ignore[attr-defined]
    page.close = _fake_close  # type: ignore[attr-defined]

    async def _new_page_returning_spy(_platform: str) -> Any:
        return page

    spy.new_page = _new_page_returning_spy  # type: ignore[method-assign]

    collector = LinkedInCollector(session_manager=spy)  # type: ignore[arg-type]

    with pytest.raises(BotShapedError) as excinfo:
        await collector.collect_from_source("in/melissacote1")

    assert excinfo.value.platform == "linkedin"
    assert excinfo.value.source_id == "in/melissacote1"
    assert "418" in excinfo.value.signal


@pytest.mark.asyncio
async def test_linkedin_collector_returns_empty_when_no_bot_signal(monkeypatch: pytest.MonkeyPatch) -> None:
    """Regression: a legitimately empty profile (no posts, no errors) must
    NOT raise BotShapedError. It returns [] and the orchestrator classifies
    as empty_legitimate (slow threshold). T-138 narrows the bot-shaped
    signal to actual anti-bot signatures; absence of items alone is not a
    bot signal."""

    class _SessionManagerSpy:
        async def new_page(self, _platform: str) -> Any:
            return _FakePage(
                url="https://www.linkedin.com/in/quietprofile/recent-activity/all/",
                debug_events=[],  # Clean page, no errors
            )

        async def capture_empty_result_snapshot(self, *_args: Any, **_kwargs: Any) -> None:
            return None

        async def capture_debug_snapshot(self, *_args: Any, **_kwargs: Any) -> None:
            return None

    async def _fake_goto(_url: str, **_kwargs: Any) -> None:
        return None

    async def _fake_wait_for_timeout(_ms: int) -> None:
        return None

    async def _fake_evaluate(_js: str, _max_items: int) -> list:
        return []

    async def _fake_close() -> None:
        return None

    spy = _SessionManagerSpy()
    page = await spy.new_page("linkedin")
    page.goto = _fake_goto  # type: ignore[attr-defined]
    page.wait_for_timeout = _fake_wait_for_timeout  # type: ignore[attr-defined]
    page.evaluate = _fake_evaluate  # type: ignore[attr-defined]
    page.close = _fake_close  # type: ignore[attr-defined]

    async def _new_page_returning_spy(_platform: str) -> Any:
        return page

    spy.new_page = _new_page_returning_spy  # type: ignore[method-assign]

    collector = LinkedInCollector(session_manager=spy)  # type: ignore[arg-type]

    items = await collector.collect_from_source("in/quietprofile")
    assert items == []


@pytest.mark.asyncio
async def test_linkedin_collector_tries_posts_route_after_empty_activity_route() -> None:
    class _SessionManagerSpy:
        async def new_page(self, _platform: str) -> Any:
            return _FakePage(url="https://www.linkedin.com/in/lennyrachitsky/recent-activity/all/")

        async def capture_empty_result_snapshot(self, *_args: Any, **_kwargs: Any) -> None:
            raise AssertionError("snapshot should not be captured after posts fallback succeeds")

        async def capture_debug_snapshot(self, *_args: Any, **_kwargs: Any) -> None:
            return None

    visited: list[str] = []

    async def _fake_goto(url: str, **_kwargs: Any) -> None:
        visited.append(url)
        page.url = url

    async def _fake_wait_for_timeout(_ms: int) -> None:
        return None

    async def _fake_evaluate(_js: str, _max_items: int) -> list:
        if visited[-1].endswith("/posts/"):
            return [
                {
                    "external_id": "123456789",
                    "author": "Lenny Rachitsky",
                    "url": "https://www.linkedin.com/feed/update/urn:li:activity:123456789/",
                    "text": "A useful product-growth post with practical hiring lessons.",
                    "published_at": "2026-05-12T00:00:00.000Z",
                    "reaction_count": 1200,
                    "comment_count": 44,
                    "repost_count": 8,
                }
            ]
        return []

    async def _fake_close() -> None:
        return None

    spy = _SessionManagerSpy()
    page = await spy.new_page("linkedin")
    page.goto = _fake_goto  # type: ignore[attr-defined]
    page.wait_for_timeout = _fake_wait_for_timeout  # type: ignore[attr-defined]
    page.evaluate = _fake_evaluate  # type: ignore[attr-defined]
    page.close = _fake_close  # type: ignore[attr-defined]

    async def _new_page_returning_spy(_platform: str) -> Any:
        return page

    spy.new_page = _new_page_returning_spy  # type: ignore[method-assign]

    collector = LinkedInCollector(session_manager=spy)  # type: ignore[arg-type]

    items = await collector.collect_from_source("in/lennyrachitsky")

    assert [url.rsplit("/", 2)[-2] for url in visited[:2]] == ["all", "posts"]
    assert len(items) == 1
    assert items[0].author == "Lenny Rachitsky"
    assert items[0].published_at == "2026-05-12T00:00:00.000Z"
    assert items[0].metadata["reaction_count"] == 1200


@pytest.mark.asyncio
async def test_t156_linkedin_timeline_collector_extracts_posts_from_feed_dom() -> None:
    class _Mouse:
        async def wheel(self, _x: int, _y: int) -> None:
            return None

    class _SessionManagerSpy:
        async def new_page(self, _platform: str) -> Any:
            page = _FakePage(url="https://www.linkedin.com/feed/")
            page.mouse = _Mouse()  # type: ignore[attr-defined]
            return page

        async def capture_empty_result_snapshot(self, *_args: Any, **_kwargs: Any) -> None:
            raise AssertionError("timeline should not be empty")

        async def capture_debug_snapshot(self, *_args: Any, **_kwargs: Any) -> None:
            return None

    visited: list[str] = []

    async def _fake_goto(url: str, **_kwargs: Any) -> None:
        visited.append(url)
        page.url = url

    async def _fake_wait_for_timeout(_ms: int) -> None:
        return None

    async def _fake_evaluate(_js: str, _max_items: int) -> list:
        return [
            {
                "external_id": "123",
                "author": "Connection Author",
                "url": "https://www.linkedin.com/feed/update/urn:li:activity:123/",
                "text": "Connection-driven post with practical AI PM lesson.",
                "published_at": "2026-05-17T00:00:00.000Z",
                "reaction_count": 10,
                "comment_count": 2,
                "repost_count": 1,
            }
        ]

    async def _fake_close() -> None:
        return None

    spy = _SessionManagerSpy()
    page = await spy.new_page("linkedin")
    page.goto = _fake_goto  # type: ignore[attr-defined]
    page.wait_for_timeout = _fake_wait_for_timeout  # type: ignore[attr-defined]
    page.evaluate = _fake_evaluate  # type: ignore[attr-defined]
    page.close = _fake_close  # type: ignore[attr-defined]

    async def _new_page_returning_spy(_platform: str) -> Any:
        return page

    spy.new_page = _new_page_returning_spy  # type: ignore[method-assign]

    collector = LinkedInTimelineCollector(session_manager=spy, max_items=5, scroll_iterations=1)  # type: ignore[arg-type]
    items = await collector.collect_from_source("linkedin:my-timeline")

    assert visited == ["https://www.linkedin.com/feed/"]
    assert len(items) == 1
    assert items[0].external_id == "timeline:123"
    assert items[0].author == "Connection Author"
    assert items[0].metadata["linkedin_timeline"] is True
