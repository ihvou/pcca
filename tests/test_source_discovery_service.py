from __future__ import annotations

import pytest

from pcca.services.source_discovery_service import SourceDiscoveryService


class FakeAppleDiscoveryService(SourceDiscoveryService):
    async def _lookup_apple_podcast_feed(self, apple_url: str) -> str | None:
        _ = apple_url
        return "https://feeds.soundcloud.com/users/soundcloud:users:123/sounds.rss"

    async def _discover_rss_links(self, url: str) -> list[str]:
        _ = url
        return []


@pytest.mark.asyncio
async def test_discover_prefixed_source() -> None:
    service = SourceDiscoveryService()
    discovered = await service.discover("x:borischerny")
    assert len(discovered) == 1
    assert discovered[0].platform == "x"
    assert discovered[0].source_id == "borischerny"


@pytest.mark.asyncio
async def test_discover_substack_url_to_feed() -> None:
    service = SourceDiscoveryService()
    discovered = await service.discover("https://newsletter.substack.com")
    assert len(discovered) == 1
    assert discovered[0].platform == "rss"
    assert discovered[0].source_id == "https://newsletter.substack.com/feed"


@pytest.mark.asyncio
async def test_discover_medium_url_to_feed() -> None:
    service = SourceDiscoveryService()
    discovered = await service.discover("https://medium.com/@openai/some-post")
    assert len(discovered) == 1
    assert discovered[0].platform == "rss"
    assert discovered[0].source_id == "https://medium.com/feed/@openai"


@pytest.mark.asyncio
async def test_discover_google_podcast_feed_url() -> None:
    service = SourceDiscoveryService()
    discovered = await service.discover(
        "https://podcasts.google.com/feed/https%3A%2F%2Fexample.com%2Fpodcast.xml"
    )
    assert len(discovered) == 1
    assert discovered[0].platform == "rss"
    assert discovered[0].source_id == "https://example.com/podcast.xml"


@pytest.mark.asyncio
async def test_discover_apple_podcast_via_lookup() -> None:
    service = FakeAppleDiscoveryService()
    discovered = await service.discover("https://podcasts.apple.com/us/podcast/example/id123456789")
    assert len(discovered) == 1
    assert discovered[0].platform == "rss"
    assert "soundcloud" in discovered[0].source_id


@pytest.mark.asyncio
async def test_discover_x_profile_url() -> None:
    service = SourceDiscoveryService()
    discovered = await service.discover("https://x.com/borischerny")
    assert len(discovered) == 1
    assert discovered[0].platform == "x"
    assert discovered[0].source_id == "borischerny"
