from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from html.parser import HTMLParser
from urllib.parse import unquote, urljoin, urlparse

import httpx

logger = logging.getLogger(__name__)


@dataclass
class DiscoveredSource:
    platform: str
    source_id: str
    display_name: str
    confidence: float
    reason: str


class _FeedLinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.feed_hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "link":
            return
        data = {k.lower(): (v or "") for k, v in attrs}
        href = data.get("href", "").strip()
        if not href:
            return
        rel = data.get("rel", "").lower()
        content_type = data.get("type", "").lower()
        if "alternate" in rel and ("rss" in content_type or "atom" in content_type):
            self.feed_hrefs.append(href)


@dataclass
class SourceDiscoveryService:
    async def discover(self, raw_input: str) -> list[DiscoveredSource]:
        value = raw_input.strip()
        if not value:
            return []

        if not (value.startswith("http://") or value.startswith("https://")):
            prefixed = self._discover_platform_prefixed(value)
            return prefixed

        try:
            parsed = urlparse(value)
            host = parsed.netloc.lower()
            if host.startswith("www."):
                host = host[4:]

            discovered: list[DiscoveredSource] = []
            discovered.extend(await self._discover_native_social(value, parsed, host))
            discovered.extend(await self._discover_blog_and_podcast(value, parsed, host))
            return self._dedupe(discovered)
        except Exception:
            logger.exception("Source discovery failed for input=%s", value)
            return []

    def _discover_platform_prefixed(self, value: str) -> list[DiscoveredSource]:
        match = re.match(r"^(x|linkedin|youtube|reddit|rss)\s*[:=]\s*(.+)$", value, flags=re.IGNORECASE)
        if not match:
            return []
        platform = match.group(1).strip().lower()
        source_id = match.group(2).strip()
        return [
            DiscoveredSource(
                platform=platform,
                source_id=source_id,
                display_name=source_id,
                confidence=0.95,
                reason="explicit platform:id input",
            )
        ]

    async def _discover_native_social(self, raw_url: str, parsed, host: str) -> list[DiscoveredSource]:
        path = parsed.path or "/"
        out: list[DiscoveredSource] = []

        if host in {"x.com", "twitter.com"}:
            # Accept direct profile URLs like https://x.com/borischerny
            m = re.match(r"^/([A-Za-z0-9_]{1,20})/?$", path)
            if m:
                handle = m.group(1)
                blocked = {"home", "explore", "notifications", "messages", "compose", "settings", "i"}
                if handle.lower() not in blocked:
                    out.append(
                        DiscoveredSource(
                            platform="x",
                            source_id=handle,
                            display_name=handle,
                            confidence=0.95,
                            reason="x profile URL",
                        )
                    )

        if host.endswith("linkedin.com"):
            m = re.search(r"/(in/[^/?#]+|company/[^/?#]+)", path)
            if m:
                source_id = m.group(1)
                out.append(
                    DiscoveredSource(
                        platform="linkedin",
                        source_id=source_id,
                        display_name=source_id.split("/", 1)[-1],
                        confidence=0.95,
                        reason="linkedin profile/company URL",
                    )
                )

        if host.endswith("youtube.com") or host == "youtu.be":
            source_id = self._parse_youtube_source_id(raw_url)
            if source_id:
                out.append(
                    DiscoveredSource(
                        platform="youtube",
                        source_id=source_id,
                        display_name=source_id,
                        confidence=0.9,
                        reason="youtube channel URL",
                    )
                )

        if host.endswith("reddit.com"):
            m_sub = re.search(r"/r/([^/?#]+)", path)
            if m_sub:
                out.append(
                    DiscoveredSource(
                        platform="reddit",
                        source_id=f"r/{m_sub.group(1)}",
                        display_name=f"r/{m_sub.group(1)}",
                        confidence=0.9,
                        reason="reddit subreddit URL",
                    )
                )
            m_user = re.search(r"/(?:u|user)/([^/?#]+)", path)
            if m_user:
                out.append(
                    DiscoveredSource(
                        platform="reddit",
                        source_id=f"u/{m_user.group(1)}",
                        display_name=f"u/{m_user.group(1)}",
                        confidence=0.9,
                        reason="reddit user URL",
                    )
                )

        return out

    async def _discover_blog_and_podcast(self, raw_url: str, parsed, host: str) -> list[DiscoveredSource]:
        out: list[DiscoveredSource] = []

        # Substack publications have a stable feed endpoint at /feed.
        if host.endswith("substack.com"):
            feed_url = f"{parsed.scheme or 'https'}://{host}/feed"
            out.append(
                DiscoveredSource(
                    platform="rss",
                    source_id=feed_url,
                    display_name=host.split(".")[0],
                    confidence=0.98,
                    reason="substack publication feed",
                )
            )
            return out

        # Medium paths can be mapped into RSS directly.
        if host == "medium.com" or host.endswith(".medium.com"):
            medium_feed = self._derive_medium_feed(parsed.path)
            if medium_feed:
                out.append(
                    DiscoveredSource(
                        platform="rss",
                        source_id=medium_feed,
                        display_name="medium",
                        confidence=0.95,
                        reason="medium feed mapping",
                    )
                )
                return out

        if host == "podcasts.google.com":
            feed_url = self._extract_google_podcasts_feed(raw_url)
            if feed_url:
                out.append(
                    DiscoveredSource(
                        platform="rss",
                        source_id=feed_url,
                        display_name="google-podcast-feed",
                        confidence=0.95,
                        reason="google podcasts feed URL",
                    )
                )
                return out

        if host == "podcasts.apple.com":
            feed_url = await self._lookup_apple_podcast_feed(raw_url)
            if feed_url:
                out.append(
                    DiscoveredSource(
                        platform="rss",
                        source_id=feed_url,
                        display_name="apple-podcast-feed",
                        confidence=0.92,
                        reason="apple podcasts iTunes lookup",
                    )
                )
                return out

        # Spotify, custom blogs, and any page exposing link rel=alternate RSS/Atom.
        feed_links = await self._discover_rss_links(raw_url)
        for feed in feed_links:
            out.append(
                DiscoveredSource(
                    platform="rss",
                    source_id=feed,
                    display_name=urlparse(feed).netloc or "rss-feed",
                    confidence=0.8,
                    reason="rss/atom link discovered in page html",
                )
            )

        # If user pasted direct feed URL, accept it.
        if self._looks_like_feed_url(raw_url):
            out.append(
                DiscoveredSource(
                    platform="rss",
                    source_id=raw_url,
                    display_name=urlparse(raw_url).netloc or "rss-feed",
                    confidence=0.9,
                    reason="direct rss/atom URL",
                )
            )
        return out

    async def _discover_rss_links(self, url: str) -> list[str]:
        try:
            async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
                response = await client.get(
                    url,
                    headers={
                        "User-Agent": "pcca/0.1 (+local agent)",
                        "Accept": "text/html,application/xhtml+xml",
                    },
                )
                response.raise_for_status()
                html = response.text
        except Exception:
            return []

        parser = _FeedLinkParser()
        try:
            parser.feed(html)
        except Exception:
            return []
        resolved = [urljoin(str(response.url), href) for href in parser.feed_hrefs]
        return self._dedupe_urls(resolved)

    async def _lookup_apple_podcast_feed(self, apple_url: str) -> str | None:
        match = re.search(r"/id(\d+)", apple_url)
        if not match:
            return None
        podcast_id = match.group(1)
        lookup_url = f"https://itunes.apple.com/lookup?id={podcast_id}"
        try:
            async with httpx.AsyncClient(timeout=12.0) as client:
                response = await client.get(lookup_url)
                response.raise_for_status()
                payload = response.json()
            results = payload.get("results") or []
            if not results:
                return None
            feed = results[0].get("feedUrl")
            if isinstance(feed, str) and feed.startswith(("http://", "https://")):
                return feed
            return None
        except Exception:
            return None

    def _extract_google_podcasts_feed(self, google_url: str) -> str | None:
        parsed = urlparse(google_url)
        path = parsed.path or ""
        # Typical format:
        # https://podcasts.google.com/feed/<url-encoded-feed-url>
        marker = "/feed/"
        if marker not in path:
            return None
        encoded = path.split(marker, 1)[1]
        if not encoded:
            return None
        decoded = unquote(encoded)
        if decoded.startswith(("http://", "https://")):
            return decoded
        return None

    def _derive_medium_feed(self, path: str) -> str | None:
        normalized = path.strip("/")
        if not normalized:
            return "https://medium.com/feed"
        parts = normalized.split("/")
        first = parts[0]
        if first.startswith("@"):
            return f"https://medium.com/feed/{first}"
        # publication slug
        return f"https://medium.com/feed/{first}"

    def _parse_youtube_source_id(self, raw_url: str) -> str | None:
        parsed = urlparse(raw_url)
        path = parsed.path or ""
        m_handle = re.search(r"/(@[A-Za-z0-9_.-]+)", path)
        if m_handle:
            return m_handle.group(1)
        m_channel = re.search(r"/channel/([A-Za-z0-9_-]+)", path)
        if m_channel:
            return m_channel.group(1)
        m_custom = re.search(r"/(?:c|user)/([A-Za-z0-9_.-]+)", path)
        if m_custom:
            return m_custom.group(1)
        return None

    def _looks_like_feed_url(self, url: str) -> bool:
        lowered = url.lower()
        return lowered.endswith(".xml") or "/feed" in lowered or "rss" in lowered or "atom" in lowered

    def _dedupe(self, rows: list[DiscoveredSource]) -> list[DiscoveredSource]:
        seen: set[tuple[str, str]] = set()
        out: list[DiscoveredSource] = []
        for row in rows:
            key = (row.platform, row.source_id)
            if key in seen:
                continue
            seen.add(key)
            out.append(row)
        return out

    def _dedupe_urls(self, urls: list[str]) -> list[str]:
        seen: set[str] = set()
        out: list[str] = []
        for url in urls:
            key = url.strip()
            if not key or key in seen:
                continue
            seen.add(key)
            out.append(key)
        return out
