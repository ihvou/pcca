from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from urllib.parse import urlparse

from pcca.browser.session_manager import BrowserSessionManager
from pcca.services.source_discovery_service import SourceDiscoveryService
from pcca.services.source_service import SourceService

logger = logging.getLogger(__name__)


def normalize_youtube_subscription_href(href: str) -> str | None:
    raw = href.strip()
    if not raw:
        return None
    if raw.startswith("http://") or raw.startswith("https://"):
        parsed = urlparse(raw)
        path = parsed.path
    else:
        path = raw

    m_handle = re.search(r"/(@[A-Za-z0-9_.-]+)", path)
    if m_handle:
        return m_handle.group(1)

    m_channel = re.search(r"/channel/([A-Za-z0-9_-]+)", path)
    if m_channel:
        return m_channel.group(1)
    return None


@dataclass
class ImportedFollowSource:
    platform: str
    account_or_channel_id: str
    display_name: str
    raw_source: str


@dataclass
class FollowImportService:
    session_manager: BrowserSessionManager
    source_service: SourceService
    source_discovery: SourceDiscoveryService = field(default_factory=SourceDiscoveryService)

    @staticmethod
    def supported_platforms() -> tuple[str, ...]:
        return ("x", "linkedin", "youtube", "substack", "medium", "spotify", "apple_podcasts")

    async def import_x_follows(self, *, limit: int = 200) -> list[str]:
        page = await self.session_manager.new_page("x")
        try:
            await page.goto("https://x.com/home", wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(2500)
            profile_href = await page.evaluate(
                """
                () => {
                  // Try several selectors X has used over time so a UI tweak
                  // doesn't immediately break follow import.
                  const candidates = [
                    'a[data-testid="AppTabBar_Profile_Link"]',
                    'a[aria-label="Profile"][href^="/"]',
                    'nav a[href^="/"][role="link"][data-testid$="Profile_Link"]',
                  ];
                  for (const sel of candidates) {
                    const a = document.querySelector(sel);
                    if (a && a.getAttribute("href")) return a.getAttribute("href");
                  }
                  return null;
                }
                """
            )
            if not profile_href:
                landing_url = page.url
                hint = (
                    "Capture your X session via the wizard's 'Capture Session' button "
                    "before clicking Stage Follows. If you did capture, your saved cookies "
                    "may have expired — re-log into x.com in "
                    "your browser, then run Capture Session again."
                )
                raise RuntimeError(
                    f"Could not detect own X profile handle. Page settled at {landing_url}. "
                    f"This usually means PCCA's profile is not logged in. {hint}"
                )

            own_handle = profile_href.strip("/").split("/")[-1]
            await page.goto(f"https://x.com/{own_handle}/following", wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(2500)

            handles: set[str] = set()
            for _ in range(20):
                batch = await page.evaluate(
                    """
                    () => {
                      const out = [];
                      const links = Array.from(document.querySelectorAll('a[href^="/"]'));
                      for (const link of links) {
                        const href = link.getAttribute("href") || "";
                        const m = href.match(/^\\/([A-Za-z0-9_]{1,20})$/);
                        if (m) out.push(m[1]);
                      }
                      return out;
                    }
                    """
                )
                handles.update(str(h) for h in batch)
                if len(handles) >= limit:
                    break
                await page.mouse.wheel(0, 2800)
                await page.wait_for_timeout(700)

            # Remove known non-user routes.
            blocked = {"home", "explore", "notifications", "messages", "settings", "i", "compose"}
            return [h for h in sorted(handles) if h.lower() not in blocked][:limit]
        finally:
            await page.close()

    async def import_linkedin_follows(self, *, limit: int = 200) -> list[str]:
        page = await self.session_manager.new_page("linkedin")
        try:
            await page.goto(
                "https://www.linkedin.com/feed/following/",
                wait_until="domcontentloaded",
                timeout=60000,
            )
            await page.wait_for_timeout(3000)
            ids: set[str] = set()
            for _ in range(20):
                batch = await page.evaluate(
                    """
                    () => {
                      const out = [];
                      const links = Array.from(document.querySelectorAll('a[href*="/in/"], a[href*="/company/"]'));
                      for (const link of links) {
                        const href = link.getAttribute("href") || "";
                        const mIn = href.match(/linkedin\\.com\\/(in\\/[^\\/?#]+)/);
                        if (mIn) out.push(mIn[1]);
                        const mCo = href.match(/linkedin\\.com\\/(company\\/[^\\/?#]+)/);
                        if (mCo) out.push(mCo[1]);
                      }
                      return out;
                    }
                    """
                )
                ids.update(str(i) for i in batch)
                if len(ids) >= limit:
                    break
                await page.mouse.wheel(0, 2600)
                await page.wait_for_timeout(800)
            return sorted(ids)[:limit]
        finally:
            await page.close()

    async def import_youtube_subscriptions(self, *, limit: int = 200) -> list[str]:
        page = await self.session_manager.new_page("youtube")
        try:
            await page.goto(
                "https://www.youtube.com/feed/channels",
                wait_until="domcontentloaded",
                timeout=60000,
            )
            await page.wait_for_timeout(3000)
            ids: set[str] = set()
            for _ in range(20):
                batch = await page.evaluate(
                    """
                    () => {
                      const out = [];
                      const selectors = [
                        "ytd-channel-renderer a#main-link[href]",
                        "a#main-link[href]",
                        "a[href*='/@']",
                        "a[href*='/channel/']"
                      ];
                      for (const selector of selectors) {
                        const links = Array.from(document.querySelectorAll(selector));
                        for (const link of links) {
                          const href = link.getAttribute("href");
                          if (href) out.push(href);
                        }
                      }
                      return out;
                    }
                    """
                )
                for href in batch:
                    normalized = normalize_youtube_subscription_href(str(href))
                    if normalized:
                        ids.add(normalized)
                if len(ids) >= limit:
                    break
                await page.mouse.wheel(0, 2400)
                await page.wait_for_timeout(700)
            return sorted(ids)[:limit]
        finally:
            await page.close()

    async def import_substack_subscriptions(self, *, limit: int = 200) -> list[str]:
        page = await self.session_manager.new_page("substack")
        try:
            await page.goto("https://substack.com/settings", wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(3000)
            urls: set[str] = set()
            for _ in range(18):
                batch = await page.evaluate(
                    """
                    () => {
                      const out = [];
                      const links = Array.from(document.querySelectorAll('a[href*=".substack.com"]'));
                      for (const link of links) {
                        const href = link.getAttribute("href") || "";
                        if (!href) continue;
                        if (!href.includes(".substack.com")) continue;
                        if (href.includes("/publish")) continue;
                        out.push(href);
                      }
                      return out;
                    }
                    """
                )
                for href in batch:
                    normalized = self._normalize_substack_url(str(href))
                    if normalized:
                        urls.add(normalized)
                if len(urls) >= limit:
                    break
                await page.mouse.wheel(0, 2600)
                await page.wait_for_timeout(700)
            return sorted(urls)[:limit]
        finally:
            await page.close()

    async def import_medium_following(self, *, limit: int = 200) -> list[str]:
        page = await self.session_manager.new_page("medium")
        try:
            await page.goto("https://medium.com/me/following", wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(3200)
            urls: set[str] = set()
            for _ in range(20):
                batch = await page.evaluate(
                    """
                    () => {
                      const out = [];
                      const links = Array.from(document.querySelectorAll('a[href*="medium.com/"]'));
                      for (const link of links) {
                        const href = link.getAttribute("href") || "";
                        if (!href.includes("medium.com/")) continue;
                        if (href.includes("/m/signin")) continue;
                        out.push(href);
                      }
                      return out;
                    }
                    """
                )
                for href in batch:
                    normalized = self._normalize_medium_url(str(href))
                    if normalized:
                        urls.add(normalized)
                if len(urls) >= limit:
                    break
                await page.mouse.wheel(0, 2400)
                await page.wait_for_timeout(700)
            return sorted(urls)[:limit]
        finally:
            await page.close()

    async def import_spotify_podcast_follows(self, *, limit: int = 200) -> list[str]:
        page = await self.session_manager.new_page("spotify")
        try:
            await page.goto("https://open.spotify.com/collection/podcasts", wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(3200)
            urls: set[str] = set()
            for _ in range(22):
                batch = await page.evaluate(
                    """
                    () => {
                      const out = [];
                      const links = Array.from(document.querySelectorAll('a[href*="/show/"]'));
                      for (const link of links) {
                        const href = link.getAttribute("href") || "";
                        if (!href.includes("/show/")) continue;
                        out.push(href);
                      }
                      return out;
                    }
                    """
                )
                for href in batch:
                    normalized = self._normalize_spotify_show_url(str(href))
                    if normalized:
                        urls.add(normalized)
                if len(urls) >= limit:
                    break
                await page.mouse.wheel(0, 2600)
                await page.wait_for_timeout(700)
            return sorted(urls)[:limit]
        finally:
            await page.close()

    async def import_apple_podcast_subscriptions(self, *, limit: int = 200) -> list[str]:
        page = await self.session_manager.new_page("apple_podcasts")
        try:
            await page.goto("https://podcasts.apple.com/us/library/shows", wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(3200)
            urls: set[str] = set()
            for _ in range(20):
                batch = await page.evaluate(
                    """
                    () => {
                      const out = [];
                      const links = Array.from(document.querySelectorAll('a[href*="/podcast/"]'));
                      for (const link of links) {
                        const href = link.getAttribute("href") || "";
                        if (!href.includes("/podcast/")) continue;
                        out.push(href);
                      }
                      return out;
                    }
                    """
                )
                for href in batch:
                    normalized = self._normalize_apple_podcast_url(str(href))
                    if normalized:
                        urls.add(normalized)
                if len(urls) >= limit:
                    break
                await page.mouse.wheel(0, 2200)
                await page.wait_for_timeout(700)
            return sorted(urls)[:limit]
        finally:
            await page.close()

    async def import_sources(self, *, platform: str, limit: int = 200) -> list[ImportedFollowSource]:
        normalized_platform = platform.strip().lower()
        if normalized_platform == "x":
            imported = await self.import_x_follows(limit=limit)
        elif normalized_platform == "linkedin":
            imported = await self.import_linkedin_follows(limit=limit)
        elif normalized_platform == "youtube":
            imported = await self.import_youtube_subscriptions(limit=limit)
        elif normalized_platform == "substack":
            imported = await self.import_substack_subscriptions(limit=limit)
        elif normalized_platform == "medium":
            imported = await self.import_medium_following(limit=limit)
        elif normalized_platform == "spotify":
            imported = await self.import_spotify_podcast_follows(limit=limit)
        elif normalized_platform == "apple_podcasts":
            imported = await self.import_apple_podcast_subscriptions(limit=limit)
        else:
            supported = ", ".join(self.supported_platforms())
            raise ValueError(f"Supported platforms for follow import: {supported}")

        out: list[ImportedFollowSource] = []
        for raw_source in imported:
            out.extend(await self._normalize_imported_source(platform=normalized_platform, raw_source=raw_source))
        return out

    async def import_to_subject(self, *, subject_name: str, platform: str, limit: int = 200) -> int:
        normalized_platform = platform.strip().lower()
        imported = await self.import_sources(platform=normalized_platform, limit=limit)
        linked = 0
        for source in imported:
            await self.source_service.add_source_to_subject(
                subject_name=subject_name,
                platform=source.platform,
                account_or_channel_id=source.account_or_channel_id,
                display_name=source.display_name,
                priority=0,
            )
            linked += 1
        logger.info("Imported %d %s follows into subject=%s", linked, normalized_platform, subject_name)
        return linked

    async def _normalize_imported_source(self, *, platform: str, raw_source: str) -> list[ImportedFollowSource]:
        source_value = raw_source.strip()
        if not source_value:
            return []

        # URL-backed platforms resolve into canonical source IDs (feed URLs for some platforms).
        if platform in {"substack", "medium", "apple_podcasts", "spotify"} and source_value.startswith(("http://", "https://")):
            discovered = await self.source_discovery.discover(source_value)
            normalized: list[ImportedFollowSource] = []
            for row in discovered:
                if row.platform != platform:
                    continue
                normalized.append(
                    ImportedFollowSource(
                        platform=row.platform,
                        account_or_channel_id=row.source_id,
                        display_name=row.display_name,
                        raw_source=source_value,
                    )
                )
            if normalized:
                return normalized

        display = source_value
        if platform == "linkedin":
            display = re.sub(r"^(in|company)/", "", source_value)
        return [
            ImportedFollowSource(
                platform=platform,
                account_or_channel_id=source_value,
                display_name=display,
                raw_source=source_value,
            )
        ]

    def _normalize_substack_url(self, href: str) -> str | None:
        parsed = urlparse(href.strip())
        host = parsed.netloc.lower()
        if host.startswith("www."):
            host = host[4:]
        if not host.endswith(".substack.com"):
            return None
        return f"https://{host}"

    def _normalize_medium_url(self, href: str) -> str | None:
        parsed = urlparse(href.strip())
        host = parsed.netloc.lower()
        if host.startswith("www."):
            host = host[4:]
        if not host.endswith("medium.com"):
            return None
        path = parsed.path or "/"
        if path.startswith("/@"):
            handle = path.split("/", 2)[1]
            return f"https://medium.com/{handle}"
        # publication-like slug
        first = path.strip("/").split("/", 1)[0]
        if not first or first in {"me", "topics"}:
            return None
        return f"https://medium.com/{first}"

    def _normalize_spotify_show_url(self, href: str) -> str | None:
        parsed = urlparse(href.strip())
        host = parsed.netloc.lower()
        if host.startswith("www."):
            host = host[4:]
        if host != "open.spotify.com":
            return None
        m_show = re.search(r"/show/([A-Za-z0-9]+)", parsed.path or "")
        if not m_show:
            return None
        return f"https://open.spotify.com/show/{m_show.group(1)}"

    def _normalize_apple_podcast_url(self, href: str) -> str | None:
        parsed = urlparse(href.strip())
        host = parsed.netloc.lower()
        if host.startswith("www."):
            host = host[4:]
        if host != "podcasts.apple.com":
            return None
        if "/podcast/" not in (parsed.path or ""):
            return None
        return f"https://{host}{parsed.path}"
