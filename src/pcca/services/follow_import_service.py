from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from urllib.parse import urlparse

from pcca.browser.session_manager import BrowserSessionManager
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
class FollowImportService:
    session_manager: BrowserSessionManager
    source_service: SourceService

    @staticmethod
    def supported_platforms() -> tuple[str, ...]:
        return ("x", "linkedin", "youtube")

    async def import_x_follows(self, *, limit: int = 200) -> list[str]:
        page = await self.session_manager.new_page("x")
        try:
            await page.goto("https://x.com/home", wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(2500)
            profile_href = await page.evaluate(
                """
                () => {
                  const a = document.querySelector('a[data-testid="AppTabBar_Profile_Link"]');
                  return a ? a.getAttribute("href") : null;
                }
                """
            )
            if not profile_href:
                raise RuntimeError("Could not detect own X profile handle. Ensure session is logged in.")

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

    async def import_to_subject(self, *, subject_name: str, platform: str, limit: int = 200) -> int:
        normalized_platform = platform.strip().lower()
        if normalized_platform == "x":
            imported = await self.import_x_follows(limit=limit)
        elif normalized_platform == "linkedin":
            imported = await self.import_linkedin_follows(limit=limit)
        elif normalized_platform == "youtube":
            imported = await self.import_youtube_subscriptions(limit=limit)
        else:
            supported = ", ".join(self.supported_platforms())
            raise ValueError(f"Supported platforms for follow import: {supported}")

        linked = 0
        for source_id in imported:
            display = source_id
            if normalized_platform == "linkedin":
                display = re.sub(r"^(in|company)/", "", source_id)
            await self.source_service.add_source_to_subject(
                subject_name=subject_name,
                platform=normalized_platform,
                account_or_channel_id=source_id,
                display_name=display,
                priority=0,
            )
            linked += 1
        logger.info("Imported %d %s follows into subject=%s", linked, normalized_platform, subject_name)
        return linked
