from __future__ import annotations

import logging
from dataclasses import dataclass

from pcca.browser.session_manager import BrowserSessionManager
from pcca.collectors.base import CollectedItem
from pcca.collectors.errors import SessionChallengedError

logger = logging.getLogger(__name__)


@dataclass
class LinkedInCollector:
    session_manager: BrowserSessionManager
    max_items: int = 20
    platform: str = "linkedin"

    async def collect_from_source(self, source_id: str) -> list[CollectedItem]:
        source = source_id.strip()
        if source.startswith("http://") or source.startswith("https://"):
            url = source
        elif source.startswith("company/"):
            url = f"https://www.linkedin.com/{source}/posts/"
        else:
            url = f"https://www.linkedin.com/in/{source}/recent-activity/all/"

        page = await self.session_manager.new_page(self.platform)
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(3500)
            current_url = page.url
            if "linkedin.com/login" in current_url or "linkedin.com/uas/login" in current_url:
                raise SessionChallengedError(
                    platform=self.platform,
                    source_id=source_id,
                    current_url=current_url,
                    challenge_kind="login_redirect",
                )
            raw_items = await page.evaluate(
                """
                (maxItems) => {
                  const out = [];
                  const containers = Array.from(document.querySelectorAll("div.feed-shared-update-v2, div.occludable-update"));
                  for (const node of containers) {
                    const anchor = Array.from(node.querySelectorAll('a[href*="/feed/update/"], a[href*="/posts/"]'))
                      .map(a => a.getAttribute("href"))
                      .find(Boolean);
                    if (!anchor) continue;
                    const absUrl = anchor.startsWith("http") ? anchor : `https://www.linkedin.com${anchor}`;
                    const urnMatch = absUrl.match(/activity:(\\d+)/);
                    const extId = urnMatch ? urnMatch[1] : absUrl;

                    const authorAnchor = node.querySelector('a[href*="/in/"], a[href*="/company/"]');
                    const author = authorAnchor ? (authorAnchor.textContent || "").trim() : null;

                    const textNode = node.querySelector("span.break-words, div.update-components-text");
                    const text = textNode ? textNode.innerText.trim() : node.innerText.slice(0, 1200);
                    const timeEl = node.querySelector("time");
                    const publishedAt = timeEl ? timeEl.getAttribute("datetime") : null;

                    out.push({
                      external_id: extId,
                      author: author,
                      url: absUrl,
                      text: text,
                      published_at: publishedAt
                    });
                    if (out.length >= maxItems) break;
                  }
                  return out;
                }
                """,
                self.max_items,
            )
        except Exception as exc:
            await self.session_manager.capture_debug_snapshot(page, "linkedin_collect_failed", error=exc)
            logger.exception("LinkedIn collection failed for source=%s", source_id)
            raise
        finally:
            await page.close()

        return [
            CollectedItem(
                platform=self.platform,
                external_id=item["external_id"],
                author=item.get("author"),
                url=item.get("url"),
                text=item.get("text"),
                transcript_text=None,
                published_at=item.get("published_at"),
                metadata={"source_id": source_id},
            )
            for item in raw_items
        ]
