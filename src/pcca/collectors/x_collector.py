from __future__ import annotations

import logging
from dataclasses import dataclass

from pcca.browser.session_manager import BrowserSessionManager
from pcca.collectors.base import CollectedItem
from pcca.collectors.errors import SessionChallengedError

logger = logging.getLogger(__name__)


@dataclass
class XCollector:
    session_manager: BrowserSessionManager
    max_items: int = 20
    platform: str = "x"

    async def collect_from_source(self, source_id: str) -> list[CollectedItem]:
        handle = source_id.strip().lstrip("@")
        if handle.startswith("http://") or handle.startswith("https://"):
            url = handle
        else:
            url = f"https://x.com/{handle}"

        page = await self.session_manager.new_page(self.platform)
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(3500)
            current_url = page.url
            if "/i/flow/login" in current_url or "/login" in current_url:
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
                  const articles = Array.from(document.querySelectorAll("article"));
                  for (const article of articles) {
                    const statusAnchor = Array.from(article.querySelectorAll('a[href*="/status/"]'))
                      .map(a => a.getAttribute("href"))
                      .find(Boolean);
                    if (!statusAnchor) continue;
                    const absUrl = statusAnchor.startsWith("http") ? statusAnchor : `https://x.com${statusAnchor}`;
                    const idMatch = absUrl.match(/status\\/(\\d+)/);
                    const extId = idMatch ? idMatch[1] : absUrl;

                    const authorMatch = absUrl.match(/x\\.com\\/([^\\/\\?]+)\\/status\\//);
                    const author = authorMatch ? authorMatch[1] : null;

                    const textNode = article.querySelector('[data-testid="tweetText"]');
                    const text = textNode ? textNode.innerText.trim() : article.innerText.slice(0, 1200);

                    const timeEl = article.querySelector("time");
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
            await self.session_manager.capture_debug_snapshot(page, "x_collect_failed", error=exc)
            logger.exception("X collection failed for source=%s", source_id)
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
