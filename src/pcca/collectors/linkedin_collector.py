from __future__ import annotations

import logging
from dataclasses import dataclass

from pcca.browser.session_manager import BrowserSessionManager
from pcca.collectors.base import CollectedItem
from pcca.collectors.errors import SessionChallengedError
from pcca.collectors.linkedin_utils import (
    build_linkedin_activity_url,
    is_opaque_linkedin_member_id,
    linked_in_profile_url,
    normalize_linkedin_source_id,
)

logger = logging.getLogger(__name__)


@dataclass
class LinkedInCollector:
    session_manager: BrowserSessionManager
    max_items: int = 20
    platform: str = "linkedin"

    async def resolve_source_identifier(self, source_id: str) -> str | None:
        normalized = normalize_linkedin_source_id(source_id)
        if not is_opaque_linkedin_member_id(normalized):
            return normalized
        page = await self.session_manager.new_page(self.platform)
        try:
            return await self._resolve_source_identifier_with_page(page, normalized)
        finally:
            await page.close()

    async def collect_from_source(self, source_id: str) -> list[CollectedItem]:
        page = await self.session_manager.new_page(self.platform)
        try:
            source = await self._resolve_source_identifier_with_page(page, source_id) or normalize_linkedin_source_id(source_id)
            url = build_linkedin_activity_url(source)
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
                  const parseCount = (raw) => {
                    const text = String(raw || "").replace(/,/g, "").trim();
                    const match = text.match(/([0-9]+(?:\\.[0-9]+)?)\\s*([kKmM]?)/);
                    if (!match) return null;
                    const mult = match[2].toLowerCase() === "m" ? 1000000 : match[2].toLowerCase() === "k" ? 1000 : 1;
                    return Math.round(Number(match[1]) * mult);
                  };
                  const countNear = (text, labelPattern) => {
                    const re = new RegExp(`([0-9][0-9,.]*\\\\s*[kKmM]?)\\\\s+${labelPattern}`, "i");
                    const match = text.match(re);
                    return match ? parseCount(match[1]) : null;
                  };
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
                    const allText = node.innerText || "";

                    out.push({
                      external_id: extId,
                      author: author,
                      url: absUrl,
                      text: text,
                      published_at: publishedAt,
                      reaction_count: countNear(allText, "reaction|reactions"),
                      comment_count: countNear(allText, "comment|comments"),
                      repost_count: countNear(allText, "repost|reposts")
                    });
                    if (out.length >= maxItems) break;
                  }
                  return out;
                }
                """,
                self.max_items,
            )
            if not raw_items:
                await self.session_manager.capture_empty_result_snapshot(
                    page,
                    platform=self.platform,
                    source_id=source_id,
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
                metadata={
                    "source_id": source_id,
                    "resolved_source_id": source,
                    "reaction_count": item.get("reaction_count"),
                    "comment_count": item.get("comment_count"),
                    "repost_count": item.get("repost_count"),
                    "like_count": item.get("reaction_count"),
                },
            )
            for item in raw_items
        ]

    async def _resolve_source_identifier_with_page(self, page, source_id: str) -> str | None:
        normalized = normalize_linkedin_source_id(source_id)
        if not normalized or normalized.startswith("company/") or not is_opaque_linkedin_member_id(normalized):
            return normalized

        try:
            await page.goto(linked_in_profile_url(normalized), wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(800)
        except Exception:
            logger.debug("LinkedIn opaque id redirect resolution failed source=%s", source_id, exc_info=True)
            return normalized

        resolved = normalize_linkedin_source_id(page.url)
        if resolved and resolved != normalized and not is_opaque_linkedin_member_id(resolved):
            logger.info("LinkedIn source resolved opaque_id=%s resolved=%s", normalized, resolved)
            return resolved
        return normalized
