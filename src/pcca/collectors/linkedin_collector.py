from __future__ import annotations

import logging
from dataclasses import dataclass

from pcca.browser.session_manager import BrowserSessionManager
from pcca.collectors.base import CollectedItem
from pcca.collectors.errors import BotShapedError, SessionChallengedError
from pcca.content_quality import mark_low_quality_metadata

# Substring matches in pageerror events that indicate LinkedIn served a
# bot-blocked page rather than a real feed. Most common today (2026-05-11
# observation): React error #418 = hydration mismatch typically caused by
# anti-bot fingerprint detection. Captcha/checkpoint redirects produce
# different signatures but should also trip this guard.
_BOT_SHAPED_PAGEERROR_SIGNATURES: tuple[str, ...] = (
    "React error #418",  # hydration mismatch — LinkedIn anti-bot
    "minified react error #418",
)
_BOT_SHAPED_URL_SIGNATURES: tuple[str, ...] = (
    "/checkpoint/",
    "/uas/captcha",
)


def _detect_bot_shaped_signal(page) -> str | None:
    """Inspect page debug events for bot-detection signatures. Returns the
    matching signature string when found, else None.

    Reads from BrowserSessionManager's `_pcca_debug_events` attached to the
    page object (populated by `pageerror` and `response` listeners). Only
    looks at events on THIS page, not the whole session — avoids false
    positives from earlier collection passes.
    """
    events = getattr(page, "_pcca_debug_events", None) or []
    for event in events:
        kind = event.get("event") if isinstance(event, dict) else None
        if kind != "pageerror":
            continue
        error_text = str(event.get("error", "")).lower()
        for signature in _BOT_SHAPED_PAGEERROR_SIGNATURES:
            if signature.lower() in error_text:
                return signature
    current_url = (getattr(page, "url", None) or "").lower()
    for signature in _BOT_SHAPED_URL_SIGNATURES:
        if signature in current_url:
            return signature
    return None
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
                # T-138: when raw_items is empty AND the page emitted an
                # anti-bot signal (React #418 hydration error, captcha
                # redirect), surface as BotShapedError instead of silently
                # returning []. Orchestrator classifies this as bot_shaped
                # (threshold ~5, fast circuit trip) rather than
                # empty_legitimate (threshold ~25). Without this signal,
                # 33 silent-empty LinkedIn sources never trip the breaker
                # and the user sees no LinkedIn content for days while
                # the system reports "no failures."
                signal = _detect_bot_shaped_signal(page)
                if signal is not None:
                    raise BotShapedError(
                        platform=self.platform,
                        source_id=source_id,
                        signal=signal,
                        current_url=getattr(page, "url", None),
                    )
        except Exception as exc:
            await self.session_manager.capture_debug_snapshot(page, "linkedin_collect_failed", error=exc)
            logger.exception("LinkedIn collection failed for source=%s", source_id)
            raise
        finally:
            await page.close()

        out: list[CollectedItem] = []
        for item in raw_items:
            text = item.get("text")
            metadata = mark_low_quality_metadata(
                {
                    "source_id": source_id,
                    "resolved_source_id": source,
                    "reaction_count": item.get("reaction_count"),
                    "comment_count": item.get("comment_count"),
                    "repost_count": item.get("repost_count"),
                    "like_count": item.get("reaction_count"),
                },
                text,
            )
            out.append(
                CollectedItem(
                    platform=self.platform,
                    external_id=item["external_id"],
                    author=item.get("author"),
                    url=item.get("url"),
                    text=text,
                    transcript_text=None,
                    published_at=item.get("published_at"),
                    metadata=metadata,
                )
            )
        return out

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
