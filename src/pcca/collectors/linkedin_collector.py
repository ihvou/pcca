from __future__ import annotations

import logging
from dataclasses import dataclass

from pcca.browser.session_manager import BrowserSessionManager
from pcca.collectors.base import CollectedItem
from pcca.collectors.errors import BotShapedError, SessionChallengedError
from pcca.collectors.linkedin_utils import (
    LINKEDIN_TIMELINE_SOURCE_ID,
    build_linkedin_activity_url,
    is_opaque_linkedin_member_id,
    linked_in_profile_url,
    normalize_linkedin_source_id,
)
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


async def _detect_empty_page_reason(page) -> str | None:
    try:
        text = await page.evaluate("""() => (document.body ? document.body.innerText : "").slice(0, 4000)""")
    except Exception:
        return None
    lowered = str(text or "").lower()
    if "follow" in lowered and "to see" in lowered and ("posts" in lowered or "activity" in lowered):
        return "follow_interstitial"
    if "no posts" in lowered or "hasn't posted" in lowered or "has not posted" in lowered:
        return "empty_profile"
    return None


logger = logging.getLogger(__name__)


def _linkedin_collection_urls(source_id: str) -> list[str]:
    normalized = normalize_linkedin_source_id(source_id)
    primary = build_linkedin_activity_url(normalized)
    if not normalized.startswith("in/"):
        return [primary]
    slug = normalized.split("/", 1)[1]
    candidates = [
        primary,
        f"https://www.linkedin.com/in/{slug}/posts/",
        f"https://www.linkedin.com/in/{slug}/detail/recent-activity/shares/",
    ]
    out: list[str] = []
    for candidate in candidates:
        if candidate not in out:
            out.append(candidate)
    return out


_LINKEDIN_POST_EXTRACTION_SCRIPT = """
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
  const parseRelativeTime = (raw) => {
    const value = String(raw || "").trim().toLowerCase();
    if (!value) return null;
    const cleaned = value
      .replace(/edited/g, "")
      .replace(/•/g, " ")
      .replace(/ago/g, "")
      .replace(/posted/g, "")
      .trim();
    const match = cleaned.match(/(\\d+)\\s*(s|sec|second|seconds|min|m|minute|minutes|h|hr|hour|hours|d|day|days|w|week|weeks|mo|month|months|y|yr|year|years)\\b/);
    if (!match) return null;
    const amount = Number(match[1]);
    const unit = match[2];
    const multipliers = {
      s: 1000, sec: 1000, second: 1000, seconds: 1000,
      min: 60000, m: 60000, minute: 60000, minutes: 60000,
      h: 3600000, hr: 3600000, hour: 3600000, hours: 3600000,
      d: 86400000, day: 86400000, days: 86400000,
      w: 604800000, week: 604800000, weeks: 604800000,
      mo: 2592000000, month: 2592000000, months: 2592000000,
      y: 31536000000, yr: 31536000000, year: 31536000000, years: 31536000000
    };
    const ms = multipliers[unit] || null;
    return ms ? new Date(Date.now() - amount * ms).toISOString() : null;
  };
  const firstText = (node, selectors) => {
    for (const selector of selectors) {
      const found = node.querySelector(selector);
      const text = found && (found.innerText || found.textContent || "").trim();
      if (text) return text;
    }
    return "";
  };
  const cleanedAuthor = (raw) => {
    const text = String(raw || "")
      .replace(/\\s+/g, " ")
      .replace(/^(view|open)\\s+/i, "")
      .trim();
    if (!text) return "";
    const lowered = text.toLowerCase();
    const badExact = new Set(["follow", "following", "connect", "message", "see more", "view profile"]);
    if (badExact.has(lowered)) return "";
    if (/^(follow|connect|message)\\b/i.test(text)) return "";
    return text;
  };
  const firstAuthor = (node) => {
    const selectors = [
      "a.update-components-actor__meta-link span[aria-hidden='true']",
      ".update-components-actor__name span[aria-hidden='true']",
      ".feed-shared-actor__name span[aria-hidden='true']",
      "[data-test-id*='actor-name']",
      "[data-control-name='actor']",
      ".update-components-actor__name",
      ".feed-shared-actor__name",
      "a[href*='/in/']",
      "a[href*='/company/']"
    ];
    for (const selector of selectors) {
      for (const found of Array.from(node.querySelectorAll(selector))) {
        const text = cleanedAuthor(found.getAttribute("aria-label") || found.innerText || found.textContent || "");
        if (text) return text;
      }
    }
    return null;
  };
  const containers = Array.from(new Set([
    ...document.querySelectorAll("div.feed-shared-update-v2, div.occludable-update"),
    ...document.querySelectorAll("article"),
    ...document.querySelectorAll("[data-urn*='activity']"),
    ...document.querySelectorAll("section.profile-creator-shared-feed-update__container"),
    ...document.querySelectorAll("div.scaffold-finite-scroll__content > div")
  ]));
  for (const node of containers) {
    const dataUrn = node.getAttribute("data-urn") || "";
    const anchor = Array.from(node.querySelectorAll('a[href*="/feed/update/"], a[href*="/posts/"], a[href*="activity-"], a[href*="urn:li:activity"]'))
      .map(a => a.getAttribute("href"))
      .find(Boolean);
    const hrefSource = anchor || dataUrn;
    if (!hrefSource) continue;
    const absUrl = hrefSource.startsWith("http")
      ? hrefSource
      : hrefSource.startsWith("/")
        ? `https://www.linkedin.com${hrefSource}`
        : `https://www.linkedin.com/feed/update/${hrefSource}`;
    const urnMatch = `${absUrl} ${dataUrn}`.match(/(?:activity[:\\/-]|urn:li:activity:)(\\d+)/);
    const extId = urnMatch ? urnMatch[1] : absUrl;

    const author = firstAuthor(node);

    const text = firstText(node, [
      "span.break-words",
      "div.update-components-text",
      ".feed-shared-update-v2__description",
      "[data-test-id*='main-feed-activity-card']",
      ".update-components-text"
    ]) || (node.innerText || "").slice(0, 1500).trim();
    if (!text) continue;

    const timeEl = node.querySelector("time");
    const timeText = firstText(node, [
      "time",
      ".update-components-actor__sub-description",
      ".feed-shared-actor__sub-description",
      "[data-test-id*='published-date']"
    ]);
    const publishedAt = (timeEl && timeEl.getAttribute("datetime")) || parseRelativeTime(timeText);
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
"""


@dataclass
class LinkedInTimelineCollector:
    session_manager: BrowserSessionManager
    max_items: int = 120
    platform: str = "linkedin"
    scroll_iterations: int = 8

    async def collect_from_source(self, source_id: str) -> list[CollectedItem]:
        normalized = normalize_linkedin_source_id(source_id)
        if normalized != LINKEDIN_TIMELINE_SOURCE_ID:
            raise ValueError(f"LinkedInTimelineCollector only supports {LINKEDIN_TIMELINE_SOURCE_ID}.")
        page = await self.session_manager.new_page(self.platform)
        raw_items: list[dict] = []
        try:
            await page.goto("https://www.linkedin.com/feed/", wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(3500)
            current_url = page.url
            if "linkedin.com/login" in current_url or "linkedin.com/uas/login" in current_url:
                raise SessionChallengedError(
                    platform=self.platform,
                    source_id=source_id,
                    current_url=current_url,
                    challenge_kind="login_redirect",
                )
            seen: dict[str, dict] = {}
            for iteration in range(max(1, self.scroll_iterations)):
                batch = await page.evaluate(_LINKEDIN_POST_EXTRACTION_SCRIPT, self.max_items)
                for item in batch or []:
                    key = str(item.get("external_id") or item.get("url") or "")
                    if not key:
                        continue
                    seen.setdefault(key, item)
                logger.debug(
                    "LinkedIn timeline scroll source=%s iteration=%d batch=%d total=%d",
                    source_id,
                    iteration + 1,
                    len(batch or []),
                    len(seen),
                )
                if len(seen) >= self.max_items:
                    break
                await page.mouse.wheel(0, 3000)
                await page.wait_for_timeout(900)
            raw_items = list(seen.values())[: self.max_items]
            if not raw_items:
                await self.session_manager.capture_empty_result_snapshot(
                    page,
                    platform=self.platform,
                    source_id=source_id,
                    sample_rate=1.0,
                    label="timeline_empty",
                    html_file_chars=30000,
                    include_source_in_filename=True,
                )
                signal = _detect_bot_shaped_signal(page)
                if signal is not None:
                    raise BotShapedError(
                        platform=self.platform,
                        source_id=source_id,
                        signal=signal,
                        current_url=getattr(page, "url", None),
                    )
        except Exception as exc:
            await self.session_manager.capture_debug_snapshot(page, "linkedin_timeline_collect_failed", error=exc)
            logger.exception("LinkedIn timeline collection failed for source=%s", source_id)
            raise
        finally:
            await page.close()

        out: list[CollectedItem] = []
        for item in raw_items:
            text = item.get("text")
            metadata = mark_low_quality_metadata(
                {
                    "source_id": LINKEDIN_TIMELINE_SOURCE_ID,
                    "resolved_source_id": LINKEDIN_TIMELINE_SOURCE_ID,
                    "linkedin_timeline": True,
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
                    external_id=f"timeline:{item['external_id']}",
                    author=item.get("author"),
                    url=item.get("url"),
                    text=text,
                    transcript_text=None,
                    published_at=item.get("published_at"),
                    metadata=metadata,
                )
            )
        logger.info("LinkedIn timeline collection succeeded source=%s items=%d", source_id, len(out))
        return out


@dataclass
class LinkedInCollector:
    session_manager: BrowserSessionManager
    max_items: int = 20
    platform: str = "linkedin"

    async def resolve_source_identifier(self, source_id: str) -> str | None:
        normalized = normalize_linkedin_source_id(source_id)
        if normalized == LINKEDIN_TIMELINE_SOURCE_ID:
            return LINKEDIN_TIMELINE_SOURCE_ID
        if not is_opaque_linkedin_member_id(normalized):
            return normalized
        page = await self.session_manager.new_page(self.platform)
        try:
            return await self._resolve_source_identifier_with_page(page, normalized)
        finally:
            await page.close()

    async def collect_from_source(self, source_id: str) -> list[CollectedItem]:
        source_normalized = normalize_linkedin_source_id(source_id)
        if source_normalized == LINKEDIN_TIMELINE_SOURCE_ID:
            return await LinkedInTimelineCollector(
                session_manager=self.session_manager,
                max_items=max(self.max_items, 80),
                platform=self.platform,
            ).collect_from_source(source_normalized)
        page = await self.session_manager.new_page(self.platform)
        raw_items: list[dict] = []
        try:
            source = await self._resolve_source_identifier_with_page(page, source_id) or normalize_linkedin_source_id(source_id)
            attempted_urls = _linkedin_collection_urls(source)
            for url in attempted_urls:
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
                raw_items = await page.evaluate(_LINKEDIN_POST_EXTRACTION_SCRIPT, self.max_items)
                if raw_items:
                    logger.info(
                        "LinkedIn collection succeeded source=%s resolved=%s url=%s items=%d",
                        source_id,
                        source,
                        url,
                        len(raw_items),
                    )
                    break
                logger.info("LinkedIn collection route returned no items source=%s url=%s", source_id, url)
            if not raw_items:
                await self.session_manager.capture_empty_result_snapshot(
                    page,
                    platform=self.platform,
                    source_id=source_id,
                    sample_rate=1.0,
                    label="empty",
                    html_file_chars=30000,
                    include_source_in_filename=True,
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
                empty_reason = await _detect_empty_page_reason(page)
                if empty_reason == "follow_interstitial":
                    raise BotShapedError(
                        platform=self.platform,
                        source_id=source_id,
                        signal=empty_reason,
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
