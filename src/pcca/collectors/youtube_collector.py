from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from html.parser import HTMLParser
from typing import Any

from pcca.browser.session_manager import BrowserSessionManager
from pcca.collectors.base import CollectedItem
from pcca.collectors.errors import SessionChallengedError
from pcca.collectors.youtube_utils import build_channel_videos_url, extract_video_id
from pcca.services.youtube_transcript_service import YouTubeTranscriptService

logger = logging.getLogger(__name__)

YOUTUBE_VIDEO_SELECTOR = "a#video-title-link, a#video-title"
YOUTUBE_DOM_EXTRACTION_JS = """
(maxItems) => {
  const out = [];
  const channelName =
    (document.querySelector("ytd-channel-name yt-formatted-string")?.textContent || "").trim() ||
    (document.querySelector("meta[itemprop='name']")?.getAttribute("content") || "").trim() ||
    (document.querySelector("meta[property='og:title']")?.getAttribute("content") || "").trim() ||
    null;
  const links = Array.from(document.querySelectorAll("a#video-title-link, a#video-title"));
  for (const link of links) {
    const href = link.getAttribute("href");
    if (!href || !href.includes("watch")) continue;
    const absUrl = href.startsWith("http") ? href : `https://www.youtube.com${href}`;
    const title = (link.getAttribute("title") || link.textContent || "").trim();
    const container = link.closest("ytd-rich-item-renderer, ytd-grid-video-renderer, ytd-video-renderer");
    const metaText = container ? (container.innerText || "") : "";
    out.push({ url: absUrl, title, channel_name: channelName, meta_text: metaText.slice(0, 500) });
    if (out.length >= maxItems) break;
  }
  return out;
}
"""


def parse_count_text(value: str | None) -> int | None:
    if not value:
        return None
    match = re.search(r"([0-9][0-9,.]*)([kKmM]?)", value)
    if not match:
        return None
    number = float(match.group(1).replace(",", ""))
    suffix = match.group(2).lower()
    multiplier = 1_000_000 if suffix == "m" else 1_000 if suffix == "k" else 1
    return int(number * multiplier)


def parse_youtube_meta(meta_text: str | None) -> dict:
    text = meta_text or ""
    views_match = re.search(r"([0-9][0-9,.]*[kKmM]?)\s+views?", text)
    duration_match = re.search(r"\b(?:(\d+):)?(\d{1,2}):(\d{2})\b", text)
    duration_seconds = None
    if duration_match:
        hours = int(duration_match.group(1) or 0)
        minutes = int(duration_match.group(2))
        seconds = int(duration_match.group(3))
        duration_seconds = hours * 3600 + minutes * 60 + seconds
    return {
        "view_count": parse_count_text(views_match.group(1)) if views_match else None,
        "duration_seconds": duration_seconds,
    }


def is_youtube_login_url(url: str) -> bool:
    lowered = url.lower()
    return (
        "accounts.google.com" in lowered
        or "/signin/" in lowered
        or "youtube.com/signin" in lowered
        or "service=youtube" in lowered
        or "youtube.com/o/oauth" in lowered
    )


def detect_youtube_interstitial(title: str | None, body_text: str | None = None) -> str | None:
    text = f"{title or ''}\n{body_text or ''}".lower()
    markers = {
        "consent_wall": [
            "before you continue to youtube",
            "before you continue",
            "review your choices",
            "accept all",
        ],
        "age_gate": ["age-restricted", "sign in to confirm your age"],
        "bot_challenge": ["unusual traffic", "our systems have detected unusual traffic"],
    }
    for kind, needles in markers.items():
        if any(needle in text for needle in needles):
            return kind
    return None


class _YouTubeVideoLinkParser(HTMLParser):
    def __init__(self, max_items: int) -> None:
        super().__init__()
        self.max_items = max_items
        self.rows: list[dict[str, Any]] = []
        self._capture: dict[str, Any] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if len(self.rows) >= self.max_items or tag.lower() != "a":
            return
        data = {key.lower(): value or "" for key, value in attrs}
        element_id = data.get("id", "")
        href = data.get("href", "")
        if element_id not in {"video-title-link", "video-title"} or "watch" not in href:
            return
        self._capture = {
            "href": href,
            "title": data.get("title", ""),
            "text": [],
        }

    def handle_data(self, data: str) -> None:
        if self._capture is not None:
            self._capture["text"].append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() != "a" or self._capture is None:
            return
        href = str(self._capture["href"])
        url = href if href.startswith("http") else f"https://www.youtube.com{href}"
        title = str(self._capture["title"] or "".join(self._capture["text"])).strip()
        self.rows.append({"url": url, "title": title, "channel_name": None, "meta_text": ""})
        self._capture = None


def extract_youtube_dom_video_rows(html: str, *, max_items: int = 8) -> list[dict[str, Any]]:
    parser = _YouTubeVideoLinkParser(max_items=max_items)
    parser.feed(html)
    return parser.rows[:max_items]


def extract_yt_initial_data_from_html(html: str) -> dict[str, Any] | None:
    marker = "ytInitialData"
    idx = html.find(marker)
    while idx >= 0:
        equals = html.find("=", idx)
        brace = html.find("{", equals)
        if equals < 0 or brace < 0:
            return None
        depth = 0
        in_string = False
        escaped = False
        for pos in range(brace, len(html)):
            ch = html[pos]
            if in_string:
                if escaped:
                    escaped = False
                elif ch == "\\":
                    escaped = True
                elif ch == '"':
                    in_string = False
                continue
            if ch == '"':
                in_string = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    try:
                        payload = json.loads(html[brace : pos + 1])
                    except json.JSONDecodeError:
                        break
                    return payload if isinstance(payload, dict) else None
        idx = html.find(marker, idx + len(marker))
    return None


def extract_youtube_initial_data_video_rows(data: dict[str, Any] | None, *, max_items: int = 8) -> list[dict[str, Any]]:
    if not data:
        return []
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()

    def text_value(value: Any) -> str | None:
        if isinstance(value, str):
            return value
        if isinstance(value, dict):
            if isinstance(value.get("simpleText"), str):
                return value["simpleText"]
            runs = value.get("runs")
            if isinstance(runs, list):
                return "".join(str(run.get("text", "")) for run in runs if isinstance(run, dict)).strip() or None
        return None

    def visit(node: Any) -> None:
        if len(rows) >= max_items:
            return
        if isinstance(node, dict):
            renderer = None
            for key in ("videoRenderer", "gridVideoRenderer", "compactVideoRenderer"):
                value = node.get(key)
                if isinstance(value, dict):
                    renderer = value
                    break
            if renderer is not None:
                video_id = renderer.get("videoId")
                if isinstance(video_id, str) and video_id and video_id not in seen:
                    seen.add(video_id)
                    title = text_value(renderer.get("title")) or ""
                    nav_url = (
                        renderer.get("navigationEndpoint", {})
                        .get("commandMetadata", {})
                        .get("webCommandMetadata", {})
                        .get("url")
                    )
                    url = nav_url if isinstance(nav_url, str) and nav_url else f"/watch?v={video_id}"
                    if url.startswith("/"):
                        url = f"https://www.youtube.com{url}"
                    owner = (
                        text_value(renderer.get("ownerText"))
                        or text_value(renderer.get("longBylineText"))
                        or text_value(renderer.get("shortBylineText"))
                    )
                    meta_parts = [
                        text_value(renderer.get("viewCountText")),
                        text_value(renderer.get("lengthText")),
                        text_value(renderer.get("publishedTimeText")),
                    ]
                    rows.append(
                        {
                            "url": url,
                            "title": title,
                            "channel_name": owner,
                            "meta_text": " ".join(part for part in meta_parts if part),
                        }
                    )
            for value in node.values():
                visit(value)
        elif isinstance(node, list):
            for value in node:
                visit(value)

    visit(data)
    return rows[:max_items]


@dataclass
class YouTubeCollector:
    session_manager: BrowserSessionManager
    transcript_service: YouTubeTranscriptService = field(default_factory=YouTubeTranscriptService)
    max_items: int = 8
    platform: str = "youtube"

    async def collect_from_source(self, source_id: str) -> list[CollectedItem]:
        url = build_channel_videos_url(source_id)
        page = await self.session_manager.new_page(self.platform)
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            current_url = page.url
            if is_youtube_login_url(current_url):
                raise SessionChallengedError(
                    platform=self.platform,
                    source_id=source_id,
                    current_url=current_url,
                    challenge_kind="login_redirect",
                )
            try:
                await page.wait_for_selector(YOUTUBE_VIDEO_SELECTOR, timeout=15000)
            except Exception:
                logger.warning(
                    "YouTube video selector did not appear before fallback source=%s url=%s selector=%s",
                    source_id,
                    page.url,
                    YOUTUBE_VIDEO_SELECTOR,
                    exc_info=True,
                )

            raw_videos = await page.evaluate(YOUTUBE_DOM_EXTRACTION_JS, self.max_items)
            if not raw_videos:
                html = await page.content()
                title = await page.title()
                body_text = await page.evaluate(
                    "() => document.body ? document.body.innerText.slice(0, 4000) : ''"
                )
                interstitial = detect_youtube_interstitial(title, body_text)
                if interstitial:
                    logger.warning(
                        "YouTube interstitial detected source=%s url=%s title=%s interstitial=%s consent_wall=%s",
                        source_id,
                        page.url,
                        title,
                        interstitial,
                        interstitial == "consent_wall",
                    )
                    await self.session_manager.capture_empty_result_snapshot(
                        page,
                        platform=self.platform,
                        source_id=source_id,
                        sample_rate=1.0,
                    )
                else:
                    raw_videos = extract_youtube_initial_data_video_rows(
                        extract_yt_initial_data_from_html(html),
                        max_items=self.max_items,
                    )
                    if raw_videos:
                        logger.info(
                            "YouTube ytInitialData fallback extracted source=%s items=%d",
                            source_id,
                            len(raw_videos),
                        )
                    else:
                        await self.session_manager.capture_empty_result_snapshot(
                            page,
                            platform=self.platform,
                            source_id=source_id,
                            sample_rate=1.0,
                        )
        except Exception as exc:
            await self.session_manager.capture_debug_snapshot(page, "youtube_collect_failed", error=exc)
            logger.exception("YouTube collection failed for source=%s", source_id)
            raise
        finally:
            await page.close()

        results: list[CollectedItem] = []
        for row in raw_videos:
            video_url = row.get("url")
            video_id = extract_video_id(video_url or "")
            if not video_id:
                continue
            transcript_text = await self.transcript_service.get_transcript_text(video_id)
            text = row.get("title") or ""
            if transcript_text:
                snippet = transcript_text[:1200]
                text = f"{text}\n\n{snippet}".strip()
            metadata = {
                "source_id": source_id,
                "title": row.get("title"),
                "meta_text": row.get("meta_text"),
                **parse_youtube_meta(row.get("meta_text")),
            }
            results.append(
                CollectedItem(
                    platform=self.platform,
                    external_id=video_id,
                    author=row.get("channel_name") or source_id,
                    url=video_url,
                    text=text,
                    transcript_text=transcript_text,
                    published_at=None,
                    metadata=metadata,
                )
            )
        return results
