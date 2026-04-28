from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

from pcca.browser.session_manager import BrowserSessionManager
from pcca.collectors.base import CollectedItem
from pcca.collectors.errors import SessionChallengedError
from pcca.collectors.youtube_utils import build_channel_videos_url, extract_video_id
from pcca.services.youtube_transcript_service import YouTubeTranscriptService

logger = logging.getLogger(__name__)


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
            await page.wait_for_timeout(2500)
            current_url = page.url
            if is_youtube_login_url(current_url):
                raise SessionChallengedError(
                    platform=self.platform,
                    source_id=source_id,
                    current_url=current_url,
                    challenge_kind="login_redirect",
                )
            raw_videos = await page.evaluate(
                """
                (maxItems) => {
                  const out = [];
                  const channelName =
                    (document.querySelector("ytd-channel-name yt-formatted-string")?.textContent || "").trim() ||
                    (document.querySelector("meta[itemprop='name']")?.getAttribute("content") || "").trim() ||
                    null;
                  const links = Array.from(document.querySelectorAll("a#video-title-link, a#video-title"));
                  for (const link of links) {
                    const href = link.getAttribute("href");
                    if (!href || !href.includes("watch")) continue;
                    const absUrl = href.startsWith("http") ? href : `https://www.youtube.com${href}`;
                    const title = (link.textContent || "").trim();
                    const container = link.closest("ytd-rich-item-renderer, ytd-grid-video-renderer, ytd-video-renderer");
                    const metaText = container ? (container.innerText || "") : "";
                    out.push({ url: absUrl, title, channel_name: channelName, meta_text: metaText.slice(0, 500) });
                    if (out.length >= maxItems) break;
                  }
                  return out;
                }
                """,
                self.max_items,
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
