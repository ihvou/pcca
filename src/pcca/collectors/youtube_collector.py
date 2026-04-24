from __future__ import annotations

import logging
from dataclasses import dataclass, field

from pcca.browser.session_manager import BrowserSessionManager
from pcca.collectors.base import CollectedItem
from pcca.collectors.youtube_utils import build_channel_videos_url, extract_video_id
from pcca.services.youtube_transcript_service import YouTubeTranscriptService

logger = logging.getLogger(__name__)


@dataclass
class YouTubeCollector:
    session_manager: BrowserSessionManager
    transcript_service: YouTubeTranscriptService = field(default_factory=YouTubeTranscriptService)
    max_videos_per_source: int = 8
    platform: str = "youtube"

    async def collect_from_source(self, source_id: str) -> list[CollectedItem]:
        url = build_channel_videos_url(source_id)
        page = await self.session_manager.new_page(self.platform)
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(2500)
            raw_videos = await page.evaluate(
                """
                (maxItems) => {
                  const out = [];
                  const links = Array.from(document.querySelectorAll("a#video-title-link, a#video-title"));
                  for (const link of links) {
                    const href = link.getAttribute("href");
                    if (!href || !href.includes("watch")) continue;
                    const absUrl = href.startsWith("http") ? href : `https://www.youtube.com${href}`;
                    const title = (link.textContent || "").trim();
                    out.push({ url: absUrl, title });
                    if (out.length >= maxItems) break;
                  }
                  return out;
                }
                """,
                self.max_videos_per_source,
            )
        except Exception:
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
            results.append(
                CollectedItem(
                    platform=self.platform,
                    external_id=video_id,
                    author=source_id,
                    url=video_url,
                    text=text,
                    transcript_text=transcript_text,
                    published_at=None,
                    metadata={"source_id": source_id, "title": row.get("title")},
                )
            )
        return results
