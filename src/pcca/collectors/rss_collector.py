from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from dataclasses import dataclass

from pcca.collectors.base import CollectedItem

logger = logging.getLogger(__name__)


@dataclass
class RSSCollector:
    max_entries_per_source: int = 20
    platform: str = "rss"

    async def collect_from_source(self, source_id: str) -> list[CollectedItem]:
        try:
            import feedparser
        except Exception:
            logger.warning("feedparser not installed; RSS collector skipped.")
            return []

        feed = await asyncio.to_thread(feedparser.parse, source_id)
        entries = feed.entries[: self.max_entries_per_source]
        items: list[CollectedItem] = []
        for entry in entries:
            ext_id = getattr(entry, "id", None) or getattr(entry, "link", None)
            if not ext_id:
                continue
            published_at = None
            if getattr(entry, "published_parsed", None):
                published_at = datetime(*entry.published_parsed[:6]).isoformat()
            summary = getattr(entry, "summary", "") or ""
            title = getattr(entry, "title", "") or ""
            text = (title + "\n\n" + summary).strip()[:4000]
            items.append(
                CollectedItem(
                    platform=self.platform,
                    external_id=str(ext_id),
                    author=getattr(entry, "author", None),
                    url=getattr(entry, "link", None),
                    text=text,
                    transcript_text=None,
                    published_at=published_at,
                    metadata={"feed_url": source_id},
                )
            )
        return items
