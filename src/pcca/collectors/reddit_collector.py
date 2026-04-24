from __future__ import annotations

import logging
from datetime import datetime, timezone
from dataclasses import dataclass

import httpx

from pcca.collectors.base import CollectedItem

logger = logging.getLogger(__name__)


@dataclass
class RedditCollector:
    max_posts_per_source: int = 20
    platform: str = "reddit"

    async def collect_from_source(self, source_id: str) -> list[CollectedItem]:
        source = source_id.strip()
        if source.startswith("r/"):
            path = f"/r/{source[2:]}/new.json"
        elif source.startswith("u/"):
            path = f"/user/{source[2:]}/submitted.json"
        elif source.startswith("/r/"):
            path = f"{source}/new.json"
        elif source.startswith("/user/"):
            path = f"{source}/submitted.json"
        else:
            path = f"/r/{source}/new.json"

        url = f"https://www.reddit.com{path}?limit={self.max_posts_per_source}"
        headers = {"User-Agent": "pcca/0.1 (+local agent)"}
        try:
            async with httpx.AsyncClient(timeout=20.0, headers=headers, follow_redirects=True) as client:
                response = await client.get(url)
                response.raise_for_status()
                payload = response.json()
        except Exception:
            logger.exception("Reddit collection failed for source=%s", source_id)
            raise

        children = payload.get("data", {}).get("children", [])
        items: list[CollectedItem] = []
        for child in children:
            data = child.get("data", {})
            post_id = data.get("id")
            if not post_id:
                continue
            permalink = data.get("permalink") or ""
            post_url = f"https://www.reddit.com{permalink}" if permalink else data.get("url")
            created_ts = data.get("created_utc")
            published_at = (
                datetime.fromtimestamp(created_ts, tz=timezone.utc).isoformat() if isinstance(created_ts, (int, float)) else None
            )
            body = data.get("selftext") or ""
            title = data.get("title") or ""
            text = (title + "\n\n" + body).strip()
            items.append(
                CollectedItem(
                    platform=self.platform,
                    external_id=post_id,
                    author=data.get("author"),
                    url=post_url,
                    text=text[:4000],
                    transcript_text=None,
                    published_at=published_at,
                    metadata={
                        "subreddit": data.get("subreddit"),
                        "score": data.get("score"),
                        "num_comments": data.get("num_comments"),
                    },
                )
            )
        return items
