from __future__ import annotations

import logging
import re
from dataclasses import dataclass

from pcca.browser.session_manager import BrowserSessionManager
from pcca.collectors.base import CollectedItem

logger = logging.getLogger(__name__)


def normalize_spotify_show_source(source_id: str) -> str:
    raw = source_id.strip()
    if raw.startswith("http://") or raw.startswith("https://"):
        m = re.search(r"/show/([A-Za-z0-9]+)", raw)
        if m:
            return f"https://open.spotify.com/show/{m.group(1)}"
        return raw
    if re.fullmatch(r"[A-Za-z0-9]{10,}", raw):
        return f"https://open.spotify.com/show/{raw}"
    return raw


@dataclass
class SpotifyCollector:
    session_manager: BrowserSessionManager
    max_episodes_per_source: int = 12
    platform: str = "spotify"

    async def collect_from_source(self, source_id: str) -> list[CollectedItem]:
        show_url = normalize_spotify_show_source(source_id)
        page = await self.session_manager.new_page(self.platform)
        try:
            await page.goto(show_url, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(3200)
            raw_items = await page.evaluate(
                """
                (maxItems) => {
                  const out = [];
                  const links = Array.from(document.querySelectorAll('a[href*="/episode/"]'));
                  for (const link of links) {
                    const href = link.getAttribute("href") || "";
                    if (!href.includes("/episode/")) continue;
                    const abs = href.startsWith("http") ? href : `https://open.spotify.com${href}`;
                    const idMatch = abs.match(/\\/episode\\/([A-Za-z0-9]+)/);
                    const extId = idMatch ? idMatch[1] : abs;
                    const title = (link.textContent || "").trim();
                    if (!title) continue;
                    out.push({ external_id: extId, url: abs, title });
                    if (out.length >= maxItems) break;
                  }
                  return out;
                }
                """,
                self.max_episodes_per_source,
            )
        except Exception:
            logger.exception("Spotify collection failed for source=%s", source_id)
            raise
        finally:
            await page.close()

        # stable unique by episode id
        seen: set[str] = set()
        rows: list[CollectedItem] = []
        for row in raw_items:
            ext_id = str(row.get("external_id") or "").strip()
            if not ext_id or ext_id in seen:
                continue
            seen.add(ext_id)
            title = str(row.get("title") or "").strip()
            rows.append(
                CollectedItem(
                    platform=self.platform,
                    external_id=ext_id,
                    author=None,
                    url=row.get("url"),
                    text=title,
                    transcript_text=None,
                    published_at=None,
                    metadata={"source_id": source_id},
                )
            )
        return rows
