from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from html.parser import HTMLParser
from typing import Any
from urllib.parse import quote
from xml.etree import ElementTree

import httpx

from pcca.browser.session_manager import BrowserSessionManager
from pcca.collectors.base import CollectedItem
from pcca.collectors.errors import SessionChallengedError
from pcca.collectors.youtube_utils import extract_video_id
from pcca.services.youtube_transcript_service import YouTubeTranscriptService

logger = logging.getLogger(__name__)

YOUTUBE_VIDEO_SELECTOR = "a#video-title-link, a#video-title"
YOUTUBE_CHANNEL_ID_RE = re.compile(r"\b(UC[A-Za-z0-9_-]{10,})\b")
YOUTUBE_RSS_URL = "https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
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


def youtube_rss_url(channel_id: str) -> str:
    return YOUTUBE_RSS_URL.format(channel_id=quote(channel_id.strip(), safe=""))


def is_youtube_channel_id(source_id: str) -> bool:
    return bool(YOUTUBE_CHANNEL_ID_RE.fullmatch(source_id.strip()))


def build_youtube_about_url(source_id: str) -> str:
    source = source_id.strip()
    if source.startswith("http://") or source.startswith("https://"):
        return source.rstrip("/") + "/about"
    if is_youtube_channel_id(source):
        return f"https://www.youtube.com/channel/{quote(source, safe='')}/about"
    handle = source if source.startswith("@") else f"@{source}"
    return f"https://www.youtube.com/{quote(handle, safe='@')}/about"


def extract_youtube_channel_id_from_html(html: str) -> str | None:
    if not html:
        return None
    patterns = [
        r'<link[^>]+rel=["\']canonical["\'][^>]+href=["\']https://www\.youtube\.com/channel/(UC[A-Za-z0-9_-]{10,})["\']',
        r'<meta[^>]+property=["\']og:url["\'][^>]+content=["\']https://www\.youtube\.com/channel/(UC[A-Za-z0-9_-]{10,})["\']',
        r'"channelId"\s*:\s*"(UC[A-Za-z0-9_-]{10,})"',
        r"/channel/(UC[A-Za-z0-9_-]{10,})",
    ]
    for pattern in patterns:
        match = re.search(pattern, html)
        if match:
            return match.group(1)
    fallback = YOUTUBE_CHANNEL_ID_RE.search(html)
    return fallback.group(1) if fallback else None


def _xml_text(node: ElementTree.Element | None) -> str | None:
    if node is None or node.text is None:
        return None
    return node.text.strip() or None


def parse_youtube_rss(feed_xml: str, *, max_items: int = 8) -> tuple[str | None, str | None, list[dict[str, Any]]]:
    root = ElementTree.fromstring(feed_xml)
    ns = {
        "atom": "http://www.w3.org/2005/Atom",
        "yt": "http://www.youtube.com/xml/schemas/2015",
        "media": "http://search.yahoo.com/mrss/",
    }
    channel_id = _xml_text(root.find("yt:channelId", ns))
    author_name = _xml_text(root.find("atom:author/atom:name", ns)) or _xml_text(root.find("atom:title", ns))
    rows: list[dict[str, Any]] = []
    for entry in root.findall("atom:entry", ns):
        video_id = _xml_text(entry.find("yt:videoId", ns))
        title = _xml_text(entry.find("atom:title", ns)) or _xml_text(entry.find("media:group/media:title", ns))
        published_at = _xml_text(entry.find("atom:published", ns))
        description = _xml_text(entry.find("media:group/media:description", ns))
        link = entry.find("atom:link", ns)
        url = link.attrib.get("href") if link is not None else None
        statistics = entry.find("media:group/media:community/media:statistics", ns)
        view_count = None
        if statistics is not None and statistics.attrib.get("views"):
            try:
                view_count = int(statistics.attrib["views"])
            except ValueError:
                view_count = None
        if not video_id and url:
            video_id = extract_video_id(url)
        if not video_id or not title:
            continue
        rows.append(
            {
                "video_id": video_id,
                "channel_id": _xml_text(entry.find("yt:channelId", ns)) or channel_id,
                "channel_name": author_name,
                "url": url or f"https://www.youtube.com/watch?v={video_id}",
                "title": title,
                "description": description,
                "published_at": published_at,
                "view_count": view_count,
            }
        )
        if len(rows) >= max_items:
            break
    return channel_id, author_name, rows


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
    session_manager: BrowserSessionManager | None = None
    transcript_service: YouTubeTranscriptService = field(default_factory=YouTubeTranscriptService)
    max_items: int = 8
    platform: str = "youtube"
    http_client: httpx.AsyncClient | None = None

    async def _get_text(self, url: str) -> str:
        if self.http_client is not None:
            response = await self.http_client.get(url, follow_redirects=True)
            response.raise_for_status()
            return response.text
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            response = await client.get(
                url,
                headers={"User-Agent": "Mozilla/5.0 PCCA/0.1 (+https://youtube.com)"},
            )
            response.raise_for_status()
            return response.text

    async def resolve_source_identifier(self, source_id: str) -> str:
        source = source_id.strip()
        if is_youtube_channel_id(source):
            return source
        about_url = build_youtube_about_url(source)
        html = await self._get_text(about_url)
        channel_id = extract_youtube_channel_id_from_html(html)
        if not channel_id:
            raise RuntimeError(f"Could not resolve YouTube channel id for {source_id}.")
        logger.info("Resolved YouTube source identifier old=%s new=%s about_url=%s", source_id, channel_id, about_url)
        return channel_id

    async def collect_from_source(self, source_id: str) -> list[CollectedItem]:
        channel_id = source_id.strip()
        if not is_youtube_channel_id(channel_id):
            channel_id = await self.resolve_source_identifier(source_id)
        feed_url = youtube_rss_url(channel_id)
        try:
            feed_xml = await self._get_text(feed_url)
            parsed_channel_id, channel_name, raw_videos = parse_youtube_rss(feed_xml, max_items=self.max_items)
            channel_id = parsed_channel_id or channel_id
        except Exception:
            logger.exception("YouTube RSS collection failed for source=%s feed_url=%s", source_id, feed_url)
            raise

        results: list[CollectedItem] = []
        for row in raw_videos:
            video_url = row.get("url")
            video_id = row.get("video_id") or extract_video_id(video_url or "")
            if not video_id:
                continue
            transcript_text = await self.transcript_service.get_transcript_text(video_id)
            title = row.get("title") or ""
            description = row.get("description") or ""
            text = "\n\n".join(part for part in (title, description) if part).strip()
            if transcript_text:
                snippet = transcript_text[:1200]
                text = f"{text}\n\n{snippet}".strip()
            metadata = {
                "source_id": source_id,
                "channel_id": row.get("channel_id") or channel_id,
                "title": title,
                "description": description,
                "rss_feed_url": feed_url,
                "view_count": row.get("view_count"),
            }
            results.append(
                CollectedItem(
                    platform=self.platform,
                    external_id=video_id,
                    author=row.get("channel_name") or channel_name or source_id,
                    url=video_url,
                    text=text,
                    transcript_text=transcript_text,
                    published_at=row.get("published_at"),
                    metadata=metadata,
                )
            )
        return results
