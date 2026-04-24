from __future__ import annotations

import re
from urllib.parse import parse_qs, urlparse


def extract_video_id(value: str) -> str | None:
    raw = value.strip()
    if re.fullmatch(r"[A-Za-z0-9_-]{11}", raw):
        return raw

    if raw.startswith("http://") or raw.startswith("https://"):
        parsed = urlparse(raw)
        if "youtu.be" in parsed.netloc:
            video_id = parsed.path.strip("/")
            return video_id if video_id else None
        if "/watch" in parsed.path:
            return parse_qs(parsed.query).get("v", [None])[0]
        if "/shorts/" in parsed.path:
            m = re.search(r"/shorts/([A-Za-z0-9_-]{11})", parsed.path)
            return m.group(1) if m else None
    return None


def build_channel_videos_url(source_id: str) -> str:
    source = source_id.strip()
    if source.startswith("http://") or source.startswith("https://"):
        return source if source.endswith("/videos") else source.rstrip("/") + "/videos"
    if source.startswith("@"):
        return f"https://www.youtube.com/{source}/videos"
    if source.startswith("UC"):
        return f"https://www.youtube.com/channel/{source}/videos"
    return f"https://www.youtube.com/@{source}/videos"

