from __future__ import annotations

import re
from typing import Any


EXCLUDED_FROM_BRIEFS_KEY = "excluded_from_briefs_reason"
FORCE_KEEP_KEY = "_pcca_force_keep"

_MARKETING_PHRASES = (
    "we've won",
    "we have won",
    "g2 award",
    "roi",
    "launching @",
    "we are excited to announce",
    "honored to",
    "thrilled to share",
)


def is_low_quality(text: str | None) -> str | None:
    value = " ".join(str(text or "").split())
    if not value:
        return None
    lowered = value.lower()
    if _looks_like_js_dump(value, lowered):
        return "js_dump"
    if _looks_like_link_list(str(text or "")):
        return "link_list"
    if _looks_like_marketing(lowered):
        return "marketing_prose"
    return None


def mark_low_quality_metadata(metadata: dict[str, Any] | None, text: str | None) -> dict[str, Any]:
    out = dict(metadata or {})
    if out.get(FORCE_KEEP_KEY):
        out.pop(EXCLUDED_FROM_BRIEFS_KEY, None)
        return out
    reason = is_low_quality(text)
    if reason:
        out[EXCLUDED_FROM_BRIEFS_KEY] = reason
    return out


def excluded_from_briefs_reason(metadata: dict[str, Any] | None, text: str | None = None) -> str | None:
    metadata = metadata if isinstance(metadata, dict) else {}
    if metadata.get(FORCE_KEEP_KEY):
        return None
    reason = metadata.get(EXCLUDED_FROM_BRIEFS_KEY)
    if reason:
        return str(reason)
    return is_low_quality(text)


def _looks_like_js_dump(text: str, lowered: str) -> bool:
    stripped = text.lstrip()
    if stripped.startswith(("window.", "function ", "var ytInitialData", "ytInitialData")):
        return True
    first = stripped[:500]
    if not first:
        return False
    structural_chars = sum(1 for ch in first if ch in "{}[]\":,")
    density = structural_chars / max(1, len(first))
    return density > 0.25 and structural_chars >= 40 and any(
        token in lowered[:800]
        for token in ("wiz_global_data", "ytinitialdata", "youtube_web", "web-front-end")
    )


def _looks_like_link_list(text: str) -> bool:
    urls = re.findall(r"https?://\S+", text)
    words = re.findall(r"\b[\w'-]+\b", text)
    if len(urls) >= 3 and len(urls) > max(1, len(words)) * 0.05:
        return True
    consecutive = 0
    for line in text.splitlines():
        stripped = line.strip()
        if re.match(r"^(?:[•*\-]|\d+\.)\s+", stripped):
            consecutive += 1
            if consecutive >= 3:
                return True
        elif stripped:
            consecutive = 0
    return False


def _looks_like_marketing(lowered: str) -> bool:
    phrase_hits = sum(1 for phrase in _MARKETING_PHRASES if phrase in lowered)
    if phrase_hits >= 2:
        return True
    hashtags = re.findall(r"#[a-z0-9_]+", lowered)
    unique_words = set(re.findall(r"\b[a-z][a-z0-9'-]{2,}\b", lowered))
    return len(hashtags) >= 4 and len(unique_words) <= 30
