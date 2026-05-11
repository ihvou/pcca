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
    """Detect raw JS/JSON object dumps captured as transcript text.

    Live target: YouTube music-video pages where the captured `raw_text` is
    the page's `window.WIZ_global_data = {...}` JS config because the video
    genuinely has no captions. **Do NOT trip on legitimate prose that
    happens to mention JS** — Karpathy / programming tutorials frequently
    start with words like "Window functions in SQL" or "Function overloading".

    Decision logic combines two independent signals; either is sufficient:
      A. **YouTube-specific token + JSON density**: very strong signal for
         the actual MJ-style page-config dumps we observe in the wild.
      B. **Generic high structural density**: real JS/JSON is 35-40%+
         `{}[]":,` chars in the first 500. Real English prose is <5%.
         Catches non-YouTube dumps without a token list update.

    A bare `startswith("window.", "function ", ...)` is NOT sufficient by
    itself; it produced false positives on Karpathy transcripts.
    """
    stripped = text.lstrip()
    first = stripped[:500]
    if not first:
        return False
    structural_chars = sum(1 for ch in first if ch in "{}[]\":,")
    density = structural_chars / max(1, len(first))
    has_jsdump_token = any(
        token in lowered[:800]
        for token in ("wiz_global_data", "ytinitialdata", "youtube_web", "web-front-end")
    )
    # Signal A: known YouTube dump token + non-trivial structural shape.
    # A mention of "youtube_web" in prose wouldn't have 10+ braces/quotes;
    # a real config dump has many.
    if has_jsdump_token and structural_chars >= 10:
        return True
    # Signal B: very high structural density on substantial content.
    # English prose densities are <5%. Spoken-word transcripts spell out
    # punctuation in words, so even code-heavy transcripts stay <0.20.
    # `density >= 0.35` on 200+ chars is structurally JS/JSON.
    if len(first) >= 200 and density >= 0.35 and structural_chars >= 80:
        return True
    return False


def _looks_like_link_list(text: str) -> bool:
    """Detect link-dump items (subscribe/follow blocks, channel link bundles).

    Two signals; either is sufficient:
      A. URL-heavy text: ≥3 URLs AND >5% URL-to-word ratio.
      B. ≥3 consecutive bullet lines **that each contain a URL** —
         classic "subscribe to my channel" layout.

    NOTE: A bare ≥3-consecutive-bullets rule false-positives on legitimate
    podcast/article episode outlines (e.g., "- Arguments from men who say
    refusing a gett is ok"). Require URL presence in the bullets so we
    only catch link-dumps, not content outlines.
    """
    urls = re.findall(r"https?://\S+", text)
    words = re.findall(r"\b[\w'-]+\b", text)
    if len(urls) >= 3 and len(urls) > max(1, len(words)) * 0.05:
        return True
    # Count words in non-bullet prose vs bullet-link content. Substantive
    # prose (e.g., a real article followed by 3 "see also" links) should
    # NOT be flagged as a link-list. Only flag when the bullets DOMINATE.
    bullet_link_words = 0
    prose_words = 0
    consecutive = 0
    bullet_link_streak_found = False
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            consecutive = 0
            continue
        line_words = re.findall(r"\b[\w'-]+\b", stripped)
        is_bullet_link = bool(
            re.match(r"^(?:[•*\-]|\d+\.)\s+", stripped)
            and re.search(r"https?://", stripped)
        )
        if is_bullet_link:
            consecutive += 1
            bullet_link_words += len(line_words)
            if consecutive >= 3:
                bullet_link_streak_found = True
        else:
            consecutive = 0
            prose_words += len(line_words)
    if not bullet_link_streak_found:
        return False
    # Bullet-link streak exists. Flag only when prose is thin enough that
    # the bullets ARE the content. ~50 words of prose is enough to make
    # an item Brief-worthy (a real first paragraph) even if a "see also"
    # link block follows.
    return prose_words < 50 or bullet_link_words > prose_words


def _looks_like_marketing(lowered: str) -> bool:
    phrase_hits = sum(1 for phrase in _MARKETING_PHRASES if phrase in lowered)
    if phrase_hits >= 2:
        return True
    hashtags = re.findall(r"#[a-z0-9_]+", lowered)
    unique_words = set(re.findall(r"\b[a-z][a-z0-9'-]{2,}\b", lowered))
    return len(hashtags) >= 4 and len(unique_words) <= 30
