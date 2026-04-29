from __future__ import annotations

import re
from urllib.parse import urlparse


def normalize_linkedin_source_id(raw: str) -> str:
    value = raw.strip()
    if not value:
        return ""
    if value.startswith("http://") or value.startswith("https://"):
        parsed = urlparse(value)
        parts = [part for part in (parsed.path or "").strip("/").split("/") if part]
        if len(parts) >= 2 and parts[0] in {"in", "company"}:
            return f"{parts[0]}/{parts[1]}"
        return value.rstrip("/")

    value = value.strip("/")
    if value.startswith("in/") or value.startswith("company/"):
        parts = value.split("/")
        return f"{parts[0]}/{parts[1]}" if len(parts) >= 2 and parts[1] else value
    return f"in/{value}"


def is_opaque_linkedin_member_id(source_id: str) -> bool:
    normalized = normalize_linkedin_source_id(source_id)
    if not normalized.startswith("in/"):
        return False
    slug = normalized.split("/", 1)[1]
    return bool(re.match(r"^ACo[A-Za-z0-9_-]+$", slug))


def build_linkedin_activity_url(source_id: str) -> str:
    normalized = normalize_linkedin_source_id(source_id)
    if normalized.startswith("company/"):
        return f"https://www.linkedin.com/{normalized}/posts/"
    if normalized.startswith("in/"):
        slug = normalized.split("/", 1)[1]
        return f"https://www.linkedin.com/in/{slug}/recent-activity/all/"
    return normalized


def linked_in_profile_url(source_id: str) -> str:
    normalized = normalize_linkedin_source_id(source_id)
    if normalized.startswith(("in/", "company/")):
        return f"https://www.linkedin.com/{normalized}/"
    return normalized
