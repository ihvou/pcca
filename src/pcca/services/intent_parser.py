from __future__ import annotations

import re

from pcca.models import IntentAction, ParsedIntent


CREATE_PATTERNS = (
    r"\bcreate\b.*\b(subject|topic)\b",
    r"\bnew\b.*\b(subject|topic)\b",
    r"\bi want\b.*\b(subject|topic)\b",
    r"\btrack\b.*\bin\b.*\bseparate\b",
)

LIST_PATTERNS = (
    r"\blist\b.*\b(subjects|topics)\b",
    r"\bwhat\b.*\b(subjects|topics)\b",
)

LIST_SOURCES_PATTERNS = (
    r"\blist\b.*\bsources\b",
    r"\bwhat\b.*\bsources\b",
)

PLATFORMS = ("x", "linkedin", "youtube", "reddit", "rss")


def _extract_subject_name(text: str) -> str | None:
    # Examples:
    # - "Create subject: Agentic PM"
    # - "I want a new topic for Vibe Coding"
    # - "new subject 'Ukraine OSINT'"
    by_colon = re.search(r"(?:subject|topic)\s*:\s*(.+)$", text, flags=re.IGNORECASE)
    if by_colon:
        return by_colon.group(1).strip().strip("\"'")

    by_for = re.search(r"\b(?:subject|topic)\b.*?\bfor\b\s+(.+)$", text, flags=re.IGNORECASE)
    if by_for:
        return by_for.group(1).strip().strip("\"'")

    by_quotes = re.search(r"['\"]([^'\"]{2,80})['\"]", text)
    if by_quotes:
        return by_quotes.group(1).strip()

    return None


def _extract_subject_for_sources(text: str) -> str | None:
    by_for = re.search(r"\bfor\b\s+(.+)$", text, flags=re.IGNORECASE)
    if by_for:
        return by_for.group(1).strip().strip("\"'")
    return _extract_subject_name(text)


def _extract_add_source(text: str) -> tuple[str | None, str | None, str | None]:
    # Format examples:
    # - "add source x:borischerny to Vibe Coding"
    # - "add source youtube:UC123 to Agentic PM"
    # - "track borischerny on x for Vibe Coding"
    source_match = re.search(r"\b(x|linkedin|youtube|reddit|rss)\s*[:=]\s*([^\s,]+)", text, flags=re.IGNORECASE)
    platform = None
    source_id = None
    if source_match:
        platform = source_match.group(1).lower()
        source_id = source_match.group(2).strip()
    else:
        platform_match = re.search(r"\b(on|from)\s+(x|linkedin|youtube|reddit|rss)\b", text, flags=re.IGNORECASE)
        if platform_match:
            platform = platform_match.group(2).lower()
            handle_match = re.search(r"\b(track|add)\s+([@\w\-.]+)", text, flags=re.IGNORECASE)
            if handle_match:
                source_id = handle_match.group(2).lstrip("@")

    subject_match = re.search(r"\b(to|for)\s+(.+)$", text, flags=re.IGNORECASE)
    subject_name = subject_match.group(2).strip().strip("\"'") if subject_match else None
    return platform, source_id, subject_name


def _extract_source_url(text: str) -> str | None:
    match = re.search(r"(https?://[^\s]+)", text, flags=re.IGNORECASE)
    if not match:
        return None
    return match.group(1).strip().rstrip(".,)")


def parse_intent(text: str) -> ParsedIntent:
    normalized = text.strip()
    lowered = normalized.lower()

    if lowered in {"help", "/help", "what can you do?"}:
        return ParsedIntent(action=IntentAction.HELP, raw_text=text)

    for pattern in CREATE_PATTERNS:
        if re.search(pattern, lowered):
            return ParsedIntent(
                action=IntentAction.CREATE_SUBJECT,
                subject_name=_extract_subject_name(normalized),
                raw_text=text,
            )

    for pattern in LIST_PATTERNS:
        if re.search(pattern, lowered):
            return ParsedIntent(action=IntentAction.LIST_SUBJECTS, raw_text=text)

    for pattern in LIST_SOURCES_PATTERNS:
        if re.search(pattern, lowered):
            return ParsedIntent(
                action=IntentAction.LIST_SOURCES,
                subject_name=_extract_subject_for_sources(normalized),
                raw_text=text,
            )

    if "add source" in lowered or "track " in lowered:
        source_url = _extract_source_url(normalized)
        if source_url:
            subject_match = re.search(r"\b(to|for)\s+(.+)$", normalized, flags=re.IGNORECASE)
            subject_name = subject_match.group(2).strip().strip("\"'") if subject_match else None
            return ParsedIntent(
                action=IntentAction.ADD_SOURCE_URL,
                source_url=source_url,
                subject_name=subject_name,
                raw_text=text,
            )
        platform, source_id, subject_name = _extract_add_source(normalized)
        if platform in PLATFORMS and source_id:
            return ParsedIntent(
                action=IntentAction.ADD_SOURCE,
                platform=platform,
                source_id=source_id,
                subject_name=subject_name,
                raw_text=text,
            )

    return ParsedIntent(action=IntentAction.UNKNOWN, raw_text=text)
