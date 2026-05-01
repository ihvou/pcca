from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from pcca.collectors.base import CollectedItem


SHORT_ITEM_WORD_THRESHOLD = 600
WINDOW_WORDS = 300
OVERLAP_WORDS = 50
TIMED_TARGET_WORDS = 250
TIMED_TARGET_SECONDS = 180


@dataclass(frozen=True)
class SegmentDraft:
    text: str
    segment_type: str
    start_offset: int
    end_offset: int
    start_offset_seconds: float | None = None
    end_offset_seconds: float | None = None


def segment_item(item: CollectedItem) -> list[SegmentDraft]:
    """Split an item into the curation units T-11 needs.

    Prefer timestamped transcript rows when collectors provide them; otherwise
    use transcript text, then raw text, falling back to one compact segment for
    short-form items.
    """

    rows = _transcript_rows(item)
    if rows:
        timed = _segments_from_transcript_rows(rows)
        if timed:
            return timed

    text = (item.transcript_text or "").strip() or (item.text or "").strip()
    if not text:
        return []
    words = _words(text)
    if len(words) < SHORT_ITEM_WORD_THRESHOLD:
        has_timed_body = bool((item.transcript_text or "").strip())
        return [
            SegmentDraft(
                text=text,
                segment_type="single",
                start_offset=0,
                end_offset=len(text),
                start_offset_seconds=0.0 if has_timed_body and item.platform in {"youtube", "spotify", "apple_podcasts"} else None,
                end_offset_seconds=None,
            )
        ]
    return _window_segments(text)


def _transcript_rows(item: CollectedItem) -> list[dict[str, Any]]:
    metadata = item.metadata if isinstance(item.metadata, dict) else {}
    raw = metadata.get("transcript_rows")
    if not isinstance(raw, list):
        return []
    rows: list[dict[str, Any]] = []
    for row in raw:
        if not isinstance(row, dict):
            continue
        text = str(row.get("text") or "").strip()
        if not text:
            continue
        start = _float_or_none(row.get("start"))
        duration = _float_or_none(row.get("duration"))
        rows.append({"text": text, "start": start, "duration": duration})
    return rows


def _segments_from_transcript_rows(rows: list[dict[str, Any]]) -> list[SegmentDraft]:
    segments: list[SegmentDraft] = []
    current: list[dict[str, Any]] = []
    current_words = 0
    current_start: float | None = None
    current_end: float | None = None

    def flush() -> None:
        nonlocal current, current_words, current_start, current_end
        if not current:
            return
        text = "\n".join(str(row["text"]).strip() for row in current if str(row["text"]).strip()).strip()
        if text:
            segments.append(
                SegmentDraft(
                    text=text,
                    segment_type="transcript",
                    start_offset=0,
                    end_offset=len(text),
                    start_offset_seconds=current_start,
                    end_offset_seconds=current_end,
                )
            )
        current = []
        current_words = 0
        current_start = None
        current_end = None

    for row in rows:
        row_text = str(row["text"]).strip()
        row_words = len(_words(row_text))
        start = _float_or_none(row.get("start"))
        duration = _float_or_none(row.get("duration"))
        end = start + duration if start is not None and duration is not None else None
        if current_start is None and start is not None:
            current_start = start
        if end is not None:
            current_end = end
        current.append(row)
        current_words += row_words
        duration_so_far = (current_end - current_start) if current_start is not None and current_end is not None else 0
        if current_words >= TIMED_TARGET_WORDS or duration_so_far >= TIMED_TARGET_SECONDS:
            flush()
    flush()
    return segments


def _window_segments(text: str) -> list[SegmentDraft]:
    tokens = list(re.finditer(r"\S+", text))
    if not tokens:
        return []
    segments: list[SegmentDraft] = []
    step = max(1, WINDOW_WORDS - OVERLAP_WORDS)
    for start_word in range(0, len(tokens), step):
        end_word = min(len(tokens), start_word + WINDOW_WORDS)
        start_char = tokens[start_word].start()
        end_char = tokens[end_word - 1].end()
        start_char, end_char = _expand_to_sentence_boundaries(text, start_char, end_char)
        segment_text = text[start_char:end_char].strip()
        if segment_text:
            segments.append(
                SegmentDraft(
                    text=segment_text,
                    segment_type="text_window",
                    start_offset=start_char,
                    end_offset=end_char,
                    start_offset_seconds=None,
                    end_offset_seconds=None,
                )
            )
        if end_word >= len(tokens):
            break
    return segments


def _expand_to_sentence_boundaries(text: str, start: int, end: int) -> tuple[int, int]:
    if start > 0:
        left = max(text.rfind(".", 0, start), text.rfind("!", 0, start), text.rfind("?", 0, start), text.rfind("\n", 0, start))
        if left >= 0 and start - left < 160:
            start = left + 1
    if end < len(text):
        candidates = [idx for idx in (text.find(".", end), text.find("!", end), text.find("?", end), text.find("\n", end)) if idx >= 0]
        if candidates:
            right = min(candidates)
            if right - end < 240:
                end = right + 1
    return (max(0, start), min(len(text), end))


def _words(text: str) -> list[str]:
    return re.findall(r"[^\W_]+", text or "", flags=re.UNICODE)


def _float_or_none(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
