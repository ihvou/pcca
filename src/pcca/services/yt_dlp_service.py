from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx

from pcca.collectors.youtube_utils import build_channel_videos_url, extract_video_id
from pcca.services.youtube_transcript_service import TranscriptResult

logger = logging.getLogger(__name__)


class YtDlpUnavailableError(RuntimeError):
    pass


@dataclass
class YtDlpVideo:
    external_id: str
    url: str
    title: str
    description: str | None = None
    published_at: str | None = None
    channel_name: str | None = None
    channel_id: str | None = None
    view_count: int | None = None
    like_count: int | None = None
    duration_seconds: int | None = None


@dataclass
class YtDlpService:
    timeout_seconds: float = 60.0
    failure_counts: dict[str, int] = field(default_factory=dict)

    async def list_channel_videos(
        self,
        source_id: str,
        *,
        max_items: int = 8,
        cookiefile: Path | str | None = None,
    ) -> list[YtDlpVideo]:
        url = build_channel_videos_url(source_id)
        info = await asyncio.to_thread(
            self._extract_info,
            url,
            cookiefile=Path(cookiefile) if cookiefile else None,
            playlistend=max_items,
            extract_flat="in_playlist",
        )
        failure_class = _failure_class(info)
        if failure_class:
            logger.warning(
                "yt-dlp channel listing failed source=%s failure_class=%s message=%s",
                source_id,
                failure_class,
                info.get("_failure_message"),
            )
            return []
        entries = info.get("entries") if isinstance(info, dict) else None
        if not isinstance(entries, list):
            entries = [info] if isinstance(info, dict) else []
        out: list[YtDlpVideo] = []
        for entry in entries[:max_items]:
            if not isinstance(entry, dict):
                continue
            video = self._video_from_info(entry)
            if video is not None:
                out.append(video)
        logger.info("yt-dlp listed YouTube videos source=%s count=%d cookiefile=%s", source_id, len(out), bool(cookiefile))
        return out

    async def get_transcript(
        self,
        video_id: str,
        *,
        prefer_languages: list[str] | tuple[str, ...] = ("en",),
        translate_to: str = "en",
        cookiefile: Path | str | None = None,
    ) -> TranscriptResult | None:
        video_url = f"https://www.youtube.com/watch?v={video_id}"
        info = await asyncio.to_thread(
            self._extract_info,
            video_url,
            cookiefile=Path(cookiefile) if cookiefile else None,
            playlistend=None,
            extract_flat=False,
        )
        if not isinstance(info, dict):
            return None
        failure_class = _failure_class(info)
        if failure_class:
            logger.warning(
                "yt-dlp transcript info failed video_id=%s failure_class=%s message=%s",
                video_id,
                failure_class,
                info.get("_failure_message"),
            )
            return None
        selected = select_caption(info, prefer_languages=prefer_languages, translate_to=translate_to)
        if selected is None:
            logger.info("yt-dlp transcript unavailable video_id=%s", video_id)
            return None
        caption_url, language_code, translated = selected
        async with httpx.AsyncClient(timeout=self.timeout_seconds, follow_redirects=True) as client:
            response = await client.get(caption_url)
            response.raise_for_status()
        rows = parse_caption_payload(response.text)
        text = "\n".join(row["text"] for row in rows if row.get("text")).strip()
        if not text:
            return None
        logger.info(
            "yt-dlp transcript fetched video_id=%s language=%s translated=%s rows=%d chars=%d",
            video_id,
            language_code,
            translated,
            len(rows),
            len(text),
        )
        return TranscriptResult(text=text, rows=rows, language_code=language_code, translated=translated)

    def _extract_info(
        self,
        url: str,
        *,
        cookiefile: Path | None,
        playlistend: int | None,
        extract_flat: str | bool,
    ) -> dict:
        try:
            from yt_dlp import YoutubeDL  # type: ignore[import-not-found]
        except Exception as exc:  # pragma: no cover - environment dependent
            raise YtDlpUnavailableError("yt-dlp is not installed. Install project dependencies and retry.") from exc

        options: dict[str, Any] = {
            "quiet": True,
            "no_warnings": True,
            "skip_download": True,
            "extract_flat": extract_flat,
            "socket_timeout": self.timeout_seconds,
        }
        if playlistend is not None:
            options["playlistend"] = max(1, int(playlistend))
        if cookiefile is not None and Path(cookiefile).exists():
            options["cookiefile"] = str(cookiefile)
        try:
            with YoutubeDL(options) as ydl:
                info = ydl.extract_info(url, download=False)
        except Exception as exc:
            failure_class = classify_yt_dlp_error(exc)
            self.failure_counts[failure_class] = self.failure_counts.get(failure_class, 0) + 1
            logger.warning(
                "yt-dlp extract_info failed url=%s failure_class=%s error=%s",
                url,
                failure_class,
                exc,
                exc_info=failure_class == "unknown",
            )
            return {
                "_failure_class": failure_class,
                "_failure_message": str(exc),
                "_url": url,
            }
        return info if isinstance(info, dict) else {}

    def drain_failure_counts(self) -> dict[str, int]:
        out = dict(self.failure_counts)
        self.failure_counts.clear()
        return out

    @staticmethod
    def _video_from_info(info: dict[str, Any]) -> YtDlpVideo | None:
        video_id = str(info.get("id") or "").strip()
        webpage_url = str(info.get("webpage_url") or info.get("url") or "").strip()
        if not video_id and webpage_url:
            video_id = extract_video_id(webpage_url) or ""
        if (not webpage_url or not webpage_url.startswith(("http://", "https://"))) and video_id:
            webpage_url = f"https://www.youtube.com/watch?v={video_id}"
        title = str(info.get("title") or "").strip()
        if not video_id or not title:
            return None
        return YtDlpVideo(
            external_id=video_id,
            url=webpage_url,
            title=title,
            description=str(info.get("description") or "").strip() or None,
            published_at=_format_yt_date(info.get("upload_date") or info.get("release_date") or info.get("timestamp")),
            channel_name=str(info.get("channel") or info.get("uploader") or "").strip() or None,
            channel_id=str(info.get("channel_id") or info.get("uploader_id") or "").strip() or None,
            view_count=_int_or_none(info.get("view_count")),
            like_count=_int_or_none(info.get("like_count")),
            duration_seconds=_int_or_none(info.get("duration")),
        )


def select_caption(
    info: dict[str, Any],
    *,
    prefer_languages: list[str] | tuple[str, ...] = ("en",),
    translate_to: str = "en",
) -> tuple[str, str | None, bool] | None:
    subtitles = info.get("subtitles") if isinstance(info.get("subtitles"), dict) else {}
    automatic = info.get("automatic_captions") if isinstance(info.get("automatic_captions"), dict) else {}
    preferred = [lang for lang in prefer_languages if lang]
    lookup_order: list[tuple[dict, str, bool]] = []
    for lang in preferred:
        lookup_order.append((subtitles, lang, False))
        lookup_order.append((automatic, lang, False))
    if translate_to and translate_to not in preferred:
        lookup_order.append((subtitles, translate_to, True))
        lookup_order.append((automatic, translate_to, True))
    for caption_map, lang, translated in lookup_order:
        url = _caption_url(caption_map.get(lang))
        if url:
            return (url, lang, translated)
    for caption_map, translated in ((subtitles, False), (automatic, False)):
        for lang, entries in caption_map.items():
            url = _caption_url(entries)
            if url:
                return (url, str(lang), translated)
    return None


def classify_yt_dlp_error(exc: Exception) -> str:
    name = type(exc).__name__.lower()
    if "georestricted" in name or ("geo" in name and "restricted" in name):
        return "geo_restricted"
    if "unavailablevideo" in name or "unavailable" in name:
        return "unavailable"
    if "regexnotfound" in name or "regex" in name:
        return "regex_not_found"
    if "extractor" in name:
        return "extractor_error"
    if "download" in name:
        return "download_error"
    return "unknown"


def _failure_class(info: Any) -> str | None:
    if isinstance(info, dict) and isinstance(info.get("_failure_class"), str):
        return str(info["_failure_class"])
    return None


def parse_caption_payload(raw: str) -> list[dict[str, Any]]:
    text = raw.strip()
    if not text:
        return []
    if text.startswith("{"):
        return _parse_json3_caption(text)
    return _parse_vtt_caption(text)


def _caption_url(entries: Any) -> str | None:
    if isinstance(entries, dict):
        entries = [entries]
    if not isinstance(entries, list):
        return None
    preferred_exts = ("json3", "vtt", "srv3", "ttml")
    for ext in preferred_exts:
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            if str(entry.get("ext") or "").lower() == ext and entry.get("url"):
                return str(entry["url"])
    for entry in entries:
        if isinstance(entry, dict) and entry.get("url"):
            return str(entry["url"])
    return None


def _parse_json3_caption(text: str) -> list[dict[str, Any]]:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return []
    events = payload.get("events") if isinstance(payload, dict) else None
    if not isinstance(events, list):
        return []
    rows: list[dict[str, Any]] = []
    for event in events:
        if not isinstance(event, dict):
            continue
        segs = event.get("segs")
        if not isinstance(segs, list):
            continue
        row_text = "".join(str(seg.get("utf8", "")) for seg in segs if isinstance(seg, dict)).strip()
        if not row_text:
            continue
        start = _millis_to_seconds(event.get("tStartMs"))
        duration = _millis_to_seconds(event.get("dDurationMs"))
        rows.append({"text": _clean_caption_text(row_text), "start": start, "duration": duration})
    return rows


def _parse_vtt_caption(text: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    current_start: float | None = None
    current_duration: float | None = None
    current_text: list[str] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line == "WEBVTT" or line.startswith(("Kind:", "Language:", "NOTE")):
            if current_text:
                rows.append(
                    {
                        "text": _clean_caption_text(" ".join(current_text)),
                        "start": current_start,
                        "duration": current_duration,
                    }
                )
                current_text = []
                current_start = None
                current_duration = None
            continue
        if "-->" in line:
            if current_text:
                rows.append(
                    {
                        "text": _clean_caption_text(" ".join(current_text)),
                        "start": current_start,
                        "duration": current_duration,
                    }
                )
                current_text = []
            start_raw, end_raw = [part.strip().split()[0] for part in line.split("-->", 1)]
            current_start = _parse_vtt_time(start_raw)
            end = _parse_vtt_time(end_raw)
            current_duration = (end - current_start) if current_start is not None and end is not None else None
            continue
        if re.fullmatch(r"\d+", line):
            continue
        current_text.append(line)
    if current_text:
        rows.append({"text": _clean_caption_text(" ".join(current_text)), "start": current_start, "duration": current_duration})
    return [row for row in rows if row["text"]]


def _clean_caption_text(text: str) -> str:
    cleaned = re.sub(r"<[^>]+>", "", text)
    cleaned = cleaned.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    return " ".join(cleaned.split()).strip()


def _parse_vtt_time(value: str) -> float | None:
    parts = value.replace(",", ".").split(":")
    try:
        if len(parts) == 3:
            hours, minutes, seconds = parts
            return int(hours) * 3600 + int(minutes) * 60 + float(seconds)
        if len(parts) == 2:
            minutes, seconds = parts
            return int(minutes) * 60 + float(seconds)
    except ValueError:
        return None
    return None


def _millis_to_seconds(value: Any) -> float | None:
    try:
        return round(float(value) / 1000.0, 3)
    except (TypeError, ValueError):
        return None


def _format_yt_date(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, int):
        try:
            return datetime.utcfromtimestamp(value).date().isoformat()
        except (OverflowError, OSError, ValueError):
            return None
    raw = str(value)
    if re.fullmatch(r"\d{8}", raw):
        return f"{raw[0:4]}-{raw[4:6]}-{raw[6:8]}"
    return raw or None


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
