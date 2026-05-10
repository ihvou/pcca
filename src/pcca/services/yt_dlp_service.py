from __future__ import annotations

import asyncio
import json
import logging
import re
import tempfile
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

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


@dataclass(frozen=True)
class CaptionSelection:
    url: str
    language_code: str | None
    translated: bool
    source: str


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
        selected = _select_caption_track(info, prefer_languages=prefer_languages, translate_to=translate_to)
        if selected is None:
            logger.info("yt-dlp transcript unavailable video_id=%s", video_id)
            return None
        raw_caption = await asyncio.to_thread(
            self._download_caption_payload,
            video_url,
            selection=selected,
            cookiefile=Path(cookiefile) if cookiefile else None,
        )
        if not raw_caption:
            logger.info("yt-dlp transcript download produced no subtitle file video_id=%s", video_id)
            return None
        rows = parse_caption_payload(raw_caption)
        text = "\n".join(row["text"] for row in rows if row.get("text")).strip()
        if not _caption_rows_are_usable(rows, text):
            logger.info(
                "yt-dlp transcript rejected by sanity checks video_id=%s language=%s rows=%d chars=%d",
                video_id,
                selected.language_code,
                len(rows),
                len(text),
            )
            return None
        logger.info(
            "yt-dlp transcript fetched video_id=%s language=%s translated=%s rows=%d chars=%d",
            video_id,
            selected.language_code,
            selected.translated,
            len(rows),
            len(text),
        )
        return TranscriptResult(text=text, rows=rows, language_code=selected.language_code, translated=selected.translated)

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

    def _download_caption_payload(
        self,
        video_url: str,
        *,
        selection: CaptionSelection,
        cookiefile: Path | None,
    ) -> str | None:
        try:
            from yt_dlp import YoutubeDL  # type: ignore[import-not-found]
        except Exception as exc:  # pragma: no cover - environment dependent
            raise YtDlpUnavailableError("yt-dlp is not installed. Install project dependencies and retry.") from exc

        with tempfile.TemporaryDirectory(prefix="pcca-yt-captions-") as tmpdir:
            tmp_path = Path(tmpdir)
            options: dict[str, Any] = {
                "quiet": True,
                "no_warnings": True,
                "skip_download": True,
                "writesubtitles": selection.source == "subtitles",
                "writeautomaticsub": selection.source == "automatic_captions",
                "subtitlesformat": "json3/vtt/best",
                "outtmpl": str(tmp_path / "%(id)s.%(ext)s"),
                "socket_timeout": self.timeout_seconds,
            }
            if selection.language_code:
                options["subtitleslangs"] = [selection.language_code]
            if cookiefile is not None and Path(cookiefile).exists():
                options["cookiefile"] = str(cookiefile)
            try:
                with YoutubeDL(options) as ydl:
                    ydl.download([video_url])
            except Exception as exc:
                failure_class = classify_yt_dlp_error(exc)
                self.failure_counts[failure_class] = self.failure_counts.get(failure_class, 0) + 1
                logger.warning(
                    "yt-dlp subtitle download failed url=%s language=%s source=%s failure_class=%s error=%s",
                    video_url,
                    selection.language_code,
                    selection.source,
                    failure_class,
                    exc,
                    exc_info=failure_class == "unknown",
                )
                return None
            return _read_best_caption_file(tmp_path)

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
    selection = _select_caption_track(info, prefer_languages=prefer_languages, translate_to=translate_to)
    if selection is None:
        return None
    return (selection.url, selection.language_code, selection.translated)


def _select_caption_track(
    info: dict[str, Any],
    *,
    prefer_languages: list[str] | tuple[str, ...] = ("en",),
    translate_to: str = "en",
) -> CaptionSelection | None:
    subtitles = info.get("subtitles") if isinstance(info.get("subtitles"), dict) else {}
    automatic = info.get("automatic_captions") if isinstance(info.get("automatic_captions"), dict) else {}
    preferred = [lang for lang in prefer_languages if lang]

    # T-132 part 2: YouTube's `automatic_captions[<target>]` for a non-English
    # video IS the auto-translated track — the underlying timedtext URL
    # carries a `tlang=<target>` parameter and is heavily rate-limited (HTTP
    # 429). The dict key alone (`"en"`) is indistinguishable from a real
    # native English track. The reliable signal is the URL's `tlang=` param:
    # native tracks do NOT have it, translations always do. Steps 1 and 2
    # below skip any entry whose URL is a translation; step 3 (translation
    # last-resort) accepts them only when explicitly invoked.
    def _is_translation_url(url: str) -> bool:
        return "tlang=" in url

    # Step 1: preferred-language NATIVE tracks. Skip translations even when
    # the dict key matches the preferred language code.
    for lang in preferred:
        for caption_map, source in ((subtitles, "subtitles"), (automatic, "automatic_captions")):
            entry = _caption_entry(caption_map.get(lang), lang=lang)
            url = str(entry.get("url")) if entry and entry.get("url") else None
            if url and not _is_translation_url(url):
                return CaptionSelection(url=url, language_code=lang, translated=False, source=source)

    # Step 2: ANY native track. Picks the source-language transcript verbatim.
    # Critical: this MUST run before the translate_to fallback. YouTube heavily
    # rate-limits the timedtext translation endpoint (`tlang=` URL parameter)
    # and returns HTTP 429 for non-English source content. Native-language
    # downloads are unaffected. Embedding model `nomic-embed-text:v1.5` is
    # multilingual, so a native-Ukrainian track scores correctly without
    # round-tripping through YouTube translation. See yt-dlp issues #13770,
    # #13831, #14023, #12056. Live-verified 2026-05-10 against STERNENKO uk
    # tracks: native download succeeds in <1s; tlang=en variant returns 429.
    for caption_map, source in ((subtitles, "subtitles"), (automatic, "automatic_captions")):
        for lang, entries in caption_map.items():
            entry = _caption_entry(entries, lang=str(lang))
            url = str(entry.get("url")) if entry and entry.get("url") else None
            if url and not _is_translation_url(url):
                return CaptionSelection(url=url, language_code=str(lang), translated=False, source=source)

    # Step 3: translation last resort. Accepts `tlang=` URLs explicitly.
    if translate_to and translate_to not in preferred:
        for caption_map, source in ((subtitles, "subtitles"), (automatic, "automatic_captions")):
            entry = _caption_entry(caption_map.get(translate_to), lang=translate_to)
            url = str(entry.get("url")) if entry and entry.get("url") else None
            if url:
                return CaptionSelection(url=url, language_code=translate_to, translated=True, source=source)
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
    entry = _caption_entry(entries, lang=None)
    return str(entry.get("url")) if entry and entry.get("url") else None


def _caption_entry(entries: Any, *, lang: str | None) -> dict[str, Any] | None:
    if isinstance(entries, dict):
        entries = [entries]
    if not isinstance(entries, list):
        return None
    if _is_denied_caption_track(lang, entries):
        return None
    preferred_exts = ("json3", "vtt", "srv3", "ttml")
    for ext in preferred_exts:
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            if str(entry.get("ext") or "").lower() == ext and entry.get("url"):
                return entry
    for entry in entries:
        if isinstance(entry, dict) and entry.get("url"):
            return entry
    return None


def _is_denied_caption_track(lang: str | None, entries: list[Any]) -> bool:
    lang_text = str(lang or "").strip().lower()
    if any(token in lang_text for token in ("live_chat", "rechat", "live", "fan_funded")):
        return True
    if lang_text in {"live", "auto"}:
        return True
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        name = str(entry.get("name") or entry.get("label") or "").strip().lower()
        if any(token in name for token in ("live_chat", "live chat", "rechat", "fan_funded", "fan funded")):
            return True
    return False


def _caption_rows_are_usable(rows: list[dict[str, Any]], text: str) -> bool:
    if len(rows) < 10 or len(text.strip()) < 200:
        return False
    starts: dict[str, int] = {}
    for row in rows:
        key = str(row.get("start"))
        starts[key] = starts.get(key, 0) + 1
    if starts and max(starts.values()) / max(1, len(rows)) >= 0.8:
        return False
    return True


def _read_best_caption_file(directory: Path) -> str | None:
    preferred_exts = (".json3", ".vtt", ".srv3", ".ttml")
    files = [
        path
        for path in directory.rglob("*")
        if path.is_file() and path.suffix.lower() in preferred_exts
    ]
    if not files:
        return None
    files.sort(key=lambda path: preferred_exts.index(path.suffix.lower()))
    try:
        return files[0].read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return files[0].read_text(encoding="utf-8", errors="ignore")


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
