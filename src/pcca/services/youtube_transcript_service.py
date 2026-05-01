from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class TranscriptResult:
    text: str
    rows: list[dict[str, Any]]
    language_code: str | None = None
    translated: bool = False


@dataclass
class YouTubeTranscriptService:
    """Fetch a video transcript and return its text in English.

    Strategy mirrors yt-dlp's fallback chain:
      1. Prefer a manually-uploaded transcript in `preferred_languages` order
         (default `("en",)`); fetch it directly.
      2. Otherwise prefer any manually-uploaded transcript in any language
         and translate it to English. (Manual transcripts are higher quality
         than auto-generated.)
      3. Otherwise pick any auto-generated transcript and translate it.
      4. If translation isn't available for the picked transcript, fetch its
         original-language text rather than returning nothing — at least we
         get rich body text for the practicality / novelty / trust signals;
         multilingual embedding handles the rest.

    Returns the joined transcript text. Returns None on failures we treat as
    permanent for the video (no transcripts available, transcripts disabled,
    rate-limited, etc.). Callers should not retry on a None.
    """

    preferred_languages: tuple[str, ...] = ("en",)
    target_translation_language: str = "en"

    async def get_transcript_text(self, video_id: str) -> str | None:
        result = await self.get_transcript(video_id)
        return result.text if result is not None else None

    async def get_transcript(self, video_id: str) -> TranscriptResult | None:
        try:
            from youtube_transcript_api import YouTubeTranscriptApi  # type: ignore[import-not-found]
            from youtube_transcript_api._errors import (  # type: ignore[import-not-found]
                NoTranscriptFound,
                TranscriptsDisabled,
            )
        except Exception:
            return None

        def _load_sync() -> TranscriptResult | None:
            try:
                api = YouTubeTranscriptApi()
                # `list` returns an iterable of Transcript objects describing
                # which languages are available, whether they are
                # auto-generated, and whether they are translatable.
                if hasattr(api, "list"):
                    transcripts = api.list(video_id)
                else:
                    # Older API versions exposed `list_transcripts` as a
                    # static method; preserve compatibility for environments
                    # where the package hasn't been refreshed.
                    transcripts = YouTubeTranscriptApi.list_transcripts(video_id)  # type: ignore[attr-defined]

                picked, picked_lang, was_translated = self._pick_transcript(transcripts)
                if picked is None:
                    return None
                rows = self._fetch_rows(picked)
                text = self._rows_to_text(rows)
                if text:
                    logger.debug(
                        "YouTube transcript fetched video_id=%s picked_lang=%s translated=%s chars=%d",
                        video_id,
                        picked_lang,
                        was_translated,
                        len(text),
                    )
                    return TranscriptResult(
                        text=text,
                        rows=self._normalize_rows(rows),
                        language_code=picked_lang,
                        translated=was_translated,
                    )
                return None
            except (NoTranscriptFound, TranscriptsDisabled):
                return None
            except Exception as exc:
                # Library raises a single base RequestBlocked / IpBlocked /
                # CouldNotRetrieveTranscript class for transient YouTube
                # blocks; we treat them as "no transcript right now" so the
                # collector keeps making progress and a later run can fill
                # the gap. Log at INFO so the failure is greppable but not
                # noisy.
                logger.info(
                    "YouTube transcript failed video_id=%s error=%s",
                    video_id,
                    type(exc).__name__,
                )
                return None

        return await asyncio.to_thread(_load_sync)

    # -- internals ----------------------------------------------------------

    def _pick_transcript(self, transcripts: Any) -> tuple[Any, str | None, bool]:
        """Pick the best transcript available, with translation if needed.

        Returns a tuple `(transcript_obj_or_translated, language_code, was_translated)`
        where `transcript_obj_or_translated` is whatever `_fetch_rows` knows
        how to call. On miss returns `(None, None, False)`.
        """
        as_list = list(transcripts) if not isinstance(transcripts, list) else transcripts
        if not as_list:
            return (None, None, False)

        # 1. Prefer a manual transcript in one of our preferred languages.
        manual_preferred = self._first(
            as_list,
            lambda t: not getattr(t, "is_generated", True)
            and getattr(t, "language_code", None) in self.preferred_languages,
        )
        if manual_preferred is not None:
            return (manual_preferred, manual_preferred.language_code, False)

        # 2. Auto-generated in preferred languages — common case for English
        #    videos that don't have manual captions.
        auto_preferred = self._first(
            as_list,
            lambda t: getattr(t, "is_generated", False)
            and getattr(t, "language_code", None) in self.preferred_languages,
        )
        if auto_preferred is not None:
            return (auto_preferred, auto_preferred.language_code, False)

        # 3. Manual transcript in ANY language, translatable to English.
        manual_translatable = self._first(
            as_list,
            lambda t: not getattr(t, "is_generated", True)
            and getattr(t, "is_translatable", False),
        )
        if manual_translatable is not None:
            translated = self._safe_translate(manual_translatable)
            if translated is not None:
                return (translated, manual_translatable.language_code, True)
            return (manual_translatable, manual_translatable.language_code, False)

        # 4. Auto-generated in ANY language, translatable.
        auto_translatable = self._first(
            as_list,
            lambda t: getattr(t, "is_translatable", False),
        )
        if auto_translatable is not None:
            translated = self._safe_translate(auto_translatable)
            if translated is not None:
                return (translated, auto_translatable.language_code, True)
            return (auto_translatable, auto_translatable.language_code, False)

        # 5. Last resort — anything at all, even non-translatable.
        any_transcript = as_list[0]
        return (any_transcript, getattr(any_transcript, "language_code", None), False)

    def _safe_translate(self, transcript: Any) -> Any | None:
        try:
            return transcript.translate(self.target_translation_language)
        except Exception:
            return None

    @staticmethod
    def _first(items: list[Any], predicate: Any) -> Any | None:
        for item in items:
            try:
                if predicate(item):
                    return item
            except Exception:
                continue
        return None

    @staticmethod
    def _fetch_rows(transcript: Any) -> list[dict[str, Any]]:
        # Newer API: Transcript.fetch() → FetchedTranscript with to_raw_data().
        # Older API: list[dict] direct.
        fetched = transcript.fetch() if hasattr(transcript, "fetch") else transcript
        if hasattr(fetched, "to_raw_data"):
            return list(fetched.to_raw_data())
        if isinstance(fetched, list):
            return fetched
        try:
            return list(fetched)
        except Exception:
            return []

    @staticmethod
    def _rows_to_text(rows: list[dict[str, Any]]) -> str | None:
        text_parts = [str(row.get("text", "")).strip() for row in rows if str(row.get("text", "")).strip()]
        return "\n".join(text_parts) if text_parts else None

    @staticmethod
    def _normalize_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        for row in rows:
            text = str(row.get("text", "")).strip()
            if not text:
                continue
            normalized.append(
                {
                    "text": text,
                    "start": row.get("start"),
                    "duration": row.get("duration"),
                }
            )
        return normalized
