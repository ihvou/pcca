from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any


@dataclass
class YouTubeTranscriptService:
    preferred_languages: tuple[str, ...] = ("en",)

    async def get_transcript_text(self, video_id: str) -> str | None:
        try:
            from youtube_transcript_api import YouTubeTranscriptApi
            from youtube_transcript_api._errors import NoTranscriptFound, TranscriptsDisabled
        except Exception:
            return None

        def _load_sync() -> str | None:
            try:
                # Old API style.
                if hasattr(YouTubeTranscriptApi, "get_transcript"):
                    rows: list[dict[str, Any]] = YouTubeTranscriptApi.get_transcript(
                        video_id, languages=list(self.preferred_languages)
                    )
                else:
                    # Newer API object style.
                    api = YouTubeTranscriptApi()
                    fetched = api.fetch(video_id, languages=list(self.preferred_languages))
                    if hasattr(fetched, "to_raw_data"):
                        rows = fetched.to_raw_data()
                    else:
                        rows = list(fetched)
                text_parts = [str(row.get("text", "")).strip() for row in rows if str(row.get("text", "")).strip()]
                return "\n".join(text_parts) if text_parts else None
            except (NoTranscriptFound, TranscriptsDisabled):
                return None
            except Exception:
                return None

        return await asyncio.to_thread(_load_sync)

