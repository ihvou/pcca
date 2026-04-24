from __future__ import annotations

from dataclasses import dataclass


@dataclass
class VoiceTranscriptionService:
    """
    Placeholder for local-first voice transcription.

    v1 behavior:
    - returns None for now, signaling that transcription is not yet wired.
    - integration hooks are present in Telegram service so we can add a local
      speech-to-text backend without changing bot flow.
    """

    async def transcribe_telegram_voice(self, _file_bytes: bytes) -> str | None:
        return None
