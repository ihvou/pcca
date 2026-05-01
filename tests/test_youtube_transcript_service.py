from __future__ import annotations

import sys
import types
from typing import Any

import pytest

from pcca.services.youtube_transcript_service import YouTubeTranscriptService


class FakeFetched:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def to_raw_data(self) -> list[dict[str, Any]]:
        return list(self._rows)


class FakeTranscript:
    def __init__(
        self,
        language_code: str,
        *,
        is_generated: bool,
        is_translatable: bool,
        text_rows: list[dict[str, Any]] | None = None,
        translates_to: dict[str, "FakeTranscript"] | None = None,
    ) -> None:
        self.language_code = language_code
        self.language = language_code
        self.is_generated = is_generated
        self.is_translatable = is_translatable
        self._text_rows = text_rows or [{"text": f"original-{language_code}"}]
        self._translates_to = translates_to or {}

    def fetch(self) -> FakeFetched:
        return FakeFetched(self._text_rows)

    def translate(self, target: str) -> "FakeTranscript":
        if not self.is_translatable:
            raise RuntimeError("not translatable")
        if target in self._translates_to:
            return self._translates_to[target]
        # Default: produce a translated transcript object whose fetched text
        # is the original prefixed with the target language code.
        return FakeTranscript(
            target,
            is_generated=False,
            is_translatable=False,
            text_rows=[{"text": f"{target}-translated-from-{self.language_code}"}],
        )


class FakeApi:
    def __init__(self, transcripts: list[FakeTranscript]) -> None:
        self._transcripts = transcripts

    def list(self, video_id: str) -> list[FakeTranscript]:  # noqa: ARG002
        return list(self._transcripts)


def _install_fake_module(monkeypatch: pytest.MonkeyPatch, api: FakeApi) -> None:
    """Install a stub `youtube_transcript_api` module so the service imports it."""
    mod = types.ModuleType("youtube_transcript_api")
    errors_mod = types.ModuleType("youtube_transcript_api._errors")

    class NoTranscriptFound(Exception):
        pass

    class TranscriptsDisabled(Exception):
        pass

    errors_mod.NoTranscriptFound = NoTranscriptFound
    errors_mod.TranscriptsDisabled = TranscriptsDisabled

    class YouTubeTranscriptApi:
        def __init__(self) -> None:
            pass

        def list(self, video_id: str) -> list[FakeTranscript]:
            return api.list(video_id)

    mod.YouTubeTranscriptApi = YouTubeTranscriptApi
    mod._errors = errors_mod  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, "youtube_transcript_api", mod)
    monkeypatch.setitem(sys.modules, "youtube_transcript_api._errors", errors_mod)


@pytest.mark.asyncio
async def test_prefers_manual_english_transcript_when_available(monkeypatch: pytest.MonkeyPatch) -> None:
    api = FakeApi([
        FakeTranscript(
            "en",
            is_generated=False,
            is_translatable=True,
            text_rows=[{"text": "english manual", "start": 12.5, "duration": 3.0}],
        ),
        FakeTranscript("uk", is_generated=False, is_translatable=True, text_rows=[{"text": "ukrainian"}]),
    ])
    _install_fake_module(monkeypatch, api)
    svc = YouTubeTranscriptService()
    transcript = await svc.get_transcript("video-1")
    assert transcript is not None
    assert transcript.text == "english manual"
    assert transcript.rows == [{"text": "english manual", "start": 12.5, "duration": 3.0}]
    assert await svc.get_transcript_text("video-1") == "english manual"


@pytest.mark.asyncio
async def test_falls_back_to_auto_generated_english_when_no_manual(monkeypatch: pytest.MonkeyPatch) -> None:
    api = FakeApi([
        FakeTranscript("en", is_generated=True, is_translatable=True, text_rows=[{"text": "auto-en"}]),
        FakeTranscript("uk", is_generated=False, is_translatable=True, text_rows=[{"text": "uk manual"}]),
    ])
    _install_fake_module(monkeypatch, api)
    svc = YouTubeTranscriptService()
    text = await svc.get_transcript_text("video-2")
    assert text == "auto-en"


@pytest.mark.asyncio
async def test_translates_manual_ukrainian_to_english(monkeypatch: pytest.MonkeyPatch) -> None:
    api = FakeApi([
        FakeTranscript(
            "uk",
            is_generated=False,
            is_translatable=True,
            text_rows=[{"text": "оригінальний український"}],
        ),
    ])
    _install_fake_module(monkeypatch, api)
    svc = YouTubeTranscriptService()
    text = await svc.get_transcript_text("video-3")
    assert text == "en-translated-from-uk"


@pytest.mark.asyncio
async def test_falls_back_to_original_when_translate_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    api = FakeApi([
        FakeTranscript("ru", is_generated=False, is_translatable=False, text_rows=[{"text": "russian-only"}]),
    ])
    _install_fake_module(monkeypatch, api)
    svc = YouTubeTranscriptService()
    text = await svc.get_transcript_text("video-4")
    # Even if translation isn't supported, we still surface the original-language
    # body text so the practicality / novelty signals have something to work with.
    assert text == "russian-only"


@pytest.mark.asyncio
async def test_translates_auto_generated_ukrainian_when_no_manual(monkeypatch: pytest.MonkeyPatch) -> None:
    api = FakeApi([
        FakeTranscript(
            "uk",
            is_generated=True,
            is_translatable=True,
            text_rows=[{"text": "auto uk"}],
        ),
    ])
    _install_fake_module(monkeypatch, api)
    svc = YouTubeTranscriptService()
    text = await svc.get_transcript_text("video-5")
    assert text == "en-translated-from-uk"


@pytest.mark.asyncio
async def test_returns_none_when_no_transcripts_available(monkeypatch: pytest.MonkeyPatch) -> None:
    api = FakeApi([])
    _install_fake_module(monkeypatch, api)
    svc = YouTubeTranscriptService()
    text = await svc.get_transcript_text("video-empty")
    assert text is None


@pytest.mark.asyncio
async def test_returns_none_when_transcripts_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    """TranscriptsDisabled / NoTranscriptFound exceptions are treated as 'no transcript'."""
    mod = types.ModuleType("youtube_transcript_api")
    errors_mod = types.ModuleType("youtube_transcript_api._errors")

    class NoTranscriptFound(Exception):
        pass

    class TranscriptsDisabled(Exception):
        pass

    errors_mod.NoTranscriptFound = NoTranscriptFound
    errors_mod.TranscriptsDisabled = TranscriptsDisabled

    class YouTubeTranscriptApi:
        def __init__(self) -> None:
            pass

        def list(self, video_id: str) -> list[FakeTranscript]:  # noqa: ARG002
            raise TranscriptsDisabled("disabled")

    mod.YouTubeTranscriptApi = YouTubeTranscriptApi
    mod._errors = errors_mod  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, "youtube_transcript_api", mod)
    monkeypatch.setitem(sys.modules, "youtube_transcript_api._errors", errors_mod)
    svc = YouTubeTranscriptService()
    assert await svc.get_transcript_text("video-disabled") is None
