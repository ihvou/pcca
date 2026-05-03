from __future__ import annotations

import builtins
import sys
import types
from pathlib import Path

import httpx
import pytest

from pcca.services.yt_dlp_service import (
    YtDlpService,
    YtDlpUnavailableError,
    classify_yt_dlp_error,
    parse_caption_payload,
    select_caption,
)


FIXTURES = Path(__file__).parent / "fixtures" / "yt_dlp"


def test_json3_caption_fixture_parses_timing_unicode_and_markup() -> None:
    rows = parse_caption_payload((FIXTURES / "captions_json3.json").read_text(encoding="utf-8"))

    assert rows == [
        {"text": "Привіт світе", "start": 1.0, "duration": 2.5},
        {"text": "Claude & Code ships handoffs", "start": 4.2, "duration": 1.8},
    ]


def test_vtt_caption_fixture_parses_multiline_cues() -> None:
    rows = parse_caption_payload((FIXTURES / "captions.vtt").read_text(encoding="utf-8"))

    assert rows == [
        {"text": "First cue with & entity.", "start": 1.0, "duration": 3.5},
        {"text": "Second cue spans multiple lines.", "start": 62.25, "duration": 2.75},
    ]


def test_select_caption_priority_rungs() -> None:
    manual_en = {"ext": "json3", "url": "https://caption.test/manual-en"}
    auto_en = {"ext": "json3", "url": "https://caption.test/auto-en"}
    manual_uk = {"ext": "json3", "url": "https://caption.test/manual-uk"}
    auto_uk = {"ext": "json3", "url": "https://caption.test/auto-uk"}

    assert select_caption(
        {"subtitles": {"en": [manual_en]}, "automatic_captions": {"en": [auto_en]}},
        prefer_languages=("en",),
        translate_to="en",
    ) == ("https://caption.test/manual-en", "en", False)
    assert select_caption(
        {"subtitles": {}, "automatic_captions": {"en": [auto_en]}},
        prefer_languages=("en",),
        translate_to="en",
    ) == ("https://caption.test/auto-en", "en", False)
    assert select_caption(
        {"subtitles": {"en": [manual_en], "uk": [manual_uk]}, "automatic_captions": {"en": [auto_en]}},
        prefer_languages=("de",),
        translate_to="en",
    ) == ("https://caption.test/manual-en", "en", True)
    assert select_caption(
        {"subtitles": {"uk": [manual_uk]}, "automatic_captions": {"en": [auto_en], "uk": [auto_uk]}},
        prefer_languages=("de",),
        translate_to="en",
    ) == ("https://caption.test/auto-en", "en", True)
    assert select_caption(
        {"subtitles": {"uk": [manual_uk]}, "automatic_captions": {}},
        prefer_languages=("de",),
        translate_to="en",
    ) == ("https://caption.test/manual-uk", "uk", False)


def test_extract_info_sets_cookiefile_when_provided(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    seen: dict[str, object] = {}
    cookiefile = tmp_path / "cookies.txt"
    cookiefile.write_text("# cookies\n", encoding="utf-8")

    class FakeYoutubeDL:
        def __init__(self, options):
            seen["options"] = options

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def extract_info(self, url, *, download):
            seen["url"] = url
            seen["download"] = download
            return {"id": "video1234567", "title": "Demo", "url": "video1234567"}

    monkeypatch.setitem(sys.modules, "yt_dlp", types.SimpleNamespace(YoutubeDL=FakeYoutubeDL))

    info = YtDlpService()._extract_info(
        "https://www.youtube.com/watch?v=video1234567",
        cookiefile=cookiefile,
        playlistend=None,
        extract_flat=False,
    )

    assert info["id"] == "video1234567"
    assert seen["options"]["cookiefile"] == str(cookiefile)
    assert seen["options"]["extract_flat"] is False
    assert seen["download"] is False


def test_extract_info_omits_cookiefile_when_none(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, object] = {}

    class FakeYoutubeDL:
        def __init__(self, options):
            seen["options"] = options

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def extract_info(self, url, *, download):
            return {"id": "video1234567", "title": "Demo", "url": "video1234567"}

    monkeypatch.setitem(sys.modules, "yt_dlp", types.SimpleNamespace(YoutubeDL=FakeYoutubeDL))

    YtDlpService()._extract_info(
        "https://www.youtube.com/watch?v=video1234567",
        cookiefile=None,
        playlistend=3,
        extract_flat="in_playlist",
    )

    assert "cookiefile" not in seen["options"]
    assert seen["options"]["playlistend"] == 3


def test_extract_info_raises_helpful_unavailable_error(monkeypatch: pytest.MonkeyPatch) -> None:
    original_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "yt_dlp":
            raise ImportError("missing")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    with pytest.raises(YtDlpUnavailableError, match="yt-dlp is not installed"):
        YtDlpService()._extract_info(
            "https://www.youtube.com/watch?v=video1234567",
            cookiefile=None,
            playlistend=None,
            extract_flat=False,
        )


@pytest.mark.asyncio
async def test_list_channel_videos_maps_extract_info_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeYoutubeDL:
        def __init__(self, options):
            self.options = options

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def extract_info(self, url, *, download):
            return {
                "entries": [
                    {
                        "id": "video1234567",
                        "url": "video1234567",
                        "title": "Demo title",
                        "description": "Demo description",
                        "upload_date": "20260503",
                        "channel": "OpenAI",
                        "channel_id": "UC123",
                        "view_count": 123,
                        "like_count": 45,
                        "duration": 678,
                    }
                ]
            }

    monkeypatch.setitem(sys.modules, "yt_dlp", types.SimpleNamespace(YoutubeDL=FakeYoutubeDL))

    videos = await YtDlpService().list_channel_videos("@openai", max_items=1)

    assert len(videos) == 1
    assert videos[0].external_id == "video1234567"
    assert videos[0].url == "https://www.youtube.com/watch?v=video1234567"
    assert videos[0].published_at == "2026-05-03"
    assert videos[0].view_count == 123
    assert videos[0].like_count == 45
    assert videos[0].duration_seconds == 678


@pytest.mark.asyncio
async def test_get_transcript_sets_translated_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeService(YtDlpService):
        def _extract_info(self, *args, **kwargs):
            return {"subtitles": {"en": [{"ext": "vtt", "url": "https://caption.test/en.vtt"}]}, "automatic_captions": {}}

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, url: str):
            assert url == "https://caption.test/en.vtt"
            return httpx.Response(
                200,
                text=(FIXTURES / "captions.vtt").read_text(encoding="utf-8"),
                request=httpx.Request("GET", url),
            )

    monkeypatch.setattr(httpx, "AsyncClient", FakeClient)

    transcript = await FakeService().get_transcript("video1234567", prefer_languages=("uk",), translate_to="en")

    assert transcript is not None
    assert transcript.language_code == "en"
    assert transcript.translated is True
    assert "First cue" in transcript.text


@pytest.mark.parametrize(
    ("class_name", "failure_class"),
    [
        ("ExtractorError", "extractor_error"),
        ("DownloadError", "download_error"),
        ("GeoRestrictedError", "geo_restricted"),
        ("UnavailableVideoError", "unavailable"),
        ("RegexNotFoundError", "regex_not_found"),
        ("UnexpectedBoom", "unknown"),
    ],
)
def test_yt_dlp_error_classification_and_failure_payload(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    class_name: str,
    failure_class: str,
) -> None:
    error_type = type(class_name, (Exception,), {})

    class FakeYoutubeDL:
        def __init__(self, options):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def extract_info(self, url, *, download):
            raise error_type("boom")

    monkeypatch.setitem(sys.modules, "yt_dlp", types.SimpleNamespace(YoutubeDL=FakeYoutubeDL))
    service = YtDlpService()

    with caplog.at_level("WARNING"):
        info = service._extract_info(
            "https://www.youtube.com/watch?v=video1234567",
            cookiefile=None,
            playlistend=None,
            extract_flat=False,
        )

    assert classify_yt_dlp_error(error_type("boom")) == failure_class
    assert info["_failure_class"] == failure_class
    assert service.drain_failure_counts() == {failure_class: 1}
    assert f"failure_class={failure_class}" in caplog.text
