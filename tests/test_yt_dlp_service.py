from __future__ import annotations

import builtins
import json
import sys
import types
from pathlib import Path

import pytest

from pcca.services.yt_dlp_service import (
    YtDlpService,
    YtDlpUnavailableError,
    classify_yt_dlp_error,
    parse_caption_payload,
    select_caption,
)


FIXTURES = Path(__file__).parent / "fixtures" / "yt_dlp"


def _caption_json3(*, rows: int = 12, repeated_start: bool = False) -> str:
    events = []
    for idx in range(rows):
        events.append(
            {
                "tStartMs": 1000 if repeated_start else idx * 2500,
                "dDurationMs": 1800,
                "segs": [
                    {
                        "utf8": (
                            f"Transcript row {idx} explains concrete Ukrainian frontline context, "
                            "why the update matters, and what practical signal a reader should keep."
                        )
                    }
                ],
            }
        )
    return json.dumps({"events": events})


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

    # Rung 1: preferred manual track wins over preferred auto track.
    assert select_caption(
        {"subtitles": {"en": [manual_en]}, "automatic_captions": {"en": [auto_en]}},
        prefer_languages=("en",),
        translate_to="en",
    ) == ("https://caption.test/manual-en", "en", False)
    # Rung 1 (auto): preferred auto track when no manual exists.
    assert select_caption(
        {"subtitles": {}, "automatic_captions": {"en": [auto_en]}},
        prefer_languages=("en",),
        translate_to="en",
    ) == ("https://caption.test/auto-en", "en", False)
    # Rung 2 (T-132): when preferred isn't found, pick ANY native track from
    # subtitles before falling through to translation. With manual `en`+`uk`
    # subtitles available, the en native track wins (first in dict iteration)
    # and we report translated=False (not the old translated=True). This
    # avoids YouTube's rate-limited tlang= path entirely.
    assert select_caption(
        {"subtitles": {"en": [manual_en], "uk": [manual_uk]}, "automatic_captions": {"en": [auto_en]}},
        prefer_languages=("de",),
        translate_to="en",
    ) == ("https://caption.test/manual-en", "en", False)
    # Rung 2: subtitles takes precedence over automatic_captions when both
    # have native tracks. Picks manual_uk (subtitles dict) instead of auto_en.
    assert select_caption(
        {"subtitles": {"uk": [manual_uk]}, "automatic_captions": {"en": [auto_en], "uk": [auto_uk]}},
        prefer_languages=("de",),
        translate_to="en",
    ) == ("https://caption.test/manual-uk", "uk", False)
    # Rung 2: subtitles-only with native uk picks the native track.
    assert select_caption(
        {"subtitles": {"uk": [manual_uk]}, "automatic_captions": {}},
        prefer_languages=("de",),
        translate_to="en",
    ) == ("https://caption.test/manual-uk", "uk", False)


def test_select_caption_prefers_native_over_translation_for_ukrainian() -> None:
    """Regression for T-132: STERNENKO/Portnikov/Butusov Ukrainian content
    has only `uk` tracks available, but our default prefer_languages=("en",)
    used to trigger a translate_to=en request — which YouTube returns 429
    on. Live evidence 2026-05-10: native uk download succeeded in <1s while
    PCCA's prior code path returned HTTP 429. After T-132, we must never
    request translation when a native track is available.
    """
    manual_uk = {"ext": "json3", "url": "https://caption.test/uk-native"}
    # Ukrainian-only video, defaults match production rebackfill caller.
    selection = select_caption(
        {"subtitles": {"uk": [manual_uk]}, "automatic_captions": {}},
        prefer_languages=("en",),
        translate_to="en",
    )
    assert selection == ("https://caption.test/uk-native", "uk", False)


def test_select_caption_skips_translation_url_when_picking_native() -> None:
    """T-132 part 2: YouTube's `automatic_captions["en"]` for a Ukrainian
    video is an auto-translated track whose timedtext URL carries
    `tlang=en`. The dict key alone is indistinguishable from a real native
    English track. We must filter on URL `tlang=` to avoid HTTP 429.

    Live evidence (2026-05-10): 52 of 60 stuck items hit 429 with
    `language=en source=automatic_captions` because the original T-132
    fix picked the auto-translated track from automatic_captions before
    falling through to the native uk track.
    """
    # Auto-translated en track for a Ukrainian video — URL has tlang=en.
    auto_en_translated = {
        "ext": "json3",
        "url": "https://www.youtube.com/api/timedtext?v=X&lang=uk&tlang=en&fmt=json3",
        "name": "English (auto-translated)",
    }
    # Native Ukrainian auto-caption — URL has lang=uk only, no tlang=.
    auto_uk_native = {
        "ext": "json3",
        "url": "https://www.youtube.com/api/timedtext?v=X&lang=uk&fmt=json3",
        "name": "Ukrainian",
    }
    # Caller asks for English (default). With both tracks present, picking
    # the en-keyed entry would return the translation URL → 429. Native uk
    # MUST win.
    selection = select_caption(
        {
            "subtitles": {},
            "automatic_captions": {
                "en": [auto_en_translated],
                "uk": [auto_uk_native],
            },
        },
        prefer_languages=("en",),
        translate_to="en",
    )
    assert selection == (
        "https://www.youtube.com/api/timedtext?v=X&lang=uk&fmt=json3",
        "uk",
        False,
    )


def test_select_caption_translation_only_when_no_native_track() -> None:
    """T-132: translation is the LAST resort. Only fire when there is
    literally nothing else — and even then, the native fallback would have
    found any track that exists, so this is essentially dead code preserved
    for symmetry. Test that the path is intact for the (rare) case where
    only the translation-target language is offered as automatic."""
    auto_en_translated = {"ext": "json3", "url": "https://caption.test/translated-en"}
    selection = select_caption(
        # No native subtitles, but `en` translation is offered in automatic.
        # In practice yt-dlp returns this only when the video's auto-track
        # is itself in `en` — in which case it's a native track, not a
        # translation. For the test, we rely on the dict-shape contract.
        {"subtitles": {}, "automatic_captions": {"en": [auto_en_translated]}},
        prefer_languages=("de",),
        translate_to="en",
    )
    # Step 2 (any native) finds en first in automatic_captions before step 3
    # (translation) ever runs. translated=False because we treated it as a
    # native track, not a tlang= request.
    assert selection == ("https://caption.test/translated-en", "en", False)


def test_select_caption_skips_live_chat_tracks() -> None:
    manual_uk = {"ext": "json3", "url": "https://caption.test/manual-uk"}
    live_chat = {"ext": "json3", "url": "https://caption.test/live-chat", "name": "Live chat replay"}

    assert select_caption(
        {"subtitles": {"live_chat": [live_chat], "uk": [manual_uk]}, "automatic_captions": {}},
        prefer_languages=("en",),
        translate_to="en",
    ) == ("https://caption.test/manual-uk", "uk", False)
    assert select_caption(
        {"subtitles": {"live_chat": [live_chat]}, "automatic_captions": {"rechat": [live_chat]}},
        prefer_languages=("en",),
        translate_to="en",
    ) is None


def test_extract_info_sets_cookiefile_when_provided(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    seen: dict[str, object] = {}
    cookiefile = tmp_path / "cookies.txt"
    cookiefile.write_text("# cookies\n", encoding="utf-8")

    class FakeYoutubeDL:
        def __init__(self, options):
            seen["options"] = options
            self.options = options

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
            self.options = options

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
async def test_get_transcript_picks_native_track_when_preferred_lang_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """T-132 (post-fix): when preferred language is unavailable, fall back to
    the native track of whatever language is offered, NOT the translation
    endpoint. Pre-T-132 this returned translated=True because we'd request
    `tlang=en`; YouTube rate-limits that path. Native track download works
    cleanly, so we report it as a native track (translated=False).
    """
    seen: dict[str, object] = {}

    class FakeService(YtDlpService):
        def _extract_info(self, *args, **kwargs):
            return {
                "subtitles": {
                    "en": [
                        {
                            "ext": "json3",
                            "url": "https://caption.test/en.json3",
                            "impersonate": True,
                        }
                    ]
                },
                "automatic_captions": {},
            }

    class FakeYoutubeDL:
        def __init__(self, options):
            seen["options"] = options
            self.options = options

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def download(self, urls):
            seen["urls"] = urls
            tmpdir = Path(str(self.options["outtmpl"])).parent
            (tmpdir / "video1234567.en.json3").write_text(_caption_json3(), encoding="utf-8")

    monkeypatch.setitem(sys.modules, "yt_dlp", types.SimpleNamespace(YoutubeDL=FakeYoutubeDL))

    transcript = await FakeService().get_transcript("video1234567", prefer_languages=("uk",), translate_to="en")

    assert transcript is not None
    assert transcript.language_code == "en"
    assert transcript.translated is False  # T-132: native fallback, not translation
    assert "Ukrainian frontline context" in transcript.text
    assert seen["urls"] == ["https://www.youtube.com/watch?v=video1234567"]
    assert seen["options"]["writesubtitles"] is True
    assert seen["options"]["writeautomaticsub"] is False
    assert seen["options"]["subtitleslangs"] == ["en"]


@pytest.mark.asyncio
async def test_get_transcript_ignores_only_live_chat_tracks() -> None:
    class FakeService(YtDlpService):
        def _extract_info(self, *args, **kwargs):
            return {
                "subtitles": {
                    "live_chat": [
                        {
                            "ext": "json3",
                            "url": "https://caption.test/live-chat.json3",
                            "name": "Live chat replay",
                        }
                    ]
                },
                "automatic_captions": {},
            }

    assert await FakeService().get_transcript("video1234567") is None


@pytest.mark.asyncio
async def test_get_transcript_rejects_too_short_caption_file(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeService(YtDlpService):
        def _extract_info(self, *args, **kwargs):
            return {"subtitles": {"en": [{"ext": "json3", "url": "https://caption.test/en.json3"}]}, "automatic_captions": {}}

    class FakeYoutubeDL:
        def __init__(self, options):
            self.options = options

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def download(self, urls):
            tmpdir = Path(str(self.options["outtmpl"])).parent
            (tmpdir / "video1234567.en.json3").write_text(_caption_json3(rows=2), encoding="utf-8")

    monkeypatch.setitem(sys.modules, "yt_dlp", types.SimpleNamespace(YoutubeDL=FakeYoutubeDL))

    assert await FakeService().get_transcript("video1234567") is None


@pytest.mark.asyncio
async def test_get_transcript_rejects_repeated_timestamp_caption_file(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeService(YtDlpService):
        def _extract_info(self, *args, **kwargs):
            return {"subtitles": {"en": [{"ext": "json3", "url": "https://caption.test/en.json3"}]}, "automatic_captions": {}}

    class FakeYoutubeDL:
        def __init__(self, options):
            self.options = options

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def download(self, urls):
            tmpdir = Path(str(self.options["outtmpl"])).parent
            (tmpdir / "video1234567.en.json3").write_text(
                _caption_json3(rows=12, repeated_start=True),
                encoding="utf-8",
            )

    monkeypatch.setitem(sys.modules, "yt_dlp", types.SimpleNamespace(YoutubeDL=FakeYoutubeDL))

    assert await FakeService().get_transcript("video1234567") is None


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
