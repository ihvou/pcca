import httpx
import pytest

from pcca.collectors.youtube_collector import (
    YouTubeCollector,
    build_youtube_about_url,
    detect_youtube_interstitial,
    extract_youtube_channel_id_from_html,
    extract_yt_initial_data_from_html,
    extract_youtube_dom_video_rows,
    extract_youtube_initial_data_video_rows,
    is_youtube_login_url,
    parse_youtube_rss,
    youtube_rss_url,
)
from pcca.collectors.errors import SourceNotFoundError
from pcca.collectors.youtube_utils import build_channel_videos_url, extract_video_id
from pcca.services.youtube_transcript_service import TranscriptResult
from pcca.services.yt_dlp_service import YtDlpVideo, parse_caption_payload, select_caption


SAMPLE_CHANNEL_ID = "UC1234567890123456789012"
SAMPLE_RSS = f"""<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns:yt="http://www.youtube.com/xml/schemas/2015"
      xmlns:media="http://search.yahoo.com/mrss/"
      xmlns="http://www.w3.org/2005/Atom">
  <yt:channelId>{SAMPLE_CHANNEL_ID}</yt:channelId>
  <title>OpenAI</title>
  <author><name>OpenAI</name></author>
  <entry>
    <yt:videoId>abc123</yt:videoId>
    <yt:channelId>{SAMPLE_CHANNEL_ID}</yt:channelId>
    <title>Claude Code practical release notes</title>
    <link rel="alternate" href="https://www.youtube.com/watch?v=abc123"/>
    <published>2026-04-28T10:00:00+00:00</published>
    <media:group>
      <media:title>Claude Code practical release notes</media:title>
      <media:description>Concrete workflow updates for builders.</media:description>
      <media:community><media:statistics views="12345"/></media:community>
    </media:group>
  </entry>
</feed>
"""


def test_extract_video_id_from_watch_url() -> None:
    assert extract_video_id("https://www.youtube.com/watch?v=dQw4w9WgXcQ") == "dQw4w9WgXcQ"


def test_extract_video_id_from_short_url() -> None:
    assert extract_video_id("https://youtu.be/dQw4w9WgXcQ") == "dQw4w9WgXcQ"


def test_build_channel_url_from_handle() -> None:
    assert build_channel_videos_url("@openai") == "https://www.youtube.com/@openai/videos"
    assert build_youtube_about_url("@openai") == "https://www.youtube.com/@openai/about"
    assert youtube_rss_url(SAMPLE_CHANNEL_ID).endswith(f"channel_id={SAMPLE_CHANNEL_ID}")


def test_extract_youtube_channel_id_from_about_html() -> None:
    html = f'<link rel="canonical" href="https://www.youtube.com/channel/{SAMPLE_CHANNEL_ID}">'

    assert extract_youtube_channel_id_from_html(html) == SAMPLE_CHANNEL_ID


def test_parse_youtube_rss_feed() -> None:
    channel_id, author, rows = parse_youtube_rss(SAMPLE_RSS, max_items=5)

    assert channel_id == SAMPLE_CHANNEL_ID
    assert author == "OpenAI"
    assert rows[0]["video_id"] == "abc123"
    assert rows[0]["published_at"] == "2026-04-28T10:00:00+00:00"
    assert rows[0]["view_count"] == 12345


@pytest.mark.asyncio
async def test_youtube_collector_uses_rss_without_browser() -> None:
    class NoTranscript:
        async def get_transcript_text(self, _video_id: str):
            return None

    class ExplodingSession:
        async def new_page(self, _platform: str):
            raise AssertionError("YouTube RSS collector should not launch a browser page")

    async def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == youtube_rss_url(SAMPLE_CHANNEL_ID)
        return httpx.Response(200, text=SAMPLE_RSS)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = YouTubeCollector(
            session_manager=ExplodingSession(),  # type: ignore[arg-type]
            transcript_service=NoTranscript(),  # type: ignore[arg-type]
            http_client=client,
            max_items=5,
        )
        items = await collector.collect_from_source(SAMPLE_CHANNEL_ID)

    assert len(items) == 1
    assert items[0].external_id == "abc123"
    assert items[0].author == "OpenAI"
    assert items[0].published_at == "2026-04-28T10:00:00+00:00"
    assert items[0].metadata["view_count"] == 12345


@pytest.mark.asyncio
async def test_youtube_collector_raises_source_not_found_for_rss_404() -> None:
    class NoTranscript:
        async def get_transcript_text(self, _video_id: str):
            return None

    async def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == youtube_rss_url(SAMPLE_CHANNEL_ID)
        return httpx.Response(404, text="Not Found")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = YouTubeCollector(
            transcript_service=NoTranscript(),  # type: ignore[arg-type]
            http_client=client,
            max_items=5,
        )
        with pytest.raises(SourceNotFoundError) as exc_info:
            await collector.collect_from_source(SAMPLE_CHANNEL_ID)

    assert exc_info.value.platform == "youtube"
    assert exc_info.value.not_found_kind == "rss_404"
    assert exc_info.value.status_code == 404


@pytest.mark.asyncio
async def test_youtube_collector_preserves_transcript_rows_for_segmenter() -> None:
    class TranscriptService:
        async def get_transcript(self, _video_id: str):
            return TranscriptResult(
                text="first transcript row\nsecond transcript row",
                rows=[
                    {"text": "first transcript row", "start": 10.0, "duration": 4.0},
                    {"text": "second transcript row", "start": 14.0, "duration": 5.0},
                ],
                language_code="en",
                translated=False,
            )

    class ExplodingSession:
        async def new_page(self, _platform: str):
            raise AssertionError("YouTube RSS collector should not launch a browser page")

    async def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == youtube_rss_url(SAMPLE_CHANNEL_ID)
        return httpx.Response(200, text=SAMPLE_RSS)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        collector = YouTubeCollector(
            session_manager=ExplodingSession(),  # type: ignore[arg-type]
            transcript_service=TranscriptService(),  # type: ignore[arg-type]
            http_client=client,
            max_items=5,
        )
        items = await collector.collect_from_source(SAMPLE_CHANNEL_ID)

    assert items[0].transcript_text == "first transcript row\nsecond transcript row"
    assert items[0].metadata["transcript_rows"][0]["start"] == 10.0
    assert items[0].metadata["transcript_language"] == "en"


@pytest.mark.asyncio
async def test_youtube_collector_prefers_yt_dlp_with_cookie_export() -> None:
    class FakeYtDlpService:
        async def list_channel_videos(self, source_id: str, *, max_items: int, cookiefile):
            assert source_id == "@openai"
            assert max_items == 5
            assert str(cookiefile).endswith("cookies.txt")
            return [
                YtDlpVideo(
                    external_id="video1234567",
                    url="https://www.youtube.com/watch?v=video1234567",
                    title="Practical Claude Code rollout",
                    description="Specific workflow details.",
                    published_at="2026-05-03",
                    channel_name="OpenAI",
                    channel_id=SAMPLE_CHANNEL_ID,
                    view_count=1234,
                    like_count=56,
                    duration_seconds=600,
                )
            ]

        async def get_transcript(self, video_id: str, *, cookiefile=None):
            assert video_id == "video1234567"
            return TranscriptResult(
                text="The rollout adds a practical handoff workflow.",
                rows=[{"text": "The rollout adds a practical handoff workflow.", "start": 30.0, "duration": 5.0}],
                language_code="en",
                translated=False,
            )

    class FakeSession:
        async def export_netscape_cookies(self, *, platform: str):
            assert platform == "youtube"
            return "/tmp/cookies.txt"

    collector = YouTubeCollector(
        session_manager=FakeSession(),  # type: ignore[arg-type]
        yt_dlp_service=FakeYtDlpService(),  # type: ignore[arg-type]
        max_items=5,
    )

    items = await collector.collect_from_source("@openai")

    assert len(items) == 1
    assert items[0].external_id == "video1234567"
    assert items[0].metadata["youtube_data_source"] == "yt_dlp"
    assert items[0].metadata["view_count"] == 1234
    assert items[0].metadata["like_count"] == 56
    assert items[0].metadata["duration_seconds"] == 600
    assert items[0].metadata["cookiefile_used"] is True
    assert items[0].transcript_text == "The rollout adds a practical handoff workflow."


def test_yt_dlp_caption_helpers_parse_json_and_select_caption() -> None:
    info = {
        "subtitles": {},
        "automatic_captions": {
            "uk": [{"ext": "json3", "url": "https://caption.test/uk.json3"}],
            "en": [{"ext": "vtt", "url": "https://caption.test/en.vtt"}],
        },
    }

    assert select_caption(info, prefer_languages=("en",), translate_to="en") == (
        "https://caption.test/en.vtt",
        "en",
        False,
    )
    rows = parse_caption_payload(
        '{"events":[{"tStartMs":12000,"dDurationMs":3000,"segs":[{"utf8":"hello "},{"utf8":"world"}]}]}'
    )
    assert rows == [{"text": "hello world", "start": 12.0, "duration": 3.0}]


def test_youtube_login_url_detection() -> None:
    assert is_youtube_login_url("https://accounts.google.com/signin/v2/identifier?service=youtube")
    assert is_youtube_login_url("https://www.youtube.com/signin?action_handle_signin=true")
    assert not is_youtube_login_url("https://www.youtube.com/@openai/videos")


def test_youtube_dom_fixture_extraction() -> None:
    html = """
    <html><body>
      <a id="video-title-link" href="/watch?v=abc123" title="Practical Claude Code workflow"></a>
      <a id="video-title" href="https://www.youtube.com/watch?v=def456">Skills hype recap</a>
    </body></html>
    """

    rows = extract_youtube_dom_video_rows(html, max_items=5)

    assert rows == [
        {
            "url": "https://www.youtube.com/watch?v=abc123",
            "title": "Practical Claude Code workflow",
            "channel_name": None,
            "meta_text": "",
        },
        {
            "url": "https://www.youtube.com/watch?v=def456",
            "title": "Skills hype recap",
            "channel_name": None,
            "meta_text": "",
        },
    ]


def test_youtube_initial_data_fixture_extraction() -> None:
    html = """
    <script>
      var ytInitialData = {"contents":{"twoColumnBrowseResultsRenderer":{"tabs":[{"tabRenderer":{"content":{"richGridRenderer":{"contents":[
        {"richItemRenderer":{"content":{"videoRenderer":{
          "videoId":"abc123",
          "title":{"runs":[{"text":"Claude Code practical release notes"}]},
          "ownerText":{"runs":[{"text":"Boris Cherny"}]},
          "viewCountText":{"simpleText":"12K views"},
          "lengthText":{"simpleText":"14:05"},
          "publishedTimeText":{"simpleText":"2 days ago"},
          "navigationEndpoint":{"commandMetadata":{"webCommandMetadata":{"url":"/watch?v=abc123"}}}
        }}}},
        {"richItemRenderer":{"content":{"videoRenderer":{
          "videoId":"def456",
          "title":{"simpleText":"Generic Skills tutorial"},
          "shortBylineText":{"runs":[{"text":"AI Enthusiast"}]},
          "viewCountText":{"simpleText":"99 views"},
          "navigationEndpoint":{"commandMetadata":{"webCommandMetadata":{"url":"/watch?v=def456"}}}
        }}}}
      ]}}}}]}}};
    </script>
    """

    data = extract_yt_initial_data_from_html(html)
    rows = extract_youtube_initial_data_video_rows(data, max_items=5)

    assert rows[0]["url"] == "https://www.youtube.com/watch?v=abc123"
    assert rows[0]["title"] == "Claude Code practical release notes"
    assert rows[0]["channel_name"] == "Boris Cherny"
    assert rows[0]["meta_text"] == "12K views 14:05 2 days ago"
    assert rows[1]["title"] == "Generic Skills tutorial"


def test_youtube_interstitial_detection() -> None:
    assert detect_youtube_interstitial("Before you continue to YouTube", "Review your choices") == "consent_wall"
    assert detect_youtube_interstitial("Video unavailable", "Sign in to confirm your age") == "age_gate"
    assert detect_youtube_interstitial("OpenAI videos", "normal page") is None
