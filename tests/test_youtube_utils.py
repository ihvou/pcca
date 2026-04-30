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
from pcca.collectors.youtube_utils import build_channel_videos_url, extract_video_id


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
