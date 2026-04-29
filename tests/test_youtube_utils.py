from pcca.collectors.youtube_collector import (
    detect_youtube_interstitial,
    extract_yt_initial_data_from_html,
    extract_youtube_dom_video_rows,
    extract_youtube_initial_data_video_rows,
    is_youtube_login_url,
)
from pcca.collectors.youtube_utils import build_channel_videos_url, extract_video_id


def test_extract_video_id_from_watch_url() -> None:
    assert extract_video_id("https://www.youtube.com/watch?v=dQw4w9WgXcQ") == "dQw4w9WgXcQ"


def test_extract_video_id_from_short_url() -> None:
    assert extract_video_id("https://youtu.be/dQw4w9WgXcQ") == "dQw4w9WgXcQ"


def test_build_channel_url_from_handle() -> None:
    assert build_channel_videos_url("@openai") == "https://www.youtube.com/@openai/videos"


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
