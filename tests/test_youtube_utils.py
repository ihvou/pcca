from pcca.collectors.youtube_collector import is_youtube_login_url
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
