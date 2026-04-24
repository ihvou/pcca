from pcca.collectors.linkedin_collector import LinkedInCollector
from pcca.collectors.reddit_collector import RedditCollector
from pcca.collectors.rss_collector import RSSCollector
from pcca.collectors.spotify_collector import SpotifyCollector
from pcca.collectors.x_collector import XCollector
from pcca.collectors.youtube_collector import YouTubeCollector


def test_collectors_expose_common_max_items_knob() -> None:
    session_manager = object()
    collectors = [
        XCollector(session_manager=session_manager, max_items=3),  # type: ignore[arg-type]
        LinkedInCollector(session_manager=session_manager, max_items=3),  # type: ignore[arg-type]
        YouTubeCollector(session_manager=session_manager, max_items=3),  # type: ignore[arg-type]
        SpotifyCollector(session_manager=session_manager, max_items=3),  # type: ignore[arg-type]
        RedditCollector(max_items=3),
        RSSCollector(max_items=3),
    ]

    assert [collector.max_items for collector in collectors] == [3, 3, 3, 3, 3, 3]
