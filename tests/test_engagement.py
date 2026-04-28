from pcca.collectors.spotify_collector import parse_duration_seconds
from pcca.collectors.youtube_collector import parse_youtube_meta
from pcca.engagement import EngagementSignals


def test_engagement_signals_normalize_counts() -> None:
    signals = EngagementSignals.from_metadata(
        {
            "view_count": "12.5k",
            "like_count": "1,200",
            "comment_count": 42,
            "repost_count": "2m",
        }
    )

    assert signals.views == 12_500
    assert signals.likes == 1_200
    assert signals.comments == 42
    assert signals.reposts == 2_000_000
    assert signals.strength() > 0


def test_youtube_and_spotify_metadata_helpers() -> None:
    assert parse_youtube_meta("12K views 2 days ago 1:02:03") == {
        "view_count": 12_000,
        "duration_seconds": 3723,
    }
    assert parse_duration_seconds("1 hr 24 min") == 5040
