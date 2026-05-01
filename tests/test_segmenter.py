from __future__ import annotations

from pcca.collectors.base import CollectedItem
from pcca.pipeline.segmenter import segment_item


def _row(index: int, *, start: float, duration: float = 90.0) -> dict:
    words = " ".join(f"word{index}_{n}" for n in range(40))
    return {"text": f"Sentence {index}. {words}.", "start": start, "duration": duration}


def test_youtube_transcript_rows_preserve_timestamped_segments() -> None:
    rows = [_row(index, start=index * 90.0) for index in range(30)]
    item = CollectedItem(
        platform="youtube",
        external_id="video-1",
        author="Builder",
        url="https://www.youtube.com/watch?v=video-1",
        text="Long interview",
        transcript_text="\n".join(row["text"] for row in rows),
        published_at=None,
        metadata={"transcript_rows": rows},
    )

    segments = segment_item(item)

    assert len(segments) >= 10
    assert segments[0].segment_type == "transcript"
    assert segments[0].start_offset_seconds == 0.0
    assert segments[0].end_offset_seconds == 180.0
    assert segments[1].start_offset_seconds == 180.0


def test_short_post_becomes_single_segment() -> None:
    item = CollectedItem(
        platform="x",
        external_id="post-1",
        author="Builder",
        url="https://x.com/builder/status/1",
        text="Claude Code added a practical workflow feature.",
        transcript_text=None,
        published_at=None,
        metadata={},
    )

    segments = segment_item(item)

    assert len(segments) == 1
    assert segments[0].text == item.text
    assert segments[0].start_offset_seconds is None


def test_description_only_podcast_does_not_emit_fake_timestamp() -> None:
    item = CollectedItem(
        platform="apple_podcasts",
        external_id="episode-1",
        author="Podcast",
        url="https://podcasts.apple.com/us/podcast/demo/id123?i=456",
        text="A short episode description without transcript timing.",
        transcript_text=None,
        published_at=None,
        metadata={},
    )

    segments = segment_item(item)

    assert len(segments) == 1
    assert segments[0].start_offset_seconds is None
