from __future__ import annotations

from pcca.content_quality import (
    EXCLUDED_FROM_BRIEFS_KEY,
    FORCE_KEEP_KEY,
    excluded_from_briefs_reason,
    is_low_quality,
    mark_low_quality_metadata,
)


def test_detects_youtube_js_dump() -> None:
    text = 'window.WIZ_global_data = {"AfY8Hf":false,"MUE6Ne":"youtube_web","cfb2h":"youtube.web-front-end"};'

    assert is_low_quality(text) == "js_dump"


def test_detects_json_dense_youtube_page_config() -> None:
    text = '{"foo":"bar","baz":[1,2,3],"WIZ_global_data":{"youtube_web":true},"more":{"x":"y","a":"b"}}' * 5

    assert is_low_quality(text) == "js_dump"


def test_detects_url_heavy_link_list() -> None:
    text = " • Anthropic: https://anthropic.com • OpenClaw: https://openclaw.ai • Docs: https://docs.example.com"

    assert is_low_quality(text) == "link_list"


def test_detects_consecutive_bullet_dump() -> None:
    text = "\n".join(
        [
            "Useful heading",
            "• Anthropic: https://anthropic.com",
            "• OpenClaw: https://openclaw.ai",
            "• Docs: https://docs.example.com",
        ]
    )

    assert is_low_quality(text) == "link_list"


def test_detects_marketing_prose() -> None:
    text = "We are excited to announce that we've won another G2 award after launching @Plumm HR."

    assert is_low_quality(text) == "marketing_prose"


def test_detects_hashtag_spam() -> None:
    text = "Thrilled to share our update #ai #growth #startup #productivity"

    assert is_low_quality(text) == "marketing_prose"


def test_force_keep_bypasses_low_quality_flag() -> None:
    metadata = mark_low_quality_metadata(
        {FORCE_KEEP_KEY: True},
        'window.WIZ_global_data = {"youtube_web": true}',
    )

    assert EXCLUDED_FROM_BRIEFS_KEY not in metadata
    assert excluded_from_briefs_reason(metadata, 'window.WIZ_global_data = {"youtube_web": true}') is None
