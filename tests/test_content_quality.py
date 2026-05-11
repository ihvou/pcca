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


def test_does_not_flag_legitimate_prose_starting_with_window() -> None:
    """Bug fix (2026-05-12): the old `_looks_like_js_dump` short-circuited
    on `startswith("window.")` which false-positives on legitimate Karpathy /
    SQL / JS-tutorial transcripts. Real prose has structural-char density
    <5% and lacks YouTube tokens — should pass through.
    """
    karpathy_window_prose = (
        "Window functions in SQL are powerful tools for aggregations over "
        "groups of rows. In this video I will walk through PARTITION BY and "
        "ORDER BY clauses, showing how the OVER keyword changes the result "
        "set. We will compare LAG, LEAD, ROW_NUMBER, RANK, and DENSE_RANK "
        "with concrete examples on a sales table."
    )
    assert is_low_quality(karpathy_window_prose) is None


def test_does_not_flag_prose_followed_by_see_also_links() -> None:
    """Bug fix (2026-05-12): Anthropic item id=35 (`How Anthropic uses
    Claude in Product Engineering`) has 100+ words of substantive prose
    + a 3-bullet "see also" link block at the end. Previously flagged as
    link_list because of the 3 consecutive bullet-with-URL lines. Real
    content followed by links is NOT a link dump — flag only when
    bullets DOMINATE the word count.
    """
    prose_then_links = (
        "How Anthropic uses Claude in Product Engineering\n\n"
        "Product engineers lose hours toggling between tools and tackling "
        "subtasks one at a time. Software engineer Chuma Kabaghe shows how "
        "she uses Claude Code to onboard onto unfamiliar codebases in "
        "minutes, run autonomous testing loops, and manage parallel coding "
        "sessions, reducing context switching and shipping faster.\n\n"
        "Check out other stories in the \"How Anthropic uses Claude\" series, below:\n"
        "- How Anthropic uses Claude in Legal: https://www.youtube.com/watch?v=abc\n"
        "- How Anthropic uses Claude in Marketing: https://www.youtube.com/watch?v=def\n"
        "- How Anthropic uses Claude in Product Management: https://www.youtube.com/watch?v=ghi\n"
        "Get started with Claude Code: https://code.claude.com/docs/en/quickstart"
    )
    assert is_low_quality(prose_then_links) is None


def test_does_not_flag_legitimate_podcast_episode_outline() -> None:
    """Bug fix (2026-05-12): the bullet detector previously fired on ANY
    3+ consecutive hyphen-bullets, false-positiving on legitimate podcast
    episode outlines. Live example (item_id=673 "International Beit Din"):
    legitimate content with 4 hyphen-bullets describing episode segments,
    no URLs in bullets, was flagged as link_list. Real link-lists have
    URLs in each bullet ('• Anthropic: https://...'); content outlines
    have plain text. Detector now requires URLs in the bullets.
    """
    legit_outline = (
        "What about the men?\n\n"
        "They don't want their wives back. It's been years since they lived "
        "together. Why stay married to someone you despise?\n\n"
        "In This Episode\n"
        "- Arguments from men who say refusing a gett is ok.\n"
        "- Results from an International Beit Din study looking at clients.\n"
        "- Rabbinical rulings on when husbands should give the gett.\n"
        "- What we hear when we talk to gett-refusing husbands.\n"
    )
    assert is_low_quality(legit_outline) is None


def test_does_not_flag_legitimate_prose_starting_with_function() -> None:
    function_prose = (
        "Function overloading in C++ allows multiple functions to share the "
        "same name as long as their parameter lists differ. The compiler "
        "selects the appropriate function based on argument types at compile "
        "time. This contrasts with function overriding which relies on "
        "virtual dispatch at runtime."
    )
    assert is_low_quality(function_prose) is None


def test_detects_generic_dense_js_dump_without_youtube_tokens() -> None:
    """Signal B fallback: real JS/JSON with >35% structural density should
    be flagged even without the YouTube token list. Catches dumps from new
    sites we haven't enumerated."""
    generic_dump = (
        '{"name":"x","items":[{"a":1,"b":2,"c":3},{"a":4,"b":5,"c":6},'
        '{"a":7,"b":8,"c":9}],"meta":{"version":1,"tags":["a","b","c","d"]}}' * 4
    )
    assert is_low_quality(generic_dump) == "js_dump"


def test_force_keep_bypasses_low_quality_flag() -> None:
    metadata = mark_low_quality_metadata(
        {FORCE_KEEP_KEY: True},
        'window.WIZ_global_data = {"youtube_web": true}',
    )

    assert EXCLUDED_FROM_BRIEFS_KEY not in metadata
    assert excluded_from_briefs_reason(metadata, 'window.WIZ_global_data = {"youtube_web": true}') is None
