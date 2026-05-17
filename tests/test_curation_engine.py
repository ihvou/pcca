import pytest

from pcca.collectors.base import CollectedItem
from pcca.pipeline.curation import CurationEngine


def test_curation_engine_scores_practical_content_higher() -> None:
    engine = CurationEngine()
    practical_item = CollectedItem(
        platform="youtube",
        external_id="vid-1",
        author="author",
        url="https://youtube.com/watch?v=abc",
        text="Claude Code workflow and release feature implementation example",
        transcript_text=None,
        published_at=None,
        metadata={},
    )
    noisy_item = CollectedItem(
        platform="youtube",
        external_id="vid-2",
        author="author",
        url="https://youtube.com/watch?v=def",
        text="My story and motivation, like and share, beginner tips",
        transcript_text=None,
        published_at=None,
        metadata={},
    )

    practical_score = engine.score("Vibe Coding", practical_item)
    noisy_score = engine.score("Vibe Coding", noisy_item)
    assert practical_score.final_score > noisy_score.final_score


def test_curation_engine_respects_include_exclude_terms() -> None:
    engine = CurationEngine()
    item = CollectedItem(
        platform="youtube",
        external_id="vid-3",
        author="author",
        url="https://youtube.com/watch?v=ghi",
        text="Release notes for Claude Code. Also includes long biography and motivation segment.",
        transcript_text=None,
        published_at=None,
        metadata={},
    )
    baseline = engine.score("Vibe Coding", item)
    tuned = engine.score(
        "Vibe Coding",
        item,
        include_terms=["claude code", "release notes"],
        exclude_terms=["biography", "motivation"],
    )
    # Include and exclude both hit, but preference-aware scoring should alter the decision.
    assert tuned.final_score != baseline.final_score


def test_curation_engine_uses_engagement_signals() -> None:
    engine = CurationEngine()
    low_signal = CollectedItem(
        platform="x",
        external_id="post-1",
        author="author",
        url="https://x.com/a/status/1",
        text="Claude Code workflow feature implementation example",
        transcript_text=None,
        published_at=None,
        metadata={},
    )
    high_signal = CollectedItem(
        platform="x",
        external_id="post-2",
        author="author",
        url="https://x.com/a/status/2",
        text="Claude Code workflow feature implementation example",
        transcript_text=None,
        published_at=None,
        metadata={"view_count": 250_000, "like_count": 4_200, "comment_count": 180, "repost_count": 650},
    )

    low = engine.score("Vibe Coding", low_signal)
    high = engine.score("Vibe Coding", high_signal)

    assert high.final_score > low.final_score
    assert "engagement_strength=" in high.rationale
    assert "views=250000" in high.rationale


def test_curation_engine_can_use_semantic_similarity_as_relevance() -> None:
    engine = CurationEngine()
    item = CollectedItem(
        platform="rss",
        external_id="war-update",
        author="analyst",
        url="https://example.com/war",
        text="Frontline update with maps and practical context from Kyiv.",
        transcript_text=None,
        published_at=None,
        metadata={},
    )

    low = engine.score("Ukraine War News", item, semantic_similarity=0.1)
    high = engine.score("Ukraine War News", item, semantic_similarity=0.95)

    assert high.final_score > low.final_score
    assert "semantic_similarity=0.950" in high.rationale


def test_t153_high_relevance_news_tweet_not_demoted_by_min_practicality() -> None:
    """T-153 (2026-05-14): a tweet that's highly on-topic but lacks practical
    keywords should NOT be demoted by the min_practicality guardrail.

    Live evidence (run_id=100): @ClaudeDevs "Claude Code limits +50%" had
    relevance≈0.60 and practicality=0.25 — direct AI-tools news for a
    subject literally titled "AI Tools & Tips" — but landed at final_score
    0.38 (below the 0.55 floor) because the original demote subtracted 0.20
    flat whenever practicality < min_practicality. The new behavior only
    applies the demote when BOTH relevance < 0.55 AND practicality is low
    (the actual "fluffy content" signal). Strong-relevance news passes
    through at its formula score.
    """
    engine = CurationEngine()

    # On-topic news tweet (short, no "workflow"/"code"/"release" keywords).
    news_tweet = CollectedItem(
        platform="x",
        external_id="claudedevs-news",
        author="ClaudeDevs",
        url="https://x.com/ClaudeDevs/status/1",
        text="Weekly limits are increasing 50%, now through July 13. Live now for all Pro, Max, Team, and seat-based Enterprise users.",
        transcript_text=None,
        published_at=None,
        metadata={},
    )

    # Genuinely fluffy content (low relevance + no practical signals).
    fluffy = CollectedItem(
        platform="x",
        external_id="motivational",
        author="randomguru",
        url="https://x.com/random/1",
        text="The journey is the destination. Trust the process.",
        transcript_text=None,
        published_at=None,
        metadata={},
    )

    # Strong relevance via embedding similarity.
    news_scored = engine.score(
        "AI Tools and Tips",
        news_tweet,
        semantic_similarity=0.60,
        min_practicality=0.5,
    )
    # Both relevance AND practicality weak — the genuine fluff case.
    fluffy_scored = engine.score(
        "AI Tools and Tips",
        fluffy,
        semantic_similarity=0.20,
        min_practicality=0.5,
    )

    # News tweet: practicality is still below 0.5 but receives the short-form
    # floor from T-161, and relevance is 0.60 (strong) —
    # the demote must NOT fire. Final score should be ~0.55 (formula value),
    # not 0.35 (formula - 0.20).
    assert news_scored.practicality_score < 0.5
    assert news_scored.final_score > 0.50, (
        f"News tweet got final_score={news_scored.final_score}; T-153 expects "
        ">0.50 because strong relevance should override the practicality demote"
    )

    # Fluffy content: BOTH signals weak → demote DOES fire as designed.
    assert fluffy_scored.practicality_score < 0.5
    assert fluffy_scored.final_score < 0.30, (
        f"Fluffy content got final_score={fluffy_scored.final_score}; T-153 "
        "preserves the demote when relevance is also weak (<0.55)"
    )


def test_t161_x_tweet_with_strong_relevance_clears_floor_but_fluff_stays_low() -> None:
    engine = CurationEngine()
    useful_tweet = CollectedItem(
        platform="x",
        external_id="anthropic-news",
        author="AnthropicAI",
        url="https://x.com/AnthropicAI/status/1",
        text="Claude Code adds higher usage limits for paid plans this week.",
        transcript_text=None,
        published_at=None,
        metadata={},
    )
    fluff = CollectedItem(
        platform="x",
        external_id="fluff",
        author="randomguru",
        url="https://x.com/random/status/1",
        text="big things coming soon",
        transcript_text=None,
        published_at=None,
        metadata={},
    )

    useful = engine.score(
        "AI Tools and Tips",
        useful_tweet,
        semantic_similarity=0.52,
        min_practicality=0.5,
    )
    noisy = engine.score(
        "AI Tools and Tips",
        fluff,
        semantic_similarity=0.20,
        min_practicality=0.5,
    )

    assert useful.practicality_score == pytest.approx(0.40)
    assert useful.final_score > 0.54
    assert noisy.final_score < 0.35
