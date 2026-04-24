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
