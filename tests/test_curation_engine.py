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

