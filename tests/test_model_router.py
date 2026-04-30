import pytest

from pcca.services.model_router import ModelRouter, build_preference_extraction_prompt


@pytest.mark.asyncio
async def test_model_router_disabled_returns_none() -> None:
    router = ModelRouter(enabled=False, ollama_base_url="http://localhost:11434", ollama_model="qwen2.5:7b")
    result = await router.rerank(subject_name="Vibe Coding", text="workflow release", heuristic_score=0.7)
    assert result is None


def test_preference_extraction_prompt_separates_topic_and_quality() -> None:
    prompt = build_preference_extraction_prompt(text="Ukraine war news from reputable sources")

    assert "literal topic words" in prompt
    assert "Do NOT put quality criteria in include_terms" in prompt
    assert '"ukraine", "war", "russia", "kyiv"' in prompt
    assert "Authority:" in prompt
    assert "Engagement: do not boost" in prompt
