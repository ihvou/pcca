import pytest

from pcca.services.model_router import ModelRouter


@pytest.mark.asyncio
async def test_model_router_disabled_returns_none() -> None:
    router = ModelRouter(enabled=False, ollama_base_url="http://localhost:11434", ollama_model="qwen2.5:7b")
    result = await router.rerank(subject_name="Vibe Coding", text="workflow release", heuristic_score=0.7)
    assert result is None

