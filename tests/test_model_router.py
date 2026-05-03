import json
import logging

import httpx
import pytest

from pcca.services.model_router import (
    ModelRerankCandidate,
    ModelRouter,
    build_preference_extraction_prompt,
    parse_model_json_response,
)


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


def test_parse_model_json_response_strips_markdown_fence() -> None:
    parsed = parse_model_json_response('```json\n{"ranked": []}\n```', context="test")

    assert parsed == {"ranked": []}


@pytest.mark.asyncio
async def test_model_router_batch_rerank_uses_configured_timeout_and_key_messages() -> None:
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        seen["payload"] = json.loads(request.read().decode())
        return httpx.Response(
            200,
            json={
                "response": json.dumps(
                    {
                        "ranked": [
                            {
                                "item_id": 1,
                                "score_delta": 0.1,
                                "reason": "practical details",
                                "key_message": "The useful point is clear.",
                            }
                        ]
                    }
                )
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler), timeout=180.0) as client:
        router = ModelRouter(
            enabled=True,
            ollama_base_url="http://ollama.test",
            ollama_model="qwen2.5:7b",
            timeout_seconds=180.0,
            http_client=client,
        )
        results = await router.rerank_batch(
            subject_name="AI Tools",
            subject_description="Practical Claude Code updates.",
            candidates=[
                ModelRerankCandidate(
                    item_id=1,
                    text="Claude Code introduced a better handoff workflow.",
                    heuristic_score=0.7,
                )
            ],
        )

    assert seen["path"] == "/api/generate"
    assert "refined_segment" not in seen["payload"]["prompt"]
    assert results[1].score_delta == pytest.approx(0.1)
    assert results[1].key_message == "The useful point is clear."
    assert results[1].refined_segment is None


@pytest.mark.asyncio
async def test_model_router_refinement_batch_is_separate_top_n_call() -> None:
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["payload"] = json.loads(request.read().decode())
        return httpx.Response(
            200,
            json={
                "response": json.dumps(
                    {
                        "refined": [
                            {
                                "item_id": 2,
                                "refined_segment": "Cleaned up practical explanation.",
                            }
                        ]
                    }
                )
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        router = ModelRouter(
            enabled=True,
            ollama_base_url="http://ollama.test",
            ollama_model="qwen2.5:7b",
            http_client=client,
        )
        results = await router.refine_batch(
            subject_name="AI Tools",
            subject_description="Practical Claude Code updates.",
            candidates=[
                ModelRerankCandidate(item_id=2, text="uh Claude Code can help", heuristic_score=0.9),
                ModelRerankCandidate(item_id=3, text="not selected", heuristic_score=0.8),
            ],
            limit=1,
        )

    assert "refined_segment" in seen["payload"]["prompt"]
    assert "not selected" not in seen["payload"]["prompt"]
    assert results == {2: "Cleaned up practical explanation."}


@pytest.mark.asyncio
async def test_model_router_logs_malformed_json_preview(caplog: pytest.LogCaptureFixture) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        _ = request
        return httpx.Response(200, json={"response": "not json and no ranked objects"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        router = ModelRouter(
            enabled=True,
            ollama_base_url="http://ollama.test",
            ollama_model="qwen2.5:7b",
            http_client=client,
        )
        with caplog.at_level(logging.WARNING):
            results = await router.rerank_batch(
                subject_name="AI Tools",
                subject_description="Practical Claude Code updates.",
                candidates=[ModelRerankCandidate(item_id=1, text="Claude Code", heuristic_score=0.7)],
            )

    assert results == {}
    assert "response_preview=not json and no ranked objects" in caplog.text
