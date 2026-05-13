import json
import logging

import httpx
import pytest

from pcca.services.model_router import (
    ModelRerankCandidate,
    ModelRouter,
    REFINE_BATCH_RESPONSE_SCHEMA,
    RERANK_BATCH_RESPONSE_SCHEMA,
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
    assert seen["payload"]["format"] == RERANK_BATCH_RESPONSE_SCHEMA
    prompt = seen["payload"]["prompt"]
    assert "refined_segment" not in prompt
    assert "Use only content present in the candidate text" in prompt
    assert "Do not introduce names, claims, products, companies, or context" in prompt
    assert "15-30 words" in prompt
    # T-151: prompt must use "balanced" framing (not "strict") and must
    # establish 0.00 as the DEFAULT score_delta. Without these, the LLM
    # systematically biases negative and drags healthy items below the
    # relevance floor — see model_router.rerank_batch for the full history.
    assert "balanced curator" in prompt
    assert "strict curator" not in prompt
    assert "0.00 — DEFAULT" in prompt
    assert "If you have to reach for a reason to lower the score, return 0.0 instead." in prompt
    # T-151: heuristic_score must NOT be visible to the model — it anchored
    # downward corrections.
    assert "heuristic_score" not in prompt
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
    assert "paraphrasing only literal claims present in the candidate text" in seen["payload"]["prompt"]
    assert "Do not introduce names, claims, products, companies, or context" in seen["payload"]["prompt"]
    assert seen["payload"]["format"] == REFINE_BATCH_RESPONSE_SCHEMA
    assert "not selected" not in seen["payload"]["prompt"]
    assert results == {2: "Cleaned up practical explanation."}


@pytest.mark.asyncio
async def test_model_router_batch_rerank_gracefully_handles_wrong_structured_shape(
    caplog: pytest.LogCaptureFixture,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.read().decode())
        assert payload["format"] == RERANK_BATCH_RESPONSE_SCHEMA
        return httpx.Response(
            200,
            json={
                "response": json.dumps(
                    {
                        "articles": [
                            {
                                "title": "Fabricated feed item",
                                "url": "https://example.com",
                                "author": "Model",
                                "date": "2026-05-04",
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
        with caplog.at_level(logging.WARNING):
            results = await router.rerank_batch(
                subject_name="AI Jobs",
                subject_description="Impact of AI on IT labor market.",
                candidates=[ModelRerankCandidate(item_id=1, text="AI jobs analysis", heuristic_score=0.7)],
            )

    assert results == {}
    assert "returned no ranked items" in caplog.text
    assert "articles" in caplog.text


@pytest.mark.asyncio
async def test_t151_batch_rerank_log_line_reports_delta_distribution(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Per-batch telemetry: mean/neg/zero/pos surfaced in INFO log.

    T-151 (2026-05-14): without these counters in the log line, ops have no
    way to spot a returning negative-bias regression without re-running a DB
    audit. With them, a single grep on `mean_delta=` and `neg=` per run shows
    the trend.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "response": json.dumps(
                    {
                        "ranked": [
                            {"item_id": 1, "score_delta": 0.20, "reason": "strong", "key_message": "A strong point."},
                            {"item_id": 2, "score_delta": 0.00, "reason": "neutral", "key_message": "A neutral point."},
                            {"item_id": 3, "score_delta": -0.10, "reason": "weak", "key_message": "A weak point."},
                            {"item_id": 4, "score_delta": -0.05, "reason": "weak", "key_message": "Another weak."},
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
        with caplog.at_level(logging.INFO, logger="pcca.services.model_router"):
            await router.rerank_batch(
                subject_name="AI Tools",
                subject_description="Practical Claude Code updates.",
                candidates=[
                    ModelRerankCandidate(item_id=1, text="x", heuristic_score=0.7),
                    ModelRerankCandidate(item_id=2, text="x", heuristic_score=0.7),
                    ModelRerankCandidate(item_id=3, text="x", heuristic_score=0.7),
                    ModelRerankCandidate(item_id=4, text="x", heuristic_score=0.7),
                ],
            )

    # mean = (0.20 + 0.00 - 0.10 - 0.05) / 4 = 0.0125
    assert "mean_delta=+0.013" in caplog.text or "mean_delta=+0.012" in caplog.text
    assert "neg=2" in caplog.text
    assert "zero=1" in caplog.text
    assert "pos=1" in caplog.text


@pytest.mark.asyncio
async def test_t151_single_rerank_prompt_uses_balanced_framing() -> None:
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["payload"] = json.loads(request.read().decode())
        return httpx.Response(
            200,
            json={
                "response": json.dumps(
                    {
                        "item_id": 7,
                        "score_delta": 0.0,
                        "reason": "on-topic",
                        "key_message": "On-topic point about workflows.",
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
        result = await router.rerank(
            subject_name="AI Tools",
            text="Practical Claude Code workflow.",
            heuristic_score=0.72,
            item_id=7,
        )

    prompt = seen["payload"]["prompt"]
    # T-151: single-item fallback prompt must mirror the batch prompt — same
    # bias surfaces on both paths because per-item fallback fires for every
    # item the batch fails to score.
    assert "balanced curator" in prompt
    assert "strict curator" not in prompt
    assert "0.00 — DEFAULT" in prompt
    assert "Heuristic score: 0.720" not in prompt  # T-151: no anchor
    assert "Heuristic score:" not in prompt
    assert result is not None
    assert result.score_delta == 0.0


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


@pytest.mark.asyncio
async def test_preference_extraction_uses_shared_model_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    seen = {}

    class FakeAsyncClient:
        def __init__(self, *, timeout):
            seen["timeout"] = timeout

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url: str, *, json: dict):
            seen["url"] = url
            seen["prompt"] = json["prompt"]
            return httpx.Response(
                200,
                json={
                    "response": '{"title":"Ukraine War News","include_terms":["ukraine"],"exclude_terms":["rumors"],"quality_notes":"Authority: reputable"}'
                },
                request=httpx.Request("POST", url),
            )

    monkeypatch.setattr(httpx, "AsyncClient", FakeAsyncClient)
    router = ModelRouter(
        enabled=True,
        ollama_base_url="http://ollama.test",
        ollama_model="qwen2.5:7b",
        timeout_seconds=37.0,
    )

    result = await router.extract_subject_preferences(text="Ukraine war updates from reputable sources")

    assert seen["timeout"] == 37.0
    assert seen["url"] == "http://ollama.test/api/generate"
    assert result is not None
    assert result.title == "Ukraine War News"
    assert result.include_terms == ["ukraine"]
