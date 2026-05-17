import json
import logging

import httpx
import pytest

from pcca.services.model_router import (
    ModelRerankCandidate,
    ModelRouter,
    SUMMARY_BATCH_RESPONSE_SCHEMA,
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
async def test_t159_model_router_summarize_batch_produces_both_outputs() -> None:
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        seen["payload"] = json.loads(request.read().decode())
        return httpx.Response(
            200,
            json={
                "response": json.dumps(
                    {
                        "summaries": [
                            {
                                "item_id": 1,
                                "brief_summary": "Claude Code introduced a better handoff workflow for implementation reviews.",
                                "detailed_summary": "The item says Claude Code introduced a better handoff workflow. It frames the change around implementation reviews. The summary stays within the candidate text.",
                                "is_low_content": False,
                                "reason": "practical details",
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
        results = await router.summarize_batch(
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
    assert seen["payload"]["format"] == SUMMARY_BATCH_RESPONSE_SCHEMA
    prompt = seen["payload"]["prompt"]
    assert "brief_summary" in prompt
    assert "detailed_summary" in prompt
    assert "Use only content present in the candidate text" in prompt
    assert "Do not introduce names, claims, products, companies, or context" in prompt
    assert "15-30 words" in prompt
    assert "Your job is NOT to score it" in prompt
    assert "heuristic_score" not in prompt
    assert results[1].brief_summary.startswith("Claude Code introduced")
    assert results[1].detailed_summary.startswith("The item says")
    assert results[1].is_low_content is False


@pytest.mark.asyncio
async def test_t159_model_router_summarize_batch_handles_low_content() -> None:
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["payload"] = json.loads(request.read().decode())
        return httpx.Response(
            200,
            json={
                "response": json.dumps(
                    {
                        "summaries": [
                            {
                                "item_id": 2,
                                "brief_summary": "",
                                "detailed_summary": "",
                                "is_low_content": True,
                                "reason": "transition filler",
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
        results = await router.summarize_batch(
            subject_name="AI Tools",
            subject_description="Practical Claude Code updates.",
            candidates=[
                ModelRerankCandidate(item_id=2, text="uh Claude Code can help", heuristic_score=0.9),
                ModelRerankCandidate(item_id=3, text="not selected", heuristic_score=0.8),
            ],
        )

    assert "detailed_summary" in seen["payload"]["prompt"]
    assert "Do not introduce names, claims, products, companies, or context" in seen["payload"]["prompt"]
    assert seen["payload"]["format"] == SUMMARY_BATCH_RESPONSE_SCHEMA
    assert results[2].is_low_content is True
    assert results[2].brief_summary is None
    assert results[2].detailed_summary is None


@pytest.mark.asyncio
async def test_model_router_batch_rerank_gracefully_handles_wrong_structured_shape(
    caplog: pytest.LogCaptureFixture,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.read().decode())
        assert payload["format"] == SUMMARY_BATCH_RESPONSE_SCHEMA
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
            results = await router.summarize_batch(
                subject_name="AI Jobs",
                subject_description="Impact of AI on IT labor market.",
                candidates=[ModelRerankCandidate(item_id=1, text="AI jobs analysis", heuristic_score=0.7)],
            )

    assert results == {}
    assert "returned no summary items" in caplog.text
    assert "articles" in caplog.text


@pytest.mark.asyncio
async def test_t159_summary_batch_log_line_reports_summary_counts(
    caplog: pytest.LogCaptureFixture,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "response": json.dumps(
                    {
                        "summaries": [
                            {"item_id": 1, "brief_summary": "A strong practical point is summarized clearly.", "detailed_summary": "Detailed summary one. More detail. Final detail.", "is_low_content": False},
                            {"item_id": 2, "brief_summary": "A second useful point is summarized clearly.", "detailed_summary": "Detailed summary two. More detail. Final detail.", "is_low_content": False},
                            {"item_id": 3, "brief_summary": "", "detailed_summary": "", "is_low_content": True},
                            {"item_id": 4, "brief_summary": "Another usable point is summarized clearly.", "detailed_summary": "Detailed summary four. More detail. Final detail.", "is_low_content": False},
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
            await router.summarize_batch(
                subject_name="AI Tools",
                subject_description="Practical Claude Code updates.",
                candidates=[
                    ModelRerankCandidate(item_id=1, text="x", heuristic_score=0.7),
                    ModelRerankCandidate(item_id=2, text="x", heuristic_score=0.7),
                    ModelRerankCandidate(item_id=3, text="x", heuristic_score=0.7),
                    ModelRerankCandidate(item_id=4, text="x", heuristic_score=0.7),
                ],
            )

    assert "brief_count=3" in caplog.text
    assert "detailed_count=3" in caplog.text
    assert "low_content=1" in caplog.text


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
            results = await router.summarize_batch(
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
