from __future__ import annotations

import json

import httpx
import pytest

from pcca.services.embedding_service import EmbeddingService, cosine_similarity, truncate_embedding_text


@pytest.mark.asyncio
async def test_embedding_service_wraps_ollama_embeddings_endpoint() -> None:
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        seen["json"] = request.read().decode()
        return httpx.Response(200, json={"embedding": [0.1, 0.2, 0.3]})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        service = EmbeddingService(
            enabled=True,
            ollama_base_url="http://ollama.test",
            embedding_model="nomic-embed-text:v1.5",
            http_client=client,
        )
        embedding = await service.embed("hello world")

    assert embedding == [0.1, 0.2, 0.3]
    assert seen["path"] == "/api/embeddings"
    assert "nomic-embed-text:v1.5" in seen["json"]


def test_cosine_similarity_handles_vectors() -> None:
    assert cosine_similarity([1.0, 0.0], [1.0, 0.0]) == pytest.approx(1.0)
    assert cosine_similarity([1.0, 0.0], [0.0, 1.0]) == pytest.approx(0.0)
    assert cosine_similarity([], [0.0, 1.0]) is None


def test_truncate_embedding_text_prefers_sentence_boundary() -> None:
    text = "First sentence. Second sentence keeps going. Third sentence should be dropped."

    truncated = truncate_embedding_text(text, max_chars=48)

    assert truncated == "First sentence. Second sentence keeps going."
    assert len(truncated) < 48


@pytest.mark.asyncio
async def test_embedding_service_sends_configured_truncated_prompt() -> None:
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["json"] = json.loads(request.read().decode())
        return httpx.Response(200, json={"embedding": [0.1, 0.2]})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        service = EmbeddingService(
            enabled=True,
            ollama_base_url="http://ollama.test",
            embedding_model="nomic-embed-text:v1.5",
            http_client=client,
            max_chars=32,
        )
        embedding = await service.embed("A practical point. " + "x" * 200)

    assert embedding == [0.1, 0.2]
    assert seen["json"]["prompt"] == "A practical point."
