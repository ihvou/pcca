from __future__ import annotations

import httpx
import pytest

from pcca.services.embedding_service import EmbeddingService, cosine_similarity


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
