from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass

import httpx

logger = logging.getLogger(__name__)


def cosine_similarity(left: list[float], right: list[float]) -> float | None:
    if not left or not right or len(left) != len(right):
        return None
    dot = sum(a * b for a, b in zip(left, right))
    left_norm = math.sqrt(sum(a * a for a in left))
    right_norm = math.sqrt(sum(b * b for b in right))
    if left_norm <= 0 or right_norm <= 0:
        return None
    return dot / (left_norm * right_norm)


@dataclass
class EmbeddingService:
    enabled: bool
    ollama_base_url: str
    embedding_model: str
    timeout_seconds: float = 30.0
    http_client: httpx.AsyncClient | None = None

    async def embed(self, text: str) -> list[float] | None:
        normalized = " ".join((text or "").split()).strip()
        if not self.enabled:
            logger.debug("Embedding skipped: disabled model=%s", self.embedding_model)
            return None
        if not normalized:
            return None

        started_at = time.monotonic()
        payload = {
            "model": self.embedding_model,
            "prompt": normalized[:8000],
        }
        try:
            if self.http_client is not None:
                response = await self.http_client.post(f"{self.ollama_base_url}/api/embeddings", json=payload)
            else:
                async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                    response = await client.post(f"{self.ollama_base_url}/api/embeddings", json=payload)
            response.raise_for_status()
            data = response.json()
            embedding = data.get("embedding")
            if not isinstance(embedding, list):
                logger.warning("Embedding response missing embedding field model=%s", self.embedding_model)
                return None
            vector = [float(value) for value in embedding]
            logger.info(
                "Embedding finished model=%s dims=%d text_chars=%d duration_ms=%d",
                self.embedding_model,
                len(vector),
                len(normalized),
                int((time.monotonic() - started_at) * 1000),
            )
            return vector
        except Exception as exc:
            logger.warning(
                "Embedding failed model=%s text_chars=%d duration_ms=%d error=%s",
                self.embedding_model,
                len(normalized),
                int((time.monotonic() - started_at) * 1000),
                exc,
                exc_info=True,
            )
            return None
