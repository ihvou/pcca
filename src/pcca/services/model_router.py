from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass

import httpx

logger = logging.getLogger(__name__)


@dataclass
class ModelRerankResult:
    score_delta: float
    rationale: str


@dataclass
class ModelRouter:
    enabled: bool
    ollama_base_url: str
    ollama_model: str

    async def rerank(self, *, subject_name: str, text: str, heuristic_score: float) -> ModelRerankResult | None:
        if not self.enabled:
            logger.debug("Model rerank skipped: disabled subject=%s", subject_name)
            return None
        started_at = time.monotonic()
        prompt = (
            "You are a strict curator.\n"
            f"Subject: {subject_name}\n"
            f"Heuristic score: {heuristic_score:.3f}\n"
            "Given this content snippet, return JSON with fields:\n"
            '{"score_delta": number between -0.25 and 0.25, "reason": "short reason"}\n'
            "Only return JSON.\n\n"
            f"CONTENT:\n{text[:4000]}"
        )
        payload = {
            "model": self.ollama_model,
            "prompt": prompt,
            "stream": False,
            "format": "json",
            "options": {"temperature": 0.1},
        }
        try:
            logger.debug(
                "Model rerank started subject=%s model=%s text_chars=%d heuristic_score=%.3f",
                subject_name,
                self.ollama_model,
                len(text),
                heuristic_score,
            )
            async with httpx.AsyncClient(timeout=25.0) as client:
                response = await client.post(f"{self.ollama_base_url}/api/generate", json=payload)
                response.raise_for_status()
                data = response.json()
            raw = data.get("response", "")
            parsed = json.loads(raw)
            delta = float(parsed.get("score_delta", 0.0))
            delta = max(-0.25, min(0.25, delta))
            reason = str(parsed.get("reason", "model rerank")).strip()
            logger.info(
                "Model rerank finished subject=%s model=%s duration_ms=%d delta=%.3f",
                subject_name,
                self.ollama_model,
                int((time.monotonic() - started_at) * 1000),
                delta,
            )
            return ModelRerankResult(score_delta=delta, rationale=reason)
        except Exception as exc:
            logger.warning(
                "Model rerank failed subject=%s model=%s duration_ms=%d error=%s",
                subject_name,
                self.ollama_model,
                int((time.monotonic() - started_at) * 1000),
                exc,
                exc_info=True,
            )
            return None
