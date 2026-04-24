from __future__ import annotations

import json
from dataclasses import dataclass

import httpx


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
            return None
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
            async with httpx.AsyncClient(timeout=25.0) as client:
                response = await client.post(f"{self.ollama_base_url}/api/generate", json=payload)
                response.raise_for_status()
                data = response.json()
            raw = data.get("response", "")
            parsed = json.loads(raw)
            delta = float(parsed.get("score_delta", 0.0))
            delta = max(-0.25, min(0.25, delta))
            reason = str(parsed.get("reason", "model rerank")).strip()
            return ModelRerankResult(score_delta=delta, rationale=reason)
        except Exception:
            return None

