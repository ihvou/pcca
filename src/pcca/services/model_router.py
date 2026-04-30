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
class ModelPreferenceExtractionResult:
    title: str
    include_terms: list[str]
    exclude_terms: list[str]
    quality_notes: str | None = None


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

    async def extract_subject_preferences(
        self,
        *,
        text: str,
        previous_title: str | None = None,
        previous_include_terms: list[str] | None = None,
        previous_exclude_terms: list[str] | None = None,
        previous_quality_notes: str | None = None,
    ) -> ModelPreferenceExtractionResult | None:
        if not self.enabled:
            logger.debug("Preference extraction skipped: model disabled.")
            return None
        started_at = time.monotonic()
        prompt = build_preference_extraction_prompt(
            text=text,
            previous_title=previous_title,
            previous_include_terms=previous_include_terms,
            previous_exclude_terms=previous_exclude_terms,
            previous_quality_notes=previous_quality_notes,
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
                "Preference extraction started model=%s text_chars=%d previous_title=%s",
                self.ollama_model,
                len(text),
                previous_title,
            )
            # Subject creation is a rare, one-off interaction. The first call
            # after Ollama startup loads the model into VRAM, which can take
            # 30-60s on a 7B model — bumped from 25s to tolerate cold start.
            async with httpx.AsyncClient(timeout=90.0) as client:
                response = await client.post(f"{self.ollama_base_url}/api/generate", json=payload)
                response.raise_for_status()
                data = response.json()
            raw = data.get("response", "")
            parsed = json.loads(raw)
            title = str(parsed.get("title") or previous_title or "New Subject").strip()
            include_terms = [
                str(term).strip().lower()
                for term in parsed.get("include_terms", [])
                if str(term).strip()
            ][:12]
            exclude_terms = [
                str(term).strip().lower()
                for term in parsed.get("exclude_terms", [])
                if str(term).strip()
            ][:12]
            quality_notes = str(parsed.get("quality_notes") or "").strip() or None
            logger.info(
                "Preference extraction finished model=%s duration_ms=%d title=%s include=%d exclude=%d",
                self.ollama_model,
                int((time.monotonic() - started_at) * 1000),
                title,
                len(include_terms),
                len(exclude_terms),
            )
            return ModelPreferenceExtractionResult(
                title=title,
                include_terms=include_terms,
                exclude_terms=exclude_terms,
                quality_notes=quality_notes,
            )
        except Exception as exc:
            logger.warning(
                "Preference extraction failed model=%s duration_ms=%d error=%s",
                self.ollama_model,
                int((time.monotonic() - started_at) * 1000),
                exc,
                exc_info=True,
            )
            return None


def build_preference_extraction_prompt(
    *,
    text: str,
    previous_title: str | None = None,
    previous_include_terms: list[str] | None = None,
    previous_exclude_terms: list[str] | None = None,
    previous_quality_notes: str | None = None,
) -> str:
    return (
        "You turn a user's free-form curation request into a compact subject draft.\n"
        "Return JSON only with fields:\n"
        '{"title": "up to 7 words", "include_terms": ["..."], '
        '"exclude_terms": ["..."], "quality_notes": "optional short note"}\n\n'
        "Critical separation rule:\n"
        "- include_terms and exclude_terms must be literal topic words or phrases likely to appear in matching content: proper nouns, named entities, product names, domain terms.\n"
        "- Do NOT put quality criteria in include_terms. Terms like reputable sources, high quality analytics, key thoughts, novelty, insight, practical, trustworthy, and useful belong in quality_notes.\n"
        "- If the user describes an abstract topic, enrich it into 5-10 likely content anchors.\n"
        "  Example: AI impact on IT jobs -> include_terms=[\"ai\", \"automation\", \"ml\", \"jobs\", \"employment\", \"layoff\", \"displacement\", \"augmentation\"].\n"
        "  Example: Ukraine war news from reputable sources -> include_terms=[\"ukraine\", \"war\", \"russia\", \"kyiv\"], quality_notes=\"prefer reputable sources; avoid propaganda\".\n"
        "- If the user names authoritative companies, domains, channels, or people, add a line in quality_notes starting with \"Authority:\".\n"
        "- Summarize conditional rules in quality_notes, e.g. \"Conditional: exclude X unless it discusses Y\".\n"
        "- If the user says not to boost likes, virality, drama, or engagement bait, add \"Engagement: do not boost\" to quality_notes.\n"
        "- Keep title distinctive and no longer than 7 words.\n"
        "- Keep include_terms/exclude_terms lowercase and concrete.\n\n"
        f"PREVIOUS TITLE: {previous_title or ''}\n"
        f"PREVIOUS INCLUDE: {previous_include_terms or []}\n"
        f"PREVIOUS EXCLUDE: {previous_exclude_terms or []}\n"
        f"PREVIOUS QUALITY NOTES: {previous_quality_notes or ''}\n\n"
        f"USER MESSAGE:\n{text[:4000]}"
    )
