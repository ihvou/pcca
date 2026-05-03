from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass, field

import httpx

logger = logging.getLogger(__name__)


@dataclass
class ModelRerankResult:
    score_delta: float
    rationale: str
    key_message: str | None = None
    refined_segment: str | None = None


@dataclass
class ModelRerankCandidate:
    item_id: int
    text: str
    heuristic_score: float
    author: str | None = None
    url: str | None = None
    published_at: str | None = None


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
    timeout_seconds: float = 180.0
    http_client: httpx.AsyncClient | None = field(default=None, repr=False)

    async def _post_generate(self, payload: dict) -> dict:
        if self.http_client is not None:
            response = await self.http_client.post(f"{self.ollama_base_url}/api/generate", json=payload)
        else:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                response = await client.post(f"{self.ollama_base_url}/api/generate", json=payload)
        response.raise_for_status()
        return response.json()

    async def rerank_batch(
        self,
        *,
        subject_name: str,
        subject_description: str,
        candidates: list[ModelRerankCandidate],
    ) -> dict[int, ModelRerankResult]:
        if not self.enabled or not candidates:
            logger.debug("Model batch rerank skipped: disabled_or_empty subject=%s", subject_name)
            return {}
        started_at = time.monotonic()
        compact_candidates = [
            {
                "item_id": candidate.item_id,
                "heuristic_score": round(candidate.heuristic_score, 4),
                "author": candidate.author,
                "published_at": candidate.published_at,
                "url": candidate.url,
                "text": candidate.text[:1000],
            }
            for candidate in candidates[:20]
        ]
        prompt = (
            "You are a strict curator. Score candidate items for one user's subject.\n"
            "Use the full subject description directly. Respect authority, conditional rules, anti-signals, novelty, and practicality stated by the user.\n"
            "Return JSON only with field ranked: an array of objects with item_id, score_delta, reason, key_message.\n"
            "score_delta must be between -0.25 and 0.25. Include every candidate item exactly once.\n\n"
            "key_message should be 1-2 concise sentences that rephrase the useful core idea for this user's subject. "
            "Do not include biography, hype, or internal scoring details.\n\n"
            f"SUBJECT TITLE: {subject_name}\n"
            f"FULL SUBJECT DESCRIPTION:\n{subject_description[:4000]}\n\n"
            f"CANDIDATES:\n{json.dumps(compact_candidates, ensure_ascii=False)}"
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
                "Model batch rerank started subject=%s model=%s candidates=%d",
                subject_name,
                self.ollama_model,
                len(compact_candidates),
            )
            data = await self._post_generate(payload)
            raw = data.get("response", "")
            parsed = parse_model_json_response(raw, context=f"batch rerank subject={subject_name}") or {}
            ranked = parsed.get("ranked", [])
            if not isinstance(ranked, list) or not ranked:
                logger.warning(
                    "Model batch rerank returned no ranked items subject=%s model=%s top_k_count=%d response_preview=%s",
                    subject_name,
                    self.ollama_model,
                    len(compact_candidates),
                    _preview(raw),
                )
                return {}
            out: dict[int, ModelRerankResult] = {}
            allowed_ids = {candidate.item_id for candidate in candidates}
            for row in ranked:
                if not isinstance(row, dict):
                    continue
                try:
                    item_id = int(row.get("item_id"))
                except (TypeError, ValueError):
                    continue
                if item_id not in allowed_ids:
                    continue
                delta = max(-0.25, min(0.25, float(row.get("score_delta", 0.0) or 0.0)))
                reason = str(row.get("reason") or "model batch rerank").strip()
                key_message = str(row.get("key_message") or "").strip() or None
                out[item_id] = ModelRerankResult(
                    score_delta=delta,
                    rationale=reason,
                    key_message=key_message,
                )
            key_message_count = sum(1 for result in out.values() if result.key_message)
            logger.info(
                "Model batch rerank finished subject=%s model=%s top_k_count=%d results=%d key_message_count=%d duration_ms=%d",
                subject_name,
                self.ollama_model,
                len(compact_candidates),
                len(out),
                key_message_count,
                int((time.monotonic() - started_at) * 1000),
            )
            return out
        except Exception as exc:
            logger.warning(
                "Model batch rerank failed subject=%s model=%s candidates=%d duration_ms=%d error=%s",
                subject_name,
                self.ollama_model,
                len(compact_candidates),
                int((time.monotonic() - started_at) * 1000),
                exc,
                exc_info=True,
            )
            return {}

    async def refine_batch(
        self,
        *,
        subject_name: str,
        subject_description: str,
        candidates: list[ModelRerankCandidate],
        limit: int = 5,
    ) -> dict[int, str]:
        if not self.enabled or not candidates or limit <= 0:
            logger.debug("Model refinement skipped: disabled_or_empty subject=%s", subject_name)
            return {}
        started_at = time.monotonic()
        compact_candidates = [
            {
                "item_id": candidate.item_id,
                "author": candidate.author,
                "published_at": candidate.published_at,
                "url": candidate.url,
                "text": candidate.text[:1800],
            }
            for candidate in candidates[:limit]
        ]
        prompt = (
            "You clean up matched content segments for a user's Brief.\n"
            "Return JSON only with field refined: an array of objects with item_id and refined_segment.\n"
            "For each candidate, write 3-6 concise sentences. Drop fillers, repetition, and transcript artifacts. "
            "Keep concrete claims, quotes, numbers, and speaker context. Do not invent details.\n\n"
            f"SUBJECT TITLE: {subject_name}\n"
            f"FULL SUBJECT DESCRIPTION:\n{subject_description[:2500]}\n\n"
            f"CANDIDATES:\n{json.dumps(compact_candidates, ensure_ascii=False)}"
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
                "Model refinement batch started subject=%s model=%s candidates=%d",
                subject_name,
                self.ollama_model,
                len(compact_candidates),
            )
            data = await self._post_generate(payload)
            raw = data.get("response", "")
            parsed = parse_model_json_response(raw, context=f"refinement subject={subject_name}") or {}
            refined = parsed.get("refined", [])
            if not isinstance(refined, list) or not refined:
                logger.warning(
                    "Model refinement returned no items subject=%s model=%s top_n=%d response_preview=%s",
                    subject_name,
                    self.ollama_model,
                    len(compact_candidates),
                    _preview(raw),
                )
                return {}
            allowed_ids = {int(candidate["item_id"]) for candidate in compact_candidates}
            out: dict[int, str] = {}
            for row in refined:
                if not isinstance(row, dict):
                    continue
                try:
                    item_id = int(row.get("item_id"))
                except (TypeError, ValueError):
                    continue
                if item_id not in allowed_ids:
                    continue
                refined_segment = str(row.get("refined_segment") or "").strip()
                if refined_segment:
                    out[item_id] = refined_segment
            logger.info(
                "Model refinement batch finished subject=%s model=%s top_n=%d results=%d duration_ms=%d",
                subject_name,
                self.ollama_model,
                len(compact_candidates),
                len(out),
                int((time.monotonic() - started_at) * 1000),
            )
            return out
        except Exception as exc:
            logger.warning(
                "Model refinement batch failed subject=%s model=%s candidates=%d duration_ms=%d error=%s",
                subject_name,
                self.ollama_model,
                len(compact_candidates),
                int((time.monotonic() - started_at) * 1000),
                exc,
                exc_info=True,
            )
            return {}

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
            '{"score_delta": number between -0.25 and 0.25, "reason": "short reason", "key_message": "1-2 useful sentences"}\n'
            "key_message should rephrase the useful core idea for the user and avoid biography, hype, and scoring details.\n"
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
            data = await self._post_generate(payload)
            raw = data.get("response", "")
            parsed = parse_model_json_response(raw, context=f"single rerank subject={subject_name}") or {}
            delta = float(parsed.get("score_delta", 0.0))
            delta = max(-0.25, min(0.25, delta))
            reason = str(parsed.get("reason", "model rerank")).strip()
            key_message = str(parsed.get("key_message") or "").strip() or None
            logger.info(
                "Model rerank finished subject=%s model=%s duration_ms=%d delta=%.3f key_message=%s",
                subject_name,
                self.ollama_model,
                int((time.monotonic() - started_at) * 1000),
                delta,
                bool(key_message),
            )
            return ModelRerankResult(
                score_delta=delta,
                rationale=reason,
                key_message=key_message,
            )
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


def parse_model_json_response(raw: str, *, context: str) -> dict | None:
    """Parse Ollama JSON output while tolerating common local-model drift."""
    text = str(raw or "").strip()
    if not text:
        logger.warning("Model response empty context=%s", context)
        return None
    candidates = [text]
    fenced = re.search(r"```(?:json)?\s*(.*?)```", text, flags=re.IGNORECASE | re.DOTALL)
    if fenced:
        candidates.insert(0, fenced.group(1).strip())
    extracted = _extract_first_json_object(text)
    if extracted and extracted not in candidates:
        candidates.append(extracted)
    for candidate in candidates:
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            return parsed
    logger.warning(
        "Model response JSON parse failed context=%s response_preview=%s",
        context,
        _preview(text),
    )
    return None


def _extract_first_json_object(text: str) -> str | None:
    start = text.find("{")
    if start < 0:
        return None
    depth = 0
    in_string = False
    escaped = False
    for pos in range(start, len(text)):
        ch = text[pos]
        if in_string:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[start : pos + 1]
    return None


def _preview(value: str, *, limit: int = 500) -> str:
    return " ".join(str(value or "").split())[:limit]
