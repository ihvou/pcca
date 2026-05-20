from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass, field

import httpx

logger = logging.getLogger(__name__)


SUMMARY_BATCH_RESPONSE_SCHEMA: dict = {
    "type": "object",
    "properties": {
        "summaries": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "item_id": {"type": "integer"},
                    "brief_summary": {"type": "string"},
                    "detailed_summary": {"type": "string"},
                    "is_low_content": {"type": "boolean"},
                    "reason": {"type": "string"},
                },
                "required": ["item_id", "brief_summary", "detailed_summary", "is_low_content"],
            },
        }
    },
    "required": ["summaries"],
}


RERANK_BATCH_RESPONSE_SCHEMA: dict = SUMMARY_BATCH_RESPONSE_SCHEMA


REFINE_BATCH_RESPONSE_SCHEMA: dict = {
    "type": "object",
    "properties": {
        "refined": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "item_id": {"type": "integer"},
                    "refined_segment": {"type": "string"},
                },
                "required": ["item_id", "refined_segment"],
            },
        }
    },
    "required": ["refined"],
}


SINGLE_RERANK_RESPONSE_SCHEMA: dict = {
    "type": "object",
    "properties": {
        "item_id": {"type": "integer"},
        "score_delta": {"type": "number"},
        "reason": {"type": "string"},
        "key_message": {"type": "string"},
    },
    "required": ["item_id", "score_delta", "reason", "key_message"],
}


@dataclass
class ModelRerankResult:
    score_delta: float
    rationale: str
    key_message: str | None = None
    refined_segment: str | None = None


@dataclass
class ModelSummaryResult:
    brief_summary: str | None
    detailed_summary: str | None
    is_low_content: bool = False
    rationale: str = "model summary"


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
    llm_provider: str = "ollama"
    llm_model: str | None = None
    gemini_api_key: str | None = None
    gemini_base_url: str = "https://generativelanguage.googleapis.com"
    http_client: httpx.AsyncClient | None = field(default=None, repr=False)
    last_summary_duration_ms: int = field(default=0, init=False)
    last_summary_provider: str = field(default="", init=False)
    last_summary_model: str = field(default="", init=False)
    last_summary_usage: dict = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        provider = (self.llm_provider or "ollama").strip().lower()
        if provider not in {"gemini", "openai", "ollama"}:
            provider = "ollama"
        self.llm_provider = provider
        if not self.llm_model:
            self.llm_model = "gemini-2.5-flash" if provider == "gemini" else self.ollama_model

    async def _post_generate(self, payload: dict, *, schema: dict | None = None) -> dict:
        if self.llm_provider == "gemini":
            try:
                data = await self._call_gemini(prompt=str(payload.get("prompt") or ""), schema=schema)
                self.last_summary_provider = "gemini"
                self.last_summary_model = str(self.llm_model or "gemini-2.5-flash")
                self.last_summary_usage = data.get("_pcca_usage", {}) if isinstance(data, dict) else {}
                return data
            except Exception:
                logger.warning(
                    "Gemini model call failed; falling back to Ollama model=%s",
                    self.ollama_model,
                    exc_info=True,
                )
                self.last_summary_provider = "ollama"
                self.last_summary_model = self.ollama_model
                self.last_summary_usage = {}
        # All model calls, including subject creation/rebuild, share this
        # timeout so PCCA_MODEL_ROUTER_TIMEOUT_SECONDS has one meaning.
        request_payload = dict(payload)
        if schema is not None:
            request_payload["format"] = schema
        if self.http_client is not None:
            response = await self.http_client.post(f"{self.ollama_base_url}/api/generate", json=request_payload)
        else:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                response = await client.post(f"{self.ollama_base_url}/api/generate", json=request_payload)
        response.raise_for_status()
        self.last_summary_provider = "ollama"
        self.last_summary_model = self.ollama_model
        self.last_summary_usage = {}
        return response.json()

    async def _call_gemini(self, *, prompt: str, schema: dict | None = None) -> dict:
        if not self.gemini_api_key:
            raise RuntimeError("PCCA_GEMINI_API_KEY is required for Gemini LLM provider.")
        model_name = str(self.llm_model or "gemini-2.5-flash").strip()
        model_path = model_name if model_name.startswith("models/") else f"models/{model_name}"
        url = f"{self.gemini_base_url.rstrip('/')}/v1beta/{model_path}:generateContent?key={self.gemini_api_key}"
        generation_config: dict = {
            "temperature": 0.1,
            "responseMimeType": "application/json",
        }
        if schema is not None:
            generation_config["responseSchema"] = schema
        request_payload = {
            "contents": [
                {
                    "role": "user",
                    "parts": [{"text": prompt}],
                }
            ],
            "generationConfig": generation_config,
        }
        if self.http_client is not None:
            response = await self.http_client.post(url, json=request_payload)
        else:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                response = await client.post(url, json=request_payload)
        response.raise_for_status()
        data = response.json()
        raw = _extract_gemini_text(data)
        return {
            "response": raw,
            "_pcca_usage": data.get("usageMetadata", {}),
        }

    async def summarize_batch(
        self,
        *,
        subject_name: str,
        subject_description: str,
        candidates: list[ModelRerankCandidate],
    ) -> dict[int, ModelSummaryResult]:
        if not self.enabled or not candidates:
            logger.debug("Model summary batch skipped: disabled_or_empty subject=%s", subject_name)
            return {}
        started_at = time.monotonic()
        compact_candidates = [
            {
                "item_id": candidate.item_id,
                "author": candidate.author,
                "published_at": candidate.published_at,
                "url": candidate.url,
                "text": candidate.text[:1000],
            }
            for candidate in candidates[:20]
        ]
        prompt = (
            "You prepare candidate content for one user's Brief.\n"
            "Each candidate already passed a relevance filter. Your job is NOT to score it; your job is "
            "to produce clean user-facing summaries or reject low-content candidates.\n\n"
            "Return JSON only with field summaries: an array of objects with item_id, brief_summary, "
            "detailed_summary, is_low_content, and optional reason. Include every candidate item exactly once.\n\n"
            "Content rules:\n"
            "- Use only content present in the candidate text. Do not introduce names, claims, products, companies, or context from the subject description.\n"
            "- brief_summary must be one complete sentence, 15-30 words, no direct quotes, summarizing the speaker's specific point.\n"
            "- detailed_summary must be 3-5 concise sentences paraphrasing literal claims from the candidate text, with speaker/source context when clear.\n"
            "- If the candidate is ambiguous, say it briefly mentions the topic without elaborating; do not fill gaps.\n"
            "- Name the speaker/source when clear from author or title; avoid generic 'the author' or 'the speaker'.\n"
            "- If the candidate is filler, ad read, transition, greeting, or low-content, set is_low_content=true and return empty summaries.\n"
            "- If the candidate is substantive but its topic clearly does NOT match the SUBJECT DESCRIPTION below "
            "(for example: military or war content shown to a tax-law subject; generic AI industry news or tool "
            "tutorials shown to a 'concrete success stories' subject; political commentary shown to a 'product "
            "management' subject), set is_low_content=true with reason='off-topic for this subject' and return "
            "empty summaries. The user prefers an empty digest over off-topic content. Only fire this when the "
            "mismatch is clear — when the connection is plausible or borderline, summarize and let the user judge.\n"
            "- For every non-low-content candidate, return BOTH brief_summary and detailed_summary. Do not return only one of them.\n"
            "- Do not include biography, hype, or internal scoring details.\n\n"
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
        provider_label = self.llm_provider
        model_label = str(self.llm_model or self.ollama_model)
        try:
            logger.debug(
                "Model summary batch started subject=%s provider=%s model=%s candidates=%d",
                subject_name,
                provider_label,
                model_label,
                len(compact_candidates),
            )
            data = await self._post_generate(payload, schema=SUMMARY_BATCH_RESPONSE_SCHEMA)
            raw = data.get("response", "")
            parsed = parse_model_json_response(raw, context=f"summary batch subject={subject_name}") or {}
            summaries = parsed.get("summaries", [])
            if not isinstance(summaries, list) or not summaries:
                self.last_summary_duration_ms = int((time.monotonic() - started_at) * 1000)
                logger.warning(
                    "Model summary batch returned no summary items subject=%s provider=%s model=%s top_k_count=%d response_preview=%s",
                    subject_name,
                    self.last_summary_provider or provider_label,
                    self.last_summary_model or model_label,
                    len(compact_candidates),
                    _preview(raw),
                )
                return {}
            out: dict[int, ModelSummaryResult] = {}
            allowed_ids = {candidate.item_id for candidate in candidates}
            for row in summaries:
                if not isinstance(row, dict):
                    continue
                try:
                    item_id = int(row.get("item_id"))
                except (TypeError, ValueError):
                    continue
                if item_id not in allowed_ids:
                    continue
                brief_summary = str(row.get("brief_summary") or "").strip() or None
                detailed_summary = str(row.get("detailed_summary") or "").strip() or None
                is_low_content = bool(row.get("is_low_content"))
                reason = str(row.get("reason") or "model summary").strip()
                if is_low_content:
                    brief_summary = None
                    detailed_summary = None
                out[item_id] = ModelSummaryResult(
                    brief_summary=brief_summary,
                    detailed_summary=detailed_summary,
                    is_low_content=is_low_content,
                    rationale=reason,
                )
            brief_count = sum(1 for result in out.values() if result.brief_summary)
            detailed_count = sum(1 for result in out.values() if result.detailed_summary)
            low_content_count = sum(1 for result in out.values() if result.is_low_content)
            duration_ms = int((time.monotonic() - started_at) * 1000)
            self.last_summary_duration_ms = duration_ms
            logger.info(
                "Model summary batch finished subject=%s provider=%s model=%s top_k_count=%d results=%d "
                "brief_count=%d detailed_count=%d low_content=%d duration_ms=%d",
                subject_name,
                self.last_summary_provider or provider_label,
                self.last_summary_model or model_label,
                len(compact_candidates),
                len(out),
                brief_count,
                detailed_count,
                low_content_count,
                duration_ms,
            )
            return out
        except Exception as exc:
            self.last_summary_duration_ms = int((time.monotonic() - started_at) * 1000)
            logger.warning(
                "Model summary batch failed subject=%s provider=%s model=%s candidates=%d duration_ms=%d error=%s",
                subject_name,
                self.last_summary_provider or provider_label,
                self.last_summary_model or model_label,
                len(compact_candidates),
                self.last_summary_duration_ms,
                exc,
                exc_info=True,
            )
            return {}

    async def rerank_batch(
        self,
        *,
        subject_name: str,
        subject_description: str,
        candidates: list[ModelRerankCandidate],
    ) -> dict[int, ModelRerankResult]:
        summaries = await self.summarize_batch(
            subject_name=subject_name,
            subject_description=subject_description,
            candidates=candidates,
        )
        return {
            item_id: ModelRerankResult(
                score_delta=0.0,
                rationale=result.rationale,
                key_message="(low-content segment)" if result.is_low_content else result.brief_summary,
                refined_segment=result.detailed_summary,
            )
            for item_id, result in summaries.items()
        }

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
            "For each candidate, write 2-4 concise sentences paraphrasing only literal claims present in the candidate text. "
            "Drop fillers, repetition, and transcript artifacts. Keep concrete claims, numbers, and speaker context. "
            "Do not introduce names, claims, products, companies, or context from the subject description. "
            "If the candidate is ambiguous, write that it briefly mentions the topic without elaborating. "
            "If it is filler, ad read, transition, greeting, or low-content, return an empty refined_segment.\n\n"
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
            data = await self._post_generate(payload, schema=REFINE_BATCH_RESPONSE_SCHEMA)
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

    async def rerank(
        self,
        *,
        subject_name: str,
        text: str,
        heuristic_score: float,
        item_id: int | None = None,
    ) -> ModelRerankResult | None:
        if not self.enabled:
            logger.debug("Model rerank skipped: disabled subject=%s", subject_name)
            return None
        started_at = time.monotonic()
        # T-151: same neutralized framing as rerank_batch. heuristic_score is
        # no longer shown to the model — it was anchoring downward corrections.
        # See model_router.rerank_batch comment for the A/B evidence.
        prompt = (
            "You are a balanced curator.\n"
            f"Subject: {subject_name}\n"
            f"Item ID: {item_id if item_id is not None else ''}\n"
            "The item already passed an earlier relevance filter. Adjust its score only "
            "when you have clear evidence the earlier score was wrong.\n\n"
            "Return JSON with fields:\n"
            '{"item_id": integer, "score_delta": number between -0.25 and 0.25, "reason": "short reason", "key_message": "one complete 15-30 word sentence"}\n\n'
            "How to choose score_delta:\n"
            "- 0.00 — DEFAULT. On-topic and no clear evidence to adjust. Most items belong here.\n"
            "- Positive (up to +0.25) — unusually strong: specific concrete claims from a credible source plus novelty or practical value.\n"
            "- Negative (down to -0.25) — clearly off-topic, promotional, low-content, or hits an anti-signal. If you have to reach for a reason, return 0.0.\n\n"
            "Use only content present in CONTENT. Do not introduce names, claims, products, companies, or context from the subject.\n"
            "key_message should summarize the speaker's specific point, avoid direct quotes, and avoid biography, hype, and scoring details.\n"
            "If CONTENT is filler, ad read, transition, greeting, or low-content, set key_message to \"(low-content segment)\" and use a negative score_delta.\n"
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
            data = await self._post_generate(payload, schema=SINGLE_RERANK_RESPONSE_SCHEMA)
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
            data = await self._post_generate(payload)
            raw = data.get("response", "")
            parsed = parse_model_json_response(raw, context="preference extraction") or {}
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


def _extract_gemini_text(data: dict) -> str:
    candidates = data.get("candidates", []) if isinstance(data, dict) else []
    if not candidates or not isinstance(candidates[0], dict):
        return ""
    content = candidates[0].get("content", {})
    parts = content.get("parts", []) if isinstance(content, dict) else []
    texts = [str(part.get("text") or "") for part in parts if isinstance(part, dict)]
    return "\n".join(text for text in texts if text).strip()


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
