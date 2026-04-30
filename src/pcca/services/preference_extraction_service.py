from __future__ import annotations

import logging
import re
from dataclasses import dataclass

from pcca.repositories.subject_drafts import SubjectDraft
from pcca.services.model_router import ModelRouter

logger = logging.getLogger(__name__)


STOPWORDS = {
    "about",
    "and",
    "are",
    "for",
    "from",
    "in",
    "into",
    "new",
    "not",
    "only",
    "subject",
    "that",
    "the",
    "this",
    "topic",
    "useful",
    "want",
    "with",
}


@dataclass
class ExtractedSubjectDraft:
    title: str
    description_text: str
    include_terms: list[str]
    exclude_terms: list[str]
    quality_notes: str | None = None
    # Set to a short user-facing string when the LLM extraction path was
    # attempted (model_router.enabled=True) but failed — typically because
    # Ollama is not running on the configured base_url. Renderers (Telegram,
    # wizard) surface this so the user knows extraction was degraded rather
    # than thinking the bot ignored their input.
    extraction_warning: str | None = None


def draft_has_actionable_rules(draft: ExtractedSubjectDraft | SubjectDraft) -> bool:
    """Return True when a subject draft is specific enough to create a subject."""
    include_terms = [term for term in draft.include_terms if term.strip()]
    exclude_terms = [term for term in draft.exclude_terms if term.strip()]
    if not include_terms and not exclude_terms:
        return False
    if exclude_terms or draft.quality_notes:
        return True
    word_count = len(re.findall(r"[^\W_]+", draft.description_text or "", flags=re.UNICODE))
    # A bare "Create subject: Vibe Coding" produces title-shaped terms, but it
    # does not say what to keep, drop, or consider high quality yet.
    return word_count >= 8


@dataclass
class PreferenceExtractionService:
    model_router: ModelRouter | None = None

    async def extract(self, text: str, *, previous: SubjectDraft | None = None) -> ExtractedSubjectDraft:
        normalized = " ".join(text.split()).strip()
        prev_inc = len(previous.include_terms) if previous else 0
        prev_exc = len(previous.exclude_terms) if previous else 0
        logger.info(
            "Preference extraction entry text_chars=%d model_router=%s previous=%s prev_include=%d prev_exclude=%d",
            len(normalized),
            self.model_router is not None,
            previous is not None,
            prev_inc,
            prev_exc,
        )
        model_result = None
        llm_attempted = self.model_router is not None and getattr(self.model_router, "enabled", False)
        if self.model_router is not None:
            model_result = await self.model_router.extract_subject_preferences(
                text=normalized,
                previous_title=previous.title if previous else None,
                previous_include_terms=previous.include_terms if previous else None,
                previous_exclude_terms=previous.exclude_terms if previous else None,
                previous_quality_notes=previous.quality_notes if previous else None,
            )
        if model_result is not None:
            logger.info(
                "Preference extraction path=llm include=%d exclude=%d quality_notes=%s",
                len(model_result.include_terms),
                len(model_result.exclude_terms),
                bool(model_result.quality_notes),
            )
            return ExtractedSubjectDraft(
                title=model_result.title,
                description_text=self._merge_description(normalized, previous),
                include_terms=self._stable_unique(model_result.include_terms),
                exclude_terms=self._stable_unique(model_result.exclude_terms),
                quality_notes=model_result.quality_notes,
            )
        logger.info(
            "Preference extraction path=heuristic (model_router=%s, model_result=None, llm_attempted=%s)",
            self.model_router is not None,
            llm_attempted,
        )
        draft = self._heuristic_extract(normalized, previous=previous)
        if llm_attempted:
            # LLM path was enabled but produced no result (timeout, connection
            # refused, malformed JSON). Surface a degraded-mode hint so the
            # user knows why extraction is thinner than expected.
            draft.extraction_warning = (
                "LLM extraction (Ollama) was unreachable or timed out; "
                "using a basic keyword fallback. Start Ollama "
                "(brew services start ollama) for better results."
            )
        return draft

    def _heuristic_extract(self, text: str, *, previous: SubjectDraft | None) -> ExtractedSubjectDraft:
        title = self._extract_title(text) or (previous.title if previous else None) or self._title_from_text(text)
        include_terms = list(previous.include_terms if previous else [])
        exclude_terms = list(previous.exclude_terms if previous else [])

        explicit_include = self._terms_after_keyword(text, ("include", "focus on", "interested in"))
        explicit_exclude = self._terms_after_keyword(text, ("exclude", "avoid", "not interested in", "no "))
        include_terms.extend(explicit_include)
        exclude_terms.extend(explicit_exclude)
        logger.info(
            "Heuristic explicit-keyword pass: explicit_include=%d explicit_exclude=%d sample_inc=%s sample_exc=%s",
            len(explicit_include),
            len(explicit_exclude),
            explicit_include[:3],
            explicit_exclude[:3],
        )

        # Fallback fires when the user gave no explicit "include:" / "exclude:"
        # markers AND the prior draft has no usable rules. The previous
        # `not previous` guard incorrectly suppressed the fallback whenever
        # any draft existed, even one with empty rules — leaving the user
        # stuck in a "tell me more" loop with no extraction progress.
        previous_has_rules = bool(previous and (previous.include_terms or previous.exclude_terms))
        fallback_taken = not explicit_include and not previous_has_rules
        logger.info(
            "Heuristic description-fallback decision: explicit_include_empty=%s previous_has_rules=%s fallback_taken=%s",
            not explicit_include,
            previous_has_rules,
            fallback_taken,
        )
        if fallback_taken:
            description_terms = self._terms_from_description(text)
            logger.info("Heuristic description fallback added terms=%d sample=%s", len(description_terms), description_terms[:5])
            include_terms.extend(description_terms)

        for dropped in self._terms_after_keyword(text, ("drop", "remove", "less")):
            exclude_terms.append(dropped)
            include_terms = [term for term in include_terms if dropped not in term and term not in dropped]

        quality_notes = previous.quality_notes if previous else None
        if re.search(r"\b(practical|concrete|actionable|high[- ]quality|good looks like)\b", text, re.IGNORECASE):
            quality_notes = text

        final_inc = self._stable_unique(include_terms)[:12]
        final_exc = self._stable_unique(exclude_terms)[:12]
        logger.info(
            "Heuristic final: include=%d exclude=%d quality_notes=%s title=%s",
            len(final_inc),
            len(final_exc),
            bool(quality_notes),
            title,
        )
        return ExtractedSubjectDraft(
            title=title,
            description_text=self._merge_description(text, previous),
            include_terms=final_inc,
            exclude_terms=final_exc,
            quality_notes=quality_notes,
        )

    def _extract_title(self, text: str) -> str | None:
        by_colon = re.search(r"(?:subject|topic)\s*:\s*([^.;]+)", text, flags=re.IGNORECASE)
        if by_colon:
            return self._clean_title(by_colon.group(1))
        by_named = re.search(r"\b(?:called|named)\s+['\"]?([^'\"]{2,80})", text, flags=re.IGNORECASE)
        if by_named:
            return self._clean_title(by_named.group(1))
        by_quotes = re.search(r"['\"]([^'\"]{2,80})['\"]", text)
        if by_quotes:
            return self._clean_title(by_quotes.group(1))
        return None

    def _title_from_text(self, text: str) -> str:
        cleaned = re.sub(r"\b(create|new|subject|topic|i want|track|follow)\b", " ", text, flags=re.IGNORECASE)
        cleaned = re.split(r"\b(?:exclude|avoid|not interested in|no)\b", cleaned, maxsplit=1, flags=re.IGNORECASE)[0]
        words = []
        for word in re.findall(r"[^\W_]+", cleaned, flags=re.UNICODE):
            cleaned_word = word.strip("-_")
            if (
                (len(cleaned_word) > 2 or (len(cleaned_word) >= 2 and cleaned_word.isupper()))
                and cleaned_word.lower() not in STOPWORDS
            ):
                words.append(cleaned_word if cleaned_word.isupper() else cleaned_word.capitalize())
        return " ".join(words[:5]) or "New Subject"

    def _clean_title(self, value: str) -> str:
        value = re.split(r"\b(?:include|exclude|avoid|no|not interested in)\b", value, maxsplit=1, flags=re.IGNORECASE)[0]
        words = value.strip(" :'\".,").split()
        return " ".join(words[:6]) or "New Subject"

    def _terms_after_keyword(self, text: str, keywords: tuple[str, ...]) -> list[str]:
        terms: list[str] = []
        for keyword in keywords:
            keyword_pattern = re.escape(keyword).replace(r"\ ", r"\s+")
            match = re.search(
                rf"\b{keyword_pattern}\b\s*[:=]?\s*(.+?)(?=\b(?:include|exclude|avoid|drop|remove|less|focus on|not interested in|no )\b|$)",
                text,
                flags=re.IGNORECASE,
            )
            if not match:
                continue
            terms.extend(self._split_terms(match.group(1)))
        return terms

    def _terms_from_description(self, text: str) -> list[str]:
        text = re.split(r"\b(?:exclude|avoid|not interested in|no )\b", text, maxsplit=1, flags=re.IGNORECASE)[0]
        pieces = self._split_terms(text)
        if len(pieces) > 1:
            return pieces
        words = [
            word.lower()
            for word in re.findall(r"[^\W_]+", text, flags=re.UNICODE)
            if len(word) > 2 and word.lower() not in STOPWORDS
        ]
        chunks: list[str] = []
        for size in (3, 2):
            if len(words) >= size:
                chunks.append(" ".join(words[:size]))
                break
        chunks.extend(words[:5])
        return chunks

    def _split_terms(self, payload: str) -> list[str]:
        raw_parts = re.split(r",|;|/|\band\b|\n", payload, flags=re.IGNORECASE)
        out: list[str] = []
        for part in raw_parts:
            normalized = part.strip().strip("\"'. ").lower()
            normalized = re.sub(r"^(that|the|a|an|to|about)\s+", "", normalized)
            if normalized and normalized not in STOPWORDS:
                out.append(normalized)
        return out

    def _merge_description(self, text: str, previous: SubjectDraft | None) -> str:
        if previous is None:
            return text
        return f"{previous.description_text}\n{text}".strip()

    def _stable_unique(self, terms: list[str]) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        for term in terms:
            normalized = " ".join(term.lower().split()).strip(" ,.;:")
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            out.append(normalized)
        return out
