from __future__ import annotations

import re
from dataclasses import dataclass

from pcca.repositories.subject_drafts import SubjectDraft
from pcca.services.model_router import ModelRouter


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
        model_result = None
        if self.model_router is not None:
            model_result = await self.model_router.extract_subject_preferences(
                text=normalized,
                previous_title=previous.title if previous else None,
                previous_include_terms=previous.include_terms if previous else None,
                previous_exclude_terms=previous.exclude_terms if previous else None,
                previous_quality_notes=previous.quality_notes if previous else None,
            )
        if model_result is not None:
            return ExtractedSubjectDraft(
                title=model_result.title,
                description_text=self._merge_description(normalized, previous),
                include_terms=self._stable_unique(model_result.include_terms),
                exclude_terms=self._stable_unique(model_result.exclude_terms),
                quality_notes=model_result.quality_notes,
            )
        return self._heuristic_extract(normalized, previous=previous)

    def _heuristic_extract(self, text: str, *, previous: SubjectDraft | None) -> ExtractedSubjectDraft:
        title = self._extract_title(text) or (previous.title if previous else None) or self._title_from_text(text)
        include_terms = list(previous.include_terms if previous else [])
        exclude_terms = list(previous.exclude_terms if previous else [])

        explicit_include = self._terms_after_keyword(text, ("include", "focus on", "interested in"))
        explicit_exclude = self._terms_after_keyword(text, ("exclude", "avoid", "not interested in", "no "))
        include_terms.extend(explicit_include)
        exclude_terms.extend(explicit_exclude)

        if not explicit_include and not previous:
            include_terms.extend(self._terms_from_description(text))

        for dropped in self._terms_after_keyword(text, ("drop", "remove", "less")):
            exclude_terms.append(dropped)
            include_terms = [term for term in include_terms if dropped not in term and term not in dropped]

        quality_notes = previous.quality_notes if previous else None
        if re.search(r"\b(practical|concrete|actionable|high[- ]quality|good looks like)\b", text, re.IGNORECASE):
            quality_notes = text

        return ExtractedSubjectDraft(
            title=title,
            description_text=self._merge_description(text, previous),
            include_terms=self._stable_unique(include_terms)[:12],
            exclude_terms=self._stable_unique(exclude_terms)[:12],
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
