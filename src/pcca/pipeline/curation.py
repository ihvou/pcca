from __future__ import annotations

import re
from dataclasses import dataclass

from pcca.collectors.base import CollectedItem
from pcca.engagement import EngagementSignals


@dataclass
class ScoredItem:
    pass1_score: float
    pass2_score: float
    practicality_score: float
    novelty_score: float
    trust_score: float
    noise_penalty: float
    final_score: float
    rationale: str


@dataclass
class CurationEngine:
    practical_terms: tuple[str, ...] = (
        "workflow",
        "step",
        "implementation",
        "code",
        "release",
        "feature",
        "changelog",
        "example",
        "benchmark",
        "how to",
        # Russian/Ukrainian practical terms
        "релиз",
        "реліз",
        "обновлен",
        "оновлен",
        "фича",
        "функц",
        "пример",
        "приклад",
        "практич",
        "кейc",
        "кейс",
        "інструкц",
        "инструкц",
    )
    noise_terms: tuple[str, ...] = (
        "subscribe",
        "like and share",
        "giveaway",
        "bio",
        "my story",
        "beginner tips",
        "motivation",
        # Russian/Ukrainian noisy terms
        "подпиш",
        "лайк",
        "моя история",
        "моя історія",
        "мотивац",
        "биограф",
        "біограф",
    )

    def score(
        self,
        subject_name: str,
        item: CollectedItem,
        *,
        include_terms: list[str] | None = None,
        exclude_terms: list[str] | None = None,
        min_practicality: float | None = None,
    ) -> ScoredItem:
        text = (item.text or "").lower()
        if not text and item.transcript_text:
            text = item.transcript_text[:2000].lower()
        # Unicode-aware tokenization for English + Cyrillic (Ukrainian/Russian) and others.
        subject_tokens = [t for t in re.findall(r"[^\W_]+", subject_name.lower(), flags=re.UNICODE) if len(t) > 2]

        relevance_hits = sum(1 for token in subject_tokens if token in text)
        relevance = min(1.0, 0.2 + 0.2 * relevance_hits) if subject_tokens else 0.5

        practical_hits = sum(1 for term in self.practical_terms if term in text)
        practicality = min(1.0, practical_hits / 4.0)

        novelty = 0.8
        if any(term in text for term in ("introduction", "overview", "top 10", "beginner")):
            novelty = 0.35

        trust = 0.5
        if item.platform == "reddit":
            score = float(item.metadata.get("score", 0) or 0)
            if score >= 100:
                trust = 0.75
        if item.platform in {"x", "linkedin"} and item.author:
            trust += 0.1
        engagement = EngagementSignals.from_metadata(item.metadata)
        engagement_strength = engagement.strength()
        trust += min(0.15, engagement_strength * 0.10)
        novelty += min(0.10, engagement_strength * 0.07)
        if engagement.comments and engagement.comments >= 25:
            novelty += 0.05
        novelty = min(1.0, novelty)
        trust = min(1.0, trust)

        noise_hits = sum(1 for term in self.noise_terms if term in text)
        noise_penalty = min(1.0, noise_hits / 3.0)

        include_hits = 0
        if include_terms:
            include_hits = sum(1 for term in include_terms if term and term.lower() in text)
            relevance = min(1.0, relevance + min(0.35, include_hits * 0.12))

        exclude_hits = 0
        if exclude_terms:
            exclude_hits = sum(1 for term in exclude_terms if term and term.lower() in text)
            noise_penalty = min(1.0, noise_penalty + min(0.5, exclude_hits * 0.2))

        pass1_score = 0.6 * relevance + 0.4 * practicality
        pass2_score = 0.4 * relevance + 0.3 * practicality + 0.2 * novelty + 0.1 * trust
        final_score = (
            0.35 * relevance + 0.30 * practicality + 0.20 * novelty + 0.15 * trust - 0.20 * noise_penalty
        )
        if min_practicality is not None and practicality < min_practicality:
            # Preference guardrail: demote items that are likely too fluffy for this subject.
            final_score -= 0.2
        final_score = max(0.0, min(1.0, final_score))

        rationale = (
            f"relevance={relevance:.2f}, practicality={practicality:.2f}, "
            f"novelty={novelty:.2f}, trust={trust:.2f}, noise={noise_penalty:.2f}, "
            f"include_hits={include_hits}, exclude_hits={exclude_hits}, "
            f"engagement_strength={engagement_strength:.2f}, engagement={engagement.rationale_fragment()}"
        )
        return ScoredItem(
            pass1_score=pass1_score,
            pass2_score=pass2_score,
            practicality_score=practicality,
            novelty_score=novelty,
            trust_score=trust,
            noise_penalty=noise_penalty,
            final_score=final_score,
            rationale=rationale,
        )
