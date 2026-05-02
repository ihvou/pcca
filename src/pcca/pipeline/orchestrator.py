from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from pcca.collectors.base import CollectedItem, Collector
from pcca.collectors.errors import SessionChallengedError, SourceNotFoundError
from pcca.repositories.items import ItemRepository
from pcca.repositories.item_segments import ItemSegment, ItemSegmentRepository
from pcca.repositories.item_scores import ItemScoreRepository
from pcca.repositories.run_logs import RunLogRepository
from pcca.repositories.runtime_locks import RuntimeLockRepository
from pcca.pipeline.curation import CurationEngine
from pcca.services.embedding_service import EmbeddingService, cosine_similarity
from pcca.services.model_router import ModelRerankCandidate, ModelRouter
from pcca.services.preference_service import PreferenceService
from pcca.services.session_capture_service import SessionRefreshService
from pcca.services.source_service import SourceService
from pcca.services.subject_service import SubjectService

logger = logging.getLogger(__name__)


@dataclass
class PreferenceContext:
    include_terms: list[str]
    exclude_terms: list[str]
    min_practicality: float | None
    shortlist_limit: int
    description_text: str | None = None
    quality_notes: str | None = None


@dataclass
class PipelineOrchestrator:
    subject_service: SubjectService
    source_service: SourceService
    item_repo: ItemRepository
    item_score_repo: ItemScoreRepository
    run_log_repo: RunLogRepository
    item_segment_repo: ItemSegmentRepository | None = None
    preference_service: PreferenceService | None = None
    curation_engine: CurationEngine = field(default_factory=CurationEngine)
    model_router: ModelRouter | None = None
    embedding_service: EmbeddingService | None = None
    session_refresh_service: SessionRefreshService | None = None
    collectors: dict[str, Collector] = field(default_factory=dict)
    scorer: str = "keyword"
    circuit_threshold: int | None = None
    empty_threshold: int | None = None
    runtime_lock_repo: RuntimeLockRepository | None = None
    collection_lock_ttl_seconds: int = 6 * 60 * 60
    auto_backfill_embeddings: bool = True
    embedding_backfill_concurrency: int = 2

    def __post_init__(self) -> None:
        if self.runtime_lock_repo is None:
            self.runtime_lock_repo = RuntimeLockRepository(conn=self.run_log_repo.conn)
        if self.item_segment_repo is None:
            self.item_segment_repo = ItemSegmentRepository(conn=self.run_log_repo.conn)

    def _scorer_mode(self) -> str:
        mode = (self.scorer or "keyword").strip().lower()
        return mode if mode in {"keyword", "embedding", "both"} else "keyword"

    def _embedding_enabled(self) -> bool:
        return bool(
            self.embedding_service is not None
            and self.embedding_service.enabled
            and self._scorer_mode() in {"embedding", "both"}
        )

    async def _preference_context(self, subject_id: int) -> PreferenceContext:
        include_terms: list[str] = []
        exclude_terms: list[str] = []
        min_practicality: float | None = None
        shortlist_limit = 20
        description_text = None
        quality_notes = None
        description_getter = getattr(self.subject_service.repository, "get_description_text", None)
        if callable(description_getter):
            description_text = await description_getter(subject_id)
        if self.preference_service is None:
            return PreferenceContext(include_terms, exclude_terms, min_practicality, shortlist_limit, description_text)

        pref = await self.preference_service.get_preferences_by_subject_id(subject_id)
        include_terms = [
            t for t in pref.include_rules.get("topics", []) if isinstance(t, str) and t.strip()
        ]
        exclude_terms = [
            t for t in pref.exclude_rules.get("topics", []) if isinstance(t, str) and t.strip()
        ]
        if isinstance(pref.quality_rules.get("min_practicality"), (float, int)):
            min_practicality = float(pref.quality_rules["min_practicality"])
        if isinstance(pref.quality_rules.get("model_shortlist_limit"), (float, int)):
            shortlist_limit = max(0, int(pref.quality_rules["model_shortlist_limit"]))
        if isinstance(pref.quality_rules.get("notes"), str):
            quality_notes = pref.quality_rules["notes"].strip() or None
        return PreferenceContext(
            include_terms=include_terms,
            exclude_terms=exclude_terms,
            min_practicality=min_practicality,
            shortlist_limit=shortlist_limit,
            description_text=description_text,
            quality_notes=quality_notes,
        )

    def _subject_embedding_text(self, *, subject, context: PreferenceContext) -> str:
        if context.description_text:
            # The natural-language description is the primary semantic signal.
            # Legacy include/exclude terms often contain quality labels, so do not
            # mix them into the embedding unless no description exists.
            parts = [
                f"Subject: {subject.name}",
                context.description_text,
                f"Quality: {context.quality_notes}" if context.quality_notes else "",
            ]
        else:
            parts = [
                f"Subject: {subject.name}",
                f"Include: {', '.join(context.include_terms)}" if context.include_terms else "",
                f"Avoid: {', '.join(context.exclude_terms)}" if context.exclude_terms else "",
                f"Quality: {context.quality_notes}" if context.quality_notes else "",
            ]
        return "\n".join(part for part in parts if part.strip())

    async def _get_or_create_subject_embedding(self, *, subject_id: int, text: str) -> list[float] | None:
        if self.embedding_service is None:
            return None
        model = self.embedding_service.embedding_model
        text_hash = self.subject_service.repository.embedding_text_hash(text)
        existing = await self.subject_service.repository.get_description_embedding_for_text(
            subject_id,
            model=model,
            text_hash=text_hash,
        )
        if existing is not None:
            return existing
        embedding = await self.embedding_service.embed(text)
        if embedding is not None:
            await self.subject_service.repository.save_description_embedding(
                subject_id,
                model=model,
                embedding=embedding,
                text_hash=text_hash,
            )
        return embedding

    async def _get_or_create_item_embedding(self, *, item_id: int, item: CollectedItem) -> list[float] | None:
        if self.embedding_service is None:
            return None
        model = self.embedding_service.embedding_model
        text = self.item_repo.embedding_text(item)
        text_hash = self.item_repo.embedding_text_hash(text)
        existing = await self.item_repo.get_content_embedding_for_text(
            item_id,
            model=model,
            text_hash=text_hash,
        )
        if existing is not None:
            return existing
        embedding = await self.embedding_service.embed(text)
        if embedding is not None:
            await self.item_repo.save_content_embedding(
                item_id,
                model=model,
                embedding=embedding,
                text_hash=text_hash,
            )
        return embedding

    async def _get_or_create_segment_embedding(self, *, segment: ItemSegment) -> list[float] | None:
        if self.embedding_service is None or self.item_segment_repo is None:
            return None
        model = self.embedding_service.embedding_model
        text = self.item_segment_repo.embedding_text(segment)
        text_hash = self.item_segment_repo.embedding_text_hash(text)
        existing = await self.item_segment_repo.get_embedding_for_text(
            segment.id,
            model=model,
            text_hash=text_hash,
        )
        if existing is not None:
            return existing
        embedding = await self.embedding_service.embed(text)
        if embedding is not None:
            await self.item_segment_repo.save_embedding(
                segment.id,
                model=model,
                embedding=embedding,
                text_hash=text_hash,
            )
        return embedding

    @staticmethod
    def _segment_scoring_item(item: CollectedItem, segment: ItemSegment) -> CollectedItem:
        metadata = item.metadata if isinstance(item.metadata, dict) else {}
        title = str(metadata.get("title") or "").strip()
        if not title and item.text:
            title = item.text.splitlines()[0].strip()
        text = "\n\n".join(part for part in (title, segment.text) if part).strip()
        return CollectedItem(
            platform=item.platform,
            external_id=f"{item.external_id}#segment-{segment.id}",
            author=item.author,
            url=item.url,
            text=text,
            transcript_text=segment.text,
            published_at=item.published_at,
            metadata={
                **metadata,
                "pcca_segment_id": segment.id,
                "pcca_segment_start_seconds": segment.start_offset_seconds,
                "pcca_segment_end_seconds": segment.end_offset_seconds,
            },
        )

    def _record_embedding_degradation(
        self,
        *,
        run_id: int,
        subject,
        stats: dict,
        fallback_before: int,
        scored_count: int,
        subject_embedding_available: bool,
    ) -> None:
        if not self._embedding_enabled() or scored_count <= 0:
            return
        fallback_count = int(stats.get("embedding_fallback_items", 0)) - fallback_before
        fallback_rate = fallback_count / scored_count if scored_count else 0.0
        degraded = (not subject_embedding_available) or fallback_rate >= 0.5
        if not degraded:
            return
        reason = "subject_embedding_unavailable" if not subject_embedding_available else "item_embedding_fallback_rate"
        detail = {
            "subject_id": subject.id,
            "subject_name": subject.name,
            "candidate_items": scored_count,
            "fallback_items": fallback_count,
            "fallback_rate": round(fallback_rate, 3),
            "reason": reason,
        }
        stats["embedding_degraded"] = True
        stats.setdefault("embedding_degraded_subjects", []).append(detail)
        logger.warning(
            "Embedding scoring degraded run_id=%s subject=%s reason=%s fallback_items=%d candidate_items=%d fallback_rate=%.3f",
            run_id,
            subject.name,
            reason,
            fallback_count,
            scored_count,
            fallback_rate,
        )

    @staticmethod
    def _emit_progress(
        progress_callback: Callable[[dict[str, Any]], None] | None,
        event: dict[str, Any],
    ) -> None:
        if progress_callback is None:
            return
        try:
            progress_callback(event)
        except Exception:
            logger.exception("Pipeline progress callback failed event=%s", event)

    def _not_found_recovery_candidates(self, source) -> list[str]:
        metadata = source.metadata if isinstance(getattr(source, "metadata", None), dict) else {}
        candidates: list[str] = []
        for key in ("original_handle", "resolved_from", "raw_source", "source_url"):
            value = metadata.get(key)
            if isinstance(value, str) and value.strip():
                candidates.append(value.strip())
        current = str(source.account_or_channel_id or "").strip()
        if current and not current.startswith("UC"):
            candidates.append(current)
        out: list[str] = []
        seen: set[str] = set()
        for candidate in candidates:
            if candidate not in seen:
                seen.add(candidate)
                out.append(candidate)
        return out

    async def _attempt_not_found_recovery(
        self,
        *,
        run_id: int,
        source,
        collector,
        exc: SourceNotFoundError,
        stats: dict,
        metadata: dict,
    ) -> bool:
        resolver = getattr(collector, "resolve_source_identifier", None)
        if source.platform != "youtube" or not callable(resolver):
            return False
        old_identifier = source.account_or_channel_id
        for candidate in self._not_found_recovery_candidates(source):
            try:
                resolved_identifier = await resolver(candidate)
            except Exception:
                logger.info(
                    "Source not-found re-resolution failed run_id=%s source_id=%s platform=%s candidate=%s",
                    run_id,
                    source.source_id,
                    source.platform,
                    candidate,
                    exc_info=True,
                )
                continue
            if (
                not isinstance(resolved_identifier, str)
                or not resolved_identifier.strip()
                or resolved_identifier == old_identifier
            ):
                continue
            resolved_identifier = resolved_identifier.strip()
            await self.source_service.merge_source_metadata(
                source_id=source.source_id,
                values={
                    "not_found_recovered_from": old_identifier,
                    "not_found_recovered_via": candidate,
                    "not_found_recovered_url": exc.current_url,
                    "resolved_channel_id": resolved_identifier,
                },
            )
            updated_source = await self.source_service.update_source_identifier(
                source_id=source.source_id,
                account_or_channel_id=resolved_identifier,
            )
            source.account_or_channel_id = resolved_identifier
            if updated_source is not None:
                source.source_id = updated_source.source_id
            stats["sources_reresolved"] = int(stats.get("sources_reresolved", 0)) + 1
            recovered = {
                "source_id": source.source_id,
                "platform": source.platform,
                "old_identifier": old_identifier,
                "new_identifier": resolved_identifier,
                "candidate": candidate,
            }
            metadata.setdefault("sources_reresolved", []).append(recovered)
            logger.info(
                "Recovered source after not-found run_id=%s source_id=%s platform=%s old=%s new=%s candidate=%s",
                run_id,
                source.source_id,
                source.platform,
                old_identifier,
                resolved_identifier,
                candidate,
            )
            return True
        return False

    async def _mark_source_not_found(
        self,
        *,
        run_id: int,
        source,
        exc: SourceNotFoundError,
        stats: dict,
        metadata: dict,
    ) -> None:
        stats["sources_not_found"] = int(stats.get("sources_not_found", 0)) + 1
        detail = {
            "source_id": source.source_id,
            "platform": source.platform,
            "identifier": source.account_or_channel_id,
            "display_name": source.display_name,
            "kind": exc.not_found_kind,
            "url": exc.current_url,
            "status_code": exc.status_code,
        }
        metadata.setdefault("sources_not_found", []).append(detail)
        await self.source_service.mark_source_inactive(
            source.source_id,
            reason="channel_not_found" if source.platform == "youtube" else "source_not_found",
            details=detail,
        )
        logger.warning(
            "Source marked inactive after not-found run_id=%s source_id=%s platform=%s identifier=%s kind=%s url=%s",
            run_id,
            source.source_id,
            source.platform,
            source.account_or_channel_id,
            exc.not_found_kind,
            exc.current_url,
        )

    async def _embedding_cold_cache_detail(
        self,
        *,
        subject,
        item_rows: list[tuple[int, CollectedItem]],
        sample_limit: int = 100,
    ) -> dict[str, Any] | None:
        if not self._embedding_enabled() or self.embedding_service is None:
            return None
        model = self.embedding_service.embedding_model
        if not model:
            return None
        checked = 0
        missing = 0
        for item_id, item in item_rows[: max(1, int(sample_limit))]:
            text = self.item_repo.embedding_text(item)
            if not text.strip():
                continue
            checked += 1
            text_hash = self.item_repo.embedding_text_hash(text)
            existing = await self.item_repo.get_content_embedding_for_text(
                item_id,
                model=model,
                text_hash=text_hash,
            )
            if existing is None:
                missing += 1
        missing_rate = missing / checked if checked else 0.0
        if checked < 20 or missing_rate <= 0.5:
            return None
        return {
            "subject_id": subject.id,
            "subject_name": subject.name,
            "sampled_items": checked,
            "missing_item_embeddings": missing,
            "missing_rate": round(missing_rate, 3),
            "reason": "embedding_cache_not_warmed",
        }

    @staticmethod
    def _copy_embedding_degradation_metadata(stats: dict, metadata: dict) -> None:
        metadata["embedding_degraded"] = bool(stats.get("embedding_degraded"))
        metadata["embedding_degraded_subjects"] = list(stats.get("embedding_degraded_subjects") or [])
        metadata["embedding_fallback_items"] = int(stats.get("embedding_fallback_items") or 0)
        metadata["embedding_items_scored"] = int(stats.get("embedding_items_scored") or 0)
        metadata["embedding_not_warmed"] = bool(stats.get("embedding_not_warmed"))
        metadata["embedding_not_warmed_subjects"] = list(stats.get("embedding_not_warmed_subjects") or [])

    async def _score_items_for_subject(
        self,
        *,
        run_id: int,
        subject,
        item_rows: list[tuple[int, CollectedItem]],
        inactive_source_ids: set[int],
        stats: dict,
    ) -> None:
        if not item_rows:
            return
        context = await self._preference_context(subject.id)
        subject_embedding_text = self._subject_embedding_text(subject=subject, context=context)
        cold_cache_detail = await self._embedding_cold_cache_detail(subject=subject, item_rows=item_rows)
        if cold_cache_detail is not None:
            stats["embedding_not_warmed"] = True
            stats.setdefault("embedding_not_warmed_subjects", []).append(cold_cache_detail)
            logger.warning(
                "Embedding cache is not warmed enough for scoring; using keyword fallback run_id=%s subject=%s sampled=%d missing=%d missing_rate=%.3f",
                run_id,
                subject.name,
                cold_cache_detail["sampled_items"],
                cold_cache_detail["missing_item_embeddings"],
                cold_cache_detail["missing_rate"],
            )
        subject_embedding = (
            await self._get_or_create_subject_embedding(subject_id=subject.id, text=subject_embedding_text)
            if self._embedding_enabled() and cold_cache_detail is None
            else None
        )
        fallback_before = int(stats.get("embedding_fallback_items", 0))
        scored_rows: list[tuple[int, CollectedItem, Any, bool, ItemSegment | None]] = []
        segment_score_rows: list[tuple[int, int, ItemSegment, Any]] = []
        scored_segment_count = 0
        for item_id, item in item_rows:
            source_id = item.metadata.get("pcca_source_id") if isinstance(item.metadata, dict) else None
            if source_id is not None:
                try:
                    if int(source_id) in inactive_source_ids:
                        stats["items_skipped_subject_source_override"] += 1
                        continue
                except (TypeError, ValueError):
                    logger.warning(
                        "Invalid item source metadata run_id=%s subject=%s item_id=%s pcca_source_id=%r",
                        run_id,
                        subject.name,
                        item_id,
                        source_id,
                    )

            segments = (
                await self.item_segment_repo.ensure_segments(item_id=item_id, item=item)
                if self.item_segment_repo is not None
                else []
            )
            best_row: tuple[int, CollectedItem, Any, bool, ItemSegment | None] | None = None
            if segments:
                stats["segments_seen"] = int(stats.get("segments_seen", 0)) + len(segments)
            for segment in segments:
                scored_segment_count += 1
                segment_item = self._segment_scoring_item(item, segment)
                keyword_scored = self.curation_engine.score(
                    subject.name,
                    segment_item,
                    include_terms=context.include_terms,
                    exclude_terms=context.exclude_terms,
                    min_practicality=context.min_practicality,
                )
                semantic_similarity = None
                if subject_embedding is not None:
                    segment_embedding = await self._get_or_create_segment_embedding(segment=segment)
                    if segment_embedding is not None:
                        semantic_similarity = cosine_similarity(subject_embedding, segment_embedding)
                if semantic_similarity is not None:
                    scored = self.curation_engine.score(
                        subject.name,
                        segment_item,
                        include_terms=context.include_terms,
                        exclude_terms=context.exclude_terms,
                        min_practicality=context.min_practicality,
                        semantic_similarity=semantic_similarity,
                    )
                    scored.rationale = (
                        f"{scored.rationale}; scorer=embedding; segment_id={segment.id}; "
                        f"keyword_shadow_final={keyword_scored.final_score:.3f}"
                    )
                    stats["embedding_items_scored"] += 1
                    stats["segments_scored_with_embedding"] = int(stats.get("segments_scored_with_embedding", 0)) + 1
                    if self._scorer_mode() == "both":
                        stats["keyword_shadow_items_scored"] += 1
                else:
                    if self._embedding_enabled():
                        stats["embedding_fallback_items"] += 1
                    scored = keyword_scored
                    scored.rationale = f"{scored.rationale}; scorer=keyword; segment_id={segment.id}"
                segment_score_rows.append((item_id, subject.id, segment, scored))
                row = (item_id, segment_item, scored, semantic_similarity is not None, segment)
                if best_row is None or scored.final_score > best_row[2].final_score:
                    best_row = row

            if best_row is None:
                keyword_scored = self.curation_engine.score(
                    subject.name,
                    item,
                    include_terms=context.include_terms,
                    exclude_terms=context.exclude_terms,
                    min_practicality=context.min_practicality,
                )
                semantic_similarity = None
                if subject_embedding is not None:
                    item_embedding = await self._get_or_create_item_embedding(item_id=item_id, item=item)
                    if item_embedding is not None:
                        semantic_similarity = cosine_similarity(subject_embedding, item_embedding)
                if semantic_similarity is not None:
                    scored = self.curation_engine.score(
                        subject.name,
                        item,
                        include_terms=context.include_terms,
                        exclude_terms=context.exclude_terms,
                        min_practicality=context.min_practicality,
                        semantic_similarity=semantic_similarity,
                    )
                    scored.rationale = f"{scored.rationale}; scorer=embedding; keyword_shadow_final={keyword_scored.final_score:.3f}"
                    stats["embedding_items_scored"] += 1
                    if self._scorer_mode() == "both":
                        stats["keyword_shadow_items_scored"] += 1
                    scored_rows.append((item_id, item, scored, True, None))
                else:
                    if self._embedding_enabled():
                        stats["embedding_fallback_items"] += 1
                    scored_rows.append((item_id, item, keyword_scored, False, None))
            else:
                scored_rows.append(best_row)

        self._record_embedding_degradation(
            run_id=run_id,
            subject=subject,
            stats=stats,
            fallback_before=fallback_before,
            scored_count=scored_segment_count or len(scored_rows),
            subject_embedding_available=subject_embedding is not None,
        )
        shortlist_rows = sorted(scored_rows, key=lambda row: row[2].final_score, reverse=True)[: context.shortlist_limit]
        shortlist_ids = {item_id for item_id, _item, _scored, _used_embedding, _segment in shortlist_rows}
        stats["items_score_candidates"] += len(scored_rows)
        stats["segments_score_candidates"] = int(stats.get("segments_score_candidates", 0)) + scored_segment_count
        stats["model_shortlist_items"] += len(shortlist_ids)
        logger.info(
            "Scoring subject run_id=%s subject=%s items=%d segments=%d shortlist=%d include_terms=%d exclude_terms=%d scorer=%s embedding_enabled=%s",
            run_id,
            subject.name,
            len(scored_rows),
            scored_segment_count,
            len(shortlist_ids),
            len(context.include_terms),
            len(context.exclude_terms),
            self._scorer_mode(),
            bool(subject_embedding is not None),
        )

        batch_results = {}
        batch_attempted = False
        batch_rerank = getattr(self.model_router, "rerank_batch", None) if self.model_router is not None else None
        shortlist_used_embedding = any(used_embedding for _item_id, _item, _scored, used_embedding, _segment in shortlist_rows)
        if callable(batch_rerank) and shortlist_used_embedding and getattr(self.model_router, "enabled", True):
            batch_attempted = True
            candidates = [
                ModelRerankCandidate(
                    item_id=item_id,
                    text=item.text or item.transcript_text or "",
                    heuristic_score=scored.final_score,
                    author=item.author,
                    url=item.url,
                    published_at=item.published_at,
                )
                for item_id, item, scored, _used_embedding, _segment in shortlist_rows
            ]
            batch_results = await batch_rerank(
                subject_name=subject.name,
                subject_description=subject_embedding_text,
                candidates=candidates,
            )
            stats["model_batch_rerank_calls"] += 1

        adjusted_segment_scores: dict[int, Any] = {}
        item_key_messages: dict[int, str] = {}
        for item_id, item, scored, _used_embedding, segment in scored_rows:
            rerank = batch_results.get(item_id) if isinstance(batch_results, dict) else None
            if rerank is not None:
                adjusted_final = max(0.0, min(1.0, scored.final_score + rerank.score_delta))
                scored.final_score = adjusted_final
                scored.rationale = f"{scored.rationale}; model_batch={rerank.rationale}"
                key_message = getattr(rerank, "key_message", None)
                if key_message:
                    item_key_messages[item_id] = str(key_message)
                stats["items_model_reranked"] += 1
            elif self.model_router is not None and item_id in shortlist_ids and not batch_attempted:
                rerank = await self.model_router.rerank(
                    subject_name=subject.name,
                    text=item.text or item.transcript_text or "",
                    heuristic_score=scored.final_score,
                )
                if rerank is not None:
                    adjusted_final = max(0.0, min(1.0, scored.final_score + rerank.score_delta))
                    scored.final_score = adjusted_final
                    scored.rationale = f"{scored.rationale}; model={rerank.rationale}"
                    key_message = getattr(rerank, "key_message", None)
                    if key_message:
                        item_key_messages[item_id] = str(key_message)
                    stats["items_model_reranked"] += 1
            if segment is not None:
                adjusted_segment_scores[segment.id] = scored
            await self.item_score_repo.upsert_score(
                item_id=item_id,
                subject_id=subject.id,
                pass1_score=scored.pass1_score,
                pass2_score=scored.pass2_score,
                practicality_score=scored.practicality_score,
                novelty_score=scored.novelty_score,
                trust_score=scored.trust_score,
                noise_penalty=scored.noise_penalty,
                final_score=scored.final_score,
                rationale=scored.rationale,
                key_message=item_key_messages.get(item_id),
            )
            stats["items_scored"] += 1

        for item_id, subject_id, segment, scored in segment_score_rows:
            effective_scored = adjusted_segment_scores.get(segment.id, scored)
            await self.item_score_repo.upsert_segment_score(
                segment_id=segment.id,
                item_id=item_id,
                subject_id=subject_id,
                pass1_score=effective_scored.pass1_score,
                pass2_score=effective_scored.pass2_score,
                practicality_score=effective_scored.practicality_score,
                novelty_score=effective_scored.novelty_score,
                trust_score=effective_scored.trust_score,
                noise_penalty=effective_scored.noise_penalty,
                final_score=effective_scored.final_score,
                rationale=effective_scored.rationale,
                key_message=item_key_messages.get(item_id),
            )
            stats["segments_scored"] = int(stats.get("segments_scored", 0)) + 1

    async def backfill_embeddings(
        self,
        *,
        concurrency: int = 4,
        limit: int | None = None,
        include_segments: bool = False,
        item_ids: set[int] | None = None,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict:
        model = self.embedding_service.embedding_model if self.embedding_service is not None else None
        stats: dict[str, Any] = {
            "enabled": self._embedding_enabled(),
            "model": model,
            "concurrency": max(1, int(concurrency)),
            "limit": limit,
            "subjects_total": 0,
            "subjects_embedded": 0,
            "subjects_skipped": 0,
            "subjects_failed": 0,
            "items_total": 0,
            "items_embedded": 0,
            "items_skipped": 0,
            "items_failed": 0,
            "include_segments": include_segments,
            "item_ids_scoped": len(item_ids or []),
            "segments_total": 0,
            "segments_embedded": 0,
            "segments_skipped": 0,
            "segments_failed": 0,
            "segments_prepared": 0,
        }
        if not self._embedding_enabled() or self.embedding_service is None or model is None:
            logger.warning("Embedding backfill skipped because embedding scorer is disabled.")
            return stats

        def emit(kind: str, processed: int, total: int) -> None:
            event = {"kind": kind, "processed": processed, "total": total, "stats": dict(stats)}
            logger.info("Embedding backfill progress kind=%s processed=%d total=%d", kind, processed, total)
            if progress_callback is None:
                return
            try:
                progress_callback(event)
            except Exception:
                logger.exception("Embedding backfill progress callback failed kind=%s", kind)

        subjects = await self.subject_service.list_subjects()
        stats["subjects_total"] = len(subjects)
        for processed, subject in enumerate(subjects, start=1):
            context = await self._preference_context(subject.id)
            text = self._subject_embedding_text(subject=subject, context=context)
            text_hash = self.subject_service.repository.embedding_text_hash(text)
            existing = await self.subject_service.repository.get_description_embedding_for_text(
                subject.id,
                model=model,
                text_hash=text_hash,
            )
            if existing is not None:
                stats["subjects_skipped"] += 1
            else:
                embedding = await self.embedding_service.embed(text)
                if embedding is None:
                    stats["subjects_failed"] += 1
                else:
                    await self.subject_service.repository.save_description_embedding(
                        subject.id,
                        model=model,
                        embedding=embedding,
                        text_hash=text_hash,
                    )
                    stats["subjects_embedded"] += 1
            emit("subjects", processed, len(subjects))

        semaphore = asyncio.Semaphore(max(1, int(concurrency)))
        if item_ids is not None:
            rows = await self.item_repo.list_by_ids_for_scoring(item_ids)
            if limit is not None and limit > 0:
                rows = rows[: int(limit)]
            target_count = len(rows)
            stats["items_total"] = target_count
        else:
            missing_count = await self.item_repo.count_missing_embeddings(model=model)
            target_count = min(missing_count, int(limit)) if limit is not None and limit > 0 else missing_count
            stats["items_total"] = target_count
            if target_count <= 0:
                emit("items", 0, 0)
                rows = []
            else:
                rows = await self.item_repo.list_missing_embeddings(model=model, limit=target_count)

        async def embed_item(item_id: int, item: CollectedItem) -> str:
            async with semaphore:
                text = self.item_repo.embedding_text(item)
                text_hash = self.item_repo.embedding_text_hash(text)
                existing = await self.item_repo.get_content_embedding_for_text(
                    item_id,
                    model=model,
                    text_hash=text_hash,
                )
                if existing is not None:
                    return "skipped"
                embedding = await self.embedding_service.embed(text)
                if embedding is None:
                    return "failed"
                await self.item_repo.save_content_embedding(
                    item_id,
                    model=model,
                    embedding=embedding,
                    text_hash=text_hash,
                )
                return "embedded"

        processed_items = 0
        batch_size = 50
        for batch_start in range(0, len(rows), batch_size):
            batch = rows[batch_start : batch_start + batch_size]
            results = await asyncio.gather(*(embed_item(item_id, item) for item_id, item in batch))
            for result in results:
                if result == "embedded":
                    stats["items_embedded"] += 1
                elif result == "skipped":
                    stats["items_skipped"] += 1
                else:
                    stats["items_failed"] += 1
            processed_items += len(batch)
            emit("items", processed_items, target_count)

        if include_segments and self.item_segment_repo is not None:
            item_rows = rows if item_ids is not None else await self.item_repo.list_all_for_scoring(limit=limit)
            for item_id, item in item_rows:
                segments = await self.item_segment_repo.ensure_segments(item_id=item_id, item=item)
                stats["segments_prepared"] += len(segments)
            if item_ids is not None:
                segment_rows = []
                for item_id, _item in item_rows:
                    segment_rows.extend(await self.item_segment_repo.list_for_item(item_id=item_id))
                target_segments = len(segment_rows)
            else:
                missing_segments = await self.item_segment_repo.count_missing_embeddings(model=model)
                target_segments = (
                    min(missing_segments, int(limit)) if limit is not None and limit > 0 else missing_segments
                )
                segment_rows = await self.item_segment_repo.list_missing_embeddings(model=model, limit=target_segments) if target_segments else []
            stats["segments_total"] = target_segments

            async def embed_segment(segment: ItemSegment) -> str:
                async with semaphore:
                    text = self.item_segment_repo.embedding_text(segment)
                    text_hash = self.item_segment_repo.embedding_text_hash(text)
                    existing = await self.item_segment_repo.get_embedding_for_text(
                        segment.id,
                        model=model,
                        text_hash=text_hash,
                    )
                    if existing is not None:
                        return "skipped"
                    embedding = await self.embedding_service.embed(text)
                    if embedding is None:
                        return "failed"
                    await self.item_segment_repo.save_embedding(
                        segment.id,
                        model=model,
                        embedding=embedding,
                        text_hash=text_hash,
                    )
                    return "embedded"

            processed_segments = 0
            for batch_start in range(0, len(segment_rows), batch_size):
                batch = segment_rows[batch_start : batch_start + batch_size]
                results = await asyncio.gather(*(embed_segment(segment) for segment in batch))
                for result in results:
                    if result == "embedded":
                        stats["segments_embedded"] += 1
                    elif result == "skipped":
                        stats["segments_skipped"] += 1
                    else:
                        stats["segments_failed"] += 1
                processed_segments += len(batch)
                emit("segments", processed_segments, target_segments)
        return stats

    def _empty_scoring_stats(self) -> dict[str, Any]:
        return {
            "subjects_seen": 0,
            "items_seen": 0,
            "items_score_candidates": 0,
            "model_shortlist_items": 0,
            "items_model_reranked": 0,
            "model_batch_rerank_calls": 0,
            "embedding_items_scored": 0,
            "embedding_fallback_items": 0,
            "embedding_degraded": False,
            "embedding_degraded_subjects": [],
            "embedding_not_warmed": False,
            "embedding_not_warmed_subjects": [],
            "keyword_shadow_items_scored": 0,
            "items_skipped_subject_source_override": 0,
            "items_scored": 0,
            "segments_seen": 0,
            "segments_score_candidates": 0,
            "segments_scored": 0,
            "segments_scored_with_embedding": 0,
        }

    async def rescore_existing_items(
        self,
        *,
        limit: int | None = None,
        subject_ids: set[int] | None = None,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict:
        run_id = await self.run_log_repo.start_run("embedding_rescore")
        run_started_at = time.monotonic()
        stats = self._empty_scoring_stats()
        metadata: dict[str, Any] = {
            "limit": limit,
            "subject_ids": sorted(subject_ids) if subject_ids is not None else None,
        }
        try:
            subjects = await self.subject_service.list_subjects()
            if subject_ids is not None:
                subjects = [subject for subject in subjects if subject.id in subject_ids]
            item_rows = await self.item_repo.list_all_for_scoring(limit=limit)
            stats["subjects_seen"] = len(subjects)
            stats["items_seen"] = len(item_rows)
            logger.info(
                "Embedding rescore started run_id=%s subjects=%d items=%d scorer=%s",
                run_id,
                len(subjects),
                len(item_rows),
                self._scorer_mode(),
            )
            metadata["scoring_subjects"] = [subject.name for subject in subjects]
            for subject_index, subject in enumerate(subjects, start=1):
                event = {
                    "kind": "scoring",
                    "phase": "scoring",
                    "run_id": run_id,
                    "run_type": "embedding_rescore",
                    "subject_index": subject_index,
                    "subject_total": len(subjects),
                    "subject_id": subject.id,
                    "subject_name": subject.name,
                }
                stats.setdefault("scoring_progress_events", []).append(event)
                logger.info(
                    "Scoring progress run_id=%s run_type=embedding_rescore subject=%s subject_index=%d subject_total=%d",
                    run_id,
                    subject.name,
                    subject_index,
                    len(subjects),
                )
                self._emit_progress(progress_callback, event)
                inactive_source_ids = await self.source_service.list_inactive_source_ids_for_subject(subject.id)
                await self._score_items_for_subject(
                    run_id=run_id,
                    subject=subject,
                    item_rows=item_rows,
                    inactive_source_ids=inactive_source_ids,
                    stats=stats,
                )
            self._copy_embedding_degradation_metadata(stats, metadata)
            await self.run_log_repo.finish_run(run_id, status="success", stats=stats, metadata=metadata)
            logger.info(
                "Embedding rescore succeeded run_id=%s duration_ms=%d stats=%s",
                run_id,
                int((time.monotonic() - run_started_at) * 1000),
                stats,
            )
            return stats
        except Exception:
            self._copy_embedding_degradation_metadata(stats, metadata)
            await self.run_log_repo.finish_run(run_id, status="failed", stats=stats, metadata=metadata)
            logger.exception(
                "Embedding rescore failed run_id=%s duration_ms=%d stats=%s",
                run_id,
                int((time.monotonic() - run_started_at) * 1000),
                stats,
            )
            raise

    async def run_nightly_collection(
        self,
        *,
        platform: str | None = None,
        auto_backfill: bool | None = None,
        score: bool = True,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict:
        platform_filter = platform.strip().lower() if platform and platform.strip() else None
        lock_name = "nightly_collection"
        lock_owner = f"pipeline:{id(self)}:{time.time_ns()}"
        if self.runtime_lock_repo is not None:
            acquired = await self.runtime_lock_repo.acquire(
                lock_name=lock_name,
                owner_id=lock_owner,
                ttl_seconds=self.collection_lock_ttl_seconds,
            )
            if not acquired:
                active_lock = await self.runtime_lock_repo.get(lock_name=lock_name)
                logger.warning(
                    "Nightly collection skipped because another collection is already running platform_filter=%s lock=%s",
                    platform_filter,
                    active_lock,
                )
                return {
                    "status": "skipped_already_running",
                    "skipped_already_running": True,
                    "platform_filter": platform_filter,
                    "lock": active_lock or {},
                    "items_collected": 0,
                    "items_inserted": 0,
                    "items_updated": 0,
                    "sources_seen": 0,
                    "sources_crawled": 0,
                }
        try:
            run_id = await self.run_log_repo.start_run("nightly_collection")
        except Exception:
            if self.runtime_lock_repo is not None:
                try:
                    await self.runtime_lock_repo.release(lock_name=lock_name, owner_id=lock_owner)
                except Exception:
                    logger.exception("Failed to release runtime lock after run start failure lock_name=%s", lock_name)
            raise
        run_started_at = time.monotonic()
        threshold = self._effective_circuit_threshold()
        empty_threshold = self._effective_empty_threshold()
        stats = {
            "subjects_seen": 0,
            "sources_seen": 0,
            "items_collected": 0,
            "items_inserted": 0,
            "items_updated": 0,
            "sources_crawled": 0,
            "sources_needing_reauth": 0,
            "sources_not_found": 0,
            "sources_reresolved": 0,
            "collector_errors": 0,
            "items_score_candidates": 0,
            "model_shortlist_items": 0,
            "items_model_reranked": 0,
            "model_batch_rerank_calls": 0,
            "embedding_items_scored": 0,
            "embedding_fallback_items": 0,
            "embedding_degraded": False,
            "embedding_degraded_subjects": [],
            "embedding_not_warmed": False,
            "embedding_not_warmed_subjects": [],
            "keyword_shadow_items_scored": 0,
            "items_skipped_subject_source_override": 0,
            "items_scored": 0,
            "scoring_enabled": bool(score),
            "scoring_skipped": False,
            "segments_seen": 0,
            "segments_score_candidates": 0,
            "segments_scored": 0,
            "segments_scored_with_embedding": 0,
            "segments_rebuilt": 0,
            "auto_backfill_enabled": self.auto_backfill_embeddings if auto_backfill is None else bool(auto_backfill),
            "embedding_pending": False,
            "embedding_backfill": {},
            "sources_skipped_circuit_breaker": 0,
            "circuit_broken": [],
            "circuit_broken_reason": [],
            "platform_filter": platform_filter,
        }
        metadata: dict = {
            "circuit_broken": [],
            "circuit_broken_reason": [],
            "circuit_broken_reasons_by_platform": {},
            "circuit_skipped": {},
            "sources_not_found": [],
            "sources_reresolved": [],
            "platform_filter": platform_filter,
            "embedding_degraded": False,
            "embedding_degraded_subjects": [],
            "embedding_fallback_items": 0,
            "embedding_items_scored": 0,
            "embedding_not_warmed": False,
            "embedding_not_warmed_subjects": [],
        }
        bot_failure_streaks: dict[str, int] = {}
        empty_streaks: dict[str, int] = {}
        circuit_broken: set[str] = set()
        circuit_broken_reasons: dict[str, str] = {}
        circuit_skipped: dict[str, int] = {}

        def record_platform_success(platform: str) -> None:
            if bot_failure_streaks.get(platform) or empty_streaks.get(platform):
                logger.info(
                    "Platform circuit breaker streak reset run_id=%s platform=%s previous_bot_streak=%d previous_empty_streak=%d",
                    run_id,
                    platform,
                    bot_failure_streaks.get(platform, 0),
                    empty_streaks.get(platform, 0),
                )
            bot_failure_streaks[platform] = 0
            empty_streaks[platform] = 0

        def failure_class(reason: str) -> str:
            if reason == "empty_result":
                return "empty_legitimate"
            return "bot_shaped"

        def record_platform_failure(platform: str, *, reason: str) -> None:
            if platform in circuit_broken:
                return
            cls = failure_class(reason)
            active_threshold = empty_threshold if cls == "empty_legitimate" else threshold
            streaks = empty_streaks if cls == "empty_legitimate" else bot_failure_streaks
            next_streak = streaks.get(platform, 0) + 1
            streaks[platform] = next_streak
            logger.warning(
                "Platform collection failure streak run_id=%s platform=%s class=%s streak=%d threshold=%d reason=%s",
                run_id,
                platform,
                cls,
                next_streak,
                active_threshold,
                reason,
            )
            if next_streak >= active_threshold:
                circuit_broken.add(platform)
                circuit_broken_reasons[platform] = cls
                stats["circuit_broken"] = sorted(circuit_broken)
                stats["circuit_broken_reason"] = sorted(set(circuit_broken_reasons.values()))
                metadata["circuit_broken"] = sorted(circuit_broken)
                metadata["circuit_broken_reason"] = sorted(set(circuit_broken_reasons.values()))
                metadata["circuit_broken_reasons_by_platform"] = dict(sorted(circuit_broken_reasons.items()))
                logger.error(
                    "Platform circuit breaker tripped run_id=%s platform=%s class=%s threshold=%d reason=%s",
                    run_id,
                    platform,
                    cls,
                    active_threshold,
                    reason,
                )
        try:
            subjects = await self.subject_service.list_subjects()
            stats["subjects_seen"] = len(subjects)
            monitored_sources = await self.source_service.list_monitored_sources()
            if platform_filter:
                monitored_sources = [source for source in monitored_sources if source.platform == platform_filter]
            stats["sources_seen"] = len(monitored_sources)
            logger.info(
                "Nightly collection started run_id=%s subjects=%d monitored_sources=%d platform_filter=%s",
                run_id,
                len(subjects),
                len(monitored_sources),
                platform_filter,
            )

            changed_items: dict[int, CollectedItem] = {}
            for source in monitored_sources:
                source_started_at = time.monotonic()
                if source.platform in circuit_broken:
                    stats["sources_skipped_circuit_breaker"] += 1
                    circuit_skipped[source.platform] = circuit_skipped.get(source.platform, 0) + 1
                    metadata["circuit_skipped"] = dict(sorted(circuit_skipped.items()))
                    logger.warning(
                        "Skipping source after platform circuit breaker run_id=%s source_id=%s platform=%s identifier=%s skipped=%d",
                        run_id,
                        source.source_id,
                        source.platform,
                        source.account_or_channel_id,
                        circuit_skipped[source.platform],
                    )
                    continue
                collector = self.collectors.get(source.platform)
                if collector is None:
                    logger.warning(
                        "No collector registered for source_id=%s platform=%s",
                        source.source_id,
                        source.platform,
                    )
                    continue
                if self.session_refresh_service is not None:
                    refresh = await self.session_refresh_service.refresh_platform(source.platform)
                    logger.info(
                        "Pre-collection session refresh run_id=%s platform=%s refreshed=%s skipped=%s reason=%s browser=%s profile=%s missing=%s",
                        run_id,
                        source.platform,
                        refresh.refreshed,
                        refresh.skipped,
                        refresh.reason,
                        refresh.browser,
                        refresh.profile_name,
                        refresh.missing_cookie_names,
                    )
                try:
                    logger.info(
                        "Collecting source run_id=%s source_id=%s platform=%s identifier=%s",
                        run_id,
                        source.source_id,
                        source.platform,
                        source.account_or_channel_id,
                    )
                    resolver = getattr(collector, "resolve_source_identifier", None)
                    if callable(resolver):
                        original_identifier = source.account_or_channel_id
                        resolved_identifier = await resolver(source.account_or_channel_id)
                        if (
                            isinstance(resolved_identifier, str)
                            and resolved_identifier
                            and resolved_identifier != source.account_or_channel_id
                        ):
                            metadata_values = {
                                "resolved_identifier": resolved_identifier,
                                "resolved_from": original_identifier,
                            }
                            if source.platform == "youtube":
                                metadata_values["resolved_channel_id"] = resolved_identifier
                            await self.source_service.merge_source_metadata(
                                source_id=source.source_id,
                                values=metadata_values,
                            )
                            updated_source = await self.source_service.update_source_identifier(
                                source_id=source.source_id,
                                account_or_channel_id=resolved_identifier,
                            )
                            logger.info(
                                "Resolved source identifier run_id=%s source_id=%s platform=%s old=%s new=%s",
                                run_id,
                                source.source_id,
                                source.platform,
                                source.account_or_channel_id,
                                resolved_identifier,
                            )
                            source.account_or_channel_id = resolved_identifier
                            if updated_source is not None:
                                source.source_id = updated_source.source_id
                    try:
                        items = await collector.collect_from_source(source.account_or_channel_id)
                    except SourceNotFoundError as exc:
                        recovered = await self._attempt_not_found_recovery(
                            run_id=run_id,
                            source=source,
                            collector=collector,
                            exc=exc,
                            stats=stats,
                            metadata=metadata,
                        )
                        if not recovered:
                            await self._mark_source_not_found(
                                run_id=run_id,
                                source=source,
                                exc=exc,
                                stats=stats,
                                metadata=metadata,
                            )
                            continue
                        try:
                            items = await collector.collect_from_source(source.account_or_channel_id)
                        except SourceNotFoundError as retry_exc:
                            await self._mark_source_not_found(
                                run_id=run_id,
                                source=source,
                                exc=retry_exc,
                                stats=stats,
                                metadata=metadata,
                            )
                            continue
                    for item in items:
                        item.metadata = {
                            **(item.metadata or {}),
                            "pcca_source_id": source.source_id,
                            "pcca_source_platform": source.platform,
                            "pcca_source_account_or_channel_id": source.account_or_channel_id,
                        }
                    await self.source_service.mark_source_crawl_success(source.source_id)
                    stats["sources_crawled"] += 1
                    stats["items_collected"] += len(items)
                    if items:
                        record_platform_success(source.platform)
                    else:
                        record_platform_failure(source.platform, reason="empty_result")
                    logger.info(
                        "Collected source run_id=%s source_id=%s platform=%s items=%d duration_ms=%d",
                        run_id,
                        source.source_id,
                        source.platform,
                        len(items),
                        int((time.monotonic() - source_started_at) * 1000),
                    )
                    if items:
                        upsert_stats = await self.item_repo.upsert_many(items)
                        stats["items_inserted"] += upsert_stats["inserted"]
                        stats["items_updated"] += upsert_stats["updated"]
                        changed_item_ids = set(upsert_stats.get("changed_item_ids", []))
                        for item, item_id in zip(items, upsert_stats["item_ids"]):
                            if item_id in changed_item_ids:
                                changed_items[int(item_id)] = item
                                if self.item_segment_repo is not None:
                                    segments = await self.item_segment_repo.ensure_segments(
                                        item_id=int(item_id),
                                        item=item,
                                        replace=True,
                                    )
                                    stats["segments_rebuilt"] += len(segments)
                        logger.info(
                            "Upserted source items run_id=%s source_id=%s inserted=%d updated=%d changed=%d",
                            run_id,
                            source.source_id,
                            upsert_stats["inserted"],
                            upsert_stats["updated"],
                            len(changed_item_ids),
                        )
                except SessionChallengedError as exc:
                    await self.source_service.mark_source_needs_reauth(source.source_id)
                    stats["sources_needing_reauth"] += 1
                    record_platform_failure(source.platform, reason=f"session_challenged:{exc.challenge_kind}")
                    logger.warning(
                        "Session challenge detected run_id=%s source_id=%s platform=%s challenge=%s url=%s duration_ms=%d",
                        run_id,
                        source.source_id,
                        exc.platform,
                        exc.challenge_kind,
                        exc.current_url,
                        int((time.monotonic() - source_started_at) * 1000),
                    )
                except Exception:
                    logger.exception(
                        "Collector failed run_id=%s platform=%s source=%s duration_ms=%d",
                        run_id,
                        source.platform,
                        source.account_or_channel_id,
                        int((time.monotonic() - source_started_at) * 1000),
                    )
                    stats["collector_errors"] += 1
                    record_platform_failure(source.platform, reason="exception")

            if stats["auto_backfill_enabled"] and changed_items:
                try:
                    logger.info(
                        "Auto embedding backfill started run_id=%s changed_items=%d include_segments=True",
                        run_id,
                        len(changed_items),
                    )
                    if progress_callback is not None:
                        try:
                            progress_callback(
                                {
                                    "kind": "auto_backfill",
                                    "phase": "embedding",
                                    "processed": 0,
                                    "total": len(changed_items),
                                    "stats": dict(stats),
                                }
                            )
                        except Exception:
                            logger.exception("Auto embedding progress callback failed run_id=%s", run_id)
                    backfill_stats = await self.backfill_embeddings(
                        concurrency=self.embedding_backfill_concurrency,
                        include_segments=True,
                        item_ids=set(changed_items.keys()),
                        progress_callback=progress_callback,
                    )
                    stats["embedding_backfill"] = backfill_stats
                    stats["embedding_pending"] = bool(
                        backfill_stats.get("items_failed")
                        or backfill_stats.get("segments_failed")
                    )
                    logger.info(
                        "Auto embedding backfill finished run_id=%s pending=%s stats=%s",
                        run_id,
                        stats["embedding_pending"],
                        backfill_stats,
                    )
                except Exception:
                    stats["embedding_pending"] = True
                    logger.warning(
                        "Auto embedding backfill failed run_id=%s changed_items=%d; collection will continue.",
                        run_id,
                        len(changed_items),
                        exc_info=True,
                    )
            elif changed_items:
                logger.info(
                    "Auto embedding backfill skipped run_id=%s enabled=%s changed_items=%d",
                    run_id,
                    stats["auto_backfill_enabled"],
                    len(changed_items),
                )

            if score:
                changed_rows = list(changed_items.items())
                metadata["scoring_subjects"] = [subject.name for subject in subjects]
                for subject_index, subject in enumerate(subjects, start=1):
                    event = {
                        "kind": "scoring",
                        "phase": "scoring",
                        "run_id": run_id,
                        "run_type": "nightly_collection",
                        "subject_index": subject_index,
                        "subject_total": len(subjects),
                        "subject_id": subject.id,
                        "subject_name": subject.name,
                    }
                    stats.setdefault("scoring_progress_events", []).append(event)
                    logger.info(
                        "Scoring progress run_id=%s run_type=nightly_collection subject=%s subject_index=%d subject_total=%d",
                        run_id,
                        subject.name,
                        subject_index,
                        len(subjects),
                    )
                    self._emit_progress(progress_callback, event)
                    inactive_source_ids = await self.source_service.list_inactive_source_ids_for_subject(subject.id)
                    unscored_rows = await self.item_repo.list_unscored_for_subject(subject_id=subject.id)
                    missing_segment_rows = await self.item_repo.list_missing_segment_scores_for_subject(
                        subject_id=subject.id,
                    )
                    changed_ids = {item_id for item_id, _item in changed_rows}
                    seen_ids = set(changed_ids)
                    rows_to_score = changed_rows + [
                        (item_id, item) for item_id, item in unscored_rows if item_id not in seen_ids
                    ]
                    seen_ids.update(item_id for item_id, _item in unscored_rows)
                    rows_to_score.extend(
                        (item_id, item) for item_id, item in missing_segment_rows if item_id not in seen_ids
                    )
                    await self._score_items_for_subject(
                        run_id=run_id,
                        subject=subject,
                        item_rows=rows_to_score,
                        inactive_source_ids=inactive_source_ids,
                        stats=stats,
                    )
            else:
                stats["scoring_skipped"] = True
                logger.info(
                    "Nightly collection scoring skipped run_id=%s changed_items=%d platform_filter=%s",
                    run_id,
                    len(changed_items),
                    platform_filter,
                )

            metadata["circuit_broken"] = sorted(circuit_broken)
            metadata["circuit_broken_reason"] = sorted(set(circuit_broken_reasons.values()))
            metadata["circuit_broken_reasons_by_platform"] = dict(sorted(circuit_broken_reasons.items()))
            metadata["circuit_skipped"] = dict(sorted(circuit_skipped.items()))
            self._copy_embedding_degradation_metadata(stats, metadata)
            await self.run_log_repo.finish_run(run_id, status="success", stats=stats, metadata=metadata)
            logger.info(
                "Nightly collection succeeded run_id=%s duration_ms=%d stats=%s",
                run_id,
                int((time.monotonic() - run_started_at) * 1000),
                stats,
            )
            return stats
        except Exception:
            metadata["circuit_broken"] = sorted(circuit_broken)
            metadata["circuit_broken_reason"] = sorted(set(circuit_broken_reasons.values()))
            metadata["circuit_broken_reasons_by_platform"] = dict(sorted(circuit_broken_reasons.items()))
            metadata["circuit_skipped"] = dict(sorted(circuit_skipped.items()))
            self._copy_embedding_degradation_metadata(stats, metadata)
            await self.run_log_repo.finish_run(run_id, status="failed", stats=stats, metadata=metadata)
            logger.exception(
                "Nightly collection failed run_id=%s duration_ms=%d stats=%s",
                run_id,
                int((time.monotonic() - run_started_at) * 1000),
                stats,
            )
            raise
        finally:
            if self.runtime_lock_repo is not None:
                try:
                    await self.runtime_lock_repo.release(lock_name=lock_name, owner_id=lock_owner)
                except Exception:
                    logger.exception("Failed to release runtime lock lock_name=%s owner_id=%s", lock_name, lock_owner)

    def _effective_circuit_threshold(self) -> int:
        if self.circuit_threshold is not None:
            return max(1, int(self.circuit_threshold))
        try:
            return max(1, int(os.getenv("PCCA_PLATFORM_CIRCUIT_THRESHOLD", "5") or "5"))
        except ValueError:
            return 5

    def _effective_empty_threshold(self) -> int:
        if self.empty_threshold is not None:
            return max(1, int(self.empty_threshold))
        try:
            return max(1, int(os.getenv("PCCA_PLATFORM_EMPTY_THRESHOLD", "25") or "25"))
        except ValueError:
            return 25
