from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field

from pcca.collectors.base import CollectedItem, Collector
from pcca.collectors.errors import SessionChallengedError
from pcca.repositories.items import ItemRepository
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

    def __post_init__(self) -> None:
        if self.runtime_lock_repo is None:
            self.runtime_lock_repo = RuntimeLockRepository(conn=self.run_log_repo.conn)

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
        parts = [
            f"Subject: {subject.name}",
            context.description_text or "",
            f"Include: {', '.join(context.include_terms)}" if context.include_terms else "",
            f"Avoid: {', '.join(context.exclude_terms)}" if context.exclude_terms else "",
            f"Quality: {context.quality_notes}" if context.quality_notes else "",
        ]
        return "\n".join(part for part in parts if part.strip())

    async def _get_or_create_subject_embedding(self, *, subject_id: int, text: str) -> list[float] | None:
        if self.embedding_service is None:
            return None
        model = self.embedding_service.embedding_model
        existing = await self.subject_service.repository.get_description_embedding(subject_id, model=model)
        if existing is not None:
            return existing
        embedding = await self.embedding_service.embed(text)
        if embedding is not None:
            await self.subject_service.repository.save_description_embedding(subject_id, model=model, embedding=embedding)
        return embedding

    async def _get_or_create_item_embedding(self, *, item_id: int, item: CollectedItem) -> list[float] | None:
        if self.embedding_service is None:
            return None
        model = self.embedding_service.embedding_model
        existing = await self.item_repo.get_content_embedding(item_id, model=model)
        if existing is not None:
            return existing
        text = self.item_repo.embedding_text(item)
        embedding = await self.embedding_service.embed(text)
        if embedding is not None:
            await self.item_repo.save_content_embedding(item_id, model=model, embedding=embedding)
        return embedding

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
        subject_embedding = (
            await self._get_or_create_subject_embedding(subject_id=subject.id, text=subject_embedding_text)
            if self._embedding_enabled()
            else None
        )
        scored_rows: list[tuple[int, CollectedItem, object, bool]] = []
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
                scored_rows.append((item_id, item, scored, True))
            else:
                if self._embedding_enabled():
                    stats["embedding_fallback_items"] += 1
                scored_rows.append((item_id, item, keyword_scored, False))

        shortlist_rows = sorted(scored_rows, key=lambda row: row[2].final_score, reverse=True)[: context.shortlist_limit]
        shortlist_ids = {item_id for item_id, _item, _scored, _used_embedding in shortlist_rows}
        stats["items_score_candidates"] += len(scored_rows)
        stats["model_shortlist_items"] += len(shortlist_ids)
        logger.info(
            "Scoring subject run_id=%s subject=%s items=%d shortlist=%d include_terms=%d exclude_terms=%d scorer=%s embedding_enabled=%s",
            run_id,
            subject.name,
            len(scored_rows),
            len(shortlist_ids),
            len(context.include_terms),
            len(context.exclude_terms),
            self._scorer_mode(),
            bool(subject_embedding is not None),
        )

        batch_results = {}
        batch_rerank = getattr(self.model_router, "rerank_batch", None) if self.model_router is not None else None
        shortlist_used_embedding = any(used_embedding for _item_id, _item, _scored, used_embedding in shortlist_rows)
        if callable(batch_rerank) and shortlist_used_embedding and getattr(self.model_router, "enabled", True):
            candidates = [
                ModelRerankCandidate(
                    item_id=item_id,
                    text=item.text or item.transcript_text or "",
                    heuristic_score=scored.final_score,
                    author=item.author,
                    url=item.url,
                    published_at=item.published_at,
                )
                for item_id, item, scored, _used_embedding in shortlist_rows
            ]
            batch_results = await batch_rerank(
                subject_name=subject.name,
                subject_description=subject_embedding_text,
                candidates=candidates,
            )
            stats["model_batch_rerank_calls"] += 1

        for item_id, item, scored, _used_embedding in scored_rows:
            rerank = batch_results.get(item_id) if isinstance(batch_results, dict) else None
            if rerank is not None:
                adjusted_final = max(0.0, min(1.0, scored.final_score + rerank.score_delta))
                scored.final_score = adjusted_final
                scored.rationale = f"{scored.rationale}; model_batch={rerank.rationale}"
                stats["items_model_reranked"] += 1
            elif self.model_router is not None and item_id in shortlist_ids and not batch_results:
                rerank = await self.model_router.rerank(
                    subject_name=subject.name,
                    text=item.text or item.transcript_text or "",
                    heuristic_score=scored.final_score,
                )
                if rerank is not None:
                    adjusted_final = max(0.0, min(1.0, scored.final_score + rerank.score_delta))
                    scored.final_score = adjusted_final
                    scored.rationale = f"{scored.rationale}; model={rerank.rationale}"
                    stats["items_model_reranked"] += 1
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
            )
            stats["items_scored"] += 1

    async def run_nightly_collection(self, *, platform: str | None = None) -> dict:
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
            "collector_errors": 0,
            "items_score_candidates": 0,
            "model_shortlist_items": 0,
            "items_model_reranked": 0,
            "model_batch_rerank_calls": 0,
            "embedding_items_scored": 0,
            "embedding_fallback_items": 0,
            "keyword_shadow_items_scored": 0,
            "items_skipped_subject_source_override": 0,
            "items_scored": 0,
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
            "platform_filter": platform_filter,
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
                    items = await collector.collect_from_source(source.account_or_channel_id)
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

            changed_rows = list(changed_items.items())
            for subject in subjects:
                inactive_source_ids = await self.source_service.list_inactive_source_ids_for_subject(subject.id)
                unscored_rows = await self.item_repo.list_unscored_for_subject(subject_id=subject.id)
                changed_ids = {item_id for item_id, _item in changed_rows}
                rows_to_score = changed_rows + [
                    (item_id, item) for item_id, item in unscored_rows if item_id not in changed_ids
                ]
                await self._score_items_for_subject(
                    run_id=run_id,
                    subject=subject,
                    item_rows=rows_to_score,
                    inactive_source_ids=inactive_source_ids,
                    stats=stats,
                )

            metadata["circuit_broken"] = sorted(circuit_broken)
            metadata["circuit_broken_reason"] = sorted(set(circuit_broken_reasons.values()))
            metadata["circuit_broken_reasons_by_platform"] = dict(sorted(circuit_broken_reasons.items()))
            metadata["circuit_skipped"] = dict(sorted(circuit_skipped.items()))
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
