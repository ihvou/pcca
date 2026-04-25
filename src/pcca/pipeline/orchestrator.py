from __future__ import annotations

import logging
from dataclasses import dataclass, field

from pcca.collectors.base import Collector
from pcca.collectors.errors import SessionChallengedError
from pcca.repositories.items import ItemRepository
from pcca.repositories.item_scores import ItemScoreRepository
from pcca.repositories.run_logs import RunLogRepository
from pcca.pipeline.curation import CurationEngine
from pcca.services.model_router import ModelRouter
from pcca.services.preference_service import PreferenceService
from pcca.services.source_service import SourceService
from pcca.services.subject_service import SubjectService

logger = logging.getLogger(__name__)


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
    collectors: dict[str, Collector] = field(default_factory=dict)

    async def run_nightly_collection(self) -> dict:
        run_id = await self.run_log_repo.start_run("nightly_collection")
        stats = {
            "subjects_seen": 0,
            "sources_seen": 0,
            "items_collected": 0,
            "items_inserted": 0,
            "items_updated": 0,
            "sources_crawled": 0,
            "sources_needing_reauth": 0,
            "collector_errors": 0,
            "items_scored": 0,
        }
        try:
            subjects = await self.subject_service.list_subjects()
            stats["subjects_seen"] = len(subjects)
            for subject in subjects:
                include_terms: list[str] = []
                exclude_terms: list[str] = []
                min_practicality: float | None = None
                if self.preference_service is not None:
                    pref = await self.preference_service.get_preferences_by_subject_id(subject.id)
                    include_terms = [
                        t for t in pref.include_rules.get("topics", []) if isinstance(t, str) and t.strip()
                    ]
                    exclude_terms = [
                        t for t in pref.exclude_rules.get("topics", []) if isinstance(t, str) and t.strip()
                    ]
                    if isinstance(pref.quality_rules.get("min_practicality"), (float, int)):
                        min_practicality = float(pref.quality_rules["min_practicality"])
                subject_sources = await self.source_service.list_sources_for_subject(subject.name)
                stats["sources_seen"] += len(subject_sources)
                for source in subject_sources:
                    collector = self.collectors.get(source.platform)
                    if collector is None:
                        logger.warning(
                            "No collector registered for subject=%s source_id=%s platform=%s",
                            subject.name,
                            source.source_id,
                            source.platform,
                        )
                        continue
                    try:
                        logger.info(
                            "Collecting source subject=%s source_id=%s platform=%s identifier=%s",
                            subject.name,
                            source.source_id,
                            source.platform,
                            source.account_or_channel_id,
                        )
                        items = await collector.collect_from_source(source.account_or_channel_id)
                        await self.source_service.mark_source_crawl_success(source.source_id)
                        stats["sources_crawled"] += 1
                        stats["items_collected"] += len(items)
                        logger.info(
                            "Collected source subject=%s source_id=%s platform=%s items=%d",
                            subject.name,
                            source.source_id,
                            source.platform,
                            len(items),
                        )
                        if items:
                            upsert_stats = await self.item_repo.upsert_many(items)
                            stats["items_inserted"] += upsert_stats["inserted"]
                            stats["items_updated"] += upsert_stats["updated"]
                            changed_item_ids = set(upsert_stats.get("changed_item_ids", []))

                            for item, item_id in zip(items, upsert_stats["item_ids"]):
                                if item_id not in changed_item_ids:
                                    continue
                                scored = self.curation_engine.score(
                                    subject.name,
                                    item,
                                    include_terms=include_terms,
                                    exclude_terms=exclude_terms,
                                    min_practicality=min_practicality,
                                )
                                if self.model_router is not None:
                                    rerank = await self.model_router.rerank(
                                        subject_name=subject.name,
                                        text=item.text or item.transcript_text or "",
                                        heuristic_score=scored.final_score,
                                    )
                                    if rerank is not None:
                                        adjusted_final = max(0.0, min(1.0, scored.final_score + rerank.score_delta))
                                        scored.final_score = adjusted_final
                                        scored.rationale = f"{scored.rationale}; model={rerank.rationale}"
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
                    except SessionChallengedError as exc:
                        await self.source_service.mark_source_needs_reauth(source.source_id)
                        stats["sources_needing_reauth"] += 1
                        logger.warning(
                            "Session challenge detected subject=%s source_id=%s platform=%s challenge=%s url=%s",
                            subject.name,
                            source.source_id,
                            exc.platform,
                            exc.challenge_kind,
                            exc.current_url,
                        )
                    except Exception:
                        logger.exception(
                            "Collector failed for subject=%s platform=%s source=%s",
                            subject.name,
                            source.platform,
                            source.account_or_channel_id,
                        )
                        stats["collector_errors"] += 1

            await self.run_log_repo.finish_run(run_id, status="success", stats=stats)
            return stats
        except Exception:
            await self.run_log_repo.finish_run(run_id, status="failed", stats=stats)
            raise
