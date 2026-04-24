from __future__ import annotations

import logging
from dataclasses import dataclass, field

from pcca.collectors.base import Collector
from pcca.repositories.items import ItemRepository
from pcca.repositories.item_scores import ItemScoreRepository
from pcca.repositories.run_logs import RunLogRepository
from pcca.pipeline.curation import CurationEngine
from pcca.services.model_router import ModelRouter
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
            "collector_errors": 0,
        }
        try:
            subjects = await self.subject_service.list_subjects()
            stats["subjects_seen"] = len(subjects)
            for subject in subjects:
                subject_sources = await self.source_service.list_sources_for_subject(subject.name)
                stats["sources_seen"] += len(subject_sources)
                for source in subject_sources:
                    collector = self.collectors.get(source.platform)
                    if collector is None:
                        continue
                    try:
                        items = await collector.collect_from_source(source.account_or_channel_id)
                        stats["items_collected"] += len(items)
                        if items:
                            upsert_stats = await self.item_repo.upsert_many(items)
                            stats["items_inserted"] += upsert_stats["inserted"]
                            stats["items_updated"] += upsert_stats["updated"]

                            for item, item_id in zip(items, upsert_stats["item_ids"]):
                                scored = self.curation_engine.score(subject.name, item)
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
