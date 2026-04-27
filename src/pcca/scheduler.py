from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import date

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from pcca.pipeline.orchestrator import PipelineOrchestrator
from pcca.repositories.digests import DigestRepository
from pcca.repositories.item_scores import ItemScoreRepository
from pcca.repositories.run_logs import RunLogRepository
from pcca.services.subject_service import SubjectService
from pcca.services.telegram_service import TelegramService
from pcca.services.routing_service import RoutingService

logger = logging.getLogger(__name__)


@dataclass
class JobRunner:
    subject_service: SubjectService
    routing_service: RoutingService | None = None
    item_score_repo: ItemScoreRepository | None = None
    digest_repo: DigestRepository | None = None
    run_log_repo: RunLogRepository | None = None
    pipeline_orchestrator: PipelineOrchestrator | None = None
    telegram_service: TelegramService | None = None

    async def run_nightly_collection(self) -> None:
        started_at = time.monotonic()
        if self.pipeline_orchestrator is None:
            subjects = await self.subject_service.list_subjects()
            logger.info("Nightly collection placeholder: %d subjects.", len(subjects))
            return
        try:
            stats = await self.pipeline_orchestrator.run_nightly_collection()
            logger.info(
                "Nightly collection finished duration_ms=%d stats=%s",
                int((time.monotonic() - started_at) * 1000),
                stats,
            )
        except Exception:
            logger.exception("Nightly collection failed duration_ms=%d", int((time.monotonic() - started_at) * 1000))
            raise

    async def run_morning_digest(
        self,
        *,
        run_type: str = "morning_digest",
        force_rebuild: bool = False,
        subject_ids: set[int] | None = None,
    ) -> dict:
        started_at = time.monotonic()
        run_id = await self.run_log_repo.start_run(run_type) if self.run_log_repo is not None else None
        stats = {
            "subjects_seen": 0,
            "subjects_with_routes": 0,
            "digests_created_or_reused": 0,
            "digests_rebuilt": 0,
            "items_selected": 0,
            "deliveries_sent": 0,
            "deliveries_failed": 0,
            "skipped_missing_dependencies": False,
        }
        try:
            subjects = await self.subject_service.list_subjects()
            if subject_ids is not None:
                subjects = [subject for subject in subjects if subject.id in subject_ids]
            stats["subjects_seen"] = len(subjects)
            logger.info(
                "Morning digest run started run_id=%s run_type=%s subjects=%d force_rebuild=%s subject_ids=%s",
                run_id,
                run_type,
                len(subjects),
                force_rebuild,
                sorted(subject_ids) if subject_ids is not None else None,
            )
            if (
                self.telegram_service is None
                or self.telegram_service.application is None
                or self.item_score_repo is None
                or self.digest_repo is None
                or self.routing_service is None
            ):
                stats["skipped_missing_dependencies"] = True
                return stats

            if force_rebuild:
                rebuilt = await self.digest_repo.delete_digests_for_date(
                    run_date=date.today(),
                    subject_ids=subject_ids,
                )
                stats["digests_rebuilt"] = rebuilt
                logger.info(
                    "Deleted existing digest rows for rebuild run_id=%s run_type=%s date=%s count=%d subject_ids=%s",
                    run_id,
                    run_type,
                    date.today().isoformat(),
                    rebuilt,
                    sorted(subject_ids) if subject_ids is not None else None,
                )

            for subject in subjects:
                subject_started_at = time.monotonic()
                routes = await self.routing_service.list_routes_for_subject(subject.id)
                if not routes:
                    logger.info("Morning digest subject skipped no_routes run_id=%s subject=%s", run_id, subject.name)
                    continue
                stats["subjects_with_routes"] += 1

                digest = await self.digest_repo.get_or_create_digest(subject_id=subject.id, run_date=date.today())
                stats["digests_created_or_reused"] += 1
                existing_items = await self.digest_repo.list_digest_items(digest_id=digest.id)
                if existing_items:
                    candidates = await self.item_score_repo.candidates_by_item_ids(
                        subject_id=subject.id,
                        item_ids=[item.item_id for item in existing_items],
                    )
                    candidate_by_id = {candidate.item_id: candidate for candidate in candidates}
                    ordered_candidates = [
                        candidate_by_id[item.item_id]
                        for item in existing_items
                        if item.item_id in candidate_by_id
                    ]
                else:
                    ordered_candidates = await self.item_score_repo.top_unsent_candidates(subject_id=subject.id, limit=5)
                stats["items_selected"] += len(ordered_candidates)
                logger.info(
                    "Morning digest subject selected run_id=%s subject=%s routes=%d items=%d existing=%s",
                    run_id,
                    subject.name,
                    len(routes),
                    len(ordered_candidates),
                    bool(existing_items),
                )
                lines: list[str] = []
                item_actions: list[dict] = []
                for idx, candidate in enumerate(ordered_candidates, start=1):
                    title = candidate.title_or_text.splitlines()[0][:180] if candidate.title_or_text else "(no title)"
                    reason = candidate.rationale or f"score={candidate.final_score:.2f}"
                    line = (
                        f"{idx}. {title}\n"
                        f"   via {candidate.author or 'unknown'}\n"
                        f"   published: {candidate.published_at or 'unknown'}\n"
                        f"   {candidate.url or ''}\n"
                        f"   why: {reason}"
                    )
                    lines.append(line)
                    tokens = {
                        action: await self.digest_repo.create_button_token(
                            digest_id=digest.id,
                            item_id=candidate.item_id,
                            subject_id=subject.id,
                            action=action,
                        )
                        for action in ("up", "down", "save")
                    }
                    item_actions.append({"rank": idx, "tokens": tokens})
                    await self.digest_repo.add_digest_item(
                        digest_id=digest.id,
                        item_id=candidate.item_id,
                        rank=idx,
                        reason_selected=reason,
                    )

                for route in routes:
                    thread_id_int = int(route.thread_id) if route.thread_id and route.thread_id.isdigit() else None
                    try:
                        message_id = await self.telegram_service.send_digest_message(
                            chat_id=route.chat_id,
                            subject_name=subject.name,
                            items=lines,
                            item_actions=item_actions,
                            thread_id=thread_id_int,
                        )
                        await self.digest_repo.record_delivery(
                            digest_id=digest.id,
                            chat_id=route.chat_id,
                            thread_id=route.thread_id,
                            status="sent",
                            message_id=message_id,
                        )
                        stats["deliveries_sent"] += 1
                    except Exception as exc:
                        logger.exception("Digest delivery failed for subject=%s chat_id=%s", subject.name, route.chat_id)
                        await self.digest_repo.record_delivery(
                            digest_id=digest.id,
                            chat_id=route.chat_id,
                            thread_id=route.thread_id,
                            status="failed",
                            error_text=str(exc),
                        )
                        stats["deliveries_failed"] += 1
                await self.digest_repo.mark_sent(digest_id=digest.id)
                logger.info(
                    "Morning digest subject finished run_id=%s subject=%s duration_ms=%d",
                    run_id,
                    subject.name,
                    int((time.monotonic() - subject_started_at) * 1000),
                )
            if run_id is not None and self.run_log_repo is not None:
                await self.run_log_repo.finish_run(run_id, "success", stats)
            logger.info(
                "Morning digest run finished run_id=%s duration_ms=%d stats=%s",
                run_id,
                int((time.monotonic() - started_at) * 1000),
                stats,
            )
            return stats
        except Exception:
            if run_id is not None and self.run_log_repo is not None:
                await self.run_log_repo.finish_run(run_id, "failed", stats)
            logger.exception(
                "Morning digest run failed run_id=%s duration_ms=%d stats=%s",
                run_id,
                int((time.monotonic() - started_at) * 1000),
                stats,
            )
            raise
        finally:
            if stats["skipped_missing_dependencies"] and run_id is not None and self.run_log_repo is not None:
                await self.run_log_repo.finish_run(run_id, "skipped", stats)
                logger.warning(
                    "Morning digest run skipped missing dependencies run_id=%s duration_ms=%d stats=%s",
                    run_id,
                    int((time.monotonic() - started_at) * 1000),
                    stats,
                )

    async def rebuild_todays_digest(self, *, subject_ids: set[int] | None = None) -> dict:
        return await self.run_morning_digest(
            run_type="digest_rebuild",
            force_rebuild=True,
            subject_ids=subject_ids,
        )


@dataclass
class AgentScheduler:
    nightly_cron: str
    morning_cron: str
    timezone: str
    job_runner: JobRunner
    digest_auto_send: bool = False
    scheduler: AsyncIOScheduler = field(init=False)

    def __post_init__(self) -> None:
        self.scheduler = AsyncIOScheduler(timezone=self.timezone)

    def start(self) -> None:
        self.scheduler.add_job(
            self.job_runner.run_nightly_collection,
            trigger=CronTrigger.from_crontab(self.nightly_cron, timezone=self.timezone),
            id="nightly_collection",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600,
        )
        if self.digest_auto_send:
            self.scheduler.add_job(
                self.job_runner.run_morning_digest,
                trigger=CronTrigger.from_crontab(self.morning_cron, timezone=self.timezone),
                id="morning_digest",
                replace_existing=True,
                max_instances=1,
                coalesce=True,
                misfire_grace_time=3600,
            )
        self.scheduler.start()
        logger.info(
            "Scheduler started. nightly=%s morning=%s digest_auto_send=%s",
            self.nightly_cron,
            self.morning_cron if self.digest_auto_send else "(on-demand only)",
            self.digest_auto_send,
        )

    def shutdown(self) -> None:
        if self.scheduler.running:
            self.scheduler.shutdown(wait=False)
            logger.info("Scheduler stopped.")
