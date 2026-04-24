from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import date

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from pcca.pipeline.orchestrator import PipelineOrchestrator
from pcca.repositories.digests import DigestRepository
from pcca.repositories.item_scores import ItemScoreRepository
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
    pipeline_orchestrator: PipelineOrchestrator | None = None
    telegram_service: TelegramService | None = None

    async def run_nightly_collection(self) -> None:
        if self.pipeline_orchestrator is None:
            subjects = await self.subject_service.list_subjects()
            logger.info("Nightly collection placeholder: %d subjects.", len(subjects))
            return
        stats = await self.pipeline_orchestrator.run_nightly_collection()
        logger.info("Nightly collection finished: %s", stats)

    async def run_morning_digest(self) -> None:
        subjects = await self.subject_service.list_subjects()
        logger.info("Morning digest run: %d subjects.", len(subjects))

        if (
            self.telegram_service is None
            or self.telegram_service.application is None
            or self.item_score_repo is None
            or self.digest_repo is None
            or self.routing_service is None
        ):
            return

        for subject in subjects:
            routes = await self.routing_service.list_routes_for_subject(subject.id)
            if not routes:
                continue
            candidates = await self.item_score_repo.top_unsent_candidates(subject_id=subject.id, limit=5)

            digest_id = await self.digest_repo.create_digest(subject_id=subject.id, run_date=date.today())
            lines: list[str] = []
            for idx, candidate in enumerate(candidates, start=1):
                title = candidate.title_or_text.splitlines()[0][:180] if candidate.title_or_text else "(no title)"
                reason = candidate.rationale or f"score={candidate.final_score:.2f}"
                line = (
                    f"{idx}. {title}\n"
                    f"   via {candidate.author or 'unknown'}\n"
                    f"   {candidate.url or ''}\n"
                    f"   why: {reason}"
                )
                lines.append(line)
                await self.digest_repo.add_digest_item(
                    digest_id=digest_id,
                    item_id=candidate.item_id,
                    rank=idx,
                    reason_selected=reason,
                )

            for route in routes:
                thread_id_int = int(route.thread_id) if route.thread_id and route.thread_id.isdigit() else None
                await self.telegram_service.send_digest_message(
                    chat_id=route.chat_id,
                    subject_name=subject.name,
                    items=lines,
                    thread_id=thread_id_int,
                )
            await self.digest_repo.mark_sent(digest_id=digest_id)


@dataclass
class AgentScheduler:
    nightly_cron: str
    morning_cron: str
    timezone: str
    job_runner: JobRunner
    scheduler: AsyncIOScheduler = field(init=False)

    def __post_init__(self) -> None:
        self.scheduler = AsyncIOScheduler(timezone=self.timezone)

    def start(self) -> None:
        self.scheduler.add_job(
            self._run_async_job,
            trigger=CronTrigger.from_crontab(self.nightly_cron, timezone=self.timezone),
            args=[self.job_runner.run_nightly_collection],
            id="nightly_collection",
            replace_existing=True,
        )
        self.scheduler.add_job(
            self._run_async_job,
            trigger=CronTrigger.from_crontab(self.morning_cron, timezone=self.timezone),
            args=[self.job_runner.run_morning_digest],
            id="morning_digest",
            replace_existing=True,
        )
        self.scheduler.start()
        logger.info("Scheduler started. nightly=%s morning=%s", self.nightly_cron, self.morning_cron)

    def shutdown(self) -> None:
        if self.scheduler.running:
            self.scheduler.shutdown(wait=False)
            logger.info("Scheduler stopped.")

    @staticmethod
    def _run_async_job(coro_fn) -> None:  # type: ignore[no-untyped-def]
        asyncio.create_task(coro_fn())
