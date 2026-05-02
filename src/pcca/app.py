from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from pcca.collectors.linkedin_collector import LinkedInCollector
from pcca.collectors.reddit_collector import RedditCollector
from pcca.collectors.rss_collector import RSSCollector
from pcca.collectors.spotify_collector import SpotifyCollector
from pcca.collectors.x_collector import XCollector
from pcca.collectors.youtube_collector import YouTubeCollector
from pcca.browser.session_manager import BrowserSessionManager
from pcca.config import Settings
from pcca.db import Database
from pcca.pipeline.orchestrator import PipelineOrchestrator
from pcca.repositories.digests import DigestRepository
from pcca.repositories.feedback import FeedbackRepository
from pcca.repositories.item_scores import ItemScoreRepository
from pcca.repositories.items import ItemRepository
from pcca.repositories.lookup_cache import LookupCacheRepository
from pcca.repositories.onboarding import OnboardingRepository
from pcca.repositories.preferences import SubjectPreferenceRepository
from pcca.repositories.routing import RoutingRepository
from pcca.repositories.run_logs import RunLogRepository
from pcca.repositories.sources import SourceRepository
from pcca.repositories.subject_drafts import SubjectDraftRepository
from pcca.repositories.subjects import SubjectRepository
from pcca.services.feedback_service import FeedbackService
from pcca.scheduler import AgentScheduler, JobRunner
from pcca.services.embedding_service import EmbeddingService
from pcca.services.follow_import_service import FollowImportService
from pcca.services.model_router import ModelRouter
from pcca.services.preference_extraction_service import PreferenceExtractionService
from pcca.services.preference_service import PreferenceService
from pcca.services.routing_service import RoutingService
from pcca.services.session_capture_service import SessionRefreshService
from pcca.services.source_discovery_service import SourceDiscoveryService
from pcca.services.source_service import SourceService
from pcca.services.subject_service import SubjectService
from pcca.services.telegram_service import TelegramService
from pcca.services.voice_transcription_service import VoiceTranscriptionService

logger = logging.getLogger(__name__)


@dataclass
class PCCAApp:
    settings: Settings
    db: Database = field(init=False)
    subject_service: SubjectService = field(init=False)
    source_service: SourceService = field(init=False)
    preference_service: PreferenceService = field(init=False)
    feedback_service: FeedbackService = field(init=False)
    follow_import_service: FollowImportService = field(init=False)
    session_refresh_service: SessionRefreshService = field(init=False)
    routing_service: RoutingService = field(init=False)
    pipeline_orchestrator: PipelineOrchestrator = field(init=False)
    browser_session_manager: BrowserSessionManager = field(init=False)
    scheduler: AgentScheduler = field(init=False)
    embedding_service: EmbeddingService = field(init=False)
    telegram_service: TelegramService | None = field(default=None, init=False)

    async def start(self, *, with_scheduler: bool = True, with_telegram: bool = True) -> None:
        started_at = time.monotonic()
        logger.info(
            "PCCA app starting scheduler=%s telegram=%s db=%s data_dir=%s",
            with_scheduler,
            with_telegram,
            self.settings.db_path,
            self.settings.data_dir,
        )
        self.settings.ensure_dirs()
        self.db = Database(path=self.settings.db_path)
        await self.db.connect()
        await self.db.initialize()

        if self.db.conn is None:
            raise RuntimeError("Database connection unavailable after startup.")

        subject_repo = SubjectRepository(conn=self.db.conn)
        source_repo = SourceRepository(conn=self.db.conn)
        routing_repo = RoutingRepository(conn=self.db.conn)
        preference_repo = SubjectPreferenceRepository(conn=self.db.conn)
        feedback_repo = FeedbackRepository(conn=self.db.conn)
        digest_repo = DigestRepository(conn=self.db.conn)
        run_log_repo = RunLogRepository(conn=self.db.conn)
        subject_draft_repo = SubjectDraftRepository(conn=self.db.conn)
        lookup_cache_repo = LookupCacheRepository(conn=self.db.conn)
        model_router = ModelRouter(
            enabled=self.settings.ollama_enabled,
            ollama_base_url=self.settings.ollama_base_url,
            ollama_model=self.settings.ollama_model,
        )
        embedding_service = EmbeddingService(
            enabled=self.settings.ollama_enabled and self.settings.scorer in {"embedding", "both"},
            ollama_base_url=self.settings.ollama_base_url,
            embedding_model=self.settings.embedding_model,
            timeout_seconds=self.settings.embedding_timeout_seconds,
        )
        self.embedding_service = embedding_service
        self.subject_service = SubjectService(repository=subject_repo)
        self.source_service = SourceService(source_repo=source_repo, subject_repo=subject_repo)
        self.preference_service = PreferenceService(preference_repo=preference_repo, subject_repo=subject_repo)
        self.feedback_service = FeedbackService(
            feedback_repo=feedback_repo,
            subject_repo=subject_repo,
            digest_repo=digest_repo,
            preference_repo=preference_repo,
        )
        self.routing_service = RoutingService(routing_repo=routing_repo, subject_repo=subject_repo)
        self.browser_session_manager = BrowserSessionManager(
            profiles_root=self.settings.browser_profiles_dir,
            headless=self.settings.browser_headless,
            headful_platforms=self.settings.browser_headful_platforms,
            browser_channel=self.settings.browser_channel,
        )
        self.session_refresh_service = SessionRefreshService(
            settings=self.settings,
            session_manager=self.browser_session_manager,
        )
        self.follow_import_service = FollowImportService(
            session_manager=self.browser_session_manager,
            source_service=self.source_service,
            source_discovery=SourceDiscoveryService(cache_repo=lookup_cache_repo),
            session_refresh_service=self.session_refresh_service,
        )

        self.pipeline_orchestrator = PipelineOrchestrator(
            subject_service=self.subject_service,
            source_service=self.source_service,
            item_repo=ItemRepository(conn=self.db.conn),
            item_score_repo=ItemScoreRepository(conn=self.db.conn),
            run_log_repo=run_log_repo,
            preference_service=self.preference_service,
            model_router=model_router,
            embedding_service=embedding_service,
            session_refresh_service=self.session_refresh_service,
            scorer=self.settings.scorer,
            circuit_threshold=self.settings.platform_circuit_threshold,
            empty_threshold=self.settings.platform_empty_threshold,
            auto_backfill_embeddings=self.settings.auto_backfill_embeddings,
            embedding_backfill_concurrency=self.settings.embedding_backfill_concurrency,
            collectors={
                "x": XCollector(session_manager=self.browser_session_manager),
                "linkedin": LinkedInCollector(session_manager=self.browser_session_manager),
                "youtube": YouTubeCollector(session_manager=self.browser_session_manager),
                "reddit": RedditCollector(),
                "rss": RSSCollector(),
                "substack": RSSCollector(platform="substack"),
                "medium": RSSCollector(platform="medium"),
                "apple_podcasts": RSSCollector(platform="apple_podcasts"),
                "spotify": SpotifyCollector(session_manager=self.browser_session_manager),
            },
        )

        if with_telegram and self.settings.telegram_bot_token:
            self.telegram_service = TelegramService(
                bot_token=self.settings.telegram_bot_token,
                subject_service=self.subject_service,
                source_service=self.source_service,
                preference_service=self.preference_service,
                feedback_service=self.feedback_service,
                source_discovery=SourceDiscoveryService(),
                routing_service=self.routing_service,
                subject_draft_repo=subject_draft_repo,
                preference_extractor=PreferenceExtractionService(model_router=model_router),
                voice_transcriber=VoiceTranscriptionService(),
            )
            await self.telegram_service.start()
        elif with_telegram:
            logger.warning("PCCA_TELEGRAM_BOT_TOKEN is not set. Telegram service will be disabled.")

        self.scheduler = AgentScheduler(
            nightly_cron=self.settings.nightly_cron,
            morning_cron=self.settings.morning_cron,
            timezone=self.settings.timezone,
            digest_auto_send=self.settings.digest_auto_send,
            job_runner=JobRunner(
                subject_service=self.subject_service,
                routing_service=self.routing_service,
                item_score_repo=ItemScoreRepository(conn=self.db.conn),
                digest_repo=digest_repo,
                run_log_repo=run_log_repo,
                pipeline_orchestrator=self.pipeline_orchestrator,
                telegram_service=self.telegram_service,
            ),
        )
        if with_scheduler:
            self.scheduler.start()

        if self.telegram_service is not None:
            self.telegram_service.attach_manual_actions(
                read_content_action=self.scheduler.job_runner.run_nightly_collection,
                get_digest_action=self.scheduler.job_runner.run_smart_briefs,
                rebuild_digest_action=self.scheduler.job_runner.rebuild_todays_digest,
            )
        logger.info("PCCA app started duration_ms=%d", int((time.monotonic() - started_at) * 1000))

    async def stop(self) -> None:
        started_at = time.monotonic()
        logger.info("PCCA app stopping.")
        if hasattr(self, "scheduler"):
            self.scheduler.shutdown()
        if hasattr(self, "browser_session_manager"):
            await self.browser_session_manager.stop()
        if self.telegram_service is not None:
            await self.telegram_service.stop()
        if hasattr(self, "db"):
            await self.db.close()
        logger.info("PCCA app stopped duration_ms=%d", int((time.monotonic() - started_at) * 1000))

    async def run_forever(self) -> None:
        await self.start()
        logger.info("PCCA agent is running.")
        try:
            while True:
                await asyncio.sleep(3600)
        finally:
            await self.stop()

    async def run_nightly_once(
        self,
        *,
        platform: str | None = None,
        auto_backfill: bool | None = None,
        score: bool = False,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict:
        started_at = time.monotonic()
        await self.start(with_scheduler=False, with_telegram=False)
        try:
            stats = await self.pipeline_orchestrator.run_nightly_collection(
                platform=platform,
                auto_backfill=auto_backfill,
                score=score,
                progress_callback=progress_callback,
            )
            logger.info("PCCA one-shot nightly finished duration_ms=%d stats=%s", int((time.monotonic() - started_at) * 1000), stats)
            return stats
        finally:
            await self.stop()

    async def backfill_embeddings_current(
        self,
        *,
        concurrency: int = 4,
        limit: int | None = None,
        rescore: bool = True,
        include_segments: bool = False,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict:
        if not hasattr(self, "pipeline_orchestrator"):
            raise RuntimeError("PCCA app is not started.")
        backfill_stats = await self.pipeline_orchestrator.backfill_embeddings(
            concurrency=concurrency,
            limit=limit,
            include_segments=include_segments,
            progress_callback=progress_callback,
        )
        rescore_stats = {}
        if rescore and backfill_stats.get("enabled"):
            rescore_stats = await self.pipeline_orchestrator.rescore_existing_items(
                limit=limit,
                progress_callback=progress_callback,
            )
        return {"backfill": backfill_stats, "rescore": rescore_stats}

    async def run_embedding_backfill_once(
        self,
        *,
        concurrency: int = 4,
        limit: int | None = None,
        rescore: bool = True,
        include_segments: bool = False,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict:
        started_at = time.monotonic()
        await self.start(with_scheduler=False, with_telegram=False)
        try:
            stats = await self.backfill_embeddings_current(
                concurrency=concurrency,
                limit=limit,
                rescore=rescore,
                include_segments=include_segments,
                progress_callback=progress_callback,
            )
            logger.info(
                "PCCA embedding backfill finished duration_ms=%d stats=%s",
                int((time.monotonic() - started_at) * 1000),
                stats,
            )
            return stats
        finally:
            await self.stop()

    async def run_briefs_once(
        self,
        *,
        subject_ids: set[int] | None = None,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict:
        started_at = time.monotonic()
        await self.start(with_scheduler=False, with_telegram=True)
        try:
            stats = await self.scheduler.job_runner.run_smart_briefs(
                subject_ids=subject_ids,
                progress_callback=progress_callback,
            )
            logger.info("PCCA one-shot Briefs finished duration_ms=%d stats=%s", int((time.monotonic() - started_at) * 1000), stats)
            return stats
        finally:
            await self.stop()

    async def run_digest_once(self) -> dict:
        return await self.run_briefs_once()

    async def rebuild_briefs_once(self, *, subject_ids: set[int] | None = None) -> dict:
        started_at = time.monotonic()
        await self.start(with_scheduler=False, with_telegram=True)
        try:
            stats = await self.scheduler.job_runner.rebuild_todays_digest(subject_ids=subject_ids)
            logger.info(
                "PCCA one-shot Briefs rebuild finished duration_ms=%d stats=%s",
                int((time.monotonic() - started_at) * 1000),
                stats,
            )
            return stats
        finally:
            await self.stop()

    async def rebuild_digest_once(self, *, subject_ids: set[int] | None = None) -> dict:
        return await self.rebuild_briefs_once(subject_ids=subject_ids)

    async def import_follows_once(self, *, subject_name: str, platform: str, limit: int = 200) -> int:
        await self.start(with_scheduler=False, with_telegram=False)
        try:
            return await self.follow_import_service.import_to_subject(
                subject_name=subject_name,
                platform=platform,
                limit=limit,
            )
        finally:
            await self.stop()

    async def stage_follows_once(self, *, platform: str, limit: int = 200) -> int:
        await self.start(with_scheduler=False, with_telegram=False)
        try:
            if self.db.conn is None:
                raise RuntimeError("Database connection unavailable.")
            imported = await self.follow_import_service.import_sources(platform=platform, limit=limit)
            onboarding_repo = OnboardingRepository(conn=self.db.conn)
            for source in imported:
                await onboarding_repo.stage_source(
                    platform=source.platform,
                    account_or_channel_id=source.account_or_channel_id,
                    display_name=source.display_name,
                    raw_source=source.raw_source,
                )
            await onboarding_repo.update_state(current_step="sources_imported")
            return len(imported)
        finally:
            await self.stop()

    async def login_platform_once(
        self,
        *,
        platform: str,
        login_url: str | None = None,
        wait_for_enter: bool = True,
    ) -> None:
        self.settings.ensure_dirs()
        target_platform = platform.strip().lower()
        default_urls = {
            "x": "https://x.com/i/flow/login",
            "linkedin": "https://www.linkedin.com/login",
            "youtube": "https://accounts.google.com/signin/v2/identifier?service=youtube",
            "substack": "https://substack.com/sign-in",
            "medium": "https://medium.com/m/signin",
            "spotify": "https://accounts.spotify.com/en/login",
            "apple_podcasts": "https://podcasts.apple.com/us/library/shows",
        }
        url = login_url or default_urls.get(target_platform)
        if not url:
            raise ValueError(
                "Unsupported platform for login flow. Use one of: "
                "x, linkedin, youtube, substack, medium, spotify, apple_podcasts"
            )

        manager = BrowserSessionManager(
            profiles_root=self.settings.browser_profiles_dir,
            headless=False,
            headful_platforms={target_platform},
            browser_channel=self.settings.browser_channel,
        )
        await manager.start()
        page_closed = False
        try:
            page = await manager.new_page(target_platform)
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(1000)
            if wait_for_enter:
                print(
                    f"Browser opened for {target_platform} login.\n"
                    "Complete login manually, then press Enter here to store session and continue."
                )
                await asyncio.to_thread(input, "")
            else:
                print(
                    f"Browser opened for {target_platform} login.\n"
                    "Complete login manually, then close the browser window to store session and continue."
                )
                await page.wait_for_event("close", timeout=0)
                page_closed = True
            activated = 0
            db = Database(path=self.settings.db_path)
            await db.connect()
            await db.initialize()
            try:
                if db.conn is None:
                    raise RuntimeError("Database connection unavailable.")
                activated = await SourceService(
                    source_repo=SourceRepository(conn=db.conn),
                    subject_repo=SubjectRepository(conn=db.conn),
                ).mark_platform_active_after_login(target_platform)
            finally:
                await db.close()
            print(f"Saved {target_platform} browser profile at: {self.settings.browser_profiles_dir / target_platform}")
            if activated:
                print(f"Marked {activated} {target_platform} source(s) active after login.")
            if not page_closed and not page.is_closed():
                await page.close()
        finally:
            await manager.stop()
