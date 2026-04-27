from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable

from pcca.app import PCCAApp
from pcca.config import Settings
from pcca.db import Database
from pcca.repositories.onboarding import OnboardingRepository
from pcca.repositories.preferences import SubjectPreferenceRepository
from pcca.repositories.routing import RoutingRepository
from pcca.repositories.sources import SourceRepository
from pcca.repositories.subjects import SubjectRepository
from pcca.services.preference_service import PreferenceService
from pcca.services.routing_service import RoutingService
from pcca.services.session_capture_service import SessionCaptureService
from pcca.services.source_service import SourceService
from pcca.services.subject_service import SubjectService

logger = logging.getLogger(__name__)

SUPPORTED_ONBOARDING_PLATFORMS = [
    "x",
    "linkedin",
    "youtube",
    "substack",
    "medium",
    "spotify",
    "apple_podcasts",
]


@dataclass
class CommandResult:
    ok: bool
    message: str
    data: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {"ok": self.ok, "message": self.message, "data": self.data}


@dataclass
class SmokeEvaluation:
    ok: bool
    items_collected: int
    deliveries_sent: int
    message: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def cron_to_digest_time(cron: str) -> str:
    parts = cron.split()
    if len(parts) < 2:
        return "08:30"
    minute, hour = parts[0], parts[1]
    try:
        return f"{int(hour):02d}:{int(minute):02d}"
    except ValueError:
        return "08:30"


def digest_time_to_cron(digest_time: str) -> str:
    hour, minute = (digest_time.strip() or "08:30").split(":", 1)
    return f"{int(minute)} {int(hour)} * * *"


def _read_env_lines(path: Path) -> list[str]:
    return path.read_text(encoding="utf-8").splitlines() if path.exists() else []


def write_env_values(values: dict[str, str], path: Path = Path(".env")) -> None:
    existing = _read_env_lines(path)
    seen: set[str] = set()
    output: list[str] = []
    for raw_line in existing:
        if not raw_line.strip() or raw_line.lstrip().startswith("#") or "=" not in raw_line:
            output.append(raw_line)
            continue
        key, _ = raw_line.split("=", 1)
        normalized_key = key.strip()
        if normalized_key in values:
            output.append(f"{normalized_key}={values[normalized_key]}")
            seen.add(normalized_key)
        else:
            output.append(raw_line)
    for key, value in values.items():
        if key not in seen:
            output.append(f"{key}={value}")
    path.write_text("\n".join(output) + "\n", encoding="utf-8")


def evaluate_smoke_result(nightly_stats: dict[str, Any], digest_stats: dict[str, Any] | None) -> SmokeEvaluation:
    items_collected = int(nightly_stats.get("items_collected") or 0)
    deliveries_sent = int((digest_stats or {}).get("deliveries_sent") or 0)
    if items_collected < 1:
        return SmokeEvaluation(
            ok=False,
            items_collected=items_collected,
            deliveries_sent=deliveries_sent,
            message=(
                "Smoke crawl collected 0 items. Re-check captured sessions and staged sources, "
                "then run Smoke Crawl + Test Digest again."
            ),
        )
    if deliveries_sent < 1:
        return SmokeEvaluation(
            ok=False,
            items_collected=items_collected,
            deliveries_sent=deliveries_sent,
            message=(
                "Smoke digest was composed but not delivered. Send /start to your Telegram bot "
                "and make sure the subject is linked to that chat."
            ),
        )
    return SmokeEvaluation(
        ok=True,
        items_collected=items_collected,
        deliveries_sent=deliveries_sent,
        message=f"Smoke crawl: {items_collected} items collected, {deliveries_sent} deliveries sent.",
    )


class DesktopCommandService:
    """Shared business operations for CLI onboarding and the desktop web UI."""

    def __init__(self, settings_factory: Callable[[], Settings] = Settings.from_env) -> None:
        self._settings_factory = settings_factory
        self._agent_app: PCCAApp | None = None
        self._agent_task: asyncio.Task | None = None
        self._logs: list[str] = []

    @property
    def logs(self) -> list[str]:
        return list(self._logs[-200:])

    def log(self, message: str) -> None:
        self._logs.append(message)
        logger.info("%s", message)

    def settings(self) -> Settings:
        return self._settings_factory()

    async def startup_for_wizard(self) -> CommandResult:
        self.log("Wizard startup: initializing local storage and starting local agent.")
        try:
            await self.init_db()
        except Exception as exc:
            logger.exception("Wizard startup DB initialization failed.")
            self.log(f"Wizard startup failed during DB initialization: {exc}")
            return CommandResult(False, f"Wizard startup failed during DB initialization: {exc}")

        try:
            await self.start_agent()
        except Exception as exc:
            # Keep the wizard usable so the user can fix config (most often an
            # invalid/missing Telegram token) without relaunching the app.
            logger.exception("Wizard startup agent start failed.")
            self.log(f"Wizard opened, but auto-starting the agent failed: {exc}")
            return CommandResult(False, f"Wizard opened, but auto-starting the agent failed: {exc}")

        return CommandResult(True, "Wizard startup complete. Local agent is running.")

    async def init_db(self) -> CommandResult:
        started_at = time.monotonic()
        settings = self.settings()
        settings.ensure_dirs()
        db = Database(path=settings.db_path)
        await db.connect()
        try:
            await db.initialize()
            if db.conn is None:
                raise RuntimeError("Database connection unavailable.")
            onboarding_repo = OnboardingRepository(conn=db.conn)
            state = await onboarding_repo.get_state()
            if state.current_step == "start":
                await onboarding_repo.update_state(current_step="db_initialized")
        finally:
            await db.close()
        self.log(f"Database initialized at {settings.db_path} in {int((time.monotonic() - started_at) * 1000)}ms.")
        return CommandResult(True, f"Database initialized at {settings.db_path}.")

    async def get_state(self) -> dict[str, Any]:
        settings = self.settings()
        settings.ensure_dirs()
        db = Database(path=settings.db_path)
        await db.connect()
        await db.initialize()
        try:
            if db.conn is None:
                raise RuntimeError("Database connection unavailable.")
            onboarding_repo = OnboardingRepository(conn=db.conn)
            state = await onboarding_repo.get_state()
            staged = await onboarding_repo.list_sources(status=None)
            subject_repo = SubjectRepository(conn=db.conn)
            source_service = SourceService(
                source_repo=SourceRepository(conn=db.conn),
                subject_repo=subject_repo,
            )
            subjects = await SubjectService(repository=subject_repo).list_subjects()
            reauth_sources = await source_service.list_sources_needing_reauth()
            return {
                "settings": {
                    "timezone": settings.timezone,
                    "digest_time": cron_to_digest_time(settings.morning_cron),
                    "telegram_token_configured": bool(settings.telegram_bot_token),
                    "data_dir": str(settings.data_dir),
                    "db_path": str(settings.db_path),
                    "log_file": str(settings.data_dir / "logs" / "pcca.log"),
                    "debug_dir": str(settings.data_dir / "debug"),
                    "browser_channel": settings.browser_channel or "bundled",
                    "session_refresh_enabled": settings.session_refresh_enabled,
                    "session_refresh_cooldown_seconds": settings.session_refresh_cooldown_seconds,
                    "session_refresh_browser": settings.session_refresh_browser or "auto",
                },
                "onboarding": {
                    "current_step": state.current_step,
                    "timezone": state.timezone,
                    "digest_time": state.digest_time,
                    "telegram_verified": state.telegram_verified,
                    "subject_name": state.subject_name,
                    "include_terms": state.include_terms,
                    "exclude_terms": state.exclude_terms,
                    "high_quality_examples": state.high_quality_examples,
                    "completed_at": state.completed_at,
                },
                "staged_sources": [asdict(row) for row in staged],
                "reauth_sources": [asdict(row) for row in reauth_sources],
                "subjects": [asdict(subject) for subject in subjects],
                "platforms": SUPPORTED_ONBOARDING_PLATFORMS,
                "agent_running": self.agent_running,
                "logs": self.logs,
            }
        finally:
            await db.close()

    @property
    def agent_running(self) -> bool:
        return self._agent_task is not None and not self._agent_task.done()

    async def save_runtime_settings(self, *, token: str, timezone: str, digest_time: str) -> CommandResult:
        was_running = self.agent_running
        values = {
            "PCCA_TELEGRAM_BOT_TOKEN": token.strip(),
            "PCCA_TIMEZONE": timezone.strip() or "UTC",
            "PCCA_MORNING_CRON": digest_time_to_cron(digest_time),
        }
        write_env_values(values)
        os.environ.update(values)
        settings = self.settings()
        settings.ensure_dirs()
        db = Database(path=settings.db_path)
        await db.connect()
        await db.initialize()
        try:
            if db.conn is None:
                raise RuntimeError("Database connection unavailable.")
            await OnboardingRepository(conn=db.conn).update_state(
                current_step="runtime_configured",
                timezone=values["PCCA_TIMEZONE"],
                digest_time=digest_time.strip() or "08:30",
                telegram_verified=False,
            )
        finally:
            await db.close()
        self.log("Runtime settings saved. Telegram token is configured but not printed for safety.")
        restart_warning = ""
        if was_running:
            self.log("Restarting local agent to apply runtime settings.")
            try:
                await self.stop_agent()
                await self.start_agent()
            except Exception as exc:
                logger.exception("Failed to restart local agent after runtime settings update.")
                restart_warning = f" Runtime settings were saved, but agent restart failed: {exc}"
                self.log(restart_warning.strip())
        return CommandResult(
            True,
            f"Runtime settings saved.{restart_warning}",
            {"telegram_token_configured": bool(token.strip()), "agent_running": self.agent_running},
        )

    async def start_agent(self) -> CommandResult:
        if self.agent_running:
            return CommandResult(True, "Agent is already running.", {"agent_running": True})
        started_at = time.monotonic()
        app = PCCAApp(settings=self.settings())
        task = asyncio.create_task(app.run_forever())
        self._agent_app = app
        self._agent_task = task
        self.log("Starting local agent.")
        await asyncio.sleep(0.2)
        if task.done():
            exc = task.exception()
            self._agent_task = None
            self._agent_app = None
            raise RuntimeError(f"Agent failed to start: {exc}")
        self.log(f"Local agent started in {int((time.monotonic() - started_at) * 1000)}ms.")
        return CommandResult(True, "Agent started.", {"agent_running": True})

    async def stop_agent(self) -> CommandResult:
        if self._agent_task is None:
            return CommandResult(True, "Agent is not running.", {"agent_running": False})
        started_at = time.monotonic()
        self._agent_task.cancel()
        try:
            await self._agent_task
        except asyncio.CancelledError:
            pass
        finally:
            self._agent_task = None
            self._agent_app = None
        self.log(f"Local agent stopped in {int((time.monotonic() - started_at) * 1000)}ms.")
        return CommandResult(True, "Agent stopped.", {"agent_running": False})

    async def open_login_window(self, *, platform: str) -> CommandResult:
        platform = platform.strip().lower()
        if platform not in SUPPORTED_ONBOARDING_PLATFORMS:
            raise ValueError(f"Unsupported login platform: {platform}")
        self.log(f"Opening login window for {platform}.")
        app = PCCAApp(settings=self.settings())
        await app.login_platform_once(platform=platform, wait_for_enter=False)
        self.log(f"Login window closed for {platform}; session profile saved.")
        return CommandResult(True, f"Saved {platform} login session.")

    async def capture_session(self, *, platform: str, browser: str | None = None) -> CommandResult:
        platform = platform.strip().lower()
        result = await SessionCaptureService(settings=self.settings()).capture_and_inject(
            platform=platform,
            browser=browser,
        )
        settings = self.settings()
        activated = 0
        db = Database(path=settings.db_path)
        await db.connect()
        await db.initialize()
        try:
            if db.conn is None:
                raise RuntimeError("Database connection unavailable.")
            if result.ok:
                activated = await SourceService(
                    source_repo=SourceRepository(conn=db.conn),
                    subject_repo=SubjectRepository(conn=db.conn),
                ).mark_platform_active_after_login(platform)
        finally:
            await db.close()
        self.log(
            f"Captured {platform} session from {result.browser}/{result.profile_name}; "
            f"injected {result.injected_cookie_count} cookie(s)."
        )
        message = (
            f"Captured {platform} session from {result.browser}/{result.profile_name}."
            if result.ok
            else f"Captured partial {platform} session; missing: {', '.join(result.missing_cookie_names)}."
        )
        return CommandResult(
            result.ok,
            message,
            {
                "session_capture": result.safe_summary(),
                "reactivated_sources": activated,
            },
        )

    async def stage_follows(self, *, platform: str, limit: int = 100) -> CommandResult:
        platform = platform.strip().lower()
        if platform not in SUPPORTED_ONBOARDING_PLATFORMS:
            raise ValueError(f"Unsupported follow-import platform: {platform}")
        started_at = time.monotonic()
        self.log(f"Staging follows from {platform} with limit={limit}.")
        app = PCCAApp(settings=self.settings())
        count = await app.stage_follows_once(platform=platform, limit=limit)
        self.log(f"Staged {count} source(s) from {platform} in {int((time.monotonic() - started_at) * 1000)}ms.")
        return CommandResult(True, f"Staged {count} source(s) from {platform}.", {"count": count})

    async def list_staged_sources(self) -> CommandResult:
        settings = self.settings()
        settings.ensure_dirs()
        db = Database(path=settings.db_path)
        await db.connect()
        await db.initialize()
        try:
            if db.conn is None:
                raise RuntimeError("Database connection unavailable.")
            rows = await OnboardingRepository(conn=db.conn).list_sources(status="pending")
        finally:
            await db.close()
        return CommandResult(True, "Loaded staged sources.", {"sources": [asdict(row) for row in rows]})

    async def remove_staged_source(self, *, source_id: int) -> CommandResult:
        settings = self.settings()
        settings.ensure_dirs()
        db = Database(path=settings.db_path)
        await db.connect()
        await db.initialize()
        try:
            if db.conn is None:
                raise RuntimeError("Database connection unavailable.")
            removed = await OnboardingRepository(conn=db.conn).mark_removed(source_id)
        finally:
            await db.close()
        if removed:
            self.log(f"Removed staged source id={source_id}.")
            return CommandResult(True, f"Removed staged source id={source_id}.")
        return CommandResult(False, f"No pending staged source found for id={source_id}.")

    async def confirm_staged_sources(
        self,
        *,
        subject: str,
        include_terms: list[str] | None = None,
        exclude_terms: list[str] | None = None,
        high_quality_examples: str | None = None,
    ) -> CommandResult:
        include_terms = include_terms or []
        exclude_terms = exclude_terms or []
        subject_name = subject.strip()
        if not subject_name:
            raise ValueError("Subject name is required.")
        settings = self.settings()
        settings.ensure_dirs()
        db = Database(path=settings.db_path)
        await db.connect()
        await db.initialize()
        try:
            if db.conn is None:
                raise RuntimeError("Database connection unavailable.")
            subject_repo = SubjectRepository(conn=db.conn)
            subject_service = SubjectService(repository=subject_repo)
            created = await subject_service.create_subject(subject_name)
            source_service = SourceService(
                source_repo=SourceRepository(conn=db.conn),
                subject_repo=subject_repo,
            )
            onboarding_repo = OnboardingRepository(conn=db.conn)
            staged = await onboarding_repo.list_sources(status="pending")
            for row in staged:
                await source_service.add_source_to_subject(
                    subject_name=created.name,
                    platform=row.platform,
                    account_or_channel_id=row.account_or_channel_id,
                    display_name=row.display_name,
                )
            if staged:
                await onboarding_repo.mark_confirmed([row.id for row in staged])
            if include_terms or exclude_terms:
                await PreferenceService(
                    preference_repo=SubjectPreferenceRepository(conn=db.conn),
                    subject_repo=subject_repo,
                ).refine_subject_rules(
                    subject_name=created.name,
                    include_terms=include_terms,
                    exclude_terms=exclude_terms,
                )
            new_routes = await RoutingService(
                routing_repo=RoutingRepository(conn=db.conn),
                subject_repo=subject_repo,
            ).ensure_routes_for_subject(subject_name=created.name)
            await onboarding_repo.update_state(
                current_step="subject_confirmed",
                subject_name=created.name,
                include_terms=include_terms,
                exclude_terms=exclude_terms,
                high_quality_examples=high_quality_examples,
                completed=False,
            )
        finally:
            await db.close()
        self.log(f"Created subject '{subject_name}' and confirmed {len(staged)} staged source(s).")
        return CommandResult(
            True,
            f"Created subject '{subject_name}' and confirmed {len(staged)} staged source(s).",
            {"subject": subject_name, "confirmed_sources": len(staged), "new_routes": new_routes},
        )

    async def run_smoke_crawl_and_digest(self) -> CommandResult:
        self.log("Running smoke crawl.")
        nightly_app = PCCAApp(settings=self.settings())
        nightly_stats = await nightly_app.run_nightly_once()
        self.log(f"Smoke crawl finished: {json.dumps(nightly_stats, sort_keys=True)}")

        self.log("Running test digest.")
        digest_app = PCCAApp(settings=self.settings())
        digest_stats = await digest_app.run_digest_once()
        self.log(f"Test digest finished: {json.dumps(digest_stats or {}, sort_keys=True)}")

        evaluation = evaluate_smoke_result(nightly_stats, digest_stats)
        settings = self.settings()
        db = Database(path=settings.db_path)
        await db.connect()
        await db.initialize()
        try:
            if db.conn is None:
                raise RuntimeError("Database connection unavailable.")
            await OnboardingRepository(conn=db.conn).update_state(
                current_step="completed" if evaluation.ok else "smoke_failed",
                completed=evaluation.ok,
            )
        finally:
            await db.close()
        self.log(evaluation.message)
        return CommandResult(
            evaluation.ok,
            evaluation.message,
            {
                "nightly_stats": nightly_stats,
                "digest_stats": digest_stats or {},
                "smoke": evaluation.to_dict(),
            },
        )

    async def rebuild_todays_digest(self) -> CommandResult:
        self.log("Rebuilding today's digest.")
        app = PCCAApp(settings=self.settings())
        stats = await app.rebuild_digest_once()
        self.log(f"Digest rebuild finished: {json.dumps(stats or {}, sort_keys=True)}")
        return CommandResult(
            True,
            "Rebuilt today's digest.",
            {"digest_stats": stats or {}},
        )

    async def shutdown(self) -> None:
        self.log("Wizard shutdown: stopping local agent.")
        await self.stop_agent()
