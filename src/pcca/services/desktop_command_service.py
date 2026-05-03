from __future__ import annotations

import asyncio
import inspect
import json
import logging
import os
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable

from pcca.app import PCCAApp
from pcca.collectors.errors import SessionChallengedError, SourceNotFoundError
from pcca.config import Settings
from pcca.db import Database
from pcca.repositories.onboarding import OnboardingRepository
from pcca.repositories.preferences import SubjectPreferenceRepository
from pcca.repositories.routing import RoutingRepository
from pcca.repositories.sources import SourceRepository
from pcca.repositories.subject_drafts import DESKTOP_SUBJECT_DRAFT_CHAT_ID, SubjectDraft, SubjectDraftRepository
from pcca.repositories.subjects import SubjectRepository
from pcca.services.model_router import ModelRouter
from pcca.services.preference_extraction_service import PreferenceExtractionService, draft_has_actionable_rules
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


@dataclass
class InflightAction:
    key: str
    label: str
    started_at: str
    task: asyncio.Task
    action_id: str


@dataclass
class CompletedAction:
    action_id: str
    key: str
    label: str
    finished_at: str
    result: CommandResult
    expires_at_monotonic: float


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
                "then run Smoke Crawl + Test Briefs again."
            ),
        )
    if deliveries_sent < 1:
        return SmokeEvaluation(
            ok=False,
            items_collected=items_collected,
            deliveries_sent=deliveries_sent,
            message=(
                "Smoke Briefs were composed but not delivered. Send /start to your Telegram bot "
                "and make sure the subject is linked to that chat."
            ),
        )
    return SmokeEvaluation(
        ok=True,
        items_collected=items_collected,
        deliveries_sent=deliveries_sent,
        message=f"Smoke crawl: {items_collected} items collected, {deliveries_sent} Brief delivery route(s) sent.",
    )


class DesktopCommandService:
    """Shared business operations for CLI onboarding and the desktop web UI."""

    def __init__(self, settings_factory: Callable[[], Settings] = Settings.from_env) -> None:
        self._settings_factory = settings_factory
        self._agent_app: PCCAApp | None = None
        self._agent_task: asyncio.Task | None = None
        self._inflight_actions: dict[str, InflightAction] = {}
        self._completed_actions: dict[str, CompletedAction] = {}
        self._known_action_expiry: dict[str, float] = {}
        self._action_result_ttl_seconds = 300
        self._inflight_lock = asyncio.Lock()
        self._logs: list[str] = []

    @property
    def logs(self) -> list[str]:
        return list(self._logs[-200:])

    def log(self, message: str) -> None:
        self._logs.append(message)
        logger.info("%s", message)

    def inflight_actions(self) -> list[dict[str, str]]:
        out: list[dict[str, str]] = []
        stale: list[str] = []
        for key, action in self._inflight_actions.items():
            if action.task.done():
                stale.append(key)
                continue
            out.append(
                {
                    "key": action.key,
                    "label": action.label,
                    "started_at": action.started_at,
                    "action_id": action.action_id,
                }
            )
        for key in stale:
            self._inflight_actions.pop(key, None)
        return out

    def _prune_completed_actions(self) -> None:
        now = time.monotonic()
        expired = [
            action_id
            for action_id, completed in self._completed_actions.items()
            if completed.expires_at_monotonic <= now
        ]
        for action_id in expired:
            self._completed_actions.pop(action_id, None)

    def _set_inflight_label(self, *, key: str, label: str) -> None:
        action = self._inflight_actions.get(key)
        if action is not None and not action.task.done():
            action.label = label

    async def _run_guarded_action(
        self,
        *,
        key: str,
        label: str,
        runner: Callable[[], Awaitable[CommandResult]],
        action_id: str | None = None,
    ) -> CommandResult:
        action_id = action_id or f"{key}-{time.time_ns()}"
        async with self._inflight_lock:
            existing = self._inflight_actions.get(key)
            if existing is not None and not existing.task.done():
                message = f"A {existing.label} run is already in progress; check Logs."
                self.log(message)
                return CommandResult(
                    False,
                    message,
                    {
                        "already_running": True,
                        "pending": True,
                        "action_id": existing.action_id,
                        "inflight": self.inflight_actions(),
                    },
                )
            task = asyncio.create_task(runner())
            self._inflight_actions[key] = InflightAction(
                key=key,
                label=label,
                started_at=datetime.now(timezone.utc).isoformat(),
                task=task,
                action_id=action_id,
            )
        try:
            return await task
        finally:
            async with self._inflight_lock:
                current = self._inflight_actions.get(key)
                if current is not None and current.task is task:
                    self._inflight_actions.pop(key, None)

    async def _dispatch_guarded_action(
        self,
        *,
        key: str,
        label: str,
        action_id: str,
        runner: Callable[[], Awaitable[CommandResult]],
    ) -> CommandResult:
        async with self._inflight_lock:
            self._prune_completed_actions()
            existing = self._inflight_actions.get(key)
            if existing is not None and not existing.task.done():
                message = f"{existing.label} is already running; watching the existing run."
                self.log(message)
                return CommandResult(
                    True,
                    message,
                    {
                        "accepted": True,
                        "already_running": True,
                        "pending": True,
                        "action_id": existing.action_id,
                        "inflight": self.inflight_actions(),
                    },
                )

            async def background_runner() -> CommandResult:
                try:
                    result = await runner()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.exception("Background desktop action failed action_id=%s key=%s", action_id, key)
                    result = CommandResult(
                        False,
                        f"{label} failed: {exc}",
                        {
                            "error_type": type(exc).__name__,
                            "failure_class": self._failure_class_for_exception(exc),
                        },
                    )
                await self._complete_dispatched_action(
                    key=key,
                    action_id=action_id,
                    label=label,
                    result=result,
                )
                return result

            task = asyncio.create_task(background_runner())
            self._inflight_actions[key] = InflightAction(
                key=key,
                label=label,
                started_at=datetime.now(timezone.utc).isoformat(),
                task=task,
                action_id=action_id,
            )

        self.log(f"{label} started in the background action_id={action_id}.")
        return CommandResult(
            True,
            f"{label} started.",
            {
                "accepted": True,
                "pending": True,
                "action_id": action_id,
                "inflight": self.inflight_actions(),
            },
        )

    async def _complete_dispatched_action(
        self,
        *,
        key: str,
        action_id: str,
        label: str,
        result: CommandResult,
    ) -> None:
        finished_at = datetime.now(timezone.utc).isoformat()
        expires_at = time.monotonic() + self._action_result_ttl_seconds
        result.data = {
            **(result.data or {}),
            "action_id": action_id,
            "finished_at": finished_at,
            "expires_in_seconds": self._action_result_ttl_seconds,
        }
        async with self._inflight_lock:
            current = self._inflight_actions.get(key)
            if current is not None and current.action_id == action_id:
                self._inflight_actions.pop(key, None)
            self._completed_actions[action_id] = CompletedAction(
                action_id=action_id,
                key=key,
                label=label,
                finished_at=finished_at,
                result=result,
                expires_at_monotonic=expires_at,
            )
            self._known_action_expiry[action_id] = expires_at
            self._prune_completed_actions()

    def _failure_class_for_exception(self, exc: Exception) -> str | None:
        if isinstance(exc, SessionChallengedError):
            return "session_challenge"
        if isinstance(exc, SourceNotFoundError):
            return "not_found"
        return None

    async def get_action_result(self, *, action_id: str) -> tuple[int, CommandResult]:
        normalized = action_id.strip()
        if not normalized:
            return 400, CommandResult(False, "Action id is required.", {"action_id": action_id})
        async with self._inflight_lock:
            self._prune_completed_actions()
            for action in self._inflight_actions.values():
                if action.action_id == normalized and not action.task.done():
                    return 404, CommandResult(
                        False,
                        "Action is still running.",
                        {"pending": True, "action_id": normalized, "inflight": self.inflight_actions()},
                    )
            completed = self._completed_actions.get(normalized)
            if completed is not None:
                return 200, completed.result
            expiry = self._known_action_expiry.get(normalized)
            if expiry is not None and expiry <= time.monotonic():
                return 410, CommandResult(
                    False,
                    "Action result expired. Run the action again if needed.",
                    {"expired": True, "action_id": normalized},
                )
        return 404, CommandResult(
            False,
            "Action result is not ready or was not found.",
            {"pending": True, "action_id": normalized},
        )

    async def record_wizard_event(self, event: dict[str, Any]) -> CommandResult:
        allowed_kinds = {"fetch_error", "timeout", "success"}
        event_kind = str(event.get("event_kind") or "").strip()
        if event_kind not in allowed_kinds:
            raise ValueError("Unsupported wizard event kind.")
        action_key = str(event.get("action_key") or "")[:80]
        action_id = str(event.get("action_id") or "")[:120]
        timestamp = str(event.get("timestamp") or datetime.now(timezone.utc).isoformat())[:80]
        error_type = str(event.get("error_type") or "")[:80]
        error_message = str(event.get("error_message") or "")[:500]
        elapsed_ms = int(event.get("elapsed_ms") or 0)
        http_status = int(event.get("http_status") or 0) or None
        logger.info(
            "[wizard] event=%s action_key=%s action_id=%s elapsed_ms=%s http_status=%s error_type=%s message=%s",
            event_kind,
            action_key,
            action_id,
            elapsed_ms,
            http_status,
            error_type,
            error_message,
        )
        settings = self.settings()
        settings.ensure_dirs()
        db = Database(path=settings.db_path)
        await db.connect()
        await db.initialize()
        try:
            if db.conn is None:
                raise RuntimeError("Database connection unavailable.")
            await db.conn.execute(
                """
                INSERT INTO wizard_events(
                  timestamp, action_key, action_id, event_kind, elapsed_ms,
                  http_status, error_type, error_message
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    timestamp,
                    action_key,
                    action_id,
                    event_kind,
                    elapsed_ms,
                    http_status,
                    error_type,
                    error_message,
                ),
            )
            await db.conn.execute(
                """
                DELETE FROM wizard_events
                WHERE id NOT IN (
                  SELECT id FROM wizard_events ORDER BY id DESC LIMIT 200
                )
                """
            )
            await db.conn.commit()
        finally:
            await db.close()
        return CommandResult(True, "Wizard event recorded.")

    def settings(self) -> Settings:
        return self._settings_factory()

    def preference_extractor(self) -> PreferenceExtractionService:
        settings = self.settings()
        model_router = ModelRouter(
            enabled=settings.ollama_enabled,
            ollama_base_url=settings.ollama_base_url,
            ollama_model=settings.ollama_model,
            timeout_seconds=settings.model_router_timeout_seconds,
        )
        return PreferenceExtractionService(model_router=model_router)

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
            preference_repo = SubjectPreferenceRepository(conn=db.conn)
            subject_preferences = {}
            subject_source_overrides = {}
            for subject in subjects:
                pref = await preference_repo.get_latest(subject.id)
                if pref is None:
                    subject_preferences[str(subject.id)] = {
                        "version": 0,
                        "include_terms": [],
                        "exclude_terms": [],
                        "updated_at": None,
                    }
                else:
                    subject_preferences[str(subject.id)] = {
                        "version": pref.version,
                        "include_terms": list(pref.include_rules.get("topics") or []),
                        "exclude_terms": list(pref.exclude_rules.get("topics") or []),
                        "quality_rules": pref.quality_rules,
                        "updated_at": pref.updated_at,
                    }
                subject_source_overrides[str(subject.id)] = [
                    asdict(row)
                    for row in await source_service.list_source_overrides_for_subject(subject.id)
                    if row.status != "active"
                ]
            draft_repo = SubjectDraftRepository(conn=db.conn)
            subject_draft = await draft_repo.get(DESKTOP_SUBJECT_DRAFT_CHAT_ID)
            subject_drafts = await draft_repo.list_all()
            routing_service = RoutingService(
                routing_repo=RoutingRepository(conn=db.conn),
                subject_repo=subject_repo,
            )
            routes = await routing_service.list_all_routes()
            chats = await routing_service.list_registered_chats()
            reauth_sources = await source_service.list_sources_needing_reauth()
            monitored_sources = await source_service.list_all_sources()
            run_log_rows = await (
                await db.conn.execute(
                    """
                    SELECT id, run_type, started_at, ended_at, status, stats_json, metadata_json
                    FROM run_logs
                    ORDER BY id DESC
                    LIMIT 10
                    """
                )
            ).fetchall()
            recent_run_logs = []
            circuit_broken: list[str] = []
            circuit_broken_reasons_by_platform: dict[str, str] = {}
            embedding_degraded: dict[str, Any] = {"degraded": False}
            embedding_not_warmed: dict[str, Any] = {"not_warmed": False}
            for row in run_log_rows:
                try:
                    metadata = json.loads(row["metadata_json"] or "{}")
                except json.JSONDecodeError:
                    metadata = {}
                if row["run_type"] == "nightly_collection" and not circuit_broken:
                    broken = metadata.get("circuit_broken") if isinstance(metadata, dict) else None
                    if isinstance(broken, list):
                        circuit_broken = [str(item) for item in broken]
                    reasons = metadata.get("circuit_broken_reasons_by_platform") if isinstance(metadata, dict) else None
                    if isinstance(reasons, dict):
                        circuit_broken_reasons_by_platform = {
                            str(key): str(value) for key, value in reasons.items()
                        }
                if row["run_type"] in {"nightly_collection", "embedding_rescore"} and not embedding_degraded.get("degraded"):
                    if isinstance(metadata, dict) and metadata.get("embedding_degraded"):
                        subjects_payload = metadata.get("embedding_degraded_subjects")
                        embedding_degraded = {
                            "degraded": True,
                            "run_id": row["id"],
                            "run_type": row["run_type"],
                            "subjects": subjects_payload if isinstance(subjects_payload, list) else [],
                            "fallback_items": int(metadata.get("embedding_fallback_items") or 0),
                            "items_scored": int(metadata.get("embedding_items_scored") or 0),
                        }
                if row["run_type"] in {"nightly_collection", "embedding_rescore"} and not embedding_not_warmed.get("not_warmed"):
                    if isinstance(metadata, dict) and metadata.get("embedding_not_warmed"):
                        subjects_payload = metadata.get("embedding_not_warmed_subjects")
                        embedding_not_warmed = {
                            "not_warmed": True,
                            "run_id": row["id"],
                            "run_type": row["run_type"],
                            "subjects": subjects_payload if isinstance(subjects_payload, list) else [],
                        }
                recent_run_logs.append(
                    {
                        "id": row["id"],
                        "run_type": row["run_type"],
                        "started_at": row["started_at"],
                        "ended_at": row["ended_at"],
                        "status": row["status"],
                        "metadata": metadata,
                    }
                )
            pending_staged = [row for row in staged if row.status == "pending"]
            staged_counts: dict[str, int] = {}
            for row in pending_staged:
                staged_counts[row.platform] = staged_counts.get(row.platform, 0) + 1
            return {
                "settings": {
                    "timezone": settings.timezone,
                    "digest_time": cron_to_digest_time(settings.morning_cron),
                    "telegram_token_configured": bool(settings.telegram_bot_token),
                    "telegram_status": (
                        "ready"
                        if settings.telegram_bot_token
                        else "Telegram service is disabled - token is missing. Add your bot token in Config."
                    ),
                    "telegram_token_missing": not bool(settings.telegram_bot_token),
                    "data_dir": str(settings.data_dir),
                    "db_path": str(settings.db_path),
                    "log_file": str(settings.data_dir / "logs" / "pcca.log"),
                    "debug_dir": str(settings.data_dir / "debug"),
                    "browser_channel": settings.browser_channel or "bundled",
                    "session_refresh_enabled": settings.session_refresh_enabled,
                    "session_refresh_cooldown_seconds": settings.session_refresh_cooldown_seconds,
                    "session_refresh_browser": settings.session_refresh_browser or "auto",
                    "platform_circuit_threshold": settings.platform_circuit_threshold,
                    "platform_empty_threshold": settings.platform_empty_threshold,
                    "scorer": settings.scorer,
                    "embedding_model": settings.embedding_model,
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
                "monitored_sources": [asdict(row) for row in monitored_sources],
                "staged_counts": staged_counts,
                "pending_staged_count": len(pending_staged),
                "reauth_sources": [asdict(row) for row in reauth_sources],
                "recent_run_logs": recent_run_logs,
                "circuit_broken": circuit_broken,
                "circuit_broken_reasons_by_platform": circuit_broken_reasons_by_platform,
                "embedding_degraded": embedding_degraded,
                "embedding_not_warmed": embedding_not_warmed,
                "subjects": [asdict(subject) for subject in subjects],
                "subject_preferences": subject_preferences,
                "subject_source_overrides": subject_source_overrides,
                "subject_draft": asdict(subject_draft) if subject_draft is not None else None,
                "subject_draft_actionable": (
                    draft_has_actionable_rules(subject_draft) if subject_draft is not None else False
                ),
                "subject_drafts": [
                    {**asdict(draft), "actionable": draft_has_actionable_rules(draft)}
                    for draft in subject_drafts
                ],
                "routes": [asdict(route) for route in routes],
                "chats": [asdict(chat) for chat in chats],
                "platforms": SUPPORTED_ONBOARDING_PLATFORMS,
                "agent_running": self.agent_running,
                "inflight_actions": self.inflight_actions(),
                "logs": self.logs,
            }
        finally:
            await db.close()

    @property
    def agent_running(self) -> bool:
        return self._agent_task is not None and not self._agent_task.done()

    async def save_runtime_settings(self, *, token: str, timezone: str, digest_time: str) -> CommandResult:
        was_running = self.agent_running
        stripped_token = token.strip()
        values = {
            "PCCA_TIMEZONE": timezone.strip() or "UTC",
            "PCCA_MORNING_CRON": digest_time_to_cron(digest_time),
        }
        if stripped_token:
            values["PCCA_TELEGRAM_BOT_TOKEN"] = stripped_token
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
        if stripped_token:
            self.log("Runtime settings saved. Telegram token was updated but not printed for safety.")
        else:
            self.log("Runtime settings saved. Blank Telegram token field preserved the existing token, if any.")
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
            {"telegram_token_configured": bool(settings.telegram_bot_token), "agent_running": self.agent_running},
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

    async def stage_follows(
        self,
        *,
        platform: str,
        limit: int = 100,
        async_response: bool = False,
        action_id: str | None = None,
    ) -> CommandResult:
        platform = platform.strip().lower()
        all_platforms = platform in {"", "all"}
        if not all_platforms and platform not in SUPPORTED_ONBOARDING_PLATFORMS:
            raise ValueError(f"Unsupported follow-import platform: {platform}")

        async def runner() -> CommandResult:
            started_at = time.monotonic()
            platforms = SUPPORTED_ONBOARDING_PLATFORMS if all_platforms else [platform]
            counts: dict[str, int] = {}
            errors: dict[str, str] = {}
            total_count = 0
            for index, current_platform in enumerate(platforms, start=1):
                self._set_inflight_label(
                    key="stage_follows",
                    label=f"Get Sources: staging {current_platform} ({index}/{len(platforms)})",
                )
                self.log(
                    f"Staging {current_platform} follows ({index}/{len(platforms)}) with limit={limit}."
                )
                try:
                    app = PCCAApp(settings=self.settings())
                    count = await app.stage_follows_once(platform=current_platform, limit=limit)
                    counts[current_platform] = count
                    total_count += count
                    self.log(
                        f"Staged {count} source(s) from {current_platform} "
                        f"in {int((time.monotonic() - started_at) * 1000)}ms."
                    )
                except Exception as exc:
                    errors[current_platform] = str(exc)
                    self.log(f"Staging {current_platform} failed: {exc}")
                    if not all_platforms:
                        raise
            elapsed_ms = int((time.monotonic() - started_at) * 1000)
            if all_platforms:
                suffix = f" {len(errors)} platform(s) failed; check Debug logs." if errors else ""
                return CommandResult(
                    True,
                    f"Staged {total_count} source(s) across {len(platforms)} platform(s) in {elapsed_ms}ms.{suffix}",
                    {"count": total_count, "counts": counts, "errors": errors, "platform": None},
                )
            return CommandResult(
                True,
                f"Staged {total_count} source(s) from {platform}.",
                {"count": total_count, "counts": counts, "errors": errors, "platform": platform},
            )

        if async_response:
            if action_id is None:
                raise ValueError("action_id is required for async desktop actions.")
            return await self._dispatch_guarded_action(
                key="stage_follows",
                label="Get Sources",
                action_id=action_id,
                runner=runner,
            )
        return await self._run_guarded_action(key="stage_follows", label="Get Sources", runner=runner)

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

    async def unlink_subject_route(
        self,
        *,
        subject_id: int,
        chat_id: int,
        thread_id: str | None = None,
    ) -> CommandResult:
        settings = self.settings()
        settings.ensure_dirs()
        db = Database(path=settings.db_path)
        await db.connect()
        await db.initialize()
        try:
            if db.conn is None:
                raise RuntimeError("Database connection unavailable.")
            removed = await RoutingService(
                routing_repo=RoutingRepository(conn=db.conn),
                subject_repo=SubjectRepository(conn=db.conn),
            ).unlink_subject_route(subject_id=subject_id, chat_id=chat_id, thread_id=thread_id)
        finally:
            await db.close()
        if removed:
            self.log(f"Unlinked subject_id={subject_id} from chat_id={chat_id} thread_id={thread_id or ''}.")
            return CommandResult(True, "Route unlinked.")
        return CommandResult(False, "Route was already absent.")

    async def move_subject_route(
        self,
        *,
        subject_id: int,
        from_chat_id: int,
        from_thread_id: str | None,
        to_chat_id: int,
    ) -> CommandResult:
        settings = self.settings()
        settings.ensure_dirs()
        db = Database(path=settings.db_path)
        await db.connect()
        await db.initialize()
        try:
            if db.conn is None:
                raise RuntimeError("Database connection unavailable.")
            moved = await RoutingService(
                routing_repo=RoutingRepository(conn=db.conn),
                subject_repo=SubjectRepository(conn=db.conn),
            ).move_subject_route(
                subject_id=subject_id,
                from_chat_id=from_chat_id,
                from_thread_id=from_thread_id,
                to_chat_id=to_chat_id,
                to_thread_id=None,
            )
        finally:
            await db.close()
        if moved:
            self.log(f"Moved subject_id={subject_id} route from chat_id={from_chat_id} to chat_id={to_chat_id}.")
            return CommandResult(True, "Route moved.")
        return CommandResult(True, "Route already points there.")

    async def monitor_staged_sources(self) -> CommandResult:
        settings = self.settings()
        settings.ensure_dirs()
        db = Database(path=settings.db_path)
        await db.connect()
        await db.initialize()
        try:
            if db.conn is None:
                raise RuntimeError("Database connection unavailable.")
            subject_repo = SubjectRepository(conn=db.conn)
            source_service = SourceService(
                source_repo=SourceRepository(conn=db.conn),
                subject_repo=subject_repo,
            )
            onboarding_repo = OnboardingRepository(conn=db.conn)
            staged = await onboarding_repo.list_sources(status="pending")
            for row in staged:
                await source_service.monitor_source(
                    platform=row.platform,
                    account_or_channel_id=row.account_or_channel_id,
                    display_name=row.display_name,
                )
            if staged:
                await onboarding_repo.mark_confirmed([row.id for row in staged])
                await onboarding_repo.update_state(current_step="sources_reviewed")
        finally:
            await db.close()
        self.log(f"Now monitoring {len(staged)} staged source(s).")
        return CommandResult(
            True,
            f"Now monitoring {len(staged)} source(s). Run Read Content to collect; results will be checked for all subjects.",
            {"monitored_sources": len(staged)},
        )

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
        if not include_terms and not exclude_terms:
            raise ValueError("Subject preferences cannot be empty. Use Add Subject and describe what to include/avoid.")
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
            created = await subject_service.create_subject(
                subject_name,
                include_terms=include_terms,
                exclude_terms=exclude_terms,
            )
            source_service = SourceService(
                source_repo=SourceRepository(conn=db.conn),
                subject_repo=subject_repo,
            )
            onboarding_repo = OnboardingRepository(conn=db.conn)
            staged = await onboarding_repo.list_sources(status="pending")
            for row in staged:
                await source_service.monitor_source(
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
        self.log(f"Created subject '{subject_name}' and monitored {len(staged)} staged source(s).")
        return CommandResult(
            True,
            f"Created subject '{subject_name}' and monitored {len(staged)} staged source(s).",
            {"subject": subject_name, "monitored_sources": len(staged), "new_routes": new_routes},
        )

    async def draft_subject(self, *, text: str, subject_id: int | None = None) -> CommandResult:
        normalized = " ".join(text.split()).strip()
        if not normalized:
            raise ValueError("Describe the subject first.")
        settings = self.settings()
        settings.ensure_dirs()
        db = Database(path=settings.db_path)
        await db.connect()
        await db.initialize()
        try:
            if db.conn is None:
                raise RuntimeError("Database connection unavailable.")
            draft_repo = SubjectDraftRepository(conn=db.conn)
            previous = await draft_repo.get(DESKTOP_SUBJECT_DRAFT_CHAT_ID)
            if subject_id is not None and subject_id > 0:
                subject_repo = SubjectRepository(conn=db.conn)
                subject = await subject_repo.get_by_id(subject_id)
                pref = await SubjectPreferenceRepository(conn=db.conn).get_latest(subject.id)
                previous = SubjectDraft(
                    chat_id=DESKTOP_SUBJECT_DRAFT_CHAT_ID,
                    title=subject.name,
                    description_text=(
                        f"Existing subject: {subject.name}\n"
                        f"Include: {', '.join((pref.include_rules.get('topics') if pref else []) or [])}\n"
                        f"Avoid: {', '.join((pref.exclude_rules.get('topics') if pref else []) or [])}"
                    ),
                    include_terms=list((pref.include_rules.get("topics") if pref else []) or []),
                    exclude_terms=list((pref.exclude_rules.get("topics") if pref else []) or []),
                    quality_notes=None,
                    last_user_message="",
                    updated_at="",
                )
            draft = await self.preference_extractor().extract(normalized, previous=previous)
            if subject_id is not None and subject_id > 0:
                draft.title = previous.title
            saved = await draft_repo.upsert(
                chat_id=DESKTOP_SUBJECT_DRAFT_CHAT_ID,
                title=draft.title,
                description_text=draft.description_text,
                include_terms=draft.include_terms,
                exclude_terms=draft.exclude_terms,
                quality_notes=draft.quality_notes,
                last_user_message=normalized,
            )
        finally:
            await db.close()
        actionable = draft_has_actionable_rules(saved)
        message = (
            "Subject draft ready to save."
            if actionable
            else "Tell me more before saving: what should be included, avoided, or considered high quality?"
        )
        if draft.extraction_warning:
            message = f"{message} ⚠️ {draft.extraction_warning}"
        return CommandResult(
            True,
            message,
            {"draft": asdict(saved), "actionable": actionable},
        )

    async def confirm_subject_draft(self, *, chat_id: int | None = None) -> CommandResult:
        draft_chat_id = chat_id if chat_id is not None else DESKTOP_SUBJECT_DRAFT_CHAT_ID
        settings = self.settings()
        settings.ensure_dirs()
        db = Database(path=settings.db_path)
        await db.connect()
        await db.initialize()
        try:
            if db.conn is None:
                raise RuntimeError("Database connection unavailable.")
            draft_repo = SubjectDraftRepository(conn=db.conn)
            draft = await draft_repo.get(draft_chat_id)
            if draft is None:
                raise ValueError("No subject draft is waiting to be saved.")
            if not draft_has_actionable_rules(draft):
                raise ValueError(
                    "Subject preferences are still too thin. Add what to include, what to avoid, "
                    "or an example of high-quality content."
                )
            subject_repo = SubjectRepository(conn=db.conn)
            subject_service = SubjectService(repository=subject_repo)
            existing = await subject_repo.get_by_name(draft.title)
            if existing is None:
                created = await subject_service.create_subject(
                    draft.title,
                    include_terms=draft.include_terms,
                    exclude_terms=draft.exclude_terms,
                    quality_notes=draft.quality_notes,
                    description_text=draft.description_text,
                )
            else:
                created = existing
                await PreferenceService(
                    preference_repo=SubjectPreferenceRepository(conn=db.conn),
                    subject_repo=subject_repo,
                ).refine_subject_rules(
                    subject_name=created.name,
                    include_terms=draft.include_terms,
                    exclude_terms=draft.exclude_terms,
                    quality_notes=draft.quality_notes,
                )
                await subject_repo.update_description(created.id, draft.description_text)
            routing_service = RoutingService(
                routing_repo=RoutingRepository(conn=db.conn),
                subject_repo=subject_repo,
            )
            if draft_chat_id > 0:
                await routing_service.link_subject_id(subject_id=created.id, chat_id=draft_chat_id)
                new_routes = 1
            else:
                new_routes = await routing_service.ensure_routes_for_subject(subject_name=created.name)
            await draft_repo.delete(draft_chat_id)
            await OnboardingRepository(conn=db.conn).update_state(
                current_step="subject_confirmed",
                subject_name=created.name,
                include_terms=draft.include_terms,
                exclude_terms=draft.exclude_terms,
                high_quality_examples=draft.quality_notes,
                completed=False,
            )
        finally:
            await db.close()
        self.log(f"Created subject '{created.name}' from free-form draft.")
        return CommandResult(
            True,
            f"Subject saved: {created.name}.",
            {"subject": created.name, "new_routes": new_routes},
        )

    async def cancel_subject_draft(self, *, chat_id: int | None = None) -> CommandResult:
        draft_chat_id = chat_id if chat_id is not None else DESKTOP_SUBJECT_DRAFT_CHAT_ID
        settings = self.settings()
        settings.ensure_dirs()
        db = Database(path=settings.db_path)
        await db.connect()
        await db.initialize()
        try:
            if db.conn is None:
                raise RuntimeError("Database connection unavailable.")
            await SubjectDraftRepository(conn=db.conn).delete(draft_chat_id)
        finally:
            await db.close()
        return CommandResult(True, "Subject draft cancelled.")

    async def rebuild_subject_rules(self, *, subject_id: int, text: str | None = None) -> CommandResult:
        settings = self.settings()
        settings.ensure_dirs()
        db = Database(path=settings.db_path)
        await db.connect()
        await db.initialize()
        try:
            if db.conn is None:
                raise RuntimeError("Database connection unavailable.")
            subject_repo = SubjectRepository(conn=db.conn)
            subject = await subject_repo.get_by_id(subject_id)
            preference_repo = SubjectPreferenceRepository(conn=db.conn)
            current = await preference_repo.get_latest(subject.id)
            stored_description = await subject_repo.get_description_text(subject.id)
            source_text = (text or stored_description or "").strip()
            if not source_text:
                include = list((current.include_rules.get("topics") if current else []) or [])
                exclude = list((current.exclude_rules.get("topics") if current else []) or [])
                quality_notes = (current.quality_rules.get("notes") if current else None) or ""
                source_text = "\n".join(
                    part
                    for part in (
                        f"Subject: {subject.name}",
                        f"Current include terms: {', '.join(include)}" if include else "",
                        f"Current avoid terms: {', '.join(exclude)}" if exclude else "",
                        f"Current quality notes: {quality_notes}" if quality_notes else "",
                        "Rebuild this into literal topic terms that would appear in matching content.",
                    )
                    if part
                )
            previous = SubjectDraft(
                chat_id=DESKTOP_SUBJECT_DRAFT_CHAT_ID,
                title=subject.name,
                description_text=source_text,
                include_terms=[],
                exclude_terms=[],
                quality_notes=(current.quality_rules.get("notes") if current else None),
                last_user_message="",
                updated_at="",
            )
            draft = await self.preference_extractor().extract(source_text, previous=previous)
            pref = await PreferenceService(preference_repo=preference_repo, subject_repo=subject_repo).replace_subject_rules(
                subject_id=subject.id,
                include_terms=draft.include_terms,
                exclude_terms=draft.exclude_terms,
                quality_notes=draft.quality_notes,
            )
        finally:
            await db.close()
        self.log(f"Rebuilt subject rules for subject_id={subject_id} version={pref.version}.")
        return CommandResult(
            True,
            f"Rebuilt rules for {subject.name} -> version {pref.version}.",
            {
                "subject_id": subject.id,
                "version": pref.version,
                "include_terms": pref.include_rules.get("topics", []),
                "exclude_terms": pref.exclude_rules.get("topics", []),
                "quality_rules": pref.quality_rules,
            },
        )

    async def rebuild_all_subject_rules(
        self,
        *,
        async_response: bool = False,
        action_id: str | None = None,
    ) -> CommandResult:
        async def runner() -> CommandResult:
            settings = self.settings()
            settings.ensure_dirs()
            db = Database(path=settings.db_path)
            await db.connect()
            await db.initialize()
            try:
                if db.conn is None:
                    raise RuntimeError("Database connection unavailable.")
                subjects = await SubjectRepository(conn=db.conn).list_all()
            finally:
                await db.close()

            rebuilt: list[dict[str, Any]] = []
            errors: list[dict[str, Any]] = []
            for index, subject in enumerate(subjects, start=1):
                self._set_inflight_label(
                    key="rebuild_all_subject_rules",
                    label=f"Rebuild All Subjects: rules {index}/{len(subjects)}",
                )
                try:
                    result = await self.rebuild_subject_rules(subject_id=subject.id)
                    rebuilt.append(
                        {
                            "subject_id": subject.id,
                            "subject_name": subject.name,
                            "version": (result.data or {}).get("version"),
                        }
                    )
                except Exception as exc:
                    self.log(f"Failed to rebuild rules for subject_id={subject.id}: {exc}")
                    logger.exception("Failed to rebuild subject rules subject_id=%s", subject.id)
                    errors.append({"subject_id": subject.id, "subject_name": subject.name, "error": str(exc)})

            self._set_inflight_label(
                key="rebuild_all_subject_rules",
                label="Rebuild All Subjects: warming embeddings",
            )

            def progress(event: dict[str, Any]) -> None:
                self.log(
                    "Embedding backfill after subject rebuild "
                    f"{event.get('kind')}: {event.get('processed')}/{event.get('total')}"
                )

            effective_concurrency = self.settings().embedding_backfill_concurrency
            if self._agent_app is not None and self.agent_running:
                embedding_stats = await self._agent_app.backfill_embeddings_current(
                    concurrency=effective_concurrency,
                    rescore=False,
                    include_segments=True,
                    progress_callback=progress,
                )
            else:
                app = PCCAApp(settings=self.settings())
                embedding_stats = await app.run_embedding_backfill_once(
                    concurrency=effective_concurrency,
                    rescore=False,
                    include_segments=True,
                    progress_callback=progress,
                )
            ok = not errors
            message = (
                f"Rebuilt {len(rebuilt)} subject(s) and warmed embeddings."
                if ok
                else f"Rebuilt {len(rebuilt)} subject(s), {len(errors)} failed; warmed embeddings afterward."
            )
            self.log(message)
            return CommandResult(
                ok,
                message,
                {
                    "subjects_total": len(subjects),
                    "subjects_rebuilt": len(rebuilt),
                    "subjects_failed": len(errors),
                    "rebuilt": rebuilt,
                    "errors": errors,
                    "embedding_stats": embedding_stats or {},
                },
            )

        if async_response:
            if action_id is None:
                raise ValueError("action_id is required for async desktop actions.")
            return await self._dispatch_guarded_action(
                key="rebuild_all_subject_rules",
                label="Rebuild All Subjects",
                action_id=action_id,
                runner=runner,
            )
        return await self._run_guarded_action(
            key="rebuild_all_subject_rules",
            label="Rebuild All Subjects",
            runner=runner,
        )

    async def reassign_subject_route(self, *, subject_id: int, chat_id: int) -> CommandResult:
        settings = self.settings()
        settings.ensure_dirs()
        db = Database(path=settings.db_path)
        await db.connect()
        await db.initialize()
        try:
            if db.conn is None:
                raise RuntimeError("Database connection unavailable.")
            subject_repo = SubjectRepository(conn=db.conn)
            subject = await subject_repo.get_by_id(subject_id)
            routing_service = RoutingService(
                routing_repo=RoutingRepository(conn=db.conn),
                subject_repo=subject_repo,
            )
            routes = await routing_service.list_routes_for_subject(subject_id)
            if routes:
                moved = await routing_service.move_subject_route(
                    subject_id=subject_id,
                    from_chat_id=routes[0].chat_id,
                    from_thread_id=routes[0].thread_id,
                    to_chat_id=chat_id,
                    to_thread_id=None,
                )
                changed = 1 if moved else 0
            else:
                await routing_service.link_subject_id(subject_id=subject_id, chat_id=chat_id)
                changed = 1
        finally:
            await db.close()
        self.log(f"Route assigned subject_id={subject_id} chat_id={chat_id}.")
        return CommandResult(
            True,
            f"Route assigned for {subject.name}.",
            {"subject_id": subject_id, "chat_id": chat_id, "changed": changed},
        )

    async def run_smoke_crawl_and_digest(self) -> CommandResult:
        self.log("Running smoke crawl.")
        nightly_app = PCCAApp(settings=self.settings())
        nightly_stats = await nightly_app.run_nightly_once()
        self.log(f"Smoke crawl finished: {json.dumps(nightly_stats, sort_keys=True)}")

        self.log("Running test Briefs.")
        digest_stats = await self._run_briefs_with_available_agent()
        self.log(f"Test Briefs finished: {json.dumps(digest_stats or {}, sort_keys=True)}")

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

    async def read_content(
        self,
        *,
        platform: str | None = None,
        async_response: bool = False,
        action_id: str | None = None,
    ) -> CommandResult:
        platform_filter = platform.strip().lower() if platform and platform.strip() else None

        async def runner() -> CommandResult:
            label = f" for {platform_filter}" if platform_filter else " for all platforms"
            self.log(f"Reading content now{label}. Phase: collecting.")

            def progress(event: dict[str, Any]) -> None:
                if event.get("phase") == "embedding" or event.get("kind") in {"subjects", "items", "segments", "auto_backfill"}:
                    self._set_inflight_label(
                        key="read_content",
                        label=f"Get Content: embedding {event.get('kind')} {event.get('processed')}/{event.get('total')}",
                    )
                    self.log(
                        "Reading content phase: embedding "
                        f"{event.get('kind')}: {event.get('processed')}/{event.get('total')}"
                    )
                elif event.get("kind") == "scoring":
                    self._set_inflight_label(
                        key="read_content",
                        label=(
                            f"Get Content: scoring {event.get('subject_name')} "
                            f"({event.get('subject_index')}/{event.get('subject_total')})"
                        ),
                    )
                    self.log(
                        "Reading content phase: scoring "
                        f"{event.get('subject_name')} ({event.get('subject_index')}/{event.get('subject_total')})"
                    )
                elif event.get("kind") == "embedding_not_warmed":
                    self._set_inflight_label(
                        key="read_content",
                        label=f"Get Content: embeddings not warmed for {event.get('subject_name')}; using keyword fallback",
                    )
                    self.log(
                        "Reading content warning: embeddings not warmed "
                        f"for {event.get('subject_name')} missing_rate={event.get('missing_rate')}"
                    )

            if self._agent_app is not None and self.agent_running and hasattr(self._agent_app, "pipeline_orchestrator"):
                runner_method = self._agent_app.pipeline_orchestrator.run_nightly_collection
                kwargs = {"platform": platform_filter}
                if "score" in inspect.signature(runner_method).parameters:
                    kwargs["score"] = False
                if "progress_callback" in inspect.signature(runner_method).parameters:
                    kwargs["progress_callback"] = progress
                stats = await runner_method(**kwargs)
            else:
                app = PCCAApp(settings=self.settings())
                stats = await app.run_nightly_once(platform=platform_filter, score=False, progress_callback=progress)
            if stats.get("skipped_already_running"):
                message = "Content collection is already running; check Logs."
                self.log(message)
                return CommandResult(
                    False,
                    message,
                    {
                        "already_running": True,
                        "nightly_stats": stats or {},
                        "platform": platform_filter,
                    },
                )
            self.log(f"Read content finished: {json.dumps(stats or {}, sort_keys=True)}")
            embedding_pending = bool((stats or {}).get("embedding_pending"))
            message = (
                f"Content read finished for {platform_filter}."
                if platform_filter
                else "Content read finished for all platforms."
            )
            if embedding_pending:
                message += " Embedding is pending; keyword fallback remains available."
            elif (stats or {}).get("embedding_backfill", {}).get("enabled"):
                message += " New embeddings are warmed."
            return CommandResult(
                True,
                message,
                {
                    "nightly_stats": stats or {},
                    "platform": platform_filter,
                    "embedding_pending": embedding_pending,
                },
            )

        if async_response:
            if action_id is None:
                raise ValueError("action_id is required for async desktop actions.")
            return await self._dispatch_guarded_action(
                key="read_content",
                label="Get Content",
                action_id=action_id,
                runner=runner,
            )
        return await self._run_guarded_action(key="read_content", label="Get Content", runner=runner)

    async def backfill_embeddings(
        self,
        *,
        concurrency: int | None = None,
        limit: int | None = None,
        rescore: bool = True,
        include_segments: bool = True,
        async_response: bool = False,
        action_id: str | None = None,
    ) -> CommandResult:
        async def runner() -> CommandResult:
            started_at = time.monotonic()
            effective_concurrency = (
                int(concurrency)
                if concurrency is not None
                else self.settings().embedding_backfill_concurrency
            )
            self.log(
                "Backfilling embeddings "
                f"concurrency={max(1, effective_concurrency)} limit={limit or 'all'} "
                f"rescore={rescore} include_segments={include_segments}."
            )

            def progress(event: dict[str, Any]) -> None:
                if event.get("kind") == "scoring":
                    self._set_inflight_label(
                        key="embedding_backfill",
                        label=(
                            f"Backfill Embeddings: scoring {event.get('subject_name')} "
                            f"({event.get('subject_index')}/{event.get('subject_total')})"
                        ),
                    )
                elif event.get("kind") == "embedding_not_warmed":
                    self._set_inflight_label(
                        key="embedding_backfill",
                        label=f"Backfill Embeddings: embeddings not warmed for {event.get('subject_name')}; using keyword fallback",
                    )
                else:
                    self._set_inflight_label(
                        key="embedding_backfill",
                        label=f"Backfill Embeddings: {event.get('kind')} {event.get('processed')}/{event.get('total')}",
                    )
                self.log(
                    "Embedding backfill "
                    f"{event.get('kind')}: {event.get('processed')}/{event.get('total')}"
                )

            if self._agent_app is not None and self.agent_running:
                stats = await self._agent_app.backfill_embeddings_current(
                    concurrency=effective_concurrency,
                    limit=limit,
                    rescore=rescore,
                    include_segments=include_segments,
                    progress_callback=progress,
                )
            else:
                app = PCCAApp(settings=self.settings())
                stats = await app.run_embedding_backfill_once(
                    concurrency=effective_concurrency,
                    limit=limit,
                    rescore=rescore,
                    include_segments=include_segments,
                    progress_callback=progress,
                )
            elapsed_ms = int((time.monotonic() - started_at) * 1000)
            self.log(f"Embedding backfill finished in {elapsed_ms}ms: {json.dumps(stats or {}, sort_keys=True)}")
            backfill_stats = stats.get("backfill", {}) if isinstance(stats, dict) else {}
            if backfill_stats and not backfill_stats.get("enabled", True):
                return CommandResult(
                    False,
                    "Embedding backfill skipped because Ollama embeddings are disabled. Enable PCCA_OLLAMA_ENABLED and PCCA_SCORER=embedding or both.",
                    {"embedding_stats": stats or {}},
                )
            return CommandResult(True, "Embedding backfill finished.", {"embedding_stats": stats or {}})

        if async_response:
            if action_id is None:
                raise ValueError("action_id is required for async desktop actions.")
            return await self._dispatch_guarded_action(
                key="embedding_backfill",
                label="Backfill Embeddings",
                action_id=action_id,
                runner=runner,
            )
        return await self._run_guarded_action(
            key="embedding_backfill",
            label="Backfill Embeddings",
            runner=runner,
        )

    async def get_briefs(
        self,
        *,
        subject_id: int | None = None,
        async_response: bool = False,
        action_id: str | None = None,
    ) -> CommandResult:
        async def runner() -> CommandResult:
            subject_ids = {subject_id} if subject_id is not None and subject_id > 0 else None
            self.log(f"Getting Briefs subject_ids={sorted(subject_ids) if subject_ids else 'all'}.")

            def progress(event: dict[str, Any]) -> None:
                if event.get("kind") == "embedding_not_warmed":
                    self._set_inflight_label(
                        key="get_briefs",
                        label=f"Get Briefs: embeddings not warmed for {event.get('subject_name')}; using keyword fallback",
                    )
                    self.log(
                        "Briefs warning: embeddings not warmed "
                        f"for {event.get('subject_name')} missing_rate={event.get('missing_rate')}"
                    )
                    return
                if event.get("kind") != "scoring":
                    return
                self._set_inflight_label(
                    key="get_briefs",
                    label=(
                        f"Get Briefs: scoring {event.get('subject_name')} "
                        f"({event.get('subject_index')}/{event.get('subject_total')})"
                    ),
                )
                self.log(
                    "Briefs phase: scoring "
                    f"{event.get('subject_name')} ({event.get('subject_index')}/{event.get('subject_total')})"
                )

            stats = await self._run_briefs_with_available_agent(
                subject_ids=subject_ids,
                progress_callback=progress,
            )
            self.log(f"Briefs finished: {json.dumps(stats or {}, sort_keys=True)}")
            return CommandResult(
                True,
                "Briefs sent.",
                {"digest_stats": stats or {}},
            )

        if async_response:
            if action_id is None:
                raise ValueError("action_id is required for async desktop actions.")
            return await self._dispatch_guarded_action(
                key="get_briefs",
                label="Get Briefs",
                action_id=action_id,
                runner=runner,
            )
        return await self._run_guarded_action(key="get_briefs", label="Get Briefs", runner=runner)

    async def rebuild_todays_digest(self) -> CommandResult:
        self.log("Rebuilding today's Briefs.")
        stats = await self._rebuild_briefs_with_available_agent()
        self.log(f"Brief rebuild finished: {json.dumps(stats or {}, sort_keys=True)}")
        return CommandResult(
            True,
            "Rebuilt today's Briefs.",
            {"digest_stats": stats or {}},
        )

    async def _run_briefs_with_available_agent(
        self,
        *,
        subject_ids: set[int] | None = None,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict:
        if self._agent_app is not None and self.agent_running and hasattr(self._agent_app, "scheduler"):
            self.log("Using running local agent for Brief delivery.")
            return await self._agent_app.scheduler.job_runner.run_smart_briefs(
                subject_ids=subject_ids,
                progress_callback=progress_callback,
            )
        self.log("Local agent is unavailable; starting one-shot Brief delivery.")
        app = PCCAApp(settings=self.settings())
        return await app.run_briefs_once(subject_ids=subject_ids, progress_callback=progress_callback)

    async def _rebuild_briefs_with_available_agent(self) -> dict:
        if self._agent_app is not None and self.agent_running and hasattr(self._agent_app, "scheduler"):
            self.log("Using running local agent to rebuild Briefs.")
            return await self._agent_app.scheduler.job_runner.rebuild_todays_digest()
        self.log("Local agent is unavailable; starting one-shot Brief rebuild.")
        app = PCCAApp(settings=self.settings())
        return await app.rebuild_briefs_once()

    async def shutdown(self) -> None:
        self.log("Wizard shutdown: stopping local agent.")
        await self.stop_agent()
