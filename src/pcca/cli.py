from __future__ import annotations

import argparse
import asyncio
import json
import logging
from pathlib import Path
from typing import Sequence

from pcca.app import PCCAApp
from pcca.config import Settings
from pcca.db import Database
from pcca.logging_utils import configure_logging
from pcca.repositories.routing import RoutingRepository
from pcca.repositories.onboarding import OnboardingRepository
from pcca.repositories.sources import SourceRepository
from pcca.repositories.subjects import SubjectRepository
from pcca.repositories.preferences import SubjectPreferenceRepository
from pcca.services.preference_service import PreferenceService
from pcca.services.routing_service import RoutingService
from pcca.services.source_discovery_service import SourceDiscoveryService
from pcca.services.source_service import SourceService
from pcca.services.subject_service import SubjectService
from pcca.services.desktop_command_service import DesktopCommandService
from pcca.services.debug_bundle_service import create_debug_bundle


async def _init_db(settings: Settings) -> None:
    settings.ensure_dirs()
    db = Database(path=settings.db_path)
    await db.connect()
    try:
        await db.initialize()
    finally:
        await db.close()


async def _create_subject(
    settings: Settings,
    name: str,
    thread_id: str | None,
    *,
    include_terms: list[str],
    exclude_terms: list[str],
) -> None:
    settings.ensure_dirs()
    db = Database(path=settings.db_path)
    await db.connect()
    await db.initialize()
    try:
        if db.conn is None:
            raise RuntimeError("Database connection unavailable.")
        subject_repo = SubjectRepository(conn=db.conn)
        subject_service = SubjectService(repository=subject_repo)
        subject = await subject_service.create_subject(
            name,
            telegram_thread_id=thread_id,
            include_terms=include_terms,
            exclude_terms=exclude_terms,
        )
        routing_service = RoutingService(
            routing_repo=RoutingRepository(conn=db.conn),
            subject_repo=subject_repo,
        )
        new_routes = await routing_service.ensure_routes_for_subject(subject_name=subject.name)
        print(f"Subject ready: {subject.name} (id={subject.id})")
        if new_routes:
            print(f"Linked subject to {new_routes} registered Telegram chat(s).")
    finally:
        await db.close()


async def _list_subjects(settings: Settings) -> None:
    settings.ensure_dirs()
    db = Database(path=settings.db_path)
    await db.connect()
    await db.initialize()
    try:
        if db.conn is None:
            raise RuntimeError("Database connection unavailable.")
        service = SubjectService(repository=SubjectRepository(conn=db.conn))
        subjects = await service.list_subjects()
        if not subjects:
            print("No subjects configured.")
            return
        for subject in subjects:
            print(f"{subject.id}\t{subject.name}\t{subject.status}\tthread={subject.telegram_thread_id}")
    finally:
        await db.close()


async def _add_source(
    settings: Settings,
    subject: str,
    platform: str,
    source_id: str,
    display_name: str | None,
    priority: int,
) -> None:
    settings.ensure_dirs()
    db = Database(path=settings.db_path)
    await db.connect()
    await db.initialize()
    try:
        if db.conn is None:
            raise RuntimeError("Database connection unavailable.")
        subject_repo = SubjectRepository(conn=db.conn)
        source_repo = SourceRepository(conn=db.conn)
        service = SourceService(source_repo=source_repo, subject_repo=subject_repo)
        candidate = source_id.strip()
        if candidate.startswith(("http://", "https://")):
            discovery = SourceDiscoveryService()
            discovered = await discovery.discover(candidate)
            matched = [row for row in discovered if row.platform == platform]
            if matched:
                for row in matched:
                    await service.add_source_to_subject(
                        subject_name=subject,
                        platform=row.platform,
                        account_or_channel_id=row.source_id,
                        display_name=display_name or row.display_name,
                        priority=priority,
                    )
                    print(f"Source linked: [{row.platform}] {row.source_id} -> subject '{subject}' ({row.reason})")
                return

        await service.add_source_to_subject(
            subject_name=subject,
            platform=platform,
            account_or_channel_id=candidate,
            display_name=display_name,
            priority=priority,
        )
        print(f"Source linked: [{platform}] {candidate} -> subject '{subject}'")
    finally:
        await db.close()


async def _remove_source(
    settings: Settings,
    subject: str,
    platform: str,
    source_id: str,
) -> None:
    settings.ensure_dirs()
    db = Database(path=settings.db_path)
    await db.connect()
    await db.initialize()
    try:
        if db.conn is None:
            raise RuntimeError("Database connection unavailable.")
        subject_repo = SubjectRepository(conn=db.conn)
        source_repo = SourceRepository(conn=db.conn)
        service = SourceService(source_repo=source_repo, subject_repo=subject_repo)
        candidate = source_id.strip()
        removed_any = False
        if candidate.startswith(("http://", "https://")):
            discovery = SourceDiscoveryService()
            discovered = await discovery.discover(candidate)
            matched = [row for row in discovered if row.platform == platform]
            for row in matched:
                removed = await service.remove_source_from_subject(
                    subject_name=subject,
                    platform=row.platform,
                    account_or_channel_id=row.source_id,
                )
                if removed:
                    print(f"Source removed: [{row.platform}] {row.source_id} from subject '{subject}'")
                removed_any = removed_any or removed
            if matched:
                if not removed_any:
                    print(f"Source was not active: [{platform}] {candidate} for subject '{subject}'")
                return

        removed = await service.remove_source_from_subject(
            subject_name=subject,
            platform=platform,
            account_or_channel_id=candidate,
        )
        if removed:
            print(f"Source removed: [{platform}] {candidate} from subject '{subject}'")
        else:
            print(f"Source was not active: [{platform}] {candidate} for subject '{subject}'")
    finally:
        await db.close()


async def _list_sources(settings: Settings, subject: str) -> None:
    settings.ensure_dirs()
    db = Database(path=settings.db_path)
    await db.connect()
    await db.initialize()
    try:
        if db.conn is None:
            raise RuntimeError("Database connection unavailable.")
        subject_repo = SubjectRepository(conn=db.conn)
        source_repo = SourceRepository(conn=db.conn)
        service = SourceService(source_repo=source_repo, subject_repo=subject_repo)
        rows = await service.list_sources_for_subject(subject)
        if not rows:
            print(f"No sources configured for subject '{subject}'.")
            return
        for row in rows:
            print(
                f"{row.source_id}\t{row.platform}\t{row.account_or_channel_id}\t"
                f"{row.display_name}\tpriority={row.priority}\tstatus={row.status}\t"
                f"last_crawled_at={row.last_crawled_at or 'never'}"
            )
    finally:
        await db.close()


async def _show_preferences(settings: Settings, subject: str) -> None:
    settings.ensure_dirs()
    db = Database(path=settings.db_path)
    await db.connect()
    await db.initialize()
    try:
        if db.conn is None:
            raise RuntimeError("Database connection unavailable.")
        service = PreferenceService(
            preference_repo=SubjectPreferenceRepository(conn=db.conn),
            subject_repo=SubjectRepository(conn=db.conn),
        )
        pref = await service.get_preferences_for_subject(subject)
        include_terms = pref.include_rules.get("topics", [])
        exclude_terms = pref.exclude_rules.get("topics", [])
        print(f"subject={subject} version={pref.version}")
        print(f"include={include_terms}")
        print(f"exclude={exclude_terms}")
    finally:
        await db.close()


async def _refine_preferences(
    settings: Settings,
    subject: str,
    include_terms: list[str],
    exclude_terms: list[str],
) -> None:
    settings.ensure_dirs()
    db = Database(path=settings.db_path)
    await db.connect()
    await db.initialize()
    try:
        if db.conn is None:
            raise RuntimeError("Database connection unavailable.")
        service = PreferenceService(
            preference_repo=SubjectPreferenceRepository(conn=db.conn),
            subject_repo=SubjectRepository(conn=db.conn),
        )
        pref = await service.refine_subject_rules(
            subject_name=subject,
            include_terms=include_terms,
            exclude_terms=exclude_terms,
        )
        print(f"Updated preferences for {subject} -> version {pref.version}")
    finally:
        await db.close()


async def _add_source_url(
    settings: Settings,
    subject: str,
    url: str,
    display_name: str | None,
    priority: int,
) -> None:
    settings.ensure_dirs()
    db = Database(path=settings.db_path)
    await db.connect()
    await db.initialize()
    try:
        if db.conn is None:
            raise RuntimeError("Database connection unavailable.")
        subject_repo = SubjectRepository(conn=db.conn)
        source_repo = SourceRepository(conn=db.conn)
        service = SourceService(source_repo=source_repo, subject_repo=subject_repo)
        discovery = SourceDiscoveryService()
        discovered = await discovery.discover(url)
        if not discovered:
            print(f"No supported source discovered from URL: {url}")
            return

        linked = 0
        for row in discovered:
            await service.add_source_to_subject(
                subject_name=subject,
                platform=row.platform,
                account_or_channel_id=row.source_id,
                display_name=display_name or row.display_name,
                priority=priority,
            )
            linked += 1
            print(f"Source linked: [{row.platform}] {row.source_id} -> subject '{subject}' ({row.reason})")
        print(f"Linked {linked} source(s) from URL.")
    finally:
        await db.close()


async def _link_subject_chat(settings: Settings, subject: str, chat_id: int, thread_id: str | None) -> None:
    settings.ensure_dirs()
    db = Database(path=settings.db_path)
    await db.connect()
    await db.initialize()
    try:
        if db.conn is None:
            raise RuntimeError("Database connection unavailable.")
        service = RoutingService(
            routing_repo=RoutingRepository(conn=db.conn),
            subject_repo=SubjectRepository(conn=db.conn),
        )
        await service.register_chat(chat_id=chat_id, title=None)
        await service.link_subject(subject_name=subject, chat_id=chat_id, thread_id=thread_id)
        print(f"Linked subject '{subject}' to chat_id={chat_id} thread_id={thread_id}")
    finally:
        await db.close()


async def _list_staged_sources(settings: Settings) -> None:
    settings.ensure_dirs()
    db = Database(path=settings.db_path)
    await db.connect()
    await db.initialize()
    try:
        if db.conn is None:
            raise RuntimeError("Database connection unavailable.")
        repo = OnboardingRepository(conn=db.conn)
        rows = await repo.list_sources(status="pending")
        if not rows:
            print("No staged onboarding sources.")
            return
        for row in rows:
            print(
                f"{row.id}\t{row.platform}\t{row.account_or_channel_id}\t"
                f"{row.display_name}\tstatus={row.status}"
            )
    finally:
        await db.close()


async def _remove_staged_source(settings: Settings, source_id: int) -> None:
    settings.ensure_dirs()
    db = Database(path=settings.db_path)
    await db.connect()
    await db.initialize()
    try:
        if db.conn is None:
            raise RuntimeError("Database connection unavailable.")
        removed = await OnboardingRepository(conn=db.conn).mark_removed(source_id)
        if removed:
            print(f"Removed staged source id={source_id}.")
        else:
            print(f"No pending staged source found for id={source_id}.")
    finally:
        await db.close()


async def _confirm_staged_sources(
    settings: Settings,
    *,
    subject: str,
    include_terms: list[str],
    exclude_terms: list[str],
    high_quality_examples: str | None,
) -> None:
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
            subject,
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
            pref_service = PreferenceService(
                preference_repo=SubjectPreferenceRepository(conn=db.conn),
                subject_repo=subject_repo,
            )
            await pref_service.refine_subject_rules(
                subject_name=created.name,
                include_terms=include_terms,
                exclude_terms=exclude_terms,
            )
        routing_service = RoutingService(
            routing_repo=RoutingRepository(conn=db.conn),
            subject_repo=subject_repo,
        )
        new_routes = await routing_service.ensure_routes_for_subject(subject_name=created.name)
        await onboarding_repo.update_state(
            current_step="completed",
            subject_name=created.name,
            include_terms=include_terms,
            exclude_terms=exclude_terms,
            high_quality_examples=high_quality_examples,
            completed=True,
        )
        print(f"Created subject '{created.name}' and monitored {len(staged)} staged source(s).")
        if new_routes:
            print(f"Linked subject to {new_routes} registered Telegram chat(s) for Brief delivery.")
        else:
            print(
                "No Telegram chat is registered yet. Send /start to your bot in Telegram "
                "to complete Brief routing."
            )
    finally:
        await db.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pcca", description="Personal Content Curation Agent")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init-db", help="Initialize local database schema")

    create_subject_parser = sub.add_parser("create-subject", help="Create a subject with non-empty preferences")
    create_subject_parser.add_argument("--name", required=True, help="Subject name")
    create_subject_parser.add_argument("--thread-id", required=False, help="Optional Telegram thread id")
    create_subject_parser.add_argument("--include", action="append", default=[], help="Include term (repeatable)")
    create_subject_parser.add_argument("--exclude", action="append", default=[], help="Exclude term (repeatable)")

    sub.add_parser("list-subjects", help="List configured subjects")

    add_source_parser = sub.add_parser("add-source", help="Link a source to a subject")
    add_source_parser.add_argument("--subject", required=True, help="Subject name")
    add_source_parser.add_argument(
        "--platform",
        required=True,
        help=(
            "Platform, e.g. x/linkedin/youtube/substack/reddit/spotify/"
            "apple_podcasts/medium/rss"
        ),
    )
    add_source_parser.add_argument("--source-id", required=True, help="Handle/channel/feed identifier")
    add_source_parser.add_argument("--display-name", required=False, help="Display name override")
    add_source_parser.add_argument("--priority", type=int, default=0, help="Source priority weight")

    remove_source_parser = sub.add_parser("remove-source", help="Deactivate source for a subject")
    remove_source_parser.add_argument("--subject", required=True, help="Subject name")
    remove_source_parser.add_argument("--platform", required=True, help="Platform")
    remove_source_parser.add_argument("--source-id", required=True, help="Handle/channel/feed identifier")

    add_source_url_parser = sub.add_parser(
        "add-source-url",
        help="Discover source(s) from URL and link them to a subject",
    )
    add_source_url_parser.add_argument("--subject", required=True, help="Subject name")
    add_source_url_parser.add_argument("--url", required=True, help="Source/profile/blog/podcast URL")
    add_source_url_parser.add_argument("--display-name", required=False, help="Display name override")
    add_source_url_parser.add_argument("--priority", type=int, default=0, help="Source priority weight")

    list_sources_parser = sub.add_parser("list-sources", help="List sources for one subject")
    list_sources_parser.add_argument("--subject", required=True, help="Subject name")

    show_preferences_parser = sub.add_parser("show-preferences", help="Show current preferences for a subject")
    show_preferences_parser.add_argument("--subject", required=True, help="Subject name")

    refine_preferences_parser = sub.add_parser("refine-preferences", help="Append include/exclude terms for a subject")
    refine_preferences_parser.add_argument("--subject", required=True, help="Subject name")
    refine_preferences_parser.add_argument("--include", action="append", default=[], help="Include term (repeatable)")
    refine_preferences_parser.add_argument("--exclude", action="append", default=[], help="Exclude term (repeatable)")

    rebuild_subject_rules_parser = sub.add_parser(
        "rebuild-subject-rules",
        help="Re-run free-form preference extraction for an existing subject",
    )
    rebuild_subject_rules_parser.add_argument("subject_id", type=int, help="Subject id")
    rebuild_subject_rules_parser.add_argument(
        "--text",
        required=False,
        help="Optional original subject description. If omitted, stored description/current rules are used.",
    )

    link_route_parser = sub.add_parser("link-subject-chat", help="Link subject delivery route to Telegram chat/thread")
    link_route_parser.add_argument("--subject", required=True, help="Subject name")
    link_route_parser.add_argument("--chat-id", required=True, type=int, help="Telegram chat id")
    link_route_parser.add_argument("--thread-id", required=False, help="Optional Telegram thread id")

    import_follows_parser = sub.add_parser(
        "import-follows",
        help=(
            "Import follows/subscriptions from logged-in browser session into one subject "
            "(x/linkedin/youtube/substack/medium/spotify/apple_podcasts)"
        ),
    )
    import_follows_parser.add_argument("--subject", required=True, help="Subject name")
    import_follows_parser.add_argument(
        "--platform",
        required=True,
        choices=["x", "linkedin", "youtube", "substack", "medium", "spotify", "apple_podcasts"],
        help="Platform",
    )
    import_follows_parser.add_argument("--limit", required=False, type=int, default=200, help="Maximum follows to import")

    stage_follows_parser = sub.add_parser(
        "stage-follows",
        help="Import follows/subscriptions into onboarding review queue before creating the first subject",
    )
    stage_follows_parser.add_argument(
        "--platform",
        required=True,
        choices=["x", "linkedin", "youtube", "substack", "medium", "spotify", "apple_podcasts"],
        help="Platform",
    )
    stage_follows_parser.add_argument("--limit", required=False, type=int, default=200, help="Maximum follows to stage")

    sub.add_parser("list-staged-sources", help="List pending onboarding sources before monitoring confirmation")

    remove_staged_parser = sub.add_parser("remove-staged-source", help="Remove one source from onboarding review")
    remove_staged_parser.add_argument("--id", required=True, type=int, help="Staged source id")

    confirm_staged_parser = sub.add_parser(
        "confirm-staged-sources",
        help="Create first subject and monitor any pending onboarding sources",
    )
    confirm_staged_parser.add_argument("--subject", required=True, help="Subject name")
    confirm_staged_parser.add_argument("--include", action="append", default=[], help="Include term (repeatable)")
    confirm_staged_parser.add_argument("--exclude", action="append", default=[], help="Exclude term (repeatable)")
    confirm_staged_parser.add_argument("--high-quality", required=False, help="High quality examples/notes")

    sub.add_parser(
        "monitor-staged-sources",
        help="Confirm pending onboarding sources into the global monitored source list",
    )

    login_parser = sub.add_parser(
        "login",
        help="Developer escape hatch: open the old automated browser login flow",
    )
    login_parser.add_argument(
        "--platform",
        required=True,
        choices=["x", "linkedin", "youtube", "substack", "medium", "spotify", "apple_podcasts"],
        help="Platform",
    )
    login_parser.add_argument("--url", required=False, help="Custom login URL")
    login_parser.add_argument(
        "--wait-until-closed",
        action="store_true",
        help="For desktop onboarding: store session after the user closes the login browser window",
    )

    capture_session_parser = sub.add_parser(
        "capture-session",
        help="Capture a logged-in session from your normal browser and inject it into PCCA",
    )
    capture_session_parser.add_argument(
        "--platform",
        required=True,
        choices=["x", "linkedin", "youtube", "substack", "medium", "spotify", "apple_podcasts"],
        help="Platform to capture from your normal browser session.",
    )
    capture_session_parser.add_argument(
        "--browser",
        required=False,
        choices=["chrome", "arc", "brave", "edge"],
        help="Browser to read from. Defaults to first browser with required cookies.",
    )

    nightly_parser = sub.add_parser("run-nightly-once", help="Run nightly collection pipeline once")
    nightly_parser.add_argument(
        "--no-backfill",
        action="store_true",
        help="Skip automatic embedding warm-up after collection.",
    )
    nightly_parser.add_argument(
        "--score",
        action="store_true",
        help="Also run the legacy per-subject scoring phase after collection.",
    )
    embed_backfill_parser = sub.add_parser(
        "embed-backfill",
        help="Warm missing Ollama embedding cache and optionally rescore existing items",
    )
    embed_backfill_parser.add_argument("--limit", required=False, type=int, help="Maximum items to embed/rescore")
    embed_backfill_parser.add_argument(
        "--concurrency",
        required=False,
        type=int,
        default=None,
        help=(
            "Maximum concurrent embedding requests. Defaults to "
            "PCCA_EMBEDDING_BACKFILL_CONCURRENCY (default 2). Lower values "
            "produce a cooler chip at the cost of slightly slower backfill."
        ),
    )
    embed_backfill_parser.add_argument(
        "--no-rescore",
        action="store_true",
        help="Only warm embeddings; do not rebuild existing item scores",
    )
    embed_backfill_parser.add_argument(
        "--include-segments",
        action="store_true",
        help="Also create segment rows and warm segment embeddings for long transcripts/articles.",
    )
    sub.add_parser("run-briefs-once", help="Run smart Brief sending once")
    sub.add_parser("rebuild-briefs-once", help="Force rebuild today's Briefs and send them")
    sub.add_parser("run-digest-once", help="Deprecated alias for run-briefs-once")
    sub.add_parser("rebuild-digest-once", help="Deprecated alias for rebuild-briefs-once")
    sub.add_parser("run-agent", help="Run scheduler + Telegram bot")
    sub.add_parser("run-desktop", help="Run desktop webview wizard for onboarding/control")
    debug_bundle_parser = sub.add_parser("debug-bundle", help="Create a local redacted debug bundle")
    debug_bundle_parser.add_argument("--output", required=False, help="Optional output .zip path")

    return parser


def main(argv: Sequence[str] | None = None) -> None:
    configure_logging()
    args = build_parser().parse_args(argv)
    settings = Settings.from_env()

    if args.command == "init-db":
        result = asyncio.run(DesktopCommandService().init_db())
        print(result.message)
        return

    if args.command == "create-subject":
        asyncio.run(
            _create_subject(
                settings,
                args.name,
                args.thread_id,
                include_terms=args.include,
                exclude_terms=args.exclude,
            )
        )
        return

    if args.command == "list-subjects":
        asyncio.run(_list_subjects(settings))
        return

    if args.command == "add-source":
        asyncio.run(
            _add_source(
                settings,
                subject=args.subject,
                platform=args.platform,
                source_id=args.source_id,
                display_name=args.display_name,
                priority=args.priority,
            )
        )
        return

    if args.command == "remove-source":
        asyncio.run(
            _remove_source(
                settings,
                subject=args.subject,
                platform=args.platform,
                source_id=args.source_id,
            )
        )
        return

    if args.command == "add-source-url":
        asyncio.run(
            _add_source_url(
                settings,
                subject=args.subject,
                url=args.url,
                display_name=args.display_name,
                priority=args.priority,
            )
        )
        return

    if args.command == "list-sources":
        asyncio.run(_list_sources(settings, subject=args.subject))
        return

    if args.command == "show-preferences":
        asyncio.run(_show_preferences(settings, subject=args.subject))
        return

    if args.command == "refine-preferences":
        asyncio.run(
            _refine_preferences(
                settings,
                subject=args.subject,
                include_terms=args.include,
                exclude_terms=args.exclude,
            )
        )
        return

    if args.command == "rebuild-subject-rules":
        result = asyncio.run(
            DesktopCommandService().rebuild_subject_rules(subject_id=args.subject_id, text=args.text)
        )
        print(result.message)
        print(f"include={result.data.get('include_terms', [])}")
        print(f"exclude={result.data.get('exclude_terms', [])}")
        return

    if args.command == "link-subject-chat":
        asyncio.run(_link_subject_chat(settings, subject=args.subject, chat_id=args.chat_id, thread_id=args.thread_id))
        return

    if args.command == "import-follows":
        app = PCCAApp(settings=settings)
        count = asyncio.run(
            app.import_follows_once(
                subject_name=args.subject,
                platform=args.platform,
                limit=args.limit,
            )
        )
        print(f"Imported {count} follows into subject '{args.subject}' from {args.platform}.")
        return

    if args.command == "stage-follows":
        result = asyncio.run(
            DesktopCommandService().stage_follows(platform=args.platform, limit=args.limit)
        )
        print(result.message)
        return

    if args.command == "list-staged-sources":
        result = asyncio.run(DesktopCommandService().list_staged_sources())
        sources = result.data.get("sources", [])
        if not sources:
            print("No staged onboarding sources.")
            return
        for row in sources:
            print(
                f"{row['id']}\t{row['platform']}\t{row['account_or_channel_id']}\t"
                f"{row['display_name']}\tstatus={row['status']}"
            )
        return

    if args.command == "remove-staged-source":
        result = asyncio.run(DesktopCommandService().remove_staged_source(source_id=args.id))
        print(result.message)
        return

    if args.command == "monitor-staged-sources":
        result = asyncio.run(DesktopCommandService().monitor_staged_sources())
        print(result.message)
        return

    if args.command == "confirm-staged-sources":
        result = asyncio.run(
            DesktopCommandService().confirm_staged_sources(
                subject=args.subject,
                include_terms=args.include,
                exclude_terms=args.exclude,
                high_quality_examples=args.high_quality,
            )
        )
        print(result.message)
        if result.data.get("new_routes"):
            print(f"Linked subject to {result.data['new_routes']} registered Telegram chat(s) for Brief delivery.")
        else:
            print(
                "No Telegram chat is registered yet. Send /start to your bot in Telegram "
                "to complete Brief routing."
            )
        return

    if args.command == "login":
        app = PCCAApp(settings=settings)
        asyncio.run(
            app.login_platform_once(
                platform=args.platform,
                login_url=args.url,
                wait_for_enter=not args.wait_until_closed,
            )
        )
        return

    if args.command == "capture-session":
        result = asyncio.run(
            DesktopCommandService().capture_session(platform=args.platform, browser=args.browser)
        )
        print(result.message)
        summary = result.data.get("session_capture", {})
        if summary:
            print(
                "Captured cookies: "
                f"{', '.join(summary.get('captured_cookie_names', [])) or '(none)'}"
            )
        return

    if args.command == "run-nightly-once":
        app = PCCAApp(settings=settings)
        stats = asyncio.run(app.run_nightly_once(auto_backfill=not args.no_backfill, score=args.score))
        print(f"Nightly run completed: {stats}")
        return

    if args.command == "embed-backfill":
        app = PCCAApp(settings=settings)

        def progress(event: dict) -> None:
            print(f"{event.get('kind')}: {event.get('processed')}/{event.get('total')}", flush=True)

        effective_concurrency = (
            int(args.concurrency)
            if args.concurrency is not None
            else settings.embedding_backfill_concurrency
        )
        stats = asyncio.run(
            app.run_embedding_backfill_once(
                concurrency=max(1, effective_concurrency),
                limit=args.limit,
                rescore=not args.no_rescore,
                include_segments=args.include_segments,
                progress_callback=progress,
            )
        )
        print(json.dumps(stats, indent=2, sort_keys=True))
        return

    if args.command in {"run-briefs-once", "run-digest-once"}:
        app = PCCAApp(settings=settings)
        asyncio.run(app.run_briefs_once())
        print("Brief run completed.")
        return

    if args.command in {"rebuild-briefs-once", "rebuild-digest-once"}:
        app = PCCAApp(settings=settings)
        stats = asyncio.run(app.rebuild_briefs_once())
        print(f"Brief rebuild completed: {stats}")
        return

    if args.command == "run-agent":
        app = PCCAApp(settings=settings)
        try:
            asyncio.run(app.run_forever())
        except KeyboardInterrupt:
            logging.getLogger(__name__).info("Agent interrupted by user.")
        return

    if args.command == "run-desktop":
        from pcca.desktop_shell import run_desktop_shell

        run_desktop_shell()
        return

    if args.command == "debug-bundle":
        bundle = create_debug_bundle(settings, output=Path(args.output) if args.output else None)
        print(f"Debug bundle created: {bundle}")
        return

    raise ValueError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    main()
