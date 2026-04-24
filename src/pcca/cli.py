from __future__ import annotations

import argparse
import asyncio
import logging
from typing import Sequence

from pcca.app import PCCAApp
from pcca.config import Settings
from pcca.db import Database
from pcca.logging_utils import configure_logging
from pcca.repositories.routing import RoutingRepository
from pcca.repositories.sources import SourceRepository
from pcca.repositories.subjects import SubjectRepository
from pcca.services.routing_service import RoutingService
from pcca.services.source_discovery_service import SourceDiscoveryService
from pcca.services.source_service import SourceService
from pcca.services.subject_service import SubjectService


async def _init_db(settings: Settings) -> None:
    settings.ensure_dirs()
    db = Database(path=settings.db_path)
    await db.connect()
    try:
        await db.initialize()
    finally:
        await db.close()


async def _create_subject(settings: Settings, name: str, thread_id: str | None) -> None:
    settings.ensure_dirs()
    db = Database(path=settings.db_path)
    await db.connect()
    await db.initialize()
    try:
        if db.conn is None:
            raise RuntimeError("Database connection unavailable.")
        service = SubjectService(repository=SubjectRepository(conn=db.conn))
        subject = await service.create_subject(name, telegram_thread_id=thread_id)
        print(f"Subject ready: {subject.name} (id={subject.id})")
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
        await service.add_source_to_subject(
            subject_name=subject,
            platform=platform,
            account_or_channel_id=source_id,
            display_name=display_name,
            priority=priority,
        )
        print(f"Source linked: [{platform}] {source_id} -> subject '{subject}'")
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
                f"{row.display_name}\tpriority={row.priority}\tstatus={row.status}"
            )
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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pcca", description="Personal Content Curation Agent")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init-db", help="Initialize local database schema")

    create_subject_parser = sub.add_parser("create-subject", help="Create a subject")
    create_subject_parser.add_argument("--name", required=True, help="Subject name")
    create_subject_parser.add_argument("--thread-id", required=False, help="Optional Telegram thread id")

    sub.add_parser("list-subjects", help="List configured subjects")

    add_source_parser = sub.add_parser("add-source", help="Link a source to a subject")
    add_source_parser.add_argument("--subject", required=True, help="Subject name")
    add_source_parser.add_argument("--platform", required=True, help="Platform, e.g. x/linkedin/youtube/reddit/rss")
    add_source_parser.add_argument("--source-id", required=True, help="Handle/channel/feed identifier")
    add_source_parser.add_argument("--display-name", required=False, help="Display name override")
    add_source_parser.add_argument("--priority", type=int, default=0, help="Source priority weight")

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

    link_route_parser = sub.add_parser("link-subject-chat", help="Link subject delivery route to Telegram chat/thread")
    link_route_parser.add_argument("--subject", required=True, help="Subject name")
    link_route_parser.add_argument("--chat-id", required=True, type=int, help="Telegram chat id")
    link_route_parser.add_argument("--thread-id", required=False, help="Optional Telegram thread id")

    import_follows_parser = sub.add_parser(
        "import-follows",
        help="Import follows/subscriptions from logged-in browser session into one subject (x/linkedin/youtube)",
    )
    import_follows_parser.add_argument("--subject", required=True, help="Subject name")
    import_follows_parser.add_argument("--platform", required=True, choices=["x", "linkedin", "youtube"], help="Platform")
    import_follows_parser.add_argument("--limit", required=False, type=int, default=200, help="Maximum follows to import")

    login_parser = sub.add_parser("login", help="Open browser login flow and persist session profile")
    login_parser.add_argument("--platform", required=True, choices=["x", "linkedin", "youtube"], help="Platform")
    login_parser.add_argument("--url", required=False, help="Custom login URL")

    sub.add_parser("run-nightly-once", help="Run nightly collection pipeline once")
    sub.add_parser("run-digest-once", help="Run digest sending once")
    sub.add_parser("run-agent", help="Run scheduler + Telegram bot")

    return parser


def main(argv: Sequence[str] | None = None) -> None:
    configure_logging()
    args = build_parser().parse_args(argv)
    settings = Settings.from_env()

    if args.command == "init-db":
        asyncio.run(_init_db(settings))
        print(f"Database initialized at: {settings.db_path}")
        return

    if args.command == "create-subject":
        asyncio.run(_create_subject(settings, args.name, args.thread_id))
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

    if args.command == "login":
        app = PCCAApp(settings=settings)
        asyncio.run(app.login_platform_once(platform=args.platform, login_url=args.url))
        return

    if args.command == "run-nightly-once":
        app = PCCAApp(settings=settings)
        stats = asyncio.run(app.run_nightly_once())
        print(f"Nightly run completed: {stats}")
        return

    if args.command == "run-digest-once":
        app = PCCAApp(settings=settings)
        asyncio.run(app.run_digest_once())
        print("Digest run completed.")
        return

    if args.command == "run-agent":
        app = PCCAApp(settings=settings)
        try:
            asyncio.run(app.run_forever())
        except KeyboardInterrupt:
            logging.getLogger(__name__).info("Agent interrupted by user.")
        return

    raise ValueError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    main()
