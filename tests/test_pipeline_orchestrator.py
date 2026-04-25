from __future__ import annotations

from pathlib import Path

import pytest

from pcca.collectors.base import CollectedItem
from pcca.collectors.errors import SessionChallengedError
from pcca.db import Database
from pcca.pipeline.orchestrator import PipelineOrchestrator
from pcca.repositories.item_scores import ItemScoreRepository
from pcca.repositories.items import ItemRepository
from pcca.repositories.run_logs import RunLogRepository
from pcca.repositories.sources import SourceRepository
from pcca.repositories.subjects import SubjectRepository
from pcca.services.source_service import SourceService
from pcca.services.subject_service import SubjectService


class DummyRSSCollector:
    platform = "rss"

    async def collect_from_source(self, source_id: str) -> list[CollectedItem]:
        return [
            CollectedItem(
                platform="rss",
                external_id=f"id-{source_id}",
                author="dummy",
                url="https://example.com/post",
                text="Claude Code workflow feature release with implementation steps",
                transcript_text=None,
                published_at=None,
                metadata={"source_id": source_id},
            )
        ]


class ChallengedCollector:
    def __init__(self, platform: str = "x") -> None:
        self.platform = platform

    async def collect_from_source(self, source_id: str) -> list[CollectedItem]:
        raise SessionChallengedError(
            platform=self.platform,
            source_id=source_id,
            current_url=f"https://example.com/{self.platform}/login",
            challenge_kind="login_redirect",
        )


@pytest.mark.asyncio
async def test_pipeline_collects_and_scores(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_repo = SubjectRepository(conn=db.conn)
    source_repo = SourceRepository(conn=db.conn)
    subject_service = SubjectService(repository=subject_repo)
    source_service = SourceService(source_repo=source_repo, subject_repo=subject_repo)

    await subject_service.create_subject("Vibe Coding")
    await source_service.add_source_to_subject(
        subject_name="Vibe Coding",
        platform="rss",
        account_or_channel_id="feed://demo",
        display_name="Demo",
        priority=1,
    )

    orchestrator = PipelineOrchestrator(
        subject_service=subject_service,
        source_service=source_service,
        item_repo=ItemRepository(conn=db.conn),
        item_score_repo=ItemScoreRepository(conn=db.conn),
        run_log_repo=RunLogRepository(conn=db.conn),
        collectors={"rss": DummyRSSCollector()},
    )
    stats = await orchestrator.run_nightly_collection()
    assert stats["subjects_seen"] == 1
    assert stats["sources_seen"] == 1
    assert stats["items_collected"] == 1
    assert stats["items_inserted"] == 1
    source = await source_repo.get_by_identity(platform="rss", account_or_channel_id="feed://demo")
    assert source is not None
    assert source.follow_state == "active"
    assert source.last_crawled_at is not None

    row = await (
        await db.conn.execute("SELECT COUNT(*) AS c FROM item_scores")
    ).fetchone()
    assert int(row["c"]) == 1
    assert stats["items_scored"] == 1

    second_stats = await orchestrator.run_nightly_collection()
    assert second_stats["items_inserted"] == 0
    assert second_stats["items_updated"] == 0
    assert second_stats["items_scored"] == 0

    await db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("platform", ["youtube", "spotify"])
async def test_pipeline_marks_youtube_and_spotify_challenges_for_reauth(tmp_path: Path, platform: str) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_repo = SubjectRepository(conn=db.conn)
    source_repo = SourceRepository(conn=db.conn)
    subject_service = SubjectService(repository=subject_repo)
    source_service = SourceService(source_repo=source_repo, subject_repo=subject_repo)

    await subject_service.create_subject("Vibe Coding")
    await source_service.add_source_to_subject(
        subject_name="Vibe Coding",
        platform=platform,
        account_or_channel_id="demo",
        display_name="Demo",
        priority=1,
    )

    orchestrator = PipelineOrchestrator(
        subject_service=subject_service,
        source_service=source_service,
        item_repo=ItemRepository(conn=db.conn),
        item_score_repo=ItemScoreRepository(conn=db.conn),
        run_log_repo=RunLogRepository(conn=db.conn),
        collectors={platform: ChallengedCollector(platform)},
    )
    stats = await orchestrator.run_nightly_collection()
    assert stats["sources_needing_reauth"] == 1
    source = await source_repo.get_by_identity(platform=platform, account_or_channel_id="demo")
    assert source is not None
    assert source.follow_state == "needs_reauth"

    activated = await source_service.mark_platform_active_after_login(platform)
    assert activated == 1

    await db.close()


@pytest.mark.asyncio
async def test_pipeline_marks_challenged_sources_for_reauth(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_repo = SubjectRepository(conn=db.conn)
    source_repo = SourceRepository(conn=db.conn)
    subject_service = SubjectService(repository=subject_repo)
    source_service = SourceService(source_repo=source_repo, subject_repo=subject_repo)

    await subject_service.create_subject("Vibe Coding")
    await source_service.add_source_to_subject(
        subject_name="Vibe Coding",
        platform="x",
        account_or_channel_id="borischerny",
        display_name="Boris Cherny",
        priority=1,
    )

    orchestrator = PipelineOrchestrator(
        subject_service=subject_service,
        source_service=source_service,
        item_repo=ItemRepository(conn=db.conn),
        item_score_repo=ItemScoreRepository(conn=db.conn),
        run_log_repo=RunLogRepository(conn=db.conn),
        collectors={"x": ChallengedCollector()},
    )
    stats = await orchestrator.run_nightly_collection()
    assert stats["sources_needing_reauth"] == 1
    assert stats["collector_errors"] == 0

    source = await source_repo.get_by_identity(platform="x", account_or_channel_id="borischerny")
    assert source is not None
    assert source.follow_state == "needs_reauth"

    active_sources = await source_service.list_sources_for_subject("Vibe Coding")
    assert active_sources == []

    activated = await source_service.mark_platform_active_after_login("x")
    assert activated == 1
    active_sources = await source_service.list_sources_for_subject("Vibe Coding")
    assert len(active_sources) == 1

    await db.close()
