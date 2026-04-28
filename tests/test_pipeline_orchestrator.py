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


class CountingCollector(DummyRSSCollector):
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def collect_from_source(self, source_id: str) -> list[CollectedItem]:
        self.calls.append(source_id)
        return await super().collect_from_source(source_id)


class FakeModelRouter:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def rerank(self, *, subject_name: str, text: str, heuristic_score: float):
        _ = text, heuristic_score
        self.calls.append(subject_name)
        return None


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


class FakeSessionRefreshService:
    def __init__(self) -> None:
        self.platforms: list[str] = []

    async def refresh_platform(self, platform: str):
        self.platforms.append(platform)
        return type(
            "RefreshResult",
            (),
            {
                "refreshed": True,
                "skipped": False,
                "reason": "refreshed",
                "browser": "arc",
                "profile_name": "Default",
                "missing_cookie_names": [],
            },
        )()


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
async def test_pipeline_refreshes_session_before_collecting_source(tmp_path: Path) -> None:
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

    refresh_service = FakeSessionRefreshService()
    orchestrator = PipelineOrchestrator(
        subject_service=subject_service,
        source_service=source_service,
        item_repo=ItemRepository(conn=db.conn),
        item_score_repo=ItemScoreRepository(conn=db.conn),
        run_log_repo=RunLogRepository(conn=db.conn),
        session_refresh_service=refresh_service,  # type: ignore[arg-type]
        collectors={"x": DummyRSSCollector()},
    )
    stats = await orchestrator.run_nightly_collection()

    assert refresh_service.platforms == ["x"]
    assert stats["items_collected"] == 1

    await db.close()


@pytest.mark.asyncio
async def test_pipeline_collects_source_once_and_scores_for_all_subjects(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_repo = SubjectRepository(conn=db.conn)
    source_repo = SourceRepository(conn=db.conn)
    subject_service = SubjectService(repository=subject_repo)
    source_service = SourceService(source_repo=source_repo, subject_repo=subject_repo)

    await subject_service.create_subject("Vibe Coding")
    await subject_service.create_subject("Agentic PM")
    await source_service.monitor_source(
        platform="rss",
        account_or_channel_id="feed://demo",
        display_name="Demo",
    )

    collector = CountingCollector()
    orchestrator = PipelineOrchestrator(
        subject_service=subject_service,
        source_service=source_service,
        item_repo=ItemRepository(conn=db.conn),
        item_score_repo=ItemScoreRepository(conn=db.conn),
        run_log_repo=RunLogRepository(conn=db.conn),
        collectors={"rss": collector},
    )

    stats = await orchestrator.run_nightly_collection()

    assert collector.calls == ["feed://demo"]
    assert stats["sources_seen"] == 1
    assert stats["items_scored"] == 2
    score_count = await (await db.conn.execute("SELECT COUNT(*) AS c FROM item_scores")).fetchone()
    assert int(score_count["c"]) == 2

    await db.close()


@pytest.mark.asyncio
async def test_pipeline_backfills_existing_items_for_new_subject(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_repo = SubjectRepository(conn=db.conn)
    source_repo = SourceRepository(conn=db.conn)
    subject_service = SubjectService(repository=subject_repo)
    source_service = SourceService(source_repo=source_repo, subject_repo=subject_repo)

    await subject_service.create_subject("Vibe Coding")
    await source_service.monitor_source(
        platform="rss",
        account_or_channel_id="feed://demo",
        display_name="Demo",
    )

    collector = CountingCollector()
    orchestrator = PipelineOrchestrator(
        subject_service=subject_service,
        source_service=source_service,
        item_repo=ItemRepository(conn=db.conn),
        item_score_repo=ItemScoreRepository(conn=db.conn),
        run_log_repo=RunLogRepository(conn=db.conn),
        collectors={"rss": collector},
    )
    first_stats = await orchestrator.run_nightly_collection()
    assert first_stats["items_scored"] == 1

    await subject_service.create_subject("Agentic PM")
    second_stats = await orchestrator.run_nightly_collection()

    assert second_stats["items_inserted"] == 0
    assert second_stats["items_updated"] == 0
    assert second_stats["items_scored"] == 1
    score_count = await (await db.conn.execute("SELECT COUNT(*) AS c FROM item_scores")).fetchone()
    assert int(score_count["c"]) == 2

    await db.close()


@pytest.mark.asyncio
async def test_pipeline_respects_inactive_source_override_for_subject(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_repo = SubjectRepository(conn=db.conn)
    source_repo = SourceRepository(conn=db.conn)
    subject_service = SubjectService(repository=subject_repo)
    source_service = SourceService(source_repo=source_repo, subject_repo=subject_repo)

    await subject_service.create_subject("Vibe Coding")
    await subject_service.create_subject("Agentic PM")
    await source_service.add_source_to_subject(
        subject_name="Vibe Coding",
        platform="rss",
        account_or_channel_id="feed://demo",
        display_name="Demo",
    )
    removed = await source_service.remove_source_from_subject(
        subject_name="Vibe Coding",
        platform="rss",
        account_or_channel_id="feed://demo",
    )
    assert removed is True

    orchestrator = PipelineOrchestrator(
        subject_service=subject_service,
        source_service=source_service,
        item_repo=ItemRepository(conn=db.conn),
        item_score_repo=ItemScoreRepository(conn=db.conn),
        run_log_repo=RunLogRepository(conn=db.conn),
        collectors={"rss": DummyRSSCollector()},
    )

    stats = await orchestrator.run_nightly_collection()

    assert stats["items_scored"] == 1
    assert stats["items_skipped_subject_source_override"] == 1
    rows = await (
        await db.conn.execute(
            """
            SELECT subjects.name AS subject_name
            FROM item_scores
            JOIN subjects ON subjects.id = item_scores.subject_id
            """
        )
    ).fetchall()
    assert [row["subject_name"] for row in rows] == ["Agentic PM"]

    await db.close()


@pytest.mark.asyncio
async def test_pipeline_model_rerank_only_runs_for_subject_shortlist(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_repo = SubjectRepository(conn=db.conn)
    source_repo = SourceRepository(conn=db.conn)
    subject_service = SubjectService(repository=subject_repo)
    source_service = SourceService(source_repo=source_repo, subject_repo=subject_repo)

    await subject_service.create_subject("Vibe Coding")
    await source_service.monitor_source(
        platform="rss",
        account_or_channel_id="feed://demo",
        display_name="Demo",
    )

    class ManyItemCollector:
        platform = "rss"

        async def collect_from_source(self, source_id: str) -> list[CollectedItem]:
            return [
                CollectedItem(
                    platform="rss",
                    external_id=f"{source_id}-{idx}",
                    author="dummy",
                    url=f"https://example.com/{idx}",
                    text=f"Claude Code workflow feature release step {idx}",
                    transcript_text=None,
                    published_at=None,
                    metadata={},
                )
                for idx in range(25)
            ]

    model_router = FakeModelRouter()
    orchestrator = PipelineOrchestrator(
        subject_service=subject_service,
        source_service=source_service,
        item_repo=ItemRepository(conn=db.conn),
        item_score_repo=ItemScoreRepository(conn=db.conn),
        run_log_repo=RunLogRepository(conn=db.conn),
        model_router=model_router,  # type: ignore[arg-type]
        collectors={"rss": ManyItemCollector()},
    )
    stats = await orchestrator.run_nightly_collection()

    assert stats["items_scored"] == 25
    assert stats["model_shortlist_items"] == 20
    assert len(model_router.calls) == 20

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
