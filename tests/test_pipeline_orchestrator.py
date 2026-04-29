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


class EmptyCollector:
    def __init__(self, platform: str = "linkedin") -> None:
        self.platform = platform
        self.calls: list[str] = []

    async def collect_from_source(self, source_id: str) -> list[CollectedItem]:
        self.calls.append(source_id)
        return []


class SequenceCollector:
    def __init__(self, outcomes: list[str], platform: str = "linkedin") -> None:
        self.outcomes = outcomes
        self.platform = platform
        self.calls: list[str] = []

    async def collect_from_source(self, source_id: str) -> list[CollectedItem]:
        self.calls.append(source_id)
        outcome = self.outcomes.pop(0) if self.outcomes else "empty"
        if outcome == "exception":
            raise RuntimeError("boom")
        if outcome == "empty":
            return []
        return [
            CollectedItem(
                platform=self.platform,
                external_id=f"{source_id}-ok",
                author="dummy",
                url=f"https://example.com/{source_id}",
                text="Claude Code workflow feature release with implementation steps",
                transcript_text=None,
                published_at=None,
                metadata={},
            )
        ]


class ResolvingCollector(DummyRSSCollector):
    platform = "linkedin"

    def __init__(self) -> None:
        self.resolve_calls: list[str] = []
        self.collect_calls: list[str] = []

    async def resolve_source_identifier(self, source_id: str) -> str:
        self.resolve_calls.append(source_id)
        return "in/boris-cherny"

    async def collect_from_source(self, source_id: str) -> list[CollectedItem]:
        self.collect_calls.append(source_id)
        return [
            CollectedItem(
                platform="linkedin",
                external_id="post-1",
                author="Boris Cherny",
                url="https://www.linkedin.com/feed/update/activity:1/",
                text="Claude Code workflow feature release with implementation steps",
                transcript_text=None,
                published_at=None,
                metadata={},
            )
        ]


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

    await subject_service.create_subject("Vibe Coding", include_terms=["vibe coding"])
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

    await subject_service.create_subject("Vibe Coding", include_terms=["vibe coding"])
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
async def test_pipeline_backfills_resolved_linkedin_identifier_before_collecting(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_repo = SubjectRepository(conn=db.conn)
    source_repo = SourceRepository(conn=db.conn)
    subject_service = SubjectService(repository=subject_repo)
    source_service = SourceService(source_repo=source_repo, subject_repo=subject_repo)

    await subject_service.create_subject("Vibe Coding", include_terms=["vibe coding"])
    await source_service.monitor_source(
        platform="linkedin",
        account_or_channel_id="in/ACoAAA2WrzMBW0tblYjqmElLdB695E8tu_ZWxqg",
        display_name="Boris Cherny",
    )

    collector = ResolvingCollector()
    orchestrator = PipelineOrchestrator(
        subject_service=subject_service,
        source_service=source_service,
        item_repo=ItemRepository(conn=db.conn),
        item_score_repo=ItemScoreRepository(conn=db.conn),
        run_log_repo=RunLogRepository(conn=db.conn),
        collectors={"linkedin": collector},
    )

    stats = await orchestrator.run_nightly_collection()

    assert stats["items_collected"] == 1
    assert collector.resolve_calls == ["in/ACoAAA2WrzMBW0tblYjqmElLdB695E8tu_ZWxqg"]
    assert collector.collect_calls == ["in/boris-cherny"]
    assert await source_repo.get_by_identity(platform="linkedin", account_or_channel_id="in/boris-cherny")
    old_source = await source_repo.get_by_identity(
        platform="linkedin",
        account_or_channel_id="in/ACoAAA2WrzMBW0tblYjqmElLdB695E8tu_ZWxqg",
    )
    assert old_source is None

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

    await subject_service.create_subject("Vibe Coding", include_terms=["vibe coding"])
    await subject_service.create_subject("Agentic PM", include_terms=["agentic pm"])
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

    await subject_service.create_subject("Vibe Coding", include_terms=["vibe coding"])
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

    await subject_service.create_subject("Agentic PM", include_terms=["agentic pm"])
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

    await subject_service.create_subject("Vibe Coding", include_terms=["vibe coding"])
    await subject_service.create_subject("Agentic PM", include_terms=["agentic pm"])
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

    await subject_service.create_subject("Vibe Coding", include_terms=["vibe coding"])
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

    await subject_service.create_subject("Vibe Coding", include_terms=["vibe coding"])
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

    await subject_service.create_subject("Vibe Coding", include_terms=["vibe coding"])
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


@pytest.mark.asyncio
async def test_platform_circuit_breaker_stops_remaining_sources_and_records_metadata(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_repo = SubjectRepository(conn=db.conn)
    source_repo = SourceRepository(conn=db.conn)
    subject_service = SubjectService(repository=subject_repo)
    source_service = SourceService(source_repo=source_repo, subject_repo=subject_repo)

    await subject_service.create_subject("Vibe Coding", include_terms=["vibe coding"])
    for idx in range(6):
        await source_service.monitor_source(
            platform="linkedin",
            account_or_channel_id=f"person-{idx}",
            display_name=f"Person {idx}",
        )

    collector = EmptyCollector(platform="linkedin")
    orchestrator = PipelineOrchestrator(
        subject_service=subject_service,
        source_service=source_service,
        item_repo=ItemRepository(conn=db.conn),
        item_score_repo=ItemScoreRepository(conn=db.conn),
        run_log_repo=RunLogRepository(conn=db.conn),
        collectors={"linkedin": collector},
        circuit_threshold=3,
    )

    stats = await orchestrator.run_nightly_collection()

    assert collector.calls == ["person-0", "person-1", "person-2"]
    assert stats["circuit_broken"] == ["linkedin"]
    assert stats["sources_skipped_circuit_breaker"] == 3
    row = await (
        await db.conn.execute("SELECT metadata_json FROM run_logs WHERE run_type = 'nightly_collection'")
    ).fetchone()
    assert row is not None
    assert row["metadata_json"]
    assert '"circuit_broken": ["linkedin"]' in row["metadata_json"]

    await db.close()


@pytest.mark.asyncio
async def test_platform_circuit_breaker_counts_session_challenges(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_repo = SubjectRepository(conn=db.conn)
    source_repo = SourceRepository(conn=db.conn)
    subject_service = SubjectService(repository=subject_repo)
    source_service = SourceService(source_repo=source_repo, subject_repo=subject_repo)

    await subject_service.create_subject("Vibe Coding", include_terms=["vibe coding"])
    for idx in range(3):
        await source_service.monitor_source(
            platform="x",
            account_or_channel_id=f"person{idx}",
            display_name=f"Person {idx}",
        )

    orchestrator = PipelineOrchestrator(
        subject_service=subject_service,
        source_service=source_service,
        item_repo=ItemRepository(conn=db.conn),
        item_score_repo=ItemScoreRepository(conn=db.conn),
        run_log_repo=RunLogRepository(conn=db.conn),
        collectors={"x": ChallengedCollector("x")},
        circuit_threshold=2,
    )

    stats = await orchestrator.run_nightly_collection()

    assert stats["sources_needing_reauth"] == 2
    assert stats["sources_skipped_circuit_breaker"] == 1
    assert stats["circuit_broken"] == ["x"]

    await db.close()


@pytest.mark.asyncio
async def test_platform_circuit_breaker_resets_after_success_and_isolates_platforms(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_repo = SubjectRepository(conn=db.conn)
    source_repo = SourceRepository(conn=db.conn)
    subject_service = SubjectService(repository=subject_repo)
    source_service = SourceService(source_repo=source_repo, subject_repo=subject_repo)

    await subject_service.create_subject("Vibe Coding", include_terms=["vibe coding"])
    for idx in range(5):
        await source_service.monitor_source(
            platform="linkedin",
            account_or_channel_id=f"person-{idx}",
            display_name=f"Person {idx}",
        )
    await source_service.monitor_source(
        platform="youtube",
        account_or_channel_id="@openai",
        display_name="OpenAI",
    )

    linkedin = SequenceCollector(["empty", "ok", "empty", "empty", "empty"], platform="linkedin")
    youtube = SequenceCollector(["ok"], platform="youtube")
    orchestrator = PipelineOrchestrator(
        subject_service=subject_service,
        source_service=source_service,
        item_repo=ItemRepository(conn=db.conn),
        item_score_repo=ItemScoreRepository(conn=db.conn),
        run_log_repo=RunLogRepository(conn=db.conn),
        collectors={"linkedin": linkedin, "youtube": youtube},
        circuit_threshold=3,
    )

    stats = await orchestrator.run_nightly_collection()

    assert linkedin.calls == ["person-0", "person-1", "person-2", "person-3", "person-4"]
    assert youtube.calls == ["@openai"]
    assert stats["circuit_broken"] == ["linkedin"]
    assert stats["sources_skipped_circuit_breaker"] == 0
    assert stats["items_collected"] == 2

    second = await orchestrator.run_nightly_collection()
    assert second["sources_seen"] == 6

    await db.close()
