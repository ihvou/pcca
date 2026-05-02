from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from pcca.collectors.base import CollectedItem
from pcca.collectors.errors import SessionChallengedError, SourceNotFoundError
from pcca.config import Settings
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


class BlockingCollector:
    platform = "rss"

    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def collect_from_source(self, source_id: str) -> list[CollectedItem]:
        self.entered.set()
        await self.release.wait()
        return [
            CollectedItem(
                platform="rss",
                external_id=f"blocking-{source_id}",
                author="dummy",
                url="https://example.com/blocking",
                text="Claude Code workflow feature release with implementation steps",
                transcript_text=None,
                published_at=None,
                metadata={},
            )
        ]


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


class YouTubeNotFoundThenResolvedCollector:
    platform = "youtube"

    def __init__(self) -> None:
        self.resolve_calls: list[str] = []
        self.collect_calls: list[str] = []

    async def resolve_source_identifier(self, source_id: str) -> str:
        self.resolve_calls.append(source_id)
        return "UCNEW" if source_id == "@openai" else source_id

    async def collect_from_source(self, source_id: str) -> list[CollectedItem]:
        self.collect_calls.append(source_id)
        if source_id == "UCOLD":
            raise SourceNotFoundError(
                platform="youtube",
                source_id=source_id,
                current_url="https://www.youtube.com/feeds/videos.xml?channel_id=UCOLD",
                not_found_kind="rss_404",
                status_code=404,
            )
        return [
            CollectedItem(
                platform="youtube",
                external_id="video-1",
                author="OpenAI",
                url="https://www.youtube.com/watch?v=video-1",
                text="Claude Code workflow feature release with implementation steps",
                transcript_text=None,
                published_at=None,
                metadata={},
            )
        ]


class YouTubeAlwaysNotFoundCollector:
    platform = "youtube"

    def __init__(self) -> None:
        self.collect_calls: list[str] = []

    async def collect_from_source(self, source_id: str) -> list[CollectedItem]:
        self.collect_calls.append(source_id)
        raise SourceNotFoundError(
            platform="youtube",
            source_id=source_id,
            current_url=f"https://www.youtube.com/feeds/videos.xml?channel_id={source_id}",
            not_found_kind="rss_404",
            status_code=404,
        )


class MixedYouTubeNotFoundCollector:
    platform = "youtube"

    def __init__(self) -> None:
        self.resolve_calls: list[str] = []
        self.collect_calls: list[str] = []
        self.dead_ids = {"UCOLD1", "UCDEAD2", "UCDEAD3", "UCDEAD4", "UCDEAD5"}

    async def resolve_source_identifier(self, source_id: str) -> str:
        self.resolve_calls.append(source_id)
        return "UCNEW1" if source_id == "@good" else source_id

    async def collect_from_source(self, source_id: str) -> list[CollectedItem]:
        self.collect_calls.append(source_id)
        if source_id in self.dead_ids:
            raise SourceNotFoundError(
                platform="youtube",
                source_id=source_id,
                current_url=f"https://www.youtube.com/feeds/videos.xml?channel_id={source_id}",
                not_found_kind="rss_404",
                status_code=404,
            )
        return [
            CollectedItem(
                platform="youtube",
                external_id=f"{source_id}-video",
                author="OpenAI",
                url=f"https://www.youtube.com/watch?v={source_id}",
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


class FakeEmbeddingService:
    enabled = True
    embedding_model = "fake-embedding"

    def __init__(self) -> None:
        self.calls: list[str] = []

    async def embed(self, text: str) -> list[float] | None:
        self.calls.append(text)
        lowered = text.lower()
        if any(term in lowered for term in ("ukraine", "kyiv", "frontline", "russia")):
            return [1.0, 0.0]
        if any(term in lowered for term in ("claude", "football", "podcast")):
            return [0.0, 1.0]
        return [0.5, 0.5]


class SegmentAwareEmbeddingService:
    enabled = True
    embedding_model = "segment-aware"

    async def embed(self, text: str) -> list[float] | None:
        lowered = text.lower()
        if any(term in lowered for term in ("claude code", "anthropic", "agent handoff")):
            return [1.0, 0.0]
        return [0.0, 1.0]


class FailingEmbeddingService:
    enabled = True
    embedding_model = "fake-embedding"

    async def embed(self, text: str) -> list[float] | None:
        _ = text
        return None


class FakeBatchModelRouter:
    def __init__(self) -> None:
        self.batch_calls: list[dict] = []

    async def rerank_batch(self, *, subject_name: str, subject_description: str, candidates: list):
        self.batch_calls.append(
            {
                "subject_name": subject_name,
                "subject_description": subject_description,
                "candidate_count": len(candidates),
            }
        )
        return {
            candidate.item_id: type(
                "Rerank",
                (),
                {"score_delta": 0.01, "rationale": "batch considered full description"},
            )()
            for candidate in candidates
        }

    async def rerank(self, *, subject_name: str, text: str, heuristic_score: float):
        raise AssertionError("embedding path should use batch rerank")


class EmptyBatchModelRouter:
    enabled = True

    def __init__(self) -> None:
        self.batch_calls = 0
        self.single_calls = 0

    async def rerank_batch(self, *, subject_name: str, subject_description: str, candidates: list):
        _ = subject_name, subject_description, candidates
        self.batch_calls += 1
        return {}

    async def rerank(self, *, subject_name: str, text: str, heuristic_score: float):
        _ = subject_name, text, heuristic_score
        self.single_calls += 1
        raise AssertionError("single-item rerank should not run after an attempted batch rerank")


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
async def test_pipeline_auto_backfills_new_item_embeddings_after_collection(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_repo = SubjectRepository(conn=db.conn)
    source_repo = SourceRepository(conn=db.conn)
    source_service = SourceService(source_repo=source_repo, subject_repo=subject_repo)
    await source_service.monitor_source(
        platform="rss",
        account_or_channel_id="feed://demo",
        display_name="Demo",
    )

    item_repo = ItemRepository(conn=db.conn)
    orchestrator = PipelineOrchestrator(
        subject_service=SubjectService(repository=subject_repo),
        source_service=source_service,
        item_repo=item_repo,
        item_score_repo=ItemScoreRepository(conn=db.conn),
        run_log_repo=RunLogRepository(conn=db.conn),
        embedding_service=FakeEmbeddingService(),  # type: ignore[arg-type]
        collectors={"rss": DummyRSSCollector()},
        scorer="embedding",
        auto_backfill_embeddings=True,
    )

    stats = await orchestrator.run_nightly_collection(platform="rss")
    rows = await item_repo.list_all_for_scoring()
    item_id, item = rows[0]
    item_text = item_repo.embedding_text(item)
    item_hash = item_repo.embedding_text_hash(item_text)
    segment_row = await (
        await db.conn.execute(
            "SELECT embedding_json FROM item_segments WHERE item_id = ?",
            (item_id,),
        )
    ).fetchone()

    assert stats["items_inserted"] == 1
    assert stats["embedding_pending"] is False
    assert stats["embedding_backfill"]["items_embedded"] == 1
    assert stats["embedding_backfill"]["segments_embedded"] == 1
    assert await item_repo.get_content_embedding_for_text(
        item_id,
        model="fake-embedding",
        text_hash=item_hash,
    ) is not None
    assert segment_row is not None
    assert segment_row["embedding_json"] is not None

    await db.close()


@pytest.mark.asyncio
async def test_pipeline_auto_backfill_failure_is_non_fatal(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_repo = SubjectRepository(conn=db.conn)
    source_repo = SourceRepository(conn=db.conn)
    source_service = SourceService(source_repo=source_repo, subject_repo=subject_repo)
    await source_service.monitor_source(
        platform="rss",
        account_or_channel_id="feed://demo",
        display_name="Demo",
    )

    orchestrator = PipelineOrchestrator(
        subject_service=SubjectService(repository=subject_repo),
        source_service=source_service,
        item_repo=ItemRepository(conn=db.conn),
        item_score_repo=ItemScoreRepository(conn=db.conn),
        run_log_repo=RunLogRepository(conn=db.conn),
        embedding_service=FailingEmbeddingService(),  # type: ignore[arg-type]
        collectors={"rss": DummyRSSCollector()},
        scorer="embedding",
        auto_backfill_embeddings=True,
    )

    stats = await orchestrator.run_nightly_collection(platform="rss")
    row = await (await db.conn.execute("SELECT COUNT(*) AS c FROM items")).fetchone()

    assert row["c"] == 1
    assert stats["items_inserted"] == 1
    assert stats["embedding_pending"] is True
    assert stats["embedding_backfill"]["items_failed"] == 1

    await db.close()


@pytest.mark.asyncio
async def test_pipeline_score_false_skips_per_subject_scoring(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_repo = SubjectRepository(conn=db.conn)
    source_repo = SourceRepository(conn=db.conn)
    subject_service = SubjectService(repository=subject_repo)
    source_service = SourceService(source_repo=source_repo, subject_repo=subject_repo)
    await subject_service.create_subject("Vibe Coding", include_terms=["claude code"])
    await source_service.monitor_source(
        platform="rss",
        account_or_channel_id="feed://demo",
        display_name="Demo",
    )

    orchestrator = PipelineOrchestrator(
        subject_service=subject_service,
        source_service=source_service,
        item_repo=ItemRepository(conn=db.conn),
        item_score_repo=ItemScoreRepository(conn=db.conn),
        run_log_repo=RunLogRepository(conn=db.conn),
        collectors={"rss": DummyRSSCollector()},
        auto_backfill_embeddings=False,
    )

    stats = await orchestrator.run_nightly_collection(platform="rss", score=False)
    score_count = await (await db.conn.execute("SELECT COUNT(*) AS c FROM item_scores")).fetchone()

    assert stats["items_inserted"] == 1
    assert stats["scoring_enabled"] is False
    assert stats["scoring_skipped"] is True
    assert stats["items_scored"] == 0
    assert score_count["c"] == 0

    await db.close()


@pytest.mark.asyncio
async def test_pipeline_auto_backfill_disabled_skips_embedding_chain(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_repo = SubjectRepository(conn=db.conn)
    source_repo = SourceRepository(conn=db.conn)
    source_service = SourceService(source_repo=source_repo, subject_repo=subject_repo)
    await source_service.monitor_source(
        platform="rss",
        account_or_channel_id="feed://demo",
        display_name="Demo",
    )
    item_repo = ItemRepository(conn=db.conn)
    embedding_service = FakeEmbeddingService()
    orchestrator = PipelineOrchestrator(
        subject_service=SubjectService(repository=subject_repo),
        source_service=source_service,
        item_repo=item_repo,
        item_score_repo=ItemScoreRepository(conn=db.conn),
        run_log_repo=RunLogRepository(conn=db.conn),
        embedding_service=embedding_service,  # type: ignore[arg-type]
        collectors={"rss": DummyRSSCollector()},
        scorer="embedding",
        auto_backfill_embeddings=False,
    )

    stats = await orchestrator.run_nightly_collection(platform="rss", score=False)
    item_id, item = (await item_repo.list_all_for_scoring())[0]
    text = item_repo.embedding_text(item)

    assert stats["auto_backfill_enabled"] is False
    assert stats["embedding_backfill"] == {}
    assert stats["embedding_pending"] is False
    assert embedding_service.calls == []
    assert await item_repo.get_content_embedding_for_text(
        item_id,
        model=embedding_service.embedding_model,
        text_hash=item_repo.embedding_text_hash(text),
    ) is None

    await db.close()


@pytest.mark.asyncio
async def test_pipeline_runtime_lock_rejects_overlapping_collection(tmp_path: Path) -> None:
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

    collector = BlockingCollector()
    orchestrator = PipelineOrchestrator(
        subject_service=subject_service,
        source_service=source_service,
        item_repo=ItemRepository(conn=db.conn),
        item_score_repo=ItemScoreRepository(conn=db.conn),
        run_log_repo=RunLogRepository(conn=db.conn),
        collectors={"rss": collector},
    )

    first = asyncio.create_task(orchestrator.run_nightly_collection())
    try:
        await asyncio.wait_for(collector.entered.wait(), timeout=1)
        second = await orchestrator.run_nightly_collection()
        assert second["skipped_already_running"] is True
        assert second["status"] == "skipped_already_running"

        collector.release.set()
        first_stats = await first
        assert first_stats["items_collected"] == 1

        row = await (await db.conn.execute("SELECT COUNT(*) AS c FROM runtime_locks")).fetchone()
        assert int(row["c"]) == 0
    finally:
        collector.release.set()
        if not first.done():
            first.cancel()
            try:
                await first
            except asyncio.CancelledError:
                pass
    await db.close()


@pytest.mark.asyncio
async def test_run_nightly_once_no_backfill_skips_embed(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_repo = SubjectRepository(conn=db.conn)
    source_repo = SourceRepository(conn=db.conn)
    source_service = SourceService(source_repo=source_repo, subject_repo=subject_repo)
    await source_service.monitor_source(
        platform="rss",
        account_or_channel_id="feed://demo",
        display_name="Demo",
    )
    item_repo = ItemRepository(conn=db.conn)
    embedding_service = FakeEmbeddingService()
    orchestrator = PipelineOrchestrator(
        subject_service=SubjectService(repository=subject_repo),
        source_service=source_service,
        item_repo=item_repo,
        item_score_repo=ItemScoreRepository(conn=db.conn),
        run_log_repo=RunLogRepository(conn=db.conn),
        embedding_service=embedding_service,  # type: ignore[arg-type]
        collectors={"rss": DummyRSSCollector()},
        scorer="embedding",
        auto_backfill_embeddings=True,
    )

    stats = await orchestrator.run_nightly_collection(platform="rss", auto_backfill=False, score=False)

    assert stats["auto_backfill_enabled"] is False
    assert stats["embedding_backfill"] == {}
    assert embedding_service.calls == []

    await db.close()


@pytest.mark.asyncio
async def test_pcca_auto_backfill_env_false_disables_chain(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("PCCA_AUTO_BACKFILL", "false")
    settings = Settings.from_env()

    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_repo = SubjectRepository(conn=db.conn)
    source_repo = SourceRepository(conn=db.conn)
    source_service = SourceService(source_repo=source_repo, subject_repo=subject_repo)
    await source_service.monitor_source(
        platform="rss",
        account_or_channel_id="feed://demo",
        display_name="Demo",
    )
    embedding_service = FakeEmbeddingService()
    orchestrator = PipelineOrchestrator(
        subject_service=SubjectService(repository=subject_repo),
        source_service=source_service,
        item_repo=ItemRepository(conn=db.conn),
        item_score_repo=ItemScoreRepository(conn=db.conn),
        run_log_repo=RunLogRepository(conn=db.conn),
        embedding_service=embedding_service,  # type: ignore[arg-type]
        collectors={"rss": DummyRSSCollector()},
        scorer="embedding",
        auto_backfill_embeddings=settings.auto_backfill_embeddings,
    )

    stats = await orchestrator.run_nightly_collection(platform="rss", score=False)

    assert settings.auto_backfill_embeddings is False
    assert stats["auto_backfill_enabled"] is False
    assert stats["embedding_backfill"] == {}
    assert embedding_service.calls == []

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
    metadata_row = await (
        await db.conn.execute("SELECT metadata_json FROM sources WHERE account_or_channel_id = 'in/boris-cherny'")
    ).fetchone()
    assert metadata_row is not None
    assert json.loads(metadata_row["metadata_json"])["resolved_from"] == "in/ACoAAA2WrzMBW0tblYjqmElLdB695E8tu_ZWxqg"
    old_source = await source_repo.get_by_identity(
        platform="linkedin",
        account_or_channel_id="in/ACoAAA2WrzMBW0tblYjqmElLdB695E8tu_ZWxqg",
    )
    assert old_source is None

    await db.close()


@pytest.mark.asyncio
async def test_pipeline_can_scope_collection_to_one_platform(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_repo = SubjectRepository(conn=db.conn)
    source_repo = SourceRepository(conn=db.conn)
    subject_service = SubjectService(repository=subject_repo)
    source_service = SourceService(source_repo=source_repo, subject_repo=subject_repo)

    await subject_service.create_subject("Vibe Coding", include_terms=["vibe coding"])
    await source_service.monitor_source(platform="youtube", account_or_channel_id="@openai", display_name="OpenAI")
    await source_service.monitor_source(platform="linkedin", account_or_channel_id="in/demo", display_name="Demo")

    youtube = CountingCollector()
    youtube.platform = "youtube"
    linkedin = CountingCollector()
    linkedin.platform = "linkedin"
    orchestrator = PipelineOrchestrator(
        subject_service=subject_service,
        source_service=source_service,
        item_repo=ItemRepository(conn=db.conn),
        item_score_repo=ItemScoreRepository(conn=db.conn),
        run_log_repo=RunLogRepository(conn=db.conn),
        collectors={"youtube": youtube, "linkedin": linkedin},
    )

    scoped = await orchestrator.run_nightly_collection(platform="youtube")

    assert scoped["platform_filter"] == "youtube"
    assert scoped["sources_seen"] == 1
    assert youtube.calls == ["@openai"]
    assert linkedin.calls == []

    unscoped = await orchestrator.run_nightly_collection()
    assert unscoped["platform_filter"] is None
    assert linkedin.calls == ["in/demo"]

    await db.close()


@pytest.mark.asyncio
async def test_pipeline_reresolves_youtube_source_after_rss_404(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_repo = SubjectRepository(conn=db.conn)
    source_repo = SourceRepository(conn=db.conn)
    subject_service = SubjectService(repository=subject_repo)
    source_service = SourceService(source_repo=source_repo, subject_repo=subject_repo)

    await subject_service.create_subject("Vibe Coding", include_terms=["claude code"])
    await source_service.monitor_source(platform="youtube", account_or_channel_id="UCOLD", display_name="OpenAI")
    source = await source_repo.get_by_identity(platform="youtube", account_or_channel_id="UCOLD")
    assert source is not None
    await source_service.merge_source_metadata(source_id=source.id, values={"resolved_from": "@openai"})

    collector = YouTubeNotFoundThenResolvedCollector()
    orchestrator = PipelineOrchestrator(
        subject_service=subject_service,
        source_service=source_service,
        item_repo=ItemRepository(conn=db.conn),
        item_score_repo=ItemScoreRepository(conn=db.conn),
        run_log_repo=RunLogRepository(conn=db.conn),
        collectors={"youtube": collector},
    )

    stats = await orchestrator.run_nightly_collection(platform="youtube")

    assert collector.resolve_calls == ["UCOLD", "@openai"]
    assert collector.collect_calls == ["UCOLD", "UCNEW"]
    assert stats["sources_reresolved"] == 1
    assert stats["sources_not_found"] == 0
    assert stats["items_collected"] == 1
    assert await source_repo.get_by_identity(platform="youtube", account_or_channel_id="UCOLD") is None
    updated = await source_repo.get_by_identity(platform="youtube", account_or_channel_id="UCNEW")
    assert updated is not None
    assert updated.follow_state == "active"

    await db.close()


@pytest.mark.asyncio
async def test_pipeline_marks_youtube_rss_404_inactive_without_reauth_or_circuit(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_repo = SubjectRepository(conn=db.conn)
    source_repo = SourceRepository(conn=db.conn)
    subject_service = SubjectService(repository=subject_repo)
    source_service = SourceService(source_repo=source_repo, subject_repo=subject_repo)

    await subject_service.create_subject("Vibe Coding", include_terms=["claude code"])
    await source_service.monitor_source(platform="youtube", account_or_channel_id="UCDEAD", display_name="Dead Channel")

    collector = YouTubeAlwaysNotFoundCollector()
    orchestrator = PipelineOrchestrator(
        subject_service=subject_service,
        source_service=source_service,
        item_repo=ItemRepository(conn=db.conn),
        item_score_repo=ItemScoreRepository(conn=db.conn),
        run_log_repo=RunLogRepository(conn=db.conn),
        collectors={"youtube": collector},
    )

    stats = await orchestrator.run_nightly_collection(platform="youtube")

    assert stats["sources_not_found"] == 1
    assert stats["sources_needing_reauth"] == 0
    assert stats["collector_errors"] == 0
    assert stats["circuit_broken"] == []
    source = await source_repo.get_by_identity(platform="youtube", account_or_channel_id="UCDEAD")
    assert source is not None
    assert source.follow_state == "inactive"
    assert source.metadata["inactive_reason"] == "channel_not_found"
    assert await source_service.list_monitored_sources() == []

    await db.close()


@pytest.mark.asyncio
async def test_pipeline_handles_mixed_youtube_rss_404_run_without_session_repair_or_circuit(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_repo = SubjectRepository(conn=db.conn)
    source_repo = SourceRepository(conn=db.conn)
    source_service = SourceService(source_repo=source_repo, subject_repo=subject_repo)

    for source_id in ("UCOLD1", "UCDEAD2", "UCDEAD3", "UCDEAD4", "UCDEAD5", "UCOK6", "UCOK7"):
        await source_service.monitor_source(
            platform="youtube",
            account_or_channel_id=source_id,
            display_name=source_id,
        )
    old_source = await source_repo.get_by_identity(platform="youtube", account_or_channel_id="UCOLD1")
    assert old_source is not None
    await source_service.merge_source_metadata(source_id=old_source.id, values={"resolved_from": "@good"})

    collector = MixedYouTubeNotFoundCollector()
    orchestrator = PipelineOrchestrator(
        subject_service=SubjectService(repository=subject_repo),
        source_service=source_service,
        item_repo=ItemRepository(conn=db.conn),
        item_score_repo=ItemScoreRepository(conn=db.conn),
        run_log_repo=RunLogRepository(conn=db.conn),
        collectors={"youtube": collector},
    )

    stats = await orchestrator.run_nightly_collection(platform="youtube", score=False)
    all_sources = await source_service.list_all_sources()
    inactive_sources = [source for source in all_sources if source.follow_state == "inactive"]

    assert "@good" in collector.resolve_calls
    assert "UCNEW1" in collector.collect_calls
    assert stats["items_collected"] == 3
    assert stats["sources_reresolved"] == 1
    assert stats["sources_not_found"] == 4
    assert stats["sources_needing_reauth"] == 0
    assert stats["collector_errors"] == 0
    assert stats["circuit_broken"] == []
    assert sorted(source.account_or_channel_id for source in inactive_sources) == [
        "UCDEAD2",
        "UCDEAD3",
        "UCDEAD4",
        "UCDEAD5",
    ]
    assert await source_repo.get_by_identity(platform="youtube", account_or_channel_id="UCNEW1") is not None

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
async def test_pipeline_emits_scoring_progress_events(tmp_path: Path) -> None:
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

    events: list[dict] = []
    orchestrator = PipelineOrchestrator(
        subject_service=subject_service,
        source_service=source_service,
        item_repo=ItemRepository(conn=db.conn),
        item_score_repo=ItemScoreRepository(conn=db.conn),
        run_log_repo=RunLogRepository(conn=db.conn),
        collectors={"rss": CountingCollector()},
    )

    stats = await orchestrator.run_nightly_collection(progress_callback=events.append)

    scoring_events = [event for event in events if event.get("kind") == "scoring"]
    assert scoring_events == [
        {
            "kind": "scoring",
            "phase": "scoring",
            "run_id": 1,
            "run_type": "nightly_collection",
            "subject_index": 1,
            "subject_total": 1,
            "subject_id": 1,
            "subject_name": "Vibe Coding",
        }
    ]
    assert stats["scoring_progress_events"] == scoring_events

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
async def test_pipeline_embedding_scorer_uses_subject_description_semantics(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_repo = SubjectRepository(conn=db.conn)
    source_repo = SourceRepository(conn=db.conn)
    subject_service = SubjectService(repository=subject_repo)
    source_service = SourceService(source_repo=source_repo, subject_repo=subject_repo)

    await subject_service.create_subject(
        "Ukraine War News",
        include_terms=["reputable sources", "high quality analytics"],
        description_text="Ukraine war news, frontline analysis, Kyiv and Russia updates from reputable sources.",
    )
    await source_service.monitor_source(
        platform="rss",
        account_or_channel_id="feed://demo",
        display_name="Demo",
    )

    class MixedCollector:
        platform = "rss"

        async def collect_from_source(self, source_id: str) -> list[CollectedItem]:
            return [
                CollectedItem(
                    platform="rss",
                    external_id="ukraine-item",
                    author="analyst",
                    url="https://example.com/ukraine",
                    text="Frontline map update from Kyiv with Russia force movements.",
                    transcript_text=None,
                    published_at=None,
                    metadata={},
                ),
                CollectedItem(
                    platform="rss",
                    external_id="claude-item",
                    author="podcast",
                    url="https://example.com/claude",
                    text="Claude Code podcast biography and tooling chatter.",
                    transcript_text=None,
                    published_at=None,
                    metadata={},
                ),
            ]

    orchestrator = PipelineOrchestrator(
        subject_service=subject_service,
        source_service=source_service,
        item_repo=ItemRepository(conn=db.conn),
        item_score_repo=ItemScoreRepository(conn=db.conn),
        run_log_repo=RunLogRepository(conn=db.conn),
        embedding_service=FakeEmbeddingService(),  # type: ignore[arg-type]
        collectors={"rss": MixedCollector()},
        scorer="embedding",
    )

    stats = await orchestrator.run_nightly_collection()
    candidates = await ItemScoreRepository(conn=db.conn).top_candidates(subject_id=1, limit=2)

    assert stats["embedding_items_scored"] == 2
    assert candidates[0].url == "https://example.com/ukraine"
    assert "semantic_similarity=" in candidates[0].rationale
    assert "keyword_shadow_final=" in candidates[0].rationale

    await db.close()


@pytest.mark.asyncio
async def test_pipeline_embedding_path_uses_batch_rerank_with_full_description(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_repo = SubjectRepository(conn=db.conn)
    source_repo = SourceRepository(conn=db.conn)
    subject_service = SubjectService(repository=subject_repo)
    source_service = SourceService(source_repo=source_repo, subject_repo=subject_repo)

    await subject_service.create_subject(
        "AI Tools & Tips",
        include_terms=["leading ai companies"],
        description_text="Track practical AI agent updates from leading AI companies and thought leaders. Do not boost engagement bait.",
    )
    await source_service.monitor_source(
        platform="rss",
        account_or_channel_id="feed://demo",
        display_name="Demo",
    )

    class AICollector:
        platform = "rss"

        async def collect_from_source(self, source_id: str) -> list[CollectedItem]:
            return [
                CollectedItem(
                    platform="rss",
                    external_id=f"ai-{idx}",
                    author="Anthropic",
                    url=f"https://example.com/ai-{idx}",
                    text=f"Claude agent workflow release with practical implementation detail {idx}",
                    transcript_text=None,
                    published_at=None,
                    metadata={},
                )
                for idx in range(3)
            ]

    model_router = FakeBatchModelRouter()
    orchestrator = PipelineOrchestrator(
        subject_service=subject_service,
        source_service=source_service,
        item_repo=ItemRepository(conn=db.conn),
        item_score_repo=ItemScoreRepository(conn=db.conn),
        run_log_repo=RunLogRepository(conn=db.conn),
        model_router=model_router,  # type: ignore[arg-type]
        embedding_service=FakeEmbeddingService(),  # type: ignore[arg-type]
        collectors={"rss": AICollector()},
        scorer="both",
    )

    stats = await orchestrator.run_nightly_collection()

    assert stats["model_batch_rerank_calls"] == 1
    assert stats["items_model_reranked"] == 3
    assert model_router.batch_calls[0]["subject_name"] == "AI Tools & Tips"
    assert "leading AI companies" in model_router.batch_calls[0]["subject_description"]
    assert "Include:" not in model_router.batch_calls[0]["subject_description"]
    assert "Avoid:" not in model_router.batch_calls[0]["subject_description"]
    assert model_router.batch_calls[0]["candidate_count"] == 3

    await db.close()


@pytest.mark.asyncio
async def test_pipeline_does_not_fall_back_to_serial_rerank_after_empty_batch(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_repo = SubjectRepository(conn=db.conn)
    source_repo = SourceRepository(conn=db.conn)
    subject_service = SubjectService(repository=subject_repo)
    source_service = SourceService(source_repo=source_repo, subject_repo=subject_repo)

    await subject_service.create_subject(
        "AI Tools & Tips",
        include_terms=["claude"],
        description_text="Practical AI agent updates and Claude Code workflow details.",
    )
    await source_service.monitor_source(
        platform="rss",
        account_or_channel_id="feed://demo",
        display_name="Demo",
    )

    class AICollector:
        platform = "rss"

        async def collect_from_source(self, source_id: str) -> list[CollectedItem]:
            return [
                CollectedItem(
                    platform="rss",
                    external_id=f"ai-empty-batch-{idx}",
                    author="Anthropic",
                    url=f"https://example.com/ai-empty-batch-{idx}",
                    text=f"Claude Code agent workflow implementation detail {idx}",
                    transcript_text=None,
                    published_at=None,
                    metadata={},
                )
                for idx in range(2)
            ]

    model_router = EmptyBatchModelRouter()
    orchestrator = PipelineOrchestrator(
        subject_service=subject_service,
        source_service=source_service,
        item_repo=ItemRepository(conn=db.conn),
        item_score_repo=ItemScoreRepository(conn=db.conn),
        run_log_repo=RunLogRepository(conn=db.conn),
        model_router=model_router,  # type: ignore[arg-type]
        embedding_service=FakeEmbeddingService(),  # type: ignore[arg-type]
        collectors={"rss": AICollector()},
        scorer="both",
    )

    stats = await orchestrator.run_nightly_collection()

    assert stats["model_batch_rerank_calls"] == 1
    assert stats["items_model_reranked"] == 0
    assert model_router.batch_calls == 1
    assert model_router.single_calls == 0

    await db.close()


@pytest.mark.asyncio
async def test_rescore_existing_items_can_scope_to_subject_ids(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_repo = SubjectRepository(conn=db.conn)
    source_repo = SourceRepository(conn=db.conn)
    subject_service = SubjectService(repository=subject_repo)
    source_service = SourceService(source_repo=source_repo, subject_repo=subject_repo)
    subject_a = await subject_service.create_subject("Subject A", include_terms=["claude"])
    subject_b = await subject_service.create_subject("Subject B", include_terms=["ukraine"])

    item_repo = ItemRepository(conn=db.conn)
    stats = await item_repo.upsert_many(
        [
            CollectedItem(
                platform="rss",
                external_id="shared",
                author="Author",
                url="https://example.com/shared",
                text="Claude Code workflow detail and Ukraine update.",
                transcript_text=None,
                published_at=None,
                metadata={},
            )
        ]
    )
    item_id = stats["item_ids"][0]
    orchestrator = PipelineOrchestrator(
        subject_service=subject_service,
        source_service=source_service,
        item_repo=item_repo,
        item_score_repo=ItemScoreRepository(conn=db.conn),
        run_log_repo=RunLogRepository(conn=db.conn),
        collectors={},
    )

    events: list[dict] = []
    rescore_stats = await orchestrator.rescore_existing_items(
        subject_ids={subject_b.id},
        progress_callback=events.append,
    )
    score_rows = await (
        await db.conn.execute(
            "SELECT subject_id FROM item_scores WHERE item_id = ? ORDER BY subject_id",
            (item_id,),
        )
    ).fetchall()

    assert rescore_stats["subjects_seen"] == 1
    assert events[0]["kind"] == "scoring"
    assert events[0]["subject_name"] == "Subject B"
    assert events[0]["subject_total"] == 1
    assert [int(row["subject_id"]) for row in score_rows] == [subject_b.id]
    assert subject_a.id not in {int(row["subject_id"]) for row in score_rows}

    await db.close()


@pytest.mark.asyncio
async def test_rescore_existing_items_emits_cold_cache_progress_event(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_repo = SubjectRepository(conn=db.conn)
    source_repo = SourceRepository(conn=db.conn)
    subject_service = SubjectService(repository=subject_repo)
    source_service = SourceService(source_repo=source_repo, subject_repo=subject_repo)
    subject = await subject_service.create_subject(
        "AI Tools",
        include_terms=["claude code"],
        description_text="Practical Claude Code and AI tooling updates.",
    )
    item_repo = ItemRepository(conn=db.conn)
    await item_repo.upsert_many(
        [
            CollectedItem(
                platform="rss",
                external_id=f"cold-{index}",
                author="Author",
                url=f"https://example.com/cold-{index}",
                text=f"Claude Code workflow update {index}",
                transcript_text=None,
                published_at=None,
                metadata={},
            )
            for index in range(21)
        ]
    )
    events: list[dict] = []
    orchestrator = PipelineOrchestrator(
        subject_service=subject_service,
        source_service=source_service,
        item_repo=item_repo,
        item_score_repo=ItemScoreRepository(conn=db.conn),
        run_log_repo=RunLogRepository(conn=db.conn),
        embedding_service=FakeEmbeddingService(),  # type: ignore[arg-type]
        scorer="embedding",
    )

    stats = await orchestrator.rescore_existing_items(
        subject_ids={subject.id},
        progress_callback=events.append,
    )
    cold_events = [event for event in events if event.get("kind") == "embedding_not_warmed"]

    assert stats["embedding_not_warmed"] is True
    assert cold_events
    assert cold_events[0]["subject_name"] == "AI Tools"
    assert cold_events[0]["missing_rate"] == 1.0
    assert cold_events[0]["sampled_items"] == 21

    await db.close()


@pytest.mark.asyncio
async def test_pipeline_embedding_failure_is_warned_in_metadata(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_repo = SubjectRepository(conn=db.conn)
    source_repo = SourceRepository(conn=db.conn)
    subject_service = SubjectService(repository=subject_repo)
    source_service = SourceService(source_repo=source_repo, subject_repo=subject_repo)

    await subject_service.create_subject(
        "AI Tools",
        include_terms=["ai tools"],
        description_text="Practical AI tooling updates.",
    )
    await source_service.monitor_source(
        platform="rss",
        account_or_channel_id="feed://demo",
        display_name="Demo",
    )

    orchestrator = PipelineOrchestrator(
        subject_service=subject_service,
        source_service=source_service,
        item_repo=ItemRepository(conn=db.conn),
        item_score_repo=ItemScoreRepository(conn=db.conn),
        run_log_repo=RunLogRepository(conn=db.conn),
        embedding_service=FailingEmbeddingService(),  # type: ignore[arg-type]
        collectors={"rss": DummyRSSCollector()},
        scorer="embedding",
    )

    stats = await orchestrator.run_nightly_collection()
    row = await (
        await db.conn.execute(
            "SELECT metadata_json FROM run_logs WHERE run_type = 'nightly_collection' ORDER BY id DESC LIMIT 1"
        )
    ).fetchone()
    metadata = json.loads(row["metadata_json"])

    assert stats["embedding_degraded"] is True
    assert stats["embedding_fallback_items"] == 1
    assert metadata["embedding_degraded"] is True
    assert metadata["embedding_degraded_subjects"][0]["reason"] == "subject_embedding_unavailable"

    await db.close()


@pytest.mark.asyncio
async def test_nightly_upgrades_legacy_item_scores_to_segment_level_ranking(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_repo = SubjectRepository(conn=db.conn)
    source_repo = SourceRepository(conn=db.conn)
    item_repo = ItemRepository(conn=db.conn)
    item_score_repo = ItemScoreRepository(conn=db.conn)
    subject_service = SubjectService(repository=subject_repo)
    source_service = SourceService(source_repo=source_repo, subject_repo=subject_repo)
    subject = await subject_service.create_subject(
        "AI Tools & Tips",
        include_terms=["practical Claude Code updates"],
        description_text="Find practical Claude Code and Anthropic agent workflow updates.",
    )

    def rows(prefix: str, start_index: int, useful: bool = False) -> list[dict]:
        out = []
        for idx in range(6):
            text = f"{prefix} background discussion about markets and companies."
            if useful and idx >= 3:
                text = (
                    "Anthropic Claude Code release explains agent handoff workflow "
                    "implementation details with a concrete example."
                )
            out.append({"text": text, "start": float((start_index + idx) * 60), "duration": 60.0})
        return out

    useful_rows = rows("Interview", 0, useful=True)
    darth_rows = rows("Darth Vader of Electric Utilities", 0, useful=False)
    upsert_stats = await item_repo.upsert_many(
        [
            CollectedItem(
                platform="youtube",
                external_id="anthropic-video",
                author="Anthropic",
                url="https://www.youtube.com/watch?v=anthropic",
                text="Anthropic release interview",
                transcript_text="\n".join(row["text"] for row in useful_rows),
                published_at=None,
                metadata={"title": "Anthropic release interview", "transcript_rows": useful_rows},
            ),
            CollectedItem(
                platform="youtube",
                external_id="darth-utilities",
                author="Utility Podcast",
                url="https://www.youtube.com/watch?v=darth",
                text="Darth Vader of Electric Utilities",
                transcript_text="\n".join(row["text"] for row in darth_rows),
                published_at=None,
                metadata={"title": "Darth Vader of Electric Utilities", "transcript_rows": darth_rows},
            ),
        ]
    )
    anthropic_id, darth_id = upsert_stats["item_ids"]

    # Simulate rows scored before T-11 existed: item-level score puts the long
    # off-topic episode above the useful Anthropic item.
    await item_score_repo.upsert_score(
        item_id=anthropic_id,
        subject_id=subject.id,
        pass1_score=0.5,
        pass2_score=0.5,
        practicality_score=0.5,
        novelty_score=0.5,
        trust_score=0.5,
        noise_penalty=0.0,
        final_score=0.757,
        rationale="legacy item score",
    )
    await item_score_repo.upsert_score(
        item_id=darth_id,
        subject_id=subject.id,
        pass1_score=0.5,
        pass2_score=0.5,
        practicality_score=0.5,
        novelty_score=0.5,
        trust_score=0.5,
        noise_penalty=0.0,
        final_score=0.758,
        rationale="legacy item score",
    )

    orchestrator = PipelineOrchestrator(
        subject_service=subject_service,
        source_service=source_service,
        item_repo=item_repo,
        item_score_repo=item_score_repo,
        run_log_repo=RunLogRepository(conn=db.conn),
        embedding_service=SegmentAwareEmbeddingService(),  # type: ignore[arg-type]
        collectors={},
        scorer="embedding",
    )

    stats = await orchestrator.run_nightly_collection()
    candidates = await item_score_repo.top_candidates(subject_id=subject.id, limit=2)
    segment_count = await (await db.conn.execute("SELECT COUNT(*) AS c FROM item_segments")).fetchone()
    score_count = await (await db.conn.execute("SELECT COUNT(*) AS c FROM item_segment_scores")).fetchone()
    max_segment_score = await (
        await db.conn.execute(
            "SELECT MAX(final_score) AS s FROM item_segment_scores WHERE item_id = ? AND subject_id = ?",
            (anthropic_id, subject.id),
        )
    ).fetchone()

    assert stats["segments_scored"] >= 4
    assert segment_count["c"] >= 4
    assert score_count["c"] >= 4
    assert candidates[0].item_id == anthropic_id
    assert candidates[1].item_id == darth_id
    assert candidates[0].final_score - candidates[1].final_score >= 0.05
    assert candidates[0].segment_text is not None
    assert "Claude Code release" in candidates[0].segment_text
    assert candidates[0].segment_start_seconds == 180.0
    assert candidates[0].final_score == pytest.approx(float(max_segment_score["s"]))

    await db.close()


@pytest.mark.asyncio
async def test_embedding_backfill_warms_missing_cache_once(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    subject_repo = SubjectRepository(conn=db.conn)
    source_repo = SourceRepository(conn=db.conn)
    item_repo = ItemRepository(conn=db.conn)
    subject_service = SubjectService(repository=subject_repo)
    source_service = SourceService(source_repo=source_repo, subject_repo=subject_repo)
    subject = await subject_service.create_subject(
        "Ukraine War News",
        include_terms=["reputable sources"],
        description_text="Ukraine frontline updates and Kyiv policy analysis.",
    )
    await item_repo.upsert_many(
        [
            CollectedItem(
                platform="rss",
                external_id="ukraine-1",
                author="analyst",
                url="https://example.com/ukraine-1",
                text="Kyiv frontline update with practical policy implications.",
                transcript_text=None,
                published_at=None,
                metadata={},
            )
        ]
    )

    embedding_service = FakeEmbeddingService()
    orchestrator = PipelineOrchestrator(
        subject_service=subject_service,
        source_service=source_service,
        item_repo=item_repo,
        item_score_repo=ItemScoreRepository(conn=db.conn),
        run_log_repo=RunLogRepository(conn=db.conn),
        embedding_service=embedding_service,  # type: ignore[arg-type]
        scorer="embedding",
    )

    first = await orchestrator.backfill_embeddings(concurrency=2, include_segments=True)
    second = await orchestrator.backfill_embeddings(concurrency=2, include_segments=True)

    context = await orchestrator._preference_context(subject.id)
    subject_text = orchestrator._subject_embedding_text(subject=subject, context=context)
    subject_hash = subject_repo.embedding_text_hash(subject_text)
    rows = await item_repo.list_all_for_scoring()
    item_id, item = rows[0]
    item_text = item_repo.embedding_text(item)
    item_hash = item_repo.embedding_text_hash(item_text)

    assert first["subjects_embedded"] == 1
    assert first["items_embedded"] == 1
    assert first["segments_prepared"] == 1
    assert first["segments_embedded"] == 1
    assert second["subjects_skipped"] == 1
    assert second["items_total"] == 0
    assert second["segments_total"] == 0
    assert await subject_repo.get_description_embedding_for_text(
        subject.id,
        model="fake-embedding",
        text_hash=subject_hash,
    ) is not None
    assert await item_repo.get_content_embedding_for_text(
        item_id,
        model="fake-embedding",
        text_hash=item_hash,
    ) is not None

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
async def test_platform_circuit_breaker_stops_bot_shaped_failures_and_records_metadata(tmp_path: Path) -> None:
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

    collector = SequenceCollector(["exception", "exception", "exception"], platform="linkedin")
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
    assert stats["circuit_broken_reason"] == ["bot_shaped"]
    assert stats["sources_skipped_circuit_breaker"] == 3
    row = await (
        await db.conn.execute("SELECT metadata_json FROM run_logs WHERE run_type = 'nightly_collection'")
    ).fetchone()
    assert row is not None
    assert row["metadata_json"]
    assert '"circuit_broken": ["linkedin"]' in row["metadata_json"]
    assert '"circuit_broken_reason": ["bot_shaped"]' in row["metadata_json"]

    await db.close()


@pytest.mark.asyncio
async def test_platform_empty_results_do_not_trip_bot_shaped_breaker(tmp_path: Path) -> None:
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
            account_or_channel_id=f"quiet-{idx}",
            display_name=f"Quiet {idx}",
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
        empty_threshold=25,
    )

    stats = await orchestrator.run_nightly_collection()

    assert collector.calls == [f"quiet-{idx}" for idx in range(6)]
    assert stats["circuit_broken"] == []
    assert stats["sources_skipped_circuit_breaker"] == 0

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
    assert stats["circuit_broken_reason"] == ["bot_shaped"]

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
    assert stats["circuit_broken"] == []
    assert stats["sources_skipped_circuit_breaker"] == 0
    assert stats["items_collected"] == 2

    second = await orchestrator.run_nightly_collection()
    assert second["sources_seen"] == 6

    await db.close()
