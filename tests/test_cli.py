from __future__ import annotations

import os
import asyncio
import json
from pathlib import Path

import pytest

from pcca import cli
from pcca.collectors.base import CollectedItem
from pcca.config import Settings
from pcca.db import Database
from pcca.dependency_doctor import DependencyCheck
from pcca.repositories.items import ItemRepository
from pcca.repositories.subjects import SubjectRepository
from pcca.services.youtube_transcript_service import TranscriptResult


def _isolate_env(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.chdir(tmp_path)
    for key in list(os.environ):
        if key.startswith("PCCA_"):
            monkeypatch.delenv(key, raising=False)


def test_run_nightly_once_no_backfill_defaults_to_no_score(monkeypatch: pytest.MonkeyPatch, tmp_path, capsys) -> None:
    _isolate_env(monkeypatch, tmp_path)
    calls: list[dict] = []

    class FakePCCAApp:
        def __init__(self, *, settings):
            self.settings = settings

        async def run_nightly_once(self, **kwargs):
            calls.append(kwargs)
            return {"ok": True}

    monkeypatch.setattr(cli, "PCCAApp", FakePCCAApp)

    cli.main(["run-nightly-once", "--no-backfill"])

    assert calls == [{"auto_backfill": False, "score": False}]
    assert "Nightly run completed" in capsys.readouterr().out


def test_run_nightly_once_score_flag_restores_legacy_scoring(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    _isolate_env(monkeypatch, tmp_path)
    calls: list[dict] = []

    class FakePCCAApp:
        def __init__(self, *, settings):
            self.settings = settings

        async def run_nightly_once(self, **kwargs):
            calls.append(kwargs)
            return {"ok": True}

    monkeypatch.setattr(cli, "PCCAApp", FakePCCAApp)

    cli.main(["run-nightly-once", "--score"])

    assert calls == [{"auto_backfill": True, "score": True}]


def test_doctor_command_reports_dependency_health(monkeypatch: pytest.MonkeyPatch, tmp_path, capsys) -> None:
    _isolate_env(monkeypatch, tmp_path)

    monkeypatch.setattr(
        cli,
        "check_runtime_dependencies",
        lambda: [
            DependencyCheck(
                distribution="yt-dlp",
                import_name="yt_dlp",
                installed=True,
                version="2025.10.14",
            )
        ],
    )

    cli.main(["doctor"])

    out = capsys.readouterr().out
    assert "OK yt-dlp @ 2025.10.14" in out
    assert "Summary: 1/1 runtime dependencies import cleanly." in out


def test_doctor_command_exits_nonzero_when_dependency_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    _isolate_env(monkeypatch, tmp_path)

    monkeypatch.setattr(
        cli,
        "check_runtime_dependencies",
        lambda: [
            DependencyCheck(
                distribution="yt-dlp",
                import_name="yt_dlp",
                installed=False,
                error="ModuleNotFoundError: No module named yt_dlp",
            )
        ],
    )

    with pytest.raises(SystemExit) as excinfo:
        cli.main(["doctor"])

    assert excinfo.value.code == 1


def test_repair_subject_descriptions_command_updates_description_and_invalidates_embedding(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    _isolate_env(monkeypatch, tmp_path)

    async def setup() -> int:
        settings = Settings.from_env()
        settings.ensure_dirs()
        db = Database(path=settings.db_path)
        await db.connect()
        await db.initialize()
        assert db.conn is not None
        try:
            subject = await SubjectRepository(conn=db.conn).create(
                "Ukraine War News",
                include_terms=["reputable sources"],
                description_text="corrupted scaffold",
            )
            await SubjectRepository(conn=db.conn).save_description_embedding(
                subject.id,
                model="fake",
                embedding=[0.1],
                text_hash="old",
            )
            return subject.id
        finally:
            await db.close()

    subject_id = asyncio.run(setup())
    cli.main(
        [
            "repair-subject-descriptions",
            "--json",
            json.dumps({str(subject_id): "Track Ukraine war news from frontline sources."}),
        ]
    )

    async def inspect() -> tuple[str | None, str | None]:
        settings = Settings.from_env()
        settings.ensure_dirs()
        db = Database(path=settings.db_path)
        await db.connect()
        await db.initialize()
        assert db.conn is not None
        try:
            row = await (
                await db.conn.execute(
                    "SELECT description_text, description_embedding_json FROM subjects WHERE id = ?",
                    (subject_id,),
                )
            ).fetchone()
            return row["description_text"], row["description_embedding_json"]
        finally:
            await db.close()

    description, embedding_json = asyncio.run(inspect())
    assert description == "Track Ukraine war news from frontline sources."
    assert embedding_json is None


def test_youtube_rebackfill_transcripts_updates_historical_items(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    _isolate_env(monkeypatch, tmp_path)
    fetched: list[str] = []

    class FakeYouTubeTranscriptService:
        async def get_transcript(self, video_id: str) -> TranscriptResult:
            fetched.append(video_id)
            return TranscriptResult(
                text="Translated Ukrainian transcript with concrete frontline details.",
                rows=[{"text": "frontline details", "start": 12.0, "duration": 4.0}],
                language_code="uk",
                translated=True,
            )

    class FakeYtDlpService:
        async def get_transcript(self, video_id: str, *, cookiefile=None):
            _ = cookiefile
            _ = video_id
            return None

    monkeypatch.setattr(cli, "YouTubeTranscriptService", FakeYouTubeTranscriptService)
    monkeypatch.setattr(cli, "YtDlpService", FakeYtDlpService)

    async def setup() -> tuple[int, str | None]:
        settings = Settings.from_env()
        settings.ensure_dirs()
        db = Database(path=settings.db_path)
        await db.connect()
        await db.initialize()
        assert db.conn is not None
        try:
            item_repo = ItemRepository(conn=db.conn)
            stats = await item_repo.upsert_many(
                [
                    CollectedItem(
                        platform="youtube",
                        external_id="video-uk",
                        author="STERNENKO",
                        url="https://www.youtube.com/watch?v=video-uk",
                        text="Original title",
                        transcript_text=None,
                        published_at="2026-05-01T10:00:00",
                        metadata={"existing": True},
                    )
                ]
            )
            item_id = stats["item_ids"][0]
            await item_repo.save_content_embedding(
                item_id,
                model="fake",
                embedding=[0.1],
                text_hash="old",
            )
            row = await (
                await db.conn.execute("SELECT content_hash FROM items WHERE id = ?", (item_id,))
            ).fetchone()
            return item_id, row["content_hash"]
        finally:
            await db.close()

    item_id, old_hash = asyncio.run(setup())

    cli.main(["youtube-rebackfill-transcripts", "--limit", "10", "--concurrency", "1"])

    async def inspect() -> tuple[str | None, dict, str | None, str | None]:
        settings = Settings.from_env()
        settings.ensure_dirs()
        db = Database(path=settings.db_path)
        await db.connect()
        await db.initialize()
        assert db.conn is not None
        try:
            row = await (
                await db.conn.execute(
                    """
                    SELECT transcript_text, metadata_json, content_hash, content_embedding_json
                    FROM items
                    WHERE id = ?
                    """,
                    (item_id,),
                )
            ).fetchone()
            return (
                row["transcript_text"],
                json.loads(row["metadata_json"]),
                row["content_hash"],
                row["content_embedding_json"],
            )
        finally:
            await db.close()

    transcript_text, metadata, new_hash, embedding_json = asyncio.run(inspect())
    assert fetched == ["video-uk"]
    assert transcript_text == "Translated Ukrainian transcript with concrete frontline details."
    assert metadata["transcript_language"] == "uk"
    assert metadata["transcript_translated"] is True
    assert metadata["transcript_rows"] == [{"text": "frontline details", "start": 12.0, "duration": 4.0}]
    assert new_hash != old_hash
    assert embedding_json is None


def test_youtube_rebackfill_passes_cookiefile_to_yt_dlp(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _isolate_env(monkeypatch, tmp_path)
    seen: dict[str, object] = {}
    cookiefile = tmp_path / "cookies.txt"
    cookiefile.write_text("# cookies\n", encoding="utf-8")

    class FakeBrowserSessionManager:
        def __init__(self, **kwargs):
            seen["manager_kwargs"] = kwargs

        async def export_netscape_cookies(self, *, platform: str):
            seen["platform"] = platform
            return cookiefile

        async def stop(self):
            seen["stopped"] = True

    class FakeYtDlpService:
        async def get_transcript(self, video_id: str, *, cookiefile=None):
            seen["video_id"] = video_id
            seen["cookiefile"] = cookiefile
            return TranscriptResult(
                text="Authenticated transcript text.",
                rows=[{"text": "Authenticated transcript text.", "start": 1.0, "duration": 2.0}],
                language_code="en",
                translated=False,
            )

    class UnusedLegacyTranscriptService:
        async def get_transcript(self, video_id: str):
            raise AssertionError(f"legacy transcript fallback should not run for {video_id}")

    monkeypatch.setattr(cli, "BrowserSessionManager", FakeBrowserSessionManager)
    monkeypatch.setattr(cli, "YtDlpService", FakeYtDlpService)
    monkeypatch.setattr(cli, "YouTubeTranscriptService", UnusedLegacyTranscriptService)

    async def setup() -> int:
        settings = Settings.from_env()
        settings.ensure_dirs()
        (settings.browser_profiles_dir / "youtube").mkdir(parents=True)
        db = Database(path=settings.db_path)
        await db.connect()
        await db.initialize()
        assert db.conn is not None
        try:
            item_repo = ItemRepository(conn=db.conn)
            stats = await item_repo.upsert_many(
                [
                    CollectedItem(
                        platform="youtube",
                        external_id="video-auth",
                        author="OpenAI",
                        url="https://www.youtube.com/watch?v=video-auth",
                        text="Original title",
                        transcript_text=None,
                        published_at="2026-05-01T10:00:00",
                        metadata={},
                    )
                ]
            )
            return stats["item_ids"][0]
        finally:
            await db.close()

    item_id = asyncio.run(setup())

    cli.main(["youtube-rebackfill-transcripts", "--limit", "10", "--concurrency", "1"])

    assert seen["platform"] == "youtube"
    assert seen["stopped"] is True
    assert seen["video_id"] == "video-auth"
    assert seen["cookiefile"] == cookiefile

    async def inspect() -> str | None:
        settings = Settings.from_env()
        settings.ensure_dirs()
        db = Database(path=settings.db_path)
        await db.connect()
        await db.initialize()
        assert db.conn is not None
        try:
            row = await (
                await db.conn.execute("SELECT transcript_text FROM items WHERE id = ?", (item_id,))
            ).fetchone()
            return row["transcript_text"]
        finally:
            await db.close()

    assert asyncio.run(inspect()) == "Authenticated transcript text."


def test_youtube_rebackfill_clean_livechat_junk_resets_corrupt_rows(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _isolate_env(monkeypatch, tmp_path)

    class EmptyYtDlpService:
        async def get_transcript(self, video_id: str, *, cookiefile=None):
            return None

    class EmptyLegacyTranscriptService:
        async def get_transcript(self, video_id: str):
            return None

    monkeypatch.setattr(cli, "YtDlpService", EmptyYtDlpService)
    monkeypatch.setattr(cli, "YouTubeTranscriptService", EmptyLegacyTranscriptService)

    junk = '{"live_chat": [{"replayChatItemAction": "spam"}]}'

    async def setup() -> int:
        settings = Settings.from_env()
        settings.ensure_dirs()
        db = Database(path=settings.db_path)
        await db.connect()
        await db.initialize()
        assert db.conn is not None
        try:
            item_repo = ItemRepository(conn=db.conn)
            stats = await item_repo.upsert_many(
                [
                    CollectedItem(
                        platform="youtube",
                        external_id="video-live-chat",
                        author="OpenAI",
                        url="https://www.youtube.com/watch?v=video-live-chat",
                        text=f"Original title\n\n{junk[:1200]}",
                        transcript_text=junk,
                        published_at="2026-05-01T10:00:00",
                        metadata={"transcript_rows": [{"text": "junk"}], "existing": True},
                    )
                ]
            )
            item_id = stats["item_ids"][0]
            await item_repo.save_content_embedding(
                item_id,
                model="fake",
                embedding=[0.1],
                text_hash="old",
            )
            await db.conn.execute(
                """
                INSERT INTO item_segments(item_id, start_offset, end_offset, segment_text, segment_type)
                VALUES (?, 0, 5, 'junk', 'transcript')
                """,
                (item_id,),
            )
            await db.conn.commit()
            return item_id
        finally:
            await db.close()

    item_id = asyncio.run(setup())

    cli.main(["youtube-rebackfill-transcripts", "--clean-livechat-junk", "--limit", "1", "--concurrency", "1"])

    async def inspect() -> tuple[str | None, str | None, dict, str | None, int]:
        settings = Settings.from_env()
        settings.ensure_dirs()
        db = Database(path=settings.db_path)
        await db.connect()
        await db.initialize()
        assert db.conn is not None
        try:
            row = await (
                await db.conn.execute(
                    """
                    SELECT transcript_text, raw_text, metadata_json, content_embedding_json
                    FROM items
                    WHERE id = ?
                    """,
                    (item_id,),
                )
            ).fetchone()
            segment_row = await (
                await db.conn.execute("SELECT COUNT(*) AS c FROM item_segments WHERE item_id = ?", (item_id,))
            ).fetchone()
            return (
                row["transcript_text"],
                row["raw_text"],
                json.loads(row["metadata_json"]),
                row["content_embedding_json"],
                int(segment_row["c"]),
            )
        finally:
            await db.close()

    transcript_text, raw_text, metadata, embedding_json, segment_count = asyncio.run(inspect())
    assert transcript_text is None
    assert raw_text == "Original title"
    assert metadata["existing"] is True
    assert metadata["transcript_cleaned_livechat_junk"] is True
    assert "transcript_rows" not in metadata
    assert embedding_json is None
    assert segment_count == 0
