from __future__ import annotations

from pathlib import Path

import pytest

from pcca.collectors.base import CollectedItem
from pcca.db import Database
from pcca.repositories.items import ItemRepository


@pytest.mark.asyncio
async def test_item_upsert_many(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    repo = ItemRepository(conn=db.conn)
    first = await repo.upsert_many(
        [
            CollectedItem(
                platform="rss",
                external_id="id-1",
                author="author",
                url="https://example.com/a",
                text="hello",
                transcript_text=None,
                published_at=None,
                metadata={"k": "v"},
            )
        ]
    )
    assert first["inserted"] == 1
    assert first["updated"] == 0

    second = await repo.upsert_many(
        [
            CollectedItem(
                platform="rss",
                external_id="id-1",
                author="author2",
                url="https://example.com/a",
                text="updated",
                transcript_text=None,
                published_at=None,
                metadata={"k": "v2"},
            )
        ]
    )
    assert second["inserted"] == 0
    assert second["updated"] == 1
    assert len(second["item_ids"]) == 1

    unchanged = await repo.upsert_many(
        [
            CollectedItem(
                platform="rss",
                external_id="id-1",
                author="author2",
                url="https://example.com/a",
                text="updated",
                transcript_text=None,
                published_at=None,
                metadata={"k": "v2"},
            )
        ]
    )
    assert unchanged["inserted"] == 0
    assert unchanged["updated"] == 0

    blank_fetch = await repo.upsert_many(
        [
            CollectedItem(
                platform="rss",
                external_id="id-1",
                author="author2",
                url="https://example.com/a",
                text="",
                transcript_text=None,
                published_at=None,
                metadata={"k": "blank"},
            )
        ]
    )
    assert blank_fetch["inserted"] == 0
    assert blank_fetch["updated"] == 0

    row = await (
        await db.conn.execute("SELECT id, raw_text FROM items WHERE platform = 'rss' AND external_id = 'id-1'")
    ).fetchone()
    assert row["raw_text"] == "updated"
    await repo.save_content_embedding(int(row["id"]), model="fake", embedding=[1.0, 0.0], text_hash="hash-v1")
    assert await repo.get_content_embedding(int(row["id"]), model="fake") == [1.0, 0.0]
    assert (
        await repo.get_content_embedding_for_text(int(row["id"]), model="fake", text_hash="hash-v1")
        == [1.0, 0.0]
    )
    assert await repo.get_content_embedding_for_text(int(row["id"]), model="fake", text_hash="hash-v2") is None

    await repo.upsert_many(
        [
            CollectedItem(
                platform="rss",
                external_id="id-1",
                author="author2",
                url="https://example.com/a",
                text="updated again",
                transcript_text=None,
                published_at=None,
                metadata={"k": "v3"},
            )
        ]
    )
    assert await repo.get_content_embedding(int(row["id"]), model="fake") is None

    await db.close()


@pytest.mark.asyncio
async def test_item_upsert_marks_new_transcript_rows_as_changed(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    repo = ItemRepository(conn=db.conn)
    await repo.upsert_many(
        [
            CollectedItem(
                platform="youtube",
                external_id="video-1",
                author="author",
                url="https://www.youtube.com/watch?v=video-1",
                text="title",
                transcript_text="same transcript",
                published_at=None,
                metadata={"title": "title"},
            )
        ]
    )
    refreshed = await repo.upsert_many(
        [
            CollectedItem(
                platform="youtube",
                external_id="video-1",
                author="author",
                url="https://www.youtube.com/watch?v=video-1",
                text="title",
                transcript_text="same transcript",
                published_at=None,
                metadata={
                    "title": "title",
                    "transcript_rows": [{"text": "same transcript", "start": 42.0, "duration": 5.0}],
                },
            )
        ]
    )

    row = await (
        await db.conn.execute("SELECT metadata_json FROM items WHERE platform = 'youtube' AND external_id = 'video-1'")
    ).fetchone()

    assert refreshed["inserted"] == 0
    assert refreshed["updated"] == 1
    assert refreshed["changed_item_ids"] == refreshed["item_ids"]
    assert '"start": 42.0' in row["metadata_json"]

    await db.close()
