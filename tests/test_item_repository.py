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

    await db.close()
