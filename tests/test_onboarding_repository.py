from __future__ import annotations

from pathlib import Path

import pytest

from pcca.db import Database
from pcca.repositories.onboarding import OnboardingRepository


@pytest.mark.asyncio
async def test_onboarding_state_and_staged_sources_persist(tmp_path: Path) -> None:
    db = Database(path=tmp_path / "pcca.db")
    await db.connect()
    await db.initialize()
    assert db.conn is not None

    repo = OnboardingRepository(conn=db.conn)
    state = await repo.get_state()
    assert state.current_step == "start"

    await repo.update_state(
        current_step="sources_imported",
        timezone="UTC",
        digest_time="08:30",
        telegram_verified=True,
    )
    await repo.stage_source(
        platform="youtube",
        account_or_channel_id="@openai",
        display_name="@openai",
        raw_source="@openai",
    )
    await repo.stage_source(
        platform="youtube",
        account_or_channel_id="@openai",
        display_name="OpenAI",
        raw_source="@openai",
    )

    staged = await repo.list_sources()
    assert len(staged) == 1
    assert staged[0].display_name == "OpenAI"
    assert staged[0].status == "pending"

    removed = await repo.mark_removed(staged[0].id)
    assert removed is True
    assert await repo.list_sources() == []

    state = await repo.get_state()
    assert state.current_step == "sources_imported"
    assert state.timezone == "UTC"
    assert state.telegram_verified is True

    await db.close()
