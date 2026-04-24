from __future__ import annotations

from dataclasses import dataclass

import pytest

from pcca.services.follow_import_service import FollowImportService, normalize_youtube_subscription_href


@dataclass
class FakeSourceService:
    calls: list[tuple[str, str, str]]

    async def add_source_to_subject(
        self,
        *,
        subject_name: str,
        platform: str,
        account_or_channel_id: str,
        display_name: str | None = None,
        priority: int = 0,
    ) -> None:
        _ = display_name, priority
        self.calls.append((subject_name, platform, account_or_channel_id))


class FakeFollowImportService(FollowImportService):
    async def import_x_follows(self, *, limit: int = 200) -> list[str]:
        _ = limit
        return ["alice", "bob"]

    async def import_linkedin_follows(self, *, limit: int = 200) -> list[str]:
        _ = limit
        return ["in/charlie", "company/acme"]

    async def import_youtube_subscriptions(self, *, limit: int = 200) -> list[str]:
        _ = limit
        return ["@openai", "UC1234567890"]


@pytest.mark.asyncio
async def test_import_to_subject_wires_sources() -> None:
    fake_source_service = FakeSourceService(calls=[])
    service = FakeFollowImportService(session_manager=None, source_service=fake_source_service)  # type: ignore[arg-type]

    count = await service.import_to_subject(subject_name="Vibe Coding", platform="x", limit=10)
    assert count == 2
    assert ("Vibe Coding", "x", "alice") in fake_source_service.calls
    assert ("Vibe Coding", "x", "bob") in fake_source_service.calls


@pytest.mark.asyncio
async def test_import_youtube_to_subject_wires_sources() -> None:
    fake_source_service = FakeSourceService(calls=[])
    service = FakeFollowImportService(session_manager=None, source_service=fake_source_service)  # type: ignore[arg-type]

    count = await service.import_to_subject(subject_name="Vibe Coding", platform="youtube", limit=10)
    assert count == 2
    assert ("Vibe Coding", "youtube", "@openai") in fake_source_service.calls
    assert ("Vibe Coding", "youtube", "UC1234567890") in fake_source_service.calls


def test_normalize_youtube_subscription_href() -> None:
    assert normalize_youtube_subscription_href("/@openai") == "@openai"
    assert normalize_youtube_subscription_href("https://www.youtube.com/channel/UCabc") == "UCabc"
