from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass
class CollectedItem:
    platform: str
    external_id: str
    author: str | None
    url: str | None
    text: str | None
    transcript_text: str | None
    published_at: str | None
    metadata: dict


class Collector(Protocol):
    platform: str

    async def collect_from_source(self, source_id: str) -> list[CollectedItem]:
        ...
