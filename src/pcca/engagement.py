from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass
class EngagementSignals:
    views: int | None = None
    likes: int | None = None
    comments: int | None = None
    reposts: int | None = None
    score: int | None = None
    duration_seconds: int | None = None

    @classmethod
    def from_metadata(cls, metadata: dict | None) -> "EngagementSignals":
        metadata = metadata or {}
        return cls(
            views=_int_or_none(metadata.get("view_count") or metadata.get("views")),
            likes=_int_or_none(metadata.get("like_count") or metadata.get("likes")),
            comments=_int_or_none(metadata.get("comment_count") or metadata.get("num_comments")),
            reposts=_int_or_none(metadata.get("repost_count") or metadata.get("share_count")),
            score=_int_or_none(metadata.get("score")),
            duration_seconds=_int_or_none(metadata.get("duration_seconds")),
        )

    def strength(self) -> float:
        weighted = 0.0
        weighted += math.log10((self.views or 0) + 1) * 0.12
        weighted += math.log10((self.likes or 0) + 1) * 0.20
        weighted += math.log10((self.comments or 0) + 1) * 0.25
        weighted += math.log10((self.reposts or 0) + 1) * 0.25
        weighted += math.log10((self.score or 0) + 1) * 0.18
        return max(0.0, min(1.0, weighted))

    def rationale_fragment(self) -> str:
        parts = []
        if self.views is not None:
            parts.append(f"views={self.views}")
        if self.likes is not None:
            parts.append(f"likes={self.likes}")
        if self.comments is not None:
            parts.append(f"comments={self.comments}")
        if self.reposts is not None:
            parts.append(f"reposts={self.reposts}")
        if self.score is not None:
            parts.append(f"score={self.score}")
        if self.duration_seconds is not None:
            parts.append(f"duration_seconds={self.duration_seconds}")
        return ", ".join(parts) if parts else "none"


def _int_or_none(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        normalized = value.strip().lower().replace(",", "")
        multiplier = 1
        if normalized.endswith("k"):
            multiplier = 1_000
            normalized = normalized[:-1]
        elif normalized.endswith("m"):
            multiplier = 1_000_000
            normalized = normalized[:-1]
        try:
            return int(float(normalized) * multiplier)
        except ValueError:
            return None
    return None
