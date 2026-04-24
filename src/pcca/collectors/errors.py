from __future__ import annotations


class SessionChallengedError(RuntimeError):
    def __init__(self, *, platform: str, source_id: str, current_url: str, challenge_kind: str) -> None:
        self.platform = platform
        self.source_id = source_id
        self.current_url = current_url
        self.challenge_kind = challenge_kind
        super().__init__(
            f"{platform} session challenged for source={source_id}: "
            f"{challenge_kind} at {current_url}"
        )
