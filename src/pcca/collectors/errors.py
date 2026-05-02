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


class SourceNotFoundError(RuntimeError):
    def __init__(
        self,
        *,
        platform: str,
        source_id: str,
        current_url: str,
        not_found_kind: str = "not_found",
        status_code: int | None = None,
    ) -> None:
        self.platform = platform
        self.source_id = source_id
        self.current_url = current_url
        self.not_found_kind = not_found_kind
        self.status_code = status_code
        status = f" status={status_code}" if status_code is not None else ""
        super().__init__(
            f"{platform} source not found for source={source_id}: "
            f"{not_found_kind}{status} at {current_url}"
        )
