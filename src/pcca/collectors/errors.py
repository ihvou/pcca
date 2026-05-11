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


class BotShapedError(RuntimeError):
    """Collector detected a bot-detection signal (e.g. anti-scrape React
    hydration error, captcha redirect) and produced zero usable items.

    Distinct from `SessionChallengedError` (which is a recoverable
    re-authentication prompt) and from a legitimately-empty result (silent
    return of `[]`). The orchestrator should treat this as bot_shaped for
    circuit-breaker classification — threshold ~5 (fast trip) vs
    empty_legitimate threshold ~25.
    """

    def __init__(
        self,
        *,
        platform: str,
        source_id: str,
        signal: str,
        current_url: str | None = None,
    ) -> None:
        self.platform = platform
        self.source_id = source_id
        self.signal = signal
        self.current_url = current_url
        url_part = f" at {current_url}" if current_url else ""
        super().__init__(
            f"{platform} bot-detection signal for source={source_id}: "
            f"{signal}{url_part}"
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
