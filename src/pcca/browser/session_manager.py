from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class BrowserSessionManager:
    profiles_root: Path
    headless: bool = True
    headful_platforms: set[str] = field(default_factory=set)
    _playwright: Any = field(default=None, init=False, repr=False)
    _contexts: dict[str, Any] = field(default_factory=dict, init=False, repr=False)

    async def start(self) -> None:
        self.profiles_root.mkdir(parents=True, exist_ok=True)
        try:
            from playwright.async_api import async_playwright
        except Exception as exc:  # pragma: no cover - environment dependent
            raise RuntimeError(
                "Playwright is required for browser collectors. "
                "Install it with: python3 -m pip install playwright && playwright install chromium"
            ) from exc

        self._playwright = await async_playwright().start()
        logger.info("Browser session manager started.")

    async def stop(self) -> None:
        for context in self._contexts.values():
            await context.close()
        self._contexts.clear()
        if self._playwright is not None:
            await self._playwright.stop()
            self._playwright = None
        logger.info("Browser session manager stopped.")

    async def get_context(self, platform: str):
        if self._playwright is None:
            await self.start()

        if platform in self._contexts:
            return self._contexts[platform]

        profile_dir = self.profiles_root / platform
        profile_dir.mkdir(parents=True, exist_ok=True)
        effective_headless = self.should_launch_headless(platform)
        logger.info(
            "Launching browser context platform=%s headless=%s profile=%s",
            platform,
            effective_headless,
            profile_dir,
        )
        context = await self._playwright.chromium.launch_persistent_context(
            user_data_dir=str(profile_dir),
            headless=effective_headless,
            viewport={"width": 1440, "height": 900},
        )
        self._contexts[platform] = context
        return context

    def should_launch_headless(self, platform: str) -> bool:
        return self.headless and platform.strip().lower() not in self.headful_platforms

    async def new_page(self, platform: str):
        context = await self.get_context(platform)
        page = await context.new_page()
        logger.debug("Opened browser page platform=%s", platform)
        page.on(
            "console",
            lambda message: logger.debug(
                "Browser console platform=%s type=%s text=%s",
                platform,
                message.type,
                message.text,
            ),
        )
        page.on(
            "pageerror",
            lambda error: logger.warning("Browser page error platform=%s error=%s", platform, error),
        )
        page.on(
            "requestfailed",
            lambda request: logger.debug(
                "Browser request failed platform=%s url=%s failure=%s",
                platform,
                request.url,
                request.failure,
            ),
        )
        return page
