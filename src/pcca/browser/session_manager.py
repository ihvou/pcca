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
        context = await self._playwright.chromium.launch_persistent_context(
            user_data_dir=str(profile_dir),
            headless=self.headless,
            viewport={"width": 1440, "height": 900},
        )
        self._contexts[platform] = context
        return context

    async def new_page(self, platform: str):
        context = await self.get_context(platform)
        page = await context.new_page()
        return page

