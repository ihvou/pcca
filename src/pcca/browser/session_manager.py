from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


# Stealth init script — runs on every page in the persistent context BEFORE
# any site JS executes. Defeats the most common automation-detection signals
# used by X, Google OAuth, LinkedIn, etc.: `navigator.webdriver`, missing
# `window.chrome.runtime`, empty plugins list, suspicious permissions API.
# Applied alongside `--disable-blink-features=AutomationControlled` and
# `ignore_default_args=['--enable-automation']` in launch_options.
_STEALTH_INIT_SCRIPT = """
// 1. navigator.webdriver — the #1 detection signal. Force undefined.
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });

// 2. window.chrome — Chrome populates this; headless/automation often does not.
if (!window.chrome) {
  window.chrome = {};
}
if (!window.chrome.runtime) {
  window.chrome.runtime = {};
}

// 3. navigator.plugins — automation Chrome ships an empty list; real Chrome has PDF plugins.
Object.defineProperty(navigator, 'plugins', {
  get: () => [
    { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer' },
    { name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai' },
    { name: 'Native Client', filename: 'internal-nacl-plugin' },
  ],
});

// 4. navigator.languages — automation Chrome can ship an empty array.
if (!navigator.languages || navigator.languages.length === 0) {
  Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
}

// 5. permissions API — Notification.permission must agree with the API.
const originalQuery = window.navigator.permissions && window.navigator.permissions.query;
if (originalQuery) {
  window.navigator.permissions.query = (parameters) => (
    parameters && parameters.name === 'notifications'
      ? Promise.resolve({ state: Notification.permission })
      : originalQuery.call(window.navigator.permissions, parameters)
  );
}
"""


@dataclass
class BrowserSessionManager:
    profiles_root: Path
    headless: bool = True
    headful_platforms: set[str] = field(default_factory=set)
    browser_channel: str | None = "chrome"
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
            "Launching browser context platform=%s headless=%s channel=%s profile=%s",
            platform,
            effective_headless,
            self.effective_browser_channel(),
            profile_dir,
        )
        options = self.launch_options(profile_dir=profile_dir, platform=platform)
        try:
            context = await self._playwright.chromium.launch_persistent_context(**options)
        except Exception as exc:
            channel = self.effective_browser_channel()
            if channel is not None:
                raise RuntimeError(
                    f"Could not launch browser channel '{channel}'. Install that browser "
                    "or set PCCA_BROWSER_CHANNEL=bundled and run: playwright install chromium"
                ) from exc
            raise
        # Apply stealth patches to every page opened in this context.
        await context.add_init_script(_STEALTH_INIT_SCRIPT)
        self._contexts[platform] = context
        return context

    def should_launch_headless(self, platform: str) -> bool:
        return self.headless and platform.strip().lower() not in self.headful_platforms

    def effective_browser_channel(self) -> str | None:
        if self.browser_channel is None:
            return None
        channel = self.browser_channel.strip().lower()
        if channel in {"", "bundled", "playwright", "chromium"}:
            return None
        return channel

    def launch_options(self, *, profile_dir: Path, platform: str) -> dict[str, Any]:
        options: dict[str, Any] = {
            "user_data_dir": str(profile_dir),
            "headless": self.should_launch_headless(platform),
            "viewport": {"width": 1440, "height": 900},
            "locale": "en-US",
            # Defeat automation detection used by X, Google OAuth, LinkedIn:
            # - --disable-blink-features=AutomationControlled hides the flag from JS.
            # - --enable-automation is removed (it's the source of the "Chrome is being
            #   controlled by automated test software" banner and webdriver=true signal).
            # Combined with the init script applied in get_context, this gets X login
            # past the api.x.com/onboarding/task.json 400 rejection.
            "args": ["--disable-blink-features=AutomationControlled"],
            "ignore_default_args": ["--enable-automation"],
        }
        channel = self.effective_browser_channel()
        if channel is not None:
            options["channel"] = channel
        return options

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
