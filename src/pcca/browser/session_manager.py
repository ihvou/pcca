from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
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
        # Capture profile dirs before clearing the contexts map; we need them for
        # the orphan-Chrome sweep below.
        platforms = list(self._contexts.keys())
        for platform_name, context in self._contexts.items():
            try:
                await context.close()
            except Exception:
                logger.exception("Error closing context for platform=%s", platform_name)
        self._contexts.clear()
        if self._playwright is not None:
            await self._playwright.stop()
            self._playwright = None

        # Playwright launches Chrome with `--disable-features=DestroyProfileOnBrowserClose`;
        # combined with macOS Chrome's "stay alive in dock when last window closes" behavior,
        # `context.close()` does NOT reliably terminate the underlying Chrome process. The
        # next BrowserSessionManager that tries to use the same persistent profile then hits
        # a ProcessSingleton lock and fails.
        # Sweep any orphaned Chrome processes whose --user-data-dir points at one of our
        # platform profiles. Best-effort, macOS / Linux only (Windows uses different tooling).
        for platform_name in platforms:
            await self._kill_orphan_chrome(self.profiles_root / platform_name)

        logger.info("Browser session manager stopped.")

    async def _kill_orphan_chrome(self, profile_dir: Path) -> None:
        """Best-effort: SIGTERM any Chrome whose --user-data-dir matches profile_dir.

        Skips helper subprocesses (renderer/GPU/utility) since killing the parent
        cascades to children. Silent on Windows; the supported platform set is
        reaffirmed by T-34. Errors are logged but never raised — cleanup is
        opportunistic, not required for correctness.
        """
        if sys.platform.startswith("win"):
            return
        marker = f"--user-data-dir={profile_dir}"
        try:
            proc = await asyncio.create_subprocess_exec(
                "ps", "-eo", "pid,args",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
            out, _ = await proc.communicate()
        except Exception:
            logger.debug("Orphan Chrome sweep skipped: ps unavailable", exc_info=True)
            return

        pids_to_kill: list[int] = []
        for line in out.decode("utf-8", errors="replace").splitlines():
            stripped = line.strip()
            if marker not in stripped:
                continue
            # Children carry --type=renderer / utility / gpu-process; killing the
            # parent (the one without --type=) propagates SIGTERM to all of them.
            if "--type=" in stripped:
                continue
            parts = stripped.split(None, 1)
            if not parts:
                continue
            try:
                pids_to_kill.append(int(parts[0]))
            except ValueError:
                continue

        for pid in pids_to_kill:
            try:
                os.kill(pid, signal.SIGTERM)
                logger.info(
                    "Cleaned up orphan Chrome PID=%d for profile=%s", pid, profile_dir
                )
            except ProcessLookupError:
                pass
            except Exception:
                logger.exception("Failed to terminate orphan Chrome PID=%d", pid)

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
        # Proactive orphan-Chrome sweep — if a previous pcca run exited but Chrome
        # stayed alive (Playwright + macOS interaction; see notes in `stop()`), the
        # SingletonLock will block this launch with ProcessSingleton. Killing any
        # process actively holding our profile is safe: we only target processes
        # whose --user-data-dir matches the profile we're about to open.
        singleton_lock = profile_dir / "SingletonLock"
        if singleton_lock.exists():
            logger.info(
                "Profile %s has SingletonLock; sweeping orphan Chrome processes before launch.",
                profile_dir,
            )
            await self._kill_orphan_chrome(profile_dir)
            # Give the OS a moment to release the lock after the kill.
            await asyncio.sleep(0.3)

        options = self.launch_options(profile_dir=profile_dir, platform=platform)
        try:
            context = await self._playwright.chromium.launch_persistent_context(**options)
        except Exception as exc:
            channel = self.effective_browser_channel()
            underlying = f"{type(exc).__name__}: {exc}"
            msg_lower = str(exc).lower()
            # Chrome surfaces "ProcessSingleton" / "user data directory is already in use"
            # when another Chrome process is holding the profile lock. Most common cause:
            # the user logged in earlier, closed the window, but Chrome stayed alive in
            # the macOS dock; the next collector launch fails on the same profile.
            if (
                "processsingleton" in msg_lower
                or "singleton" in msg_lower
                or "user data directory is already in use" in msg_lower
                or "profile appears to be in use" in msg_lower
            ):
                raise RuntimeError(
                    f"Browser profile is already in use:\n"
                    f"  {profile_dir}\n"
                    f"Another Chrome process (likely from a previous login or scrape step) "
                    f"is still holding the profile lock.\n"
                    f"Fix: fully quit that Chrome instance (Cmd+Q on macOS, or kill the "
                    f"process whose --user-data-dir points at the path above), then retry.\n"
                    f"Underlying error: {underlying}"
                ) from exc
            if channel is not None:
                raise RuntimeError(
                    f"Could not launch browser channel '{channel}'.\n"
                    f"Install that browser, or set PCCA_BROWSER_CHANNEL=bundled and run: "
                    f"playwright install chromium\n"
                    f"Underlying error: {underlying}"
                ) from exc
            raise RuntimeError(f"Could not launch browser. Underlying error: {underlying}") from exc
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

    async def inject_session_cookies(self, *, platform: str, cookies: list[dict]) -> int:
        """Import cookies captured from the user's real browser into PCCA's profile.

        After `add_cookies` we navigate the context to the primary cookie domain.
        Without this navigation step, Playwright's persistent-context cookie state
        does not reliably flush to the on-disk `Cookies` SQLite file when launched
        with `channel="chrome"` — meaning the next BrowserSessionManager that
        opens the same profile (e.g. the scraper subprocess) sees no auth cookies
        and the page lands on the logged-out home view. The visit forces Chrome
        to commit the in-memory cookie jar to the persistent profile.
        """
        if not cookies:
            return 0
        context = await self.get_context(platform)
        await context.add_cookies(cookies)

        primary_domain = next(
            (
                str(cookie.get("domain", "")).lstrip(".")
                for cookie in cookies
                if cookie.get("domain")
            ),
            None,
        )
        if primary_domain:
            page = await context.new_page()
            try:
                await page.goto(
                    f"https://{primary_domain}/",
                    wait_until="domcontentloaded",
                    timeout=30000,
                )
                # Brief settle so Chrome's network process flushes the cookie
                # jar into Network/Cookies before we close the context.
                await page.wait_for_timeout(800)
            except Exception:
                logger.debug(
                    "Cookie warm-up navigation to %s failed; cookies may not flush "
                    "to the persistent profile.",
                    primary_domain,
                    exc_info=True,
                )
            finally:
                if not page.is_closed():
                    await page.close()

        logger.info(
            "Injected %d captured cookie(s) into platform=%s profile (domain=%s).",
            len(cookies),
            platform,
            primary_domain or "<unknown>",
        )
        return len(cookies)
