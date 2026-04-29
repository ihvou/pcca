from pathlib import Path

import pytest

from pcca.browser.session_manager import BrowserSessionManager


def test_browser_headful_platform_override(tmp_path: Path) -> None:
    manager = BrowserSessionManager(
        profiles_root=tmp_path,
        headless=True,
        headful_platforms={"x", "linkedin"},
    )

    assert manager.should_launch_headless("youtube") is True
    assert manager.should_launch_headless("x") is False
    assert manager.should_launch_headless("linkedin") is False


def test_browser_launch_options_prefer_installed_chrome(tmp_path: Path) -> None:
    manager = BrowserSessionManager(
        profiles_root=tmp_path,
        headless=True,
        headful_platforms={"x"},
        browser_channel="chrome",
    )

    options = manager.launch_options(profile_dir=tmp_path / "x", platform="x")

    assert options["channel"] == "chrome"
    assert options["headless"] is False
    assert options["locale"] == "en-US"


def test_browser_launch_options_can_use_bundled_chromium(tmp_path: Path) -> None:
    manager = BrowserSessionManager(
        profiles_root=tmp_path,
        headless=True,
        browser_channel="bundled",
    )

    options = manager.launch_options(profile_dir=tmp_path / "youtube", platform="youtube")

    assert "channel" not in options
    assert options["headless"] is True


@pytest.mark.asyncio
async def test_empty_result_snapshot_writes_metadata_and_screenshot(tmp_path: Path) -> None:
    class FakePage:
        url = "https://www.youtube.com/@openai/videos"

        def is_closed(self) -> bool:
            return False

        async def title(self) -> str:
            return "OpenAI - YouTube"

        async def evaluate(self, script):
            if "meta[name" in script:
                return "OpenAI videos"
            return {
                "readyState": "complete",
                "url": self.url,
                "bodyTextLength": 12,
            }

        async def content(self) -> str:
            return "<html><title>OpenAI</title><body>No videos yet</body></html>"

        async def screenshot(self, *, path: str, full_page: bool) -> None:
            _ = full_page
            Path(path).write_bytes(b"png")

    manager = BrowserSessionManager(profiles_root=tmp_path / "profiles", debug_dir=tmp_path / "debug")

    metadata_path = await manager.capture_empty_result_snapshot(
        FakePage(),
        platform="youtube",
        source_id="@openai",
        sample_rate=1.0,
    )

    assert metadata_path is not None
    assert metadata_path.name.startswith("youtube_empty_")
    assert metadata_path.with_suffix(".png").exists()
    payload = metadata_path.read_text(encoding="utf-8")
    assert "OpenAI - YouTube" in payload
    assert "No videos yet" in payload
