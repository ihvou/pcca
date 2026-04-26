from pathlib import Path

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
