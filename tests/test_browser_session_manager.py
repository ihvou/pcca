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
