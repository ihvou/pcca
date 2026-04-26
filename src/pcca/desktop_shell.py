from __future__ import annotations

import platform

from pcca.config import Settings


LINUX_UNSUPPORTED_MESSAGE = (
    "Linux desktop is not yet supported (T-35); use the CLI subcommands directly."
)


def run_desktop_shell() -> None:
    system = platform.system().lower()
    if system == "linux":
        print(LINUX_UNSUPPORTED_MESSAGE)
        raise SystemExit(2)

    try:
        import webview
    except Exception as exc:  # pragma: no cover - depends on host GUI deps
        raise RuntimeError(
            "Desktop webview dependencies are missing. Run: pip install -e ."
        ) from exc

    from pcca.desktop_web.server import DesktopWebServer

    server = DesktopWebServer(settings=Settings.from_env())
    server.start()
    try:
        webview.create_window("PCCA Onboarding", server.url, width=1180, height=820)
        webview.start()
    finally:
        server.stop()
