from __future__ import annotations

from pathlib import Path

import pytest

from pcca.config import Settings
from pcca.desktop_shell import LINUX_UNSUPPORTED_MESSAGE
from pcca.services.desktop_command_service import CommandResult


def make_settings(tmp_path: Path) -> Settings:
    data_dir = tmp_path / ".pcca"
    return Settings(
        timezone="UTC",
        nightly_cron="0 1 * * *",
        morning_cron="30 8 * * *",
        digest_auto_send=False,
        data_dir=data_dir,
        db_path=data_dir / "pcca.db",
        browser_profiles_dir=data_dir / "browser_profiles",
        browser_headless=True,
        browser_headful_platforms={"x", "linkedin"},
        browser_channel="chrome",
        ollama_enabled=False,
        ollama_base_url="http://localhost:11434",
        ollama_model="qwen2.5:7b",
        telegram_bot_token=None,
    )


def test_tkinter_removed_from_src() -> None:
    src = Path("src")
    offenders = [path for path in src.rglob("*.py") if "import tkinter" in path.read_text(encoding="utf-8")]
    assert offenders == []


def test_linux_message_is_explicit() -> None:
    assert "Linux desktop is not yet supported" in LINUX_UNSUPPORTED_MESSAGE
    assert "T-35" in LINUX_UNSUPPORTED_MESSAGE


def test_wizard_platform_dropdown_has_all_option() -> None:
    from pcca.desktop_web.server import INDEX_HTML

    assert "platformEl.add(new Option('All', ''))" in INDEX_HTML
    assert "Get Content (${platformLabel(selectedPlatform)})" in INDEX_HTML
    assert "Get Sources (${platformLabel(selectedPlatform)})" in INDEX_HTML


def test_desktop_server_rejects_missing_token(tmp_path) -> None:
    pytest.importorskip("starlette")
    from starlette.testclient import TestClient

    from pcca.desktop_web.server import DesktopWebServer

    class FakeService:
        def __init__(self) -> None:
            self.started = False
            self.stopped = False

        async def startup_for_wizard(self):
            self.started = True
            return CommandResult(True, "started")

        async def shutdown(self):
            self.stopped = True

        async def get_state(self):
            return {
                "settings": {},
                "onboarding": {"current_step": "start"},
                "staged_sources": [],
                "subjects": [],
                "platforms": [],
                "agent_running": True,
                "logs": [],
            }

    fake_service = FakeService()
    server = DesktopWebServer(
        settings=make_settings(tmp_path),
        token="secret-token",
        port=8765,
        service=fake_service,  # type: ignore[arg-type]
    )
    app = server.create_app()

    with TestClient(app) as client:
        assert fake_service.started is True
        assert client.get("/api/state").status_code == 401
        response = client.get("/api/state", headers={"Authorization": "Bearer secret-token"})
        assert response.status_code == 200
    assert fake_service.stopped is True


def test_desktop_server_reports_validation_errors_without_crashing(tmp_path) -> None:
    pytest.importorskip("starlette")
    from starlette.testclient import TestClient

    from pcca.desktop_web.server import DesktopWebServer

    class FakeService:
        async def startup_for_wizard(self):
            return CommandResult(True, "started")

        async def shutdown(self):
            return None

        async def draft_subject(self, *, text: str, subject_id: int | None = None):
            raise ValueError("Describe the subject first.")

    server = DesktopWebServer(
        settings=make_settings(tmp_path),
        token="secret-token",
        port=8765,
        service=FakeService(),  # type: ignore[arg-type]
    )
    app = server.create_app()

    with TestClient(app) as client:
        response = client.post(
            "/api/subjects/draft",
            headers={"Authorization": "Bearer secret-token"},
            json={"text": "", "subject_id": None},
        )
        assert response.status_code == 400
        assert response.json()["message"] == "Describe the subject first."


def test_desktop_server_rebuild_rules_endpoint(tmp_path) -> None:
    pytest.importorskip("starlette")
    from starlette.testclient import TestClient

    from pcca.desktop_web.server import DesktopWebServer

    class FakeService:
        def __init__(self) -> None:
            self.rebuilt: list[tuple[int, str | None]] = []

        async def startup_for_wizard(self):
            return CommandResult(True, "started")

        async def shutdown(self):
            return None

        async def rebuild_subject_rules(self, *, subject_id: int, text: str | None = None):
            self.rebuilt.append((subject_id, text))
            return CommandResult(True, "Rebuilt rules.", {"subject_id": subject_id})

        async def rebuild_all_subject_rules(self):
            return CommandResult(True, "Rebuilt all subjects.", {})

    fake_service = FakeService()
    server = DesktopWebServer(
        settings=make_settings(tmp_path),
        token="secret-token",
        port=8765,
        service=fake_service,  # type: ignore[arg-type]
    )
    app = server.create_app()

    with TestClient(app) as client:
        response = client.post(
            "/api/subjects/rebuild-rules",
            headers={"Authorization": "Bearer secret-token"},
            json={"subject_id": 7},
        )
        assert response.status_code == 200
        assert response.json()["ok"] is True

    assert fake_service.rebuilt == [(7, None)]


def test_desktop_server_long_actions_return_202_and_result_endpoint(tmp_path) -> None:
    pytest.importorskip("starlette")
    from starlette.testclient import TestClient

    from pcca.desktop_web.server import DesktopWebServer

    class FakeService:
        def __init__(self) -> None:
            self.started_action_id: str | None = None

        async def startup_for_wizard(self):
            return CommandResult(True, "started")

        async def shutdown(self):
            return None

        async def read_content(
            self,
            *,
            platform: str | None = None,
            async_response: bool = False,
            action_id: str | None = None,
        ):
            self.started_action_id = action_id
            return CommandResult(
                True,
                "Get Content started.",
                {"pending": True, "action_id": action_id, "platform": platform},
            )

        async def get_action_result(self, *, action_id: str):
            return 200, CommandResult(True, "Get Content finished.", {"action_id": action_id})

    fake_service = FakeService()
    server = DesktopWebServer(
        settings=make_settings(tmp_path),
        token="secret-token",
        port=8765,
        service=fake_service,  # type: ignore[arg-type]
    )
    app = server.create_app()

    with TestClient(app) as client:
        started = client.post(
            "/api/content/read",
            headers={"Authorization": "Bearer secret-token"},
            json={"platform": "youtube"},
        )
        assert started.status_code == 202
        started_json = started.json()
        assert started_json["data"]["pending"] is True
        assert started_json["data"]["action_id"] == started_json["action_id"]
        assert fake_service.started_action_id == started_json["action_id"]

        finished = client.get(
            f"/api/actions/{started_json['action_id']}/result",
            headers={"Authorization": "Bearer secret-token"},
        )
        assert finished.status_code == 200
        assert finished.json()["message"] == "Get Content finished."


def test_desktop_server_wizard_events_endpoint_validates_size(tmp_path) -> None:
    pytest.importorskip("starlette")
    from starlette.testclient import TestClient

    from pcca.desktop_web.server import DesktopWebServer

    class FakeService:
        def __init__(self) -> None:
            self.events: list[dict] = []

        async def startup_for_wizard(self):
            return CommandResult(True, "started")

        async def shutdown(self):
            return None

        async def record_wizard_event(self, event: dict):
            self.events.append(event)
            return CommandResult(True, "recorded")

    fake_service = FakeService()
    server = DesktopWebServer(
        settings=make_settings(tmp_path),
        token="secret-token",
        port=8765,
        service=fake_service,  # type: ignore[arg-type]
    )
    app = server.create_app()

    with TestClient(app) as client:
        ok = client.post(
            "/api/debug/wizard-events",
            headers={"Authorization": "Bearer secret-token"},
            json={"event_kind": "fetch_error", "action_key": "read_content", "error_message": "Load failed"},
        )
        assert ok.status_code == 200
        assert fake_service.events[0]["event_kind"] == "fetch_error"

        too_large = client.post(
            "/api/debug/wizard-events",
            headers={"Authorization": "Bearer secret-token", "Content-Type": "application/json"},
            content="{" + '"x":"' + ("a" * 1100) + '"}',
        )
        assert too_large.status_code == 413


def test_desktop_wizard_has_tabbed_product_surface() -> None:
    from pcca.desktop_web.server import INDEX_HTML

    for tab in ("use", "sources", "config", "debug"):
        assert f'data-tab="{tab}"' in INDEX_HTML
    assert "Capture Session" not in INDEX_HTML
    assert "Re-build briefs" not in INDEX_HTML
    assert "Rebuild Rules" in INDEX_HTML
    assert "Rebuild All Subjects" in INDEX_HTML
    assert "Backfill Embeddings" in INDEX_HTML
    assert "Embeddings not yet warmed" in INDEX_HTML
    assert "Include terms" not in INDEX_HTML
    assert "High-quality examples" not in INDEX_HTML


def test_desktop_wizard_uses_fire_and_poll_for_long_actions() -> None:
    from pcca.desktop_web.server import INDEX_HTML

    assert "async function longAction" in INDEX_HTML
    assert "/api/actions/${encodeURIComponent(actionId)}/result" in INDEX_HTML
    assert "/api/debug/wizard-events" in INDEX_HTML
    assert "actionKey:'stage_follows'" in INDEX_HTML
    assert "actionKey:'read_content'" in INDEX_HTML
    assert "actionKey:'embedding_backfill'" in INDEX_HTML
    assert "actionKey:'get_briefs'" in INDEX_HTML
    assert "actionKey:'rebuild_all_subject_rules'" in INDEX_HTML
    assert "payloadFailureClass(payload) === 'session_challenge'" in INDEX_HTML


def test_desktop_wizard_preserves_form_edits_during_refresh() -> None:
    from pcca.desktop_web.server import INDEX_HTML

    assert "function pauseRefresh" in INDEX_HTML
    assert "function formSnapshot" in INDEX_HTML
    assert "function restoreFormSnapshot" in INDEX_HTML
    assert "subjectDraftStatus" in INDEX_HTML
    assert "setInterval(() => busy ? refreshRunningState() : loadState(), 5000)" in INDEX_HTML
    assert "Get Content (${platformLabel(selectedPlatform)})" in INDEX_HTML
    assert "pollActionResult" in INDEX_HTML
    assert "inflight_actions" in INDEX_HTML
    assert "/api/subjects/rebuild-rules" in INDEX_HTML
    assert "/api/subjects/rebuild-all-rules" in INDEX_HTML
