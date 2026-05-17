from __future__ import annotations

import plistlib
import subprocess
from pathlib import Path

from pcca.config import Settings
from pcca.launchd import install_launchd, parse_daily_cron


def test_t162_install_launchd_writes_and_loads_plist(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("PCCA_DATA_DIR", str(tmp_path / ".pcca"))
    monkeypatch.setenv("PCCA_NIGHTLY_CRON", "0 7 * * *")
    settings = Settings.from_env()
    commands: list[list[str]] = []

    def fake_runner(command):
        commands.append(list(command))
        return subprocess.CompletedProcess(command, 0, "", "")

    result = install_launchd(
        settings=settings,
        executable="/tmp/pcca-python",
        working_directory=tmp_path,
        launch_agents_dir=tmp_path / "LaunchAgents",
        runner=fake_runner,
    )

    assert result.loaded is True
    assert result.plist_path == tmp_path / "LaunchAgents" / "com.pcca.nightly.plist"
    assert commands == [
        ["launchctl", "unload", "-w", str(result.plist_path)],
        ["launchctl", "load", "-w", str(result.plist_path)],
    ]
    with result.plist_path.open("rb") as handle:
        plist = plistlib.load(handle)
    assert plist["Label"] == "com.pcca.nightly"
    assert plist["ProgramArguments"] == ["/tmp/pcca-python", "-m", "pcca.cli", "nightly-once"]
    assert plist["WorkingDirectory"] == str(tmp_path.resolve())
    assert plist["StartCalendarInterval"] == {"Minute": 0, "Hour": 7}
    assert plist["Wake"] is True
    assert plist["RunAtLoad"] is False
    assert plist["KeepAlive"] is False


def test_t162_parse_daily_cron_rejects_non_daily_patterns() -> None:
    assert parse_daily_cron("15 6 * * *") == {"Minute": 15, "Hour": 6}
    try:
        parse_daily_cron("*/5 * * * *")
    except ValueError as exc:
        assert "numeric minute/hour" in str(exc)
    else:
        raise AssertionError("interval cron should be rejected for launchd install")
