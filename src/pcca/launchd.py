from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
import plistlib
import subprocess
import sys
from typing import Callable, Sequence

from pcca.config import Settings


LAUNCHD_LABEL = "com.pcca.nightly"


@dataclass(frozen=True)
class LaunchdInstallResult:
    label: str
    plist_path: Path
    loaded: bool
    message: str


def nightly_log_path(settings: Settings, *, today: date | None = None) -> Path:
    day = today or date.today()
    return settings.data_dir / "logs" / f"nightly-{day.isoformat()}.log"


def parse_daily_cron(cron: str) -> dict[str, int]:
    parts = cron.split()
    if len(parts) != 5:
        raise ValueError(f"Expected 5-field crontab, got: {cron!r}")
    minute, hour, day_of_month, month, day_of_week = parts
    if day_of_month != "*" or month != "*" or day_of_week != "*":
        raise ValueError(
            "launchd install currently supports daily crons only, "
            f"expected '* * *' date fields, got: {cron!r}"
        )
    try:
        minute_i = int(minute)
        hour_i = int(hour)
    except ValueError as exc:
        raise ValueError(f"launchd install requires numeric minute/hour, got: {cron!r}") from exc
    if not (0 <= minute_i <= 59 and 0 <= hour_i <= 23):
        raise ValueError(f"launchd install received out-of-range minute/hour: {cron!r}")
    return {"Minute": minute_i, "Hour": hour_i}


def build_nightly_plist(
    *,
    settings: Settings,
    executable: str | None = None,
    working_directory: Path | None = None,
    label: str = LAUNCHD_LABEL,
) -> dict:
    settings.ensure_dirs()
    log_dir = settings.data_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    calendar = parse_daily_cron(settings.nightly_cron)
    return {
        "Label": label,
        "ProgramArguments": [
            executable or sys.executable,
            "-m",
            "pcca.cli",
            "nightly-once",
        ],
        "WorkingDirectory": str((working_directory or Path.cwd()).resolve()),
        "StartCalendarInterval": calendar,
        "Wake": True,
        "RunAtLoad": False,
        "KeepAlive": False,
        "StandardOutPath": str((log_dir / "launchd-nightly.out.log").resolve()),
        "StandardErrorPath": str((log_dir / "launchd-nightly.err.log").resolve()),
        "EnvironmentVariables": {
            "PYTHONUNBUFFERED": "1",
        },
    }


def default_launch_agents_dir() -> Path:
    return Path.home() / "Library" / "LaunchAgents"


def install_launchd(
    *,
    settings: Settings,
    executable: str | None = None,
    working_directory: Path | None = None,
    launch_agents_dir: Path | None = None,
    runner: Callable[[Sequence[str]], subprocess.CompletedProcess] | None = None,
    label: str = LAUNCHD_LABEL,
) -> LaunchdInstallResult:
    target_dir = launch_agents_dir or default_launch_agents_dir()
    target_dir.mkdir(parents=True, exist_ok=True)
    plist_path = target_dir / f"{label}.plist"
    plist = build_nightly_plist(
        settings=settings,
        executable=executable,
        working_directory=working_directory,
        label=label,
    )
    with plist_path.open("wb") as handle:
        plistlib.dump(plist, handle, sort_keys=False)

    run = runner or (lambda cmd: subprocess.run(cmd, check=False, capture_output=True, text=True))
    run(["launchctl", "unload", "-w", str(plist_path)])
    loaded = run(["launchctl", "load", "-w", str(plist_path)])
    if getattr(loaded, "returncode", 0) not in {0, None}:
        raise RuntimeError(getattr(loaded, "stderr", "") or "launchctl load failed")
    return LaunchdInstallResult(
        label=label,
        plist_path=plist_path,
        loaded=True,
        message=(
            f"Installed {label} at {plist_path}. Verify with: "
            f"launchctl list | grep {label}"
        ),
    )


def uninstall_launchd(
    *,
    launch_agents_dir: Path | None = None,
    runner: Callable[[Sequence[str]], subprocess.CompletedProcess] | None = None,
    label: str = LAUNCHD_LABEL,
) -> LaunchdInstallResult:
    plist_path = (launch_agents_dir or default_launch_agents_dir()) / f"{label}.plist"
    run = runner or (lambda cmd: subprocess.run(cmd, check=False, capture_output=True, text=True))
    if plist_path.exists():
        run(["launchctl", "unload", "-w", str(plist_path)])
        plist_path.unlink()
    return LaunchdInstallResult(
        label=label,
        plist_path=plist_path,
        loaded=False,
        message=f"Uninstalled {label}. Verify with: launchctl list | grep {label}",
    )
