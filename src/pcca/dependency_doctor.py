from __future__ import annotations

import re
import warnings
from dataclasses import dataclass
from importlib import import_module, metadata
from pathlib import Path
from typing import Any

try:  # Python 3.11+
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - exercised only on Python 3.9/3.10
    tomllib = None  # type: ignore[assignment]


IMPORT_NAME_OVERRIDES = {
    "python-telegram-bot": "telegram",
    "youtube-transcript-api": "youtube_transcript_api",
    "yt-dlp": "yt_dlp",
    "pywebview": "webview",
}


@dataclass(frozen=True)
class DependencyCheck:
    distribution: str
    import_name: str
    installed: bool
    version: str | None = None
    error: str | None = None


def runtime_dependency_names(pyproject_path: Path | None = None) -> list[str]:
    path = pyproject_path or Path(__file__).resolve().parents[2] / "pyproject.toml"
    dependencies = _read_project_dependencies(path)
    names = [_distribution_name(spec) for spec in dependencies]
    return [name for name in names if name]


def check_runtime_dependencies(pyproject_path: Path | None = None) -> list[DependencyCheck]:
    checks: list[DependencyCheck] = []
    for distribution in runtime_dependency_names(pyproject_path):
        import_name = import_name_for_distribution(distribution)
        version: str | None = None
        try:
            version = metadata.version(distribution)
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                import_module(import_name)
        except Exception as exc:
            checks.append(
                DependencyCheck(
                    distribution=distribution,
                    import_name=import_name,
                    installed=False,
                    version=version,
                    error=f"{type(exc).__name__}: {exc}",
                )
            )
            continue
        checks.append(
            DependencyCheck(
                distribution=distribution,
                import_name=import_name,
                installed=True,
                version=version,
            )
        )
    return checks


def format_dependency_report(checks: list[DependencyCheck]) -> str:
    if not checks:
        return "No runtime dependencies found in pyproject.toml."
    lines: list[str] = []
    for check in checks:
        if check.installed:
            lines.append(f"OK {check.distribution} @ {check.version or 'unknown'} (import {check.import_name})")
        else:
            detail = f" - {check.error}" if check.error else ""
            lines.append(
                f"MISSING {check.distribution} (import {check.import_name}){detail}. "
                "Run: pip install -e \".[dev]\""
            )
    missing = sum(1 for check in checks if not check.installed)
    lines.append(f"Summary: {len(checks) - missing}/{len(checks)} runtime dependencies import cleanly.")
    return "\n".join(lines)


def import_name_for_distribution(distribution: str) -> str:
    return IMPORT_NAME_OVERRIDES.get(distribution, distribution.replace("-", "_").replace(".", "_"))


def _read_project_dependencies(path: Path) -> list[str]:
    if tomllib is not None:
        payload: dict[str, Any] = tomllib.loads(path.read_text(encoding="utf-8"))
        deps = payload.get("project", {}).get("dependencies", [])
        return [str(dep) for dep in deps] if isinstance(deps, list) else []
    return _read_project_dependencies_fallback(path.read_text(encoding="utf-8"))


def _read_project_dependencies_fallback(text: str) -> list[str]:
    match = re.search(r"(?ms)^\[project\].*?^dependencies\s*=\s*\[(.*?)^\]", text)
    if not match:
        return []
    deps: list[str] = []
    for line in match.group(1).splitlines():
        cleaned = line.split("#", 1)[0].strip().rstrip(",")
        if cleaned.startswith(("\"", "'")) and cleaned.endswith(("\"", "'")):
            deps.append(cleaned[1:-1])
    return deps


def _distribution_name(spec: str) -> str:
    base = spec.split(";", 1)[0].strip()
    base = base.split("[", 1)[0].strip()
    match = re.match(r"^[A-Za-z0-9_.-]+", base)
    return match.group(0).lower() if match else ""
