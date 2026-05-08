from __future__ import annotations

from pcca.dependency_doctor import (
    check_runtime_dependencies,
    format_dependency_report,
    import_name_for_distribution,
    runtime_dependency_names,
)


def test_runtime_dependencies_declared_in_pyproject_are_importable() -> None:
    checks = check_runtime_dependencies()
    missing = [check for check in checks if not check.installed]

    assert not missing, format_dependency_report(checks)


def test_dependency_doctor_maps_distribution_names_to_import_names() -> None:
    names = runtime_dependency_names()

    assert "yt-dlp" in names
    assert import_name_for_distribution("yt-dlp") == "yt_dlp"
    assert import_name_for_distribution("python-telegram-bot") == "telegram"
    assert import_name_for_distribution("pywebview") == "webview"
