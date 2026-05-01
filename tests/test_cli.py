from __future__ import annotations

import os

import pytest

from pcca import cli


def _isolate_env(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.chdir(tmp_path)
    for key in list(os.environ):
        if key.startswith("PCCA_"):
            monkeypatch.delenv(key, raising=False)


def test_run_nightly_once_no_backfill_defaults_to_no_score(monkeypatch: pytest.MonkeyPatch, tmp_path, capsys) -> None:
    _isolate_env(monkeypatch, tmp_path)
    calls: list[dict] = []

    class FakePCCAApp:
        def __init__(self, *, settings):
            self.settings = settings

        async def run_nightly_once(self, **kwargs):
            calls.append(kwargs)
            return {"ok": True}

    monkeypatch.setattr(cli, "PCCAApp", FakePCCAApp)

    cli.main(["run-nightly-once", "--no-backfill"])

    assert calls == [{"auto_backfill": False, "score": False}]
    assert "Nightly run completed" in capsys.readouterr().out


def test_run_nightly_once_score_flag_restores_legacy_scoring(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    _isolate_env(monkeypatch, tmp_path)
    calls: list[dict] = []

    class FakePCCAApp:
        def __init__(self, *, settings):
            self.settings = settings

        async def run_nightly_once(self, **kwargs):
            calls.append(kwargs)
            return {"ok": True}

    monkeypatch.setattr(cli, "PCCAApp", FakePCCAApp)

    cli.main(["run-nightly-once", "--score"])

    assert calls == [{"auto_backfill": True, "score": True}]
