from __future__ import annotations

import runpy
import importlib.util
from pathlib import Path

import pytest


SOURCE = (
    Path(__file__).resolve().parents[2]
    / "optional-skills"
    / "security"
    / "godmode"
    / "scripts"
    / "auto_jailbreak.py"
)


def _isolated_script(tmp_path: Path) -> Path:
    target = tmp_path / "scripts" / "auto_jailbreak.py"
    target.parent.mkdir()
    target.write_bytes(SOURCE.read_bytes())
    return target


def test_auto_jailbreak_honors_absolute_versioned_config_binding(
    tmp_path, monkeypatch
):
    candidate = tmp_path / "candidate" / "config.yaml"
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "state"))
    monkeypatch.setenv("HERMES_CONFIG_PATH", str(candidate))

    namespace = runpy.run_path(str(_isolated_script(tmp_path)))

    assert namespace["CONFIG_PATH"] == candidate


def test_auto_jailbreak_rejects_relative_config_binding(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "state"))
    monkeypatch.setenv("HERMES_CONFIG_PATH", "relative/config.yaml")

    with pytest.raises(RuntimeError, match="must be absolute"):
        runpy.run_path(str(_isolated_script(tmp_path)))


def test_canvas_error_points_to_versioned_env(tmp_path, monkeypatch, capsys):
    source = (
        Path(__file__).resolve().parents[2]
        / "optional-skills"
        / "productivity"
        / "canvas"
        / "scripts"
        / "canvas_api.py"
    )
    spec = importlib.util.spec_from_file_location("canvas_binding_test", source)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    candidate_env = tmp_path / "candidate" / ".env"
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "state"))
    monkeypatch.setenv("HERMES_ENV_PATH", str(candidate_env))
    module.CANVAS_API_TOKEN = ""
    module.CANVAS_BASE_URL = ""

    with pytest.raises(SystemExit) as exc:
        module._check_config()

    assert exc.value.code == 1
    assert str(candidate_env) in capsys.readouterr().err
