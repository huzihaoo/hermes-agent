"""Tests for shared-state VM task submission tool registration and errors."""

import json
import subprocess

from tools import vm_task_tool
from tools.registry import registry


def test_vm_task_submit_schema_is_raw_function_schema():
    schema = registry.get_schema("vm_task_submit")

    assert schema["name"] == "vm_task_submit"
    assert schema["parameters"]["type"] == "object"
    assert "function" not in schema

    definition = registry.get_definitions({"vm_task_submit"})[0]
    assert definition["type"] == "function"
    assert definition["function"]["name"] == "vm_task_submit"
    assert "function" not in definition["function"]


def test_vm_task_submit_returns_structured_timeout(monkeypatch, tmp_path):
    script = tmp_path / "create_task_v2.py"
    script.write_text("print('unused')", encoding="utf-8")
    monkeypatch.setattr(vm_task_tool, "_create_task_script", lambda: script)

    def fake_run(*args, **kwargs):
        raise subprocess.TimeoutExpired(args[0], timeout=120, output="partial", stderr="slow")

    monkeypatch.setattr(vm_task_tool.subprocess, "run", fake_run)

    result = vm_task_tool.vm_task_submit("title", "goal")

    assert result["success"] is False
    assert result["returncode"] is None
    assert "timed out" in result["error"]


def test_vm_task_submit_returns_structured_launch_error(monkeypatch, tmp_path):
    script = tmp_path / "create_task_v2.py"
    script.write_text("print('unused')", encoding="utf-8")
    monkeypatch.setattr(vm_task_tool, "_create_task_script", lambda: script)

    def fake_run(*args, **kwargs):
        raise FileNotFoundError("python missing")

    monkeypatch.setattr(vm_task_tool.subprocess, "run", fake_run)

    result = vm_task_tool.vm_task_submit("title", "goal")

    assert result["success"] is False
    assert result["returncode"] is None
    assert "failed to launch" in result["error"]


def test_vm_task_submit_json_serializes_errors(monkeypatch, tmp_path):
    script = tmp_path / "create_task_v2.py"
    script.write_text("print('unused')", encoding="utf-8")
    monkeypatch.setattr(vm_task_tool, "_create_task_script", lambda: script)
    monkeypatch.setattr(vm_task_tool.subprocess, "run", lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("boom")))

    payload = json.loads(vm_task_tool.vm_task_submit_json("title", "goal"))

    assert payload["success"] is False
    assert "RuntimeError" in payload["error"]
