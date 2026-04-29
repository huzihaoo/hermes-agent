"""Tests for shared-state VM task submission tool registration and errors."""

import json
import subprocess

from tools import vm_task_tool
from tools.registry import registry


def _disable_trusted_session(monkeypatch):
    monkeypatch.setattr(vm_task_tool, "_resolve_submitter", lambda user_id="": ("", ""))


def test_vm_task_submit_schema_is_raw_function_schema():
    schema = registry.get_schema("vm_task_submit")

    assert schema["name"] == "vm_task_submit"
    assert schema["parameters"]["type"] == "object"
    assert "function" not in schema

    definition = registry.get_definitions({"vm_task_submit"})[0]
    assert definition["type"] == "function"
    assert definition["function"]["name"] == "vm_task_submit"
    assert "function" not in definition["function"]


def test_vm_task_status_schema_is_raw_function_schema():
    schema = registry.get_schema("vm_task_status")

    assert schema["name"] == "vm_task_status"
    assert schema["parameters"]["required"] == ["task_id"]

    definition = registry.get_definitions({"vm_task_status"})[0]
    assert definition["type"] == "function"
    assert definition["function"]["name"] == "vm_task_status"


def test_vm_task_status_reads_task_status_and_result(monkeypatch, tmp_path):
    root = tmp_path / "shared-state"
    task_id = "task-123"
    task_dir = root / "tasks" / task_id
    task_dir.mkdir(parents=True)
    (task_dir / "status.md").write_text("# Status\nstate: failed\n", encoding="utf-8")
    (task_dir / "result.md").write_text("# Result\nexit_code: 1\n", encoding="utf-8")
    failed = root / "dispatch" / "failed"
    failed.mkdir(parents=True)
    (failed / f"{task_id}.json").write_text(
        json.dumps({"task_id": task_id, "state": "failed", "summary": "boom"}),
        encoding="utf-8",
    )
    monkeypatch.setattr(vm_task_tool, "_DEFAULT_VM_CANONICAL_ROOT", root)

    result = vm_task_tool.vm_task_status(task_id)

    assert result["success"] is True
    assert result["task_id"] == task_id
    assert result["state"] == "failed"
    assert result["dispatch_queue"] == "failed"
    assert result["status_md"].startswith("# Status")
    assert result["result_md"].startswith("# Result")
    assert result["paths"]["task_dir"] == str(task_dir)


def test_vm_task_status_rejects_invalid_task_id():
    result = vm_task_tool.vm_task_status("../bad")

    assert result["success"] is False
    assert "invalid task_id" in result["error"]


def test_vm_task_status_reports_missing_task(monkeypatch, tmp_path):
    monkeypatch.setattr(vm_task_tool, "_DEFAULT_VM_CANONICAL_ROOT", tmp_path / "shared-state")

    result = vm_task_tool.vm_task_status("missing-task")

    assert result["success"] is False
    assert result["state"] == "missing"


def test_vm_task_submit_returns_structured_timeout(monkeypatch, tmp_path):
    _disable_trusted_session(monkeypatch)
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
    _disable_trusted_session(monkeypatch)
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
    _disable_trusted_session(monkeypatch)
    script = tmp_path / "create_task_v2.py"
    script.write_text("print('unused')", encoding="utf-8")
    monkeypatch.setattr(vm_task_tool, "_create_task_script", lambda: script)
    monkeypatch.setattr(vm_task_tool.subprocess, "run", lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("boom")))

    payload = json.loads(vm_task_tool.vm_task_submit_json("title", "goal"))

    assert payload["success"] is False
    assert "RuntimeError" in payload["error"]


def test_vm_task_submit_uses_trusted_session_owner_and_ignores_arg(monkeypatch, tmp_path):
    script = tmp_path / "create_task_v2.py"
    script.write_text("print('unused')", encoding="utf-8")
    monkeypatch.setattr(vm_task_tool, "_create_task_script", lambda: script)
    monkeypatch.setattr(vm_task_tool, "_resolve_submitter", lambda user_id="": ("郭艳彬", "ou_guo"))
    monkeypatch.setattr(vm_task_tool, "_check_vm_task_permission", lambda *a, **kw: None)
    captured = {}

    def fake_run(cmd, **kwargs):
        captured["cmd"] = cmd
        return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps({"task_id": "t1"}), stderr="")

    monkeypatch.setattr(vm_task_tool.subprocess, "run", fake_run)

    result = vm_task_tool.vm_task_submit("title", "goal", owner="spoofed", user_id="ou_guo")

    assert result["success"] is True
    assert "--owner" in captured["cmd"]
    owner_index = captured["cmd"].index("--owner") + 1
    assert captured["cmd"][owner_index] == "郭艳彬"
    assert "spoofed" not in captured["cmd"]


def test_vm_task_submit_adds_vm_path_contract_to_goal(monkeypatch, tmp_path):
    _disable_trusted_session(monkeypatch)
    script = tmp_path / "create_task_v2.py"
    script.write_text("print('unused')", encoding="utf-8")
    monkeypatch.setattr(vm_task_tool, "_create_task_script", lambda: script)
    captured = {}

    def fake_run(cmd, **kwargs):
        goal_file = cmd[cmd.index("--goal-file") + 1]
        captured["goal"] = open(goal_file, encoding="utf-8").read()
        return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps({"task_id": "t1"}), stderr="")

    monkeypatch.setattr(vm_task_tool.subprocess, "run", fake_run)

    result = vm_task_tool.vm_task_submit("title", "do work")

    assert result["success"] is True
    assert "do work" in captured["goal"]
    assert "VM path contract" in captured["goal"]
    assert "/mnt/tmp/<task_id>/" in captured["goal"]
    assert "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/<task_id>/" in captured["goal"]
    assert "/home/mini/<repo>" in captured["goal"]
    assert "/home/mini/worktrees/<repo>/<user>" in captured["goal"]
    assert "If the user asks where a download/output/path is" in captured["goal"]


def test_vm_task_submit_denies_member_before_creating_task(monkeypatch, tmp_path):
    script = tmp_path / "create_task_v2.py"
    script.write_text("print('unused')", encoding="utf-8")
    monkeypatch.setattr(vm_task_tool, "_create_task_script", lambda: script)
    monkeypatch.setattr(vm_task_tool, "_resolve_submitter", lambda user_id="": ("王平", "ou_wang"))
    monkeypatch.setattr(vm_task_tool, "_check_vm_task_permission", lambda *a, **kw: "permission denied for vm_task_submit: role 'member' is not allowed")
    called = False

    def fake_run(*args, **kwargs):
        nonlocal called
        called = True
        raise AssertionError("subprocess should not run")

    monkeypatch.setattr(vm_task_tool.subprocess, "run", fake_run)

    result = vm_task_tool.vm_task_submit("title", "goal", owner="spoofed", user_id="ou_wang")

    assert result["success"] is False
    assert "permission denied" in result["error"]
    assert called is False
