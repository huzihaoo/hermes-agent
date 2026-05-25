"""RED test: vm_task_submit should capture session routing context into task meta_json."""

import json
import subprocess

from tools import permission_policy
from tools import vm_task_tool


def _disable_trusted_session(monkeypatch):
    monkeypatch.setattr(vm_task_tool, "_resolve_submitter", lambda user_id="", owner="": ("", ""))


def test_vm_task_submit_captures_session_routing_context_into_meta_json(monkeypatch, tmp_path):
    """vm_task_submit should read session context, then register a completion notifier."""
    _disable_trusted_session(monkeypatch)
    script = tmp_path / "create_task_v2.py"
    script.write_text("print('unused')", encoding="utf-8")
    monkeypatch.setattr(vm_task_tool, "_create_task_script", lambda: script)
    probe = tmp_path / "vm_task_completion_probe.py"
    probe.write_text("print('unused')", encoding="utf-8")
    monkeypatch.setattr(vm_task_tool, "_host_completion_probe_script", lambda: probe)

    # Mock session context
    monkeypatch.setattr(
        vm_task_tool,
        "_session_value",
        lambda name: {
            "HERMES_SESSION_PLATFORM": "feishu",
            "HERMES_SESSION_CHAT_ID": "oc_123",
            "HERMES_SESSION_CHAT_NAME": "PNC Agent 任务话题",
            "HERMES_SESSION_THREAD_ID": "om_456",
            "HERMES_SESSION_USER_ID": "<feishu-user-id>",
            "HERMES_SESSION_USER_NAME": "胡子豪",
            "HERMES_SESSION_KEY": "session_abc",
        }.get(name, ""),
    )

    captured = {}

    def fake_run(cmd, **kwargs):
        captured["cmd"] = cmd
        return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps({"task_id": "t1"}), stderr="")

    monkeypatch.setattr(vm_task_tool.subprocess, "run", fake_run)
    captured_notify = {}

    def fake_terminal_tool(**kwargs):
        captured_notify.update(kwargs)
        return json.dumps({"session_id": "proc_notify", "notify_on_complete": True})

    monkeypatch.setattr("tools.terminal_tool.terminal_tool", fake_terminal_tool)

    result = vm_task_tool.vm_task_submit("title", "goal")

    assert result["success"] is True
    assert result["notify_process"]["started"] is True
    assert result["notify_process"]["session_id"] == "proc_notify"
    assert captured_notify["background"] is True
    assert captured_notify["notify_on_complete"] is True
    assert "vm_task_completion_probe.py" in captured_notify["command"]
    assert "--task-id t1" in captured_notify["command"]
    assert "--meta" in captured["cmd"]
    meta_index = captured["cmd"].index("--meta") + 1
    meta_json = json.loads(captured["cmd"][meta_index])
    assert meta_json["platform"] == "feishu"
    assert meta_json["chat_id"] == "oc_123"
    assert meta_json["chat_name"] == "PNC Agent 任务话题"
    assert meta_json["thread_id"] == "om_456"
    assert meta_json["user_id"] == "<feishu-user-id>"
    assert meta_json["user_name"] == "胡子豪"
    assert meta_json["session_key"] == "session_abc"


def test_vm_task_submit_preserves_routing_meta_when_scheduler_metadata_present(monkeypatch, tmp_path):
    """Scheduler metadata should be additive and must not remove Feishu routing context."""
    _disable_trusted_session(monkeypatch)
    script = tmp_path / "create_task_v2.py"
    script.write_text("print('unused')", encoding="utf-8")
    monkeypatch.setattr(vm_task_tool, "_create_task_script", lambda: script)
    monkeypatch.setattr(
        vm_task_tool,
        "_session_value",
        lambda name: {
            "HERMES_SESSION_PLATFORM": "feishu",
            "HERMES_SESSION_CHAT_ID": "oc_123",
            "HERMES_SESSION_THREAD_ID": "topic:om_456",
            "HERMES_SESSION_USER_ID": "<feishu-user-id>",
            "HERMES_SESSION_KEY": "session_abc",
        }.get(name, ""),
    )
    captured = {}

    def fake_run(cmd, **kwargs):
        captured["cmd"] = cmd
        return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps({"task_id": "t1"}), stderr="")

    monkeypatch.setattr(vm_task_tool.subprocess, "run", fake_run)

    result = vm_task_tool.vm_task_submit(
        "title",
        "goal",
        lane="heavy",
        resource_class="pnc_data",
        repo_scope="pnc_specs",
        workspace_scope="owner_main_repo",
        risk_class="normal",
    )

    assert result["success"] is True
    meta_json = json.loads(captured["cmd"][captured["cmd"].index("--meta") + 1])
    assert meta_json["platform"] == "feishu"
    assert meta_json["chat_id"] == "oc_123"
    assert meta_json["thread_id"] == "topic:om_456"
    assert meta_json["user_id"] == "<feishu-user-id>"
    assert meta_json["session_key"] == "session_abc"
    assert meta_json["lane"] == "heavy"
    assert meta_json["resource_class"] == "pnc_data"
    assert meta_json["repo_scope"] == "pnc_specs"
    assert meta_json["workspace_scope"] == "owner_main_repo"
    assert meta_json["risk_class"] == "normal"


def test_vm_task_submit_uses_explicit_owner_when_user_id_matches_owner_mapping(monkeypatch, tmp_path):
    """Local owner fallback may pass display name as user_id; it should resolve as owner, not member."""
    script = tmp_path / "create_task_v2.py"
    script.write_text("print('unused')", encoding="utf-8")
    monkeypatch.setattr(vm_task_tool, "_create_task_script", lambda: script)
    monkeypatch.setattr(vm_task_tool, "_session_value", lambda name: "")
    monkeypatch.setattr(
        "tools.permission_policy._load_config",
        lambda: {"user_id_mapping": {"ou_owner": "胡子豪"}},
    )
    monkeypatch.setattr("tools.permission_policy.get_user_role_by_id", lambda user_id: "member")
    monkeypatch.setattr("tools.permission_policy.get_user_role", lambda user_name: "owner" if user_name == "胡子豪" else "member")
    captured = {}

    def fake_run(cmd, **kwargs):
        captured["cmd"] = cmd
        return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps({"task_id": "t1"}), stderr="")

    monkeypatch.setattr(vm_task_tool.subprocess, "run", fake_run)

    result = vm_task_tool.vm_task_submit("title", "goal", owner="胡子豪", user_id="胡子豪")

    assert result["success"] is True
    assert "--owner" in captured["cmd"]
    assert captured["cmd"][captured["cmd"].index("--owner") + 1] == "胡子豪"


def test_permission_policy_unmapped_user_id_fails_closed_even_with_elevated_default(monkeypatch):
    monkeypatch.setattr(
        permission_policy,
        "_load_config",
        lambda: {"users": {"default": "owner"}, "user_id_mapping": {}},
    )

    assert permission_policy.get_user_role_by_id("<unknown-feishu-user-id>") == "member"
