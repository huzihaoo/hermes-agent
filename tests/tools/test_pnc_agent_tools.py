"""Tests for pnc_agent_tools.py."""

import json
import subprocess

from tools import pnc_agent_tools


def test_generate_dbc_invokes_remote_agent_with_expected_args(monkeypatch):
    captured = {}
    monkeypatch.setattr(pnc_agent_tools, "_check_pnc_permission", lambda *a, **kw: None)
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_name", lambda: "郭艳彬")
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_id", lambda: "")

    def fake_run(cmd, input, text, capture_output, timeout, check):
        captured.update(
            cmd=cmd,
            input=input,
            text=text,
            capture_output=capture_output,
            timeout=timeout,
            check=check,
        )
        monkeypatch.setattr(pnc_agent_tools, "_check_pnc_permission", lambda *a, **kw: None)
        return subprocess.CompletedProcess(
            cmd,
            0,
            stdout=json.dumps({"ok": True, "exit_code": 0, "stdout": "done", "stderr": ""}),
            stderr="",
        )

    monkeypatch.setattr(pnc_agent_tools.subprocess, "run", fake_run)

    result = json.loads(
        pnc_agent_tools.generate_dbc_tool(
            {
                "project": "p1",
                "platform": "j5",
                "profile": "dev",
                "input": "/tmp/in.dbc",
                "output": "/tmp/out",
                "regression": "/tmp/reg",
                "timeout": 123,
            }
        )
    )

    assert result["ok"] is True
    assert result["agent"] == "generate-dbc"
    assert captured["cmd"] == ["ssh-mini-agent", "run_bash_json"]
    assert captured["timeout"] == 128
    assert "generate-dbc" in captured["input"]
    assert "--input" in captured["input"]
    assert "/tmp/in.dbc" in captured["input"]
    assert "--output" in captured["input"]
    assert "/tmp/out" in captured["input"]


def test_generate_dbc_ensures_user_worktree_before_running(monkeypatch):
    captured = {}
    monkeypatch.setattr(pnc_agent_tools, "_allow_debug_user_override", lambda: True)
    monkeypatch.setattr(pnc_agent_tools, "_check_pnc_permission", lambda *a, **kw: None)
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_name", lambda: "郭艳彬")
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_id", lambda: "")

    def fake_run(cmd, input, text, capture_output, timeout, check):
        captured.update(cmd=cmd, input=input, timeout=timeout)
        return subprocess.CompletedProcess(
            cmd,
            0,
            stdout=json.dumps({"ok": True, "exit_code": 0, "stdout": "done", "stderr": ""}),
            stderr="",
        )

    monkeypatch.setattr(pnc_agent_tools.subprocess, "run", fake_run)

    result = json.loads(
        pnc_agent_tools.generate_dbc_tool(
            {
                "user": "郭艳彬",
                "repo": "pnc_specs",
                "input": "/tmp/in.dbc",
            }
        )
    )

    assert result["ok"] is True
    assert "worktree_manager.py ensure" in captured["input"]
    assert "郭艳彬" in captured["input"]
    assert "ignored-public-user" not in captured["input"]
    assert "pnc_specs" in captured["input"]
    assert "WORKTREE_PATH=$(" in captured["input"]
    assert "cd \"$AGENT_ROOT\"" in captured["input"]


def test_generate_dbc_uses_gateway_sender_when_user_omitted(monkeypatch):
    captured = {}
    monkeypatch.setattr(pnc_agent_tools, "_check_pnc_permission", lambda *a, **kw: None)

    def fake_run(cmd, input, text, capture_output, timeout, check):
        captured.update(cmd=cmd, input=input, timeout=timeout)
        monkeypatch.setattr(pnc_agent_tools, "_check_pnc_permission", lambda *a, **kw: None)
        return subprocess.CompletedProcess(
            cmd,
            0,
            stdout=json.dumps({"ok": True, "exit_code": 0, "stdout": "done", "stderr": ""}),
            stderr="",
        )

    monkeypatch.setattr(pnc_agent_tools.subprocess, "run", fake_run)
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_name", lambda: "宋伟军")
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_id", lambda: "")

    result = json.loads(pnc_agent_tools.generate_dbc_tool({"input": "/tmp/in.dbc"}))

    assert result["ok"] is True
    assert "worktree_manager.py ensure" in captured["input"]
    assert "宋伟军" in captured["input"]
    assert "/home/mini/worktrees/pnc_specs/宋伟军" not in captured["input"]


def test_generate_dbc_maps_gateway_sender_id_when_name_missing(monkeypatch, tmp_path):
    captured = {}
    monkeypatch.setattr(pnc_agent_tools, "_check_pnc_permission", lambda *a, **kw: None)
    roles = tmp_path / "user-roles.json"
    roles.write_text(
        json.dumps({"user_id_mapping": {"ou_test": "郭艳彬"}}, ensure_ascii=False),
        encoding="utf-8",
    )

    def fake_run(cmd, input, text, capture_output, timeout, check):
        captured.update(cmd=cmd, input=input, timeout=timeout)
        monkeypatch.setattr(pnc_agent_tools, "_check_pnc_permission", lambda *a, **kw: None)
        return subprocess.CompletedProcess(
            cmd,
            0,
            stdout=json.dumps({"ok": True, "exit_code": 0, "stdout": "done", "stderr": ""}),
            stderr="",
        )

    monkeypatch.setattr(pnc_agent_tools.subprocess, "run", fake_run)
    monkeypatch.setattr(pnc_agent_tools, "USER_ROLES_CONFIG", str(roles))
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_name", lambda: "")
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_id", lambda: "ou_test")

    result = json.loads(pnc_agent_tools.generate_dbc_tool({"input": "/tmp/in.dbc"}))

    assert result["ok"] is True
    assert "郭艳彬" in captured["input"]
    assert "worktree_manager.py ensure" in captured["input"]


def test_generate_dbc_resolves_user_from_explicit_user_id(monkeypatch):
    captured = {}
    monkeypatch.setattr(pnc_agent_tools, "_check_pnc_permission", lambda *a, **kw: None)

    def fake_run(cmd, input, text, capture_output, timeout, check):
        captured.update(cmd=cmd, input=input, timeout=timeout)
        monkeypatch.setattr(pnc_agent_tools, "_check_pnc_permission", lambda *a, **kw: None)
        return subprocess.CompletedProcess(
            cmd,
            0,
            stdout=json.dumps({"ok": True, "exit_code": 0, "stdout": "done", "stderr": ""}),
            stderr="",
        )

    monkeypatch.setattr(pnc_agent_tools.subprocess, "run", fake_run)
    monkeypatch.setattr(
        pnc_agent_tools,
        "_resolve_user_name_from_id",
        lambda uid: "郭艳彬" if uid == "ou_guo" else "",
    )

    result = json.loads(
        pnc_agent_tools.generate_dbc_tool(
            {"input": "/tmp/in.dbc"},
            user_id="ou_guo",
        )
    )

    assert result["ok"] is True
    assert "worktree_manager.py ensure" in captured["input"]
    assert "郭艳彬" in captured["input"]
    assert "pnc_specs" in captured["input"]


def test_generate_dbc_ignores_public_user_argument_by_default(monkeypatch):
    captured = {}
    monkeypatch.setattr(pnc_agent_tools, "_check_pnc_permission", lambda *a, **kw: None)

    def fake_run(cmd, input, text, capture_output, timeout, check):
        captured.update(input=input)
        return subprocess.CompletedProcess(
            cmd,
            0,
            stdout=json.dumps({"ok": True, "exit_code": 0, "stdout": "done", "stderr": ""}),
            stderr="",
        )

    monkeypatch.setattr(pnc_agent_tools.subprocess, "run", fake_run)
    monkeypatch.setattr(pnc_agent_tools, "_resolve_user_name_from_id", lambda user_id: "会话用户")

    result = json.loads(
        pnc_agent_tools.generate_dbc_tool(
            {"user": "攻击者指定用户", "input": "/tmp/in.dbc"},
            user_id="ou_other",
        )
    )

    assert result["ok"] is True
    assert "会话用户" in captured["input"]
    assert "攻击者指定用户" not in captured["input"]


def test_generate_dbc_prefers_user_id_mapping_over_spoofable_display_name(monkeypatch):
    captured = {}
    monkeypatch.setattr(pnc_agent_tools, "_check_pnc_permission", lambda *a, **kw: None)
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_name", lambda: "宋伟军")
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_id", lambda: "ou_guo")
    monkeypatch.setattr(pnc_agent_tools, "_resolve_user_name_from_id", lambda uid: "郭艳彬" if uid == "ou_guo" else "")

    def fake_run(cmd, input, text, capture_output, timeout, check):
        captured.update(input=input)
        return subprocess.CompletedProcess(
            cmd,
            0,
            stdout=json.dumps({"ok": True, "exit_code": 0, "stdout": "done", "stderr": ""}),
            stderr="",
        )

    monkeypatch.setattr(pnc_agent_tools.subprocess, "run", fake_run)

    result = json.loads(pnc_agent_tools.generate_dbc_tool({"input": "/tmp/in.dbc"}, user_id="ou_guo"))

    assert result["ok"] is True
    assert "郭艳彬" in captured["input"]
    assert "宋伟军" not in captured["input"]


def test_generate_dbc_accepts_ssh_warning_before_json_payload(monkeypatch):
    monkeypatch.setattr(pnc_agent_tools, "_check_pnc_permission", lambda *a, **kw: None)
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_name", lambda: "郭艳彬")
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_id", lambda: "")

    def fake_run(cmd, input, text, capture_output, timeout, check):
        return subprocess.CompletedProcess(
            cmd,
            0,
            stdout=(
                "** WARNING: connection is not using a post-quantum key exchange algorithm.\n"
                "** This session may be vulnerable to store now, decrypt later attacks.\n"
                + json.dumps({"ok": True, "exit_code": 0, "stdout": "done", "stderr": ""})
                + "\n"
            ),
            stderr="",
        )

    monkeypatch.setattr(pnc_agent_tools.subprocess, "run", fake_run)

    result = json.loads(pnc_agent_tools.generate_dbc_tool({"input": "/tmp/in.dbc"}))

    assert result["ok"] is True
    assert result["agent"] == "generate-dbc"
    assert result["stdout"] == "done"


def test_pnc_agents_smoke_resolves_user_and_checks_agent_root(monkeypatch):
    captured = {}
    monkeypatch.setattr(pnc_agent_tools, "_check_pnc_permission", lambda *a, **kw: None)

    def fake_run(cmd, input, text, capture_output, timeout, check):
        captured.update(cmd=cmd, input=input, timeout=timeout)
        return subprocess.CompletedProcess(
            cmd,
            0,
            stdout=json.dumps(
                {
                    "ok": True,
                    "worktree_path": "/home/mini/worktrees/pnc_specs/郭艳彬",
                    "agent_root": "/home/mini/worktrees/pnc_specs/郭艳彬/pnc_tools_ai_native/32_AI_Native_repo_骨架包_真实首批版_v1",
                    "agent_root_exists": True,
                    "generate_dbc_executable": True,
                    "parse_bus_data_executable": True,
                    "ensure_json": {"path": "/home/mini/worktrees/pnc_specs/郭艳彬", "branch": "HEAD", "created": False},
                },
                ensure_ascii=False,
            ),
            stderr="",
        )

    monkeypatch.setattr(pnc_agent_tools.subprocess, "run", fake_run)
    monkeypatch.setattr(
        pnc_agent_tools,
        "_resolve_user_name_from_id",
        lambda uid: "郭艳彬" if uid == "ou_guo" else "",
    )

    result = json.loads(pnc_agent_tools.pnc_agents_smoke_tool({}, user_id="ou_guo"))

    assert result["ok"] is True
    assert result["user"] == "郭艳彬"
    assert result["worktree_path"] == "/home/mini/worktrees/pnc_specs/郭艳彬"
    assert result["agent_root_exists"] is True
    assert result["generate_dbc_executable"] is True
    assert result["parse_bus_data_executable"] is True
    assert "./generate-dbc" not in captured["input"]
    assert "./parse-bus-data" not in captured["input"]
    assert "worktree_manager.py ensure" in captured["input"]


def test_generate_dbc_remote_script_rejects_paths_outside_resolved_worktree(monkeypatch):
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_name", lambda: "郭艳彬")
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_id", lambda: "")

    script = pnc_agent_tools._build_remote_script(
        "generate-dbc",
        {"input": "/home/mini/worktrees/pnc_specs/宋伟军/leak.dbc", "output": "/tmp/out"},
    )

    assert "WORKTREE_REAL=" in script
    assert "path outside resolved worktree for input" in script
    assert "path outside resolved worktree for output" in script
    assert "/home/mini/worktrees/pnc_specs/*" in script
    assert "/home/mini/pnc_specs)" not in script


def test_parse_bus_data_reports_local_wrapper_failure(monkeypatch):
    monkeypatch.setattr(pnc_agent_tools, "_check_pnc_permission", lambda *a, **kw: None)
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_name", lambda: "郭艳彬")
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_id", lambda: "")

    def fake_run(*args, **kwargs):
        return subprocess.CompletedProcess(args[0], 127, stdout="", stderr="ssh-mini-agent: not found")

    monkeypatch.setattr(pnc_agent_tools.subprocess, "run", fake_run)

    result = json.loads(pnc_agent_tools.parse_bus_data_tool({"input": "/tmp/data", "output": "/tmp/out"}))

    assert "error" in result
    assert result["agent"] == "parse-bus-data"
    assert result["exit_code"] == 127
    assert "ssh-mini-agent" in result["stderr"]


def test_rejects_non_absolute_file_paths():
    result = json.loads(pnc_agent_tools.generate_dbc_tool({"input": "relative.dbc"}))

    assert "error" in result
    assert "absolute" in result["error"]


def test_rejects_unresolved_user_before_remote_execution(monkeypatch):
    called = False

    def fake_run(*args, **kwargs):
        nonlocal called
        called = True
        raise AssertionError("subprocess should not be called")

    monkeypatch.setattr(pnc_agent_tools.subprocess, "run", fake_run)
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_name", lambda: "")
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_id", lambda: "")

    result = json.loads(pnc_agent_tools.generate_dbc_tool({"input": "/tmp/in.dbc"}))

    assert "error" in result
    assert "Unable to resolve Feishu user" in result["error"]
    assert called is False


def test_rejects_member_role_before_remote_execution(monkeypatch):
    called = False

    def fake_run(*args, **kwargs):
        nonlocal called
        called = True
        raise AssertionError("subprocess should not be called")

    monkeypatch.setattr(pnc_agent_tools.subprocess, "run", fake_run)
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_name", lambda: "王平")
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_id", lambda: "")
    monkeypatch.setattr(pnc_agent_tools, "_check_pnc_permission", lambda *a, **kw: "permission denied for generate-dbc: role 'member' is not allowed")

    result = json.loads(pnc_agent_tools.generate_dbc_tool({"input": "/tmp/in.dbc"}))

    assert "error" in result
    assert "permission denied" in result["error"]
    assert called is False
