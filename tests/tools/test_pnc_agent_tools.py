"""Tests for pnc_agent_tools.py."""

import json
import subprocess

from tools import pnc_agent_tools


def test_generate_dbc_submits_vm_task_instead_of_direct_ssh(monkeypatch):
    captured = {}
    monkeypatch.setattr(pnc_agent_tools, "_check_pnc_permission", lambda *a, **kw: None)
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_name", lambda: "郭艳彬")
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_id", lambda: "")

    def fake_submit(title, goal, owner="", user_id=""):
        captured.update(title=title, goal=goal, owner=owner, user_id=user_id)
        return json.dumps(
            {
                "success": True,
                "task": {"task_id": "task-generate-dbc"},
                "routing": {
                    "host_state": "host-created",
                    "delivery_attempted": True,
                    "next_truth_checks": ["confirm task appears in VM canonical queue before saying delivered-to-VM"],
                },
            },
            ensure_ascii=False,
        )

    monkeypatch.setattr(pnc_agent_tools.vm_task_tool, "vm_task_submit_json", fake_submit)

    result = json.loads(
        pnc_agent_tools.generate_dbc_tool(
            {
                "project": "p1",
                "platform": "j5",
                "profile": "dev",
                "input": "/home/mini/worktrees/pnc_specs/郭艳彬/in.dbc",
                "output": "/home/mini/worktrees/pnc_specs/郭艳彬/out",
                "regression": "/home/mini/worktrees/pnc_specs/郭艳彬/reg",
                "timeout": 123,
            },
            user_id="ou_guo",
        )
    )

    assert result["ok"] is True
    assert result["mode"] == "submitted"
    assert result["agent"] == "generate-dbc"
    assert result["task_id"] == "task-generate-dbc"
    assert result["routing"]["host_state"] == "host-created"
    assert result["routing"]["delivery_attempted"] is True
    assert "canonical queue" in result["routing"]["next_truth_checks"][0]
    assert result["user_message"].startswith("已提交")
    assert "不是完成" in result["user_message"]
    assert result["next_action"]["tool"] == "vm_task_status"
    assert result["next_action"]["task_id"] == "task-generate-dbc"
    assert captured["owner"] == "郭艳彬"
    assert captured["user_id"] == "ou_guo"
    assert "generate-dbc" in captured["title"]
    assert "./generate-dbc" in captured["goal"]
    assert "worktree_manager.py ensure" in captured["goal"]
    assert "/mnt/tmp/pnc-generate-dbc/downloads" in captured["goal"]
    assert "work_tmp_dir=/mnt/tmp/pnc-generate-dbc" in captured["goal"]
    assert "user_visible_path=//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/pnc-generate-dbc/" in captured["goal"]
    assert "/home/mini/nas/miniPan/tmp/pdcl_downloads" not in captured["goal"]
    assert "ssh-mini-agent run_bash_json" not in captured["goal"]


def test_pnc_task_goal_requires_clean_runtime_snapshot_preflight(monkeypatch):
    captured = {}
    monkeypatch.setattr(pnc_agent_tools, "_check_pnc_permission", lambda *a, **kw: None)
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_name", lambda: "宋伟军")
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_id", lambda: "")

    def fake_submit(title, goal, owner="", user_id=""):
        captured.update(title=title, goal=goal, owner=owner, user_id=user_id)
        return json.dumps({"success": True, "task": {"task_id": "task-preflight"}, "routing": {}}, ensure_ascii=False)

    monkeypatch.setattr(pnc_agent_tools.vm_task_tool, "vm_task_submit_json", fake_submit)

    result = json.loads(
        pnc_agent_tools.parse_bus_data_tool(
            {"input": "/mnt/pnc_tools/data/case", "output": "/mnt/tmp/pnc-parse-output"},
            user_id="ou_song",
        )
    )

    assert result["ok"] is True
    assert "Repository freshness preflight" in captured["goal"]
    assert "runtime_worktree: clean-latest" in captured["goal"]
    assert "git fetch origin --prune" in captured["goal"]
    assert "dirty=false" in captured["goal"]
    assert "behind=false" in captured["goal"]
    assert "manifest_sha256" in captured["goal"]
    assert "resolved_snapshot" in captured["goal"]
    assert "commit" in captured["goal"]


def test_parse_bus_data_d1q9_control_udp_defaults_to_standard_profile(monkeypatch):
    captured = {}
    monkeypatch.setattr(pnc_agent_tools, "_check_pnc_permission", lambda *a, **kw: None)
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_name", lambda: "宋伟军")
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_id", lambda: "")

    def fake_submit(title, goal, owner="", user_id=""):
        captured.update(goal=goal)
        return json.dumps({"success": True, "task": {"task_id": "task-d1q9"}, "routing": {}}, ensure_ascii=False)

    monkeypatch.setattr(pnc_agent_tools.vm_task_tool, "vm_task_submit_json", fake_submit)

    result = json.loads(
        pnc_agent_tools.parse_bus_data_tool(
            {
                "input": "/mnt/pnc_tools/pnc_tools/case_data/data-parse/parse-bus-data/D1Q9/D1Q9_Control_UDP_测试数据集/2026-02-03_13-44-53_voice-过路口退的晚",
                "output": "/mnt/tmp/d1q9-asc-output",
            },
            user_id="ou_song",
        )
    )

    assert result["ok"] is True
    assert "./parse-bus-data --project D1Q9 --platform mcu --profile control-udp-bin-to-asc" in captured["goal"]


def test_validate_data_validity_submits_vm_task_with_python_cli(monkeypatch):
    captured = {}
    monkeypatch.setattr(pnc_agent_tools, "_check_pnc_permission", lambda *a, **kw: None)
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_name", lambda: "宋伟军")
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_id", lambda: "")

    def fake_submit(title, goal, owner="", user_id=""):
        captured.update(title=title, goal=goal, owner=owner, user_id=user_id)
        return json.dumps(
            {
                "success": True,
                "task": {"task_id": "task-validate-data-validity"},
                "routing": {"host_state": "host-created", "delivery_attempted": True},
            },
            ensure_ascii=False,
        )

    monkeypatch.setattr(pnc_agent_tools.vm_task_tool, "vm_task_submit_json", fake_submit)

    result = json.loads(
        pnc_agent_tools.validate_data_validity_tool(
            {
                "input": "/mnt/tmp/mcu-validity/input",
                "output": "/mnt/tmp/mcu-validity/output",
            },
            user_id="ou_song",
        )
    )

    assert result["ok"] is True
    assert result["mode"] == "submitted"
    assert result["agent"] == "validate-data-validity"
    assert result["task_id"] == "task-validate-data-validity"
    assert captured["owner"] == "宋伟军"
    assert captured["user_id"] == "ou_song"
    assert "validate-data-validity" in captured["title"]
    assert "python3 src/tools/validate-data-validity/cli.py" in captured["goal"]
    assert "--project d4q --platform soc --profile soc-simple" in captured["goal"]
    assert "work_tmp_dir=/mnt/tmp/pnc-validate-data-validity" in captured["goal"]
    assert "user_visible_path=//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/pnc-validate-data-validity/" in captured["goal"]


def test_validate_data_validity_preserves_explicit_cli_context(monkeypatch):
    captured = {}
    monkeypatch.setattr(pnc_agent_tools, "_check_pnc_permission", lambda *a, **kw: None)
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_name", lambda: "宋伟军")
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_id", lambda: "")

    def fake_submit(title, goal, owner="", user_id=""):
        captured.update(title=title, goal=goal, owner=owner, user_id=user_id)
        return json.dumps({"success": True, "task": {"task_id": "task-explicit-validity"}, "routing": {}}, ensure_ascii=False)

    monkeypatch.setattr(pnc_agent_tools.vm_task_tool, "vm_task_submit_json", fake_submit)

    result = json.loads(
        pnc_agent_tools.validate_data_validity_tool(
            {
                "project": "custom-project",
                "platform": "mcu",
                "profile": "mcu-default",
                "input": "/mnt/tmp/mcu-validity/input",
                "output": "/mnt/tmp/mcu-validity/output",
            },
            user_id="ou_song",
        )
    )

    assert result["ok"] is True
    assert "--project custom-project --platform mcu --profile mcu-default" in captured["goal"]


def test_validate_data_validity_rejects_relative_input_output(monkeypatch):
    monkeypatch.setattr(pnc_agent_tools, "_check_pnc_permission", lambda *a, **kw: None)
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_name", lambda: "宋伟军")
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_id", lambda: "")

    result = json.loads(
        pnc_agent_tools.validate_data_validity_tool(
            {"input": "relative/input", "output": "/mnt/tmp/validity-output"},
            user_id="ou_song",
        )
    )

    assert result["agent"] == "validate-data-validity"
    assert "input must be an absolute VM path" in result["error"]


def test_validate_data_validity_is_exposed_in_feishu_toolset():
    from toolsets import resolve_toolset

    tools = resolve_toolset("hermes-feishu")
    assert "validate_data_validity" in tools



def test_generate_dbc_task_goal_uses_gateway_sender_when_user_omitted(monkeypatch):
    monkeypatch.setattr(pnc_agent_tools, "_check_pnc_permission", lambda *a, **kw: None)
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_name", lambda: "宋伟军")
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_id", lambda: "")
    captured = {}

    def fake_submit(title, goal, owner="", user_id=""):
        captured.update(title=title, goal=goal, owner=owner, user_id=user_id)
        return json.dumps({"success": True, "task": {"task_id": "task-user"}, "routing": {}}, ensure_ascii=False)

    monkeypatch.setattr(pnc_agent_tools.vm_task_tool, "vm_task_submit_json", fake_submit)

    result = json.loads(pnc_agent_tools.generate_dbc_tool({"input": "/tmp/in.dbc"}))

    assert result["ok"] is True
    assert captured["owner"] == "宋伟军"
    assert "worktree_manager.py ensure" in captured["goal"]
    assert "宋伟军" in captured["goal"]
    assert "/home/mini/worktrees/pnc_specs/宋伟军" not in captured["goal"]
    assert "--project D2L3 --platform mcu --profile default" in captured["goal"]


def test_generate_dbc_task_goal_defaults_required_cli_context(monkeypatch):
    monkeypatch.setattr(pnc_agent_tools, "_check_pnc_permission", lambda *a, **kw: None)
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_name", lambda: "郭艳彬")
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_id", lambda: "")
    captured = {}

    def fake_submit(title, goal, owner="", user_id=""):
        captured.update(title=title, goal=goal, owner=owner, user_id=user_id)
        return json.dumps({"success": True, "task": {"task_id": "task-defaults"}, "routing": {}}, ensure_ascii=False)

    monkeypatch.setattr(pnc_agent_tools.vm_task_tool, "vm_task_submit_json", fake_submit)

    result = json.loads(
        pnc_agent_tools.generate_dbc_tool(
            {
                "input": "/home/mini/worktrees/pnc_specs/郭艳彬/in.dbc",
                "output": "/mnt/tmp/pnc-generate-dbc-smoke-output",
            },
            user_id="ou_guo",
        )
    )

    assert result["ok"] is True
    assert "./generate-dbc --project D2L3 --platform mcu --profile default" in captured["goal"]


def test_generate_dbc_task_goal_preserves_explicit_cli_context(monkeypatch):
    monkeypatch.setattr(pnc_agent_tools, "_check_pnc_permission", lambda *a, **kw: None)
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_name", lambda: "郭艳彬")
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_id", lambda: "")
    captured = {}

    def fake_submit(title, goal, owner="", user_id=""):
        captured.update(title=title, goal=goal, owner=owner, user_id=user_id)
        return json.dumps({"success": True, "task": {"task_id": "task-explicit"}, "routing": {}}, ensure_ascii=False)

    monkeypatch.setattr(pnc_agent_tools.vm_task_tool, "vm_task_submit_json", fake_submit)

    result = json.loads(
        pnc_agent_tools.generate_dbc_tool(
            {
                "project": "custom-project",
                "platform": "custom-platform",
                "profile": "custom-profile",
                "input": "/home/mini/worktrees/pnc_specs/郭艳彬/in.dbc",
                "output": "/mnt/tmp/pnc-generate-dbc-smoke-output",
            },
            user_id="ou_guo",
        )
    )

    assert result["ok"] is True
    assert "--project custom-project --platform custom-platform --profile custom-profile" in captured["goal"]


def test_generate_dbc_task_goal_maps_gateway_sender_id_when_name_missing(monkeypatch, tmp_path):
    monkeypatch.setattr(pnc_agent_tools, "_check_pnc_permission", lambda *a, **kw: None)
    roles = tmp_path / "user-roles.json"
    roles.write_text(
        json.dumps({"user_id_mapping": {"ou_test": "郭艳彬"}}, ensure_ascii=False),
        encoding="utf-8",
    )
    captured = {}

    def fake_submit(title, goal, owner="", user_id=""):
        captured.update(title=title, goal=goal, owner=owner, user_id=user_id)
        return json.dumps({"success": True, "task": {"task_id": "task-map"}, "routing": {}}, ensure_ascii=False)

    monkeypatch.setattr(pnc_agent_tools.vm_task_tool, "vm_task_submit_json", fake_submit)
    monkeypatch.setattr(pnc_agent_tools, "USER_ROLES_CONFIG", str(roles))
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_name", lambda: "")
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_id", lambda: "ou_test")

    result = json.loads(pnc_agent_tools.generate_dbc_tool({"input": "/tmp/in.dbc"}))

    assert result["ok"] is True
    assert captured["owner"] == "郭艳彬"
    assert captured["user_id"] == "ou_test"
    assert "requester_user_id: ou_test" in captured["goal"]
    assert "郭艳彬" in captured["goal"]
    assert "worktree_manager.py ensure" in captured["goal"]


def test_generate_dbc_task_goal_resolves_user_from_explicit_user_id(monkeypatch):
    captured = {}
    monkeypatch.setattr(pnc_agent_tools, "_check_pnc_permission", lambda *a, **kw: None)
    monkeypatch.setattr(
        pnc_agent_tools,
        "_resolve_user_name_from_id",
        lambda uid: "郭艳彬" if uid == "ou_guo" else "",
    )

    def fake_submit(title, goal, owner="", user_id=""):
        captured.update(title=title, goal=goal, owner=owner, user_id=user_id)
        return json.dumps({"success": True, "task": {"task_id": "task-explicit"}, "routing": {}}, ensure_ascii=False)

    monkeypatch.setattr(pnc_agent_tools.vm_task_tool, "vm_task_submit_json", fake_submit)

    result = json.loads(
        pnc_agent_tools.generate_dbc_tool(
            {"input": "/tmp/in.dbc"},
            user_id="ou_guo",
        )
    )

    assert result["ok"] is True
    assert captured["owner"] == "郭艳彬"
    assert captured["user_id"] == "ou_guo"
    assert "郭艳彬" in captured["goal"]
    assert "pnc_specs" in captured["goal"]


def test_generate_dbc_task_goal_ignores_public_user_argument_by_default(monkeypatch):
    captured = {}
    monkeypatch.setattr(pnc_agent_tools, "_check_pnc_permission", lambda *a, **kw: None)
    monkeypatch.setattr(pnc_agent_tools, "_resolve_user_name_from_id", lambda user_id: "会话用户")

    def fake_submit(title, goal, owner="", user_id=""):
        captured.update(title=title, goal=goal, owner=owner, user_id=user_id)
        return json.dumps({"success": True, "task": {"task_id": "task-ignore"}, "routing": {}}, ensure_ascii=False)

    monkeypatch.setattr(pnc_agent_tools.vm_task_tool, "vm_task_submit_json", fake_submit)

    result = json.loads(
        pnc_agent_tools.generate_dbc_tool(
            {"user": "攻击者指定用户", "input": "/tmp/in.dbc"},
            user_id="ou_other",
        )
    )

    assert result["ok"] is True
    assert captured["owner"] == "会话用户"
    assert "会话用户" in captured["goal"]
    assert "攻击者指定用户" not in captured["goal"]


def test_generate_dbc_task_goal_prefers_user_id_mapping_over_spoofable_display_name(monkeypatch):
    captured = {}
    monkeypatch.setattr(pnc_agent_tools, "_check_pnc_permission", lambda *a, **kw: None)
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_name", lambda: "宋伟军")
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_id", lambda: "ou_guo")
    monkeypatch.setattr(pnc_agent_tools, "_resolve_user_name_from_id", lambda uid: "郭艳彬" if uid == "ou_guo" else "")

    def fake_submit(title, goal, owner="", user_id=""):
        captured.update(title=title, goal=goal, owner=owner, user_id=user_id)
        return json.dumps({"success": True, "task": {"task_id": "task-prefer"}, "routing": {}}, ensure_ascii=False)

    monkeypatch.setattr(pnc_agent_tools.vm_task_tool, "vm_task_submit_json", fake_submit)

    result = json.loads(pnc_agent_tools.generate_dbc_tool({"input": "/tmp/in.dbc"}, user_id="ou_guo"))

    assert result["ok"] is True
    assert captured["owner"] == "郭艳彬"
    assert "郭艳彬" in captured["goal"]
    assert "宋伟军" not in captured["goal"]


def test_parse_bus_data_reports_vm_task_submit_failure(monkeypatch):
    monkeypatch.setattr(pnc_agent_tools, "_check_pnc_permission", lambda *a, **kw: None)
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_name", lambda: "郭艳彬")
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_id", lambda: "")

    def fake_submit(title, goal, owner="", user_id=""):
        return json.dumps({"success": False, "error": "bridge delivery failed", "returncode": 2}, ensure_ascii=False)

    monkeypatch.setattr(pnc_agent_tools.vm_task_tool, "vm_task_submit_json", fake_submit)

    result = json.loads(pnc_agent_tools.parse_bus_data_tool({"input": "/tmp/data", "output": "/tmp/out"}))

    assert "error" in result
    assert result["agent"] == "parse-bus-data"
    assert "bridge delivery failed" in result["error"]
    assert result["vm_task"]["returncode"] == 2


def test_pnc_permission_requires_repo_acl_for_senior(monkeypatch):
    monkeypatch.setattr(pnc_agent_tools, "_resolve_user_name_from_id", lambda uid: "郭艳彬" if uid == "ou_guo" else "")
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_name", lambda: "郭艳彬")
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_id", lambda: "ou_guo")

    class Policy:
        @staticmethod
        def get_user_role_by_id(user_id):
            return "senior"

        @staticmethod
        def get_user_role(user):
            return "senior"

        @staticmethod
        def repo_acl_allows(user, repo, action):
            return False

    monkeypatch.setitem(__import__("sys").modules, "tools.permission_policy", Policy)

    error = pnc_agent_tools._check_pnc_permission("generate-dbc", "郭艳彬", user_id="ou_guo")

    assert error is not None
    assert "missing repo ACL" in error
    assert "pnc_specs" in error


def test_pnc_permission_allows_senior_with_repo_acl(monkeypatch):
    class Policy:
        @staticmethod
        def get_user_role_by_id(user_id):
            return "senior"

        @staticmethod
        def get_user_role(user):
            return "senior"

        @staticmethod
        def repo_acl_allows(user, repo, action):
            return user == "郭艳彬" and repo == "pnc_specs" and action == "read"

    monkeypatch.setitem(__import__("sys").modules, "tools.permission_policy", Policy)

    assert pnc_agent_tools._check_pnc_permission("generate-dbc", "郭艳彬", user_id="ou_guo") is None


def test_pnc_agents_smoke_requires_repo_acl_before_subprocess(monkeypatch):
    called = False

    def fake_run(*args, **kwargs):
        nonlocal called
        called = True
        raise AssertionError("subprocess should not be called")

    monkeypatch.setattr(pnc_agent_tools.subprocess, "run", fake_run)
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_name", lambda: "郭艳彬")
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_id", lambda: "ou_guo")
    monkeypatch.setattr(pnc_agent_tools, "_resolve_user_name_from_id", lambda uid: "郭艳彬")

    class Policy:
        @staticmethod
        def get_user_role_by_id(user_id):
            return "senior"

        @staticmethod
        def get_user_role(user):
            return "senior"

        @staticmethod
        def repo_acl_allows(user, repo, action):
            return False

    monkeypatch.setitem(__import__("sys").modules, "tools.permission_policy", Policy)

    result = json.loads(pnc_agent_tools.pnc_agents_smoke_tool({}, user_id="ou_guo"))

    assert "error" in result
    assert "missing repo ACL" in result["error"]
    assert called is False


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



def test_open_foxglove_submits_vm_task_with_standard_defaults(monkeypatch):
    captured = {}
    monkeypatch.setattr(pnc_agent_tools, "_check_pnc_permission", lambda *a, **kw: None)
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_name", lambda: "宋伟军")
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_id", lambda: "")

    def fake_submit(title, goal, owner="", user_id=""):
        captured.update(title=title, goal=goal, owner=owner, user_id=user_id)
        return json.dumps({"success": True, "task": {"task_id": "task-open-foxglove"}, "routing": {}}, ensure_ascii=False)

    monkeypatch.setattr(pnc_agent_tools.vm_task_tool, "vm_task_submit_json", fake_submit)

    result = json.loads(
        pnc_agent_tools.open_foxglove_tool(
            {
                "project": "D2L3",
                "input": "/mnt/pnc_tools/case_data/open-foxglove/input.mcap",
                "output": "/mnt/tmp/pnc-open-foxglove-output",
            },
            user_id="ou_song",
        )
    )

    assert result["ok"] is True
    assert result["mode"] == "submitted"
    assert result["agent"] == "open-foxglove"
    assert result["task_id"] == "task-open-foxglove"
    assert captured["owner"] == "宋伟军"
    assert captured["user_id"] == "ou_song"
    assert "open-foxglove" in captured["title"]
    assert "./open-foxglove --project d2l3 --platform soc --profile one-click-convert" in captured["goal"]
    assert "manifest_relpath: src/tools/open-foxglove/manifest.yaml" in captured["goal"]
    assert "work_tmp_dir=/mnt/tmp/pnc-open-foxglove" in captured["goal"]
    assert "user_visible_path=//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/pnc-open-foxglove/" in captured["goal"]
    assert "generate fresh outputs" in captured["goal"]


def test_open_foxglove_accepts_d2j_and_g3y_project_packs(monkeypatch):
    captured_goals = []
    monkeypatch.setattr(pnc_agent_tools, "_check_pnc_permission", lambda *a, **kw: None)
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_name", lambda: "宋伟军")
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_id", lambda: "")

    def fake_submit(title, goal, owner="", user_id=""):
        captured_goals.append(goal)
        return json.dumps({"success": True, "task": {"task_id": f"task-{len(captured_goals)}"}, "routing": {}}, ensure_ascii=False)

    monkeypatch.setattr(pnc_agent_tools.vm_task_tool, "vm_task_submit_json", fake_submit)

    for project in ("D2J", "g3y"):
        result = json.loads(
            pnc_agent_tools.open_foxglove_tool(
                {
                    "project": project,
                    "input": f"/mnt/pnc_tools/case_data/open-foxglove/{project.lower()}.mcap",
                    "output": f"/mnt/tmp/pnc-open-foxglove-{project.lower()}",
                },
                user_id="ou_song",
            )
        )
        assert result["ok"] is True

    assert "./open-foxglove --project d2j --platform soc --profile one-click-convert" in captured_goals[0]
    assert "./open-foxglove --project g3y --platform soc --profile one-click-convert" in captured_goals[1]


def test_open_foxglove_schema_documents_all_supported_projects():
    entry = pnc_agent_tools.registry.get_entry("open_foxglove")
    assert entry is not None
    schema = entry.schema
    description = schema["description"]
    project_description = schema["parameters"]["properties"]["project"]["description"]

    for project in ("d4q", "d2l3", "g1q3", "d2j", "g3y"):
        assert project in description.lower()
        assert project in project_description.lower()


def test_open_foxglove_requires_absolute_input_output(monkeypatch):
    monkeypatch.setattr(pnc_agent_tools, "_check_pnc_permission", lambda *a, **kw: None)
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_name", lambda: "宋伟军")
    monkeypatch.setattr(pnc_agent_tools, "_current_session_user_id", lambda: "")

    result = json.loads(
        pnc_agent_tools.open_foxglove_tool(
            {"project": "d2l3", "input": "relative.mcap", "output": "/mnt/tmp/out"},
            user_id="ou_song",
        )
    )

    assert result["agent"] == "open-foxglove"
    assert "input must be an absolute VM path" in result["error"]


def test_open_foxglove_is_exposed_in_feishu_toolset():
    from toolsets import resolve_toolset

    tools = resolve_toolset("hermes-feishu")
    assert "open_foxglove" in tools
