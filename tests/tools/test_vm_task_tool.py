"""Tests for shared-state VM task submission tool registration and errors."""

import json
import sqlite3
import subprocess

import pytest

from tools import vm_task_tool
from tools.registry import registry


def _disable_trusted_session(monkeypatch):
    monkeypatch.setattr(vm_task_tool, "_resolve_submitter", lambda user_id="", owner="": ("", ""))


def test_vm_task_submit_schema_is_raw_function_schema():
    schema = registry.get_schema("vm_task_submit")

    assert schema["name"] == "vm_task_submit"
    assert schema["parameters"]["type"] == "object"
    assert "function" not in schema
    properties = schema["parameters"]["properties"]
    assert properties["lane"]["enum"] == ["fast", "standard", "heavy"]
    assert properties["resource_class"]["enum"] == ["cpu", "io", "repo", "pnc_data", "network", "mixed"]
    assert properties["executor_type"]["enum"] == ["coding_agent", "direct_cli", "governed_tool"]
    assert properties["agent_backend"]["enum"] == ["codex", "openclaw", "none"]
    assert "VM scheduler" in properties["lane"]["description"]

    definition = registry.get_definitions({"vm_task_submit"})[0]
    assert definition["type"] == "function"
    assert definition["function"]["name"] == "vm_task_submit"
    assert "function" not in definition["function"]


def test_rca_prod_resource_class_is_reserved_for_scoped_service(monkeypatch):
    _disable_trusted_session(monkeypatch)
    result = vm_task_tool.vm_task_submit(
        title="ordinary task",
        goal="ordinary repository check",
        resource_class="rca_prod",
    )
    assert result["success"] is False
    assert "capability-scoped vm_task_submit_service API" in result["error"]

    trusted = vm_task_tool._vm_task_submit_trusted(
        title="ordinary task",
        goal="ordinary repository check",
        task_id="ordinary-task",
        resource_class="rca_prod",
    )
    assert trusted["success"] is False
    assert "capability-scoped vm_task_submit_service API" in trusted["error"]


def test_rca_shared_state_source_refs_use_exact_create_once_contract():
    source_refs = {
        "project_key": "project-key",
        "project_simple_name": "G1Q3",
        "work_item_type_key": "issue",
        "work_item_id": "7051566847",
        "rule_version": "rule-v1",
        "topic": "feishu-project-workflow-event",
        "partition": 0,
        "offset": 676,
    }

    projected = vm_task_tool._rca_shared_state_source_refs(source_refs)

    assert set(projected) == {
        "project_key",
        "work_item_type_key",
        "work_item_id",
        "rule_version",
        "topic",
        "partition",
        "offset",
    }
    assert projected["work_item_id"] == "7051566847"


def test_rca_fixed_goal_matches_shared_state_create_once_markers():
    goal = vm_task_tool.build_rca_fixed_cli_goal(
        task_id="g1q3-rca-s1-" + "a" * 64,
        admission={"contract": "admission"},
        execution_request={"contract": "request"},
    )

    assert goal.startswith(vm_task_tool._RCA_SHARED_STATE_GOAL_PREFIX)
    assert "## RcaAdmission JSON\n" in goal
    assert "## RcaExecutionRequest JSON\n" in goal
    assert vm_task_tool._RCA_ADMISSION_JSON_BEGIN in goal
    assert vm_task_tool._RCA_EXECUTION_REQUEST_JSON_BEGIN in goal


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


def test_vm_task_status_prefers_state_db_over_stale_meta_when_dispatch_missing(monkeypatch, tmp_path):
    root = tmp_path / "shared-state"
    task_id = "task-123"
    task_dir = root / "tasks" / task_id
    task_dir.mkdir(parents=True)
    (task_dir / "meta.json").write_text(
        json.dumps({"task_id": task_id, "state": "pending", "owner": "郭艳彬"}),
        encoding="utf-8",
    )
    (task_dir / "status.md").write_text("- state: completed\n", encoding="utf-8")
    db = root / "state.db"
    conn = sqlite3.connect(db)
    conn.execute(
        "CREATE TABLE tasks (task_id TEXT PRIMARY KEY, state TEXT, owner TEXT, updated_at TEXT, "
        "run_id TEXT, agent_host TEXT, latest_summary TEXT)"
    )
    conn.execute(
        "INSERT INTO tasks VALUES (?, ?, ?, ?, ?, ?, ?)",
        (task_id, "completed", "郭艳彬", "2026-05-20T15:49:54", "run-1", "mini", "success"),
    )
    conn.commit()
    conn.close()
    monkeypatch.setattr(vm_task_tool, "_DEFAULT_VM_CANONICAL_ROOT", root)

    result = vm_task_tool.vm_task_status(task_id)

    assert result["state"] == "completed"
    assert result["summary"] == "success"
    assert result["updated_at"] == "2026-05-20T15:49:54"
    assert result["run_id"] == "run-1"
    assert result["agent_host"] == "mini"


def test_vm_task_status_rejects_invalid_task_id():
    result = vm_task_tool.vm_task_status("../bad")

    assert result["success"] is False
    assert "invalid task_id" in result["error"]


def test_vm_task_status_falls_back_to_vm_canonical_root(monkeypatch, tmp_path):
    host_root = tmp_path / "host-shared-state"
    vm_root = tmp_path / "vm-shared-state"
    task_id = "task-vm-only"
    task_dir = vm_root / "tasks" / task_id
    task_dir.mkdir(parents=True)
    (task_dir / "meta.json").write_text(json.dumps({"state": "claimed", "owner": "mini"}), encoding="utf-8")
    monkeypatch.setattr(vm_task_tool, "_DEFAULT_HOST_CANONICAL_ROOT", host_root)
    monkeypatch.setattr(vm_task_tool, "_DEFAULT_VM_CANONICAL_ROOT", vm_root)

    result = vm_task_tool.vm_task_status(task_id)

    assert result["success"] is True
    assert result["state"] == "claimed"
    assert result["paths"]["root"] == str(vm_root)
    assert result["paths"]["checked_roots"] == [str(host_root), str(vm_root)]


def test_vm_task_status_reports_missing_task(monkeypatch, tmp_path):
    host_root = tmp_path / "host-shared-state"
    vm_root = tmp_path / "vm-shared-state"
    monkeypatch.setattr(vm_task_tool, "_DEFAULT_HOST_CANONICAL_ROOT", host_root)
    monkeypatch.setattr(vm_task_tool, "_DEFAULT_VM_CANONICAL_ROOT", vm_root)

    result = vm_task_tool.vm_task_status("missing-task")

    assert result["success"] is False
    assert result["state"] == "missing"
    assert result["paths"]["checked_roots"] == [str(host_root), str(vm_root)]


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


def test_general_vm_submit_keeps_workspace_work_creator_and_never_uses_rca_bundle(
    monkeypatch,
    tmp_path,
):
    _disable_trusted_session(monkeypatch)
    workspace_work_creator = tmp_path / "workspace-work" / "bin" / "create_task_v2.py"
    workspace_work_creator.parent.mkdir(parents=True)
    workspace_work_creator.write_text("print('unused')\n", encoding="utf-8")
    monkeypatch.setattr(
        vm_task_tool,
        "_create_task_script",
        lambda: workspace_work_creator,
    )
    monkeypatch.setattr(
        vm_task_tool,
        "validate_workspace_runtime",
        lambda: pytest.fail("general VM submit must not inspect the RCA bundle"),
    )
    captured = {}

    def fake_run(cmd, **kwargs):
        captured["cmd"] = cmd
        captured["env"] = kwargs["env"]
        return subprocess.CompletedProcess(
            cmd,
            0,
            stdout=json.dumps({"task_id": "t-general"}),
            stderr="",
        )

    monkeypatch.setattr(vm_task_tool.subprocess, "run", fake_run)

    result = vm_task_tool.vm_task_submit("ordinary title", "ordinary goal")

    assert result["success"] is True
    assert str(workspace_work_creator) in captured["cmd"]
    assert "rca-workspace-runtime" not in " ".join(captured["cmd"])
    assert captured["env"]["PYTHONDONTWRITEBYTECODE"] == "1"


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
    monkeypatch.setattr(vm_task_tool, "_resolve_submitter", lambda user_id="", owner="": ("郭艳彬", "ou_guo"))
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


def test_vm_task_submit_accepts_explicit_task_id(monkeypatch, tmp_path):
    _disable_trusted_session(monkeypatch)
    script = tmp_path / "create_task_v2.py"
    script.write_text("print('unused')", encoding="utf-8")
    monkeypatch.setattr(vm_task_tool, "_create_task_script", lambda: script)
    monkeypatch.setattr(vm_task_tool, "_spawn_completion_probe_background", lambda task_id: {"started": True, "task_id": task_id})
    captured = {}

    def fake_run(cmd, **kwargs):
        captured["cmd"] = cmd
        return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps({"task_id": "20260623-120000-explicit"}), stderr="")

    monkeypatch.setattr(vm_task_tool.subprocess, "run", fake_run)

    result = vm_task_tool.vm_task_submit("title", "goal", task_id="20260623-120000-explicit")

    assert result["success"] is True
    assert "--task-id" in captured["cmd"]
    assert captured["cmd"][captured["cmd"].index("--task-id") + 1] == "20260623-120000-explicit"
    assert result["notify_process"]["task_id"] == "20260623-120000-explicit"


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
    monkeypatch.setattr(vm_task_tool, "_resolve_submitter", lambda user_id="", owner="": ("王平", "ou_wang"))
    monkeypatch.setattr(
        vm_task_tool,
        "_check_vm_task_permission",
        lambda *a, **kw: vm_task_tool._vm_task_permission_denied_payload("王平", "member"),
    )
    called = False

    def fake_run(*args, **kwargs):
        nonlocal called
        called = True
        raise AssertionError("subprocess should not run")

    monkeypatch.setattr(vm_task_tool.subprocess, "run", fake_run)

    result = vm_task_tool.vm_task_submit("title", "goal", owner="spoofed", user_id="ou_wang")

    assert result["success"] is False
    assert result["error_code"] == "vm_task_permission_denied"
    assert "当前账号没有 VM 编译/执行任务权限" in result["error"]
    assert "管理员" in result["error"]
    assert "owner" in result["error"]
    assert result["retryable"] is False
    assert called is False
    leaked = json.dumps(result, ensure_ascii=False)
    assert "vm_task_submit" not in leaked
    assert "ssh-mini" not in leaked
    assert "role 'member'" not in leaked
    assert "member" not in result["error"]


def test_vm_task_submit_includes_scheduler_metadata_in_meta(monkeypatch, tmp_path):
    _disable_trusted_session(monkeypatch)
    script = tmp_path / "create_task_v2.py"
    script.write_text("print('unused')", encoding="utf-8")
    monkeypatch.setattr(vm_task_tool, "_create_task_script", lambda: script)
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
        artifact_root="/mnt/tmp/task-1/",
        artifact_cifs_root="//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/task-1/",
    )

    assert result["success"] is True
    meta = json.loads(captured["cmd"][captured["cmd"].index("--meta") + 1])
    assert meta["lane"] == "heavy"
    assert meta["resource_class"] == "pnc_data"
    assert meta["repo_scope"] == "pnc_specs"
    assert meta["workspace_scope"] == "owner_main_repo"
    assert meta["risk_class"] == "normal"
    assert meta["artifact_root"] == "/mnt/tmp/task-1/"
    assert meta["artifact_cifs_root"] == "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/task-1/"
    assert meta["executor_type"] == "coding_agent"
    assert meta["agent_backend"] == "codex"
    assert meta["codex_backend_enabled"] is True


def test_vm_task_submit_accepts_direct_cli_execution_plane_without_codex_gate(monkeypatch, tmp_path):
    _disable_trusted_session(monkeypatch)
    script = tmp_path / "create_task_v2.py"
    script.write_text("print('unused')", encoding="utf-8")
    monkeypatch.setattr(vm_task_tool, "_create_task_script", lambda: script)
    captured = {}

    def fake_run(cmd, **kwargs):
        captured["cmd"] = cmd
        return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps({"task_id": "t1"}), stderr="")

    monkeypatch.setattr(vm_task_tool.subprocess, "run", fake_run)

    result = vm_task_tool.vm_task_submit(
        "title", "goal", executor_type="direct_cli", agent_backend="none"
    )

    assert result["success"] is True
    meta = json.loads(captured["cmd"][captured["cmd"].index("--meta") + 1])
    assert meta["executor_type"] == "direct_cli"
    assert meta["agent_backend"] == "none"
    assert "codex_backend_enabled" not in meta


@pytest.mark.parametrize(
    "overrides",
    [
        {"title": "G1Q3 RCA issue intake: 7041712812"},
        {"task_id": "g1q3-rca-s1-" + "a" * 64},
        {"task_id": "g1q3-rca-issue-intake-7041712812"},
        {"task_id": "g1q3_rca_issue_intake_7041712812"},
        {"goal": "- template_id: rca_issue_intake"},
        {"goal": "<!-- G1Q3_RCA_ADMISSION_JSON:BEGIN -->"},
        {
            "goal": (
                "./api/g1q3_rca/scripts/run_rca_service_request.py "
                "--task-id forged"
            )
        },
        {"goal": "python3 api/g1q3_rca/scripts/run_rca_auto_pipeline.py"},
        {
            "goal": (
                "请分析 https://project.feishu.cn/t03o4q/issue/detail/7041712812"
            )
        },
        {
            "title": (
                "https://project.feishu.cn/t03o4q/issue/detail/7041712812?from=card"
            )
        },
        {"artifact_root": "/mnt/tmp/g1q3-rca-s1-" + "b" * 64 + "/"},
        {"artifact_root": "/mnt/tmp/g1q3_rca_issue_intake_7041712812/"},
        {
            "artifact_cifs_root": (
                "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/"
                "tmp/g1q3-rca-s1-" + "c" * 64 + "/"
            )
        },
    ],
)
def test_public_vm_submit_rejects_reserved_rca_issue_intake_boundary(
    monkeypatch, overrides
):
    _disable_trusted_session(monkeypatch)
    monkeypatch.setattr(
        vm_task_tool.subprocess,
        "run",
        lambda *_args, **_kwargs: pytest.fail(
            "reserved RCA public submit must not create a task"
        ),
    )
    args = {"title": "ordinary title", "goal": "ordinary goal", **overrides}

    result = vm_task_tool.vm_task_submit(**args)

    assert result == {
        "success": False,
        "error_code": "g1q3_rca_service_boundary_required",
        "error": (
            "G1Q3 RCA issue intake is reserved for the capability-scoped "
            "vm_task_submit_service API"
        ),
        "retryable": False,
        "returncode": None,
    }


def test_vm_task_submit_rejects_invalid_execution_plane_metadata(monkeypatch, tmp_path):
    _disable_trusted_session(monkeypatch)
    script = tmp_path / "create_task_v2.py"
    script.write_text("print('unused')", encoding="utf-8")
    monkeypatch.setattr(vm_task_tool, "_create_task_script", lambda: script)
    called = False

    def fake_run(*args, **kwargs):
        nonlocal called
        called = True
        raise AssertionError("subprocess should not run for invalid execution plane metadata")

    monkeypatch.setattr(vm_task_tool.subprocess, "run", fake_run)

    invalid_cases = [
        ({"executor_type": "shell"}, "invalid executor_type"),
        ({"agent_backend": "claude"}, "invalid agent_backend"),
        ({"executor_type": "coding_agent", "agent_backend": "none"}, "none is only valid"),
    ]
    for kwargs, error_text in invalid_cases:
        result = vm_task_tool.vm_task_submit("title", "goal", **kwargs)  # type: ignore[arg-type]
        assert result["success"] is False
        assert error_text in result["error"]
    assert called is False


def test_vm_task_submit_rejects_invalid_scheduler_metadata_before_subprocess(monkeypatch, tmp_path):
    _disable_trusted_session(monkeypatch)
    script = tmp_path / "create_task_v2.py"
    script.write_text("print('unused')", encoding="utf-8")
    monkeypatch.setattr(vm_task_tool, "_create_task_script", lambda: script)
    called = False

    def fake_run(*args, **kwargs):
        nonlocal called
        called = True
        raise AssertionError("subprocess should not run for invalid scheduler metadata")

    monkeypatch.setattr(vm_task_tool.subprocess, "run", fake_run)

    invalid_cases = [
        ({"lane": "urgent"}, "invalid lane"),
        ({"repo_scope": "../pnc_specs"}, "invalid repo_scope"),
        ({"repo_scope": "pnc_specs/nested"}, "invalid repo_scope"),
        ({"artifact_root": "/home/mini/.cache/task-1/"}, "invalid artifact_root"),
        ({"artifact_root": "/mnt/tmp/../escape/"}, "invalid artifact_root"),
        ({"artifact_root": "/mnt/tmp/task-1/../escape/"}, "invalid artifact_root"),
        ({"artifact_root": "/mnt/tmp/task-1/./escape/"}, "invalid artifact_root"),
        ({"artifact_cifs_root": "//hfs.minieye.tech/department-pnc_team-planning_algo-driving/tmp/task-1/"}, "invalid artifact_cifs_root"),
        ({"artifact_cifs_root": "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/task-1/../escape/"}, "invalid artifact_cifs_root"),
    ]
    for kwargs, error_text in invalid_cases:
        result = vm_task_tool.vm_task_submit("title", "goal", **kwargs)
        assert result["success"] is False
        assert error_text in result["error"]
    assert called is False


def test_vm_task_submit_registry_handler_forwards_scheduler_metadata(monkeypatch, tmp_path):
    _disable_trusted_session(monkeypatch)
    script = tmp_path / "create_task_v2.py"
    script.write_text("print('unused')", encoding="utf-8")
    monkeypatch.setattr(vm_task_tool, "_create_task_script", lambda: script)
    captured = {}

    def fake_run(cmd, **kwargs):
        captured["cmd"] = cmd
        return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps({"task_id": "t1"}), stderr="")

    monkeypatch.setattr(vm_task_tool.subprocess, "run", fake_run)

    entry = registry.get_entry("vm_task_submit")
    assert entry is not None
    payload = json.loads(entry.handler({
        "title": "title",
        "goal": "goal",
        "lane": "heavy",
        "resource_class": "pnc_data",
        "repo_scope": "pnc_specs",
        "workspace_scope": "owner_main_repo",
        "risk_class": "normal",
        "artifact_root": "/mnt/tmp/task-1/",
        "artifact_cifs_root": "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/task-1/",
    }))

    assert payload["success"] is True
    meta = json.loads(captured["cmd"][captured["cmd"].index("--meta") + 1])
    assert meta["lane"] == "heavy"
    assert meta["resource_class"] == "pnc_data"
    assert meta["repo_scope"] == "pnc_specs"
    assert meta["workspace_scope"] == "owner_main_repo"
    assert meta["risk_class"] == "normal"
    assert meta["artifact_root"] == "/mnt/tmp/task-1/"
    assert meta["artifact_cifs_root"] == "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/task-1/"
