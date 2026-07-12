"""Tests for read-only VM task status collector contract."""

import json
import subprocess
from pathlib import Path

import pytest

from scripts import vm_task_status_collect


def test_collect_rejects_invalid_task_id_without_remote_calls(monkeypatch):
    calls = []

    def fake_run(*args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("remote command should not run")

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(ValueError, match="invalid task_id"):
        vm_task_status_collect.collect_vm_task_status("../bad")

    assert calls == []


@pytest.mark.parametrize(
    "root",
    ["relative/shared-state", "/home/mini/other", "/mnt/tmp", "/mnt/tmp/../escape"],
)
def test_isolated_shared_state_root_rejects_unsafe_paths(root):
    with pytest.raises(ValueError, match="shared_state_root"):
        vm_task_status_collect._normalize_shared_state_root(root)


def test_collect_isolated_shared_state_root_never_scans_global_tmp(monkeypatch):
    calls = []
    shared_root = "/mnt/tmp/hermes-v0182-smoke-test/shared-state"
    artifact_root = "/mnt/tmp/hermes-v0182-smoke-test/artifacts/"

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        assert cmd[1] == "read_file"
        assert cmd[2].startswith(shared_root + "/tasks/task-isolated/")
        if cmd[2].endswith("/status.json"):
            payload = {"state": "running", "summary": "中文 smoke 正常"}
        elif cmd[2].endswith("/meta.json"):
            payload = {
                "artifact_root": artifact_root,
                "artifact_cifs_root": (
                    "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/"
                    "tmp/hermes-v0182-smoke-test/artifacts/"
                ),
            }
        else:
            return subprocess.CompletedProcess(cmd, 1, stdout="", stderr="not ready")
        return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps(payload), stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)
    payload = vm_task_status_collect.collect_vm_task_status(
        "task-isolated",
        ssh_mini_agent="agent",
        include_artifacts=False,
        shared_state_root=shared_root,
    )

    assert payload["paths"]["shared_state_root"] == shared_root
    assert payload["paths"]["status_json"] == f"{shared_root}/tasks/task-isolated/status.json"
    assert payload["state"]["summary"] == "中文 smoke 正常"
    assert payload["vm_bridge"]["sources"]["global_artifact_discovery"] is False
    assert [cmd[1] for cmd in calls] == ["read_file", "read_file", "read_file"]


def test_isolated_shared_state_root_cannot_reenable_global_discovery(monkeypatch):
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("remote call not expected")),
    )
    with pytest.raises(ValueError, match="cannot enable global artifact discovery"):
        vm_task_status_collect.collect_vm_task_status(
            "task-isolated",
            shared_state_root="/mnt/tmp/hermes-v0182-smoke-test/shared-state",
            allow_global_artifact_discovery=True,
        )


@pytest.mark.parametrize(
    ("artifact_root", "artifact_cifs_root", "error"),
    [
        (
            "/mnt/tmp/another-run/artifacts/",
            "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/another-run/artifacts/",
            "isolated artifact_root",
        ),
        (
            "/mnt/tmp/hermes-v0182-smoke-test/artifacts/",
            "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/another-run/artifacts/",
            "isolated artifact_cifs_root",
        ),
        (
            "/mnt/tmp/hermes-v0182-smoke-test/../production-task/",
            "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/hermes-v0182-smoke-test/artifacts/",
            "artifact_root",
        ),
    ],
)
def test_isolated_shared_state_rejects_artifact_namespace_escape(
    monkeypatch,
    artifact_root,
    artifact_cifs_root,
    error,
):
    shared_root = "/mnt/tmp/hermes-v0182-smoke-test/shared-state"

    def fake_run(cmd, **kwargs):
        if cmd[2].endswith("/status.json"):
            payload = {"state": "running"}
        elif cmd[2].endswith("/meta.json"):
            payload = {
                "artifact_root": artifact_root,
                "artifact_cifs_root": artifact_cifs_root,
            }
        else:
            return subprocess.CompletedProcess(cmd, 1, stdout="", stderr="not ready")
        return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps(payload), stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)
    with pytest.raises(ValueError, match=error):
        vm_task_status_collect.collect_vm_task_status(
            "task-isolated",
            ssh_mini_agent="agent",
            include_artifacts=False,
            shared_state_root=shared_root,
        )


def test_collect_uses_only_ssh_mini_agent_read_verbs(monkeypatch):
    calls = []

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        if cmd[:2] == ["/Users/songying/.local/bin/ssh-mini-agent", "read_file"]:
            return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps({
                "state": "running",
                "summary": "VM worker is running",
                "updated_at": "2026-05-25T12:00:00Z",
            }), stderr="")
        if cmd[:2] == ["/Users/songying/.local/bin/ssh-mini-agent", "list_files"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="report.html\n", stderr="")
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.setattr(subprocess, "run", fake_run)

    payload = vm_task_status_collect.collect_vm_task_status(
        "task-1",
        ssh_mini_agent="/Users/songying/.local/bin/ssh-mini-agent",
    )

    assert payload["success"] is True
    assert payload["read_only"] is True
    assert payload["task_id"] == "task-1"
    assert payload["state"]["value"] == "running"
    assert payload["vm_bridge"]["summary"] == "VM worker is running"
    assert payload["vm_bridge"]["work_tmp_dir"] == "/mnt/tmp/task-1/"
    assert payload["vm_bridge"]["user_visible_path"] == "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/task-1/"
    assert payload["artifacts"] == [
        "VM: /mnt/tmp/task-1/report.html",
        "CIFS: //hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/task-1/report.html",
    ]
    assert calls
    for cmd in calls:
        assert cmd[0] == "/Users/songying/.local/bin/ssh-mini-agent"
        assert cmd[1] in {"read_file", "list_files", "head", "tail"}
        assert "ssh-mini-run" not in cmd[0]
        assert "run_bash_json" not in cmd
        assert "run_py_json" not in cmd
        assert "edit_file" not in cmd


def test_collect_handles_malformed_status_and_list_files_failure(monkeypatch):
    def fake_run(cmd, **kwargs):
        if cmd[1] == "read_file":
            return subprocess.CompletedProcess(cmd, 0, stdout="not-json", stderr="")
        if cmd[1] == "list_files":
            return subprocess.CompletedProcess(cmd, 2, stdout="", stderr="permission denied")
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.setattr(subprocess, "run", fake_run)

    payload = vm_task_status_collect.collect_vm_task_status("task-1", ssh_mini_agent="agent")

    assert payload["success"] is True
    assert payload["state"]["value"] == "unknown"
    assert payload["vm_bridge"]["summary"] == "No VM status summary yet."
    assert payload["artifacts"] == []
    assert any("json parse failed" in err for err in payload["errors"])
    assert "permission denied" in payload["errors"]


def test_collect_falls_back_to_status_md_and_meta_artifact_roots(monkeypatch):
    def fake_run(cmd, **kwargs):
        if cmd[1] == "read_file" and cmd[2].endswith("/status.json"):
            return subprocess.CompletedProcess(cmd, 1, stdout="", stderr="not found")
        if cmd[1] == "read_file" and cmd[2].endswith("/status.md"):
            return subprocess.CompletedProcess(
                cmd,
                0,
                stdout=(
                    "# Status\n\n"
                    "- state: in_progress\n"
                    "- summary: VM worker running\n"
                    "- updated_at: 2026-06-10T14:34:34\n"
                ),
                stderr="",
            )
        if cmd[1] == "read_file" and cmd[2].endswith("/meta.json"):
            return subprocess.CompletedProcess(
                cmd,
                0,
                stdout=json.dumps({
                    "artifact_root": "/mnt/tmp/g1q3_rca_issue_intake_6986500860/",
                    "artifact_cifs_root": "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/g1q3_rca_issue_intake_6986500860/",
                }),
                stderr="",
            )
        if cmd[1] == "read_file" and cmd[2].endswith("/result.md"):
            return subprocess.CompletedProcess(cmd, 1, stdout="", stderr="not ready")
        if cmd[1] == "list_files":
            assert cmd[2] == "/mnt/tmp/g1q3_rca_issue_intake_6986500860/"
            return subprocess.CompletedProcess(cmd, 0, stdout="intake_summary.md\n", stderr="")
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.setattr(subprocess, "run", fake_run)

    payload = vm_task_status_collect.collect_vm_task_status("task-1", ssh_mini_agent="agent")

    assert payload["state"]["value"] == "in_progress"
    assert payload["state"]["summary"] == "VM worker running"
    assert payload["errors"] == []
    assert payload["paths"]["artifact_root_vm"] == "/mnt/tmp/g1q3_rca_issue_intake_6986500860/"
    assert payload["vm_bridge"]["user_visible_path"].endswith("/g1q3_rca_issue_intake_6986500860/")
    assert payload["artifacts"] == [
        "VM: /mnt/tmp/g1q3_rca_issue_intake_6986500860/intake_summary.md",
        "CIFS: //hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/g1q3_rca_issue_intake_6986500860/intake_summary.md",
    ]


def test_collect_accepts_absolute_and_bare_list_files_and_skips_unsafe(monkeypatch):
    def fake_run(cmd, **kwargs):
        if cmd[1] == "read_file":
            return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps({"status": "completed", "message": "done"}), stderr="")
        if cmd[1] == "list_files":
            return subprocess.CompletedProcess(
                cmd,
                0,
                stdout="report.html\n/mnt/tmp/task-1/log.txt\n../secret.txt\n/mnt/tmp/other-task/oops.txt\nsubdir/\n",
                stderr="",
            )
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.setattr(subprocess, "run", fake_run)

    payload = vm_task_status_collect.collect_vm_task_status("task-1", ssh_mini_agent="agent")

    assert payload["state"]["value"] == "completed"
    assert payload["state"]["terminal"] is True
    assert payload["artifacts"] == [
        "VM: /mnt/tmp/task-1/report.html",
        "CIFS: //hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/task-1/report.html",
        "VM: /mnt/tmp/task-1/log.txt",
        "CIFS: //hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/task-1/log.txt",
    ]
    assert any("skipped unsafe artifact path" in err for err in payload["errors"])
    assert any("skipped outside artifact root" in err for err in payload["errors"])


def test_collect_no_artifacts_flag_skips_list_files(monkeypatch):
    calls = []

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        if cmd[1] == "read_file":
            return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps({"state": "running"}), stderr="")
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.setattr(subprocess, "run", fake_run)

    payload = vm_task_status_collect.collect_vm_task_status("task-1", ssh_mini_agent="agent", include_artifacts=False)

    assert payload["artifacts"] == []
    assert payload["vm_bridge"]["sources"]["artifact_scan"] is False
    assert [cmd[1] for cmd in calls] == ["read_file", "read_file", "read_file"]
    assert all(cmd[1] != "list_files" for cmd in calls)


def test_write_collected_status_to_sidecar_includes_artifacts_and_vm_bridge(tmp_path, monkeypatch):
    from hermes_constants import reset_hermes_home_override, set_hermes_home_override

    token = set_hermes_home_override(tmp_path)
    try:
        payload = {
            "state": {"value": "running", "summary": "VM worker is running"},
            "artifacts": [
                "VM: /mnt/tmp/task-1/report.html",
                "CIFS: //hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/task-1/report.html",
            ],
            "vm_bridge": {
                "summary": "VM worker is running",
                "state": "running",
                "work_tmp_dir": "/mnt/tmp/task-1/",
                "user_visible_path": "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/task-1/",
            },
        }

        path = vm_task_status_collect.write_collected_status_to_sidecar("task-1", payload)
        body = json.loads(path.read_text(encoding="utf-8"))
    finally:
        reset_hermes_home_override(token)

    assert body["current_phase"] == "vm_running"
    assert body["artifacts"] == payload["artifacts"]
    assert body["vm_bridge"]["summary"] == "VM worker is running"
    assert body["vm_bridge"]["user_visible_path"].startswith("//hfs1.minieye.tech/")


def test_write_collected_status_to_sidecar_surfaces_collector_errors(tmp_path):
    from hermes_constants import reset_hermes_home_override, set_hermes_home_override

    token = set_hermes_home_override(tmp_path)
    try:
        payload = {
            "state": {"value": "unknown", "summary": "No VM status summary yet."},
            "artifacts": [],
            "vm_bridge": {
                "summary": "No VM status summary yet.",
                "state": "unknown",
                "work_tmp_dir": "/mnt/tmp/task-err/",
                "user_visible_path": "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/task-err/",
            },
            "errors": ["json parse failed", "permission denied"],
        }

        path = vm_task_status_collect.write_collected_status_to_sidecar("task-err", payload)
        body = json.loads(path.read_text(encoding="utf-8"))
    finally:
        reset_hermes_home_override(token)

    assert body["blockers"] == ["collector: json parse failed", "collector: permission denied"]


def test_write_collected_status_to_sidecar_preserves_cancelled_phase(tmp_path):
    from hermes_constants import reset_hermes_home_override, set_hermes_home_override

    token = set_hermes_home_override(tmp_path)
    try:
        payload = {
            "state": {"value": "cancelled", "summary": "cancelled by operator"},
            "artifacts": [],
            "vm_bridge": {
                "summary": "cancelled by operator",
                "state": "cancelled",
                "work_tmp_dir": "/mnt/tmp/task-cancel/",
                "user_visible_path": "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/task-cancel/",
            },
            "errors": [],
        }

        path = vm_task_status_collect.write_collected_status_to_sidecar("task-cancel", payload)
        body = json.loads(path.read_text(encoding="utf-8"))
    finally:
        reset_hermes_home_override(token)

    assert body["current_phase"] == "cancelled"
    assert body["vm_bridge"]["state"] == "cancelled"


def test_main_prints_valid_json_with_sidecar_path(tmp_path, capsys, monkeypatch):
    from hermes_constants import reset_hermes_home_override, set_hermes_home_override

    token = set_hermes_home_override(tmp_path)
    try:
        def fake_collect(task_id, **kwargs):
            return {
                "task_id": task_id,
                "state": {"value": "running", "summary": "running"},
                "artifacts": ["artifact-a"],
                "vm_bridge": {
                    "summary": "running",
                    "state": "running",
                    "work_tmp_dir": "/mnt/tmp/task-main/",
                    "user_visible_path": "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/task-main/",
                },
                "errors": [],
            }

        monkeypatch.setattr(vm_task_status_collect, "collect_vm_task_status", fake_collect)
        rc = vm_task_status_collect.main(["--task-id", "task-main", "--write-sidecar"])
        captured = capsys.readouterr()
        payload = json.loads(captured.out)
    finally:
        reset_hermes_home_override(token)

    assert rc == 0
    assert captured.err == ""
    assert payload["sidecar_path"]
    path = Path(payload["sidecar_path"])
    assert path.exists()
    body = json.loads(path.read_text(encoding="utf-8"))
    assert body["artifacts"] == ["artifact-a"]


def test_b63_meta_artifact_root_hash_dir_is_trusted(monkeypatch):
    calls = []

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        if cmd[1] == "read_file" and cmd[2].endswith("/status.json"):
            return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps({"state": "completed", "summary": "done"}), stderr="")
        if cmd[1] == "read_file" and cmd[2].endswith("/meta.json"):
            return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps({"artifact_root": "/mnt/tmp/g1q3_rca_issue_intake_7017699515_460c71/"}), stderr="")
        if cmd[1] == "read_file" and cmd[2].endswith("/result.md"):
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
        if cmd[1] == "list_files" and cmd[2] == "/mnt/tmp/":
            return subprocess.CompletedProcess(cmd, 0, stdout="g1q3_rca_issue_intake_7017699515_460c71/\n", stderr="")
        if cmd[1] == "read_file" and cmd[2] == "/mnt/tmp/g1q3_rca_issue_intake_7017699515_460c71/pipeline_result.json":
            return subprocess.CompletedProcess(cmd, 1, stdout="", stderr="not found")
        if cmd[1] == "list_files":
            assert cmd[2] == "/mnt/tmp/g1q3_rca_issue_intake_7017699515_460c71/"
            return subprocess.CompletedProcess(cmd, 0, stdout="report_data.json\n", stderr="")
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.setattr(subprocess, "run", fake_run)

    payload = vm_task_status_collect.collect_vm_task_status("20260616-120456-g1q3-rca-issue-intake-7017699515", ssh_mini_agent="agent")

    assert payload["paths"]["artifact_root_vm"] == "/mnt/tmp/g1q3_rca_issue_intake_7017699515_460c71/"
    assert payload["errors"] == []
    assert payload["artifacts"][0] == "VM: /mnt/tmp/g1q3_rca_issue_intake_7017699515_460c71/report_data.json"


def test_b63_meta_missing_discovers_latest_g1q3_hash_dir(monkeypatch):
    def fake_run(cmd, **kwargs):
        if cmd[1] == "read_file" and cmd[2].endswith("/status.json"):
            return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps({"state": "completed"}), stderr="")
        if cmd[1] == "read_file" and cmd[2].endswith("/meta.json"):
            return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps({"work_item": {"work_item_id": "7017699515"}}), stderr="")
        if cmd[1] == "read_file" and cmd[2].endswith("/result.md"):
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
        if cmd[1] == "list_files" and cmd[2] == "/mnt/tmp/":
            return subprocess.CompletedProcess(cmd, 0, stdout="g1q3_rca_issue_intake_7017699515_111111/\ng1q3_rca_issue_intake_7017699515_460c71/\nother/\n", stderr="")
        if cmd[1] == "read_file" and cmd[2].endswith("/pipeline_result.json"):
            return subprocess.CompletedProcess(cmd, 1, stdout="", stderr="not found")
        if cmd[1] == "list_files" and cmd[2] == "/mnt/tmp/g1q3_rca_issue_intake_7017699515_460c71/":
            return subprocess.CompletedProcess(cmd, 0, stdout="index.html\n", stderr="")
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.setattr(subprocess, "run", fake_run)

    payload = vm_task_status_collect.collect_vm_task_status("20260616-120456-g1q3-rca-issue-intake-7017699515", ssh_mini_agent="agent")

    assert payload["paths"]["artifact_root_vm"] == "/mnt/tmp/g1q3_rca_issue_intake_7017699515_460c71/"
    assert payload["vm_bridge"]["sources"]["artifact_root_discovered"] is True
    assert payload["errors"] == []


def test_b63_artifact_root_from_paths_recovers_verify_runtime_glued_candidate():
    root = vm_task_status_collect._artifact_root_from_paths(
        ["/mnt/tmp/g1q3_rca_issue_intake_7017699515_460c71_verify_report_runtime.json"],
        "task-1",
    )

    assert root == "/mnt/tmp/g1q3_rca_issue_intake_7017699515_460c71/"


def test_b63_non_directory_artifact_root_warning_not_blocker(monkeypatch):
    def fake_run(cmd, **kwargs):
        if cmd[1] == "read_file":
            return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps({"state": "completed"}), stderr="")
        if cmd[1] == "list_files":
            return subprocess.CompletedProcess(cmd, 2, stdout="", stderr="error: directory not found: /mnt/tmp/task-1/")
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.setattr(subprocess, "run", fake_run)

    payload = vm_task_status_collect.collect_vm_task_status("task-1", ssh_mini_agent="agent")

    assert payload["artifacts"] == []
    assert payload["errors"] == []


def test_collect_reads_delivery_contract_from_artifact_root(monkeypatch):
    def fake_run(cmd, **kwargs):
        if cmd[1] == "read_file" and cmd[2].endswith("/status.json"):
            return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps({"status": "completed", "message": "done"}), stderr="")
        if cmd[1] == "read_file" and cmd[2].endswith("/meta.json"):
            return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps({
                "artifact_root": "/mnt/tmp/task-1/",
                "artifact_cifs_root": "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/task-1/",
            }), stderr="")
        if cmd[1] == "read_file" and cmd[2].endswith("/result.md"):
            return subprocess.CompletedProcess(cmd, 1, stdout="", stderr="not ready")
        if cmd[1] == "read_file" and cmd[2].endswith("/delivery_contract.json"):
            return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps({
                "schema_version": "g1q3_delivery_contract_v1",
                "business_state": "report_completed",
                "report": {"is_deliverable": True},
            }), stderr="")
        if cmd[1] == "list_files":
            return subprocess.CompletedProcess(cmd, 0, stdout="delivery_contract.json\n", stderr="")
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.setattr(subprocess, "run", fake_run)

    payload = vm_task_status_collect.collect_vm_task_status("task-1", ssh_mini_agent="agent")

    assert payload["delivery_contract"]["schema_version"] == "g1q3_delivery_contract_v1"
    assert payload["delivery_contract"]["business_state"] == "report_completed"
    assert payload["vm_bridge"]["sources"]["delivery_contract"] is True


def test_collect_stitches_latest_report_ready_root_for_same_work_item(monkeypatch):
    """Regression: report generated under a newer trigger slug must update the original card."""

    def fake_run(cmd, **kwargs):
        if cmd[1] == "read_file" and cmd[2].endswith("/status.json"):
            return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps({"state": "completed", "summary": "old task done"}), stderr="")
        if cmd[1] == "read_file" and cmd[2].endswith("/meta.json"):
            return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps({
                "artifact_root": "/mnt/tmp/g1q3_rca_issue_intake_7029768863_4a42ba/",
            }), stderr="")
        if cmd[1] == "read_file" and cmd[2].endswith("/result.md"):
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
        if cmd[1] == "list_files" and cmd[2] == "/mnt/tmp/":
            return subprocess.CompletedProcess(cmd, 0, stdout="\n".join([
                "g1q3_rca_issue_intake_7029768863_4a42ba/",
                "g1q3_rca_issue_intake_7029768863_8a6bed/",
                "g1q3_rca_issue_intake_9999999999_bad/",
            ]), stderr="")
        if cmd[1] == "read_file" and cmd[2] == "/mnt/tmp/g1q3_rca_issue_intake_7029768863_4a42ba/pipeline_result.json":
            return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps({"status": "blocked", "stage": "s1_gate"}), stderr="")
        if cmd[1] == "read_file" and cmd[2] == "/mnt/tmp/g1q3_rca_issue_intake_7029768863_8a6bed/pipeline_result.json":
            return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps({
                "status": "report_generated_need_review",
                "rca_terminal_state": "report_ready",
            }), stderr="")
        if cmd[1] == "read_file" and cmd[2] == "/mnt/tmp/g1q3_rca_issue_intake_7029768863_8a6bed/rca_execution_request.json":
            return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps({
                "work_item": {"work_item_id": "7029768863"},
            }), stderr="")
        if cmd[1] == "read_file" and cmd[2] == "/mnt/tmp/g1q3_rca_issue_intake_7029768863_8a6bed/delivery_contract.json":
            return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps({
                "schema_version": "g1q3_delivery_contract_v1",
                "business_state": "report_completed",
                "report": {"is_deliverable": True},
            }), stderr="")
        if cmd[1] == "list_files" and cmd[2] == "/mnt/tmp/g1q3_rca_issue_intake_7029768863_4a42ba/":
            return subprocess.CompletedProcess(cmd, 0, stdout="pipeline_result.json\n", stderr="")
        if cmd[1] == "list_files" and cmd[2] == "/mnt/tmp/g1q3_rca_issue_intake_7029768863_8a6bed/":
            return subprocess.CompletedProcess(cmd, 0, stdout="pipeline_result.json\ndelivery_contract.json\n", stderr="")
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.setattr(subprocess, "run", fake_run)

    payload = vm_task_status_collect.collect_vm_task_status(
        "20260627-142804-g1q3-rca-issue-intake-7029768863-68863_4a42ba",
        ssh_mini_agent="agent",
    )

    assert payload["paths"]["artifact_root_vm"] == "/mnt/tmp/g1q3_rca_issue_intake_7029768863_8a6bed/"
    assert payload["paths"]["artifact_root_cifs"].endswith("/g1q3_rca_issue_intake_7029768863_8a6bed/")
    assert payload["vm_bridge"]["sources"]["artifact_root_stitched_by_work_item"] is True
    assert payload["delivery_contract"]["business_state"] == "report_completed"
