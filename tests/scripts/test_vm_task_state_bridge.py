"""Tests for VM bridge sidecar writer script."""

import json
from pathlib import Path

from hermes_constants import reset_hermes_home_override, set_hermes_home_override
from scripts import vm_task_state_bridge


def test_write_sidecar_sanitizes_task_id_and_preserves_vm_bridge(tmp_path):
    token = set_hermes_home_override(tmp_path)
    try:
        path = vm_task_state_bridge.write_task_state(
            "task.with/slash",
            phase="vm_running",
            event="VM worker picked up the task",
            artifact="CIFS: //hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/task.with/slash/report.html",
            verification="Dashboard sidecar write verified",
            vm_summary="VM worker is running",
            vm_state="running",
            work_tmp_dir="/mnt/tmp/task.with/slash",
            user_visible_path="//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/task.with/slash/",
        )
        body = json.loads(path.read_text(encoding="utf-8"))
    finally:
        reset_hermes_home_override(token)

    assert path == tmp_path / "task-state" / "task.with_slash.json"
    assert body["current_phase"] == "vm_running"
    assert body["recent_events"][-1]["summary"] == "VM worker picked up the task"
    assert body["artifacts"] == [
        "CIFS: //hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/task.with/slash/report.html"
    ]
    assert body["verification"] == ["Dashboard sidecar write verified"]
    assert body["blockers"] == []
    assert body["vm_bridge"]["summary"] == "VM worker is running"
    assert body["vm_bridge"]["state"] == "running"
    assert body["vm_bridge"]["work_tmp_dir"] == "/mnt/tmp/task.with/slash"
    assert body["vm_bridge"]["user_visible_path"].startswith("//hfs1.minieye.tech/")


def test_write_sidecar_appends_to_existing_lists_and_caps_events(tmp_path):
    token = set_hermes_home_override(tmp_path)
    try:
        sidecar = tmp_path / "task-state" / "task-1.json"
        sidecar.parent.mkdir(parents=True)
        sidecar.write_text(json.dumps({
            "recent_events": [{"summary": f"old-{i}"} for i in range(55)],
            "artifacts": ["old-artifact"],
            "verification": [],
            "blockers": ["old-blocker"],
        }), encoding="utf-8")

        path = vm_task_state_bridge.write_task_state(
            "task-1",
            phase="blocked",
            event="new-event",
            blocker="new-blocker",
        )
        body = json.loads(path.read_text(encoding="utf-8"))
    finally:
        reset_hermes_home_override(token)

    assert len(body["recent_events"]) == 50
    assert body["recent_events"][-1]["summary"] == "new-event"
    assert body["recent_events"][-1]["time"] == body["recent_events"][-1]["ts"]
    assert body["artifacts"] == ["old-artifact"]
    assert body["blockers"] == ["old-blocker", "new-blocker"]
    assert body["current_phase"] == "blocked"


def test_write_sidecar_invalid_progress_json_and_dedupes_lists(tmp_path):
    token = set_hermes_home_override(tmp_path)
    try:
        first = vm_task_state_bridge.write_task_state(
            "task-2",
            artifact="artifact-a",
            verification="verified-a",
            blocker="blocker-a",
            progress_json="not-json",
        )
        second = vm_task_state_bridge.write_task_state(
            "task-2",
            artifact="artifact-a",
            verification="verified-a",
            blocker="blocker-a",
        )
        body = json.loads(second.read_text(encoding="utf-8"))
    finally:
        reset_hermes_home_override(token)

    assert first == second
    assert body["vm_bridge"]["progress"] == {"raw": "not-json"}
    assert body["artifacts"] == ["artifact-a"]
    assert body["verification"] == ["verified-a"]
    assert body["blockers"] == ["blocker-a"]


def test_write_sidecar_replaces_corrupt_existing_file(tmp_path):
    token = set_hermes_home_override(tmp_path)
    try:
        sidecar = tmp_path / "task-state" / "task-3.json"
        sidecar.parent.mkdir(parents=True)
        sidecar.write_text("{not-json", encoding="utf-8")
        path = vm_task_state_bridge.write_task_state("task-3", phase="vm_running", event="fresh")
        body = json.loads(path.read_text(encoding="utf-8"))
    finally:
        reset_hermes_home_override(token)

    assert body["current_phase"] == "vm_running"
    assert body["recent_events"][-1]["summary"] == "fresh"


def test_main_emits_single_json_object_with_path(tmp_path, capsys):
    token = set_hermes_home_override(tmp_path)
    try:
        rc = vm_task_state_bridge.main([
            "--task-id", "task-main",
            "--phase", "vm_running",
            "--event", "main event",
            "--artifact", "artifact-a",
        ])
        captured = capsys.readouterr()
        payload = json.loads(captured.out)
    finally:
        reset_hermes_home_override(token)

    assert rc == 0
    assert captured.err == ""
    assert payload["ok"] is True
    path = Path(payload["path"])
    assert path.exists()
    body = json.loads(path.read_text(encoding="utf-8"))
    assert body["current_phase"] == "vm_running"
    assert body["artifacts"] == ["artifact-a"]
