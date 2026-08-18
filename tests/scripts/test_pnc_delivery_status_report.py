import json

from gateway.tasks.store import TaskStore
from gateway.tasks.types import Task, TaskStatus, TaskType
from hermes_constants import reset_hermes_home_override, set_hermes_home_override
from scripts import pnc_delivery_status_report
from scripts.pnc_vm_task_sync import G1Q3_RCA_CHAT_ID


def test_status_report_summarizes_groups_notices_and_launchd(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    try:
        store = TaskStore(tmp_path / "analytics" / "tasks.db")
        store.upsert(Task(
            task_id="task-1",
            status=TaskStatus.RUNNING,
            task_type=TaskType.CHAT,
            user_id="user-1",
            platform="feishu",
            request_summary="summary",
            started_at=1000.0,
            chat_id=G1Q3_RCA_CHAT_ID,
            vm_task_id="vm-1",
        ))
        sidecar = tmp_path / "task-state" / "task-1.json"
        sidecar.parent.mkdir(parents=True)
        sidecar.write_text(json.dumps({
            "completion_notice": {
                "send_status": "pending",
                "chat_id": G1Q3_RCA_CHAT_ID,
                "message_id": "om_1",
                "text": "done",
            }
        }), encoding="utf-8")
        monkeypatch.setattr(pnc_delivery_status_report, "_http_probe", lambda url: {"ok": True, "status": 200, "url": url})
        monkeypatch.setattr(pnc_delivery_status_report, "run_guard", lambda: {"ok": True, "errors": [], "warnings": []})
        ok_launchd = {"ok": True, "errors": [], "loaded": True}
        monkeypatch.setattr(pnc_delivery_status_report, "validate_pnc_vm_task_sync_launchd", lambda: ok_launchd)
        monkeypatch.setattr(pnc_delivery_status_report, "validate_pnc_completion_notice_relay_launchd", lambda: ok_launchd)
        monkeypatch.setattr(pnc_delivery_status_report, "validate_pnc_feishu_delivery_repair_launchd", lambda: ok_launchd)

        report = pnc_delivery_status_report.build_report()
    finally:
        reset_hermes_home_override(token)

    assert report["ok"] is True
    assert report["governance_ok"] is True
    g1q3 = [row for row in report["business_groups"] if row["chat_id"] == G1Q3_RCA_CHAT_ID][0]
    assert g1q3["counts"]["running"] == 1
    assert report["completion_notices"]["counts"]["pending"] == 1
    assert report["completion_notices"]["pending_or_retryable_count"] == 1


def test_status_report_surfaces_public_probe_failure(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    try:
        monkeypatch.setattr(pnc_delivery_status_report, "_http_probe", lambda url: {"ok": False, "error": "refused", "url": url})
        monkeypatch.setattr(pnc_delivery_status_report, "run_guard", lambda: {"ok": True, "errors": [], "warnings": []})
        ok_launchd = {"ok": True, "errors": [], "loaded": True}
        monkeypatch.setattr(pnc_delivery_status_report, "validate_pnc_vm_task_sync_launchd", lambda: ok_launchd)
        monkeypatch.setattr(pnc_delivery_status_report, "validate_pnc_completion_notice_relay_launchd", lambda: ok_launchd)
        monkeypatch.setattr(pnc_delivery_status_report, "validate_pnc_feishu_delivery_repair_launchd", lambda: ok_launchd)

        report = pnc_delivery_status_report.build_report()
    finally:
        reset_hermes_home_override(token)

    assert report["ok"] is False
    assert any("public task page unreachable" in item for item in report["errors"])


def test_status_report_counts_effective_vm_terminal_status(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    try:
        store = TaskStore(tmp_path / "analytics" / "tasks.db")
        store.upsert(Task(
            task_id="vm-finished",
            status=TaskStatus.RUNNING,
            task_type=TaskType.CHAT,
            user_id="user-1",
            platform="feishu",
            request_summary="summary",
            started_at=1000.0,
            chat_id=G1Q3_RCA_CHAT_ID,
            vm_task_id="vm-1",
        ))
        sidecar = tmp_path / "task-state" / "vm-finished.json"
        sidecar.parent.mkdir(parents=True)
        sidecar.write_text(json.dumps({
            "current_phase": "completed",
            "vm_bridge": {"state": "completed", "summary": "done"},
        }), encoding="utf-8")
        monkeypatch.setattr(pnc_delivery_status_report, "_http_probe", lambda url: {"ok": True, "status": 200, "url": url})
        monkeypatch.setattr(pnc_delivery_status_report, "run_guard", lambda: {"ok": True, "errors": [], "warnings": []})
        ok_launchd = {"ok": True, "errors": [], "loaded": True}
        monkeypatch.setattr(pnc_delivery_status_report, "validate_pnc_vm_task_sync_launchd", lambda: ok_launchd)
        monkeypatch.setattr(pnc_delivery_status_report, "validate_pnc_completion_notice_relay_launchd", lambda: ok_launchd)
        monkeypatch.setattr(pnc_delivery_status_report, "validate_pnc_feishu_delivery_repair_launchd", lambda: ok_launchd)

        report = pnc_delivery_status_report.build_report()
    finally:
        reset_hermes_home_override(token)

    g1q3 = [row for row in report["business_groups"] if row["chat_id"] == G1Q3_RCA_CHAT_ID][0]
    assert g1q3["counts"]["completed"] == 1
    assert g1q3["counts"]["running"] == 0


def test_status_report_warns_on_taskstore_effective_status_mismatch(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    try:
        store = TaskStore(tmp_path / "analytics" / "tasks.db")
        store.upsert(Task(
            task_id="vm-finished",
            status=TaskStatus.RUNNING,
            task_type=TaskType.CHAT,
            user_id="user-1",
            platform="feishu",
            request_summary="summary",
            started_at=1000.0,
            chat_id=G1Q3_RCA_CHAT_ID,
            vm_task_id="vm-1",
        ))
        sidecar = tmp_path / "task-state" / "vm-finished.json"
        sidecar.parent.mkdir(parents=True)
        sidecar.write_text(json.dumps({
            "current_phase": "completed",
            "vm_bridge": {"state": "completed", "summary": "done"},
        }), encoding="utf-8")
        monkeypatch.setattr(pnc_delivery_status_report, "_http_probe", lambda url: {"ok": True, "status": 200, "url": url})
        monkeypatch.setattr(pnc_delivery_status_report, "run_guard", lambda: {"ok": True, "errors": [], "warnings": []})
        ok_launchd = {"ok": True, "errors": [], "loaded": True}
        monkeypatch.setattr(pnc_delivery_status_report, "validate_pnc_vm_task_sync_launchd", lambda: ok_launchd)
        monkeypatch.setattr(pnc_delivery_status_report, "validate_pnc_completion_notice_relay_launchd", lambda: ok_launchd)
        monkeypatch.setattr(pnc_delivery_status_report, "validate_pnc_feishu_delivery_repair_launchd", lambda: ok_launchd)

        report = pnc_delivery_status_report.build_report()
    finally:
        reset_hermes_home_override(token)

    g1q3 = [row for row in report["business_groups"] if row["chat_id"] == G1Q3_RCA_CHAT_ID][0]
    assert report["ok"] is True
    assert report["governance_ok"] is False
    assert g1q3["taskstore_effective_status_mismatch_count"] == 1
    assert g1q3["taskstore_effective_status_mismatches"][0]["raw_status"] == "running"
    assert g1q3["taskstore_effective_status_mismatches"][0]["effective_status"] == "completed"
    assert any("TaskStore/effective status mismatch" in item for item in report["warnings"])


def test_format_markdown_report_is_feishu_friendly():
    report = {
        "ok": True,
        "governance_ok": True,
        "user_visible_url": "http://192.168.14.32:9125/tasks",
        "local_public_probe": {"status": 200, "ok": True},
        "business_groups": [
            {"label": "PNC", "counts": {"running": 1, "completed": 2, "failed": 0}, "taskstore_effective_status_mismatch_count": 0},
            {"label": "G1Q3 RCA", "counts": {"running": 0, "completed": 1, "failed": 1}, "taskstore_effective_status_mismatch_count": 0},
        ],
        "completion_notices": {"counts": {"pending": 0, "failed": 0, "sent": 1}},
        "warnings": [],
        "errors": [],
    }

    text = pnc_delivery_status_report.format_markdown_report(report)

    assert text.startswith("✅ PNC 任务可观测状态：OK")
    assert "用户侧入口：http://192.168.14.32:9125/tasks" in text
    assert "- G1Q3 RCA：running=0，completed=1，failed=1" in text
    assert "当前无治理告警。" in text


def test_format_markdown_report_includes_warnings_and_reconcile_command():
    report = {
        "ok": True,
        "governance_ok": False,
        "user_visible_url": "http://192.168.14.32:9125/tasks",
        "local_public_probe": {"status": 200, "ok": True},
        "business_groups": [
            {"label": "G1Q3 RCA", "counts": {"running": 1, "completed": 1, "failed": 0}, "taskstore_effective_status_mismatch_count": 1},
        ],
        "completion_notices": {"counts": {"pending": 1, "failed": 0, "sent": 1}},
        "reconcile_command": "python3 scripts/pnc_taskstore_status_reconcile.py --apply",
        "warnings": ["G1Q3 RCA TaskStore/effective status mismatch count=1"],
        "errors": [],
    }

    text = pnc_delivery_status_report.format_markdown_report(report)

    assert text.startswith("⚠️ PNC 任务可观测状态：需关注")
    assert "状态收敛命令：`python3 scripts/pnc_taskstore_status_reconcile.py --apply`" in text
    assert "Warnings：" in text
