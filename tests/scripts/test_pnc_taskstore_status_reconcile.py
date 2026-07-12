import json

from gateway.tasks.store import TaskStore
from gateway.tasks.types import Task, TaskStatus, TaskType
from hermes_constants import reset_hermes_home_override, set_hermes_home_override
from scripts import pnc_taskstore_status_reconcile
from scripts.pnc_vm_task_sync import G1Q3_RCA_CHAT_ID


def _task(task_id="task-1", *, status=TaskStatus.RUNNING, chat_id=G1Q3_RCA_CHAT_ID, platform="feishu"):
    return Task(
        task_id=task_id,
        status=status,
        task_type=TaskType.CHAT,
        user_id="user-1",
        platform=platform,
        request_summary="summary",
        started_at=1000.0,
        chat_id=chat_id,
        vm_task_id="vm-1",
    )


def _sidecar(tmp_path, task_id="task-1", phase="completed"):
    path = tmp_path / "task-state" / f"{task_id}.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({
        "current_phase": phase,
        "updated_at": "2026-06-10T00:00:00+00:00",
        "vm_bridge": {"state": phase, "summary": "done"},
    }), encoding="utf-8")
    return path


def test_reconcile_dry_run_reports_nonterminal_to_terminal_candidate(tmp_path):
    token = set_hermes_home_override(tmp_path)
    try:
        store = TaskStore(tmp_path / "analytics" / "tasks.db")
        store.upsert(_task())
        _sidecar(tmp_path)
        result = pnc_taskstore_status_reconcile.reconcile_statuses(apply=False)
        unchanged = store.get("task-1")
    finally:
        reset_hermes_home_override(token)

    assert result["ok"] is True
    assert result["dry_run"] is True
    assert result["candidate_count"] == 1
    assert result["rows"][0]["raw_status"] == "running"
    assert result["rows"][0]["effective_status"] == "completed"
    assert unchanged is not None
    assert unchanged.status == TaskStatus.RUNNING


def test_reconcile_apply_updates_taskstore_status(tmp_path):
    token = set_hermes_home_override(tmp_path)
    try:
        store = TaskStore(tmp_path / "analytics" / "tasks.db")
        store.upsert(_task())
        _sidecar(tmp_path)
        result = pnc_taskstore_status_reconcile.reconcile_statuses(apply=True)
        updated = store.get("task-1")
    finally:
        reset_hermes_home_override(token)

    assert result["applied_count"] == 1
    assert updated is not None
    assert updated.status == TaskStatus.COMPLETED
    assert updated.completed_at is not None


def test_reconcile_never_overwrites_existing_terminal_status(tmp_path):
    token = set_hermes_home_override(tmp_path)
    try:
        store = TaskStore(tmp_path / "analytics" / "tasks.db")
        store.upsert(_task(status=TaskStatus.FAILED))
        _sidecar(tmp_path)
        result = pnc_taskstore_status_reconcile.reconcile_statuses(apply=True)
        updated = store.get("task-1")
    finally:
        reset_hermes_home_override(token)

    assert result["candidate_count"] == 0
    assert updated is not None
    assert updated.status == TaskStatus.FAILED


def test_reconcile_ignores_non_pnc_chat_by_default(tmp_path):
    token = set_hermes_home_override(tmp_path)
    try:
        store = TaskStore(tmp_path / "analytics" / "tasks.db")
        store.upsert(_task(chat_id="oc_other"))
        _sidecar(tmp_path)
        result = pnc_taskstore_status_reconcile.reconcile_statuses(apply=True)
        unchanged = store.get("task-1")
    finally:
        reset_hermes_home_override(token)

    assert result["candidate_count"] == 0
    assert unchanged is not None
    assert unchanged.status == TaskStatus.RUNNING
