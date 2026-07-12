"""Tests for Dashboard task observability APIs."""

import json
import time
from datetime import datetime, timezone

from fastapi.testclient import TestClient

from gateway.tasks.store import TaskStore
from gateway.tasks.types import Task, TaskStatus, TaskType
from hermes_constants import reset_hermes_home_override, set_hermes_home_override
from hermes_cli.web_server import _SESSION_TOKEN, app


HEADERS = {"X-Hermes-Session-Token": _SESSION_TOKEN}


def _client_for_home(tmp_path):
    token = set_hermes_home_override(tmp_path)
    return TestClient(app), token


def _store(tmp_path):
    return TaskStore(tmp_path / "analytics" / "tasks.db")


def _insert_task(
    store,
    *,
    task_id="task-1",
    status=TaskStatus.RUNNING,
    platform="feishu",
    started_at=1000.0,
):
    store.upsert(Task(
        task_id=task_id,
        status=status,
        task_type=TaskType.CODING,
        user_id="user-1",
        platform=platform,
        request_summary=f"summary for {task_id}",
        started_at=started_at,
        chat_id="chat-1",
        thread_id="thread-1",
        message_id="msg-1",
        agent_route="coding",
    ))


def test_api_tasks_requires_session_token(tmp_path):
    client, token = _client_for_home(tmp_path)
    try:
        response = client.get("/api/tasks")
    finally:
        reset_hermes_home_override(token)

    assert response.status_code == 401


def test_api_tasks_lists_and_filters_task_views(tmp_path):
    client, token = _client_for_home(tmp_path)
    try:
        store = _store(tmp_path)
        now = time.time()
        _insert_task(store, task_id="feishu-running", status=TaskStatus.RUNNING, platform="feishu", started_at=now)
        _insert_task(store, task_id="cli-running", status=TaskStatus.RUNNING, platform="cli", started_at=now - 1)
        _insert_task(store, task_id="feishu-completed", status=TaskStatus.COMPLETED, platform="feishu", started_at=now - 2)

        response = client.get(
            "/api/tasks?status=running&platform=feishu&limit=20&offset=0",
            headers=HEADERS,
        )
    finally:
        reset_hermes_home_override(token)

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["total"] == 1
    assert body["status_counts"] == {
        "pending": 0,
        "running": 1,
        "completed": 1,
        "failed": 0,
        "cancelled": 0,
    }
    assert body["limit"] == 20
    assert body["offset"] == 0
    assert [task["task_id"] for task in body["tasks"]] == ["feishu-running"]
    task = body["tasks"][0]
    assert task["platform"] == "feishu"
    assert task["status"] == "running"
    assert task["stage_label"] == "运行中"
    assert task["chat_id"] == "chat-1"
    assert task["thread_id"] == "thread-1"


def test_api_tasks_omits_shared_state_only_active_tasks(tmp_path):
    client, token = _client_for_home(tmp_path)
    try:
        store = _store(tmp_path)
        now = time.time()
        _insert_task(store, task_id="store-running", status=TaskStatus.RUNNING, platform="feishu", started_at=now)
        task_id = "shared-list-api-task"
        task_dir = tmp_path / "runtime" / "shared-state" / "tasks" / task_id
        task_dir.mkdir(parents=True)
        (task_dir / "meta.json").write_text(json.dumps({
            "task_id": task_id,
            "title": "shared list api task",
            "state": "pending",
            "created_at": datetime.fromtimestamp(now, timezone.utc).isoformat(),
            "updated_at": datetime.fromtimestamp(now, timezone.utc).isoformat(),
            "latest_summary": "shared list api summary",
        }), encoding="utf-8")

        response = client.get("/api/tasks?limit=20&offset=0", headers=HEADERS)
    finally:
        reset_hermes_home_override(token)

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["total"] == 1
    assert body["status_counts"]["running"] == 1
    assert body["status_counts"]["pending"] == 0
    task_ids = [task["task_id"] for task in body["tasks"]]
    assert task_ids == ["store-running"]


def test_api_task_detail_merges_task_state_sidecar(tmp_path):
    client, token = _client_for_home(tmp_path)
    try:
        store = _store(tmp_path)
        now = time.time()
        _insert_task(store, task_id="task.with/slash", status=TaskStatus.RUNNING, platform="feishu", started_at=now)
        sidecar = tmp_path / "task-state" / "task.with_slash.json"
        sidecar.parent.mkdir(parents=True)
        sidecar.write_text(json.dumps({
            "current_phase": "tool_execution",
            "updated_at": datetime.fromtimestamp(now, timezone.utc).isoformat(),
            "recent_events": [{"tool": "read_file", "status": "ok"}],
            "artifacts": [{"path": "/tmp/result.md"}],
            "verification": [{"command": "pytest", "status": "passed"}],
            "blockers": [],
            "vm_bridge": {"summary": "VM visible", "state": "running"},
        }), encoding="utf-8")

        response = client.get("/api/tasks/task.with%2Fslash", headers=HEADERS)
    finally:
        reset_hermes_home_override(token)

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["task_id"] == "task.with/slash"
    assert body["current_phase"] == "tool_execution"
    assert body["stage_label"] == "工具执行中"
    assert body["recent_events"] == [{"tool": "read_file", "status": "ok"}]
    assert body["artifacts"] == [{"path": "/tmp/result.md"}]
    assert body["verification"] == [{"command": "pytest", "status": "passed"}]
    assert body["vm_bridge"] == {"summary": "VM visible", "state": "running"}


def test_api_tasks_list_keeps_detail_payload_out_of_summary(tmp_path):
    client, token = _client_for_home(tmp_path)
    try:
        store = _store(tmp_path)
        now = time.time()
        _insert_task(store, task_id="task-vm", status=TaskStatus.RUNNING, platform="feishu", started_at=now)
        sidecar = tmp_path / "task-state" / "task-vm.json"
        sidecar.parent.mkdir(parents=True)
        sidecar.write_text(json.dumps({
            "current_phase": "vm_running",
            "recent_events": [{"summary": "event"}],
            "artifacts": ["artifact"],
            "verification": ["verified"],
            "blockers": ["blocked"],
            "vm_bridge": {"summary": "VM visible", "state": "running"},
            "completion_notice": {"send_status": "sent", "sent_at": "2026-06-10T00:00:00Z"},
        }), encoding="utf-8")

        response = client.get("/api/tasks?status=running", headers=HEADERS)
    finally:
        reset_hermes_home_override(token)

    assert response.status_code == 200, response.text
    task = response.json()["tasks"][0]
    assert task["task_id"] == "task-vm"
    assert task["stage_label"] == "VM 执行中"
    assert task["completion_notice_status"] == "sent"
    assert task["completion_notice_sent_at"] == "2026-06-10T00:00:00Z"
    assert "vm_bridge" not in task
    assert "recent_events" not in task
    assert "artifacts" not in task
    assert "verification" not in task
    assert "blockers" not in task


def test_api_task_detail_rejects_shared_state_sidecar_only_task(tmp_path):
    client, token = _client_for_home(tmp_path)
    try:
        task_id = "shared-only-task"
        shared = tmp_path / "runtime" / "shared-state"
        task_dir = shared / "tasks" / task_id
        task_dir.mkdir(parents=True)
        now = time.time()
        created_iso = datetime.fromtimestamp(now, timezone.utc).isoformat()
        (task_dir / "meta.json").write_text(json.dumps({
            "task_id": task_id,
            "title": "shared state task",
            "state": "pending",
            "owner": "胡子豪",
            "created_at": created_iso,
            "updated_at": created_iso,
            "latest_summary": "shared state task summary",
            "stale": False,
        }), encoding="utf-8")
        (task_dir / "status.md").write_text(
            "---\n"
            "task_id: shared-only-task\n"
            "title: shared state task\n"
            "state: pending\n"
            "owner: 胡子豪\n"
            f"created_at: {created_iso}\n"
            f"updated_at: {created_iso}\n"
            "---\n"
            "shared state task summary\n",
            encoding="utf-8",
        )
        sidecar = tmp_path / "task-state" / f"{task_id}.json"
        sidecar.parent.mkdir(parents=True)
        sidecar.write_text(json.dumps({
            "current_phase": "vm_running",
            "vm_bridge": {"summary": "VM bridge sees shared-only task", "state": "running"},
            "recent_events": [{"summary": "bridge event"}],
        }), encoding="utf-8")

        response = client.get(f"/api/tasks/{task_id}", headers=HEADERS)
    finally:
        reset_hermes_home_override(token)

    assert response.status_code == 404, response.text


def test_api_task_detail_404_for_missing_task(tmp_path):
    client, token = _client_for_home(tmp_path)
    try:
        response = client.get("/api/tasks/missing", headers=HEADERS)
    finally:
        reset_hermes_home_override(token)

    assert response.status_code == 404


def test_api_tasks_rejects_unknown_status(tmp_path):
    client, token = _client_for_home(tmp_path)
    try:
        response = client.get("/api/tasks?status=bogus", headers=HEADERS)
    finally:
        reset_hermes_home_override(token)

    assert response.status_code == 400


def test_api_tasks_clamps_limit_and_offset(tmp_path):
    client, token = _client_for_home(tmp_path)
    try:
        store = _store(tmp_path)
        now = time.time()
        for i in range(3):
            _insert_task(
                store,
                task_id=f"task-{i}",
                status=TaskStatus.RUNNING,
                platform="feishu",
                started_at=now - i,
            )

        response = client.get("/api/tasks?limit=1000&offset=-5", headers=HEADERS)
    finally:
        reset_hermes_home_override(token)

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["limit"] == 100
    assert body["offset"] == 0
    assert body["total"] == 3
    assert len(body["tasks"]) == 3


def test_api_tasks_rejects_non_integer_pagination(tmp_path):
    client, token = _client_for_home(tmp_path)
    try:
        response = client.get("/api/tasks?limit=abc", headers=HEADERS)
    finally:
        reset_hermes_home_override(token)

    assert response.status_code == 422


def test_api_tasks_rejects_dashboard_intake_creation(tmp_path):
    client, token = _client_for_home(tmp_path)
    try:
        response = client.post(
            "/api/tasks",
            headers=HEADERS,
            json={
                "requester": "胡子豪",
                "request_summary": "请先创建 pending review intake",
                "acceptance_criteria": ["只登记不执行", "detail 可见协作字段"],
                "next_action": "等待人工 review",
                "owner": "triage-bot",
                "needs_user_input": False,
                "last_operator_note": "intake captured",
            },
        )
    finally:
        reset_hermes_home_override(token)

    assert response.status_code == 403, response.text
    assert "Feishu intake" in response.text


def test_api_tasks_create_intake_is_disabled_before_validation(tmp_path):
    client, token = _client_for_home(tmp_path)
    try:
        response = client.post(
            "/api/tasks",
            headers=HEADERS,
            json={
                "requester": "胡子豪",
                "request_summary": "bad intake",
                "acceptance_criteria": [],
            },
        )
    finally:
        reset_hermes_home_override(token)

    assert response.status_code == 403
    assert "Feishu intake" in response.text
