import json
import time
from datetime import datetime, timezone

from fastapi.testclient import TestClient

from gateway.tasks.store import TaskStore
from gateway.tasks.types import Task, TaskStatus, TaskType
from hermes_constants import reset_hermes_home_override, set_hermes_home_override
from hermes_cli.web_server import _SESSION_TOKEN, app
from scripts import pnc_completion_notice_relay, pnc_vm_task_sync

HEADERS = {"X-Hermes-Session-Token": _SESSION_TOKEN}


def test_pnc_delivery_feishu_vm_completion_smoke(tmp_path):
    token = set_hermes_home_override(tmp_path)
    try:
        task_id = "task-smoke"
        chat_id = pnc_vm_task_sync.G1Q3_RCA_CHAT_ID
        store = TaskStore(tmp_path / "analytics" / "tasks.db")
        store.upsert(Task(
            task_id=task_id,
            status=TaskStatus.COMPLETED,
            task_type=TaskType.CHAT,
            user_id="user-1",
            platform="feishu",
            request_summary="G1Q3 smoke task",
            started_at=time.time() - 120,
            completed_at=time.time(),
            agent_route="g1q3-rca",
            chat_id=chat_id,
            chat_type="group",
            thread_id="topic:om_smoke",
            message_id="om_smoke",
            vm_task_id="vm-smoke",
        ))
        sidecar = tmp_path / "task-state" / f"{task_id}.json"
        sidecar.parent.mkdir(parents=True)
        sidecar.write_text(json.dumps({
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "current_phase": "completed",
            "artifacts": ["//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/vm-smoke/report.md"],
            "verification": [],
            "blockers": [],
            "vm_bridge": {"summary": "done", "state": "completed", "vm_task_id": "vm-smoke"},
            "completion_notice": {
                "send_status": "pending",
                "chat_id": chat_id,
                "thread_id": "topic:om_smoke",
                "message_id": "om_smoke",
                "vm_task_id": "vm-smoke",
                "text": "任务完成",
            },
        }), encoding="utf-8")

        client = TestClient(app)
        list_response = client.get(f"/api/tasks?public=1&platform=feishu&chat_id={chat_id}", headers=HEADERS)
        detail_response = client.get(f"/api/tasks/{task_id}?public=1&chat_id={chat_id}", headers=HEADERS)
        relay_preview = pnc_completion_notice_relay.relay_pending_notices(task_ids=[task_id], send=False)
    finally:
        reset_hermes_home_override(token)

    assert list_response.status_code == 200, list_response.text
    listed = list_response.json()["tasks"][0]
    assert listed["task_id"] == task_id
    assert listed["completion_notice_status"] == "pending"
    assert listed["artifact_count"] == 1

    assert detail_response.status_code == 200, detail_response.text
    detail = detail_response.json()
    assert detail["public_context"]["completion_notice"]["send_status"] == "pending"
    assert detail["vm_bridge"]["state"] == "completed"
    assert detail["artifacts"]

    assert relay_preview["ok"] is True
    assert relay_preview["candidate_count"] == 1
    assert relay_preview["rows"][0]["target"] == f"feishu:{chat_id}:om_smoke"
