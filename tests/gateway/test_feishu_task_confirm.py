import json

from hermes_constants import reset_hermes_home_override, set_hermes_home_override
from gateway.feishu_task_confirm import resolve_task_confirm, resolve_task_confirm_by_text


def _write_confirm_sidecar(tmp_path, task_id="task-confirm"):
    path = tmp_path / "task-state" / f"{task_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "task_card": {
            "schema_version": 1,
            "task_id": task_id,
            "chat_id": "oc_test",
            "thread_id": "topic:om_thread",
            "user_state": "awaiting_user",
            "last_sent_hash": "old-hash",
            "last_render_hash": "old-hash",
            "pending_confirms": [
                {"id": "c1", "question": "是否继续？", "preset": "continue_stop", "resolved": None},
            ],
            "delivery": {},
        }
    }), encoding="utf-8")
    return path


def test_button_confirm_resolves_once_and_duplicate_is_idempotent(tmp_path):
    token = set_hermes_home_override(tmp_path)
    try:
        path = _write_confirm_sidecar(tmp_path)
        first = resolve_task_confirm(
            task_id="task-confirm",
            confirm_id="c1",
            choice="继续",
            actor_id="ou_user",
            actor_name="User",
            source="button",
            event_id="evt-1",
        )
        duplicate = resolve_task_confirm(
            task_id="task-confirm",
            confirm_id="c1",
            choice="中止",
            actor_id="ou_user",
            actor_name="User",
            source="button",
            event_id="evt-1-redelivered",
        )
        body = json.loads(path.read_text(encoding="utf-8"))
    finally:
        reset_hermes_home_override(token)

    assert first["changed"] is True
    assert duplicate["duplicate"] is True
    resolved = body["task_card"]["pending_confirms"][0]["resolved"]
    assert resolved["choice"] == "继续"
    assert resolved["source"] == "button"
    assert "last_sent_hash" not in body["task_card"]
    assert "last_render_hash" not in body["task_card"]


def test_text_fallback_uses_same_resolution_shape(tmp_path):
    token = set_hermes_home_override(tmp_path)
    try:
        path = _write_confirm_sidecar(tmp_path)
        result = resolve_task_confirm_by_text(
            chat_id="oc_test",
            thread_id="topic:om_thread",
            text="继续",
            actor_id="ou_user",
            actor_name="User",
            event_id="om_text",
        )
        body = json.loads(path.read_text(encoding="utf-8"))
    finally:
        reset_hermes_home_override(token)

    assert result["changed"] is True
    assert result["choice"] == "继续"
    resolved = body["task_card"]["pending_confirms"][0]["resolved"]
    assert resolved["choice"] == "继续"
    assert resolved["source"] == "text"
    assert resolved["event_id"] == "om_text"
