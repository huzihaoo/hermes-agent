import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from gateway.config import PlatformConfig, Platform
from gateway.platforms.base import MessageEvent, MessageType, SessionSource
from gateway.platforms.feishu import FeishuAdapter
from hermes_constants import reset_hermes_home_override, set_hermes_home_override


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
            "pending_confirms": [
                {"id": "c1", "question": "是否继续？", "preset": "continue_stop", "resolved": None},
            ],
            "delivery": {},
        }
    }), encoding="utf-8")
    return path


class _Response:
    def __init__(self):
        self.card = None


class _Card:
    def __init__(self):
        self.type = None
        self.data = None


def test_feishu_task_confirm_button_action_is_idempotent(monkeypatch, tmp_path):
    token = set_hermes_home_override(tmp_path)
    try:
        path = _write_confirm_sidecar(tmp_path)
        adapter = FeishuAdapter(PlatformConfig())
        monkeypatch.setattr("gateway.platforms.feishu.P2CardActionTriggerResponse", _Response)
        monkeypatch.setattr("gateway.platforms.feishu.CallBackCard", _Card)
        event = SimpleNamespace(operator=SimpleNamespace(open_id="ou_user", name="User"), token="evt-1")
        action = {"hermes_action": "task_confirm", "task_id": "task-confirm", "confirm_id": "c1", "choice": "继续"}

        first = adapter._handle_task_confirm_card_action(event=event, action_value=action)
        duplicate = adapter._handle_task_confirm_card_action(event=event, action_value={**action, "choice": "中止"})
        body = json.loads(path.read_text(encoding="utf-8"))
    finally:
        reset_hermes_home_override(token)

    assert isinstance(first, _Response)
    assert isinstance(duplicate, _Response)
    assert "已记录选择" in json.dumps(first.card.data, ensure_ascii=False)
    assert "已经记录过" in json.dumps(duplicate.card.data, ensure_ascii=False)
    assert body["task_card"]["pending_confirms"][0]["resolved"]["choice"] == "继续"


@pytest.mark.asyncio
async def test_feishu_text_confirm_uses_same_writeback_and_short_circuits(monkeypatch, tmp_path):
    token = set_hermes_home_override(tmp_path)
    try:
        path = _write_confirm_sidecar(tmp_path)
        adapter = FeishuAdapter(PlatformConfig())
        adapter.handle_message = AsyncMock()
        adapter._add_ack_reaction = AsyncMock()
        event = MessageEvent(
            text="继续",
            message_type=MessageType.TEXT,
            source=SessionSource(
                platform=Platform.FEISHU,
                user_id="ou_user",
                user_name="User",
                chat_id="oc_test",
                chat_type="group",
                thread_id="topic:om_thread",
            ),
            message_id="om_text",
        )
        await adapter._handle_message_with_guards(event)
        body = json.loads(path.read_text(encoding="utf-8"))
    finally:
        reset_hermes_home_override(token)

    adapter.handle_message.assert_not_called()
    adapter._add_ack_reaction.assert_awaited_once_with("om_text")
    resolved = body["task_card"]["pending_confirms"][0]["resolved"]
    assert resolved["choice"] == "继续"
    assert resolved["source"] == "text"
    assert resolved["event_id"] == "om_text"
