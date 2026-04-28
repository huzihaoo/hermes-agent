"""Regression tests for Feishu admission routing metadata."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from gateway.config import Platform
from gateway.platforms.feishu import FeishuAdapter
from gateway.admission.feishu_integration import FeishuAdmissionBridge


@pytest.mark.asyncio
async def test_feishu_dispatch_admission_preserves_request_message_id_for_worker():
    adapter = object.__new__(FeishuAdapter)
    adapter.platform = Platform.FEISHU
    adapter._admission_enabled = True
    adapter._admission_controller = AsyncMock()
    adapter._admission_controller.admit.return_value = (True, "queued", MagicMock(lane="standard", id="queue-1"))

    source = adapter.build_source(
        chat_id="oc_chat_1",
        chat_name="PNC task topic",
        chat_type="group",
        user_id="ou_user_1",
        user_name="Alice",
        thread_id="topic:om_anchor_1",
    )
    from gateway.platforms.base import MessageEvent, MessageType
    event = MessageEvent(
        text="让 VM 查一下日志",
        message_type=MessageType.TEXT,
        source=source,
        message_id="om_request_1",
    )

    await adapter._dispatch_inbound_event(event)

    adapter._admission_controller.admit.assert_awaited_once()
    kwargs = adapter._admission_controller.admit.await_args.kwargs
    assert kwargs["chat_id"] == "oc_chat_1"
    assert kwargs["chat_type"] == "group"
    assert kwargs["thread_id"] == "topic:om_anchor_1"
    assert kwargs["request_message_id"] == "om_request_1"
    assert kwargs["platform"] == "feishu"


@pytest.mark.asyncio
async def test_feishu_queue_item_reconstructs_original_request_message_id():
    adapter = object.__new__(FeishuAdapter)
    adapter.platform = Platform.FEISHU
    adapter._handle_message_with_guards = AsyncMock()

    item = MagicMock()
    item.id = "queue-1"
    item.user_id = "ou_user_1"
    item.chat_id = "oc_chat_1"
    item.chat_type = "group"
    item.thread_id = "topic:om_anchor_1"
    item.request_message_id = "om_request_1"
    item.message = "VM 返回后要进同一话题"

    result = await adapter._process_queue_item(item)

    assert result == {"status": "completed"}
    processed_event = adapter._handle_message_with_guards.await_args.args[0]
    assert processed_event.message_id == "om_request_1"
    assert processed_event.source.chat_id == "oc_chat_1"
    assert processed_event.source.chat_type == "group"
    assert processed_event.source.thread_id == "topic:om_anchor_1"


@pytest.mark.asyncio
async def test_legacy_feishu_admission_bridge_passes_thread_and_request_message_id():
    feishu = MagicMock()
    feishu._handle_message = AsyncMock()
    admission = AsyncMock()
    admission.admit.return_value = (True, "queued", MagicMock(lane="standard", id="queue-1"))
    bridge = FeishuAdmissionBridge(feishu, admission)

    await bridge._intercept_message({
        "message_id": "om_request_1",
        "chat_id": "oc_chat_1",
        "chat_type": "group",
        "thread_id": "topic:om_anchor_1",
        "sender": {"sender_id": {"open_id": "ou_user_1"}},
        "message": {"content": "{\"text\":\"run VM task\"}"},
    })

    admission.admit.assert_awaited_once()
    kwargs = admission.admit.await_args.kwargs
    assert kwargs["chat_id"] == "oc_chat_1"
    assert kwargs["chat_type"] == "group"
    assert kwargs["thread_id"] == "topic:om_anchor_1"
    assert kwargs["request_message_id"] == "om_request_1"
