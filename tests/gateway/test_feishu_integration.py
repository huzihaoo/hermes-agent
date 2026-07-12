"""Tests for Feishu admission integration."""

import pytest
from unittest.mock import AsyncMock, MagicMock

from gateway.admission import AdmissionController, FeishuAdmissionBridge


@pytest.fixture
def mock_feishu_adapter():
    """Mock FeishuAdapter."""
    adapter = MagicMock()
    adapter._handle_message = AsyncMock()
    return adapter


@pytest.fixture
def admission_controller(tmp_path):
    """Create admission controller with temp storage."""
    return AdmissionController(
        db_path=tmp_path / "queue.db",
        audit_dir=tmp_path / "audit",
    )


@pytest.fixture
def bridge(mock_feishu_adapter, admission_controller):
    """Create bridge instance."""
    return FeishuAdmissionBridge(
        feishu_adapter=mock_feishu_adapter,
        admission_controller=admission_controller,
    )


@pytest.mark.asyncio
async def test_bridge_start_hooks_handler(bridge, mock_feishu_adapter):
    original = mock_feishu_adapter._handle_message
    await bridge.start()

    assert mock_feishu_adapter._handle_message != original
    assert bridge._original_handler == original


@pytest.mark.asyncio
async def test_bridge_stop_restores_handler(bridge, mock_feishu_adapter):
    original = mock_feishu_adapter._handle_message
    await bridge.start()
    await bridge.stop()

    assert mock_feishu_adapter._handle_message == original


@pytest.mark.asyncio
async def test_intercept_group_message_routes_to_group_domain(bridge, admission_controller):
    """Group messages route to group domain with chat_id as domain_id."""
    await bridge.start()

    event = {
        "message_id": "msg_001",
        "chat_id": "chat_group_123",
        "chat_type": "group",
        "sender": {"sender_id": {"open_id": "user_alice"}},
        "message": {"content": "帮我查一下这个 bug 的原因"},
    }

    await bridge._intercept_message(event)

    items = admission_controller.queue.list_pending()
    assert len(items) == 1
    assert items[0].user_id == "user_alice"
    assert items[0].chat_id == "chat_group_123"
    assert items[0].lane == "standard"
    assert items[0].domain == "group"
    assert items[0].domain_id == "chat_group_123"


@pytest.mark.asyncio
async def test_intercept_group_message_filtered_by_domain(bridge, admission_controller):
    await bridge.start()

    event = {
        "message_id": "msg_002",
        "chat_id": "chat_group_123",
        "chat_type": "group",
        "sender": {"sender_id": {"open_id": "user_bob"}},
        "message": {"content": "帮我查一下这个问题的原因"},
    }

    await bridge._intercept_message(event)

    group_items = admission_controller.queue.list_pending(domain="group")
    assert len(group_items) == 1
    assert group_items[0].domain == "group"
    assert group_items[0].domain_id == "chat_group_123"


@pytest.mark.asyncio
async def test_intercept_p2p_message_routes_to_user_domain(bridge, admission_controller):
    """p2p messages route to user domain with user_id as domain_id."""
    await bridge.start()

    event = {
        "message_id": "msg_p2p",
        "chat_id": "chat_p2p_456",
        "chat_type": "p2p",
        "sender": {"sender_id": {"open_id": "user_carol"}},
        "message": {"content": "帮我查一下这个问题"},
    }

    await bridge._intercept_message(event)

    items = admission_controller.queue.list_pending()
    assert len(items) == 1
    assert items[0].domain == "user"
    assert items[0].domain_id == "user_carol"


@pytest.mark.asyncio
async def test_intercept_coding_message_heavy_lane(bridge, admission_controller):
    await bridge.start()

    event = {
        "message_id": "msg_003",
        "chat_id": "chat_group_123",
        "chat_type": "group",
        "sender": {"sender_id": {"open_id": "user_bob"}},
        "message": {"content": "帮我写代码实现一个排序算法"},
    }

    await bridge._intercept_message(event)

    items = admission_controller.queue.list_pending(domain="group")
    assert len(items) == 1
    assert items[0].lane == "heavy"
    assert items[0].domain == "group"


@pytest.mark.asyncio
async def test_intercept_fallback_on_error(bridge, mock_feishu_adapter):
    await bridge.start()

    bridge.admission.admit = AsyncMock(side_effect=RuntimeError("boom"))

    event = {"message_id": "msg_004", "chat_id": "c", "sender": {}, "message": {}}
    await bridge._intercept_message(event)

    bridge._original_handler.assert_awaited_once_with(event)
