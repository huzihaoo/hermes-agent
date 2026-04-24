"""Tests for Feishu admission integration."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

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
    """Test that starting the bridge hooks into Feishu handler."""
    original = mock_feishu_adapter._handle_message
    await bridge.start()
    
    assert mock_feishu_adapter._handle_message != original
    assert bridge._original_handler == original


@pytest.mark.asyncio
async def test_bridge_stop_restores_handler(bridge, mock_feishu_adapter):
    """Test that stopping the bridge restores original handler."""
    original = mock_feishu_adapter._handle_message
    await bridge.start()
    await bridge.stop()
    
    assert mock_feishu_adapter._handle_message == original


@pytest.mark.asyncio
async def test_intercept_message_admits_to_queue(bridge, admission_controller):
    """Test that intercepted messages go through admission."""
    await bridge.start()

    event = {
        "message_id": "msg_001",
        "chat_id": "chat_group_123",
        "sender": {"sender_id": {"open_id": "user_alice"}},
        "message": {"content": "帮我查一下这个 bug 的原因"},
    }

    await bridge._intercept_message(event)

    # Verify item was queued
    items = admission_controller.queue.list_pending()
    assert len(items) == 1
    assert items[0].user_id == "user_alice"
    assert items[0].chat_id == "chat_group_123"
    assert items[0].lane == "standard"


@pytest.mark.asyncio
async def test_intercept_coding_message_heavy_lane(bridge, admission_controller):
    """Test that coding messages get classified as heavy lane."""
    await bridge.start()

    event = {
        "message_id": "msg_002",
        "chat_id": "chat_group_123",
        "sender": {"sender_id": {"open_id": "user_bob"}},
        "message": {"content": "帮我写代码实现一个排序算法"},
    }

    await bridge._intercept_message(event)

    items = admission_controller.queue.list_pending()
    assert len(items) == 1
    assert items[0].lane == "heavy"


@pytest.mark.asyncio
async def test_intercept_fallback_on_error(bridge, mock_feishu_adapter):
    """Test that errors fall back to original handler."""
    await bridge.start()

    # Make admission fail
    bridge.admission.admit = AsyncMock(side_effect=RuntimeError("boom"))

    event = {"message_id": "msg_003", "chat_id": "c", "sender": {}, "message": {}}
    await bridge._intercept_message(event)

    # Should have called original handler as fallback
    bridge._original_handler.assert_awaited_once_with(event)

