"""Test approval timeout → Feishu card update flow."""

from __future__ import annotations

import os
import threading
import time
from unittest.mock import patch, AsyncMock, MagicMock

import pytest


def _clear_approval_state():
    """Reset all module-level approval state between tests."""
    from tools import approval as mod
    mod._gateway_queues.clear()
    mod._gateway_notify_cbs.clear()
    mod._session_approved.clear()
    mod._permanent_approved.clear()
    mod._pending.clear()
    mod._gateway_timeout_cbs.clear()


class TestApprovalTimeoutCardUpdate:
    """Tests for approval timeout → Feishu card expiry flow."""

    def setup_method(self):
        _clear_approval_state()

    def test_timeout_returns_blocked_message(self):
        """Baseline: approval timeout returns 'timed out' message."""
        from tools.approval import (
            register_gateway_notify,
            unregister_gateway_notify,
            check_all_command_guards,
            set_current_session_key,
            reset_current_session_key,
        )

        session_key = "test-timeout-session"
        notify_called = []

        def notify_cb(approval_data: dict):
            notify_called.append(approval_data)

        register_gateway_notify(session_key, notify_cb)

        # Set the session key via contextvar so get_current_session_key() returns it
        token = set_current_session_key(session_key)
        try:
            # Trigger approval with very short timeout
            with patch.dict(os.environ, {"HERMES_GATEWAY_SESSION": session_key}):
                with patch("tools.approval._get_approval_config") as mock_cfg:
                    mock_cfg.return_value = {"gateway_timeout": 1}  # 1 second timeout

                    result = check_all_command_guards(
                        "rm -rf /tmp/test",
                        env_type="host",
                    )

            # Should have timed out
            assert result["approved"] is False
            assert "timed out" in result["message"]
            assert len(notify_called) == 1  # Approval card was sent
        finally:
            reset_current_session_key(token)
            unregister_gateway_notify(session_key)

    def test_timeout_callback_is_invoked_on_timeout(self):
        """Timeout callback should be invoked when approval times out."""
        from tools.approval import (
            register_gateway_notify,
            register_gateway_timeout_callback,
            unregister_gateway_notify,
            check_all_command_guards,
            set_current_session_key,
            reset_current_session_key,
        )

        session_key = "test-timeout-cb-session"
        notify_called = []
        timeout_called = []

        def notify_cb(approval_data: dict):
            notify_called.append(approval_data)

        def timeout_cb(approval_data: dict):
            """Timeout callback receives the same approval_data dict."""
            timeout_called.append(approval_data)

        register_gateway_notify(session_key, notify_cb)
        register_gateway_timeout_callback(session_key, timeout_cb)

        token = set_current_session_key(session_key)
        try:
            with patch.dict(os.environ, {"HERMES_GATEWAY_SESSION": session_key}):
                with patch("tools.approval._get_approval_config") as mock_cfg:
                    mock_cfg.return_value = {"gateway_timeout": 1}

                    result = check_all_command_guards(
                        "rm -rf /tmp/test",
                        env_type="host",
                    )

            assert result["approved"] is False
            assert "timed out" in result["message"]
            assert len(notify_called) == 1  # Approval card was sent
            assert len(timeout_called) == 1  # Timeout callback was invoked
            assert timeout_called[0]["command"] == "rm -rf /tmp/test"
        finally:
            reset_current_session_key(token)
            unregister_gateway_notify(session_key)

    def test_feishu_card_updated_on_timeout(self):
        """Feishu approval card should be updated to show 'expired' on timeout."""
        from tools.approval import (
            register_gateway_notify,
            register_gateway_timeout_callback,
            unregister_gateway_notify,
            check_all_command_guards,
            set_current_session_key,
            reset_current_session_key,
        )

        session_key = "test-feishu-timeout-session"
        notify_called = []
        timeout_called = []
        
        # Mock Feishu platform with edit_message capability
        mock_feishu = MagicMock()
        mock_feishu.edit_message = AsyncMock(return_value=MagicMock(success=True))
        
        def notify_cb(approval_data: dict):
            """Simulate Feishu sending an approval card."""
            notify_called.append(approval_data)
            # Store message_id in approval_data for timeout callback to use
            approval_data["message_id"] = "om_test_message_123"
            approval_data["chat_id"] = "oc_test_chat_456"

        def timeout_cb(approval_data: dict):
            """Timeout callback that updates the Feishu card."""
            timeout_called.append(approval_data)
            
            # Build expired card content
            expired_card = {
                "config": {"wide_screen_mode": True},
                "header": {
                    "title": {"content": "⏱️ Approval Expired", "tag": "plain_text"},
                    "template": "grey",
                },
                "elements": [
                    {
                        "tag": "markdown",
                        "content": f"```\\n{approval_data['command']}\\n```\\n**Status:** Timed out waiting for approval",
                    }
                ],
            }
            
            # Simulate updating the card (in real code, this would call platform.edit_message)
            import json
            import asyncio
            message_id = approval_data.get("message_id")
            chat_id = approval_data.get("chat_id")
            if message_id and chat_id:
                # In real implementation, this would be called by the gateway
                asyncio.run(mock_feishu.edit_message(
                    chat_id=chat_id,
                    message_id=message_id,
                    content=json.dumps(expired_card, ensure_ascii=False),
                ))

        register_gateway_notify(session_key, notify_cb)
        register_gateway_timeout_callback(session_key, timeout_cb)

        token = set_current_session_key(session_key)
        try:
            with patch.dict(os.environ, {"HERMES_GATEWAY_SESSION": session_key}):
                with patch("tools.approval._get_approval_config") as mock_cfg:
                    mock_cfg.return_value = {"gateway_timeout": 1}

                    result = check_all_command_guards(
                        "rm -rf /tmp/test",
                        env_type="host",
                    )

            # Verify timeout occurred
            assert result["approved"] is False
            assert "timed out" in result["message"]
            assert len(notify_called) == 1
            assert len(timeout_called) == 1
            
            # Verify the card was updated
            assert mock_feishu.edit_message.called
            call_args = mock_feishu.edit_message.call_args
            assert call_args[1]["chat_id"] == "oc_test_chat_456"
            assert call_args[1]["message_id"] == "om_test_message_123"
            assert "Approval Expired" in call_args[1]["content"]
        finally:
            reset_current_session_key(token)
            unregister_gateway_notify(session_key)
