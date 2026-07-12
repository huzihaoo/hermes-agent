"""Tests for Feishu approval timeout and expired card updates."""

import importlib.util
import json
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Ensure the repo root is importable
# ---------------------------------------------------------------------------
_repo = str(Path(__file__).resolve().parents[2])
if _repo not in sys.path:
    sys.path.insert(0, _repo)


# ---------------------------------------------------------------------------
# Minimal Feishu mock so FeishuAdapter can be imported without lark-oapi
# ---------------------------------------------------------------------------
def _ensure_feishu_mocks():
    """Provide stubs for lark-oapi / aiohttp.web so the import succeeds."""
    try:
        has_lark = importlib.util.find_spec("lark_oapi") is not None
    except (ValueError, ModuleNotFoundError):
        has_lark = "lark_oapi" in sys.modules

    if not has_lark and "lark_oapi" not in sys.modules:
        mod = MagicMock()
        for name in (
            "lark_oapi", "lark_oapi.api.im.v1",
            "lark_oapi.event", "lark_oapi.event.callback_type",
        ):
            sys.modules.setdefault(name, mod)

    try:
        has_aiohttp = importlib.util.find_spec("aiohttp") is not None
    except (ValueError, ModuleNotFoundError):
        has_aiohttp = "aiohttp" in sys.modules

    if not has_aiohttp and "aiohttp" not in sys.modules:
        aio = MagicMock()
        sys.modules.setdefault("aiohttp", aio)
        sys.modules.setdefault("aiohttp.web", aio.web)


_ensure_feishu_mocks()

from gateway.config import PlatformConfig
from gateway.platforms.feishu import FeishuAdapter


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_adapter() -> FeishuAdapter:
    """Create a FeishuAdapter with mocked internals."""
    config = PlatformConfig(enabled=True)
    adapter = FeishuAdapter(config)
    adapter._client = MagicMock()
    return adapter


# ===========================================================================
# Timeout callback registration and cleanup
# ===========================================================================

class TestApprovalTimeoutCallbacks:
    """Test approval timeout callback registration and cleanup."""

    def test_register_timeout_callback(self):
        adapter = _make_adapter()
        callback = MagicMock()

        adapter.register_approval_timeout_callback("session-1", callback)

        assert "session-1" in adapter._approval_callbacks
        assert adapter._approval_callbacks["session-1"] is callback

    def test_timeout_callback_invoked_on_gc(self):
        adapter = _make_adapter()
        callback = MagicMock()

        # Register callback and create approval state
        adapter.register_approval_timeout_callback("session-1", callback)
        adapter._approval_state[1] = {
            "session_key": "session-1",
            "message_id": "msg_001",
            "chat_id": "oc_12345",
            "created_at": time.monotonic() - 1000,  # Old enough to be stale
        }

        # Run GC
        adapter._gc_stale_approval_state(max_age_seconds=10)

        # Callback should have been invoked with "timeout"
        callback.assert_called_once_with("timeout")
        assert 1 not in adapter._approval_state
        assert "session-1" not in adapter._approval_callbacks

    @pytest.mark.asyncio
    async def test_timeout_callback_cleaned_on_normal_resolve(self):
        adapter = _make_adapter()
        callback = MagicMock()

        # Register callback and create approval state
        adapter.register_approval_timeout_callback("session-1", callback)
        adapter._approval_state[1] = {
            "session_key": "session-1",
            "message_id": "msg_001",
            "chat_id": "oc_12345",
        }

        with patch("tools.approval.resolve_gateway_approval", return_value=1):
            await adapter._resolve_approval(
                1,
                "once",
                "Alice",
                open_id="ou_authorized_operator",
                chat_id="oc_12345",
            )

        # Callback should NOT have been invoked (normal resolution)
        callback.assert_not_called()
        # But it should have been cleaned up
        assert "session-1" not in adapter._approval_callbacks


# ===========================================================================
# Expired card builder
# ===========================================================================

class TestExpiredApprovalCard:
    """Test _build_expired_approval_card static method."""

    def test_builds_expired_card(self):
        card = FeishuAdapter._build_expired_approval_card()

        assert card["config"]["wide_screen_mode"] is True
        assert card["header"]["template"] == "grey"
        assert "Approval Expired" in card["header"]["title"]["content"]
        assert len(card["elements"]) == 1
        assert "expired" in card["elements"][0]["content"].lower()


# ===========================================================================
# Card update on timeout
# ===========================================================================

class TestUpdateApprovalCardToExpired:
    """Test _update_approval_card_to_expired async method."""

    @pytest.mark.asyncio
    async def test_updates_card_to_expired(self):
        adapter = _make_adapter()

        mock_response = SimpleNamespace(
            success=lambda: True,
            data=SimpleNamespace(message_id="msg_001"),
        )

        with (
            patch.object(adapter, "_build_update_message_body", return_value=MagicMock()) as mock_body,
            patch.object(adapter, "_build_update_message_request", return_value=MagicMock()) as mock_req,
            patch("asyncio.to_thread", new_callable=AsyncMock, return_value=mock_response) as mock_update,
        ):
            await adapter._update_approval_card_to_expired("msg_001", "oc_12345")

        # Verify the card was built and sent
        mock_body.assert_called_once()
        assert mock_body.call_args[1]["msg_type"] == "interactive"

        # Verify the payload contains expired card
        payload = mock_body.call_args[1]["content"]
        card = json.loads(payload)
        assert "Approval Expired" in card["header"]["title"]["content"]

        mock_req.assert_called_once()
        mock_update.assert_called_once()

    @pytest.mark.asyncio
    async def test_handles_update_failure_gracefully(self):
        adapter = _make_adapter()

        mock_response = SimpleNamespace(
            success=lambda: False,
            data=None,
        )

        with (
            patch.object(adapter, "_build_update_message_body", return_value=MagicMock()),
            patch.object(adapter, "_build_update_message_request", return_value=MagicMock()),
            patch("asyncio.to_thread", new_callable=AsyncMock, return_value=mock_response),
        ):
            # Should not raise
            await adapter._update_approval_card_to_expired("msg_001", "oc_12345")

    @pytest.mark.asyncio
    async def test_skips_update_when_not_connected(self):
        adapter = _make_adapter()
        adapter._client = None

        with patch.object(adapter, "_build_update_message_body") as mock_body:
            await adapter._update_approval_card_to_expired("msg_001", "oc_12345")

        # Should not attempt to build or send
        mock_body.assert_not_called()
