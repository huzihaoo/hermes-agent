"""End-to-end rate limiting tests — rejection + sliding window recovery."""

from __future__ import annotations

import asyncio
import tempfile
import time
from pathlib import Path
from unittest.mock import patch

from gateway.admission.controller import AdmissionController


def _make_controller(tmp_path: Path, rate_limit: int = 3, window: int = 2) -> AdmissionController:
    """Create controller with tight rate limits for testing."""
    return AdmissionController(
        db_path=tmp_path / "queue.db",
        audit_dir=tmp_path / "audit",
        rate_limit_per_user=rate_limit,
        rate_limit_window_seconds=window,
    )


def test_rate_limit_rejects_after_quota_exceeded():
    """Requests beyond quota should be rejected with rate limit message."""
    with tempfile.TemporaryDirectory() as tmpdir:
        ctrl = _make_controller(Path(tmpdir), rate_limit=3, window=60)

        with patch("gateway.admission.controller._resolve_role", return_value="member"):
            # First 3 requests should succeed
            for i in range(3):
                admitted, msg, item = asyncio.run(
                    ctrl.admit(f"u1", f"request {i}", chat_id="c1")
                )
                assert admitted, f"Request {i} should be admitted"
                assert item is not None

            # 4th request should be rejected
            admitted, msg, item = asyncio.run(
                ctrl.admit("u1", "request 3", chat_id="c1")
            )
            assert not admitted, "Request should be rejected after quota exceeded"
            assert "请求过于频繁" in msg
            assert item is None


def test_rate_limit_sliding_window_recovery():
    """After window expires, quota should reset and requests succeed again."""
    with tempfile.TemporaryDirectory() as tmpdir:
        ctrl = _make_controller(Path(tmpdir), rate_limit=2, window=1)

        with patch("gateway.admission.controller._resolve_role", return_value="member"):
            # Fill quota
            for i in range(2):
                admitted, _, _ = asyncio.run(ctrl.admit("u1", f"req {i}", chat_id="c1"))
                assert admitted

            # Next request should be rejected
            admitted, msg, _ = asyncio.run(ctrl.admit("u1", "req 2", chat_id="c1"))
            assert not admitted
            assert "请求过于频繁" in msg

            # Wait for window to expire
            time.sleep(1.1)

            # Should succeed again after window reset
            admitted, msg, item = asyncio.run(ctrl.admit("u1", "req 3", chat_id="c1"))
            assert admitted, f"Request should succeed after window reset, got: {msg}"
            assert item is not None
