"""Tests for next_retry_at backoff window — items should not be dequeued during backoff."""

from __future__ import annotations

import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from gateway.admission.controller import AdmissionController
from gateway.admission.types import DEFAULT_RETRY_BASE_DELAY


def _make_controller(tmp_path: Path) -> AdmissionController:
    return AdmissionController(
        db_path=tmp_path / "queue.db",
        audit_dir=tmp_path / "audit",
    )


def test_dequeue_skips_items_in_backoff_window():
    """Items with next_retry_at in the future should not be dequeued."""
    with tempfile.TemporaryDirectory() as tmpdir:
        ctrl = _make_controller(Path(tmpdir))

        import asyncio as _aio
        with patch("gateway.admission.controller._resolve_role", return_value="member"):
            _, _, item = _aio.get_event_loop().run_until_complete(
                ctrl.admit("u1", "test backoff", chat_id="c1")
            )

        # Dequeue and fail to trigger retry with backoff
        dequeued = ctrl.dequeue_next(item.lane, domain=item.domain)
        assert dequeued is not None
        ctrl.fail(item.id, "transient error")

        # Item should be re-queued with next_retry_at set
        stored = ctrl.queue.get_item(item.id)
        assert stored.status == "queued"
        assert stored.retry_count == 1
        assert stored.next_retry_at is not None
        assert stored.next_retry_at > datetime.now()

        # Attempt to dequeue immediately — should return None (item still in backoff)
        dequeued_again = ctrl.dequeue_next(item.lane, domain=item.domain)
        assert dequeued_again is None, "Item should not be dequeued during backoff window"


def test_dequeue_returns_item_after_backoff_expires():
    """Items should become available after next_retry_at passes."""
    with tempfile.TemporaryDirectory() as tmpdir:
        ctrl = _make_controller(Path(tmpdir))

        import asyncio as _aio
        with patch("gateway.admission.controller._resolve_role", return_value="member"):
            _, _, item = _aio.get_event_loop().run_until_complete(
                ctrl.admit("u1", "test backoff expiry", chat_id="c1")
            )

        # Dequeue and fail
        dequeued = ctrl.dequeue_next(item.lane, domain=item.domain)
        assert dequeued is not None
        ctrl.fail(item.id, "transient error")

        stored = ctrl.queue.get_item(item.id)
        assert stored.next_retry_at is not None

        # Manually set next_retry_at to the past to simulate backoff expiry
        past_time = datetime.now() - timedelta(seconds=1)
        stored = ctrl.queue.get_item(item.id)
        stored.next_retry_at = past_time

        # Now dequeue should succeed
        dequeued_after_expiry = ctrl.dequeue_next(item.lane, domain=item.domain)
        assert dequeued_after_expiry is not None
        assert dequeued_after_expiry.id == item.id
