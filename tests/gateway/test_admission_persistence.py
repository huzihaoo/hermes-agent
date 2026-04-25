"""Tests for persistence and recovery of the admission controller."""

from __future__ import annotations

import tempfile
from pathlib import Path
from unittest.mock import patch

from gateway.admission.controller import AdmissionController


def _make_controller(tmp_path: Path) -> AdmissionController:
    return AdmissionController(
        db_path=tmp_path / "queue.db",
        audit_dir=tmp_path / "audit",
    )


def test_queue_survives_restart():
    """Items in queue should persist across controller restarts."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)

        # First session: admit 3 items
        ctrl1 = _make_controller(tmp_path)
        import asyncio as _aio
        with patch("gateway.admission.controller._resolve_role", return_value="member"):
            _, _, item1 = _aio.get_event_loop().run_until_complete(
                ctrl1.admit("u1", "msg1", chat_id="c1")
            )
            _, _, item2 = _aio.get_event_loop().run_until_complete(
                ctrl1.admit("u2", "msg2", chat_id="c2")
            )
            _, _, item3 = _aio.get_event_loop().run_until_complete(
                ctrl1.admit("u1", "msg3", chat_id="c1")
            )

        # Restart controller
        ctrl2 = _make_controller(tmp_path)

        # All items should still be there
        stored1 = ctrl2.queue.get_item(item1.id)
        stored2 = ctrl2.queue.get_item(item2.id)
        stored3 = ctrl2.queue.get_item(item3.id)

        assert stored1 is not None
        assert stored2 is not None
        assert stored3 is not None

        assert stored1.status == "queued"
        assert stored2.status == "queued"
        assert stored3.status == "queued"

        # Pending counts should match
        status = ctrl2.get_status()
        # ctrl1 admitted 3 items, ctrl2 loaded metrics from disk
        assert status["metrics"]["total_admitted"] >= 3


def test_processing_items_survive_restart():
    """Items in 'processing' state should persist across restarts."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)

        ctrl1 = _make_controller(tmp_path)
        import asyncio as _aio
        with patch("gateway.admission.controller._resolve_role", return_value="member"):
            _, _, item = _aio.get_event_loop().run_until_complete(
                ctrl1.admit("u1", "msg", chat_id="c1")
            )

        # Dequeue to mark as processing
        dequeued = ctrl1.dequeue_next(item.lane, domain=item.domain)
        assert dequeued.status == "processing"

        # Restart
        ctrl2 = _make_controller(tmp_path)

        stored = ctrl2.queue.get_item(item.id)
        assert stored.status == "processing"
        assert stored.started_at is not None


def test_completed_items_survive_restart():
    """Completed items should persist across restarts."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)

        ctrl1 = _make_controller(tmp_path)
        import asyncio as _aio
        with patch("gateway.admission.controller._resolve_role", return_value="member"):
            _, _, item = _aio.get_event_loop().run_until_complete(
                ctrl1.admit("u1", "msg", chat_id="c1")
            )

        dequeued = ctrl1.dequeue_next(item.lane, domain=item.domain)
        ctrl1.complete(item.id, {"result": "ok"})

        # Restart
        ctrl2 = _make_controller(tmp_path)

        stored = ctrl2.queue.get_item(item.id)
        assert stored.status == "completed"
        assert stored.result == {"result": "ok"}


def test_dead_items_survive_restart():
    """Dead-lettered items should persist across restarts."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)

        ctrl1 = _make_controller(tmp_path)
        import asyncio as _aio
        with patch("gateway.admission.controller._resolve_role", return_value="member"):
            _, _, item = _aio.get_event_loop().run_until_complete(
                ctrl1.admit("u1", "msg", chat_id="c1")
            )

        item.max_retries = 0
        dequeued = ctrl1.dequeue_next(item.lane, domain=item.domain)
        ctrl1.fail(item.id, "instant death")

        # Restart
        ctrl2 = _make_controller(tmp_path)

        stored = ctrl2.queue.get_item(item.id)
        assert stored.status == "dead"
        assert stored.last_error == "instant death"


def test_audit_log_persists():
    """Audit log should be written to disk and survive restarts."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)

        ctrl1 = _make_controller(tmp_path)
        import asyncio as _aio
        with patch("gateway.admission.controller._resolve_role", return_value="member"):
            _, _, item = _aio.get_event_loop().run_until_complete(
                ctrl1.admit("u1", "msg", chat_id="c1", platform="feishu")
            )

        # Audit file should exist
        audit_dir = tmp_path / "audit"
        audit_files = list(audit_dir.glob("*.jsonl"))
        assert len(audit_files) > 0

        # Read audit log
        import json
        with open(audit_files[0]) as f:
            lines = f.readlines()
            assert len(lines) >= 1
            entry = json.loads(lines[0])
            assert entry["action"] == "enqueue"
            assert entry["resource"] == item.id
            assert entry["user_id"] == "u1"


def test_metrics_survive_restart():
    """Metrics should be recalculated correctly after restart."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)

        ctrl1 = _make_controller(tmp_path)
        import asyncio as _aio
        with patch("gateway.admission.controller._resolve_role", return_value="member"):
            _, _, item1 = _aio.get_event_loop().run_until_complete(
                ctrl1.admit("u1", "msg1", chat_id="c1")
            )
            _, _, item2 = _aio.get_event_loop().run_until_complete(
                ctrl1.admit("u2", "msg2", chat_id="c2")
            )

        # Complete one, fail one
        dequeued1 = ctrl1.dequeue_next(item1.lane, domain=item1.domain)
        ctrl1.complete(item1.id, {"ok": True})

        dequeued2 = ctrl1.dequeue_next(item2.lane, domain=item2.domain)
        item2.max_retries = 0
        ctrl1.fail(item2.id, "err")

        # Restart
        ctrl2 = _make_controller(tmp_path)

        status = ctrl2.get_status()
        assert status["metrics"]["total_completed"] >= 1
        assert status["metrics"]["total_dead"] >= 1


def test_lane_state_survives_restart():
    """Lane-specific state should persist across restarts."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)

        ctrl1 = _make_controller(tmp_path)
        import asyncio as _aio
        with patch("gateway.admission.controller._resolve_role", return_value="member"):
            _, _, item1 = _aio.get_event_loop().run_until_complete(
                ctrl1.admit("u1", "msg1", chat_id="c1")
            )
            _, _, item2 = _aio.get_event_loop().run_until_complete(
                ctrl1.admit("u2", "msg2", chat_id="c2")
            )

        # Restart
        ctrl2 = _make_controller(tmp_path)

        # Each domain_id should have 1 item
        assert ctrl2.queue.pending_count(lane=item1.lane, domain=item1.domain, domain_id=item1.domain_id) == 1
        assert ctrl2.queue.pending_count(lane=item2.lane, domain=item2.domain, domain_id=item2.domain_id) == 1


def test_retry_state_survives_restart():
    """Retry count and next_retry_at should persist across restarts."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)

        ctrl1 = _make_controller(tmp_path)
        import asyncio as _aio
        with patch("gateway.admission.controller._resolve_role", return_value="member"):
            _, _, item = _aio.get_event_loop().run_until_complete(
                ctrl1.admit("u1", "msg", chat_id="c1")
            )

        dequeued = ctrl1.dequeue_next(item.lane, domain=item.domain)
        ctrl1.fail(item.id, "err")

        stored1 = ctrl1.queue.get_item(item.id)
        assert stored1.retry_count == 1
        assert stored1.next_retry_at is not None

        # Restart
        ctrl2 = _make_controller(tmp_path)

        stored2 = ctrl2.queue.get_item(item.id)
        assert stored2.retry_count == 1
        assert stored2.next_retry_at == stored1.next_retry_at
