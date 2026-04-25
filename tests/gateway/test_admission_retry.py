"""Tests for retry and dead-letter logic in the admission controller."""

from __future__ import annotations

import asyncio
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest

from gateway.admission.controller import AdmissionController
from gateway.admission.types import DEFAULT_MAX_RETRIES, DEFAULT_RETRY_BASE_DELAY
from gateway.admission.worker import QueueWorker


def _make_controller(tmp_path: Path) -> AdmissionController:
    return AdmissionController(
        db_path=tmp_path / "queue.db",
        audit_dir=tmp_path / "audit",
    )


# ------------------------------------------------------------------
# Controller-level retry
# ------------------------------------------------------------------


def test_fail_requeues_when_retries_remain():
    """First failure should re-enqueue the item with retry_count=1."""
    with tempfile.TemporaryDirectory() as tmpdir:
        ctrl = _make_controller(Path(tmpdir))

        import asyncio as _aio
        with patch("gateway.admission.controller._resolve_role", return_value="member"):
            _, _, item = _aio.get_event_loop().run_until_complete(
                ctrl.admit("u1", "hello world test", chat_id="c1")
            )

        # Dequeue so it becomes "processing"
        dequeued = ctrl.dequeue_next(item.lane, domain=item.domain)
        assert dequeued is not None
        assert dequeued.status == "processing"

        # Fail it — should re-enqueue (retry_count < max_retries)
        ctrl.fail(item.id, "transient error")

        stored = ctrl.queue.get_item(item.id)
        assert stored.status == "queued"
        assert stored.retry_count == 1
        assert stored.last_error == "transient error"
        assert stored.next_retry_at is not None
        assert stored.started_at is None  # reset on re-enqueue

        # Should be back in the pending queue
        assert ctrl.queue.pending_count(lane=item.lane, domain=item.domain) == 1


def test_fail_exponential_backoff():
    """Each retry should double the backoff delay."""
    with tempfile.TemporaryDirectory() as tmpdir:
        ctrl = _make_controller(Path(tmpdir))

        import asyncio as _aio
        with patch("gateway.admission.controller._resolve_role", return_value="member"):
            _, _, item = _aio.get_event_loop().run_until_complete(
                ctrl.admit("u1", "hello world test", chat_id="c1")
            )

        delays = []
        for i in range(DEFAULT_MAX_RETRIES):
            # Expire backoff so dequeue can pick it up
            stored = ctrl.queue.get_item(item.id)
            if stored and stored.next_retry_at:
                stored.next_retry_at = datetime.now() - timedelta(seconds=1)

            dequeued = ctrl.dequeue_next(item.lane, domain=item.domain)
            assert dequeued is not None, f"retry {i}: expected item in queue"

            before = datetime.now()
            ctrl.fail(item.id, f"error-{i}")

            stored = ctrl.queue.get_item(item.id)
            if stored.next_retry_at:
                delay = (stored.next_retry_at - before).total_seconds()
                delays.append(delay)

        # Verify exponential growth: 2s, 4s, 8s (with DEFAULT_RETRY_BASE_DELAY=2)
        for idx in range(1, len(delays)):
            assert delays[idx] > delays[idx - 1], (
                f"Backoff not increasing: {delays}"
            )


def test_fail_dead_letters_after_max_retries():
    """After exhausting retries, item should be dead-lettered."""
    with tempfile.TemporaryDirectory() as tmpdir:
        ctrl = _make_controller(Path(tmpdir))

        import asyncio as _aio
        with patch("gateway.admission.controller._resolve_role", return_value="member"):
            _, _, item = _aio.get_event_loop().run_until_complete(
                ctrl.admit("u1", "hello world test", chat_id="c1")
            )

        # Exhaust all retries
        for i in range(DEFAULT_MAX_RETRIES):
            # Expire backoff so dequeue can pick it up
            stored = ctrl.queue.get_item(item.id)
            if stored and stored.next_retry_at:
                stored.next_retry_at = datetime.now() - timedelta(seconds=1)

            dequeued = ctrl.dequeue_next(item.lane, domain=item.domain)
            assert dequeued is not None, f"retry {i}: expected item"
            ctrl.fail(item.id, f"error-{i}")

        # One more dequeue + fail should dead-letter
        stored = ctrl.queue.get_item(item.id)
        if stored and stored.next_retry_at:
            stored.next_retry_at = datetime.now() - timedelta(seconds=1)
        dequeued = ctrl.dequeue_next(item.lane, domain=item.domain)
        assert dequeued is not None
        ctrl.fail(item.id, "final error")

        stored = ctrl.queue.get_item(item.id)
        assert stored.status == "dead"
        assert stored.last_error == "final error"
        assert stored.retry_count == DEFAULT_MAX_RETRIES

        # Should not be in pending queue anymore
        assert ctrl.queue.pending_count(lane=item.lane, domain=item.domain) == 0

        # Metrics
        status = ctrl.get_status()
        assert status["metrics"]["total_dead"] >= 1


def test_fail_with_max_retries_zero_goes_straight_to_dead():
    """Item with max_retries=0 should dead-letter on first failure."""
    with tempfile.TemporaryDirectory() as tmpdir:
        ctrl = _make_controller(Path(tmpdir))

        import asyncio as _aio
        with patch("gateway.admission.controller._resolve_role", return_value="member"):
            _, _, item = _aio.get_event_loop().run_until_complete(
                ctrl.admit("u1", "hello world test", chat_id="c1")
            )

        item.max_retries = 0

        dequeued = ctrl.dequeue_next(item.lane, domain=item.domain)
        assert dequeued is not None
        ctrl.fail(item.id, "instant death")

        stored = ctrl.queue.get_item(item.id)
        assert stored.status == "dead"
        assert stored.last_error == "instant death"


def test_retry_metrics_tracked():
    """Retry count should be reflected in metrics."""
    with tempfile.TemporaryDirectory() as tmpdir:
        ctrl = _make_controller(Path(tmpdir))

        import asyncio as _aio
        with patch("gateway.admission.controller._resolve_role", return_value="member"):
            _, _, item = _aio.get_event_loop().run_until_complete(
                ctrl.admit("u1", "hello world test", chat_id="c1")
            )

        dequeued = ctrl.dequeue_next(item.lane, domain=item.domain)
        ctrl.fail(item.id, "err")

        status = ctrl.get_status()
        assert status["metrics"]["total_retried"] >= 1


# ------------------------------------------------------------------
# Worker-level retry integration
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_worker_retries_then_dead_letters():
    """Worker should retry failing items and eventually dead-letter them."""
    with tempfile.TemporaryDirectory() as tmpdir:
        ctrl = _make_controller(Path(tmpdir))

        call_count = 0

        async def always_fail(item):
            nonlocal call_count
            call_count += 1
            raise RuntimeError(f"fail-{call_count}")

        with patch("gateway.admission.controller._resolve_role", return_value="member"):
            _, _, item = await ctrl.admit(
                "u1", "hello world test", chat_id="c1", platform="feishu",
            )

        # Set low retries for fast test
        item.max_retries = 2

        # Use zero backoff so worker retries immediately
        with patch("gateway.admission.controller.DEFAULT_RETRY_BASE_DELAY", 0):
            worker = QueueWorker(ctrl, always_fail)
            await worker.start()

            # Worker polls every 0.5s; 3 attempts with zero backoff
            await asyncio.sleep(4.0)
            await worker.stop()

        stored = ctrl.queue.get_item(item.id)
        assert stored.status == "dead"
        # 1 initial + 2 retries = 3 calls total, then dead-letter on 3rd fail
        assert call_count >= 3
        assert stored.retry_count == 2


@pytest.mark.asyncio
async def test_worker_retry_then_succeed():
    """Item that fails once then succeeds should complete normally."""
    with tempfile.TemporaryDirectory() as tmpdir:
        ctrl = _make_controller(Path(tmpdir))

        call_count = 0

        async def fail_once(item):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("transient")
            return {"status": "ok"}

        with patch("gateway.admission.controller._resolve_role", return_value="member"):
            _, _, item = await ctrl.admit(
                "u1", "hello world test", chat_id="c1", platform="feishu",
            )

        # Use zero backoff so worker retries immediately
        with patch("gateway.admission.controller.DEFAULT_RETRY_BASE_DELAY", 0):
            worker = QueueWorker(ctrl, fail_once)
            await worker.start()

            await asyncio.sleep(4.0)
            await worker.stop()

        stored = ctrl.queue.get_item(item.id)
        assert stored.status == "completed"
        assert call_count == 2
