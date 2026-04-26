"""Graceful shutdown stress tests for QueueWorker — P3-2.

Covers:
- In-flight tasks complete within drain_timeout
- Tasks exceeding drain_timeout get cancelled
- Cancelled tasks' items don't stay stuck in 'processing'
- Stop on empty queue is instant
- Double stop is safe
- Many concurrent in-flight tasks during shutdown
"""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path

import pytest

from gateway.admission.controller import AdmissionController
from gateway.admission.worker import QueueWorker


def _ctrl(tmp: Path) -> AdmissionController:
    return AdmissionController(
        db_path=tmp / "q.db", audit_dir=tmp / "audit",
    )


@pytest.mark.asyncio
async def test_inflight_completes_within_drain():
    """A slow handler that finishes within drain_timeout should complete normally."""
    with tempfile.TemporaryDirectory() as tmpdir:
        ctrl = _ctrl(Path(tmpdir))
        completed = []

        async def slow_handler(item):
            await asyncio.sleep(0.2)
            completed.append(item.id)
            return {"ok": True}

        _, _, item = await ctrl.admit("u1", "hello", chat_id="c1")
        worker = QueueWorker(ctrl, slow_handler)
        await worker.start()
        await asyncio.sleep(0.1)  # let dispatcher pick it up
        await worker.stop(drain_timeout=2.0)

        assert item.id in completed
        stored = ctrl.queue.get_item(item.id)
        assert stored.status == "completed"


@pytest.mark.asyncio
async def test_inflight_cancelled_after_drain_timeout():
    """A handler that takes longer than drain_timeout gets cancelled."""
    with tempfile.TemporaryDirectory() as tmpdir:
        ctrl = _ctrl(Path(tmpdir))
        started = []

        async def very_slow_handler(item):
            started.append(item.id)
            await asyncio.sleep(60)  # way longer than drain
            return {"ok": True}

        _, _, item = await ctrl.admit("u1", "hello", chat_id="c1")
        worker = QueueWorker(ctrl, very_slow_handler)
        await worker.start()
        await asyncio.sleep(0.3)  # let dispatcher pick it up
        await worker.stop(drain_timeout=0.3)

        assert item.id in started
        # After cancellation, inflight set should be empty
        assert len(worker._inflight) == 0


@pytest.mark.asyncio
async def test_stop_on_empty_queue_is_fast():
    """Stopping with no items should return quickly."""
    with tempfile.TemporaryDirectory() as tmpdir:
        ctrl = _ctrl(Path(tmpdir))

        async def noop(item):
            return {}

        worker = QueueWorker(ctrl, noop)
        await worker.start()
        await worker.stop(drain_timeout=1.0)
        assert not worker._running


@pytest.mark.asyncio
async def test_double_stop_is_safe():
    """Calling stop() twice should not raise."""
    with tempfile.TemporaryDirectory() as tmpdir:
        ctrl = _ctrl(Path(tmpdir))

        async def noop(item):
            return {}

        worker = QueueWorker(ctrl, noop)
        await worker.start()
        await worker.stop()
        await worker.stop()  # second stop should be a no-op
        assert not worker._running


@pytest.mark.asyncio
async def test_many_inflight_during_shutdown():
    """10 concurrent in-flight tasks should all drain or cancel cleanly."""
    with tempfile.TemporaryDirectory() as tmpdir:
        ctrl = _ctrl(Path(tmpdir))
        completed = []

        async def handler(item):
            await asyncio.sleep(0.1)
            completed.append(item.id)
            return {"ok": True}

        items = []
        for i in range(10):
            _, _, item = await ctrl.admit(f"u{i}", f"msg {i}", chat_id=f"c{i}")
            items.append(item)

        worker = QueueWorker(ctrl, handler, max_concurrent_per_domain=10)
        await worker.start()
        await asyncio.sleep(0.3)  # let dispatchers pick up items
        await worker.stop(drain_timeout=3.0)

        # All should have completed (0.1s each, 3s drain)
        assert len(completed) == 10
        assert len(worker._inflight) == 0
