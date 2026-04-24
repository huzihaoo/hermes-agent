"""Tests for the async queue worker."""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path

import pytest

from gateway.admission.controller import AdmissionController
from gateway.admission.worker import QueueWorker


def _make_controller(tmp_path: Path) -> AdmissionController:
    return AdmissionController(
        db_path=tmp_path / "queue.db",
        audit_dir=tmp_path / "audit",
    )


# ------------------------------------------------------------------
# Basic lifecycle
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_worker_start_stop():
    """Worker starts and stops without error."""
    with tempfile.TemporaryDirectory() as tmpdir:
        ctrl = _make_controller(Path(tmpdir))

        async def noop(item):
            return {"ok": True}

        worker = QueueWorker(ctrl, noop)
        await worker.start()
        assert worker._running is True
        assert len(worker._tasks) == 4  # 3 lanes + 1 cleanup

        await worker.stop()
        assert worker._running is False
        assert len(worker._tasks) == 0


@pytest.mark.asyncio
async def test_worker_double_start_is_noop():
    """Calling start() twice doesn't spawn extra tasks."""
    with tempfile.TemporaryDirectory() as tmpdir:
        ctrl = _make_controller(Path(tmpdir))

        async def noop(item):
            return {}

        worker = QueueWorker(ctrl, noop)
        await worker.start()
        await worker.start()  # should warn and return
        assert len(worker._tasks) == 4  # 3 lanes + 1 cleanup
        await worker.stop()


# ------------------------------------------------------------------
# Processing
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_worker_processes_item():
    """Worker dequeues and processes an item."""
    with tempfile.TemporaryDirectory() as tmpdir:
        ctrl = _make_controller(Path(tmpdir))

        processed = []

        async def handler(item):
            processed.append(item.id)
            return {"status": "done"}

        # Enqueue an item
        admitted, feedback, item = await ctrl.admit(
            user_id="user-1",
            message="hello",
            chat_id="chat-1",
            platform="feishu",
        )
        assert admitted

        worker = QueueWorker(ctrl, handler)
        await worker.start()

        # Give the worker loop time to pick up the item
        await asyncio.sleep(0.3)

        await worker.stop()

        assert len(processed) == 1
        assert processed[0] == item.id


@pytest.mark.asyncio
async def test_worker_handles_failure():
    """Worker marks item as failed when process_fn raises."""
    with tempfile.TemporaryDirectory() as tmpdir:
        ctrl = _make_controller(Path(tmpdir))

        async def failing_handler(item):
            raise RuntimeError("boom")

        admitted, _, item = await ctrl.admit(
            user_id="user-2",
            message="do something",
            chat_id="chat-2",
            platform="feishu",
        )
        assert admitted

        worker = QueueWorker(ctrl, failing_handler)
        await worker.start()

        await asyncio.sleep(0.3)
        await worker.stop()

        # Item should be marked failed in the queue
        stored = ctrl.queue.get_item(item.id)
        assert stored is not None
        assert stored.status == "failed"


@pytest.mark.asyncio
async def test_worker_processes_multiple_lanes():
    """Worker processes items from different lanes concurrently."""
    with tempfile.TemporaryDirectory() as tmpdir:
        ctrl = _make_controller(Path(tmpdir))

        processed = []

        async def handler(item):
            processed.append((item.id, item.lane))
            return {"ok": True}

        # Enqueue items in different lanes
        _, _, item_fast = await ctrl.admit(
            user_id="u1", message="hi", chat_id="c1", platform="feishu",
        )  # short msg -> fast lane

        _, _, item_heavy = await ctrl.admit(
            user_id="u2", message="请帮我写代码实现一个功能", chat_id="c2", platform="feishu",
        )  # coding keyword -> heavy lane

        _, _, item_std = await ctrl.admit(
            user_id="u3", message="帮我查一下这个问题的原因", chat_id="c3", platform="feishu",
        )  # standard

        worker = QueueWorker(ctrl, handler)
        await worker.start()

        await asyncio.sleep(0.5)
        await worker.stop()

        processed_ids = {pid for pid, _ in processed}
        assert item_fast.id in processed_ids
        assert item_heavy.id in processed_ids
        assert item_std.id in processed_ids
