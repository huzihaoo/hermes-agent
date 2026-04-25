"""Tests for the async queue worker (domain-based)."""

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
        assert len(worker._tasks) == 4  # 3 domains + 1 cleanup

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
        assert len(worker._tasks) == 4  # 3 domains + 1 cleanup
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

        admitted, feedback, item = await ctrl.admit(
            user_id="user-1",
            message="hello",
            chat_id="chat-1",
            platform="feishu",
        )
        assert admitted

        worker = QueueWorker(ctrl, handler)
        await worker.start()

        await asyncio.sleep(0.3)
        await worker.stop()

        assert len(processed) == 1
        assert processed[0] == item.id


@pytest.mark.asyncio
async def test_worker_handles_failure_no_retry():
    """Worker marks item as dead when process_fn raises and max_retries=0."""
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
        # Disable retries so failure goes straight to dead letter
        item.max_retries = 0

        worker = QueueWorker(ctrl, failing_handler)
        await worker.start()

        await asyncio.sleep(0.3)
        await worker.stop()

        stored = ctrl.queue.get_item(item.id)
        assert stored is not None
        # With max_retries=0, fail() marks as failed then sets status to dead
        assert stored.status == "dead"
        assert stored.last_error == "boom"


@pytest.mark.asyncio
async def test_worker_processes_multiple_lanes():
    """Worker processes items from different lanes via domain worker."""
    with tempfile.TemporaryDirectory() as tmpdir:
        ctrl = _make_controller(Path(tmpdir))

        processed = []

        async def handler(item):
            processed.append((item.id, item.lane, item.domain))
            return {"ok": True}

        _, _, item_fast = await ctrl.admit(
            user_id="u1", message="hi", chat_id="c1", platform="feishu",
        )
        _, _, item_heavy = await ctrl.admit(
            user_id="u2", message="请帮我写代码实现一个功能", chat_id="c2", platform="feishu",
        )
        _, _, item_std = await ctrl.admit(
            user_id="u3", message="帮我查一下这个问题的原因", chat_id="c3", platform="feishu",
        )

        worker = QueueWorker(ctrl, handler)
        await worker.start()

        await asyncio.sleep(0.5)
        await worker.stop()

        processed_ids = {pid for pid, _, _ in processed}
        assert item_fast.id in processed_ids
        assert item_heavy.id in processed_ids
        assert item_std.id in processed_ids


@pytest.mark.asyncio
async def test_worker_processes_across_domains():
    """Items in different domains are processed by their respective domain workers."""
    with tempfile.TemporaryDirectory() as tmpdir:
        ctrl = _make_controller(Path(tmpdir))

        processed = []

        async def handler(item):
            processed.append((item.id, item.domain))
            return {"ok": True}

        # user domain (DM)
        _, _, item_user = await ctrl.admit(
            user_id="u1", message="帮我查一下", chat_id="c1",
            chat_type="p2p", platform="feishu",
        )
        # group domain
        _, _, item_group = await ctrl.admit(
            user_id="u2", message="帮我查一下", chat_id="group-1",
            chat_type="group", platform="feishu",
        )
        # vm domain
        _, _, item_vm = await ctrl.admit(
            user_id="u3", message="帮我查一下",
            platform="vm", vm_id="vm-1",
        )

        worker = QueueWorker(ctrl, handler)
        await worker.start()

        await asyncio.sleep(0.5)
        await worker.stop()

        domain_map = {pid: dom for pid, dom in processed}
        assert item_user.id in domain_map
        assert item_group.id in domain_map
        assert item_vm.id in domain_map
        assert domain_map[item_user.id] == "user"
        assert domain_map[item_group.id] == "group"
        assert domain_map[item_vm.id] == "vm"
