"""Concurrency stress tests for admission queue — multi-threaded enqueue/dequeue."""

from __future__ import annotations

import asyncio
import tempfile
import threading
from pathlib import Path
from unittest.mock import patch

from gateway.admission.controller import AdmissionController


def _make_controller(tmp_path: Path) -> AdmissionController:
    return AdmissionController(
        db_path=tmp_path / "queue.db",
        audit_dir=tmp_path / "audit",
        rate_limit_per_user=10000,  # effectively unlimited for stress tests
    )


def test_concurrent_enqueue_no_data_loss():
    """Multiple threads enqueuing simultaneously should not lose items."""
    with tempfile.TemporaryDirectory() as tmpdir:
        ctrl = _make_controller(Path(tmpdir))
        num_threads = 10
        items_per_thread = 50
        total_expected = num_threads * items_per_thread

        def enqueue_worker(thread_id: int):
            with patch("gateway.admission.controller._resolve_role", return_value="member"):
                for i in range(items_per_thread):
                    asyncio.run(
                        ctrl.admit(f"u{thread_id}", f"msg {i}", chat_id=f"c{thread_id}")
                    )

        threads = []
        for tid in range(num_threads):
            t = threading.Thread(target=enqueue_worker, args=(tid,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # Count all items across all domains/lanes
        total_items = 0
        for domain in ctrl.queue._queues.values():
            for domain_id_queues in domain.values():
                for lane_list in domain_id_queues.values():
                    total_items += len(lane_list)

        assert total_items == total_expected, (
            f"Expected {total_expected} items, got {total_items}. "
            f"Data loss detected in concurrent enqueue."
        )


def test_concurrent_dequeue_no_duplicate_items():
    """Multiple threads dequeuing simultaneously should not return duplicate items."""
    with tempfile.TemporaryDirectory() as tmpdir:
        ctrl = _make_controller(Path(tmpdir))
        num_items = 100

        # Enqueue items — use messages long enough to land in "standard" lane
        with patch("gateway.admission.controller._resolve_role", return_value="member"):
            for i in range(num_items):
                asyncio.run(ctrl.admit("u1", f"standard length message number {i}", chat_id="c1"))

        # Dequeue concurrently
        dequeued_ids = []
        lock = threading.Lock()

        def dequeue_worker():
            while True:
                item = ctrl.dequeue_next("standard", domain="user")
                if item is None:
                    break
                with lock:
                    dequeued_ids.append(item.id)

        num_workers = 5
        threads = []
        for _ in range(num_workers):
            t = threading.Thread(target=dequeue_worker)
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # Check for duplicates
        assert len(dequeued_ids) == len(set(dequeued_ids)), (
            f"Duplicate items detected: {len(dequeued_ids)} dequeued, "
            f"{len(set(dequeued_ids))} unique"
        )
        assert len(dequeued_ids) == num_items, (
            f"Expected {num_items} items, got {len(dequeued_ids)}"
        )


def test_concurrent_enqueue_dequeue_mixed():
    """Concurrent enqueue and dequeue should maintain consistency."""
    with tempfile.TemporaryDirectory() as tmpdir:
        ctrl = _make_controller(Path(tmpdir))
        num_producers = 5
        num_consumers = 3
        items_per_producer = 20
        total_expected = num_producers * items_per_producer

        dequeued_ids = []
        lock = threading.Lock()
        stop_flag = threading.Event()

        def producer(producer_id: int):
            with patch("gateway.admission.controller._resolve_role", return_value="member"):
                for i in range(items_per_producer):
                    asyncio.run(
                        ctrl.admit(f"u{producer_id}", f"standard length message {i}", chat_id=f"c{producer_id}")
                    )

        def consumer():
            while not stop_flag.is_set():
                item = ctrl.dequeue_next("standard", domain="user")
                if item:
                    with lock:
                        dequeued_ids.append(item.id)

        # Start consumers first
        consumer_threads = []
        for _ in range(num_consumers):
            t = threading.Thread(target=consumer, daemon=True)
            consumer_threads.append(t)
            t.start()

        # Start producers
        producer_threads = []
        for pid in range(num_producers):
            t = threading.Thread(target=producer, args=(pid,))
            producer_threads.append(t)
            t.start()

        # Wait for producers to finish
        for t in producer_threads:
            t.join()

        # Give consumers time to drain queue
        import time
        time.sleep(0.5)
        stop_flag.set()

        for t in consumer_threads:
            t.join(timeout=1)

        # Verify all items were dequeued
        assert len(dequeued_ids) == total_expected, (
            f"Expected {total_expected} items, got {len(dequeued_ids)}"
        )
        assert len(dequeued_ids) == len(set(dequeued_ids)), (
            "Duplicate items detected in concurrent enqueue/dequeue"
        )
