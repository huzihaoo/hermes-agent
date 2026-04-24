"""Tests for admission queue persistence."""

import tempfile
from pathlib import Path

from gateway.admission.queue import AdmissionQueue
from gateway.admission.types import QueueItem


def test_queue_survives_restart():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "queue.db"

        # Create queue, enqueue item, save
        queue1 = AdmissionQueue(db_path=db_path)
        item = QueueItem(
            id="test-1",
            user_id="u1",
            user_role="owner",
            message="msg",
            lane="standard",
            priority=100,
        )
        queue1.enqueue(item)
        queue1.save()

        # Create new queue instance, load
        queue2 = AdmissionQueue(db_path=db_path)
        queue2.load()

        out = queue2.dequeue("standard")
        assert out is not None
        assert out.id == "test-1"
        assert out.user_role == "owner"


def test_only_queued_items_are_restored():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "queue.db"

        queue1 = AdmissionQueue(db_path=db_path)
        queued = QueueItem(
            id="queued-1",
            user_id="u1",
            user_role="member",
            message="queued",
            lane="standard",
            priority=10,
        )
        processing = QueueItem(
            id="processing-1",
            user_id="u2",
            user_role="owner",
            message="processing",
            lane="standard",
            priority=100,
        )
        queue1.enqueue(queued)
        queue1.enqueue(processing)
        queue1.dequeue("standard")  # owner item becomes processing
        queue1.save()

        queue2 = AdmissionQueue(db_path=db_path)
        queue2.load()

        # Only queued item should remain
        out = queue2.dequeue("standard")
        assert out is not None
        assert out.id == "queued-1"
        assert queue2.dequeue("standard") is None
