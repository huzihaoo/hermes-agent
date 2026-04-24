"""Tests for the admission queue."""

import sys
from pathlib import Path

# Ensure repo root is importable
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from gateway.admission.queue import AdmissionQueue
from gateway.admission.types import QueueItem


def _make_item(id: str, role: str = "member", lane: str = "standard", priority: int = 10, **kw):
    return QueueItem(
        id=id,
        user_id=f"u-{id}",
        user_role=role,
        message=f"msg-{id}",
        lane=lane,
        priority=priority,
        **kw,
    )


# ------------------------------------------------------------------
# Basic enqueue / dequeue
# ------------------------------------------------------------------

def test_enqueue_and_dequeue():
    q = AdmissionQueue()
    item = _make_item("t1")
    q.enqueue(item)

    out = q.dequeue("standard")
    assert out is not None
    assert out.id == "t1"
    assert out.status == "processing"
    assert out.started_at is not None


def test_dequeue_empty_returns_none():
    q = AdmissionQueue()
    assert q.dequeue("fast") is None


# ------------------------------------------------------------------
# Priority ordering
# ------------------------------------------------------------------

def test_priority_ordering():
    q = AdmissionQueue()
    q.enqueue(_make_item("member-1", role="member", priority=10))
    q.enqueue(_make_item("owner-1", role="owner", priority=100))
    q.enqueue(_make_item("admin-1", role="admin", priority=50))

    first = q.dequeue("standard")
    assert first.id == "owner-1"

    second = q.dequeue("standard")
    assert second.id == "admin-1"

    third = q.dequeue("standard")
    assert third.id == "member-1"


# ------------------------------------------------------------------
# Lane isolation
# ------------------------------------------------------------------

def test_lanes_are_isolated():
    q = AdmissionQueue()
    q.enqueue(_make_item("fast-1", lane="fast", priority=10))
    q.enqueue(_make_item("heavy-1", lane="heavy", priority=10))

    assert q.dequeue("standard") is None
    assert q.dequeue("fast").id == "fast-1"
    assert q.dequeue("heavy").id == "heavy-1"


# ------------------------------------------------------------------
# Position tracking
# ------------------------------------------------------------------

def test_get_position():
    q = AdmissionQueue()
    q.enqueue(_make_item("a", priority=10))
    q.enqueue(_make_item("b", priority=100))

    # b has higher priority → position 1
    pos_b = q.get_position("b")
    assert pos_b == ("standard", 1)

    pos_a = q.get_position("a")
    assert pos_a == ("standard", 2)


def test_get_position_after_dequeue():
    q = AdmissionQueue()
    q.enqueue(_make_item("a", priority=10))
    q.dequeue("standard")

    # No longer queued
    assert q.get_position("a") is None


# ------------------------------------------------------------------
# Mark completed / failed / cancel
# ------------------------------------------------------------------

def test_mark_completed():
    q = AdmissionQueue()
    q.enqueue(_make_item("c1"))
    q.dequeue("standard")

    q.mark_completed("c1", {"output": "done"})
    item = q.get_item("c1")
    assert item.status == "completed"
    assert item.result == {"output": "done"}
    assert item.completed_at is not None


def test_mark_failed():
    q = AdmissionQueue()
    q.enqueue(_make_item("f1"))
    q.dequeue("standard")

    q.mark_failed("f1", "timeout")
    item = q.get_item("f1")
    assert item.status == "failed"
    assert item.result == {"error": "timeout"}


def test_cancel_queued_item():
    q = AdmissionQueue()
    q.enqueue(_make_item("x1"))

    assert q.cancel("x1") is True
    assert q.get_item("x1").status == "cancelled"
    assert q.pending_count("standard") == 0


def test_cancel_processing_item_fails():
    q = AdmissionQueue()
    q.enqueue(_make_item("x2"))
    q.dequeue("standard")

    assert q.cancel("x2") is False


# ------------------------------------------------------------------
# Pending count
# ------------------------------------------------------------------

def test_pending_count():
    q = AdmissionQueue()
    q.enqueue(_make_item("p1", lane="fast"))
    q.enqueue(_make_item("p2", lane="standard"))
    q.enqueue(_make_item("p3", lane="standard"))

    assert q.pending_count() == 3
    assert q.pending_count("fast") == 1
    assert q.pending_count("standard") == 2
    assert q.pending_count("heavy") == 0
