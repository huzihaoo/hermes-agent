"""Tests for the admission queue with domain isolation."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from gateway.admission.queue import AdmissionQueue
from gateway.admission.types import QueueItem


def _make_item(id: str, role: str = "member", lane: str = "standard",
               priority: int = 10, domain: str = "user", domain_id: str = "", **kw):
    return QueueItem(
        id=id,
        user_id=f"u-{id}",
        user_role=role,
        message=f"msg-{id}",
        lane=lane,
        priority=priority,
        domain=domain,
        domain_id=domain_id or f"u-{id}",
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
# Domain isolation
# ------------------------------------------------------------------

def test_domains_are_isolated():
    """Items in different domains don't interfere."""
    q = AdmissionQueue()
    q.enqueue(_make_item("u1", domain="user", domain_id="user-1"))
    q.enqueue(_make_item("g1", domain="group", domain_id="group-1"))
    q.enqueue(_make_item("v1", domain="vm", domain_id="vm-1"))

    # Dequeue from user domain only
    out = q.dequeue("standard", domain="user")
    assert out is not None
    assert out.id == "u1"

    # Group and VM still have their items
    assert q.pending_count(domain="group") == 1
    assert q.pending_count(domain="vm") == 1

    # Dequeue from group
    out_g = q.dequeue("standard", domain="group")
    assert out_g.id == "g1"

    # Dequeue from vm
    out_v = q.dequeue("standard", domain="vm")
    assert out_v.id == "v1"


def test_cross_domain_dequeue_picks_highest_priority():
    """Without domain filter, dequeue picks highest priority across domains."""
    q = AdmissionQueue()
    q.enqueue(_make_item("low", domain="user", priority=10))
    q.enqueue(_make_item("high", domain="group", priority=100))

    out = q.dequeue("standard")  # no domain filter
    assert out.id == "high"


def test_pending_count_by_domain():
    q = AdmissionQueue()
    q.enqueue(_make_item("a", domain="user", lane="fast"))
    q.enqueue(_make_item("b", domain="user", lane="standard"))
    q.enqueue(_make_item("c", domain="group", lane="standard"))

    assert q.pending_count() == 3
    assert q.pending_count(domain="user") == 2
    assert q.pending_count(domain="group") == 1
    assert q.pending_count(domain="vm") == 0
    assert q.pending_count(lane="standard") == 2
    assert q.pending_count(lane="standard", domain="user") == 1


def test_list_pending_by_domain():
    q = AdmissionQueue()
    q.enqueue(_make_item("a", domain="user"))
    q.enqueue(_make_item("b", domain="group"))

    user_items = q.list_pending(domain="user")
    assert len(user_items) == 1
    assert user_items[0].id == "a"

    all_items = q.list_pending()
    assert len(all_items) == 2


# ------------------------------------------------------------------
# Position tracking
# ------------------------------------------------------------------

def test_get_position():
    q = AdmissionQueue()
    # Same domain_id so they share a sub-queue
    q.enqueue(_make_item("a", priority=10, domain_id="shared"))
    q.enqueue(_make_item("b", priority=100, domain_id="shared"))

    pos_b = q.get_position("b")
    assert pos_b == ("standard", 1)

    pos_a = q.get_position("a")
    assert pos_a == ("standard", 2)


def test_get_position_different_domain_ids():
    """Items with different domain_ids each get position 1."""
    q = AdmissionQueue()
    q.enqueue(_make_item("a", priority=10, domain_id="did-a"))
    q.enqueue(_make_item("b", priority=100, domain_id="did-b"))

    assert q.get_position("a") == ("standard", 1)
    assert q.get_position("b") == ("standard", 1)


def test_get_position_after_dequeue():
    q = AdmissionQueue()
    q.enqueue(_make_item("a", priority=10))
    q.dequeue("standard")
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
