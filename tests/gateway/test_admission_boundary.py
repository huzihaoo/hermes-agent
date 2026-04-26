"""Boundary / edge-case tests for admission queue — P3-1.

Covers:
- Empty queue dequeue across all lanes/domains
- Extreme domain_id count (100+)
- Single lane saturation (1000 items)
- Cancel nonexistent item
- Double dequeue same item
- Dequeue with all items in backoff
- Cleanup old items boundary
- Position of nonexistent item
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta

from gateway.admission.queue import AdmissionQueue
from gateway.admission.types import ALL_DOMAINS, ALL_LANES, QueueItem


def _item(id: str, lane: str = "standard", priority: int = 10,
          domain: str = "user", domain_id: str = "", **kw) -> QueueItem:
    return QueueItem(
        id=id, user_id=f"u-{id}", user_role="member",
        message=f"msg-{id}", lane=lane, priority=priority,
        domain=domain, domain_id=domain_id or f"did-{id}", **kw,
    )


# ── Empty queue ──────────────────────────────────────────────────


class TestEmptyQueue:
    def test_dequeue_all_lanes_empty(self):
        q = AdmissionQueue()
        for lane in ALL_LANES:
            assert q.dequeue(lane) is None

    def test_dequeue_all_domains_empty(self):
        q = AdmissionQueue()
        for domain in ALL_DOMAINS:
            for lane in ALL_LANES:
                assert q.dequeue(lane, domain=domain) is None

    def test_pending_count_zero(self):
        q = AdmissionQueue()
        assert q.pending_count() == 0
        for lane in ALL_LANES:
            assert q.pending_count(lane=lane) == 0

    def test_list_pending_empty(self):
        q = AdmissionQueue()
        assert q.list_pending() == []

    def test_active_domain_ids_empty(self):
        q = AdmissionQueue()
        for d in ALL_DOMAINS:
            assert q.active_domain_ids(d) == []

    def test_get_position_nonexistent(self):
        q = AdmissionQueue()
        assert q.get_position("does-not-exist") is None

    def test_cancel_nonexistent(self):
        q = AdmissionQueue()
        assert q.cancel("nope") is False

    def test_mark_completed_nonexistent(self):
        q = AdmissionQueue()
        q.mark_completed("nope", {"x": 1})  # should not raise

    def test_mark_failed_nonexistent(self):
        q = AdmissionQueue()
        q.mark_failed("nope", "err")  # should not raise

    def test_cleanup_empty(self):
        q = AdmissionQueue()
        assert q.cleanup_old_items() == 0


# ── Extreme domain_id count ──────────────────────────────────────


class TestExtremeDomainIds:
    def test_100_domain_ids_enqueue_dequeue(self):
        q = AdmissionQueue()
        n = 100
        for i in range(n):
            q.enqueue(_item(f"item-{i}", domain_id=f"did-{i}"))

        assert q.pending_count() == n
        assert len(q.active_domain_ids("user")) == n

        dequeued = set()
        for _ in range(n):
            item = q.dequeue("standard", domain="user")
            assert item is not None
            dequeued.add(item.id)

        assert len(dequeued) == n
        assert q.pending_count() == 0

    def test_round_robin_fairness_across_many_domain_ids(self):
        """With 50 domain_ids each having 2 items, round-robin should
        visit different domain_ids before revisiting."""
        q = AdmissionQueue()
        n_dids = 50
        for i in range(n_dids):
            q.enqueue(_item(f"a-{i}", domain_id=f"did-{i}", priority=10))
            q.enqueue(_item(f"b-{i}", domain_id=f"did-{i}", priority=10))

        first_batch_dids = set()
        for _ in range(n_dids):
            item = q.dequeue("standard", domain="user")
            assert item is not None
            first_batch_dids.add(item.domain_id)

        # Should have visited all 50 domain_ids in the first batch
        assert len(first_batch_dids) == n_dids


# ── Single lane saturation ───────────────────────────────────────


class TestLaneSaturation:
    def test_1000_items_single_lane(self):
        q = AdmissionQueue()
        n = 1000
        for i in range(n):
            q.enqueue(_item(f"sat-{i}", lane="heavy", domain_id="shared"))

        assert q.pending_count(lane="heavy") == n

        dequeued = []
        for _ in range(n):
            item = q.dequeue("heavy", domain="user", domain_id="shared")
            assert item is not None
            dequeued.append(item.id)

        assert len(dequeued) == n
        assert q.pending_count(lane="heavy") == 0

    def test_priority_preserved_under_saturation(self):
        q = AdmissionQueue()
        # Insert 500 low-priority, then 1 high-priority
        for i in range(500):
            q.enqueue(_item(f"low-{i}", lane="fast", priority=10, domain_id="shared"))
        q.enqueue(_item("vip", lane="fast", priority=100, domain_id="shared"))

        first = q.dequeue("fast", domain="user", domain_id="shared")
        assert first.id == "vip"


# ── Double dequeue / backoff ─────────────────────────────────────


class TestDoubleDequeue:
    def test_dequeue_same_item_twice_returns_none_second_time(self):
        q = AdmissionQueue()
        q.enqueue(_item("once", domain_id="shared"))
        first = q.dequeue("standard", domain="user", domain_id="shared")
        assert first is not None
        second = q.dequeue("standard", domain="user", domain_id="shared")
        assert second is None

    def test_all_items_in_backoff_returns_none(self):
        q = AdmissionQueue()
        item = _item("backoff", domain_id="shared",
                      next_retry_at=datetime.now() + timedelta(hours=1))
        q.enqueue(item)
        assert q.dequeue("standard", domain="user", domain_id="shared") is None


# ── Cleanup boundary ─────────────────────────────────────────────


class TestCleanupBoundary:
    def test_cleanup_only_removes_old_completed(self):
        q = AdmissionQueue()
        q.enqueue(_item("old"))
        q.dequeue("standard")
        q.mark_completed("old")
        # Manually backdate
        q.get_item("old").completed_at = datetime.now() - timedelta(hours=48)

        q.enqueue(_item("recent"))
        q.dequeue("standard")
        q.mark_completed("recent")

        removed = q.cleanup_old_items(max_age_hours=24)
        assert removed == 1
        assert q.get_item("old") is None
        assert q.get_item("recent") is not None

    def test_cleanup_skips_queued_items(self):
        q = AdmissionQueue()
        q.enqueue(_item("still-queued"))
        removed = q.cleanup_old_items(max_age_hours=0)
        assert removed == 0
        assert q.get_item("still-queued") is not None
