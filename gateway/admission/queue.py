"""In-memory admission queue with domain_id sub-queue isolation.

Structure: domain -> domain_id -> lane -> [QueueItem, ...]

  user/alice:fast    user/alice:standard    user/alice:heavy
  user/bob:fast      user/bob:standard      user/bob:heavy
  group/g1:fast      group/g1:standard      group/g1:heavy
  vm/vm-1:fast       vm/vm-1:standard       vm/vm-1:heavy
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import fields
import threading
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, List

from .types import ALL_DOMAINS, ALL_LANES, Domain, Lane, QueueItem


class AdmissionPersistenceError(RuntimeError):
    """Raised when a DB-backed queue can no longer make durable progress."""


class AdmissionQueue:
    """Thread-safe priority queue with domain + domain_id + lane isolation.

    Each (domain, domain_id) pair gets its own set of 3 lanes.
    Workers round-robin across domain_ids within a domain.
    """

    def __init__(self, db_path: Path | None = None):
        # domain -> domain_id -> lane -> [QueueItem]
        self._queues: Dict[Domain, Dict[str, Dict[Lane, List[QueueItem]]]] = {
            d: defaultdict(lambda: {l: [] for l in ALL_LANES})
            for d in ALL_DOMAINS
        }
        self._lock = threading.Lock()
        self._items_by_id: Dict[str, QueueItem] = {}
        self._db_path = db_path
        self._persistence_healthy = True
        self._persistence_errors: list[str] = []
        # Round-robin index per domain for fair scheduling across domain_ids
        self._rr_index: Dict[Domain, int] = {d: 0 for d in ALL_DOMAINS}

    # ------------------------------------------------------------------
    # Core operations
    # ------------------------------------------------------------------

    def enqueue(self, item: QueueItem) -> None:
        with self._lock:
            self._enqueue_locked(item)

    def enqueue_persisted(self, item: QueueItem) -> None:
        """Enqueue and persist as one operation, rolling memory back on failure."""
        if not self._db_path:
            self.enqueue(item)
            return

        from .persistence import save_items

        with self._lock:
            self._require_persistence_healthy_locked()
            previous = self._items_by_id.get(item.id)
            previous_locations = self._remove_queued_id_locked(item.id)
            self._enqueue_locked(item)
            try:
                save_items(self._db_path, list(self._items_by_id.values()))
            except BaseException as exc:
                self._remove_queued_id_locked(item.id)
                if previous is None:
                    self._items_by_id.pop(item.id, None)
                else:
                    self._items_by_id[item.id] = previous
                self._restore_queue_locations_locked(previous_locations)
                self._mark_persistence_unhealthy_locked(
                    f"save_failed:{type(exc).__name__}"
                )
                raise AdmissionPersistenceError(
                    "admission queue durable enqueue failed"
                ) from exc

    def _enqueue_locked(self, item: QueueItem) -> None:
        self._remove_queued_id_locked(item.id)
        lane_list = self._queues[item.domain][item.domain_id][item.lane]
        lane_list.append(item)
        self._items_by_id[item.id] = item
        lane_list.sort(key=lambda x: x.priority, reverse=True)

    def _remove_queued_id_locked(
        self, item_id: str
    ) -> list[tuple[Domain, str, Lane, int, QueueItem]]:
        removed: list[tuple[Domain, str, Lane, int, QueueItem]] = []
        for domain in ALL_DOMAINS:
            for domain_id, lanes in list(self._queues[domain].items()):
                for lane in ALL_LANES:
                    queue = lanes[lane]
                    for index in range(len(queue) - 1, -1, -1):
                        queued = queue[index]
                        if queued.id == item_id:
                            removed.append((domain, domain_id, lane, index, queued))
                            queue.pop(index)
                self._gc_empty_domain_id(domain, domain_id)
        return removed

    def _restore_queue_locations_locked(
        self,
        locations: list[tuple[Domain, str, Lane, int, QueueItem]],
    ) -> None:
        for domain, domain_id, lane, index, item in reversed(locations):
            queue = self._queues[domain][domain_id][lane]
            queue.insert(min(index, len(queue)), item)

    def dequeue(self, lane: Lane, domain: Domain | None = None,
                domain_id: str | None = None) -> QueueItem | None:
        """Dequeue highest-priority item.

        Specificity levels:
          - domain + domain_id: exact sub-queue
          - domain only: round-robin across domain_ids within domain
          - neither: scan all domains, pick highest priority
        """
        with self._lock:
            self._require_persistence_healthy_locked()
            return self._dequeue_locked(lane, domain=domain, domain_id=domain_id)

    def dequeue_persisted(
        self,
        lane: Lane,
        domain: Domain | None = None,
        domain_id: str | None = None,
    ) -> QueueItem | None:
        """Move one item to processing only after that state is persisted."""
        if not self._db_path:
            return self.dequeue(lane, domain=domain, domain_id=domain_id)

        from .persistence import save_items

        with self._lock:
            self._require_persistence_healthy_locked()
            item = self._dequeue_locked(lane, domain=domain, domain_id=domain_id)
            if item is None:
                return None
            try:
                save_items(self._db_path, list(self._items_by_id.values()))
            except BaseException as exc:
                item.status = "queued"
                item.started_at = None
                self._enqueue_locked(item)
                self._mark_persistence_unhealthy_locked(
                    f"save_failed:{type(exc).__name__}"
                )
                raise AdmissionPersistenceError(
                    "admission queue durable dequeue failed"
                ) from exc
            return item

    def _dequeue_locked(
        self,
        lane: Lane,
        domain: Domain | None = None,
        domain_id: str | None = None,
    ) -> QueueItem | None:
        if domain is not None and domain_id is not None:
            return self._pop_first(domain, domain_id, lane)

        if domain is not None:
            return self._pop_round_robin(domain, lane)

        # Cross-domain: find highest priority ready item across everything
        best: QueueItem | None = None
        best_d: Domain | None = None
        best_did: str | None = None
        now = datetime.now()
        for candidate_domain in ALL_DOMAINS:
            for candidate_domain_id, lanes in self._queues[candidate_domain].items():
                queue = lanes[lane]
                for candidate in queue:
                    if candidate.next_retry_at and candidate.next_retry_at > now:
                        continue
                    if best is None or candidate.priority > best.priority:
                        best = candidate
                        best_d = candidate_domain
                        best_did = candidate_domain_id
                    break  # first ready item in this sub-queue
        if best is not None and best_d is not None and best_did is not None:
            return self._pop_first(best_d, best_did, lane)
        return None

    def _pop_first(self, domain: Domain, domain_id: str, lane: Lane) -> QueueItem | None:
        """Pop first ready item from exact sub-queue. Caller holds lock.

        Skips items whose next_retry_at is in the future (backoff not yet expired).
        """
        q = self._queues[domain][domain_id][lane]
        if not q:
            return None
        now = datetime.now()
        for i, candidate in enumerate(q):
            if candidate.next_retry_at and candidate.next_retry_at > now:
                continue  # still in backoff window
            item = q.pop(i)
            item.status = "processing"
            item.started_at = now
            self._gc_empty_domain_id(domain, domain_id)
            return item
        return None  # all items in backoff

    def _pop_round_robin(self, domain: Domain, lane: Lane) -> QueueItem | None:
        """Round-robin across domain_ids within a domain. Caller holds lock.

        Tries each domain_id starting from the round-robin index.
        Skips domain_ids where all items are still in backoff.
        """
        domain_ids = [did for did, lanes in self._queues[domain].items()
                      if lanes.get(lane)]
        if not domain_ids:
            return None

        domain_ids.sort()  # stable order for deterministic round-robin
        n = len(domain_ids)
        start = self._rr_index[domain] % n

        for offset in range(n):
            idx = (start + offset) % n
            did = domain_ids[idx]
            item = self._pop_first(domain, did, lane)
            if item is not None:
                self._rr_index[domain] = idx + 1
                return item

        return None  # all domain_ids in backoff

    def _gc_empty_domain_id(self, domain: Domain, domain_id: str) -> None:
        """Remove domain_id entry if all its lanes are empty. Caller holds lock.

        Safety: only delete if the entry is a plain dict (not the defaultdict
        factory result that concurrent enqueue might be writing to).  Because
        defaultdict.__getitem__ re-creates the key atomically, the worst case
        of a race is that we skip the GC — which is harmless.
        """
        lanes = self._queues[domain].get(domain_id)
        if lanes is None:
            return
        if all(len(lanes.get(l, [])) == 0 for l in ALL_LANES):
            # Use pop to avoid KeyError if another thread already removed it
            self._queues[domain].pop(domain_id, None)

    def get_position(self, item_id: str) -> tuple[Lane, int] | None:
        """Return (lane, 1-based position) or None if not queued."""
        with self._lock:
            item = self._items_by_id.get(item_id)
            if not item or item.status != "queued":
                return None
            q = self._queues[item.domain][item.domain_id][item.lane]
            for i, qi in enumerate(q):
                if qi.id == item_id:
                    return (item.lane, i + 1)
            return None

    def update_persisted(
        self,
        item_id: str,
        mutation: Callable[[QueueItem], None],
    ) -> QueueItem | None:
        """Apply and persist a queue-item transition, rolling back on failure."""
        from .persistence import save_items

        with self._lock:
            self._require_persistence_healthy_locked()
            item = self._items_by_id.get(item_id)
            if item is None:
                return None
            snapshot = deepcopy(item)
            previous_locations = self._remove_queued_id_locked(item_id)

            def restore() -> None:
                self._remove_queued_id_locked(item_id)
                for item_field in fields(QueueItem):
                    setattr(
                        item,
                        item_field.name,
                        deepcopy(getattr(snapshot, item_field.name)),
                    )
                self._items_by_id[item_id] = item
                self._restore_queue_locations_locked(previous_locations)

            try:
                mutation(item)
                if item.id != item_id:
                    raise ValueError("queue item mutation cannot change identity")
            except BaseException:
                restore()
                raise

            if item.status == "queued":
                self._enqueue_locked(item)
            else:
                self._items_by_id[item_id] = item

            if not self._db_path:
                return item
            try:
                save_items(self._db_path, list(self._items_by_id.values()))
            except BaseException as exc:
                restore()
                self._mark_persistence_unhealthy_locked(
                    f"save_failed:{type(exc).__name__}"
                )
                raise AdmissionPersistenceError(
                    "admission queue durable state transition failed"
                ) from exc
            return item

    def mark_completed(self, item_id: str, result: dict | None = None) -> None:
        with self._lock:
            item = self._items_by_id.get(item_id)
            if item:
                item.status = "completed"
                item.result = result
                item.completed_at = datetime.now()

    def mark_failed(self, item_id: str, error: str | None = None) -> None:
        with self._lock:
            item = self._items_by_id.get(item_id)
            if item:
                item.status = "failed"
                item.result = {"error": error} if error else None
                item.completed_at = datetime.now()

    def cancel(self, item_id: str) -> bool:
        """Cancel a queued item. Returns True if cancelled."""
        with self._lock:
            item = self._items_by_id.get(item_id)
            if not item or item.status != "queued":
                return False
            item.status = "cancelled"
            q = self._queues[item.domain][item.domain_id][item.lane]
            self._queues[item.domain][item.domain_id][item.lane] = [
                qi for qi in q if qi.id != item_id
            ]
            self._gc_empty_domain_id(item.domain, item.domain_id)
            return True

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    def pending_count(self, lane: Lane | None = None, domain: Domain | None = None,
                      domain_id: str | None = None) -> int:
        with self._lock:
            return self._count(lane=lane, domain=domain, domain_id=domain_id)

    def _count(self, lane: Lane | None, domain: Domain | None,
               domain_id: str | None) -> int:
        """Count pending items. Caller holds lock."""
        total = 0
        domains = [domain] if domain else list(ALL_DOMAINS)
        for d in domains:
            dids = [domain_id] if domain_id else list(self._queues[d].keys())
            for did in dids:
                if did not in self._queues[d]:
                    continue
                lanes_map = self._queues[d][did]
                lanes = [lane] if lane else list(ALL_LANES)
                for l in lanes:
                    total += len(lanes_map[l])
        return total

    def get_item(self, item_id: str) -> QueueItem | None:
        with self._lock:
            return self._items_by_id.get(item_id)

    def list_pending(self, lane: Lane | None = None, domain: Domain | None = None,
                     domain_id: str | None = None) -> list[QueueItem]:
        """Return pending items, optionally filtered."""
        with self._lock:
            result: list[QueueItem] = []
            domains = [domain] if domain else list(ALL_DOMAINS)
            for d in domains:
                dids = [domain_id] if domain_id else list(self._queues[d].keys())
                for did in dids:
                    if did not in self._queues[d]:
                        continue
                    lanes_map = self._queues[d][did]
                    lanes = [lane] if lane else list(ALL_LANES)
                    for l in lanes:
                        result.extend(lanes_map[l])
            return result

    def active_domain_ids(self, domain: Domain) -> list[str]:
        """Return domain_ids that have pending items in this domain."""
        with self._lock:
            return sorted(self._queues[domain].keys())

    def cleanup_old_items(self, max_age_hours: int = 24) -> int:
        """Remove completed/failed items older than max_age_hours."""
        from datetime import timedelta

        cutoff = datetime.now() - timedelta(hours=max_age_hours)
        removed = 0

        with self._lock:
            self._require_persistence_healthy_locked()
            to_remove = [
                item_id
                for item_id, item in self._items_by_id.items()
                if item.status in ("completed", "failed", "cancelled")
                and item.completed_at
                and item.completed_at < cutoff
            ]
            for item_id in to_remove:
                del self._items_by_id[item_id]
                removed += 1

        return removed

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    @property
    def persistence_healthy(self) -> bool:
        with self._lock:
            return self._persistence_healthy

    @property
    def persistence_errors(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(self._persistence_errors)

    def persistence_status(self) -> dict[str, object]:
        with self._lock:
            return {
                "configured": self._db_path is not None,
                "healthy": self._persistence_healthy,
                "errors": list(self._persistence_errors),
            }

    def require_persistence_healthy(self) -> None:
        with self._lock:
            self._require_persistence_healthy_locked()

    def _require_persistence_healthy_locked(self) -> None:
        if self._db_path and not self._persistence_healthy:
            detail = self._persistence_errors[-1] if self._persistence_errors else "unknown"
            raise AdmissionPersistenceError(
                f"admission queue persistence is unhealthy: {detail}"
            )

    def _mark_persistence_unhealthy_locked(self, error: str) -> None:
        self._persistence_healthy = False
        if error not in self._persistence_errors:
            self._persistence_errors.append(error)

    def save(self) -> None:
        if not self._db_path:
            return
        from .persistence import save_items

        with self._lock:
            self._require_persistence_healthy_locked()
            # Save all items from _items_by_id, not just queued ones
            all_items = list(self._items_by_id.values())
            try:
                save_items(self._db_path, all_items)
            except BaseException as exc:
                self._mark_persistence_unhealthy_locked(
                    f"save_failed:{type(exc).__name__}"
                )
                raise AdmissionPersistenceError(
                    "admission queue persistence failed"
                ) from exc

    def load(self) -> None:
        if not self._db_path:
            return
        from .persistence import load_items

        corruption_errors: list[str] = []
        try:
            items = load_items(
                self._db_path,
                corruption_errors=corruption_errors,
            )
        except BaseException as exc:
            with self._lock:
                self._mark_persistence_unhealthy_locked(
                    f"load_failed:{type(exc).__name__}"
                )
            raise AdmissionPersistenceError(
                "admission queue persistence load failed"
            ) from exc
        with self._lock:
            for item in items:
                self._items_by_id[item.id] = item
                # A process can exit after dequeue persisted ``processing`` but
                # before the worker completed. No live task owns that item after
                # restart, so make it retryable instead of stranding it forever.
                if item.status == "processing":
                    item.status = "queued"
                    item.started_at = None
                if item.status == "queued":
                    self._queues[item.domain][item.domain_id][item.lane].append(item)
            # Sort each sub-queue
            for d in ALL_DOMAINS:
                for did, lanes_map in self._queues[d].items():
                    for l in ALL_LANES:
                        lanes_map[l].sort(key=lambda x: x.priority, reverse=True)
            for error in corruption_errors:
                self._mark_persistence_unhealthy_locked(error)
