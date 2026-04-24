"""In-memory admission queue with 3 lanes and priority ordering."""

from __future__ import annotations

import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from .types import Lane, QueueItem


class AdmissionQueue:
    """Thread-safe priority queue with fast / standard / heavy lanes."""

    def __init__(self, db_path: Path | None = None):
        self._lanes: Dict[Lane, List[QueueItem]] = {
            "fast": [],
            "standard": [],
            "heavy": [],
        }
        self._lock = threading.Lock()
        self._items_by_id: Dict[str, QueueItem] = {}
        self._db_path = db_path

    # ------------------------------------------------------------------
    # Core operations
    # ------------------------------------------------------------------

    def enqueue(self, item: QueueItem) -> None:
        with self._lock:
            self._lanes[item.lane].append(item)
            self._items_by_id[item.id] = item
            self._lanes[item.lane].sort(key=lambda x: x.priority, reverse=True)

    def dequeue(self, lane: Lane) -> QueueItem | None:
        with self._lock:
            if not self._lanes[lane]:
                return None
            item = self._lanes[lane].pop(0)
            item.status = "processing"
            item.started_at = datetime.now()
            return item

    def get_position(self, item_id: str) -> tuple[Lane, int] | None:
        """Return (lane, 1-based position) or None if not queued."""
        with self._lock:
            item = self._items_by_id.get(item_id)
            if not item or item.status != "queued":
                return None
            for i, q in enumerate(self._lanes[item.lane]):
                if q.id == item_id:
                    return (item.lane, i + 1)
            return None

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
            # Remove from lane
            self._lanes[item.lane] = [
                q for q in self._lanes[item.lane] if q.id != item_id
            ]
            return True

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    def pending_count(self, lane: Lane | None = None) -> int:
        with self._lock:
            if lane:
                return len(self._lanes[lane])
            return sum(len(v) for v in self._lanes.values())

    def get_item(self, item_id: str) -> QueueItem | None:
        with self._lock:
            return self._items_by_id.get(item_id)

    def list_pending(self, lane: Lane | None = None) -> list[QueueItem]:
        """Return all pending items, optionally filtered by lane."""
        with self._lock:
            if lane:
                return list(self._lanes[lane])
            result: list[QueueItem] = []
            for lane_items in self._lanes.values():
                result.extend(lane_items)
            return result

    # ------------------------------------------------------------------
    # Persistence (optional — wired in Task 2)
    # ------------------------------------------------------------------

    def save(self) -> None:
        if not self._db_path:
            return
        from .persistence import save_items

        with self._lock:
            all_items: list[QueueItem] = []
            for lane_items in self._lanes.values():
                all_items.extend(lane_items)
            save_items(self._db_path, all_items)

    def load(self) -> None:
        if not self._db_path:
            return
        from .persistence import load_items

        items = load_items(self._db_path)
        with self._lock:
            for item in items:
                self._lanes[item.lane].append(item)
                self._items_by_id[item.id] = item
            for lane in self._lanes:
                self._lanes[lane].sort(key=lambda x: x.priority, reverse=True)
