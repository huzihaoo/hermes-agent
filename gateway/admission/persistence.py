"""SQLite persistence for admission queue items with domain support."""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime
from pathlib import Path
from typing import List

from .types import QueueItem


class ConnectionPool:
    """Thread-safe single-connection pool for SQLite.

    Reuses one connection per db_path, protected by a lock.
    """

    def __init__(self, db_path: Path):
        self._db_path = db_path
        self._lock = threading.Lock()
        self._conn: sqlite3.Connection | None = None

    def get(self) -> sqlite3.Connection:
        with self._lock:
            if self._conn is None:
                self._db_path.parent.mkdir(parents=True, exist_ok=True)
                self._conn = sqlite3.connect(str(self._db_path), check_same_thread=False)
                self._conn.execute("PRAGMA journal_mode=WAL")
            return self._conn

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                try:
                    self._conn.close()
                except Exception:
                    pass
                self._conn = None


# Module-level pool cache: db_path -> ConnectionPool
_pools: dict[str, ConnectionPool] = {}
_pools_lock = threading.Lock()


def _get_pool(db_path: Path) -> ConnectionPool:
    key = str(db_path)
    with _pools_lock:
        if key not in _pools:
            _pools[key] = ConnectionPool(db_path)
        return _pools[key]


def init_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = _get_pool(db_path).get()
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS queue_items (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            user_role TEXT NOT NULL,
            message TEXT NOT NULL,
            lane TEXT NOT NULL,
            priority INTEGER NOT NULL,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            started_at TEXT,
            completed_at TEXT,
            result TEXT,
            chat_id TEXT,
            chat_type TEXT,
            thread_id TEXT,
            request_message_id TEXT,
            platform TEXT,
            domain TEXT NOT NULL DEFAULT 'user',
            domain_id TEXT NOT NULL DEFAULT '',
            retry_count INTEGER NOT NULL DEFAULT 0,
            max_retries INTEGER NOT NULL DEFAULT 3,
            last_error TEXT,
            next_retry_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS metrics (
            key TEXT PRIMARY KEY,
            value INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    # Migrate old tables that lack domain columns
    _migrate_add_column(conn, "domain", "TEXT NOT NULL DEFAULT 'user'")
    _migrate_add_column(conn, "domain_id", "TEXT NOT NULL DEFAULT ''")
    _migrate_add_column(conn, "chat_type", "TEXT")
    _migrate_add_column(conn, "request_message_id", "TEXT")
    # Migrate retry fields
    _migrate_add_column(conn, "retry_count", "INTEGER NOT NULL DEFAULT 0")
    _migrate_add_column(conn, "max_retries", "INTEGER NOT NULL DEFAULT 3")
    _migrate_add_column(conn, "last_error", "TEXT")
    _migrate_add_column(conn, "next_retry_at", "TEXT")
    conn.commit()


def _migrate_add_column(conn: sqlite3.Connection, col_name: str, col_def: str) -> None:
    """Add a column if it doesn't exist yet (idempotent migration)."""
    cursor = conn.execute("PRAGMA table_info(queue_items)")
    existing = {row[1] for row in cursor.fetchall()}
    if col_name not in existing:
        conn.execute(f"ALTER TABLE queue_items ADD COLUMN {col_name} {col_def}")


def save_items(db_path: Path, items: List[QueueItem]) -> None:
    """Save queue items to SQLite using upsert (no DELETE, safe for concurrent writes)."""
    init_db(db_path)
    conn = _get_pool(db_path).get()

    # Get all current item IDs in DB
    cursor = conn.execute("SELECT id FROM queue_items")
    existing_ids = {row[0] for row in cursor.fetchall()}

    # Upsert all items
    for item in items:
        conn.execute(
            """
            INSERT OR REPLACE INTO queue_items (
                id, user_id, user_role, message, lane, priority, status,
                created_at, started_at, completed_at, result,
                chat_id, chat_type, thread_id, request_message_id, platform,
                domain, domain_id,
                retry_count, max_retries, last_error, next_retry_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                item.id,
                item.user_id,
                item.user_role,
                item.message,
                item.lane,
                item.priority,
                item.status,
                item.created_at.isoformat(),
                item.started_at.isoformat() if item.started_at else None,
                item.completed_at.isoformat() if item.completed_at else None,
                json.dumps(item.result) if item.result else None,
                item.chat_id,
                item.chat_type,
                item.thread_id,
                item.request_message_id,
                item.platform,
                item.domain,
                item.domain_id,
                item.retry_count,
                item.max_retries,
                item.last_error,
                item.next_retry_at.isoformat() if item.next_retry_at else None,
            ),
        )

    # Delete items that are no longer in memory
    current_ids = {item.id for item in items}
    to_delete = existing_ids - current_ids
    for item_id in to_delete:
        conn.execute("DELETE FROM queue_items WHERE id = ?", (item_id,))

    conn.commit()


def load_items(db_path: Path) -> List[QueueItem]:
    if not db_path.exists():
        return []

    init_db(db_path)
    conn = _get_pool(db_path).get()
    cursor = conn.execute(
        """SELECT id, user_id, user_role, message, lane, priority, status,
                  created_at, started_at, completed_at, result,
                  chat_id, chat_type, thread_id, request_message_id, platform,
                  domain, domain_id,
                  retry_count, max_retries, last_error, next_retry_at
           FROM queue_items"""
    )

    items: list[QueueItem] = []
    for row in cursor.fetchall():
        domain = row[16] if row[16] else "user"
        domain_id = row[17] if row[17] else row[1]  # fallback to user_id
        items.append(
            QueueItem(
                id=row[0],
                user_id=row[1],
                user_role=row[2],
                message=row[3],
                lane=row[4],
                priority=row[5],
                status=row[6],
                created_at=datetime.fromisoformat(row[7]),
                started_at=datetime.fromisoformat(row[8]) if row[8] else None,
                completed_at=datetime.fromisoformat(row[9]) if row[9] else None,
                result=json.loads(row[10]) if row[10] else None,
                chat_id=row[11],
                chat_type=row[12],
                thread_id=row[13],
                request_message_id=row[14],
                platform=row[15],
                domain=domain,
                domain_id=domain_id,
                retry_count=row[18] if row[18] is not None else 0,
                max_retries=row[19] if row[19] is not None else 3,
                last_error=row[20],
                next_retry_at=datetime.fromisoformat(row[21]) if row[21] else None,
            )
        )

    return items


def save_metrics(db_path: Path, metrics: dict[str, int]) -> None:
    """Save metrics to SQLite."""
    init_db(db_path)
    conn = _get_pool(db_path).get()

    for key, value in metrics.items():
        conn.execute(
            "INSERT OR REPLACE INTO metrics (key, value) VALUES (?, ?)",
            (key, value)
        )

    conn.commit()


def load_metrics(db_path: Path) -> dict[str, int]:
    """Load metrics from SQLite."""
    if not db_path.exists():
        return {}

    init_db(db_path)
    conn = _get_pool(db_path).get()
    cursor = conn.execute("SELECT key, value FROM metrics")

    metrics = {row[0]: row[1] for row in cursor.fetchall()}

    return metrics
