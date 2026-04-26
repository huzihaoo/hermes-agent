"""Tests for persistence connection pool — P3-3.

Covers:
- ConnectionPool reuses the same connection
- ConnectionPool is thread-safe
- save/load round-trip still works with pooled connections
- Pool close releases the connection
"""

from __future__ import annotations

import tempfile
import threading
from pathlib import Path

import pytest

from gateway.admission.persistence import ConnectionPool, load_items, save_items
from gateway.admission.types import QueueItem


def _item(id: str) -> QueueItem:
    return QueueItem(
        id=id, user_id=f"u-{id}", user_role="member",
        message=f"msg-{id}", lane="standard", priority=10,
        domain="user", domain_id=f"did-{id}",
    )


class TestConnectionPool:
    def test_get_returns_connection(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            pool = ConnectionPool(Path(tmpdir) / "test.db")
            conn = pool.get()
            assert conn is not None
            pool.close()

    def test_get_returns_same_connection(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            pool = ConnectionPool(Path(tmpdir) / "test.db")
            c1 = pool.get()
            c2 = pool.get()
            assert c1 is c2
            pool.close()

    def test_close_then_get_creates_new(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            pool = ConnectionPool(Path(tmpdir) / "test.db")
            c1 = pool.get()
            pool.close()
            c2 = pool.get()
            assert c2 is not None
            # After close + reopen, it's a new connection object
            assert c1 is not c2
            pool.close()

    def test_thread_safety(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            pool = ConnectionPool(Path(tmpdir) / "test.db")
            conns = []
            errors = []

            def worker():
                try:
                    c = pool.get()
                    conns.append(id(c))
                except Exception as e:
                    errors.append(e)

            threads = [threading.Thread(target=worker) for _ in range(10)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            assert not errors
            # All threads should get the same connection
            assert len(set(conns)) == 1
            pool.close()


class TestPooledPersistence:
    def test_save_load_roundtrip_with_pool(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db = Path(tmpdir) / "q.db"
            items = [_item("a"), _item("b"), _item("c")]
            save_items(db, items)
            loaded = load_items(db)
            assert len(loaded) == 3
            assert {i.id for i in loaded} == {"a", "b", "c"}
