"""Tests for Memory v2 store."""

import time
import pytest

from agent.memory_v2.store import MemoryStore


@pytest.fixture
def store(tmp_path):
    return MemoryStore(db_path=tmp_path / "memory.db")


def test_add_and_get(store):
    mid = store.add("alice", "Python is great for scripting", category="knowledge")
    mem = store.get(mid)
    assert mem is not None
    assert mem["content"] == "Python is great for scripting"
    assert mem["user_id"] == "alice"
    assert mem["category"] == "knowledge"


def test_search_finds_relevant(store):
    store.add("alice", "Python is great for scripting")
    store.add("alice", "JavaScript runs in the browser")
    store.add("alice", "Rust is fast and safe")
    
    results = store.search("Python scripting")
    assert len(results) >= 1
    assert any("Python" in r["content"] for r in results)


def test_search_filters_by_user(store):
    store.add("alice", "Alice likes Python")
    store.add("bob", "Bob likes Java")
    
    results = store.search("likes", user_id="alice")
    assert len(results) == 1
    assert results[0]["user_id"] == "alice"


def test_list_recent(store):
    for i in range(5):
        store.add("alice", f"memory {i}")
    
    recent = store.list_recent(user_id="alice", limit=3)
    assert len(recent) == 3


def test_soft_delete(store):
    mid = store.add("alice", "to be deleted")
    assert store.get(mid) is not None
    
    store.delete(mid)
    assert store.get(mid) is None  # Soft-deleted


def test_decay_unused(store):
    mid = store.add("alice", "old memory", importance=5.0)
    
    # Decay memories not accessed in 0 days (all of them)
    count = store.decay_unused(days=0, decay_factor=0.5)
    assert count >= 1
    
    mem = store.get(mid)
    assert mem["importance"] < 5.0


def test_promote_frequent(store):
    mid = store.add("alice", "popular memory", importance=1.0)
    
    # Simulate accesses
    for _ in range(6):
        store.search("popular")
    
    count = store.promote_frequent(min_access=5, boost=1.5)
    assert count >= 1
    
    mem = store.get(mid)
    assert mem["importance"] > 1.0


def test_stats(store):
    store.add("alice", "memory 1")
    store.add("alice", "memory 2")
    store.add("bob", "memory 3")
    
    stats = store.stats()
    assert stats["total_memories"] == 3
    
    alice_stats = store.stats(user_id="alice")
    assert alice_stats["total_memories"] == 2


def test_access_count_increments_on_search(store):
    mid = store.add("alice", "searchable content")
    
    store.search("searchable")
    store.search("searchable")
    
    mem = store.get(mid)
    assert mem["access_count"] >= 2
    assert mem["last_accessed_at"] is not None
