"""Multi-process concurrency tests for memory system."""
import json
import multiprocessing
import pytest
import os


@pytest.fixture(autouse=True)
def _isolate_hermes_home(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))


def _write_memory_process(hermes_home, user_id, content, result_queue):
    """Worker function for multiprocess test."""
    try:
        # Set HERMES_HOME in subprocess
        os.environ["HERMES_HOME"] = hermes_home
        
        from tools.memory_tool import MemoryStore, memory_tool
        store = MemoryStore(user_id=user_id)
        store.load_from_disk()
        r = json.loads(memory_tool(
            action="add",
            target="memory",
            content=content,
            store=store
        ))
        result_queue.put(("success", user_id, r["success"]))
    except Exception as e:
        result_queue.put(("error", user_id, str(e)))


def test_multiprocess_writes_different_users(tmp_path):
    """Multiple processes writing to different users should not interfere."""
    # Use spawn to ensure clean process state
    ctx = multiprocessing.get_context("spawn")
    result_queue = ctx.Queue()
    
    processes = []
    for i in range(5):
        user_id = f"user_{i}"
        content = f"User {i} secret data"
        p = ctx.Process(
            target=_write_memory_process,
            args=(str(tmp_path), user_id, content, result_queue)
        )
        processes.append(p)
        p.start()
    
    for p in processes:
        p.join(timeout=10)
    
    # Collect results
    results = []
    while not result_queue.empty():
        results.append(result_queue.get())
    
    # All should succeed
    assert len(results) == 5, f"Expected 5 results, got {len(results)}"
    errors = [r for r in results if r[0] == "error"]
    assert len(errors) == 0, f"Errors: {errors}"
    
    # Verify isolation
    os.environ["HERMES_HOME"] = str(tmp_path)
    from tools.memory_tool import MemoryStore
    for i in range(5):
        store = MemoryStore(user_id=f"user_{i}")
        store.load_from_disk()
        assert len(store.memory_entries) == 1, f"User {i} should have 1 entry"
        assert f"User {i}" in store.memory_entries[0]


def test_multiprocess_writes_same_user(tmp_path):
    """Multiple processes writing to same user should be safe (file lock)."""
    ctx = multiprocessing.get_context("spawn")
    result_queue = ctx.Queue()
    
    user_id = "shared_user"
    processes = []
    for i in range(3):
        content = f"Entry {i}"
        p = ctx.Process(
            target=_write_memory_process,
            args=(str(tmp_path), user_id, content, result_queue)
        )
        processes.append(p)
        p.start()
    
    for p in processes:
        p.join(timeout=10)
    
    results = []
    while not result_queue.empty():
        results.append(result_queue.get())
    
    # All should succeed (file lock prevents corruption)
    assert len(results) == 3, f"Expected 3 results, got {len(results)}"
    errors = [r for r in results if r[0] == "error"]
    assert len(errors) == 0, f"Errors: {errors}"
    
    # Verify all entries saved
    os.environ["HERMES_HOME"] = str(tmp_path)
    from tools.memory_tool import MemoryStore
    store = MemoryStore(user_id=user_id)
    store.load_from_disk()
    assert len(store.memory_entries) == 3, f"Expected 3 entries, got {len(store.memory_entries)}"
