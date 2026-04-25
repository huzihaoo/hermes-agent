"""Tests for gateway.tasks.store — SQLite task persistence."""

import pytest
from gateway.tasks.store import TaskStore
from gateway.tasks.types import Task, TaskStatus, TaskType


@pytest.fixture
def store(tmp_path):
    return TaskStore(db_path=tmp_path / "tasks.db")


def test_upsert_and_get(store):
    task = Task(
        task_id="t1",
        status=TaskStatus.RUNNING,
        task_type=TaskType.CODING,
        user_id="alice",
        platform="feishu",
        request_summary="写一个函数",
        started_at=1000.0,
    )
    store.upsert(task)
    loaded = store.get("t1")
    assert loaded is not None
    assert loaded.task_id == "t1"
    assert loaded.status == TaskStatus.RUNNING
    assert loaded.task_type == TaskType.CODING
    assert loaded.user_id == "alice"


def test_upsert_updates_status(store):
    task = Task(
        task_id="t1",
        status=TaskStatus.RUNNING,
        task_type=TaskType.CHAT,
        user_id="bob",
        platform="cli",
        request_summary="hello",
        started_at=2000.0,
    )
    store.upsert(task)
    # Complete the task
    task.status = TaskStatus.COMPLETED
    task.completed_at = 2010.0
    store.upsert(task)
    loaded = store.get("t1")
    assert loaded.status == TaskStatus.COMPLETED
    assert loaded.completed_at == 2010.0


def test_get_nonexistent(store):
    assert store.get("nope") is None


def test_list_recent_returns_sorted(store):
    for i, ts in enumerate([1000.0, 3000.0, 2000.0]):
        store.upsert(Task(
            task_id=f"t{i}",
            status=TaskStatus.COMPLETED,
            task_type=TaskType.CHAT,
            user_id="alice",
            platform="cli",
            request_summary=f"task {i}",
            started_at=ts,
        ))
    tasks = store.list_recent(limit=10)
    assert [t.task_id for t in tasks] == ["t1", "t2", "t0"]


def test_list_recent_filters_by_user(store):
    store.upsert(Task(
        task_id="t-alice",
        status=TaskStatus.COMPLETED,
        task_type=TaskType.CHAT,
        user_id="alice",
        platform="cli",
        request_summary="alice's task",
        started_at=1000.0,
    ))
    store.upsert(Task(
        task_id="t-bob",
        status=TaskStatus.COMPLETED,
        task_type=TaskType.CHAT,
        user_id="bob",
        platform="cli",
        request_summary="bob's task",
        started_at=2000.0,
    ))
    tasks = store.list_recent(user_id="alice")
    assert len(tasks) == 1
    assert tasks[0].task_id == "t-alice"


def test_list_recent_respects_limit(store):
    for i in range(20):
        store.upsert(Task(
            task_id=f"t{i}",
            status=TaskStatus.COMPLETED,
            task_type=TaskType.CHAT,
            user_id="u",
            platform="cli",
            request_summary=f"task {i}",
            started_at=float(i),
        ))
    tasks = store.list_recent(limit=5)
    assert len(tasks) == 5
