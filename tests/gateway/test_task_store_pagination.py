"""Tests for TaskStore pagination and count."""

import time
import pytest
from pathlib import Path

from gateway.tasks.store import TaskStore
from gateway.tasks.types import Task, TaskStatus, TaskType


@pytest.fixture
def store(tmp_path):
    """Create a TaskStore with 25 test tasks."""
    db_path = tmp_path / "tasks.db"
    s = TaskStore(db_path=db_path)
    for i in range(25):
        s.upsert(Task(
            task_id=f"t{i:03d}",
            status=TaskStatus.COMPLETED if i % 3 == 0 else TaskStatus.FAILED if i % 3 == 1 else TaskStatus.RUNNING,
            task_type=TaskType.CODING if i % 2 == 0 else TaskType.CHAT,
            user_id="alice" if i < 15 else "bob",
            platform="feishu",
            request_summary=f"Task {i}",
            started_at=1000.0 + i,
        ))
    return s


def test_pagination_first_page(store):
    tasks = store.list_recent(limit=10, offset=0)
    assert len(tasks) == 10
    # Most recent first
    assert tasks[0].task_id == "t024"


def test_pagination_second_page(store):
    tasks = store.list_recent(limit=10, offset=10)
    assert len(tasks) == 10
    assert tasks[0].task_id == "t014"


def test_pagination_last_page(store):
    tasks = store.list_recent(limit=10, offset=20)
    assert len(tasks) == 5


def test_pagination_beyond_end(store):
    tasks = store.list_recent(limit=10, offset=100)
    assert len(tasks) == 0


def test_count_all(store):
    assert store.count_tasks() == 25


def test_count_by_user(store):
    assert store.count_tasks(user_id="alice") == 15
    assert store.count_tasks(user_id="bob") == 10


def test_count_by_status(store):
    completed = store.count_tasks(status=TaskStatus.COMPLETED)
    failed = store.count_tasks(status=TaskStatus.FAILED)
    running = store.count_tasks(status=TaskStatus.RUNNING)
    assert completed + failed + running == 25


def test_pagination_with_user_filter(store):
    tasks = store.list_recent(limit=5, offset=0, user_id="alice")
    assert len(tasks) == 5
    assert all(t.user_id == "alice" for t in tasks)


def test_pagination_with_status_filter(store):
    tasks = store.list_recent(limit=100, status=TaskStatus.COMPLETED)
    count = store.count_tasks(status=TaskStatus.COMPLETED)
    assert len(tasks) == count
    assert all(t.status == TaskStatus.COMPLETED for t in tasks)
