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
        chat_id="oc_chat",
        chat_type="group",
        thread_id="topic:om_anchor",
        message_id="om_request",
        error_class="",
        error_message="",
    )
    store.upsert(task)
    loaded = store.get("t1")
    assert loaded is not None
    assert loaded.task_id == "t1"
    assert loaded.status == TaskStatus.RUNNING
    assert loaded.task_type == TaskType.CODING
    assert loaded.user_id == "alice"
    assert loaded.chat_id == "oc_chat"
    assert loaded.chat_type == "group"
    assert loaded.thread_id == "topic:om_anchor"
    assert loaded.message_id == "om_request"


def test_existing_task_schema_migrates_new_columns(tmp_path):
    import sqlite3

    db_path = tmp_path / "legacy_tasks.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            CREATE TABLE tasks (
                task_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                task_type TEXT NOT NULL,
                user_id TEXT,
                platform TEXT,
                request_summary TEXT,
                started_at REAL NOT NULL,
                completed_at REAL,
                agent_route TEXT
            )
        """)
        conn.execute("""
            INSERT INTO tasks (
                task_id, status, task_type, user_id, platform,
                request_summary, started_at, completed_at, agent_route
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, ("legacy", "running", "chat", "u", "feishu", "old", 1.0, None, None))

    migrated = TaskStore(db_path=db_path)
    loaded = migrated.get("legacy")
    assert loaded is not None
    assert loaded.chat_id is None
    assert loaded.thread_id is None

    loaded.chat_id = "oc_chat"
    loaded.chat_type = "group"
    loaded.thread_id = "topic:om_anchor"
    loaded.message_id = "om_request"
    migrated.upsert(loaded)

    reloaded = migrated.get("legacy")
    assert reloaded.chat_id == "oc_chat"
    assert reloaded.chat_type == "group"
    assert reloaded.thread_id == "topic:om_anchor"
    assert reloaded.message_id == "om_request"


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
