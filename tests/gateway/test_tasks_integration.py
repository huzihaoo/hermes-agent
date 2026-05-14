"""Integration test: EventEmitter syncs task events to TaskStore."""

import json
from pathlib import Path

from hermes_events import EventEmitter
from gateway.tasks.store import TaskStore
from gateway.tasks.types import TaskStatus


def test_emitter_syncs_task_start_to_store(tmp_path):
    trace_file = tmp_path / "events.jsonl"
    store = TaskStore(db_path=tmp_path / "tasks.db")
    emitter = EventEmitter(trace_file=trace_file, task_store=store)

    emitter.emit("task:start", {
        "task_id": "t1",
        "platform": "feishu",
        "user_id": "alice",
        "request_summary": "写代码实现功能",
    })

    task = store.get("t1")
    assert task is not None
    assert task.status == TaskStatus.RUNNING
    assert task.user_id == "alice"
    assert task.task_type.value == "coding"  # inferred from "写代码"


def test_emitter_syncs_request_routing_fields_to_store(tmp_path):
    trace_file = tmp_path / "events.jsonl"
    store = TaskStore(db_path=tmp_path / "tasks.db")
    emitter = EventEmitter(trace_file=trace_file, task_store=store)

    emitter.emit("request:start", {
        "task_id": "feishu-topic-task",
        "platform": "feishu",
        "user_id": "ou_user",
        "chat_id": "oc_chat",
        "chat_type": "group",
        "thread_id": "topic:om_anchor",
        "message_id": "om_request",
        "request_summary": "持续推进",
    })

    task = store.get("feishu-topic-task")
    assert task is not None
    assert task.user_id == "ou_user"
    assert task.chat_id == "oc_chat"
    assert task.chat_type == "group"
    assert task.thread_id == "topic:om_anchor"
    assert task.message_id == "om_request"


def test_emitter_syncs_failure_error_fields_to_store(tmp_path):
    trace_file = tmp_path / "events.jsonl"
    store = TaskStore(db_path=tmp_path / "tasks.db")
    emitter = EventEmitter(trace_file=trace_file, task_store=store)

    emitter.emit("task:start", {"task_id": "t-error", "platform": "feishu", "user_id": "ou_user"})
    emitter.emit("task:failed", {
        "task_id": "t-error",
        "error_class": "FeishuTopicDeliveryError",
        "error_message": "topic reply rejected",
    })

    task = store.get("t-error")
    assert task is not None
    assert task.status == TaskStatus.FAILED
    assert task.error_class == "FeishuTopicDeliveryError"
    assert task.error_message == "topic reply rejected"


def test_emitter_syncs_task_complete_to_store(tmp_path):
    trace_file = tmp_path / "events.jsonl"
    store = TaskStore(db_path=tmp_path / "tasks.db")
    emitter = EventEmitter(trace_file=trace_file, task_store=store)

    emitter.emit("task:start", {"task_id": "t2", "platform": "cli", "user_id": "bob"})
    emitter.emit("task:complete", {"task_id": "t2", "total_tokens": 500})

    task = store.get("t2")
    assert task is not None
    assert task.status == TaskStatus.COMPLETED
    assert task.completed_at is not None


def test_emitter_marks_completion_delivery_verified_for_feishu_topic_with_message(tmp_path):
    trace_file = tmp_path / "events.jsonl"
    store = TaskStore(db_path=tmp_path / "tasks.db")
    emitter = EventEmitter(trace_file=trace_file, task_store=store)

    emitter.emit("request:start", {
        "task_id": "feishu-topic-task",
        "platform": "feishu",
        "user_id": "ou_user",
        "chat_id": "oc_chat",
        "chat_type": "group",
        "thread_id": "topic:om_anchor",
        "message_id": "om_request",
    })
    emitter.emit("task:complete", {
        "task_id": "feishu-topic-task",
        "message_id": "om_reply",
    })

    task = store.get("feishu-topic-task")
    assert task is not None
    assert task.status == TaskStatus.COMPLETED
    assert task.message_id == "om_reply"
    assert task.delivery_verified is True


def test_emitter_syncs_task_failed_to_store(tmp_path):
    trace_file = tmp_path / "events.jsonl"
    store = TaskStore(db_path=tmp_path / "tasks.db")
    emitter = EventEmitter(trace_file=trace_file, task_store=store)

    emitter.emit("task:start", {"task_id": "t3", "platform": "cli", "user_id": "carol"})
    emitter.emit("task:failed", {"task_id": "t3", "error_class": "timeout"})

    task = store.get("t3")
    assert task is not None
    assert task.status == TaskStatus.FAILED


def test_emitter_without_store_still_works(tmp_path):
    """EventEmitter should work fine without a TaskStore (backward compat)."""
    trace_file = tmp_path / "events.jsonl"
    emitter = EventEmitter(trace_file=trace_file)

    emitter.emit("task:start", {"task_id": "t4", "platform": "cli"})

    # JSONL should still be written
    lines = trace_file.read_text().strip().split("\n")
    assert len(lines) == 1
    event = json.loads(lines[0])
    assert event["event"] == "task:start"


def test_store_list_recent_after_sync(tmp_path):
    trace_file = tmp_path / "events.jsonl"
    store = TaskStore(db_path=tmp_path / "tasks.db")
    emitter = EventEmitter(trace_file=trace_file, task_store=store)

    emitter.emit("task:start", {"task_id": "t1", "platform": "cli", "user_id": "u1", "request_summary": "first"})
    emitter.emit("task:start", {"task_id": "t2", "platform": "cli", "user_id": "u1", "request_summary": "second"})
    emitter.emit("task:complete", {"task_id": "t1", "total_tokens": 100})

    tasks = store.list_recent(limit=10)
    assert len(tasks) == 2
    # t2 is more recent (emitted later)
    assert tasks[0].task_id == "t2"
    assert tasks[1].task_id == "t1"
    assert tasks[1].status == TaskStatus.COMPLETED
