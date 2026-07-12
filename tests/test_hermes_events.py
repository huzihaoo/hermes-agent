"""Tests for the structured task event trace."""

import json
import os
import time

import pytest

from hermes_events import EventEmitter, TaskEvent, cleanup_stale_pending, trace_task_events


@pytest.fixture()
def emitter(tmp_path):
    return EventEmitter(trace_file=tmp_path / "task_trace.jsonl")


class TestEventEmitter:
    def test_emit_writes_jsonl_line(self, emitter, tmp_path):
        trace_file = tmp_path / "task_trace.jsonl"
        assert emitter.emit("test_event", {"key": "value"}) is True
        event = json.loads(trace_file.read_text(encoding="utf-8").strip())
        assert event["event"] == "test_event"
        assert event["data"] == {"key": "value"}
        assert isinstance(event["timestamp"], (int, float))

    def test_emit_appends_multiple_events(self, emitter, tmp_path):
        assert emitter.emit("event1", {"a": 1}) is True
        assert emitter.emit("event2", {"b": 2}) is True
        lines = (tmp_path / "task_trace.jsonl").read_text(encoding="utf-8").splitlines()
        assert [json.loads(line)["event"] for line in lines] == ["event1", "event2"]

    def test_emit_handles_missing_directory(self, tmp_path):
        trace_file = tmp_path / "nested" / "dir" / "trace.jsonl"
        assert EventEmitter(trace_file=trace_file).emit("test", {}) is True
        assert trace_file.exists()

    def test_emit_returns_false_without_trace_file(self):
        assert EventEmitter(trace_file=None).emit("test", {"key": "value"}) is False

    def test_emit_returns_false_when_trace_path_is_directory(self, tmp_path):
        trace_file = tmp_path / "events.jsonl"
        trace_file.mkdir()
        assert EventEmitter(trace_file=trace_file).emit("test", {}) is False

    def test_task_store_failure_does_not_invalidate_jsonl_append(self, tmp_path):
        class BrokenStore:
            def upsert(self, task):
                raise RuntimeError("store unavailable")

        trace_file = tmp_path / "events.jsonl"
        emitter = EventEmitter(trace_file=trace_file, task_store=BrokenStore())
        assert emitter.emit("task:start", {"task_id": "task-1"}) is True
        assert trace_file.is_file()

    def test_mark_and_finalize_pending(self, tmp_path):
        emitter = EventEmitter(trace_file=tmp_path / "events.jsonl")
        emitter.mark_pending("task-1")
        pending_file = tmp_path / ".pending-task-1"
        assert pending_file.exists()
        emitter.finalize_pending("task-1")
        assert not pending_file.exists()

    def test_cleanup_stale_pending_emits_timeout(self, tmp_path):
        trace_file = tmp_path / "events.jsonl"
        emitter = EventEmitter(trace_file=trace_file)
        emitter.mark_pending("task-1")
        pending_file = tmp_path / ".pending-task-1"
        old = time.time() - 3600
        os.utime(pending_file, (old, old))
        cleanup_stale_pending(trace_file=trace_file, timeout_minutes=1)
        assert not pending_file.exists()
        event = trace_task_events(trace_file=trace_file, task_id="task-1")[-1]
        assert event["event"] == "task:timeout"

    def test_cleanup_keeps_fresh_pending_marker(self, tmp_path):
        trace_file = tmp_path / "events.jsonl"
        emitter = EventEmitter(trace_file=trace_file)
        emitter.mark_pending("task-fresh")
        cleanup_stale_pending(trace_file=trace_file, timeout_minutes=30)
        assert (tmp_path / ".pending-task-fresh").exists()
        assert trace_task_events(trace_file=trace_file, task_id="task-fresh") == []


class TestTaskEvent:
    def test_task_start_event(self):
        event = TaskEvent.task_start(task_id="test-123", platform="cli", user_id="user1")
        assert event["event"] == "task:start"
        assert event["data"]["task_id"] == "test-123"
        assert event["data"]["platform"] == "cli"
        assert event["data"]["user_id"] == "user1"

    def test_api_call_event(self):
        event = TaskEvent.api_call(
            task_id="test-123",
            model="claude-sonnet-4",
            provider="anthropic",
            input_tokens=100,
            output_tokens=50,
        )
        assert event["event"] == "api:call"
        assert event["data"]["model"] == "claude-sonnet-4"
        assert event["data"]["input_tokens"] == 100
        assert event["data"]["output_tokens"] == 50

    def test_tool_call_event(self):
        event = TaskEvent.tool_call(
            task_id="test-123",
            tool_name="read_file",
            args_preview="path=test.txt",
        )
        assert event["event"] == "tool:call"
        assert event["data"]["tool_name"] == "read_file"

    def test_task_complete_event(self):
        event = TaskEvent.task_complete(
            task_id="test-123",
            total_tokens=150,
            api_calls=3,
            tool_calls=5,
        )
        assert event["event"] == "task:complete"
        assert event["data"]["total_tokens"] == 150


class TestTraceTaskEvents:
    def test_trace_task_events_filters_and_orders(self, tmp_path):
        trace_file = tmp_path / "events.jsonl"
        emitter = EventEmitter(trace_file=trace_file)
        emitter.emit("task:start", {"task_id": "b"})
        emitter.emit("task:start", {"task_id": "a"})
        emitter.emit("tool:call", {"task_id": "a", "tool_name": "read_file"})
        traced = trace_task_events(trace_file=trace_file, task_id="a")
        assert [event["event"] for event in traced] == ["task:start", "tool:call"]
