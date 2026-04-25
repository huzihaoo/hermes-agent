"""Tests for hermes_events.py — structured event emission for task tracing."""

import json
import time
import pytest
from pathlib import Path

from hermes_events import EventEmitter, TaskEvent, cleanup_stale_pending, trace_task_events


@pytest.fixture()
def emitter(tmp_path):
    """Create an EventEmitter with a temp trace file."""
    trace_file = tmp_path / "task_trace.jsonl"
    return EventEmitter(trace_file=trace_file)


class TestEventEmitter:
    def test_emit_writes_jsonl_line(self, emitter, tmp_path):
        trace_file = tmp_path / "task_trace.jsonl"
        emitter.emit("test_event", {"key": "value"})
        assert trace_file.exists()
        lines = trace_file.read_text().strip().split("\n")
        assert len(lines) == 1
        event = json.loads(lines[0])
        assert event["event"] == "test_event"
        assert event["data"] == {"key": "value"}
        assert "timestamp" in event
        assert isinstance(event["timestamp"], (int, float))

    def test_emit_appends_multiple_events(self, emitter, tmp_path):
        trace_file = tmp_path / "task_trace.jsonl"
        emitter.emit("event1", {"a": 1})
        emitter.emit("event2", {"b": 2})
        lines = trace_file.read_text().strip().split("\n")
        assert len(lines) == 2
        e1 = json.loads(lines[0])
        e2 = json.loads(lines[1])
        assert e1["event"] == "event1"
        assert e2["event"] == "event2"

    def test_emit_handles_missing_directory(self, tmp_path):
        trace_file = tmp_path / "nested" / "dir" / "trace.jsonl"
        emitter = EventEmitter(trace_file=trace_file)
        emitter.emit("test", {})
        assert trace_file.exists()

    def test_emit_disabled_when_no_trace_file(self):
        emitter = EventEmitter(trace_file=None)
        emitter.emit("test", {"key": "value"})

    def test_mark_and_finalize_pending(self, tmp_path):
        trace_file = tmp_path / "events.jsonl"
        emitter = EventEmitter(trace_file=trace_file)
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
        import os
        os.utime(pending_file, (old, old))
        cleanup_stale_pending(trace_file=trace_file, timeout_minutes=1)
        assert not pending_file.exists()
        lines = trace_file.read_text().strip().split("\n")
        event = json.loads(lines[-1])
        assert event["event"] == "task:timeout"
        assert event["data"]["task_id"] == "task-1"


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
        assert event["data"]["task_id"] == "test-123"
        assert event["data"]["model"] == "claude-sonnet-4"
        assert event["data"]["provider"] == "anthropic"
        assert event["data"]["input_tokens"] == 100
        assert event["data"]["output_tokens"] == 50

    def test_tool_call_event(self):
        event = TaskEvent.tool_call(task_id="test-123", tool_name="read_file", args_preview="path=test.txt")
        assert event["event"] == "tool:call"
        assert event["data"]["task_id"] == "test-123"
        assert event["data"]["tool_name"] == "read_file"
        assert event["data"]["args_preview"] == "path=test.txt"

    def test_task_complete_event(self):
        event = TaskEvent.task_complete(task_id="test-123", total_tokens=150, api_calls=3, tool_calls=5)
        assert event["event"] == "task:complete"
        assert event["data"]["task_id"] == "test-123"
        assert event["data"]["total_tokens"] == 150
        assert event["data"]["api_calls"] == 3
        assert event["data"]["tool_calls"] == 5


class TestTraceTaskEvents:
    def test_trace_task_events_filters_and_orders(self, tmp_path):
        trace_file = tmp_path / "events.jsonl"
        emitter = EventEmitter(trace_file=trace_file)
        emitter.emit("task:start", {"task_id": "b"})
        emitter.emit("task:start", {"task_id": "a"})
        emitter.emit("tool:call", {"task_id": "a", "tool_name": "read_file"})
        traced = trace_task_events(trace_file=trace_file, task_id="a")
        assert len(traced) == 2
        assert [e["data"]["task_id"] for e in traced] == ["a", "a"]
        assert traced[0]["event"] == "task:start"
        assert traced[1]["event"] == "tool:call"
