"""Tests for task browsing from the event log."""

import json
import time
from pathlib import Path

from hermes_cli.task_trace import list_tasks, get_task_summary
from gateway.tasks.types import Task, TaskStatus, TaskType


def _write_events(path: Path, events):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for event in events:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")


def test_list_tasks_returns_recent_tasks(tmp_path):
    events_file = tmp_path / "events.jsonl"
    now = time.time()
    _write_events(events_file, [
        {"timestamp": now - 30, "event": "request:start", "data": {"task_id": "t2", "platform": "feishu", "user_id": "u-1", "request_summary": "second task"}},
        {"timestamp": now - 20, "event": "task:complete", "data": {"task_id": "t2", "total_tokens": 20, "api_calls": 1, "tool_calls": 1}},
        {"timestamp": now - 10, "event": "request:start", "data": {"task_id": "t1", "platform": "feishu", "user_id": "u-1", "request_summary": "first task"}},
        {"timestamp": now - 5, "event": "task:failed", "data": {"task_id": "t1", "error_class": "api_error", "error_message": "boom"}},
    ])

    tasks = list_tasks(trace_file=events_file, limit=10)
    assert len(tasks) == 2
    assert isinstance(tasks[0], Task)
    assert [t.task_id for t in tasks] == ["t1", "t2"]
    assert tasks[0].status == TaskStatus.FAILED
    assert tasks[1].status == TaskStatus.COMPLETED


def test_get_task_summary_returns_single_task_view(tmp_path):
    events_file = tmp_path / "events.jsonl"
    now = time.time()
    _write_events(events_file, [
        {"timestamp": now - 10, "event": "request:start", "data": {"task_id": "t1", "platform": "feishu", "user_id": "u-1", "request_summary": "first task"}},
        {"timestamp": now - 8, "event": "api:call", "data": {"task_id": "t1", "model": "claude-sonnet-4", "input_tokens": 10, "output_tokens": 5}},
        {"timestamp": now - 6, "event": "tool:call", "data": {"task_id": "t1", "tool_name": "read_file", "args_preview": "path=a.md"}},
        {"timestamp": now - 4, "event": "task:failed", "data": {"task_id": "t1", "error_class": "api_error", "error_message": "boom"}},
    ])

    summary = get_task_summary(trace_file=events_file, task_id="t1")
    assert summary["task_id"] == "t1"
    assert summary["status"] == "failed"
    assert summary["request_summary"] == "first task"
    assert summary["error_class"] == "api_error"
    assert summary["error_message"] == "boom"
