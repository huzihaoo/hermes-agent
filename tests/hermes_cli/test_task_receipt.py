"""Tests for task receipt generation from events.jsonl."""

import json
from pathlib import Path

import pytest
from gateway.tasks.types import TaskReceipt, TaskStatus, TaskType


def test_generate_receipt_from_completed_task(tmp_path):
    """Receipt should summarize a completed task from events."""
    from hermes_cli.task_trace import generate_receipt

    events_file = tmp_path / "events.jsonl"
    events_file.write_text(
        json.dumps({"event": "task:start", "data": {"task_id": "t1", "user_id": "alice", "request_summary": "hello"}}) + "\n" +
        json.dumps({"event": "tool:call", "data": {"task_id": "t1", "tool": "read_file", "duration_s": 1.2}}) + "\n" +
        json.dumps({"event": "task:complete", "data": {"task_id": "t1", "total_tokens": 500, "api_calls": 1}}) + "\n"
    )

    receipt = generate_receipt(trace_file=events_file, task_id="t1")

    assert isinstance(receipt, TaskReceipt)
    assert receipt.task_id == "t1"
    assert receipt.status == TaskStatus.COMPLETED
    assert receipt.user_id == "alice"
    assert receipt.total_tokens == 500
    assert receipt.tool_calls == 1


def test_generate_receipt_from_failed_task(tmp_path):
    """Receipt should include error info for failed tasks."""
    from hermes_cli.task_trace import generate_receipt

    events_file = tmp_path / "events.jsonl"
    events_file.write_text(
        json.dumps({"event": "task:start", "data": {"task_id": "t2", "user_id": "bob"}}) + "\n" +
        json.dumps({"event": "task:failed", "data": {"task_id": "t2", "error_class": "api_error", "error_message": "boom"}}) + "\n"
    )

    receipt = generate_receipt(trace_file=events_file, task_id="t2")

    assert isinstance(receipt, TaskReceipt)
    assert receipt.task_id == "t2"
    assert receipt.status == TaskStatus.FAILED
    assert receipt.error_class == "api_error"
    assert receipt.error_message == "boom"
