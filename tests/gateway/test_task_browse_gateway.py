"""Tests for gateway task browsing commands."""

from types import SimpleNamespace

import pytest

import gateway.run as gateway_run
from gateway.tasks.types import Task, TaskReceipt, TaskStatus, TaskType


@pytest.mark.asyncio
async def test_gateway_tasks_command_uses_event_log(tmp_path, monkeypatch):
    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    event = SimpleNamespace(source=source, get_command_args=lambda: "")

    called = {}

    def _fake_list_tasks(*, trace_file, limit=10, user_id=None):
        called["trace_file"] = trace_file
        called["limit"] = limit
        called["user_id"] = user_id
        return [Task(
            task_id="t1",
            status=TaskStatus.COMPLETED,
            task_type=TaskType.CHAT,
            user_id="u-1",
            platform="feishu",
            request_summary="hello",
            started_at=1000.0,
            completed_at=1010.0,
        )]

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr("hermes_cli.task_trace.list_tasks", _fake_list_tasks)

    result = await gateway_run.GatewayRunner._handle_tasks_command(runner, event)
    assert "t1" in result
    assert called["user_id"] == "u-1"


@pytest.mark.asyncio
async def test_gateway_task_command_uses_event_log(tmp_path, monkeypatch):
    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    event = SimpleNamespace(source=source, get_command_args=lambda: "t1")

    called = {}

    def _fake_generate_receipt(*, trace_file, task_id):
        called["trace_file"] = trace_file
        called["task_id"] = task_id
        return TaskReceipt(
            task_id="t1",
            status=TaskStatus.FAILED,
            task_type=TaskType.CHAT,
            user_id="u-1",
            platform="feishu",
            request_summary="hello",
            started_at=1000.0,
            completed_at=1010.0,
            error_class="api_error",
            error_message="boom",
        )

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr("hermes_cli.task_trace.generate_receipt", _fake_generate_receipt)

    result = await gateway_run.GatewayRunner._handle_task_command(runner, event)
    assert "t1" in result
    assert "failed" in result.lower()
    assert called["task_id"] == "t1"
