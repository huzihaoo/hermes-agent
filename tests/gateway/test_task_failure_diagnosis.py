"""Tests for task failure diagnosis in task detail view."""

from types import SimpleNamespace

import pytest

import gateway.run as gateway_run
from gateway.tasks.types import TaskReceipt, TaskStatus, TaskType


@pytest.mark.asyncio
async def test_task_command_shows_failure_diagnosis_for_api_error(tmp_path, monkeypatch):
    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    event = SimpleNamespace(source=source, get_command_args=lambda: "t-fail-1")

    def _fake_receipt(*, trace_file, task_id):
        return TaskReceipt(
            task_id="t-fail-1",
            status=TaskStatus.FAILED,
            task_type=TaskType.CHAT,
            user_id="alice",
            platform="feishu",
            request_summary="问一个问题",
            started_at=1000.0,
            completed_at=1010.0,
            total_tokens=500,
            tool_calls=0,
            error_class="api_error",
            error_message="OpenAI API returned 500 Internal server error",
        )

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr("hermes_cli.task_trace.generate_receipt", _fake_receipt)

    result = await gateway_run.GatewayRunner._handle_task_command(runner, event)
    assert "错误类型" in result
    assert "诊断" in result
    assert "上游模型/API" in result


@pytest.mark.asyncio
async def test_task_command_shows_failure_diagnosis_for_context_overflow(tmp_path, monkeypatch):
    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    event = SimpleNamespace(source=source, get_command_args=lambda: "t-fail-2")

    def _fake_receipt(*, trace_file, task_id):
        return TaskReceipt(
            task_id="t-fail-2",
            status=TaskStatus.FAILED,
            task_type=TaskType.RESEARCH,
            user_id="alice",
            platform="feishu",
            request_summary="长上下文任务",
            started_at=1000.0,
            completed_at=1010.0,
            total_tokens=50000,
            tool_calls=1,
            error_class="context_overflow",
            error_message="Context length exceeded the model limit",
        )

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr("hermes_cli.task_trace.generate_receipt", _fake_receipt)

    result = await gateway_run.GatewayRunner._handle_task_command(runner, event)
    assert "诊断" in result
    assert "上下文过长" in result


@pytest.mark.asyncio
async def test_task_command_shows_failure_diagnosis_for_tool_failure(tmp_path, monkeypatch):
    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    event = SimpleNamespace(source=source, get_command_args=lambda: "t-fail-3")

    def _fake_receipt(*, trace_file, task_id):
        return TaskReceipt(
            task_id="t-fail-3",
            status=TaskStatus.FAILED,
            task_type=TaskType.CODING,
            user_id="alice",
            platform="feishu",
            request_summary="读文件后分析",
            started_at=1000.0,
            completed_at=1010.0,
            total_tokens=500,
            tool_calls=2,
            tool_call_details=[{"tool_name": "read_file"}, {"tool_name": "search_files"}],
            error_class="tool_error",
            error_message="read_file: file not found",
        )

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr("hermes_cli.task_trace.generate_receipt", _fake_receipt)

    result = await gateway_run.GatewayRunner._handle_task_command(runner, event)
    assert "诊断" in result
    assert "工具调用失败" in result
    assert "read_file" in result
