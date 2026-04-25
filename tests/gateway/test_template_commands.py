"""Tests for gateway template commands."""

from types import SimpleNamespace

import pytest

import gateway.run as gateway_run
from gateway.tasks.types import TaskReceipt, TaskStatus, TaskType


@pytest.mark.asyncio
async def test_templates_command_empty(tmp_path, monkeypatch):
    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    event = SimpleNamespace(source=source, get_command_args=lambda: "")

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)

    result = await gateway_run.GatewayRunner._handle_templates_command(runner, event)
    assert "暂无模板" in result


@pytest.mark.asyncio
async def test_template_create_success(tmp_path, monkeypatch):
    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    event = SimpleNamespace(source=source, get_command_args=lambda: "create t1 我的模板")

    def _fake_receipt(*, trace_file, task_id):
        return TaskReceipt(
            task_id="t1",
            status=TaskStatus.COMPLETED,
            task_type=TaskType.CODING,
            user_id="alice",
            platform="feishu",
            request_summary="写代码",
            started_at=1000.0,
            completed_at=1010.0,
            total_tokens=500,
            tool_calls=2,
        )

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr("hermes_cli.task_trace.generate_receipt", _fake_receipt)

    result = await gateway_run.GatewayRunner._handle_template_command(runner, event)
    assert "模板已创建" in result
    assert "我的模板" in result
    assert "coding" in result


@pytest.mark.asyncio
async def test_template_create_failed_task_rejected(tmp_path, monkeypatch):
    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    event = SimpleNamespace(source=source, get_command_args=lambda: "create t2 失败模板")

    def _fake_receipt(*, trace_file, task_id):
        return TaskReceipt(
            task_id="t2",
            status=TaskStatus.FAILED,
            task_type=TaskType.CHAT,
            user_id="bob",
            platform="cli",
            request_summary="test",
            started_at=1000.0,
            completed_at=1010.0,
            error_class="timeout",
            error_message="boom",
        )

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr("hermes_cli.task_trace.generate_receipt", _fake_receipt)

    result = await gateway_run.GatewayRunner._handle_template_command(runner, event)
    assert "只能从成功任务" in result


@pytest.mark.asyncio
async def test_template_create_not_found(tmp_path, monkeypatch):
    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    event = SimpleNamespace(source=source, get_command_args=lambda: "create nope 不存在")

    def _fake_receipt(*, trace_file, task_id):
        return TaskReceipt(
            task_id="nope",
            status=TaskStatus.PENDING,
            task_type=TaskType.UNKNOWN,
            user_id=None,
            platform=None,
            request_summary=None,
            started_at=0,
            completed_at=None,
        )

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr("hermes_cli.task_trace.generate_receipt", _fake_receipt)

    result = await gateway_run.GatewayRunner._handle_template_command(runner, event)
    assert "未找到" in result
