"""Tests for Phase 3 template display enhancements."""

import time
from types import SimpleNamespace

import pytest

import gateway.run as gateway_run
from gateway.tasks.template import TemplateStore


@pytest.mark.asyncio
async def test_templates_command_shows_params(tmp_path, monkeypatch):
    """Templates list should show parameter names."""
    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    event = SimpleNamespace(source=source, get_command_args=lambda: "")

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    
    store = TemplateStore(db_path=tmp_path / "analytics" / "templates.db")
    store.create_from_task(
        source_task_id="t1",
        name="日报模板",
        task_type="docs",
        request_summary="写 {{date}} 的工作日报，重点是 {{project}}",
        created_at=time.time(),
    )

    result = await gateway_run.GatewayRunner._handle_templates_command(runner, event)
    assert "日报模板" in result
    assert "参数" in result
    assert "{{date}}" in result
    assert "{{project}}" in result
    assert "hermes cron create" in result


@pytest.mark.asyncio
async def test_templates_command_without_params(tmp_path, monkeypatch):
    """Templates without params should not show param section."""
    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    event = SimpleNamespace(source=source, get_command_args=lambda: "")

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    
    store = TemplateStore(db_path=tmp_path / "analytics" / "templates.db")
    store.create_from_task(
        source_task_id="t1",
        name="简单模板",
        task_type="docs",
        request_summary="写今天的工作日报",
        created_at=time.time(),
    )

    result = await gateway_run.GatewayRunner._handle_templates_command(runner, event)
    assert "简单模板" in result
    assert "参数" not in result


@pytest.mark.asyncio
async def test_template_create_shows_params_and_usage(tmp_path, monkeypatch):
    """Template create should show extracted params and usage example."""
    from gateway.tasks.types import TaskReceipt, TaskStatus, TaskType
    
    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    event = SimpleNamespace(source=source, get_command_args=lambda: "create t-123 日报模板")

    def _fake_receipt(*, trace_file, task_id):
        return TaskReceipt(
            task_id="t-123",
            status=TaskStatus.COMPLETED,
            task_type=TaskType.DOCS,
            user_id="alice",
            platform="feishu",
            request_summary="写 {{date}} 的工作日报",
            started_at=1000.0,
            completed_at=1010.0,
            total_tokens=500,
            tool_calls=0,
        )

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr("hermes_cli.task_trace.generate_receipt", _fake_receipt)

    result = await gateway_run.GatewayRunner._handle_template_command(runner, event)
    assert "模板已创建" in result
    assert "参数" in result
    assert "{{date}}" in result
    assert "使用示例" in result
    assert "hermes cron create" in result


@pytest.mark.asyncio
async def test_template_create_without_params_no_usage(tmp_path, monkeypatch):
    """Template create without params should not show usage example."""
    from gateway.tasks.types import TaskReceipt, TaskStatus, TaskType
    
    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    event = SimpleNamespace(source=source, get_command_args=lambda: "create t-123 简单模板")

    def _fake_receipt(*, trace_file, task_id):
        return TaskReceipt(
            task_id="t-123",
            status=TaskStatus.COMPLETED,
            task_type=TaskType.DOCS,
            user_id="alice",
            platform="feishu",
            request_summary="写今天的工作日报",
            started_at=1000.0,
            completed_at=1010.0,
            total_tokens=500,
            tool_calls=0,
        )

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr("hermes_cli.task_trace.generate_receipt", _fake_receipt)

    result = await gateway_run.GatewayRunner._handle_template_command(runner, event)
    assert "模板已创建" in result
    assert "参数" not in result
    assert "使用示例" not in result
