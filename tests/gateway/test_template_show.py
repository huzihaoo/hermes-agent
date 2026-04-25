"""Tests for /template show command."""

import time
from types import SimpleNamespace

import pytest

import gateway.run as gateway_run
from gateway.tasks.template import TemplateStore


@pytest.mark.asyncio
async def test_template_show_displays_full_details(tmp_path, monkeypatch):
    """Template show should display all template details including params."""
    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    
    store = TemplateStore(db_path=tmp_path / "analytics" / "templates.db")
    template_id = store.create_from_task(
        source_task_id="t-123",
        name="日报模板",
        task_type="docs",
        request_summary="写 {{date}} 的工作日报，重点是 {{project}}",
        created_at=time.time(),
    )
    
    event = SimpleNamespace(source=source, get_command_args=lambda: f"show {template_id}")
    result = await gateway_run.GatewayRunner._handle_template_command(runner, event)
    
    assert "日报模板" in result
    assert template_id in result
    assert "docs" in result
    assert "{{date}}" in result
    assert "{{project}}" in result
    assert "写 {{date}} 的工作日报" in result


@pytest.mark.asyncio
async def test_template_show_with_short_id(tmp_path, monkeypatch):
    """Template show should work with short ID (first 8 chars)."""
    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    
    store = TemplateStore(db_path=tmp_path / "analytics" / "templates.db")
    template_id = store.create_from_task(
        source_task_id="t-123",
        name="测试模板",
        task_type="chat",
        request_summary="测试内容",
        created_at=time.time(),
    )
    
    short_id = template_id[:8]
    event = SimpleNamespace(source=source, get_command_args=lambda: f"show {short_id}")
    result = await gateway_run.GatewayRunner._handle_template_command(runner, event)
    
    assert "测试模板" in result
    assert template_id in result


@pytest.mark.asyncio
async def test_template_show_nonexistent_returns_error(tmp_path, monkeypatch):
    """Template show with nonexistent ID should return error."""
    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    
    # Initialize empty store
    TemplateStore(db_path=tmp_path / "analytics" / "templates.db")
    
    event = SimpleNamespace(source=source, get_command_args=lambda: "show nonexistent")
    result = await gateway_run.GatewayRunner._handle_template_command(runner, event)
    
    assert "未找到" in result or "not found" in result.lower()


@pytest.mark.asyncio
async def test_template_show_without_params(tmp_path, monkeypatch):
    """Template show should work for templates without parameters."""
    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    
    store = TemplateStore(db_path=tmp_path / "analytics" / "templates.db")
    template_id = store.create_from_task(
        source_task_id="t-123",
        name="简单模板",
        task_type="docs",
        request_summary="写今天的工作日报",
        created_at=time.time(),
    )
    
    event = SimpleNamespace(source=source, get_command_args=lambda: f"show {template_id}")
    result = await gateway_run.GatewayRunner._handle_template_command(runner, event)
    
    assert "简单模板" in result
    assert "写今天的工作日报" in result
    assert "参数" not in result


@pytest.mark.asyncio
async def test_template_show_displays_default_values(tmp_path, monkeypatch):
    """Template show should display default values for optional params."""
    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    
    store = TemplateStore(db_path=tmp_path / "analytics" / "templates.db")
    template_id = store.create_from_task(
        source_task_id="t-123",
        name="灵活模板",
        task_type="docs",
        request_summary="写 {{date}} 的日报，格式：{{format}}",
        created_at=time.time(),
        params={
            "date": {"type": "string", "required": True},
            "format": {"type": "string", "required": False, "default": "markdown"},
        },
    )
    
    event = SimpleNamespace(source=source, get_command_args=lambda: f"show {template_id}")
    result = await gateway_run.GatewayRunner._handle_template_command(runner, event)
    
    assert "{{date}}" in result
    assert "{{format}}" in result
    assert "必填" in result
    assert "可选" in result
    assert "markdown" in result
