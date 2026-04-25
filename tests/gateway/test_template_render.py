"""Tests for template render preview command."""

import time
from types import SimpleNamespace

import pytest

import gateway.run as gateway_run
from gateway.tasks.template import TemplateStore


@pytest.mark.asyncio
async def test_template_render_with_all_params(tmp_path, monkeypatch):
    """Template render should show rendered result with provided params."""
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
    
    event = SimpleNamespace(
        source=source,
        get_command_args=lambda: f"render {template_id} date=2026-04-25 project=Hermes"
    )
    result = await gateway_run.GatewayRunner._handle_template_command(runner, event)
    
    assert "预览" in result or "渲染结果" in result
    assert "写 2026-04-25 的工作日报，重点是 Hermes" in result


@pytest.mark.asyncio
async def test_template_render_with_optional_params_uses_defaults(tmp_path, monkeypatch):
    """Template render should use defaults for optional params."""
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
    
    event = SimpleNamespace(
        source=source,
        get_command_args=lambda: f"render {template_id} date=2026-04-25"
    )
    result = await gateway_run.GatewayRunner._handle_template_command(runner, event)
    
    assert "写 2026-04-25 的日报，格式：markdown" in result


@pytest.mark.asyncio
async def test_template_render_missing_required_param_shows_error(tmp_path, monkeypatch):
    """Template render should show error for missing required params."""
    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    
    store = TemplateStore(db_path=tmp_path / "analytics" / "templates.db")
    template_id = store.create_from_task(
        source_task_id="t-123",
        name="日报模板",
        task_type="docs",
        request_summary="写 {{date}} 的工作日报",
        created_at=time.time(),
    )
    
    event = SimpleNamespace(
        source=source,
        get_command_args=lambda: f"render {template_id}"
    )
    result = await gateway_run.GatewayRunner._handle_template_command(runner, event)
    
    assert "缺少" in result or "missing" in result.lower()
    assert "date" in result


@pytest.mark.asyncio
async def test_template_render_with_short_id(tmp_path, monkeypatch):
    """Template render should work with short ID."""
    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    
    store = TemplateStore(db_path=tmp_path / "analytics" / "templates.db")
    template_id = store.create_from_task(
        source_task_id="t-123",
        name="测试模板",
        task_type="docs",
        request_summary="测试 {{value}}",
        created_at=time.time(),
    )
    
    short_id = template_id[:8]
    event = SimpleNamespace(
        source=source,
        get_command_args=lambda: f"render {short_id} value=123"
    )
    result = await gateway_run.GatewayRunner._handle_template_command(runner, event)
    
    assert "测试 123" in result


@pytest.mark.asyncio
async def test_template_render_nonexistent_returns_error(tmp_path, monkeypatch):
    """Template render with nonexistent ID should return error."""
    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    
    # Initialize empty store
    TemplateStore(db_path=tmp_path / "analytics" / "templates.db")
    
    event = SimpleNamespace(
        source=source,
        get_command_args=lambda: "render nonexistent value=123"
    )
    result = await gateway_run.GatewayRunner._handle_template_command(runner, event)
    
    assert "未找到" in result or "not found" in result.lower()
