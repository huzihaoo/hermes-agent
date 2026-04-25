"""Tests for template delete command."""

import time
from types import SimpleNamespace

import pytest

import gateway.run as gateway_run
from gateway.tasks.template import TemplateStore


@pytest.mark.asyncio
async def test_template_delete_removes_template(tmp_path, monkeypatch):
    """Template delete should remove the template."""
    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    
    store = TemplateStore(db_path=tmp_path / "analytics" / "templates.db")
    template_id = store.create_from_task(
        source_task_id="t-123",
        name="测试模板",
        task_type="docs",
        request_summary="测试内容",
        created_at=time.time(),
    )
    
    # Verify template exists
    assert store.get(template_id) is not None
    
    # Delete template
    event = SimpleNamespace(
        source=source,
        get_command_args=lambda: f"delete {template_id}"
    )
    result = await gateway_run.GatewayRunner._handle_template_command(runner, event)
    
    assert "已删除" in result or "deleted" in result.lower()
    
    # Verify template is gone
    assert store.get(template_id) is None


@pytest.mark.asyncio
async def test_template_delete_with_short_id(tmp_path, monkeypatch):
    """Template delete should work with short ID."""
    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    
    store = TemplateStore(db_path=tmp_path / "analytics" / "templates.db")
    template_id = store.create_from_task(
        source_task_id="t-123",
        name="测试模板",
        task_type="docs",
        request_summary="测试内容",
        created_at=time.time(),
    )
    
    short_id = template_id[:8]
    event = SimpleNamespace(
        source=source,
        get_command_args=lambda: f"delete {short_id}"
    )
    result = await gateway_run.GatewayRunner._handle_template_command(runner, event)
    
    assert "已删除" in result or "deleted" in result.lower()
    assert store.get(template_id) is None


@pytest.mark.asyncio
async def test_template_delete_nonexistent_returns_error(tmp_path, monkeypatch):
    """Template delete with nonexistent ID should return error."""
    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    
    # Initialize empty store
    TemplateStore(db_path=tmp_path / "analytics" / "templates.db")
    
    event = SimpleNamespace(
        source=source,
        get_command_args=lambda: "delete nonexistent"
    )
    result = await gateway_run.GatewayRunner._handle_template_command(runner, event)
    
    assert "未找到" in result or "not found" in result.lower()
