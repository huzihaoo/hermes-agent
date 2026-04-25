"""Tests for template edit command."""

import time
from types import SimpleNamespace

import pytest

import gateway.run as gateway_run
from gateway.tasks.template import TemplateStore


@pytest.mark.asyncio
async def test_template_edit_updates_name(tmp_path, monkeypatch):
    """Template edit should update template name."""
    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    
    store = TemplateStore(db_path=tmp_path / "analytics" / "templates.db")
    template_id = store.create_from_task(
        source_task_id="t-123",
        name="旧名称",
        task_type="docs",
        request_summary="测试内容",
        created_at=time.time(),
    )
    
    event = SimpleNamespace(
        source=source,
        get_command_args=lambda: f"edit {template_id[:8]} --name 新名称"
    )
    result = await gateway_run.GatewayRunner._handle_template_command(runner, event)
    
    assert "已更新" in result or "updated" in result.lower()
    
    # Verify name changed
    updated = store.get(template_id)
    assert updated["name"] == "新名称"


@pytest.mark.asyncio
async def test_template_edit_updates_content(tmp_path, monkeypatch):
    """Template edit should update template content."""
    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    
    store = TemplateStore(db_path=tmp_path / "analytics" / "templates.db")
    template_id = store.create_from_task(
        source_task_id="t-123",
        name="测试模板",
        task_type="docs",
        request_summary="旧内容",
        created_at=time.time(),
    )
    
    event = SimpleNamespace(
        source=source,
        get_command_args=lambda: f"edit {template_id[:8]} --content 新内容 {{{{param}}}}"
    )
    result = await gateway_run.GatewayRunner._handle_template_command(runner, event)
    
    assert "已更新" in result or "updated" in result.lower()
    
    # Verify content changed and params extracted
    updated = store.get(template_id)
    assert updated["request_summary"] == "新内容 {{param}}"
    assert "param" in updated.get("params", {})


@pytest.mark.asyncio
async def test_template_edit_both_name_and_content(tmp_path, monkeypatch):
    """Template edit should update both name and content."""
    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    
    store = TemplateStore(db_path=tmp_path / "analytics" / "templates.db")
    template_id = store.create_from_task(
        source_task_id="t-123",
        name="旧名称",
        task_type="docs",
        request_summary="旧内容",
        created_at=time.time(),
    )
    
    event = SimpleNamespace(
        source=source,
        get_command_args=lambda: f"edit {template_id[:8]} --name 新名称 --content 新内容"
    )
    result = await gateway_run.GatewayRunner._handle_template_command(runner, event)
    
    assert "已更新" in result or "updated" in result.lower()
    
    # Verify both changed
    updated = store.get(template_id)
    assert updated["name"] == "新名称"
    assert updated["request_summary"] == "新内容"


@pytest.mark.asyncio
async def test_template_edit_nonexistent_returns_error(tmp_path, monkeypatch):
    """Template edit with nonexistent ID should return error."""
    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    
    # Initialize empty store
    TemplateStore(db_path=tmp_path / "analytics" / "templates.db")
    
    event = SimpleNamespace(
        source=source,
        get_command_args=lambda: "edit nonexistent --name 新名称"
    )
    result = await gateway_run.GatewayRunner._handle_template_command(runner, event)
    
    assert "未找到" in result or "not found" in result.lower()
