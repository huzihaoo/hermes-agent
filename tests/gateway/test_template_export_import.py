"""Tests for template export and import."""

import json
import time
from types import SimpleNamespace

import pytest

import gateway.run as gateway_run
from gateway.tasks.template import TemplateStore


@pytest.mark.asyncio
async def test_template_export_returns_json(tmp_path, monkeypatch):
    """Template export should return JSON representation."""
    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    
    store = TemplateStore(db_path=tmp_path / "analytics" / "templates.db")
    template_id = store.create_from_task(
        source_task_id="t-123",
        name="测试模板",
        task_type="docs",
        request_summary="写 {{date}} 的日报",
        created_at=time.time(),
    )
    
    event = SimpleNamespace(
        source=source,
        get_command_args=lambda: f"export {template_id[:8]}"
    )
    result = await gateway_run.GatewayRunner._handle_template_command(runner, event)
    
    # Should contain JSON
    assert "{" in result and "}" in result
    
    # Parse and verify structure
    data = json.loads(result)
    assert data["name"] == "测试模板"
    assert data["task_type"] == "docs"
    assert data["request_summary"] == "写 {{date}} 的日报"
    assert "date" in data["params"]


@pytest.mark.asyncio
async def test_template_import_creates_template(tmp_path, monkeypatch):
    """Template import should create a new template from JSON."""
    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    
    # Initialize store
    store = TemplateStore(db_path=tmp_path / "analytics" / "templates.db")
    
    template_json = json.dumps({
        "name": "导入的模板",
        "task_type": "docs",
        "request_summary": "写 {{date}} 的周报",
        "params": {"date": {"type": "string", "required": True}}
    })
    
    event = SimpleNamespace(
        source=source,
        get_command_args=lambda: f"import {template_json}"
    )
    result = await gateway_run.GatewayRunner._handle_template_command(runner, event)
    
    assert "已导入" in result or "imported" in result.lower()
    
    # Verify template was created
    templates = store.list_recent(limit=10)
    assert len(templates) == 1
    assert templates[0]["name"] == "导入的模板"
    assert templates[0]["request_summary"] == "写 {{date}} 的周报"


@pytest.mark.asyncio
async def test_template_import_invalid_json_returns_error(tmp_path, monkeypatch):
    """Template import with invalid JSON should return error."""
    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    
    # Initialize store
    TemplateStore(db_path=tmp_path / "analytics" / "templates.db")
    
    event = SimpleNamespace(
        source=source,
        get_command_args=lambda: "import {invalid json}"
    )
    result = await gateway_run.GatewayRunner._handle_template_command(runner, event)
    
    assert "无效" in result or "invalid" in result.lower()


@pytest.mark.asyncio
async def test_template_export_nonexistent_returns_error(tmp_path, monkeypatch):
    """Template export with nonexistent ID should return error."""
    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    
    # Initialize empty store
    TemplateStore(db_path=tmp_path / "analytics" / "templates.db")
    
    event = SimpleNamespace(
        source=source,
        get_command_args=lambda: "export nonexistent"
    )
    result = await gateway_run.GatewayRunner._handle_template_command(runner, event)
    
    assert "未找到" in result or "not found" in result.lower()
