"""Tests for template search and filtering."""

import time
from types import SimpleNamespace

import pytest

import gateway.run as gateway_run
from gateway.tasks.template import TemplateStore


@pytest.mark.asyncio
async def test_templates_filter_by_name(tmp_path, monkeypatch):
    """Templates command should support filtering by name."""
    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    
    store = TemplateStore(db_path=tmp_path / "analytics" / "templates.db")
    store.create_from_task(
        source_task_id="t1",
        name="日报模板",
        task_type="docs",
        request_summary="写日报",
        created_at=time.time(),
    )
    store.create_from_task(
        source_task_id="t2",
        name="周报模板",
        task_type="docs",
        request_summary="写周报",
        created_at=time.time(),
    )
    store.create_from_task(
        source_task_id="t3",
        name="代码审查",
        task_type="coding",
        request_summary="审查代码",
        created_at=time.time(),
    )
    
    event = SimpleNamespace(source=source, get_command_args=lambda: "日报")
    result = await gateway_run.GatewayRunner._handle_templates_command(runner, event)
    
    assert "日报模板" in result
    assert "周报模板" not in result
    assert "代码审查" not in result


@pytest.mark.asyncio
async def test_templates_filter_by_type(tmp_path, monkeypatch):
    """Templates command should support filtering by type."""
    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    
    store = TemplateStore(db_path=tmp_path / "analytics" / "templates.db")
    store.create_from_task(
        source_task_id="t1",
        name="日报模板",
        task_type="docs",
        request_summary="写日报",
        created_at=time.time(),
    )
    store.create_from_task(
        source_task_id="t2",
        name="代码审查",
        task_type="coding",
        request_summary="审查代码",
        created_at=time.time(),
    )
    
    event = SimpleNamespace(source=source, get_command_args=lambda: "--type coding")
    result = await gateway_run.GatewayRunner._handle_templates_command(runner, event)
    
    assert "代码审查" in result
    assert "日报模板" not in result


@pytest.mark.asyncio
async def test_templates_no_filter_shows_all(tmp_path, monkeypatch):
    """Templates command without filter should show all templates."""
    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    
    store = TemplateStore(db_path=tmp_path / "analytics" / "templates.db")
    store.create_from_task(
        source_task_id="t1",
        name="日报模板",
        task_type="docs",
        request_summary="写日报",
        created_at=time.time(),
    )
    store.create_from_task(
        source_task_id="t2",
        name="代码审查",
        task_type="coding",
        request_summary="审查代码",
        created_at=time.time(),
    )
    
    event = SimpleNamespace(source=source, get_command_args=lambda: "")
    result = await gateway_run.GatewayRunner._handle_templates_command(runner, event)
    
    assert "日报模板" in result
    assert "代码审查" in result
