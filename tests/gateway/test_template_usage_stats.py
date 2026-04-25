"""Tests for template usage statistics."""

import time
from types import SimpleNamespace

import pytest

import gateway.run as gateway_run
from gateway.tasks.template import TemplateStore


@pytest.mark.asyncio
async def test_template_render_increments_usage_count(tmp_path, monkeypatch):
    """Rendering a template should increment its usage count."""
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
    
    # Initial usage should be 0
    template = store.get(template_id)
    assert template.get("usage_count", 0) == 0
    assert template.get("last_used_at") is None
    
    # Render template
    event = SimpleNamespace(
        source=source,
        get_command_args=lambda: f"render {template_id[:8]} date=2026-04-25"
    )
    await gateway_run.GatewayRunner._handle_template_command(runner, event)
    
    # Usage count should be incremented
    template = store.get(template_id)
    assert template.get("usage_count", 0) == 1
    assert template.get("last_used_at") is not None


@pytest.mark.asyncio
async def test_template_list_shows_usage_stats(tmp_path, monkeypatch):
    """Template list should show usage statistics."""
    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    
    store = TemplateStore(db_path=tmp_path / "analytics" / "templates.db")
    template_id = store.create_from_task(
        source_task_id="t-123",
        name="常用模板",
        task_type="docs",
        request_summary="写 {{date}} 的日报",
        created_at=time.time(),
    )
    
    # Record usage
    store.record_usage(template_id)
    store.record_usage(template_id)
    store.record_usage(template_id)
    
    # List templates
    event = SimpleNamespace(
        source=source,
        get_command_args=lambda: ""
    )
    result = await gateway_run.GatewayRunner._handle_templates_command(runner, event)
    
    # Should show usage count
    assert "使用 3 次" in result or "3 次" in result


def test_template_store_record_usage(tmp_path):
    """TemplateStore.record_usage should update usage stats."""
    store = TemplateStore(db_path=tmp_path / "analytics" / "templates.db")
    template_id = store.create_from_task(
        source_task_id="t-123",
        name="测试模板",
        task_type="docs",
        request_summary="写 {{date}} 的日报",
        created_at=time.time(),
    )
    
    # Record usage multiple times
    before = time.time()
    store.record_usage(template_id)
    time.sleep(0.01)
    store.record_usage(template_id)
    after = time.time()
    
    # Verify stats
    template = store.get(template_id)
    assert template["usage_count"] == 2
    assert before <= template["last_used_at"] <= after


def test_template_list_sorted_by_usage(tmp_path):
    """Templates should be sortable by usage count."""
    store = TemplateStore(db_path=tmp_path / "analytics" / "templates.db")
    
    # Create templates with different usage
    t1 = store.create_from_task(
        source_task_id="task1",
        name="模板1",
        task_type="docs",
        request_summary="内容1",
        created_at=time.time(),
    )
    t2 = store.create_from_task(
        source_task_id="task2",
        name="模板2",
        task_type="docs",
        request_summary="内容2",
        created_at=time.time(),
    )
    t3 = store.create_from_task(
        source_task_id="task3",
        name="模板3",
        task_type="docs",
        request_summary="内容3",
        created_at=time.time(),
    )
    
    # Record different usage counts
    store.record_usage(t1)
    store.record_usage(t2)
    store.record_usage(t2)
    store.record_usage(t3)
    store.record_usage(t3)
    store.record_usage(t3)
    
    # List by usage
    templates = store.list_recent(limit=10, sort_by="usage")
    
    # Should be sorted by usage count (descending)
    assert templates[0]["template_id"] == t3  # 3 uses
    assert templates[1]["template_id"] == t2  # 2 uses
    assert templates[2]["template_id"] == t1  # 1 use
