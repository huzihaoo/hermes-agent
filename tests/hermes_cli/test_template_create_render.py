"""Tests for hermes template create and render CLI commands."""

import json
import time

import pytest

from gateway.tasks.template import TemplateStore
from gateway.tasks.types import TaskStatus, TaskType
from hermes_cli.template import template_create, template_render


@pytest.fixture()
def tmp_template_dir(tmp_path, monkeypatch):
    monkeypatch.setattr("hermes_constants.get_hermes_home", lambda: tmp_path)
    return tmp_path


def _write_task_event(hermes_home, task_id, status, task_type, request_summary):
    """Helper to write a task event to events.jsonl."""
    events_file = hermes_home / "analytics" / "events.jsonl"
    events_file.parent.mkdir(parents=True, exist_ok=True)
    
    event = {
        "event": "task:start",
        "timestamp": time.time(),
        "data": {
            "task_id": task_id,
            "task_type": task_type,
            "request_summary": request_summary,
        },
    }
    with open(events_file, "a") as f:
        f.write(json.dumps(event) + "\n")
    
    if status == TaskStatus.COMPLETED:
        event = {
            "event": "task:complete",
            "timestamp": time.time(),
            "data": {"task_id": task_id},
        }
        with open(events_file, "a") as f:
            f.write(json.dumps(event) + "\n")
    elif status == TaskStatus.FAILED:
        event = {
            "event": "task:failed",
            "timestamp": time.time(),
            "data": {"task_id": task_id, "error": "test error"},
        }
        with open(events_file, "a") as f:
            f.write(json.dumps(event) + "\n")


def test_template_create_from_successful_task(tmp_template_dir, capsys):
    """Template create should create template from successful task."""
    _write_task_event(
        tmp_template_dir,
        "test-task-123",
        TaskStatus.COMPLETED,
        "docs",
        "写 {{date}} 的日报"
    )
    
    # Create template
    template_create("test-task-123", "日报模板", hermes_home=tmp_template_dir)
    out = capsys.readouterr().out
    
    assert "已创建" in out or "created" in out.lower()
    assert "日报模板" in out
    assert "{{date}}" in out
    
    # Verify template exists
    store = TemplateStore(db_path=tmp_template_dir / "analytics" / "templates.db")
    templates = store.list_recent(limit=10)
    assert len(templates) == 1
    assert templates[0]["name"] == "日报模板"


def test_template_create_from_failed_task_rejected(tmp_template_dir, capsys):
    """Template create should reject failed tasks."""
    _write_task_event(
        tmp_template_dir,
        "failed-task",
        TaskStatus.FAILED,
        "docs",
        "测试"
    )
    
    template_create("failed-task", "测试模板", hermes_home=tmp_template_dir)
    out = capsys.readouterr().out
    
    assert "失败" in out or "failed" in out.lower()


def test_template_create_nonexistent_task(tmp_template_dir, capsys):
    """Template create should reject nonexistent task."""
    template_create("nonexistent", "测试模板", hermes_home=tmp_template_dir)
    out = capsys.readouterr().out
    
    assert "未找到" in out or "not found" in out.lower()


def test_template_render_with_params(tmp_template_dir, capsys):
    """Template render should show rendered result."""
    store = TemplateStore(db_path=tmp_template_dir / "analytics" / "templates.db")
    template_id = store.create_from_task(
        source_task_id="t1",
        name="日报模板",
        task_type="docs",
        request_summary="写 {{date}} 的日报，重点是 {{project}}",
        created_at=time.time(),
    )
    
    template_render(template_id[:8], {"date": "2026-04-25", "project": "Hermes"}, hermes_home=tmp_template_dir)
    out = capsys.readouterr().out
    
    assert "写 2026-04-25 的日报，重点是 Hermes" in out


def test_template_render_missing_required_param(tmp_template_dir, capsys):
    """Template render should show error for missing params."""
    store = TemplateStore(db_path=tmp_template_dir / "analytics" / "templates.db")
    template_id = store.create_from_task(
        source_task_id="t1",
        name="日报模板",
        task_type="docs",
        request_summary="写 {{date}} 的日报",
        created_at=time.time(),
    )
    
    template_render(template_id[:8], {}, hermes_home=tmp_template_dir)
    out = capsys.readouterr().out
    
    assert "缺少" in out or "missing" in out.lower()
    assert "date" in out
