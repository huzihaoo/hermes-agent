"""Tests for hermes template CLI commands."""

import time

import pytest

from gateway.tasks.template import TemplateStore
from hermes_cli.template import template_list, template_show, template_delete


@pytest.fixture()
def tmp_template_dir(tmp_path, monkeypatch):
    monkeypatch.setattr("hermes_constants.get_hermes_home", lambda: tmp_path)
    return tmp_path


def test_template_list_empty(tmp_template_dir, capsys):
    """Template list should show empty message when no templates."""
    template_list(hermes_home=tmp_template_dir)
    out = capsys.readouterr().out
    assert "暂无" in out and "模板" in out


def test_template_list_shows_templates(tmp_template_dir, capsys):
    """Template list should display all templates."""
    store = TemplateStore(db_path=tmp_template_dir / "analytics" / "templates.db")
    store.create_from_task(
        source_task_id="t1",
        name="日报模板",
        task_type="docs",
        request_summary="写 {{date}} 的日报",
        created_at=time.time(),
    )
    store.create_from_task(
        source_task_id="t2",
        name="代码审查",
        task_type="coding",
        request_summary="审查代码",
        created_at=time.time(),
    )
    
    template_list(hermes_home=tmp_template_dir)
    out = capsys.readouterr().out
    
    assert "日报模板" in out
    assert "代码审查" in out
    assert "{{date}}" in out


def test_template_show_displays_details(tmp_template_dir, capsys):
    """Template show should display template details."""
    store = TemplateStore(db_path=tmp_template_dir / "analytics" / "templates.db")
    template_id = store.create_from_task(
        source_task_id="t1",
        name="日报模板",
        task_type="docs",
        request_summary="写 {{date}} 的日报，格式：{{format}}",
        created_at=time.time(),
        params={
            "date": {"type": "string", "required": True},
            "format": {"type": "string", "required": False, "default": "markdown"},
        },
    )
    
    template_show(template_id[:8], hermes_home=tmp_template_dir)
    out = capsys.readouterr().out
    
    assert "日报模板" in out
    assert "{{date}}" in out
    assert "{{format}}" in out
    assert "必填" in out or "required" in out.lower()
    assert "可选" in out or "optional" in out.lower()
    assert "markdown" in out


def test_template_delete_removes_template(tmp_template_dir, capsys):
    """Template delete should remove the template."""
    store = TemplateStore(db_path=tmp_template_dir / "analytics" / "templates.db")
    template_id = store.create_from_task(
        source_task_id="t1",
        name="测试模板",
        task_type="docs",
        request_summary="测试内容",
        created_at=time.time(),
    )
    
    # Verify exists
    assert store.get(template_id) is not None
    
    # Delete
    template_delete(template_id[:8], hermes_home=tmp_template_dir)
    out = capsys.readouterr().out
    
    assert "已删除" in out or "deleted" in out.lower()
    
    # Verify gone
    assert store.get(template_id) is None
