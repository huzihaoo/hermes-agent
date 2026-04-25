"""Tests for hermes template edit CLI command."""

import time

import pytest

from gateway.tasks.template import TemplateStore
from hermes_cli.template import template_edit


@pytest.fixture()
def tmp_template_dir(tmp_path, monkeypatch):
    monkeypatch.setattr("hermes_constants.get_hermes_home", lambda: tmp_path)
    return tmp_path


def test_template_edit_name(tmp_template_dir, capsys):
    store = TemplateStore(db_path=tmp_template_dir / "analytics" / "templates.db")
    template_id = store.create_from_task(
        source_task_id="t1",
        name="旧名称",
        task_type="docs",
        request_summary="旧内容",
        created_at=time.time(),
    )
    
    template_edit(template_id[:8], name="新名称", hermes_home=tmp_template_dir)
    out = capsys.readouterr().out
    assert "已更新" in out or "updated" in out.lower()
    assert store.get(template_id)["name"] == "新名称"


def test_template_edit_content(tmp_template_dir, capsys):
    store = TemplateStore(db_path=tmp_template_dir / "analytics" / "templates.db")
    template_id = store.create_from_task(
        source_task_id="t1",
        name="测试模板",
        task_type="docs",
        request_summary="旧内容",
        created_at=time.time(),
    )
    
    template_edit(template_id[:8], content="新内容 {{param}}", hermes_home=tmp_template_dir)
    out = capsys.readouterr().out
    assert "已更新" in out or "updated" in out.lower()
    updated = store.get(template_id)
    assert updated["request_summary"] == "新内容 {{param}}"
    assert "param" in updated.get("params", {})
