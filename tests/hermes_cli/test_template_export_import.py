"""Tests for hermes template export/import CLI commands."""

import json
import time

import pytest

from gateway.tasks.template import TemplateStore
from hermes_cli.template import template_export, template_import


@pytest.fixture()
def tmp_template_dir(tmp_path, monkeypatch):
    monkeypatch.setattr("hermes_constants.get_hermes_home", lambda: tmp_path)
    return tmp_path


def test_template_export(tmp_template_dir, capsys):
    store = TemplateStore(db_path=tmp_template_dir / "analytics" / "templates.db")
    template_id = store.create_from_task(
        source_task_id="t1",
        name="测试模板",
        task_type="docs",
        request_summary="写 {{date}} 的日报",
        created_at=time.time(),
    )
    
    template_export(template_id[:8], hermes_home=tmp_template_dir)
    out = capsys.readouterr().out
    
    # Should contain JSON
    assert "{" in out and "}" in out
    
    # Parse and verify
    data = json.loads(out)
    assert data["name"] == "测试模板"
    assert data["request_summary"] == "写 {{date}} 的日报"


def test_template_import(tmp_template_dir, capsys):
    store = TemplateStore(db_path=tmp_template_dir / "analytics" / "templates.db")
    
    json_str = json.dumps({
        "name": "导入的模板",
        "task_type": "docs",
        "request_summary": "写 {{date}} 的周报",
        "params": {"date": {"type": "string", "required": True}}
    })
    
    template_import(json_str, hermes_home=tmp_template_dir)
    out = capsys.readouterr().out
    
    assert "已导入" in out or "imported" in out.lower()
    
    # Verify template was created
    templates = store.list_recent(limit=10)
    assert len(templates) == 1
    assert templates[0]["name"] == "导入的模板"
