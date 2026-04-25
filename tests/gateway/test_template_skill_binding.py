"""Tests for template skill binding."""

import time
import pytest

from gateway.tasks.template import TemplateStore


def test_create_template_with_skills(tmp_path):
    """Templates should support skill binding."""
    store = TemplateStore(db_path=tmp_path / "templates.db")
    tid = store.create_from_task(
        source_task_id="t1",
        name="CR 模板",
        task_type="coding",
        request_summary="Review {{branch}} on {{repo}}",
        created_at=time.time(),
        skills=["github-code-review", "requesting-code-review"],
    )
    
    tpl = store.get(tid)
    assert tpl is not None
    assert tpl.get("skills") == ["github-code-review", "requesting-code-review"]


def test_template_without_skills_returns_empty_list(tmp_path):
    """Templates without skills should return empty list."""
    store = TemplateStore(db_path=tmp_path / "templates.db")
    tid = store.create_from_task(
        source_task_id="t1",
        name="简单模板",
        task_type="docs",
        request_summary="写日报",
        created_at=time.time(),
    )
    
    tpl = store.get(tid)
    assert tpl.get("skills") == []


def test_edit_template_skills(tmp_path):
    """Template edit should support updating skills."""
    store = TemplateStore(db_path=tmp_path / "templates.db")
    tid = store.create_from_task(
        source_task_id="t1",
        name="模板",
        task_type="coding",
        request_summary="test",
        created_at=time.time(),
    )
    
    store.update(tid, skills=["github-code-review"])
    tpl = store.get(tid)
    assert tpl["skills"] == ["github-code-review"]


def test_export_includes_skills(tmp_path):
    """Template export should include skills."""
    store = TemplateStore(db_path=tmp_path / "templates.db")
    tid = store.create_from_task(
        source_task_id="t1",
        name="CR 模板",
        task_type="coding",
        request_summary="Review code",
        created_at=time.time(),
        skills=["github-code-review"],
    )
    
    exported = store.export_template(tid)
    assert exported["skills"] == ["github-code-review"]


def test_import_preserves_skills(tmp_path):
    """Template import should preserve skills."""
    store = TemplateStore(db_path=tmp_path / "templates.db")
    
    new_id = store.import_template({
        "name": "导入的 CR 模板",
        "task_type": "coding",
        "request_summary": "Review code",
        "skills": ["github-code-review", "requesting-code-review"],
    })
    
    tpl = store.get(new_id)
    assert tpl["skills"] == ["github-code-review", "requesting-code-review"]
