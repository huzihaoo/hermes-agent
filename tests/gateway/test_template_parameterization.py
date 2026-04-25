"""Tests for Phase 2 template parameterization."""

import time
import pytest
from gateway.tasks.template import TemplateStore


@pytest.fixture
def store(tmp_path):
    return TemplateStore(db_path=tmp_path / "templates.db")


def test_extract_params_from_request_summary(store):
    """Auto-extract {{variable}} placeholders from request_summary."""
    tid = store.create_from_task(
        source_task_id="t1",
        name="日报模板",
        task_type="docs",
        request_summary="写 {{date}} 的工作日报，重点是 {{project}}",
        created_at=time.time(),
    )
    tpl = store.get(tid)
    assert tpl is not None
    assert "params" in tpl
    assert "date" in tpl["params"]
    assert "project" in tpl["params"]
    assert tpl["params"]["date"]["type"] == "string"
    assert tpl["params"]["date"]["required"] is True


def test_render_template_with_values(store):
    """Render template by substituting {{variable}} with provided values."""
    tid = store.create_from_task(
        source_task_id="t1",
        name="日报模板",
        task_type="docs",
        request_summary="写 {{date}} 的工作日报，重点是 {{project}}",
        created_at=time.time(),
    )
    rendered = store.render(tid, {"date": "2026-04-25", "project": "Hermes Gateway"})
    assert rendered == "写 2026-04-25 的工作日报，重点是 Hermes Gateway"


def test_render_missing_required_param_raises(store):
    """Render should raise ValueError if required param is missing."""
    tid = store.create_from_task(
        source_task_id="t1",
        name="日报模板",
        task_type="docs",
        request_summary="写 {{date}} 的工作日报",
        created_at=time.time(),
    )
    with pytest.raises(ValueError, match="Missing required parameters: date"):
        store.render(tid, {})


def test_render_nonexistent_template_returns_none(store):
    """Render should return None if template doesn't exist."""
    assert store.render("nonexistent-id", {}) is None


def test_template_without_params(store):
    """Templates without {{}} should have empty params dict."""
    tid = store.create_from_task(
        source_task_id="t1",
        name="固定模板",
        task_type="chat",
        request_summary="写一个固定的任务",
        created_at=time.time(),
    )
    tpl = store.get(tid)
    assert tpl["params"] == {}
    rendered = store.render(tid, {})
    assert rendered == "写一个固定的任务"


def test_schema_migration_adds_params_column(tmp_path):
    """Schema migration should add params column to existing templates table."""
    import sqlite3
    
    db_path = tmp_path / "templates.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Create old schema without params column
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            CREATE TABLE templates (
                template_id TEXT PRIMARY KEY,
                source_task_id TEXT NOT NULL,
                name TEXT NOT NULL,
                task_type TEXT NOT NULL,
                request_summary TEXT,
                created_at REAL NOT NULL
            )
        """)
        conn.execute("""
            INSERT INTO templates VALUES (?, ?, ?, ?, ?, ?)
        """, ("old-id", "t1", "old", "chat", "test", 1000.0))
        conn.commit()
    
    # Initialize TemplateStore (should trigger migration)
    store = TemplateStore(db_path=db_path)
    
    # Verify params column exists and old row has empty params
    tpl = store.get("old-id")
    assert tpl is not None
    assert "params" in tpl
    assert tpl["params"] == {}
