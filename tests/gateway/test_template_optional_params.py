"""Tests for template parameter defaults and optional params."""

import pytest

from gateway.tasks.template import TemplateStore


def test_create_template_with_optional_params(tmp_path):
    """Templates can have optional parameters with defaults."""
    store = TemplateStore(db_path=tmp_path / "templates.db")
    
    # Create template with explicit params including optional ones
    tid = store.create_from_task(
        source_task_id="t1",
        name="灵活日报",
        task_type="docs",
        request_summary="写 {{date}} 的工作日报，重点是 {{project}}，格式：{{format}}",
        created_at=1000.0,
        params={
            "date": {"type": "string", "required": True},
            "project": {"type": "string", "required": True},
            "format": {"type": "string", "required": False, "default": "markdown"},
        },
    )
    
    template = store.get(tid)
    assert template["params"]["date"]["required"] is True
    assert template["params"]["project"]["required"] is True
    assert template["params"]["format"]["required"] is False
    assert template["params"]["format"]["default"] == "markdown"


def test_render_with_optional_params_uses_defaults(tmp_path):
    """Rendering without optional params should use defaults."""
    store = TemplateStore(db_path=tmp_path / "templates.db")
    
    tid = store.create_from_task(
        source_task_id="t1",
        name="灵活日报",
        task_type="docs",
        request_summary="写 {{date}} 的工作日报，格式：{{format}}",
        created_at=1000.0,
        params={
            "date": {"type": "string", "required": True},
            "format": {"type": "string", "required": False, "default": "markdown"},
        },
    )
    
    # Render with only required param
    result = store.render(tid, {"date": "2026-04-25"})
    assert result == "写 2026-04-25 的工作日报，格式：markdown"


def test_render_with_optional_params_overrides_defaults(tmp_path):
    """Providing optional params should override defaults."""
    store = TemplateStore(db_path=tmp_path / "templates.db")
    
    tid = store.create_from_task(
        source_task_id="t1",
        name="灵活日报",
        task_type="docs",
        request_summary="写 {{date}} 的工作日报，格式：{{format}}",
        created_at=1000.0,
        params={
            "date": {"type": "string", "required": True},
            "format": {"type": "string", "required": False, "default": "markdown"},
        },
    )
    
    # Render with explicit format
    result = store.render(tid, {"date": "2026-04-25", "format": "plain"})
    assert result == "写 2026-04-25 的工作日报，格式：plain"


def test_render_missing_required_param_raises(tmp_path):
    """Missing required params should raise ValueError."""
    store = TemplateStore(db_path=tmp_path / "templates.db")
    
    tid = store.create_from_task(
        source_task_id="t1",
        name="灵活日报",
        task_type="docs",
        request_summary="写 {{date}} 的工作日报，格式：{{format}}",
        created_at=1000.0,
        params={
            "date": {"type": "string", "required": True},
            "format": {"type": "string", "required": False, "default": "markdown"},
        },
    )
    
    # Missing required param should raise
    with pytest.raises(ValueError, match="Missing required parameters: date"):
        store.render(tid, {})


def test_auto_extracted_params_are_required_by_default(tmp_path):
    """Auto-extracted params should default to required=True."""
    store = TemplateStore(db_path=tmp_path / "templates.db")
    
    tid = store.create_from_task(
        source_task_id="t1",
        name="自动提取",
        task_type="docs",
        request_summary="写 {{date}} 的工作日报",
        created_at=1000.0,
        # params not provided, should auto-extract
    )
    
    template = store.get(tid)
    assert "date" in template["params"]
    assert template["params"]["date"]["required"] is True
    assert "default" not in template["params"]["date"]
