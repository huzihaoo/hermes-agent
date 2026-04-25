"""Test cron auto-loads template skills."""

import time
import pytest
from pathlib import Path
from unittest.mock import patch

from cron.jobs import create_job
from gateway.tasks.template import TemplateStore


def test_cron_auto_loads_template_skills(tmp_path):
    """Cron should auto-load skills from template if not explicitly provided."""
    # Create template with skills
    store = TemplateStore(db_path=tmp_path / "analytics" / "templates.db")
    tid = store.create_from_task(
        source_task_id="t1",
        name="CR 模板",
        task_type="coding",
        request_summary="Review {{branch}}",
        created_at=time.time(),
        skills=["github-code-review", "requesting-code-review"],
    )
    
    # Create cron job with template_id but no explicit skills
    # prompt is not required when template_id is provided
    with patch("hermes_constants.get_hermes_home", return_value=tmp_path):
        job = create_job(
            prompt="",  # Will be loaded from template
            schedule="30m",
            template_id=tid,
        )
    
    # Job should have template skills
    assert job["skills"] == ["github-code-review", "requesting-code-review"]


def test_cron_explicit_skills_override_template(tmp_path):
    """Explicit skills should override template skills."""
    # Create template with skills
    store = TemplateStore(db_path=tmp_path / "analytics" / "templates.db")
    tid = store.create_from_task(
        source_task_id="t1",
        name="CR 模板",
        task_type="coding",
        request_summary="Review {{branch}}",
        created_at=time.time(),
        skills=["github-code-review"],
    )
    
    # Create cron job with explicit skills
    with patch("hermes_constants.get_hermes_home", return_value=tmp_path):
        job = create_job(
            prompt="",  # Will be loaded from template
            schedule="30m",
            template_id=tid,
            skills=["systematic-debugging"],
        )
    
    # Job should use explicit skills, not template skills
    assert job["skills"] == ["systematic-debugging"]
