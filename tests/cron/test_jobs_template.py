"""Tests for cron job template integration."""

import time
from pathlib import Path

import pytest

from cron.jobs import create_job
from gateway.tasks.template import TemplateStore


@pytest.fixture
def template_store(tmp_path):
    return TemplateStore(db_path=tmp_path / "templates.db")


@pytest.fixture
def mock_hermes_home(tmp_path, monkeypatch):
    monkeypatch.setattr("hermes_constants.get_hermes_home", lambda: tmp_path)
    return tmp_path


def test_create_job_with_template_id(mock_hermes_home):
    # Create a template in the mocked hermes home
    store = TemplateStore(db_path=mock_hermes_home / "analytics" / "templates.db")
    tid = store.create_from_task(
        source_task_id="t1",
        name="日报模板",
        task_type="docs",
        request_summary="写今天的工作日报",
        created_at=time.time(),
    )

    # Create a cron job from the template
    job = create_job(
        prompt="",  # Will be overridden by template
        schedule="every 1d",
        name="每日日报",
        template_id=tid,
    )

    assert job["prompt"] == "写今天的工作日报"
    assert job["template_id"] == tid
    assert job["name"] == "每日日报"


def test_create_job_with_nonexistent_template_raises(mock_hermes_home):
    with pytest.raises(ValueError, match="Template .* not found"):
        create_job(
            prompt="",
            schedule="every 1h",
            template_id="nonexistent-template-id",
        )


def test_create_job_without_template_uses_prompt(mock_hermes_home):
    job = create_job(
        prompt="手动写的 prompt",
        schedule="every 1h",
        name="手动任务",
    )

    assert job["prompt"] == "手动写的 prompt"
    assert job.get("template_id") is None
