"""Tests for cron list showing template source."""

import time

import pytest

from cron.jobs import create_job
from gateway.tasks.template import TemplateStore
from hermes_cli.cron import cron_list


@pytest.fixture()
def tmp_cron_dir(tmp_path, monkeypatch):
    monkeypatch.setattr("cron.jobs.CRON_DIR", tmp_path / "cron")
    monkeypatch.setattr("cron.jobs.JOBS_FILE", tmp_path / "cron" / "jobs.json")
    monkeypatch.setattr("cron.jobs.OUTPUT_DIR", tmp_path / "cron" / "output")
    monkeypatch.setattr("hermes_constants.get_hermes_home", lambda: tmp_path)
    return tmp_path


def test_cron_list_shows_template_id(tmp_cron_dir, capsys):
    """cron list should show source template ID for template-based jobs."""
    store = TemplateStore(db_path=tmp_cron_dir / "analytics" / "templates.db")
    template_id = store.create_from_task(
        source_task_id="t1",
        name="日报模板",
        task_type="docs",
        request_summary="写今天的工作日报",
        created_at=time.time(),
    )
    
    create_job(
        prompt="",
        schedule="every 1d",
        name="每日日报",
        template_id=template_id,
    )
    
    cron_list()
    out = capsys.readouterr().out
    
    assert "每日日报" in out
    assert "Template:" in out
    assert template_id[:8] in out
