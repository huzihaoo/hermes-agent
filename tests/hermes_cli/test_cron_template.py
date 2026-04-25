"""Tests for cron template integration in CLI."""

import time
from argparse import Namespace

import pytest

from cron.jobs import create_job, get_job, list_jobs
from gateway.tasks.template import TemplateStore
from hermes_cli.cron import cron_command


@pytest.fixture
def tmp_cron_dir(tmp_path, monkeypatch):
    monkeypatch.setattr("cron.jobs.CRON_DIR", tmp_path / "cron")
    monkeypatch.setattr("cron.jobs.JOBS_FILE", tmp_path / "cron" / "jobs.json")
    monkeypatch.setattr("cron.jobs.OUTPUT_DIR", tmp_path / "cron" / "output")
    monkeypatch.setattr("hermes_constants.get_hermes_home", lambda: tmp_path)
    return tmp_path


@pytest.fixture
def template_store(tmp_cron_dir):
    return TemplateStore(db_path=tmp_cron_dir / "analytics" / "templates.db")


def test_cron_create_with_template_id(tmp_cron_dir, template_store, capsys):
    """hermes cron create --template <id> should use template prompt."""
    tid = template_store.create_from_task(
        source_task_id="t1",
        name="日报模板",
        task_type="docs",
        request_summary="写今天的工作日报",
        created_at=time.time(),
    )

    cron_command(
        Namespace(
            cron_command="create",
            schedule="every 1d",
            prompt="",  # Will be overridden by template
            name="每日日报",
            deliver=None,
            repeat=None,
            skill=None,
            skills=None,
            script=None,
            template=tid,
            template_params=None,
        )
    )

    out = capsys.readouterr().out
    assert "Created job" in out

    jobs = list_jobs()
    assert len(jobs) == 1
    assert jobs[0]["prompt"] == "写今天的工作日报"
    assert jobs[0]["template_id"] == tid


def test_cron_create_with_parameterized_template(tmp_cron_dir, template_store, capsys):
    """hermes cron create --template <id> --param key=value should render template."""
    tid = template_store.create_from_task(
        source_task_id="t1",
        name="日报模板",
        task_type="docs",
        request_summary="写 {{date}} 的工作日报，重点是 {{project}}",
        created_at=time.time(),
    )

    cron_command(
        Namespace(
            cron_command="create",
            schedule="every 1d",
            prompt="",
            name="每日日报",
            deliver=None,
            repeat=None,
            skill=None,
            skills=None,
            script=None,
            template=tid,
            template_params=["date=2026-04-25", "project=Hermes Gateway"],
        )
    )

    out = capsys.readouterr().out
    assert "Created job" in out

    jobs = list_jobs()
    assert len(jobs) == 1
    assert jobs[0]["prompt"] == "写 2026-04-25 的工作日报，重点是 Hermes Gateway"


def test_cron_create_with_template_missing_params_fails(tmp_cron_dir, template_store, capsys):
    """hermes cron create --template <id> without required params should fail."""
    tid = template_store.create_from_task(
        source_task_id="t1",
        name="日报模板",
        task_type="docs",
        request_summary="写 {{date}} 的工作日报",
        created_at=time.time(),
    )

    result = cron_command(
        Namespace(
            cron_command="create",
            schedule="every 1d",
            prompt="",
            name="每日日报",
            deliver=None,
            repeat=None,
            skill=None,
            skills=None,
            script=None,
            template=tid,
            template_params=None,
        )
    )

    assert result == 1
    out = capsys.readouterr().out
    assert "Missing required parameters" in out


def test_cron_create_with_nonexistent_template_fails(tmp_cron_dir, capsys):
    """hermes cron create --template <nonexistent> should fail."""
    result = cron_command(
        Namespace(
            cron_command="create",
            schedule="every 1d",
            prompt="",
            name="每日日报",
            deliver=None,
            repeat=None,
            skill=None,
            skills=None,
            script=None,
            template="nonexistent-template-id",
            template_params=None,
        )
    )

    assert result == 1
    out = capsys.readouterr().out
    assert "Template" in out and "not found" in out
