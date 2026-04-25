"""Tests for audit logging on task browse commands."""

from types import SimpleNamespace

import pytest

import gateway.run as gateway_run


@pytest.mark.asyncio
async def test_tasks_command_writes_audit_event(tmp_path, monkeypatch):
    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    event = SimpleNamespace(source=source, get_command_args=lambda: "")

    audit_calls = []

    def _fake_audit(event_obj, audit_dir=None):
        audit_calls.append((event_obj.action, event_obj.resource, event_obj.result, event_obj.user_id))

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr("hermes_cli.task_trace.list_tasks", lambda **kwargs: [])
    monkeypatch.setattr("gateway.admission.audit.log_audit", _fake_audit)

    await gateway_run.GatewayRunner._handle_tasks_command(runner, event)

    assert audit_calls
    assert audit_calls[0][0] == "list_tasks"
    assert audit_calls[0][2] == "allowed"
    assert audit_calls[0][3] == "u-1"
