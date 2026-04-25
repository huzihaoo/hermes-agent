"""Tests for gateway /insights using event-log mode."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

import gateway.run as gateway_run


@pytest.mark.asyncio
async def test_gateway_insights_defaults_to_current_user_event_log(tmp_path, monkeypatch):
    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    event = MagicMock()
    event.source = source
    event.get_command_args.return_value = ""

    trace_file = tmp_path / "analytics" / "events.jsonl"
    trace_file.parent.mkdir(parents=True, exist_ok=True)
    trace_file.write_text("")

    called = {}

    class _FakeEngine:
        def __init__(self, trace_file):
            called["trace_file"] = trace_file
        def generate(self, days=30, user_id=None, admin=False):
            called["days"] = days
            called["user_id"] = user_id
            called["admin"] = admin
            return {"empty": True, "overview": {}}
        def format_gateway(self, report):
            return "gateway fake insights"

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr("agent.event_insights.EventInsightsEngine", _FakeEngine)

    result = await gateway_run.GatewayRunner._handle_insights_command(runner, event)

    assert result == "gateway fake insights"
    assert called["trace_file"] == trace_file
    assert called["days"] == 30
    assert called["user_id"] == "u-1"
    assert called["admin"] is False


@pytest.mark.asyncio
async def test_gateway_insights_admin_flag_uses_global_event_log(tmp_path, monkeypatch):
    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    event = MagicMock()
    event.source = source
    event.get_command_args.return_value = "--admin --days 7"

    trace_file = tmp_path / "analytics" / "events.jsonl"
    trace_file.parent.mkdir(parents=True, exist_ok=True)
    trace_file.write_text("")

    called = {}

    class _FakeEngine:
        def __init__(self, trace_file):
            called["trace_file"] = trace_file
        def generate(self, days=30, user_id=None, admin=False):
            called["days"] = days
            called["user_id"] = user_id
            called["admin"] = admin
            return {"empty": True, "overview": {}}
        def format_gateway(self, report):
            return "gateway admin insights"

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr("agent.event_insights.EventInsightsEngine", _FakeEngine)

    result = await gateway_run.GatewayRunner._handle_insights_command(runner, event)

    assert result == "gateway admin insights"
    assert called["trace_file"] == trace_file
    assert called["days"] == 7
    assert called["user_id"] is None
    assert called["admin"] is True
