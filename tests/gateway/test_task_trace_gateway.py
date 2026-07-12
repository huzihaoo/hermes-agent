"""Tests for gateway task-trace integration."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import gateway.run as gateway_run


@pytest.mark.asyncio
async def test_gateway_start_runs_stale_pending_cleanup(tmp_path, monkeypatch):
    runner = object.__new__(gateway_run.GatewayRunner)
    runner.config = SimpleNamespace(sessions_dir=tmp_path / "sessions", platforms={})
    runner.adapters = {}
    runner.session_store = MagicMock()
    runner.delivery_router = MagicMock()
    runner.hooks = MagicMock()
    runner.hooks.emit = AsyncMock()
    runner.hooks.loaded_hooks = []
    runner._shutdown_event = AsyncMock()
    runner._failed_platforms = {}
    runner._sync_voice_mode_state_to_adapter = MagicMock()
    runner._update_platform_runtime_status = MagicMock()
    runner._request_clean_exit = MagicMock()
    runner._suspend_stuck_loop_sessions = MagicMock(return_value=0)
    runner._voice_mode = {}
    runner._running = False
    runner._running_agents = {}
    runner._running_agents_ts = {}
    runner._background_tasks = set()

    cleaned = {}

    def _fake_cleanup(*, trace_file, timeout_minutes=30):
        cleaned["trace_file"] = trace_file
        cleaned["timeout_minutes"] = timeout_minutes

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)

    import hermes_events
    monkeypatch.setattr(hermes_events, "cleanup_stale_pending", _fake_cleanup)

    result = await gateway_run.GatewayRunner.start(runner)

    assert result is True
    assert cleaned["trace_file"] == tmp_path / "analytics" / "events.jsonl"
    assert cleaned["timeout_minutes"] == 30


def test_emit_request_start_event(tmp_path, monkeypatch):
    runner = object.__new__(gateway_run.GatewayRunner)
    emitted = []

    class _FakeEmitter:
        def __init__(self, trace_file=None, task_store=None):
            self.trace_file = trace_file
            self.task_store = task_store
        def emit(self, event, data):
            emitted.append((event, data))

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)

    import hermes_events
    monkeypatch.setattr(hermes_events, "EventEmitter", _FakeEmitter)

    source = SimpleNamespace(
        platform=SimpleNamespace(value="feishu"),
        user_id="u-1",
        user_name="User One",
        chat_id="chat-1",
        thread_id=None,
        chat_type="p2p",
    )
    event = SimpleNamespace(
        text="hello world",
        message_id="msg-1",
        source=source,
        metadata={"raw": True},
        auto_skill=None,
    )
    session_entry = SimpleNamespace(session_id="sess-1")

    gateway_run.GatewayRunner._emit_request_start_event(runner, event, source, session_entry)

    request_events = [x for x in emitted if x[0] == "request:start"]
    assert request_events, "expected request:start event"
    data = request_events[0][1]
    assert data["task_id"] == "sess-1"
    assert data["platform"] == "feishu"
    assert data["user_id"] == "u-1"
    assert data["chat_id"] == "chat-1"
    assert data["message_id"] == "msg-1"
    assert data["request_summary"] == "hello world"


def test_emit_request_start_event_falls_back_to_alt_user_id(tmp_path, monkeypatch):
    runner = object.__new__(gateway_run.GatewayRunner)
    emitted = []

    class _FakeEmitter:
        def __init__(self, trace_file=None, task_store=None):
            self.trace_file = trace_file
            self.task_store = task_store
        def emit(self, event, data):
            emitted.append((event, data))

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)

    import hermes_events
    monkeypatch.setattr(hermes_events, "EventEmitter", _FakeEmitter)

    source = SimpleNamespace(
        platform=SimpleNamespace(value="feishu"),
        user_id=None,
        user_id_alt="ou_fallback",
        user_name="Fallback User",
        chat_id="chat-1",
        thread_id="topic:om_1",
        chat_type="group",
    )
    event = SimpleNamespace(
        text="hello from topic",
        message_id="msg-1",
        source=source,
        metadata={},
        auto_skill=None,
    )
    session_entry = SimpleNamespace(session_id="sess-1")

    gateway_run.GatewayRunner._emit_request_start_event(runner, event, source, session_entry)

    request_events = [x for x in emitted if x[0] == "request:start"]
    assert request_events, "expected request:start event"
    data = request_events[0][1]
    assert data["user_id"] == "ou_fallback"
    assert data["thread_id"] == "topic:om_1"
    assert data["chat_type"] == "group"
