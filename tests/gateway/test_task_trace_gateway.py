"""Tests for gateway task-trace integration."""

import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import gateway.run as gateway_run


def _stale_pending_marker(tmp_path, task_id="task-stale"):
    marker = tmp_path / "analytics" / f".pending-{task_id}"
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(json.dumps({"task_id": task_id}), encoding="utf-8")
    stale = time.time() - 3600
    os.utime(marker, (stale, stale))
    return marker


def test_stale_pending_cleanup_removes_marker_after_successful_timeout_append(tmp_path):
    from hermes_events import cleanup_stale_pending, trace_task_events

    trace_file = tmp_path / "analytics" / "events.jsonl"
    marker = _stale_pending_marker(tmp_path)

    cleanup_stale_pending(trace_file=trace_file, timeout_minutes=1)

    assert not marker.exists()
    assert trace_task_events(trace_file=trace_file, task_id="task-stale")[0]["event"] == "task:timeout"


def test_stale_pending_cleanup_keeps_marker_when_timeout_event_write_fails(tmp_path):
    from hermes_events import cleanup_stale_pending

    trace_file = tmp_path / "analytics" / "events.jsonl"
    marker = _stale_pending_marker(tmp_path)
    trace_file.mkdir()

    cleanup_stale_pending(trace_file=trace_file, timeout_minutes=1)

    assert marker.exists()


def test_stale_pending_cleanup_is_single_writer_under_concurrency(tmp_path):
    from hermes_events import cleanup_stale_pending, trace_task_events

    trace_file = tmp_path / "analytics" / "events.jsonl"
    _stale_pending_marker(tmp_path)
    start = threading.Barrier(2)

    def clean():
        start.wait(timeout=5)
        cleanup_stale_pending(trace_file=trace_file, timeout_minutes=1)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(clean) for _ in range(2)]
        for future in futures:
            future.result(timeout=5)

    events = trace_task_events(trace_file=trace_file, task_id="task-stale")
    assert [event["event"] for event in events] == ["task:timeout"]


def test_stale_pending_cleanup_skips_non_file_and_continues_sweep(tmp_path):
    from hermes_events import cleanup_stale_pending, trace_task_events

    trace_file = tmp_path / "analytics" / "events.jsonl"
    malformed = tmp_path / "analytics" / ".pending-a-malformed"
    malformed.mkdir(parents=True)
    marker = _stale_pending_marker(tmp_path, task_id="z-valid")

    cleanup_stale_pending(trace_file=trace_file, timeout_minutes=1)

    assert malformed.is_dir()
    assert not marker.exists()
    assert trace_task_events(trace_file=trace_file, task_id="z-valid")[0]["event"] == "task:timeout"


def test_stale_pending_cleanup_deduplicates_append_unlink_crash_window(tmp_path, monkeypatch):
    from hermes_events import cleanup_stale_pending, trace_task_events

    trace_file = tmp_path / "analytics" / "events.jsonl"
    marker = _stale_pending_marker(tmp_path)
    original_unlink = type(marker).unlink
    failed_once = False

    def fail_marker_unlink_once(path, *args, **kwargs):
        nonlocal failed_once
        if path == marker and not failed_once:
            failed_once = True
            raise OSError("simulated crash window")
        return original_unlink(path, *args, **kwargs)

    monkeypatch.setattr(type(marker), "unlink", fail_marker_unlink_once)

    cleanup_stale_pending(trace_file=trace_file, timeout_minutes=1)
    assert marker.exists()
    cleanup_stale_pending(trace_file=trace_file, timeout_minutes=1)

    events = trace_task_events(trace_file=trace_file, task_id="task-stale")
    assert not marker.exists()
    assert [event["event"] for event in events] == ["task:timeout"]
    assert events[0]["data"]["timeout_event_id"].startswith("stale-timeout:")


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
    runner._shutdown_event = MagicMock()
    runner._shutdown_event.is_set.return_value = False
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
        # Stop at the post-cleanup lifecycle gate. This proves cleanup happens
        # before plugin discovery or any platform can accept work.
        runner._shutdown_event.is_set.return_value = True

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)

    import hermes_events
    monkeypatch.setattr(hermes_events, "cleanup_stale_pending", _fake_cleanup)

    result = await gateway_run.GatewayRunner.start(runner)

    assert result is True
    assert cleaned["trace_file"] == tmp_path / "analytics" / "events.jsonl"
    assert cleaned["timeout_minutes"] == 30
    runner.hooks.discover_and_load.assert_not_called()


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
