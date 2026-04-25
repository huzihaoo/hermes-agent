"""Tests for hermes trace CLI commands."""

import time

import pytest

from gateway.observability.trace import Trace
from gateway.observability.store import TraceStore


@pytest.fixture
def populated_store(tmp_path, monkeypatch):
    """Create a store with test data."""
    # Patch get_hermes_home before importing trace module
    import hermes_constants
    monkeypatch.setattr(hermes_constants, "get_hermes_home", lambda: tmp_path)
    
    store = TraceStore(db_path=tmp_path / "analytics" / "traces.db")
    
    # Create test traces
    for i in range(3):
        t = Trace(user_id="alice", platform="feishu", request_summary=f"task {i}")
        s = t.start_span("llm:call", kind="llm")
        s.input_tokens = 1000
        s.output_tokens = 500
        s.cost_usd = 0.05
        s.model = "claude-opus-4-6"
        s.finish()
        s2 = t.start_span("tool:read_file", kind="tool")
        s2.tool_name = "read_file"
        s2.finish()
        t.finish()
        store.save(t)
    
    return store


def test_trace_list_shows_traces(populated_store, capsys):
    from hermes_cli.trace import trace_list
    trace_list()
    out = capsys.readouterr().out
    assert "task 0" in out or "task 1" in out or "task 2" in out
    assert "Trace" in out


def test_trace_show_displays_details(populated_store, capsys, tmp_path):
    from hermes_cli.trace import trace_show, _get_store
    
    # Verify store can find the trace
    store = _get_store()
    traces = store.list_recent(limit=1)
    assert len(traces) > 0, f"Store at {store.db_path} has no traces"
    trace_id = traces[0]["trace_id"]
    
    trace_show(trace_id)
    out = capsys.readouterr().out
    assert "alice" in out
    assert "feishu" in out


def test_cost_summary_shows_stats(populated_store, capsys):
    from hermes_cli.trace import cost_summary
    cost_summary(days=1)
    out = capsys.readouterr().out
    assert "$" in out
    assert "Token" in out or "tok" in out.lower()


def test_cost_summary_by_user(populated_store, capsys):
    from hermes_cli.trace import cost_summary
    cost_summary(days=1, group_by="user")
    out = capsys.readouterr().out
    assert "alice" in out
    assert "$" in out
