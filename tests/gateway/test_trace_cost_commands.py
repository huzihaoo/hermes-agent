"""Tests for gateway /trace and /cost commands."""

import time
import pytest

from gateway.observability.trace import Trace
from gateway.observability.store import TraceStore


@pytest.fixture
def trace_store(tmp_path, monkeypatch):
    import hermes_constants
    monkeypatch.setattr(hermes_constants, "get_hermes_home", lambda: tmp_path)
    
    # Also patch gateway.run._hermes_home
    import gateway.run as gateway_run
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    
    store = TraceStore(db_path=tmp_path / "analytics" / "traces.db")
    for i in range(3):
        t = Trace(user_id="alice", platform="feishu", request_summary=f"task {i}")
        s = t.start_span("llm:call", kind="llm")
        s.input_tokens = 1000
        s.output_tokens = 500
        s.cost_usd = 0.05
        s.finish()
        t.finish()
        store.save(t)
    return store


def test_trace_list_logic(trace_store):
    """Test trace list logic directly."""
    traces = trace_store.list_recent(limit=15)
    assert len(traces) == 3
    assert all("trace_id" in t for t in traces)


def test_trace_show_logic(trace_store):
    """Test trace show logic directly."""
    traces = trace_store.list_recent(limit=1)
    trace_id = traces[0]["trace_id"]
    
    trace = trace_store.get(trace_id)
    assert trace is not None
    assert trace["user_id"] == "alice"
    
    spans = trace_store.get_spans(trace_id)
    assert len(spans) == 1


def test_cost_stats_logic(trace_store):
    """Test cost stats logic directly."""
    stats = trace_store.stats_daily(days=1)
    assert stats["total_traces"] == 3
    assert stats["total_cost_usd"] > 0
    
    by_user = trace_store.stats_by_user(days=1)
    assert len(by_user) == 1
    assert by_user[0]["user_id"] == "alice"
