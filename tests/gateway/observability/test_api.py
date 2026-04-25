"""Tests for observability REST API."""

import time
import pytest

from gateway.observability.trace import Trace
from gateway.observability.store import TraceStore
from gateway.observability.api import (
    api_list_traces, api_get_trace, api_stats_daily, api_stats_cost
)


@pytest.fixture
def api_store(tmp_path, monkeypatch):
    import hermes_constants
    monkeypatch.setattr(hermes_constants, "get_hermes_home", lambda: tmp_path)
    
    from gateway.observability.api import reset_api_store
    reset_api_store()  # Clear cached store so it picks up new hermes_home
    
    store = TraceStore(db_path=tmp_path / "analytics" / "traces.db")
    for i in range(5):
        t = Trace(user_id="alice" if i < 3 else "bob", platform="feishu", request_summary=f"task {i}")
        s = t.start_span("llm:call", kind="llm")
        s.input_tokens = 1000
        s.output_tokens = 500
        s.cost_usd = 0.05
        s.finish()
        t.finish(status="completed" if i < 4 else "failed")
        store.save(t)
    
    yield store
    reset_api_store()  # Cleanup


def test_api_list_traces(api_store):
    result = api_list_traces(limit=10)
    assert result["count"] == 5
    assert len(result["traces"]) == 5


def test_api_list_traces_filter_user(api_store):
    result = api_list_traces(user_id="alice")
    assert result["count"] == 3


def test_api_list_traces_filter_status(api_store):
    result = api_list_traces(status="failed")
    assert result["count"] == 1


def test_api_get_trace(api_store):
    traces = api_store.list_recent(limit=1)
    result = api_get_trace(traces[0]["trace_id"])
    assert result is not None
    assert "trace" in result
    assert "spans" in result
    assert len(result["spans"]) == 1


def test_api_get_trace_not_found(api_store):
    assert api_get_trace("nonexistent") is None


def test_api_stats_daily(api_store):
    result = api_stats_daily(days=1)
    assert result["total_traces"] == 5
    assert result["total_cost_usd"] > 0


def test_api_stats_cost_by_user(api_store):
    result = api_stats_cost(days=1, group_by="user")
    assert "by_user" in result
    assert len(result["by_user"]) == 2
    assert result["total_cost_usd"] > 0
