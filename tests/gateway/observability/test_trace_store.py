"""Tests for Trace/Span data model and TraceStore."""

import time

import pytest

from gateway.observability.trace import Trace, Span
from gateway.observability.store import TraceStore


@pytest.fixture
def store(tmp_path):
    return TraceStore(db_path=tmp_path / "traces.db")


# --- Trace/Span model ---

def test_trace_creates_with_defaults():
    t = Trace(user_id="alice", platform="feishu", request_summary="hello")
    assert t.trace_id  # non-empty
    assert t.status == "running"
    assert t.start_time > 0


def test_span_creates_with_defaults():
    s = Span(trace_id="t1", name="tool:read_file", kind="tool")
    assert s.span_id
    assert s.duration_ms == 0.0


def test_trace_add_span():
    t = Trace(user_id="alice")
    s = t.start_span("llm:call", kind="llm")
    assert s.trace_id == t.trace_id
    assert len(t.spans) == 1


def test_span_finish_records_duration():
    t = Trace(user_id="alice")
    s = t.start_span("tool:search", kind="tool")
    time.sleep(0.02)
    s.finish()
    assert s.duration_ms >= 15  # at least 15ms
    assert s.end_time > s.start_time


def test_trace_finish_aggregates_tokens_and_cost():
    t = Trace(user_id="alice")
    s1 = t.start_span("llm:call", kind="llm")
    s1.input_tokens = 100
    s1.output_tokens = 50
    s1.cost_usd = 0.01
    s1.finish()
    s2 = t.start_span("llm:call", kind="llm")
    s2.input_tokens = 200
    s2.output_tokens = 100
    s2.cost_usd = 0.02
    s2.finish()
    t.finish()
    assert t.total_tokens == 450
    assert t.total_cost_usd == pytest.approx(0.03)
    assert t.status == "completed"


def test_trace_finish_with_error():
    t = Trace(user_id="alice")
    t.finish(status="failed", error="timeout")
    assert t.status == "failed"
    assert t.error == "timeout"


# --- TraceStore persistence ---

def test_store_save_and_get(store):
    t = Trace(user_id="alice", platform="feishu", request_summary="hello")
    s = t.start_span("llm:call", kind="llm")
    s.input_tokens = 100
    s.output_tokens = 50
    s.cost_usd = 0.01
    s.finish()
    t.finish()
    store.save(t)

    loaded = store.get(t.trace_id)
    assert loaded is not None
    assert loaded["user_id"] == "alice"
    assert loaded["total_tokens"] == 150
    assert loaded["total_cost_usd"] == pytest.approx(0.01)


def test_store_get_nonexistent(store):
    assert store.get("nope") is None


def test_store_list_recent(store):
    for i in range(5):
        t = Trace(user_id=f"user-{i}", request_summary=f"task {i}")
        t.finish()
        store.save(t)
    
    traces = store.list_recent(limit=3)
    assert len(traces) == 3


def test_store_get_spans(store):
    t = Trace(user_id="alice")
    t.start_span("llm:call", kind="llm").finish()
    t.start_span("tool:read_file", kind="tool").finish()
    t.finish()
    store.save(t)

    spans = store.get_spans(t.trace_id)
    assert len(spans) == 2
    assert spans[0]["kind"] in ("llm", "tool")


def test_store_stats_daily(store):
    for i in range(3):
        t = Trace(user_id="alice")
        s = t.start_span("llm:call", kind="llm")
        s.input_tokens = 100
        s.cost_usd = 0.01
        s.finish()
        t.finish()
        store.save(t)

    stats = store.stats_daily(days=1)
    assert stats["total_traces"] == 3
    assert stats["total_tokens"] >= 300
    assert stats["total_cost_usd"] >= 0.03


def test_store_stats_by_user(store):
    for name in ["alice", "alice", "bob"]:
        t = Trace(user_id=name)
        s = t.start_span("llm:call", kind="llm")
        s.input_tokens = 100
        s.cost_usd = 0.01
        s.finish()
        t.finish()
        store.save(t)

    by_user = store.stats_by_user(days=1)
    assert len(by_user) == 2
    alice = next(u for u in by_user if u["user_id"] == "alice")
    assert alice["trace_count"] == 2
