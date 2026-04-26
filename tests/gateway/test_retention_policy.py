"""Tests for retention policy / data cleanup."""

import time
import pytest
from pathlib import Path

from gateway.observability.store import TraceStore
from gateway.observability.trace import Trace, Span
from gateway.tasks.store import TaskStore
from gateway.tasks.types import Task, TaskStatus, TaskType


@pytest.fixture
def trace_store(tmp_path):
    """TraceStore with old and new traces."""
    db_path = tmp_path / "traces.db"
    store = TraceStore(db_path)
    
    now = time.time()
    old_time = now - (100 * 86400)  # 100 days ago
    recent_time = now - (10 * 86400)  # 10 days ago
    
    # Old trace (should be deleted)
    old_trace = Trace(
        trace_id="old_trace",
        user_id="alice",
        start_time=old_time,
        end_time=old_time + 10,
    )
    old_trace.spans.append(Span(
        span_id="old_span",
        trace_id="old_trace",
        name="old_span",
        start_time=old_time,
        end_time=old_time + 5,
    ))
    store.save(old_trace)
    
    # Recent trace (should be kept)
    recent_trace = Trace(
        trace_id="recent_trace",
        user_id="bob",
        start_time=recent_time,
        end_time=recent_time + 10,
    )
    recent_trace.spans.append(Span(
        span_id="recent_span",
        trace_id="recent_trace",
        name="recent_span",
        start_time=recent_time,
        end_time=recent_time + 5,
    ))
    store.save(recent_trace)
    
    return store


@pytest.fixture
def task_store(tmp_path):
    """TaskStore with old and new tasks."""
    db_path = tmp_path / "tasks.db"
    store = TaskStore(db_path)
    
    now = time.time()
    old_time = now - (100 * 86400)
    recent_time = now - (10 * 86400)
    
    # Old task
    store.upsert(Task(
        task_id="old_task",
        status=TaskStatus.COMPLETED,
        task_type=TaskType.CODING,
        user_id="alice",
        platform="feishu",
        request_summary="Old task",
        started_at=old_time,
    ))
    
    # Recent task
    store.upsert(Task(
        task_id="recent_task",
        status=TaskStatus.COMPLETED,
        task_type=TaskType.CHAT,
        user_id="bob",
        platform="feishu",
        request_summary="Recent task",
        started_at=recent_time,
    ))
    
    return store


def test_trace_cleanup_90_days(trace_store):
    """Default 90-day retention should delete old trace."""
    result = trace_store.cleanup_old_data(retention_days=90)
    assert result["deleted_traces"] == 1
    assert result["deleted_spans"] == 1
    
    # Verify old trace is gone
    assert trace_store.get("old_trace") is None
    # Verify recent trace is kept
    assert trace_store.get("recent_trace") is not None


def test_trace_cleanup_30_days(trace_store):
    """30-day retention should delete only old trace (100 days old)."""
    result = trace_store.cleanup_old_data(retention_days=30)
    assert result["deleted_traces"] == 1  # Only the 100-day-old trace
    assert result["deleted_spans"] == 1


def test_trace_cleanup_no_old_data(trace_store):
    """Cleanup with long retention should delete nothing."""
    result = trace_store.cleanup_old_data(retention_days=365)
    assert result["deleted_traces"] == 0
    assert result["deleted_spans"] == 0


def test_task_cleanup_90_days(task_store):
    """Default 90-day retention should delete old task."""
    deleted = task_store.cleanup_old_tasks(retention_days=90)
    assert deleted == 1
    
    # Verify old task is gone
    assert task_store.get("old_task") is None
    # Verify recent task is kept
    assert task_store.get("recent_task") is not None


def test_task_cleanup_30_days(task_store):
    """30-day retention should only delete the 100-day-old task."""
    deleted = task_store.cleanup_old_tasks(retention_days=30)
    assert deleted == 1


def test_task_cleanup_no_old_data(task_store):
    """Cleanup with long retention should delete nothing."""
    deleted = task_store.cleanup_old_tasks(retention_days=365)
    assert deleted == 0
