"""Tests for unified failure exit helper."""

import json
from pathlib import Path

import pytest


@pytest.fixture
def agent(tmp_path, monkeypatch):
    from run_agent import AIAgent
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setenv("GOOGLE_API_KEY", "fake-key")
    agent = AIAgent(model="gemini-3-flash-preview", max_turns=10, verbose=False)
    agent._persist_session = lambda *args, **kwargs: None
    agent._save_trajectory = lambda *args, **kwargs: None
    agent._save_session_log = lambda *args, **kwargs: None
    return agent


def test_unified_failure_exit_emits_task_failed_event(agent):
    """Unified failure exit should emit task:failed event."""
    from run_agent import _unified_failure_exit
    
    trace_path = Path(agent.logs_dir).parent / "analytics" / "events.jsonl"
    
    result = _unified_failure_exit(
        task_id="test-task",
        error_class="test_error",
        error_message="Test error message",
        trace_file=trace_path,
        response_text="Partial response",
        conversation_history=[],
        iterations=5,
    )
    
    assert result["completed"] is False
    assert result["failed"] is True
    assert result["error_class"] == "test_error"
    assert result["error_message"] == "Test error message"
    
    # Verify event was emitted
    assert trace_path.exists()
    text = trace_path.read_text(encoding="utf-8")
    assert '"event": "task:failed"' in text
    assert '"task_id": "test-task"' in text
    assert '"error_class": "test_error"' in text


def test_unified_failure_exit_without_trace_file(agent):
    """Unified failure exit should work even without trace file."""
    from run_agent import _unified_failure_exit
    
    result = _unified_failure_exit(
        task_id="test-task",
        error_class="test_error",
        error_message="Test error message",
        trace_file=None,
        response_text="Partial response",
        conversation_history=[],
        iterations=5,
    )
    
    assert result["completed"] is False
    assert result["failed"] is True
