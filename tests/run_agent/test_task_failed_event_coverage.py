"""Tests for task:failed event coverage across all failure paths."""

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


def test_api_error_emits_task_failed_event(agent):
    """API errors should emit task:failed event."""
    trace_path = Path(agent.logs_dir).parent / "analytics" / "events.jsonl"

    class _FakeAPIError(Exception):
        status_code = 500
        def __str__(self):
            return "Internal server error"

    agent._interruptible_api_call = lambda *_args, **_kwargs: (_ for _ in ()).throw(_FakeAPIError())

    result = agent.run_conversation("hello", task_id="task-api-error")

    assert result["completed"] is False
    assert trace_path.exists()
    text = trace_path.read_text(encoding="utf-8")
    assert '"event": "task:failed"' in text
    assert '"task_id": "task-api-error"' in text


def test_context_overflow_emits_task_failed_event(agent):
    """Context overflow should emit task:failed event."""
    trace_path = Path(agent.logs_dir).parent / "analytics" / "events.jsonl"

    # Force context overflow by setting a tiny context window
    agent.context_compressor.context_length = 100

    result = agent.run_conversation("hello " * 1000, task_id="task-overflow")

    assert result["completed"] is False
    assert trace_path.exists()
    text = trace_path.read_text(encoding="utf-8")
    assert '"event": "task:failed"' in text
    assert '"task_id": "task-overflow"' in text
