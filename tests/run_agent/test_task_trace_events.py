"""Integration tests for task trace emission from AIAgent."""

from pathlib import Path
from unittest.mock import MagicMock, patch
import sys
import types

import pytest

sys.modules.setdefault("fire", types.SimpleNamespace(Fire=lambda *a, **k: None))

from run_agent import AIAgent


def _make_tool_defs(*names: str) -> list:
    return [
        {
            "type": "function",
            "function": {
                "name": n,
                "description": f"{n} tool",
                "parameters": {"type": "object", "properties": {}},
            },
        }
        for n in names
    ]


def _mock_response(content="Hello", finish_reason="stop", tool_calls=None):
    from types import SimpleNamespace
    msg = SimpleNamespace(content=content, tool_calls=tool_calls)
    choice = SimpleNamespace(message=msg, finish_reason=finish_reason)
    usage = SimpleNamespace(
        prompt_tokens=10,
        completion_tokens=5,
        total_tokens=15,
        input_tokens=10,
        output_tokens=5,
        cache_read_input_tokens=0,
        cache_creation_input_tokens=0,
    )
    return SimpleNamespace(choices=[choice], usage=usage, model="test-model")


def _mock_tool_call(name="web_search", arguments="{}", call_id="c1"):
    from types import SimpleNamespace
    return SimpleNamespace(
        id=call_id,
        type="function",
        function=SimpleNamespace(name=name, arguments=arguments),
    )


@pytest.fixture()
def agent(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    with (
        patch("run_agent.get_tool_definitions", return_value=_make_tool_defs("web_search")),
        patch("run_agent.check_toolset_requirements", return_value={}),
        patch("run_agent.OpenAI"),
    ):
        a = AIAgent(
            api_key="test-key-1234567890",
            quiet_mode=True,
            skip_context_files=True,
            skip_memory=True,
            platform="cli",
            user_id="user-1",
        )
        a.client = MagicMock()
        a._persist_session = lambda *args, **kwargs: None
        a._save_trajectory = lambda *args, **kwargs: None
        a._save_session_log = lambda *args, **kwargs: None
        return a


def test_run_conversation_emits_task_trace_events(agent, tmp_path):
    trace_path = Path(agent.logs_dir).parent / "analytics" / "events.jsonl"
    agent._interruptible_api_call = lambda *_args, **_kwargs: _mock_response(content="Done")

    result = agent.run_conversation("hello", task_id="task-123")

    assert result["completed"] is True
    assert trace_path.exists()
    text = trace_path.read_text(encoding="utf-8")
    assert '"event": "task:start"' in text
    assert '"event": "api:call"' in text
    assert '"event": "task:complete"' in text
    assert '"task_id": "task-123"' in text


def test_execute_tool_calls_emits_tool_event(agent):
    from types import SimpleNamespace
    trace_path = Path(agent.logs_dir).parent / "analytics" / "events.jsonl"
    tc = _mock_tool_call(name="web_search", arguments='{"q":"test"}', call_id="c1")
    assistant_message = SimpleNamespace(content="", tool_calls=[tc])
    messages = []

    with patch("run_agent.handle_function_call", return_value="search result"):
        agent._execute_tool_calls(assistant_message, messages, "task-xyz")

    assert trace_path.exists()
    text = trace_path.read_text(encoding="utf-8")
    assert '"event": "tool:call"' in text
    assert '"tool_name": "web_search"' in text
    assert '"task_id": "task-xyz"' in text
