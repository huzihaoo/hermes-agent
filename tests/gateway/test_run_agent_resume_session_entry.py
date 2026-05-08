"""Regression tests for gateway agent resume marker handling."""

import sys
import threading
import types
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import gateway.run as gateway_run
from gateway.config import Platform
from gateway.session import SessionSource


class _CapturingAgent:
    last_run = None

    def __init__(self, *args, **kwargs):
        self.tools = []

    def run_conversation(self, user_message, conversation_history=None, task_id=None, **kwargs):
        type(self).last_run = {
            "user_message": user_message,
            "conversation_history": conversation_history,
            "task_id": task_id,
        }
        return {
            "final_response": "ok",
            "messages": [],
            "api_calls": 1,
            "completed": True,
        }


def _install_fake_agent(monkeypatch):
    fake_run_agent = types.ModuleType("run_agent")
    fake_run_agent.AIAgent = _CapturingAgent
    monkeypatch.setitem(sys.modules, "run_agent", fake_run_agent)


def _make_runner():
    runner = object.__new__(gateway_run.GatewayRunner)
    runner.adapters = {}
    runner._ephemeral_system_prompt = ""
    runner._prefill_messages = []
    runner._reasoning_config = None
    runner._service_tier = None
    runner._provider_routing = {}
    runner._fallback_model = None
    runner._running_agents = {}
    runner._pending_model_notes = {}
    runner._session_db = None
    runner._agent_cache = {}
    runner._agent_cache_lock = threading.Lock()
    runner._session_model_overrides = {}
    runner.hooks = SimpleNamespace(loaded_hooks=False)
    runner.config = SimpleNamespace(streaming=None)
    runner.session_store = SimpleNamespace(load_transcript=lambda session_id: [])
    runner._get_or_create_gateway_honcho = lambda session_key: (None, None)
    runner._enrich_message_with_vision = AsyncMock(return_value="ENRICHED")
    return runner


def _make_source() -> SessionSource:
    return SessionSource(
        platform=Platform.FEISHU,
        chat_id="oc_test",
        chat_type="group",
        user_id="ou_test",
    )


@pytest.mark.asyncio
async def test_run_agent_without_session_entry_does_not_raise_name_error(monkeypatch, tmp_path):
    _install_fake_agent(monkeypatch)
    runner = _make_runner()

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(gateway_run, "_env_path", tmp_path / ".env")
    monkeypatch.setattr(gateway_run, "load_dotenv", lambda *args, **kwargs: None)
    monkeypatch.setattr(gateway_run, "_load_gateway_config", lambda: {})
    monkeypatch.setattr(gateway_run, "_resolve_gateway_model", lambda config=None: "gpt-5.5")
    monkeypatch.setattr(
        gateway_run,
        "_resolve_runtime_agent_kwargs",
        lambda: {
            "provider": "custom",
            "api_mode": "chat_completions",
            "base_url": "https://example.invalid/v1",
            "api_key": "***",
        },
    )

    import hermes_cli.tools_config as tools_config
    monkeypatch.setattr(tools_config, "_get_platform_tools", lambda user_config, platform_key: {"core"})

    result = await runner._run_agent(
        message="看一下评测仓库目前暂存区的内容",
        context_prompt="",
        history=[],
        source=_make_source(),
        session_id="session-1",
        session_key="agent:main:feishu:group:oc_test",
    )

    assert result["final_response"] == "ok"
    assert _CapturingAgent.last_run["user_message"] == "看一下评测仓库目前暂存区的内容"


@pytest.mark.asyncio
async def test_run_agent_uses_passed_session_entry_for_resume_note(monkeypatch, tmp_path):
    _install_fake_agent(monkeypatch)
    runner = _make_runner()

    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(gateway_run, "_env_path", tmp_path / ".env")
    monkeypatch.setattr(gateway_run, "load_dotenv", lambda *args, **kwargs: None)
    monkeypatch.setattr(gateway_run, "_load_gateway_config", lambda: {})
    monkeypatch.setattr(gateway_run, "_resolve_gateway_model", lambda config=None: "gpt-5.5")
    monkeypatch.setattr(gateway_run, "_auto_continue_freshness_window", lambda: 3600)
    monkeypatch.setattr(gateway_run, "_is_fresh_gateway_interruption", lambda value, window_secs: bool(value))
    monkeypatch.setattr(
        gateway_run,
        "_resolve_runtime_agent_kwargs",
        lambda: {
            "provider": "custom",
            "api_mode": "chat_completions",
            "base_url": "https://example.invalid/v1",
            "api_key": "***",
        },
    )

    import hermes_cli.tools_config as tools_config
    monkeypatch.setattr(tools_config, "_get_platform_tools", lambda user_config, platform_key: {"core"})

    session_entry = SimpleNamespace(
        resume_pending=True,
        resume_reason="restart_timeout",
        last_resume_marked_at="2026-05-08T09:45:00+08:00",
    )

    result = await runner._run_agent(
        message="继续",
        context_prompt="",
        history=[],
        source=_make_source(),
        session_id="session-1",
        session_key="agent:main:feishu:group:oc_test",
        session_entry=session_entry,
    )

    assert result["final_response"] == "ok"
    assert "previous turn in this session was interrupted by a gateway restart" in _CapturingAgent.last_run["user_message"]
    assert _CapturingAgent.last_run["user_message"].endswith("继续")
