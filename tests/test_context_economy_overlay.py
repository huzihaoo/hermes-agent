"""Focused regression tests for the isolated context-economy overlay."""

from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest


def _make_skill_tree(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    skill_dir = tmp_path / "skills" / "general" / "demo-skill"
    skill_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        "---\nname: demo-skill\ndescription: Demo workflow\n---\n# Demo\n",
        encoding="utf-8",
    )


def test_minimal_skill_prompt_uses_progressive_disclosure(tmp_path, monkeypatch):
    import agent.prompt_builder as prompt_builder
    import hermes_cli.config as config

    _make_skill_tree(tmp_path, monkeypatch)
    monkeypatch.setattr(
        config,
        "load_config_readonly",
        lambda: {"skills": {"system_prompt_mode": "minimal"}},
    )

    prompt = prompt_builder.build_skills_system_prompt()

    assert prompt == prompt_builder.MINIMAL_SKILLS_SYSTEM_PROMPT
    assert "skills_list" in prompt
    assert "skill_view" in prompt
    assert "<available_skills>" not in prompt
    assert "demo-skill" not in prompt


def test_full_skill_prompt_remains_upstream_default(tmp_path, monkeypatch):
    import agent.prompt_builder as prompt_builder
    import hermes_cli.config as config

    _make_skill_tree(tmp_path, monkeypatch)
    prompt_builder.clear_skills_system_prompt_cache(clear_snapshot=True)
    monkeypatch.setattr(config, "load_config_readonly", lambda: {})

    prompt = prompt_builder.build_skills_system_prompt()

    assert "## Skills (mandatory)" in prompt
    assert "<available_skills>" in prompt
    assert "demo-skill" in prompt


class _SessionDB:
    def __init__(self, stored_prompt: str):
        self.stored_prompt = stored_prompt
        self.updated = None

    def get_session(self, _session_id):
        return {"system_prompt": self.stored_prompt}

    def update_system_prompt(self, session_id, prompt):
        self.updated = (session_id, prompt)


def _prompt_agent(stored_prompt: str, rebuilt_prompt: str = "LEAN_PROMPT"):
    db = _SessionDB(stored_prompt)
    agent = SimpleNamespace(
        _cached_system_prompt=None,
        _session_db=db,
        session_id="session-1",
        model="main-model",
        provider="main-provider",
        platform="cli",
        _build_system_prompt=MagicMock(return_value=rebuilt_prompt),
    )
    return agent, db


def test_legacy_full_skill_prompt_is_rebuilt_without_session_start_hook(monkeypatch):
    from agent.conversation_loop import _restore_or_build_system_prompt
    import agent.credits_tracker as credits_tracker
    import agent.prompt_builder as prompt_builder
    import hermes_cli.plugins as plugins

    stored = (
        "Model: main-model\nProvider: main-provider\n"
        "## Skills (mandatory)\n<available_skills>\n  demo\n</available_skills>"
    )
    agent, db = _prompt_agent(stored)
    start_hook = MagicMock()
    monkeypatch.setattr(prompt_builder, "get_skills_system_prompt_mode", lambda: "minimal")
    monkeypatch.setattr(plugins, "invoke_hook", start_hook)
    monkeypatch.setattr(credits_tracker, "seed_credits_at_session_start", lambda _agent: None)

    _restore_or_build_system_prompt(
        agent,
        system_message=None,
        conversation_history=[{"role": "user", "content": "continue"}],
    )

    assert agent._cached_system_prompt == "LEAN_PROMPT"
    assert db.updated == ("session-1", "LEAN_PROMPT")
    agent._build_system_prompt.assert_called_once_with(None)
    start_hook.assert_not_called()


def test_minimal_stored_prompt_is_reused_verbatim(monkeypatch):
    from agent.conversation_loop import _restore_or_build_system_prompt
    import agent.prompt_builder as prompt_builder

    stored = (
        "Model: main-model\nProvider: main-provider\n"
        "## Skills\nUse progressive disclosure for skills."
    )
    agent, db = _prompt_agent(stored)
    monkeypatch.setattr(prompt_builder, "get_skills_system_prompt_mode", lambda: "minimal")

    _restore_or_build_system_prompt(
        agent,
        system_message=None,
        conversation_history=[{"role": "user", "content": "continue"}],
    )

    assert agent._cached_system_prompt == stored
    assert db.updated is None
    agent._build_system_prompt.assert_not_called()


class _Compressor:
    threshold_tokens = 190_000
    threshold_percent = 0.5

    def __init__(self):
        self.seen_tokens = []

    @staticmethod
    def _compute_threshold_tokens(context_length, threshold_percent, max_tokens=None):
        return int((context_length - (max_tokens or 0)) * threshold_percent)

    def should_compress(self, prompt_tokens):
        self.seen_tokens.append(prompt_tokens)
        return prompt_tokens >= self.threshold_tokens

    def should_defer_preflight_to_real_usage(self, _prompt_tokens):
        return True


def test_fallback_safe_threshold_uses_smallest_explicit_window():
    from agent.turn_context import fallback_safe_preflight_threshold

    agent = SimpleNamespace(
        context_compressor=_Compressor(),
        _fallback_chain=[
            {
                "provider": "backup",
                "model": "small-model",
                "context_length": 128_000,
                "max_tokens": 8_000,
            },
            {"provider": "unknown", "model": "no-size"},
        ],
    )

    assert fallback_safe_preflight_threshold(agent) == (
        60_000,
        "fallback-safe:backup/small-model",
    )


def test_fallback_safe_decision_preserves_upstream_compressor_policy():
    from agent.turn_context import should_compress_for_preflight

    compressor = _Compressor()

    assert should_compress_for_preflight(compressor, 60_000, 60_000) is True
    assert compressor.seen_tokens == [190_000]
    assert compressor.threshold_tokens == 190_000


def test_primary_usage_deferral_does_not_override_smaller_fallback_gate():
    from agent.turn_context import should_defer_preflight

    compressor = _Compressor()

    assert should_defer_preflight(compressor, 60_000, 60_000) is False
    assert should_defer_preflight(compressor, 190_000, 190_000) is True


class _FakeAIAgent:
    _VALID_API_ROLES = {"system", "user", "assistant", "tool"}

    @staticmethod
    def _get_tool_call_id_static(tool_call):
        return tool_call.get("id")

    @staticmethod
    def _get_tool_call_name_static(tool_call):
        return (tool_call.get("function") or {}).get("name")


@pytest.fixture
def patched_runtime_ra(monkeypatch):
    import agent.agent_runtime_helpers as runtime_helpers

    fake_module = SimpleNamespace(AIAgent=_FakeAIAgent, logger=logging.getLogger(__name__))
    monkeypatch.setattr(runtime_helpers, "_ra", lambda: fake_module)
    return runtime_helpers


def _paired_tool_messages(content: str):
    return [
        {
            "role": "assistant",
            "content": "",
            "tool_calls": [
                {
                    "id": "call-1",
                    "type": "function",
                    "function": {"name": "terminal", "arguments": "{}"},
                }
            ],
        },
        {
            "role": "tool",
            "tool_call_id": "call-1",
            "name": "terminal",
            "content": content,
        },
    ]


def test_historical_tool_compaction_only_changes_provider_copy(patched_runtime_ra):
    content = "x" * 25_000
    messages = _paired_tool_messages(content)

    sanitized = patched_runtime_ra.sanitize_api_messages(messages)

    assert messages[1]["content"] == content
    assert len(sanitized[1]["content"]) < 3_000
    assert "Historical tool result compacted for API context" in sanitized[1]["content"]


def test_persisted_tool_pointer_is_not_compacted(patched_runtime_ra):
    from tools.tool_result_storage import PERSISTED_OUTPUT_TAG

    content = PERSISTED_OUTPUT_TAG + ("x" * 25_000)
    sanitized = patched_runtime_ra.sanitize_api_messages(
        _paired_tool_messages(content)
    )

    assert sanitized[1]["content"] == content


def test_raw_output_merely_mentioning_persisted_tag_is_still_compacted(
    patched_runtime_ra,
):
    from tools.tool_result_storage import PERSISTED_OUTPUT_TAG

    content = ("x" * 21_000) + PERSISTED_OUTPUT_TAG
    sanitized = patched_runtime_ra.sanitize_api_messages(
        _paired_tool_messages(content)
    )

    assert "Historical tool result compacted for API context" in sanitized[1]["content"]


def test_live_budget_caps_are_preserved_with_upstream_dynamic_scaling():
    from tools.budget_config import BudgetConfig, budget_for_context_window

    budget = BudgetConfig()
    assert budget.default_result_size == 80_000
    assert budget.turn_budget == 80_000
    assert budget.historical_tool_message_max_chars == 20_000
    assert budget.resolve_threshold("read_file") == 80_000
    assert budget.resolve_threshold("skill_view") == 20_000
    assert budget.resolve_threshold("session_search") == 20_000
    assert budget.resolve_threshold("search_files") == 20_000
    assert budget.resolve_threshold("terminal") == 40_000

    large = budget_for_context_window(400_000)
    assert (large.default_result_size, large.turn_budget) == (80_000, 80_000)
    assert large.historical_tool_message_max_chars == 20_000

    small = budget_for_context_window(65_000)
    assert (small.default_result_size, small.turn_budget) == (39_000, 78_000)
    assert small.historical_tool_message_max_chars == 20_000
