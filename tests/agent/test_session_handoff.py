"""Tests for agent/session_handoff.py — Session auto-continuation."""

import pytest
from unittest.mock import patch, MagicMock

from agent.session_handoff import (
    SessionHandoff,
    HandoffResult,
    DEFAULT_PRESERVE_TAIL,
    DEFAULT_SUMMARY_MAX_TOKENS,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_messages(n: int, with_tools: bool = False) -> list:
    """Generate a synthetic conversation with n user/assistant pairs."""
    msgs = [{"role": "system", "content": "You are helpful."}]
    for i in range(n):
        msgs.append({"role": "user", "content": f"User message {i}"})
        if with_tools and i % 3 == 0:
            msgs.append({
                "role": "assistant",
                "content": None,
                "tool_calls": [{
                    "id": f"call_{i}",
                    "function": {"name": "terminal", "arguments": '{"command":"echo hi"}'},
                }],
            })
            msgs.append({
                "role": "tool",
                "tool_call_id": f"call_{i}",
                "content": f"Tool output for call {i}",
            })
        msgs.append({"role": "assistant", "content": f"Assistant response {i}"})
    return msgs


# ---------------------------------------------------------------------------
# HandoffResult.new_messages
# ---------------------------------------------------------------------------

class TestHandoffResult:
    def test_new_messages_with_summary(self):
        result = HandoffResult(
            old_session_id="old_123",
            new_session_id="new_456",
            summary="Task: building a widget. Status: 50% done.",
            tail_messages=[
                {"role": "user", "content": "continue"},
                {"role": "assistant", "content": "ok"},
            ],
            user_notice="test",
        )
        msgs = result.new_messages("You are helpful.")
        assert msgs[0]["role"] == "system"
        assert msgs[1]["role"] == "user"
        assert "SESSION CONTINUATION" in msgs[1]["content"]
        assert "building a widget" in msgs[1]["content"]
        assert msgs[2]["role"] == "assistant"
        assert "up to speed" in msgs[2]["content"]
        # Tail messages follow
        assert msgs[3]["role"] == "user"
        assert msgs[3]["content"] == "continue"
        assert msgs[4]["role"] == "assistant"
        assert msgs[4]["content"] == "ok"

    def test_new_messages_fallback_mode(self):
        result = HandoffResult(
            old_session_id="old",
            new_session_id="new",
            summary="Recent user messages...",
            tail_messages=[],
            user_notice="test",
            fallback_mode=True,
        )
        msgs = result.new_messages("sys")
        assert "summary unavailable" in msgs[1]["content"]

    def test_new_messages_no_system_prompt(self):
        result = HandoffResult(
            old_session_id="old",
            new_session_id="new",
            summary="summary here",
            tail_messages=[],
            user_notice="test",
        )
        msgs = result.new_messages("")
        assert msgs[0]["role"] == "user"  # No system message

    def test_new_messages_empty_summary(self):
        result = HandoffResult(
            old_session_id="old",
            new_session_id="new",
            summary="",
            tail_messages=[{"role": "user", "content": "hi"}],
            user_notice="test",
        )
        msgs = result.new_messages("sys")
        # system + tail only, no summary injection
        assert len(msgs) == 2
        assert msgs[0]["role"] == "system"
        assert msgs[1]["role"] == "user"
        assert msgs[1]["content"] == "hi"


# ---------------------------------------------------------------------------
# SessionHandoff._extract_tail
# ---------------------------------------------------------------------------

class TestExtractTail:
    def test_basic_tail(self):
        h = SessionHandoff(preserve_tail_messages=3)
        msgs = _make_messages(10)
        tail = h._extract_tail(msgs)
        assert len(tail) == 3
        # Should be the last 3 messages
        assert tail[-1] == msgs[-1]

    def test_short_conversation(self):
        h = SessionHandoff(preserve_tail_messages=20)
        msgs = _make_messages(3)
        tail = h._extract_tail(msgs)
        assert len(tail) == len(msgs)

    def test_empty_messages(self):
        h = SessionHandoff()
        assert h._extract_tail([]) == []

    def test_preserves_tool_group(self):
        """If tail_start lands on a tool message, include the assistant that called it."""
        h = SessionHandoff(preserve_tail_messages=2)
        msgs = [
            {"role": "user", "content": "old stuff"},
            {"role": "assistant", "content": "old response"},
            {"role": "user", "content": "do something"},
            {"role": "assistant", "content": None, "tool_calls": [
                {"id": "c1", "function": {"name": "terminal", "arguments": "{}"}}
            ]},
            {"role": "tool", "tool_call_id": "c1", "content": "done"},
            {"role": "assistant", "content": "finished"},
        ]
        tail = h._extract_tail(msgs)
        # Last 2 would be tool + assistant, but we should also get the
        # assistant that issued the tool call
        roles = [m["role"] for m in tail]
        # Should not start with "tool"
        assert roles[0] != "tool"
        # The tool call assistant should be included
        assert "tool" in roles


# ---------------------------------------------------------------------------
# SessionHandoff._serialize_for_handoff
# ---------------------------------------------------------------------------

class TestSerialize:
    def test_basic_serialization(self):
        h = SessionHandoff()
        msgs = [
            {"role": "system", "content": "sys prompt"},
            {"role": "user", "content": "hello"},
            {"role": "assistant", "content": "hi there"},
        ]
        text = h._serialize_for_handoff(msgs)
        assert "USER: hello" in text
        assert "ASSISTANT: hi there" in text
        # System prompt should be skipped
        assert "sys prompt" not in text

    def test_tool_calls_noted(self):
        h = SessionHandoff()
        msgs = [
            {"role": "assistant", "content": None, "tool_calls": [
                {"id": "c1", "function": {"name": "terminal", "arguments": "{}"}},
                {"id": "c2", "function": {"name": "read_file", "arguments": "{}"}},
            ]},
        ]
        text = h._serialize_for_handoff(msgs)
        assert "called tools: terminal, read_file" in text

    def test_large_content_truncated(self):
        h = SessionHandoff()
        msgs = [{"role": "user", "content": "x" * 5000}]
        text = h._serialize_for_handoff(msgs)
        assert "[truncated]" in text
        assert len(text) < 5000

    def test_tool_output_abbreviated(self):
        h = SessionHandoff()
        msgs = [
            {"role": "tool", "tool_call_id": "c1", "content": "a" * 500},
        ]
        text = h._serialize_for_handoff(msgs)
        assert "500 chars output" in text

    def test_multimodal_content(self):
        h = SessionHandoff()
        msgs = [{
            "role": "user",
            "content": [
                {"type": "text", "text": "look at this"},
                {"type": "image_url", "image_url": {"url": "http://..."}},
            ],
        }]
        text = h._serialize_for_handoff(msgs)
        assert "look at this" in text


# ---------------------------------------------------------------------------
# SessionHandoff.generate_handoff_summary
# ---------------------------------------------------------------------------

class TestGenerateSummary:
    def test_too_few_messages_returns_none(self):
        h = SessionHandoff()
        assert h.generate_handoff_summary([{"role": "user", "content": "hi"}]) is None

    def test_empty_messages_returns_none(self):
        h = SessionHandoff()
        assert h.generate_handoff_summary([]) is None

    @patch("agent.auxiliary_client.call_llm")
    def test_successful_summary(self, mock_llm):
        mock_llm.return_value = "Task: building widget. Status: in progress. Pending: tests."
        h = SessionHandoff()
        msgs = _make_messages(10)
        result = h.generate_handoff_summary(msgs)
        assert result is not None
        assert "widget" in result
        mock_llm.assert_called_once()

    @patch("agent.auxiliary_client.call_llm")
    def test_llm_returns_short_string(self, mock_llm):
        mock_llm.return_value = "ok"  # Too short
        h = SessionHandoff()
        msgs = _make_messages(10)
        assert h.generate_handoff_summary(msgs) is None

    @patch("agent.auxiliary_client.call_llm")
    def test_llm_raises_exception(self, mock_llm):
        mock_llm.side_effect = RuntimeError("API down")
        h = SessionHandoff()
        msgs = _make_messages(10)
        assert h.generate_handoff_summary(msgs) is None

    @patch("agent.auxiliary_client.call_llm")
    def test_large_input_truncated(self, mock_llm):
        mock_llm.return_value = "Summary of a very long conversation about deployment."
        h = SessionHandoff()
        msgs = [{"role": "system", "content": "sys"}]
        for i in range(100):
            msgs.append({"role": "user", "content": f"msg {i} " + "x" * 1000})
            msgs.append({"role": "assistant", "content": f"resp {i} " + "y" * 1000})
        result = h.generate_handoff_summary(msgs)
        assert result is not None
        call_args = mock_llm.call_args
        user_content = call_args[1]["messages"][1]["content"] if "messages" in call_args[1] else call_args[0][0][1]["content"]
        assert "middle of conversation omitted" in user_content or len(user_content) <= 85000


# ---------------------------------------------------------------------------
# SessionHandoff._build_fallback_summary
# ---------------------------------------------------------------------------

class TestFallbackSummary:
    def test_basic_fallback(self):
        h = SessionHandoff()
        msgs = [
            {"role": "user", "content": "deploy the app"},
            {"role": "assistant", "content": "deploying..."},
            {"role": "user", "content": "check status"},
        ]
        result = h._build_fallback_summary(msgs)
        assert "deploy the app" in result
        assert "check status" in result
        assert "LLM summary unavailable" in result

    def test_no_user_messages(self):
        h = SessionHandoff()
        msgs = [{"role": "assistant", "content": "hello"}]
        result = h._build_fallback_summary(msgs)
        assert "No conversation context" in result

    def test_long_messages_truncated(self):
        h = SessionHandoff()
        msgs = [{"role": "user", "content": "x" * 1000}]
        result = h._build_fallback_summary(msgs)
        assert "..." in result
        assert len(result) < 1000


# ---------------------------------------------------------------------------
# SessionHandoff.execute_handoff
# ---------------------------------------------------------------------------

class TestExecuteHandoff:
    @patch("agent.auxiliary_client.call_llm")
    def test_successful_handoff(self, mock_llm):
        mock_llm.return_value = "Task: building widget. Status: halfway. Pending: write tests."
        h = SessionHandoff(preserve_tail_messages=4)
        msgs = _make_messages(20)
        result = h.execute_handoff("old_session", msgs, new_session_id="new_session")
        
        assert result.old_session_id == "old_session"
        assert result.new_session_id == "new_session"
        assert not result.fallback_mode
        assert "widget" in result.summary
        assert len(result.tail_messages) == 4
        assert "💫" in result.user_notice

    @patch("agent.auxiliary_client.call_llm")
    def test_fallback_handoff(self, mock_llm):
        mock_llm.side_effect = RuntimeError("API down")
        h = SessionHandoff(preserve_tail_messages=4)
        msgs = _make_messages(20)
        result = h.execute_handoff("old_session", msgs, new_session_id="new_session")
        
        assert result.fallback_mode
        assert "🔄" in result.user_notice
        assert len(result.tail_messages) == 4

    @patch("agent.auxiliary_client.call_llm")
    def test_auto_generated_session_id(self, mock_llm):
        mock_llm.return_value = "Summary of the conversation about deployment and testing."
        h = SessionHandoff()
        msgs = _make_messages(10)
        result = h.execute_handoff("old_123", msgs)
        
        assert result.new_session_id != ""
        assert result.new_session_id != "old_123"
        # Format: YYYYMMDD_HHMMSS_hexhash
        parts = result.new_session_id.split("_")
        assert len(parts) == 3

    @patch("agent.auxiliary_client.call_llm")
    def test_new_messages_are_valid(self, mock_llm):
        mock_llm.return_value = "Task: refactoring auth module. Status: planning phase."
        h = SessionHandoff(preserve_tail_messages=2)
        msgs = _make_messages(15)
        result = h.execute_handoff("old", msgs, new_session_id="new")
        
        new_msgs = result.new_messages("You are helpful.")
        # system + summary_user + summary_assistant + 2 tail
        assert len(new_msgs) == 5
        assert new_msgs[0]["role"] == "system"
        assert new_msgs[1]["role"] == "user"
        assert "SESSION CONTINUATION" in new_msgs[1]["content"]
        assert new_msgs[2]["role"] == "assistant"

    @patch("agent.auxiliary_client.call_llm")
    def test_summary_tokens_estimated(self, mock_llm):
        mock_llm.return_value = "A" * 400  # 400 chars ≈ 100 tokens
        h = SessionHandoff()
        msgs = _make_messages(10)
        result = h.execute_handoff("old", msgs, new_session_id="new")
        assert result.summary_tokens == 100  # 400 // 4


# ---------------------------------------------------------------------------
# Integration: HandoffResult round-trip
# ---------------------------------------------------------------------------

class TestRoundTrip:
    """Verify that execute_handoff → new_messages produces a valid conversation."""

    @patch("agent.auxiliary_client.call_llm")
    def test_round_trip_roles_alternate(self, mock_llm):
        mock_llm.return_value = "Working on session handoff feature. Tests pending."
        h = SessionHandoff(preserve_tail_messages=4)
        msgs = _make_messages(20)
        result = h.execute_handoff("old", msgs, new_session_id="new")
        new_msgs = result.new_messages("system prompt")
        
        # Verify no two consecutive messages have the same role
        # (except tool messages which can follow assistant)
        for i in range(1, len(new_msgs)):
            prev_role = new_msgs[i - 1]["role"]
            curr_role = new_msgs[i]["role"]
            if curr_role == "tool":
                continue  # tool can follow assistant
            if prev_role == "tool":
                continue  # anything can follow tool
            # user/assistant should alternate
            assert prev_role != curr_role, (
                f"Consecutive {prev_role} messages at index {i-1} and {i}"
            )
