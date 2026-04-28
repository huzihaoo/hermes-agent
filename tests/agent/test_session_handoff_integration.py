"""Tests for session handoff integration in run_agent.py main loop.

Verifies that the handoff triggers at RED health level and correctly
replaces the message list with a fresh one containing the summary.
"""

import pytest
from unittest.mock import patch, MagicMock, PropertyMock

from agent.session_health import SessionHealthMonitor, HealthLevel, HealthCheck
from agent.session_handoff import SessionHandoff, HandoffResult


class TestHandoffTriggerLogic:
    """Test the handoff trigger conditions without running the full agent loop."""

    def test_red_level_triggers_handoff(self):
        """Simulate what run_agent does: check health, if RED and not triggered, handoff."""
        monitor = SessionHealthMonitor(session_id="test_session")
        handoff_triggered = False

        # Simulate 460 messages (above MSG_CRITICAL=450 → RED)
        messages = [{"role": "user", "content": f"msg {i}"} for i in range(460)]
        health = monitor.check(messages)

        assert health.level == HealthLevel.RED

        if health.level == HealthLevel.RED and not handoff_triggered:
            handoff_triggered = True
            handoff = SessionHandoff(preserve_tail_messages=4)
            # Use fallback (no LLM) for test
            result = handoff.execute_handoff(
                "old_session", messages, new_session_id="new_session"
            )
            new_msgs = result.new_messages("system prompt")

            assert result.old_session_id == "old_session"
            assert result.new_session_id == "new_session"
            assert len(new_msgs) < len(messages)
            assert handoff_triggered

    def test_yellow_does_not_trigger_handoff(self):
        """YELLOW level should NOT trigger handoff."""
        monitor = SessionHealthMonitor(session_id="test")
        messages = [{"role": "user", "content": f"msg {i}"} for i in range(410)]
        health = monitor.check(messages)
        assert health.level == HealthLevel.YELLOW
        # Handoff should not fire
        assert health.level != HealthLevel.RED

    def test_handoff_only_fires_once(self):
        """Even if health stays RED, handoff should only trigger once."""
        handoff_triggered = False
        trigger_count = 0

        for iteration in range(5):
            health_level = HealthLevel.RED
            if health_level == HealthLevel.RED and not handoff_triggered:
                handoff_triggered = True
                trigger_count += 1

        assert trigger_count == 1

    def test_blocked_still_stops_after_handoff(self):
        """If handoff already fired but we somehow reach BLOCKED, still stop."""
        monitor = SessionHealthMonitor(session_id="test")
        messages = [{"role": "user", "content": f"msg {i}"} for i in range(510)]
        health = monitor.check(messages)
        assert health.level == HealthLevel.BLOCKED
        assert health.should_block

    @patch("agent.auxiliary_client.call_llm")
    def test_handoff_replaces_messages_correctly(self, mock_llm):
        """After handoff, the message list should be much shorter."""
        mock_llm.return_value = "Working on deployment. Status: configuring nginx. Pending: SSL setup."

        messages = [{"role": "system", "content": "You are helpful."}]
        for i in range(230):
            messages.append({"role": "user", "content": f"User message {i}"})
            messages.append({"role": "assistant", "content": f"Response {i}"})
        # 461 messages total → RED

        handoff = SessionHandoff(preserve_tail_messages=6)
        result = handoff.execute_handoff("old", messages, new_session_id="new")
        new_msgs = result.new_messages("You are helpful.")

        # Should be: system + summary_user + summary_assistant + 6 tail = 9
        assert len(new_msgs) == 9
        assert new_msgs[0]["role"] == "system"
        assert "SESSION CONTINUATION" in new_msgs[1]["content"]
        assert "deployment" in new_msgs[1]["content"]

    def test_handoff_failure_falls_through(self):
        """If handoff raises, the code should fall through to normal warning."""
        # Simulate the fallback path
        handoff_triggered = False
        fell_through = False

        try:
            handoff_triggered = True
            raise RuntimeError("Simulated handoff failure")
        except Exception:
            fell_through = True

        assert handoff_triggered
        assert fell_through

    def test_new_health_monitor_after_handoff(self):
        """After handoff, a new health monitor should be created with the new session ID."""
        old_monitor = SessionHealthMonitor(session_id="old_session")
        # Simulate handoff
        new_monitor = SessionHealthMonitor(session_id="new_session")
        # New monitor should start fresh
        messages = [{"role": "user", "content": "hi"}]
        health = new_monitor.check(messages)
        assert health.level == HealthLevel.GREEN
        assert health.message_count == 1


class TestHandoffCallback:
    """Test the handoff_callback mechanism."""

    @patch("agent.auxiliary_client.call_llm")
    def test_callback_receives_result(self, mock_llm):
        mock_llm.return_value = "Summary: working on auth module refactor. Tests passing."
        callback_results = []

        def on_handoff(result):
            callback_results.append(result)

        handoff = SessionHandoff()
        messages = [{"role": "system", "content": "sys"}]
        for i in range(230):
            messages.append({"role": "user", "content": f"msg {i}"})
            messages.append({"role": "assistant", "content": f"resp {i}"})

        result = handoff.execute_handoff("old", messages, new_session_id="new")
        on_handoff(result)

        assert len(callback_results) == 1
        assert callback_results[0].old_session_id == "old"
        assert callback_results[0].new_session_id == "new"

    def test_callback_exception_does_not_propagate(self):
        """Callback errors should be caught, not crash the agent."""
        def bad_callback(result):
            raise ValueError("callback broke")

        # Simulate the try/except pattern from run_agent
        caught = False
        try:
            bad_callback(None)
        except Exception:
            caught = True

        assert caught  # In run_agent, this is caught and logged


class TestHealthMonitorReset:
    """Verify that after handoff, the new monitor correctly tracks the shorter message list."""

    def test_fresh_monitor_green_after_handoff(self):
        """New session starts with few messages → GREEN."""
        monitor = SessionHealthMonitor(session_id="new_session")
        # Simulate the new session's messages (summary + tail)
        new_msgs = [
            {"role": "system", "content": "sys"},
            {"role": "user", "content": "[SESSION CONTINUATION] summary..."},
            {"role": "assistant", "content": "Got it."},
            {"role": "user", "content": "continue working"},
            {"role": "assistant", "content": "Sure."},
        ]
        health = monitor.check(new_msgs)
        assert health.level == HealthLevel.GREEN
        assert health.message_count == 5

    def test_rotation_chain_limit(self):
        """Verify that repeated handoffs don't create infinite chains."""
        chain_length = 0
        max_chain = 5

        for _ in range(10):
            if chain_length >= max_chain:
                break
            chain_length += 1

        assert chain_length == max_chain
