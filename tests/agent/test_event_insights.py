"""Tests for event-based insights aggregation."""

import json
import time
from pathlib import Path

import pytest

from agent.event_insights import EventInsightsEngine


@pytest.fixture
def empty_events_file(tmp_path):
    """Empty events.jsonl file."""
    events_file = tmp_path / "events.jsonl"
    events_file.touch()
    return events_file


@pytest.fixture
def populated_events_file(tmp_path):
    """Populated events.jsonl with sample events."""
    events_file = tmp_path / "events.jsonl"
    now = time.time()
    
    events = [
        # User A - 2 tasks
        {"timestamp": now - 86400, "event": "task:start", "data": {"task_id": "t1", "platform": "feishu", "user_id": "u-a"}},
        {"timestamp": now - 86400 + 10, "event": "api:call", "data": {"task_id": "t1", "model": "claude-sonnet-4", "input_tokens": 1000, "output_tokens": 500}},
        {"timestamp": now - 86400 + 20, "event": "tool:call", "data": {"task_id": "t1", "tool_name": "terminal"}},
        {"timestamp": now - 86400 + 30, "event": "task:complete", "data": {"task_id": "t1", "total_tokens": 1500, "api_calls": 1, "tool_calls": 1}},
        
        {"timestamp": now - 3600, "event": "task:start", "data": {"task_id": "t2", "platform": "feishu", "user_id": "u-a"}},
        {"timestamp": now - 3600 + 10, "event": "api:call", "data": {"task_id": "t2", "model": "claude-sonnet-4", "input_tokens": 2000, "output_tokens": 1000}},
        {"timestamp": now - 3600 + 20, "event": "tool:call", "data": {"task_id": "t2", "tool_name": "read_file"}},
        {"timestamp": now - 3600 + 30, "event": "task:complete", "data": {"task_id": "t2", "total_tokens": 3000, "api_calls": 1, "tool_calls": 1}},
        
        # User B - 1 task
        {"timestamp": now - 7200, "event": "task:start", "data": {"task_id": "t3", "platform": "feishu", "user_id": "u-b"}},
        {"timestamp": now - 7200 + 10, "event": "api:call", "data": {"task_id": "t3", "model": "gpt-4o", "input_tokens": 500, "output_tokens": 300}},
        {"timestamp": now - 7200 + 20, "event": "tool:call", "data": {"task_id": "t3", "tool_name": "web_search"}},
        {"timestamp": now - 7200 + 30, "event": "task:complete", "data": {"task_id": "t3", "total_tokens": 800, "api_calls": 1, "tool_calls": 1}},
        
        # Old event outside 30-day window
        {"timestamp": now - 86400 * 40, "event": "task:start", "data": {"task_id": "t-old", "platform": "feishu", "user_id": "u-a"}},
    ]
    
    with open(events_file, "w", encoding="utf-8") as f:
        for event in events:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")
    
    return events_file


class TestEventInsightsEmpty:
    def test_empty_file_returns_empty_report(self, empty_events_file):
        engine = EventInsightsEngine(trace_file=empty_events_file)
        report = engine.generate(days=30)
        assert report["empty"] is True
        assert report["overview"] == {}
    
    def test_empty_file_terminal_format(self, empty_events_file):
        engine = EventInsightsEngine(trace_file=empty_events_file)
        report = engine.generate(days=30)
        text = engine.format_terminal(report)
        assert "No events found" in text


class TestEventInsightsPopulated:
    def test_generate_returns_all_sections(self, populated_events_file):
        engine = EventInsightsEngine(trace_file=populated_events_file)
        report = engine.generate(days=30)
        
        assert report["empty"] is False
        assert "overview" in report
        assert "models" in report
        assert "tools" in report
        assert "users" in report
    
    def test_overview_task_count(self, populated_events_file):
        engine = EventInsightsEngine(trace_file=populated_events_file)
        report = engine.generate(days=30)
        overview = report["overview"]
        
        # t1, t2, t3 are within 30 days; t-old is 40 days ago
        assert overview["total_tasks"] == 3
    
    def test_overview_token_totals(self, populated_events_file):
        engine = EventInsightsEngine(trace_file=populated_events_file)
        report = engine.generate(days=30)
        overview = report["overview"]
        
        expected_input = 1000 + 2000 + 500
        expected_output = 500 + 1000 + 300
        assert overview["total_input_tokens"] == expected_input
        assert overview["total_output_tokens"] == expected_output
        assert overview["total_tokens"] == expected_input + expected_output
    
    def test_user_filter(self, populated_events_file):
        engine = EventInsightsEngine(trace_file=populated_events_file)
        report = engine.generate(days=30, user_id="u-a")
        
        # Only t1 and t2 belong to u-a
        assert report["overview"]["total_tasks"] == 2
        assert report["overview"]["total_input_tokens"] == 1000 + 2000
    
    def test_admin_mode_shows_all_users(self, populated_events_file):
        engine = EventInsightsEngine(trace_file=populated_events_file)
        report = engine.generate(days=30, admin=True)
        
        users = report["users"]
        user_ids = [u["user_id"] for u in users]
        assert "u-a" in user_ids
        assert "u-b" in user_ids
        
        # u-a has 2 tasks
        user_a = next(u for u in users if u["user_id"] == "u-a")
        assert user_a["tasks"] == 2

    def test_overview_includes_success_and_efficiency_metrics(self, populated_events_file):
        engine = EventInsightsEngine(trace_file=populated_events_file)
        report = engine.generate(days=30)
        overview = report["overview"]

        assert overview["completed_tasks"] == 3
        assert overview["success_rate"] == pytest.approx(100.0, abs=0.1)
        assert overview["avg_tokens_per_task"] == pytest.approx((1500 + 3000 + 800) / 3, abs=0.1)
        assert overview["avg_tool_calls_per_task"] == pytest.approx(1.0, abs=0.1)
    
    def test_model_breakdown(self, populated_events_file):
        engine = EventInsightsEngine(trace_file=populated_events_file)
        report = engine.generate(days=30)
        models = report["models"]
        
        model_names = [m["model"] for m in models]
        assert "claude-sonnet-4" in model_names
        assert "gpt-4o" in model_names
        
        # claude-sonnet-4 has 2 tasks (t1 + t2)
        claude = next(m for m in models if m["model"] == "claude-sonnet-4")
        assert claude["tasks"] == 2
    
    def test_tool_breakdown(self, populated_events_file):
        engine = EventInsightsEngine(trace_file=populated_events_file)
        report = engine.generate(days=30)
        tools = report["tools"]
        
        tool_names = [t["tool"] for t in tools]
        assert "terminal" in tool_names
        assert "read_file" in tool_names
        assert "web_search" in tool_names
        
        # Each tool used once
        terminal = next(t for t in tools if t["tool"] == "terminal")
        assert terminal["count"] == 1
    
    def test_days_filter(self, populated_events_file):
        engine = EventInsightsEngine(trace_file=populated_events_file)
        report = engine.generate(days=1)
        
        # t2 (1 hour ago) and t3 (2 hours ago) are within 1 day; t1 is 1 day ago and falls outside cutoff
        assert report["overview"]["total_tasks"] == 2
