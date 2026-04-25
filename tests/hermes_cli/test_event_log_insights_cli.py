"""Tests for event-log-backed CLI insights routing."""

from pathlib import Path
from unittest.mock import patch

from hermes_cli import main as cli_main


def test_cmd_insights_event_log_user_mode(tmp_path, capsys, monkeypatch):
    trace_file = tmp_path / "events.jsonl"
    trace_file.write_text("")

    called = {}

    class _FakeEngine:
        def __init__(self, trace_file):
            called["trace_file"] = Path(trace_file)
        def generate(self, days=30, user_id=None, admin=False):
            called["days"] = days
            called["user_id"] = user_id
            called["admin"] = admin
            return {"empty": True, "overview": {}}
        def format_terminal(self, report):
            return "fake insights"

    monkeypatch.setattr("agent.event_insights.EventInsightsEngine", _FakeEngine)

    with patch("sys.argv", ["hermes", "insights", "--days", "7", "--event-log", str(trace_file), "--user", "u-1"]):
        cli_main.main()

    out = capsys.readouterr().out
    assert "fake insights" in out
    assert called["trace_file"] == trace_file
    assert called["days"] == 7
    assert called["user_id"] == "u-1"
    assert called["admin"] is False
