"""Tests for observability alert rules and /alerts command."""

import time
from types import SimpleNamespace

import pytest

from gateway.observability.alerts import AlertChecker, AlertMetric, AlertRule, AlertSeverity
from gateway.observability.store import TraceStore
from gateway.observability.trace import Trace


def _save_trace(store, trace_id, *, cost=0.0, status="ok", hours_ago=1, user_id="u1"):
    now = time.time() - (hours_ago * 3600)
    trace = Trace(
        trace_id=trace_id,
        user_id=user_id,
        platform="feishu",
        request_summary="test",
        start_time=now,
        end_time=now + 1,
        status=status,
        total_tokens=100,
        total_cost_usd=cost,
        task_type="chat",
    )
    store.save(trace)


def test_alert_checker_cost_trigger(tmp_path):
    store = TraceStore(tmp_path / "traces.db")
    _save_trace(store, "t1", cost=6.0)

    checker = AlertChecker(store, rules=[
        AlertRule("cost_warn", AlertMetric.COST, threshold=5.0, window_hours=24, severity=AlertSeverity.WARNING)
    ])
    results = checker.check_all()
    assert len(results) == 1
    assert results[0].triggered is True
    assert results[0].current_value == 6.0


def test_alert_checker_cost_not_triggered(tmp_path):
    store = TraceStore(tmp_path / "traces.db")
    _save_trace(store, "t1", cost=2.0)

    checker = AlertChecker(store, rules=[
        AlertRule("cost_warn", AlertMetric.COST, threshold=5.0, window_hours=24)
    ])
    assert checker.check_all()[0].triggered is False


def test_alert_checker_error_rate_trigger(tmp_path):
    store = TraceStore(tmp_path / "traces.db")
    _save_trace(store, "t1", status="error")
    _save_trace(store, "t2", status="error")
    _save_trace(store, "t3", status="ok")

    checker = AlertChecker(store, rules=[
        AlertRule("err_warn", AlertMetric.ERROR_RATE, threshold=0.5, window_hours=24)
    ])
    result = checker.check_all()[0]
    assert result.triggered is True
    assert abs(result.current_value - (2/3)) < 1e-6


def test_alert_checker_error_rate_not_triggered(tmp_path):
    store = TraceStore(tmp_path / "traces.db")
    _save_trace(store, "t1", status="error")
    _save_trace(store, "t2", status="ok")
    _save_trace(store, "t3", status="ok")

    checker = AlertChecker(store, rules=[
        AlertRule("err_warn", AlertMetric.ERROR_RATE, threshold=0.5, window_hours=24)
    ])
    assert checker.check_all()[0].triggered is False


def test_alert_checker_ignores_old_traces(tmp_path):
    store = TraceStore(tmp_path / "traces.db")
    _save_trace(store, "old", cost=100.0, hours_ago=48)
    _save_trace(store, "new", cost=1.0, hours_ago=1)

    checker = AlertChecker(store, rules=[
        AlertRule("cost_warn", AlertMetric.COST, threshold=5.0, window_hours=24)
    ])
    result = checker.check_all()[0]
    assert result.triggered is False
    assert result.current_value == 1.0


@pytest.mark.asyncio
async def test_alerts_command_no_trigger(tmp_path, monkeypatch):
    import gateway.run as gateway_run

    db_path = tmp_path / "analytics" / "traces.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    store = TraceStore(db_path)
    _save_trace(store, "t1", cost=1.0, status="ok")

    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    event = SimpleNamespace(source=source, get_command_args=lambda: "")
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)

    result = await gateway_run.GatewayRunner._handle_alerts_command(runner, event)
    assert "一切正常" in result
    assert "🟢" in result


@pytest.mark.asyncio
async def test_alerts_command_triggered(tmp_path, monkeypatch):
    import gateway.run as gateway_run

    db_path = tmp_path / "analytics" / "traces.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    store = TraceStore(db_path)
    _save_trace(store, "t1", cost=25.0, status="ok")
    _save_trace(store, "t2", cost=0.0, status="error")
    _save_trace(store, "t3", cost=0.0, status="error")

    runner = object.__new__(gateway_run.GatewayRunner)
    source = SimpleNamespace(platform=SimpleNamespace(value="feishu"), user_id="u-1")
    event = SimpleNamespace(source=source, get_command_args=lambda: "")
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)

    result = await gateway_run.GatewayRunner._handle_alerts_command(runner, event)
    assert "告警触发" in result
    assert ("🔴" in result) or ("🟡" in result)
