"""Tests for admission alert rules — P2-5.

Covers:
- QueueDepthAlert: fires when pending count exceeds threshold
- ErrorRateAlert: fires when error ratio exceeds threshold in window
- AlertManager: rule registration, check cycle, cooldown, callbacks
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# These imports will fail until we implement the module — RED phase.
from gateway.admission.alerts import (
    AlertLevel,
    AlertManager,
    AlertRecord,
    ErrorRateAlert,
    QueueDepthAlert,
)


# ── QueueDepthAlert ──────────────────────────────────────────────


class TestQueueDepthAlert:
    def test_no_fire_below_threshold(self):
        rule = QueueDepthAlert(warning=10, critical=50)
        result = rule.check({"pending_count": 5})
        assert result is None

    def test_fire_warning_at_threshold(self):
        rule = QueueDepthAlert(warning=10, critical=50)
        result = rule.check({"pending_count": 10})
        assert result is not None
        assert result.level == AlertLevel.WARNING
        assert "10" in result.message

    def test_fire_critical_at_threshold(self):
        rule = QueueDepthAlert(warning=10, critical=50)
        result = rule.check({"pending_count": 50})
        assert result is not None
        assert result.level == AlertLevel.CRITICAL

    def test_critical_overrides_warning(self):
        rule = QueueDepthAlert(warning=10, critical=50)
        result = rule.check({"pending_count": 60})
        assert result.level == AlertLevel.CRITICAL

    def test_label_included_in_message(self):
        rule = QueueDepthAlert(warning=5, critical=20, label="user:fast")
        result = rule.check({"pending_count": 7})
        assert "user:fast" in result.message


# ── ErrorRateAlert ───────────────────────────────────────────────


class TestErrorRateAlert:
    def test_no_fire_below_threshold(self):
        rule = ErrorRateAlert(threshold=0.3, window_seconds=60)
        metrics = {"total_completed": 90, "total_failed": 10, "total_dead": 0}
        result = rule.check(metrics)
        assert result is None

    def test_fire_when_error_rate_exceeded(self):
        rule = ErrorRateAlert(threshold=0.2, window_seconds=60)
        metrics = {"total_completed": 60, "total_failed": 30, "total_dead": 10}
        result = rule.check(metrics)
        assert result is not None
        assert result.level == AlertLevel.WARNING

    def test_no_fire_when_zero_total(self):
        rule = ErrorRateAlert(threshold=0.1, window_seconds=60)
        metrics = {"total_completed": 0, "total_failed": 0, "total_dead": 0}
        result = rule.check(metrics)
        assert result is None

    def test_dead_letter_counted_as_error(self):
        rule = ErrorRateAlert(threshold=0.1, window_seconds=60)
        metrics = {"total_completed": 80, "total_failed": 0, "total_dead": 20}
        result = rule.check(metrics)
        assert result is not None

    def test_critical_at_high_error_rate(self):
        rule = ErrorRateAlert(threshold=0.2, window_seconds=60, critical_threshold=0.5)
        metrics = {"total_completed": 40, "total_failed": 50, "total_dead": 10}
        result = rule.check(metrics)
        assert result.level == AlertLevel.CRITICAL


# ── AlertManager ─────────────────────────────────────────────────


class TestAlertManager:
    def test_register_and_check(self):
        mgr = AlertManager()
        rule = QueueDepthAlert(warning=5, critical=20)
        mgr.register(rule)
        fired = mgr.check_all({"pending_count": 6})
        assert len(fired) == 1
        assert fired[0].level == AlertLevel.WARNING

    def test_callback_invoked(self):
        cb = MagicMock()
        mgr = AlertManager(callbacks=[cb])
        rule = QueueDepthAlert(warning=5, critical=20)
        mgr.register(rule)
        mgr.check_all({"pending_count": 6})
        cb.assert_called_once()
        record = cb.call_args[0][0]
        assert isinstance(record, AlertRecord)

    def test_cooldown_suppresses_repeat(self):
        mgr = AlertManager(cooldown_seconds=10)
        rule = QueueDepthAlert(warning=5, critical=20)
        mgr.register(rule)

        fired1 = mgr.check_all({"pending_count": 6})
        assert len(fired1) == 1

        fired2 = mgr.check_all({"pending_count": 7})
        assert len(fired2) == 0  # suppressed by cooldown

    def test_cooldown_expires(self):
        mgr = AlertManager(cooldown_seconds=0)  # no cooldown
        rule = QueueDepthAlert(warning=5, critical=20)
        mgr.register(rule)

        fired1 = mgr.check_all({"pending_count": 6})
        assert len(fired1) == 1

        fired2 = mgr.check_all({"pending_count": 7})
        assert len(fired2) == 1  # fires again

    def test_multiple_rules(self):
        mgr = AlertManager(cooldown_seconds=0)
        mgr.register(QueueDepthAlert(warning=5, critical=20))
        mgr.register(ErrorRateAlert(threshold=0.1, window_seconds=60))

        metrics = {
            "pending_count": 6,
            "total_completed": 80,
            "total_failed": 15,
            "total_dead": 5,
        }
        fired = mgr.check_all(metrics)
        assert len(fired) == 2

    def test_history_recorded(self):
        mgr = AlertManager(cooldown_seconds=0)
        mgr.register(QueueDepthAlert(warning=5, critical=20))
        mgr.check_all({"pending_count": 6})
        mgr.check_all({"pending_count": 25})

        history = mgr.get_history()
        assert len(history) == 2

    def test_no_fire_returns_empty(self):
        mgr = AlertManager()
        mgr.register(QueueDepthAlert(warning=100, critical=200))
        fired = mgr.check_all({"pending_count": 1})
        assert fired == []
