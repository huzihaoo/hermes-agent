"""Alert rules for admission control.

Provides configurable alert rules that fire when queue metrics exceed thresholds.
Rules are checked by AlertManager which handles cooldown, callbacks, and history.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Protocol

logger = logging.getLogger(__name__)


class AlertLevel(str, Enum):
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass
class AlertRecord:
    """A single fired alert."""
    rule_name: str
    level: AlertLevel
    message: str
    timestamp: float = field(default_factory=time.time)
    metrics_snapshot: dict | None = None


class AlertRule(Protocol):
    """Protocol for alert rules."""
    name: str

    def check(self, metrics: dict) -> AlertRecord | None:
        ...


# ── Concrete rules ───────────────────────────────────────────────


class QueueDepthAlert:
    """Fires when pending_count exceeds warning or critical threshold."""

    def __init__(self, warning: int, critical: int, label: str = ""):
        self.name = f"queue_depth:{label}" if label else "queue_depth"
        self._warning = warning
        self._critical = critical
        self._label = label

    def check(self, metrics: dict) -> AlertRecord | None:
        count = metrics.get("pending_count", 0)
        if count >= self._critical:
            return AlertRecord(
                rule_name=self.name,
                level=AlertLevel.CRITICAL,
                message=f"Queue depth CRITICAL: {count} pending (threshold {self._critical})"
                        + (f" [{self._label}]" if self._label else ""),
            )
        if count >= self._warning:
            return AlertRecord(
                rule_name=self.name,
                level=AlertLevel.WARNING,
                message=f"Queue depth WARNING: {count} pending (threshold {self._warning})"
                        + (f" [{self._label}]" if self._label else ""),
            )
        return None


class ErrorRateAlert:
    """Fires when (failed + dead) / total exceeds threshold."""

    def __init__(self, threshold: float, window_seconds: int = 60,
                 critical_threshold: float | None = None):
        self.name = "error_rate"
        self._threshold = threshold
        self._critical_threshold = critical_threshold
        self._window = window_seconds

    def check(self, metrics: dict) -> AlertRecord | None:
        completed = metrics.get("total_completed", 0)
        failed = metrics.get("total_failed", 0)
        dead = metrics.get("total_dead", 0)
        total = completed + failed + dead
        if total == 0:
            return None
        error_count = failed + dead
        rate = error_count / total

        if self._critical_threshold is not None and rate >= self._critical_threshold:
            return AlertRecord(
                rule_name=self.name,
                level=AlertLevel.CRITICAL,
                message=f"Error rate CRITICAL: {rate:.1%} ({error_count}/{total})",
            )
        if rate >= self._threshold:
            return AlertRecord(
                rule_name=self.name,
                level=AlertLevel.WARNING,
                message=f"Error rate WARNING: {rate:.1%} ({error_count}/{total})",
            )
        return None


# ── Manager ──────────────────────────────────────────────────────

AlertCallback = Callable[[AlertRecord], Any]


class AlertManager:
    """Manages alert rules, cooldown, callbacks, and history."""

    def __init__(
        self,
        cooldown_seconds: float = 300,
        callbacks: list[AlertCallback] | None = None,
    ):
        self._rules: list[AlertRule] = []
        self._cooldown = cooldown_seconds
        self._callbacks = callbacks or []
        self._last_fired: dict[str, float] = {}
        self._history: list[AlertRecord] = []

    def register(self, rule: AlertRule) -> None:
        self._rules.append(rule)

    def check_all(self, metrics: dict) -> list[AlertRecord]:
        """Check all rules against metrics. Returns list of fired alerts."""
        fired: list[AlertRecord] = []
        now = time.time()

        for rule in self._rules:
            record = rule.check(metrics)
            if record is None:
                continue

            last = self._last_fired.get(rule.name, 0)
            if now - last < self._cooldown:
                continue  # suppressed

            self._last_fired[rule.name] = now
            self._history.append(record)
            fired.append(record)

            for cb in self._callbacks:
                try:
                    cb(record)
                except Exception as exc:
                    logger.warning("[alerts] Callback error: %s", exc)

        return fired

    def get_history(self, limit: int = 100) -> list[AlertRecord]:
        return self._history[-limit:]
