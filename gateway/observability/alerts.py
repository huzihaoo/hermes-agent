"""Alert rules for observability — cost threshold and error rate alerts.

Provides AlertRule definitions and an AlertChecker that evaluates rules
against TraceStore data.
"""

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional

from gateway.observability.store import TraceStore


class AlertMetric(Enum):
    COST = "cost"
    ERROR_RATE = "error_rate"


class AlertSeverity(Enum):
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass
class AlertRule:
    """A single alert rule definition."""
    name: str
    metric: AlertMetric
    threshold: float
    window_hours: int = 24
    severity: AlertSeverity = AlertSeverity.WARNING

    def describe(self) -> str:
        if self.metric == AlertMetric.COST:
            return f"{self.name}: cost > ${self.threshold:.2f} in {self.window_hours}h"
        return f"{self.name}: error_rate > {self.threshold:.0%} in {self.window_hours}h"


@dataclass
class AlertResult:
    """Result of evaluating an alert rule."""
    rule: AlertRule
    triggered: bool
    current_value: float
    details: str = ""


# Default rules — can be overridden via config
DEFAULT_RULES: List[AlertRule] = [
    AlertRule("daily_cost_warning", AlertMetric.COST, threshold=5.0, window_hours=24, severity=AlertSeverity.WARNING),
    AlertRule("daily_cost_critical", AlertMetric.COST, threshold=20.0, window_hours=24, severity=AlertSeverity.CRITICAL),
    AlertRule("error_rate_warning", AlertMetric.ERROR_RATE, threshold=0.3, window_hours=24, severity=AlertSeverity.WARNING),
    AlertRule("error_rate_critical", AlertMetric.ERROR_RATE, threshold=0.5, window_hours=24, severity=AlertSeverity.CRITICAL),
]


class AlertChecker:
    """Evaluate alert rules against a TraceStore."""

    def __init__(self, store: TraceStore, rules: Optional[List[AlertRule]] = None):
        self.store = store
        self.rules = rules or list(DEFAULT_RULES)

    def check_all(self) -> List[AlertResult]:
        """Evaluate all rules and return results."""
        results = []
        for rule in self.rules:
            results.append(self._check_rule(rule))
        return results

    def check_triggered(self) -> List[AlertResult]:
        """Return only triggered alerts."""
        return [r for r in self.check_all() if r.triggered]

    def _check_rule(self, rule: AlertRule) -> AlertResult:
        cutoff = time.time() - (rule.window_hours * 3600)

        if rule.metric == AlertMetric.COST:
            return self._check_cost(rule, cutoff)
        elif rule.metric == AlertMetric.ERROR_RATE:
            return self._check_error_rate(rule, cutoff)
        return AlertResult(rule=rule, triggered=False, current_value=0.0, details="unknown metric")

    def _check_cost(self, rule: AlertRule, cutoff: float) -> AlertResult:
        import sqlite3
        conn = sqlite3.connect(self.store.db_path)
        row = conn.execute(
            "SELECT COALESCE(SUM(total_cost_usd), 0) FROM traces WHERE start_time >= ?",
            (cutoff,),
        ).fetchone()
        conn.close()
        total_cost = row[0] if row else 0.0
        triggered = total_cost > rule.threshold
        return AlertResult(
            rule=rule,
            triggered=triggered,
            current_value=total_cost,
            details=f"${total_cost:.4f} / ${rule.threshold:.2f} threshold",
        )

    def _check_error_rate(self, rule: AlertRule, cutoff: float) -> AlertResult:
        import sqlite3
        conn = sqlite3.connect(self.store.db_path)
        row = conn.execute(
            """SELECT
                COUNT(*) as total,
                SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as errors
            FROM traces WHERE start_time >= ?""",
            (cutoff,),
        ).fetchone()
        conn.close()
        total = row[0] if row else 0
        errors = row[1] if row else 0
        if total == 0:
            return AlertResult(rule=rule, triggered=False, current_value=0.0, details="no traces in window")
        rate = errors / total
        triggered = rate > rule.threshold
        return AlertResult(
            rule=rule,
            triggered=triggered,
            current_value=rate,
            details=f"{errors}/{total} errors ({rate:.1%}) / {rule.threshold:.0%} threshold",
        )
