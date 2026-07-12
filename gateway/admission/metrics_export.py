"""Prometheus-compatible metrics exporter for admission control.

Generates text in Prometheus exposition format (text/plain; version=0.0.4).
No dependency on prometheus_client — pure string generation.
"""

from __future__ import annotations

from .types import ALL_DOMAINS, ALL_LANES


class MetricsExporter:
    """Export admission metrics in Prometheus text format."""

    def __init__(self, controller):
        self._ctrl = controller

    def export(self) -> str:
        lines: list[str] = []

        # Counters from _metrics
        counter_map = {
            "admission_total_admitted": ("Total items admitted to queue", "total_admitted"),
            "admission_total_rejected": ("Total items rejected (rate limit)", "total_rejected"),
            "admission_total_completed": ("Total items completed", "total_completed"),
            "admission_total_failed": ("Total items failed", "total_failed"),
            "admission_total_retried": ("Total items retried", "total_retried"),
            "admission_total_dead": ("Total items dead-lettered", "total_dead"),
        }
        for prom_name, (help_text, key) in counter_map.items():
            val = self._ctrl._metrics.get(key, 0)
            lines.append(f"# HELP {prom_name} {help_text}")
            lines.append(f"# TYPE {prom_name} counter")
            lines.append(f"{prom_name} {val}")

        # Queue depth gauges per domain/lane
        lines.append("# HELP admission_queue_depth Current pending items in queue")
        lines.append("# TYPE admission_queue_depth gauge")
        for domain in ALL_DOMAINS:
            for lane in ALL_LANES:
                depth = self._ctrl.queue.pending_count(lane=lane, domain=domain)
                lines.append(
                    f'admission_queue_depth{{domain="{domain}",lane="{lane}"}} {depth}'
                )

        # Alert history count
        history = self._ctrl.get_alert_history()
        lines.append("# HELP admission_alerts_fired_total Total alerts fired")
        lines.append("# TYPE admission_alerts_fired_total counter")
        lines.append(f"admission_alerts_fired_total {len(history)}")

        return "\n".join(lines) + "\n"
