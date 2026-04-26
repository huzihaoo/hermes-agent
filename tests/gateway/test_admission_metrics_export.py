"""Tests for Prometheus metrics exporter — P3-4.

Covers:
- MetricsExporter generates valid Prometheus text format
- Counters from controller._metrics are exported
- Queue depth gauges per lane/domain are exported
- Alert history count is exported
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from unittest.mock import patch

from gateway.admission.controller import AdmissionController
from gateway.admission.metrics_export import MetricsExporter


def _ctrl(tmp: Path) -> AdmissionController:
    return AdmissionController(db_path=tmp / "q.db", audit_dir=tmp / "audit")


class TestMetricsExporter:
    def test_export_returns_string(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ctrl = _ctrl(Path(tmpdir))
            exporter = MetricsExporter(ctrl)
            text = exporter.export()
            assert isinstance(text, str)
            assert len(text) > 0

    def test_counters_present(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ctrl = _ctrl(Path(tmpdir))
            exporter = MetricsExporter(ctrl)
            text = exporter.export()
            assert "admission_total_admitted" in text
            assert "admission_total_completed" in text
            assert "admission_total_failed" in text
            assert "admission_total_retried" in text
            assert "admission_total_dead" in text

    def test_queue_depth_gauges(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ctrl = _ctrl(Path(tmpdir))
            import asyncio
            with patch("gateway.admission.controller._resolve_role", return_value="member"):
                asyncio.get_event_loop().run_until_complete(
                    ctrl.admit("u1", "hello", chat_id="c1")
                )
            exporter = MetricsExporter(ctrl)
            text = exporter.export()
            assert "admission_queue_depth" in text

    def test_alert_history_count(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ctrl = _ctrl(Path(tmpdir))
            exporter = MetricsExporter(ctrl)
            text = exporter.export()
            assert "admission_alerts_fired_total" in text

    def test_format_is_prometheus_compatible(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ctrl = _ctrl(Path(tmpdir))
            exporter = MetricsExporter(ctrl)
            text = exporter.export()
            for line in text.strip().split("\n"):
                if line.startswith("#"):
                    assert line.startswith("# HELP") or line.startswith("# TYPE")
                elif line.strip():
                    # metric_name{labels} value
                    parts = line.split()
                    assert len(parts) >= 2
                    float(parts[-1])  # value must be numeric
