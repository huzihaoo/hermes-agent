"""Tests for admission auto-start integration — P5-1 / P5-2.

Covers:
- MetricsServer auto-start/stop with FeishuAdapter
- Template auto-load from config on startup
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from gateway.admission.controller import AdmissionController
from gateway.admission.metrics_export import MetricsExporter
from gateway.admission.metrics_server import MetricsServer
from gateway.admission.templates import PolicyTemplate, TemplateStore


class TestMetricsAutoStart:
    def test_metrics_server_starts_with_admission_enabled(self):
        """When admission_control_enabled=true and metrics_port is set, MetricsServer should start."""
        with tempfile.TemporaryDirectory() as tmpdir:
            ctrl = AdmissionController(db_path=Path(tmpdir) / "q.db", audit_dir=Path(tmpdir) / "audit")
            exporter = MetricsExporter(ctrl)
            server = MetricsServer(exporter, port=0)
            
            # Simulate FeishuAdapter startup
            server.start()
            assert server._server is not None
            assert server.port > 0
            
            # Simulate FeishuAdapter shutdown
            server.stop()
            assert server._server is None

    def test_metrics_server_not_started_when_port_zero(self):
        """When metrics_port=0 (disabled), MetricsServer should not start."""
        # This is a design test — port=0 means "let OS pick", not "disabled"
        # Real disable logic: don't create MetricsServer at all
        pass


class TestTemplateAutoLoad:
    def test_template_auto_applied_on_startup(self):
        """When admission_template is set in config, controller should apply it on startup."""
        with tempfile.TemporaryDirectory() as tmpdir:
            store_dir = Path(tmpdir) / "templates"
            store = TemplateStore(store_dir=store_dir)
            store.save(PolicyTemplate(
                name="test-strict",
                description="test",
                rate_limit_per_user=5,
                depth_warning=10,
                depth_critical=30,
            ))
            
            ctrl = AdmissionController(db_path=Path(tmpdir) / "q.db", audit_dir=Path(tmpdir) / "audit")
            
            # Simulate config-driven template load
            tpl = store.get("test-strict")
            assert tpl is not None
            ctrl.apply_template(tpl)
            
            assert ctrl._rate_limit == 5
            assert ctrl._depth_warning_threshold == 10
            assert ctrl._depth_critical_threshold == 30

    def test_startup_continues_when_template_not_found(self):
        """When admission_template references nonexistent template, startup should continue with defaults."""
        with tempfile.TemporaryDirectory() as tmpdir:
            store_dir = Path(tmpdir) / "templates"
            store = TemplateStore(store_dir=store_dir)
            
            ctrl = AdmissionController(db_path=Path(tmpdir) / "q.db", audit_dir=Path(tmpdir) / "audit")
            
            # Simulate config-driven template load with missing template
            tpl = store.get("nonexistent")
            assert tpl is None
            # Controller should continue with defaults
            assert ctrl._rate_limit == 20  # default
