"""Tests for /metrics HTTP endpoint — P4-2."""

from __future__ import annotations

import tempfile
import threading
import time
import urllib.request
from pathlib import Path

import pytest

from gateway.admission.controller import AdmissionController
from gateway.admission.metrics_export import MetricsExporter
from gateway.admission.metrics_server import MetricsServer


def _ctrl(tmp: Path) -> AdmissionController:
    return AdmissionController(db_path=tmp / "q.db", audit_dir=tmp / "audit")


class TestMetricsServer:
    def test_server_starts_and_responds(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ctrl = _ctrl(Path(tmpdir))
            exporter = MetricsExporter(ctrl)
            server = MetricsServer(exporter, port=0)  # port=0 → OS picks free port
            server.start()
            try:
                port = server.port
                url = f"http://127.0.0.1:{port}/metrics"
                resp = urllib.request.urlopen(url, timeout=3)
                body = resp.read().decode()
                assert resp.status == 200
                assert "admission_total_admitted" in body
                assert "admission_queue_depth" in body
            finally:
                server.stop()

    def test_non_metrics_path_returns_404(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ctrl = _ctrl(Path(tmpdir))
            exporter = MetricsExporter(ctrl)
            server = MetricsServer(exporter, port=0)
            server.start()
            try:
                port = server.port
                url = f"http://127.0.0.1:{port}/other"
                try:
                    urllib.request.urlopen(url, timeout=3)
                    assert False, "Should have raised"
                except urllib.error.HTTPError as e:
                    assert e.code == 404
            finally:
                server.stop()

    def test_server_stop_is_idempotent(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ctrl = _ctrl(Path(tmpdir))
            exporter = MetricsExporter(ctrl)
            server = MetricsServer(exporter, port=0)
            server.start()
            server.stop()
            server.stop()  # should not raise
