"""Tests for CLI alerts and apply subcommands — P4-3 / P4-4."""

from __future__ import annotations

import io
import tempfile
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace

from gateway.admission.cli import cmd_alerts, cmd_apply
from gateway.admission.controller import AdmissionController
from gateway.admission.templates import PolicyTemplate, TemplateStore


def _ctrl(tmp: Path) -> AdmissionController:
    return AdmissionController(db_path=tmp / "q.db", audit_dir=tmp / "audit")


class TestCmdAlerts:
    def test_alerts_empty_history(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ctrl = _ctrl(Path(tmpdir))
            args = SimpleNamespace(
                db_path=str(Path(tmpdir) / "q.db"),
                audit_dir=str(Path(tmpdir) / "audit"),
                limit=50,
            )
            buf = io.StringIO()
            with redirect_stdout(buf):
                cmd_alerts(args, controller=ctrl)
            assert "No alerts" in buf.getvalue()

    def test_alerts_shows_fired_alerts(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ctrl = _ctrl(Path(tmpdir))
            # Force an alert by checking with high depth
            ctrl._alert_manager.check_all({"pending_count": 60, **ctrl._metrics})
            args = SimpleNamespace(
                db_path=str(Path(tmpdir) / "q.db"),
                audit_dir=str(Path(tmpdir) / "audit"),
                limit=50,
            )
            buf = io.StringIO()
            with redirect_stdout(buf):
                cmd_alerts(args, controller=ctrl)
            out = buf.getvalue()
            assert "CRITICAL" in out or "WARNING" in out


class TestCmdApply:
    def test_apply_template_from_store(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ctrl = _ctrl(Path(tmpdir))
            store_dir = Path(tmpdir) / "templates"
            store = TemplateStore(store_dir=store_dir)
            store.save(PolicyTemplate(
                name="test-tpl", description="test",
                rate_limit_per_user=7, depth_warning=3, depth_critical=15,
            ))
            args = SimpleNamespace(
                name="test-tpl",
                store_dir=str(store_dir),
                db_path=str(Path(tmpdir) / "q.db"),
                audit_dir=str(Path(tmpdir) / "audit"),
            )
            buf = io.StringIO()
            with redirect_stdout(buf):
                cmd_apply(args, controller=ctrl)
            assert "Applied" in buf.getvalue()
            assert ctrl._rate_limit == 7

    def test_apply_nonexistent_template(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ctrl = _ctrl(Path(tmpdir))
            args = SimpleNamespace(
                name="nope",
                store_dir=str(Path(tmpdir) / "templates"),
                db_path=str(Path(tmpdir) / "q.db"),
                audit_dir=str(Path(tmpdir) / "audit"),
            )
            buf = io.StringIO()
            with redirect_stdout(buf):
                cmd_apply(args, controller=ctrl)
            assert "not found" in buf.getvalue()
