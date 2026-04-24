"""Tests for audit trail."""

import json
import tempfile
from pathlib import Path

from gateway.admission.audit import AuditEvent, log_audit


def test_audit_log_writes_jsonl():
    with tempfile.TemporaryDirectory() as tmpdir:
        audit_dir = Path(tmpdir)

        event = AuditEvent(
            user_id="user-1",
            action="execute_task",
            resource="task-123",
            result="allowed",
            metadata={"role": "owner"},
        )

        log_audit(event, audit_dir=audit_dir)

        log_files = list(audit_dir.glob("*.jsonl"))
        assert len(log_files) == 1

        with log_files[0].open() as f:
            line = f.readline()
            data = json.loads(line)
            assert data["user_id"] == "user-1"
            assert data["action"] == "execute_task"
            assert data["result"] == "allowed"
            assert data["metadata"] == {"role": "owner"}
            assert "timestamp" in data


def test_audit_appends_multiple_lines():
    with tempfile.TemporaryDirectory() as tmpdir:
        audit_dir = Path(tmpdir)

        log_audit(AuditEvent(
            user_id="u1",
            action="enqueue_task",
            resource="q1",
            result="queued",
        ), audit_dir=audit_dir)

        log_audit(AuditEvent(
            user_id="u1",
            action="complete_task",
            resource="q1",
            result="completed",
        ), audit_dir=audit_dir)

        log_files = list(audit_dir.glob("*.jsonl"))
        assert len(log_files) == 1

        with log_files[0].open() as f:
            lines = f.readlines()
            assert len(lines) == 2
