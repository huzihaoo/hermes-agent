import json
import sqlite3
from datetime import datetime, timezone

import pytest

from gateway import pnc_rca_same_task_resume as resume
from gateway import pnc_rca_same_task_watch_rearm as rearm
from scripts import pnc_rca_same_task_recovery as recovery


NOW = datetime(2026, 8, 27, 6, 0, tzinfo=timezone.utc)


def _db(tmp_path):
    path = tmp_path / "control.sqlite3"
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE business_triggers(
                business_key TEXT NOT NULL,
                generation INTEGER NOT NULL,
                work_item_id TEXT NOT NULL,
                PRIMARY KEY(business_key, generation)
            );
            CREATE TABLE rca_outbox(
                outbox_id INTEGER PRIMARY KEY,
                submission_key TEXT NOT NULL,
                business_key TEXT NOT NULL,
                generation INTEGER NOT NULL,
                status TEXT NOT NULL
            );
            CREATE TABLE rca_execution_watch(
                submission_key TEXT PRIMARY KEY,
                submission_outbox_id INTEGER NOT NULL,
                business_key TEXT NOT NULL,
                generation INTEGER NOT NULL,
                project_key TEXT NOT NULL,
                work_item_type_key TEXT NOT NULL,
                work_item_id TEXT NOT NULL,
                task_id TEXT NOT NULL,
                state TEXT NOT NULL,
                poll_attempt INTEGER NOT NULL,
                next_poll_at TEXT NOT NULL,
                last_observed_at TEXT,
                terminal_at TEXT,
                terminal_first_seen_at TEXT,
                fence INTEGER NOT NULL,
                lease_token TEXT,
                lease_owner TEXT,
                lease_expires_at TEXT,
                last_status_json TEXT,
                last_error_code TEXT,
                last_error_detail TEXT,
                delivery_id TEXT,
                created_at TEXT,
                updated_at TEXT
            );
            CREATE TABLE rca_delivery_jobs(
                delivery_id TEXT PRIMARY KEY,
                submission_key TEXT NOT NULL
            );
            """
        )
        conn.execute(
            "INSERT INTO business_triggers VALUES(?, ?, ?)",
            (
                resume.AUTHORIZED_BUSINESS_KEY,
                resume.AUTHORIZED_GENERATION,
                resume.AUTHORIZED_ISSUE_ID,
            ),
        )
        conn.execute(
            "INSERT INTO rca_outbox VALUES(1, ?, ?, ?, 'completed')",
            (
                resume.AUTHORIZED_TASK_ID,
                resume.AUTHORIZED_BUSINESS_KEY,
                resume.AUTHORIZED_GENERATION,
            ),
        )
        conn.execute(
            """
            INSERT INTO rca_execution_watch VALUES(
                ?, 1, ?, ?, 'project', 'issue', ?, ?, 'terminal_failed',
                21, '2026-08-27T05:00:00+00:00',
                '2026-08-27T05:00:00+00:00',
                '2026-08-27T05:00:00+00:00',
                '2026-08-27T04:30:00+00:00', 21,
                NULL, NULL, NULL, ?, ?, 'detail', NULL,
                '2026-08-27T04:00:00+00:00',
                '2026-08-27T05:00:00+00:00'
            )
            """,
            (
                resume.AUTHORIZED_TASK_ID,
                resume.AUTHORIZED_BUSINESS_KEY,
                resume.AUTHORIZED_GENERATION,
                resume.AUTHORIZED_ISSUE_ID,
                resume.AUTHORIZED_TASK_ID,
                json.dumps({"old": "terminal"}),
                resume.SUPPORTED_BLOCKER,
            ),
        )
        conn.commit()
    return path


def _remediation_result():
    return {
        "schema_version": resume.INFRA_REMEDIATION_SCHEMA_VERSION,
        "success": True,
        "status": "succeeded",
        "submission_key": resume.AUTHORIZED_TASK_ID,
        "business_key": resume.AUTHORIZED_BUSINESS_KEY,
        "generation": resume.AUTHORIZED_GENERATION,
        "task_id": resume.AUTHORIZED_TASK_ID,
        "operation": resume.SUPPORTED_OPERATION,
        "blocker_kind": resume.SUPPORTED_BLOCKER,
        "resumed_same_task": True,
        "external_writes": False,
        "timeout_seconds": 90,
        "error_code": "",
    }


def test_rearm_is_exact_idempotent_and_expedited_after_vm_success(tmp_path):
    db = _db(tmp_path)

    before = rearm.preflight(db)
    first = rearm.rearm(db, defer_seconds=120, now=NOW)
    second = rearm.rearm(db, defer_seconds=120, now=NOW)
    expedited = rearm.expedite(
        db,
        rearm_token=first["rearm_token"],
        remediation_result=_remediation_result(),
        now=NOW,
    )

    assert before["state"] == "terminal_failed"
    assert first["created"] is True
    assert first["state"] == "pending"
    assert first["next_poll_at"] == "2026-08-27T06:02:00+00:00"
    assert first["terminal_at"] == ""
    assert second["created"] is False
    assert second["rearm_token"] == first["rearm_token"]
    assert expedited["phase"] == "vm_resume_succeeded"
    assert expedited["next_poll_at"] == NOW.isoformat()
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM rca_execution_watch").fetchone()[0] == 1
        row = conn.execute(
            "SELECT state, generation, delivery_id FROM rca_execution_watch"
        ).fetchone()
    assert row == ("pending", 7, None)


def test_rearm_rejects_higher_generation(tmp_path):
    db = _db(tmp_path)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO rca_outbox VALUES(2, 'generation-8', ?, 8, 'completed')",
            (resume.AUTHORIZED_BUSINESS_KEY,),
        )
        conn.commit()

    with pytest.raises(rearm.WatchRearmError, match="watch_rearm_higher_generation_exists"):
        rearm.preflight(db)


def test_operator_apply_rearms_then_expedites(tmp_path, monkeypatch, capsys):
    db = _db(tmp_path)
    monkeypatch.setattr(recovery, "resume_same_task", lambda *_args, **_kwargs: _remediation_result())

    code = recovery.main(
        [
            "--apply",
            "--db-path",
            str(db),
            "--env-file",
            str(tmp_path / "missing.env"),
        ]
    )

    output = json.loads(capsys.readouterr().out)
    assert code == 0
    assert output["ok"] is True
    assert output["external_writes"] is False
    assert output["created_task_ids"] == []
    assert output["expedited"]["phase"] == "vm_resume_succeeded"
