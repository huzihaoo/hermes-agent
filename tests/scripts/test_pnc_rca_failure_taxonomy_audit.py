from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys

from gateway.pnc_rca_delivery_store import RcaDeliveryStore
from scripts import pnc_fault_taxonomy
from scripts import pnc_rca_failure_taxonomy_audit as audit
from tests.gateway.test_pnc_rca_delivery_store import NOW, _control


def _db(tmp_path, rows):
    path = tmp_path / "control.sqlite3"
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE rca_execution_watch(
            submission_key TEXT PRIMARY KEY,
            last_status_json TEXT
        );
        CREATE TABLE rca_delivery_jobs(
            delivery_id TEXT PRIMARY KEY,
            submission_key TEXT,
            terminal_error_code TEXT,
            outcome TEXT,
            created_at TEXT
        );
        """
    )
    for index, (code, taxonomy) in enumerate(rows):
        submission = f"submission-{index}"
        conn.execute(
            "INSERT INTO rca_execution_watch VALUES (?, ?)",
            (submission, json.dumps({"failure_taxonomy": taxonomy})),
        )
        conn.execute(
            "INSERT INTO rca_delivery_jobs VALUES (?, ?, ?, 'terminal_failed', ?)",
            (f"delivery-{index}", submission, code, "2026-07-25T10:16:00+00:00"),
        )
    conn.commit()
    conn.close()
    return path


def _routed_db(tmp_path):
    reference_sha256 = "a" * 64
    for index in range(3):
        _control(
            tmp_path,
            offset=20 + index,
            issue_id=7041712900 + index,
        )
    store = RcaDeliveryStore(tmp_path / "control.sqlite3")
    assert store.backfill_completed_submissions(now=NOW) == 3
    cases = [
        {"kind": "translate_workdir_permission", "retryable": True},
        {
            "kind": "remote_event_not_found",
            "retryable": False,
            "audit": {
                "parse_attempts": [
                    {
                        "attempt_id": "parse-attempt-1",
                        "parser": "remote_event_reader",
                        "status": "parsed",
                        "reference_sha256": reference_sha256,
                    }
                ],
                "data_sources": [
                    {
                        "source_id": "data-source-1",
                        "source_kind": "pdcl_event",
                        "status": "not_found",
                        "reference_sha256": reference_sha256,
                    }
                ],
                "results": [
                    {
                        "attempt_id": "parse-attempt-1",
                        "source_id": "data-source-1",
                        "status": "not_found",
                        "returned_count": 0,
                        "reference_sha256": reference_sha256,
                    }
                ],
            },
        },
        {"kind": "html_capability_payload_mismatch", "retryable": False},
    ]
    owners = {
        pnc_fault_taxonomy.INFRA_SELF_HEALABLE: "rca-infra",
        pnc_fault_taxonomy.NEEDS_HUMAN_INPUT: "rca-triage",
        pnc_fault_taxonomy.HARD_DEFECT: "rca-engineering",
    }
    for blocker in cases:
        claim = store.claim_due_watch(lease_owner="audit-test", now=NOW)
        assert claim is not None
        decision = pnc_fault_taxonomy.decide_failure(blocker)
        route = store.upsert_failure_route(
            claim=claim,
            terminal_error_code=decision.terminal_error_code,
            lane=decision.lane,
            route_kind=decision.internal_route,
            owner=owners[decision.lane],
            work_started_at=claim.work_started_at,
            deadline_at=(NOW + timedelta(seconds=1800)).isoformat(),
            audit={
                "schema_version": "pnc_rca_failure_route_audit_v1",
                "taxonomy_audit": decision.audit,
                "contract_errors": list(decision.contract_errors),
                "source": "audit_test",
                "receipt": {},
            },
            route_payload={
                "schema_version": "pnc_rca_failure_route_payload_v1",
                "decision": decision.as_dict(),
                "remediation": {},
                "blocker": blocker,
            },
            now=NOW,
        )
        next_poll = NOW + timedelta(seconds=60)
        store.reschedule_failure_route(
            claim=claim,
            route_key=route.route_key,
            next_retry_at=next_poll,
            now=NOW,
        )
        store.reschedule_watch(
            submission_key=claim.submission_key,
            lease_token=claim.lease_token,
            observed_state=claim.state,
            status={"failure_taxonomy": decision.as_dict()},
            next_poll_at=next_poll,
            error_code=decision.terminal_error_code,
            now=NOW,
        )
    return store.db_path


def test_read_only_report_recomputes_three_lanes_and_event_audit(tmp_path):
    path = _routed_db(tmp_path)
    before = path.stat()

    report = audit.build_report(path, baseline=(NOW - timedelta(seconds=1)).isoformat())

    after = path.stat()
    assert report["ga_acceptance_ready"] is True
    assert report["lanes"] == {
        "hard_defect": 1,
        "infra_self_healable": 1,
        "needs_human_input": 1,
    }
    assert report["remote_event_not_found"] == {
        "rows": 1,
        "audit_complete_rows": 1,
    }
    assert report["post_baseline"]["failure_route_rows"] == 3
    assert report["post_baseline"]["terminal_rows"] == 0
    assert report["route_contract_errors"] == []
    assert report["failure_route_schema_errors"] == []
    assert (before.st_ino, before.st_size, before.st_mtime_ns) == (
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    )


def test_malformed_nonempty_remote_event_audit_fails_route_gate(tmp_path):
    path = _routed_db(tmp_path)
    with sqlite3.connect(path) as conn:
        [row] = conn.execute(
            "SELECT route_key, audit_json FROM rca_failure_routes "
            "WHERE terminal_error_code = 'remote_event_not_found'"
        ).fetchall()
        payload = json.loads(row[1])
        payload["taxonomy_audit"] = {
            "parse_attempts": [{}],
            "data_sources": [{}],
            "results": [{}],
        }
        conn.execute(
            "UPDATE rca_failure_routes SET audit_json = ? WHERE route_key = ?",
            (json.dumps(payload), row[0]),
        )

    report = audit.build_report(path, baseline=(NOW - timedelta(seconds=1)).isoformat())

    assert report["ga_acceptance_ready"] is False
    assert "remote_event_not_found_audit_incomplete" in report["gate_errors"]
    assert "durable_route_contract_invalid" in report["gate_errors"]


def test_terminal_route_audit_requires_bound_oracle_v2_effect(tmp_path):
    path = _routed_db(tmp_path)
    store = RcaDeliveryStore(path)
    with sqlite3.connect(path) as conn:
        hard = conn.execute(
            "SELECT submission_key FROM rca_failure_routes WHERE lane = 'hard_defect'"
        ).fetchone()
        assert hard is not None
        conn.execute(
            "UPDATE rca_execution_watch SET next_poll_at = ? WHERE submission_key != ?",
            ((NOW + timedelta(days=1)).isoformat(), hard[0]),
        )
    claim = store.claim_due_watch(
        lease_owner="terminal-audit-test",
        now=NOW + timedelta(seconds=1800),
    )
    assert claim is not None
    [route] = [
        row
        for row in store.list_rows("rca_failure_routes")
        if row["submission_key"] == claim.submission_key
    ]
    fallback = {
        "schema_version": "pnc_rca_bounded_terminal_fallback_v1",
        "work_started_at": route["work_started_at"],
        "deadline_at": route["deadline_at"],
        "elapsed_seconds": 1800,
        "confidence_tier": "low",
        "terminal_class": "honest_non_attribution",
        "route_key": route["route_key"],
        "route_kind": route["route_kind"],
        "route_owner": route["owner"],
    }
    store.create_terminal_delivery(
        claim=claim,
        status={
            "failure_taxonomy": json.loads(route["route_payload_json"])["decision"]
        },
        outcome="terminal_failed",
        terminal_state="failed",
        error_code=route["terminal_error_code"],
        error_detail="private detail",
        terminal_fallback=fallback,
        now=NOW + timedelta(seconds=1800),
    )
    report = audit.build_report(path, baseline=(NOW - timedelta(seconds=1)).isoformat())

    assert report["ga_acceptance_ready"] is True
    assert report["post_baseline"]["terminal_route_effects"] == 1
    assert report["route_statuses"]["terminal_fallback"] == 1
    assert report["route_contract_errors"] == []


def test_historical_unclassified_without_source_is_not_guessed(tmp_path):
    path = _db(tmp_path, [("vm_terminal_failed_unclassified", {})])

    report = audit.build_report(path)

    assert report["historical"]["unclassified_rows"] == 1
    assert report["historical"]["evidence_unrecoverable_rows"] == 1
    assert report["historical"]["history_rewrite_policy"] == "forbidden"


def test_terminal_delivery_without_execution_watch_is_not_hidden(tmp_path):
    path = _db(tmp_path, [])
    with sqlite3.connect(path) as conn:
        conn.execute(
            "INSERT INTO rca_delivery_jobs VALUES (?, ?, ?, 'terminal_failed', ?)",
            (
                "delivery-orphan-watch",
                "submission-orphan-watch",
                "vm_terminal_failed_unclassified",
                "2026-07-25T10:16:00+00:00",
            ),
        )

    report = audit.build_report(path)

    assert report["historical"]["terminal_rows"] == 1
    assert report["historical"]["unclassified_rows"] == 1
    assert report["historical"]["evidence_unrecoverable_rows"] == 1
    assert report["historical"]["evidence_unrecoverable_submission_keys"] == [
        "submission-orphan-watch"
    ]


def test_missing_route_table_keeps_all_three_live_gates_fail_closed(tmp_path):
    path = _db(tmp_path, [])

    report = audit.build_report(path)

    assert report["failure_route_table_present"] is False
    assert report["failure_route_schema_errors"] == []
    assert report["gate_errors"] == [
        "failure_route_table_missing",
        "no_post_baseline_durable_route_evidence",
        "three_lane_live_evidence_incomplete",
    ]
    assert report["ga_acceptance_ready"] is False


def test_unknown_injection_cli_is_nonzero_and_emits_taxonomy_gap(capsys):
    result = audit.main(["--inject-unknown", "brand-new-vm-code"])

    payload = json.loads(capsys.readouterr().out)
    assert result == 2
    assert payload["ok"] is False
    assert payload["decision"]["terminal_error_code"] == (
        "taxonomy_gap:brand-new-vm-code"
    )


def test_system_python_help_and_injection_are_dependency_light():
    interpreter = shutil.which("python3")
    assert interpreter is not None
    script = str(Path(audit.__file__).resolve())

    help_result = subprocess.run(
        [interpreter, script, "--help"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert help_result.returncode == 0
    assert "Read-only W2 migration/audit report" in help_result.stdout

    injection_result = subprocess.run(
        [interpreter, script, "--inject-unknown", "system-python-gap"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert injection_result.returncode == 2
    payload = json.loads(injection_result.stdout)
    assert payload["decision"]["terminal_error_code"] == (
        "taxonomy_gap:system-python-gap"
    )


def test_schema_index_injection_cli_is_nonzero_and_fail_closed(tmp_path, capsys):
    path = _routed_db(tmp_path)
    with sqlite3.connect(path) as conn:
        conn.execute("DROP INDEX idx_failure_routes_status")

    result = audit.main([
        "--db",
        str(path),
        "--baseline",
        (NOW - timedelta(seconds=1)).isoformat(),
        "--gate-new",
    ])

    payload = json.loads(capsys.readouterr().out)
    assert result == 1
    assert payload["ga_acceptance_ready"] is False
    assert "durable_route_schema_invalid" in payload["gate_errors"]
    assert payload["failure_route_schema_errors"] == ["index_contract"]
    assert payload["post_baseline"]["failure_route_rows"] == 0
