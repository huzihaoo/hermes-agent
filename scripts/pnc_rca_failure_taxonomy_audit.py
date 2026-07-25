#!/usr/bin/env python3
"""Read-only W2 migration/audit report for RCA terminal failures."""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import sys
import tempfile
from typing import Any, Mapping

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import pnc_fault_taxonomy


SCHEMA_VERSION = "pnc_rca_failure_taxonomy_audit_v2"
DEFAULT_DB = (
    Path.home() / ".hermes/runtime/pnc_agent/feishu_issue_kafka_rca/control.sqlite3"
)
DEFAULT_BASELINE = "2026-07-25T10:15:43+00:00"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_object(value: Any) -> dict[str, Any]:
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _unclassified(code: str) -> bool:
    return str(code or "").strip().lower().endswith("_unclassified")


def _taxonomy_projection(status: Mapping[str, Any]) -> dict[str, Any]:
    persisted = status.get("failure_taxonomy")
    if isinstance(persisted, Mapping):
        return dict(persisted)
    blocker = status.get("blocker")
    if isinstance(blocker, Mapping):
        return pnc_fault_taxonomy.decide_failure(blocker).as_dict()
    return {}


def _connect_readonly(path: Path) -> sqlite3.Connection:
    resolved = path.expanduser().resolve(strict=True)
    conn = sqlite3.connect(
        f"file:{resolved.as_posix()}?mode=ro",
        uri=True,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    return conn


def _sqlite_identities(path: Path) -> dict[str, tuple[int, int, int] | None]:
    identities: dict[str, tuple[int, int, int] | None] = {}
    for label, candidate in (
        ("main", path),
        ("wal", Path(f"{path}-wal")),
        ("shm", Path(f"{path}-shm")),
    ):
        try:
            observed = candidate.stat()
        except FileNotFoundError:
            identities[label] = None
        else:
            identities[label] = (
                observed.st_ino,
                observed.st_size,
                observed.st_mtime_ns,
            )
    return identities


def _canonical_sha256(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        dict(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table,),
        ).fetchone()
        is not None
    )


def build_report(db_path: Path, *, baseline: str = DEFAULT_BASELINE) -> dict[str, Any]:
    resolved_db = db_path.expanduser().resolve(strict=True)
    before = resolved_db.stat()
    before_sqlite = _sqlite_identities(resolved_db)
    with tempfile.TemporaryDirectory(prefix="pnc-rca-taxonomy-audit-") as temp_dir:
        snapshot_db = Path(temp_dir) / resolved_db.name
        shutil.copyfile(resolved_db, snapshot_db)
        source_wal = Path(f"{resolved_db}-wal")
        if before_sqlite["wal"] is not None:
            shutil.copyfile(source_wal, Path(f"{snapshot_db}-wal"))
        if before_sqlite != _sqlite_identities(resolved_db):
            raise RuntimeError("control_db_changed_during_read_only_audit")
        conn = _connect_readonly(snapshot_db)
        try:
            all_delivery_rows = int(
                conn.execute("SELECT COUNT(*) FROM rca_delivery_jobs").fetchone()[0]
            )
            all_delivery_codes = {
                str(row[0] or "<empty>"): int(row[1])
                for row in conn.execute(
                    """
                    SELECT terminal_error_code, COUNT(*)
                      FROM rca_delivery_jobs
                     GROUP BY terminal_error_code
                     ORDER BY terminal_error_code
                    """
                ).fetchall()
            }
            terminal_rows = conn.execute(
                """
                SELECT j.delivery_id, j.submission_key, j.terminal_error_code,
                       j.outcome, j.created_at, w.last_status_json
                  FROM rca_delivery_jobs AS j
                  JOIN rca_execution_watch AS w
                    ON w.submission_key = j.submission_key
                 WHERE j.outcome IN ('terminal_failed', 'quarantined')
                 ORDER BY j.created_at, j.delivery_id
                """
            ).fetchall()
            failure_route_table_present = _table_exists(conn, "rca_failure_routes")
            route_rows = (
                conn.execute(
                    """
                    SELECT r.*,
                           j.delivery_id, j.terminal_error_code AS job_error_code,
                           e.effect_key, e.payload_json AS effect_payload_json
                      FROM rca_failure_routes AS r
                      LEFT JOIN rca_delivery_jobs AS j
                        ON j.submission_key = r.submission_key
                      LEFT JOIN rca_delivery_effects AS e
                        ON e.delivery_id = j.delivery_id
                       AND e.effect_kind = 'feishu_issue_comment'
                       AND e.target_key = j.target_key
                     WHERE r.created_at > ?
                     ORDER BY r.created_at, r.route_key
                    """,
                    (baseline,),
                ).fetchall()
                if failure_route_table_present
                else []
            )
        finally:
            conn.close()
    after_sqlite = _sqlite_identities(resolved_db)
    if before_sqlite != after_sqlite:
        raise RuntimeError("control_db_changed_during_read_only_audit")

    existing_codes: Counter[str] = Counter()
    projected_codes: Counter[str] = Counter()
    historical_lanes: Counter[str] = Counter()
    historical_route_counts: Counter[str] = Counter()
    unrecoverable: list[str] = []
    new_terminal_rows = 0
    new_unclassified = 0
    new_taxonomy_gaps = 0
    historical_event_not_found = 0

    for row in terminal_rows:
        existing = str(row["terminal_error_code"] or "")
        existing_codes[existing or "<empty>"] += 1
        status = _json_object(row["last_status_json"])
        projection = _taxonomy_projection(status)
        projected = str(projection.get("terminal_error_code") or "")
        if not projected:
            if existing and not _unclassified(existing):
                projected = existing
            else:
                unrecoverable.append(str(row["submission_key"]))
                projected = "evidence_unrecoverable"
        projected_codes[projected] += 1
        lane = str(projection.get("lane") or "")
        route = str(projection.get("internal_route") or "")
        if lane:
            historical_lanes[lane] += 1
        if route:
            historical_route_counts[route] += 1
        if projected == "remote_event_not_found":
            historical_event_not_found += 1
        if str(row["created_at"] or "") > baseline:
            new_terminal_rows += 1
            if _unclassified(existing):
                new_unclassified += 1
            if projected.startswith("taxonomy_gap:"):
                new_taxonomy_gaps += 1

    expected_routes = {
        pnc_fault_taxonomy.INFRA_SELF_HEALABLE: (
            pnc_fault_taxonomy.INFRA_REMEDIATION_HOLD,
            "rca-infra",
            {
                "remediation_pending",
                "remediation_started",
                "remediation_succeeded",
                "remediation_held",
                "terminal_fallback",
                "resolved",
            },
        ),
        pnc_fault_taxonomy.NEEDS_HUMAN_INPUT: (
            pnc_fault_taxonomy.INTERNAL_BACKLOG,
            "rca-triage",
            {"backlog_pending", "terminal_fallback", "resolved"},
        ),
        pnc_fault_taxonomy.HARD_DEFECT: (
            pnc_fault_taxonomy.INTERNAL_ALERT,
            "rca-engineering",
            {"alert_pending", "terminal_fallback", "resolved"},
        ),
    }
    post_lanes: Counter[str] = Counter()
    post_routes: Counter[str] = Counter()
    post_statuses: Counter[str] = Counter()
    route_contract_errors: list[dict[str, str]] = []
    terminal_route_effects = 0
    post_taxonomy_gap_routes = 0
    event_not_found = 0
    event_not_found_audit_complete = 0
    for row in route_rows:
        route_key = str(row["route_key"] or "")
        submission_key = str(row["submission_key"] or "")
        code = str(row["terminal_error_code"] or "")
        lane = str(row["lane"] or "")
        route_kind = str(row["route_kind"] or "")
        owner = str(row["owner"] or "")
        status = str(row["status"] or "")
        if code.startswith("taxonomy_gap:"):
            post_taxonomy_gap_routes += 1
        post_lanes[lane] += 1
        post_routes[route_kind] += 1
        post_statuses[status] += 1
        expected = expected_routes.get(lane)
        expected_digest = hashlib.sha256(
            "\0".join((submission_key, code, lane, route_kind)).encode("utf-8")
        ).hexdigest()

        def invalid(reason: str) -> None:
            item = {"route_key": route_key, "error": reason}
            if item not in route_contract_errors:
                route_contract_errors.append(item)

        if (
            expected is None
            or route_kind != expected[0]
            or owner != expected[1]
            or status not in expected[2]
        ):
            invalid("route_owner_status_invalid")
        if (
            route_key != f"rca-failure-route-{expected_digest}"
            or str(row["dedupe_key"] or "") != expected_digest
        ):
            invalid("route_dedupe_identity_invalid")
        try:
            work_started = datetime.fromisoformat(
                str(row["work_started_at"] or "").replace("Z", "+00:00")
            )
            deadline = datetime.fromisoformat(
                str(row["deadline_at"] or "").replace("Z", "+00:00")
            )
            next_retry = (
                datetime.fromisoformat(str(row["next_retry_at"]).replace("Z", "+00:00"))
                if row["next_retry_at"]
                else None
            )
        except (TypeError, ValueError):
            invalid("route_work_window_invalid")
            work_started = deadline = next_retry = None
        if (
            work_started is None
            or deadline is None
            or work_started.tzinfo is None
            or work_started.utcoffset() is None
            or deadline.tzinfo is None
            or deadline.utcoffset() is None
            or int((deadline - work_started).total_seconds()) != 1800
            or (
                next_retry is not None
                and (
                    next_retry.tzinfo is None
                    or next_retry.utcoffset() is None
                    or next_retry > deadline
                )
            )
        ):
            invalid("route_work_window_invalid")
        if (
            type(row["observation_count"]) is not int
            or int(row["observation_count"]) < 1
            or type(row["retry_count"]) is not int
            or int(row["retry_count"]) < 0
            or int(row["remediation_attempt_count"]) not in {0, 1}
            or int(row["retry_exhausted"]) not in {0, 1}
        ):
            invalid("route_retry_state_invalid")
        audit = _json_object(row["audit_json"])
        payload = _json_object(row["route_payload_json"])
        decision = payload.get("decision")
        blocker = payload.get("blocker")
        if (
            audit.get("schema_version") != "pnc_rca_failure_route_audit_v1"
            or not isinstance(audit.get("contract_errors"), list)
            or not str(audit.get("source") or "")
            or not isinstance(audit.get("receipt"), Mapping)
            or not isinstance(decision, Mapping)
            or not isinstance(blocker, Mapping)
            or not str(blocker.get("kind") or "")
            or decision.get("terminal_error_code") != code
            or decision.get("lane") != lane
            or decision.get("internal_route") != route_kind
        ):
            invalid("route_audit_payload_invalid")
        if status not in {"terminal_fallback", "resolved"} and not str(
            row["next_retry_at"] or ""
        ):
            invalid("route_retry_schedule_missing")
        if int(row["remediation_attempt_count"]) == 1:
            remediation_result = _json_object(row["remediation_result_json"])
            if (
                remediation_result.get("submission_key") != submission_key
                or remediation_result.get("generation") != row["generation"]
                or remediation_result.get("task_id") != row["task_id"]
            ):
                invalid("route_remediation_result_invalid")
        if code == "remote_event_not_found":
            event_not_found += 1
            taxonomy_audit = audit.get("taxonomy_audit")
            try:
                pnc_fault_taxonomy.decide_failure(
                    {
                        "kind": code,
                        "retryable": False,
                        "audit": taxonomy_audit,
                    },
                    require_complete=True,
                )
            except pnc_fault_taxonomy.TaxonomyContractError:
                invalid("remote_event_not_found_audit_incomplete")
            else:
                event_not_found_audit_complete += 1
        if status == "terminal_fallback":
            effect_payload = _json_object(row["effect_payload_json"])
            oracle = effect_payload.get("quality_oracle")
            fallback = effect_payload.get("terminal_fallback")
            if (
                not row["delivery_id"]
                or not row["effect_key"]
                or row["job_error_code"] != code
                or not row["completed_at"]
                or effect_payload.get("schema_version")
                != "pnc_rca_terminal_delivery_effect_v3"
                or effect_payload.get("terminal_class") != "honest_non_attribution"
                or effect_payload.get("confidence_tier") != "low"
                or not isinstance(oracle, Mapping)
                or oracle.get("schema_version") != "pnc_rca_structural_tier_oracle_v2"
                or oracle.get("publication_allowed") is not True
                or oracle.get("classification_conflict") is not False
                or effect_payload.get("quality_oracle_sha256")
                != _canonical_sha256(oracle if isinstance(oracle, Mapping) else {})
                or not isinstance(fallback, Mapping)
                or fallback.get("route_key") != route_key
                or fallback.get("route_kind") != route_kind
                or fallback.get("route_owner") != owner
                or fallback.get("work_started_at") != row["work_started_at"]
                or fallback.get("deadline_at") != row["deadline_at"]
            ):
                invalid("terminal_fallback_effect_invalid")
            else:
                terminal_route_effects += 1

    missing_lanes = sorted(pnc_fault_taxonomy.FAULT_CLASSES - set(post_lanes))
    gate_errors: list[str] = []
    if not failure_route_table_present:
        gate_errors.append("failure_route_table_missing")
    if not route_rows:
        gate_errors.append("no_post_baseline_durable_route_evidence")
    if new_unclassified:
        gate_errors.append("post_baseline_unclassified_present")
    if new_taxonomy_gaps or post_taxonomy_gap_routes:
        gate_errors.append("post_baseline_taxonomy_gap_present")
    if event_not_found != event_not_found_audit_complete:
        gate_errors.append("remote_event_not_found_audit_incomplete")
    if missing_lanes:
        gate_errors.append("three_lane_live_evidence_incomplete")
    if route_contract_errors:
        gate_errors.append("durable_route_contract_invalid")

    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "read_only": True,
        "db": {
            "path": str(resolved_db),
            "open_mode": "source-byte-snapshot;mode=ro;query_only=ON;wal_aware=true",
            "inode": before.st_ino,
            "size": before.st_size,
            "mtime_ns": before.st_mtime_ns,
            "sqlite_identities": before_sqlite,
            "unchanged_during_audit": True,
        },
        "baseline": baseline,
        "historical": {
            "all_delivery_rows": all_delivery_rows,
            "all_delivery_terminal_error_codes": all_delivery_codes,
            "terminal_rows": len(terminal_rows),
            "existing_terminal_error_codes": dict(sorted(existing_codes.items())),
            "projected_terminal_error_codes": dict(sorted(projected_codes.items())),
            "unclassified_rows": sum(
                count for code, count in existing_codes.items() if _unclassified(code)
            ),
            "evidence_unrecoverable_rows": len(unrecoverable),
            "evidence_unrecoverable_submission_keys": unrecoverable[:25],
            "history_rewrite_policy": "forbidden",
            "lanes": dict(sorted(historical_lanes.items())),
            "internal_routes": dict(sorted(historical_route_counts.items())),
            "remote_event_not_found_rows": historical_event_not_found,
        },
        "post_baseline": {
            "terminal_rows": new_terminal_rows,
            "unclassified_rows": new_unclassified,
            "taxonomy_gap_rows": new_taxonomy_gaps + post_taxonomy_gap_routes,
            "failure_route_rows": len(route_rows),
            "terminal_route_effects": terminal_route_effects,
        },
        "failure_route_table_present": failure_route_table_present,
        "lanes": dict(sorted(post_lanes.items())),
        "internal_routes": dict(sorted(post_routes.items())),
        "route_statuses": dict(sorted(post_statuses.items())),
        "route_contract_errors": route_contract_errors[:50],
        "missing_live_lanes": missing_lanes,
        "remote_event_not_found": {
            "rows": event_not_found,
            "audit_complete_rows": event_not_found_audit_complete,
        },
        "ga_acceptance_ready": not gate_errors,
        "gate_errors": gate_errors,
    }


def _emit(payload: Mapping[str, Any], output: Path | None) -> None:
    rendered = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if output is None:
        print(rendered, end="")
        return
    output.parent.mkdir(parents=True, exist_ok=True)
    temp = output.with_name(f".{output.name}.tmp.{os.getpid()}")
    temp.write_text(rendered, encoding="utf-8")
    os.replace(temp, output)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--baseline", default=DEFAULT_BASELINE)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--gate-new", action="store_true")
    parser.add_argument("--inject-unknown", default="")
    args = parser.parse_args(argv)

    if args.inject_unknown:
        try:
            pnc_fault_taxonomy.decide_failure(
                {"kind": args.inject_unknown}, require_complete=True
            )
        except pnc_fault_taxonomy.TaxonomyContractError as exc:
            _emit(
                {
                    "ok": False,
                    "error": "taxonomy_contract_gap",
                    "decision": exc.decision.as_dict(),
                },
                args.output,
            )
            return 2
        return 0

    report = build_report(args.db, baseline=args.baseline)
    _emit(report, args.output)
    if args.gate_new and not report["ga_acceptance_ready"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
