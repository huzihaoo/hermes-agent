#!/usr/bin/env python3
"""Read-only, recomputable audit of PNC RCA conclusion adjudications."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import stat
import sys
from types import SimpleNamespace
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from gateway.pnc_rca_conclusion_adjudication import (
    ADJUDICATION_EFFECT_SCHEMA_VERSIONS,
    ADJUDICATION_EFFECT_TARGET_KEY_PREFIX,
    ADJUDICATION_SCHEMA_VERSION,
    validate_adjudication_effect_claim,
    validate_adjudication_effect_ledger_binding,
    validate_conclusion_adjudication_artifact_receipt,
    validate_conclusion_adjudication_schema,
)


AUDIT_SCHEMA_VERSION = "pnc_rca_conclusion_adjudication_audit_v2"
EXPECTED_DELIVERY_STORE_SCHEMA_VERSION = "pnc_rca_delivery_store_v9"
_UNRESOLVED_EFFECT_STATUSES = frozenset(
    {"pending", "claimed", "retry_wait", "uncertain"}
)
_VALID_EFFECT_WRITE_PHASES = {
    "pending": {"prewrite"},
    "claimed": {"prewrite", "write_started"},
    "retry_wait": {"prewrite"},
    "uncertain": {"write_started"},
    "succeeded": {"settled"},
    "quarantined": {"settled"},
    "suppressed": {"settled"},
}
_ADJUDICATION_EFFECT_IDENTITY_SQL = """
(
    (
        json_valid(payload_json)
        AND json_extract(payload_json, '$.schema_version') IN (?, ?)
    )
    OR substr(target_key, 1, ?) = ?
)
"""
_ADJUDICATION_EFFECT_IDENTITY_SQL_E = """
(
    (
        json_valid(e.payload_json)
        AND json_extract(e.payload_json, '$.schema_version') IN (?, ?)
    )
    OR substr(e.target_key, 1, ?) = ?
)
"""
_ADJUDICATION_EFFECT_IDENTITY_PARAMETERS = (
    *ADJUDICATION_EFFECT_SCHEMA_VERSIONS,
    len(ADJUDICATION_EFFECT_TARGET_KEY_PREFIX),
    ADJUDICATION_EFFECT_TARGET_KEY_PREFIX,
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone() is not None


def _scalar(conn: sqlite3.Connection, sql: str, parameters: tuple[Any, ...] = ()) -> int:
    row = conn.execute(sql, parameters).fetchone()
    return int(row[0] or 0) if row is not None else 0


def _recompute_adjudication_bindings(
    conn: sqlite3.Connection,
) -> tuple[int, list[dict[str, str]]]:
    rows = conn.execute(
        """
        SELECT a.*,
               correction.effect_key AS correction_row_effect_key,
               correction.delivery_id AS correction_delivery_id,
               correction.effect_kind AS correction_effect_kind,
               correction.required AS correction_required,
               correction.target_key AS correction_target_key,
               correction.payload_json AS correction_payload_json,
               correction.payload_sha256 AS correction_payload_sha256,
               correction.status AS correction_status,
               correction.write_phase AS correction_write_phase,
               correction_job.artifact_set_id AS correction_artifact_set_id,
               correction_job.project_key AS correction_project_key,
               correction_job.work_item_type_key
                   AS correction_work_item_type_key,
               correction_job.work_item_id AS correction_work_item_id,
               correction_job.business_key AS correction_business_key,
               correction_job.generation AS correction_generation,
               original.status AS original_effect_status,
               original.write_phase AS original_effect_write_phase,
               original.delivery_id AS original_effect_delivery_id,
               original.effect_kind AS original_effect_kind,
               original.required AS original_effect_required,
               original.outcome AS original_effect_outcome,
               original.target_key AS original_effect_target_key,
               original_job.business_key AS original_job_business_key,
               original_job.generation AS original_job_generation,
               original_job.project_key AS original_job_project_key,
               original_job.work_item_type_key
                   AS original_job_work_item_type_key,
               original_job.work_item_id AS original_job_work_item_id,
               original_job.target_key AS original_job_target_key,
               original_job.outcome AS original_job_outcome,
               epoch.state AS activation_state,
               epoch.is_current AS activation_is_current,
               repair.status AS artifact_repair_status,
               repair.receipt_schema_version AS artifact_receipt_schema_version,
               repair.receipt_path AS artifact_receipt_path,
               repair.receipt_offset AS artifact_receipt_offset,
               repair.receipt_length AS artifact_receipt_length,
               repair.receipt_sha256 AS artifact_receipt_sha256,
               repair.receipt_device AS artifact_receipt_device,
               repair.receipt_inode AS artifact_receipt_inode,
               repair.receipt_event_id AS artifact_receipt_event_id
          FROM rca_conclusion_adjudications AS a
     LEFT JOIN rca_delivery_effects AS correction
            ON correction.effect_key = a.correction_effect_key
     LEFT JOIN rca_delivery_jobs AS correction_job
            ON correction_job.delivery_id = correction.delivery_id
     LEFT JOIN rca_delivery_effects AS original
            ON original.effect_key = a.original_effect_key
     LEFT JOIN rca_delivery_jobs AS original_job
            ON original_job.delivery_id = a.original_delivery_id
     LEFT JOIN rca_activation_epochs AS epoch
            ON epoch.epoch_id = a.activation_epoch_id
     LEFT JOIN rca_conclusion_adjudication_repairs AS repair
            ON repair.adjudication_id = a.adjudication_id
      ORDER BY a.adjudication_id
        """
    ).fetchall()
    failures: list[dict[str, str]] = []
    for row in rows:
        adjudication_id = str(row["adjudication_id"] or "")
        try:
            if (
                row["correction_row_effect_key"] is None
                or row["correction_artifact_set_id"] is None
                or row["original_effect_status"] is None
                or row["original_job_business_key"] is None
            ):
                raise RuntimeError("adjudication_related_row_missing")
            correction_status = str(row["correction_status"] or "")
            correction_write_phase = str(row["correction_write_phase"] or "")
            if (
                row["correction_effect_kind"] != "feishu_issue_comment"
                or row["correction_required"] != 1
                or row["correction_delivery_id"] != row["original_delivery_id"]
                or correction_write_phase
                not in _VALID_EFFECT_WRITE_PHASES.get(correction_status, set())
            ):
                raise RuntimeError("adjudication_correction_storage_binding_invalid")
            payload = json.loads(str(row["correction_payload_json"] or ""))
            if not isinstance(payload, dict):
                raise ValueError("correction_payload_not_object")
            claim = SimpleNamespace(
                payload=payload,
                delivery_id=str(row["correction_delivery_id"] or ""),
                effect_kind=str(row["correction_effect_kind"] or ""),
                required=row["correction_required"],
                target_key=str(row["correction_target_key"] or ""),
                project_key=str(row["correction_project_key"] or ""),
                work_item_type_key=str(
                    row["correction_work_item_type_key"] or ""
                ),
                work_item_id=str(row["correction_work_item_id"] or ""),
                business_key=str(row["correction_business_key"] or ""),
                generation=int(row["correction_generation"] or 0),
                payload_sha256=str(row["correction_payload_sha256"] or ""),
                effect_key=str(row["correction_row_effect_key"] or ""),
                artifact_set_id=str(row["correction_artifact_set_id"] or ""),
            )
            validate_adjudication_effect_claim(claim)
            validate_adjudication_effect_ledger_binding(
                claim,
                dict(row),
                require_current_activation=(
                    correction_status in _UNRESOLVED_EFFECT_STATUSES
                ),
            )
            if row["artifact_repair_status"] is None:
                raise RuntimeError("adjudication_artifact_repair_row_missing")
            if str(row["artifact_repair_status"] or "") == "succeeded":
                validate_conclusion_adjudication_artifact_receipt(
                    {
                        "schema_version": row["artifact_receipt_schema_version"],
                        "path": row["artifact_receipt_path"],
                        "offset": row["artifact_receipt_offset"],
                        "length": row["artifact_receipt_length"],
                        "sha256": row["artifact_receipt_sha256"],
                        "device": row["artifact_receipt_device"],
                        "inode": row["artifact_receipt_inode"],
                        "review_event_id": row["artifact_receipt_event_id"],
                    },
                    adjudication=dict(row),
                )
        except Exception as exc:
            failures.append(
                {
                    "adjudication_id": adjudication_id,
                    "error": f"{type(exc).__name__}:{exc}"[:500],
                }
            )
    return len(failures), failures[:20]


def audit_conclusion_adjudications(control_db: str | Path) -> dict[str, Any]:
    path = Path(control_db).expanduser()
    if not path.is_absolute():
        raise ValueError("control DB path must be absolute")
    observed = path.lstat()
    if (
        stat.S_ISLNK(observed.st_mode)
        or not stat.S_ISREG(observed.st_mode)
        or observed.st_nlink != 1
        or observed.st_size <= 0
    ):
        raise ValueError("control DB must be a non-empty, single-link regular file")
    wal_path = Path(f"{path}-wal")
    try:
        wal_size = wal_path.lstat().st_size
    except FileNotFoundError:
        wal_size = 0
    if wal_size:
        raise RuntimeError(
            "immutable audit requires a checkpointed database with no WAL bytes"
        )
    uri = f"{path.resolve().as_uri()}?mode=ro&immutable=1"
    conn = sqlite3.connect(uri, uri=True, isolation_level=None)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA query_only=ON")
        conn.execute("BEGIN")
        required_delivery_tables = {
            "rca_delivery_jobs",
            "rca_delivery_effects",
        }
        available = {
            str(row["name"])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        if not required_delivery_tables.issubset(available):
            raise RuntimeError("delivery tables are unavailable")
        delivery_marker = ""
        if "rca_delivery_meta" in available:
            marker = conn.execute(
                "SELECT value FROM rca_delivery_meta WHERE key = 'schema_version'"
            ).fetchone()
            delivery_marker = str(marker["value"] or "") if marker else ""
        schema_errors: list[str] = []
        schema_ready = False
        if delivery_marker != EXPECTED_DELIVERY_STORE_SCHEMA_VERSION:
            schema_errors.append("delivery_schema_version_not_current")
        adjudication_tables_present = {
            "rca_conclusion_adjudications",
            "rca_conclusion_adjudication_repairs",
        }.issubset(available)
        if not adjudication_tables_present:
            schema_errors.append("adjudication_schema_missing")
        else:
            try:
                validate_conclusion_adjudication_schema(conn)
            except RuntimeError as exc:
                schema_errors.append(str(exc))
        schema_ready = not schema_errors
        published = _scalar(
            conn,
            """
            SELECT COUNT(*)
              FROM rca_delivery_jobs AS j
              JOIN rca_delivery_effects AS e
                ON e.delivery_id = j.delivery_id
               AND e.effect_kind = 'feishu_issue_comment'
               AND e.target_key = j.target_key
               AND e.required = 1
             WHERE j.outcome = 'success'
               AND e.status = 'succeeded'
               AND e.write_phase = 'settled'
            """,
        )
        adjudication_effects = _scalar(
            conn,
            "SELECT COUNT(*) FROM rca_delivery_effects WHERE "
            + _ADJUDICATION_EFFECT_IDENTITY_SQL,
            _ADJUDICATION_EFFECT_IDENTITY_PARAMETERS,
        )
        status_rows = conn.execute(
            f"""
            SELECT status, COUNT(*) AS count
              FROM rca_delivery_effects
             WHERE {_ADJUDICATION_EFFECT_IDENTITY_SQL}
             GROUP BY status ORDER BY status
            """,
            _ADJUDICATION_EFFECT_IDENTITY_PARAMETERS,
        ).fetchall()
        adjudication_statuses = {
            str(row["status"]): int(row["count"]) for row in status_rows
        }
        if adjudication_tables_present:
            adjudications = _scalar(
                conn, "SELECT COUNT(*) FROM rca_conclusion_adjudications"
            )
            invalidated = _scalar(
                conn,
                "SELECT COUNT(*) FROM rca_conclusion_adjudications "
                "WHERE conclusion_state = 'invalidated'",
            )
            recognized = _scalar(
                conn,
                "SELECT COUNT(*) FROM rca_conclusion_adjudications "
                "WHERE conclusion_state = 'recognized'",
            )
            budget_violations = _scalar(
                conn,
                f"""
                SELECT COUNT(*) FROM (
                    SELECT j.business_key
                      FROM rca_delivery_effects AS e
                      JOIN rca_delivery_jobs AS j
                        ON j.delivery_id = e.delivery_id
                     WHERE {_ADJUDICATION_EFFECT_IDENTITY_SQL_E}
                     GROUP BY j.business_key HAVING COUNT(*) > 1
                )
                """,
                _ADJUDICATION_EFFECT_IDENTITY_PARAMETERS,
            )
            attempt_violations = _scalar(
                conn,
                f"""
                SELECT COUNT(*)
                  FROM rca_delivery_effects
                 WHERE {_ADJUDICATION_EFFECT_IDENTITY_SQL}
                   AND (
                        recovery_write_count != 0
                        OR adjudication_comment_attempt_count NOT IN (0, 1)
                        OR (
                            status = 'succeeded'
                            AND adjudication_comment_attempt_count != 1
                        )
                   )
                """,
                _ADJUDICATION_EFFECT_IDENTITY_PARAMETERS,
            )
            lineage_unresolved = _scalar(
                conn,
                """
                SELECT COUNT(*) FROM rca_conclusion_adjudications
                 WHERE evaluator_refs_json = '[]'
                    OR responsibility_domain = 'unresolved'
                """,
            )
            dangling = _scalar(
                conn,
                """
                SELECT COUNT(*)
                  FROM rca_conclusion_adjudications AS a
             LEFT JOIN rca_delivery_effects AS e
                    ON e.effect_key = a.correction_effect_key
                 WHERE e.effect_key IS NULL
                """,
            )
            orphan_effects = _scalar(
                conn,
                f"""
                SELECT COUNT(*)
                  FROM rca_delivery_effects AS e
             LEFT JOIN rca_conclusion_adjudications AS a
                    ON a.correction_effect_key = e.effect_key
                 WHERE {_ADJUDICATION_EFFECT_IDENTITY_SQL_E}
                   AND a.adjudication_id IS NULL
                """,
                _ADJUDICATION_EFFECT_IDENTITY_PARAMETERS,
            )
            try:
                binding_mismatches, binding_validation_errors = (
                    _recompute_adjudication_bindings(conn)
                )
            except Exception as exc:
                binding_mismatches = max(1, adjudications)
                binding_validation_errors = [
                    {
                        "adjudication_id": "",
                        "error": f"{type(exc).__name__}:{exc}"[:500],
                    }
                ]
            activation_violations = _scalar(
                conn,
                """
                SELECT COUNT(*)
                  FROM rca_conclusion_adjudications AS a
                  JOIN rca_delivery_effects AS e
                    ON e.effect_key = a.correction_effect_key
             LEFT JOIN rca_activation_epochs AS epoch
                    ON epoch.epoch_id = a.activation_epoch_id
                 WHERE epoch.epoch_id IS NULL
                    OR (
                        e.status IN (
                            'pending', 'claimed', 'retry_wait', 'uncertain'
                        )
                        AND (
                            epoch.is_current != 1
                            OR epoch.state NOT IN (
                                'bounded_active', 'steady_active'
                            )
                        )
                    )
                """,
            )
            repair_pending = _scalar(
                conn,
                "SELECT COUNT(*) FROM rca_conclusion_adjudication_repairs "
                "WHERE status != 'succeeded'",
            )
            invalid_schema_versions = _scalar(
                conn,
                "SELECT COUNT(*) FROM rca_conclusion_adjudications "
                "WHERE schema_version != ?",
                (ADJUDICATION_SCHEMA_VERSION,),
            )
            schema_versions = [
                str(row[0])
                for row in conn.execute(
                    "SELECT DISTINCT schema_version "
                    "FROM rca_conclusion_adjudications ORDER BY schema_version"
                ).fetchall()
            ]
        else:
            adjudications = invalidated = recognized = 0
            budget_violations = attempt_violations = lineage_unresolved = 0
            dangling = orphan_effects = binding_mismatches = 0
            binding_validation_errors = []
            activation_violations = repair_pending = invalid_schema_versions = 0
            schema_versions = []
        conn.commit()
    finally:
        conn.close()
    db_sha256 = _sha256(path)
    final_observed = path.lstat()
    if (
        final_observed.st_dev != observed.st_dev
        or final_observed.st_ino != observed.st_ino
        or final_observed.st_size != observed.st_size
        or final_observed.st_mtime_ns != observed.st_mtime_ns
    ):
        raise RuntimeError("control DB changed during immutable audit")
    invariants = {
        "schema_current": schema_ready,
        "ledger_effect_count_equal": adjudications == adjudication_effects,
        "comment_budget_clean": budget_violations == 0,
        "correction_attempts_bounded": attempt_violations == 0,
        "correction_effects_linked": dangling == 0 and orphan_effects == 0,
        "ledger_payload_bindings_valid": binding_mismatches == 0,
        "activation_bindings_valid": activation_violations == 0,
        "lineage_resolved": lineage_unresolved == 0,
        "artifact_repairs_complete": repair_pending == 0,
        "schema_versions_current": invalid_schema_versions == 0,
    }
    return {
        "schema_version": AUDIT_SCHEMA_VERSION,
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "external_writes": False,
        "sqlite_mode": "ro+immutable",
        "control_db": {
            "path": str(path),
            "device": int(observed.st_dev),
            "inode": int(observed.st_ino),
            "size": int(observed.st_size),
            "mtime_ns": int(observed.st_mtime_ns),
            "sha256": db_sha256,
            "delivery_schema_version": delivery_marker,
            "wal_size": wal_size,
        },
        "adjudication_schema": {
            "ready": schema_ready,
            "errors": schema_errors,
            "expected_delivery_version": EXPECTED_DELIVERY_STORE_SCHEMA_VERSION,
            "expected_version": ADJUDICATION_SCHEMA_VERSION,
            "observed_versions": schema_versions,
        },
        "counts": {
            "published_conclusions": published,
            "adjudications": adjudications,
            "invalidated": invalidated,
            "recognized": recognized,
            "adjudication_effects": adjudication_effects,
            "adjudication_effect_statuses": adjudication_statuses,
            "comment_budget_violations": budget_violations,
            "correction_attempt_violations": attempt_violations,
            "lineage_unresolved": lineage_unresolved,
            "dangling_correction_effects": dangling,
            "orphan_correction_effects": orphan_effects,
            "ledger_payload_binding_mismatches": binding_mismatches,
            "activation_binding_violations": activation_violations,
            "artifact_repairs_pending": repair_pending,
            "invalid_schema_versions": invalid_schema_versions,
        },
        "binding_validation_errors": binding_validation_errors,
        "invariants": invariants,
        "ok": schema_ready and all(invariants.values()),
        "ga_acceptance_claimed": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--control-db", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    try:
        result = audit_conclusion_adjudications(args.control_db)
    except (OSError, RuntimeError, ValueError, sqlite3.Error) as exc:
        result = {
            "schema_version": AUDIT_SCHEMA_VERSION,
            "observed_at": datetime.now(timezone.utc).isoformat(),
            "external_writes": False,
            "ok": False,
            "error": f"{type(exc).__name__}:{exc}",
        }
    encoded = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded, encoding="utf-8")
    print(encoded, end="")
    return 0 if result.get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())
