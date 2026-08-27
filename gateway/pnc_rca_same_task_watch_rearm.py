"""Exact Host execution-watch rearm for the authorized RCA canary."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

from gateway.pnc_rca_same_task_resume import (
    AUTHORIZED_BUSINESS_KEY,
    AUTHORIZED_GENERATION,
    AUTHORIZED_ISSUE_ID,
    AUTHORIZED_TASK_ID,
    INFRA_REMEDIATION_SCHEMA_VERSION,
    SUPPORTED_BLOCKER,
    SUPPORTED_OPERATION,
)


SCHEMA_VERSION = "pnc_rca_same_task_watch_rearm_v1"
MIN_DEFER_SECONDS = 30
MAX_DEFER_SECONDS = 300


class WatchRearmError(RuntimeError):
    def __init__(self, code: str):
        self.code = str(code)
        super().__init__(self.code)


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _iso(value: datetime | None = None) -> str:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None:
        raise WatchRearmError("watch_rearm_time_invalid")
    return current.astimezone(timezone.utc).isoformat()


def _connect(path: Path, *, read_only: bool) -> sqlite3.Connection:
    if not path.is_file() or path.is_symlink():
        raise WatchRearmError("watch_rearm_db_invalid")
    try:
        if read_only:
            conn = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
        else:
            conn = sqlite3.connect(str(path), timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        if read_only:
            conn.execute("PRAGMA query_only=ON")
        return conn
    except sqlite3.Error as exc:
        raise WatchRearmError("watch_rearm_db_unavailable") from exc


def _row(conn: sqlite3.Connection) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT w.*, o.status AS outbox_status,
               o.business_key AS outbox_business_key,
               o.generation AS outbox_generation,
               t.work_item_id AS trigger_work_item_id
          FROM rca_execution_watch AS w
          JOIN rca_outbox AS o ON o.outbox_id = w.submission_outbox_id
          JOIN business_triggers AS t
            ON t.business_key = o.business_key
           AND t.generation = o.generation
         WHERE w.submission_key = ?
        """,
        (AUTHORIZED_TASK_ID,),
    ).fetchone()


def _status_payload(row: sqlite3.Row) -> dict[str, Any]:
    try:
        value = json.loads(str(row["last_status_json"] or "{}"))
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _validate_row(conn: sqlite3.Connection, row: sqlite3.Row | None) -> None:
    if row is None:
        raise WatchRearmError("watch_rearm_target_missing")
    expected = {
        "submission_key": AUTHORIZED_TASK_ID,
        "business_key": AUTHORIZED_BUSINESS_KEY,
        "generation": AUTHORIZED_GENERATION,
        "work_item_id": AUTHORIZED_ISSUE_ID,
        "task_id": AUTHORIZED_TASK_ID,
        "outbox_status": "completed",
        "outbox_business_key": AUTHORIZED_BUSINESS_KEY,
        "outbox_generation": AUTHORIZED_GENERATION,
        "trigger_work_item_id": AUTHORIZED_ISSUE_ID,
    }
    if any(str(row[key]) != str(value) for key, value in expected.items()):
        raise WatchRearmError("watch_rearm_identity_mismatch")
    if row["delivery_id"] is not None:
        raise WatchRearmError("watch_rearm_delivery_already_bound")
    delivery_count = conn.execute(
        "SELECT COUNT(*) FROM rca_delivery_jobs WHERE submission_key = ?",
        (AUTHORIZED_TASK_ID,),
    ).fetchone()[0]
    if int(delivery_count) != 0:
        raise WatchRearmError("watch_rearm_delivery_already_exists")
    higher = conn.execute(
        """
        SELECT COUNT(*)
          FROM rca_outbox
         WHERE business_key = ? AND generation > ?
        """,
        (AUTHORIZED_BUSINESS_KEY, AUTHORIZED_GENERATION),
    ).fetchone()[0]
    if int(higher) != 0:
        raise WatchRearmError("watch_rearm_higher_generation_exists")


def _view(row: sqlite3.Row) -> dict[str, Any]:
    status = _status_payload(row)
    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": str(row["task_id"]),
        "submission_key": str(row["submission_key"]),
        "business_key": str(row["business_key"]),
        "generation": int(row["generation"]),
        "issue_id": str(row["work_item_id"]),
        "state": str(row["state"]),
        "next_poll_at": str(row["next_poll_at"]),
        "terminal_at": str(row["terminal_at"] or ""),
        "terminal_first_seen_at": str(row["terminal_first_seen_at"] or ""),
        "last_error_code": str(row["last_error_code"] or ""),
        "delivery_id": row["delivery_id"],
        "lease_token": row["lease_token"],
        "phase": str(status.get("phase") or "terminal"),
        "rearm_token": str(status.get("rearm_token") or ""),
        "external_writes": False,
    }


def preflight(db_path: str | Path) -> dict[str, Any]:
    conn = _connect(Path(db_path), read_only=True)
    try:
        row = _row(conn)
        _validate_row(conn, row)
        result = _view(row)
    except sqlite3.Error as exc:
        raise WatchRearmError("watch_rearm_preflight_failed") from exc
    finally:
        conn.close()
    if result["state"] == "terminal_failed":
        if result["last_error_code"] != SUPPORTED_BLOCKER:
            raise WatchRearmError("watch_rearm_terminal_error_mismatch")
    elif result["state"] == "pending" and result["phase"] in {
        "watch_rearmed",
        "vm_resume_succeeded",
    }:
        pass
    else:
        raise WatchRearmError("watch_rearm_state_invalid")
    return result


def rearm(
    db_path: str | Path,
    *,
    defer_seconds: int = 120,
    now: datetime | None = None,
) -> dict[str, Any]:
    if (
        isinstance(defer_seconds, bool)
        or not isinstance(defer_seconds, int)
        or not MIN_DEFER_SECONDS <= defer_seconds <= MAX_DEFER_SECONDS
    ):
        raise WatchRearmError("watch_rearm_defer_invalid")
    current = _iso(now)
    deferred = _iso(
        (now or datetime.now(timezone.utc)) + timedelta(seconds=defer_seconds)
    )
    path = Path(db_path)
    conn = _connect(path, read_only=False)
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = _row(conn)
        _validate_row(conn, row)
        existing = _view(row)
        if existing["state"] == "pending" and existing["phase"] in {
            "watch_rearmed",
            "vm_resume_succeeded",
        }:
            conn.commit()
            return {**existing, "created": False}
        if (
            existing["state"] != "terminal_failed"
            or existing["last_error_code"] != SUPPORTED_BLOCKER
            or row["lease_token"] is not None
        ):
            raise WatchRearmError("watch_rearm_state_invalid")
        terminal_material = {
            "submission_key": AUTHORIZED_TASK_ID,
            "business_key": AUTHORIZED_BUSINESS_KEY,
            "generation": AUTHORIZED_GENERATION,
            "issue_id": AUTHORIZED_ISSUE_ID,
            "terminal_at": existing["terminal_at"],
            "terminal_first_seen_at": existing["terminal_first_seen_at"],
            "last_error_code": existing["last_error_code"],
            "last_status_sha256": hashlib.sha256(
                str(row["last_status_json"] or "").encode("utf-8")
            ).hexdigest(),
        }
        token = "rearm-" + hashlib.sha256(
            _canonical_json(terminal_material).encode("utf-8")
        ).hexdigest()
        status = {
            "schema_version": SCHEMA_VERSION,
            "phase": "watch_rearmed",
            "rearm_token": token,
            "authorized_identity": {
                "task_id": AUTHORIZED_TASK_ID,
                "submission_key": AUTHORIZED_TASK_ID,
                "business_key": AUTHORIZED_BUSINESS_KEY,
                "generation": AUTHORIZED_GENERATION,
                "issue_id": AUTHORIZED_ISSUE_ID,
            },
            "blocker_kind": SUPPORTED_BLOCKER,
            "operation": SUPPORTED_OPERATION,
            "rearmed_at": current,
            "deferred_until": deferred,
            "terminal_preimage": terminal_material,
            "external_writes": False,
            "created_task_ids": [],
        }
        updated = conn.execute(
            """
            UPDATE rca_execution_watch
               SET state = 'pending', next_poll_at = ?, last_observed_at = ?,
                   terminal_at = NULL, terminal_first_seen_at = NULL,
                   last_status_json = ?, last_error_code = '',
                   last_error_detail = '', lease_token = NULL,
                   lease_owner = NULL, lease_expires_at = NULL, updated_at = ?
             WHERE submission_key = ? AND state = 'terminal_failed'
               AND delivery_id IS NULL AND lease_token IS NULL
            """,
            (deferred, current, _canonical_json(status), current, AUTHORIZED_TASK_ID),
        )
        if updated.rowcount != 1:
            raise WatchRearmError("watch_rearm_compare_and_swap_failed")
        read_after = _row(conn)
        _validate_row(conn, read_after)
        result = _view(read_after)
        if (
            result["state"] != "pending"
            or result["phase"] != "watch_rearmed"
            or result["rearm_token"] != token
            or result["terminal_at"]
            or result["terminal_first_seen_at"]
            or result["last_error_code"]
        ):
            raise WatchRearmError("watch_rearm_read_after_failed")
        conn.commit()
        return {**result, "created": True}
    except Exception as exc:
        conn.rollback()
        if isinstance(exc, WatchRearmError):
            raise
        raise WatchRearmError("watch_rearm_apply_failed") from exc
    finally:
        conn.close()


def expedite(
    db_path: str | Path,
    *,
    rearm_token: str,
    remediation_result: Mapping[str, Any],
    now: datetime | None = None,
) -> dict[str, Any]:
    result = dict(remediation_result) if isinstance(remediation_result, Mapping) else {}
    if (
        result.get("schema_version") != INFRA_REMEDIATION_SCHEMA_VERSION
        or result.get("success") is not True
        or result.get("status") != "succeeded"
        or result.get("task_id") != AUTHORIZED_TASK_ID
        or result.get("submission_key") != AUTHORIZED_TASK_ID
        or result.get("business_key") != AUTHORIZED_BUSINESS_KEY
        or result.get("generation") != AUTHORIZED_GENERATION
        or result.get("operation") != SUPPORTED_OPERATION
        or result.get("blocker_kind") != SUPPORTED_BLOCKER
        or result.get("resumed_same_task") is not True
        or result.get("external_writes") is not False
        or result.get("error_code") != ""
    ):
        raise WatchRearmError("watch_rearm_remediation_receipt_invalid")
    current = _iso(now)
    conn = _connect(Path(db_path), read_only=False)
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = _row(conn)
        _validate_row(conn, row)
        before = _view(row)
        if (
            before["state"] != "pending"
            or before["phase"] not in {"watch_rearmed", "vm_resume_succeeded"}
            or before["rearm_token"] != rearm_token
            or row["lease_token"] is not None
        ):
            raise WatchRearmError("watch_rearm_expedite_state_invalid")
        status = _status_payload(row)
        status.update(
            {
                "phase": "vm_resume_succeeded",
                "vm_resume_succeeded_at": current,
                "remediation_result": result,
            }
        )
        updated = conn.execute(
            """
            UPDATE rca_execution_watch
               SET next_poll_at = ?, last_status_json = ?, updated_at = ?
             WHERE submission_key = ? AND state = 'pending'
               AND lease_token IS NULL AND delivery_id IS NULL
            """,
            (current, _canonical_json(status), current, AUTHORIZED_TASK_ID),
        )
        if updated.rowcount != 1:
            raise WatchRearmError("watch_rearm_expedite_compare_and_swap_failed")
        read_after = _row(conn)
        _validate_row(conn, read_after)
        after = _view(read_after)
        if (
            after["state"] != "pending"
            or after["phase"] != "vm_resume_succeeded"
            or after["rearm_token"] != rearm_token
            or after["next_poll_at"] != current
        ):
            raise WatchRearmError("watch_rearm_expedite_read_after_failed")
        conn.commit()
        return after
    except Exception as exc:
        conn.rollback()
        if isinstance(exc, WatchRearmError):
            raise
        raise WatchRearmError("watch_rearm_expedite_failed") from exc
    finally:
        conn.close()
