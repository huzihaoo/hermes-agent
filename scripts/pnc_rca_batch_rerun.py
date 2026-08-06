#!/usr/bin/env python3
"""Run an issue-only RCA rerun queue through the resident production path."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import subprocess
import sys
import time
from typing import Any, Mapping, Sequence
from urllib.parse import quote

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from gateway.pnc_rca_control_store import (  # noqa: E402
    MANUAL_TRIGGER_SCHEMA_VERSION,
    ManualRcaTriggerRequest,
    RcaControlStore,
    build_batch_terminal_rerun_authority,
    build_silent_terminal_rerun_authority,
)


SCHEMA_VERSION = "pnc_rca_batch_rerun_state_v3"
OWNER_RECEIPT_SCHEMA_VERSION = "pnc_rca_batch_owner_receipt_v1"
OWNER_RECEIPT_FIELDS = frozenset(
    {
        "schema_version",
        "approved",
        "batch_id",
        "queue_sha256",
        "selected_issue_ids",
        "production_effects",
        "no_other_task_boundary",
        "approved_by",
        "approved_at",
        "requester_id",
        "reason",
        "activation_required",
    }
)
OWNER_RECEIPT_EFFECT_SCOPE = {
    "rca_issue_rerun": True,
    "feishu_issue_comment": True,
    "feishu_issue_field_update": True,
    "vm_submit": True,
    "resident_restart": False,
    "kafka_consume": False,
    "other_task": False,
}
OWNER_RECEIPT_NO_OTHER_TASK_BOUNDARY = {
    "mode": "exclusive",
    "scope": "g1q3_rca_selected_issue_ids_only",
    "production_release_task_untouched": True,
    "other_codex_tasks_untouched": True,
}
QUEUE_SCHEMA_VERSION = "g1q3_rca_bootstrap_rerun_queue_v1"
QUEUE_AUTHORITY_FLAGS = {
    "project_g1q3_only": True,
    "issue_only": True,
    "owner_or_proposer_scope": True,
    "no_other_task": True,
    "activation_required": True,
}
MAX_INPUT_BYTES = 2 * 1024 * 1024
ISSUE_ID_RE = re.compile(r"^[0-9]{6,24}$")
BATCH_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{2,63}$")
TERMINAL_JOB_STATUSES = frozenset({"delivered", "partial", "quarantined"})
ACTIVE_EFFECT_STATUSES = frozenset({"pending", "claimed", "retry_wait", "uncertain"})


class BatchRerunError(RuntimeError):
    def __init__(self, code: str):
        self.code = str(code or "batch_rerun_failed")[:120]
        super().__init__(self.code)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _read_json(path: Path) -> tuple[Any, str]:
    raw = path.read_bytes()
    if not raw or len(raw) > MAX_INPUT_BYTES:
        raise BatchRerunError("batch_input_size_invalid")
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BatchRerunError("batch_input_json_invalid") from exc
    return value, _sha256_bytes(raw)


def _validate_owner_receipt(
    value: Any,
    *,
    expected_batch_id: str | None = None,
    expected_queue_sha256: str | None = None,
    expected_issue_ids: Sequence[str] | None = None,
    expected_requester_id: str | None = None,
) -> dict[str, Any]:
    """Validate the semantic owner approval contract before any admission.

    The raw receipt is separately required to be canonical and its exact byte
    hash is bound into batch state and every silent-terminal authority.  This
    function intentionally rejects a merely owner-readable ``{approved:true}``
    marker: approval must describe the exact queue and the exclusive effects
    boundary of this batch.
    """
    if not isinstance(value, Mapping) or set(value) != OWNER_RECEIPT_FIELDS:
        raise BatchRerunError("batch_owner_receipt_schema_invalid")
    if value.get("schema_version") != OWNER_RECEIPT_SCHEMA_VERSION:
        raise BatchRerunError("batch_owner_receipt_schema_invalid")
    if value.get("approved") is not True:
        raise BatchRerunError("batch_owner_receipt_not_approved")
    batch_id = str(value.get("batch_id") or "").strip()
    queue_sha256 = str(value.get("queue_sha256") or "").strip().lower()
    if BATCH_ID_RE.fullmatch(batch_id) is None or not re.fullmatch(
        r"[0-9a-f]{64}", queue_sha256
    ) or queue_sha256 == "0" * 64:
        raise BatchRerunError("batch_owner_receipt_binding_invalid")
    selected = value.get("selected_issue_ids")
    if not isinstance(selected, list) or not selected:
        raise BatchRerunError("batch_owner_receipt_selection_invalid")
    if any(
        not isinstance(issue_id, str) or ISSUE_ID_RE.fullmatch(issue_id) is None
        for issue_id in selected
    ) or selected != sorted(set(selected)):
        raise BatchRerunError("batch_owner_receipt_selection_invalid")
    if expected_batch_id is not None and batch_id != expected_batch_id:
        raise BatchRerunError("batch_owner_receipt_binding_mismatch")
    if expected_queue_sha256 is not None and queue_sha256 != expected_queue_sha256:
        raise BatchRerunError("batch_owner_receipt_binding_mismatch")
    if expected_issue_ids is not None and selected != sorted(set(expected_issue_ids)):
        raise BatchRerunError("batch_owner_receipt_selection_mismatch")

    effects = value.get("production_effects")
    if effects != OWNER_RECEIPT_EFFECT_SCOPE:
        raise BatchRerunError("batch_owner_receipt_effect_scope_invalid")
    boundary = value.get("no_other_task_boundary")
    if boundary != OWNER_RECEIPT_NO_OTHER_TASK_BOUNDARY:
        raise BatchRerunError("batch_owner_receipt_task_boundary_invalid")
    if value.get("activation_required") is not True:
        raise BatchRerunError("batch_owner_receipt_activation_required")

    approved_by = value.get("approved_by")
    if (
        not isinstance(approved_by, str)
        or not approved_by.strip()
        or len(approved_by) > 128
        or approved_by != approved_by.strip()
        or approved_by.startswith("automation:")
        or any(ord(char) < 0x20 for char in approved_by)
    ):
        raise BatchRerunError("batch_owner_receipt_approver_invalid")
    approved_at = value.get("approved_at")
    if not isinstance(approved_at, str) or not approved_at or approved_at != approved_at.strip():
        raise BatchRerunError("batch_owner_receipt_timestamp_invalid")
    try:
        observed_at = datetime.fromisoformat(approved_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise BatchRerunError("batch_owner_receipt_timestamp_invalid") from exc
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        raise BatchRerunError("batch_owner_receipt_timestamp_invalid")
    now = datetime.now(timezone.utc)
    if observed_at.astimezone(timezone.utc) > now + timedelta(minutes=5):
        raise BatchRerunError("batch_owner_receipt_timestamp_invalid")

    requester_id = value.get("requester_id")
    if (
        not isinstance(requester_id, str)
        or not requester_id.startswith("automation:")
        or len(requester_id) > 128
        or requester_id != requester_id.strip()
    ):
        raise BatchRerunError("batch_owner_receipt_requester_invalid")
    if expected_requester_id is not None and requester_id != expected_requester_id:
        raise BatchRerunError("batch_owner_receipt_requester_mismatch")
    reason = value.get("reason")
    if reason != f"production_gray_batch:{batch_id}":
        raise BatchRerunError("batch_owner_receipt_reason_invalid")
    return dict(value)


def _owner_receipt_binding(
    path: Path,
    *,
    expected_batch_id: str | None = None,
    expected_queue_sha256: str | None = None,
    expected_issue_ids: Sequence[str] | None = None,
    expected_requester_id: str | None = None,
) -> tuple[str, str]:
    """Validate and hash an owner-only canonical receipt without exposing it."""
    selected = path.expanduser()
    if not selected.is_absolute():
        raise BatchRerunError("batch_owner_receipt_path_invalid")
    try:
        observed = selected.lstat()
    except OSError as exc:
        raise BatchRerunError("batch_owner_receipt_unavailable") from exc
    if (
        not stat.S_ISREG(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_nlink != 1
        or observed.st_uid != os.getuid()
        or stat.S_IMODE(observed.st_mode) & 0o077
    ):
        raise BatchRerunError("batch_owner_receipt_identity_invalid")
    if observed.st_size < 1 or observed.st_size > MAX_INPUT_BYTES:
        raise BatchRerunError("batch_owner_receipt_size_invalid")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(
        os, "O_NOFOLLOW", 0
    )
    try:
        descriptor = os.open(selected, flags)
        try:
            opened = os.fstat(descriptor)
            if (
                opened.st_dev != observed.st_dev
                or opened.st_ino != observed.st_ino
                or opened.st_size != observed.st_size
                or not stat.S_ISREG(opened.st_mode)
                or opened.st_nlink != 1
                or opened.st_uid != os.getuid()
            ):
                raise BatchRerunError("batch_owner_receipt_identity_changed")
            digest = hashlib.sha256()
            chunks: list[bytes] = []
            remaining = observed.st_size
            while remaining:
                chunk = os.read(descriptor, min(64 * 1024, remaining))
                if not chunk:
                    raise BatchRerunError("batch_owner_receipt_size_changed")
                digest.update(chunk)
                chunks.append(chunk)
                remaining -= len(chunk)
            if os.read(descriptor, 1):
                raise BatchRerunError("batch_owner_receipt_size_changed")
            final = os.fstat(descriptor)
            if (
                final.st_dev != observed.st_dev
                or final.st_ino != observed.st_ino
                or final.st_size != observed.st_size
            ):
                raise BatchRerunError("batch_owner_receipt_identity_changed")
        finally:
            os.close(descriptor)
    except BatchRerunError:
        raise
    except OSError as exc:
        raise BatchRerunError("batch_owner_receipt_read_failed") from exc
    raw = b"".join(chunks)
    try:
        decoded = raw.decode("utf-8")
        value = json.loads(decoded)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BatchRerunError("batch_owner_receipt_json_invalid") from exc
    if raw != (_canonical_json(value) + "\n").encode("utf-8"):
        raise BatchRerunError("batch_owner_receipt_noncanonical")
    _validate_owner_receipt(
        value,
        expected_batch_id=expected_batch_id,
        expected_queue_sha256=expected_queue_sha256,
        expected_issue_ids=expected_issue_ids,
        expected_requester_id=expected_requester_id,
    )
    return str(selected), digest.hexdigest()


def _write_state(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = (_canonical_json(value) + "\n").encode("utf-8")
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
    fd = os.open(tmp, flags, 0o600)
    try:
        with os.fdopen(fd, "wb", closefd=True) as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, path)
    except Exception:
        try:
            tmp.unlink()
        except OSError:
            pass
        raise


def _runtime_commit() -> str:
    try:
        return subprocess.check_output(
            ["git", "-C", str(REPO_ROOT), "rev-parse", "HEAD"],
            text=True,
            stderr=subprocess.DEVNULL,
            timeout=5,
        ).strip()
    except (OSError, subprocess.SubprocessError) as exc:
        raise BatchRerunError("batch_runtime_commit_unavailable") from exc


def _load_queue(
    path: Path, *, expected_batch_id: str | None = None
) -> tuple[list[dict[str, Any]], str]:
    value, raw_sha = _read_json(path)
    if (
        not isinstance(value, Mapping)
        or value.get("schema_version") != QUEUE_SCHEMA_VERSION
        or not isinstance(value.get("items"), list)
        or BATCH_ID_RE.fullmatch(str(value.get("batch_id") or "")) is None
        or value.get("project_key") not in {"g1q3", "t03o4q"}
        or value.get("authority_flags") != QUEUE_AUTHORITY_FLAGS
        or (
            expected_batch_id is not None
            and value.get("batch_id") != expected_batch_id
        )
    ):
        raise BatchRerunError("batch_queue_schema_invalid")
    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in value["items"]:
        if not isinstance(raw, Mapping):
            raise BatchRerunError("batch_queue_item_invalid")
        issue_id = str(raw.get("issue_id") or "").strip()
        if ISSUE_ID_RE.fullmatch(issue_id) is None or issue_id in seen:
            raise BatchRerunError("batch_queue_issue_invalid")
        title = str(raw.get("title") or "").strip()
        classification = str(raw.get("quality_classification") or "").strip()
        submission_key = str(raw.get("current_submission_key") or "").strip()
        priority = raw.get("priority")
        if (
            not title
            or not classification
            or not submission_key
            or isinstance(priority, bool)
            or not isinstance(priority, int)
            or priority < 0
            or re.fullmatch(r"g1q3-rca-s1-[0-9a-f]{64}", submission_key)
            is None
            or (
                raw.get("project_key") is not None
                and raw.get("project_key") != value.get("project_key")
            )
        ):
            raise BatchRerunError("batch_queue_item_invalid")
        seen.add(issue_id)
        items.append({
            "issue_id": issue_id,
            "title": title,
            "quality_classification": classification,
            "queue_submission_key": submission_key,
            "priority": priority,
        })
    if not items:
        raise BatchRerunError("batch_queue_empty")
    return sorted(items, key=lambda row: (row["priority"], row["issue_id"])), raw_sha


def _load_or_create_state(
    path: Path,
    *,
    batch_id: str,
    queue_sha256: str,
    runtime_commit: str,
    owner_receipt_path: str,
    owner_receipt_sha256: str,
    selected_issue_ids: Sequence[str],
) -> dict[str, Any]:
    if path.exists():
        value, _raw_sha = _read_json(path)
        if not isinstance(value, Mapping):
            raise BatchRerunError("batch_state_schema_invalid")
        state = dict(value)
        if (
            state.get("schema_version") != SCHEMA_VERSION
            or state.get("batch_id") != batch_id
            or state.get("queue_sha256") != queue_sha256
            or state.get("owner_receipt_path") != owner_receipt_path
            or state.get("owner_receipt_sha256") != owner_receipt_sha256
            or state.get("selected_issue_ids") != list(selected_issue_ids)
            or state.get("activation_required") is not True
            or state.get("runtime_commit") != runtime_commit
        ):
            raise BatchRerunError("batch_state_binding_mismatch")
        if not isinstance(state.get("items"), Mapping):
            raise BatchRerunError("batch_state_schema_invalid")
        state["items"] = dict(state["items"])
        return state
    current = _now()
    return {
        "schema_version": SCHEMA_VERSION,
        "batch_id": batch_id,
        "queue_sha256": queue_sha256,
        "owner_receipt_path": owner_receipt_path,
        "owner_receipt_sha256": owner_receipt_sha256,
        "selected_issue_ids": list(selected_issue_ids),
        "activation_required": True,
        "runtime_commit": runtime_commit,
        "created_at": current,
        "updated_at": current,
        "status": "running",
        "items": {},
    }


def _json_object(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, str) or not raw:
        return {}
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return dict(value) if isinstance(value, Mapping) else {}


def _causal_delivery_quality(contract_raw: Any) -> dict[str, str] | None:
    """Return the user-facing causal evidence that qualifies for approval."""
    contract = _json_object(contract_raw)
    report = contract.get("report")
    artifacts = contract.get("artifacts")
    public_result = contract.get("public_result")
    if not isinstance(report, Mapping) or not isinstance(artifacts, Mapping):
        return None
    if report.get("diagnostic_only") is True:
        return None
    responsibility = str(report.get("candidate_owner") or "").strip()
    causal_text = str(artifacts.get("attribution_causal_text") or "").strip()
    if not responsibility or not causal_text:
        return None
    if isinstance(public_result, Mapping):
        summary = public_result.get("summary")
        responsibility_result = public_result.get("responsibility")
        responsibility_status = (
            str(responsibility_result.get("status") or "")
            if isinstance(responsibility_result, Mapping)
            else ""
        )
        data_integrity_cause = (
            responsibility_status == "candidate_data_integrity_conflict"
        )
        if (
            isinstance(summary, Mapping)
            and summary.get("status") in {"blocked", "diagnostic_report_ready"}
            and not data_integrity_cause
        ):
            return None
        if isinstance(responsibility_result, Mapping) and str(
            responsibility_result.get("status") or ""
        ).startswith("suppressed"):
            return None
        terminal = public_result.get("terminal_diagnostic")
        if isinstance(terminal, Mapping) and terminal:
            return None
    return {
        "status": "causal_candidate",
        "responsibility": responsibility,
        "causal_text_sha256": _sha256_bytes(causal_text.encode("utf-8")),
    }


def _issue_snapshot(
    db_path: Path, issue_id: str, *, submission_key: str = ""
) -> dict[str, Any] | None:
    # The batch runner only observes the control DB.  Open it in SQLite
    # read-only/query-only mode so a SELECT cannot create WAL/sidecar state or
    # accidentally acquire a writable handle while the resident producer runs.
    selected = db_path.expanduser().absolute()
    uri = f"file:{quote(str(selected), safe='/')}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA query_only = ON")
        submission_filter = ""
        params: tuple[str, ...] = (issue_id,)
        if submission_key:
            submission_filter = " AND b.submission_key = ?"
            params = (issue_id, submission_key)
        row = conn.execute(
            f"""
            SELECT b.generation, b.submission_key,
                   o.status AS outbox_status,
                   o.last_error_code AS outbox_error_code,
                   o.last_error_detail AS outbox_error_detail,
                   o.completed_at AS outbox_completed_at,
                   w.state AS watch_state,
                   w.delivery_id AS watch_delivery_id,
                   w.last_error_code AS watch_error_code,
                   j.delivery_id, j.status AS job_status,
                   j.outcome AS job_outcome, j.outcome_key,
                   j.terminal_state, j.terminal_error_code,
                   j.issue_url, j.report_url, j.manifest_json,
                   j.contract_json, j.artifacts_json,
                   j.updated_at AS job_updated_at
              FROM business_triggers AS b
              JOIN rca_outbox AS o
                ON o.business_key = b.business_key
               AND o.generation = b.generation
         LEFT JOIN rca_execution_watch AS w
                ON w.submission_outbox_id = o.outbox_id
         LEFT JOIN rca_delivery_jobs AS j
                ON j.submission_key = b.submission_key
             WHERE b.work_item_id = ?{submission_filter}
             ORDER BY b.generation DESC
             LIMIT 1
            """,
            params,
        ).fetchone()
        if row is None:
            return None
        snapshot = {key: row[key] for key in row.keys()}
        effects: list[dict[str, Any]] = []
        if row["delivery_id"]:
            for effect in conn.execute(
                """
                SELECT effect_key, effect_kind, required, target_key, status,
                       remote_receipt_json, last_error_code, completed_at,
                       updated_at
                  FROM rca_delivery_effects
                 WHERE delivery_id = ?
                 ORDER BY effect_kind, effect_key
                """,
                (row["delivery_id"],),
            ).fetchall():
                item = {key: effect[key] for key in effect.keys()}
                item["remote_receipt"] = _json_object(item.pop("remote_receipt_json"))
                effects.append(item)
        snapshot["effects"] = effects
        return snapshot
    finally:
        conn.close()


def _approval(snapshot: Mapping[str, Any]) -> dict[str, Any] | None:
    if snapshot.get("job_status") != "delivered":
        return None
    if snapshot.get("job_outcome") != "success":
        return None
    required = [
        effect
        for effect in snapshot.get("effects", [])
        if isinstance(effect, Mapping) and int(effect.get("required") or 0) == 1
    ]
    issue_effects = [
        effect
        for effect in required
        if effect.get("effect_kind") == "feishu_issue_comment"
    ]
    if len(issue_effects) != 1 or any(
        effect.get("status") != "succeeded" for effect in required
    ):
        return None
    receipt = dict(issue_effects[0].get("remote_receipt") or {})
    field_keys = sorted(str(value) for value in receipt.get("confirmed_field_keys", []))
    if not receipt.get("remote_id") or field_keys != ["field_8c912e", "field_9193cb"]:
        return None
    quality = _causal_delivery_quality(snapshot.get("contract_json"))
    if quality is None:
        return None
    return {
        "generation": int(snapshot["generation"]),
        "submission_key": str(snapshot["submission_key"]),
        "delivery_id": str(snapshot["delivery_id"]),
        "outcome_key": str(snapshot.get("outcome_key") or ""),
        "issue_url": str(snapshot.get("issue_url") or ""),
        "report_url": str(snapshot.get("report_url") or ""),
        "official_comment_id": str(receipt["remote_id"]),
        "official_field_keys": field_keys,
        "official_readback_source": str(receipt.get("source") or ""),
        "quality": quality,
        "manifest": _json_object(snapshot.get("manifest_json")),
        "artifacts": _json_object(snapshot.get("artifacts_json")),
        "completed_at": str(issue_effects[0].get("completed_at") or ""),
    }


def _terminal_failure(snapshot: Mapping[str, Any]) -> dict[str, Any] | None:
    effects = [
        effect
        for effect in snapshot.get("effects", [])
        if isinstance(effect, Mapping) and int(effect.get("required") or 0) == 1
    ]
    if any(effect.get("status") in ACTIVE_EFFECT_STATUSES for effect in effects):
        return None
    job_status = str(snapshot.get("job_status") or "")
    if job_status in TERMINAL_JOB_STATUSES and _approval(snapshot) is None:
        return {
            "job_status": job_status,
            "job_outcome": str(snapshot.get("job_outcome") or ""),
            "outcome_key": str(snapshot.get("outcome_key") or ""),
            "terminal_state": str(snapshot.get("terminal_state") or ""),
            "terminal_error_code": str(snapshot.get("terminal_error_code") or ""),
            "effects": [
                {
                    "effect_kind": str(effect.get("effect_kind") or ""),
                    "status": str(effect.get("status") or ""),
                    "error_code": str(effect.get("last_error_code") or ""),
                }
                for effect in effects
            ],
        }
    if snapshot.get("outbox_status") == "quarantined" and not job_status:
        return {
            "job_status": "",
            "job_outcome": "",
            "outcome_key": "",
            "terminal_state": "outbox_quarantined",
            "terminal_error_code": str(
                snapshot.get("outbox_error_code") or "outbox_quarantined"
            ),
            "effects": [],
        }
    return None


def _request(
    *, batch_id: str, issue_id: str, request_index: int, requester_id: str
) -> ManualRcaTriggerRequest:
    return ManualRcaTriggerRequest(
        schema_version=MANUAL_TRIGGER_SCHEMA_VERSION,
        issue_url=f"https://project.feishu.cn/t03o4q/issue/detail/{issue_id}",
        mode="rerun",
        reason=f"production_gray_batch:{batch_id}",
        platform="operator",
        chat_id="",
        thread_id="",
        message_id=f"{batch_id}-{issue_id}-try-{request_index}",
        requester_id=requester_id,
    )


def _silent_terminal_authority(
    *,
    snapshot: Mapping[str, Any],
    batch_id: str,
    queue_sha256: str,
    issue_id: str,
    owner_receipt_path: str,
    owner_receipt_sha256: str,
    requester_id: str,
    reason: str,
) -> dict[str, Any] | None:
    """Bind only an exact no-delivery deadline terminal to batch authority."""
    if (
        snapshot.get("watch_state") != "terminal_failed"
        or snapshot.get("watch_delivery_id") is not None
        or snapshot.get("watch_error_code") != "rca_work_deadline_exceeded"
    ):
        return None
    return build_silent_terminal_rerun_authority(
        batch_id=batch_id,
        queue_sha256=queue_sha256,
        issue_id=issue_id,
        prior_submission_key=str(snapshot.get("submission_key") or ""),
        prior_generation=int(snapshot.get("generation") or 0),
        owner_receipt_path=owner_receipt_path,
        owner_receipt_sha256=owner_receipt_sha256,
        requester_id=requester_id,
        reason=reason,
    )


def _batch_terminal_authority(
    *,
    snapshot: Mapping[str, Any],
    batch_id: str,
    queue_sha256: str,
    issue_id: str,
    owner_receipt_path: str,
    owner_receipt_sha256: str,
    requester_id: str,
    reason: str,
) -> dict[str, Any] | None:
    """Bind an ordinary settled delivery terminal to the owner-approved batch."""
    if (
        snapshot.get("watch_state") != "delivery_created"
        or not str(snapshot.get("watch_delivery_id") or "").strip()
        or str(snapshot.get("job_status") or "")
        not in TERMINAL_JOB_STATUSES
        or not snapshot.get("delivery_id")
        or snapshot.get("job_outcome") == "success"
        or _approval(snapshot) is not None
        or (
            not str(snapshot.get("terminal_error_code") or "").strip()
            and str(snapshot.get("job_outcome") or "") != "terminal_failed"
        )
    ):
        return None
    required_effects = [
        effect
        for effect in snapshot.get("effects", [])
        if isinstance(effect, Mapping) and int(effect.get("required") or 0) == 1
    ]
    if not required_effects or any(
        str(effect.get("status") or "")
        not in {"succeeded", "suppressed", "quarantined"}
        for effect in required_effects
    ):
        return None
    return build_batch_terminal_rerun_authority(
        batch_id=batch_id,
        queue_sha256=queue_sha256,
        issue_id=issue_id,
        prior_submission_key=str(snapshot.get("submission_key") or ""),
        prior_generation=int(snapshot.get("generation") or 0),
        prior_delivery_id=str(snapshot.get("watch_delivery_id") or snapshot.get("delivery_id") or ""),
        owner_receipt_path=owner_receipt_path,
        owner_receipt_sha256=owner_receipt_sha256,
        requester_id=requester_id,
        reason=reason,
    )


def run(args: argparse.Namespace) -> dict[str, Any]:
    batch_id = str(args.batch_id or "").strip()
    if BATCH_ID_RE.fullmatch(batch_id) is None:
        raise BatchRerunError("batch_id_invalid")
    runtime_commit = _runtime_commit()
    if runtime_commit != str(args.expected_runtime_commit or "").strip():
        raise BatchRerunError("batch_runtime_commit_mismatch")
    queue, queue_sha = _load_queue(Path(args.queue), expected_batch_id=batch_id)
    selected_issue_ids = [str(item["issue_id"]) for item in queue]
    owner_receipt_path, owner_receipt_sha256 = _owner_receipt_binding(
        Path(args.owner_receipt),
        expected_batch_id=batch_id,
        expected_queue_sha256=queue_sha,
        expected_issue_ids=selected_issue_ids,
        expected_requester_id=args.requester_id,
    )
    state_path = Path(args.state)
    state = _load_or_create_state(
        state_path,
        batch_id=batch_id,
        queue_sha256=queue_sha,
        runtime_commit=runtime_commit,
        owner_receipt_path=owner_receipt_path,
        owner_receipt_sha256=owner_receipt_sha256,
        selected_issue_ids=selected_issue_ids,
    )
    store = RcaControlStore(Path(args.control_db))
    completed = 0
    for queue_item in queue:
        issue_id = queue_item["issue_id"]
        item = dict(state["items"].get(issue_id) or {})
        latest = _issue_snapshot(Path(args.control_db), issue_id)
        accepted = _approval(latest) if latest is not None else None
        queue_submission = queue_item["queue_submission_key"]
        if accepted is not None and (
            item.get("status") == "accepted"
            or str(latest["submission_key"]) != queue_submission
        ):
            item.update({
                **queue_item,
                "status": "accepted",
                "approval": accepted,
                "updated_at": _now(),
            })
            state["items"][issue_id] = item
            state["updated_at"] = _now()
            _write_state(state_path, state)
            completed += 1
            continue
        if item.get("status") == "failed" and not args.retry_failed:
            raise BatchRerunError("batch_failed_item_requires_retry_flag")
        request_index = int(item.get("request_index") or 0)
        if item.get("status") not in {"submitted", "running"}:
            if (
                latest is None
                or not queue_submission
                or str(latest["submission_key"] or "") != queue_submission
            ):
                raise BatchRerunError("batch_issue_generation_drift")
            request_index += 1
            request = _request(
                batch_id=batch_id,
                issue_id=issue_id,
                request_index=request_index,
                requester_id=args.requester_id,
            )
            silent_authority = _silent_terminal_authority(
                snapshot=latest,
                batch_id=batch_id,
                queue_sha256=queue_sha,
                issue_id=issue_id,
                owner_receipt_path=owner_receipt_path,
                owner_receipt_sha256=owner_receipt_sha256,
                requester_id=args.requester_id,
                reason=request.reason,
            )
            batch_authority = None
            if silent_authority is None:
                batch_authority = _batch_terminal_authority(
                    snapshot=latest,
                    batch_id=batch_id,
                    queue_sha256=queue_sha,
                    issue_id=issue_id,
                    owner_receipt_path=owner_receipt_path,
                    owner_receipt_sha256=owner_receipt_sha256,
                    requester_id=args.requester_id,
                    reason=request.reason,
                )
            admission_kwargs: dict[str, Any] = {}
            if silent_authority is not None:
                admission_kwargs["silent_terminal_rerun_authority"] = (
                    silent_authority
                )
            elif batch_authority is not None:
                admission_kwargs["batch_terminal_rerun_authority"] = (
                    batch_authority
                )
            admitted = store.admit_manual_trigger(
                request,
                allowed_chat_ids=set(),
                submit_enabled=True,
                operator_authorized=True,
                activation_required=True,
                **admission_kwargs,
            )
            item.update({
                **queue_item,
                "status": "submitted",
                "request_index": request_index,
                "generation": admitted.generation,
                "submission_key": admitted.submission_key,
                "source_id": admitted.source_id,
                "submitted_at": _now(),
                "updated_at": _now(),
            })
            state["items"][issue_id] = item
            state["updated_at"] = _now()
            _write_state(state_path, state)
        deadline = time.monotonic() + args.item_timeout_seconds
        while True:
            current = _issue_snapshot(Path(args.control_db), issue_id)
            if current is None or int(current["generation"]) > int(item["generation"]):
                raise BatchRerunError("batch_issue_generation_drift")
            latest = _issue_snapshot(
                Path(args.control_db),
                issue_id,
                submission_key=str(item["submission_key"]),
            )
            if latest is None:
                raise BatchRerunError("batch_issue_generation_drift")
            accepted = _approval(latest)
            if accepted is not None:
                item.update({
                    "status": "accepted",
                    "approval": accepted,
                    "updated_at": _now(),
                })
                state["items"][issue_id] = item
                state["updated_at"] = _now()
                _write_state(state_path, state)
                completed += 1
                break
            failure = _terminal_failure(latest)
            if failure is not None:
                item.update({
                    "status": "failed",
                    "failure": failure,
                    "updated_at": _now(),
                })
                state["items"][issue_id] = item
                state["status"] = "blocked_on_item_failure"
                state["updated_at"] = _now()
                _write_state(state_path, state)
                raise BatchRerunError("batch_item_terminal_failure")
            if time.monotonic() >= deadline:
                item.update({"status": "running", "updated_at": _now()})
                state["items"][issue_id] = item
                state["status"] = "running"
                state["updated_at"] = _now()
                _write_state(state_path, state)
                raise BatchRerunError("batch_item_timeout")
            item["status"] = "running"
            item["updated_at"] = _now()
            state["items"][issue_id] = item
            state["updated_at"] = _now()
            _write_state(state_path, state)
            time.sleep(args.poll_seconds)
    state["status"] = "completed"
    state["completed_at"] = _now()
    state["updated_at"] = state["completed_at"]
    state["summary"] = {"accepted": completed, "total": len(queue)}
    _write_state(state_path, state)
    return state


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--control-db", required=True)
    parser.add_argument("--queue", required=True)
    parser.add_argument("--state", required=True)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--expected-runtime-commit", required=True)
    parser.add_argument("--owner-receipt", required=True)
    parser.add_argument("--requester-id", default="automation:rca-batch-rerun")
    parser.add_argument("--poll-seconds", type=int, default=5)
    parser.add_argument("--item-timeout-seconds", type=int, default=7200)
    parser.add_argument("--retry-failed", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = _parser().parse_args(argv)
        if args.poll_seconds < 1 or args.item_timeout_seconds < 30:
            raise BatchRerunError("batch_poll_config_invalid")
        result = run(args)
        print(_canonical_json(result))
        return 0
    except BatchRerunError as exc:
        print(_canonical_json({"ok": False, "error_code": exc.code}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
