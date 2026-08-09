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
import tempfile
import time
from typing import Any, Mapping, Sequence
from urllib.parse import quote

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from gateway.pnc_rca_control_store import (  # noqa: E402
    CONTROL_STORE_SCHEMA_VERSION,
    DEFAULT_OUTBOX_HIGH_WATERMARK,
    MANUAL_TRIGGER_SCHEMA_VERSION,
    SILENT_TERMINAL_RERUN_ERROR_CODES,
    ManualRcaTriggerRequest,
    RcaControlStore,
    build_batch_terminal_rerun_authority,
    build_historical_epoch_rerun_authority,
    build_silent_terminal_rerun_authority,
)
from gateway.pnc_rca_abstention_projection import (  # noqa: E402
    RcaEvidenceProjectionError,
    build_gate_a_identifier_binding,
    build_gate_a_public_result,
    validate_gate_a_projection,
)
from gateway.pnc_rca_issue_focus import (  # noqa: E402
    ANALYSIS_COMPLETE,
    IssueFocusContractError,
    validate_issue_focus_evidence,
)


SCHEMA_VERSION = "pnc_rca_batch_rerun_state_v4"
DRY_RUN_SCHEMA_VERSION = "pnc_rca_batch_rerun_dry_run_receipt_v1"
OWNER_RECEIPT_SCHEMA_VERSION = "pnc_rca_batch_owner_receipt_v2"
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
        "runtime_commit",
        "runtime_tree",
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
QUEUE_SCHEMA_VERSION = "g1q3_rca_exact_refresh_queue_v2"
QUEUE_AUTHORITY_FLAGS = {
    "project_g1q3_only": True,
    "issue_only": True,
    "owner_or_proposer_scope": True,
    "no_other_task": True,
    "activation_required": True,
    "exact_and_scope": True,
    "daily_started_attempt_quota": None,
}
QUEUE_FIELDS = frozenset(
    {
        "schema_version",
        "batch_id",
        "project_key",
        "scope",
        "source_inventory_sha256",
        "authority_flags",
        "items",
    }
)
QUEUE_ITEM_FIELDS = frozenset(
    {
        "issue_id",
        "title",
        "quality_classification",
        "current_submission_key",
        "current_generation",
        "priority",
        "project_key",
    }
)
QUEUE_SCOPE = {
    "logic": "AND",
    "project_relation_id": "6670325063",
    "project_name": "G1Q3_T1LFL1 ICE_捷途",
    "creator_key": "7649830284321508335",
    "creator_name": "黎涛华",
}
QUEUE_QUALITY_CLASSIFICATIONS = frozenset({"missing", "legacy_or_other"})
MAX_INPUT_BYTES = 2 * 1024 * 1024
ISSUE_ID_RE = re.compile(r"^[0-9]{6,24}$")
BATCH_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{2,63}$")
TERMINAL_JOB_STATUSES = frozenset({"delivered", "partial", "quarantined"})
ACTIVE_EFFECT_STATUSES = frozenset({"pending", "claimed", "retry_wait", "uncertain"})
OFFICIAL_READBACK_SOURCES = frozenset(
    {
        "read_after_write",
        "read_after_recovery_write",
        "read_before_write",
        "recovery_read_before_write",
    }
)
DRY_RUN_PRODUCTION_EFFECTS = {
    "state_write": False,
    "control_db_write": False,
    "vm_submit": False,
    "feishu_issue_comment": False,
    "feishu_issue_field_update": False,
    "kafka_consume": False,
    "resident_restart": False,
}


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
    expected_runtime_commit: str | None = None,
    expected_runtime_tree: str | None = None,
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
    runtime_commit = str(value.get("runtime_commit") or "").strip().lower()
    runtime_tree = str(value.get("runtime_tree") or "").strip().lower()
    if (
        re.fullmatch(r"[0-9a-f]{40}", runtime_commit) is None
        or runtime_commit == "0" * 40
        or re.fullmatch(r"[0-9a-f]{40}", runtime_tree) is None
        or runtime_tree == "0" * 40
    ):
        raise BatchRerunError("batch_owner_receipt_runtime_invalid")
    if (
        expected_runtime_commit is not None
        and runtime_commit != expected_runtime_commit
    ) or (
        expected_runtime_tree is not None
        and runtime_tree != expected_runtime_tree
    ):
        raise BatchRerunError("batch_owner_receipt_runtime_mismatch")

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
    expected_runtime_commit: str | None = None,
    expected_runtime_tree: str | None = None,
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
        expected_runtime_commit=expected_runtime_commit,
        expected_runtime_tree=expected_runtime_tree,
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


def _runtime_identity() -> tuple[str, str]:
    read_only_git_env = dict(os.environ)
    read_only_git_env["GIT_OPTIONAL_LOCKS"] = "0"
    try:
        identity = subprocess.check_output(
            ["git", "-C", str(REPO_ROOT), "rev-parse", "HEAD", "HEAD^{tree}"],
            text=True,
            stderr=subprocess.DEVNULL,
            timeout=5,
            env=read_only_git_env,
        ).splitlines()
        dirty = subprocess.check_output(
            [
                "git",
                "-C",
                str(REPO_ROOT),
                "status",
                "--porcelain",
                "--untracked-files=all",
            ],
            text=True,
            stderr=subprocess.DEVNULL,
            timeout=5,
            env=read_only_git_env,
        ).strip()
    except (OSError, subprocess.SubprocessError) as exc:
        raise BatchRerunError("batch_runtime_commit_unavailable") from exc
    if len(identity) != 2:
        raise BatchRerunError("batch_runtime_commit_unavailable")
    if dirty:
        raise BatchRerunError("batch_runtime_dirty")
    return identity[0].strip(), identity[1].strip()


def _load_queue(
    path: Path, *, expected_batch_id: str | None = None
) -> tuple[list[dict[str, Any]], str]:
    value, raw_sha = _read_json(path)
    if (
        not isinstance(value, Mapping)
        or set(value) != QUEUE_FIELDS
        or value.get("schema_version") != QUEUE_SCHEMA_VERSION
        or not isinstance(value.get("items"), list)
        or BATCH_ID_RE.fullmatch(str(value.get("batch_id") or "")) is None
        or value.get("project_key") not in {"g1q3", "t03o4q"}
        or re.fullmatch(
            r"[0-9a-f]{64}", str(value.get("source_inventory_sha256") or "")
        )
        is None
        or value.get("source_inventory_sha256") == "0" * 64
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
        if not isinstance(raw, Mapping) or set(raw) != QUEUE_ITEM_FIELDS:
            raise BatchRerunError("batch_queue_item_invalid")
        if not all(isinstance(raw.get(field), str) for field in (
            "issue_id",
            "title",
            "quality_classification",
            "current_submission_key",
        )):
            raise BatchRerunError("batch_queue_item_invalid")
        issue_id = str(raw.get("issue_id") or "").strip()
        if ISSUE_ID_RE.fullmatch(issue_id) is None or issue_id in seen:
            raise BatchRerunError("batch_queue_issue_invalid")
        title = str(raw.get("title") or "").strip()
        classification = str(raw.get("quality_classification") or "").strip()
        submission_key = str(raw.get("current_submission_key") or "").strip()
        generation = raw.get("current_generation")
        priority = raw.get("priority")
        if (
            not title
            or classification not in QUEUE_QUALITY_CLASSIFICATIONS
            or isinstance(priority, bool)
            or not isinstance(priority, int)
            or priority < 0
            or isinstance(generation, bool)
            or not isinstance(generation, int)
            or generation < 0
            or (
                generation == 0
                and submission_key
            )
            or (
                generation > 0
                and re.fullmatch(r"g1q3-rca-s1-[0-9a-f]{64}", submission_key)
                is None
            )
            or raw.get("project_key") != value.get("project_key")
        ):
            raise BatchRerunError("batch_queue_item_invalid")
        seen.add(issue_id)
        items.append({
            "issue_id": issue_id,
            "title": title,
            "quality_classification": classification,
            "queue_submission_key": submission_key,
            "queue_generation": generation,
            "priority": priority,
        })
    if not items:
        raise BatchRerunError("batch_queue_empty")
    scope = value.get("scope")
    issue_ids_sha256 = _sha256_bytes(
        ("\n".join(sorted(seen)) + "\n").encode("utf-8")
    )
    if not isinstance(scope, Mapping) or dict(scope) != {
        **QUEUE_SCOPE,
        "issue_count": len(items),
        "issue_ids_sha256": issue_ids_sha256,
    }:
        raise BatchRerunError("batch_queue_scope_invalid")
    return sorted(items, key=lambda row: (row["priority"], row["issue_id"])), raw_sha


def _load_or_create_state(
    path: Path,
    *,
    batch_id: str,
    queue_sha256: str,
    runtime_commit: str,
    runtime_tree: str,
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
            or state.get("runtime_tree") != runtime_tree
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
        "runtime_tree": runtime_tree,
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
    if contract.get("gate_a_projection") is not None:
        return None
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


def _explicit_focus_stop_quality(
    contract_raw: Any, *, issue_title: str
) -> dict[str, str] | None:
    contract = _json_object(contract_raw)
    focus = contract.get("issue_focus")
    if not isinstance(focus, Mapping):
        return None
    title = str(issue_title or "").strip()
    if not title:
        return None
    try:
        validation = validate_issue_focus_evidence(issue_title=title, value=focus)
    except IssueFocusContractError:
        return None
    if validation.analysis_status == ANALYSIS_COMPLETE or validation.attribution_allowed:
        return None
    return {
        "status": "explicit_focus_stop",
        "analysis_status": validation.analysis_status,
        "responsibility": "暂无法判断",
        "causal_text_sha256": "",
    }


def _observational_non_attribution_quality(
    contract_raw: Any,
) -> dict[str, Any] | None:
    """Accept only a canonical L1 observation after official delivery readback."""
    contract = _json_object(contract_raw)
    report = contract.get("report")
    artifacts = contract.get("artifacts")
    consumer_capability = contract.get("consumer_capability")
    projection = contract.get("gate_a_projection")
    public_result = contract.get("public_result")
    if not all(
        isinstance(value, Mapping)
        for value in (report, artifacts, consumer_capability, projection, public_result)
    ):
        return None
    if (
        (
            "diagnostic_only" in report
            and report.get("diagnostic_only") is not False
        )
        or "candidate_owner" in report
        or "candidate_responsibility" in report
        or "candidate_owner_domain" in report
        or "responsibility_candidate" in report
        or "is_candidate" in report
        or "attribution_causal_text" in artifacts
        or any(
            field in contract
            for field in (
                "quality_classification",
                "terminal_class",
                "confidence_tier",
                "approval_ready",
                "human_decision",
            )
        )
        or "terminal_diagnostic" in contract
        or "terminal_diagnostic" in report
        or "terminal_diagnostic" in artifacts
    ):
        return None
    try:
        binding = build_gate_a_identifier_binding(consumer_capability)
        canonical_projection = validate_gate_a_projection(
            projection,
            identifier_binding=binding,
        )
        canonical_public_result = build_gate_a_public_result(canonical_projection)
    except RcaEvidenceProjectionError:
        return None
    if (
        canonical_projection.get("level") != "L1_observation"
        or dict(public_result) != canonical_public_result
        or canonical_public_result.get("responsibility")
        != {"status": "not_attributed", "candidate": "暂无法判断"}
    ):
        return None
    observations = canonical_public_result.get("evaluator_observations")
    observation_count = canonical_public_result.get("evaluator_observation_count")
    if (
        isinstance(observation_count, bool)
        or not isinstance(observation_count, int)
        or observation_count < 1
        or not isinstance(observations, list)
        or not observations
        or len(observations) > observation_count
        or canonical_public_result.get("evaluator_observation_omitted_count")
        != observation_count - len(observations)
    ):
        return None
    projection_sha256 = _sha256_bytes(
        json.dumps(
            canonical_projection,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    )
    return {
        "status": "observational_non_attribution",
        "gate_a_level": "L1_observation",
        "responsibility": "暂无法判断",
        "observation_count": observation_count,
        "projection_sha256": projection_sha256,
        "causal_text_sha256": "",
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
                   b.activation_epoch_id,
                   b.activation_ledger_id,
                   o.status AS outbox_status,
                   o.last_error_code AS outbox_error_code,
                   o.last_error_detail AS outbox_error_detail,
                   o.completed_at AS outbox_completed_at,
                   o.lease_token AS outbox_lease_token,
                   o.lease_owner AS outbox_lease_owner,
                   o.lease_expires_at AS outbox_lease_expires_at,
                   o.activation_epoch_id AS outbox_activation_epoch_id,
                   o.activation_ledger_id AS outbox_activation_ledger_id,
                   w.state AS watch_state,
                   w.task_id AS watch_task_id,
                   w.delivery_id AS watch_delivery_id,
                   w.last_error_code AS watch_error_code,
                   w.lease_token AS watch_lease_token,
                   w.lease_owner AS watch_lease_owner,
                   w.lease_expires_at AS watch_lease_expires_at,
                   j.delivery_id, j.status AS job_status,
                   j.outcome AS job_outcome, j.outcome_key,
                   j.terminal_state, j.terminal_error_code,
                   j.issue_url, j.report_url, j.manifest_json,
                   j.contract_json, j.artifacts_json,
                   j.updated_at AS job_updated_at,
                   (SELECT epoch_id FROM rca_activation_epochs
                     WHERE is_current = 1) AS current_activation_epoch_id,
                   (SELECT state FROM rca_activation_epochs
                     WHERE is_current = 1) AS current_activation_state
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
                       write_phase, attempt, lease_token, lease_owner,
                       lease_expires_at, remote_receipt_json, last_error_code,
                       completed_at, updated_at,
                       (SELECT COUNT(*) FROM rca_delivery_attempts AS attempt_row
                         WHERE attempt_row.effect_key =
                               rca_delivery_effects.effect_key
                       ) AS provider_attempt_count
                  FROM rca_delivery_effects
                 WHERE delivery_id = ?
                 ORDER BY effect_kind, effect_key
                """,
                (row["delivery_id"],),
            ).fetchall():
                item = {key: effect[key] for key in effect.keys()}
                remote_receipt_json = item.pop("remote_receipt_json")
                item["remote_receipt_present"] = remote_receipt_json is not None
                item["remote_receipt"] = _json_object(remote_receipt_json)
                effects.append(item)
        snapshot["effects"] = effects
        return snapshot
    finally:
        conn.close()


def _approval(
    snapshot: Mapping[str, Any], *, issue_title: str = ""
) -> dict[str, Any] | None:
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
    if (
        not receipt.get("remote_id")
        or receipt.get("source") not in OFFICIAL_READBACK_SOURCES
        or field_keys != ["field_8c912e", "field_9193cb"]
    ):
        return None
    quality = _causal_delivery_quality(snapshot.get("contract_json"))
    if quality is None:
        quality = _explicit_focus_stop_quality(
            snapshot.get("contract_json"), issue_title=issue_title
        )
    if quality is None:
        quality = _observational_non_attribution_quality(
            snapshot.get("contract_json")
        )
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


def _batch_completion(
    snapshot: Mapping[str, Any], *, issue_title: str = ""
) -> dict[str, Any] | None:
    """Return a terminal batch result once the two issue fields were read back.

    Quality classification remains visible, but it cannot trigger another
    external write after the provider already confirmed both requested fields.
    """
    approved = _approval(snapshot, issue_title=issue_title)
    if approved is not None:
        return approved
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
    if (
        not receipt.get("remote_id")
        or receipt.get("source") not in OFFICIAL_READBACK_SOURCES
        or field_keys != ["field_8c912e", "field_9193cb"]
    ):
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
        "quality": {
            "status": "post_write_review_required",
            "reason": "quality_contract_not_satisfied",
            "responsibility": "暂无法判断",
            "causal_text_sha256": "",
        },
        "manifest": _json_object(snapshot.get("manifest_json")),
        "artifacts": _json_object(snapshot.get("artifacts_json")),
        "completed_at": str(issue_effects[0].get("completed_at") or ""),
    }


def _terminal_failure(
    snapshot: Mapping[str, Any], *, issue_title: str = ""
) -> dict[str, Any] | None:
    effects = [
        effect
        for effect in snapshot.get("effects", [])
        if isinstance(effect, Mapping) and int(effect.get("required") or 0) == 1
    ]
    if any(effect.get("status") in ACTIVE_EFFECT_STATUSES for effect in effects):
        return None
    job_status = str(snapshot.get("job_status") or "")
    if job_status in TERMINAL_JOB_STATUSES and _approval(
        snapshot, issue_title=issue_title
    ) is None:
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
    if (
        snapshot.get("watch_state") == "terminal_failed"
        and snapshot.get("watch_delivery_id") is None
        and snapshot.get("watch_error_code")
        in SILENT_TERMINAL_RERUN_ERROR_CODES
        and not job_status
    ):
        return {
            "job_status": "",
            "job_outcome": "",
            "outcome_key": "",
            "terminal_state": "watch_terminal_failed",
            "terminal_error_code": str(
                snapshot.get("watch_error_code") or "watch_terminal_failed"
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


def _snapshot_lease_active(
    snapshot: Mapping[str, Any], prefix: str, *, now: datetime | None = None
) -> bool:
    values = tuple(
        snapshot.get(f"{prefix}_{name}" if prefix else name)
        for name in ("lease_token", "lease_owner", "lease_expires_at")
    )
    if not any(value is not None for value in values):
        return False
    if not all(str(value or "").strip() for value in values):
        return True
    try:
        expiry = datetime.fromisoformat(str(values[2]).replace("Z", "+00:00"))
    except (TypeError, ValueError, OverflowError):
        return True
    if expiry.tzinfo is None or expiry.utcoffset() is None:
        return True
    current = now or datetime.now(timezone.utc)
    return expiry.astimezone(timezone.utc) > current.astimezone(timezone.utc)


def _historical_epoch_rerun_ineligibility(
    snapshot: Mapping[str, Any], *, now: datetime | None = None
) -> str:
    prior_epoch = str(snapshot.get("activation_epoch_id") or "").strip()
    prior_ledger = snapshot.get("activation_ledger_id")
    if prior_epoch != str(snapshot.get("outbox_activation_epoch_id") or "").strip():
        return "prior_epoch_binding_mismatch"
    if prior_ledger != snapshot.get("outbox_activation_ledger_id"):
        return "prior_ledger_binding_mismatch"
    current_epoch = str(snapshot.get("current_activation_epoch_id") or "").strip()
    if (
        not current_epoch
        or str(snapshot.get("current_activation_state") or "") != "steady_active"
    ):
        return "current_epoch_not_steady"
    if prior_epoch == current_epoch:
        return "prior_epoch_is_current"
    if (not prior_epoch and prior_ledger is not None) or (
        prior_epoch
        and (
            isinstance(prior_ledger, bool)
            or not isinstance(prior_ledger, int)
            or prior_ledger < 1
        )
    ):
        return "prior_epoch_binding_invalid"
    if _snapshot_lease_active(snapshot, "outbox", now=now):
        return "prior_outbox_lease_active"
    if _snapshot_lease_active(snapshot, "watch", now=now):
        return "prior_watch_lease_active"
    for effect in snapshot.get("effects", []):
        if not isinstance(effect, Mapping):
            return "prior_effect_invalid"
        if str(effect.get("write_phase") or "") == "write_started":
            return "prior_write_started"
        if effect.get("remote_receipt_present") is True:
            return "prior_remote_receipt_present"
        if (
            int(effect.get("attempt") or 0) > 0
            or int(effect.get("provider_attempt_count") or 0) > 0
        ):
            return "prior_provider_attempt_present"
        if _snapshot_lease_active(effect, "", now=now):
            return "prior_effect_lease_active"
    return ""


def _historical_epoch_rerun_authority(
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
    if _historical_epoch_rerun_ineligibility(snapshot):
        return None
    return build_historical_epoch_rerun_authority(
        batch_id=batch_id,
        queue_sha256=queue_sha256,
        issue_id=issue_id,
        prior_submission_key=str(snapshot.get("submission_key") or ""),
        prior_generation=int(snapshot.get("generation") or 0),
        prior_activation_epoch_id=str(snapshot.get("activation_epoch_id") or ""),
        prior_activation_ledger_id=snapshot.get("activation_ledger_id"),
        target_activation_epoch_id=str(
            snapshot.get("current_activation_epoch_id") or ""
        ),
        owner_receipt_path=owner_receipt_path,
        owner_receipt_sha256=owner_receipt_sha256,
        requester_id=requester_id,
        reason=reason,
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
        or snapshot.get("watch_error_code")
        not in SILENT_TERMINAL_RERUN_ERROR_CODES
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
    """Bind any settled delivery terminal to the owner-approved refresh batch."""
    if (
        snapshot.get("watch_state") != "delivery_created"
        or not str(snapshot.get("watch_delivery_id") or "").strip()
        or str(snapshot.get("job_status") or "")
        not in TERMINAL_JOB_STATUSES
        or not snapshot.get("delivery_id")
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


def _queue_precondition_matches(
    queue_item: Mapping[str, Any], snapshot: Mapping[str, Any] | None
) -> bool:
    expected_submission = str(queue_item.get("queue_submission_key") or "")
    expected_generation = int(queue_item.get("queue_generation") or 0)
    if expected_generation == 0:
        return not expected_submission and snapshot is None
    return bool(
        snapshot is not None
        and str(snapshot.get("submission_key") or "") == expected_submission
        and int(snapshot.get("generation") or 0) == expected_generation
    )


def _dry_run_database_plan(
    db_path: Path,
    queue: Sequence[Mapping[str, Any]],
    *,
    batch_id: str,
    queue_sha256: str,
    owner_receipt_path: str,
    owner_receipt_sha256: str,
    requester_id: str,
) -> dict[str, Any]:
    """Validate a SQLite-consistent online backup of the source DB."""
    selected = db_path.expanduser().absolute()
    with tempfile.TemporaryDirectory(prefix="pnc-rca-batch-dry-run-") as raw_tmp:
        snapshot_db = Path(raw_tmp) / "control.sqlite3"
        source_uri = f"file:{quote(str(selected), safe='/')}?mode=ro"
        try:
            source = sqlite3.connect(source_uri, uri=True, timeout=30)
            snapshot = sqlite3.connect(snapshot_db, timeout=30)
            try:
                source.execute("PRAGMA query_only = ON")
                source.backup(snapshot, pages=1024, sleep=0.01)
                snapshot.commit()
            finally:
                snapshot.close()
                source.close()
        except sqlite3.Error as exc:
            raise BatchRerunError("batch_control_db_snapshot_failed") from exc

        uri = f"file:{quote(str(snapshot_db), safe='/')}?mode=ro&immutable=1"
        try:
            conn = sqlite3.connect(uri, uri=True, timeout=10)
            conn.row_factory = sqlite3.Row
            try:
                conn.execute("PRAGMA query_only = ON")
                conn.execute("BEGIN")
                schema = conn.execute(
                    "SELECT value FROM control_meta WHERE key = 'schema_version'"
                ).fetchone()
                if schema is None or str(schema["value"]) != CONTROL_STORE_SCHEMA_VERSION:
                    raise BatchRerunError("batch_control_schema_not_current")
                quick_check = [
                    str(row[0]) for row in conn.execute("PRAGMA quick_check").fetchall()
                ]
                if quick_check != ["ok"]:
                    raise BatchRerunError("batch_control_integrity_invalid")
                foreign_key_violations = len(
                    conn.execute("PRAGMA foreign_key_check").fetchall()
                )
                if foreign_key_violations:
                    raise BatchRerunError("batch_control_foreign_key_invalid")
                epochs = conn.execute(
                    "SELECT epoch_id, state, is_current "
                    "FROM rca_activation_epochs WHERE is_current = 1"
                ).fetchall()
                if len(epochs) != 1 or str(epochs[0]["state"]) != "steady_active":
                    raise BatchRerunError("batch_activation_not_ready")

                preconditions: list[dict[str, Any]] = []
                eligibility_items: list[dict[str, Any]] = []
                for item in queue:
                    issue_id = str(item["issue_id"])
                    trigger = conn.execute(
                        "SELECT generation, submission_key FROM business_triggers "
                        "WHERE work_item_id = ? ORDER BY generation DESC LIMIT 1",
                        (issue_id,),
                    ).fetchone()
                    snapshot = (
                        None
                        if trigger is None
                        else {
                            "generation": int(trigger["generation"]),
                            "submission_key": str(trigger["submission_key"]),
                        }
                    )
                    if not _queue_precondition_matches(item, snapshot):
                        raise BatchRerunError("batch_issue_generation_drift")
                    route = "initial_generation"
                    ineligibility = ""
                    if trigger is not None:
                        observed = _issue_snapshot(snapshot_db, issue_id)
                        if observed is None:
                            raise BatchRerunError("batch_issue_outbox_binding_invalid")
                        ineligibility = _historical_epoch_rerun_ineligibility(observed)
                        if not ineligibility:
                            route = "historical_epoch_rerun"
                        else:
                            authority_args = {
                                "snapshot": observed,
                                "batch_id": batch_id,
                                "queue_sha256": queue_sha256,
                                "issue_id": issue_id,
                                "owner_receipt_path": (
                                    owner_receipt_path or "/dry-run-owner-receipt.json"
                                ),
                                "owner_receipt_sha256": (
                                    owner_receipt_sha256 or "1" * 64
                                ),
                                "requester_id": requester_id,
                                "reason": f"production_gray_batch:{batch_id}",
                            }
                            if _silent_terminal_authority(**authority_args) is not None:
                                route = "silent_terminal_rerun"
                                ineligibility = ""
                            elif (
                                _batch_terminal_authority(**authority_args) is not None
                            ):
                                route = "batch_terminal_rerun"
                                ineligibility = ""
                    eligibility_items.append({
                        "issue_id": issue_id,
                        "eligible": not ineligibility,
                        "route": route if not ineligibility else "",
                        "reason": ineligibility,
                    })
                    outbox_status = ""
                    if trigger is not None:
                        outboxes = conn.execute(
                            "SELECT status FROM rca_outbox "
                            "WHERE submission_key = ? AND generation = ?",
                            (trigger["submission_key"], trigger["generation"]),
                        ).fetchall()
                        if len(outboxes) != 1:
                            raise BatchRerunError("batch_issue_outbox_binding_invalid")
                        outbox_status = str(outboxes[0]["status"])
                    preconditions.append(
                        {
                            "issue_id": issue_id,
                            "expected_generation": int(item["queue_generation"]),
                            "expected_submission_key": str(
                                item["queue_submission_key"]
                            ),
                            "observed_generation": (
                                int(trigger["generation"])
                                if trigger is not None
                                else 0
                            ),
                            "observed_submission_key": (
                                str(trigger["submission_key"])
                                if trigger is not None
                                else ""
                            ),
                            "observed_outbox_status": outbox_status,
                            "matched": True,
                        }
                    )
                conn.rollback()
            finally:
                conn.close()
        except BatchRerunError:
            raise
        except sqlite3.Error as exc:
            raise BatchRerunError("batch_control_db_invalid") from exc

    preconditions_sha256 = _sha256_bytes(
        _canonical_json(preconditions).encode("utf-8")
    )
    return {
        "path": str(selected),
        "schema_version": CONTROL_STORE_SCHEMA_VERSION,
        "quick_check": "ok",
        "foreign_key_violations": 0,
        "source_snapshot": {
            "transport": "sqlite_online_backup",
            "source_open_mode": "read_only",
            "source_query_only": True,
            "copy_verified": True,
        },
        "activation": {
            "epoch_id": str(epochs[0]["epoch_id"]),
            "state": "steady_active",
            "is_current": True,
            "ready": True,
        },
        "preconditions": {
            "matched": len(preconditions),
            "total": len(queue),
            "sha256": preconditions_sha256,
            "items": preconditions,
        },
        "eligibility": {
            "all_eligible": all(item["eligible"] for item in eligibility_items),
            "eligible": sum(1 for item in eligibility_items if item["eligible"]),
            "total": len(eligibility_items),
            "items": eligibility_items,
        },
    }


def _dry_run_plan(
    *,
    batch_id: str,
    queue: Sequence[Mapping[str, Any]],
    queue_sha256: str,
    runtime_commit: str,
    runtime_tree: str,
    owner_receipt_path: str,
    owner_receipt_sha256: str,
    database: Mapping[str, Any],
) -> dict[str, Any]:
    issue_ids = sorted(str(item["issue_id"]) for item in queue)
    material = {
        "schema_version": DRY_RUN_SCHEMA_VERSION,
        "mode": "dry_run",
        "batch_id": batch_id,
        "queue_sha256": queue_sha256,
        "scope": {
            **QUEUE_SCOPE,
            "issue_count": len(issue_ids),
            "issue_ids_sha256": _sha256_bytes(
                ("\n".join(issue_ids) + "\n").encode("utf-8")
            ),
        },
        "execution_policy": {
            "activation_required": QUEUE_AUTHORITY_FLAGS["activation_required"],
            "daily_started_attempt_quota": QUEUE_AUTHORITY_FLAGS[
                "daily_started_attempt_quota"
            ],
            "fixed_issue_allowlist": None,
            "selected_issue_count": len(issue_ids),
        },
        "runtime": {
            "commit": runtime_commit,
            "tree": runtime_tree,
            "clean": True,
        },
        "owner_receipt": {
            "provided": bool(owner_receipt_path),
            "validated": bool(owner_receipt_path),
            "path": owner_receipt_path,
            "sha256": owner_receipt_sha256,
        },
        "execution_authorized": bool(owner_receipt_path),
        "database": dict(database),
        "production_effects": dict(DRY_RUN_PRODUCTION_EFFECTS),
        "external_effects_triggered": False,
    }
    return {
        **material,
        "ok": bool(database.get("eligibility", {}).get("all_eligible")),
        "plan_sha256": _sha256_bytes(_canonical_json(material).encode("utf-8")),
    }


def _refresh_authorities(
    *,
    snapshot: Mapping[str, Any],
    batch_id: str,
    queue_sha256: str,
    issue_id: str,
    owner_receipt_path: str,
    owner_receipt_sha256: str,
    requester_id: str,
    reason: str,
) -> tuple[
    dict[str, Any] | None,
    dict[str, Any] | None,
    dict[str, Any] | None,
]:
    historical = _historical_epoch_rerun_authority(
        snapshot=snapshot,
        batch_id=batch_id,
        queue_sha256=queue_sha256,
        issue_id=issue_id,
        owner_receipt_path=owner_receipt_path,
        owner_receipt_sha256=owner_receipt_sha256,
        requester_id=requester_id,
        reason=reason,
    )
    if historical is not None:
        return historical, None, None
    silent = _silent_terminal_authority(
        snapshot=snapshot,
        batch_id=batch_id,
        queue_sha256=queue_sha256,
        issue_id=issue_id,
        owner_receipt_path=owner_receipt_path,
        owner_receipt_sha256=owner_receipt_sha256,
        requester_id=requester_id,
        reason=reason,
    )
    batch = None
    if silent is None:
        batch = _batch_terminal_authority(
            snapshot=snapshot,
            batch_id=batch_id,
            queue_sha256=queue_sha256,
            issue_id=issue_id,
            owner_receipt_path=owner_receipt_path,
            owner_receipt_sha256=owner_receipt_sha256,
            requester_id=requester_id,
            reason=reason,
        )
    return historical, silent, batch


def run(args: argparse.Namespace) -> dict[str, Any]:
    batch_id = str(args.batch_id or "").strip()
    if BATCH_ID_RE.fullmatch(batch_id) is None:
        raise BatchRerunError("batch_id_invalid")
    runtime_commit, runtime_tree = _runtime_identity()
    if (
        runtime_commit != str(args.expected_runtime_commit or "").strip()
        or runtime_tree != str(args.expected_runtime_tree or "").strip()
    ):
        raise BatchRerunError("batch_runtime_commit_mismatch")
    queue, queue_sha = _load_queue(Path(args.queue), expected_batch_id=batch_id)
    selected_issue_ids = sorted(str(item["issue_id"]) for item in queue)
    dry_run = bool(getattr(args, "dry_run", False))
    owner_receipt_arg = str(getattr(args, "owner_receipt", "") or "").strip()
    if not dry_run and not owner_receipt_arg:
        raise BatchRerunError("batch_owner_receipt_required")
    owner_receipt_path = ""
    owner_receipt_sha256 = ""
    if owner_receipt_arg:
        owner_receipt_path, owner_receipt_sha256 = _owner_receipt_binding(
            Path(owner_receipt_arg),
            expected_batch_id=batch_id,
            expected_queue_sha256=queue_sha,
            expected_issue_ids=selected_issue_ids,
            expected_requester_id=args.requester_id,
            expected_runtime_commit=runtime_commit,
            expected_runtime_tree=runtime_tree,
        )
    if dry_run:
        database = _dry_run_database_plan(
            Path(args.control_db),
            queue,
            batch_id=batch_id,
            queue_sha256=queue_sha,
            owner_receipt_path=owner_receipt_path,
            owner_receipt_sha256=owner_receipt_sha256,
            requester_id=args.requester_id,
        )
        return _dry_run_plan(
            batch_id=batch_id,
            queue=queue,
            queue_sha256=queue_sha,
            runtime_commit=runtime_commit,
            runtime_tree=runtime_tree,
            owner_receipt_path=owner_receipt_path,
            owner_receipt_sha256=owner_receipt_sha256,
            database=database,
        )
    state_path = Path(args.state)
    state = _load_or_create_state(
        state_path,
        batch_id=batch_id,
        queue_sha256=queue_sha,
        runtime_commit=runtime_commit,
        runtime_tree=runtime_tree,
        owner_receipt_path=owner_receipt_path,
        owner_receipt_sha256=owner_receipt_sha256,
        selected_issue_ids=selected_issue_ids,
    )
    store = RcaControlStore(Path(args.control_db))
    completed = 0
    submitted = 0
    deferred = 0
    submit_all = bool(getattr(args, "submit_all", False))
    requested_watermark = getattr(args, "outbox_high_watermark", None)
    if requested_watermark is None and submit_all:
        # A submit-all batch is intentionally admitted as one bounded batch;
        # retain the normal default for single-item/interactive callers.
        requested_watermark = max(DEFAULT_OUTBOX_HIGH_WATERMARK, len(queue) * 4)
    try:
        outbox_high_watermark = int(
            requested_watermark
            if requested_watermark is not None
            else DEFAULT_OUTBOX_HIGH_WATERMARK
        )
    except (TypeError, ValueError) as exc:
        raise BatchRerunError("batch_outbox_high_watermark_invalid") from exc
    if outbox_high_watermark < 1:
        raise BatchRerunError("batch_outbox_high_watermark_invalid")
    for queue_item in queue:
        issue_id = queue_item["issue_id"]
        item = dict(state["items"].get(issue_id) or {})
        latest = _issue_snapshot(Path(args.control_db), issue_id)
        if item.get("status") == "accepted":
            if (
                latest is None
                or str(latest.get("submission_key") or "")
                != str(item.get("submission_key") or "")
                or int(latest.get("generation") or 0)
                != int(item.get("generation") or 0)
            ):
                raise BatchRerunError("batch_issue_generation_drift")
            accepted_snapshot = _issue_snapshot(
                Path(args.control_db),
                issue_id,
                submission_key=str(item["submission_key"]),
            )
            if accepted_snapshot is None or _batch_completion(
                accepted_snapshot, issue_title=str(queue_item["title"])
            ) is None:
                raise BatchRerunError("batch_accepted_item_invalid")
            completed += 1
            continue
        if item.get("status") in {
            "submitted",
            "running",
            "waiting_for_prior_terminal",
        } and (
            latest is not None
            and str(latest.get("submission_key") or "")
            == str(item.get("submission_key") or "")
            and int(latest.get("generation") or 0)
            == int(item.get("generation") or 0)
        ):
            tracked_snapshot = _issue_snapshot(
                Path(args.control_db),
                issue_id,
                submission_key=str(item["submission_key"]),
            )
            completion = (
                _batch_completion(
                    tracked_snapshot,
                    issue_title=str(queue_item["title"]),
                )
                if tracked_snapshot is not None
                else None
            )
            if completion is not None:
                item.pop("failure", None)
                item.update({
                    "status": "accepted",
                    "approval": completion,
                    "updated_at": _now(),
                })
                state["items"][issue_id] = item
                state["updated_at"] = _now()
                _write_state(state_path, state)
                completed += 1
                continue
            failure = (
                _terminal_failure(
                    tracked_snapshot,
                    issue_title=str(queue_item["title"]),
                )
                if tracked_snapshot is not None
                else None
            )
            if failure is not None:
                item.update({
                    "status": "failed",
                    "failure": failure,
                    "updated_at": _now(),
                })
                state["items"][issue_id] = item
                state["updated_at"] = _now()
                _write_state(state_path, state)
        if item.get("status") == "failed" and (
            latest is not None
            and str(latest.get("submission_key") or "")
            == str(item.get("submission_key") or "")
            and int(latest.get("generation") or 0)
            == int(item.get("generation") or 0)
        ):
            failed_snapshot = _issue_snapshot(
                Path(args.control_db),
                issue_id,
                submission_key=str(item["submission_key"]),
            )
            completion = (
                _batch_completion(
                    failed_snapshot,
                    issue_title=str(queue_item["title"]),
                )
                if failed_snapshot is not None
                else None
            )
            if completion is not None:
                item.pop("failure", None)
                item.update({
                    "status": "accepted",
                    "approval": completion,
                    "updated_at": _now(),
                })
                state["items"][issue_id] = item
                state["updated_at"] = _now()
                _write_state(state_path, state)
                completed += 1
                continue
        if item.get("status") == "failed" and not args.retry_failed:
            raise BatchRerunError("batch_failed_item_requires_retry_flag")
        if (
            item.get("status") == "waiting_for_prior_terminal"
            and not args.retry_failed
        ):
            raise BatchRerunError("batch_prior_item_requires_retry_flag")
        request_index = int(item.get("request_index") or 0)
        if item.get("status") not in {"submitted", "running"}:
            precondition = dict(queue_item)
            if item.get("status") in {
                "failed",
                "waiting_for_prior_terminal",
            } and args.retry_failed:
                precondition.update({
                    "queue_submission_key": str(item.get("submission_key") or ""),
                    "queue_generation": int(item.get("generation") or 0),
                })
            if not _queue_precondition_matches(precondition, latest):
                raise BatchRerunError("batch_issue_generation_drift")
            request_index += 1
            request = _request(
                batch_id=batch_id,
                issue_id=issue_id,
                request_index=request_index,
                requester_id=args.requester_id,
            )
            historical_authority = None
            silent_authority = None
            batch_authority = None
            authority_deferred = False
            if latest is not None:
                wait_deadline = time.monotonic() + args.item_timeout_seconds
                while True:
                    (
                        historical_authority,
                        silent_authority,
                        batch_authority,
                    ) = _refresh_authorities(
                        snapshot=latest,
                        batch_id=batch_id,
                        queue_sha256=queue_sha,
                        issue_id=issue_id,
                        owner_receipt_path=owner_receipt_path,
                        owner_receipt_sha256=owner_receipt_sha256,
                        requester_id=args.requester_id,
                        reason=request.reason,
                    )
                    if any(
                        authority is not None
                        for authority in (
                            historical_authority,
                            silent_authority,
                            batch_authority,
                        )
                    ):
                        break
                    if submit_all:
                        item.update({
                            **queue_item,
                            "status": "waiting_for_prior_terminal",
                            "request_index": request_index - 1,
                            "updated_at": _now(),
                        })
                        state["items"][issue_id] = item
                        state["updated_at"] = _now()
                        _write_state(state_path, state)
                        deferred += 1
                        authority_deferred = True
                        break
                    if time.monotonic() >= wait_deadline:
                        item.update({
                            **queue_item,
                            "status": "waiting_for_prior_terminal",
                            "request_index": request_index - 1,
                            "updated_at": _now(),
                        })
                        state["items"][issue_id] = item
                        state["status"] = "waiting_for_prior_terminal"
                        state["updated_at"] = _now()
                        _write_state(state_path, state)
                        raise BatchRerunError("batch_prior_item_timeout")
                    time.sleep(args.poll_seconds)
                    latest = _issue_snapshot(Path(args.control_db), issue_id)
                    if not _queue_precondition_matches(precondition, latest):
                        raise BatchRerunError("batch_issue_generation_drift")
            if authority_deferred:
                continue
            admission_kwargs: dict[str, Any] = {}
            if historical_authority is not None:
                admission_kwargs["historical_epoch_rerun_authority"] = (
                    historical_authority
                )
            elif silent_authority is not None:
                admission_kwargs["silent_terminal_rerun_authority"] = silent_authority
            elif batch_authority is not None:
                admission_kwargs["batch_terminal_rerun_authority"] = batch_authority
            admitted = store.admit_manual_trigger(
                request,
                allowed_chat_ids=set(),
                submit_enabled=True,
                operator_authorized=True,
                activation_required=True,
                outbox_high_watermark=outbox_high_watermark,
                **admission_kwargs,
            )
            expected_generation = int(precondition["queue_generation"]) + 1
            if (
                admitted.outcome != "created"
                or admitted.generation != expected_generation
            ):
                raise BatchRerunError("batch_admission_did_not_create_generation")
            item.pop("failure", None)
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
        if submit_all:
            submitted += 1
            continue
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
            accepted = _batch_completion(
                latest, issue_title=str(queue_item["title"])
            )
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
            failure = _terminal_failure(
                latest, issue_title=str(queue_item["title"])
            )
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
    state["status"] = "submitted_all" if submit_all else "completed"
    finished_at = _now()
    if submit_all:
        state["submitted_at"] = finished_at
        state["summary"] = {
            "accepted": completed,
            "submitted": submitted,
            "total": len(queue),
        }
        if deferred:
            state["summary"]["deferred"] = deferred
    else:
        state["completed_at"] = finished_at
        state["summary"] = {"accepted": completed, "total": len(queue)}
    state["updated_at"] = finished_at
    _write_state(state_path, state)
    return state


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--control-db", required=True)
    parser.add_argument("--queue", required=True)
    parser.add_argument("--state", required=True)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--expected-runtime-commit", required=True)
    parser.add_argument("--expected-runtime-tree", required=True)
    parser.add_argument("--owner-receipt")
    parser.add_argument("--requester-id", default="automation:rca-batch-rerun")
    parser.add_argument("--poll-seconds", type=int, default=5)
    parser.add_argument("--item-timeout-seconds", type=int, default=7200)
    parser.add_argument("--retry-failed", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--submit-all", action="store_true")
    parser.add_argument(
        "--outbox-high-watermark",
        type=int,
        default=None,
        help="Admission outbox watermark; raise explicitly for a large batch.",
    )
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
