"""Durable SQLite inbox, trigger, outbox, and DLQ for Kafka-driven PNC RCA."""

from __future__ import annotations

import base64
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import tempfile
from typing import Any, Callable, Iterable, Literal, Mapping
import uuid

from gateway.pnc_rca_admission import (
    RCA_KAFKA_TRIGGER_KINDS,
    RcaAdmission,
    build_rca_admission,
    build_rca_issue_scope_key,
    build_rca_trigger_context,
    validate_rca_admission,
)
from gateway.pnc_rca_kafka_contract import (
    WorkflowEventPolicy,
    build_event_admission,
    classify_workflow_event,
)
from gateway.pnc_rca_runtime_transition import (
    ensure_host_runtime_transition_schema,
    insert_host_runtime_transition,
    validate_host_runtime_transition_schema,
)
from gateway.pnc_rca_requester_identity import validate_rca_requester


CONTROL_STORE_SCHEMA_VERSION = "pnc_rca_control_store_v14"
CONTROL_STORE_SCHEMA_PREDECESSOR_VERSION = "pnc_rca_control_store_v13"
CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION = "pnc_rca_control_store_v15"
ACTIVATION_TRANSITION_BINDING_SCHEMA_V14 = "pnc_rca_activation_transition_binding_v14"
ACTIVATION_TRANSITION_BINDING_SCHEMA_V15 = "pnc_rca_activation_transition_binding_v15"
SUPPORTED_CONTROL_STORE_SCHEMA_VERSIONS = frozenset({
    "pnc_rca_control_store_v3",
    "pnc_rca_control_store_v4",
    "pnc_rca_control_store_v5",
    "pnc_rca_control_store_v6",
    "pnc_rca_control_store_v7",
    "pnc_rca_control_store_v8",
    "pnc_rca_control_store_v9",
    "pnc_rca_control_store_v10",
    "pnc_rca_control_store_v11",
    "pnc_rca_control_store_v12",
    "pnc_rca_control_store_v13",
    CONTROL_STORE_SCHEMA_VERSION,
})
_V14_COMPAT_RELEASE_BINDING_COLUMNS = frozenset({
    "preauthorization_fingerprint",
    "preauthorization_gate_receipt_sha256",
    "preauthorization_capsule_sha256",
    "preproduction_fingerprint",
    "preproduction_gate_receipt_sha256",
    "preproduction_capsule_sha256",
    "production_fingerprint",
    "production_gate_receipt_sha256",
})
_V14_ACTIVATION_EPOCH_COLUMNS = (
    "epoch_id",
    "state",
    "is_current",
    "preauthorization_fingerprint",
    "preauthorization_gate_receipt_sha256",
    "preauthorization_capsule_sha256",
    "preproduction_fingerprint",
    "preproduction_gate_receipt_sha256",
    "preproduction_capsule_sha256",
    "config_sha256",
    "db_logical_identity_json",
    "db_logical_identity_sha256",
    "partition_start_fence_json",
    "partition_start_fence_sha256",
    "partition_end_fence_json",
    "partition_end_fence_sha256",
    "production_fingerprint",
    "production_gate_receipt_sha256",
    "created_at",
    "updated_at",
    "bounded_activated_at",
    "confirmed_at",
    "steady_activated_at",
    "aborted_at",
    "superseded_at",
)
_V15_ACTIVATION_EPOCH_COLUMNS = (
    "epoch_id",
    "state",
    "is_current",
    "release_fingerprint_sha256",
    "release_note_sha256",
    "config_sha256",
    "db_logical_identity_json",
    "db_logical_identity_sha256",
    "partition_start_fence_json",
    "partition_start_fence_sha256",
    "created_at",
    "updated_at",
    "activated_at",
    "retired_at",
)
_V14_ACTIVATION_AUDIT_COLUMNS = (
    "audit_id",
    "epoch_id",
    "from_state",
    "to_state",
    "operator",
    "reason",
    "binding_fingerprint",
    "transitioned_at",
)
_V15_ACTIVATION_AUDIT_COLUMNS = (
    "audit_id",
    "epoch_id",
    "from_state",
    "to_state",
    "operator",
    "reason",
    "binding_fingerprint",
    "transitioned_at",
    "binding_schema_version",
)
_V15_DISTINCT_ACTIVATION_COLUMNS = frozenset({
    "release_fingerprint_sha256",
    "release_note_sha256",
})
_V15_ACTIVATION_EPOCH_NEW_TABLE_SQL = """
CREATE TABLE rca_activation_epochs_v15_new (
    epoch_id TEXT PRIMARY KEY CHECK (
        length(trim(epoch_id)) BETWEEN 1 AND 128
        AND substr(epoch_id, 1, 1) GLOB '[A-Za-z0-9]'
        AND epoch_id NOT GLOB '*[^A-Za-z0-9._:-]*'
    ),
    state TEXT NOT NULL CHECK (state IN ('steady_active', 'retired')),
    is_current INTEGER NOT NULL CHECK (is_current IN (0, 1)),
    release_fingerprint_sha256 TEXT CHECK (
        release_fingerprint_sha256 IS NULL OR (
            length(release_fingerprint_sha256) = 64
            AND release_fingerprint_sha256 NOT GLOB '*[^0-9a-f]*'
            AND release_fingerprint_sha256 != printf('%064d', 0)
        )
    ),
    release_note_sha256 TEXT CHECK (
        release_note_sha256 IS NULL OR (
            length(release_note_sha256) = 64
            AND release_note_sha256 NOT GLOB '*[^0-9a-f]*'
            AND release_note_sha256 != printf('%064d', 0)
        )
    ),
    config_sha256 TEXT NOT NULL CHECK (
        length(config_sha256) = 64
        AND config_sha256 NOT GLOB '*[^0-9a-f]*'
        AND config_sha256 != printf('%064d', 0)
    ),
    db_logical_identity_json TEXT NOT NULL CHECK (
        json_valid(db_logical_identity_json)
        AND json_type(db_logical_identity_json) = 'object'
    ),
    db_logical_identity_sha256 TEXT NOT NULL CHECK (
        length(db_logical_identity_sha256) = 64
        AND db_logical_identity_sha256 NOT GLOB '*[^0-9a-f]*'
        AND db_logical_identity_sha256 != printf('%064d', 0)
    ),
    partition_start_fence_json TEXT NOT NULL CHECK (
        json_valid(partition_start_fence_json)
        AND json_type(partition_start_fence_json) = 'object'
    ),
    partition_start_fence_sha256 TEXT NOT NULL CHECK (
        length(partition_start_fence_sha256) = 64
        AND partition_start_fence_sha256 NOT GLOB '*[^0-9a-f]*'
        AND partition_start_fence_sha256 != printf('%064d', 0)
    ),
    created_at TEXT NOT NULL CHECK (length(trim(created_at)) > 0),
    updated_at TEXT NOT NULL CHECK (length(trim(updated_at)) > 0),
    activated_at TEXT,
    retired_at TEXT,
    CHECK (
        (release_fingerprint_sha256 IS NULL AND release_note_sha256 IS NULL)
        OR (
            release_fingerprint_sha256 IS NOT NULL
            AND release_note_sha256 IS NOT NULL
        )
    ),
    CHECK (
        (
            state = 'steady_active'
            AND is_current = 1
            AND release_fingerprint_sha256 IS NOT NULL
            AND release_note_sha256 IS NOT NULL
            AND activated_at IS NOT NULL
            AND retired_at IS NULL
        ) OR (
            state = 'retired'
            AND is_current = 0
            AND retired_at IS NOT NULL
        )
    )
)
"""
_V15_ACTIVATION_EPOCH_TABLE_SQL = _V15_ACTIVATION_EPOCH_NEW_TABLE_SQL.replace(
    "rca_activation_epochs_v15_new",
    '"rca_activation_epochs"',
    1,
)
_V15_CURRENT_ACTIVATION_INDEX_SQL = """
CREATE UNIQUE INDEX idx_rca_single_current_activation_epoch
    ON rca_activation_epochs(is_current) WHERE is_current = 1
"""
_V15_ACTIVATION_AUDIT_TABLE_SQL = f"""
CREATE TABLE rca_activation_transition_audit (
    audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
    epoch_id TEXT NOT NULL,
    from_state TEXT NOT NULL,
    to_state TEXT NOT NULL,
    operator TEXT NOT NULL,
    reason TEXT NOT NULL,
    binding_fingerprint TEXT NOT NULL,
    transitioned_at TEXT NOT NULL,
    binding_schema_version TEXT NOT NULL DEFAULT
        '{ACTIVATION_TRANSITION_BINDING_SCHEMA_V14}'
        CHECK(binding_schema_version IN ('{ACTIVATION_TRANSITION_BINDING_SCHEMA_V14}',
            '{ACTIVATION_TRANSITION_BINDING_SCHEMA_V15}')),
    FOREIGN KEY(epoch_id) REFERENCES rca_activation_epochs(epoch_id)
)
"""
_V15_ACTIVATION_CROSS_TRIGGER_NAMES = (
    "trg_rca_admission_snapshot_execution_guard",
    "trg_terminal_rerun_delivery_authority_binding_guard",
    "trg_historical_epoch_rerun_delivery_authority_binding_guard",
)
_KNOWN_V14_TERMINAL_RERUN_BINDING_GUARD_SHA256 = (
    "d591104bdaa90523aaf57e29c7de8254612f321934eba537895a0deffd4eef1f"
)
_MINIMAL_RELEASE_EPOCH_CONTRACT_SCHEMA_VERSION = (
    "pnc_rca_minimal_release_epoch_contract_v1"
)
CONTROL_DB_MIN_AVAILABLE_BYTES = 1024 * 1024 * 1024
DEFAULT_OUTBOX_HIGH_WATERMARK = 100
MANUAL_OUTBOX_SHARE_NUMERATOR = 4
MANUAL_OUTBOX_SHARE_DENOMINATOR = 5
OUTBOX_MAX_CONSECUTIVE_KAFKA_CLAIMS = 3
OUTBOX_KAFKA_CLAIM_STREAK_META_KEY = "outbox_kafka_claim_streak"
OUTBOX_CIRCUIT_RESET_META_PREFIX = "rca_dispatcher_circuit_reset:"
OUTBOX_CIRCUIT_RESET_SCHEMA_VERSION = "pnc_rca_outbox_circuit_reset_v1"
OUTBOX_CIRCUIT_RESET_MAX_AUDIT_BYTES = 256 * 1024
OUTBOX_CIRCUIT_RESET_REQUIRED_FIELDS = frozenset(
    {
        "schema_version",
        "command",
        "reset_id",
        "recorded_at",
        "operator",
        "reason",
        "control_db_identity",
        "config_binding_sha256",
        "before",
        "after",
        "pre_state",
        "post_state",
        "effect_delta",
        "receipt_fingerprint",
    }
)
DEFAULT_MANUAL_OPERATOR_RATE_LIMIT = 3
DEFAULT_MANUAL_OPERATOR_RATE_WINDOW_SECONDS = 600
GROUP_USER_RERUN_SCHEMA_VERSION = "pnc_rca_group_user_rerun_v1"
GROUP_USER_RERUN_DEDUPE_SECONDS = 600
SILENT_TERMINAL_RERUN_AUTHORITY_SCHEMA_VERSION = (
    "pnc_rca_silent_terminal_rerun_authority_v1"
)
SILENT_TERMINAL_RERUN_ERROR_CODES = frozenset({
    "delivery_lineage_unavailable",
    "failure_receipt_missing",
    "rca_work_deadline_exceeded",
    "report_public_origin_invalid",
    "service_provenance_unavailable",
    "taxonomy_gap:derived_capacity_hfs_target_identity_mismatch",
    "taxonomy_gap:derived_capacity_reservation_activate_failed",
    "taxonomy_gap:gate_a_projection_invalid",
    "taxonomy_gap:viz_evidence_unavailable",
    "remote_evidence_domain_unsupported",
    "taxonomy_gap:remote_evidence_domain_unsupported",
})
SILENT_TERMINAL_RERUN_AUTHORITY_FIELDS = frozenset({
    "schema_version",
    "batch_id",
    "queue_sha256",
    "issue_id",
    "prior_submission_key",
    "prior_generation",
    "owner_receipt_path",
    "owner_receipt_sha256",
    "activation_required",
    "requester_id",
    "reason",
    "selection_sha256",
})
BATCH_TERMINAL_RERUN_AUTHORITY_SCHEMA_VERSION = (
    "pnc_rca_batch_terminal_rerun_authority_v1"
)
BATCH_TERMINAL_RERUN_AUTHORITY_FIELDS = frozenset({
    "schema_version",
    "batch_id",
    "queue_sha256",
    "issue_id",
    "prior_submission_key",
    "prior_generation",
    "prior_delivery_id",
    "owner_receipt_path",
    "owner_receipt_sha256",
    "activation_required",
    "terminal_mode",
    "requester_id",
    "reason",
    "selection_sha256",
})
HISTORICAL_EPOCH_RERUN_AUTHORITY_SCHEMA_VERSION = (
    "pnc_rca_historical_epoch_rerun_authority_v1"
)
HISTORICAL_EPOCH_RERUN_AUTHORITY_FIELDS = frozenset({
    "schema_version",
    "batch_id",
    "queue_sha256",
    "issue_id",
    "prior_submission_key",
    "prior_generation",
    "prior_activation_epoch_id",
    "prior_activation_ledger_id",
    "target_activation_epoch_id",
    "owner_receipt_path",
    "owner_receipt_sha256",
    "activation_required",
    "requester_id",
    "reason",
    "selection_sha256",
})
HISTORICAL_EPOCH_RERUN_DELIVERY_AUTHORITY_SCHEMA_VERSION = (
    "pnc_rca_historical_epoch_rerun_delivery_authority_v1"
)
REPLAY_RAW_RETENTION = timedelta(days=7)
PROCESSED_RAW_RETENTION = timedelta(days=30)
REPLAY_RAW_PRUNE_BATCH = 1000
INPUT_WAIT_QUARANTINE_REARMED_REASON = "input_wait_quarantine_rearmed"
INPUT_WAIT_TERMINAL_NEW_GENERATION_REASON = "input_wait_terminal_new_generation_created"
INPUT_WAIT_EXECUTION_WATCH_PRESENT_REASON = "input_wait_execution_watch_present"
W3_KAFKA_OBSERVATION_JOIN_REASON = "w3_automatic_generation_observation_joined"
W3_LEGACY_PARENT_SNAPSHOT_MISSING_REASON = "w3_legacy_parent_snapshot_missing"
W3_AUTOMATIC_OBSERVATION_SNAPSHOT_MISMATCH_REASON = (
    "w3_automatic_observation_snapshot_mismatch"
)
ISSUE_SCOPE_CONFLICT_REASON = "issue_scope_business_key_conflict"
LEGACY_KAFKA_GENERATION_REASON = (
    "issue_scope_legacy_kafka_generation_requires_migration"
)
MANUAL_POLICY_OBSERVED_OUTCOME = "manual_active_policy_observed"
W3_TICKET_AUTHORITY_SCHEMA_VERSION = "pnc_rca_w3_ticket_authority_v1"
W3_INGRESS_AUTHORIZATION_SCHEMA_VERSION = (
    "pnc_rca_w3_ingress_authorization_evidence_v1"
)
W3_MANUAL_AUTHORITY_SCHEMA_VERSION = "pnc_rca_w3_manual_ingress_authority_v1"
INPUT_WAIT_REARM_ERROR_CODES = frozenset(
    {
        "host_issue_preread_empty",
        "host_mcp_preread_empty",
        "host_meegle_preread_empty",
        "issue_enrichment_not_ready",
        "issue_field_invalid_remote_data_reference",
        "issue_field_missing_remote_data_reference",
        "issue_fields_not_ready",
        "issue_not_visible",
    }
)
INPUT_WAIT_RETRY_WINDOW_ERROR_CODES = INPUT_WAIT_REARM_ERROR_CODES | frozenset(
    {
        "host_issue_preread_failed",
        "host_issue_preread_timeout",
        "host_mcp_preread_failed",
        "host_mcp_preread_timeout",
        "host_meegle_preread_failed",
        "host_meegle_preread_timeout",
        "host_preread_unavailable",
        "issue_field_invalid_frame_reference",
        "issue_field_untrusted_remote_data_reference",
    }
)
MANUAL_TRIGGER_SCHEMA_VERSION = "pnc_rca_manual_trigger_v1"
MANUAL_ADMISSION_RESULT_SCHEMA_VERSION = "pnc_rca_manual_admission_result_v1"
MANUAL_ADMISSION_RESULT_FIELDS = frozenset(
    {
        "schema_version",
        "outcome",
        "business_key",
        "submission_key",
        "generation",
        "source_id",
        "subscription_key",
        "state",
        "reason",
    }
)
OUTBOX_PAYLOAD_SCHEMA_VERSION = "pnc_rca_submission_outbox_v2"
DELIVERY_TARGET_SCHEMA_VERSION = "pnc_rca_delivery_target_v1"
LEARNING_LANE_ADMISSION_SCHEMA_VERSION = "g1q3_rca_learning_lane_admission_v1"
LEARNING_LANE_COHORT_SCHEMA_VERSION = "g1q3_rca_learning_lane_cohort_v1"
TERMINAL_RERUN_DELIVERY_AUTHORITY_SCHEMA_VERSION = (
    "pnc_rca_terminal_rerun_delivery_authority_v1"
)
TERMINAL_RERUN_DELIVERY_AUTHORITY_KINDS = frozenset(
    {"silent_terminal", "batch_terminal"}
)
# The W6 stock boundary is a release contract, not an operator-controlled flag.
STOCK_CUTOFF = "2026-07-25T10:15:43.473251+00:00"
LEARNING_LANE_ALLOWED_WRITE_KINDS = ("internal_alert", "vm_submit")
ACTIVATION_ENTRYPOINTS = frozenset({"kafka_ingest", "manual_admit"})
ACTIVATION_SOURCE_KINDS = frozenset({"kafka", "manual"})
_SILENT_TERMINAL_POLICY = "silent_internal_alert_only"
_SILENT_TERMINAL_ROUTE_LANES = frozenset(
    {
        ("internal_alert", "hard_defect"),
        ("internal_backlog", "needs_human_input"),
    }
)
_ACTIVATION_IMMEDIATE_TAXONOMY_GAP_CODES = frozenset(
    {
        # The collector deliberately terminalizes this evidence failure
        # immediately.  Keep its activation exception narrower than the
        # deadline-based taxonomy-gap contract below.
        "viz_evidence_unavailable",
    }
)
_ACTIVATION_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_ACTIVATION_EPOCH_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_CONTROL_STORE_INSTALLATION_MARKERS = (
    ("maintenance", ".pnc-rca-maintenance"),
    ("rollback_tombstone", ".pnc-rca-tombstone"),
)
_ISSUE_URL_RE = re.compile(
    r"^https://project\.feishu\.cn/([A-Za-z0-9._-]+)/issue/detail/([0-9]+)/*$"
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _utc_datetime(value: datetime | None = None) -> datetime:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None:
        raise ValueError("datetime values must be timezone-aware")
    return current.astimezone(timezone.utc)


def _iso(value: datetime | None = None) -> str:
    return _utc_datetime(value).isoformat()


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _canonical_sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def build_silent_terminal_rerun_authority(
    *,
    batch_id: str,
    queue_sha256: str,
    issue_id: str,
    prior_submission_key: str,
    prior_generation: int,
    owner_receipt_path: str,
    owner_receipt_sha256: str,
    requester_id: str,
    reason: str,
    activation_required: bool = True,
) -> dict[str, Any]:
    """Build the exact owner-approved batch authority for one silent terminal."""
    material: dict[str, Any] = {
        "batch_id": str(batch_id or "").strip(),
        "queue_sha256": str(queue_sha256 or "").strip().lower(),
        "issue_id": str(issue_id or "").strip(),
        "prior_submission_key": str(prior_submission_key or "").strip(),
        "prior_generation": prior_generation,
        "owner_receipt_path": str(owner_receipt_path or "").strip(),
        "owner_receipt_sha256": str(owner_receipt_sha256 or "").strip().lower(),
        "activation_required": activation_required,
        "requester_id": str(requester_id or "").strip(),
        "reason": str(reason or "").strip(),
    }
    if (
        re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{2,63}", material["batch_id"])
        is None
        or _ACTIVATION_SHA256_RE.fullmatch(material["queue_sha256"]) is None
        or material["queue_sha256"] == "0" * 64
        or re.fullmatch(r"[0-9]{1,32}", material["issue_id"]) is None
        or re.fullmatch(
            r"g1q3-rca-s1-[0-9a-f]{64}", material["prior_submission_key"]
        )
        is None
        or isinstance(prior_generation, bool)
        or not isinstance(prior_generation, int)
        or prior_generation < 1
        or not Path(material["owner_receipt_path"]).is_absolute()
        or len(material["owner_receipt_path"].encode("utf-8")) > 4096
        or any(
            marker in material["owner_receipt_path"] for marker in ("\x00", "\n", "\r")
        )
        or _ACTIVATION_SHA256_RE.fullmatch(material["owner_receipt_sha256"])
        is None
        or material["owner_receipt_sha256"] == "0" * 64
        or material["activation_required"] is not True
        or not material["requester_id"].startswith("automation:")
        or material["reason"] != f"production_gray_batch:{material['batch_id']}"
    ):
        raise ValueError("silent_terminal_rerun_authority_invalid")
    return {
        "schema_version": SILENT_TERMINAL_RERUN_AUTHORITY_SCHEMA_VERSION,
        **material,
        "selection_sha256": _canonical_sha256(material),
    }


def build_batch_terminal_rerun_authority(
    *,
    batch_id: str,
    queue_sha256: str,
    issue_id: str,
    prior_submission_key: str,
    prior_generation: int,
    prior_delivery_id: str,
    owner_receipt_path: str,
    owner_receipt_sha256: str,
    requester_id: str,
    reason: str,
    activation_required: bool = True,
) -> dict[str, Any]:
    """Build owner-approved authority for a settled delivered-generation correction."""
    material: dict[str, Any] = {
        "batch_id": str(batch_id or "").strip(),
        "queue_sha256": str(queue_sha256 or "").strip().lower(),
        "issue_id": str(issue_id or "").strip(),
        "prior_submission_key": str(prior_submission_key or "").strip(),
        "prior_generation": prior_generation,
        "prior_delivery_id": str(prior_delivery_id or "").strip(),
        "owner_receipt_path": str(owner_receipt_path or "").strip(),
        "owner_receipt_sha256": str(owner_receipt_sha256 or "").strip().lower(),
        "activation_required": activation_required,
        "terminal_mode": "settled_delivery_correction",
        "requester_id": str(requester_id or "").strip(),
        "reason": str(reason or "").strip(),
    }
    if (
        re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{2,63}", material["batch_id"])
        is None
        or _ACTIVATION_SHA256_RE.fullmatch(material["queue_sha256"]) is None
        or material["queue_sha256"] == "0" * 64
        or re.fullmatch(r"[0-9]{1,32}", material["issue_id"]) is None
        or re.fullmatch(
            r"g1q3-rca-s1-[0-9a-f]{64}", material["prior_submission_key"]
        ) is None
        or isinstance(material["prior_generation"], bool)
        or not isinstance(material["prior_generation"], int)
        or material["prior_generation"] < 1
        or re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9._:-]{0,255}", material["prior_delivery_id"]
        ) is None
        or not Path(material["owner_receipt_path"]).is_absolute()
        or len(material["owner_receipt_path"].encode("utf-8")) > 4096
        or any(
            marker in material["owner_receipt_path"] for marker in ("\x00", "\n", "\r")
        )
        or _ACTIVATION_SHA256_RE.fullmatch(material["owner_receipt_sha256"])
        is None
        or material["owner_receipt_sha256"] == "0" * 64
        or material["activation_required"] is not True
        or not material["requester_id"].startswith("automation:")
        or material["reason"] != f"production_gray_batch:{material['batch_id']}"
    ):
        raise ValueError("batch_terminal_rerun_authority_invalid")
    return {
        "schema_version": BATCH_TERMINAL_RERUN_AUTHORITY_SCHEMA_VERSION,
        **material,
        "selection_sha256": _canonical_sha256(material),
    }


def build_historical_epoch_rerun_authority(
    *,
    batch_id: str,
    queue_sha256: str,
    issue_id: str,
    prior_submission_key: str,
    prior_generation: int,
    prior_activation_epoch_id: str,
    prior_activation_ledger_id: int | None,
    target_activation_epoch_id: str,
    owner_receipt_path: str,
    owner_receipt_sha256: str,
    requester_id: str,
    reason: str,
    activation_required: bool = True,
) -> dict[str, Any]:
    """Build exact owner authority to append one generation in the current epoch."""
    material: dict[str, Any] = {
        "batch_id": str(batch_id or "").strip(),
        "queue_sha256": str(queue_sha256 or "").strip().lower(),
        "issue_id": str(issue_id or "").strip(),
        "prior_submission_key": str(prior_submission_key or "").strip(),
        "prior_generation": prior_generation,
        "prior_activation_epoch_id": str(prior_activation_epoch_id or "").strip(),
        "prior_activation_ledger_id": prior_activation_ledger_id,
        "target_activation_epoch_id": str(target_activation_epoch_id or "").strip(),
        "owner_receipt_path": str(owner_receipt_path or "").strip(),
        "owner_receipt_sha256": str(owner_receipt_sha256 or "").strip().lower(),
        "activation_required": activation_required,
        "requester_id": str(requester_id or "").strip(),
        "reason": str(reason or "").strip(),
    }
    prior_epoch = material["prior_activation_epoch_id"]
    target_epoch = material["target_activation_epoch_id"]
    prior_ledger = material["prior_activation_ledger_id"]
    valid_prior_binding = (not prior_epoch and prior_ledger is None) or (
        _ACTIVATION_EPOCH_ID_RE.fullmatch(prior_epoch) is not None
        and not isinstance(prior_ledger, bool)
        and isinstance(prior_ledger, int)
        and prior_ledger >= 1
    )
    if (
        re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{2,63}", material["batch_id"]) is None
        or _ACTIVATION_SHA256_RE.fullmatch(material["queue_sha256"]) is None
        or material["queue_sha256"] == "0" * 64
        or re.fullmatch(r"[0-9]{1,32}", material["issue_id"]) is None
        or re.fullmatch(r"g1q3-rca-s1-[0-9a-f]{64}", material["prior_submission_key"])
        is None
        or isinstance(prior_generation, bool)
        or not isinstance(prior_generation, int)
        or prior_generation < 1
        or not valid_prior_binding
        or _ACTIVATION_EPOCH_ID_RE.fullmatch(target_epoch) is None
        or target_epoch == prior_epoch
        or not Path(material["owner_receipt_path"]).is_absolute()
        or len(material["owner_receipt_path"].encode("utf-8")) > 4096
        or any(
            marker in material["owner_receipt_path"] for marker in ("\x00", "\n", "\r")
        )
        or _ACTIVATION_SHA256_RE.fullmatch(material["owner_receipt_sha256"]) is None
        or material["owner_receipt_sha256"] == "0" * 64
        or material["activation_required"] is not True
        or material["requester_id"] != "automation:rca-batch-rerun"
        or material["reason"] != f"production_gray_batch:{material['batch_id']}"
    ):
        raise ValueError("historical_epoch_rerun_authority_invalid")
    return {
        "schema_version": HISTORICAL_EPOCH_RERUN_AUTHORITY_SCHEMA_VERSION,
        **material,
        "selection_sha256": _canonical_sha256(material),
    }


def _dispatcher_circuit_reset_fingerprint(value: Mapping[str, Any]) -> str:
    payload = dict(value)
    payload.pop("receipt_fingerprint", None)
    return hashlib.sha256(
        json.dumps(
            payload,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def _reject_dispatcher_reset_json_constant(_value: str) -> None:
    raise ValueError("dispatcher_circuit_reset_non_finite_json")


def _validate_dispatcher_circuit_reset_audit(
    value: Mapping[str, Any],
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("dispatcher_circuit_reset_audit_invalid")
    normalized = dict(value)
    if not OUTBOX_CIRCUIT_RESET_REQUIRED_FIELDS.issubset(normalized):
        raise ValueError("dispatcher_circuit_reset_audit_fields_invalid")
    if (
        normalized.get("schema_version") != OUTBOX_CIRCUIT_RESET_SCHEMA_VERSION
        or normalized.get("command") != "clear-circuit"
    ):
        raise ValueError("dispatcher_circuit_reset_audit_schema_invalid")
    reset_id = normalized.get("reset_id")
    recorded_at = normalized.get("recorded_at")
    operator = normalized.get("operator")
    reason = normalized.get("reason")
    if (
        not isinstance(reset_id, str)
        or not reset_id
        or len(reset_id) > 200
        or any(char in reset_id for char in "\n\r\x00")
        or not isinstance(recorded_at, str)
        or not recorded_at
        or not isinstance(operator, str)
        or not operator
        or operator != operator.strip()
        or len(operator.encode("utf-8")) > 200
        or any(char in operator for char in "\n\r\x00")
        or not isinstance(reason, str)
        or not reason
        or reason != reason.strip()
        or len(reason.encode("utf-8")) > 1000
        or any(char in reason for char in "\n\r\x00")
    ):
        raise ValueError("dispatcher_circuit_reset_audit_text_invalid")
    try:
        recorded_datetime = datetime.fromisoformat(recorded_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("dispatcher_circuit_reset_audit_timestamp_invalid") from exc
    if (
        recorded_datetime.tzinfo is None
        or recorded_datetime.utcoffset() != timedelta(0)
    ):
        raise ValueError("dispatcher_circuit_reset_audit_timestamp_invalid")
    identity = normalized.get("control_db_identity")
    if not isinstance(identity, Mapping):
        raise ValueError("dispatcher_circuit_reset_audit_db_identity_invalid")
    identity_path = identity.get("path")
    if (
        not isinstance(identity_path, str)
        or not Path(identity_path).is_absolute()
        or isinstance(identity.get("device"), bool)
        or not isinstance(identity.get("device"), int)
        or isinstance(identity.get("inode"), bool)
        or not isinstance(identity.get("inode"), int)
    ):
        raise ValueError("dispatcher_circuit_reset_audit_db_identity_invalid")
    config_sha = normalized.get("config_binding_sha256")
    if (
        not isinstance(config_sha, str)
        or _ACTIVATION_SHA256_RE.fullmatch(config_sha) is None
        or config_sha == "0" * 64
    ):
        raise ValueError("dispatcher_circuit_reset_audit_config_invalid")
    before = normalized.get("before")
    after = normalized.get("after")
    pre_state = normalized.get("pre_state")
    post_state = normalized.get("post_state")
    if (
        not isinstance(before, Mapping)
        or not isinstance(after, Mapping)
        or dict(before) != dict(pre_state)
        or dict(after) != dict(post_state)
    ):
        raise ValueError("dispatcher_circuit_reset_audit_state_invalid")
    state_fields = frozenset(
        {"state", "reason_code", "reason_detail", "opened_at", "updated_at"}
    )
    for state in (before, after, pre_state, post_state):
        if set(state) != state_fields:
            raise ValueError("dispatcher_circuit_reset_audit_state_invalid")
        if (
            not isinstance(state.get("state"), str)
            or not isinstance(state.get("reason_code"), str)
            or not isinstance(state.get("reason_detail"), str)
            or state.get("opened_at") is not None
            and not isinstance(state.get("opened_at"), str)
            or state.get("updated_at") is not None
            and not isinstance(state.get("updated_at"), str)
        ):
            raise ValueError("dispatcher_circuit_reset_audit_state_invalid")
    if (
        after.get("state") != "closed"
        or after.get("reason_code") != ""
        or after.get("reason_detail") != ""
        or after.get("opened_at") is not None
        or after.get("updated_at") != recorded_at
    ):
        raise ValueError("dispatcher_circuit_reset_audit_state_invalid")
    effect_delta = normalized.get("effect_delta")
    if (
        not isinstance(effect_delta, Mapping)
        or isinstance(effect_delta.get("external_writes"), bool)
        or effect_delta.get("external_writes") != 0
        or not str(effect_delta.get("scope") or "").strip()
    ):
        raise ValueError("dispatcher_circuit_reset_audit_effect_invalid")
    fingerprint = normalized.get("receipt_fingerprint")
    if (
        not isinstance(fingerprint, str)
        or _ACTIVATION_SHA256_RE.fullmatch(fingerprint) is None
        or fingerprint == "0" * 64
        or fingerprint != _dispatcher_circuit_reset_fingerprint(normalized)
    ):
        raise ValueError("dispatcher_circuit_reset_fingerprint_invalid")
    try:
        serialized = _canonical_json(normalized)
    except (TypeError, ValueError) as exc:
        raise ValueError("dispatcher_circuit_reset_audit_json_invalid") from exc
    if len(serialized.encode("utf-8")) > OUTBOX_CIRCUIT_RESET_MAX_AUDIT_BYTES:
        raise ValueError("dispatcher_circuit_reset_audit_too_large")
    return normalized


def build_w3_ticket_authority_receipt(
    *,
    source_kind: str,
    source_evidence_sha256: str,
    observed_at: str,
    project_key: str,
    project_simple_name: str,
    work_item_type_key: str,
    work_item_id: str,
    issue_url: str,
    title: str,
) -> dict[str, Any]:
    """Build the external title pin consumed by W3 admission."""
    from gateway.pnc_rca_snapshot import canonical_ticket_title_sha256

    source = str(source_kind or "").strip()
    evidence = str(source_evidence_sha256 or "").strip()
    if source not in {"kafka_durable_inbox", "feishu_official_preread"}:
        raise ValueError("w3_ticket_authority_source_invalid")
    if _ACTIVATION_SHA256_RE.fullmatch(evidence) is None or evidence == "0" * 64:
        raise ValueError("w3_ticket_authority_evidence_invalid")
    current = _iso(datetime.fromisoformat(str(observed_at).replace("Z", "+00:00")))
    ticket = {
        "project_key": str(project_key or "").strip(),
        "project_simple_name": str(project_simple_name or "").strip(),
        "work_item_type_key": str(work_item_type_key or "").strip(),
        "work_item_id": str(work_item_id or "").strip(),
        "issue_url": str(issue_url or "").strip().rstrip("/"),
        "title": str(title or "").strip(),
    }
    if not all(ticket.values()):
        raise ValueError("w3_ticket_authority_ticket_invalid")
    expected_url = (
        f"https://project.feishu.cn/{ticket['project_simple_name']}/issue/detail/"
        f"{ticket['work_item_id']}"
    )
    if ticket["issue_url"] != expected_url:
        raise ValueError("w3_ticket_authority_issue_url_invalid")
    ticket["title_sha256"] = canonical_ticket_title_sha256(ticket["title"])
    payload = {
        "schema_version": W3_TICKET_AUTHORITY_SCHEMA_VERSION,
        "source_kind": source,
        "source_evidence_sha256": evidence,
        "observed_at": current,
        "ticket": ticket,
    }
    return {**payload, "ticket_authority_sha256": _canonical_sha256(payload)}


def _validate_w3_ticket_authority_receipt(value: Mapping[str, Any]) -> dict[str, Any]:
    expected_fields = {
        "schema_version",
        "ticket_authority_sha256",
        "source_kind",
        "source_evidence_sha256",
        "observed_at",
        "ticket",
    }
    if not isinstance(value, Mapping) or set(value) != expected_fields:
        raise ValueError("w3_ticket_authority_schema_invalid")
    ticket = value.get("ticket")
    if not isinstance(ticket, Mapping) or set(ticket) != {
        "project_key",
        "project_simple_name",
        "work_item_type_key",
        "work_item_id",
        "issue_url",
        "title",
        "title_sha256",
    }:
        raise ValueError("w3_ticket_authority_schema_invalid")
    rebuilt = build_w3_ticket_authority_receipt(
        source_kind=str(value.get("source_kind") or ""),
        source_evidence_sha256=str(value.get("source_evidence_sha256") or ""),
        observed_at=str(value.get("observed_at") or ""),
        project_key=str(ticket.get("project_key") or ""),
        project_simple_name=str(ticket.get("project_simple_name") or ""),
        work_item_type_key=str(ticket.get("work_item_type_key") or ""),
        work_item_id=str(ticket.get("work_item_id") or ""),
        issue_url=str(ticket.get("issue_url") or ""),
        title=str(ticket.get("title") or ""),
    )
    if rebuilt != dict(value):
        raise ValueError("w3_ticket_authority_binding_invalid")
    return rebuilt


def build_w3_manual_ingress_authority(
    *,
    manual_authorization: Mapping[str, Any],
    gateway_runtime_identity: Mapping[str, Any],
    source_identity: Mapping[str, Any],
    ticket_authority_sha256: str,
    snapshot_authority_sha256: str,
) -> dict[str, Any]:
    """Bind a human Gateway decision to one exact manual source identity."""
    from gateway.pnc_rca_runtime_identity import runtime_identity_is_valid

    authorization = dict(manual_authorization)
    if (
        authorization.get("schema_version") != "pnc_rca_manual_authorization_v2"
        or authorization.get("authorized") is not True
        or authorization.get("manual_intake_enabled") is not True
        or authorization.get("manual_chat_allowlist_valid") is not True
        or authorization.get("chat_allowed") is not True
        or authorization.get("mention_verified") is not True
    ):
        raise ValueError("w3_manual_authorization_invalid")
    runtime_identity = dict(gateway_runtime_identity)
    if not runtime_identity_is_valid(
        runtime_identity,
        service_label="ai.hermes.gateway",
    ):
        raise ValueError("w3_manual_gateway_runtime_identity_invalid")
    identity = {
        str(key): value for key, value in dict(source_identity).items()
    }
    required_identity = {
        "platform",
        "chat_id",
        "thread_id",
        "message_id",
        "requester_id",
        "issue_url",
        "mode",
    }
    if set(identity) != required_identity or not all(
        isinstance(identity[name], str) and identity[name].strip()
        for name in required_identity
    ):
        raise ValueError("w3_manual_source_identity_invalid")
    ticket_authority = str(ticket_authority_sha256 or "").strip()
    snapshot_authority = str(snapshot_authority_sha256 or "").strip()
    if any(
        _ACTIVATION_SHA256_RE.fullmatch(item) is None or item == "0" * 64
        for item in (ticket_authority, snapshot_authority)
    ):
        raise ValueError("w3_manual_root_authority_invalid")
    payload = {
        "schema_version": W3_MANUAL_AUTHORITY_SCHEMA_VERSION,
        "manual_authorization_sha256": _canonical_sha256(authorization),
        "gateway_runtime_identity_sha256": _canonical_sha256(runtime_identity),
        "source_identity_sha256": _canonical_sha256(identity),
        "ticket_authority_sha256": ticket_authority,
        "snapshot_authority_sha256": snapshot_authority,
    }
    return {**payload, "authority_sha256": _canonical_sha256(payload)}


def _validate_w3_manual_ingress_authority(
    value: Mapping[str, Any],
    *,
    expected_source_identity: Mapping[str, Any],
    expected_ticket_authority_sha256: str,
    expected_snapshot_authority_sha256: str,
) -> dict[str, Any]:
    expected_fields = {
        "schema_version",
        "manual_authorization_sha256",
        "gateway_runtime_identity_sha256",
        "source_identity_sha256",
        "ticket_authority_sha256",
        "snapshot_authority_sha256",
        "authority_sha256",
    }
    if not isinstance(value, Mapping) or set(value) != expected_fields:
        raise ValueError("w3_manual_ingress_authority_schema_invalid")
    normalized = {str(key): item for key, item in value.items()}
    if normalized["schema_version"] != W3_MANUAL_AUTHORITY_SCHEMA_VERSION:
        raise ValueError("w3_manual_ingress_authority_schema_invalid")
    for name in expected_fields - {"schema_version"}:
        if (
            not isinstance(normalized[name], str)
            or _ACTIVATION_SHA256_RE.fullmatch(normalized[name]) is None
            or normalized[name] == "0" * 64
        ):
            raise ValueError("w3_manual_ingress_authority_hash_invalid")
    if normalized["source_identity_sha256"] != _canonical_sha256(
        dict(expected_source_identity)
    ):
        raise ValueError("w3_manual_ingress_authority_source_mismatch")
    if (
        normalized["ticket_authority_sha256"]
        != expected_ticket_authority_sha256
        or normalized["snapshot_authority_sha256"]
        != expected_snapshot_authority_sha256
    ):
        raise ValueError("w3_manual_ingress_authority_root_mismatch")
    payload = {
        key: normalized[key] for key in expected_fields if key != "authority_sha256"
    }
    if normalized["authority_sha256"] != _canonical_sha256(payload):
        raise ValueError("w3_manual_ingress_authority_binding_invalid")
    return normalized


def _normalize_w3_snapshot_authority(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    runtime_value = (
        value.to_runtime_dict() if hasattr(value, "to_runtime_dict") else value
    )
    if not isinstance(runtime_value, Mapping) or set(runtime_value) != {
        "schema_version",
        "authority_sha256",
        "policies",
    }:
        raise ValueError("w3_snapshot_authority_schema_invalid")
    from gateway.pnc_rca_policy_config import (
        W3_SNAPSHOT_AUTHORITY_SCHEMA_VERSION,
        W3_SNAPSHOT_POLICY_NAMES,
    )
    from gateway.pnc_rca_snapshot import canonical_json_sha256

    if runtime_value["schema_version"] != W3_SNAPSHOT_AUTHORITY_SCHEMA_VERSION:
        raise ValueError("w3_snapshot_authority_schema_invalid")
    policies = runtime_value["policies"]
    if not isinstance(policies, Mapping) or set(policies) != set(
        W3_SNAPSHOT_POLICY_NAMES
    ):
        raise ValueError("w3_snapshot_authority_policy_invalid")
    normalized_policies: dict[str, dict[str, Any]] = {}
    for name in W3_SNAPSHOT_POLICY_NAMES:
        policy = policies[name]
        if not isinstance(policy, Mapping) or set(policy) != {
            "version",
            "sha256",
            "value",
        }:
            raise ValueError("w3_snapshot_authority_policy_invalid")
        version = str(policy["version"] or "").strip()
        policy_sha256 = str(policy["sha256"] or "").strip()
        policy_value = policy["value"]
        if (
            not version
            or _ACTIVATION_SHA256_RE.fullmatch(policy_sha256) is None
            or not isinstance(policy_value, Mapping)
            or not policy_value
            or policy_value.get("state") == "unbound"
        ):
            raise ValueError("w3_snapshot_authority_policy_invalid")
        normalized_policy = {
            "version": version,
            "sha256": policy_sha256,
            "value": dict(policy_value),
        }
        if canonical_json_sha256(
            {"version": version, "value": normalized_policy["value"]}
        ) != policy_sha256:
            raise ValueError("w3_snapshot_authority_policy_invalid")
        normalized_policies[name] = normalized_policy
    authority_body = {
        "schema_version": W3_SNAPSHOT_AUTHORITY_SCHEMA_VERSION,
        "policies": normalized_policies,
    }
    authority_sha256 = str(runtime_value["authority_sha256"] or "").strip()
    if (
        _ACTIVATION_SHA256_RE.fullmatch(authority_sha256) is None
        or authority_sha256 != canonical_json_sha256(authority_body)
    ):
        raise ValueError("w3_snapshot_authority_hash_invalid")
    return {**authority_body, "authority_sha256": authority_sha256}


def _stable_key(prefix: str, material: Mapping[str, Any]) -> str:
    return f"{prefix}-{_canonical_sha256(material)}"


def _bytes(value: bytes | bytearray | memoryview | str) -> bytes:
    if isinstance(value, str):
        return value.encode("utf-8")
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    raise TypeError("Kafka record value must be bytes or string")


def _headers_json(headers: Iterable[tuple[str, bytes | None]]) -> str:
    encoded = []
    for name, value in headers:
        encoded.append({
            "name": str(name),
            "value_b64": base64.b64encode(value).decode("ascii")
            if value is not None
            else None,
        })
    return _canonical_json(encoded)


def _submission_outbox_payload(
    *,
    admission: Any,
    normalized: Any,
    event_uid: str,
    topic: str,
    partition: int,
    offset: int,
    origin_source_id: str,
) -> str:
    """Build the one canonical outbox payload used for create and safe rearm."""
    trigger_context = build_rca_trigger_context(
        source_kind="kafka_workflow_event",
        project_key=normalized.project_key,
        project_simple_name=normalized.project_simple_name,
        work_item_type_key=normalized.work_item_type_key,
        work_item_id=normalized.work_item_id,
        rule_version=normalized.creation_rule_version,
        issue_url=normalized.issue_url,
        title=normalized.title,
    )
    return _canonical_json(
        {
            "schema_version": OUTBOX_PAYLOAD_SCHEMA_VERSION,
            "business_key": admission.business_key,
            "submission_key": admission.submission_key,
            "creation_rule_version": normalized.creation_rule_version,
            "generation": admission.generation,
            "origin_source_id": origin_source_id,
            "source_event_id": event_uid,
            "topic": topic,
            "partition": partition,
            "offset": offset,
            "admission": admission.to_dict(),
            "trigger_context": trigger_context.to_dict(),
            "normalized_event": normalized.to_dict(),
        }
    )


def _manual_submission_outbox_payload(
    *, admission: RcaAdmission, trigger_context: Any, origin_source_id: str
) -> str:
    return _canonical_json(
        {
            "schema_version": OUTBOX_PAYLOAD_SCHEMA_VERSION,
            "business_key": admission.business_key,
            "submission_key": admission.submission_key,
            "creation_rule_version": admission.source_refs.rule_version,
            "generation": admission.generation,
            "origin_source_id": origin_source_id,
            "admission": admission.to_dict(),
            "trigger_context": trigger_context.to_dict(),
        }
    )


@dataclass(frozen=True)
class KafkaRecord:
    topic: str
    partition: int
    offset: int
    value: bytes | bytearray | memoryview | str
    key: bytes | bytearray | memoryview | str | None = None
    timestamp_ms: int | None = None
    headers: tuple[tuple[str, bytes | None], ...] = ()

    def __post_init__(self) -> None:
        topic = str(self.topic or "").strip()
        if not topic:
            raise ValueError("topic must not be empty")
        if isinstance(self.partition, bool) or int(self.partition) < 0:
            raise ValueError("partition must be a non-negative integer")
        if isinstance(self.offset, bool) or int(self.offset) < 0:
            raise ValueError("offset must be a non-negative integer")
        object.__setattr__(self, "topic", topic)
        object.__setattr__(self, "partition", int(self.partition))
        object.__setattr__(self, "offset", int(self.offset))
        object.__setattr__(self, "value", _bytes(self.value))
        if self.key is not None:
            object.__setattr__(self, "key", _bytes(self.key))
        object.__setattr__(self, "headers", tuple(self.headers or ()))

    @property
    def event_uid(self) -> str:
        return f"{self.topic}:{self.partition}:{self.offset}"


@dataclass(frozen=True)
class RawPersistResult:
    event_uid: str
    inserted: bool


@dataclass(frozen=True)
class IngestResult:
    event_uid: str
    decision: str
    reason: str
    raw_inserted: bool
    transport_duplicate: bool
    trigger_created: bool = False
    outbox_created: bool = False
    business_key: str = ""
    submission_key: str = ""
    generation: int = 0
    outbox_rearmed: bool = False
    rearm_reason: str = ""
    ack_safe: bool = True


ManualTriggerMode = Literal["run_or_join", "rerun", "debug"]


@dataclass(frozen=True)
class ManualRcaTriggerRequest:
    schema_version: str
    issue_url: str
    mode: ManualTriggerMode
    reason: str
    platform: str
    chat_id: str
    thread_id: str
    message_id: str
    requester_id: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "issue_url": self.issue_url,
            "mode": self.mode,
            "reason": self.reason,
            "platform": self.platform,
            "chat_id": self.chat_id,
            "thread_id": self.thread_id,
            "message_id": self.message_id,
            "requester_id": self.requester_id,
        }


@dataclass(frozen=True)
class ManualRcaAdmissionResult:
    schema_version: str
    outcome: str
    business_key: str
    submission_key: str
    generation: int
    source_id: str
    subscription_key: str
    state: str
    reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "outcome": self.outcome,
            "business_key": self.business_key,
            "submission_key": self.submission_key,
            "generation": self.generation,
            "source_id": self.source_id,
            "subscription_key": self.subscription_key,
            "state": self.state,
            "reason": self.reason,
        }


ActivationAdmissionOutcome = Literal["admit", "join"]


@dataclass(frozen=True)
class ActivationAdmissionDecision:
    epoch_id: str
    epoch_state: str
    decision: ActivationAdmissionOutcome
    reason: str
    ledger_id: int | None = None


class ActivationEpochError(RuntimeError):
    """The durable production activation state rejected an unsafe mutation."""


class ManualRcaAdmissionError(ValueError):
    """A manual request failed closed before creating execution work."""


class RecordConflictError(RuntimeError):
    """The same Kafka coordinate was observed with different raw bytes."""


class RecordProcessingBlockedError(RuntimeError):
    """One durable record hit an unknown code path and must remain unacknowledged."""

    def __init__(self, event_uid: str):
        self.event_uid = str(event_uid)
        super().__init__(f"durable record processing blocked: {self.event_uid}")


class StaleOutboxLeaseError(RuntimeError):
    """An outbox mutation was attempted without the current fencing token."""


class ControlStoreCapacityError(RuntimeError):
    """The control-store filesystem cannot safely accept another raw event."""


@dataclass(frozen=True)
class OutboxClaim:
    outbox_id: int
    action: str
    business_key: str
    submission_key: str
    creation_rule_version: str
    generation: int
    source_event_id: str | None
    source_topic: str | None
    source_partition: int | None
    source_offset: int | None
    payload: dict[str, Any]
    attempt: int
    fence: int
    lease_token: str
    lease_owner: str
    lease_expires_at: str
    created_at: str
    origin_source_id: str = ""


@dataclass(frozen=True)
class OutboxMutationResult:
    outbox_id: int
    status: str
    attempt: int
    next_attempt_at: str | None = None


@dataclass(frozen=True)
class DispatcherCircuit:
    state: str
    reason_code: str = ""
    reason_detail: str = ""
    opened_at: str | None = None
    updated_at: str | None = None

    @property
    def is_open(self) -> bool:
        return self.state == "open"


class ControlStoreSchemaSnapshot:
    """Own one raw-stable control DB snapshot used for schema probing."""

    def __init__(
        self,
        *,
        temporary: tempfile.TemporaryDirectory[str],
        db_path: Path,
        schema_version: str | None,
        source_identity: Mapping[str, Any],
    ) -> None:
        self._temporary = temporary
        self.db_path = db_path
        self.schema_version = schema_version
        self.source_identity = json.loads(_canonical_json(dict(source_identity)))
        self._closed = False

    def close(self) -> None:
        if not self._closed:
            self._temporary.cleanup()
            self._closed = True

    def __enter__(self) -> ControlStoreSchemaSnapshot:
        if self._closed:
            raise RuntimeError("rca_control_store_schema_snapshot_closed")
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()


class RcaControlStore:
    """SQLite control plane with raw-first persistence and create-once triggers."""

    def __init__(
        self,
        db_path: str | Path,
        *,
        busy_timeout_ms: int = 5000,
        require_current: bool = False,
        read_only: bool = False,
        allow_successor_read_only: bool = False,
        allow_successor_write: bool = False,
    ):
        self.db_path = Path(db_path).expanduser()
        if not isinstance(require_current, bool):
            raise TypeError("require_current must be true or false")
        if not isinstance(read_only, bool):
            raise TypeError("read_only must be true or false")
        if not isinstance(allow_successor_read_only, bool):
            raise TypeError("allow_successor_read_only must be true or false")
        if not isinstance(allow_successor_write, bool):
            raise TypeError("allow_successor_write must be true or false")
        if read_only and not require_current:
            raise ValueError("read_only control store requires current schema")
        if allow_successor_read_only and not require_current:
            raise ValueError(
                "successor-read-only control store requires current schema"
            )
        if allow_successor_write and not require_current:
            raise ValueError("successor-write control store requires current schema")
        if allow_successor_write and (read_only or allow_successor_read_only):
            raise ValueError(
                "successor-write control store cannot also be read-only"
            )
        self.require_current = require_current
        self.requested_read_only = read_only
        self.allow_successor_read_only = allow_successor_read_only
        self.allow_successor_write = allow_successor_write
        self.read_only = read_only
        self._binary_write_schema_version = CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION
        self._connection_write_schema_version = (
            CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION
            if allow_successor_write
            else CONTROL_STORE_SCHEMA_VERSION
        )
        self._enforce_binary_write_schema = False
        self._schema_probe_snapshot: ControlStoreSchemaSnapshot | None = None
        self._read_only_db_path: Path | None = None
        self._read_only_source_identity: dict[str, Any] | None = None
        if require_current:
            self._validate_no_installation_marker()
            self._validate_existing_path()
        else:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.busy_timeout_ms = max(1, int(busy_timeout_ms))
        source_schema_version: str | None = None
        if self.db_path.is_file() and self.db_path.stat().st_size > 0:
            if self.requested_read_only:
                self._create_read_only_snapshot()
                assert self._schema_probe_snapshot is not None
                source_schema_version = self._schema_probe_snapshot.schema_version
            else:
                (
                    source_schema_version,
                    probe_snapshot,
                ) = self.probe_writable_schema_source(
                    self.db_path,
                    expected_write_schema_version=(
                        self._connection_write_schema_version
                        if self.allow_successor_write
                        else None
                    ),
                )
                if probe_snapshot is not None:
                    if source_schema_version == CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION:
                        self._schema_probe_snapshot = probe_snapshot
                        self._read_only_db_path = probe_snapshot.db_path
                        self._read_only_source_identity = probe_snapshot.source_identity
                    else:
                        probe_snapshot.close()
        if source_schema_version == CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION:
            if require_current and allow_successor_write:
                self.read_only = False
            elif not require_current or not allow_successor_read_only:
                self._discard_schema_probe_snapshot()
                raise RuntimeError("incompatible_control_store_schema:version")
            elif self._schema_probe_snapshot is None:
                self._create_read_only_snapshot()
                assert self._schema_probe_snapshot is not None
                if (
                    self._schema_probe_snapshot.schema_version
                    != CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION
                ):
                    self._discard_schema_probe_snapshot()
                    raise RuntimeError(
                        "incompatible_control_store_schema:source_changed"
                    )
            if not allow_successor_write:
                self.read_only = True
        elif allow_successor_write:
            self._discard_schema_probe_snapshot()
            raise RuntimeError("rca_control_store_successor_write_schema_required")
        if self.read_only and self._schema_probe_snapshot is None:
            self._create_read_only_snapshot()
        self._initialization_mode = "unknown"
        self._initialization_backfill_runs = 0
        try:
            if source_schema_version not in {
                CONTROL_STORE_SCHEMA_VERSION,
                CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION,
            }:
                self._discard_schema_probe_snapshot()
            self._initialize(source_schema_version)
            observed_schema_version = (
                source_schema_version
                if source_schema_version
                in {
                    CONTROL_STORE_SCHEMA_VERSION,
                    CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION,
                }
                else self._preflight_schema_version()
            )
        except Exception:
            self._discard_schema_probe_snapshot()
            raise
        if observed_schema_version not in {
            CONTROL_STORE_SCHEMA_VERSION,
            CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION,
        }:
            self._discard_schema_probe_snapshot()
            raise RuntimeError("incompatible_control_store_schema:version")
        self._observed_schema_version = observed_schema_version
        if self.read_only:
            assert self._schema_probe_snapshot is not None
            try:
                self._verify_schema_probe_source_unchanged(self._schema_probe_snapshot)
            except Exception:
                self._discard_schema_probe_snapshot()
                raise
        else:
            self._discard_schema_probe_snapshot()
            self._require_binary_write_schema_at_source()
        self._enforce_binary_write_schema = True

    def _validate_existing_path(self) -> None:
        if not self.db_path.is_absolute():
            raise RuntimeError("rca_control_store_existing_path_not_absolute")
        try:
            observed = self.db_path.lstat()
        except OSError as exc:
            raise RuntimeError("rca_control_store_existing_path_missing") from exc
        if (
            stat.S_ISLNK(observed.st_mode)
            or not stat.S_ISREG(observed.st_mode)
            or observed.st_nlink != 1
            or observed.st_size <= 0
        ):
            raise RuntimeError("rca_control_store_existing_path_invalid")

    def _validate_no_installation_marker(self) -> None:
        for marker_kind, suffix in _CONTROL_STORE_INSTALLATION_MARKERS:
            marker_path = Path(f"{self.db_path}{suffix}")
            try:
                marker_path.lstat()
            except FileNotFoundError:
                continue
            except OSError as exc:
                raise RuntimeError(
                    f"rca_control_store_installation_marker_unreadable:{marker_kind}"
                ) from exc
            raise RuntimeError(
                f"rca_control_store_installation_marker_present:{marker_kind}"
            )

    @staticmethod
    def _snapshot_file(
        source: Path,
        *,
        destination: Path | None,
        required: bool,
    ) -> dict[str, Any]:
        try:
            lexical = source.lstat()
        except FileNotFoundError:
            if required:
                raise RuntimeError("rca_control_store_snapshot_source_missing")
            return {"present": False}
        except OSError as exc:
            raise RuntimeError("rca_control_store_snapshot_source_unreadable") from exc
        if (
            stat.S_ISLNK(lexical.st_mode)
            or not stat.S_ISREG(lexical.st_mode)
            or lexical.st_nlink != 1
            or lexical.st_uid != os.getuid()
            or stat.S_IMODE(lexical.st_mode) & 0o022
        ):
            raise RuntimeError("rca_control_store_snapshot_source_invalid")
        try:
            descriptor = os.open(
                source,
                os.O_RDONLY
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
            )
        except FileNotFoundError:
            if required:
                raise RuntimeError("rca_control_store_snapshot_source_missing")
            return {"present": False}
        except OSError as exc:
            raise RuntimeError("rca_control_store_snapshot_source_unreadable") from exc
        output = -1
        try:
            before = os.fstat(descriptor)
            identity = (
                before.st_dev,
                before.st_ino,
                before.st_size,
                before.st_mtime_ns,
            )
            if identity != (
                lexical.st_dev,
                lexical.st_ino,
                lexical.st_size,
                lexical.st_mtime_ns,
            ):
                raise RuntimeError("rca_control_store_snapshot_source_changed")
            if destination is not None:
                output = os.open(
                    destination,
                    os.O_WRONLY
                    | os.O_CREAT
                    | os.O_EXCL
                    | getattr(os, "O_CLOEXEC", 0)
                    | getattr(os, "O_NOFOLLOW", 0),
                    0o600,
                )
            digest = hashlib.sha256()
            copied = 0
            while True:
                chunk = os.read(descriptor, 1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
                copied += len(chunk)
                if output >= 0:
                    view = memoryview(chunk)
                    while view:
                        written = os.write(output, view)
                        if written <= 0:
                            raise OSError("short snapshot write")
                        view = view[written:]
            if copied != before.st_size:
                raise RuntimeError("rca_control_store_snapshot_source_changed")
            after = os.fstat(descriptor)
            if identity != (
                after.st_dev,
                after.st_ino,
                after.st_size,
                after.st_mtime_ns,
            ):
                raise RuntimeError("rca_control_store_snapshot_source_changed")
            if output >= 0:
                os.fsync(output)
                os.fchmod(output, 0o600)
            return {
                "present": True,
                "device": int(before.st_dev),
                "inode": int(before.st_ino),
                "size": int(before.st_size),
                "mtime_ns": int(before.st_mtime_ns),
                "sha256": digest.hexdigest(),
            }
        except RuntimeError:
            raise
        except OSError as exc:
            raise RuntimeError("rca_control_store_snapshot_copy_failed") from exc
        finally:
            if output >= 0:
                os.close(output)
            os.close(descriptor)

    @classmethod
    def create_schema_probe_snapshot(
        cls,
        db_path: str | Path,
        *,
        allow_successor_read_only: bool = True,
    ) -> ControlStoreSchemaSnapshot:
        """Copy DB/WAL without SQLite source access, then probe only the copy."""

        source_db = Path(db_path).expanduser()
        temporary = tempfile.TemporaryDirectory(prefix="pnc-rca-control-ro-")
        root = Path(temporary.name)
        os.chmod(root, 0o700)
        snapshot_db = root / "control.sqlite3"
        sources = {
            "database": (source_db, snapshot_db, True),
            "wal": (Path(f"{source_db}-wal"), Path(f"{snapshot_db}-wal"), False),
            "shm": (Path(f"{source_db}-shm"), None, False),
        }
        try:
            first = {
                name: cls._snapshot_file(
                    source, destination=destination, required=required
                )
                for name, (source, destination, required) in sources.items()
            }
            second = {
                name: cls._snapshot_file(source, destination=None, required=required)
                for name, (source, _destination, required) in sources.items()
            }
            if first != second:
                raise RuntimeError("rca_control_store_snapshot_source_changed")
            database = first["database"]
            source_identity = {
                "schema_version": "pnc_rca_control_store_source_snapshot_v1",
                "path": str(source_db.absolute()),
                "present": True,
                "device": database["device"],
                "inode": database["inode"],
                "size": database["size"],
                "mtime_ns": database["mtime_ns"],
                "sha256": database["sha256"],
                "wal": first["wal"],
                "shm": first["shm"],
                "logical_db_identity": {
                    "database": database,
                    "wal": first["wal"],
                },
                "coordination_observation": {"shm": first["shm"]},
            }
            schema_version = cls._preflight_schema_version_at(
                snapshot_db,
                allow_successor_read_only=allow_successor_read_only,
            )
            return ControlStoreSchemaSnapshot(
                temporary=temporary,
                db_path=snapshot_db,
                schema_version=schema_version,
                source_identity=source_identity,
            )
        except Exception:
            temporary.cleanup()
            raise

    def _create_read_only_snapshot(self) -> None:
        snapshot = self.create_schema_probe_snapshot(
            self.db_path,
            allow_successor_read_only=self.allow_successor_read_only,
        )
        self._schema_probe_snapshot = snapshot
        self._read_only_db_path = snapshot.db_path
        self._read_only_source_identity = snapshot.source_identity

    def _discard_schema_probe_snapshot(self) -> None:
        if self._schema_probe_snapshot is not None:
            self._schema_probe_snapshot.close()
        self._schema_probe_snapshot = None
        self._read_only_db_path = None
        self._read_only_source_identity = None

    @staticmethod
    def _source_file_metadata(path: Path, *, required: bool) -> dict[str, Any]:
        """Return bounded identity metadata without opening a SQLite source file."""

        try:
            observed = path.lstat()
        except FileNotFoundError:
            if required:
                raise RuntimeError("rca_control_store_snapshot_source_missing")
            return {"present": False}
        except OSError as exc:
            raise RuntimeError("rca_control_store_snapshot_source_unreadable") from exc
        if (
            stat.S_ISLNK(observed.st_mode)
            or not stat.S_ISREG(observed.st_mode)
            or observed.st_nlink != 1
            or observed.st_uid != os.getuid()
            or stat.S_IMODE(observed.st_mode) & 0o022
        ):
            raise RuntimeError("rca_control_store_snapshot_source_invalid")
        return {
            "present": True,
            "device": int(observed.st_dev),
            "inode": int(observed.st_ino),
            "size": int(observed.st_size),
            "mtime_ns": int(observed.st_mtime_ns),
        }

    @classmethod
    def _source_storage_metadata(cls, db_path: Path) -> dict[str, Any]:
        return {
            "database": cls._source_file_metadata(db_path, required=True),
            "wal": cls._source_file_metadata(Path(f"{db_path}-wal"), required=False),
            "shm": cls._source_file_metadata(Path(f"{db_path}-shm"), required=False),
        }

    @classmethod
    def probe_writable_schema_source(
        cls,
        db_path: str | Path,
        *,
        expected_write_schema_version: str | None = None,
    ) -> tuple[str | None, ControlStoreSchemaSnapshot | None]:
        """Probe a writer source; the caller owns any returned snapshot lease."""

        if expected_write_schema_version not in {
            None,
            CONTROL_STORE_SCHEMA_VERSION,
            CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION,
        }:
            raise ValueError("control_store_expected_write_schema_invalid")
        source_db = Path(db_path).expanduser()
        before = cls._source_storage_metadata(source_db)
        wal_present = bool(before["wal"]["present"])
        # A writable runtime must observe uncheckpointed schema commits through
        # SQLite itself. In WAL mode this may create or coordinate the SHM file,
        # but it leaves the main database and WAL payload unchanged. Stable raw
        # snapshots remain reserved for explicit read-only release inspection.
        version = cls._preflight_schema_version_at(
            source_db,
            allow_successor_read_only=True,
            immutable=not wal_present,
        )
        if (
            expected_write_schema_version is not None
            and version != expected_write_schema_version
        ):
            raise RuntimeError("incompatible_control_store_schema:write_marker")
        return version, None

    def _require_binary_write_schema_at_source(self) -> None:
        """Close the constructor TOCTOU window before enabling this writer."""

        try:
            conn = sqlite3.connect(
                self.db_path,
                timeout=self.busy_timeout_ms / 1000,
                isolation_level=None,
            )
            conn.row_factory = sqlite3.Row
            conn.execute("BEGIN")
            observed = self._activation_schema_version_tx(conn)
            conn.commit()
        except (RuntimeError, sqlite3.Error) as exc:
            if "conn" in locals() and conn.in_transaction:
                conn.rollback()
            raise RuntimeError(
                "incompatible_control_store_schema:write_marker"
            ) from exc
        finally:
            if "conn" in locals():
                conn.close()
        if observed != self._connection_write_schema_version:
            raise RuntimeError("incompatible_control_store_schema:write_marker")

    @classmethod
    def _install_connection_write_guards_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        expected_schema_version: str,
        require_exact_schema_cookie: bool,
    ) -> None:
        """Fence every main-table DML statement to one exact schema snapshot."""

        if not conn.in_transaction:
            raise RuntimeError("rca_control_store_write_guard_transaction_required")
        if expected_schema_version not in {
            CONTROL_STORE_SCHEMA_VERSION,
            CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION,
        }:
            raise RuntimeError("incompatible_control_store_schema:write_marker")
        if cls._activation_schema_version_tx(conn) != expected_schema_version:
            raise RuntimeError("incompatible_control_store_schema:write_marker")
        expected_schema_cookie: int | None = None
        if require_exact_schema_cookie:
            schema_cookie = conn.execute("PRAGMA main.schema_version").fetchone()
            if schema_cookie is None:
                raise RuntimeError("incompatible_control_store_schema:write_marker")
            expected_schema_cookie = int(schema_cookie[0])
        table_names = tuple(
            sorted(
                str(row["name"])
                for row in conn.execute(
                    "SELECT name FROM main.sqlite_master "
                    "WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
                ).fetchall()
            )
        )
        if not table_names or "control_meta" not in table_names:
            raise RuntimeError("incompatible_control_store_schema:write_marker")

        marker_literal = expected_schema_version.replace("'", "''")
        trigger_prefix = "pnc_rca_write_guard_"
        expected_triggers: set[str] = set()
        for table_index, table_name in enumerate(table_names):
            quoted_table = '"' + table_name.replace('"', '""') + '"'
            for operation, suffix in (
                ("INSERT", "insert"),
                ("UPDATE", "update"),
                ("DELETE", "delete"),
            ):
                trigger_name = f"{trigger_prefix}{table_index:03d}_{suffix}"
                expected_triggers.add(trigger_name)
                quoted_trigger = '"' + trigger_name.replace('"', '""') + '"'
                conditions = [
                    "COALESCE((SELECT value FROM main.control_meta "
                    "WHERE key = 'schema_version'), '') "
                    f"IS NOT '{marker_literal}'",
                ]
                if expected_schema_cookie is not None:
                    conditions.append(
                        "(SELECT schema_version FROM pragma_schema_version) "
                        f"IS NOT {expected_schema_cookie}"
                    )
                if table_name == "control_meta":
                    if operation == "INSERT":
                        conditions.append("NEW.key IS 'schema_version'")
                    elif operation == "UPDATE":
                        conditions.extend((
                            "OLD.key IS 'schema_version'",
                            "NEW.key IS 'schema_version'",
                        ))
                    else:
                        conditions.append("OLD.key IS 'schema_version'")
                conn.execute(
                    f"CREATE TEMP TRIGGER {quoted_trigger} BEFORE {operation} "
                    f"ON main.{quoted_table} WHEN "
                    + " OR ".join(conditions)
                    + " BEGIN SELECT RAISE(ABORT, "
                    "'incompatible_control_store_schema:write_marker'); END"
                )

        observed_triggers = {
            str(row["name"])
            for row in conn.execute(
                "SELECT name FROM sqlite_temp_master "
                "WHERE type = 'trigger' AND name LIKE ?",
                (f"{trigger_prefix}%",),
            ).fetchall()
        }
        if observed_triggers != expected_triggers:
            raise RuntimeError("incompatible_control_store_schema:write_guard")

    @classmethod
    def _install_v14_connection_write_guards_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        require_exact_schema_cookie: bool,
    ) -> None:
        """Compatibility wrapper for DeliveryStore's MR-A v14 writer."""

        cls._install_connection_write_guards_tx(
            conn,
            expected_schema_version=CONTROL_STORE_SCHEMA_VERSION,
            require_exact_schema_cookie=require_exact_schema_cookie,
        )

    @classmethod
    def _verify_schema_probe_source_unchanged(
        cls,
        snapshot: ControlStoreSchemaSnapshot,
    ) -> None:
        expected = snapshot.source_identity
        source_db = Path(str(expected["path"]))
        expected_database = {
            key: expected[key]
            for key in ("present", "device", "inode", "size", "mtime_ns", "sha256")
        }
        observed = {
            "database": cls._snapshot_file(
                source_db,
                destination=None,
                required=True,
            ),
            "wal": cls._snapshot_file(
                Path(f"{source_db}-wal"),
                destination=None,
                required=False,
            ),
            "shm": cls._snapshot_file(
                Path(f"{source_db}-shm"),
                destination=None,
                required=False,
            ),
        }
        if observed != {
            "database": expected_database,
            "wal": expected["wal"],
            "shm": expected["shm"],
        }:
            raise RuntimeError("rca_control_store_snapshot_source_changed")

    def control_db_source_snapshot_identity(self) -> dict[str, Any]:
        if not self.read_only or self._read_only_source_identity is None:
            raise RuntimeError("rca_control_store_source_snapshot_unavailable")
        return json.loads(_canonical_json(self._read_only_source_identity))

    @property
    def _sqlite_path(self) -> Path:
        if self._read_only_db_path is not None:
            return self._read_only_db_path
        if self.read_only:
            raise RuntimeError("rca_control_store_read_only_snapshot_missing")
        return self.db_path

    def _connect(self) -> sqlite3.Connection:
        if self.require_current:
            self._validate_no_installation_marker()
        if self.read_only:
            conn = sqlite3.connect(
                f"{self._sqlite_path.resolve().as_uri()}?mode=ro",
                uri=True,
                timeout=self.busy_timeout_ms / 1000,
                isolation_level=None,
            )
        else:
            conn = sqlite3.connect(
                self.db_path,
                timeout=self.busy_timeout_ms / 1000,
                isolation_level=None,
            )
        conn.row_factory = sqlite3.Row
        if not self.read_only and self._enforce_binary_write_schema:
            try:
                observed_write_schema = self._activation_schema_version_tx(conn)
            except (RuntimeError, sqlite3.Error) as exc:
                conn.close()
                raise RuntimeError(
                    "incompatible_control_store_schema:write_marker"
                ) from exc
            if observed_write_schema != self._connection_write_schema_version:
                conn.close()
                raise RuntimeError("incompatible_control_store_schema:write_marker")
        conn.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA recursive_triggers=ON")
        recursive_triggers = conn.execute("PRAGMA recursive_triggers").fetchone()
        if recursive_triggers is None or int(recursive_triggers[0]) != 1:
            conn.close()
            raise RuntimeError("rca_control_store_recursive_triggers_disabled")
        if self.read_only:
            conn.execute("PRAGMA query_only=ON")
        else:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=FULL")
            if self._enforce_binary_write_schema:
                try:
                    conn.execute("BEGIN")
                    self._install_connection_write_guards_tx(
                        conn,
                        expected_schema_version=self._connection_write_schema_version,
                        require_exact_schema_cookie=self.require_current,
                    )
                    conn.commit()
                except (RuntimeError, sqlite3.Error) as exc:
                    if conn.in_transaction:
                        conn.rollback()
                    conn.close()
                    raise RuntimeError(
                        "incompatible_control_store_schema:write_marker"
                    ) from exc
        return conn

    def _initialize(self, marker_value: str | None) -> None:
        accepted_current_versions = {CONTROL_STORE_SCHEMA_VERSION}
        if self.allow_successor_read_only:
            accepted_current_versions.add(CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION)
        if self.allow_successor_write:
            accepted_current_versions = {CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION}
        if self.require_current and marker_value not in accepted_current_versions:
            raise RuntimeError("rca_control_store_schema_not_current")
        if marker_value == CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION:
            if self.allow_successor_write and not self.read_only:
                self._validate_current_schema_read_only()
                self._validate_no_installation_marker()
                self._initialization_mode = "steady"
                return
            if not self.allow_successor_read_only or not self.read_only:
                raise RuntimeError("rca_control_store_successor_requires_read_only")
            self._validate_current_schema_read_only()
            self._validate_no_installation_marker()
            self._initialization_mode = "successor_read_only"
            return
        if marker_value == CONTROL_STORE_SCHEMA_VERSION:
            self._validate_current_schema_read_only()
            if self.require_current:
                self._validate_no_installation_marker()
            self._initialization_mode = "steady"
            return

        if marker_value == "pnc_rca_control_store_v10":
            migrated = self._migrate_v10_to_v11()
            migrated = self._migrate_v11_to_v12() or migrated
            migrated = self._migrate_v12_to_v13() or migrated
            migrated = self._migrate_v13_to_v14() or migrated
            self._initialization_mode = "migration" if migrated else "steady"
            return

        if marker_value == "pnc_rca_control_store_v11":
            migrated = self._migrate_v11_to_v12()
            migrated = self._migrate_v12_to_v13() or migrated
            migrated = self._migrate_v13_to_v14() or migrated
            self._initialization_mode = "migration" if migrated else "steady"
            return

        if marker_value == "pnc_rca_control_store_v12":
            migrated = self._migrate_v12_to_v13()
            migrated = self._migrate_v13_to_v14() or migrated
            self._initialization_mode = "migration" if migrated else "steady"
            return

        if marker_value == "pnc_rca_control_store_v13":
            self._initialization_mode = (
                "migration" if self._migrate_v13_to_v14() else "steady"
            )
            return

        self._initialization_mode = "migration"
        conn = self._connect()
        try:
            if self._table_exists(conn, "kafka_inbox"):
                self._migrate_inbox_columns(conn)
            if self._table_exists(conn, "rca_outbox"):
                self._migrate_outbox_columns(conn)
            # A previously installed additive authority trigger references
            # source tables that this legacy migration temporarily removes.
            self._drop_historical_epoch_rerun_delivery_authority_triggers(conn)
            conn.execute("PRAGMA foreign_keys=OFF")
            self._migrate_source_neutral_parents(conn)
            conn.execute("PRAGMA foreign_keys=ON")
            # The physical v14 epoch DDL stays inline and byte-stable because
            # sqlite_master.sql is part of the audited compatibility surface.
            conn.executescript(
                """
                BEGIN IMMEDIATE;

                CREATE TABLE IF NOT EXISTS control_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS rca_activation_epochs (
                    epoch_id TEXT PRIMARY KEY,
                    state TEXT NOT NULL CHECK (state IN (
                        'safe_off', 'preauthorized', 'bounded_active',
                        'confirmed', 'steady_active', 'aborted'
                    )),
                    is_current INTEGER NOT NULL CHECK (is_current IN (0, 1)),
                    preauthorization_fingerprint TEXT NOT NULL,
                    preauthorization_gate_receipt_sha256 TEXT NOT NULL,
                    preauthorization_capsule_sha256 TEXT NOT NULL,
                    preproduction_fingerprint TEXT,
                    preproduction_gate_receipt_sha256 TEXT,
                    preproduction_capsule_sha256 TEXT,
                    config_sha256 TEXT NOT NULL,
                    db_logical_identity_json TEXT NOT NULL,
                    db_logical_identity_sha256 TEXT NOT NULL,
                    partition_start_fence_json TEXT NOT NULL,
                    partition_start_fence_sha256 TEXT NOT NULL,
                    partition_end_fence_json TEXT,
                    partition_end_fence_sha256 TEXT,
                    production_fingerprint TEXT,
                    production_gate_receipt_sha256 TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    bounded_activated_at TEXT,
                    confirmed_at TEXT,
                    steady_activated_at TEXT,
                    aborted_at TEXT,
                    superseded_at TEXT,
                    CHECK (
                        (
                            preproduction_fingerprint IS NULL
                            AND preproduction_gate_receipt_sha256 IS NULL
                            AND preproduction_capsule_sha256 IS NULL
                        ) OR (
                            preproduction_fingerprint IS NOT NULL
                            AND preproduction_gate_receipt_sha256 IS NOT NULL
                            AND preproduction_capsule_sha256 IS NOT NULL
                        )
                    ),
                    CHECK (
                        state != 'safe_off' OR preproduction_fingerprint IS NULL
                    ),
                    CHECK (
                        state NOT IN (
                            'preauthorized', 'bounded_active',
                            'confirmed', 'steady_active'
                        ) OR preproduction_fingerprint IS NOT NULL
                    )
                );

                CREATE UNIQUE INDEX IF NOT EXISTS idx_rca_single_current_activation_epoch
                    ON rca_activation_epochs(is_current) WHERE is_current = 1;

                CREATE TABLE IF NOT EXISTS rca_activation_admission_ledger (
                    ledger_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    epoch_id TEXT NOT NULL,
                    admission_key TEXT NOT NULL,
                    entrypoint TEXT NOT NULL CHECK (entrypoint IN (
                        'kafka_ingest', 'manual_admit', 'shadow_promotion'
                    )),
                    source_kind TEXT NOT NULL CHECK (source_kind IN ('kafka', 'manual')),
                    source_identity_sha256 TEXT NOT NULL,
                    decision TEXT NOT NULL CHECK (
                        decision IN ('admit', 'join', 'shadow', 'reject')
                    ),
                    reason TEXT NOT NULL,
                    business_key TEXT NOT NULL,
                    submission_key TEXT NOT NULL,
                    generation INTEGER NOT NULL CHECK (generation >= 1),
                    adjudication_count INTEGER NOT NULL DEFAULT 1
                        CHECK (adjudication_count >= 1),
                    first_adjudicated_at TEXT NOT NULL,
                    last_adjudicated_at TEXT NOT NULL,
                    admitted_at TEXT,
                    bound_at TEXT,
                    UNIQUE(epoch_id, admission_key),
                    FOREIGN KEY(epoch_id) REFERENCES rca_activation_epochs(epoch_id)
                );

                CREATE INDEX IF NOT EXISTS idx_rca_activation_ledger_submission
                    ON rca_activation_admission_ledger(
                        submission_key, generation, ledger_id
                    );

                CREATE TABLE IF NOT EXISTS rca_activation_transition_audit (
                    audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    epoch_id TEXT NOT NULL,
                    from_state TEXT NOT NULL,
                    to_state TEXT NOT NULL,
                    operator TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    binding_fingerprint TEXT NOT NULL,
                    transitioned_at TEXT NOT NULL,
                    FOREIGN KEY(epoch_id) REFERENCES rca_activation_epochs(epoch_id)
                );

                CREATE INDEX IF NOT EXISTS idx_rca_activation_transition_epoch
                    ON rca_activation_transition_audit(epoch_id, audit_id);

                CREATE TABLE IF NOT EXISTS kafka_inbox (
                    event_uid TEXT PRIMARY KEY,
                    topic TEXT NOT NULL,
                    partition_id INTEGER NOT NULL CHECK (partition_id >= 0),
                    offset_id INTEGER NOT NULL CHECK (offset_id >= 0),
                    kafka_timestamp_ms INTEGER,
                    record_key BLOB,
                    raw_value BLOB NOT NULL,
                    raw_size_bytes INTEGER NOT NULL CHECK (raw_size_bytes >= 0),
                    raw_sha256 TEXT NOT NULL,
                    headers_json TEXT NOT NULL,
                    policy_json TEXT NOT NULL,
                    creation_rule_version TEXT NOT NULL,
                    submission_mode TEXT NOT NULL CHECK (submission_mode IN ('shadow', 'pending')),
                    submit_enabled_requested INTEGER NOT NULL DEFAULT 0
                        CHECK (submit_enabled_requested IN (0, 1)),
                    received_at TEXT NOT NULL,
                    decision TEXT NOT NULL DEFAULT 'pending',
                    reason TEXT NOT NULL DEFAULT '',
                    normalized_json TEXT,
                    business_key TEXT,
                    submission_key TEXT,
                    generation INTEGER,
                    activation_epoch_id TEXT,
                    activation_ingress_state TEXT NOT NULL DEFAULT 'legacy_unconfigured',
                    activation_required INTEGER NOT NULL DEFAULT 0
                        CHECK (activation_required IN (0, 1)),
                    activation_source_identity_sha256 TEXT NOT NULL DEFAULT '',
                    rearm_reason TEXT NOT NULL DEFAULT '',
                    processing_attempts INTEGER NOT NULL DEFAULT 0,
                    last_processing_error_code TEXT NOT NULL DEFAULT '',
                    last_processing_error_detail TEXT NOT NULL DEFAULT '',
                    processing_failed_at TEXT,
                    processed_at TEXT,
                    raw_pruned_at TEXT,
                    UNIQUE(topic, partition_id, offset_id)
                );

                CREATE TABLE IF NOT EXISTS business_triggers (
                    business_key TEXT NOT NULL,
                    generation INTEGER NOT NULL CHECK (generation >= 1),
                    submission_key TEXT NOT NULL UNIQUE,
                    creation_rule_version TEXT NOT NULL,
                    work_item_id TEXT NOT NULL,
                    project_key TEXT NOT NULL,
                    work_item_type_key TEXT NOT NULL,
                    activation_epoch_id TEXT,
                    activation_ledger_id INTEGER,
                    origin_source_id TEXT,
                    source_event_id TEXT,
                    source_topic TEXT,
                    source_partition INTEGER,
                    source_offset INTEGER,
                    normalized_json TEXT NOT NULL,
                    state TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (business_key, generation)
                );

                CREATE TABLE IF NOT EXISTS kafka_partition_progress (
                    topic TEXT NOT NULL,
                    partition_id INTEGER NOT NULL CHECK (partition_id >= 0),
                    first_offset INTEGER NOT NULL CHECK (first_offset >= 0),
                    durable_next_offset INTEGER NOT NULL CHECK (durable_next_offset > 0),
                    last_event_uid TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (topic, partition_id),
                    FOREIGN KEY (last_event_uid) REFERENCES kafka_inbox(event_uid)
                );

                CREATE TABLE IF NOT EXISTS rca_outbox (
                    outbox_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    action TEXT NOT NULL,
                    business_key TEXT NOT NULL,
                    submission_key TEXT NOT NULL UNIQUE,
                    creation_rule_version TEXT NOT NULL,
                    generation INTEGER NOT NULL,
                    activation_epoch_id TEXT,
                    activation_ledger_id INTEGER,
                    origin_source_id TEXT,
                    source_event_id TEXT,
                    source_topic TEXT,
                    source_partition INTEGER,
                    source_offset INTEGER,
                    payload_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    attempt INTEGER NOT NULL DEFAULT 0,
                    next_attempt_at TEXT,
                    fence INTEGER NOT NULL DEFAULT 0,
                    lease_token TEXT,
                    lease_owner TEXT,
                    lease_expires_at TEXT,
                    claimed_at TEXT,
                    completed_at TEXT,
                    quarantined_at TEXT,
                    last_error_code TEXT NOT NULL DEFAULT '',
                    last_error_detail TEXT NOT NULL DEFAULT '',
                    result_json TEXT,
                    retry_window_started_at TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (business_key, generation)
                        REFERENCES business_triggers(business_key, generation)
                );

                CREATE TABLE IF NOT EXISTS rca_policy_snapshots (
                    policy_sha256 TEXT PRIMARY KEY,
                    policy_version TEXT NOT NULL,
                    policy_json TEXT NOT NULL,
                    active INTEGER NOT NULL CHECK(active IN (0, 1)),
                    activated_at TEXT NOT NULL
                );

                CREATE UNIQUE INDEX IF NOT EXISTS idx_rca_one_active_policy
                    ON rca_policy_snapshots(active) WHERE active = 1;

                CREATE TABLE IF NOT EXISTS rca_trigger_sources (
                    source_id TEXT PRIMARY KEY,
                    source_kind TEXT NOT NULL CHECK(
                        source_kind IN ('kafka_workflow_event', 'feishu_group_manual')
                    ),
                    source_dedupe_key TEXT NOT NULL UNIQUE,
                    payload_sha256 TEXT NOT NULL,
                    platform TEXT NOT NULL DEFAULT '',
                    chat_id TEXT NOT NULL DEFAULT '',
                    thread_id TEXT NOT NULL DEFAULT '',
                    message_id TEXT NOT NULL DEFAULT '',
                    requester_id TEXT NOT NULL DEFAULT '',
                    kafka_event_uid TEXT UNIQUE,
                    mode TEXT NOT NULL DEFAULT '',
                    outcome TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(kafka_event_uid) REFERENCES kafka_inbox(event_uid)
                );

                CREATE TABLE IF NOT EXISTS rca_trigger_bindings (
                    source_id TEXT PRIMARY KEY,
                    business_key TEXT NOT NULL,
                    generation INTEGER NOT NULL CHECK(generation >= 1),
                    role TEXT NOT NULL,
                    bound_at TEXT NOT NULL,
                    FOREIGN KEY(source_id) REFERENCES rca_trigger_sources(source_id),
                    FOREIGN KEY(business_key, generation)
                        REFERENCES business_triggers(business_key, generation)
                );

                CREATE TABLE IF NOT EXISTS rca_delivery_subscriptions (
                    subscription_key TEXT PRIMARY KEY,
                    business_key TEXT NOT NULL,
                    generation INTEGER NOT NULL CHECK(generation >= 1),
                    source_id TEXT,
                    effect_kind TEXT NOT NULL CHECK(
                        effect_kind IN ('feishu_issue_comment', 'feishu_thread_reply')
                    ),
                    target_key TEXT NOT NULL,
                    target_json TEXT NOT NULL,
                    required INTEGER NOT NULL CHECK(required IN (0, 1)),
                    status TEXT NOT NULL CHECK(
                        status IN ('pending', 'materialized', 'suppressed', 'quarantined')
                    ),
                    reason TEXT NOT NULL DEFAULT 'awaiting_delivery_materialization',
                    delivery_id TEXT,
                    effect_key TEXT,
                    catchup_requested_at TEXT,
                    materialized_at TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(business_key, generation, effect_kind, target_key),
                    FOREIGN KEY(business_key, generation)
                        REFERENCES business_triggers(business_key, generation),
                    FOREIGN KEY(source_id) REFERENCES rca_trigger_sources(source_id)
                );

                CREATE INDEX IF NOT EXISTS idx_rca_delivery_sub_pending
                    ON rca_delivery_subscriptions(status, catchup_requested_at, created_at);

                CREATE TABLE IF NOT EXISTS rca_trigger_delivery_bindings (
                    source_id TEXT NOT NULL,
                    subscription_key TEXT NOT NULL,
                    bound_at TEXT NOT NULL,
                    PRIMARY KEY(source_id, subscription_key),
                    FOREIGN KEY(source_id) REFERENCES rca_trigger_sources(source_id),
                    FOREIGN KEY(subscription_key)
                        REFERENCES rca_delivery_subscriptions(subscription_key)
                );

                CREATE TABLE IF NOT EXISTS kafka_dead_letters (
                    dead_letter_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_event_id TEXT NOT NULL UNIQUE,
                    source_topic TEXT NOT NULL,
                    source_partition INTEGER NOT NULL,
                    source_offset INTEGER NOT NULL,
                    error_code TEXT NOT NULL,
                    error_detail TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (source_event_id) REFERENCES kafka_inbox(event_uid)
                );

                CREATE TABLE IF NOT EXISTS rca_dispatcher_circuit (
                    circuit_name TEXT PRIMARY KEY,
                    state TEXT NOT NULL CHECK (state IN ('closed', 'open')),
                    reason_code TEXT NOT NULL DEFAULT '',
                    reason_detail TEXT NOT NULL DEFAULT '',
                    opened_at TEXT,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS rca_shadow_promotion_audit (
                    audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_uid TEXT NOT NULL,
                    outbox_id INTEGER,
                    submission_key TEXT NOT NULL DEFAULT '',
                    operator TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    from_status TEXT NOT NULL DEFAULT '',
                    to_status TEXT NOT NULL DEFAULT '',
                    detail TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS rca_outbox_rearm_audit (
                    audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    outbox_id INTEGER NOT NULL,
                    submission_key TEXT NOT NULL,
                    prior_source_event_id TEXT NOT NULL,
                    replacement_source_event_id TEXT NOT NULL,
                    prior_attempt INTEGER NOT NULL CHECK (prior_attempt >= 0),
                    prior_fence INTEGER NOT NULL CHECK (prior_fence >= 0),
                    prior_error_code TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (outbox_id) REFERENCES rca_outbox(outbox_id),
                    FOREIGN KEY (prior_source_event_id)
                        REFERENCES kafka_inbox(event_uid),
                    FOREIGN KEY (replacement_source_event_id)
                        REFERENCES kafka_inbox(event_uid)
                );

                CREATE INDEX IF NOT EXISTS idx_inbox_decision
                    ON kafka_inbox(decision, received_at);
                CREATE INDEX IF NOT EXISTS idx_outbox_status
                    ON rca_outbox(status, next_attempt_at, outbox_id);
                """
            )
            ensure_host_runtime_transition_schema(conn)
            self._migrate_inbox_columns(conn)
            self._migrate_outbox_columns(conn)
            self._migrate_activation_columns(conn)
            # The source backfill below may classify post-cutoff stock rows;
            # install its durable target schema before invoking that path.
            self._create_v12_learning_lane_schema(conn)
            self._create_v14_terminal_rerun_delivery_authority_schema(conn)
            self._create_historical_epoch_rerun_delivery_authority_schema(conn)
            if self._learning_delivery_schema_present(conn):
                self._ensure_learning_lane_cohort_tx(conn, sealed_at=_now_iso())
            self._initialization_backfill_runs += 1
            self._backfill_kafka_sources_and_subscriptions(conn)
            marker = conn.execute(
                "SELECT value FROM control_meta WHERE key = 'schema_version'"
            ).fetchone()
            marker_value = str(marker["value"]) if marker is not None else ""
            if (
                marker is not None
                and marker_value not in SUPPORTED_CONTROL_STORE_SCHEMA_VERSIONS
            ):
                raise RuntimeError("incompatible_control_store_schema:version")
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_outbox_lease
                    ON rca_outbox(status, lease_expires_at, outbox_id)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_shadow_promotion_event
                    ON rca_shadow_promotion_audit(event_uid, audit_id)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_outbox_rearm_submission
                    ON rca_outbox_rearm_audit(submission_key, audit_id)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_business_triggers_issue_scope
                    ON business_triggers(
                        project_key, work_item_type_key, work_item_id,
                        generation DESC, created_at DESC
                    )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_rca_manual_operator_rate
                    ON rca_trigger_sources(requester_id, created_at)
                    WHERE source_kind = 'feishu_group_manual'
                      AND mode IN ('rerun', 'debug')
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_outbox_activation_claim
                    ON rca_outbox(
                        activation_epoch_id, status, next_attempt_at, outbox_id
                    )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_outbox_source_status
                    ON rca_outbox(source_topic, status, outbox_id)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_trigger_bindings_generation
                    ON rca_trigger_bindings(business_key, generation, source_id)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_trigger_sources_kind_outcome
                    ON rca_trigger_sources(source_kind, outcome, source_id)
                """
            )
            self._create_v11_snapshot_schema(conn)
            self._create_v12_learning_lane_schema(conn)
            self._create_v14_terminal_rerun_delivery_authority_schema(conn)
            self._create_historical_epoch_rerun_delivery_authority_schema(conn)
            self._validate_structural_contract(
                conn,
                integrity_check=marker_value != CONTROL_STORE_SCHEMA_VERSION,
            )
            conn.execute(
                """
                INSERT OR IGNORE INTO rca_dispatcher_circuit(
                    circuit_name, state, updated_at
                ) VALUES('submission', 'closed', ?)
                """,
                (_now_iso(),),
            )
            conn.execute(
                "INSERT INTO control_meta(key, value) VALUES('schema_version', ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (CONTROL_STORE_SCHEMA_VERSION,),
            )
            conn.commit()
        except Exception:
            if conn.in_transaction:
                conn.rollback()
            raise
        finally:
            conn.close()

    def _migrate_v10_to_v11(self) -> bool:
        """Install the inert W3 snapshot schema in one rollback-safe transaction."""
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            marker = conn.execute(
                "SELECT value FROM control_meta WHERE key = 'schema_version'"
            ).fetchone()
            marker_value = str(marker["value"]) if marker is not None else ""
            if marker_value == CONTROL_STORE_SCHEMA_VERSION:
                self._validate_structural_contract(conn, integrity_check=False)
                conn.commit()
                return False
            if marker_value != "pnc_rca_control_store_v10":
                raise RuntimeError("incompatible_control_store_schema:version_marker")
            self._create_v11_snapshot_schema(conn)
            self._validate_structural_contract(conn, integrity_check=True)
            conn.execute(
                "UPDATE control_meta SET value = ? "
                "WHERE key = 'schema_version' AND value = ?",
                (
                    "pnc_rca_control_store_v11",
                    "pnc_rca_control_store_v10",
                ),
            )
            if conn.execute("SELECT changes()").fetchone()[0] != 1:
                raise RuntimeError("incompatible_control_store_schema:version_marker")
            conn.commit()
            return True
        except Exception:
            if conn.in_transaction:
                conn.rollback()
            raise
        finally:
            conn.close()

    def _migrate_v11_to_v12(self) -> bool:
        """Install the immutable W6 stock cohort and learning admission schema."""
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            marker = conn.execute(
                "SELECT value FROM control_meta WHERE key = 'schema_version'"
            ).fetchone()
            marker_value = str(marker["value"]) if marker is not None else ""
            if marker_value in {
                "pnc_rca_control_store_v12",
                "pnc_rca_control_store_v13",
                CONTROL_STORE_SCHEMA_VERSION,
            }:
                self._validate_structural_contract(conn, integrity_check=False)
                conn.commit()
                return False
            if marker_value != "pnc_rca_control_store_v11":
                raise RuntimeError("incompatible_control_store_schema:version_marker")
            self._create_v12_learning_lane_schema(conn)
            if self._learning_delivery_schema_present(conn):
                self._ensure_learning_lane_cohort_tx(
                    conn, sealed_at=_now_iso()
                )
            self._validate_structural_contract(conn, integrity_check=True)
            updated = conn.execute(
                "UPDATE control_meta SET value = ? "
                "WHERE key = 'schema_version' AND value = ?",
                ("pnc_rca_control_store_v12", "pnc_rca_control_store_v11"),
            )
            if updated.rowcount != 1:
                raise RuntimeError("incompatible_control_store_schema:version_marker")
            conn.commit()
            return True
        except Exception:
            if conn.in_transaction:
                conn.rollback()
            raise
        finally:
            conn.close()

    def _migrate_v12_to_v13(self) -> bool:
        """Advance the legacy marker without installing retired hold schema."""
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            marker = conn.execute(
                "SELECT value FROM control_meta WHERE key = 'schema_version'"
            ).fetchone()
            marker_value = str(marker["value"]) if marker is not None else ""
            if marker_value in {
                "pnc_rca_control_store_v13",
                CONTROL_STORE_SCHEMA_VERSION,
            }:
                self._validate_structural_contract(conn, integrity_check=False)
                conn.commit()
                return False
            if marker_value != "pnc_rca_control_store_v12":
                raise RuntimeError("incompatible_control_store_schema:version_marker")
            self._validate_v12_learning_lane_schema(conn)
            self._validate_structural_contract(conn, integrity_check=True)
            updated = conn.execute(
                "UPDATE control_meta SET value = ? "
                "WHERE key = 'schema_version' AND value = ?",
                ("pnc_rca_control_store_v13", "pnc_rca_control_store_v12"),
            )
            if updated.rowcount != 1:
                raise RuntimeError("incompatible_control_store_schema:version_marker")
            conn.commit()
            return True
        except Exception:
            if conn.in_transaction:
                conn.rollback()
            raise
        finally:
            conn.close()

    def _migrate_v13_to_v14(self) -> bool:
        """Install immutable terminal-rerun delivery authority atomically."""
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            marker = conn.execute(
                "SELECT value FROM control_meta WHERE key = 'schema_version'"
            ).fetchone()
            marker_value = str(marker["value"]) if marker is not None else ""
            if marker_value == CONTROL_STORE_SCHEMA_VERSION:
                self._validate_structural_contract(conn, integrity_check=False)
                conn.commit()
                return False
            if marker_value != "pnc_rca_control_store_v13":
                raise RuntimeError("incompatible_control_store_schema:version_marker")
            self._create_v14_terminal_rerun_delivery_authority_schema(conn)
            self._create_historical_epoch_rerun_delivery_authority_schema(conn)
            self._validate_structural_contract(conn, integrity_check=True)
            self._validate_historical_epoch_rerun_delivery_authority_schema(conn)
            updated = conn.execute(
                "UPDATE control_meta SET value = ? "
                "WHERE key = 'schema_version' AND value = ?",
                (CONTROL_STORE_SCHEMA_VERSION, "pnc_rca_control_store_v13"),
            )
            if updated.rowcount != 1:
                raise RuntimeError("incompatible_control_store_schema:version_marker")
            conn.commit()
            return True
        except Exception:
            if conn.in_transaction:
                conn.rollback()
            raise
        finally:
            conn.close()

    @staticmethod
    def _v11_snapshot_schema_statements() -> tuple[str, ...]:
        """Return the exact inert W3 DDL used by migration and validation."""
        statements = (
            """
            CREATE TABLE IF NOT EXISTS rca_canonical_requests (
                request_sha256 TEXT PRIMARY KEY
                    CHECK(length(request_sha256) = 64
                          AND request_sha256 NOT GLOB '*[^0-9a-f]*'),
                schema_version TEXT NOT NULL
                    CHECK(schema_version = 'pnc_rca_canonical_request_v1'),
                ticket_title_sha256 TEXT NOT NULL
                    CHECK(length(ticket_title_sha256) = 64
                          AND ticket_title_sha256 NOT GLOB '*[^0-9a-f]*'),
                creation_policy_sha256 TEXT NOT NULL
                    CHECK(length(creation_policy_sha256) = 64
                          AND creation_policy_sha256 NOT GLOB '*[^0-9a-f]*'),
                business_profile_sha256 TEXT NOT NULL
                    CHECK(length(business_profile_sha256) = 64
                          AND business_profile_sha256 NOT GLOB '*[^0-9a-f]*'),
                execution_policy_sha256 TEXT NOT NULL
                    CHECK(length(execution_policy_sha256) = 64
                          AND execution_policy_sha256 NOT GLOB '*[^0-9a-f]*'),
                publication_policy_sha256 TEXT NOT NULL
                    CHECK(length(publication_policy_sha256) = 64
                          AND publication_policy_sha256 NOT GLOB '*[^0-9a-f]*'),
                correction_lineage_policy_sha256 TEXT NOT NULL
                    CHECK(length(correction_lineage_policy_sha256) = 64
                          AND correction_lineage_policy_sha256 NOT GLOB '*[^0-9a-f]*'),
                generation_reason TEXT NOT NULL
                    CHECK(generation_reason IN ('initial', 'explicit_user_rerun')),
                generation_authorization_evidence_sha256 TEXT
                    CHECK(generation_authorization_evidence_sha256 IS NULL OR (
                        length(generation_authorization_evidence_sha256) = 64
                        AND generation_authorization_evidence_sha256
                            NOT GLOB '*[^0-9a-f]*'
                        AND generation_authorization_evidence_sha256 != printf('%064d', 0)
                    )),
                canonical_request_json TEXT NOT NULL
                    CHECK(json_valid(canonical_request_json)
                          AND json_type(canonical_request_json) = 'object'),
                persisted_at TEXT NOT NULL CHECK(length(trim(persisted_at)) > 0),
                CHECK(
                    (generation_reason = 'initial'
                     AND generation_authorization_evidence_sha256 IS NULL)
                    OR
                    (generation_reason = 'explicit_user_rerun'
                     AND generation_authorization_evidence_sha256 IS NOT NULL)
                )
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS rca_source_authority_receipts (
                authority_sha256 TEXT PRIMARY KEY
                    CHECK(length(authority_sha256) = 64
                          AND authority_sha256 NOT GLOB '*[^0-9a-f]*'),
                schema_version TEXT NOT NULL
                    CHECK(schema_version = 'pnc_rca_source_authority_receipt_v1'),
                source_id TEXT NOT NULL CHECK(length(trim(source_id)) > 0),
                source_kind TEXT NOT NULL CHECK(source_kind IN (
                    'kafka_workflow_event', 'feishu_group_manual'
                )),
                payload_sha256 TEXT NOT NULL
                    CHECK(length(payload_sha256) = 64
                          AND payload_sha256 NOT GLOB '*[^0-9a-f]*'
                          AND payload_sha256 != printf('%064d', 0)),
                authorization_evidence_sha256 TEXT NOT NULL
                    CHECK(length(authorization_evidence_sha256) = 64
                          AND authorization_evidence_sha256 NOT GLOB '*[^0-9a-f]*'
                          AND authorization_evidence_sha256 != printf('%064d', 0)),
                binding_action TEXT NOT NULL
                    CHECK(binding_action IN ('create', 'join')),
                decision TEXT NOT NULL CHECK(decision IN ('admit', 'shadow')),
                source_metadata_sha256 TEXT NOT NULL
                    CHECK(length(source_metadata_sha256) = 64
                          AND source_metadata_sha256 NOT GLOB '*[^0-9a-f]*'),
                anchor_sha256 TEXT NOT NULL
                    CHECK(length(anchor_sha256) = 64
                          AND anchor_sha256 NOT GLOB '*[^0-9a-f]*'),
                ingress_decision_sha256 TEXT NOT NULL
                    CHECK(length(ingress_decision_sha256) = 64
                          AND ingress_decision_sha256 NOT GLOB '*[^0-9a-f]*'),
                source_metadata_json TEXT NOT NULL
                    CHECK(json_valid(source_metadata_json)
                          AND json_type(source_metadata_json) = 'object'),
                anchor_json TEXT NOT NULL
                    CHECK(json_valid(anchor_json) AND json_type(anchor_json) = 'object'),
                ingress_decision_json TEXT NOT NULL
                    CHECK(json_valid(ingress_decision_json)
                          AND json_type(ingress_decision_json) = 'object'),
                authority_receipt_json TEXT NOT NULL
                    CHECK(json_valid(authority_receipt_json)
                          AND json_type(authority_receipt_json) = 'object'),
                persisted_at TEXT NOT NULL CHECK(length(trim(persisted_at)) > 0),
                FOREIGN KEY(source_id) REFERENCES rca_trigger_sources(source_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS rca_admission_snapshots (
                snapshot_sha256 TEXT PRIMARY KEY
                    CHECK(length(snapshot_sha256) = 64
                          AND snapshot_sha256 NOT GLOB '*[^0-9a-f]*'),
                snapshot_id TEXT NOT NULL UNIQUE
                    CHECK(snapshot_id = 'pnc-rca-snapshot-v1-' || snapshot_sha256),
                schema_version TEXT NOT NULL
                    CHECK(schema_version = 'pnc_rca_admission_snapshot_v1'),
                request_sha256 TEXT NOT NULL
                    CHECK(length(request_sha256) = 64
                          AND request_sha256 NOT GLOB '*[^0-9a-f]*'),
                business_key TEXT NOT NULL CHECK(length(trim(business_key)) > 0),
                submission_key TEXT NOT NULL CHECK(length(trim(submission_key)) > 0),
                generation INTEGER NOT NULL CHECK(generation >= 1),
                activation_epoch_id TEXT NOT NULL DEFAULT '' CHECK(
                    activation_epoch_id = trim(activation_epoch_id)
                ),
                activation_ledger_id INTEGER CHECK(activation_ledger_id >= 1),
                execution_decision TEXT NOT NULL
                    CHECK(execution_decision IN ('admit', 'shadow')),
                execution_reason TEXT NOT NULL
                    CHECK(length(trim(execution_reason)) > 0),
                execution_state TEXT NOT NULL CHECK(execution_state IN (
                    'legacy_unconfigured', 'unconfigured', 'safe_off',
                    'preauthorized', 'bounded_active', 'confirmed',
                    'steady_active', 'aborted'
                )),
                legacy_unconfigured INTEGER NOT NULL
                    CHECK(legacy_unconfigured IN (0, 1)),
                creator_source_envelope_sha256 TEXT NOT NULL
                    CHECK(length(creator_source_envelope_sha256) = 64
                          AND creator_source_envelope_sha256 NOT GLOB '*[^0-9a-f]*'),
                creator_authority_sha256 TEXT NOT NULL
                    CHECK(length(creator_authority_sha256) = 64
                          AND creator_authority_sha256 NOT GLOB '*[^0-9a-f]*'),
                creator_source_id TEXT NOT NULL
                    CHECK(length(trim(creator_source_id)) > 0),
                admission_snapshot_json TEXT NOT NULL
                    CHECK(json_valid(admission_snapshot_json)
                          AND json_type(admission_snapshot_json) = 'object'),
                persisted_at TEXT NOT NULL CHECK(length(trim(persisted_at)) > 0),
                FOREIGN KEY(request_sha256)
                    REFERENCES rca_canonical_requests(request_sha256),
                FOREIGN KEY(activation_ledger_id)
                    REFERENCES rca_activation_admission_ledger(ledger_id),
                FOREIGN KEY(
                    creator_source_envelope_sha256,
                    creator_authority_sha256,
                    creator_source_id
                ) REFERENCES rca_snapshot_source_envelopes(
                    source_envelope_sha256,
                    source_authority_sha256,
                    source_id
                ) DEFERRABLE INITIALLY DEFERRED,
                CHECK(
                    (
                        execution_state = 'legacy_unconfigured'
                        AND legacy_unconfigured = 1
                        AND activation_epoch_id = ''
                        AND activation_ledger_id IS NULL
                        AND execution_decision = 'admit'
                        AND execution_reason = 'activation_legacy_unconfigured'
                    ) OR (
                        execution_state = 'unconfigured'
                        AND legacy_unconfigured = 0
                        AND activation_epoch_id = ''
                        AND activation_ledger_id IS NULL
                        AND execution_decision = 'shadow'
                        AND execution_reason = 'activation_epoch_held_unconfigured'
                    ) OR (
                        execution_state = 'safe_off'
                        AND legacy_unconfigured = 0
                        AND activation_epoch_id != ''
                        AND activation_ledger_id IS NOT NULL
                        AND execution_decision = 'shadow'
                        AND execution_reason IN (
                            'activation_epoch_held_safe_off',
                            'activation_epoch_held_ingress_safe_off'
                        )
                    ) OR (
                        execution_state = 'preauthorized'
                        AND legacy_unconfigured = 0
                        AND activation_epoch_id != ''
                        AND activation_ledger_id IS NOT NULL
                        AND execution_decision = 'shadow'
                        AND execution_reason IN (
                            'activation_epoch_held_preauthorized',
                            'activation_epoch_held_ingress_preauthorized'
                        )
                    ) OR (
                        execution_state = 'bounded_active'
                        AND legacy_unconfigured = 0
                        AND activation_epoch_id != ''
                        AND activation_ledger_id IS NOT NULL
                        AND (
                            (
                                execution_decision = 'admit'
                                AND execution_reason IN (
                                    'activation_bounded_slot_consumed',
                                    'activation_admission_idempotent'
                                )
                            ) OR (
                                execution_decision = 'shadow'
                                AND execution_reason IN (
                                    'activation_bounded_slot_required',
                                    'activation_bounded_identity_not_authorized',
                                    'activation_kafka_partition_not_fenced',
                                    'activation_kafka_before_start_fence',
                                    'activation_kafka_at_or_after_end_fence',
                                    'activation_bounded_slot_consumed'
                                )
                            )
                        )
                    ) OR (
                        execution_state = 'confirmed'
                        AND legacy_unconfigured = 0
                        AND activation_epoch_id != ''
                        AND activation_ledger_id IS NOT NULL
                        AND (
                            (
                                execution_decision = 'admit'
                                AND execution_reason IN (
                                    'activation_confirmed_shadow_reconciliation',
                                    'activation_admission_idempotent'
                                )
                            ) OR (
                                execution_decision = 'shadow'
                                AND execution_reason IN (
                                    'activation_epoch_held_confirmed',
                                    'activation_epoch_held_ingress_confirmed'
                                )
                            )
                        )
                    ) OR (
                        execution_state = 'steady_active'
                        AND legacy_unconfigured = 0
                        AND activation_epoch_id != ''
                        AND activation_ledger_id IS NOT NULL
                        AND execution_decision = 'admit'
                        AND execution_reason IN (
                            'activation_steady_active',
                            'activation_admission_idempotent'
                        )
                    ) OR (
                        execution_state = 'aborted'
                        AND legacy_unconfigured = 0
                        AND activation_epoch_id != ''
                        AND activation_ledger_id IS NOT NULL
                        AND execution_decision = 'shadow'
                        AND execution_reason =
                            'activation_epoch_held_ingress_aborted'
                    )
                )
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS rca_snapshot_source_envelopes (
                source_envelope_sha256 TEXT PRIMARY KEY
                    CHECK(length(source_envelope_sha256) = 64
                          AND source_envelope_sha256 NOT GLOB '*[^0-9a-f]*'),
                source_envelope_id TEXT NOT NULL UNIQUE CHECK(
                    source_envelope_id =
                        'pnc-rca-source-envelope-v1-' || source_envelope_sha256
                ),
                schema_version TEXT NOT NULL CHECK(
                    schema_version = 'pnc_rca_snapshot_source_envelope_v1'
                ),
                snapshot_sha256 TEXT NOT NULL
                    CHECK(length(snapshot_sha256) = 64
                          AND snapshot_sha256 NOT GLOB '*[^0-9a-f]*'),
                snapshot_id TEXT NOT NULL CHECK(
                    snapshot_id = 'pnc-rca-snapshot-v1-' || snapshot_sha256
                ),
                submission_key TEXT NOT NULL CHECK(length(trim(submission_key)) > 0),
                source_authority_sha256 TEXT NOT NULL
                    CHECK(length(source_authority_sha256) = 64
                          AND source_authority_sha256 NOT GLOB '*[^0-9a-f]*'),
                source_id TEXT NOT NULL CHECK(length(trim(source_id)) > 0),
                source_kind TEXT NOT NULL CHECK(source_kind IN (
                    'kafka_workflow_event', 'feishu_group_manual'
                )),
                payload_sha256 TEXT NOT NULL
                    CHECK(length(payload_sha256) = 64
                          AND payload_sha256 NOT GLOB '*[^0-9a-f]*'
                          AND payload_sha256 != printf('%064d', 0)),
                authorization_evidence_sha256 TEXT NOT NULL
                    CHECK(length(authorization_evidence_sha256) = 64
                          AND authorization_evidence_sha256 NOT GLOB '*[^0-9a-f]*'
                          AND authorization_evidence_sha256 != printf('%064d', 0)),
                binding_action TEXT NOT NULL
                    CHECK(binding_action IN ('create', 'join')),
                decision TEXT NOT NULL CHECK(decision IN ('admit', 'shadow')),
                source_metadata_json TEXT NOT NULL
                    CHECK(json_valid(source_metadata_json)
                          AND json_type(source_metadata_json) = 'object'),
                anchor_json TEXT NOT NULL
                    CHECK(json_valid(anchor_json) AND json_type(anchor_json) = 'object'),
                ingress_decision_json TEXT NOT NULL
                    CHECK(json_valid(ingress_decision_json)
                          AND json_type(ingress_decision_json) = 'object'),
                source_envelope_json TEXT NOT NULL
                    CHECK(json_valid(source_envelope_json)
                          AND json_type(source_envelope_json) = 'object'),
                persisted_at TEXT NOT NULL CHECK(length(trim(persisted_at)) > 0),
                FOREIGN KEY(snapshot_sha256)
                    REFERENCES rca_admission_snapshots(snapshot_sha256),
                FOREIGN KEY(
                    source_authority_sha256, source_id, source_kind, payload_sha256
                ) REFERENCES rca_source_authority_receipts(
                    authority_sha256, source_id, source_kind, payload_sha256
                ),
                FOREIGN KEY(source_id) REFERENCES rca_trigger_sources(source_id)
            )
            """,
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_rca_snapshot_submission
                ON rca_admission_snapshots(submission_key)
            """,
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_rca_snapshot_business_generation
                ON rca_admission_snapshots(business_key, generation)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_rca_snapshot_request
                ON rca_admission_snapshots(request_sha256, snapshot_sha256)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_rca_snapshot_creator
                ON rca_admission_snapshots(
                    creator_source_envelope_sha256,
                    creator_authority_sha256,
                    creator_source_id
                )
            """,
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_rca_source_authority_source
                ON rca_source_authority_receipts(source_id)
            """,
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_rca_source_authority_reference
                ON rca_source_authority_receipts(
                    authority_sha256, source_id, source_kind, payload_sha256
                )
            """,
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_rca_snapshot_envelope_source
                ON rca_snapshot_source_envelopes(source_id)
            """,
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_rca_snapshot_envelope_creator_reference
                ON rca_snapshot_source_envelopes(
                    source_envelope_sha256, source_authority_sha256, source_id
                )
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_rca_snapshot_envelope_snapshot
                ON rca_snapshot_source_envelopes(
                    snapshot_sha256, binding_action, source_envelope_sha256
                )
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_rca_snapshot_envelope_authority
                ON rca_snapshot_source_envelopes(
                    source_authority_sha256, source_id, source_kind, payload_sha256
                )
            """,
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_rca_one_create_envelope_per_snapshot
                ON rca_snapshot_source_envelopes(snapshot_sha256)
                WHERE binding_action = 'create'
            """,
            """
            CREATE TRIGGER IF NOT EXISTS trg_rca_canonical_request_no_update
            BEFORE UPDATE ON rca_canonical_requests
            BEGIN
                SELECT RAISE(ABORT, 'rca_canonical_request_update_forbidden');
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS trg_rca_canonical_request_no_delete
            BEFORE DELETE ON rca_canonical_requests
            BEGIN
                SELECT RAISE(ABORT, 'rca_canonical_request_delete_forbidden');
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS trg_rca_canonical_request_no_replace
            BEFORE INSERT ON rca_canonical_requests
            WHEN EXISTS (
                SELECT 1 FROM rca_canonical_requests
                 WHERE request_sha256 = NEW.request_sha256
            )
            BEGIN
                SELECT RAISE(ABORT, 'rca_canonical_request_replace_forbidden');
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS trg_rca_canonical_request_projection_guard
            BEFORE INSERT ON rca_canonical_requests
            WHEN NOT COALESCE((
                json_extract(NEW.canonical_request_json, '$.schema_version') =
                    NEW.schema_version
                AND json_extract(
                    NEW.canonical_request_json, '$.ticket.title_sha256'
                ) = NEW.ticket_title_sha256
                AND json_extract(
                    NEW.canonical_request_json, '$.creation_policy.sha256'
                ) = NEW.creation_policy_sha256
                AND json_extract(
                    NEW.canonical_request_json, '$.business_profile.sha256'
                ) = NEW.business_profile_sha256
                AND json_extract(
                    NEW.canonical_request_json, '$.execution_policy.sha256'
                ) = NEW.execution_policy_sha256
                AND json_extract(
                    NEW.canonical_request_json, '$.publication_policy.sha256'
                ) = NEW.publication_policy_sha256
                AND json_extract(
                    NEW.canonical_request_json,
                    '$.correction_lineage_policy.sha256'
                ) = NEW.correction_lineage_policy_sha256
                AND json_extract(
                    NEW.canonical_request_json,
                    '$.execution_intent.generation_reason'
                ) = NEW.generation_reason
                AND json_extract(
                    NEW.canonical_request_json,
                    '$.execution_intent.generation_authorization_evidence_sha256'
                ) IS NEW.generation_authorization_evidence_sha256
            ), 0)
            BEGIN
                SELECT RAISE(ABORT, 'rca_canonical_request_projection_mismatch');
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS trg_rca_source_authority_no_update
            BEFORE UPDATE ON rca_source_authority_receipts
            BEGIN
                SELECT RAISE(ABORT, 'rca_source_authority_update_forbidden');
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS trg_rca_source_authority_no_delete
            BEFORE DELETE ON rca_source_authority_receipts
            BEGIN
                SELECT RAISE(ABORT, 'rca_source_authority_delete_forbidden');
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS trg_rca_source_authority_no_replace
            BEFORE INSERT ON rca_source_authority_receipts
            WHEN EXISTS (
                SELECT 1 FROM rca_source_authority_receipts
                 WHERE authority_sha256 = NEW.authority_sha256
                    OR source_id = NEW.source_id
            )
            BEGIN
                SELECT RAISE(ABORT, 'rca_source_authority_replace_forbidden');
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS trg_rca_source_authority_source_guard
            BEFORE INSERT ON rca_source_authority_receipts
            WHEN NOT EXISTS (
                SELECT 1
                  FROM rca_trigger_sources AS source
             LEFT JOIN kafka_inbox AS inbox
                    ON inbox.event_uid = json_extract(
                        NEW.source_metadata_json, '$.event_uid'
                    )
                 WHERE source.source_id = NEW.source_id
                   AND source.source_kind = NEW.source_kind
                   AND source.payload_sha256 = NEW.payload_sha256
                   AND source.created_at = json_extract(
                       NEW.source_metadata_json, '$.observed_at'
                   )
                   AND (
                        (
                            NEW.source_kind = 'feishu_group_manual'
                            AND source.platform = json_extract(
                                NEW.source_metadata_json, '$.platform'
                            )
                            AND source.chat_id = json_extract(
                                NEW.source_metadata_json, '$.chat_id'
                            )
                            AND source.thread_id = json_extract(
                                NEW.source_metadata_json, '$.thread_id'
                            )
                            AND source.message_id = json_extract(
                                NEW.source_metadata_json, '$.message_id'
                            )
                            AND source.requester_id = json_extract(
                                NEW.source_metadata_json, '$.requester_id'
                            )
                            AND source.mode = json_extract(
                                NEW.source_metadata_json, '$.mode'
                            )
                        ) OR (
                            NEW.source_kind = 'kafka_workflow_event'
                            AND inbox.topic = json_extract(
                                NEW.source_metadata_json, '$.topic'
                            )
                            AND inbox.partition_id = json_extract(
                                NEW.source_metadata_json, '$.partition'
                            )
                            AND inbox.offset_id = json_extract(
                                NEW.source_metadata_json, '$.offset'
                            )
                            AND inbox.raw_sha256 = NEW.payload_sha256
                            AND (
                                source.kafka_event_uid IS NULL
                                OR source.kafka_event_uid = inbox.event_uid
                            )
                            AND (
                                source.source_dedupe_key = inbox.event_uid
                                OR substr(
                                    source.source_dedupe_key,
                                    1,
                                    length(inbox.event_uid) + 12
                                ) = inbox.event_uid || ':generation:'
                            )
                        )
                   )
            )
            BEGIN
                SELECT RAISE(ABORT, 'rca_source_authority_source_mismatch');
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS trg_rca_source_authority_projection_guard
            BEFORE INSERT ON rca_source_authority_receipts
            WHEN NOT COALESCE((
                json_extract(NEW.authority_receipt_json, '$.schema_version') =
                    NEW.schema_version
                AND json_extract(
                    NEW.authority_receipt_json, '$.authority_sha256'
                ) = NEW.authority_sha256
                AND json_extract(NEW.authority_receipt_json, '$.source_id') =
                    NEW.source_id
                AND json_extract(NEW.authority_receipt_json, '$.source_kind') =
                    NEW.source_kind
                AND json_extract(
                    NEW.authority_receipt_json, '$.source_metadata_sha256'
                ) = NEW.source_metadata_sha256
                AND json_extract(
                    NEW.authority_receipt_json, '$.anchor_sha256'
                ) = NEW.anchor_sha256
                AND json_extract(
                    NEW.authority_receipt_json, '$.ingress_decision_sha256'
                ) = NEW.ingress_decision_sha256
                AND json_extract(NEW.source_metadata_json, '$.source_kind') =
                    NEW.source_kind
                AND json_extract(NEW.source_metadata_json, '$.payload_sha256') =
                    NEW.payload_sha256
                AND json_extract(
                    NEW.ingress_decision_json,
                    '$.authorization_evidence_sha256'
                ) = NEW.authorization_evidence_sha256
                AND json_extract(
                    NEW.ingress_decision_json, '$.binding_action'
                ) = NEW.binding_action
                AND json_extract(NEW.ingress_decision_json, '$.decision') =
                    NEW.decision
            ), 0)
            BEGIN
                SELECT RAISE(ABORT, 'rca_source_authority_projection_mismatch');
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS trg_rca_admission_snapshot_no_update
            BEFORE UPDATE ON rca_admission_snapshots
            BEGIN
                SELECT RAISE(ABORT, 'rca_admission_snapshot_update_forbidden');
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS trg_rca_admission_snapshot_no_delete
            BEFORE DELETE ON rca_admission_snapshots
            BEGIN
                SELECT RAISE(ABORT, 'rca_admission_snapshot_delete_forbidden');
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS trg_rca_admission_snapshot_no_replace
            BEFORE INSERT ON rca_admission_snapshots
            WHEN EXISTS (
                SELECT 1 FROM rca_admission_snapshots
                 WHERE snapshot_sha256 = NEW.snapshot_sha256
                    OR snapshot_id = NEW.snapshot_id
                    OR submission_key = NEW.submission_key
            )
            BEGIN
                SELECT RAISE(ABORT, 'rca_admission_snapshot_replace_forbidden');
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS trg_rca_admission_snapshot_execution_guard
            BEFORE INSERT ON rca_admission_snapshots
            WHEN (
                NEW.activation_ledger_id IS NOT NULL AND NOT EXISTS (
                    SELECT 1
                      FROM rca_activation_admission_ledger AS ledger
                      JOIN rca_activation_epochs AS epoch
                        ON epoch.epoch_id = ledger.epoch_id
                     WHERE ledger.ledger_id = NEW.activation_ledger_id
                       AND ledger.epoch_id = NEW.activation_epoch_id
                       AND ledger.business_key = NEW.business_key
                       AND ledger.submission_key = NEW.submission_key
                       AND ledger.generation = NEW.generation
                       AND ledger.decision = NEW.execution_decision
                       AND epoch.state = NEW.execution_state
                       AND epoch.is_current = 1
                       AND (
                            ledger.reason = NEW.execution_reason
                            OR (
                                NEW.execution_reason =
                                    'activation_admission_idempotent'
                                AND NEW.execution_decision = 'admit'
                            )
                       )
                )
            ) OR (
                NEW.activation_ledger_id IS NULL AND EXISTS (
                    SELECT 1 FROM rca_activation_epochs WHERE is_current = 1
                )
            )
            BEGIN
                SELECT RAISE(ABORT, 'rca_admission_snapshot_execution_mismatch');
            END
            """,
            """
            DROP TRIGGER IF EXISTS trg_rca_admission_snapshot_projection_guard
            """,
            """
            CREATE TRIGGER IF NOT EXISTS trg_rca_admission_snapshot_projection_guard
            BEFORE INSERT ON rca_admission_snapshots
            WHEN NOT COALESCE((
                json_extract(NEW.admission_snapshot_json, '$.schema_version') =
                    NEW.schema_version
                AND json_extract(
                    NEW.admission_snapshot_json, '$.snapshot_id'
                ) = NEW.snapshot_id
                AND json_extract(
                    NEW.admission_snapshot_json, '$.snapshot_sha256'
                ) = NEW.snapshot_sha256
                AND json_extract(
                    NEW.admission_snapshot_json, '$.request_sha256'
                ) = NEW.request_sha256
                AND json_extract(
                    NEW.admission_snapshot_json,
                    '$.resolved_admission.business_key'
                ) = NEW.business_key
                AND json_extract(
                    NEW.admission_snapshot_json,
                    '$.resolved_admission.submission_key'
                ) = NEW.submission_key
                AND json_extract(
                    NEW.admission_snapshot_json,
                    '$.resolved_admission.generation'
                ) = NEW.generation
                AND json_extract(
                    NEW.admission_snapshot_json,
                    '$.execution_admission.activation_epoch_id'
                ) = NEW.activation_epoch_id
                AND json_extract(
                    NEW.admission_snapshot_json,
                    '$.execution_admission.activation_ledger_id'
                ) IS NEW.activation_ledger_id
                AND json_extract(
                    NEW.admission_snapshot_json,
                    '$.execution_admission.decision'
                ) = NEW.execution_decision
                AND json_extract(
                    NEW.admission_snapshot_json,
                    '$.execution_admission.reason'
                ) = NEW.execution_reason
                AND json_extract(
                    NEW.admission_snapshot_json,
                    '$.execution_admission.state'
                ) = NEW.execution_state
                AND json_extract(
                    NEW.admission_snapshot_json,
                    '$.execution_admission.legacy_unconfigured'
                ) = NEW.legacy_unconfigured
                AND json_extract(
                    NEW.admission_snapshot_json, '$.write_fence.schema_version'
                ) IN ('pnc_rca_write_fence_slot_v1', 'pnc_rca_write_fence_v1')
                AND json_extract(
                    NEW.admission_snapshot_json, '$.write_fence.state'
                ) IN ('unissued', 'issued')
            ), 0)
            BEGIN
                SELECT RAISE(ABORT, 'rca_admission_snapshot_projection_mismatch');
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS trg_rca_snapshot_envelope_no_update
            BEFORE UPDATE ON rca_snapshot_source_envelopes
            BEGIN
                SELECT RAISE(ABORT, 'rca_snapshot_envelope_update_forbidden');
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS trg_rca_snapshot_envelope_no_delete
            BEFORE DELETE ON rca_snapshot_source_envelopes
            BEGIN
                SELECT RAISE(ABORT, 'rca_snapshot_envelope_delete_forbidden');
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS trg_rca_snapshot_envelope_no_replace
            BEFORE INSERT ON rca_snapshot_source_envelopes
            WHEN EXISTS (
                SELECT 1 FROM rca_snapshot_source_envelopes
                 WHERE source_envelope_sha256 = NEW.source_envelope_sha256
                    OR source_envelope_id = NEW.source_envelope_id
                    OR source_id = NEW.source_id
                    OR (
                        NEW.binding_action = 'create'
                        AND snapshot_sha256 = NEW.snapshot_sha256
                        AND binding_action = 'create'
                    )
            )
            BEGIN
                SELECT RAISE(ABORT, 'rca_snapshot_envelope_replace_forbidden');
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS trg_rca_snapshot_envelope_authority_guard
            BEFORE INSERT ON rca_snapshot_source_envelopes
            WHEN NOT EXISTS (
                SELECT 1 FROM rca_source_authority_receipts AS authority
                 WHERE authority.authority_sha256 = NEW.source_authority_sha256
                   AND authority.source_id = NEW.source_id
                   AND authority.source_kind = NEW.source_kind
                   AND authority.payload_sha256 = NEW.payload_sha256
                   AND authority.authorization_evidence_sha256 =
                       NEW.authorization_evidence_sha256
                   AND authority.binding_action = NEW.binding_action
                   AND authority.decision = NEW.decision
            )
            BEGIN
                SELECT RAISE(ABORT, 'rca_snapshot_envelope_authority_mismatch');
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS trg_rca_snapshot_envelope_projection_guard
            BEFORE INSERT ON rca_snapshot_source_envelopes
            WHEN NOT COALESCE((
                json_extract(NEW.source_envelope_json, '$.schema_version') =
                    NEW.schema_version
                AND json_extract(
                    NEW.source_envelope_json, '$.source_envelope_id'
                ) = NEW.source_envelope_id
                AND json_extract(
                    NEW.source_envelope_json, '$.source_envelope_sha256'
                ) = NEW.source_envelope_sha256
                AND json_extract(
                    NEW.source_envelope_json, '$.source_authority_sha256'
                ) = NEW.source_authority_sha256
                AND json_extract(NEW.source_envelope_json, '$.snapshot_id') =
                    NEW.snapshot_id
                AND json_extract(
                    NEW.source_envelope_json, '$.snapshot_sha256'
                ) = NEW.snapshot_sha256
                AND json_extract(
                    NEW.source_envelope_json, '$.submission_key'
                ) = NEW.submission_key
                AND json_extract(NEW.source_envelope_json, '$.source_id') =
                    NEW.source_id
                AND json_extract(NEW.source_envelope_json, '$.source_kind') =
                    NEW.source_kind
                AND json_extract(
                    NEW.source_envelope_json,
                    '$.ingress_decision.authorization_evidence_sha256'
                ) = NEW.authorization_evidence_sha256
                AND json_extract(
                    NEW.source_envelope_json,
                    '$.ingress_decision.binding_action'
                ) = NEW.binding_action
                AND json_extract(
                    NEW.source_envelope_json, '$.ingress_decision.decision'
                ) = NEW.decision
                AND json(json_extract(
                    NEW.source_envelope_json, '$.source_metadata'
                )) = json(NEW.source_metadata_json)
                AND json(json_extract(
                    NEW.source_envelope_json, '$.anchor'
                )) = json(NEW.anchor_json)
                AND json(json_extract(
                    NEW.source_envelope_json, '$.ingress_decision'
                )) = json(NEW.ingress_decision_json)
            ), 0)
            BEGIN
                SELECT RAISE(ABORT, 'rca_snapshot_envelope_projection_mismatch');
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS trg_rca_snapshot_envelope_source_binding_guard
            BEFORE INSERT ON rca_snapshot_source_envelopes
            WHEN NOT EXISTS (
                SELECT 1
                  FROM rca_admission_snapshots AS snapshot
                  JOIN rca_trigger_bindings AS binding
                    ON binding.source_id = NEW.source_id
                   AND binding.business_key = snapshot.business_key
                   AND binding.generation = snapshot.generation
                  JOIN business_triggers AS trigger
                    ON trigger.business_key = binding.business_key
                   AND trigger.generation = binding.generation
                  JOIN rca_trigger_sources AS source
                    ON source.source_id = binding.source_id
                 WHERE snapshot.snapshot_sha256 = NEW.snapshot_sha256
                   AND snapshot.snapshot_id = NEW.snapshot_id
                   AND snapshot.submission_key = NEW.submission_key
                   AND trigger.submission_key = snapshot.submission_key
                   AND binding.role = CASE NEW.binding_action
                       WHEN 'create' THEN 'origin' ELSE 'observer' END
                   AND (
                        NEW.source_kind != 'kafka_workflow_event'
                        OR source.mode = CASE
                            WHEN snapshot.generation = 1
                                THEN 'issue_created'
                            ELSE 'kafka_retrigger'
                        END
                   )
            )
            BEGIN
                SELECT RAISE(ABORT, 'rca_snapshot_source_binding_mismatch');
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS trg_rca_snapshot_envelope_create_guard
            BEFORE INSERT ON rca_snapshot_source_envelopes
            WHEN NEW.binding_action = 'create' AND NOT EXISTS (
                SELECT 1
                  FROM rca_admission_snapshots AS snapshot
                  JOIN rca_canonical_requests AS request
                    ON request.request_sha256 = snapshot.request_sha256
                  JOIN rca_source_authority_receipts AS authority
                    ON authority.authority_sha256 = NEW.source_authority_sha256
                 WHERE snapshot.snapshot_sha256 = NEW.snapshot_sha256
                   AND snapshot.snapshot_id = NEW.snapshot_id
                   AND snapshot.submission_key = NEW.submission_key
                   AND snapshot.execution_decision = NEW.decision
                   AND snapshot.creator_source_envelope_sha256 =
                       NEW.source_envelope_sha256
                   AND snapshot.creator_authority_sha256 =
                       NEW.source_authority_sha256
                   AND snapshot.creator_source_id = NEW.source_id
                   AND (
                        (
                            snapshot.generation = 1
                            AND request.generation_reason = 'initial'
                            AND request.generation_authorization_evidence_sha256
                                IS NULL
                        )
                        OR (
                            snapshot.generation > 1
                            AND request.generation_reason =
                                'explicit_user_rerun'
                            AND request.generation_authorization_evidence_sha256
                                IS NOT NULL
                            AND authority.binding_action = 'create'
                            AND authority.source_kind = 'feishu_group_manual'
                            AND authority.authorization_evidence_sha256 =
                                request.generation_authorization_evidence_sha256
                            AND json_extract(
                                authority.source_metadata_json, '$.platform'
                            ) = 'feishu'
                            AND json_extract(
                                authority.source_metadata_json, '$.mode'
                            ) = 'rerun'
                            AND substr(
                                json_extract(
                                    authority.source_metadata_json, '$.requester_id'
                                ),
                                1,
                                3
                            ) = 'ou_'
                            AND length(
                                json_extract(
                                    authority.source_metadata_json, '$.requester_id'
                                )
                            ) > 3
                        )
                   )
            )
            BEGIN
                SELECT RAISE(ABORT, 'rca_snapshot_creator_binding_mismatch');
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS trg_rca_snapshot_envelope_join_guard
            BEFORE INSERT ON rca_snapshot_source_envelopes
            WHEN NEW.binding_action = 'join' AND NOT EXISTS (
                SELECT 1
                  FROM rca_admission_snapshots AS snapshot
                  JOIN rca_snapshot_source_envelopes AS creator
                    ON creator.source_envelope_sha256 =
                       snapshot.creator_source_envelope_sha256
                   AND creator.source_authority_sha256 =
                       snapshot.creator_authority_sha256
                   AND creator.source_id = snapshot.creator_source_id
                 WHERE snapshot.snapshot_sha256 = NEW.snapshot_sha256
                   AND snapshot.snapshot_id = NEW.snapshot_id
                   AND snapshot.submission_key = NEW.submission_key
                   AND snapshot.execution_decision = NEW.decision
                   AND creator.snapshot_sha256 = snapshot.snapshot_sha256
                   AND creator.binding_action = 'create'
            )
            BEGIN
                SELECT RAISE(ABORT, 'rca_snapshot_join_before_creator_forbidden');
            END
            """,
        )
        return statements

    @classmethod
    def _create_v11_snapshot_schema(cls, conn: sqlite3.Connection) -> None:
        """Create only the inert, immutable W3 authority and snapshot relations."""
        statements = cls._v11_snapshot_schema_statements()
        for statement in statements:
            conn.execute(statement)

    @staticmethod
    def _create_v12_learning_lane_schema(conn: sqlite3.Connection) -> None:
        """Create the sealed stock cohort and append-only learning admissions."""
        script = f"""
            CREATE TABLE IF NOT EXISTS rca_learning_lane_cohorts (
                cohort_id TEXT PRIMARY KEY,
                schema_version TEXT NOT NULL CHECK(
                    schema_version = '{LEARNING_LANE_COHORT_SCHEMA_VERSION}'
                ),
                stock_cutoff TEXT NOT NULL CHECK(
                    stock_cutoff = '{STOCK_CUTOFF}'
                ),
                stock_count INTEGER NOT NULL CHECK(stock_count >= 0),
                stock_ids_sha256 TEXT NOT NULL CHECK(
                    length(stock_ids_sha256) = 64
                    AND stock_ids_sha256 NOT GLOB '*[^0-9a-f]*'
                ),
                sealed_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS rca_learning_lane_stock_items (
                cohort_id TEXT NOT NULL,
                work_item_id TEXT NOT NULL,
                PRIMARY KEY(cohort_id, work_item_id),
                FOREIGN KEY(cohort_id)
                    REFERENCES rca_learning_lane_cohorts(cohort_id)
            );

            CREATE TABLE IF NOT EXISTS rca_learning_lane_admissions (
                business_key TEXT NOT NULL,
                generation INTEGER NOT NULL CHECK(generation >= 1),
                work_item_id TEXT NOT NULL,
                schema_version TEXT NOT NULL CHECK(
                    schema_version = '{LEARNING_LANE_ADMISSION_SCHEMA_VERSION}'
                ),
                lane TEXT NOT NULL CHECK(lane = 'learning'),
                reason TEXT NOT NULL CHECK(reason IN ('stock', 'legacy')),
                external_write_allowed INTEGER NOT NULL CHECK(
                    external_write_allowed = 0
                ),
                cohort_id TEXT NOT NULL,
                stock_cutoff TEXT NOT NULL CHECK(
                    stock_cutoff = '{STOCK_CUTOFF}'
                ),
                stock_ids_sha256 TEXT NOT NULL CHECK(
                    length(stock_ids_sha256) = 64
                    AND stock_ids_sha256 NOT GLOB '*[^0-9a-f]*'
                ),
                admitted_at TEXT NOT NULL,
                PRIMARY KEY(business_key, generation),
                FOREIGN KEY(business_key, generation)
                    REFERENCES business_triggers(business_key, generation),
                FOREIGN KEY(cohort_id, work_item_id)
                    REFERENCES rca_learning_lane_stock_items(cohort_id, work_item_id)
            );

            CREATE INDEX IF NOT EXISTS idx_learning_lane_admission_item
                ON rca_learning_lane_admissions(work_item_id, admitted_at);

            CREATE TRIGGER IF NOT EXISTS trg_learning_lane_stock_item_no_append
            BEFORE INSERT ON rca_learning_lane_stock_items
            WHEN (
                SELECT COUNT(*) FROM rca_learning_lane_stock_items
                 WHERE cohort_id = NEW.cohort_id
            ) >= (
                SELECT stock_count FROM rca_learning_lane_cohorts
                 WHERE cohort_id = NEW.cohort_id
            )
            BEGIN
                SELECT RAISE(ABORT, 'learning_lane_cohort_immutable');
            END;

            CREATE TRIGGER IF NOT EXISTS trg_learning_lane_cohort_no_update
            BEFORE UPDATE ON rca_learning_lane_cohorts
            BEGIN
                SELECT RAISE(ABORT, 'learning_lane_cohort_immutable');
            END;

            CREATE TRIGGER IF NOT EXISTS trg_learning_lane_cohort_no_delete
            BEFORE DELETE ON rca_learning_lane_cohorts
            BEGIN
                SELECT RAISE(ABORT, 'learning_lane_cohort_immutable');
            END;

            CREATE TRIGGER IF NOT EXISTS trg_learning_lane_cohort_no_replace
            BEFORE INSERT ON rca_learning_lane_cohorts
            WHEN EXISTS (SELECT 1 FROM rca_learning_lane_cohorts)
            BEGIN
                SELECT RAISE(ABORT, 'learning_lane_cohort_replace_forbidden');
            END;

            CREATE TRIGGER IF NOT EXISTS trg_learning_lane_stock_item_no_update
            BEFORE UPDATE ON rca_learning_lane_stock_items
            BEGIN
                SELECT RAISE(ABORT, 'learning_lane_cohort_immutable');
            END;

            CREATE TRIGGER IF NOT EXISTS trg_learning_lane_stock_item_no_delete
            BEFORE DELETE ON rca_learning_lane_stock_items
            BEGIN
                SELECT RAISE(ABORT, 'learning_lane_cohort_immutable');
            END;

            CREATE TRIGGER IF NOT EXISTS trg_learning_lane_admission_no_update
            BEFORE UPDATE ON rca_learning_lane_admissions
            BEGIN
                SELECT RAISE(ABORT, 'learning_lane_admission_immutable');
            END;

            CREATE TRIGGER IF NOT EXISTS trg_learning_lane_admission_no_delete
            BEFORE DELETE ON rca_learning_lane_admissions
            BEGIN
                SELECT RAISE(ABORT, 'learning_lane_admission_immutable');
            END;

            CREATE TRIGGER IF NOT EXISTS trg_learning_lane_admission_cohort_binding
            BEFORE INSERT ON rca_learning_lane_admissions
            WHEN NOT EXISTS (
                SELECT 1
                  FROM rca_learning_lane_cohorts AS cohort
                  JOIN rca_learning_lane_stock_items AS item
                    ON item.cohort_id = cohort.cohort_id
                   AND item.work_item_id = NEW.work_item_id
                 WHERE cohort.cohort_id = NEW.cohort_id
                   AND cohort.stock_cutoff = NEW.stock_cutoff
                   AND cohort.stock_ids_sha256 = NEW.stock_ids_sha256
            )
            BEGIN
                SELECT RAISE(ABORT, 'learning_lane_cohort_binding_mismatch');
            END;
            """
        pending = ""
        for line in script.splitlines(keepends=True):
            pending += line
            if sqlite3.complete_statement(pending):
                conn.execute(pending)
                pending = ""
        if pending.strip():
            conn.execute(pending)

    @staticmethod
    def _validate_v12_learning_lane_schema(conn: sqlite3.Connection) -> None:
        required_columns = {
            "rca_learning_lane_cohorts": {
                "cohort_id", "schema_version", "stock_cutoff", "stock_count",
                "stock_ids_sha256", "sealed_at",
            },
            "rca_learning_lane_stock_items": {"cohort_id", "work_item_id"},
            "rca_learning_lane_admissions": {
                "business_key", "generation", "work_item_id", "schema_version",
                "lane", "reason", "external_write_allowed", "cohort_id",
                "stock_cutoff", "stock_ids_sha256", "admitted_at",
            },
        }
        for table, expected in required_columns.items():
            observed = {
                str(row["name"])
                for row in conn.execute(f"PRAGMA table_info({table})")
            }
            if observed != expected:
                raise RuntimeError(f"incompatible_control_store_schema:{table}_columns")
        present_triggers = {
            str(row["name"])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'trigger'"
            )
        }
        required_triggers = {
            "trg_learning_lane_cohort_no_update",
            "trg_learning_lane_cohort_no_delete",
            "trg_learning_lane_cohort_no_replace",
            "trg_learning_lane_stock_item_no_append",
            "trg_learning_lane_stock_item_no_update",
            "trg_learning_lane_stock_item_no_delete",
            "trg_learning_lane_admission_no_update",
            "trg_learning_lane_admission_no_delete",
            "trg_learning_lane_admission_cohort_binding",
        }
        if not required_triggers.issubset(present_triggers):
            raise RuntimeError("incompatible_control_store_schema:learning_lane_triggers")
        cohorts = conn.execute(
            "SELECT * FROM rca_learning_lane_cohorts ORDER BY cohort_id"
        ).fetchall()
        if len(cohorts) > 1:
            raise RuntimeError("incompatible_control_store_schema:learning_lane_cohort_count")
        for cohort in cohorts:
            if str(cohort["stock_cutoff"]) != STOCK_CUTOFF:
                raise RuntimeError(
                    "incompatible_control_store_schema:learning_lane_cutoff"
                )
            item_ids = [
                str(row["work_item_id"])
                for row in conn.execute(
                    "SELECT work_item_id FROM rca_learning_lane_stock_items "
                    "WHERE cohort_id = ? ORDER BY work_item_id",
                    (cohort["cohort_id"],),
                ).fetchall()
            ]
            if (
                len(item_ids) != int(cohort["stock_count"])
                or RcaControlStore._learning_stock_digest(item_ids)
                != str(cohort["stock_ids_sha256"])
            ):
                raise RuntimeError(
                    "incompatible_control_store_schema:learning_lane_cohort_binding"
                )

    @staticmethod
    def _v14_terminal_rerun_delivery_authority_trigger_names() -> tuple[str, ...]:
        return (
            "trg_terminal_rerun_delivery_authority_no_update",
            "trg_terminal_rerun_delivery_authority_no_delete",
            "trg_terminal_rerun_delivery_authority_no_replace",
            "trg_terminal_rerun_delivery_authority_projection_guard",
            "trg_terminal_rerun_delivery_authority_binding_guard",
        )

    @staticmethod
    def _v14_terminal_rerun_delivery_authority_schema_statements() -> tuple[str, ...]:
        return (
            f"""
            CREATE TABLE IF NOT EXISTS rca_terminal_rerun_delivery_authorities (
                authority_sha256 TEXT PRIMARY KEY CHECK(
                    length(authority_sha256) = 64
                    AND authority_sha256 NOT GLOB '*[^0-9a-f]*'
                ),
                schema_version TEXT NOT NULL CHECK(
                    schema_version =
                        '{TERMINAL_RERUN_DELIVERY_AUTHORITY_SCHEMA_VERSION}'
                ),
                authority_kind TEXT NOT NULL CHECK(
                    authority_kind IN ('silent_terminal', 'batch_terminal')
                ),
                source_id TEXT NOT NULL UNIQUE,
                outbox_id INTEGER NOT NULL UNIQUE,
                source_payload_sha256 TEXT NOT NULL CHECK(
                    length(source_payload_sha256) = 64
                    AND source_payload_sha256 NOT GLOB '*[^0-9a-f]*'
                ),
                business_key TEXT NOT NULL,
                generation INTEGER NOT NULL CHECK(generation >= 2),
                submission_key TEXT NOT NULL UNIQUE,
                activation_epoch_id TEXT NOT NULL,
                activation_ledger_id INTEGER NOT NULL UNIQUE,
                effect_kind TEXT NOT NULL CHECK(
                    effect_kind = 'feishu_issue_comment'
                ),
                project_key TEXT NOT NULL CHECK(length(trim(project_key)) > 0),
                project_simple_name TEXT NOT NULL CHECK(
                    length(trim(project_simple_name)) > 0
                ),
                work_item_type_key TEXT NOT NULL CHECK(
                    length(trim(work_item_type_key)) > 0
                ),
                issue_id TEXT NOT NULL CHECK(
                    length(issue_id) BETWEEN 1 AND 32
                    AND issue_id NOT GLOB '*[^0-9]*'
                ),
                batch_id TEXT NOT NULL CHECK(length(trim(batch_id)) > 0),
                prior_submission_key TEXT NOT NULL,
                prior_generation INTEGER NOT NULL CHECK(prior_generation >= 1),
                prior_delivery_id TEXT NOT NULL,
                queue_sha256 TEXT NOT NULL CHECK(
                    length(queue_sha256) = 64
                    AND queue_sha256 NOT GLOB '*[^0-9a-f]*'
                ),
                owner_receipt_path TEXT NOT NULL CHECK(
                    length(trim(owner_receipt_path)) > 0
                ),
                owner_receipt_sha256 TEXT NOT NULL CHECK(
                    length(owner_receipt_sha256) = 64
                    AND owner_receipt_sha256 NOT GLOB '*[^0-9a-f]*'
                ),
                requester_id TEXT NOT NULL CHECK(
                    requester_id LIKE 'automation:%'
                ),
                reason TEXT NOT NULL CHECK(
                    reason = 'production_gray_batch:' || batch_id
                ),
                activation_required INTEGER NOT NULL CHECK(
                    activation_required = 1
                ),
                authority_json TEXT NOT NULL CHECK(json_valid(authority_json)),
                created_at TEXT NOT NULL CHECK(length(trim(created_at)) > 0),
                UNIQUE(business_key, generation),
                CHECK(generation = prior_generation + 1),
                CHECK(
                    (authority_kind = 'silent_terminal' AND prior_delivery_id = '')
                    OR
                    (authority_kind = 'batch_terminal'
                     AND length(trim(prior_delivery_id)) > 0)
                ),
                FOREIGN KEY(source_id) REFERENCES rca_trigger_sources(source_id),
                FOREIGN KEY(outbox_id) REFERENCES rca_outbox(outbox_id),
                FOREIGN KEY(business_key, generation)
                    REFERENCES business_triggers(business_key, generation),
                FOREIGN KEY(submission_key)
                    REFERENCES business_triggers(submission_key),
                FOREIGN KEY(activation_epoch_id)
                    REFERENCES rca_activation_epochs(epoch_id),
                FOREIGN KEY(activation_ledger_id)
                    REFERENCES rca_activation_admission_ledger(ledger_id),
                FOREIGN KEY(business_key, prior_generation)
                    REFERENCES business_triggers(business_key, generation),
                FOREIGN KEY(prior_submission_key)
                    REFERENCES business_triggers(submission_key)
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_terminal_rerun_delivery_authority_issue
                ON rca_terminal_rerun_delivery_authorities(
                    issue_id, generation, authority_sha256
                )
            """,
            """
            CREATE TRIGGER IF NOT EXISTS
                trg_terminal_rerun_delivery_authority_no_update
            BEFORE UPDATE ON rca_terminal_rerun_delivery_authorities
            BEGIN
                SELECT RAISE(
                    ABORT, 'terminal_rerun_delivery_authority_update_forbidden'
                );
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS
                trg_terminal_rerun_delivery_authority_no_delete
            BEFORE DELETE ON rca_terminal_rerun_delivery_authorities
            BEGIN
                SELECT RAISE(
                    ABORT, 'terminal_rerun_delivery_authority_delete_forbidden'
                );
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS
                trg_terminal_rerun_delivery_authority_no_replace
            BEFORE INSERT ON rca_terminal_rerun_delivery_authorities
            WHEN EXISTS (
                SELECT 1 FROM rca_terminal_rerun_delivery_authorities
                 WHERE authority_sha256 = NEW.authority_sha256
                    OR source_id = NEW.source_id
                    OR submission_key = NEW.submission_key
                    OR (
                        business_key = NEW.business_key
                        AND generation = NEW.generation
                    )
            )
            BEGIN
                SELECT RAISE(
                    ABORT, 'terminal_rerun_delivery_authority_replace_forbidden'
                );
            END
            """,
            f"""
            CREATE TRIGGER IF NOT EXISTS
                trg_terminal_rerun_delivery_authority_projection_guard
            BEFORE INSERT ON rca_terminal_rerun_delivery_authorities
            WHEN NOT COALESCE((
                json_extract(NEW.authority_json, '$.selection_sha256') =
                    NEW.authority_sha256
                AND json_extract(NEW.authority_json, '$.batch_id') = NEW.batch_id
                AND json_extract(NEW.authority_json, '$.queue_sha256') =
                    NEW.queue_sha256
                AND json_extract(NEW.authority_json, '$.issue_id') = NEW.issue_id
                AND json_extract(
                    NEW.authority_json, '$.prior_submission_key'
                ) = NEW.prior_submission_key
                AND json_extract(NEW.authority_json, '$.prior_generation') =
                    NEW.prior_generation
                AND json_extract(NEW.authority_json, '$.owner_receipt_path') =
                    NEW.owner_receipt_path
                AND json_extract(NEW.authority_json, '$.owner_receipt_sha256') =
                    NEW.owner_receipt_sha256
                AND json_extract(NEW.authority_json, '$.activation_required') =
                    NEW.activation_required
                AND json_extract(NEW.authority_json, '$.requester_id') =
                    NEW.requester_id
                AND json_extract(NEW.authority_json, '$.reason') = NEW.reason
                AND (
                    (
                        NEW.authority_kind = 'silent_terminal'
                        AND json_extract(
                            NEW.authority_json, '$.schema_version'
                        ) = '{SILENT_TERMINAL_RERUN_AUTHORITY_SCHEMA_VERSION}'
                        AND NEW.prior_delivery_id = ''
                        AND json_type(
                            NEW.authority_json, '$.prior_delivery_id'
                        ) IS NULL
                        AND json_type(
                            NEW.authority_json, '$.terminal_mode'
                        ) IS NULL
                        AND (
                            SELECT COUNT(*) FROM json_each(NEW.authority_json)
                        ) = 12
                    )
                    OR
                    (
                        NEW.authority_kind = 'batch_terminal'
                        AND json_extract(
                            NEW.authority_json, '$.schema_version'
                        ) = '{BATCH_TERMINAL_RERUN_AUTHORITY_SCHEMA_VERSION}'
                        AND json_extract(
                            NEW.authority_json, '$.prior_delivery_id'
                        ) = NEW.prior_delivery_id
                        AND json_extract(
                            NEW.authority_json, '$.terminal_mode'
                        ) = 'settled_delivery_correction'
                        AND (
                            SELECT COUNT(*) FROM json_each(NEW.authority_json)
                        ) = 14
                    )
                )
            ), 0)
            BEGIN
                SELECT RAISE(
                    ABORT, 'terminal_rerun_delivery_authority_projection_mismatch'
                );
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS
                trg_terminal_rerun_delivery_authority_binding_guard
            BEFORE INSERT ON rca_terminal_rerun_delivery_authorities
            WHEN NOT EXISTS (
                SELECT 1
                  FROM rca_trigger_sources AS source
                  JOIN rca_trigger_bindings AS binding
                    ON binding.source_id = source.source_id
                  JOIN business_triggers AS trigger
                    ON trigger.business_key = binding.business_key
                   AND trigger.generation = binding.generation
                  JOIN rca_outbox AS outbox
                    ON outbox.business_key = trigger.business_key
                   AND outbox.generation = trigger.generation
                   AND outbox.submission_key = trigger.submission_key
                  JOIN business_triggers AS prior
                    ON prior.business_key = trigger.business_key
                   AND prior.generation = NEW.prior_generation
                  JOIN rca_activation_admission_ledger AS ledger
                    ON ledger.ledger_id = NEW.activation_ledger_id
                   AND ledger.epoch_id = NEW.activation_epoch_id
                  JOIN rca_activation_epochs AS epoch
                    ON epoch.epoch_id = ledger.epoch_id
                 WHERE source.source_id = NEW.source_id
                   AND source.source_kind = 'feishu_group_manual'
                   AND source.payload_sha256 = NEW.source_payload_sha256
                   AND source.platform = 'operator'
                   AND source.chat_id = ''
                   AND source.thread_id = ''
                   AND source.requester_id = NEW.requester_id
                   AND source.mode = 'rerun'
                   AND source.outcome = 'created'
                   AND binding.business_key = NEW.business_key
                   AND binding.generation = NEW.generation
                   AND binding.role = 'origin'
                   AND trigger.submission_key = NEW.submission_key
                   AND trigger.origin_source_id = NEW.source_id
                   AND trigger.project_key = NEW.project_key
                   AND trigger.work_item_type_key = NEW.work_item_type_key
                   AND trigger.work_item_id = NEW.issue_id
                   AND json_extract(
                       trigger.normalized_json, '$.project_simple_name'
                   ) = NEW.project_simple_name
                   AND json_extract(trigger.normalized_json, '$.issue_url') =
                       'https://project.feishu.cn/' || NEW.project_simple_name ||
                       '/issue/detail/' || NEW.issue_id
                   AND trigger.activation_epoch_id = NEW.activation_epoch_id
                   AND trigger.activation_ledger_id = NEW.activation_ledger_id
                   AND trigger.activation_epoch_id = outbox.activation_epoch_id
                   AND trigger.activation_ledger_id = outbox.activation_ledger_id
                   AND outbox.origin_source_id = NEW.source_id
                   AND outbox.outbox_id = NEW.outbox_id
                   AND outbox.action = 'submit_rca_issue_intake'
                   AND prior.submission_key = NEW.prior_submission_key
                   AND prior.work_item_id = NEW.issue_id
                   AND ledger.entrypoint = 'manual_admit'
                   AND ledger.source_kind = 'manual'
                   AND ledger.decision = 'admit'
                   AND ledger.business_key = NEW.business_key
                   AND ledger.submission_key = NEW.submission_key
                   AND ledger.generation = NEW.generation
                   AND ledger.bound_at IS NOT NULL
                   AND epoch.is_current = 1
                   AND epoch.state = 'steady_active'
            )
            BEGIN
                SELECT RAISE(
                    ABORT, 'terminal_rerun_delivery_authority_binding_mismatch'
                );
            END
            """,
        )

    @classmethod
    def _create_v14_terminal_rerun_delivery_authority_schema(
        cls,
        conn: sqlite3.Connection,
    ) -> None:
        for statement in cls._v14_terminal_rerun_delivery_authority_schema_statements():
            conn.execute(statement)

    @classmethod
    def _v14_terminal_rerun_binding_guard_sql_contract(
        cls,
    ) -> tuple[str, str]:
        prefix = "CREATE TRIGGER IF NOT EXISTS "
        name = "trg_terminal_rerun_delivery_authority_binding_guard"
        candidates = [
            cls._normalized_schema_sql(statement)
            for statement in cls._v14_terminal_rerun_delivery_authority_schema_statements()
            if cls._normalized_schema_sql(statement).startswith(prefix + name + " ")
        ]
        if len(candidates) != 1:
            raise RuntimeError(
                "incompatible_control_store_schema:terminal_rerun_authority_trigger_contract"
            )
        strict = candidates[0].replace(
            prefix,
            "CREATE TRIGGER ",
            1,
        )
        strict_state = "AND epoch.state = 'steady_active'"
        legacy_state = "AND epoch.state IN ('bounded_active', 'steady_active')"
        if strict.count(strict_state) != 1:
            raise RuntimeError(
                "incompatible_control_store_schema:terminal_rerun_authority_trigger_contract"
            )
        legacy = strict.replace(strict_state, legacy_state, 1)
        if (
            hashlib.sha256(legacy.encode("utf-8")).hexdigest()
            != _KNOWN_V14_TERMINAL_RERUN_BINDING_GUARD_SHA256
        ):
            raise RuntimeError(
                "incompatible_control_store_schema:terminal_rerun_authority_trigger_contract"
            )
        return strict, legacy

    @classmethod
    def _validate_v14_terminal_rerun_delivery_authority_schema(
        cls,
        conn: sqlite3.Connection,
        *,
        allow_known_legacy_binding_guard: bool = False,
    ) -> None:
        expected_tables: dict[str, str] = {}
        expected_indexes: dict[str, str] = {}
        expected_triggers: dict[str, str] = {}
        for statement in cls._v14_terminal_rerun_delivery_authority_schema_statements():
            normalized = cls._normalized_schema_sql(statement)
            for prefix, destination in (
                ("CREATE TABLE IF NOT EXISTS ", expected_tables),
                ("CREATE INDEX IF NOT EXISTS ", expected_indexes),
                ("CREATE TRIGGER IF NOT EXISTS ", expected_triggers),
            ):
                if normalized.startswith(prefix):
                    name = normalized[len(prefix) :].split(" ", 1)[0]
                    destination[name] = normalized.replace(
                        prefix, prefix.replace(" IF NOT EXISTS", ""), 1
                    )
                    break

        def observed(kind: str, expected: Mapping[str, str]) -> dict[str, str]:
            return {
                str(row["name"]): cls._normalized_schema_sql(row["sql"] or "")
                for row in conn.execute(
                    "SELECT name, sql FROM sqlite_master WHERE type = ?",
                    (kind,),
                ).fetchall()
                if str(row["name"]) in expected
            }

        if observed("table", expected_tables) != expected_tables:
            raise RuntimeError(
                "incompatible_control_store_schema:terminal_rerun_authority_table_sql"
            )
        if observed("index", expected_indexes) != expected_indexes:
            raise RuntimeError(
                "incompatible_control_store_schema:terminal_rerun_authority_index_sql"
            )
        observed_triggers = observed("trigger", expected_triggers)
        if observed_triggers != expected_triggers:
            strict_binding, legacy_binding = (
                cls._v14_terminal_rerun_binding_guard_sql_contract()
            )
            binding_name = "trg_terminal_rerun_delivery_authority_binding_guard"
            if expected_triggers.get(binding_name) != strict_binding:
                raise RuntimeError(
                    "incompatible_control_store_schema:terminal_rerun_authority_trigger_contract"
                )
            legacy_triggers = dict(expected_triggers)
            legacy_triggers[binding_name] = legacy_binding
            marker = conn.execute(
                "SELECT value FROM control_meta WHERE key = 'schema_version'"
            ).fetchone()
            marker_value = str(marker[0]) if marker is not None else ""
            if (
                not allow_known_legacy_binding_guard
                or marker_value != CONTROL_STORE_SCHEMA_VERSION
                or observed_triggers != legacy_triggers
            ):
                raise RuntimeError(
                    "incompatible_control_store_schema:terminal_rerun_authority_trigger_sql"
                )
        authority_tables = {
            str(row["name"])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' "
                "AND name LIKE 'rca_terminal_rerun_delivery_authorit%'"
            ).fetchall()
        }
        authority_triggers = {
            str(row["name"])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'trigger' "
                "AND name LIKE 'trg_terminal_rerun_delivery_authority_%'"
            ).fetchall()
        }
        if authority_tables != set(expected_tables):
            raise RuntimeError(
                "incompatible_control_store_schema:terminal_rerun_authority_tables"
            )
        if authority_triggers != set(expected_triggers):
            raise RuntimeError(
                "incompatible_control_store_schema:terminal_rerun_authority_triggers"
            )
        for row in conn.execute(
            "SELECT * FROM rca_terminal_rerun_delivery_authorities "
            "ORDER BY authority_sha256"
        ).fetchall():
            try:
                authority = json.loads(str(row["authority_json"]))
                if str(row["authority_kind"]) == "silent_terminal":
                    expected = build_silent_terminal_rerun_authority(
                        batch_id=authority.get("batch_id"),
                        queue_sha256=authority.get("queue_sha256"),
                        issue_id=authority.get("issue_id"),
                        prior_submission_key=authority.get("prior_submission_key"),
                        prior_generation=authority.get("prior_generation"),
                        owner_receipt_path=authority.get("owner_receipt_path"),
                        owner_receipt_sha256=authority.get("owner_receipt_sha256"),
                        requester_id=authority.get("requester_id"),
                        reason=authority.get("reason"),
                        activation_required=authority.get("activation_required"),
                    )
                    prior_delivery_id = ""
                elif str(row["authority_kind"]) == "batch_terminal":
                    expected = build_batch_terminal_rerun_authority(
                        batch_id=authority.get("batch_id"),
                        queue_sha256=authority.get("queue_sha256"),
                        issue_id=authority.get("issue_id"),
                        prior_submission_key=authority.get("prior_submission_key"),
                        prior_generation=authority.get("prior_generation"),
                        prior_delivery_id=authority.get("prior_delivery_id"),
                        owner_receipt_path=authority.get("owner_receipt_path"),
                        owner_receipt_sha256=authority.get("owner_receipt_sha256"),
                        requester_id=authority.get("requester_id"),
                        reason=authority.get("reason"),
                        activation_required=authority.get("activation_required"),
                    )
                    prior_delivery_id = str(expected["prior_delivery_id"])
                else:
                    raise ValueError("authority_kind_invalid")
            except (AttributeError, TypeError, ValueError, json.JSONDecodeError) as exc:
                raise RuntimeError(
                    "incompatible_control_store_schema:terminal_rerun_authority_json"
                ) from exc
            activation = conn.execute(
                """
                SELECT outbox.outbox_id, trigger.activation_epoch_id,
                       trigger.activation_ledger_id,
                       trigger.project_key, trigger.work_item_type_key,
                       json_extract(
                           trigger.normalized_json, '$.project_simple_name'
                       ) AS project_simple_name
                  FROM business_triggers AS trigger
                  JOIN rca_outbox AS outbox
                    ON outbox.business_key = trigger.business_key
                   AND outbox.generation = trigger.generation
                   AND outbox.submission_key = trigger.submission_key
                   AND outbox.activation_epoch_id = trigger.activation_epoch_id
                   AND outbox.activation_ledger_id = trigger.activation_ledger_id
                 WHERE trigger.business_key = ?
                   AND trigger.generation = ?
                   AND trigger.submission_key = ?
                """,
                (
                    row["business_key"],
                    row["generation"],
                    row["submission_key"],
                ),
            ).fetchone()
            if (
                activation is None
                or not str(activation["activation_epoch_id"] or "").strip()
                or activation["activation_ledger_id"] is None
            ):
                raise RuntimeError(
                    "incompatible_control_store_schema:"
                    "terminal_rerun_authority_activation"
                )
            projected = {
                "authority_sha256": str(expected["selection_sha256"]),
                "schema_version": TERMINAL_RERUN_DELIVERY_AUTHORITY_SCHEMA_VERSION,
                "source_payload_sha256": str(row["source_payload_sha256"]),
                "outbox_id": int(activation["outbox_id"]),
                "business_key": str(row["business_key"]),
                "generation": int(row["generation"]),
                "submission_key": str(row["submission_key"]),
                "activation_epoch_id": str(activation["activation_epoch_id"]),
                "activation_ledger_id": int(activation["activation_ledger_id"]),
                "effect_kind": "feishu_issue_comment",
                "project_key": str(activation["project_key"]),
                "project_simple_name": str(activation["project_simple_name"]),
                "work_item_type_key": str(activation["work_item_type_key"]),
                "issue_id": str(expected["issue_id"]),
                "batch_id": str(expected["batch_id"]),
                "prior_submission_key": str(expected["prior_submission_key"]),
                "prior_generation": int(expected["prior_generation"]),
                "prior_delivery_id": prior_delivery_id,
                "queue_sha256": str(expected["queue_sha256"]),
                "owner_receipt_path": str(expected["owner_receipt_path"]),
                "owner_receipt_sha256": str(expected["owner_receipt_sha256"]),
                "requester_id": str(expected["requester_id"]),
                "reason": str(expected["reason"]),
                "activation_required": 1,
                "authority_json": _canonical_json(expected),
            }
            if any(row[name] != value for name, value in projected.items()):
                raise RuntimeError(
                    "incompatible_control_store_schema:terminal_rerun_authority_projection"
                )
            binding = conn.execute(
                """
                SELECT 1
                  FROM rca_trigger_sources AS source
                  JOIN rca_trigger_bindings AS bound
                    ON bound.source_id = source.source_id
                  JOIN business_triggers AS trigger
                    ON trigger.business_key = bound.business_key
                   AND trigger.generation = bound.generation
                  JOIN rca_outbox AS outbox
                    ON outbox.business_key = trigger.business_key
                   AND outbox.generation = trigger.generation
                  JOIN business_triggers AS prior
                    ON prior.business_key = trigger.business_key
                   AND prior.generation = ?
                  JOIN rca_activation_admission_ledger AS ledger
                    ON ledger.ledger_id = trigger.activation_ledger_id
                   AND ledger.epoch_id = trigger.activation_epoch_id
                  JOIN rca_activation_epochs AS epoch
                    ON epoch.epoch_id = ledger.epoch_id
                 WHERE source.source_id = ?
                   AND source.payload_sha256 = ?
                   AND source.source_kind = 'feishu_group_manual'
                   AND source.platform = 'operator'
                   AND source.chat_id = ''
                   AND source.thread_id = ''
                   AND source.mode = 'rerun'
                   AND source.outcome = 'created'
                   AND bound.role = 'origin'
                   AND trigger.business_key = ?
                   AND trigger.generation = ?
                   AND trigger.submission_key = ?
                   AND trigger.origin_source_id = source.source_id
                   AND trigger.project_key = ?
                   AND trigger.work_item_type_key = ?
                   AND trigger.work_item_id = ?
                   AND json_extract(
                       trigger.normalized_json, '$.project_simple_name'
                   ) = ?
                   AND trigger.activation_epoch_id = ?
                   AND trigger.activation_ledger_id = ?
                   AND outbox.submission_key = trigger.submission_key
                   AND outbox.outbox_id = ?
                   AND outbox.origin_source_id = source.source_id
                   AND outbox.action = 'submit_rca_issue_intake'
                   AND outbox.activation_epoch_id = trigger.activation_epoch_id
                   AND outbox.activation_ledger_id = trigger.activation_ledger_id
                   AND prior.submission_key = ?
                   AND prior.work_item_id = trigger.work_item_id
                   AND ledger.entrypoint = 'manual_admit'
                   AND ledger.source_kind = 'manual'
                   AND ledger.decision = 'admit'
                   AND ledger.business_key = trigger.business_key
                   AND ledger.submission_key = trigger.submission_key
                   AND ledger.generation = trigger.generation
                   AND ledger.bound_at IS NOT NULL
                """,
                (
                    row["prior_generation"],
                    row["source_id"],
                    row["source_payload_sha256"],
                    row["business_key"],
                    row["generation"],
                    row["submission_key"],
                    row["project_key"],
                    row["work_item_type_key"],
                    row["issue_id"],
                    row["project_simple_name"],
                    row["activation_epoch_id"],
                    row["activation_ledger_id"],
                    row["outbox_id"],
                    row["prior_submission_key"],
                ),
            ).fetchone()
            if binding is None:
                raise RuntimeError(
                    "incompatible_control_store_schema:terminal_rerun_authority_binding"
                )

    @staticmethod
    def _historical_epoch_rerun_delivery_authority_trigger_names() -> tuple[str, ...]:
        return (
            "trg_historical_epoch_rerun_delivery_authority_no_update",
            "trg_historical_epoch_rerun_delivery_authority_no_delete",
            "trg_historical_epoch_rerun_delivery_authority_no_replace",
            "trg_historical_epoch_rerun_delivery_authority_projection_guard",
            "trg_historical_epoch_rerun_delivery_authority_binding_guard",
        )

    @classmethod
    def _drop_historical_epoch_rerun_delivery_authority_triggers(
        cls, conn: sqlite3.Connection
    ) -> None:
        """Remove parent-referencing guards while legacy parents are rebuilt."""
        conn.execute(
            "DROP VIEW IF EXISTS rca_owner_authorized_rerun_delivery_authorities"
        )
        for name in cls._historical_epoch_rerun_delivery_authority_trigger_names():
            conn.execute(f"DROP TRIGGER IF EXISTS {name}")

    @staticmethod
    def _historical_epoch_rerun_delivery_authority_schema_statements() -> tuple[
        str, ...
    ]:
        return (
            f"""
            CREATE TABLE IF NOT EXISTS rca_historical_epoch_rerun_delivery_authorities (
                authority_sha256 TEXT PRIMARY KEY CHECK(
                    length(authority_sha256) = 64
                    AND authority_sha256 NOT GLOB '*[^0-9a-f]*'
                ),
                schema_version TEXT NOT NULL CHECK(
                    schema_version =
                        '{HISTORICAL_EPOCH_RERUN_DELIVERY_AUTHORITY_SCHEMA_VERSION}'
                ),
                source_id TEXT NOT NULL UNIQUE,
                outbox_id INTEGER NOT NULL UNIQUE,
                source_payload_sha256 TEXT NOT NULL CHECK(
                    length(source_payload_sha256) = 64
                    AND source_payload_sha256 NOT GLOB '*[^0-9a-f]*'
                ),
                business_key TEXT NOT NULL,
                generation INTEGER NOT NULL CHECK(generation >= 2),
                submission_key TEXT NOT NULL UNIQUE,
                activation_epoch_id TEXT NOT NULL,
                activation_ledger_id INTEGER NOT NULL UNIQUE,
                effect_kind TEXT NOT NULL CHECK(
                    effect_kind = 'feishu_issue_comment'
                ),
                project_key TEXT NOT NULL CHECK(length(trim(project_key)) > 0),
                project_simple_name TEXT NOT NULL CHECK(
                    length(trim(project_simple_name)) > 0
                ),
                work_item_type_key TEXT NOT NULL CHECK(
                    length(trim(work_item_type_key)) > 0
                ),
                issue_id TEXT NOT NULL CHECK(
                    length(issue_id) BETWEEN 1 AND 32
                    AND issue_id NOT GLOB '*[^0-9]*'
                ),
                batch_id TEXT NOT NULL CHECK(length(trim(batch_id)) > 0),
                prior_submission_key TEXT NOT NULL,
                prior_generation INTEGER NOT NULL CHECK(prior_generation >= 1),
                prior_activation_epoch_id TEXT NOT NULL,
                prior_activation_ledger_id INTEGER,
                target_activation_epoch_id TEXT NOT NULL,
                queue_sha256 TEXT NOT NULL CHECK(
                    length(queue_sha256) = 64
                    AND queue_sha256 NOT GLOB '*[^0-9a-f]*'
                ),
                owner_receipt_path TEXT NOT NULL CHECK(
                    length(trim(owner_receipt_path)) > 0
                ),
                owner_receipt_sha256 TEXT NOT NULL CHECK(
                    length(owner_receipt_sha256) = 64
                    AND owner_receipt_sha256 NOT GLOB '*[^0-9a-f]*'
                ),
                requester_id TEXT NOT NULL CHECK(
                    requester_id = 'automation:rca-batch-rerun'
                ),
                reason TEXT NOT NULL CHECK(
                    reason = 'production_gray_batch:' || batch_id
                ),
                activation_required INTEGER NOT NULL CHECK(
                    activation_required = 1
                ),
                authority_json TEXT NOT NULL CHECK(json_valid(authority_json)),
                created_at TEXT NOT NULL CHECK(length(trim(created_at)) > 0),
                UNIQUE(business_key, generation),
                CHECK(generation = prior_generation + 1),
                CHECK(target_activation_epoch_id = activation_epoch_id),
                CHECK(target_activation_epoch_id != prior_activation_epoch_id),
                CHECK(
                    (prior_activation_epoch_id = ''
                     AND prior_activation_ledger_id IS NULL)
                    OR
                    (length(trim(prior_activation_epoch_id)) > 0
                     AND prior_activation_ledger_id IS NOT NULL)
                ),
                FOREIGN KEY(source_id) REFERENCES rca_trigger_sources(source_id),
                FOREIGN KEY(outbox_id) REFERENCES rca_outbox(outbox_id),
                FOREIGN KEY(business_key, generation)
                    REFERENCES business_triggers(business_key, generation),
                FOREIGN KEY(submission_key)
                    REFERENCES business_triggers(submission_key),
                FOREIGN KEY(activation_epoch_id)
                    REFERENCES rca_activation_epochs(epoch_id),
                FOREIGN KEY(activation_ledger_id)
                    REFERENCES rca_activation_admission_ledger(ledger_id),
                FOREIGN KEY(business_key, prior_generation)
                    REFERENCES business_triggers(business_key, generation),
                FOREIGN KEY(prior_submission_key)
                    REFERENCES business_triggers(submission_key),
                FOREIGN KEY(prior_activation_ledger_id)
                    REFERENCES rca_activation_admission_ledger(ledger_id)
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_historical_epoch_rerun_authority_issue
                ON rca_historical_epoch_rerun_delivery_authorities(
                    issue_id, generation, authority_sha256
                )
            """,
            """
            CREATE TRIGGER IF NOT EXISTS
                trg_historical_epoch_rerun_delivery_authority_no_update
            BEFORE UPDATE ON rca_historical_epoch_rerun_delivery_authorities
            BEGIN
                SELECT RAISE(
                    ABORT, 'historical_epoch_rerun_authority_update_forbidden'
                );
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS
                trg_historical_epoch_rerun_delivery_authority_no_delete
            BEFORE DELETE ON rca_historical_epoch_rerun_delivery_authorities
            BEGIN
                SELECT RAISE(
                    ABORT, 'historical_epoch_rerun_authority_delete_forbidden'
                );
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS
                trg_historical_epoch_rerun_delivery_authority_no_replace
            BEFORE INSERT ON rca_historical_epoch_rerun_delivery_authorities
            WHEN EXISTS (
                SELECT 1 FROM rca_historical_epoch_rerun_delivery_authorities
                 WHERE authority_sha256 = NEW.authority_sha256
                    OR source_id = NEW.source_id
                    OR submission_key = NEW.submission_key
                    OR (
                        business_key = NEW.business_key
                        AND generation = NEW.generation
                    )
            )
            BEGIN
                SELECT RAISE(
                    ABORT, 'historical_epoch_rerun_authority_replace_forbidden'
                );
            END
            """,
            f"""
            CREATE TRIGGER IF NOT EXISTS
                trg_historical_epoch_rerun_delivery_authority_projection_guard
            BEFORE INSERT ON rca_historical_epoch_rerun_delivery_authorities
            WHEN NOT COALESCE((
                json_extract(NEW.authority_json, '$.schema_version') =
                    '{HISTORICAL_EPOCH_RERUN_AUTHORITY_SCHEMA_VERSION}'
                AND json_extract(NEW.authority_json, '$.selection_sha256') =
                    NEW.authority_sha256
                AND json_extract(NEW.authority_json, '$.batch_id') = NEW.batch_id
                AND json_extract(NEW.authority_json, '$.queue_sha256') =
                    NEW.queue_sha256
                AND json_extract(NEW.authority_json, '$.issue_id') = NEW.issue_id
                AND json_extract(
                    NEW.authority_json, '$.prior_submission_key'
                ) = NEW.prior_submission_key
                AND json_extract(NEW.authority_json, '$.prior_generation') =
                    NEW.prior_generation
                AND json_extract(
                    NEW.authority_json, '$.prior_activation_epoch_id'
                ) = NEW.prior_activation_epoch_id
                AND (
                    (
                        NEW.prior_activation_ledger_id IS NULL
                        AND json_type(
                            NEW.authority_json, '$.prior_activation_ledger_id'
                        ) = 'null'
                    ) OR json_extract(
                        NEW.authority_json, '$.prior_activation_ledger_id'
                    ) = NEW.prior_activation_ledger_id
                )
                AND json_extract(
                    NEW.authority_json, '$.target_activation_epoch_id'
                ) = NEW.target_activation_epoch_id
                AND json_extract(NEW.authority_json, '$.owner_receipt_path') =
                    NEW.owner_receipt_path
                AND json_extract(NEW.authority_json, '$.owner_receipt_sha256') =
                    NEW.owner_receipt_sha256
                AND json_extract(NEW.authority_json, '$.activation_required') =
                    NEW.activation_required
                AND json_extract(NEW.authority_json, '$.requester_id') =
                    NEW.requester_id
                AND json_extract(NEW.authority_json, '$.reason') = NEW.reason
                AND (SELECT COUNT(*) FROM json_each(NEW.authority_json)) = 15
            ), 0)
            BEGIN
                SELECT RAISE(
                    ABORT, 'historical_epoch_rerun_authority_projection_mismatch'
                );
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS
                trg_historical_epoch_rerun_delivery_authority_binding_guard
            BEFORE INSERT ON rca_historical_epoch_rerun_delivery_authorities
            WHEN NOT EXISTS (
                SELECT 1
                  FROM rca_trigger_sources AS source
                  JOIN rca_trigger_bindings AS binding
                    ON binding.source_id = source.source_id
                  JOIN business_triggers AS trigger
                    ON trigger.business_key = binding.business_key
                   AND trigger.generation = binding.generation
                  JOIN rca_outbox AS outbox
                    ON outbox.business_key = trigger.business_key
                   AND outbox.generation = trigger.generation
                   AND outbox.submission_key = trigger.submission_key
                  JOIN business_triggers AS prior
                    ON prior.business_key = trigger.business_key
                   AND prior.generation = NEW.prior_generation
                  JOIN rca_outbox AS prior_outbox
                    ON prior_outbox.business_key = prior.business_key
                   AND prior_outbox.generation = prior.generation
                   AND prior_outbox.submission_key = prior.submission_key
                  JOIN rca_activation_admission_ledger AS ledger
                    ON ledger.ledger_id = NEW.activation_ledger_id
                   AND ledger.epoch_id = NEW.activation_epoch_id
                  JOIN rca_activation_epochs AS epoch
                    ON epoch.epoch_id = ledger.epoch_id
                 WHERE source.source_id = NEW.source_id
                   AND source.source_kind = 'feishu_group_manual'
                   AND source.payload_sha256 = NEW.source_payload_sha256
                   AND source.platform = 'operator'
                   AND source.chat_id = ''
                   AND source.thread_id = ''
                   AND source.requester_id = NEW.requester_id
                   AND source.mode = 'rerun'
                   AND source.outcome = 'created'
                   AND source.message_id GLOB
                       NEW.batch_id || '-' || NEW.issue_id || '-try-[1-9]*'
                   AND binding.business_key = NEW.business_key
                   AND binding.generation = NEW.generation
                   AND binding.role = 'origin'
                   AND trigger.submission_key = NEW.submission_key
                   AND trigger.origin_source_id = NEW.source_id
                   AND trigger.project_key = NEW.project_key
                   AND trigger.work_item_type_key = NEW.work_item_type_key
                   AND trigger.work_item_id = NEW.issue_id
                   AND json_extract(
                       trigger.normalized_json, '$.project_simple_name'
                   ) = NEW.project_simple_name
                   AND trigger.activation_epoch_id = NEW.activation_epoch_id
                   AND trigger.activation_ledger_id = NEW.activation_ledger_id
                   AND trigger.activation_epoch_id = outbox.activation_epoch_id
                   AND trigger.activation_ledger_id = outbox.activation_ledger_id
                   AND outbox.origin_source_id = NEW.source_id
                   AND outbox.outbox_id = NEW.outbox_id
                   AND outbox.action = 'submit_rca_issue_intake'
                   AND prior.submission_key = NEW.prior_submission_key
                   AND prior.work_item_id = NEW.issue_id
                   AND (
                        (
                            NEW.prior_activation_epoch_id = ''
                            AND NEW.prior_activation_ledger_id IS NULL
                            AND prior.activation_epoch_id IS NULL
                            AND prior.activation_ledger_id IS NULL
                            AND prior_outbox.activation_epoch_id IS NULL
                            AND prior_outbox.activation_ledger_id IS NULL
                        ) OR (
                            NEW.prior_activation_epoch_id != ''
                            AND NEW.prior_activation_ledger_id IS NOT NULL
                            AND prior.activation_epoch_id =
                                NEW.prior_activation_epoch_id
                            AND prior.activation_ledger_id =
                                NEW.prior_activation_ledger_id
                            AND prior_outbox.activation_epoch_id =
                                NEW.prior_activation_epoch_id
                            AND prior_outbox.activation_ledger_id =
                                NEW.prior_activation_ledger_id
                            AND EXISTS (
                                SELECT 1
                                  FROM rca_activation_admission_ledger AS old_ledger
                                  JOIN rca_activation_epochs AS old_epoch
                                    ON old_epoch.epoch_id = old_ledger.epoch_id
                                 WHERE old_ledger.ledger_id =
                                       NEW.prior_activation_ledger_id
                                   AND old_ledger.epoch_id =
                                       NEW.prior_activation_epoch_id
                                   AND old_ledger.business_key = NEW.business_key
                                   AND old_ledger.submission_key =
                                       NEW.prior_submission_key
                                   AND old_ledger.generation =
                                       NEW.prior_generation
                                   AND old_ledger.decision IN ('admit', 'shadow')
                                   AND old_ledger.bound_at IS NOT NULL
                                   AND old_epoch.is_current = 0
                            )
                        )
                   )
                   AND ledger.entrypoint = 'manual_admit'
                   AND ledger.source_kind = 'manual'
                   AND ledger.decision = 'admit'
                   AND ledger.business_key = NEW.business_key
                   AND ledger.submission_key = NEW.submission_key
                   AND ledger.generation = NEW.generation
                   AND ledger.bound_at IS NOT NULL
                   AND epoch.epoch_id = NEW.target_activation_epoch_id
                   AND epoch.is_current = 1
                   AND epoch.state = 'steady_active'
            )
            BEGIN
                SELECT RAISE(
                    ABORT, 'historical_epoch_rerun_authority_binding_mismatch'
                );
            END
            """,
            """
            CREATE VIEW IF NOT EXISTS rca_owner_authorized_rerun_delivery_authorities AS
            SELECT 'terminal_rerun' AS authority_family,
                   authority_sha256, source_id, outbox_id,
                   source_payload_sha256, business_key, generation,
                   submission_key, activation_epoch_id, activation_ledger_id,
                   effect_kind, project_key, project_simple_name,
                   work_item_type_key, issue_id, batch_id, queue_sha256,
                   owner_receipt_path, owner_receipt_sha256, requester_id,
                   reason, activation_required, authority_json, created_at
              FROM rca_terminal_rerun_delivery_authorities
            UNION ALL
            SELECT 'historical_epoch_rerun' AS authority_family,
                   authority_sha256, source_id, outbox_id,
                   source_payload_sha256, business_key, generation,
                   submission_key, activation_epoch_id, activation_ledger_id,
                   effect_kind, project_key, project_simple_name,
                   work_item_type_key, issue_id, batch_id, queue_sha256,
                   owner_receipt_path, owner_receipt_sha256, requester_id,
                   reason, activation_required, authority_json, created_at
              FROM rca_historical_epoch_rerun_delivery_authorities
            """,
        )

    @classmethod
    def _create_historical_epoch_rerun_delivery_authority_schema(
        cls, conn: sqlite3.Connection
    ) -> None:
        for (
            statement
        ) in cls._historical_epoch_rerun_delivery_authority_schema_statements():
            conn.execute(statement)

    @classmethod
    def _validate_historical_epoch_rerun_delivery_authority_schema(
        cls, conn: sqlite3.Connection
    ) -> None:
        normalize_sql = lambda value: " ".join(str(value).split()).rstrip(";")
        expected_tables: dict[str, str] = {}
        expected_indexes: dict[str, str] = {}
        expected_triggers: dict[str, str] = {}
        expected_views: dict[str, str] = {}
        for (
            statement
        ) in cls._historical_epoch_rerun_delivery_authority_schema_statements():
            normalized = normalize_sql(statement)
            for prefix, destination in (
                ("CREATE TABLE IF NOT EXISTS ", expected_tables),
                ("CREATE INDEX IF NOT EXISTS ", expected_indexes),
                ("CREATE TRIGGER IF NOT EXISTS ", expected_triggers),
                ("CREATE VIEW IF NOT EXISTS ", expected_views),
            ):
                if normalized.startswith(prefix):
                    name = normalized[len(prefix) :].split(" ", 1)[0]
                    destination[name] = normalized.replace(
                        prefix, prefix.replace(" IF NOT EXISTS", ""), 1
                    )
                    break

        def observed(kind: str, expected: Mapping[str, str]) -> dict[str, str]:
            return {
                str(row["name"]): normalize_sql(row["sql"] or "")
                for row in conn.execute(
                    "SELECT name, sql FROM sqlite_master WHERE type = ?", (kind,)
                ).fetchall()
                if str(row["name"]) in expected
            }

        if observed("table", expected_tables) != expected_tables:
            raise RuntimeError(
                "incompatible_control_store_schema:"
                "historical_epoch_rerun_authority_table_sql"
            )
        if observed("index", expected_indexes) != expected_indexes:
            raise RuntimeError(
                "incompatible_control_store_schema:"
                "historical_epoch_rerun_authority_index_sql"
            )
        if observed("trigger", expected_triggers) != expected_triggers:
            raise RuntimeError(
                "incompatible_control_store_schema:"
                "historical_epoch_rerun_authority_trigger_sql"
            )
        if observed("view", expected_views) != expected_views:
            raise RuntimeError(
                "incompatible_control_store_schema:"
                "historical_epoch_rerun_authority_view_sql"
            )
        for row in conn.execute(
            "SELECT * FROM rca_historical_epoch_rerun_delivery_authorities "
            "ORDER BY authority_sha256"
        ).fetchall():
            try:
                authority = json.loads(str(row["authority_json"]))
                expected = build_historical_epoch_rerun_authority(
                    batch_id=authority.get("batch_id"),
                    queue_sha256=authority.get("queue_sha256"),
                    issue_id=authority.get("issue_id"),
                    prior_submission_key=authority.get("prior_submission_key"),
                    prior_generation=authority.get("prior_generation"),
                    prior_activation_epoch_id=authority.get(
                        "prior_activation_epoch_id"
                    ),
                    prior_activation_ledger_id=authority.get(
                        "prior_activation_ledger_id"
                    ),
                    target_activation_epoch_id=authority.get(
                        "target_activation_epoch_id"
                    ),
                    owner_receipt_path=authority.get("owner_receipt_path"),
                    owner_receipt_sha256=authority.get("owner_receipt_sha256"),
                    requester_id=authority.get("requester_id"),
                    reason=authority.get("reason"),
                    activation_required=authority.get("activation_required"),
                )
            except (AttributeError, TypeError, ValueError, json.JSONDecodeError) as exc:
                raise RuntimeError(
                    "incompatible_control_store_schema:"
                    "historical_epoch_rerun_authority_json"
                ) from exc
            projected = {
                "authority_sha256": str(expected["selection_sha256"]),
                "schema_version": (
                    HISTORICAL_EPOCH_RERUN_DELIVERY_AUTHORITY_SCHEMA_VERSION
                ),
                "issue_id": str(expected["issue_id"]),
                "batch_id": str(expected["batch_id"]),
                "prior_submission_key": str(expected["prior_submission_key"]),
                "prior_generation": int(expected["prior_generation"]),
                "prior_activation_epoch_id": str(expected["prior_activation_epoch_id"]),
                "prior_activation_ledger_id": expected["prior_activation_ledger_id"],
                "target_activation_epoch_id": str(
                    expected["target_activation_epoch_id"]
                ),
                "queue_sha256": str(expected["queue_sha256"]),
                "owner_receipt_path": str(expected["owner_receipt_path"]),
                "owner_receipt_sha256": str(expected["owner_receipt_sha256"]),
                "requester_id": str(expected["requester_id"]),
                "reason": str(expected["reason"]),
                "activation_required": 1,
                "authority_json": _canonical_json(expected),
            }
            if any(row[name] != value for name, value in projected.items()):
                raise RuntimeError(
                    "incompatible_control_store_schema:"
                    "historical_epoch_rerun_authority_projection"
                )

    @staticmethod
    def _preflight_schema_version_at(
        sqlite_path: Path,
        *,
        allow_successor_read_only: bool,
        immutable: bool = False,
    ) -> str | None:
        """Probe schema identity without issuing a write-capable SQLite pragma."""

        if not sqlite_path.is_file() or sqlite_path.stat().st_size == 0:
            return None
        immutable_option = "&immutable=1" if immutable else ""
        uri = f"{sqlite_path.resolve().as_uri()}?mode=ro{immutable_option}"
        try:
            conn = sqlite3.connect(uri, uri=True, isolation_level=None)
            conn.row_factory = sqlite3.Row
            conn.execute("BEGIN")
            table = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'control_meta'"
            ).fetchone()
            marker = (
                conn.execute(
                    "SELECT value FROM control_meta WHERE key = 'schema_version'"
                ).fetchone()
                if table is not None
                else None
            )
            epoch_columns = {
                str(row[1])
                for row in conn.execute(
                    "PRAGMA table_info(rca_activation_epochs)"
                ).fetchall()
            }
            audit_columns = {
                str(row[1])
                for row in conn.execute(
                    "PRAGMA table_info(rca_activation_transition_audit)"
                ).fetchall()
            }
            conn.commit()
        except sqlite3.Error as exc:
            if "conn" in locals() and conn.in_transaction:
                conn.rollback()
            raise RuntimeError("incompatible_control_store_schema:preflight") from exc
        finally:
            if "conn" in locals():
                conn.close()
        marker_value = str(marker["value"]) if marker is not None else None
        v15_layout_present = bool(
            epoch_columns & _V15_DISTINCT_ACTIVATION_COLUMNS
            or "binding_schema_version" in audit_columns
        )
        if (
            v15_layout_present
            and marker_value != CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION
        ):
            raise RuntimeError("incompatible_control_store_schema:activation_layout")
        if marker_value == CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION:
            if not allow_successor_read_only:
                raise RuntimeError("incompatible_control_store_schema:version")
        elif (
            marker_value is not None
            and marker_value not in SUPPORTED_CONTROL_STORE_SCHEMA_VERSIONS
        ):
            raise RuntimeError("incompatible_control_store_schema:version")
        return marker_value

    def _preflight_schema_version(self) -> str | None:
        return self._preflight_schema_version_at(
            self._sqlite_path,
            allow_successor_read_only=self.allow_successor_read_only,
        )

    @staticmethod
    def _normalized_schema_sql(value: object) -> str:
        return " ".join(str(value or "").split()).rstrip(";")

    @classmethod
    def _activation_schema_version_tx(cls, conn: sqlite3.Connection) -> str:
        """Return v14/v15 only when marker and both activation layouts are exact."""

        marker = conn.execute(
            "SELECT value FROM control_meta WHERE key = 'schema_version'"
        ).fetchone()
        marker_value = str(marker["value"] or "") if marker is not None else ""
        epoch_columns = tuple(
            str(row["name"])
            for row in conn.execute(
                "PRAGMA table_info(rca_activation_epochs)"
            ).fetchall()
        )
        audit_columns = tuple(
            str(row["name"])
            for row in conn.execute(
                "PRAGMA table_info(rca_activation_transition_audit)"
            ).fetchall()
        )
        layouts = {
            CONTROL_STORE_SCHEMA_VERSION: (
                _V14_ACTIVATION_EPOCH_COLUMNS,
                _V14_ACTIVATION_AUDIT_COLUMNS,
            ),
            CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION: (
                _V15_ACTIVATION_EPOCH_COLUMNS,
                _V15_ACTIVATION_AUDIT_COLUMNS,
            ),
        }
        expected = layouts.get(marker_value)
        if expected is None or (epoch_columns, audit_columns) != expected:
            raise RuntimeError("incompatible_control_store_schema:activation_layout")
        return marker_value

    @staticmethod
    def _validate_activation_reference_contract_tx(
        conn: sqlite3.Connection,
    ) -> None:
        no_action = ("NO ACTION", "NO ACTION", "NONE")
        expected_foreign_keys = {
            "rca_activation_admission_ledger": (
                (0, 0, "rca_activation_epochs", "epoch_id", "epoch_id", *no_action),
            ),
            "rca_activation_transition_audit": (
                (0, 0, "rca_activation_epochs", "epoch_id", "epoch_id", *no_action),
            ),
            "rca_terminal_rerun_delivery_authorities": (
                (0, 0, "business_triggers", "prior_submission_key", "submission_key", *no_action),
                (1, 0, "business_triggers", "business_key", "business_key", *no_action),
                (1, 1, "business_triggers", "prior_generation", "generation", *no_action),
                (2, 0, "rca_activation_admission_ledger", "activation_ledger_id", "ledger_id", *no_action),
                (3, 0, "rca_activation_epochs", "activation_epoch_id", "epoch_id", *no_action),
                (4, 0, "business_triggers", "submission_key", "submission_key", *no_action),
                (5, 0, "business_triggers", "business_key", "business_key", *no_action),
                (5, 1, "business_triggers", "generation", "generation", *no_action),
                (6, 0, "rca_outbox", "outbox_id", "outbox_id", *no_action),
                (7, 0, "rca_trigger_sources", "source_id", "source_id", *no_action),
            ),
            "rca_historical_epoch_rerun_delivery_authorities": (
                (0, 0, "rca_activation_admission_ledger", "prior_activation_ledger_id", "ledger_id", *no_action),
                (1, 0, "business_triggers", "prior_submission_key", "submission_key", *no_action),
                (2, 0, "business_triggers", "business_key", "business_key", *no_action),
                (2, 1, "business_triggers", "prior_generation", "generation", *no_action),
                (3, 0, "rca_activation_admission_ledger", "activation_ledger_id", "ledger_id", *no_action),
                (4, 0, "rca_activation_epochs", "activation_epoch_id", "epoch_id", *no_action),
                (5, 0, "business_triggers", "submission_key", "submission_key", *no_action),
                (6, 0, "business_triggers", "business_key", "business_key", *no_action),
                (6, 1, "business_triggers", "generation", "generation", *no_action),
                (7, 0, "rca_outbox", "outbox_id", "outbox_id", *no_action),
                (8, 0, "rca_trigger_sources", "source_id", "source_id", *no_action),
            ),
        }
        for table, expected in expected_foreign_keys.items():
            observed = tuple(
                sorted(
                    (
                        int(row["id"]),
                        int(row["seq"]),
                        str(row["table"]),
                        str(row["from"]),
                        str(row["to"]),
                        str(row["on_update"]),
                        str(row["on_delete"]),
                        str(row["match"]),
                    )
                    for row in conn.execute(
                        f"PRAGMA foreign_key_list({table})"
                    ).fetchall()
                )
            )
            if observed != expected:
                raise RuntimeError(
                    "incompatible_control_store_schema:activation_foreign_keys"
                )

        cross_trigger_names = {
            "trg_rca_admission_snapshot_execution_guard",
            "trg_terminal_rerun_delivery_authority_binding_guard",
            "trg_historical_epoch_rerun_delivery_authority_binding_guard",
        }
        cross_triggers = {
            str(row["name"]): str(row["sql"] or "")
            for row in conn.execute(
                "SELECT name, sql FROM sqlite_master WHERE type = 'trigger'"
            ).fetchall()
            if str(row["name"]) in cross_trigger_names
        }
        if set(cross_triggers) != cross_trigger_names or any(
            "rca_activation_epochs" not in sql.lower()
            or "rca_activation_epochs_v14" in sql.lower()
            for sql in cross_triggers.values()
        ):
            raise RuntimeError(
                "incompatible_control_store_schema:activation_cross_triggers"
            )

    @staticmethod
    def _validate_activation_child_rows_against_parent_tx(
        conn: sqlite3.Connection,
        *,
        parent_table: str,
    ) -> None:
        if parent_table not in {
            "rca_activation_epochs",
            "rca_activation_epochs_v15_new",
        }:
            raise RuntimeError("activation_parent_table_invalid")
        child_references = {
            "rca_activation_admission_ledger": "epoch_id",
            "rca_activation_transition_audit": "epoch_id",
            "rca_terminal_rerun_delivery_authorities": "activation_epoch_id",
            "rca_historical_epoch_rerun_delivery_authorities": (
                "activation_epoch_id"
            ),
        }
        for table, column in child_references.items():
            orphan = conn.execute(
                f"SELECT 1 FROM {table} AS child "
                f"LEFT JOIN {parent_table} AS parent "
                f"ON parent.epoch_id = child.{column} "
                f"WHERE child.{column} IS NOT NULL AND parent.epoch_id IS NULL "
                "LIMIT 1"
            ).fetchone()
            if orphan is not None:
                raise RuntimeError(
                    "incompatible_control_store_schema:activation_child_orphan"
                )

    @staticmethod
    def _validate_activation_child_rows_tx(conn: sqlite3.Connection) -> None:
        RcaControlStore._validate_activation_child_rows_against_parent_tx(
            conn,
            parent_table="rca_activation_epochs",
        )

    @staticmethod
    def _validate_partition_start_fence_cas_tx(
        conn: sqlite3.Connection,
        *,
        partition_start_fence: Mapping[str, Any],
    ) -> None:
        expected = {
            str(topic): {
                str(partition): int(offset)
                for partition, offset in dict(partitions).items()
            }
            for topic, partitions in dict(partition_start_fence).items()
        }
        for topic, partitions in expected.items():
            for partition, offset in partitions.items():
                row = conn.execute(
                    "SELECT durable_next_offset FROM kafka_partition_progress "
                    "WHERE topic = ? AND partition_id = ?",
                    (topic, int(partition)),
                ).fetchone()
                if row is None or int(row["durable_next_offset"]) != offset:
                    raise ActivationEpochError("activation_partition_fence_changed")

    @classmethod
    def _validate_v15_activation_schema_tx(cls, conn: sqlite3.Connection) -> None:
        expected_sql = {
            ("table", "rca_activation_epochs"): _V15_ACTIVATION_EPOCH_TABLE_SQL,
            (
                "table",
                "rca_activation_transition_audit",
            ): _V15_ACTIVATION_AUDIT_TABLE_SQL,
            (
                "index",
                "idx_rca_single_current_activation_epoch",
            ): _V15_CURRENT_ACTIVATION_INDEX_SQL,
        }
        observed_sql = {
            (str(row["type"]), str(row["name"])): str(row["sql"] or "")
            for row in conn.execute(
                "SELECT type, name, sql FROM sqlite_master "
                "WHERE name IN (?, ?, ?)",
                (
                    "rca_activation_epochs",
                    "rca_activation_transition_audit",
                    "idx_rca_single_current_activation_epoch",
                ),
            ).fetchall()
        }
        if set(observed_sql) != set(expected_sql) or any(
            cls._normalized_schema_sql(observed_sql[key])
            != cls._normalized_schema_sql(expected)
            for key, expected in expected_sql.items()
        ):
            raise RuntimeError("incompatible_control_store_schema:v15_activation_sql")
        binding_column = next(
            (
                row
                for row in conn.execute(
                    "PRAGMA table_info(rca_activation_transition_audit)"
                ).fetchall()
                if str(row["name"]) == "binding_schema_version"
            ),
            None,
        )
        if (
            binding_column is None
            or str(binding_column["type"]).upper() != "TEXT"
            or int(binding_column["notnull"]) != 1
            or str(binding_column["dflt_value"] or "")
            != f"'{ACTIVATION_TRANSITION_BINDING_SCHEMA_V14}'"
        ):
            raise RuntimeError(
                "incompatible_control_store_schema:v15_activation_audit_sql"
            )
        for epoch in conn.execute(
            "SELECT * FROM rca_activation_epochs ORDER BY epoch_id"
        ).fetchall():
            try:
                db_identity = json.loads(str(epoch["db_logical_identity_json"]))
                partition_fence = json.loads(
                    str(epoch["partition_start_fence_json"])
                )
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise RuntimeError(
                    "incompatible_control_store_schema:v15_activation_state"
                ) from exc
            release_pair = (
                epoch["release_fingerprint_sha256"],
                epoch["release_note_sha256"],
            )
            release_pair_is_null = all(value is None for value in release_pair)
            release_pair_is_valid = all(
                isinstance(value, str)
                and _ACTIVATION_SHA256_RE.fullmatch(value) is not None
                and value != "0" * 64
                for value in release_pair
            )
            required_digests = (
                epoch["config_sha256"],
                epoch["db_logical_identity_sha256"],
                epoch["partition_start_fence_sha256"],
            )
            state = str(epoch["state"] or "")
            if (
                _ACTIVATION_EPOCH_ID_RE.fullmatch(str(epoch["epoch_id"] or ""))
                is None
                or not all(
                    isinstance(value, str)
                    and _ACTIVATION_SHA256_RE.fullmatch(value) is not None
                    and value != "0" * 64
                    for value in required_digests
                )
                or not (release_pair_is_null or release_pair_is_valid)
                or _canonical_json(db_identity)
                != str(epoch["db_logical_identity_json"])
                or _canonical_sha256(db_identity)
                != str(epoch["db_logical_identity_sha256"])
                or _canonical_json(partition_fence)
                != str(epoch["partition_start_fence_json"])
                or _canonical_sha256(partition_fence)
                != str(epoch["partition_start_fence_sha256"])
                or (
                    state == "steady_active"
                    and (
                        int(epoch["is_current"]) != 1
                        or not release_pair_is_valid
                        or epoch["activated_at"] is None
                        or epoch["retired_at"] is not None
                    )
                )
                or (
                    state == "retired"
                    and (
                        int(epoch["is_current"]) != 0
                        or epoch["retired_at"] is None
                    )
                )
                or state not in {"steady_active", "retired"}
            ):
                raise RuntimeError(
                    "incompatible_control_store_schema:v15_activation_state"
                )

    @classmethod
    def _validate_current_activation_binding_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        schema_version: str,
    ) -> None:
        current_epoch = cls._current_activation_epoch_unchecked_tx(conn)
        if current_epoch is not None:
            cls._activation_transition_binding_tx(
                conn,
                epoch=current_epoch,
                schema_version=schema_version,
            )

    @classmethod
    def validate_current_activation_binding_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        schema_version: str,
    ) -> None:
        """Validate the exact current epoch binding in the caller's transaction."""

        if not conn.in_transaction:
            raise RuntimeError("rca_control_store_binding_transaction_required")
        observed = cls._activation_schema_version_tx(conn)
        if observed != schema_version:
            raise RuntimeError("incompatible_control_store_schema:activation_layout")
        if observed == CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION:
            cls._validate_v15_activation_schema_tx(conn)
        cls._validate_current_activation_binding_tx(
            conn,
            schema_version=observed,
        )

    def _validate_current_schema_read_only(self) -> None:
        """Validate fixed-size schema metadata without taking a SQLite write lock."""
        uri = f"{self._sqlite_path.resolve().as_uri()}?mode=ro"
        conn = sqlite3.connect(uri, uri=True)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA recursive_triggers=ON")
            conn.execute("BEGIN")
            activation_schema_version = self._activation_schema_version_tx(conn)
            allow_known_legacy_binding_guard = (
                activation_schema_version == CONTROL_STORE_SCHEMA_VERSION
                and self.read_only
                and self.allow_successor_read_only
            )
            if activation_schema_version == CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION:
                self._validate_v15_activation_schema_tx(conn)
            self._validate_structural_contract(
                conn,
                integrity_check=False,
                allow_known_legacy_binding_guard=(
                    allow_known_legacy_binding_guard
                ),
            )
            self._validate_v12_learning_lane_schema(conn)
            self._validate_v14_terminal_rerun_delivery_authority_schema(
                conn,
                allow_known_legacy_binding_guard=(
                    allow_known_legacy_binding_guard
                ),
            )
            self._validate_historical_epoch_rerun_delivery_authority_schema(conn)
            self._validate_activation_reference_contract_tx(conn)
            self._validate_current_activation_binding_tx(
                conn,
                schema_version=activation_schema_version,
            )
            conn.commit()
        except Exception:
            if conn.in_transaction:
                conn.rollback()
            raise
        finally:
            conn.close()

    def initialization_observation(self) -> dict[str, Any]:
        """Expose bounded startup work without leaking control-plane payloads."""
        return {
            "mode": self._initialization_mode,
            "backfill_runs": self._initialization_backfill_runs,
        }

    def schema_runtime_capability(self) -> dict[str, Any]:
        """Report the exact control-schema mode this binary may use."""

        observed = str(self._observed_schema_version or "")
        read_supported = observed in {
            CONTROL_STORE_SCHEMA_VERSION,
            CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION,
        }
        write_enabled = (
            read_supported
            and observed == self._connection_write_schema_version
            and not self.read_only
        )
        if observed == CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION and self.read_only:
            mode = "successor_read_only"
        elif self.read_only:
            mode = "explicit_read_only"
        else:
            mode = "current_write"
        return {
            "observed_control_schema_version": observed,
            "binary_write_schema_version": self._binary_write_schema_version,
            "mode": mode,
            "read_supported": read_supported,
            "write_enabled": write_enabled,
            "work_admission_enabled": write_enabled,
            "lease_acquisition_enabled": write_enabled,
            "external_effect_enabled": write_enabled,
        }

    @staticmethod
    def _normalize_activation_sha256(value: str, field: str) -> str:
        normalized = str(value or "").strip().lower()
        if _ACTIVATION_SHA256_RE.fullmatch(normalized) is None:
            raise ActivationEpochError(f"activation_{field}_invalid")
        return normalized

    @staticmethod
    def _normalize_activation_epoch_id(value: str) -> str:
        normalized = str(value or "").strip()
        if _ACTIVATION_EPOCH_ID_RE.fullmatch(normalized) is None:
            raise ActivationEpochError("activation_epoch_id_invalid")
        return normalized

    @staticmethod
    def _normalize_activation_db_identity(value: Mapping[str, Any]) -> str:
        if not isinstance(value, Mapping) or not value:
            raise ActivationEpochError("activation_db_logical_identity_invalid")
        canonical = _canonical_json(dict(value))
        if len(canonical.encode("utf-8")) > 4096:
            raise ActivationEpochError("activation_db_logical_identity_too_large")
        return canonical

    @staticmethod
    def _normalize_partition_fence(value: Mapping[str, Any]) -> str:
        if not isinstance(value, Mapping):
            raise ActivationEpochError("activation_partition_fence_invalid")
        if not value:
            return _canonical_json({})
        normalized: dict[str, dict[str, int]] = {}
        for raw_topic, raw_partitions in value.items():
            topic = str(raw_topic or "").strip()
            if not topic or len(topic) > 249 or not isinstance(raw_partitions, Mapping):
                raise ActivationEpochError("activation_partition_fence_invalid")
            partitions: dict[str, int] = {}
            for raw_partition, raw_offset in raw_partitions.items():
                partition_text = str(raw_partition).strip()
                if not partition_text.isdigit():
                    raise ActivationEpochError("activation_partition_fence_invalid")
                partition = int(partition_text)
                if isinstance(raw_offset, bool):
                    raise ActivationEpochError("activation_partition_fence_invalid")
                try:
                    offset = int(raw_offset)
                except (TypeError, ValueError) as exc:
                    raise ActivationEpochError(
                        "activation_partition_fence_invalid"
                    ) from exc
                if partition < 0 or offset < 0:
                    raise ActivationEpochError("activation_partition_fence_invalid")
                key = str(partition)
                if key in partitions:
                    raise ActivationEpochError("activation_partition_fence_duplicate")
                partitions[key] = offset
            if not partitions:
                raise ActivationEpochError("activation_partition_fence_invalid")
            normalized[topic] = partitions
        return _canonical_json(normalized)

    @staticmethod
    def _normalize_activation_source_identity(
        source_kind: str,
        value: Mapping[str, Any],
    ) -> tuple[str, dict[str, Any]]:
        kind = str(source_kind or "").strip()
        if kind not in ACTIVATION_SOURCE_KINDS or not isinstance(value, Mapping):
            raise ActivationEpochError("activation_source_identity_invalid")
        if kind == "kafka":
            event_uid = str(value.get("event_uid") or "").strip()
            if not event_uid or len(event_uid) > 500 or "\n" in event_uid or "\r" in event_uid:
                raise ActivationEpochError("activation_kafka_event_uid_invalid")
            try:
                topic, partition_text, offset_text = event_uid.rsplit(":", 2)
                partition = int(partition_text)
                offset = int(offset_text)
            except (ValueError, TypeError) as exc:
                raise ActivationEpochError("activation_kafka_event_uid_invalid") from exc
            if not topic or partition < 0 or offset < 0:
                raise ActivationEpochError("activation_kafka_event_uid_invalid")
            normalized: dict[str, Any] = {
                "event_uid": event_uid,
                "offset": offset,
                "partition": partition,
                "topic": topic,
            }
        else:
            required = (
                "chat_id",
                "requester_id",
                "message_id",
                "thread_id",
                "issue_url",
                "mode",
            )
            normalized = {
                field: str(value.get(field) or "").strip() for field in required
            }
            if not all(normalized.values()):
                raise ActivationEpochError("activation_manual_identity_incomplete")
            if len(_canonical_json(normalized).encode("utf-8")) > 2048:
                raise ActivationEpochError("activation_manual_identity_too_large")
            if _ISSUE_URL_RE.fullmatch(normalized["issue_url"].rstrip("/")) is None:
                raise ActivationEpochError("activation_manual_issue_url_invalid")
            normalized["issue_url"] = normalized["issue_url"].rstrip("/")
            if normalized["mode"] not in {"run_or_join", "rerun", "debug"}:
                raise ActivationEpochError("activation_manual_mode_invalid")
        canonical = _canonical_json(normalized)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest(), normalized

    @staticmethod
    def _current_activation_epoch_unchecked_tx(
        conn: sqlite3.Connection,
    ) -> sqlite3.Row | None:
        return conn.execute(
            "SELECT * FROM rca_activation_epochs WHERE is_current = 1"
        ).fetchone()

    @classmethod
    def _current_activation_epoch_tx(
        cls, conn: sqlite3.Connection
    ) -> sqlite3.Row | None:
        """Return the current epoch only after its versioned audit validates."""

        row = cls._current_activation_epoch_unchecked_tx(conn)
        if row is not None:
            cls._activation_transition_binding_tx(conn, epoch=row)
        return row

    @staticmethod
    def _v14_compat_release_epoch_projection(
        row: sqlite3.Row,
    ) -> dict[str, str]:
        """Project the physical v14 columns into the neutral release API."""

        return {
            "release_fingerprint_sha256": str(row["production_fingerprint"] or ""),
            "release_note_sha256": str(
                row["production_gate_receipt_sha256"] or ""
            ),
            "partition_end_fence_sha256": str(
                row["partition_end_fence_sha256"] or ""
            ),
        }

    @classmethod
    def _activation_release_epoch_projection(
        cls,
        row: sqlite3.Row,
        *,
        schema_version: str,
    ) -> dict[str, str]:
        if schema_version == CONTROL_STORE_SCHEMA_VERSION:
            return cls._v14_compat_release_epoch_projection(row)
        if schema_version == CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION:
            return {
                "release_fingerprint_sha256": str(
                    row["release_fingerprint_sha256"] or ""
                ),
                "release_note_sha256": str(row["release_note_sha256"] or ""),
            }
        raise RuntimeError("incompatible_control_store_schema:activation_layout")

    @staticmethod
    def _v14_compat_direct_steady_binding_matches(
        row: sqlite3.Row,
        *,
        release_fingerprint_sha256: str,
        release_note_sha256: str,
        config_sha256: str,
        db_logical_identity_json: str,
        db_logical_identity_sha256: str,
        partition_start_fence_json: str,
        partition_start_fence_sha256: str,
    ) -> bool:
        expected = {
            "state": "steady_active",
            "preauthorization_fingerprint": release_fingerprint_sha256,
            "preauthorization_gate_receipt_sha256": release_note_sha256,
            "preauthorization_capsule_sha256": release_note_sha256,
            "preproduction_fingerprint": release_fingerprint_sha256,
            "preproduction_gate_receipt_sha256": release_note_sha256,
            "preproduction_capsule_sha256": release_note_sha256,
            "config_sha256": config_sha256,
            "db_logical_identity_sha256": db_logical_identity_sha256,
            "partition_start_fence_sha256": partition_start_fence_sha256,
            "partition_end_fence_sha256": partition_start_fence_sha256,
            "production_fingerprint": release_fingerprint_sha256,
            "production_gate_receipt_sha256": release_note_sha256,
        }
        return (
            all(str(row[field] or "") == str(value) for field, value in expected.items())
            and str(row["db_logical_identity_json"]) == db_logical_identity_json
            and str(row["partition_start_fence_json"])
            == partition_start_fence_json
            and str(row["partition_end_fence_json"] or "")
            == partition_start_fence_json
        )

    @staticmethod
    def _insert_v14_compat_direct_steady_epoch_tx(
        conn: sqlite3.Connection,
        *,
        epoch_id: str,
        release_fingerprint_sha256: str,
        release_note_sha256: str,
        config_sha256: str,
        db_logical_identity_json: str,
        db_logical_identity_sha256: str,
        partition_start_fence_json: str,
        partition_start_fence_sha256: str,
        created_at: str,
    ) -> None:
        """Write one neutral direct release through the physical v14 columns."""

        conn.execute(
            """
            INSERT INTO rca_activation_epochs(
                epoch_id, state, is_current,
                preauthorization_fingerprint,
                preauthorization_gate_receipt_sha256,
                preauthorization_capsule_sha256,
                preproduction_fingerprint,
                preproduction_gate_receipt_sha256,
                preproduction_capsule_sha256,
                config_sha256, db_logical_identity_json,
                db_logical_identity_sha256, partition_start_fence_json,
                partition_start_fence_sha256, partition_end_fence_json,
                partition_end_fence_sha256, production_fingerprint,
                production_gate_receipt_sha256, created_at, updated_at,
                steady_activated_at
            ) VALUES (?, 'steady_active', 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                epoch_id,
                release_fingerprint_sha256,
                release_note_sha256,
                release_note_sha256,
                release_fingerprint_sha256,
                release_note_sha256,
                release_note_sha256,
                config_sha256,
                db_logical_identity_json,
                db_logical_identity_sha256,
                partition_start_fence_json,
                partition_start_fence_sha256,
                partition_start_fence_json,
                partition_start_fence_sha256,
                release_fingerprint_sha256,
                release_note_sha256,
                created_at,
                created_at,
                created_at,
            ),
        )

    @staticmethod
    def _v15_direct_steady_binding_matches(
        row: sqlite3.Row,
        *,
        release_fingerprint_sha256: str,
        release_note_sha256: str,
        config_sha256: str,
        db_logical_identity_json: str,
        db_logical_identity_sha256: str,
        partition_start_fence_json: str,
        partition_start_fence_sha256: str,
    ) -> bool:
        expected = {
            "state": "steady_active",
            "release_fingerprint_sha256": release_fingerprint_sha256,
            "release_note_sha256": release_note_sha256,
            "config_sha256": config_sha256,
            "db_logical_identity_sha256": db_logical_identity_sha256,
            "partition_start_fence_sha256": partition_start_fence_sha256,
        }
        return (
            int(row["is_current"]) == 1
            and all(
                str(row[field] or "") == str(value)
                for field, value in expected.items()
            )
            and str(row["db_logical_identity_json"]) == db_logical_identity_json
            and str(row["partition_start_fence_json"])
            == partition_start_fence_json
        )

    @staticmethod
    def _insert_v15_direct_steady_epoch_tx(
        conn: sqlite3.Connection,
        *,
        epoch_id: str,
        release_fingerprint_sha256: str,
        release_note_sha256: str,
        config_sha256: str,
        db_logical_identity_json: str,
        db_logical_identity_sha256: str,
        partition_start_fence_json: str,
        partition_start_fence_sha256: str,
        created_at: str,
    ) -> None:
        conn.execute(
            """
            INSERT INTO rca_activation_epochs(
                epoch_id, state, is_current,
                release_fingerprint_sha256, release_note_sha256,
                config_sha256, db_logical_identity_json,
                db_logical_identity_sha256, partition_start_fence_json,
                partition_start_fence_sha256, created_at, updated_at,
                activated_at, retired_at
            ) VALUES (?, 'steady_active', 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
            """,
            (
                epoch_id,
                release_fingerprint_sha256,
                release_note_sha256,
                config_sha256,
                db_logical_identity_json,
                db_logical_identity_sha256,
                partition_start_fence_json,
                partition_start_fence_sha256,
                created_at,
                created_at,
                created_at,
            ),
        )

    @classmethod
    def _public_activation_epoch(
        cls,
        row: sqlite3.Row,
        *,
        schema_version: str | None = None,
    ) -> dict[str, Any]:
        if schema_version is None:
            schema_version = (
                CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION
                if "release_fingerprint_sha256" in row.keys()
                else CONTROL_STORE_SCHEMA_VERSION
            )
        return {
            "epoch_id": str(row["epoch_id"]),
            "state": str(row["state"]),
            **cls._activation_release_epoch_projection(
                row,
                schema_version=schema_version,
            ),
            "config_sha256": str(row["config_sha256"]),
            "db_logical_identity_sha256": str(row["db_logical_identity_sha256"]),
            "partition_start_fence_sha256": str(
                row["partition_start_fence_sha256"]
            ),
            "created_at": str(row["created_at"]),
            "updated_at": str(row["updated_at"]),
        }

    @staticmethod
    def _normalize_activation_audit_text(
        operator: str, reason: str
    ) -> tuple[str, str]:
        actor = str(operator or "").strip()
        justification = str(reason or "").strip()
        if not actor or len(actor) > 200 or "\n" in actor or "\r" in actor:
            raise ActivationEpochError("activation_operator_invalid")
        if not justification or len(justification.encode("utf-8")) > 1000:
            raise ActivationEpochError("activation_reason_invalid")
        return actor, justification

    @classmethod
    def _normalize_direct_steady_activation_contract(
        cls,
        *,
        epoch_id: str,
        release_fingerprint_sha256: str,
        release_note_sha256: str,
        config_sha256: str,
        db_logical_identity: Mapping[str, Any],
        partition_start_fence: Mapping[str, Any],
        expected_predecessor_epoch_id: str,
        expected_predecessor_state: str,
        expected_predecessor_binding_fingerprint: str,
        expected_control_schema_version: str,
        target_control_schema_version: str,
        epoch_contract_sha256: str,
    ) -> dict[str, Any]:
        identity = cls._normalize_activation_epoch_id(epoch_id)
        release_hash = cls._normalize_activation_sha256(
            release_fingerprint_sha256,
            "release_fingerprint_sha256",
        )
        release_note_hash = cls._normalize_activation_sha256(
            release_note_sha256,
            "release_note_sha256",
        )
        config_hash = cls._normalize_activation_sha256(
            config_sha256,
            "config_sha256",
        )
        db_identity_json = cls._normalize_activation_db_identity(db_logical_identity)
        db_identity = json.loads(db_identity_json)
        db_identity_sha = hashlib.sha256(db_identity_json.encode("utf-8")).hexdigest()
        fence_json = cls._normalize_partition_fence(partition_start_fence)
        fence = json.loads(fence_json)
        fence_sha = hashlib.sha256(fence_json.encode("utf-8")).hexdigest()
        predecessor = (
            str(expected_predecessor_epoch_id or "").strip(),
            str(expected_predecessor_state or "").strip(),
            str(expected_predecessor_binding_fingerprint or "").strip().lower(),
        )
        if any(predecessor) and not all(predecessor):
            raise ActivationEpochError("activation_predecessor_binding_incomplete")
        predecessor_id, predecessor_state, predecessor_fingerprint = predecessor
        if predecessor_id:
            predecessor_id = cls._normalize_activation_epoch_id(predecessor_id)
            if predecessor_state not in {"aborted", "steady_active"}:
                raise ActivationEpochError("activation_predecessor_state_invalid")
            predecessor_fingerprint = cls._normalize_activation_sha256(
                predecessor_fingerprint,
                "predecessor_binding_fingerprint",
            )
        schema_pair = (
            str(expected_control_schema_version or "").strip(),
            str(target_control_schema_version or "").strip(),
        )
        if schema_pair not in {
            (
                CONTROL_STORE_SCHEMA_VERSION,
                CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION,
            ),
            (
                CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION,
                CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION,
            ),
        }:
            raise ActivationEpochError("activation_control_schema_transition_invalid")
        contract_hash = cls._normalize_activation_sha256(
            epoch_contract_sha256,
            "epoch_contract_sha256",
        )
        if any(
            value == "0" * 64
            for value in (
                release_hash,
                release_note_hash,
                config_hash,
                db_identity_sha,
                fence_sha,
                contract_hash,
            )
        ):
            raise ActivationEpochError("activation_v15_required_digest_invalid")
        contract_material = {
            "schema_version": _MINIMAL_RELEASE_EPOCH_CONTRACT_SCHEMA_VERSION,
            "expected_control_schema_version": schema_pair[0],
            "target_control_schema_version": schema_pair[1],
            "expected_predecessor_epoch_id": predecessor_id,
            "expected_predecessor_state": predecessor_state,
            "expected_predecessor_binding_fingerprint": predecessor_fingerprint,
            "db_logical_identity": db_identity,
            "db_logical_identity_sha256": db_identity_sha,
            "partition_start_fence": fence,
            "partition_start_fence_sha256": fence_sha,
        }
        if _canonical_sha256(contract_material) != contract_hash:
            raise ActivationEpochError("activation_epoch_contract_invalid")
        return {
            "epoch_id": identity,
            "release_fingerprint_sha256": release_hash,
            "release_note_sha256": release_note_hash,
            "config_sha256": config_hash,
            "db_logical_identity": db_identity,
            "db_logical_identity_json": db_identity_json,
            "db_logical_identity_sha256": db_identity_sha,
            "partition_start_fence": fence,
            "partition_start_fence_json": fence_json,
            "partition_start_fence_sha256": fence_sha,
            "expected_predecessor_epoch_id": predecessor_id,
            "expected_predecessor_state": predecessor_state,
            "expected_predecessor_binding_fingerprint": predecessor_fingerprint,
            "expected_control_schema_version": schema_pair[0],
            "target_control_schema_version": schema_pair[1],
            "epoch_contract_sha256": contract_hash,
        }

    @staticmethod
    def _validate_v14_parent_for_v15_projection_tx(
        conn: sqlite3.Connection,
        epoch: sqlite3.Row,
    ) -> tuple[str | None, str | None]:
        del conn
        try:
            db_identity = json.loads(str(epoch["db_logical_identity_json"]))
            start_fence = json.loads(str(epoch["partition_start_fence_json"]))
            end_raw = epoch["partition_end_fence_json"]
            end_fence = json.loads(str(end_raw)) if end_raw is not None else None
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ActivationEpochError("activation_v15_parent_projection_invalid") from exc
        release_fingerprint = epoch["production_fingerprint"]
        release_note = epoch["production_gate_receipt_sha256"]
        release_pair_null = release_fingerprint is None and release_note is None
        release_pair_valid = all(
            isinstance(value, str)
            and _ACTIVATION_SHA256_RE.fullmatch(value) is not None
            and value != "0" * 64
            for value in (release_fingerprint, release_note)
        )
        required_digests = (
            "config_sha256",
            "db_logical_identity_sha256",
            "partition_start_fence_sha256",
        )
        end_sha = epoch["partition_end_fence_sha256"]
        if (
            _ACTIVATION_EPOCH_ID_RE.fullmatch(str(epoch["epoch_id"] or "")) is None
            or any(
                _ACTIVATION_SHA256_RE.fullmatch(str(epoch[field] or "")) is None
                or str(epoch[field] or "") == "0" * 64
                for field in required_digests
            )
            or _canonical_json(db_identity) != str(epoch["db_logical_identity_json"])
            or _canonical_sha256(db_identity)
            != str(epoch["db_logical_identity_sha256"])
            or _canonical_json(start_fence) != str(epoch["partition_start_fence_json"])
            or _canonical_sha256(start_fence)
            != str(epoch["partition_start_fence_sha256"])
            or (end_fence is None) != (end_sha is None)
            or (
                end_fence is not None
                and (
                    _canonical_json(end_fence) != str(end_raw)
                    or _canonical_sha256(end_fence) != str(end_sha)
                )
            )
            or not (release_pair_null or release_pair_valid)
        ):
            raise ActivationEpochError("activation_v15_parent_projection_invalid")
        return (
            str(release_fingerprint) if release_fingerprint is not None else None,
            str(release_note) if release_note is not None else None,
        )

    @staticmethod
    def _v15_migration_fault(_stage: str) -> None:
        """No-op fault seam used only by transaction rollback tests."""

    @staticmethod
    def _commit_v15_migration_tx(conn: sqlite3.Connection) -> None:
        conn.commit()

    @staticmethod
    def _v14_compat_activation_slot_bindings_tx(
        conn: sqlite3.Connection,
        *,
        epoch_id: str,
    ) -> list[dict[str, Any]]:
        table = conn.execute(
            "SELECT 1 FROM sqlite_master "
            "WHERE type = 'table' AND name = 'rca_activation_budget_slots'"
        ).fetchone()
        if table is None:
            return []
        required_columns = {
            "epoch_id",
            "slot_kind",
            "authorized_source_kind",
            "authorized_identity_sha256",
            "authorized_operator",
            "authorized_reason",
            "consumed_ledger_id",
        }
        observed_columns = {
            str(row["name"])
            for row in conn.execute(
                "PRAGMA table_info(rca_activation_budget_slots)"
            ).fetchall()
        }
        if not required_columns.issubset(observed_columns):
            return []
        return [
            {
                "authorized_identity_sha256": str(
                    row["authorized_identity_sha256"] or ""
                ),
                "authorized_operator": str(row["authorized_operator"] or ""),
                "authorized_reason": str(row["authorized_reason"] or ""),
                "authorized_source_kind": str(
                    row["authorized_source_kind"] or ""
                ),
                "consumed_ledger_id": int(row["consumed_ledger_id"] or 0),
                "slot_kind": str(row["slot_kind"]),
            }
            for row in conn.execute(
                "SELECT slot_kind, authorized_source_kind, "
                "authorized_identity_sha256, authorized_operator, "
                "authorized_reason, consumed_ledger_id "
                "FROM rca_activation_budget_slots "
                "WHERE epoch_id = ? ORDER BY slot_kind",
                (epoch_id,),
            ).fetchall()
        ]

    @classmethod
    def _v14_compat_activation_transition_binding_material_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        epoch: sqlite3.Row,
        from_state: str,
        to_state: str,
    ) -> dict[str, Any]:
        return {
            "config_sha256": str(epoch["config_sha256"]),
            "db_logical_identity_sha256": str(
                epoch["db_logical_identity_sha256"]
            ),
            "epoch_id": str(epoch["epoch_id"]),
            "from_state": from_state,
            "partition_end_fence_sha256": str(
                epoch["partition_end_fence_sha256"] or ""
            ),
            "partition_start_fence_sha256": str(
                epoch["partition_start_fence_sha256"]
            ),
            "preauthorization_capsule_sha256": str(
                epoch["preauthorization_capsule_sha256"]
            ),
            "preauthorization_fingerprint": str(
                epoch["preauthorization_fingerprint"]
            ),
            "preauthorization_gate_receipt_sha256": str(
                epoch["preauthorization_gate_receipt_sha256"]
            ),
            "preproduction_capsule_sha256": str(
                epoch["preproduction_capsule_sha256"] or ""
            ),
            "preproduction_fingerprint": str(
                epoch["preproduction_fingerprint"] or ""
            ),
            "preproduction_gate_receipt_sha256": str(
                epoch["preproduction_gate_receipt_sha256"] or ""
            ),
            "production_fingerprint": str(
                epoch["production_fingerprint"] or ""
            ),
            "production_gate_receipt_sha256": str(
                epoch["production_gate_receipt_sha256"] or ""
            ),
            "slot_bindings_sha256": _canonical_sha256(
                cls._v14_compat_activation_slot_bindings_tx(
                    conn,
                    epoch_id=str(epoch["epoch_id"]),
                )
            ),
            "to_state": to_state,
        }

    @classmethod
    def _v14_compat_activation_transition_binding_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        epoch: sqlite3.Row,
    ) -> dict[str, Any]:
        """Validate the latest v14 transition audit against its physical row."""

        audit = conn.execute(
            """
            SELECT audit_id, from_state, to_state, binding_fingerprint,
                   transitioned_at
              FROM rca_activation_transition_audit
             WHERE epoch_id = ?
          ORDER BY audit_id DESC
             LIMIT 1
            """,
            (epoch["epoch_id"],),
        ).fetchone()
        state = str(epoch["state"] or "")
        if audit is None or str(audit["to_state"] or "") != state:
            raise ActivationEpochError("activation_predecessor_binding_invalid")
        observed = str(audit["binding_fingerprint"] or "").lower()
        expected = _canonical_sha256(
            cls._v14_compat_activation_transition_binding_material_tx(
                conn,
                epoch=epoch,
                from_state=str(audit["from_state"] or ""),
                to_state=state,
            )
        )
        if observed != expected:
            raise ActivationEpochError("activation_predecessor_binding_invalid")
        return {
            "audit_id": int(audit["audit_id"]),
            "binding_fingerprint": observed,
            "epoch_id": str(epoch["epoch_id"]),
            "state": state,
            "transitioned_at": str(audit["transitioned_at"] or ""),
        }

    @staticmethod
    def _v15_activation_transition_binding_material_tx(
        conn: sqlite3.Connection,
        *,
        epoch: sqlite3.Row,
        from_state: str,
        to_state: str,
    ) -> dict[str, Any]:
        del conn
        release_fingerprint = epoch["release_fingerprint_sha256"]
        release_note = epoch["release_note_sha256"]
        return {
            "binding_schema_version": ACTIVATION_TRANSITION_BINDING_SCHEMA_V15,
            "config_sha256": str(epoch["config_sha256"]),
            "db_logical_identity_sha256": str(epoch["db_logical_identity_sha256"]),
            "epoch_id": str(epoch["epoch_id"]),
            "from_state": from_state,
            "partition_start_fence_sha256": str(epoch["partition_start_fence_sha256"]),
            "release_fingerprint_sha256": (
                str(release_fingerprint)
                if release_fingerprint is not None
                else None
            ),
            "release_note_sha256": (
                str(release_note) if release_note is not None else None
            ),
            "to_state": to_state,
        }

    @classmethod
    def _v15_activation_transition_binding_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        epoch: sqlite3.Row,
    ) -> dict[str, Any]:
        """Validate the latest v15 audit and its neutral release material."""

        state = str(epoch["state"] or "")
        expected_current = state == "steady_active"
        try:
            db_identity = json.loads(str(epoch["db_logical_identity_json"]))
            partition_fence = json.loads(str(epoch["partition_start_fence_json"]))
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ActivationEpochError(
                "activation_predecessor_binding_invalid"
            ) from exc
        release_fingerprint = epoch["release_fingerprint_sha256"]
        release_note = epoch["release_note_sha256"]
        release_pair_is_null = (
            release_fingerprint is None and release_note is None
        )
        release_pair_is_valid = all(
            isinstance(value, str)
            and _ACTIVATION_SHA256_RE.fullmatch(value) is not None
            and value != "0" * 64
            for value in (release_fingerprint, release_note)
        )
        digest_fields = (
            "config_sha256",
            "db_logical_identity_sha256",
            "partition_start_fence_sha256",
        )
        if (
            state not in {"steady_active", "retired"}
            or bool(int(epoch["is_current"])) != expected_current
            or (
                state == "steady_active" and not release_pair_is_valid
            )
            or (
                state == "retired"
                and not (release_pair_is_null or release_pair_is_valid)
            )
            or any(
                _ACTIVATION_SHA256_RE.fullmatch(str(epoch[field] or "")) is None
                or str(epoch[field] or "") == "0" * 64
                for field in digest_fields
            )
            or _canonical_json(db_identity) != str(epoch["db_logical_identity_json"])
            or _canonical_sha256(db_identity)
            != str(epoch["db_logical_identity_sha256"])
            or _canonical_json(partition_fence)
            != str(epoch["partition_start_fence_json"])
            or _canonical_sha256(partition_fence)
            != str(epoch["partition_start_fence_sha256"])
        ):
            raise ActivationEpochError("activation_predecessor_binding_invalid")
        audit = conn.execute(
            """
            SELECT audit_id, from_state, to_state, binding_schema_version,
                   binding_fingerprint, transitioned_at
              FROM rca_activation_transition_audit
             WHERE epoch_id = ?
          ORDER BY audit_id DESC
             LIMIT 1
            """,
            (epoch["epoch_id"],),
        ).fetchone()
        if (
            audit is None
            or str(audit["to_state"] or "") != state
            or str(audit["binding_schema_version"] or "")
            != ACTIVATION_TRANSITION_BINDING_SCHEMA_V15
        ):
            raise ActivationEpochError("activation_predecessor_binding_invalid")
        observed = str(audit["binding_fingerprint"] or "").lower()
        expected = _canonical_sha256(
            cls._v15_activation_transition_binding_material_tx(
                conn,
                epoch=epoch,
                from_state=str(audit["from_state"] or ""),
                to_state=state,
            )
        )
        if observed != expected:
            raise ActivationEpochError("activation_predecessor_binding_invalid")
        return {
            "audit_id": int(audit["audit_id"]),
            "binding_fingerprint": observed,
            "binding_schema_version": ACTIVATION_TRANSITION_BINDING_SCHEMA_V15,
            "epoch_id": str(epoch["epoch_id"]),
            "state": state,
            "transitioned_at": str(audit["transitioned_at"] or ""),
        }

    @classmethod
    def _activation_transition_binding_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        epoch: sqlite3.Row,
        schema_version: str | None = None,
    ) -> dict[str, Any]:
        observed_schema_version = schema_version or cls._activation_schema_version_tx(
            conn
        )
        if observed_schema_version == CONTROL_STORE_SCHEMA_VERSION:
            return cls._v14_compat_activation_transition_binding_tx(
                conn,
                epoch=epoch,
            )
        if observed_schema_version == CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION:
            return cls._v15_activation_transition_binding_tx(conn, epoch=epoch)
        raise RuntimeError("incompatible_control_store_schema:activation_layout")

    @classmethod
    def _insert_activation_transition_audit_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        epoch: sqlite3.Row,
        from_state: str,
        to_state: str,
        operator: str,
        reason: str,
        transitioned_at: str,
        schema_version: str | None = None,
    ) -> int:
        resolved_schema_version = schema_version or (
            CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION
            if "release_fingerprint_sha256" in epoch.keys()
            else CONTROL_STORE_SCHEMA_VERSION
        )
        if resolved_schema_version == CONTROL_STORE_SCHEMA_VERSION:
            material = cls._v14_compat_activation_transition_binding_material_tx(
                conn,
                epoch=epoch,
                from_state=from_state,
                to_state=to_state,
            )
        elif resolved_schema_version == CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION:
            material = cls._v15_activation_transition_binding_material_tx(
                conn,
                epoch=epoch,
                from_state=from_state,
                to_state=to_state,
            )
        else:
            raise RuntimeError("incompatible_control_store_schema:activation_layout")
        binding_fingerprint = _canonical_sha256(material)
        columns = (
            "epoch_id, from_state, to_state, operator, reason, "
            "binding_fingerprint, transitioned_at"
        )
        placeholders = "?, ?, ?, ?, ?, ?, ?"
        parameters: tuple[Any, ...] = (
            epoch["epoch_id"],
            from_state,
            to_state,
            operator,
            reason,
            binding_fingerprint,
            transitioned_at,
        )
        if resolved_schema_version == CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION:
            columns += ", binding_schema_version"
            placeholders += ", ?"
            parameters += (ACTIVATION_TRANSITION_BINDING_SCHEMA_V15,)
        cursor = conn.execute(
            f"INSERT INTO rca_activation_transition_audit({columns}) "
            f"VALUES ({placeholders})",
            parameters,
        )
        if cursor.lastrowid is None:
            raise ActivationEpochError("activation_transition_audit_failed")
        return int(cursor.lastrowid)

    @classmethod
    def _direct_steady_current_inflight_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        epoch_id: str,
    ) -> dict[str, int]:
        scope_sql = """
            activation_epoch_id = ? OR activation_ledger_id IN (
                SELECT ledger_id FROM rca_activation_admission_ledger
                 WHERE epoch_id = ?
            )
        """
        pending_inbox = int(
            conn.execute(
                """
                SELECT COUNT(*) FROM kafka_inbox
                 WHERE decision = 'pending' AND activation_epoch_id = ?
                """,
                (epoch_id,),
            ).fetchone()[0]
        )
        dispatchable_outbox = int(
            conn.execute(
                f"""
                SELECT COUNT(*) FROM rca_outbox
                 WHERE status IN ('pending', 'claimed')
                   AND ({scope_sql})
                """,
                (epoch_id, epoch_id),
            ).fetchone()[0]
        )
        delivery_rows = conn.execute(
            f"""
            SELECT business_key, submission_key, generation
              FROM rca_outbox
             WHERE status IN ('completed', 'quarantined')
               AND ({scope_sql})
            """,
            (epoch_id, epoch_id),
        ).fetchall()
        execution_delivery = sum(
            not cls._activation_delivery_execution_complete_tx(
                conn,
                business_key=str(row["business_key"] or ""),
                submission_key=str(row["submission_key"] or ""),
                generation=int(row["generation"] or 0),
            )
            for row in delivery_rows
        )
        return {
            "dispatchable_outbox": dispatchable_outbox,
            "execution_delivery": execution_delivery,
            "pending_inbox": pending_inbox,
            "total": pending_inbox + dispatchable_outbox + execution_delivery,
        }

    @classmethod
    def _direct_steady_activation_outcome_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        contract: Mapping[str, Any],
    ) -> tuple[Literal["not_committed", "committed", "unknown"], sqlite3.Row | None]:
        try:
            if conn.execute(
                "SELECT 1 FROM sqlite_master "
                "WHERE name = 'rca_activation_epochs_v15_new'"
            ).fetchone() is not None:
                return "unknown", None
            observed = cls._activation_schema_version_tx(conn)
            cls._validate_structural_contract(
                conn,
                integrity_check=True,
                allow_known_legacy_binding_guard=(
                    observed == CONTROL_STORE_SCHEMA_VERSION
                ),
            )
            cls._validate_activation_reference_contract_tx(conn)
            cls._validate_activation_child_rows_tx(conn)
            if observed == CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION:
                cls._validate_v15_activation_schema_tx(conn)
        except (ActivationEpochError, RuntimeError, sqlite3.Error):
            return "unknown", None
        if observed not in {
            str(contract["expected_control_schema_version"]),
            str(contract["target_control_schema_version"]),
        }:
            return "unknown", None
        if observed == CONTROL_STORE_SCHEMA_VERSION:
            try:
                for parent in conn.execute(
                    "SELECT * FROM rca_activation_epochs ORDER BY epoch_id"
                ).fetchall():
                    cls._validate_v14_parent_for_v15_projection_tx(conn, parent)
                    cls._v14_compat_activation_transition_binding_tx(
                        conn,
                        epoch=parent,
                    )
            except (ActivationEpochError, RuntimeError, sqlite3.Error):
                return "unknown", None
        current = cls._current_activation_epoch_unchecked_tx(conn)
        if current is not None and str(current["epoch_id"]) == contract["epoch_id"]:
            if observed != contract["target_control_schema_version"]:
                return "unknown", None
            try:
                binding = cls._activation_transition_binding_tx(
                    conn,
                    epoch=current,
                    schema_version=observed,
                )
            except (ActivationEpochError, RuntimeError, sqlite3.Error):
                return "unknown", None
            matches = (
                cls._v15_direct_steady_binding_matches(
                    current,
                    release_fingerprint_sha256=str(
                        contract["release_fingerprint_sha256"]
                    ),
                    release_note_sha256=str(contract["release_note_sha256"]),
                    config_sha256=str(contract["config_sha256"]),
                    db_logical_identity_json=str(
                        contract["db_logical_identity_json"]
                    ),
                    db_logical_identity_sha256=str(
                        contract["db_logical_identity_sha256"]
                    ),
                    partition_start_fence_json=str(
                        contract["partition_start_fence_json"]
                    ),
                    partition_start_fence_sha256=str(
                        contract["partition_start_fence_sha256"]
                    ),
                )
                and binding["state"] == "steady_active"
                and binding["binding_schema_version"]
                == ACTIVATION_TRANSITION_BINDING_SCHEMA_V15
            )
            if not matches:
                return "unknown", None
            predecessor_id = str(contract["expected_predecessor_epoch_id"])
            if predecessor_id:
                predecessor_binding_schema = (
                    ACTIVATION_TRANSITION_BINDING_SCHEMA_V14
                    if contract["expected_control_schema_version"]
                    == CONTROL_STORE_SCHEMA_VERSION
                    else ACTIVATION_TRANSITION_BINDING_SCHEMA_V15
                )
                predecessor = conn.execute(
                    "SELECT state, is_current FROM rca_activation_epochs "
                    "WHERE epoch_id = ?",
                    (predecessor_id,),
                ).fetchone()
                predecessor_audit = conn.execute(
                    "SELECT binding_fingerprint, binding_schema_version "
                    "FROM rca_activation_transition_audit "
                    "WHERE epoch_id = ? AND binding_schema_version = ? "
                    "AND binding_fingerprint = ? "
                    "ORDER BY audit_id DESC LIMIT 1",
                    (
                        predecessor_id,
                        predecessor_binding_schema,
                        contract["expected_predecessor_binding_fingerprint"],
                    ),
                ).fetchone()
                if (
                    predecessor is None
                    or str(predecessor["state"]) != "retired"
                    or int(predecessor["is_current"]) != 0
                    or predecessor_audit is None
                ):
                    return "unknown", None
                if predecessor_binding_schema == ACTIVATION_TRANSITION_BINDING_SCHEMA_V15:
                    predecessor_row = conn.execute(
                        "SELECT * FROM rca_activation_epochs WHERE epoch_id = ?",
                        (predecessor_id,),
                    ).fetchone()
                    try:
                        if predecessor_row is None:
                            return "unknown", None
                        cls._v15_activation_transition_binding_tx(
                            conn,
                            epoch=predecessor_row,
                        )
                    except (ActivationEpochError, RuntimeError, sqlite3.Error):
                        return "unknown", None
            return "committed", current

        predecessor_id = str(contract["expected_predecessor_epoch_id"])
        if (
            observed == contract["expected_control_schema_version"]
            and current is None
            and not predecessor_id
            and conn.execute(
                "SELECT 1 FROM rca_activation_epochs WHERE epoch_id = ?",
                (contract["epoch_id"],),
            ).fetchone()
            is None
        ):
            try:
                cls._validate_partition_start_fence_cas_tx(
                    conn,
                    partition_start_fence=contract["partition_start_fence"],
                )
            except (ActivationEpochError, RuntimeError, sqlite3.Error):
                return "unknown", None
            return "not_committed", None
        if (
            observed != contract["expected_control_schema_version"]
            or current is None
            or not predecessor_id
            or str(current["epoch_id"]) != predecessor_id
            or str(current["state"] or "")
            != str(contract["expected_predecessor_state"])
        ):
            return "unknown", None
        try:
            binding = cls._activation_transition_binding_tx(
                conn,
                epoch=current,
                schema_version=observed,
            )
            cls._validate_partition_start_fence_cas_tx(
                conn,
                partition_start_fence=contract["partition_start_fence"],
            )
            inflight = cls._direct_steady_current_inflight_tx(
                conn,
                epoch_id=predecessor_id,
            )
        except (ActivationEpochError, RuntimeError, sqlite3.Error):
            return "unknown", None
        if (
            binding["binding_fingerprint"]
            != contract["expected_predecessor_binding_fingerprint"]
            or inflight["total"] != 0
            or conn.execute(
                "SELECT 1 FROM rca_activation_epochs WHERE epoch_id = ?",
                (contract["epoch_id"],),
            ).fetchone()
            is not None
            or conn.execute(
                "SELECT 1 FROM sqlite_master "
                "WHERE name = 'rca_activation_epochs_v15_new'"
            ).fetchone()
            is not None
        ):
            return "unknown", None
        return "not_committed", None

    @classmethod
    def _probe_direct_steady_activation_contract(
        cls,
        db_path: str | Path,
        *,
        contract: Mapping[str, Any],
        busy_timeout_ms: int,
    ) -> tuple[Literal["not_committed", "committed", "unknown"], dict[str, Any] | None]:
        path = Path(db_path).expanduser()
        if not path.is_absolute():
            return "unknown", None
        conn: sqlite3.Connection | None = None
        try:
            conn = sqlite3.connect(
                f"{path.resolve().as_uri()}?mode=ro",
                uri=True,
                timeout=max(1, int(busy_timeout_ms)) / 1000,
                isolation_level=None,
            )
            conn.row_factory = sqlite3.Row
            conn.execute(f"PRAGMA busy_timeout={max(1, int(busy_timeout_ms))}")
            conn.execute("PRAGMA foreign_keys=ON")
            foreign_keys = conn.execute("PRAGMA foreign_keys").fetchone()
            if foreign_keys is None or int(foreign_keys[0]) != 1:
                return "unknown", None
            conn.execute("PRAGMA query_only=ON")
            conn.execute("PRAGMA recursive_triggers=ON")
            conn.execute("BEGIN")
            outcome, row = cls._direct_steady_activation_outcome_tx(
                conn,
                contract=contract,
            )
            public = (
                cls._public_activation_epoch(
                    row,
                    schema_version=CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION,
                )
                if outcome == "committed" and row is not None
                else None
            )
            conn.commit()
            return outcome, public
        except (OSError, RuntimeError, sqlite3.Error, ValueError):
            if conn is not None and conn.in_transaction:
                conn.rollback()
            return "unknown", None
        finally:
            if conn is not None:
                conn.close()

    @classmethod
    def probe_direct_steady_activation_outcome(
        cls,
        db_path: str | Path,
        *,
        epoch_id: str,
        release_fingerprint_sha256: str,
        release_note_sha256: str,
        config_sha256: str,
        db_logical_identity: Mapping[str, Any],
        partition_start_fence: Mapping[str, Any],
        expected_predecessor_epoch_id: str,
        expected_predecessor_state: str,
        expected_predecessor_binding_fingerprint: str,
        expected_control_schema_version: str,
        target_control_schema_version: str,
        epoch_contract_sha256: str,
        busy_timeout_ms: int = 5000,
    ) -> Literal["not_committed", "committed", "unknown"]:
        """Classify one exact activation without trusting the caller's connection."""

        try:
            contract = cls._normalize_direct_steady_activation_contract(
                epoch_id=epoch_id,
                release_fingerprint_sha256=release_fingerprint_sha256,
                release_note_sha256=release_note_sha256,
                config_sha256=config_sha256,
                db_logical_identity=db_logical_identity,
                partition_start_fence=partition_start_fence,
                expected_predecessor_epoch_id=expected_predecessor_epoch_id,
                expected_predecessor_state=expected_predecessor_state,
                expected_predecessor_binding_fingerprint=(
                    expected_predecessor_binding_fingerprint
                ),
                expected_control_schema_version=expected_control_schema_version,
                target_control_schema_version=target_control_schema_version,
                epoch_contract_sha256=epoch_contract_sha256,
            )
        except (ActivationEpochError, TypeError, ValueError):
            return "unknown"
        outcome, _public = cls._probe_direct_steady_activation_contract(
            db_path,
            contract=contract,
            busy_timeout_ms=busy_timeout_ms,
        )
        return outcome

    @classmethod
    def probe_v14_to_v15_migration_outcome(
        cls,
        db_path: str | Path,
        **kwargs: Any,
    ) -> Literal["not_committed", "committed", "unknown"]:
        """Classify only the explicit v14-to-v15 migration contract."""

        if (
            kwargs.get("expected_control_schema_version")
            != CONTROL_STORE_SCHEMA_VERSION
            or kwargs.get("target_control_schema_version")
            != CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION
        ):
            return "unknown"
        return cls.probe_direct_steady_activation_outcome(db_path, **kwargs)

    @classmethod
    def migrate_v14_to_v15_and_activate(
        cls,
        db_path: str | Path,
        *,
        epoch_id: str,
        release_fingerprint_sha256: str,
        release_note_sha256: str,
        config_sha256: str,
        db_logical_identity: Mapping[str, Any],
        partition_start_fence: Mapping[str, Any],
        operator: str,
        reason: str,
        expected_predecessor_epoch_id: str,
        expected_predecessor_state: str,
        expected_predecessor_binding_fingerprint: str,
        expected_control_schema_version: str,
        target_control_schema_version: str,
        epoch_contract_sha256: str,
        busy_timeout_ms: int = 5000,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        """Atomically rebuild exact v14 activation state as writable v15."""

        contract = cls._normalize_direct_steady_activation_contract(
            epoch_id=epoch_id,
            release_fingerprint_sha256=release_fingerprint_sha256,
            release_note_sha256=release_note_sha256,
            config_sha256=config_sha256,
            db_logical_identity=db_logical_identity,
            partition_start_fence=partition_start_fence,
            expected_predecessor_epoch_id=expected_predecessor_epoch_id,
            expected_predecessor_state=expected_predecessor_state,
            expected_predecessor_binding_fingerprint=(
                expected_predecessor_binding_fingerprint
            ),
            expected_control_schema_version=expected_control_schema_version,
            target_control_schema_version=target_control_schema_version,
            epoch_contract_sha256=epoch_contract_sha256,
        )
        if (
            contract["expected_control_schema_version"]
            != CONTROL_STORE_SCHEMA_VERSION
            or contract["target_control_schema_version"]
            != CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION
        ):
            raise ActivationEpochError("activation_control_schema_transition_invalid")
        if (
            not contract["expected_predecessor_epoch_id"]
            or contract["expected_predecessor_state"] != "steady_active"
            or contract["epoch_id"] == contract["expected_predecessor_epoch_id"]
        ):
            raise ActivationEpochError("activation_predecessor_binding_incomplete")
        actor, justification = cls._normalize_activation_audit_text(operator, reason)
        path = Path(db_path).expanduser()
        if not path.is_absolute():
            raise RuntimeError("rca_control_store_existing_path_not_absolute")
        try:
            observed_path = path.lstat()
        except OSError as exc:
            raise RuntimeError("rca_control_store_existing_path_missing") from exc
        if (
            stat.S_ISLNK(observed_path.st_mode)
            or not stat.S_ISREG(observed_path.st_mode)
            or observed_path.st_nlink != 1
            or observed_path.st_size <= 0
        ):
            raise RuntimeError("rca_control_store_existing_path_invalid")

        observed_version, snapshot = cls.probe_writable_schema_source(path)
        if snapshot is not None:
            snapshot.close()
        if observed_version == CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION:
            outcome, public = cls._probe_direct_steady_activation_contract(
                path,
                contract=contract,
                busy_timeout_ms=busy_timeout_ms,
            )
            if outcome == "committed" and public is not None:
                return public
            raise ActivationEpochError("activation_v15_migration_binding_conflict")
        if observed_version != CONTROL_STORE_SCHEMA_VERSION:
            raise RuntimeError("incompatible_control_store_schema:version")

        current = _iso(now)
        conn: sqlite3.Connection | None = None
        commit_started = False
        commit_error: Exception | None = None
        try:
            conn = sqlite3.connect(
                path,
                timeout=max(1, int(busy_timeout_ms)) / 1000,
                isolation_level=None,
            )
            conn.row_factory = sqlite3.Row
            conn.execute(f"PRAGMA busy_timeout={max(1, int(busy_timeout_ms))}")
            conn.execute("PRAGMA recursive_triggers=ON")
            conn.execute("PRAGMA foreign_keys=OFF")
            foreign_keys = conn.execute("PRAGMA foreign_keys").fetchone()
            if foreign_keys is None or int(foreign_keys[0]) != 0:
                raise RuntimeError("activation_v15_migration_foreign_keys_not_disabled")
            conn.execute("BEGIN IMMEDIATE")
            if cls._activation_schema_version_tx(conn) != CONTROL_STORE_SCHEMA_VERSION:
                raise RuntimeError("incompatible_control_store_schema:activation_layout")
            cls._validate_structural_contract(
                conn,
                integrity_check=True,
                allow_known_legacy_binding_guard=True,
            )
            cls._validate_activation_reference_contract_tx(conn)
            cls._validate_activation_child_rows_tx(conn)

            old_epochs = conn.execute(
                "SELECT * FROM rca_activation_epochs ORDER BY epoch_id"
            ).fetchall()
            old_audits = [
                tuple(row)
                for row in conn.execute(
                    "SELECT audit_id, epoch_id, from_state, to_state, operator, "
                    "reason, binding_fingerprint, transitioned_at "
                    "FROM rca_activation_transition_audit ORDER BY audit_id"
                ).fetchall()
            ]
            projected: list[tuple[sqlite3.Row, str | None, str | None]] = []
            for old_epoch in old_epochs:
                release_pair = cls._validate_v14_parent_for_v15_projection_tx(
                    conn,
                    old_epoch,
                )
                cls._v14_compat_activation_transition_binding_tx(
                    conn,
                    epoch=old_epoch,
                )
                projected.append((old_epoch, *release_pair))
            predecessor = cls._current_activation_epoch_unchecked_tx(conn)
            if predecessor is None:
                raise ActivationEpochError("activation_predecessor_binding_changed")
            predecessor_binding = cls._v14_compat_activation_transition_binding_tx(
                conn,
                epoch=predecessor,
            )
            if (
                str(predecessor["epoch_id"])
                != contract["expected_predecessor_epoch_id"]
                or str(predecessor["state"])
                != contract["expected_predecessor_state"]
                or predecessor_binding["binding_fingerprint"]
                != contract["expected_predecessor_binding_fingerprint"]
            ):
                raise ActivationEpochError("activation_predecessor_binding_changed")
            if conn.execute(
                "SELECT 1 FROM rca_activation_epochs WHERE epoch_id = ?",
                (contract["epoch_id"],),
            ).fetchone() is not None:
                raise ActivationEpochError("activation_direct_steady_binding_conflict")
            inflight = cls._direct_steady_current_inflight_tx(
                conn,
                epoch_id=str(predecessor["epoch_id"]),
            )
            if inflight["total"]:
                raise ActivationEpochError(
                    "activation_predecessor_inflight_not_drained"
                )
            cls._validate_partition_start_fence_cas_tx(
                conn,
                partition_start_fence=contract["partition_start_fence"],
            )
            trigger_sql = {
                str(row["name"]): str(row["sql"] or "")
                for row in conn.execute(
                    "SELECT name, sql FROM sqlite_master "
                    "WHERE type = 'trigger' AND name IN (?, ?, ?)",
                    _V15_ACTIVATION_CROSS_TRIGGER_NAMES,
                ).fetchall()
            }
            if set(trigger_sql) != set(_V15_ACTIVATION_CROSS_TRIGGER_NAMES) or any(
                not trigger_sql[name].strip()
                for name in _V15_ACTIVATION_CROSS_TRIGGER_NAMES
            ):
                raise RuntimeError(
                    "incompatible_control_store_schema:activation_cross_triggers"
                )
            binding_guard = "trg_terminal_rerun_delivery_authority_binding_guard"
            strict_binding, legacy_binding = (
                cls._v14_terminal_rerun_binding_guard_sql_contract()
            )
            observed_binding = cls._normalized_schema_sql(trigger_sql[binding_guard])
            if observed_binding == legacy_binding:
                trigger_sql[binding_guard] = strict_binding
            elif observed_binding != strict_binding:
                raise RuntimeError(
                    "incompatible_control_store_schema:terminal_rerun_authority_trigger_sql"
                )
            transition_index_sql = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type = 'index' "
                "AND name = 'idx_rca_activation_transition_epoch'"
            ).fetchone()
            if transition_index_sql is None or not str(transition_index_sql["sql"] or ""):
                raise RuntimeError("incompatible_control_store_schema:required_indexes")
            old_sequence = {
                str(row["name"]): int(row["seq"])
                for row in conn.execute("SELECT name, seq FROM sqlite_sequence").fetchall()
            }
            old_audit_sequence = max(
                old_sequence.get("rca_activation_transition_audit", 0),
                int(old_audits[-1][0]) if old_audits else 0,
            )
            cls._v15_migration_fault("after_preflight")

            for name in _V15_ACTIVATION_CROSS_TRIGGER_NAMES:
                conn.execute(f'DROP TRIGGER "{name}"')
            cls._v15_migration_fault("after_trigger_drop")
            conn.execute(
                "ALTER TABLE rca_activation_transition_audit ADD COLUMN "
                "binding_schema_version TEXT NOT NULL "
                f"DEFAULT '{ACTIVATION_TRANSITION_BINDING_SCHEMA_V14}' "
                "CHECK(binding_schema_version IN ("
                f"'{ACTIVATION_TRANSITION_BINDING_SCHEMA_V14}', "
                f"'{ACTIVATION_TRANSITION_BINDING_SCHEMA_V15}'))"
            )
            cls._v15_migration_fault("after_audit_upgrade")
            conn.execute(_V15_ACTIVATION_EPOCH_NEW_TABLE_SQL)
            for old_epoch, old_release, old_note in projected:
                conn.execute(
                    """
                    INSERT INTO rca_activation_epochs_v15_new(
                        epoch_id, state, is_current,
                        release_fingerprint_sha256, release_note_sha256,
                        config_sha256, db_logical_identity_json,
                        db_logical_identity_sha256, partition_start_fence_json,
                        partition_start_fence_sha256, created_at, updated_at,
                        activated_at, retired_at
                    ) VALUES (?, 'retired', 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        old_epoch["epoch_id"],
                        old_release,
                        old_note,
                        old_epoch["config_sha256"],
                        old_epoch["db_logical_identity_json"],
                        old_epoch["db_logical_identity_sha256"],
                        old_epoch["partition_start_fence_json"],
                        old_epoch["partition_start_fence_sha256"],
                        old_epoch["created_at"],
                        current,
                        old_epoch["steady_activated_at"],
                        current,
                    ),
                )
            cls._v15_migration_fault("after_epoch_copy")
            cls._validate_activation_child_rows_against_parent_tx(
                conn,
                parent_table="rca_activation_epochs_v15_new",
            )
            cls._v15_migration_fault("after_child_preflight")
            conn.execute("DROP TABLE rca_activation_epochs")
            conn.execute(
                "ALTER TABLE rca_activation_epochs_v15_new "
                "RENAME TO rca_activation_epochs"
            )
            conn.execute(_V15_CURRENT_ACTIVATION_INDEX_SQL)
            for name in _V15_ACTIVATION_CROSS_TRIGGER_NAMES:
                conn.execute(trigger_sql[name])
            cls._v15_migration_fault("after_epoch_swap")

            cls._insert_v15_direct_steady_epoch_tx(
                conn,
                epoch_id=str(contract["epoch_id"]),
                release_fingerprint_sha256=str(
                    contract["release_fingerprint_sha256"]
                ),
                release_note_sha256=str(contract["release_note_sha256"]),
                config_sha256=str(contract["config_sha256"]),
                db_logical_identity_json=str(contract["db_logical_identity_json"]),
                db_logical_identity_sha256=str(
                    contract["db_logical_identity_sha256"]
                ),
                partition_start_fence_json=str(
                    contract["partition_start_fence_json"]
                ),
                partition_start_fence_sha256=str(
                    contract["partition_start_fence_sha256"]
                ),
                created_at=current,
            )

            successor = cls._current_activation_epoch_unchecked_tx(conn)
            if successor is None or str(successor["epoch_id"]) != contract["epoch_id"]:
                raise ActivationEpochError("activation_epoch_create_lost")
            cls._insert_activation_transition_audit_tx(
                conn,
                epoch=successor,
                from_state="v15_migration",
                to_state="steady_active",
                operator=actor,
                reason=justification,
                transitioned_at=current,
                schema_version=CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION,
            )
            cls._v15_migration_fault("after_successor_audit")
            updated = conn.execute(
                "UPDATE control_meta SET value = ? "
                "WHERE key = 'schema_version' AND value = ?",
                (
                    CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION,
                    CONTROL_STORE_SCHEMA_VERSION,
                ),
            )
            if updated.rowcount != 1:
                raise RuntimeError("activation_v15_migration_marker_cas_failed")
            cls._v15_migration_fault("after_marker_cas")

            preserved_audits = [
                tuple(row)
                for row in conn.execute(
                    "SELECT audit_id, epoch_id, from_state, to_state, operator, "
                    "reason, binding_fingerprint, transitioned_at "
                    "FROM rca_activation_transition_audit "
                    "WHERE binding_schema_version = ? ORDER BY audit_id",
                    (ACTIVATION_TRANSITION_BINDING_SCHEMA_V14,),
                ).fetchall()
            ]
            if preserved_audits != old_audits:
                raise RuntimeError("activation_v15_migration_audit_changed")
            observed_transition_index = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type = 'index' "
                "AND name = 'idx_rca_activation_transition_epoch'"
            ).fetchone()
            if (
                observed_transition_index is None
                or str(observed_transition_index["sql"] or "")
                != str(transition_index_sql["sql"])
            ):
                raise RuntimeError("activation_v15_migration_index_changed")
            new_sequence = {
                str(row["name"]): int(row["seq"])
                for row in conn.execute("SELECT name, seq FROM sqlite_sequence").fetchall()
            }
            for name, sequence in old_sequence.items():
                if name == "rca_activation_transition_audit":
                    continue
                expected = sequence
                if new_sequence.get(name) != expected:
                    raise RuntimeError("activation_v15_migration_sequence_changed")
            if (
                new_sequence.get("rca_activation_transition_audit")
                != old_audit_sequence + 1
            ):
                raise RuntimeError("activation_v15_migration_sequence_changed")
            cls._validate_v15_activation_schema_tx(conn)
            cls._validate_structural_contract(conn, integrity_check=True)
            cls._validate_activation_reference_contract_tx(conn)
            cls._validate_activation_child_rows_tx(conn)
            cls._validate_current_activation_binding_tx(
                conn,
                schema_version=CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION,
            )
            cls._v15_migration_fault("before_commit")
            commit_started = True
            cls._commit_v15_migration_tx(conn)
        except Exception as exc:
            if conn is not None and conn.in_transaction:
                conn.rollback()
            if commit_started:
                commit_error = exc
            else:
                raise
        finally:
            if conn is not None:
                conn.close()

        outcome, public = cls._probe_direct_steady_activation_contract(
            path,
            contract=contract,
            busy_timeout_ms=busy_timeout_ms,
        )
        if outcome == "committed" and public is not None:
            return public
        if outcome == "not_committed":
            raise ActivationEpochError("activation_v15_migration_not_committed") from commit_error
        raise ActivationEpochError("activation_v15_migration_outcome_unknown") from commit_error

    def direct_steady_predecessor(self) -> dict[str, Any] | None:
        """Return the exact current binding and current-only in-flight count."""
        conn = self._connect()
        try:
            conn.execute("BEGIN")
            current = self._current_activation_epoch_tx(conn)
            if current is None:
                conn.commit()
                return None
            binding = self._activation_transition_binding_tx(conn, epoch=current)
            binding["inflight"] = self._direct_steady_current_inflight_tx(
                conn,
                epoch_id=str(current["epoch_id"]),
            )
            conn.commit()
            return binding
        except Exception:
            if conn.in_transaction:
                conn.rollback()
            raise
        finally:
            conn.close()

    def _activate_v15_direct_steady_epoch(
        self,
        *,
        contract: Mapping[str, Any],
        operator: str,
        reason: str,
        now: datetime | None,
    ) -> dict[str, Any]:
        actor, justification = self._normalize_activation_audit_text(operator, reason)
        if (
            contract["expected_control_schema_version"]
            != CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION
            or contract["target_control_schema_version"]
            != CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION
        ):
            raise ActivationEpochError("activation_control_schema_transition_invalid")
        current_time = _iso(now)
        conn = self._connect()
        commit_started = False
        commit_error: Exception | None = None
        try:
            conn.execute("BEGIN IMMEDIATE")
            if (
                self._activation_schema_version_tx(conn)
                != CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION
            ):
                raise RuntimeError("incompatible_control_store_schema:activation_layout")
            existing = self._current_activation_epoch_tx(conn)
            if existing is not None and str(existing["epoch_id"]) == contract["epoch_id"]:
                if not self._v15_direct_steady_binding_matches(
                    existing,
                    release_fingerprint_sha256=str(
                        contract["release_fingerprint_sha256"]
                    ),
                    release_note_sha256=str(contract["release_note_sha256"]),
                    config_sha256=str(contract["config_sha256"]),
                    db_logical_identity_json=str(
                        contract["db_logical_identity_json"]
                    ),
                    db_logical_identity_sha256=str(
                        contract["db_logical_identity_sha256"]
                    ),
                    partition_start_fence_json=str(
                        contract["partition_start_fence_json"]
                    ),
                    partition_start_fence_sha256=str(
                        contract["partition_start_fence_sha256"]
                    ),
                ):
                    raise ActivationEpochError(
                        "activation_direct_steady_binding_conflict"
                    )
                conn.commit()
                return self._public_activation_epoch(
                    existing,
                    schema_version=CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION,
                )

            self._validate_partition_start_fence_cas_tx(
                conn,
                partition_start_fence=contract["partition_start_fence"],
            )
            predecessor_id = str(contract["expected_predecessor_epoch_id"])
            if existing is None:
                if predecessor_id:
                    raise ActivationEpochError("activation_predecessor_binding_changed")
            else:
                predecessor_binding = self._v15_activation_transition_binding_tx(
                    conn,
                    epoch=existing,
                )
                if (
                    not predecessor_id
                    or predecessor_binding["epoch_id"] != predecessor_id
                    or predecessor_binding["state"]
                    != contract["expected_predecessor_state"]
                    or predecessor_binding["binding_fingerprint"]
                    != contract["expected_predecessor_binding_fingerprint"]
                ):
                    raise ActivationEpochError("activation_predecessor_binding_changed")
                inflight = self._direct_steady_current_inflight_tx(
                    conn,
                    epoch_id=str(existing["epoch_id"]),
                )
                if inflight["total"]:
                    raise ActivationEpochError(
                        "activation_predecessor_inflight_not_drained"
                    )
                retired = conn.execute(
                    """
                    UPDATE rca_activation_epochs
                       SET state = 'retired', is_current = 0,
                           updated_at = ?, retired_at = ?
                     WHERE epoch_id = ? AND state = 'steady_active'
                       AND is_current = 1
                       AND EXISTS (
                           SELECT 1 FROM rca_activation_transition_audit
                            WHERE audit_id = ? AND epoch_id = ?
                              AND to_state = 'steady_active'
                              AND binding_schema_version = ?
                              AND binding_fingerprint = ?
                       )
                    """,
                    (
                        current_time,
                        current_time,
                        predecessor_id,
                        predecessor_binding["audit_id"],
                        predecessor_id,
                        ACTIVATION_TRANSITION_BINDING_SCHEMA_V15,
                        contract["expected_predecessor_binding_fingerprint"],
                    ),
                )
                if retired.rowcount != 1:
                    raise ActivationEpochError("activation_epoch_state_changed")
                retired_epoch = conn.execute(
                    "SELECT * FROM rca_activation_epochs WHERE epoch_id = ?",
                    (predecessor_id,),
                ).fetchone()
                if retired_epoch is None:
                    raise ActivationEpochError("activation_epoch_state_changed")
                self._insert_activation_transition_audit_tx(
                    conn,
                    epoch=retired_epoch,
                    from_state="steady_active",
                    to_state="retired",
                    operator=actor,
                    reason=justification,
                    transitioned_at=current_time,
                    schema_version=CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION,
                )

            self._insert_v15_direct_steady_epoch_tx(
                conn,
                epoch_id=str(contract["epoch_id"]),
                release_fingerprint_sha256=str(
                    contract["release_fingerprint_sha256"]
                ),
                release_note_sha256=str(contract["release_note_sha256"]),
                config_sha256=str(contract["config_sha256"]),
                db_logical_identity_json=str(contract["db_logical_identity_json"]),
                db_logical_identity_sha256=str(
                    contract["db_logical_identity_sha256"]
                ),
                partition_start_fence_json=str(
                    contract["partition_start_fence_json"]
                ),
                partition_start_fence_sha256=str(
                    contract["partition_start_fence_sha256"]
                ),
                created_at=current_time,
            )
            successor = self._current_activation_epoch_unchecked_tx(conn)
            if successor is None or str(successor["epoch_id"]) != contract["epoch_id"]:
                raise ActivationEpochError("activation_epoch_create_lost")
            self._insert_activation_transition_audit_tx(
                conn,
                epoch=successor,
                from_state="direct_release",
                to_state="steady_active",
                operator=actor,
                reason=justification,
                transitioned_at=current_time,
                schema_version=CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION,
            )
            self._validate_current_activation_binding_tx(
                conn,
                schema_version=CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION,
            )
            commit_started = True
            self._commit_v15_migration_tx(conn)
        except Exception as exc:
            if conn.in_transaction:
                conn.rollback()
            if commit_started:
                commit_error = exc
            else:
                raise
        finally:
            conn.close()

        outcome, public = self._probe_direct_steady_activation_contract(
            self.db_path,
            contract=contract,
            busy_timeout_ms=self.busy_timeout_ms,
        )
        if outcome == "committed" and public is not None:
            return public
        if outcome == "not_committed":
            raise ActivationEpochError("activation_direct_steady_not_committed") from commit_error
        raise ActivationEpochError("activation_direct_steady_outcome_unknown") from commit_error

    def activate_direct_steady_epoch(
        self,
        *,
        epoch_id: str,
        release_fingerprint_sha256: str,
        release_note_sha256: str,
        config_sha256: str,
        db_logical_identity: Mapping[str, Any],
        partition_start_fence: Mapping[str, Any],
        operator: str,
        reason: str,
        expected_predecessor_epoch_id: str = "",
        expected_predecessor_state: str = "",
        expected_predecessor_binding_fingerprint: str = "",
        expected_control_schema_version: str | None = None,
        target_control_schema_version: str | None = None,
        epoch_contract_sha256: str | None = None,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        """Atomically install one explicit direct-release steady epoch.

        This is a deliberately separate compatibility path for a release that
        has no bounded canary slots.  It still records the exact release,
        config, database, and optional broker-fence identities and leaves the normal
        provider/write-fence checks anchored to the current epoch.  Historical
        outbox state is observation-only here; it is not a prerequisite for
        activating a fresh current epoch.  An empty broker fence explicitly
        represents a Kafka-disabled release; a later Kafka consumer still
        fails closed when it requests a missing topic or partition fence.
        """
        if self._observed_schema_version == CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION:
            if self.read_only:
                raise sqlite3.OperationalError("attempt to write a readonly database")
            if (
                expected_control_schema_version is None
                or target_control_schema_version is None
                or epoch_contract_sha256 is None
            ):
                raise ActivationEpochError("activation_epoch_contract_required")
            contract = self._normalize_direct_steady_activation_contract(
                epoch_id=epoch_id,
                release_fingerprint_sha256=release_fingerprint_sha256,
                release_note_sha256=release_note_sha256,
                config_sha256=config_sha256,
                db_logical_identity=db_logical_identity,
                partition_start_fence=partition_start_fence,
                expected_predecessor_epoch_id=expected_predecessor_epoch_id,
                expected_predecessor_state=expected_predecessor_state,
                expected_predecessor_binding_fingerprint=(
                    expected_predecessor_binding_fingerprint
                ),
                expected_control_schema_version=expected_control_schema_version,
                target_control_schema_version=target_control_schema_version,
                epoch_contract_sha256=epoch_contract_sha256,
            )
            return self._activate_v15_direct_steady_epoch(
                contract=contract,
                operator=operator,
                reason=reason,
                now=now,
            )
        identity = self._normalize_activation_epoch_id(epoch_id)
        release_hash = self._normalize_activation_sha256(
            release_fingerprint_sha256, "release_fingerprint_sha256"
        )
        receipt_hash = self._normalize_activation_sha256(
            release_note_sha256,
            "release_note_sha256",
        )
        config_hash = self._normalize_activation_sha256(config_sha256, "config_sha256")
        db_identity_json = self._normalize_activation_db_identity(db_logical_identity)
        db_identity_sha = hashlib.sha256(db_identity_json.encode("utf-8")).hexdigest()
        start_fence_json = self._normalize_partition_fence(partition_start_fence)
        start_fence_sha = hashlib.sha256(start_fence_json.encode("utf-8")).hexdigest()
        actor, justification = self._normalize_activation_audit_text(operator, reason)
        predecessor_values = (
            str(expected_predecessor_epoch_id or "").strip(),
            str(expected_predecessor_state or "").strip(),
            str(expected_predecessor_binding_fingerprint or "").strip().lower(),
        )
        if any(predecessor_values) and not all(predecessor_values):
            raise ActivationEpochError("activation_predecessor_binding_incomplete")
        predecessor_epoch_id, predecessor_state, predecessor_fingerprint = (
            predecessor_values
        )
        if predecessor_epoch_id:
            predecessor_epoch_id = self._normalize_activation_epoch_id(
                predecessor_epoch_id
            )
            if predecessor_state not in {"aborted", "steady_active"}:
                raise ActivationEpochError("activation_predecessor_state_invalid")
            predecessor_fingerprint = self._normalize_activation_sha256(
                predecessor_fingerprint,
                "predecessor_binding_fingerprint",
            )
        current = _iso(now)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            existing = self._current_activation_epoch_tx(conn)
            if existing is not None and str(existing["epoch_id"]) == identity:
                # A completed direct release is create-once.  Retrying the
                # exact binding is safe; changing any identity is a conflict.
                identical = self._v14_compat_direct_steady_binding_matches(
                    existing,
                    release_fingerprint_sha256=release_hash,
                    release_note_sha256=receipt_hash,
                    config_sha256=config_hash,
                    db_logical_identity_json=db_identity_json,
                    db_logical_identity_sha256=db_identity_sha,
                    partition_start_fence_json=start_fence_json,
                    partition_start_fence_sha256=start_fence_sha,
                )
                if identical:
                    conn.commit()
                    return self._public_activation_epoch(existing)
                raise ActivationEpochError("activation_direct_steady_binding_conflict")

            if existing is None:
                if predecessor_epoch_id:
                    raise ActivationEpochError("activation_predecessor_binding_changed")
            else:
                existing_state = str(existing["state"] or "")
                if existing_state not in {"aborted", "steady_active"}:
                    raise ActivationEpochError("activation_current_epoch_exists")
                if existing_state == "steady_active" and not predecessor_epoch_id:
                    raise ActivationEpochError("activation_current_epoch_exists")
                predecessor_binding: dict[str, Any] | None = None
                if predecessor_epoch_id:
                    predecessor_binding = (
                        self._v14_compat_activation_transition_binding_tx(
                            conn,
                            epoch=existing,
                        )
                    )
                    if (
                        predecessor_binding["epoch_id"] != predecessor_epoch_id
                        or predecessor_binding["state"] != predecessor_state
                        or predecessor_binding["binding_fingerprint"]
                        != predecessor_fingerprint
                    ):
                        raise ActivationEpochError(
                            "activation_predecessor_binding_changed"
                        )
                if existing_state == "steady_active":
                    inflight = self._direct_steady_current_inflight_tx(
                        conn,
                        epoch_id=str(existing["epoch_id"]),
                    )
                    if inflight["total"]:
                        raise ActivationEpochError(
                            "activation_predecessor_inflight_not_drained"
                        )
            if existing is not None:
                # The exact predecessor CAS and successor insert share this
                # write transaction; readers never observe a no-current gap.
                if predecessor_epoch_id:
                    assert predecessor_binding is not None
                    updated = conn.execute(
                        """
                        UPDATE rca_activation_epochs
                           SET is_current = 0, superseded_at = ?, updated_at = ?
                         WHERE epoch_id = ? AND is_current = 1 AND state = ?
                           AND EXISTS (
                               SELECT 1
                                 FROM rca_activation_transition_audit
                                WHERE audit_id = ? AND epoch_id = ?
                                  AND to_state = ? AND binding_fingerprint = ?
                           )
                        """,
                        (
                            current,
                            current,
                            predecessor_epoch_id,
                            predecessor_state,
                            predecessor_binding["audit_id"],
                            predecessor_epoch_id,
                            predecessor_state,
                            predecessor_fingerprint,
                        ),
                    )
                else:
                    updated = conn.execute(
                        """
                        UPDATE rca_activation_epochs
                           SET is_current = 0, superseded_at = ?, updated_at = ?
                         WHERE epoch_id = ? AND is_current = 1 AND state = 'aborted'
                        """,
                        (current, current, existing["epoch_id"]),
                    )
                if updated.rowcount != 1:
                    raise ActivationEpochError("activation_epoch_state_changed")

            self._insert_v14_compat_direct_steady_epoch_tx(
                conn,
                epoch_id=identity,
                release_fingerprint_sha256=release_hash,
                release_note_sha256=receipt_hash,
                config_sha256=config_hash,
                db_logical_identity_json=db_identity_json,
                db_logical_identity_sha256=db_identity_sha,
                partition_start_fence_json=start_fence_json,
                partition_start_fence_sha256=start_fence_sha,
                created_at=current,
            )
            # The matching audit is inserted immediately below in this same
            # transaction, so this is the only post-insert unchecked read.
            row = self._current_activation_epoch_unchecked_tx(conn)
            if row is None or str(row["epoch_id"]) != identity:
                raise ActivationEpochError("activation_epoch_create_lost")
            self._insert_activation_transition_audit_tx(
                conn,
                epoch=row,
                from_state="direct_release",
                to_state="steady_active",
                operator=actor,
                reason=justification,
                transitioned_at=current,
            )
            conn.commit()
            return self._public_activation_epoch(row)
        except Exception:
            if conn.in_transaction:
                conn.rollback()
            raise
        finally:
            conn.close()

    def activation_epoch(self) -> dict[str, Any] | None:
        conn = self._connect()
        try:
            conn.execute("BEGIN")
            row = self._current_activation_epoch_tx(conn)
            if row is None:
                conn.commit()
                return None
            value = self._public_activation_epoch(row)
            conn.commit()
            return value
        except Exception:
            if conn.in_transaction:
                conn.rollback()
            raise
        finally:
            conn.close()

    def validate_manual_external_write_admission(
        self,
        result: Mapping[str, Any],
        *,
        expected_chat_id: str,
        expected_thread_id: str,
        expected_message_id: str,
        expected_requester_id: str,
    ) -> dict[str, Any]:
        """Validate one manual acknowledgement against its live admission ledger."""
        value = dict(result) if isinstance(result, Mapping) else {}
        generation = value.get("generation")
        required_text = (
            "outcome",
            "business_key",
            "submission_key",
            "source_id",
            "subscription_key",
            "state",
        )
        if (
            set(value) != MANUAL_ADMISSION_RESULT_FIELDS
            or value.get("schema_version")
            != MANUAL_ADMISSION_RESULT_SCHEMA_VERSION
            or isinstance(generation, bool)
            or not isinstance(generation, int)
            or generation < 1
            or any(
                not isinstance(value.get(field), str)
                or not str(value[field]).strip()
                for field in required_text
            )
            or not isinstance(value.get("reason"), str)
        ):
            raise RecordConflictError(
                "manual_external_write_admission_schema_invalid"
            )
        expected_source = {
            "chat_id": str(expected_chat_id or "").strip(),
            "thread_id": str(expected_thread_id or "").strip(),
            "message_id": str(expected_message_id or "").strip(),
            "requester_id": str(expected_requester_id or "").strip(),
        }
        operator_issue_only = not expected_source["chat_id"] and not expected_source[
            "thread_id"
        ]
        if (
            not expected_source["message_id"]
            or not expected_source["requester_id"]
            or (not operator_issue_only and not all(expected_source.values()))
            or (operator_issue_only and (expected_chat_id or expected_thread_id))
        ):
            raise RecordConflictError(
                "manual_external_write_source_identity_invalid"
            )

        conn = self._connect()
        try:
            conn.execute("BEGIN")
            try:
                current_epoch = self._current_activation_epoch_tx(conn)
            except ActivationEpochError as exc:
                raise RecordConflictError(
                    "manual_external_write_activation_epoch_not_current"
                ) from exc
            if (
                current_epoch is None
                or str(current_epoch["state"] or "") != "steady_active"
            ):
                raise RecordConflictError(
                    "manual_external_write_activation_epoch_not_current"
                )
            source = conn.execute(
                """
                SELECT source.source_kind, source.platform, source.chat_id,
                       source.thread_id, source.message_id, source.requester_id,
                       source.mode, source.outcome,
                       binding.business_key, binding.generation,
                       trigger.submission_key, trigger.normalized_json,
                       subscription.subscription_key,
                       delivery_binding.source_id AS delivery_source_id
                  FROM rca_trigger_sources AS source
                  JOIN rca_trigger_bindings AS binding
                    ON binding.source_id = source.source_id
                  JOIN business_triggers AS trigger
                    ON trigger.business_key = binding.business_key
                   AND trigger.generation = binding.generation
                  JOIN rca_delivery_subscriptions AS subscription
                    ON subscription.subscription_key = ?
                   AND subscription.business_key = binding.business_key
                   AND subscription.generation = binding.generation
                  JOIN rca_trigger_delivery_bindings AS delivery_binding
                    ON delivery_binding.source_id = source.source_id
                   AND delivery_binding.subscription_key =
                       subscription.subscription_key
                 WHERE source.source_id = ?
                   AND binding.business_key = ?
                   AND binding.generation = ?
                   AND trigger.submission_key = ?
                """,
                (
                    value["subscription_key"],
                    value["source_id"],
                    value["business_key"],
                    generation,
                    value["submission_key"],
                ),
            ).fetchone()
            if source is None:
                raise RecordConflictError(
                    "manual_external_write_admission_binding_missing"
                )
            observed_source = {
                "chat_id": str(source["chat_id"]),
                "thread_id": str(source["thread_id"]),
                "message_id": str(source["message_id"]),
                "requester_id": str(source["requester_id"]),
            }
            source_platform = str(source["platform"] or "")
            source_mode = str(source["mode"] or "")
            source_is_operator_issue_only = (
                source_platform == "operator"
                and not observed_source["chat_id"]
                and not observed_source["thread_id"]
                and source_mode == "rerun"
            )
            if (
                str(source["source_kind"]) != "feishu_group_manual"
                or not (
                    (source_platform == "feishu" and not operator_issue_only)
                    or (source_is_operator_issue_only and operator_issue_only)
                )
                or observed_source != expected_source
                or str(source["outcome"]) != str(value["outcome"])
                or str(source["delivery_source_id"]) != str(value["source_id"])
            ):
                raise RecordConflictError(
                    "manual_external_write_source_identity_mismatch"
                )
            try:
                context = json.loads(str(source["normalized_json"]))
                issue_url = str(context["issue_url"])
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                raise RecordConflictError(
                    "manual_external_write_trigger_context_invalid"
                ) from exc
            source_identity = {
                **observed_source,
                "issue_url": issue_url,
                "mode": source_mode,
            }
            # Operator issue-only admissions use stable transport placeholders
            # in the activation ledger while retaining empty chat/thread fields
            # in the provider-facing source identity.
            ledger_source_identity = dict(source_identity)
            if source_is_operator_issue_only:
                ledger_source_identity.update(
                    {"chat_id": "operator", "thread_id": "operator:issue-only"}
                )
            source_identity_sha256 = _canonical_sha256(ledger_source_identity)
            ledger = conn.execute(
                """
                SELECT epoch.epoch_id, epoch.state, epoch.is_current,
                       ledger.ledger_id, ledger.decision, ledger.bound_at,
                       ledger.business_key, ledger.submission_key,
                       ledger.generation
                  FROM rca_activation_epochs AS epoch
                  JOIN rca_activation_admission_ledger AS ledger
                    ON ledger.epoch_id = epoch.epoch_id
                 WHERE epoch.is_current = 1
                   AND ledger.entrypoint = 'manual_admit'
                   AND ledger.source_kind = 'manual'
                   AND ledger.source_identity_sha256 = ?
                   AND ledger.business_key = ?
                   AND ledger.submission_key = ?
                   AND ledger.generation = ?
                   AND ledger.decision IN ('admit', 'join')
                 ORDER BY ledger.ledger_id DESC
                 LIMIT 1
                """,
                (
                    source_identity_sha256,
                    value["business_key"],
                    value["submission_key"],
                    generation,
                ),
            ).fetchone()
            if ledger is None or int(ledger["is_current"]) != 1:
                raise RecordConflictError(
                    "manual_external_write_activation_ledger_missing"
                )
            if str(ledger["state"]) != "steady_active":
                raise RecordConflictError(
                    "manual_external_write_activation_epoch_not_current"
                )
            if (
                str(ledger["decision"]) == "admit"
                and not str(ledger["bound_at"] or "").strip()
            ):
                raise RecordConflictError(
                    "manual_external_write_activation_ledger_unbound"
                )
            return {
                "epoch_id": str(ledger["epoch_id"]),
                "state": str(ledger["state"]),
                "ledger_id": int(ledger["ledger_id"]),
                "decision": str(ledger["decision"]),
                "business_key": str(ledger["business_key"]),
                "submission_key": str(ledger["submission_key"]),
                "generation": int(ledger["generation"]),
                "source_id": str(value["source_id"]),
                "subscription_key": str(value["subscription_key"]),
                "issue_url": issue_url,
                **observed_source,
            }
        finally:
            if conn.in_transaction:
                conn.rollback()

    def manual_external_write_admission_for_effect(
        self,
        *,
        business_key: str,
        submission_key: str,
        generation: int,
        delivery_id: str,
        effect_kind: str,
        target_key: str,
    ) -> dict[str, Any] | None:
        """Build the exact manual admission envelope for one materialized effect.

        Manual executions intentionally do not carry a W3 snapshot.  The
        source row, trigger binding, and delivery subscription are still an
        immutable, activation-bound authority chain; expose only that chain to
        the provider-fence builder so the physical sender can reopen it.
        """
        if (
            not all(
                isinstance(value, str) and value.strip()
                for value in (business_key, submission_key, delivery_id, effect_kind, target_key)
            )
            or isinstance(generation, bool)
            or not isinstance(generation, int)
            or generation < 1
        ):
            raise RecordConflictError("manual_external_write_effect_binding_invalid")
        conn = self._connect()
        try:
            source_row = conn.execute(
                """
                SELECT source.source_kind, source.platform
                  FROM business_triggers AS trigger
                  JOIN rca_trigger_sources AS source
                    ON source.source_id = trigger.origin_source_id
                 WHERE trigger.business_key = ?
                   AND trigger.submission_key = ?
                   AND trigger.generation = ?
                """,
                (business_key, submission_key, generation),
            ).fetchone()
            if source_row is None or str(source_row["source_kind"] or "") != "feishu_group_manual":
                return None
            row = conn.execute(
                """
                SELECT source.source_id, source.source_kind, source.platform,
                       source.chat_id, source.thread_id, source.message_id,
                       source.requester_id, source.mode, source.outcome,
                       trigger.state, trigger.normalized_json,
                       subscription.subscription_key,
                       subscription.effect_kind, subscription.target_key,
                       subscription.delivery_id
                  FROM business_triggers AS trigger
                  JOIN rca_trigger_sources AS source
                    ON source.source_id = trigger.origin_source_id
                  JOIN rca_delivery_subscriptions AS subscription
                    ON subscription.business_key = trigger.business_key
                   AND subscription.generation = trigger.generation
                 WHERE trigger.business_key = ?
                   AND trigger.submission_key = ?
                   AND trigger.generation = ?
                   AND subscription.delivery_id = ?
                   AND subscription.effect_kind = ?
                   AND subscription.target_key = ?
                   AND source.source_kind = 'feishu_group_manual'
                   AND source.platform IN ('feishu', 'operator')
                 ORDER BY subscription.subscription_key
                """,
                (
                    business_key,
                    submission_key,
                    generation,
                    delivery_id,
                    effect_kind,
                    target_key,
                ),
            ).fetchone()
            if row is None:
                raise RecordConflictError(
                    "manual_external_write_effect_binding_missing"
                )
            try:
                context = json.loads(str(row["normalized_json"] or ""))
                issue_url = str(context["issue_url"] or "").strip().rstrip("/")
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                raise RecordConflictError(
                    "manual_external_write_trigger_context_invalid"
                ) from exc
            source_identity = {
                "chat_id": str(row["chat_id"] or ""),
                "thread_id": str(row["thread_id"] or ""),
                "message_id": str(row["message_id"] or ""),
                "requester_id": str(row["requester_id"] or ""),
                "issue_url": issue_url,
                "mode": str(row["mode"] or ""),
            }
            source_platform = str(row["platform"] or "")
            operator_issue_only = (
                source_platform == "operator"
                and not source_identity["chat_id"]
                and not source_identity["thread_id"]
                and source_identity["mode"] == "rerun"
            )
            admission = {
                "schema_version": MANUAL_ADMISSION_RESULT_SCHEMA_VERSION,
                "outcome": str(row["outcome"] or ""),
                "business_key": business_key,
                "submission_key": submission_key,
                "generation": generation,
                "source_id": str(row["source_id"] or ""),
                "subscription_key": str(row["subscription_key"] or ""),
                "state": str(row["state"] or ""),
                "reason": "dispatcher_manual_effect_binding",
            }
            if (
                str(row["effect_kind"] or "") != effect_kind
                or str(row["delivery_id"] or "") != delivery_id
                or source_platform not in {"feishu", "operator"}
                or not source_identity["message_id"].strip()
                or not source_identity["requester_id"].strip()
                or not source_identity["issue_url"].strip()
                or not source_identity["mode"].strip()
                or (
                    source_platform == "feishu"
                    and (
                        not source_identity["chat_id"].strip()
                        or not source_identity["thread_id"].strip()
                    )
                )
                or (source_platform == "operator" and not operator_issue_only)
                or (
                    operator_issue_only
                    and effect_kind
                    not in {"feishu_issue_comment", "feishu_issue_field_update"}
                )
                or not all(str(value).strip() for value in admission.values() if not isinstance(value, int))
            ):
                raise RecordConflictError(
                    "manual_external_write_effect_binding_invalid"
                )
            return {
                "admission": admission,
                "source_identity": source_identity,
            }
        finally:
            conn.close()

    def validate_external_write_fence_binding(
        self,
        fence: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Return the live ledger binding for a W5 fence or fail closed.

        This query is intentionally independent of dispatcher enable switches;
        a fence is live only while its exact admitted ledger row belongs to the
        current steady activation epoch.
        """
        epoch_id = str(fence.get("activation_epoch_id") or "").strip()
        ledger_id = fence.get("activation_ledger_id")
        admission_key = str(fence.get("admission_key") or "").strip()
        if not epoch_id or isinstance(ledger_id, bool) or not isinstance(ledger_id, int):
            raise RecordConflictError("external_write_fence_schema_invalid")
        conn = self._connect()
        try:
            conn.execute("BEGIN")
            try:
                current_epoch = self._current_activation_epoch_tx(conn)
            except ActivationEpochError as exc:
                raise RecordConflictError(
                    "external_write_fence_epoch_not_current"
                ) from exc
            if (
                current_epoch is None
                or str(current_epoch["epoch_id"]) != epoch_id
                or str(current_epoch["state"] or "") != "steady_active"
            ):
                raise RecordConflictError(
                    "external_write_fence_epoch_not_current"
                )
            row = conn.execute(
                """
                SELECT epoch.epoch_id, epoch.state, epoch.is_current,
                       ledger.ledger_id, ledger.admission_key,
                       ledger.business_key, ledger.submission_key,
                       ledger.generation, ledger.decision, ledger.bound_at,
                       snapshot.admission_snapshot_json,
                       envelope.source_envelope_json
                  FROM rca_activation_epochs AS epoch
                  JOIN rca_activation_admission_ledger AS ledger
                    ON ledger.epoch_id = epoch.epoch_id
                   AND ledger.ledger_id = ?
                  JOIN rca_admission_snapshots AS snapshot
                    ON snapshot.business_key = ledger.business_key
                   AND snapshot.submission_key = ledger.submission_key
                   AND snapshot.generation = ledger.generation
                   AND snapshot.activation_epoch_id = ledger.epoch_id
                   AND snapshot.activation_ledger_id = ledger.ledger_id
                  JOIN rca_snapshot_source_envelopes AS envelope
                    ON envelope.snapshot_sha256 = snapshot.snapshot_sha256
                   AND envelope.source_envelope_sha256 =
                       snapshot.creator_source_envelope_sha256
                   AND envelope.source_id = snapshot.creator_source_id
                 WHERE epoch.epoch_id = ?
                   AND ledger.admission_key = ?
                """,
                (ledger_id, epoch_id, admission_key),
            ).fetchone()
            conn.commit()
        except Exception:
            if conn.in_transaction:
                conn.rollback()
            raise
        finally:
            conn.close()
        if row is None or int(row["is_current"]) != 1:
            raise RecordConflictError("external_write_fence_epoch_not_current")
        if str(row["state"]) != "steady_active":
            raise RecordConflictError("external_write_fence_epoch_not_current")
        if str(row["decision"]) != "admit" or not row["bound_at"]:
            raise RecordConflictError("external_write_fence_operation_denied")
        try:
            from gateway.pnc_rca_write_fence import (
                ExternalWriteFenceError,
                validate_write_fence_source_binding,
            )

            targets = validate_write_fence_source_binding(
                fence,
                snapshot=json.loads(str(row["admission_snapshot_json"])),
                source_envelope=json.loads(str(row["source_envelope_json"])),
            )
        except ExternalWriteFenceError as exc:
            raise RecordConflictError(exc.code) from exc
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise RecordConflictError("external_write_fence_schema_invalid") from exc
        return {
            "epoch_id": str(row["epoch_id"]),
            "state": str(row["state"]),
            "ledger_id": int(row["ledger_id"]),
            "admission_key": str(row["admission_key"]),
            "business_key": str(row["business_key"]),
            "submission_key": str(row["submission_key"]),
            "generation": int(row["generation"]),
            **targets,
        }

    @staticmethod
    def _activation_silent_terminal_complete_tx(
        conn: sqlite3.Connection,
        *,
        business_key: str,
        submission_key: str,
        generation: int,
        watch: sqlite3.Row,
    ) -> bool:
        """Accept only the collector's zero-write terminal route contract."""
        task_id = str(watch["task_id"] or "")
        error_code = str(watch["last_error_code"] or "")
        status_raw = str(watch["last_status_json"] or "")
        if (
            str(watch["state"] or "") != "terminal_failed"
            or not task_id
            or not str(watch["terminal_at"] or "")
            or not error_code
            or str(watch["delivery_id"] or "")
            or str(watch["job_delivery_id"] or "")
            or str(watch["job_status"] or "")
            or str(watch["job_outcome"] or "")
            or str(watch["terminal_state"] or "")
            or str(watch["terminal_error_code"] or "")
        ):
            return False
        try:
            status = json.loads(status_raw)
        except (TypeError, ValueError, json.JSONDecodeError):
            return False
        if (
            not isinstance(status, dict)
            or _canonical_json(status) != status_raw
            or status.get("external_writes") is not False
            or status.get("terminal_delivery_policy")
            != _SILENT_TERMINAL_POLICY
        ):
            return False
        taxonomy = status.get("failure_taxonomy")
        if not isinstance(taxonomy, dict):
            return False
        route_kind = str(taxonomy.get("internal_route") or "")
        lane = str(taxonomy.get("lane") or "")
        durable_route = taxonomy.get("durable_route")
        fallback = taxonomy.get("terminal_fallback")
        receipt = taxonomy.get("receipt")
        taxonomy_gap_prefix = "taxonomy_gap:"
        taxonomy_gap_code = (
            error_code[len(taxonomy_gap_prefix) :]
            if error_code.startswith(taxonomy_gap_prefix)
            else ""
        )
        known_terminal = (
            taxonomy.get("known") is True
            and taxonomy.get("retryable") is False
            and not taxonomy_gap_code
        )
        taxonomy_gap_terminal = (
            bool(re.fullmatch(r"[a-z0-9_.-]{1,96}", taxonomy_gap_code))
            and taxonomy.get("known") is False
            and taxonomy.get("retryable") is False
            and str(taxonomy.get("raw_code") or "") == taxonomy_gap_code
            and taxonomy.get("source") == "durable_failure_route_deadline"
            and taxonomy.get("observed_state") == "pending"
            and taxonomy.get("source_conflict") is False
            and taxonomy.get("external_comment_policy")
            == "honest_non_attribution_only"
            and (route_kind, lane) == ("internal_alert", "hard_defect")
        )
        immediate_taxonomy_gap_terminal = (
            taxonomy_gap_code in _ACTIVATION_IMMEDIATE_TAXONOMY_GAP_CODES
            and taxonomy.get("known") is False
            and taxonomy.get("retryable") is False
            and str(taxonomy.get("raw_code") or "") == taxonomy_gap_code
            and taxonomy.get("source") == "rca_service_result"
            and taxonomy.get("observed_state") == "failed"
            and taxonomy.get("source_conflict") is False
            and taxonomy.get("external_comment_policy")
            == "honest_non_attribution_only"
            and (route_kind, lane) == ("internal_alert", "hard_defect")
            and isinstance(receipt, dict)
            and receipt.get("schema_version") == "g1q3_rca_service_result_v2"
            and receipt.get("pipeline_stage") == "s6_report"
            and receipt.get("pipeline_status") == "blocked"
            and receipt.get("status") == "pipeline_not_successful"
            and str(receipt.get("task_id") or "") == task_id
        )
        # Older collector releases classified a completed, zero-effect Gate-A
        # verification failure as a taxonomy gap after the terminal diagnostic
        # fallback window.  It is safe to drain only this exact legacy shape:
        # the route is an internal hard-defect alert, the verifier explicitly
        # rejected an unknown blocker, and no terminal receipt/effect exists.
        legacy_gate_a_terminal = (
            taxonomy_gap_code == "gate_a_projection_invalid"
            and taxonomy.get("known") is False
            and taxonomy.get("retryable") is False
            and str(taxonomy.get("raw_code") or "")
            == "gate_a_projection_invalid"
            and taxonomy.get("source") == "delivery_contract_verifier"
            and taxonomy.get("observed_state") == "completed"
            and taxonomy.get("source_conflict") is False
            and taxonomy.get("external_comment_policy")
            == "honest_non_attribution_only"
            and taxonomy.get("contract_errors") == ["unknown_blocker_kind"]
            and (
                receipt is None
                or (isinstance(receipt, dict) and not receipt)
            )
            and (route_kind, lane) == ("internal_alert", "hard_defect")
        )
        legacy_execution_identity_terminal = (
            taxonomy_gap_code == "execution_identity_readback_unavailable"
            and taxonomy.get("known") is False
            and taxonomy.get("retryable") is False
            and str(taxonomy.get("raw_code") or "")
            == "execution_identity_readback_unavailable"
            and taxonomy.get("source") == "delivery_contract_verifier"
            and taxonomy.get("observed_state") == "completed"
            and taxonomy.get("source_conflict") is False
            and taxonomy.get("external_comment_policy")
            == "honest_non_attribution_only"
            and taxonomy.get("contract_errors") == ["unknown_blocker_kind"]
            and receipt == {}
            and (route_kind, lane) == ("internal_alert", "hard_defect")
        )
        if (
            not (
                known_terminal
                or taxonomy_gap_terminal
                or immediate_taxonomy_gap_terminal
                or legacy_gate_a_terminal
                or legacy_execution_identity_terminal
            )
            or str(taxonomy.get("terminal_error_code") or "") != error_code
            or (route_kind, lane)
            not in _SILENT_TERMINAL_ROUTE_LANES
            or not isinstance(durable_route, dict)
            or not isinstance(fallback, dict)
        ):
            return False
        route_key = str(durable_route.get("route_key") or "")
        outlet = durable_route.get("internal_outlet")
        if (
            not route_key
            or str(fallback.get("route_key") or "") != route_key
            or str(fallback.get("route_kind") or "") != route_kind
            or str(fallback.get("route_owner") or "")
            != str(durable_route.get("owner") or "")
            or not isinstance(outlet, dict)
            or str(outlet.get("route_key") or "") != route_key
            or outlet.get("status") != "settled"
            or outlet.get("external_effects") != 0
            or isinstance(outlet.get("attempt"), bool)
            or not isinstance(outlet.get("attempt"), int)
            or int(outlet["attempt"]) < 1
        ):
            return False
        if taxonomy_gap_terminal and (
            str(taxonomy.get("resumed_route_key") or "") != route_key
            or fallback.get("schema_version")
            != "pnc_rca_bounded_terminal_fallback_v1"
            or fallback.get("terminal_class") != "honest_non_attribution"
            or fallback.get("confidence_tier") != "low"
            or str(durable_route.get("owner") or "") != "rca-engineering"
            or str(durable_route.get("status") or "") != "alert_pending"
        ):
            return False
        if immediate_taxonomy_gap_terminal and (
            fallback.get("schema_version")
            != "pnc_rca_bounded_terminal_fallback_v1"
            or fallback.get("terminal_class") != "honest_non_attribution"
            or fallback.get("confidence_tier") != "low"
            or str(durable_route.get("owner") or "") != "rca-engineering"
            or str(durable_route.get("status") or "") != "alert_pending"
            or not isinstance(fallback.get("elapsed_seconds"), int)
            or isinstance(fallback.get("elapsed_seconds"), bool)
            or int(fallback.get("elapsed_seconds")) < 0
        ):
            return False
        if legacy_gate_a_terminal and (
            fallback.get("schema_version")
            != "pnc_rca_bounded_terminal_fallback_v1"
            or fallback.get("terminal_class") != "honest_non_attribution"
            or fallback.get("confidence_tier") != "low"
            or str(durable_route.get("owner") or "") != "rca-engineering"
            or str(durable_route.get("status") or "") != "alert_pending"
            or not isinstance(fallback.get("elapsed_seconds"), int)
            or isinstance(fallback.get("elapsed_seconds"), bool)
            or int(fallback.get("elapsed_seconds")) < 0
            or not str(fallback.get("work_started_at") or "")
            or not str(fallback.get("deadline_at") or "")
            or taxonomy.get("terminal_fallback_seconds") != 1800
            or durable_route.get("created") is not False
            or durable_route.get("remediation_attempt_count") != 0
        ):
            return False
        if legacy_execution_identity_terminal and (
            fallback.get("schema_version")
            != "pnc_rca_bounded_terminal_fallback_v1"
            or fallback.get("terminal_class") != "honest_non_attribution"
            or fallback.get("confidence_tier") != "low"
            or str(durable_route.get("owner") or "") != "rca-engineering"
            or str(durable_route.get("status") or "") != "alert_pending"
            or not isinstance(fallback.get("elapsed_seconds"), int)
            or isinstance(fallback.get("elapsed_seconds"), bool)
            or int(fallback.get("elapsed_seconds")) < 0
            or not str(fallback.get("work_started_at") or "")
            or not str(fallback.get("deadline_at") or "")
            or taxonomy.get("terminal_fallback_seconds") != 1800
            or durable_route.get("created") is not False
            or durable_route.get("remediation_attempt_count") != 0
        ):
            return False
        route = conn.execute(
            """
            SELECT route_key, submission_key, business_key, generation,
                   task_id, terminal_error_code, lane, route_kind, owner,
                   status, audit_json, route_payload_json
              FROM rca_failure_routes
             WHERE route_key = ?
            """,
            (route_key,),
        ).fetchone()
        if route is None or (
            str(route["submission_key"] or "") != submission_key
            or str(route["business_key"] or "") != business_key
            or int(route["generation"] or 0) != generation
            or str(route["task_id"] or "") != task_id
            or str(route["terminal_error_code"] or "") != error_code
            or str(route["lane"] or "") != lane
            or str(route["route_kind"] or "") != route_kind
            or str(route["owner"] or "")
            != str(durable_route.get("owner") or "")
            or str(route["status"] or "")
            != str(durable_route.get("status") or "")
        ):
            return False
        try:
            audit = json.loads(str(route["audit_json"] or ""))
            payload = json.loads(str(route["route_payload_json"] or ""))
        except (TypeError, ValueError, json.JSONDecodeError):
            return False
        if (
            not isinstance(audit, dict)
            or audit.get("schema_version") != "pnc_rca_failure_route_audit_v1"
            or not isinstance(payload, dict)
            or payload.get("schema_version")
            != "pnc_rca_failure_route_payload_v1"
        ):
            return False
        if immediate_taxonomy_gap_terminal:
            decision = payload.get("decision")
            blocker = payload.get("blocker")
            if (
                not isinstance(decision, dict)
                or not isinstance(blocker, dict)
                or decision.get("raw_code") != taxonomy_gap_code
                or decision.get("terminal_error_code") != error_code
                or decision.get("known") is not False
                or decision.get("retryable") is not False
                or decision.get("internal_route") != "internal_alert"
                or decision.get("lane") != "hard_defect"
                or blocker.get("kind") != taxonomy_gap_code
                or blocker.get("blocks_attribution") is not True
                or audit.get("source") != "rca_service_result"
                or audit.get("receipt") != receipt
            ):
                return False
        if legacy_gate_a_terminal:
            decision = payload.get("decision")
            blocker = payload.get("blocker")
            if (
                not isinstance(decision, dict)
                or not isinstance(blocker, dict)
                or decision.get("raw_code") != "gate_a_projection_invalid"
                or decision.get("terminal_error_code") != error_code
                or decision.get("known") is not False
                or decision.get("retryable") is not False
                or decision.get("internal_route") != "internal_alert"
                or decision.get("lane") != "hard_defect"
                or decision.get("contract_errors") != ["unknown_blocker_kind"]
                or blocker.get("kind") != "gate_a_projection_invalid"
                or not str(blocker.get("message") or "").startswith(
                    "gate_a_projection_invalid:"
                )
                or audit.get("source") != "delivery_contract_verifier"
                or audit.get("receipt") not in ({}, None)
                or audit.get("contract_errors") != ["unknown_blocker_kind"]
                or not isinstance(audit.get("taxonomy_audit"), dict)
            ):
                return False
        if legacy_execution_identity_terminal:
            decision = payload.get("decision")
            blocker = payload.get("blocker")
            if (
                not isinstance(decision, dict)
                or not isinstance(blocker, dict)
                or decision.get("raw_code")
                != "execution_identity_readback_unavailable"
                or decision.get("terminal_error_code") != error_code
                or decision.get("known") is not False
                or decision.get("retryable") is not False
                or decision.get("internal_route") != "internal_alert"
                or decision.get("lane") != "hard_defect"
                or decision.get("contract_errors") != ["unknown_blocker_kind"]
                or blocker.get("kind")
                != "execution_identity_readback_unavailable"
                or not str(blocker.get("message") or "").startswith(
                    "execution_identity_readback_unavailable:"
                )
                or audit.get("source") != "delivery_contract_verifier"
                or audit.get("receipt") != {}
                or audit.get("contract_errors") != ["unknown_blocker_kind"]
                or not isinstance(audit.get("taxonomy_audit"), dict)
            ):
                return False
        subscriptions = conn.execute(
            """
            SELECT effect_kind, required, status, delivery_id, effect_key,
                   materialized_at
              FROM rca_delivery_subscriptions
             WHERE business_key = ? AND generation = ? AND required = 1
            ORDER BY effect_kind
            """,
            (business_key, generation),
        ).fetchall()
        subscription_kinds = {
            str(row["effect_kind"] or "") for row in subscriptions
        }
        trigger_source = conn.execute(
            """
            SELECT source.source_kind, source.platform, source.chat_id,
                   source.thread_id, source.requester_id, source.mode,
                   source.outcome
              FROM business_triggers AS trigger
              JOIN rca_trigger_sources AS source
                ON source.source_id = trigger.origin_source_id
             WHERE trigger.business_key = ?
               AND trigger.submission_key = ?
               AND trigger.generation = ?
             LIMIT 1
            """,
            (business_key, submission_key, generation),
        ).fetchone()
        issue_only_operator = bool(
            trigger_source is not None
            and str(trigger_source["platform"] or "") == "operator"
            and not str(trigger_source["chat_id"] or "")
            and not str(trigger_source["thread_id"] or "")
        )
        issue_only_kafka = bool(
            trigger_source is not None
            and str(trigger_source["source_kind"] or "")
            == "kafka_workflow_event"
            and not str(trigger_source["platform"] or "")
            and not str(trigger_source["chat_id"] or "")
            and not str(trigger_source["thread_id"] or "")
            and not str(trigger_source["requester_id"] or "")
            and str(trigger_source["mode"] or "") == "issue_created"
            and not str(trigger_source["outcome"] or "")
        )
        if taxonomy_gap_terminal:
            valid_subscription_shape = (
                len(subscriptions) in {1, 2}
                and len(subscription_kinds) == len(subscriptions)
                and "feishu_issue_comment" in subscription_kinds
                and subscription_kinds.issubset(
                    {"feishu_issue_comment", "feishu_thread_reply"}
                )
            )
        elif issue_only_operator or issue_only_kafka:
            valid_subscription_shape = (
                len(subscriptions) == 1
                and subscription_kinds == {"feishu_issue_comment"}
            )
        else:
            valid_subscription_shape = (
                len(subscriptions) == 2
                and subscription_kinds
                == {"feishu_issue_comment", "feishu_thread_reply"}
            )
        if not valid_subscription_shape:
            return False
        if any(
            int(row["required"] or 0) != 1
            or str(row["status"] or "") != "pending"
            or row["delivery_id"] is not None
            or row["effect_key"] is not None
            or row["materialized_at"] is not None
            for row in subscriptions
        ):
            return False
        delivery_job = conn.execute(
            "SELECT 1 FROM rca_delivery_jobs WHERE submission_key = ? LIMIT 1",
            (submission_key,),
        ).fetchone()
        delivery_effect = conn.execute(
            """
            SELECT 1
              FROM rca_delivery_effects AS effect
              JOIN rca_delivery_jobs AS job
                ON job.delivery_id = effect.delivery_id
             WHERE job.submission_key = ?
             LIMIT 1
            """,
            (submission_key,),
        ).fetchone()
        return delivery_job is None and delivery_effect is None

    @classmethod
    def _activation_delivery_execution_complete_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        business_key: str,
        submission_key: str,
        generation: int,
    ) -> bool:
        """Return whether one bound outbox row has fully settled delivery."""
        required_tables = {
            "rca_execution_watch",
            "rca_delivery_jobs",
            "rca_delivery_effects",
            "rca_delivery_subscriptions",
        }
        present = {
            str(row[0])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        if not required_tables.issubset(present):
            return False
        watch = conn.execute(
            """
            SELECT w.state, w.task_id, w.terminal_at, w.delivery_id,
                   w.last_error_code, w.last_status_json,
                   j.status AS job_status,
                   j.delivery_id AS job_delivery_id,
                   j.outcome AS job_outcome,
                   j.terminal_state,
                   j.terminal_error_code
              FROM rca_execution_watch AS w
              LEFT JOIN rca_delivery_jobs AS j
                ON j.delivery_id = w.delivery_id
             WHERE w.business_key = ? AND w.submission_key = ? AND w.generation = ?
             LIMIT 1
            """,
            (business_key, submission_key, generation),
        ).fetchone()
        if watch is None:
            return False
        watch_state = str(watch["state"] or "")
        delivery_id = str(watch["delivery_id"] or "").strip()

        # A collector deadline is a durable, zero-write terminal route.  It
        # has no delivery job by design, but it still drains the activation
        # lineage once the internal failure route is settled.
        if watch_state == "terminal_failed":
            if "rca_failure_routes" not in present:
                return False
            return cls._activation_silent_terminal_complete_tx(
                conn,
                business_key=business_key,
                submission_key=submission_key,
                generation=generation,
                watch=watch,
            )

        effects = conn.execute(
            """
            SELECT s.effect_kind, s.status AS subscription_status,
                   s.delivery_id AS subscription_delivery_id,
                   s.effect_key, e.status AS effect_status
              FROM rca_delivery_subscriptions AS s
              LEFT JOIN rca_delivery_effects AS e
                ON e.effect_key = s.effect_key
             WHERE s.business_key = ? AND s.generation = ? AND s.required = 1
             ORDER BY s.effect_kind
            """,
            (business_key, generation),
        ).fetchall()
        effect_kinds = [str(row["effect_kind"] or "") for row in effects]
        valid_subscription_shape = (
            bool(effects)
            and "feishu_issue_comment" in effect_kinds
            and len(set(effect_kinds)) == len(effect_kinds)
            and set(effect_kinds).issubset(
                {"feishu_issue_comment", "feishu_thread_reply"}
            )
        )

        if watch_state == "quarantined":
            try:
                status = json.loads(str(watch["last_status_json"] or ""))
            except (TypeError, ValueError, json.JSONDecodeError):
                return False
            if (
                delivery_id
                or not isinstance(status, Mapping)
                or status.get("success") is not False
                or status.get("state") != "quarantined"
                or status.get("external_writes") is not False
                or status.get("terminal_delivery_policy")
                != "silent_internal_alert_only"
                or not valid_subscription_shape
                or any(
                    str(row["subscription_status"] or "")
                    not in {"suppressed", "quarantined"}
                    or str(row["subscription_delivery_id"] or "")
                    or str(row["effect_key"] or "")
                    or str(row["effect_status"] or "")
                    for row in effects
                )
            ):
                return False
            historical_job = conn.execute(
                "SELECT 1 FROM rca_delivery_jobs "
                "WHERE business_key = ? AND submission_key = ? AND generation = ? "
                "LIMIT 1",
                (business_key, submission_key, generation),
            ).fetchone()
            return historical_job is None

        if watch_state != "delivery_created" or not delivery_id:
            return False
        job = conn.execute(
            "SELECT status FROM rca_delivery_jobs WHERE delivery_id = ?",
            (delivery_id,),
        ).fetchone()
        if job is None or str(job["status"] or "") not in {
            "delivered",
            "partial",
            "quarantined",
        }:
            return False
        if not valid_subscription_shape:
            return False
        settled = {"succeeded", "suppressed", "quarantined"}
        return all(
            str(row["subscription_status"] or "") in {
                "materialized",
                "suppressed",
                "quarantined",
            }
            and (
                str(row["subscription_status"] or "")
                in {"suppressed", "quarantined"}
                or (
                    str(row["subscription_delivery_id"] or "") == delivery_id
                    and str(row["effect_key"] or "")
                    and str(row["effect_status"] or "") in settled
                )
            )
            for row in effects
        )
    @staticmethod
    def _activation_admission_key(
        *,
        source_kind: str,
        source_identity_sha256: str,
        business_key: str,
        submission_key: str,
        generation: int,
    ) -> str:
        return _canonical_sha256(
            {
                "business_key": business_key,
                "generation": generation,
                "source_identity_sha256": source_identity_sha256,
                "source_kind": source_kind,
                "submission_key": submission_key,
            }
        )

    @staticmethod
    def _write_activation_ledger_tx(
        conn: sqlite3.Connection,
        *,
        epoch_id: str,
        admission_key: str,
        entrypoint: str,
        source_kind: str,
        source_identity_sha256: str,
        decision: str,
        reason: str,
        business_key: str,
        submission_key: str,
        generation: int,
        current: str,
    ) -> tuple[int, str]:
        row = conn.execute(
            """
            SELECT * FROM rca_activation_admission_ledger
             WHERE epoch_id = ? AND admission_key = ?
            """,
            (epoch_id, admission_key),
        ).fetchone()
        if row is None:
            cursor = conn.execute(
                """
                INSERT INTO rca_activation_admission_ledger(
                    epoch_id, admission_key, entrypoint, source_kind,
                    source_identity_sha256, decision, reason,
                    business_key, submission_key, generation,
                    first_adjudicated_at, last_adjudicated_at, admitted_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    epoch_id,
                    admission_key,
                    entrypoint,
                    source_kind,
                    source_identity_sha256,
                    decision,
                    reason,
                    business_key,
                    submission_key,
                    generation,
                    current,
                    current,
                    current if decision == "admit" else None,
                ),
            )
            if cursor.lastrowid is None:
                raise ActivationEpochError("activation_ledger_insert_failed")
            return int(cursor.lastrowid), ""
        immutable = (
            str(row["source_kind"]),
            str(row["source_identity_sha256"]),
            str(row["business_key"]),
            str(row["submission_key"]),
            int(row["generation"]),
        )
        expected = (
            source_kind,
            source_identity_sha256,
            business_key,
            submission_key,
            generation,
        )
        if immutable != expected:
            raise ActivationEpochError("activation_ledger_identity_conflict")
        prior_decision = str(row["decision"])
        if prior_decision == "admit":
            conn.execute(
                """
                UPDATE rca_activation_admission_ledger
                   SET adjudication_count = adjudication_count + 1,
                       last_adjudicated_at = ?
                 WHERE ledger_id = ?
                """,
                (current, row["ledger_id"]),
            )
            return int(row["ledger_id"]), prior_decision
        conn.execute(
            """
            UPDATE rca_activation_admission_ledger
               SET entrypoint = ?, decision = ?, reason = ?,
                   adjudication_count = adjudication_count + 1,
                   last_adjudicated_at = ?,
                   admitted_at = CASE WHEN ? = 'admit' THEN ? ELSE admitted_at END
             WHERE ledger_id = ?
            """,
            (
                entrypoint,
                decision,
                reason,
                current,
                decision,
                current,
                row["ledger_id"],
            ),
        )
        return int(row["ledger_id"]), prior_decision

    @classmethod
    def adjudicate_activation_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        entrypoint: str,
        source_kind: str,
        source_identity: Mapping[str, Any],
        business_key: str,
        submission_key: str,
        generation: int,
        new_execution: bool,
        ingress_epoch_id: str | None = None,
        now: datetime | None = None,
    ) -> ActivationAdmissionDecision:
        """Admit or join one execution under the exact current steady epoch."""
        if not conn.in_transaction:
            raise ActivationEpochError("activation_transaction_required")
        point = str(entrypoint or "").strip()
        kind = str(source_kind or "").strip()
        if point not in ACTIVATION_ENTRYPOINTS:
            raise ActivationEpochError("activation_entrypoint_invalid")
        expected_kind = "manual" if point == "manual_admit" else "kafka"
        if kind != expected_kind:
            raise ActivationEpochError("activation_entrypoint_source_mismatch")
        source_sha, _normalized_source = cls._normalize_activation_source_identity(
            kind, source_identity
        )
        business = str(business_key or "").strip()
        submission = str(submission_key or "").strip()
        if (
            not business
            or not submission
            or len(business) > 500
            or len(submission) > 500
        ):
            raise ActivationEpochError("activation_execution_identity_invalid")
        if isinstance(generation, bool):
            raise ActivationEpochError("activation_generation_invalid")
        try:
            generation_number = int(generation)
        except (TypeError, ValueError) as exc:
            raise ActivationEpochError("activation_generation_invalid") from exc
        if generation_number < 1:
            raise ActivationEpochError("activation_generation_invalid")
        if not isinstance(new_execution, bool):
            raise ActivationEpochError("activation_adjudication_flag_invalid")

        epoch = cls._current_activation_epoch_tx(conn)
        if epoch is None or str(epoch["state"] or "") != "steady_active":
            raise ActivationEpochError("activation_steady_epoch_required")
        epoch_id = str(epoch["epoch_id"])
        if ingress_epoch_id is not None and str(ingress_epoch_id) != epoch_id:
            raise ActivationEpochError("activation_ingress_epoch_changed")

        admission_key = cls._activation_admission_key(
            source_kind=kind,
            source_identity_sha256=source_sha,
            business_key=business,
            submission_key=submission,
            generation=generation_number,
        )
        existing_trigger = conn.execute(
            """
            SELECT business_key, generation
              FROM business_triggers
             WHERE submission_key = ?
            """,
            (submission,),
        ).fetchone()
        current = _iso(now)
        if existing_trigger is not None:
            if (
                str(existing_trigger["business_key"]) != business
                or int(existing_trigger["generation"]) != generation_number
            ):
                raise ActivationEpochError("activation_join_identity_conflict")
            ledger_id, _prior = cls._write_activation_ledger_tx(
                conn,
                epoch_id=epoch_id,
                admission_key=admission_key,
                entrypoint=point,
                source_kind=kind,
                source_identity_sha256=source_sha,
                decision="join",
                reason="activation_existing_generation_join",
                business_key=business,
                submission_key=submission,
                generation=generation_number,
                current=current,
            )
            return ActivationAdmissionDecision(
                epoch_id=epoch_id,
                epoch_state="steady_active",
                decision="join",
                reason="activation_existing_generation_join",
                ledger_id=ledger_id,
            )
        if not new_execution:
            raise ActivationEpochError("activation_join_target_missing")

        ledger_id, prior = cls._write_activation_ledger_tx(
            conn,
            epoch_id=epoch_id,
            admission_key=admission_key,
            entrypoint=point,
            source_kind=kind,
            source_identity_sha256=source_sha,
            decision="admit",
            reason="activation_steady_active",
            business_key=business,
            submission_key=submission,
            generation=generation_number,
            current=current,
        )
        return ActivationAdmissionDecision(
            epoch_id=epoch_id,
            epoch_state="steady_active",
            decision="admit",
            reason=(
                "activation_admission_idempotent"
                if prior == "admit"
                else "activation_steady_active"
            ),
            ledger_id=ledger_id,
        )

    @classmethod
    def bind_activation_admission_tx(
        cls,
        conn: sqlite3.Connection,
        decision: ActivationAdmissionDecision,
        *,
        business_key: str,
        submission_key: str,
        generation: int,
        now: datetime | None = None,
    ) -> None:
        """Bind a newly created execution to its adjudication before commit."""
        if not conn.in_transaction:
            raise ActivationEpochError("activation_transaction_required")
        if decision.decision != "admit":
            raise ActivationEpochError("activation_noncreating_decision_cannot_bind")
        if not decision.epoch_id or decision.ledger_id is None:
            raise ActivationEpochError("activation_binding_identity_missing")
        ledger = conn.execute(
            """
            SELECT * FROM rca_activation_admission_ledger
             WHERE ledger_id = ? AND epoch_id = ?
            """,
            (decision.ledger_id, decision.epoch_id),
        ).fetchone()
        if ledger is None:
            raise ActivationEpochError("activation_binding_ledger_missing")
        expected_identity = (
            str(business_key),
            str(submission_key),
            int(generation),
        )
        observed = (
            str(ledger["business_key"]),
            str(ledger["submission_key"]),
            int(ledger["generation"]),
        )
        if (
            observed != expected_identity
            or str(ledger["decision"]) != "admit"
        ):
            raise ActivationEpochError("activation_binding_ledger_conflict")
        epoch = cls._current_activation_epoch_tx(conn)
        if (
            epoch is None
            or str(epoch["epoch_id"]) != decision.epoch_id
            or str(epoch["state"] or "") != "steady_active"
            or int(epoch["is_current"] or 0) != 1
        ):
            raise ActivationEpochError("activation_steady_epoch_required")
        execution_key = (
            str(business_key),
            int(generation),
            str(submission_key),
        )
        trigger = conn.execute(
            """
            SELECT activation_epoch_id, activation_ledger_id
              FROM business_triggers
             WHERE business_key = ? AND generation = ? AND submission_key = ?
            """,
            execution_key,
        ).fetchone()
        outbox = conn.execute(
            """
            SELECT activation_epoch_id, activation_ledger_id
              FROM rca_outbox
             WHERE business_key = ? AND generation = ? AND submission_key = ?
            """,
            execution_key,
        ).fetchone()
        if trigger is None or outbox is None:
            raise ActivationEpochError("activation_binding_execution_missing")
        expected_binding = (decision.epoch_id, decision.ledger_id)
        for row in (trigger, outbox):
            observed_binding = (
                str(row["activation_epoch_id"] or ""),
                int(row["activation_ledger_id"] or 0),
            )
            if observed_binding not in {("", 0), expected_binding}:
                raise ActivationEpochError("activation_binding_conflict")
        conn.execute(
            """
            UPDATE business_triggers
               SET activation_epoch_id = ?, activation_ledger_id = ?
             WHERE business_key = ? AND generation = ? AND submission_key = ?
            """,
            (decision.epoch_id, decision.ledger_id, *execution_key),
        )
        conn.execute(
            """
            UPDATE rca_outbox
               SET activation_epoch_id = ?, activation_ledger_id = ?
             WHERE business_key = ? AND generation = ? AND submission_key = ?
            """,
            (decision.epoch_id, decision.ledger_id, *execution_key),
        )
        conn.execute(
            """
            UPDATE rca_activation_admission_ledger SET bound_at = ?
             WHERE ledger_id = ? AND epoch_id = ?
            """,
            (_iso(now), decision.ledger_id, decision.epoch_id),
        )

    @staticmethod
    def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
        return conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table,),
        ).fetchone() is not None

    @classmethod
    def _learning_delivery_schema_present(cls, conn: sqlite3.Connection) -> bool:
        required = {
            "rca_delivery_jobs": {"delivery_id", "target_key", "work_item_id"},
            "rca_delivery_effects": {
                "delivery_id",
                "effect_kind",
                "target_key",
                "status",
                "payload_json",
                "created_at",
            },
        }
        for table, columns in required.items():
            if not cls._table_exists(conn, table):
                return False
            observed = {
                str(row["name"])
                for row in conn.execute(f"PRAGMA table_info({table})")
            }
            if not columns.issubset(observed):
                return False
        return True

    @staticmethod
    def _learning_cutoff_datetime() -> datetime:
        value = datetime.fromisoformat(STOCK_CUTOFF)
        return value.astimezone(timezone.utc)

    @staticmethod
    def _learning_stock_ids_tx(conn: sqlite3.Connection) -> list[str]:
        """Derive the stock set from already-settled user-visible results."""
        if not (
            RcaControlStore._table_exists(conn, "rca_delivery_jobs")
            and RcaControlStore._table_exists(conn, "rca_delivery_effects")
        ):
            return []
        required = {
            "rca_delivery_jobs": {"delivery_id", "target_key", "work_item_id"},
            "rca_delivery_effects": {
                "delivery_id", "effect_kind", "target_key", "status",
                "payload_json", "created_at",
            },
        }
        for table, columns in required.items():
            observed = {
                str(row["name"])
                for row in conn.execute(f"PRAGMA table_info({table})")
            }
            if not columns.issubset(observed):
                return []
        cutoff = RcaControlStore._learning_cutoff_datetime()
        rows = conn.execute(
            """
            SELECT j.work_item_id, j.target_key AS job_target_key,
                   e.target_key, e.status, e.payload_json, e.created_at
              FROM rca_delivery_effects AS e
              JOIN rca_delivery_jobs AS j ON j.delivery_id = e.delivery_id
             WHERE e.effect_kind = 'feishu_issue_comment'
               AND e.status = 'succeeded'
            """
        ).fetchall()
        ids: set[str] = set()
        for row in rows:
            if str(row["target_key"] or "") != str(row["job_target_key"] or ""):
                continue
            target = str(row["target_key"] or "")
            try:
                payload = json.loads(str(row["payload_json"] or ""))
            except (TypeError, ValueError, json.JSONDecodeError):
                payload = {}
            if not isinstance(payload, Mapping):
                payload = {}
            if str(payload.get("schema_version") or "") in {
                "pnc_rca_conclusion_adjudication_effect_v1",
                "pnc_rca_conclusion_adjudication_effect_v2",
            } or target.startswith("g1q3-rca-adjudication-target-v1-"):
                continue
            try:
                created = datetime.fromisoformat(
                    str(row["created_at"]).replace("Z", "+00:00")
                )
                if created.tzinfo is None or created.utcoffset() is None:
                    continue
                created = created.astimezone(timezone.utc)
            except (TypeError, ValueError, OverflowError):
                continue
            if created <= cutoff:
                item_id = str(row["work_item_id"] or "").strip()
                if item_id:
                    ids.add(item_id)
        return sorted(ids)

    @staticmethod
    def _learning_stock_digest(work_item_ids: Iterable[str]) -> str:
        normalized = sorted({str(item).strip() for item in work_item_ids if str(item).strip()})
        return hashlib.sha256("\n".join(normalized).encode("utf-8")).hexdigest()

    @classmethod
    def _ensure_learning_lane_cohort_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        sealed_at: str,
    ) -> sqlite3.Row:
        existing = conn.execute(
            "SELECT * FROM rca_learning_lane_cohorts LIMIT 1"
        ).fetchone()
        if existing is not None:
            if str(existing["stock_cutoff"]) != STOCK_CUTOFF:
                raise RecordConflictError("learning_lane_cohort_cutoff_mismatch")
            item_ids = [
                str(row["work_item_id"])
                for row in conn.execute(
                    "SELECT work_item_id FROM rca_learning_lane_stock_items "
                    "WHERE cohort_id = ? ORDER BY work_item_id",
                    (existing["cohort_id"],),
                ).fetchall()
            ]
            item_count = len(item_ids)
            digest = cls._learning_stock_digest(item_ids)
            if digest != str(existing["stock_ids_sha256"]):
                raise RecordConflictError("learning_lane_cohort_digest_mismatch")
            if item_count != int(existing["stock_count"]):
                raise RecordConflictError("learning_lane_cohort_count_mismatch")
            return existing
        work_item_ids = cls._learning_stock_ids_tx(conn)
        digest = cls._learning_stock_digest(work_item_ids)
        material = _canonical_json(
            {
                "schema_version": LEARNING_LANE_COHORT_SCHEMA_VERSION,
                "stock_cutoff": STOCK_CUTOFF,
                "stock_count": len(work_item_ids),
                "stock_ids_sha256": digest,
            }
        )
        cohort_id = "g1q3-rca-stock-cohort-v1-" + hashlib.sha256(
            material.encode("utf-8")
        ).hexdigest()
        conn.execute(
            """
            INSERT INTO rca_learning_lane_cohorts(
                cohort_id, schema_version, stock_cutoff, stock_count,
                stock_ids_sha256, sealed_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                cohort_id,
                LEARNING_LANE_COHORT_SCHEMA_VERSION,
                STOCK_CUTOFF,
                len(work_item_ids),
                digest,
                sealed_at,
            ),
        )
        conn.executemany(
            "INSERT INTO rca_learning_lane_stock_items(cohort_id, work_item_id) VALUES (?, ?)",
            [(cohort_id, item) for item in work_item_ids],
        )
        row = conn.execute(
            "SELECT * FROM rca_learning_lane_cohorts WHERE cohort_id = ?",
            (cohort_id,),
        ).fetchone()
        if row is None:
            raise RecordConflictError("learning_lane_cohort_missing_after_seal")
        return row

    @classmethod
    def _ensure_learning_lane_admission_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        admission: RcaAdmission,
        current: str,
    ) -> bool:
        """Bind a post-cutoff stock generation to the sealed learning lane."""
        observed_at = datetime.fromisoformat(
            str(current).replace("Z", "+00:00")
        ).astimezone(timezone.utc)
        if observed_at <= cls._learning_cutoff_datetime():
            return False
        cohort = cls._ensure_learning_lane_cohort_tx(conn, sealed_at=current)
        member = conn.execute(
            """
            SELECT 1 FROM rca_learning_lane_stock_items
             WHERE cohort_id = ? AND work_item_id = ?
            """,
            (cohort["cohort_id"], admission.source_refs.work_item_id),
        ).fetchone()
        if member is None:
            return False
        existing = conn.execute(
            """
            SELECT * FROM rca_learning_lane_admissions
             WHERE business_key = ? AND generation = ?
            """,
            (admission.business_key, admission.generation),
        ).fetchone()
        values = {
            "business_key": admission.business_key,
            "generation": admission.generation,
            "work_item_id": admission.source_refs.work_item_id,
            "schema_version": LEARNING_LANE_ADMISSION_SCHEMA_VERSION,
            "lane": "learning",
            "reason": "stock",
            "external_write_allowed": 0,
            "cohort_id": str(cohort["cohort_id"]),
            "stock_cutoff": str(cohort["stock_cutoff"]),
            "stock_ids_sha256": str(cohort["stock_ids_sha256"]),
            "admitted_at": current,
        }
        if existing is not None:
            if any(existing[name] != value for name, value in values.items() if name != "admitted_at"):
                raise RecordConflictError("learning_lane_admission_binding_mismatch")
            return True
        conn.execute(
            """
            INSERT INTO rca_learning_lane_admissions(
                business_key, generation, work_item_id, schema_version, lane,
                reason, external_write_allowed, cohort_id, stock_cutoff,
                stock_ids_sha256, admitted_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            tuple(values[name] for name in (
                "business_key", "generation", "work_item_id", "schema_version",
                "lane", "reason", "external_write_allowed", "cohort_id",
                "stock_cutoff", "stock_ids_sha256", "admitted_at",
            )),
        )
        return True

    @staticmethod
    def _terminal_rerun_delivery_authority_values(
        *,
        authority_kind: str,
        authority: Mapping[str, Any],
        source_id: str,
        outbox_id: int,
        source_payload_sha256: str,
        admission: RcaAdmission,
        activation_epoch_id: str,
        activation_ledger_id: int,
        current: str,
    ) -> dict[str, Any]:
        if authority_kind not in TERMINAL_RERUN_DELIVERY_AUTHORITY_KINDS:
            raise RecordConflictError("terminal_rerun_authority_kind_invalid")
        normalized = dict(authority)
        expected = (
            build_silent_terminal_rerun_authority(
                batch_id=normalized.get("batch_id"),
                queue_sha256=normalized.get("queue_sha256"),
                issue_id=normalized.get("issue_id"),
                prior_submission_key=normalized.get("prior_submission_key"),
                prior_generation=normalized.get("prior_generation"),
                owner_receipt_path=normalized.get("owner_receipt_path"),
                owner_receipt_sha256=normalized.get("owner_receipt_sha256"),
                requester_id=normalized.get("requester_id"),
                reason=normalized.get("reason"),
                activation_required=normalized.get("activation_required"),
            )
            if authority_kind == "silent_terminal"
            else build_batch_terminal_rerun_authority(
                batch_id=normalized.get("batch_id"),
                queue_sha256=normalized.get("queue_sha256"),
                issue_id=normalized.get("issue_id"),
                prior_submission_key=normalized.get("prior_submission_key"),
                prior_generation=normalized.get("prior_generation"),
                prior_delivery_id=normalized.get("prior_delivery_id"),
                owner_receipt_path=normalized.get("owner_receipt_path"),
                owner_receipt_sha256=normalized.get("owner_receipt_sha256"),
                requester_id=normalized.get("requester_id"),
                reason=normalized.get("reason"),
                activation_required=normalized.get("activation_required"),
            )
        )
        if normalized != expected:
            raise RecordConflictError("terminal_rerun_authority_invalid")
        if (
            str(expected["issue_id"]) != admission.source_refs.work_item_id
            or int(admission.generation) != int(expected["prior_generation"]) + 1
        ):
            raise RecordConflictError("terminal_rerun_authority_admission_mismatch")
        return {
            "authority_sha256": str(expected["selection_sha256"]),
            "schema_version": TERMINAL_RERUN_DELIVERY_AUTHORITY_SCHEMA_VERSION,
            "authority_kind": authority_kind,
            "source_id": str(source_id),
            "outbox_id": int(outbox_id),
            "source_payload_sha256": str(source_payload_sha256),
            "business_key": admission.business_key,
            "generation": admission.generation,
            "submission_key": admission.submission_key,
            "activation_epoch_id": str(activation_epoch_id),
            "activation_ledger_id": int(activation_ledger_id),
            "effect_kind": "feishu_issue_comment",
            "project_key": admission.source_refs.project_key,
            "project_simple_name": admission.source_refs.project_simple_name,
            "work_item_type_key": admission.source_refs.work_item_type_key,
            "issue_id": str(expected["issue_id"]),
            "batch_id": str(expected["batch_id"]),
            "prior_submission_key": str(expected["prior_submission_key"]),
            "prior_generation": int(expected["prior_generation"]),
            "prior_delivery_id": str(expected.get("prior_delivery_id") or ""),
            "queue_sha256": str(expected["queue_sha256"]),
            "owner_receipt_path": str(expected["owner_receipt_path"]),
            "owner_receipt_sha256": str(expected["owner_receipt_sha256"]),
            "requester_id": str(expected["requester_id"]),
            "reason": str(expected["reason"]),
            "activation_required": 1,
            "authority_json": _canonical_json(expected),
            "created_at": str(current),
        }

    @classmethod
    def _persist_terminal_rerun_delivery_authority_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        authority_kind: str,
        authority: Mapping[str, Any],
        source_id: str,
        source_payload_sha256: str,
        admission: RcaAdmission,
        current: str,
    ) -> None:
        if not conn.in_transaction:
            raise RecordConflictError("terminal_rerun_authority_transaction_required")
        execution = conn.execute(
            """
            SELECT outbox.outbox_id, trigger.activation_epoch_id,
                   trigger.activation_ledger_id
              FROM business_triggers AS trigger
              JOIN rca_outbox AS outbox
                ON outbox.business_key = trigger.business_key
               AND outbox.generation = trigger.generation
               AND outbox.submission_key = trigger.submission_key
               AND outbox.activation_epoch_id = trigger.activation_epoch_id
               AND outbox.activation_ledger_id = trigger.activation_ledger_id
             WHERE trigger.business_key = ?
               AND trigger.generation = ?
               AND trigger.submission_key = ?
            """,
            (
                admission.business_key,
                admission.generation,
                admission.submission_key,
            ),
        ).fetchone()
        if (
            execution is None
            or not str(execution["activation_epoch_id"] or "").strip()
            or execution["activation_ledger_id"] is None
        ):
            raise RecordConflictError("terminal_rerun_authority_activation_missing")
        values = cls._terminal_rerun_delivery_authority_values(
            authority_kind=authority_kind,
            authority=authority,
            source_id=source_id,
            outbox_id=int(execution["outbox_id"]),
            source_payload_sha256=source_payload_sha256,
            admission=admission,
            activation_epoch_id=str(execution["activation_epoch_id"]),
            activation_ledger_id=int(execution["activation_ledger_id"]),
            current=current,
        )
        columns = tuple(values)
        conn.execute(
            "INSERT INTO rca_terminal_rerun_delivery_authorities("
            + ", ".join(columns)
            + ") VALUES ("
            + ", ".join("?" for _ in columns)
            + ")",
            tuple(values[column] for column in columns),
        )

    @classmethod
    def _require_terminal_rerun_delivery_authority_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        authority_kind: str,
        authority: Mapping[str, Any],
        source_id: str,
        source_payload_sha256: str,
        admission: RcaAdmission,
    ) -> None:
        execution = conn.execute(
            "SELECT outbox.outbox_id, trigger.activation_epoch_id, "
            "trigger.activation_ledger_id FROM business_triggers AS trigger "
            "JOIN rca_outbox AS outbox ON outbox.business_key=trigger.business_key "
            "AND outbox.generation=trigger.generation "
            "AND outbox.submission_key=trigger.submission_key "
            "WHERE trigger.business_key = ? AND trigger.generation = ? "
            "AND trigger.submission_key = ?",
            (
                admission.business_key,
                admission.generation,
                admission.submission_key,
            ),
        ).fetchone()
        if (
            execution is None
            or not str(execution["activation_epoch_id"] or "").strip()
            or execution["activation_ledger_id"] is None
        ):
            raise RecordConflictError("terminal_rerun_authority_activation_missing")
        expected = cls._terminal_rerun_delivery_authority_values(
            authority_kind=authority_kind,
            authority=authority,
            source_id=source_id,
            outbox_id=int(execution["outbox_id"]),
            source_payload_sha256=source_payload_sha256,
            admission=admission,
            activation_epoch_id=str(execution["activation_epoch_id"]),
            activation_ledger_id=int(execution["activation_ledger_id"]),
            current="",
        )
        row = conn.execute(
            "SELECT * FROM rca_terminal_rerun_delivery_authorities "
            "WHERE source_id = ?",
            (source_id,),
        ).fetchone()
        if row is None or any(
            row[name] != value
            for name, value in expected.items()
            if name != "created_at"
        ):
            raise RecordConflictError("terminal_rerun_authority_replay_mismatch")

    @staticmethod
    def _historical_epoch_rerun_delivery_authority_values(
        *,
        authority: Mapping[str, Any],
        source_id: str,
        outbox_id: int,
        source_payload_sha256: str,
        admission: RcaAdmission,
        activation_epoch_id: str,
        activation_ledger_id: int,
        current: str,
    ) -> dict[str, Any]:
        normalized = dict(authority)
        expected = build_historical_epoch_rerun_authority(
            batch_id=normalized.get("batch_id"),
            queue_sha256=normalized.get("queue_sha256"),
            issue_id=normalized.get("issue_id"),
            prior_submission_key=normalized.get("prior_submission_key"),
            prior_generation=normalized.get("prior_generation"),
            prior_activation_epoch_id=normalized.get("prior_activation_epoch_id"),
            prior_activation_ledger_id=normalized.get("prior_activation_ledger_id"),
            target_activation_epoch_id=normalized.get("target_activation_epoch_id"),
            owner_receipt_path=normalized.get("owner_receipt_path"),
            owner_receipt_sha256=normalized.get("owner_receipt_sha256"),
            requester_id=normalized.get("requester_id"),
            reason=normalized.get("reason"),
            activation_required=normalized.get("activation_required"),
        )
        if normalized != expected:
            raise RecordConflictError("historical_epoch_rerun_authority_invalid")
        if (
            str(expected["issue_id"]) != admission.source_refs.work_item_id
            or int(admission.generation) != int(expected["prior_generation"]) + 1
            or str(expected["target_activation_epoch_id"]) != activation_epoch_id
        ):
            raise RecordConflictError(
                "historical_epoch_rerun_authority_admission_mismatch"
            )
        return {
            "authority_sha256": str(expected["selection_sha256"]),
            "schema_version": (
                HISTORICAL_EPOCH_RERUN_DELIVERY_AUTHORITY_SCHEMA_VERSION
            ),
            "source_id": str(source_id),
            "outbox_id": int(outbox_id),
            "source_payload_sha256": str(source_payload_sha256),
            "business_key": admission.business_key,
            "generation": admission.generation,
            "submission_key": admission.submission_key,
            "activation_epoch_id": str(activation_epoch_id),
            "activation_ledger_id": int(activation_ledger_id),
            "effect_kind": "feishu_issue_comment",
            "project_key": admission.source_refs.project_key,
            "project_simple_name": admission.source_refs.project_simple_name,
            "work_item_type_key": admission.source_refs.work_item_type_key,
            "issue_id": str(expected["issue_id"]),
            "batch_id": str(expected["batch_id"]),
            "prior_submission_key": str(expected["prior_submission_key"]),
            "prior_generation": int(expected["prior_generation"]),
            "prior_activation_epoch_id": str(expected["prior_activation_epoch_id"]),
            "prior_activation_ledger_id": expected["prior_activation_ledger_id"],
            "target_activation_epoch_id": str(expected["target_activation_epoch_id"]),
            "queue_sha256": str(expected["queue_sha256"]),
            "owner_receipt_path": str(expected["owner_receipt_path"]),
            "owner_receipt_sha256": str(expected["owner_receipt_sha256"]),
            "requester_id": str(expected["requester_id"]),
            "reason": str(expected["reason"]),
            "activation_required": 1,
            "authority_json": _canonical_json(expected),
            "created_at": str(current),
        }

    @classmethod
    def _persist_historical_epoch_rerun_delivery_authority_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        authority: Mapping[str, Any],
        source_id: str,
        source_payload_sha256: str,
        admission: RcaAdmission,
        current: str,
    ) -> None:
        if not conn.in_transaction:
            raise RecordConflictError(
                "historical_epoch_rerun_authority_transaction_required"
            )
        execution = conn.execute(
            """
            SELECT outbox.outbox_id, trigger.activation_epoch_id,
                   trigger.activation_ledger_id
              FROM business_triggers AS trigger
              JOIN rca_outbox AS outbox
                ON outbox.business_key = trigger.business_key
               AND outbox.generation = trigger.generation
               AND outbox.submission_key = trigger.submission_key
               AND outbox.activation_epoch_id = trigger.activation_epoch_id
               AND outbox.activation_ledger_id = trigger.activation_ledger_id
             WHERE trigger.business_key = ?
               AND trigger.generation = ?
               AND trigger.submission_key = ?
            """,
            (
                admission.business_key,
                admission.generation,
                admission.submission_key,
            ),
        ).fetchone()
        if (
            execution is None
            or not str(execution["activation_epoch_id"] or "").strip()
            or execution["activation_ledger_id"] is None
        ):
            raise RecordConflictError(
                "historical_epoch_rerun_authority_activation_missing"
            )
        values = cls._historical_epoch_rerun_delivery_authority_values(
            authority=authority,
            source_id=source_id,
            outbox_id=int(execution["outbox_id"]),
            source_payload_sha256=source_payload_sha256,
            admission=admission,
            activation_epoch_id=str(execution["activation_epoch_id"]),
            activation_ledger_id=int(execution["activation_ledger_id"]),
            current=current,
        )
        columns = tuple(values)
        conn.execute(
            "INSERT INTO rca_historical_epoch_rerun_delivery_authorities("
            + ", ".join(columns)
            + ") VALUES ("
            + ", ".join("?" for _ in columns)
            + ")",
            tuple(values[column] for column in columns),
        )

    @classmethod
    def _require_historical_epoch_rerun_delivery_authority_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        authority: Mapping[str, Any],
        source_id: str,
        source_payload_sha256: str,
        admission: RcaAdmission,
    ) -> None:
        execution = conn.execute(
            "SELECT outbox.outbox_id, trigger.activation_epoch_id, "
            "trigger.activation_ledger_id FROM business_triggers AS trigger "
            "JOIN rca_outbox AS outbox ON outbox.business_key=trigger.business_key "
            "AND outbox.generation=trigger.generation "
            "AND outbox.submission_key=trigger.submission_key "
            "WHERE trigger.business_key = ? AND trigger.generation = ? "
            "AND trigger.submission_key = ?",
            (
                admission.business_key,
                admission.generation,
                admission.submission_key,
            ),
        ).fetchone()
        if (
            execution is None
            or not str(execution["activation_epoch_id"] or "").strip()
            or execution["activation_ledger_id"] is None
        ):
            raise RecordConflictError(
                "historical_epoch_rerun_authority_activation_missing"
            )
        expected = cls._historical_epoch_rerun_delivery_authority_values(
            authority=authority,
            source_id=source_id,
            outbox_id=int(execution["outbox_id"]),
            source_payload_sha256=source_payload_sha256,
            admission=admission,
            activation_epoch_id=str(execution["activation_epoch_id"]),
            activation_ledger_id=int(execution["activation_ledger_id"]),
            current="",
        )
        row = conn.execute(
            "SELECT * FROM rca_historical_epoch_rerun_delivery_authorities "
            "WHERE source_id = ?",
            (source_id,),
        ).fetchone()
        if row is None or any(
            row[name] != value
            for name, value in expected.items()
            if name != "created_at"
        ):
            raise RecordConflictError(
                "historical_epoch_rerun_authority_replay_mismatch"
            )

    def seal_learning_lane_cohort(
        self, *, now: datetime | None = None
    ) -> dict[str, Any]:
        """Seal and return the stock cohort; repeated calls are read-only."""
        current = _iso(now)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = self._ensure_learning_lane_cohort_tx(conn, sealed_at=current)
            conn.commit()
            return {
                "cohort_id": str(row["cohort_id"]),
                "schema_version": str(row["schema_version"]),
                "stock_cutoff": str(row["stock_cutoff"]),
                "stock_count": int(row["stock_count"]),
                "stock_ids_sha256": str(row["stock_ids_sha256"]),
                "sealed_at": str(row["sealed_at"]),
            }
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def learning_lane_cohort(self) -> dict[str, Any] | None:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM rca_learning_lane_cohorts LIMIT 1"
            ).fetchone()
            return dict(row) if row is not None else None
        finally:
            conn.close()

    def learning_lane_admission(
        self, business_key: str, generation: int
    ) -> dict[str, Any] | None:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM rca_learning_lane_admissions "
                "WHERE business_key = ? AND generation = ?",
                (business_key, generation),
            ).fetchone()
            return dict(row) if row is not None else None
        finally:
            conn.close()

    @classmethod
    def _migrate_source_neutral_parents(cls, conn: sqlite3.Connection) -> None:
        """Atomically remove Kafka-only parent constraints while preserving rows."""
        if not (
            cls._table_exists(conn, "business_triggers")
            and cls._table_exists(conn, "rca_outbox")
        ):
            return
        trigger_columns = {
            str(row["name"]): int(row["notnull"])
            for row in conn.execute("PRAGMA table_info(business_triggers)")
        }
        outbox_columns = {
            str(row["name"]): int(row["notnull"])
            for row in conn.execute("PRAGMA table_info(rca_outbox)")
        }
        kafka_bound = (
            trigger_columns.get("source_event_id") == 1
            or outbox_columns.get("source_event_id") == 1
            or any(
                str(row["table"]) == "kafka_inbox"
                for row in conn.execute("PRAGMA foreign_key_list(business_triggers)")
            )
            or any(
                str(row["table"]) == "kafka_inbox"
                for row in conn.execute("PRAGMA foreign_key_list(rca_outbox)")
            )
        )
        if not kafka_bound:
            return
        try:
            conn.executescript(
                """
                BEGIN IMMEDIATE;
                DROP VIEW IF EXISTS
                    rca_owner_authorized_rerun_delivery_authorities;
                DROP TRIGGER IF EXISTS
                    trg_historical_epoch_rerun_delivery_authority_no_update;
                DROP TRIGGER IF EXISTS
                    trg_historical_epoch_rerun_delivery_authority_no_delete;
                DROP TRIGGER IF EXISTS
                    trg_historical_epoch_rerun_delivery_authority_no_replace;
                DROP TRIGGER IF EXISTS
                    trg_historical_epoch_rerun_delivery_authority_projection_guard;
                DROP TRIGGER IF EXISTS
                    trg_historical_epoch_rerun_delivery_authority_binding_guard;
                CREATE TABLE business_triggers_v6 (
                    business_key TEXT NOT NULL,
                    generation INTEGER NOT NULL CHECK (generation >= 1),
                    submission_key TEXT NOT NULL UNIQUE,
                    creation_rule_version TEXT NOT NULL,
                    work_item_id TEXT NOT NULL,
                    project_key TEXT NOT NULL,
                    work_item_type_key TEXT NOT NULL,
                    origin_source_id TEXT,
                    source_event_id TEXT,
                    source_topic TEXT,
                    source_partition INTEGER,
                    source_offset INTEGER,
                    normalized_json TEXT NOT NULL,
                    state TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (business_key, generation)
                );
                INSERT INTO business_triggers_v6(
                    business_key, generation, submission_key, creation_rule_version,
                    work_item_id, project_key, work_item_type_key, origin_source_id,
                    source_event_id, source_topic, source_partition, source_offset,
                    normalized_json, state, created_at
                )
                    SELECT business_key, generation, submission_key, creation_rule_version,
                           work_item_id, project_key, work_item_type_key, NULL,
                           source_event_id, source_topic, source_partition, source_offset,
                           normalized_json, state, created_at
                      FROM business_triggers;

                CREATE TABLE rca_outbox_v6 (
                    outbox_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    action TEXT NOT NULL,
                    business_key TEXT NOT NULL,
                    submission_key TEXT NOT NULL UNIQUE,
                    creation_rule_version TEXT NOT NULL,
                    generation INTEGER NOT NULL,
                    origin_source_id TEXT,
                    source_event_id TEXT,
                    source_topic TEXT,
                    source_partition INTEGER,
                    source_offset INTEGER,
                    payload_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    attempt INTEGER NOT NULL DEFAULT 0,
                    next_attempt_at TEXT,
                    fence INTEGER NOT NULL DEFAULT 0,
                    lease_token TEXT,
                    lease_owner TEXT,
                    lease_expires_at TEXT,
                    claimed_at TEXT,
                    completed_at TEXT,
                    quarantined_at TEXT,
                    last_error_code TEXT NOT NULL DEFAULT '',
                    last_error_detail TEXT NOT NULL DEFAULT '',
                    result_json TEXT,
                    retry_window_started_at TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (business_key, generation)
                        REFERENCES business_triggers_v6(business_key, generation)
                );
                INSERT INTO rca_outbox_v6(
                    outbox_id, action, business_key, submission_key,
                    creation_rule_version, generation, origin_source_id,
                    source_event_id, source_topic, source_partition, source_offset,
                    payload_json, status, attempt, next_attempt_at, fence,
                    lease_token, lease_owner, lease_expires_at, claimed_at,
                    completed_at, quarantined_at, last_error_code,
                    last_error_detail, result_json, retry_window_started_at,
                    created_at, updated_at
                )
                    SELECT outbox_id, action, business_key, submission_key,
                           creation_rule_version, generation, NULL,
                           source_event_id, source_topic, source_partition, source_offset,
                           payload_json, status, attempt, next_attempt_at, fence,
                           lease_token, lease_owner, lease_expires_at, claimed_at,
                           completed_at, quarantined_at, last_error_code,
                           last_error_detail, result_json, retry_window_started_at,
                           created_at, updated_at
                      FROM rca_outbox;

                DROP TABLE rca_outbox;
                DROP TABLE business_triggers;
                ALTER TABLE business_triggers_v6 RENAME TO business_triggers;
                ALTER TABLE rca_outbox_v6 RENAME TO rca_outbox;
                COMMIT;
                """
            )
        except Exception:
            if conn.in_transaction:
                conn.rollback()
            raise RuntimeError("incompatible_control_store_schema:v6_migration_failed")

    @staticmethod
    def _register_policy_snapshot_tx(
        conn: sqlite3.Connection,
        policy: WorkflowEventPolicy,
        current: str,
    ) -> str:
        policy_json = _canonical_json(policy.to_dict())
        policy_sha = hashlib.sha256(policy_json.encode("utf-8")).hexdigest()
        conn.execute("UPDATE rca_policy_snapshots SET active = 0 WHERE active = 1")
        conn.execute(
            """
            INSERT INTO rca_policy_snapshots(
                policy_sha256, policy_version, policy_json, active, activated_at
            ) VALUES (?, ?, ?, 1, ?)
            ON CONFLICT(policy_sha256) DO UPDATE SET
                policy_version=excluded.policy_version,
                policy_json=excluded.policy_json,
                active=1,
                activated_at=excluded.activated_at
            """,
            (policy_sha, policy.policy_version, policy_json, current),
        )
        return policy_sha

    @classmethod
    def _select_issue_scope_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        project_key: str,
        work_item_type_key: str,
        work_item_id: str,
    ) -> tuple[sqlite3.Row | None, str, int]:
        """Return the latest row from one strict rule-neutral issue chain."""
        scope_id = build_rca_issue_scope_key(
            project_key=project_key,
            work_item_type_key=work_item_type_key,
            work_item_id=work_item_id,
        )
        rows = conn.execute(
            """
            SELECT t.*, o.outbox_id, o.status AS outbox_status,
                   o.attempt, o.completed_at, o.result_json, o.quarantined_at,
                   o.last_error_code, o.lease_token, o.lease_owner,
                   o.lease_expires_at,
                   o.activation_epoch_id AS outbox_activation_epoch_id,
                   o.activation_ledger_id AS outbox_activation_ledger_id
              FROM business_triggers AS t
              JOIN rca_outbox AS o
                ON o.business_key = t.business_key AND o.generation = t.generation
             WHERE t.project_key = ? AND t.work_item_type_key = ?
               AND t.work_item_id = ?
             ORDER BY t.generation DESC, t.created_at DESC, o.outbox_id DESC
            """,
            (project_key, work_item_type_key, work_item_id),
        ).fetchall()
        business_key_count = len(
            {str(row["business_key"] or "").strip() for row in rows}
        )
        return (rows[0] if rows else None, scope_id, business_key_count)

    @staticmethod
    def _select_issue_generation_tx(
        conn: sqlite3.Connection,
        *,
        project_key: str,
        work_item_type_key: str,
        work_item_id: str,
        generation: int,
    ) -> sqlite3.Row | None:
        """Return one exact generation from a validated issue chain."""
        return conn.execute(
            """
            SELECT t.*, o.outbox_id, o.status AS outbox_status,
                   o.attempt, o.completed_at, o.result_json, o.quarantined_at,
                   o.last_error_code, o.lease_token, o.lease_owner,
                   o.lease_expires_at
              FROM business_triggers AS t
              JOIN rca_outbox AS o
                ON o.business_key = t.business_key AND o.generation = t.generation
             WHERE t.project_key = ? AND t.work_item_type_key = ?
               AND t.work_item_id = ? AND t.generation = ?
             ORDER BY t.created_at ASC, o.outbox_id ASC
             LIMIT 1
            """,
            (project_key, work_item_type_key, work_item_id, generation),
        ).fetchone()

    @staticmethod
    def _select_latest_kafka_issue_generation_tx(
        conn: sqlite3.Connection,
        *,
        project_key: str,
        work_item_type_key: str,
        work_item_id: str,
    ) -> sqlite3.Row | None:
        """Return the latest generation whose immutable origin is Kafka."""
        return conn.execute(
            """
            SELECT t.*, o.outbox_id, o.status AS outbox_status,
                   o.attempt, o.completed_at, o.result_json, o.quarantined_at,
                   o.last_error_code, o.lease_token, o.lease_owner,
                   o.lease_expires_at,
                   o.origin_source_id AS outbox_origin_source_id,
                   o.source_event_id AS outbox_source_event_id,
                   o.source_topic AS outbox_source_topic,
                   o.source_partition AS outbox_source_partition,
                   o.source_offset AS outbox_source_offset,
                   o.payload_json AS outbox_payload_json,
                   origin.source_id AS kafka_origin_source_id,
                   origin.source_dedupe_key AS kafka_source_dedupe_key,
                   origin.kafka_event_uid AS kafka_event_uid,
                   origin.mode AS kafka_source_mode
              FROM business_triggers AS t
              JOIN rca_outbox AS o
                ON o.business_key = t.business_key AND o.generation = t.generation
              JOIN rca_trigger_sources AS origin
                ON origin.source_id = t.origin_source_id
               AND origin.source_kind = 'kafka_workflow_event'
             WHERE t.project_key = ? AND t.work_item_type_key = ?
               AND t.work_item_id = ?
             ORDER BY t.generation DESC, t.created_at DESC, o.outbox_id DESC
            LIMIT 1
            """,
            (project_key, work_item_type_key, work_item_id),
        ).fetchone()

    @staticmethod
    def _business_profile_observation_sha256(value: Any) -> str:
        try:
            payload = json.loads(value) if isinstance(value, str) else value
        except (TypeError, ValueError, json.JSONDecodeError):
            return ""
        if not isinstance(payload, Mapping) or payload.get(
            "business_profile_observed"
        ) is not True:
            return ""
        resolution = payload.get("business_profile_resolution")
        if not isinstance(resolution, Mapping):
            return ""
        return _canonical_sha256(
            {"observed": True, "resolution": dict(resolution)}
        )

    @classmethod
    def _latest_business_profile_observation_sha256_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        project_key: str,
        work_item_type_key: str,
        work_item_id: str,
    ) -> str:
        rows = conn.execute(
            """
            SELECT normalized_json FROM business_triggers
             WHERE project_key = ? AND work_item_type_key = ? AND work_item_id = ?
             ORDER BY generation DESC, created_at DESC
            """,
            (project_key, work_item_type_key, work_item_id),
        ).fetchall()
        for row in rows:
            fingerprint = cls._business_profile_observation_sha256(
                row["normalized_json"]
            )
            if fingerprint:
                return fingerprint
        return ""

    @classmethod
    def _kafka_generation_origin_contract_valid(
        cls, row: sqlite3.Row
    ) -> bool:
        try:
            generation = int(row["generation"])
            source_dedupe_key = str(
                row["kafka_source_dedupe_key"] or ""
            ).strip()
            kafka_event_uid = row["kafka_event_uid"]
            if generation == 1:
                origin_event_uid = str(kafka_event_uid or "").strip()
                if not origin_event_uid or source_dedupe_key != origin_event_uid:
                    return False
            else:
                generation_suffix = f":generation:{generation}"
                if (
                    kafka_event_uid is not None
                    or not source_dedupe_key.endswith(generation_suffix)
                    or len(source_dedupe_key) <= len(generation_suffix)
                ):
                    return False
                origin_event_uid = source_dedupe_key[: -len(generation_suffix)]
            current_event_uid = str(
                row["outbox_source_event_id"] or ""
            ).strip()
            topic = str(row["outbox_source_topic"] or "").strip()
            partition = int(row["outbox_source_partition"])
            offset = int(row["outbox_source_offset"])
            payload = json.loads(str(row["outbox_payload_json"] or "{}"))
            payload_partition = int(payload.get("partition"))
            payload_offset = int(payload.get("offset"))
            admission = validate_rca_admission(payload.get("admission") or {})
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            return False
        expected_trigger_kind = (
            "issue_created" if generation == 1 else "kafka_retrigger"
        )
        try:
            (
                expected_source_id,
                expected_dedupe_key,
                expected_kafka_event_uid,
                expected_mode,
            ) = cls._kafka_source_identity(
                event_uid=origin_event_uid,
                generation=generation,
                trigger_kind=expected_trigger_kind,
            )
        except RecordConflictError:
            return False
        refs = admission.source_refs
        return bool(
            origin_event_uid
            and current_event_uid
            and topic
            and partition >= 0
            and offset >= 0
            and str(row["origin_source_id"] or "")
            == str(row["kafka_origin_source_id"] or "")
            == str(row["outbox_origin_source_id"] or "")
            == str(payload.get("origin_source_id") or "")
            == expected_source_id
            and str(row["kafka_source_mode"] or "") == expected_mode
            and source_dedupe_key == expected_dedupe_key
            and kafka_event_uid == expected_kafka_event_uid
            and str(payload.get("source_event_id") or "")
            == current_event_uid
            and str(payload.get("topic") or "") == topic
            and payload_partition == partition
            and payload_offset == offset
            and admission.trigger_kind == expected_trigger_kind
            and admission.generation == generation
            and admission.business_key == str(row["business_key"])
            and admission.submission_key == str(row["submission_key"])
            and refs.topic == topic
            and refs.partition == partition
            and refs.offset == offset
        )

    @classmethod
    def _kafka_generation_contract_valid(
        cls, row: sqlite3.Row
    ) -> bool:
        if not cls._kafka_generation_origin_contract_valid(row):
            return False
        try:
            return bool(
                str(row["source_event_id"] or "")
                == str(row["outbox_source_event_id"] or "")
                and str(row["source_topic"] or "")
                == str(row["outbox_source_topic"] or "")
                and int(row["source_partition"])
                == int(row["outbox_source_partition"])
                and int(row["source_offset"])
                == int(row["outbox_source_offset"])
            )
        except (KeyError, TypeError, ValueError):
            return False

    @classmethod
    def _audit_issue_scope_conflict_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        event_uid: str,
        operator: str,
        scope_id: str,
        business_key_count: int,
        current: str,
    ) -> None:
        cls._insert_promotion_audit(
            conn,
            event_uid=event_uid,
            outbox_id=None,
            submission_key="",
            operator=operator,
            reason=ISSUE_SCOPE_CONFLICT_REASON,
            outcome=ISSUE_SCOPE_CONFLICT_REASON,
            from_status="multiple_business_keys",
            to_status="blocked",
            detail=_canonical_json(
                {
                    "issue_scope_id": scope_id,
                    "business_key_count": business_key_count,
                }
            ),
            created_at=current,
        )

    @classmethod
    def _audit_manual_policy_observation_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        source_id: str,
        outbox_id: int,
        submission_key: str,
        requester_id: str,
        scope_id: str,
        active_policy_sha256: str,
        active_policy_version: str,
        chain_rule_version: str,
        current: str,
    ) -> None:
        if active_policy_version == chain_rule_version:
            return
        cls._insert_promotion_audit(
            conn,
            event_uid=source_id,
            outbox_id=outbox_id,
            submission_key=submission_key,
            operator=f"manual:{requester_id}",
            reason="manual_active_policy_binding",
            outcome=MANUAL_POLICY_OBSERVED_OUTCOME,
            from_status=chain_rule_version,
            to_status=active_policy_version,
            detail=_canonical_json(
                {
                    "issue_scope_id": scope_id,
                    "active_policy_sha256": active_policy_sha256,
                }
            ),
            created_at=current,
        )

    @classmethod
    def _insert_issue_subscription_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        admission: RcaAdmission,
        current: str,
    ) -> str:
        if conn.execute(
            "SELECT 1 FROM rca_learning_lane_admissions "
            "WHERE business_key = ? AND generation = ?",
            (admission.business_key, admission.generation),
        ).fetchone() is not None:
            return ""
        refs = admission.source_refs
        target_key = (
            f"feishu_project:{refs.project_key}:{refs.work_item_type_key}:"
            f"{refs.work_item_id}"
        )
        target = {
            "schema_version": DELIVERY_TARGET_SCHEMA_VERSION,
            "platform": "feishu_project",
            "project_key": refs.project_key,
            "work_item_type_key": refs.work_item_type_key,
            "work_item_id": refs.work_item_id,
            "output_cap": "L1",
        }
        subscription_key = _stable_key(
            "g1q3-rca-sub-v1",
            {
                "business_key": admission.business_key,
                "generation": admission.generation,
                "effect_kind": "feishu_issue_comment",
                "target_key": target_key,
            },
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO rca_delivery_subscriptions(
                subscription_key, business_key, generation, source_id,
                effect_kind, target_key, target_json, required, status, reason,
                created_at, updated_at
            ) VALUES (?, ?, ?, NULL, 'feishu_issue_comment', ?, ?, 1, 'pending',
                      'awaiting_delivery_materialization', ?, ?)
            """,
            (
                subscription_key,
                admission.business_key,
                admission.generation,
                target_key,
                _canonical_json(target),
                current,
                current,
            ),
        )
        return subscription_key

    @classmethod
    def _bind_kafka_source_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        event_uid: str,
        raw_sha256: str,
        business_key: str,
        generation: int,
        trigger_kind: str,
        current: str,
    ) -> str:
        source_id = cls._ensure_kafka_source_tx(
            conn,
            event_uid=event_uid,
            raw_sha256=raw_sha256,
            generation=generation,
            trigger_kind=trigger_kind,
            current=current,
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO rca_trigger_bindings(
                source_id, business_key, generation, role, bound_at
            ) VALUES (?, ?, ?, 'observer', ?)
            """,
            (source_id, business_key, generation, current),
        )
        binding = conn.execute(
            "SELECT business_key, generation FROM rca_trigger_bindings WHERE source_id = ?",
            (source_id,),
        ).fetchone()
        if (
            binding is None
            or binding["business_key"] != business_key
            or int(binding["generation"]) != generation
        ):
            raise RecordConflictError(f"Kafka source was rebound: {event_uid}")
        return source_id

    @classmethod
    def _kafka_source_identity(
        cls,
        *,
        event_uid: str,
        generation: int,
        trigger_kind: str,
    ) -> tuple[str, str, str | None, str]:
        mode = str(trigger_kind or "").strip()
        if mode not in RCA_KAFKA_TRIGGER_KINDS:
            raise RecordConflictError(f"Kafka source mode is invalid: {event_uid}")
        if mode == "issue_created":
            if generation != 1:
                raise RecordConflictError(
                    f"Kafka issue-created source generation is invalid: {event_uid}"
                )
            dedupe_key = event_uid
            kafka_event_uid: str | None = event_uid
            material: dict[str, Any] = {
                "source_kind": "kafka_workflow_event",
                "dedupe": event_uid,
            }
        else:
            if generation < 2:
                raise RecordConflictError(
                    f"Kafka retrigger source generation is invalid: {event_uid}"
                )
            dedupe_key = f"{event_uid}:generation:{generation}"
            kafka_event_uid = None
            material = {
                "source_kind": "kafka_workflow_event",
                "dedupe": event_uid,
                "generation": generation,
                "mode": mode,
            }
        return (
            _stable_key("g1q3-rca-source-v1", material),
            dedupe_key,
            kafka_event_uid,
            mode,
        )

    @classmethod
    def _ensure_kafka_source_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        event_uid: str,
        raw_sha256: str,
        generation: int,
        trigger_kind: str,
        current: str,
    ) -> str:
        source_id, dedupe_key, kafka_event_uid, mode = cls._kafka_source_identity(
            event_uid=event_uid,
            generation=generation,
            trigger_kind=trigger_kind,
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO rca_trigger_sources(
                source_id, source_kind, source_dedupe_key, payload_sha256,
                kafka_event_uid, mode, created_at
            ) VALUES (?, 'kafka_workflow_event', ?, ?, ?, ?, ?)
            """,
            (
                source_id,
                dedupe_key,
                raw_sha256,
                kafka_event_uid,
                mode,
                current,
            ),
        )
        source = conn.execute(
            "SELECT * FROM rca_trigger_sources WHERE source_id = ?", (source_id,)
        ).fetchone()
        if (
            source is None
            or source["source_kind"] != "kafka_workflow_event"
            or source["source_dedupe_key"] != dedupe_key
            or source["payload_sha256"] != raw_sha256
            or source["kafka_event_uid"] != kafka_event_uid
            or source["mode"] != mode
        ):
            raise RecordConflictError(f"Kafka source binding conflict: {event_uid}")
        return source_id

    @classmethod
    def _bind_legacy_kafka_source_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        event_uid: str,
        raw_sha256: str,
        business_key: str,
        generation: int,
        current: str,
    ) -> str:
        """Preserve a pre-contract Kafka/manual generation without reinterpreting it."""
        source_id = _stable_key(
            "g1q3-rca-source-v1",
            {"source_kind": "kafka_workflow_event", "dedupe": event_uid},
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO rca_trigger_sources(
                source_id, source_kind, source_dedupe_key, payload_sha256,
                kafka_event_uid, mode, created_at
            ) VALUES (?, 'kafka_workflow_event', ?, ?, ?, 'issue_created', ?)
            """,
            (source_id, event_uid, raw_sha256, event_uid, current),
        )
        source = conn.execute(
            "SELECT * FROM rca_trigger_sources WHERE source_id = ?", (source_id,)
        ).fetchone()
        if (
            source is None
            or source["source_kind"] != "kafka_workflow_event"
            or source["source_dedupe_key"] != event_uid
            or source["payload_sha256"] != raw_sha256
            or source["kafka_event_uid"] != event_uid
            or source["mode"] != "issue_created"
        ):
            raise RecordConflictError(
                f"Legacy Kafka source binding conflict: {event_uid}"
            )
        conn.execute(
            """
            INSERT OR IGNORE INTO rca_trigger_bindings(
                source_id, business_key, generation, role, bound_at
            ) VALUES (?, ?, ?, 'origin', ?)
            """,
            (source_id, business_key, generation, current),
        )
        binding = conn.execute(
            "SELECT * FROM rca_trigger_bindings WHERE source_id = ?", (source_id,)
        ).fetchone()
        if (
            binding is None
            or str(binding["business_key"]) != business_key
            or int(binding["generation"]) != generation
        ):
            raise RecordConflictError(
                f"Legacy Kafka source requires explicit migration: {event_uid}"
            )
        return source_id

    @classmethod
    def _insert_thread_subscription_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        admission: RcaAdmission,
        source_id: str,
        request: ManualRcaTriggerRequest,
        current: str,
    ) -> tuple[str, bool]:
        if conn.execute(
            "SELECT 1 FROM rca_learning_lane_admissions "
            "WHERE business_key = ? AND generation = ?",
            (admission.business_key, admission.generation),
        ).fetchone() is not None:
            return "", False
        root_message_id = request.thread_id.split("topic:", 1)[1]
        target_key = f"feishu_thread:{request.chat_id}:{root_message_id}"
        target = {
            "schema_version": DELIVERY_TARGET_SCHEMA_VERSION,
            "platform": "feishu",
            "chat_id": request.chat_id,
            "thread_id": request.thread_id,
            "reply_anchor_message_id": root_message_id,
            "source_message_id": request.message_id,
            "requester_id": request.requester_id,
            "reply_in_thread": True,
            "output_cap": "L1",
        }
        subscription_key = _stable_key(
            "g1q3-rca-sub-v1",
            {
                "business_key": admission.business_key,
                "generation": admission.generation,
                "effect_kind": "feishu_thread_reply",
                "target_key": target_key,
            },
        )
        created = conn.execute(
            """
            INSERT OR IGNORE INTO rca_delivery_subscriptions(
                subscription_key, business_key, generation, source_id,
                effect_kind, target_key, target_json, required, status, reason,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, 'feishu_thread_reply', ?, ?, 1, 'pending',
                      'awaiting_delivery_materialization', ?, ?)
            """,
            (
                subscription_key,
                admission.business_key,
                admission.generation,
                source_id,
                target_key,
                _canonical_json(target),
                current,
                current,
            ),
        ).rowcount == 1
        return subscription_key, created

    @staticmethod
    def _bind_source_subscription_tx(
        conn: sqlite3.Connection,
        *,
        source_id: str,
        subscription_key: str,
        current: str,
    ) -> None:
        if not str(subscription_key or "").strip():
            return
        conn.execute(
            """
            INSERT OR IGNORE INTO rca_trigger_delivery_bindings(
                source_id, subscription_key, bound_at
            ) VALUES (?, ?, ?)
            """,
            (source_id, subscription_key, current),
        )
        binding = conn.execute(
            """
            SELECT 1 FROM rca_trigger_delivery_bindings
             WHERE source_id = ? AND subscription_key = ?
            """,
            (source_id, subscription_key),
        ).fetchone()
        if binding is None:
            raise RuntimeError("source_delivery_binding_missing")

    @classmethod
    def _mark_late_catchup_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        subscription_key: str,
        business_key: str,
        generation: int,
        current: str,
    ) -> bool:
        if not cls._table_exists(conn, "rca_delivery_jobs"):
            return False
        delivery = conn.execute(
            """
            SELECT delivery_id FROM rca_delivery_jobs
             WHERE business_key = ? AND generation = ? LIMIT 1
            """,
            (business_key, generation),
        ).fetchone()
        if delivery is None:
            return False
        updated = conn.execute(
            """
            UPDATE rca_delivery_subscriptions
               SET delivery_id = ?, catchup_requested_at = ?,
                   reason = 'late_catchup_requested', updated_at = ?
             WHERE subscription_key = ? AND status = 'pending'
            """,
            (delivery["delivery_id"], current, current, subscription_key),
        ).rowcount
        if updated == 1:
            return True
        subscription = conn.execute(
            """
            SELECT status FROM rca_delivery_subscriptions
             WHERE subscription_key = ?
            """,
            (subscription_key,),
        ).fetchone()
        if subscription is not None and str(subscription["status"] or "") == "quarantined":
            raise ManualRcaAdmissionError("manual_late_catchup_unavailable")
        return False

    @classmethod
    def _backfill_kafka_sources_and_subscriptions(
        cls, conn: sqlite3.Connection
    ) -> None:
        current = _now_iso()
        latest_policy = conn.execute(
            """
            SELECT policy_json FROM kafka_inbox
             WHERE policy_json IS NOT NULL AND policy_json != ''
             ORDER BY received_at DESC, event_uid DESC LIMIT 1
            """
        ).fetchone()
        if latest_policy is not None:
            policy = WorkflowEventPolicy.from_mapping(json.loads(latest_policy["policy_json"]))
            cls._register_policy_snapshot_tx(conn, policy, current)
        rows = conn.execute(
            """
            SELECT t.*, i.raw_sha256, o.payload_json AS outbox_payload_json
              FROM business_triggers AS t
              LEFT JOIN kafka_inbox AS i ON i.event_uid = t.source_event_id
              JOIN rca_outbox AS o
                ON o.business_key = t.business_key AND o.generation = t.generation
            """
        ).fetchall()
        for row in rows:
            event_uid = str(row["source_event_id"] or "")
            generation = int(row["generation"])
            try:
                durable_payload = json.loads(
                    str(row["outbox_payload_json"] or "{}")
                )
            except (TypeError, ValueError, json.JSONDecodeError):
                durable_payload = {}
            durable_admission = durable_payload.get("admission")
            durable_trigger_kind = (
                str(durable_admission.get("trigger_kind") or "").strip()
                if isinstance(durable_admission, Mapping)
                else ""
            )
            legacy_kafka_generation = bool(
                event_uid
                and generation > 1
                and durable_trigger_kind != "kafka_retrigger"
            )
            trigger_kind = (
                "issue_created"
                if event_uid and generation == 1
                else "kafka_retrigger"
                if event_uid and not legacy_kafka_generation
                else "manual_retrigger"
                if legacy_kafka_generation
                else "manual_issue_request"
                if generation == 1
                else "manual_retrigger"
            )
            source_id = ""
            if event_uid:
                if legacy_kafka_generation:
                    source_id = cls._bind_legacy_kafka_source_tx(
                        conn,
                        event_uid=event_uid,
                        raw_sha256=str(row["raw_sha256"] or "0" * 64),
                        business_key=str(row["business_key"]),
                        generation=generation,
                        current=current,
                    )
                else:
                    source_id = cls._bind_kafka_source_tx(
                        conn,
                        event_uid=event_uid,
                        raw_sha256=str(row["raw_sha256"] or "0" * 64),
                        business_key=str(row["business_key"]),
                        generation=generation,
                        trigger_kind=trigger_kind,
                        current=current,
                    )
                durable_origin = row["origin_source_id"]
                if durable_origin is not None and str(durable_origin) != source_id:
                    raise RecordConflictError(
                        f"Kafka origin requires explicit migration: {event_uid}"
                    )
                conn.execute(
                    """
                    UPDATE rca_trigger_bindings SET role = 'origin'
                     WHERE source_id = ?
                    """,
                    (source_id,),
                )
                conn.execute(
                    """
                    UPDATE business_triggers SET origin_source_id = ?
                     WHERE business_key = ? AND generation = ?
                       AND origin_source_id IS NULL
                    """,
                    (source_id, row["business_key"], row["generation"]),
                )
                conn.execute(
                    """
                    UPDATE rca_outbox SET origin_source_id = ?
                     WHERE business_key = ? AND generation = ?
                       AND origin_source_id IS NULL
                    """,
                    (source_id, row["business_key"], row["generation"]),
                )
            normalized = json.loads(str(row["normalized_json"] or "{}"))
            admission_kwargs: dict[str, Any] = {}
            if event_uid and not legacy_kafka_generation:
                admission_kwargs = {
                    "topic": str(row["source_topic"] or ""),
                    "partition": row["source_partition"],
                    "offset": row["source_offset"],
                }
            admission = build_rca_admission(
                project_key=row["project_key"],
                project_simple_name=str(normalized.get("project_simple_name") or ""),
                work_item_type_key=row["work_item_type_key"],
                work_item_id=row["work_item_id"],
                rule_version=row["creation_rule_version"],
                trigger_kind=trigger_kind,
                generation=generation,
                **admission_kwargs,
            )
            cls._ensure_learning_lane_admission_tx(
                conn, admission=admission, current=current
            )
            issue_subscription_key = cls._insert_issue_subscription_tx(
                conn, admission=admission, current=current
            )
            if source_id:
                cls._bind_source_subscription_tx(
                    conn,
                    source_id=source_id,
                    subscription_key=issue_subscription_key,
                    current=current,
                )

    @staticmethod
    def _migrate_inbox_columns(conn: sqlite3.Connection) -> None:
        existing = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(kafka_inbox)").fetchall()
        }
        added_submit_intent = "submit_enabled_requested" not in existing
        additions = {
            "raw_size_bytes": "INTEGER NOT NULL DEFAULT 0",
            "submit_enabled_requested": "INTEGER NOT NULL DEFAULT 0",
            "activation_epoch_id": "TEXT",
            "activation_ingress_state": (
                "TEXT NOT NULL DEFAULT 'legacy_unconfigured'"
            ),
            "activation_required": "INTEGER NOT NULL DEFAULT 0",
            "activation_source_identity_sha256": "TEXT NOT NULL DEFAULT ''",
            "rearm_reason": "TEXT NOT NULL DEFAULT ''",
            "processing_attempts": "INTEGER NOT NULL DEFAULT 0",
            "last_processing_error_code": "TEXT NOT NULL DEFAULT ''",
            "last_processing_error_detail": "TEXT NOT NULL DEFAULT ''",
            "processing_failed_at": "TEXT",
            "raw_pruned_at": "TEXT",
        }
        for name, declaration in additions.items():
            if name not in existing:
                conn.execute(f"ALTER TABLE kafka_inbox ADD COLUMN {name} {declaration}")
        conn.execute(
            """
            UPDATE kafka_inbox SET raw_size_bytes = length(raw_value)
             WHERE raw_size_bytes = 0 AND length(raw_value) > 0
            """
        )
        if added_submit_intent:
            conn.execute(
                """
                UPDATE kafka_inbox
                   SET submit_enabled_requested = CASE
                       WHEN submission_mode = 'pending' THEN 1 ELSE 0 END
                """
            )

    @staticmethod
    def _migrate_outbox_columns(conn: sqlite3.Connection) -> None:
        """Add v2 lease columns to a v1 database without rewriting durable rows."""
        existing = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(rca_outbox)").fetchall()
        }
        additions = {
            "fence": "INTEGER NOT NULL DEFAULT 0",
            "lease_token": "TEXT",
            "lease_owner": "TEXT",
            "lease_expires_at": "TEXT",
            "claimed_at": "TEXT",
            "completed_at": "TEXT",
            "quarantined_at": "TEXT",
            "last_error_code": "TEXT NOT NULL DEFAULT ''",
            "last_error_detail": "TEXT NOT NULL DEFAULT ''",
            "result_json": "TEXT",
            "retry_window_started_at": "TEXT",
        }
        for name, declaration in additions.items():
            if name not in existing:
                conn.execute(f"ALTER TABLE rca_outbox ADD COLUMN {name} {declaration}")
        conn.execute(
            """
            UPDATE rca_outbox
               SET retry_window_started_at = created_at
             WHERE retry_window_started_at IS NULL
            """
        )

    @staticmethod
    def _migrate_activation_columns(conn: sqlite3.Connection) -> None:
        """Add nullable epoch bindings without rewriting historical work."""
        for table in ("business_triggers", "rca_outbox"):
            existing = {
                str(row["name"])
                for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
            }
            additions = {
                "activation_epoch_id": "TEXT",
                "activation_ledger_id": "INTEGER",
            }
            for name, declaration in additions.items():
                if name not in existing:
                    conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {declaration}")
    @staticmethod
    def _validate_v11_snapshot_schema(conn: sqlite3.Connection) -> None:
        normalize_sql = lambda value: " ".join(str(value).split()).rstrip(";")
        required_columns = {
            "rca_canonical_requests": {
                "request_sha256",
                "schema_version",
                "ticket_title_sha256",
                "creation_policy_sha256",
                "business_profile_sha256",
                "execution_policy_sha256",
                "publication_policy_sha256",
                "correction_lineage_policy_sha256",
                "generation_reason",
                "generation_authorization_evidence_sha256",
                "canonical_request_json",
                "persisted_at",
            },
            "rca_source_authority_receipts": {
                "authority_sha256",
                "schema_version",
                "source_id",
                "source_kind",
                "payload_sha256",
                "authorization_evidence_sha256",
                "binding_action",
                "decision",
                "source_metadata_sha256",
                "anchor_sha256",
                "ingress_decision_sha256",
                "source_metadata_json",
                "anchor_json",
                "ingress_decision_json",
                "authority_receipt_json",
                "persisted_at",
            },
            "rca_admission_snapshots": {
                "snapshot_sha256",
                "snapshot_id",
                "schema_version",
                "request_sha256",
                "business_key",
                "submission_key",
                "generation",
                "activation_epoch_id",
                "activation_ledger_id",
                "execution_decision",
                "execution_reason",
                "execution_state",
                "legacy_unconfigured",
                "creator_source_envelope_sha256",
                "creator_authority_sha256",
                "creator_source_id",
                "admission_snapshot_json",
                "persisted_at",
            },
            "rca_snapshot_source_envelopes": {
                "source_envelope_sha256",
                "source_envelope_id",
                "schema_version",
                "snapshot_sha256",
                "snapshot_id",
                "submission_key",
                "source_authority_sha256",
                "source_id",
                "source_kind",
                "payload_sha256",
                "authorization_evidence_sha256",
                "binding_action",
                "decision",
                "source_metadata_json",
                "anchor_json",
                "ingress_decision_json",
                "source_envelope_json",
                "persisted_at",
            },
        }
        for table, expected in required_columns.items():
            observed = {
                str(row["name"])
                for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
            }
            if observed != expected:
                raise RuntimeError(
                    f"incompatible_control_store_schema:{table}_columns"
                )

        expected_table_sql: dict[str, str] = {}
        table_prefix = "CREATE TABLE IF NOT EXISTS "
        for statement in RcaControlStore._v11_snapshot_schema_statements():
            normalized = normalize_sql(statement)
            if normalized.startswith(table_prefix):
                table = normalized[len(table_prefix) :].split(" ", 1)[0]
                expected_table_sql[table] = normalized.replace(
                    table_prefix,
                    "CREATE TABLE ",
                    1,
                )
        observed_table_sql = {
            str(row["name"]): normalize_sql(row["sql"] or "")
            for row in conn.execute(
                "SELECT name, sql FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
            if str(row["name"]) in required_columns
        }
        if observed_table_sql != expected_table_sql:
            raise RuntimeError("incompatible_control_store_schema:v11_table_sql")

        def foreign_key_groups(
            table: str,
        ) -> set[tuple[str, frozenset[tuple[str, str]], str, str, str]]:
            grouped: dict[
                tuple[int, str, str, str, str], set[tuple[str, str]]
            ] = {}
            for row in conn.execute(f"PRAGMA foreign_key_list({table})").fetchall():
                key = (
                    int(row["id"]),
                    str(row["table"]),
                    str(row["on_update"]),
                    str(row["on_delete"]),
                    str(row["match"]),
                )
                grouped.setdefault(key, set()).add(
                    (str(row["from"]), str(row["to"]))
                )
            return {
                (parent, frozenset(pairs), on_update, on_delete, match)
                for (_identifier, parent, on_update, on_delete, match), pairs
                in grouped.items()
            }

        expected_foreign_keys = {
            "rca_canonical_requests": set(),
            "rca_source_authority_receipts": {
                (
                    "rca_trigger_sources",
                    frozenset({("source_id", "source_id")}),
                    "NO ACTION",
                    "NO ACTION",
                    "NONE",
                )
            },
            "rca_admission_snapshots": {
                (
                    "rca_canonical_requests",
                    frozenset({("request_sha256", "request_sha256")}),
                    "NO ACTION",
                    "NO ACTION",
                    "NONE",
                ),
                (
                    "rca_activation_admission_ledger",
                    frozenset(
                        {("activation_ledger_id", "ledger_id")}
                    ),
                    "NO ACTION",
                    "NO ACTION",
                    "NONE",
                ),
                (
                    "rca_snapshot_source_envelopes",
                    frozenset(
                        {
                            (
                                "creator_source_envelope_sha256",
                                "source_envelope_sha256",
                            ),
                            (
                                "creator_authority_sha256",
                                "source_authority_sha256",
                            ),
                            ("creator_source_id", "source_id"),
                        }
                    ),
                    "NO ACTION",
                    "NO ACTION",
                    "NONE",
                ),
            },
            "rca_snapshot_source_envelopes": {
                (
                    "rca_admission_snapshots",
                    frozenset({("snapshot_sha256", "snapshot_sha256")}),
                    "NO ACTION",
                    "NO ACTION",
                    "NONE",
                ),
                (
                    "rca_source_authority_receipts",
                    frozenset(
                        {
                            (
                                "source_authority_sha256",
                                "authority_sha256",
                            ),
                            ("source_id", "source_id"),
                            ("source_kind", "source_kind"),
                            ("payload_sha256", "payload_sha256"),
                        }
                    ),
                    "NO ACTION",
                    "NO ACTION",
                    "NONE",
                ),
                (
                    "rca_trigger_sources",
                    frozenset({("source_id", "source_id")}),
                    "NO ACTION",
                    "NO ACTION",
                    "NONE",
                ),
            },
        }
        for table, expected in expected_foreign_keys.items():
            if foreign_key_groups(table) != expected:
                raise RuntimeError(
                    f"incompatible_control_store_schema:{table}_foreign_keys"
                )

        snapshot_table = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' "
            "AND name = 'rca_admission_snapshots'"
        ).fetchone()
        deferred_creator_clause = normalize_sql(
            """
            FOREIGN KEY(
                creator_source_envelope_sha256,
                creator_authority_sha256,
                creator_source_id
            ) REFERENCES rca_snapshot_source_envelopes(
                source_envelope_sha256,
                source_authority_sha256,
                source_id
            ) DEFERRABLE INITIALLY DEFERRED
            """
        )
        normalized_snapshot_table = normalize_sql(
            snapshot_table["sql"] if snapshot_table is not None else ""
        )
        if (
            deferred_creator_clause not in normalized_snapshot_table
            or normalized_snapshot_table.count("DEFERRABLE INITIALLY DEFERRED") != 1
        ):
            raise RuntimeError(
                "incompatible_control_store_schema:admission_snapshot_deferred_creator"
            )

        expected_indexes = {
            "idx_rca_snapshot_submission": (
                "rca_admission_snapshots",
                1,
                0,
                ["submission_key"],
            ),
            "idx_rca_snapshot_business_generation": (
                "rca_admission_snapshots",
                1,
                0,
                ["business_key", "generation"],
            ),
            "idx_rca_snapshot_request": (
                "rca_admission_snapshots",
                0,
                0,
                ["request_sha256", "snapshot_sha256"],
            ),
            "idx_rca_snapshot_creator": (
                "rca_admission_snapshots",
                0,
                0,
                [
                    "creator_source_envelope_sha256",
                    "creator_authority_sha256",
                    "creator_source_id",
                ],
            ),
            "idx_rca_source_authority_source": (
                "rca_source_authority_receipts",
                1,
                0,
                ["source_id"],
            ),
            "idx_rca_source_authority_reference": (
                "rca_source_authority_receipts",
                1,
                0,
                ["authority_sha256", "source_id", "source_kind", "payload_sha256"],
            ),
            "idx_rca_snapshot_envelope_source": (
                "rca_snapshot_source_envelopes",
                1,
                0,
                ["source_id"],
            ),
            "idx_rca_snapshot_envelope_creator_reference": (
                "rca_snapshot_source_envelopes",
                1,
                0,
                [
                    "source_envelope_sha256",
                    "source_authority_sha256",
                    "source_id",
                ],
            ),
            "idx_rca_snapshot_envelope_snapshot": (
                "rca_snapshot_source_envelopes",
                0,
                0,
                ["snapshot_sha256", "binding_action", "source_envelope_sha256"],
            ),
            "idx_rca_snapshot_envelope_authority": (
                "rca_snapshot_source_envelopes",
                0,
                0,
                [
                    "source_authority_sha256",
                    "source_id",
                    "source_kind",
                    "payload_sha256",
                ],
            ),
            "idx_rca_one_create_envelope_per_snapshot": (
                "rca_snapshot_source_envelopes",
                1,
                1,
                ["snapshot_sha256"],
            ),
        }
        v11_tables = set(required_columns)
        observed_explicit_indexes = {
            str(row["name"]): (str(row["tbl_name"]), str(row["sql"] or ""))
            for row in conn.execute(
                "SELECT name, tbl_name, sql FROM sqlite_master "
                "WHERE type = 'index' AND sql IS NOT NULL"
            ).fetchall()
            if str(row["tbl_name"]) in v11_tables
        }
        if set(observed_explicit_indexes) != set(expected_indexes):
            raise RuntimeError("incompatible_control_store_schema:v11_indexes")
        for name, (table, unique, partial, columns) in expected_indexes.items():
            index_row = next(
                (
                    row
                    for row in conn.execute(f"PRAGMA index_list({table})").fetchall()
                    if str(row["name"]) == name
                ),
                None,
            )
            observed_columns = [
                str(row["name"])
                for row in conn.execute(f"PRAGMA index_info({name})").fetchall()
            ]
            if (
                index_row is None
                or int(index_row["unique"]) != unique
                or int(index_row["partial"]) != partial
                or observed_columns != columns
                or observed_explicit_indexes[name][0] != table
            ):
                raise RuntimeError(
                    f"incompatible_control_store_schema:v11_index_contract:{name}"
                )
        expected_partial_index_sql = normalize_sql(
            """
            CREATE UNIQUE INDEX idx_rca_one_create_envelope_per_snapshot
                ON rca_snapshot_source_envelopes(snapshot_sha256)
                WHERE binding_action = 'create'
            """
        )
        if normalize_sql(
            observed_explicit_indexes[
                "idx_rca_one_create_envelope_per_snapshot"
            ][1]
        ) != expected_partial_index_sql:
            raise RuntimeError(
                "incompatible_control_store_schema:v11_partial_index_sql"
            )

        expected_trigger_sql = {}
        trigger_prefix = "CREATE TRIGGER IF NOT EXISTS "
        for statement in RcaControlStore._v11_snapshot_schema_statements():
            normalized = normalize_sql(statement)
            if normalized.startswith(trigger_prefix):
                name = normalized[len(trigger_prefix) :].split(" ", 1)[0]
                expected_trigger_sql[name] = normalized.replace(
                    trigger_prefix,
                    "CREATE TRIGGER ",
                    1,
                )
        observed_triggers = {
            str(row["name"]): str(row["sql"] or "")
            for row in conn.execute(
                "SELECT name, tbl_name, sql FROM sqlite_master WHERE type = 'trigger'"
            ).fetchall()
            if str(row["tbl_name"]) in v11_tables
        }
        if set(observed_triggers) != set(expected_trigger_sql):
            raise RuntimeError("incompatible_control_store_schema:v11_triggers")
        if any(
            normalize_sql(observed_triggers[name]) != normalize_sql(expected)
            for name, expected in expected_trigger_sql.items()
        ):
            raise RuntimeError("incompatible_control_store_schema:v11_trigger_sql")

        integrity_queries = {
            "v11_snapshot_request_binding": """
                SELECT 1 FROM rca_admission_snapshots AS snapshot
             LEFT JOIN rca_canonical_requests AS request
                    ON request.request_sha256 = snapshot.request_sha256
                 WHERE request.request_sha256 IS NULL
                 LIMIT 1
            """,
            "v11_source_authority_binding": """
                SELECT 1 FROM rca_source_authority_receipts AS authority
             LEFT JOIN rca_trigger_sources AS source
                    ON source.source_id = authority.source_id
                   AND source.source_kind = authority.source_kind
                   AND source.payload_sha256 = authority.payload_sha256
                 WHERE source.source_id IS NULL
                 LIMIT 1
            """,
            "v11_source_authority_transport_binding": """
                SELECT 1
                  FROM rca_source_authority_receipts AS authority
             LEFT JOIN rca_trigger_sources AS source
                    ON source.source_id = authority.source_id
             LEFT JOIN kafka_inbox AS inbox
                    ON inbox.event_uid = json_extract(
                        authority.source_metadata_json, '$.event_uid'
                    )
                 WHERE source.source_id IS NULL
                    OR source.source_kind != authority.source_kind
                    OR source.payload_sha256 != authority.payload_sha256
                    OR source.created_at != json_extract(
                        authority.source_metadata_json, '$.observed_at'
                    )
                    OR (
                        authority.source_kind = 'feishu_group_manual'
                        AND NOT COALESCE((
                            source.platform = json_extract(
                                authority.source_metadata_json, '$.platform'
                            )
                            AND source.chat_id = json_extract(
                                authority.source_metadata_json, '$.chat_id'
                            )
                            AND source.thread_id = json_extract(
                                authority.source_metadata_json, '$.thread_id'
                            )
                            AND source.message_id = json_extract(
                                authority.source_metadata_json, '$.message_id'
                            )
                            AND source.requester_id = json_extract(
                                authority.source_metadata_json, '$.requester_id'
                            )
                            AND source.mode = json_extract(
                                authority.source_metadata_json, '$.mode'
                            )
                        ), 0)
                    ) OR (
                        authority.source_kind = 'kafka_workflow_event'
                        AND NOT COALESCE((
                            inbox.topic = json_extract(
                                authority.source_metadata_json, '$.topic'
                            )
                            AND inbox.partition_id = json_extract(
                                authority.source_metadata_json, '$.partition'
                            )
                            AND inbox.offset_id = json_extract(
                                authority.source_metadata_json, '$.offset'
                            )
                            AND inbox.raw_sha256 = authority.payload_sha256
                            AND (
                                source.kafka_event_uid IS NULL
                                OR source.kafka_event_uid = inbox.event_uid
                            )
                            AND (
                                source.source_dedupe_key = inbox.event_uid
                                OR substr(
                                    source.source_dedupe_key,
                                    1,
                                    length(inbox.event_uid) + 12
                                ) = inbox.event_uid || ':generation:'
                            )
                        ), 0)
                    )
                 LIMIT 1
            """,
            "v11_envelope_authority_binding": """
                SELECT 1 FROM rca_snapshot_source_envelopes AS envelope
             LEFT JOIN rca_admission_snapshots AS snapshot
                    ON snapshot.snapshot_sha256 = envelope.snapshot_sha256
                   AND snapshot.snapshot_id = envelope.snapshot_id
                   AND snapshot.submission_key = envelope.submission_key
             LEFT JOIN rca_source_authority_receipts AS authority
                    ON authority.authority_sha256 = envelope.source_authority_sha256
                   AND authority.source_id = envelope.source_id
                   AND authority.source_kind = envelope.source_kind
                   AND authority.payload_sha256 = envelope.payload_sha256
                   AND authority.authorization_evidence_sha256 =
                       envelope.authorization_evidence_sha256
                   AND authority.binding_action = envelope.binding_action
                   AND authority.decision = envelope.decision
             LEFT JOIN rca_trigger_sources AS source
                    ON source.source_id = envelope.source_id
                   AND source.source_kind = envelope.source_kind
                   AND source.payload_sha256 = envelope.payload_sha256
                 WHERE snapshot.snapshot_sha256 IS NULL
                    OR authority.authority_sha256 IS NULL
                    OR source.source_id IS NULL
                 LIMIT 1
            """,
            "v11_envelope_source_business_binding": """
                SELECT 1
                  FROM rca_snapshot_source_envelopes AS envelope
             LEFT JOIN rca_admission_snapshots AS snapshot
                    ON snapshot.snapshot_sha256 = envelope.snapshot_sha256
                   AND snapshot.snapshot_id = envelope.snapshot_id
                   AND snapshot.submission_key = envelope.submission_key
             LEFT JOIN rca_trigger_bindings AS binding
                    ON binding.source_id = envelope.source_id
                   AND binding.business_key = snapshot.business_key
                   AND binding.generation = snapshot.generation
             LEFT JOIN business_triggers AS trigger
                    ON trigger.business_key = binding.business_key
                   AND trigger.generation = binding.generation
                   AND trigger.submission_key = snapshot.submission_key
             LEFT JOIN rca_trigger_sources AS source
                    ON source.source_id = binding.source_id
             LEFT JOIN kafka_inbox AS inbox
                    ON inbox.event_uid = json_extract(
                        envelope.source_metadata_json, '$.event_uid'
                    )
                 WHERE snapshot.snapshot_sha256 IS NULL
                    OR binding.source_id IS NULL
                    OR trigger.business_key IS NULL
                    OR source.source_id IS NULL
                    OR binding.role != CASE envelope.binding_action
                        WHEN 'create' THEN 'origin' ELSE 'observer' END
                    OR (
                        envelope.source_kind = 'kafka_workflow_event'
                        AND NOT COALESCE((
                            source.mode = CASE
                                WHEN snapshot.generation = 1
                                    THEN 'issue_created'
                                ELSE 'kafka_retrigger'
                            END
                            AND source.source_dedupe_key = CASE
                                WHEN snapshot.generation = 1
                                    THEN json_extract(
                                        envelope.source_metadata_json, '$.event_uid'
                                    )
                                ELSE json_extract(
                                    envelope.source_metadata_json, '$.event_uid'
                                ) || ':generation:' || snapshot.generation
                            END
                            AND source.kafka_event_uid IS CASE
                                WHEN snapshot.generation = 1
                                    THEN json_extract(
                                        envelope.source_metadata_json, '$.event_uid'
                                    )
                                ELSE NULL
                            END
                            AND inbox.topic = json_extract(
                                envelope.source_metadata_json, '$.topic'
                            )
                            AND inbox.partition_id = json_extract(
                                envelope.source_metadata_json, '$.partition'
                            )
                            AND inbox.offset_id = json_extract(
                                envelope.source_metadata_json, '$.offset'
                            )
                            AND inbox.raw_sha256 = envelope.payload_sha256
                        ), 0)
                    )
                 LIMIT 1
            """,
            "v11_snapshot_creator_binding": """
                SELECT 1 FROM rca_admission_snapshots AS snapshot
             LEFT JOIN rca_snapshot_source_envelopes AS creator
                    ON creator.source_envelope_sha256 =
                       snapshot.creator_source_envelope_sha256
                   AND creator.source_authority_sha256 =
                       snapshot.creator_authority_sha256
                   AND creator.source_id = snapshot.creator_source_id
                   AND creator.snapshot_sha256 = snapshot.snapshot_sha256
                   AND creator.snapshot_id = snapshot.snapshot_id
                   AND creator.submission_key = snapshot.submission_key
                   AND creator.binding_action = 'create'
                   AND creator.decision = snapshot.execution_decision
                 WHERE creator.source_envelope_sha256 IS NULL
                 LIMIT 1
            """,
            "v11_create_envelope_binding": """
                SELECT 1 FROM rca_snapshot_source_envelopes AS creator
             LEFT JOIN rca_admission_snapshots AS snapshot
                    ON snapshot.snapshot_sha256 = creator.snapshot_sha256
                   AND snapshot.snapshot_id = creator.snapshot_id
                   AND snapshot.submission_key = creator.submission_key
                   AND snapshot.creator_source_envelope_sha256 =
                       creator.source_envelope_sha256
                   AND snapshot.creator_authority_sha256 =
                       creator.source_authority_sha256
                   AND snapshot.creator_source_id = creator.source_id
                   AND snapshot.execution_decision = creator.decision
             LEFT JOIN rca_canonical_requests AS request
                    ON request.request_sha256 = snapshot.request_sha256
             LEFT JOIN rca_source_authority_receipts AS authority
                    ON authority.authority_sha256 =
                       creator.source_authority_sha256
                 WHERE creator.binding_action = 'create'
                   AND (
                        snapshot.snapshot_sha256 IS NULL
                        OR (
                            snapshot.generation = 1
                            AND NOT (
                                request.generation_reason = 'initial'
                                AND request.generation_authorization_evidence_sha256
                                    IS NULL
                            )
                        )
                        OR (
                            snapshot.generation > 1
                            AND NOT (
                                request.generation_reason =
                                    'explicit_user_rerun'
                                AND request.generation_authorization_evidence_sha256
                                    IS NOT NULL
                                AND authority.binding_action = 'create'
                                AND authority.source_kind =
                                    'feishu_group_manual'
                                AND authority.authorization_evidence_sha256 =
                                    request.generation_authorization_evidence_sha256
                                AND json_extract(
                                    authority.source_metadata_json, '$.platform'
                                ) = 'feishu'
                                AND json_extract(
                                    authority.source_metadata_json, '$.mode'
                                ) = 'rerun'
                                AND substr(
                                    json_extract(
                                        authority.source_metadata_json,
                                        '$.requester_id'
                                    ),
                                    1,
                                    3
                                ) = 'ou_'
                                AND length(
                                    json_extract(
                                        authority.source_metadata_json,
                                        '$.requester_id'
                                    )
                                ) > 3
                            )
                        )
                   )
                 LIMIT 1
            """,
            "v11_join_envelope_binding": """
                SELECT 1 FROM rca_snapshot_source_envelopes AS joined
             LEFT JOIN rca_admission_snapshots AS snapshot
                    ON snapshot.snapshot_sha256 = joined.snapshot_sha256
                   AND snapshot.snapshot_id = joined.snapshot_id
                   AND snapshot.submission_key = joined.submission_key
             LEFT JOIN rca_snapshot_source_envelopes AS creator
                    ON creator.source_envelope_sha256 =
                       snapshot.creator_source_envelope_sha256
                   AND creator.source_authority_sha256 =
                       snapshot.creator_authority_sha256
                   AND creator.source_id = snapshot.creator_source_id
                   AND creator.snapshot_sha256 = snapshot.snapshot_sha256
                   AND creator.binding_action = 'create'
                   AND joined.decision = snapshot.execution_decision
                 WHERE joined.binding_action = 'join'
                   AND creator.source_envelope_sha256 IS NULL
                 LIMIT 1
            """,
        }
        for error, query in integrity_queries.items():
            if conn.execute(query).fetchone() is not None:
                raise RuntimeError(f"incompatible_control_store_schema:{error}")

    @staticmethod
    def _validate_structural_contract(
        conn: sqlite3.Connection,
        *,
        integrity_check: bool,
        allow_known_legacy_binding_guard: bool = False,
    ) -> None:
        """Refuse to label a legacy lookalike as the current durable schema."""
        try:
            recursive_triggers = conn.execute("PRAGMA recursive_triggers").fetchone()
        except sqlite3.Error as exc:
            raise RuntimeError(
                "incompatible_control_store_schema:recursive_triggers"
            ) from exc
        if recursive_triggers is None or int(recursive_triggers[0]) != 1:
            raise RuntimeError(
                "incompatible_control_store_schema:recursive_triggers"
            )
        validate_host_runtime_transition_schema(
            conn,
            error_prefix="incompatible_control_store_schema",
        )
        RcaControlStore._validate_v11_snapshot_schema(conn)
        marker = conn.execute(
            "SELECT value FROM control_meta WHERE key = 'schema_version'"
        ).fetchone()
        marker_value = str(marker["value"]) if marker is not None else ""
        v12_tables_present = any(
            RcaControlStore._table_exists(conn, table)
            for table in (
                "rca_learning_lane_cohorts",
                "rca_learning_lane_stock_items",
                "rca_learning_lane_admissions",
            )
        )
        if (
            marker_value
            in {
                "pnc_rca_control_store_v12",
                "pnc_rca_control_store_v13",
                CONTROL_STORE_SCHEMA_VERSION,
                CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION,
            }
            or v12_tables_present
        ):
            RcaControlStore._validate_v12_learning_lane_schema(conn)
        v14_table_present = RcaControlStore._table_exists(
            conn, "rca_terminal_rerun_delivery_authorities"
        )
        if (
            marker_value
            in {
                CONTROL_STORE_SCHEMA_VERSION,
                CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION,
            }
            or v14_table_present
        ):
            RcaControlStore._validate_v14_terminal_rerun_delivery_authority_schema(
                conn,
                allow_known_legacy_binding_guard=allow_known_legacy_binding_guard,
            )
        historical_epoch_rerun_table_present = RcaControlStore._table_exists(
            conn, "rca_historical_epoch_rerun_delivery_authorities"
        )
        if (
            marker_value
            in {
                CONTROL_STORE_SCHEMA_VERSION,
                CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION,
            }
            or historical_epoch_rerun_table_present
        ):
            RcaControlStore._validate_historical_epoch_rerun_delivery_authority_schema(
                conn
            )

        def foreign_key_groups(
            table: str,
        ) -> dict[tuple[int, str], set[tuple[str, str]]]:
            groups: dict[tuple[int, str], set[tuple[str, str]]] = {}
            for row in conn.execute(f"PRAGMA foreign_key_list({table})").fetchall():
                key = (int(row["id"]), str(row["table"]))
                groups.setdefault(key, set()).add((str(row["from"]), str(row["to"])))
            return groups

        outbox_groups = foreign_key_groups("rca_outbox")
        if not any(
            table == "business_triggers"
            and pairs == {
                ("business_key", "business_key"),
                ("generation", "generation"),
            }
            for (_identifier, table), pairs in outbox_groups.items()
        ):
            raise RuntimeError("incompatible_control_store_schema:rca_outbox_foreign_keys")
        required_single_foreign_keys = {
            "kafka_partition_progress": ("kafka_inbox", "last_event_uid", "event_uid"),
            "kafka_dead_letters": ("kafka_inbox", "source_event_id", "event_uid"),
            "rca_trigger_sources": ("kafka_inbox", "kafka_event_uid", "event_uid"),
            "rca_trigger_bindings": ("rca_trigger_sources", "source_id", "source_id"),
        }
        for table, (parent, source, target) in required_single_foreign_keys.items():
            groups = foreign_key_groups(table)
            if not any(
                ref_table == parent and pairs == {(source, target)}
                for (_identifier, ref_table), pairs in groups.items()
            ):
                raise RuntimeError(
                    f"incompatible_control_store_schema:{table}_foreign_keys"
                )
        required_indexes = {
            "idx_business_triggers_issue_scope",
            "idx_rca_manual_operator_rate",
            "idx_rca_single_current_activation_epoch",
            "idx_rca_activation_ledger_submission",
            "idx_rca_activation_transition_epoch",
            "idx_outbox_activation_claim",
            "idx_outbox_source_status",
            "idx_trigger_bindings_generation",
            "idx_trigger_sources_kind_outcome",
        }
        present_indexes = {
            str(row["name"])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'index'"
            ).fetchall()
        }
        if not required_indexes.issubset(present_indexes):
            raise RuntimeError("incompatible_control_store_schema:required_indexes")
        epoch_activation_columns = (
            frozenset(_V15_ACTIVATION_EPOCH_COLUMNS)
            if marker_value == CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION
            else _V14_COMPAT_RELEASE_BINDING_COLUMNS
        )
        required_activation_columns = {
            "kafka_inbox": {
                "activation_epoch_id",
                "activation_ingress_state",
                "activation_required",
                "activation_source_identity_sha256",
                "submit_enabled_requested",
            },
            "business_triggers": {"activation_epoch_id", "activation_ledger_id"},
            "rca_outbox": {"activation_epoch_id", "activation_ledger_id"},
            "rca_activation_epochs": epoch_activation_columns,
        }
        for table, required_columns in required_activation_columns.items():
            present_columns = {
                str(row["name"])
                for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
            }
            if not required_columns.issubset(present_columns):
                raise RuntimeError(
                    f"incompatible_control_store_schema:{table}_activation_columns"
                )
        required_activation_tables = {
            "rca_activation_epochs",
            "rca_activation_admission_ledger",
            "rca_activation_transition_audit",
        }
        present_tables = {
            str(row["name"])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        if not required_activation_tables.issubset(present_tables):
            raise RuntimeError("incompatible_control_store_schema:activation_tables")
        for table in ("business_triggers", "rca_outbox"):
            invalid_binding = conn.execute(
                f"""
                SELECT 1 FROM {table} AS child
           LEFT JOIN rca_activation_admission_ledger AS al
                  ON al.epoch_id = child.activation_epoch_id
                 AND al.ledger_id = child.activation_ledger_id
                 AND al.business_key = child.business_key
                 AND al.submission_key = child.submission_key
                 AND al.generation = child.generation
               WHERE (child.activation_epoch_id IS NULL)
                     != (child.activation_ledger_id IS NULL)
                  OR (child.activation_ledger_id IS NOT NULL AND al.ledger_id IS NULL)
               LIMIT 1
                """
            ).fetchone()
            if invalid_binding is not None:
                raise RuntimeError(
                    f"incompatible_control_store_schema:{table}_activation_binding"
                )
        if integrity_check:
            quick_check = conn.execute("PRAGMA quick_check").fetchone()
            if quick_check is None or str(quick_check[0]).lower() != "ok":
                raise RuntimeError("incompatible_control_store_schema:quick_check")
            if conn.execute("PRAGMA foreign_key_check").fetchone() is not None:
                raise RuntimeError("incompatible_control_store_schema:foreign_key_check")

    def journal_settings(self) -> dict[str, Any]:
        conn = self._connect()
        try:
            return {
                "journal_mode": str(
                    conn.execute("PRAGMA journal_mode").fetchone()[0]
                ).lower(),
                "synchronous": int(conn.execute("PRAGMA synchronous").fetchone()[0]),
                "foreign_keys": int(conn.execute("PRAGMA foreign_keys").fetchone()[0]),
            }
        finally:
            conn.close()

    def read_w3_execution_snapshot(
        self,
        submission_key: str,
        *,
        snapshot_authority: Any,
        required: bool = False,
    ) -> Any | None:
        """Read and independently validate the immutable W3 execution authority."""
        key = str(submission_key or "").strip()
        if not key:
            raise ValueError("w3_execution_snapshot_submission_key_required")
        if not isinstance(required, bool):
            raise TypeError("required must be true or false")
        approved_authority = _normalize_w3_snapshot_authority(snapshot_authority)
        if approved_authority is None:
            raise ValueError("w3_snapshot_authority_required")
        approved_policy_pins = {
            name: str(policy["sha256"])
            for name, policy in approved_authority["policies"].items()
        }
        conn = self._connect()
        try:
            conn.execute("BEGIN")
            snapshot_row = conn.execute(
                "SELECT * FROM rca_admission_snapshots WHERE submission_key = ?",
                (key,),
            ).fetchone()
            if snapshot_row is None:
                conn.commit()
                if required:
                    raise RecordConflictError("w3_execution_snapshot_missing")
                return None
            request_row = conn.execute(
                "SELECT * FROM rca_canonical_requests WHERE request_sha256 = ?",
                (snapshot_row["request_sha256"],),
            ).fetchone()
            envelope_row = conn.execute(
                """
                SELECT * FROM rca_snapshot_source_envelopes
                 WHERE source_envelope_sha256 = ?
                   AND source_authority_sha256 = ?
                   AND source_id = ?
                   AND binding_action = 'create'
                """,
                (
                    snapshot_row["creator_source_envelope_sha256"],
                    snapshot_row["creator_authority_sha256"],
                    snapshot_row["creator_source_id"],
                ),
            ).fetchone()
            authority_row = (
                conn.execute(
                    """
                    SELECT * FROM rca_source_authority_receipts
                     WHERE authority_sha256 = ? AND source_id = ?
                    """,
                    (
                        snapshot_row["creator_authority_sha256"],
                        snapshot_row["creator_source_id"],
                    ),
                ).fetchone()
                if envelope_row is not None
                else None
            )
            lineage_row = conn.execute(
                """
                SELECT outbox.origin_source_id AS outbox_origin_source_id,
                       trigger.origin_source_id AS trigger_origin_source_id,
                       binding.role AS binding_role,
                       binding.business_key AS binding_business_key,
                       binding.generation AS binding_generation,
                       trigger.submission_key AS trigger_submission_key
                  FROM rca_outbox AS outbox
                  JOIN business_triggers AS trigger
                    ON trigger.business_key = outbox.business_key
                   AND trigger.generation = outbox.generation
                  JOIN rca_trigger_bindings AS binding
                    ON binding.source_id = outbox.origin_source_id
                   AND binding.business_key = outbox.business_key
                   AND binding.generation = outbox.generation
                 WHERE outbox.submission_key = ?
                """,
                (key,),
            ).fetchone()
            conn.commit()
        except Exception:
            if conn.in_transaction:
                conn.rollback()
            raise
        finally:
            conn.close()

        if request_row is None or envelope_row is None or authority_row is None:
            raise RecordConflictError("w3_execution_snapshot_authority_missing")
        if lineage_row is None:
            raise RecordConflictError("w3_execution_snapshot_lineage_missing")

        from gateway.pnc_rca_snapshot import (
            build_snapshot_execution_bundle,
            strict_canonical_json_loads,
            validate_admission_snapshot,
            validate_snapshot_source_envelope,
        )

        def canonical_object(row: sqlite3.Row, column: str) -> dict[str, Any]:
            value = strict_canonical_json_loads(str(row[column]))
            if not isinstance(value, dict):
                raise ValueError(f"w3_execution_snapshot_{column}_invalid")
            return value

        try:
            snapshot_payload = canonical_object(
                snapshot_row,
                "admission_snapshot_json",
            )
            request_payload = canonical_object(request_row, "canonical_request_json")
            envelope_payload = canonical_object(envelope_row, "source_envelope_json")
            metadata_payload = canonical_object(envelope_row, "source_metadata_json")
            anchor_payload = canonical_object(envelope_row, "anchor_json")
            ingress_payload = canonical_object(envelope_row, "ingress_decision_json")
            authority_payload = canonical_object(
                authority_row,
                "authority_receipt_json",
            )
            authority_metadata = canonical_object(
                authority_row,
                "source_metadata_json",
            )
            authority_anchor = canonical_object(authority_row, "anchor_json")
            authority_ingress = canonical_object(
                authority_row,
                "ingress_decision_json",
            )
            policy_pins = {
                "creation_policy": str(request_row["creation_policy_sha256"]),
                "business_profile": str(request_row["business_profile_sha256"]),
                "execution_policy": str(request_row["execution_policy_sha256"]),
                "publication_policy": str(request_row["publication_policy_sha256"]),
                "correction_lineage_policy": str(
                    request_row["correction_lineage_policy_sha256"]
                ),
            }
            if policy_pins != approved_policy_pins:
                raise RecordConflictError(
                    "w3_execution_snapshot_authority_mismatch"
                )
            snapshot = validate_admission_snapshot(
                snapshot_payload,
                expected_snapshot_sha256=str(snapshot_row["snapshot_sha256"]),
                expected_generation_authorization_evidence_sha256=request_row[
                    "generation_authorization_evidence_sha256"
                ],
                expected_ticket_title_sha256=str(
                    request_row["ticket_title_sha256"]
                ),
                expected_policy_sha256s=policy_pins,
            )
            envelope = validate_snapshot_source_envelope(
                envelope_payload,
                expected_snapshot=snapshot,
                expected_authorization_evidence_sha256=str(
                    authority_row["authorization_evidence_sha256"]
                ),
                expected_generation_authorization_evidence_sha256=request_row[
                    "generation_authorization_evidence_sha256"
                ],
                expected_ticket_title_sha256=str(
                    request_row["ticket_title_sha256"]
                ),
                expected_source_payload_sha256=str(authority_row["payload_sha256"]),
                expected_policy_sha256s=policy_pins,
                expected_snapshot_sha256=str(snapshot_row["snapshot_sha256"]),
                expected_source_authority=authority_payload,
            )
            if request_payload != snapshot.canonical_request.to_dict():
                raise ValueError("w3_execution_snapshot_request_json_mismatch")
            if (
                metadata_payload != envelope_payload["source_metadata"]
                or anchor_payload != envelope_payload["anchor"]
                or ingress_payload != envelope_payload["ingress_decision"]
                or authority_metadata != metadata_payload
                or authority_anchor != anchor_payload
                or authority_ingress != ingress_payload
            ):
                raise ValueError("w3_execution_snapshot_projection_mismatch")

            request_columns = {
                "request_sha256": snapshot.canonical_request.request_sha256,
                "schema_version": snapshot.canonical_request.schema_version,
                "ticket_title_sha256": snapshot.canonical_request.ticket[
                    "title_sha256"
                ],
                **{
                    f"{name}_sha256": getattr(
                        snapshot.canonical_request,
                        name,
                    )["sha256"]
                    for name in (
                        "creation_policy",
                        "business_profile",
                        "execution_policy",
                        "publication_policy",
                        "correction_lineage_policy",
                    )
                },
                "generation_reason": snapshot.canonical_request.execution_intent[
                    "generation_reason"
                ],
                "generation_authorization_evidence_sha256": (
                    snapshot.canonical_request.execution_intent[
                        "generation_authorization_evidence_sha256"
                    ]
                ),
            }
            snapshot_columns = {
                "snapshot_sha256": snapshot.snapshot_sha256,
                "snapshot_id": snapshot.snapshot_id,
                "schema_version": snapshot.schema_version,
                "request_sha256": snapshot.request_sha256,
                "business_key": snapshot.resolved_admission["business_key"],
                "submission_key": snapshot.resolved_admission["submission_key"],
                "generation": snapshot.resolved_admission["generation"],
                "activation_epoch_id": snapshot.execution_admission[
                    "activation_epoch_id"
                ],
                "activation_ledger_id": snapshot.execution_admission[
                    "activation_ledger_id"
                ],
                "execution_decision": snapshot.execution_admission["decision"],
                "execution_reason": snapshot.execution_admission["reason"],
                "execution_state": snapshot.execution_admission["state"],
                "legacy_unconfigured": int(
                    bool(snapshot.execution_admission["legacy_unconfigured"])
                ),
                "creator_source_envelope_sha256": envelope.source_envelope_sha256,
                "creator_authority_sha256": envelope.source_authority_sha256,
                "creator_source_id": envelope.source_id,
            }
            envelope_columns = {
                "source_envelope_sha256": envelope.source_envelope_sha256,
                "source_envelope_id": envelope.source_envelope_id,
                "schema_version": envelope.schema_version,
                "snapshot_sha256": envelope.snapshot_sha256,
                "snapshot_id": envelope.snapshot_id,
                "submission_key": envelope.submission_key,
                "source_authority_sha256": envelope.source_authority_sha256,
                "source_id": envelope.source_id,
                "source_kind": envelope.source_kind,
                "payload_sha256": envelope.source_metadata["payload_sha256"],
                "authorization_evidence_sha256": envelope.ingress_decision[
                    "authorization_evidence_sha256"
                ],
                "binding_action": envelope.ingress_decision["binding_action"],
                "decision": envelope.ingress_decision["decision"],
            }
            authority_columns = {
                "authority_sha256": authority_payload["authority_sha256"],
                "schema_version": authority_payload["schema_version"],
                "source_id": envelope.source_id,
                "source_kind": envelope.source_kind,
                "payload_sha256": envelope.source_metadata["payload_sha256"],
                "authorization_evidence_sha256": envelope.ingress_decision[
                    "authorization_evidence_sha256"
                ],
                "binding_action": envelope.ingress_decision["binding_action"],
                "decision": envelope.ingress_decision["decision"],
                "source_metadata_sha256": authority_payload[
                    "source_metadata_sha256"
                ],
                "anchor_sha256": authority_payload["anchor_sha256"],
                "ingress_decision_sha256": authority_payload[
                    "ingress_decision_sha256"
                ],
            }
            for row, expected in (
                (request_row, request_columns),
                (snapshot_row, snapshot_columns),
                (envelope_row, envelope_columns),
                (authority_row, authority_columns),
            ):
                if any(row[column] != expected_value for column, expected_value in expected.items()):
                    raise ValueError("w3_execution_snapshot_column_mismatch")
            if (
                lineage_row["outbox_origin_source_id"] != envelope.source_id
                or lineage_row["trigger_origin_source_id"] != envelope.source_id
                or lineage_row["binding_role"] != "origin"
                or lineage_row["binding_business_key"]
                != snapshot.resolved_admission["business_key"]
                or lineage_row["binding_generation"]
                != snapshot.resolved_admission["generation"]
                or lineage_row["trigger_submission_key"]
                != snapshot.resolved_admission["submission_key"]
            ):
                raise ValueError("w3_execution_snapshot_origin_lineage_mismatch")
            return build_snapshot_execution_bundle(
                snapshot=snapshot,
                snapshot_authority_sha256=str(
                    approved_authority["authority_sha256"]
                ),
                creator_source_envelope=envelope,
                creator_source_authority=authority_payload,
            )
        except RecordConflictError:
            raise
        except Exception as exc:
            raise RecordConflictError("w3_execution_snapshot_invalid") from exc

    def persist_admission_snapshot_source(
        self,
        *,
        snapshot: Any,
        source_envelope: Any,
        expected_source_authority: Mapping[str, Any],
        expected_snapshot_sha256: str,
        expected_generation_authorization_evidence_sha256: str | None = None,
        expected_ticket_title_sha256: str,
        expected_source_payload_sha256: str,
        expected_authorization_evidence_sha256: str,
        expected_policy_sha256s: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Persist one validated W3 creator or join in a private transaction."""
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            result = self.persist_admission_snapshot_source_tx(
                conn,
                snapshot=snapshot,
                source_envelope=source_envelope,
                expected_source_authority=expected_source_authority,
                expected_snapshot_sha256=expected_snapshot_sha256,
                expected_generation_authorization_evidence_sha256=(
                    expected_generation_authorization_evidence_sha256
                ),
                expected_ticket_title_sha256=expected_ticket_title_sha256,
                expected_source_payload_sha256=expected_source_payload_sha256,
                expected_authorization_evidence_sha256=(
                    expected_authorization_evidence_sha256
                ),
                expected_policy_sha256s=expected_policy_sha256s,
            )
            conn.commit()
            return result
        except Exception:
            if conn.in_transaction:
                conn.rollback()
            raise
        finally:
            conn.close()

    def persist_admission_snapshot_source_tx(
        self,
        conn: sqlite3.Connection,
        *,
        snapshot: Any,
        source_envelope: Any,
        expected_source_authority: Mapping[str, Any],
        expected_snapshot_sha256: str,
        expected_generation_authorization_evidence_sha256: str | None = None,
        expected_ticket_title_sha256: str,
        expected_source_payload_sha256: str,
        expected_authorization_evidence_sha256: str,
        expected_policy_sha256s: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Persist W3 authority inside the caller's existing transaction."""
        if not isinstance(conn, sqlite3.Connection) or not conn.in_transaction:
            raise RuntimeError("w3_snapshot_transaction_required")
        return self._persist_admission_snapshot_source(
            conn,
            snapshot=snapshot,
            source_envelope=source_envelope,
            expected_source_authority=expected_source_authority,
            expected_snapshot_sha256=expected_snapshot_sha256,
            expected_generation_authorization_evidence_sha256=(
                expected_generation_authorization_evidence_sha256
            ),
            expected_ticket_title_sha256=expected_ticket_title_sha256,
            expected_source_payload_sha256=expected_source_payload_sha256,
            expected_authorization_evidence_sha256=(
                expected_authorization_evidence_sha256
            ),
            expected_policy_sha256s=expected_policy_sha256s,
        )

    def _persist_admission_snapshot_source(
        self,
        conn: sqlite3.Connection,
        *,
        snapshot: Any,
        source_envelope: Any,
        expected_source_authority: Mapping[str, Any],
        expected_snapshot_sha256: str,
        expected_generation_authorization_evidence_sha256: str | None = None,
        expected_ticket_title_sha256: str,
        expected_source_payload_sha256: str,
        expected_authorization_evidence_sha256: str,
        expected_policy_sha256s: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Persist one validated W3 creator or join without exposing partial roots."""
        from gateway.pnc_rca_snapshot import (
            canonical_json_bytes as canonical_w3_json_bytes,
            validate_admission_snapshot,
            validate_snapshot_source_envelope,
        )

        receipt = dict(expected_source_authority)
        policy_pins = dict(expected_policy_sha256s)
        core = validate_admission_snapshot(
            snapshot,
            expected_snapshot_sha256=expected_snapshot_sha256,
            expected_generation_authorization_evidence_sha256=(
                expected_generation_authorization_evidence_sha256
            ),
            expected_ticket_title_sha256=expected_ticket_title_sha256,
            expected_policy_sha256s=policy_pins,
        )
        if (
            core.execution_admission["state"] != "steady_active"
            or core.execution_admission["decision"] != "admit"
            or core.execution_admission["legacy_unconfigured"]
            or not core.execution_admission["activation_epoch_id"]
            or core.execution_admission["activation_ledger_id"] is None
        ):
            raise RecordConflictError("w3_snapshot_steady_activation_required")
        envelope = validate_snapshot_source_envelope(
            source_envelope,
            expected_snapshot=core,
            expected_authorization_evidence_sha256=(
                expected_authorization_evidence_sha256
            ),
            expected_generation_authorization_evidence_sha256=(
                expected_generation_authorization_evidence_sha256
            ),
            expected_ticket_title_sha256=expected_ticket_title_sha256,
            expected_source_payload_sha256=expected_source_payload_sha256,
            expected_policy_sha256s=policy_pins,
            expected_snapshot_sha256=expected_snapshot_sha256,
            expected_source_authority=receipt,
        )
        request = core.canonical_request
        request_json = canonical_w3_json_bytes(request.to_dict()).decode("utf-8")
        snapshot_json = canonical_w3_json_bytes(core.to_dict()).decode("utf-8")
        metadata_json = canonical_w3_json_bytes(
            dict(envelope.source_metadata)
        ).decode("utf-8")
        anchor_json = canonical_w3_json_bytes(dict(envelope.anchor)).decode("utf-8")
        ingress_json = canonical_w3_json_bytes(
            dict(envelope.ingress_decision)
        ).decode("utf-8")
        receipt_json = canonical_w3_json_bytes(receipt).decode("utf-8")
        envelope_json = canonical_w3_json_bytes(envelope.to_dict()).decode("utf-8")
        persisted_at = _now_iso()
        binding_action = str(envelope.ingress_decision["binding_action"])

        request_values = {
            "request_sha256": request.request_sha256,
            "schema_version": request.schema_version,
            "ticket_title_sha256": str(request.ticket["title_sha256"]),
            "creation_policy_sha256": str(request.creation_policy["sha256"]),
            "business_profile_sha256": str(request.business_profile["sha256"]),
            "execution_policy_sha256": str(request.execution_policy["sha256"]),
            "publication_policy_sha256": str(
                request.publication_policy["sha256"]
            ),
            "correction_lineage_policy_sha256": str(
                request.correction_lineage_policy["sha256"]
            ),
            "generation_reason": str(
                request.execution_intent["generation_reason"]
            ),
            "generation_authorization_evidence_sha256": (
                request.execution_intent[
                    "generation_authorization_evidence_sha256"
                ]
            ),
            "canonical_request_json": request_json,
            "persisted_at": persisted_at,
        }
        authority_values = {
            "authority_sha256": str(receipt["authority_sha256"]),
            "schema_version": str(receipt["schema_version"]),
            "source_id": envelope.source_id,
            "source_kind": envelope.source_kind,
            "payload_sha256": str(envelope.source_metadata["payload_sha256"]),
            "authorization_evidence_sha256": str(
                envelope.ingress_decision["authorization_evidence_sha256"]
            ),
            "binding_action": binding_action,
            "decision": str(envelope.ingress_decision["decision"]),
            "source_metadata_sha256": str(receipt["source_metadata_sha256"]),
            "anchor_sha256": str(receipt["anchor_sha256"]),
            "ingress_decision_sha256": str(receipt["ingress_decision_sha256"]),
            "source_metadata_json": metadata_json,
            "anchor_json": anchor_json,
            "ingress_decision_json": ingress_json,
            "authority_receipt_json": receipt_json,
            "persisted_at": persisted_at,
        }
        snapshot_values = {
            "snapshot_sha256": core.snapshot_sha256,
            "snapshot_id": core.snapshot_id,
            "schema_version": core.schema_version,
            "request_sha256": core.request_sha256,
            "business_key": str(core.resolved_admission["business_key"]),
            "submission_key": str(core.resolved_admission["submission_key"]),
            "generation": int(core.resolved_admission["generation"]),
            "activation_epoch_id": str(
                core.execution_admission["activation_epoch_id"]
            ),
            "activation_ledger_id": core.execution_admission[
                "activation_ledger_id"
            ],
            "execution_decision": str(core.execution_admission["decision"]),
            "execution_reason": str(core.execution_admission["reason"]),
            "execution_state": str(core.execution_admission["state"]),
            "legacy_unconfigured": int(
                bool(core.execution_admission["legacy_unconfigured"])
            ),
            "creator_source_envelope_sha256": envelope.source_envelope_sha256,
            "creator_authority_sha256": envelope.source_authority_sha256,
            "creator_source_id": envelope.source_id,
            "admission_snapshot_json": snapshot_json,
            "persisted_at": persisted_at,
        }
        envelope_values = {
            "source_envelope_sha256": envelope.source_envelope_sha256,
            "source_envelope_id": envelope.source_envelope_id,
            "schema_version": envelope.schema_version,
            "snapshot_sha256": envelope.snapshot_sha256,
            "snapshot_id": envelope.snapshot_id,
            "submission_key": envelope.submission_key,
            "source_authority_sha256": envelope.source_authority_sha256,
            "source_id": envelope.source_id,
            "source_kind": envelope.source_kind,
            "payload_sha256": str(envelope.source_metadata["payload_sha256"]),
            "authorization_evidence_sha256": str(
                envelope.ingress_decision["authorization_evidence_sha256"]
            ),
            "binding_action": binding_action,
            "decision": str(envelope.ingress_decision["decision"]),
            "source_metadata_json": metadata_json,
            "anchor_json": anchor_json,
            "ingress_decision_json": ingress_json,
            "source_envelope_json": envelope_json,
            "persisted_at": persisted_at,
        }

        def ensure_exact_tx(
            conn: sqlite3.Connection,
            *,
            table: str,
            key: str,
            values: Mapping[str, Any],
        ) -> bool:
            immutable_columns = tuple(
                column for column in values if column != "persisted_at"
            )
            row = conn.execute(
                f"SELECT {', '.join(immutable_columns)} FROM {table} WHERE {key} = ?",
                (values[key],),
            ).fetchone()
            if row is not None:
                if any(row[column] != values[column] for column in immutable_columns):
                    raise RecordConflictError(f"w3_{table}_binding_conflict")
                return False
            columns = tuple(values)
            conn.execute(
                f"INSERT INTO {table}({', '.join(columns)}) "
                f"VALUES ({', '.join('?' for _ in columns)})",
                tuple(values[column] for column in columns),
            )
            return True

        try:
            snapshot_row = conn.execute(
                "SELECT * FROM rca_admission_snapshots WHERE snapshot_sha256 = ?",
                (core.snapshot_sha256,),
            ).fetchone()
            durable_source = conn.execute(
                "SELECT * FROM rca_trigger_sources WHERE source_id = ?",
                (envelope.source_id,),
            ).fetchone()
            if durable_source is None:
                raise RecordConflictError("w3_snapshot_source_authority_mismatch")

            def normalized_timestamp(value: Any) -> str:
                text = str(value or "").strip()
                candidate = text[:-1] + "+00:00" if text.endswith("Z") else text
                try:
                    parsed = datetime.fromisoformat(candidate)
                except (TypeError, ValueError, OverflowError) as exc:
                    raise RecordConflictError(
                        "w3_snapshot_source_authority_mismatch"
                    ) from exc
                if parsed.tzinfo is None or parsed.utcoffset() is None:
                    raise RecordConflictError(
                        "w3_snapshot_source_authority_mismatch"
                    )
                return parsed.astimezone(timezone.utc).isoformat()

            metadata = envelope.source_metadata
            source_matches = (
                durable_source["source_kind"] == envelope.source_kind
                and durable_source["payload_sha256"] == metadata["payload_sha256"]
                and normalized_timestamp(durable_source["created_at"])
                == metadata["observed_at"]
            )
            if envelope.source_kind == "feishu_group_manual":
                source_matches = source_matches and all(
                    durable_source[column] == metadata[column]
                    for column in (
                        "platform",
                        "chat_id",
                        "thread_id",
                        "message_id",
                        "requester_id",
                        "mode",
                    )
                )
            else:
                generation = int(core.resolved_admission["generation"])
                event_uid = str(metadata["event_uid"])
                expected_mode = "issue_created" if generation == 1 else "kafka_retrigger"
                expected_dedupe = (
                    event_uid
                    if generation == 1
                    else f"{event_uid}:generation:{generation}"
                )
                expected_kafka_event_uid = event_uid if generation == 1 else None
                inbox = conn.execute(
                    """
                    SELECT 1 FROM kafka_inbox
                     WHERE event_uid = ? AND topic = ?
                       AND partition_id = ? AND offset_id = ?
                       AND raw_sha256 = ?
                    """,
                    (
                        event_uid,
                        metadata["topic"],
                        metadata["partition"],
                        metadata["offset"],
                        metadata["payload_sha256"],
                    ),
                ).fetchone()
                source_matches = source_matches and (
                    durable_source["source_dedupe_key"] == expected_dedupe
                    and durable_source["mode"] == expected_mode
                    and durable_source["kafka_event_uid"]
                    == expected_kafka_event_uid
                    and inbox is not None
                )
            if not source_matches:
                raise RecordConflictError("w3_snapshot_source_authority_mismatch")

            expected_role = "origin" if binding_action == "create" else "observer"
            durable_binding = conn.execute(
                """
                SELECT 1
                  FROM rca_trigger_bindings AS binding
                  JOIN business_triggers AS trigger
                    ON trigger.business_key = binding.business_key
                   AND trigger.generation = binding.generation
                 WHERE binding.source_id = ?
                   AND binding.business_key = ?
                   AND binding.generation = ?
                   AND binding.role = ?
                   AND trigger.submission_key = ?
                """,
                (
                    envelope.source_id,
                    core.resolved_admission["business_key"],
                    core.resolved_admission["generation"],
                    expected_role,
                    core.resolved_admission["submission_key"],
                ),
            ).fetchone()
            if durable_binding is None:
                raise RecordConflictError("w3_snapshot_source_binding_mismatch")

            activation_ledger_id = core.execution_admission["activation_ledger_id"]
            establishing_snapshot = binding_action == "create" and snapshot_row is None
            if establishing_snapshot and activation_ledger_id is not None:
                current_epoch = self._current_activation_epoch_tx(conn)
                if (
                    current_epoch is None
                    or str(current_epoch["epoch_id"])
                    != str(core.execution_admission["activation_epoch_id"])
                    or str(current_epoch["state"] or "") != "steady_active"
                ):
                    raise RecordConflictError(
                        "w3_snapshot_execution_authority_mismatch"
                    )
                activation_source_kind = (
                    "manual"
                    if envelope.source_kind == "feishu_group_manual"
                    else "kafka"
                )
                activation_identity: dict[str, Any]
                if activation_source_kind == "manual":
                    activation_chat_id = metadata["chat_id"]
                    activation_thread_id = metadata["thread_id"]
                    if metadata["platform"] == "operator":
                        activation_chat_id = "operator"
                        activation_thread_id = "operator:issue-only"
                    activation_identity = {
                        "chat_id": activation_chat_id,
                        "requester_id": metadata["requester_id"],
                        "message_id": metadata["message_id"],
                        "thread_id": activation_thread_id,
                        "issue_url": core.canonical_request.ticket["issue_url"],
                        "mode": metadata["mode"],
                    }
                else:
                    activation_identity = {"event_uid": metadata["event_uid"]}
                source_identity_sha256, _normalized_identity = (
                    self._normalize_activation_source_identity(
                        activation_source_kind,
                        activation_identity,
                    )
                )
                admission_key = self._activation_admission_key(
                    source_kind=activation_source_kind,
                    source_identity_sha256=source_identity_sha256,
                    business_key=str(core.resolved_admission["business_key"]),
                    submission_key=str(core.resolved_admission["submission_key"]),
                    generation=int(core.resolved_admission["generation"]),
                )
                expected_entrypoint = (
                    "manual_admit"
                    if activation_source_kind == "manual"
                    else "kafka_ingest"
                )
                durable_execution = conn.execute(
                    """
                    SELECT ledger.reason
                      FROM rca_activation_admission_ledger AS ledger
                      JOIN rca_activation_epochs AS epoch
                        ON epoch.epoch_id = ledger.epoch_id
                     WHERE ledger.ledger_id = ?
                       AND ledger.epoch_id = ?
                       AND ledger.business_key = ?
                       AND ledger.submission_key = ?
                       AND ledger.generation = ?
                       AND ledger.admission_key = ?
                       AND ledger.entrypoint = ?
                       AND ledger.source_kind = ?
                       AND ledger.source_identity_sha256 = ?
                       AND ledger.decision = ?
                       AND epoch.state = ?
                       AND epoch.is_current = 1
                    """,
                    (
                        activation_ledger_id,
                        core.execution_admission["activation_epoch_id"],
                        core.resolved_admission["business_key"],
                        core.resolved_admission["submission_key"],
                        core.resolved_admission["generation"],
                        admission_key,
                        expected_entrypoint,
                        activation_source_kind,
                        source_identity_sha256,
                        core.execution_admission["decision"],
                        core.execution_admission["state"],
                    ),
                ).fetchone()
                if durable_execution is None or (
                    str(durable_execution["reason"])
                    != str(core.execution_admission["reason"])
                    and core.execution_admission["reason"]
                    != "activation_admission_idempotent"
                ):
                    raise RecordConflictError(
                        "w3_snapshot_execution_authority_mismatch"
                    )
            elif establishing_snapshot and conn.execute(
                "SELECT 1 FROM rca_activation_epochs WHERE is_current = 1"
            ).fetchone() is not None:
                raise RecordConflictError("w3_snapshot_execution_authority_mismatch")
            if binding_action == "join":
                if snapshot_row is None:
                    raise RecordConflictError("w3_snapshot_creator_missing")
                expected_snapshot_columns = {
                    key: value
                    for key, value in snapshot_values.items()
                    if key
                    not in {
                        "creator_source_envelope_sha256",
                        "creator_authority_sha256",
                        "creator_source_id",
                        "persisted_at",
                    }
                }
                if any(
                    snapshot_row[key] != value
                    for key, value in expected_snapshot_columns.items()
                ):
                    raise RecordConflictError("w3_snapshot_durable_binding_conflict")
                creator = conn.execute(
                    """
                    SELECT 1
                      FROM rca_snapshot_source_envelopes AS envelope
                     WHERE envelope.source_envelope_sha256 = ?
                       AND envelope.source_authority_sha256 = ?
                       AND envelope.source_id = ?
                       AND envelope.snapshot_sha256 = ?
                       AND envelope.binding_action = 'create'
                    """,
                    (
                        snapshot_row["creator_source_envelope_sha256"],
                        snapshot_row["creator_authority_sha256"],
                        snapshot_row["creator_source_id"],
                        core.snapshot_sha256,
                    ),
                ).fetchone()
                if creator is None:
                    raise RecordConflictError("w3_snapshot_creator_missing")
            elif binding_action != "create":
                raise RecordConflictError("w3_snapshot_binding_action_invalid")

            ensure_exact_tx(
                conn,
                table="rca_canonical_requests",
                key="request_sha256",
                values=request_values,
            )
            ensure_exact_tx(
                conn,
                table="rca_source_authority_receipts",
                key="authority_sha256",
                values=authority_values,
            )
            snapshot_created = False
            if binding_action == "create":
                snapshot_created = ensure_exact_tx(
                    conn,
                    table="rca_admission_snapshots",
                    key="snapshot_sha256",
                    values=snapshot_values,
                )
            source_envelope_created = ensure_exact_tx(
                conn,
                table="rca_snapshot_source_envelopes",
                key="source_envelope_sha256",
                values=envelope_values,
            )
            return {
                "snapshot_sha256": core.snapshot_sha256,
                "source_envelope_sha256": envelope.source_envelope_sha256,
                "source_id": envelope.source_id,
                "binding_action": binding_action,
                "snapshot_created": snapshot_created,
                "source_envelope_created": source_envelope_created,
            }
        except RecordConflictError:
            raise
        except sqlite3.IntegrityError as exc:
            raise RecordConflictError(
                f"w3_snapshot_authority_integrity_conflict:{exc}"
            ) from exc

    def persist_w3_admission_snapshot_tx(
        self,
        conn: sqlite3.Connection,
        *,
        admission: RcaAdmission,
        trigger_context: Any,
        source_id: str,
        snapshot_authority: Any,
        ticket_authority: Mapping[str, Any] | None,
        activation_decision: ActivationAdmissionDecision | None,
        manual_ingress_authority: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Project and persist one steady-admitted W3 execution snapshot."""
        from gateway.pnc_rca_admission import validate_rca_trigger_context
        from gateway.pnc_rca_snapshot import (
            build_admission_snapshot,
            build_canonical_rca_request,
            build_snapshot_source_envelope,
            build_source_authority_receipt,
            validate_admission_snapshot,
        )

        if not isinstance(conn, sqlite3.Connection) or not conn.in_transaction:
            raise RuntimeError("w3_snapshot_transaction_required")
        authority = _normalize_w3_snapshot_authority(snapshot_authority)
        if authority is None:
            raise ValueError("w3_snapshot_authority_required")
        legacy_admission = validate_rca_admission(admission)
        context = validate_rca_trigger_context(trigger_context)
        policy_names = (
            "creation_policy",
            "business_profile",
            "execution_policy",
            "publication_policy",
            "correction_lineage_policy",
        )
        policies = {name: dict(authority["policies"][name]) for name in policy_names}
        policy_sha256s = {
            name: str(policies[name]["sha256"]) for name in policy_names
        }

        source = conn.execute(
            "SELECT * FROM rca_trigger_sources WHERE source_id = ?",
            (str(source_id),),
        ).fetchone()
        binding = conn.execute(
            """
            SELECT binding.business_key, binding.generation, binding.role,
                   trigger.submission_key, trigger.state,
                   outbox.status AS outbox_status
              FROM rca_trigger_bindings AS binding
              JOIN business_triggers AS trigger
                ON trigger.business_key = binding.business_key
               AND trigger.generation = binding.generation
              JOIN rca_outbox AS outbox
                ON outbox.business_key = binding.business_key
               AND outbox.generation = binding.generation
             WHERE binding.source_id = ?
            """,
            (str(source_id),),
        ).fetchone()
        if source is None or binding is None:
            raise RecordConflictError("w3_snapshot_legacy_authority_missing")
        if (
            str(binding["business_key"]) != legacy_admission.business_key
            or int(binding["generation"]) != legacy_admission.generation
            or str(binding["submission_key"]) != legacy_admission.submission_key
        ):
            raise RecordConflictError("w3_snapshot_legacy_binding_mismatch")
        role = str(binding["role"])
        if role not in {"origin", "observer"}:
            raise RecordConflictError("w3_snapshot_legacy_role_invalid")
        binding_action = "create" if role == "origin" else "join"

        source_kind = str(source["source_kind"])
        if context.source_kind != source_kind:
            raise RecordConflictError("w3_snapshot_context_source_mismatch")
        ticket_receipt: dict[str, Any]
        if source_kind == "kafka_workflow_event":
            event_uid = str(source["source_dedupe_key"]).split(
                ":generation:", 1
            )[0]
            inbox = conn.execute(
                "SELECT * FROM kafka_inbox WHERE event_uid = ?",
                (event_uid,),
            ).fetchone()
            if inbox is None or str(inbox["raw_sha256"]) != str(
                source["payload_sha256"]
            ):
                raise RecordConflictError("w3_snapshot_kafka_inbox_mismatch")
            source_metadata = {
                "source_kind": source_kind,
                "event_uid": event_uid,
                "topic": str(inbox["topic"]),
                "partition": int(inbox["partition_id"]),
                "offset": int(inbox["offset_id"]),
                "payload_sha256": str(inbox["raw_sha256"]),
                "observed_at": str(source["created_at"]),
            }
            ticket_receipt = build_w3_ticket_authority_receipt(
                source_kind="kafka_durable_inbox",
                source_evidence_sha256=str(inbox["raw_sha256"]),
                observed_at=str(source["created_at"]),
                project_key=context.project_key,
                project_simple_name=context.project_simple_name,
                work_item_type_key=context.work_item_type_key,
                work_item_id=context.work_item_id,
                issue_url=context.issue_url,
                title=context.title,
            )
            if ticket_authority is not None and dict(ticket_authority) != ticket_receipt:
                raise RecordConflictError("w3_ticket_authority_kafka_mismatch")
            anchor = {"issue_target": context.issue_url, "thread_target": None}
        elif source_kind == "feishu_group_manual":
            if ticket_authority is None:
                raise ValueError("w3_manual_ticket_authority_required")
            ticket_receipt = _validate_w3_ticket_authority_receipt(ticket_authority)
            if ticket_receipt["source_kind"] != "feishu_official_preread":
                raise ValueError("w3_manual_ticket_authority_source_invalid")
            source_metadata = {
                "source_kind": source_kind,
                "platform": str(source["platform"]),
                "chat_id": str(source["chat_id"]),
                "thread_id": str(source["thread_id"]),
                "message_id": str(source["message_id"]),
                "requester_id": str(source["requester_id"]),
                "mode": str(source["mode"]),
                "payload_sha256": str(source["payload_sha256"]),
                "observed_at": str(source["created_at"]),
            }
            anchor = {
                "issue_target": context.issue_url,
                "thread_target": (
                    str(source["thread_id"])
                    if str(source["platform"]) == "feishu"
                    else None
                ),
            }
        else:
            raise RecordConflictError("w3_snapshot_source_kind_invalid")

        ticket = ticket_receipt["ticket"]
        expected_ticket = {
            "project_key": context.project_key,
            "project_simple_name": context.project_simple_name,
            "work_item_type_key": context.work_item_type_key,
            "work_item_id": context.work_item_id,
            "issue_url": context.issue_url,
            "title": context.title,
        }
        if any(ticket[name] != value for name, value in expected_ticket.items()):
            raise RecordConflictError("w3_ticket_authority_context_mismatch")
        expected_title_sha256 = str(ticket["title_sha256"])

        source_identity = {
            "platform": str(source["platform"]),
            "chat_id": str(source["chat_id"]),
            "thread_id": str(source["thread_id"]),
            "message_id": str(source["message_id"]),
            "requester_id": str(source["requester_id"]),
            "issue_url": context.issue_url,
            "mode": str(source["mode"]),
        }
        if source_kind == "feishu_group_manual":
            if manual_ingress_authority is None:
                raise ValueError("w3_manual_ingress_authority_required")
            manual_authority = _validate_w3_manual_ingress_authority(
                manual_ingress_authority,
                expected_source_identity=source_identity,
                expected_ticket_authority_sha256=str(
                    ticket_receipt["ticket_authority_sha256"]
                ),
                expected_snapshot_authority_sha256=str(
                    authority["authority_sha256"]
                ),
            )
            authorization_evidence_sha256 = str(
                manual_authority["authority_sha256"]
            )
        else:
            authorization_evidence_sha256 = _canonical_sha256(
                {
                    "schema_version": W3_INGRESS_AUTHORIZATION_SCHEMA_VERSION,
                    "source_id": str(source_id),
                    "source_payload_sha256": str(source["payload_sha256"]),
                    "ticket_authority_sha256": str(
                        ticket_receipt["ticket_authority_sha256"]
                    ),
                    "snapshot_authority_sha256": str(
                        authority["authority_sha256"]
                    ),
                    "business_key": legacy_admission.business_key,
                    "submission_key": legacy_admission.submission_key,
                    "generation": legacy_admission.generation,
                    "binding_action": binding_action,
                }
            )

        persisted_snapshot_row = conn.execute(
            """
            SELECT * FROM rca_admission_snapshots
             WHERE business_key = ? AND generation = ?
            """,
            (legacy_admission.business_key, legacy_admission.generation),
        ).fetchone()
        generation_evidence: str | None = None
        if persisted_snapshot_row is not None:
            if (
                binding_action == "create"
                and str(persisted_snapshot_row["creator_source_id"])
                != str(source_id)
            ):
                raise RecordConflictError("w3_snapshot_creator_binding_conflict")
            persisted_value = json.loads(
                str(persisted_snapshot_row["admission_snapshot_json"])
            )
            generation_evidence = persisted_value["canonical_request"][
                "execution_intent"
            ]["generation_authorization_evidence_sha256"]
            snapshot = validate_admission_snapshot(
                persisted_value,
                expected_snapshot_sha256=str(
                    persisted_snapshot_row["snapshot_sha256"]
                ),
                expected_generation_authorization_evidence_sha256=(
                    generation_evidence
                ),
                expected_ticket_title_sha256=expected_title_sha256,
                expected_policy_sha256s=policy_sha256s,
            )
            execution_admission = dict(snapshot.execution_admission)
            if (
                execution_admission["state"] != "steady_active"
                or execution_admission["decision"] != "admit"
                or execution_admission["legacy_unconfigured"]
            ):
                raise RecordConflictError("w3_snapshot_steady_activation_required")
        else:
            if binding_action == "join":
                raise RecordConflictError("w3_snapshot_creator_missing")
            if (
                activation_decision is None
                or activation_decision.decision != "admit"
                or activation_decision.epoch_state != "steady_active"
                or activation_decision.ledger_id is None
                or not activation_decision.epoch_id
            ):
                raise RecordConflictError("w3_snapshot_steady_activation_required")
            execution_admission = {
                "activation_epoch_id": activation_decision.epoch_id,
                "activation_ledger_id": activation_decision.ledger_id,
                "decision": "admit",
                "reason": activation_decision.reason,
                "state": "steady_active",
                "legacy_unconfigured": False,
            }
            if legacy_admission.generation > 1:
                if (
                    source_kind != "feishu_group_manual"
                    or str(source["platform"]) != "feishu"
                    or str(source["mode"]) != "rerun"
                ):
                    raise RecordConflictError("w3_explicit_rerun_authority_invalid")
                generation_evidence = authorization_evidence_sha256

            request = build_canonical_rca_request(
                admission=legacy_admission,
                trigger_context=context,
                creation_policy=policies["creation_policy"],
                business_profile=policies["business_profile"],
                execution_policy=policies["execution_policy"],
                publication_policy=policies["publication_policy"],
                correction_lineage_policy=policies[
                    "correction_lineage_policy"
                ],
                generation_reason=(
                    "explicit_user_rerun"
                    if legacy_admission.generation > 1
                    else "initial"
                ),
                generation_authorization_evidence_sha256=generation_evidence,
                expected_generation_authorization_evidence_sha256=(
                    generation_evidence
                ),
                expected_ticket_title_sha256=expected_title_sha256,
                expected_policy_sha256s=policy_sha256s,
            )
            snapshot = build_admission_snapshot(
                request=request,
                admission=legacy_admission,
                execution_admission=execution_admission,
                expected_generation_authorization_evidence_sha256=(
                    generation_evidence
                ),
                expected_ticket_title_sha256=expected_title_sha256,
                expected_policy_sha256s=policy_sha256s,
            )

        # W5 issues the one external-write fence only after W3 has produced an
        # admitted snapshot and the activation ledger binding is durable in this
        # transaction.  Shadow and legacy-unconfigured snapshots retain the
        # unissued slot and therefore cannot authorize any provider call.
        if (
            snapshot.execution_admission["decision"] == "admit"
            and not snapshot.execution_admission["legacy_unconfigured"]
        ):
            if snapshot.write_fence.get("state") == "unissued":
                if persisted_snapshot_row is not None:
                    raise RecordConflictError("external_write_fence_missing")
                if (
                    activation_decision is None
                    or activation_decision.ledger_id is None
                    or not activation_decision.epoch_id
                ):
                    raise RecordConflictError("external_write_fence_identity_mismatch")
                if source_kind == "feishu_group_manual":
                    activation_identity: dict[str, Any] = {
                        "chat_id": source_metadata["chat_id"],
                        "requester_id": source_metadata["requester_id"],
                        "message_id": source_metadata["message_id"],
                        "thread_id": source_metadata["thread_id"],
                        "issue_url": context.issue_url,
                        "mode": source_metadata["mode"],
                    }
                    if source_metadata["platform"] == "operator":
                        activation_identity.update(
                            {"chat_id": "operator", "thread_id": "operator:issue-only"}
                        )
                    activation_source_kind = "manual"
                else:
                    activation_identity = {"event_uid": source_metadata["event_uid"]}
                    activation_source_kind = "kafka"
                source_identity_sha256, _normalized_identity = (
                    self._normalize_activation_source_identity(
                        activation_source_kind,
                        activation_identity,
                    )
                )
                admission_key = self._activation_admission_key(
                    source_kind=activation_source_kind,
                    source_identity_sha256=source_identity_sha256,
                    business_key=str(snapshot.resolved_admission["business_key"]),
                    submission_key=str(snapshot.resolved_admission["submission_key"]),
                    generation=int(snapshot.resolved_admission["generation"]),
                )
                try:
                    from gateway.pnc_rca_write_fence import (
                        issue_snapshot_write_fence,
                    )

                    target_set: dict[str, Any] = {
                        "issue_target": str(anchor["issue_target"]),
                        "thread_target": anchor.get("thread_target"),
                    }
                    if source_kind == "feishu_group_manual":
                        target_set["chat_id"] = str(source_metadata["chat_id"])
                    learning_lane = conn.execute(
                        "SELECT 1 FROM rca_learning_lane_admissions "
                        "WHERE business_key = ? AND generation = ?",
                        (
                            legacy_admission.business_key,
                            legacy_admission.generation,
                        ),
                    ).fetchone() is not None
                    snapshot = issue_snapshot_write_fence(
                        snapshot,
                        activation_epoch_id=str(activation_decision.epoch_id),
                        activation_ledger_id=int(activation_decision.ledger_id),
                        admission_key=admission_key,
                        target_set=target_set,
                        allowed_write_kinds=(
                            LEARNING_LANE_ALLOWED_WRITE_KINDS
                            if learning_lane
                            else None
                        ),
                        now=datetime.now(timezone.utc),
                    )
                except Exception as exc:
                    raise RecordConflictError(
                        f"external_write_fence_issue_failed:{getattr(exc, 'code', type(exc).__name__)}"
                    ) from exc

        ingress_decision = {
            "requested_mode": "pending",
            "binding_action": binding_action,
            "decision": str(snapshot.execution_admission["decision"]),
            "authorization_evidence_sha256": authorization_evidence_sha256,
        }
        source_authority = build_source_authority_receipt(
            source_id=str(source_id),
            source_kind=source_kind,
            source_metadata=source_metadata,
            anchor=anchor,
            ingress_decision=ingress_decision,
            expected_issue_target=context.issue_url,
        )
        envelope = build_snapshot_source_envelope(
            snapshot=snapshot,
            source_id=str(source_id),
            source_kind=source_kind,
            source_metadata=source_metadata,
            anchor=anchor,
            ingress_decision=ingress_decision,
            expected_authorization_evidence_sha256=(
                authorization_evidence_sha256
            ),
            expected_generation_authorization_evidence_sha256=(
                generation_evidence
            ),
            expected_ticket_title_sha256=expected_title_sha256,
            expected_source_payload_sha256=str(source["payload_sha256"]),
            expected_policy_sha256s=policy_sha256s,
            expected_snapshot_sha256=snapshot.snapshot_sha256,
            expected_source_authority=source_authority,
        )
        persisted = self.persist_admission_snapshot_source_tx(
            conn,
            snapshot=snapshot,
            source_envelope=envelope,
            expected_source_authority=source_authority,
            expected_snapshot_sha256=snapshot.snapshot_sha256,
            expected_generation_authorization_evidence_sha256=(
                generation_evidence
            ),
            expected_ticket_title_sha256=expected_title_sha256,
            expected_source_payload_sha256=str(source["payload_sha256"]),
            expected_authorization_evidence_sha256=(
                authorization_evidence_sha256
            ),
            expected_policy_sha256s=policy_sha256s,
        )
        return {
            **persisted,
            "snapshot_authority_sha256": authority["authority_sha256"],
            "ticket_authority_sha256": ticket_receipt[
                "ticket_authority_sha256"
            ],
        }

    def persist_raw(
        self,
        record: KafkaRecord,
        *,
        policy: WorkflowEventPolicy,
        submit_enabled: bool = False,
        activation_required: bool = False,
    ) -> RawPersistResult:
        """Durably persist raw bytes plus their immutable processing policy."""
        if not isinstance(activation_required, bool):
            raise ActivationEpochError("activation_adjudication_flag_invalid")
        source_sha, _normalized_source = self._normalize_activation_source_identity(
            "kafka", {"event_uid": record.event_uid}
        )
        raw = _bytes(record.value)
        raw_sha256 = hashlib.sha256(raw).hexdigest()
        policy_json = _canonical_json(policy.to_dict())
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            current = _now_iso()
            epoch = self._current_activation_epoch_tx(conn)
            activation_epoch_id: str | None = None
            activation_ingress_state = "shadow"
            if submit_enabled:
                if epoch is None or str(epoch["state"] or "") != "steady_active":
                    raise ActivationEpochError("activation_steady_epoch_required")
                activation_epoch_id = str(epoch["epoch_id"])
                activation_ingress_state = "steady_active"
            effective_activation_required = submit_enabled
            submission_mode = "pending" if submit_enabled else "shadow"
            self._register_policy_snapshot_tx(conn, policy, current)
            existing = conn.execute(
                """
                SELECT raw_sha256, policy_json, creation_rule_version,
                       submission_mode, submit_enabled_requested,
                       activation_epoch_id, activation_ingress_state,
                       activation_required, activation_source_identity_sha256
                  FROM kafka_inbox WHERE event_uid = ?
                """,
                (record.event_uid,),
            ).fetchone()
            if existing:
                if existing["raw_sha256"] != raw_sha256:
                    raise RecordConflictError(
                        f"Kafka coordinate {record.event_uid} changed raw payload"
                    )
                immutable_intent = (
                    str(existing["policy_json"]),
                    str(existing["creation_rule_version"]),
                    int(existing["activation_required"]),
                    str(existing["activation_source_identity_sha256"] or ""),
                )
                requested_intent = (
                    policy_json,
                    policy.policy_version,
                    int(effective_activation_required),
                    source_sha,
                )
                if immutable_intent != requested_intent:
                    raise RecordConflictError(
                        f"Kafka coordinate {record.event_uid} changed ingress intent"
                    )
                submit_intent_changed = int(
                    existing["submit_enabled_requested"]
                ) != int(bool(submit_enabled))
                if submit_intent_changed:
                    raise RecordConflictError(
                        f"Kafka coordinate {record.event_uid} changed submission intent"
                    )
                conn.commit()
                return RawPersistResult(record.event_uid, False)
            self._assert_control_store_capacity(required_payload_bytes=len(raw))
            conn.execute(
                """
                INSERT INTO kafka_inbox(
                    event_uid, topic, partition_id, offset_id, kafka_timestamp_ms,
                    record_key, raw_value, raw_size_bytes, raw_sha256,
                    headers_json, policy_json,
                    creation_rule_version, submission_mode,
                    submit_enabled_requested, received_at,
                    activation_epoch_id, activation_ingress_state,
                    activation_required, activation_source_identity_sha256
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.event_uid,
                    record.topic,
                    record.partition,
                    record.offset,
                    record.timestamp_ms,
                    record.key,
                    raw,
                    len(raw),
                    raw_sha256,
                    _headers_json(record.headers),
                    policy_json,
                    policy.policy_version,
                    submission_mode,
                    int(bool(submit_enabled)),
                    current,
                    activation_epoch_id,
                    activation_ingress_state,
                    int(effective_activation_required),
                    source_sha,
                ),
            )
            conn.commit()
            return RawPersistResult(record.event_uid, True)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _assert_control_store_capacity(self, *, required_payload_bytes: int = 0) -> None:
        try:
            filesystem = os.statvfs(self.db_path.parent)
        except OSError as exc:
            raise ControlStoreCapacityError(
                "control_store_filesystem_probe_failed"
            ) from exc
        available = int(filesystem.f_bavail * filesystem.f_frsize)
        required_available = max(
            CONTROL_DB_MIN_AVAILABLE_BYTES,
            max(0, int(required_payload_bytes)) * 4,
        )
        if available < required_available:
            raise ControlStoreCapacityError("control_store_capacity_below_reserve")

    def _assert_manual_storage_capacity(self) -> None:
        try:
            self._assert_control_store_capacity()
        except ControlStoreCapacityError as exc:
            raise ManualRcaAdmissionError(f"manual_{exc}") from exc

    @classmethod
    def _assert_manual_dispatch_capacity_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        outbox_high_watermark: int,
    ) -> None:
        activation_predicate, activation_parameters = (
            cls._activation_claim_predicate_tx(conn)
        )
        backlog = int(
            conn.execute(
                f"""
                SELECT COUNT(*) FROM rca_outbox AS o
                 WHERE o.status IN ('pending', 'claimed')
                   AND ({activation_predicate})
                """,
                activation_parameters,
            ).fetchone()[0]
        )
        if backlog >= outbox_high_watermark:
            raise ManualRcaAdmissionError("manual_outbox_high_watermark_reached")
        manual_limit = max(
            1,
            outbox_high_watermark
            * MANUAL_OUTBOX_SHARE_NUMERATOR
            // MANUAL_OUTBOX_SHARE_DENOMINATOR,
        )
        manual_backlog = int(
            conn.execute(
                f"""
                SELECT COUNT(*)
                  FROM rca_outbox AS o
                 WHERE o.status IN ('pending', 'claimed')
                   AND ({activation_predicate})
                   AND (
                        o.source_topic IS NULL
                        OR EXISTS (
                            SELECT 1
                              FROM rca_trigger_bindings AS b
                              JOIN rca_trigger_sources AS s
                                ON s.source_id = b.source_id
                             WHERE b.business_key = o.business_key
                               AND b.generation = o.generation
                               AND s.source_kind = 'feishu_group_manual'
                               AND s.outcome IN ('created', 'rearmed')
                        )
                   )
                """,
                activation_parameters,
            ).fetchone()[0]
        )
        if manual_backlog >= manual_limit:
            raise ManualRcaAdmissionError("manual_outbox_source_quota_reached")

    @staticmethod
    def _input_wait_replacement_candidate(
        conn: sqlite3.Connection,
        *,
        inbox_row: sqlite3.Row,
        admission: Any,
        normalized: Any,
        allow_same_event: bool = False,
    ) -> tuple[sqlite3.Row | None, str]:
        existing = conn.execute(
            """
            SELECT
                t.state AS trigger_state,
                t.submission_key AS trigger_submission_key,
                t.creation_rule_version AS trigger_creation_rule_version,
                t.source_event_id AS trigger_source_event_id,
                t.source_topic AS trigger_source_topic,
                t.source_partition AS trigger_source_partition,
                t.source_offset AS trigger_source_offset,
                o.*
              FROM business_triggers AS t
              LEFT JOIN rca_outbox AS o
                ON o.business_key = t.business_key AND o.generation = t.generation
             WHERE t.business_key = ? AND t.generation = ?
            """,
            (admission.business_key, admission.generation),
        ).fetchone()
        if existing is None:
            return None, "trigger_missing_after_business_dedupe"
        if str(inbox_row["submission_mode"]) != "pending":
            return None, "replacement_event_not_pending"
        if existing["outbox_id"] is None:
            return None, "outbox_missing"
        if (
            str(inbox_row["topic"]) != str(existing["source_topic"])
            or int(inbox_row["partition_id"])
            != int(existing["source_partition"])
        ):
            return None, "replacement_source_lineage_mismatch"
        same_event = (
            str(inbox_row["event_uid"])
            == str(existing["source_event_id"])
            and int(inbox_row["offset_id"]) == int(existing["source_offset"])
        )
        if int(inbox_row["offset_id"]) <= int(existing["source_offset"]) and not (
            allow_same_event and same_event
        ):
            return None, "replacement_offset_not_newer"
        if (
            str(existing["submission_key"]) != admission.submission_key
            or str(existing["trigger_submission_key"]) != admission.submission_key
            or str(existing["business_key"]) != admission.business_key
            or int(existing["generation"]) != admission.generation
            or str(existing["creation_rule_version"])
            != normalized.creation_rule_version
            or str(existing["trigger_creation_rule_version"])
            != normalized.creation_rule_version
            or str(existing["trigger_source_event_id"])
            != str(existing["source_event_id"])
            or str(existing["trigger_source_topic"])
            != str(existing["source_topic"])
            or int(existing["trigger_source_partition"])
            != int(existing["source_partition"])
            or int(existing["trigger_source_offset"])
            != int(existing["source_offset"])
        ):
            return None, "identity_mismatch"
        if (
            existing["completed_at"] is not None
            or existing["result_json"] is not None
            or str(existing["status"]) == "completed"
        ):
            return None, "completed_or_result_present"
        if str(existing["status"]) == "claimed":
            return None, "outbox_claimed"
        if str(existing["status"]) != "quarantined":
            return None, "outbox_not_quarantined"
        error_code = str(existing["last_error_code"] or "")
        if error_code not in INPUT_WAIT_REARM_ERROR_CODES:
            return None, "error_not_input_wait_allowlisted"
        if str(existing["trigger_state"]) != "quarantined":
            return None, "trigger_state_not_quarantined"
        if existing["quarantined_at"] is None:
            return None, "quarantine_timestamp_missing"
        if any(
            existing[name] is not None
            for name in ("lease_token", "lease_owner", "lease_expires_at")
        ):
            return None, "lease_present"
        return existing, ""

    @classmethod
    def _rearm_input_wait_quarantine(
        cls,
        conn: sqlite3.Connection,
        *,
        inbox_row: sqlite3.Row,
        event_uid: str,
        admission: Any,
        normalized: Any,
        normalized_json: str,
        payload_json: str,
        current: str,
    ) -> tuple[bool, str]:
        """Rearm only a create-free input-wait quarantine under the ingest lock.

        Rejections are persisted on the replacement ``kafka_inbox`` row through
        ``rearm_reason``.  The success audit contains identities, counters, and an
        error code only; it deliberately excludes issue payloads, field values,
        error details, URLs, and source-data UUIDs.
        """
        existing, reason = cls._input_wait_replacement_candidate(
            conn,
            inbox_row=inbox_row,
            admission=admission,
            normalized=normalized,
        )
        if existing is None:
            return False, reason
        if cls._execution_watch_exists_tx(conn, admission.submission_key):
            return False, INPUT_WAIT_EXECUTION_WATCH_PRESENT_REASON

        error_code = str(existing["last_error_code"] or "")
        prior_source_event_id = str(existing["source_event_id"])
        prior_attempt = int(existing["attempt"])
        prior_fence = int(existing["fence"])
        trigger_update = conn.execute(
            """
            UPDATE business_triggers
               SET source_event_id = ?, source_topic = ?, source_partition = ?,
                   source_offset = ?, normalized_json = ?, state = 'pending'
             WHERE business_key = ? AND generation = ? AND state = 'quarantined'
                   AND submission_key = ?
            """,
            (
                event_uid,
                inbox_row["topic"],
                inbox_row["partition_id"],
                inbox_row["offset_id"],
                normalized_json,
                admission.business_key,
                admission.generation,
                admission.submission_key,
            ),
        )
        outbox_update = conn.execute(
            """
            UPDATE rca_outbox
               SET source_event_id = ?, source_topic = ?, source_partition = ?,
                   source_offset = ?, payload_json = ?, status = 'pending',
                   attempt = 0, next_attempt_at = ?, fence = fence + 1,
                   lease_token = NULL, lease_owner = NULL,
                   lease_expires_at = NULL, claimed_at = NULL,
                   completed_at = NULL, quarantined_at = NULL,
                   last_error_code = '', last_error_detail = '', result_json = NULL,
                   retry_window_started_at = ?, updated_at = ?
             WHERE outbox_id = ? AND submission_key = ?
                   AND status = 'quarantined' AND completed_at IS NULL
                   AND result_json IS NULL AND last_error_code = ?
            """,
            (
                event_uid,
                inbox_row["topic"],
                inbox_row["partition_id"],
                inbox_row["offset_id"],
                payload_json,
                current,
                current,
                current,
                int(existing["outbox_id"]),
                admission.submission_key,
                error_code,
            ),
        )
        if trigger_update.rowcount != 1 or outbox_update.rowcount != 1:
            raise RuntimeError("input_wait_rearm_atomic_state_changed")
        conn.execute(
            """
            INSERT INTO rca_outbox_rearm_audit(
                outbox_id, submission_key, prior_source_event_id,
                replacement_source_event_id, prior_attempt, prior_fence,
                prior_error_code, reason, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(existing["outbox_id"]),
                admission.submission_key,
                prior_source_event_id,
                event_uid,
                prior_attempt,
                prior_fence,
                error_code,
                INPUT_WAIT_QUARANTINE_REARMED_REASON,
                current,
            ),
        )
        return True, INPUT_WAIT_QUARANTINE_REARMED_REASON

    @staticmethod
    def _normalize_manual_request(
        value: ManualRcaTriggerRequest | Mapping[str, Any],
    ) -> ManualRcaTriggerRequest:
        if isinstance(value, ManualRcaTriggerRequest):
            request = value
        elif isinstance(value, Mapping):
            request = ManualRcaTriggerRequest(
                schema_version=str(value.get("schema_version") or ""),
                issue_url=str(value.get("issue_url") or ""),
                mode=str(value.get("mode") or ""),  # type: ignore[arg-type]
                reason=str(value.get("reason") or ""),
                platform=str(value.get("platform") or ""),
                chat_id=str(value.get("chat_id") or ""),
                thread_id=str(value.get("thread_id") or ""),
                message_id=str(value.get("message_id") or ""),
                requester_id=str(value.get("requester_id") or ""),
            )
        else:
            raise ManualRcaAdmissionError("manual_request_invalid")
        normalized = ManualRcaTriggerRequest(
            schema_version=request.schema_version.strip(),
            issue_url=request.issue_url.strip().rstrip("/"),
            mode=str(request.mode or "").strip(),  # type: ignore[arg-type]
            reason=request.reason.strip(),
            platform=request.platform.strip().lower(),
            chat_id=request.chat_id.strip(),
            thread_id=request.thread_id.strip(),
            message_id=request.message_id.strip(),
            requester_id=request.requester_id.strip(),
        )
        if normalized.schema_version != MANUAL_TRIGGER_SCHEMA_VERSION:
            raise ManualRcaAdmissionError("manual_request_schema_unsupported")
        if normalized.mode not in {"run_or_join", "rerun", "debug"}:
            raise ManualRcaAdmissionError("manual_request_mode_invalid")
        if normalized.platform not in {"feishu", "operator"}:
            raise ManualRcaAdmissionError("manual_request_platform_invalid")
        if not all(
            (normalized.message_id, normalized.requester_id, normalized.reason)
        ):
            raise ManualRcaAdmissionError("manual_request_source_incomplete")
        if len(normalized.reason.encode("utf-8")) > 500:
            raise ManualRcaAdmissionError("manual_request_reason_too_long")
        if normalized.platform == "operator":
            if normalized.mode not in {"rerun", "debug"}:
                raise ManualRcaAdmissionError("manual_operator_mode_invalid")
            if normalized.chat_id or normalized.thread_id:
                raise ManualRcaAdmissionError("manual_operator_source_invalid")
        else:
            if not normalized.chat_id or not normalized.thread_id:
                raise ManualRcaAdmissionError("manual_request_source_incomplete")
            if not normalized.thread_id.startswith("topic:"):
                raise ManualRcaAdmissionError("manual_request_thread_invalid")
            root = normalized.thread_id.split("topic:", 1)[1]
            if not root or not re.fullmatch(r"[A-Za-z0-9_-]{3,200}", root):
                raise ManualRcaAdmissionError("manual_request_thread_invalid")
        try:
            validate_rca_requester(
                platform=normalized.platform,
                requester_id=normalized.requester_id,
            )
        except ValueError as exc:
            raise ManualRcaAdmissionError(str(exc)) from exc
        if not _ISSUE_URL_RE.fullmatch(normalized.issue_url):
            raise ManualRcaAdmissionError("manual_request_issue_url_invalid")
        return normalized

    @staticmethod
    def _manual_input_wait_rearm_eligible(row: sqlite3.Row) -> bool:
        return not (
            str(row["outbox_status"] or "") != "quarantined"
            or str(row["last_error_code"] or "") not in INPUT_WAIT_REARM_ERROR_CODES
            or row["result_json"] is not None
            or row["completed_at"] is not None
            or any(
                row[name] is not None
                for name in ("lease_token", "lease_owner", "lease_expires_at")
            )
        )

    @classmethod
    def _manual_input_wait_rearm_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        row: sqlite3.Row,
        current: str,
    ) -> bool:
        if not cls._manual_input_wait_rearm_eligible(row):
            return False
        updated = conn.execute(
            """
            UPDATE rca_outbox
               SET status = 'pending', attempt = 0, next_attempt_at = ?,
                   fence = fence + 1, claimed_at = NULL, quarantined_at = NULL,
                   last_error_code = '', last_error_detail = '',
                   retry_window_started_at = ?, updated_at = ?
             WHERE outbox_id = ? AND status = 'quarantined'
               AND completed_at IS NULL AND result_json IS NULL
               AND last_error_code = ?
            """,
            (
                current,
                current,
                current,
                row["outbox_id"],
                row["last_error_code"],
            ),
        )
        if updated.rowcount != 1:
            return False
        conn.execute(
            """
            UPDATE business_triggers SET state = 'pending'
             WHERE business_key = ? AND generation = ? AND state = 'quarantined'
            """,
            (row["business_key"], row["generation"]),
        )
        return True

    @classmethod
    def _execution_watch_exists_tx(
        cls, conn: sqlite3.Connection, submission_key: str
    ) -> bool:
        return cls._table_exists(conn, "rca_execution_watch") and conn.execute(
            "SELECT 1 FROM rca_execution_watch WHERE submission_key = ?",
            (submission_key,),
        ).fetchone() is not None

    @classmethod
    def _execution_terminal_tx(
        cls,
        conn: sqlite3.Connection,
        row: sqlite3.Row,
        *,
        allow_silent_terminal: bool = False,
    ) -> bool:
        """Return whether a generation is durably terminal for a new rerun.

        The ordinary terminal contract requires a settled delivery.  A
        technical deadline terminal is a separate, opt-in contract: it can
        authorize only an explicitly authenticated Feishu rerun after its
        internal failure route has settled and no delivery effect exists.
        """
        if cls._table_exists(conn, "rca_execution_watch"):
            watch = conn.execute(
                "SELECT state FROM rca_execution_watch WHERE submission_key = ?",
                (row["submission_key"],),
            ).fetchone()
            if watch is not None:
                watch_state = str(watch["state"] or "")
                if (
                    watch_state in {"terminal_failed", "quarantined"}
                    and allow_silent_terminal
                ):
                    return cls._silent_terminal_rerun_eligible_tx(conn, row)
                if watch_state != "delivery_created":
                    return False
                required_tables = {
                    "rca_delivery_jobs",
                    "rca_delivery_effects",
                    "rca_delivery_subscriptions",
                }
                if not all(cls._table_exists(conn, table) for table in required_tables):
                    return False
                job = conn.execute(
                    """
                    SELECT delivery_id, status FROM rca_delivery_jobs
                     WHERE business_key = ? AND generation = ? LIMIT 1
                    """,
                    (row["business_key"], row["generation"]),
                ).fetchone()
                if job is None or str(job["status"] or "") not in {
                    "delivered",
                    "partial",
                    "quarantined",
                }:
                    return False
                effects = conn.execute(
                    """
                    SELECT status FROM rca_delivery_effects
                     WHERE delivery_id = ? AND required = 1
                    """,
                    (job["delivery_id"],),
                ).fetchall()
                settled_effect_states = {"succeeded", "suppressed", "quarantined"}
                if not effects or any(
                    str(effect["status"] or "") not in settled_effect_states
                    for effect in effects
                ):
                    return False
                subscriptions = conn.execute(
                    """
                    SELECT status, effect_key FROM rca_delivery_subscriptions
                     WHERE business_key = ? AND generation = ? AND required = 1
                    """,
                    (row["business_key"], row["generation"]),
                ).fetchall()
                if not subscriptions:
                    return False
                for subscription in subscriptions:
                    subscription_status = str(subscription["status"] or "")
                    if subscription_status in {"suppressed", "quarantined"}:
                        continue
                    effect_key = str(subscription["effect_key"] or "")
                    if subscription_status != "materialized" or not effect_key:
                        return False
                    effect = conn.execute(
                        """
                        SELECT status FROM rca_delivery_effects
                         WHERE effect_key = ? AND delivery_id = ? AND required = 1
                        """,
                        (effect_key, job["delivery_id"]),
                    ).fetchone()
                    if effect is None or str(
                        effect["status"] or ""
                    ) not in settled_effect_states:
                        return False
                return True
        return False

    @staticmethod
    def _historical_epoch_rerun_active_lease(
        *,
        token: Any,
        owner: Any,
        expires_at: Any,
        current: datetime,
    ) -> bool:
        values = (token, owner, expires_at)
        if not any(value is not None for value in values):
            return False
        if not all(str(value or "").strip() for value in values):
            return True
        try:
            expiry = datetime.fromisoformat(str(expires_at).replace("Z", "+00:00"))
        except (TypeError, ValueError, OverflowError):
            return True
        if expiry.tzinfo is None or expiry.utcoffset() is None:
            return True
        return expiry.astimezone(timezone.utc) > current

    @classmethod
    def _historical_epoch_rerun_ineligibility_tx(
        cls,
        conn: sqlite3.Connection,
        row: sqlite3.Row,
        *,
        now: datetime | None,
    ) -> str:
        current = _utc_datetime(now)
        if cls._historical_epoch_rerun_active_lease(
            token=row["lease_token"],
            owner=row["lease_owner"],
            expires_at=row["lease_expires_at"],
            current=current,
        ):
            return "historical_epoch_rerun_prior_outbox_lease_active"
        submission_key = str(row["submission_key"] or "").strip()
        if cls._table_exists(conn, "rca_execution_watch"):
            watch = conn.execute(
                "SELECT state, task_id, lease_token, lease_owner, lease_expires_at "
                "FROM rca_execution_watch WHERE submission_key = ?",
                (submission_key,),
            ).fetchone()
            if watch is not None:
                if cls._historical_epoch_rerun_active_lease(
                    token=watch["lease_token"],
                    owner=watch["lease_owner"],
                    expires_at=watch["lease_expires_at"],
                    current=current,
                ):
                    return "historical_epoch_rerun_prior_watch_lease_active"
        if cls._table_exists(conn, "rca_delivery_jobs") and cls._table_exists(
            conn, "rca_delivery_effects"
        ):
            effects = conn.execute(
                """
                SELECT effect.effect_key, effect.write_phase,
                       effect.remote_receipt_json, effect.attempt,
                       effect.lease_token, effect.lease_owner,
                       effect.lease_expires_at
                  FROM rca_delivery_jobs AS job
                  JOIN rca_delivery_effects AS effect
                    ON effect.delivery_id = job.delivery_id
                 WHERE job.submission_key = ?
                """,
                (submission_key,),
            ).fetchall()
            for effect in effects:
                if str(effect["write_phase"] or "") == "write_started":
                    return "historical_epoch_rerun_prior_write_started"
                if effect["remote_receipt_json"] is not None:
                    return "historical_epoch_rerun_prior_remote_receipt_present"
                if int(effect["attempt"] or 0) > 0:
                    return "historical_epoch_rerun_prior_provider_attempt_present"
                if cls._historical_epoch_rerun_active_lease(
                    token=effect["lease_token"],
                    owner=effect["lease_owner"],
                    expires_at=effect["lease_expires_at"],
                    current=current,
                ):
                    return "historical_epoch_rerun_prior_effect_lease_active"
                if (
                    cls._table_exists(conn, "rca_delivery_attempts")
                    and conn.execute(
                        "SELECT 1 FROM rca_delivery_attempts WHERE effect_key = ? LIMIT 1",
                        (str(effect["effect_key"]),),
                    ).fetchone()
                    is not None
                ):
                    return "historical_epoch_rerun_prior_provider_attempt_present"
        return ""

    @classmethod
    def _silent_terminal_rerun_eligible_tx(
        cls,
        conn: sqlite3.Connection,
        row: sqlite3.Row,
    ) -> bool:
        """Validate one internal-only technical terminal before a new generation.

        This predicate deliberately validates the complete durable chain.  It
        does not repair or mutate the old watch, route, subscriptions, job, or
        effects; a caller may only create a separate generation after every
        check succeeds.
        """
        required_tables = {
            "rca_admission_snapshots",
            "rca_delivery_attempts",
            "rca_execution_watch",
            "rca_failure_routes",
            "rca_delivery_subscriptions",
            "rca_delivery_jobs",
            "rca_delivery_effects",
        }
        if not all(cls._table_exists(conn, table) for table in required_tables):
            return False
        submission_key = str(row["submission_key"] or "").strip()
        business_key = str(row["business_key"] or "").strip()
        try:
            generation = int(row["generation"])
        except (KeyError, TypeError, ValueError):
            return False
        if not submission_key or not business_key or generation < 1:
            return False

        terminal = conn.execute(
            """
            SELECT w.state, w.business_key AS watch_business_key,
                   w.generation AS watch_generation, w.terminal_at,
                   w.last_status_json, w.last_error_code, w.last_error_detail,
                   w.task_id, w.delivery_id, w.lease_token, w.lease_owner,
                   w.lease_expires_at, o.status AS outbox_status,
                   o.attempt AS outbox_attempt,
                   o.next_attempt_at AS outbox_next_attempt_at,
                   o.claimed_at AS outbox_claimed_at,
                   o.completed_at AS outbox_completed_at,
                   o.quarantined_at AS outbox_quarantined_at,
                   o.result_json AS outbox_result_json,
                   o.last_error_code AS outbox_error_code,
                   o.last_error_detail AS outbox_error_detail,
                   o.lease_token AS outbox_lease_token,
                   o.lease_owner AS outbox_lease_owner,
                   o.lease_expires_at AS outbox_lease_expires_at
              FROM rca_execution_watch AS w
              JOIN rca_outbox AS o
                ON o.outbox_id = w.submission_outbox_id
               AND o.business_key = w.business_key
               AND o.generation = w.generation
             WHERE w.submission_key = ?
               AND w.business_key = ?
               AND w.generation = ?
             LIMIT 1
            """,
            (submission_key, business_key, generation),
        ).fetchone()
        if terminal is None:
            return False
        try:
            terminal_generation = int(terminal["watch_generation"] or 0)
        except (TypeError, ValueError, OverflowError):
            return False
        silent_error_code = str(terminal["last_error_code"] or "")
        if (
            str(terminal["state"] or "") != "terminal_failed"
            or str(terminal["watch_business_key"] or "") != business_key
            or terminal_generation != generation
            or terminal["delivery_id"] is not None
            or not str(terminal["terminal_at"] or "").strip()
            or silent_error_code not in SILENT_TERMINAL_RERUN_ERROR_CODES
            or not str(terminal["last_error_detail"] or "").strip()
            or any(
                terminal[name] is not None
                for name in (
                    "lease_token",
                    "lease_owner",
                    "lease_expires_at",
                    "outbox_lease_token",
                    "outbox_lease_owner",
                    "outbox_lease_expires_at",
                )
            )
        ):
            return False
        outbox_status = str(terminal["outbox_status"] or "")
        if outbox_status not in {"completed", "quarantined"}:
            return False
        if outbox_status == "completed":
            if (
                not str(terminal["outbox_completed_at"] or "").strip()
                or terminal["outbox_result_json"] is None
                or str(terminal["outbox_error_code"] or "")
            ):
                return False
        elif (
            not str(terminal["outbox_quarantined_at"] or "").strip()
            or terminal["outbox_completed_at"] is not None
            or str(terminal["outbox_error_code"] or "") != silent_error_code
        ):
            return False

        try:
            status = json.loads(str(terminal["last_status_json"] or ""))
        except (TypeError, ValueError, json.JSONDecodeError):
            return False
        if not isinstance(status, Mapping):
            return False
        if (
            status.get("external_writes") is not False
            or status.get("terminal_delivery_policy")
            != "silent_internal_alert_only"
        ):
            return False
        taxonomy = status.get("failure_taxonomy")
        if not isinstance(taxonomy, Mapping):
            return False
        taxonomy_gap_prefix = "taxonomy_gap:"
        is_allowed_taxonomy_gap = silent_error_code.startswith(taxonomy_gap_prefix)
        expected_raw_code = (
            silent_error_code.removeprefix(taxonomy_gap_prefix)
            if is_allowed_taxonomy_gap
            else silent_error_code
        )
        expected_known = not is_allowed_taxonomy_gap
        if (
            not expected_raw_code
            or taxonomy.get("raw_code") != expected_raw_code
            or taxonomy.get("terminal_error_code") != silent_error_code
            or taxonomy.get("internal_route") != "internal_alert"
            or taxonomy.get("lane") != "hard_defect"
            or taxonomy.get("known") is not expected_known
            or taxonomy.get("retryable") is not False
        ):
            return False
        fallback = taxonomy.get("terminal_fallback")
        durable_route = taxonomy.get("durable_route")
        if not isinstance(fallback, Mapping) or not isinstance(
            durable_route, Mapping
        ):
            return False
        try:
            elapsed_seconds = int(fallback["elapsed_seconds"])
            fallback_seconds = int(taxonomy["terminal_fallback_seconds"])
        except (KeyError, TypeError, ValueError):
            return False
        route_key = str(fallback.get("route_key") or "").strip()
        if (
            fallback.get("schema_version")
            != "pnc_rca_bounded_terminal_fallback_v1"
            or fallback.get("confidence_tier") != "low"
            or fallback.get("terminal_class") != "honest_non_attribution"
            or fallback.get("route_kind") != "internal_alert"
            or fallback.get("route_owner") != "rca-engineering"
            or elapsed_seconds < fallback_seconds
            or fallback_seconds < 1
            or not route_key
            or str(durable_route.get("route_key") or "") != route_key
        ):
            return False
        internal_outlet = durable_route.get("internal_outlet")
        if not isinstance(internal_outlet, Mapping) or (
            internal_outlet.get("status") not in {"settled", "resolved"}
            or internal_outlet.get("external_effects") != 0
        ):
            return False

        route_rows = conn.execute(
            """
            SELECT route_key, submission_key, business_key, generation,
                   terminal_error_code, lane, route_kind, owner, status,
                   audit_json, route_payload_json
              FROM rca_failure_routes
             WHERE route_key = ? AND submission_key = ?
               AND business_key = ? AND generation = ?
             LIMIT 1
            """,
            (route_key, submission_key, business_key, generation),
        ).fetchall()
        if len(route_rows) != 1:
            return False
        route = route_rows[0]
        if str(route["status"] or "") not in {
            "alert_pending",
            "terminal_fallback",
            "resolved",
        }:
            return False
        if (
            str(route["terminal_error_code"] or "") != silent_error_code
            or str(route["lane"] or "") != "hard_defect"
            or str(route["route_kind"] or "") != "internal_alert"
            or str(route["owner"] or "") != "rca-engineering"
        ):
            return False
        try:
            route_audit = json.loads(str(route["audit_json"] or ""))
            route_payload = json.loads(str(route["route_payload_json"] or ""))
        except (TypeError, ValueError, json.JSONDecodeError):
            return False
        if not isinstance(route_audit, Mapping) or not isinstance(
            route_payload, Mapping
        ):
            return False
        decision = route_payload.get("decision")
        if not isinstance(decision, Mapping) or (
            route_payload.get("schema_version")
            != "pnc_rca_failure_route_payload_v1"
            or decision.get("raw_code") != expected_raw_code
            or decision.get("terminal_error_code") != silent_error_code
            or decision.get("internal_route") != "internal_alert"
            or decision.get("lane") != "hard_defect"
            or decision.get("known") is not expected_known
        ):
            return False

        # A silent terminal must not have an older public job/effect that could
        # race the new generation.  Subscriptions may remain pending (or be
        # explicitly suppressed/quarantined), but never materialized here.
        if conn.execute(
            """
            SELECT 1 FROM rca_delivery_jobs
             WHERE business_key = ? AND generation = ?
             LIMIT 1
            """,
            (business_key, generation),
        ).fetchone() is not None:
            return False
        subscriptions = conn.execute(
            """
            SELECT required, status, delivery_id, effect_key
              FROM rca_delivery_subscriptions
             WHERE business_key = ? AND generation = ?
            """,
            (business_key, generation),
        ).fetchall()
        required = [item for item in subscriptions if int(item["required"] or 0) == 1]
        if not required or any(
            str(item["status"] or "") == "materialized"
            or item["delivery_id"] is not None
            or item["effect_key"] is not None
            for item in subscriptions
        ):
            return False
        return True

    @classmethod
    def _terminal_duplicate_retrigger_eligible_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        inbox_row: sqlite3.Row,
    ) -> bool:
        raw_value = bytes(inbox_row["raw_value"] or b"")
        if (
            str(inbox_row["decision"] or "") not in {"accepted", "deduped"}
            or str(inbox_row["submission_mode"] or "") != "pending"
            or not str(inbox_row["business_key"] or "").strip()
            or not str(inbox_row["submission_key"] or "").strip()
            or int(inbox_row["generation"] or 0) < 1
            or not raw_value
            or len(raw_value) != int(inbox_row["raw_size_bytes"] or 0)
            or hashlib.sha256(raw_value).hexdigest()
            != str(inbox_row["raw_sha256"] or "")
        ):
            return False
        try:
            normalized = json.loads(str(inbox_row["normalized_json"] or "{}"))
            project_key = str(normalized["project_key"])
            work_item_type_key = str(normalized["work_item_type_key"])
            work_item_id = str(normalized["work_item_id"])
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            return False
        generation = cls._select_latest_kafka_issue_generation_tx(
            conn,
            project_key=project_key,
            work_item_type_key=work_item_type_key,
            work_item_id=work_item_id,
        )
        if generation is None or not cls._kafka_generation_contract_valid(generation):
            return False
        event_uid = str(inbox_row["event_uid"])
        if (
            str(generation["business_key"]) != str(inbox_row["business_key"])
            or str(generation["submission_key"])
            != str(inbox_row["submission_key"])
            or int(generation["generation"]) != int(inbox_row["generation"])
            or int(generation["generation"]) != 1
            or str(generation["kafka_source_mode"] or "") != "issue_created"
            or str(generation["source_event_id"]) != event_uid
            or str(generation["source_topic"]) != str(inbox_row["topic"])
            or int(generation["source_partition"])
            != int(inbox_row["partition_id"])
            or int(generation["source_offset"]) != int(inbox_row["offset_id"])
            or str(generation["state"] or "") != "quarantined"
            or str(generation["outbox_status"] or "") != "quarantined"
            or str(generation["last_error_code"] or "")
            not in INPUT_WAIT_REARM_ERROR_CODES
            or generation["completed_at"] is not None
            or generation["result_json"] is not None
            or generation["quarantined_at"] is None
            or any(
                generation[name] is not None
                for name in ("lease_token", "lease_owner", "lease_expires_at")
            )
        ):
            return False
        return cls._execution_watch_exists_tx(
            conn, str(generation["submission_key"])
        ) and cls._execution_terminal_tx(conn, generation)

    def admit_manual_trigger(
        self,
        request: ManualRcaTriggerRequest | Mapping[str, Any],
        *,
        allowed_chat_ids: Iterable[str],
        submit_enabled: bool = False,
        operator_authorized: bool = False,
        operator_rate_limit: int = DEFAULT_MANUAL_OPERATOR_RATE_LIMIT,
        operator_rate_window_seconds: int = DEFAULT_MANUAL_OPERATOR_RATE_WINDOW_SECONDS,
        active_policy: WorkflowEventPolicy | Mapping[str, Any] | None = None,
        outbox_high_watermark: int = DEFAULT_OUTBOX_HIGH_WATERMARK,
        activation_required: bool = False,
        user_rerun_authority: Mapping[str, Any] | None = None,
        historical_epoch_rerun_authority: Mapping[str, Any] | None = None,
        batch_terminal_rerun_authority: Mapping[str, Any] | None = None,
        silent_terminal_rerun_authority: Mapping[str, Any] | None = None,
        snapshot_authority: Any = None,
        snapshot_ticket_authority: Mapping[str, Any] | None = None,
        snapshot_manual_ingress_authority: Mapping[str, Any] | None = None,
        now: datetime | None = None,
    ) -> ManualRcaAdmissionResult:
        """Atomically create or join one manual RCA generation and delivery target."""
        manual = self._normalize_manual_request(request)
        issue_only_operator = manual.platform == "operator"
        if not isinstance(activation_required, bool):
            raise ManualRcaAdmissionError("manual_activation_required_invalid")
        normalized_user_rerun: dict[str, str] | None = None
        normalized_historical_epoch_rerun: dict[str, Any] | None = None
        normalized_batch_rerun: dict[str, Any] | None = None
        normalized_silent_rerun: dict[str, Any] | None = None
        if user_rerun_authority is not None:
            if not isinstance(user_rerun_authority, Mapping) or set(
                user_rerun_authority
            ) != {"schema_version", "command_text", "work_item_id"}:
                raise ManualRcaAdmissionError("group_user_rerun_authority_invalid")
            normalized_user_rerun = {
                "schema_version": str(
                    user_rerun_authority.get("schema_version") or ""
                ).strip(),
                "command_text": str(
                    user_rerun_authority.get("command_text") or ""
                ),
                "work_item_id": str(
                    user_rerun_authority.get("work_item_id") or ""
                ).strip(),
            }
            if (
                normalized_user_rerun["schema_version"]
                != GROUP_USER_RERUN_SCHEMA_VERSION
                or not re.fullmatch(
                    r"[0-9]{1,32}", normalized_user_rerun["work_item_id"]
                )
                or normalized_user_rerun["command_text"]
                != f"重新分析 {normalized_user_rerun['work_item_id']}"
                or manual.platform != "feishu"
                or manual.mode != "rerun"
                or not manual.requester_id.startswith("ou_")
            ):
                raise ManualRcaAdmissionError("group_user_rerun_authority_invalid")
        if historical_epoch_rerun_authority is not None:
            if (
                not isinstance(historical_epoch_rerun_authority, Mapping)
                or set(historical_epoch_rerun_authority)
                != HISTORICAL_EPOCH_RERUN_AUTHORITY_FIELDS
            ):
                raise ManualRcaAdmissionError(
                    "historical_epoch_rerun_authority_invalid"
                )
            try:
                expected_historical_epoch_rerun = (
                    build_historical_epoch_rerun_authority(
                        batch_id=historical_epoch_rerun_authority.get("batch_id"),
                        queue_sha256=historical_epoch_rerun_authority.get(
                            "queue_sha256"
                        ),
                        issue_id=historical_epoch_rerun_authority.get("issue_id"),
                        prior_submission_key=historical_epoch_rerun_authority.get(
                            "prior_submission_key"
                        ),
                        prior_generation=historical_epoch_rerun_authority.get(
                            "prior_generation"
                        ),
                        prior_activation_epoch_id=(
                            historical_epoch_rerun_authority.get(
                                "prior_activation_epoch_id"
                            )
                        ),
                        prior_activation_ledger_id=(
                            historical_epoch_rerun_authority.get(
                                "prior_activation_ledger_id"
                            )
                        ),
                        target_activation_epoch_id=(
                            historical_epoch_rerun_authority.get(
                                "target_activation_epoch_id"
                            )
                        ),
                        owner_receipt_path=historical_epoch_rerun_authority.get(
                            "owner_receipt_path"
                        ),
                        owner_receipt_sha256=historical_epoch_rerun_authority.get(
                            "owner_receipt_sha256"
                        ),
                        activation_required=historical_epoch_rerun_authority.get(
                            "activation_required"
                        ),
                        requester_id=historical_epoch_rerun_authority.get(
                            "requester_id"
                        ),
                        reason=historical_epoch_rerun_authority.get("reason"),
                    )
                )
            except (TypeError, ValueError) as exc:
                raise ManualRcaAdmissionError(
                    "historical_epoch_rerun_authority_invalid"
                ) from exc
            expected_message_id = (
                f"{expected_historical_epoch_rerun['batch_id']}-"
                f"{expected_historical_epoch_rerun['issue_id']}-try-"
            )
            if (
                dict(historical_epoch_rerun_authority)
                != expected_historical_epoch_rerun
                or manual.platform != "operator"
                or manual.mode != "rerun"
                or operator_authorized is not True
                or user_rerun_authority is not None
                or batch_terminal_rerun_authority is not None
                or silent_terminal_rerun_authority is not None
                or snapshot_authority is not None
                or snapshot_ticket_authority is not None
                or snapshot_manual_ingress_authority is not None
                or activation_required is not True
                or expected_historical_epoch_rerun["requester_id"]
                != manual.requester_id
                or expected_historical_epoch_rerun["reason"] != manual.reason
                or re.fullmatch(
                    re.escape(expected_message_id) + r"[1-9][0-9]*",
                    manual.message_id,
                )
                is None
            ):
                raise ManualRcaAdmissionError(
                    "historical_epoch_rerun_authority_invalid"
                )
            normalized_historical_epoch_rerun = expected_historical_epoch_rerun
        if silent_terminal_rerun_authority is not None:
            if (
                not isinstance(silent_terminal_rerun_authority, Mapping)
                or set(silent_terminal_rerun_authority)
                != SILENT_TERMINAL_RERUN_AUTHORITY_FIELDS
            ):
                raise ManualRcaAdmissionError(
                    "silent_terminal_rerun_authority_invalid"
                )
            try:
                expected_silent_rerun = build_silent_terminal_rerun_authority(
                    batch_id=silent_terminal_rerun_authority.get("batch_id"),
                    queue_sha256=silent_terminal_rerun_authority.get("queue_sha256"),
                    issue_id=silent_terminal_rerun_authority.get("issue_id"),
                    prior_submission_key=silent_terminal_rerun_authority.get(
                        "prior_submission_key"
                    ),
                    prior_generation=silent_terminal_rerun_authority.get(
                        "prior_generation"
                    ),
                    owner_receipt_sha256=silent_terminal_rerun_authority.get(
                        "owner_receipt_sha256"
                    ),
                    owner_receipt_path=silent_terminal_rerun_authority.get(
                        "owner_receipt_path"
                    ),
                    activation_required=silent_terminal_rerun_authority.get(
                        "activation_required"
                    ),
                    requester_id=silent_terminal_rerun_authority.get("requester_id"),
                    reason=silent_terminal_rerun_authority.get("reason"),
                )
            except (TypeError, ValueError) as exc:
                raise ManualRcaAdmissionError(
                    "silent_terminal_rerun_authority_invalid"
                ) from exc
            if dict(silent_terminal_rerun_authority) != expected_silent_rerun:
                raise ManualRcaAdmissionError(
                    "silent_terminal_rerun_authority_invalid"
                )
            if (
                manual.platform != "operator"
                or manual.mode != "rerun"
                or operator_authorized is not True
                or user_rerun_authority is not None
                or historical_epoch_rerun_authority is not None
                or snapshot_authority is not None
                or snapshot_ticket_authority is not None
                or snapshot_manual_ingress_authority is not None
                or activation_required is not True
                or expected_silent_rerun["activation_required"] is not True
                or expected_silent_rerun["requester_id"] != manual.requester_id
                or expected_silent_rerun["reason"] != manual.reason
            ):
                raise ManualRcaAdmissionError(
                    "silent_terminal_rerun_authority_invalid"
                )
            normalized_silent_rerun = expected_silent_rerun
        if batch_terminal_rerun_authority is not None:
            if (
                not isinstance(batch_terminal_rerun_authority, Mapping)
                or set(batch_terminal_rerun_authority)
                != BATCH_TERMINAL_RERUN_AUTHORITY_FIELDS
            ):
                raise ManualRcaAdmissionError(
                    "batch_terminal_rerun_authority_invalid"
                )
            try:
                expected_batch_rerun = build_batch_terminal_rerun_authority(
                    batch_id=batch_terminal_rerun_authority.get("batch_id"),
                    queue_sha256=batch_terminal_rerun_authority.get("queue_sha256"),
                    issue_id=batch_terminal_rerun_authority.get("issue_id"),
                    prior_submission_key=batch_terminal_rerun_authority.get(
                        "prior_submission_key"
                    ),
                    prior_generation=batch_terminal_rerun_authority.get(
                        "prior_generation"
                    ),
                    prior_delivery_id=batch_terminal_rerun_authority.get(
                        "prior_delivery_id"
                    ),
                    owner_receipt_sha256=batch_terminal_rerun_authority.get(
                        "owner_receipt_sha256"
                    ),
                    owner_receipt_path=batch_terminal_rerun_authority.get(
                        "owner_receipt_path"
                    ),
                    activation_required=batch_terminal_rerun_authority.get(
                        "activation_required"
                    ),
                    requester_id=batch_terminal_rerun_authority.get("requester_id"),
                    reason=batch_terminal_rerun_authority.get("reason"),
                )
            except (TypeError, ValueError) as exc:
                raise ManualRcaAdmissionError(
                    "batch_terminal_rerun_authority_invalid"
                ) from exc
            if dict(batch_terminal_rerun_authority) != expected_batch_rerun:
                raise ManualRcaAdmissionError(
                    "batch_terminal_rerun_authority_invalid"
                )
            if (
                manual.platform != "operator"
                or manual.mode != "rerun"
                or operator_authorized is not True
                or user_rerun_authority is not None
                or historical_epoch_rerun_authority is not None
                or snapshot_authority is not None
                or snapshot_ticket_authority is not None
                or snapshot_manual_ingress_authority is not None
                or silent_terminal_rerun_authority is not None
                or activation_required is not True
                or expected_batch_rerun["requester_id"] != manual.requester_id
                or expected_batch_rerun["reason"] != manual.reason
            ):
                raise ManualRcaAdmissionError(
                    "batch_terminal_rerun_authority_invalid"
                )
            normalized_batch_rerun = expected_batch_rerun
        allowed = {str(item or "").strip() for item in allowed_chat_ids}
        if not submit_enabled:
            raise ManualRcaAdmissionError("manual_intake_disabled")
        if not issue_only_operator and manual.chat_id not in allowed:
            raise ManualRcaAdmissionError("manual_intake_chat_not_allowed")
        operator_requested = manual.mode in {"rerun", "debug"}
        if (
            (operator_requested or issue_only_operator)
            and normalized_user_rerun is None
            and not operator_authorized
        ):
            raise ManualRcaAdmissionError("manual_operator_not_authorized")
        try:
            high_watermark = int(outbox_high_watermark)
        except (TypeError, ValueError) as exc:
            raise ManualRcaAdmissionError(
                "manual_outbox_high_watermark_invalid"
            ) from exc
        if isinstance(outbox_high_watermark, bool) or high_watermark < 1:
            raise ManualRcaAdmissionError("manual_outbox_high_watermark_invalid")
        rate_limit = DEFAULT_MANUAL_OPERATOR_RATE_LIMIT
        rate_window_seconds = DEFAULT_MANUAL_OPERATOR_RATE_WINDOW_SECONDS
        if operator_requested and not issue_only_operator:
            try:
                rate_limit = int(operator_rate_limit)
                rate_window_seconds = int(operator_rate_window_seconds)
            except (TypeError, ValueError) as exc:
                raise ManualRcaAdmissionError(
                    "manual_operator_rate_config_invalid"
                ) from exc
            if (
                isinstance(operator_rate_limit, bool)
                or isinstance(operator_rate_window_seconds, bool)
                or rate_limit < 1
                or rate_window_seconds < 1
            ):
                raise ManualRcaAdmissionError(
                    "manual_operator_rate_config_invalid"
                )
        configured_policy: WorkflowEventPolicy | None = None
        if active_policy is not None:
            try:
                configured_policy = (
                    active_policy
                    if isinstance(active_policy, WorkflowEventPolicy)
                    else WorkflowEventPolicy.from_mapping(active_policy)
                )
            except (TypeError, ValueError, KeyError) as exc:
                raise ManualRcaAdmissionError("manual_active_policy_invalid") from exc
        url_match = _ISSUE_URL_RE.fullmatch(manual.issue_url)
        if url_match is None:
            raise ManualRcaAdmissionError("manual_request_issue_url_invalid")
        project_simple_name, work_item_id = url_match.groups()
        try:
            w3_authority = _normalize_w3_snapshot_authority(snapshot_authority)
            if w3_authority is None:
                if (
                    snapshot_ticket_authority is not None
                    or snapshot_manual_ingress_authority is not None
                ):
                    raise ValueError("w3_snapshot_authority_required")
                w3_ticket_authority = None
                w3_manual_authority = None
            else:
                if issue_only_operator:
                    raise ValueError("w3_manual_operator_source_unsupported")
                if snapshot_ticket_authority is None:
                    raise ValueError("w3_manual_ticket_authority_required")
                if snapshot_manual_ingress_authority is None:
                    raise ValueError("w3_manual_ingress_authority_required")
                w3_ticket_authority = _validate_w3_ticket_authority_receipt(
                    snapshot_ticket_authority
                )
                w3_manual_authority = dict(snapshot_manual_ingress_authority)
        except (TypeError, ValueError, KeyError) as exc:
            raise ManualRcaAdmissionError(str(exc)) from exc
        source_payload = manual.to_dict()
        if normalized_user_rerun is not None:
            source_payload["user_rerun_authority"] = normalized_user_rerun
        if normalized_historical_epoch_rerun is not None:
            source_payload["historical_epoch_rerun_authority"] = (
                normalized_historical_epoch_rerun
            )
        if normalized_batch_rerun is not None:
            source_payload["batch_terminal_rerun_authority"] = normalized_batch_rerun
        if normalized_silent_rerun is not None:
            source_payload["silent_terminal_rerun_authority"] = (
                normalized_silent_rerun
            )
        payload_sha = _canonical_sha256(source_payload)
        source_dedupe_key = f"{manual.platform}:{manual.message_id}"
        source_id = _stable_key(
            "g1q3-rca-source-v1",
            {"source_kind": "feishu_group_manual", "dedupe": source_dedupe_key},
        )
        activation_source_identity = {
            "chat_id": manual.chat_id,
            "thread_id": manual.thread_id,
            "requester_id": manual.requester_id,
            "message_id": manual.message_id,
            "issue_url": manual.issue_url,
            "mode": manual.mode,
        }
        if issue_only_operator:
            activation_source_identity.update(
                {"chat_id": "operator", "thread_id": "operator:issue-only"}
            )
        current = _iso(now)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            existing_source = conn.execute(
                "SELECT * FROM rca_trigger_sources WHERE source_dedupe_key = ?",
                (source_dedupe_key,),
            ).fetchone()
            if existing_source is not None:
                if (
                    existing_source["source_id"] != source_id
                    or existing_source["payload_sha256"] != payload_sha
                    or existing_source["source_kind"] != "feishu_group_manual"
                ):
                    raise ManualRcaAdmissionError("manual_source_payload_conflict")
                binding = conn.execute(
                    """
                    SELECT b.*, t.submission_key, t.state, t.source_event_id,
                           t.creation_rule_version, t.work_item_id, t.project_key,
                           t.work_item_type_key,
                           o.outbox_id, o.status AS outbox_status, o.attempt,
                           o.completed_at, o.result_json, o.quarantined_at,
                           o.last_error_code, o.lease_token, o.lease_owner,
                           o.lease_expires_at
                      FROM rca_trigger_bindings AS b
                      JOIN business_triggers AS t
                        ON t.business_key = b.business_key AND t.generation = b.generation
                      JOIN rca_outbox AS o
                        ON o.business_key = b.business_key AND o.generation = b.generation
                     WHERE b.source_id = ?
                    """,
                    (source_id,),
                ).fetchone()
                if binding is None:
                    raise ManualRcaAdmissionError("manual_source_binding_missing")
                if str(binding["outbox_status"] or "") == "shadow":
                    raise ManualRcaAdmissionError(
                        "manual_historical_shadow_not_executable"
                    )
                replay_activation = self.adjudicate_activation_tx(
                    conn,
                    entrypoint="manual_admit",
                    source_kind="manual",
                    source_identity=activation_source_identity,
                    business_key=str(binding["business_key"]),
                    submission_key=str(binding["submission_key"]),
                    generation=int(binding["generation"]),
                    new_execution=False,
                    now=now,
                )
                if replay_activation.decision not in {"admit", "join"}:
                    raise ManualRcaAdmissionError(replay_activation.reason)
                replay_outcome = str(existing_source["outcome"] or "joined")
                replay_state = str(binding["state"])
                replay_reason = "idempotent_source_replay"
                replay_generation = int(binding["generation"])
                replay_admission_for_lane = build_rca_admission(
                    project_key=str(binding["project_key"]),
                    project_simple_name=project_simple_name,
                    work_item_type_key=str(binding["work_item_type_key"]),
                    work_item_id=str(binding["work_item_id"]),
                    rule_version=str(binding["creation_rule_version"]),
                    trigger_kind=(
                        "manual_retrigger"
                        if replay_generation > 1
                        else (
                            "issue_created"
                            if str(binding["source_event_id"] or "").strip()
                            else "manual_issue_request"
                        )
                    ),
                    generation=replay_generation,
                )
                if normalized_silent_rerun is not None:
                    self._require_terminal_rerun_delivery_authority_tx(
                        conn,
                        authority_kind="silent_terminal",
                        authority=normalized_silent_rerun,
                        source_id=source_id,
                        source_payload_sha256=payload_sha,
                        admission=replay_admission_for_lane,
                    )
                if normalized_batch_rerun is not None:
                    self._require_terminal_rerun_delivery_authority_tx(
                        conn,
                        authority_kind="batch_terminal",
                        authority=normalized_batch_rerun,
                        source_id=source_id,
                        source_payload_sha256=payload_sha,
                        admission=replay_admission_for_lane,
                    )
                if normalized_historical_epoch_rerun is not None:
                    self._require_historical_epoch_rerun_delivery_authority_tx(
                        conn,
                        authority=normalized_historical_epoch_rerun,
                        source_id=source_id,
                        source_payload_sha256=payload_sha,
                        admission=replay_admission_for_lane,
                    )
                learning_lane = (
                    False
                    if any(
                        authority is not None
                        for authority in (
                            normalized_user_rerun,
                            normalized_historical_epoch_rerun,
                            normalized_silent_rerun,
                            normalized_batch_rerun,
                        )
                    )
                    else self._ensure_learning_lane_admission_tx(
                        conn,
                        admission=replay_admission_for_lane,
                        current=current,
                    )
                )
                required_subscriptions = (
                    []
                    if learning_lane
                    else conn.execute(
                        """
                        SELECT subscription_key, effect_kind
                          FROM rca_delivery_subscriptions
                         WHERE business_key = ? AND generation = ?
                           AND effect_kind IN (
                               'feishu_issue_comment', 'feishu_thread_reply'
                           )
                        """,
                        (binding["business_key"], int(binding["generation"])),
                    ).fetchall()
                )
                subscriptions_by_kind = {
                    str(row["effect_kind"]): str(row["subscription_key"])
                    for row in required_subscriptions
                }
                expected_kinds = set() if learning_lane else {"feishu_issue_comment"}
                subscription_key = subscriptions_by_kind.get(
                    "feishu_issue_comment", ""
                )
                if not learning_lane and not issue_only_operator:
                    root = manual.thread_id.split("topic:", 1)[1]
                    target_key = f"feishu_thread:{manual.chat_id}:{root}"
                    subscription_key = _stable_key(
                        "g1q3-rca-sub-v1",
                        {
                            "business_key": binding["business_key"],
                            "generation": int(binding["generation"]),
                            "effect_kind": "feishu_thread_reply",
                            "target_key": target_key,
                        },
                    )
                    expected_kinds.add("feishu_thread_reply")
                if not learning_lane and (not subscription_key or (
                    not issue_only_operator
                    and subscriptions_by_kind.get("feishu_thread_reply")
                    != subscription_key
                )):
                    raise ManualRcaAdmissionError(
                        "manual_source_subscription_missing"
                    )
                missing_bindings = [
                    required_key
                    for required_key in subscriptions_by_kind.values()
                    if conn.execute(
                        "SELECT 1 FROM rca_trigger_delivery_bindings "
                        "WHERE source_id = ? AND subscription_key = ?",
                        (source_id, required_key),
                    ).fetchone()
                    is None
                ]
                if set(subscriptions_by_kind) != expected_kinds:
                    raise ManualRcaAdmissionError(
                        "manual_source_subscription_missing"
                    )
                if missing_bindings:
                    self._assert_manual_storage_capacity()
                    for required_key in missing_bindings:
                        self._bind_source_subscription_tx(
                            conn,
                            source_id=source_id,
                            subscription_key=required_key,
                            current=current,
                        )
                if w3_authority is not None:
                    existing_snapshot = conn.execute(
                        """
                        SELECT 1 FROM rca_admission_snapshots
                         WHERE business_key = ? AND generation = ?
                        """,
                        (binding["business_key"], int(binding["generation"])),
                    ).fetchone()
                    if existing_snapshot is None:
                        raise ManualRcaAdmissionError(
                            "w3_manual_legacy_snapshot_missing"
                        )
                    replay_generation = int(binding["generation"])
                    replay_admission = build_rca_admission(
                        project_key=str(binding["project_key"]),
                        project_simple_name=project_simple_name,
                        work_item_type_key=str(binding["work_item_type_key"]),
                        work_item_id=str(binding["work_item_id"]),
                        rule_version=str(binding["creation_rule_version"]),
                        trigger_kind=(
                            "manual_retrigger"
                            if replay_generation > 1
                            else (
                                "issue_created"
                                if str(binding["source_event_id"] or "").strip()
                                else "manual_issue_request"
                            )
                        ),
                        generation=replay_generation,
                    )
                    if (
                        replay_admission.business_key != str(binding["business_key"])
                        or replay_admission.submission_key
                        != str(binding["submission_key"])
                    ):
                        raise ManualRcaAdmissionError(
                            "w3_manual_replay_identity_mismatch"
                        )
                    replay_context = build_rca_trigger_context(
                        source_kind="feishu_group_manual",
                        project_key=replay_admission.source_refs.project_key,
                        project_simple_name=project_simple_name,
                        work_item_type_key=(
                            replay_admission.source_refs.work_item_type_key
                        ),
                        work_item_id=replay_admission.source_refs.work_item_id,
                        rule_version=replay_admission.source_refs.rule_version,
                        issue_url=manual.issue_url,
                        title=str(w3_ticket_authority["ticket"]["title"]),
                    )
                    self.persist_w3_admission_snapshot_tx(
                        conn,
                        admission=replay_admission,
                        trigger_context=replay_context,
                        source_id=source_id,
                        snapshot_authority=w3_authority,
                        ticket_authority=w3_ticket_authority,
                        activation_decision=replay_activation,
                        manual_ingress_authority=w3_manual_authority,
                    )
                conn.commit()
                return ManualRcaAdmissionResult(
                    schema_version=MANUAL_ADMISSION_RESULT_SCHEMA_VERSION,
                    outcome=replay_outcome,
                    business_key=str(binding["business_key"]),
                    submission_key=str(binding["submission_key"]),
                    generation=int(binding["generation"]),
                    source_id=source_id,
                    subscription_key=subscription_key,
                    state=replay_state,
                    reason=replay_reason,
                )

            if normalized_user_rerun is not None:
                if normalized_user_rerun["work_item_id"] != work_item_id:
                    raise ManualRcaAdmissionError(
                        "group_user_rerun_authority_mismatch"
                    )
                dedupe_window_start = _iso(
                    _utc_datetime(now)
                    - timedelta(seconds=GROUP_USER_RERUN_DEDUPE_SECONDS)
                )
                recent = conn.execute(
                    """
                    SELECT s.source_id, b.business_key, b.generation,
                           t.submission_key, t.state,
                           COALESCE(sub.subscription_key, '') AS subscription_key
                      FROM rca_trigger_sources AS s
                      JOIN rca_trigger_bindings AS b
                        ON b.source_id = s.source_id AND b.role = 'origin'
                      JOIN business_triggers AS t
                        ON t.business_key = b.business_key
                       AND t.generation = b.generation
                      LEFT JOIN rca_delivery_subscriptions AS sub
                        ON sub.business_key = b.business_key
                       AND sub.generation = b.generation
                       AND sub.effect_kind = 'feishu_issue_comment'
                     WHERE s.source_kind = 'feishu_group_manual'
                       AND s.platform = 'feishu'
                       AND s.mode = 'rerun'
                       AND s.requester_id = ?
                       AND t.work_item_id = ?
                       AND s.created_at >= ?
                     ORDER BY s.created_at DESC, s.source_id DESC
                     LIMIT 1
                    """,
                    (
                        manual.requester_id,
                        work_item_id,
                        dedupe_window_start,
                    ),
                ).fetchone()
                if recent is not None:
                    conn.commit()
                    return ManualRcaAdmissionResult(
                        schema_version=MANUAL_ADMISSION_RESULT_SCHEMA_VERSION,
                        outcome="deduped",
                        business_key=str(recent["business_key"]),
                        submission_key=str(recent["submission_key"]),
                        generation=int(recent["generation"]),
                        source_id=str(recent["source_id"]),
                        subscription_key=str(recent["subscription_key"]),
                        state=str(recent["state"]),
                        reason="user_rerun_duplicate_window",
                    )

            if (
                operator_requested
                and not issue_only_operator
                and normalized_user_rerun is None
            ):
                window_start = _iso(
                    _utc_datetime(now) - timedelta(seconds=rate_window_seconds)
                )
                recent_operator_actions = int(
                    conn.execute(
                        """
                        SELECT COUNT(*) FROM rca_trigger_sources
                         WHERE source_kind = 'feishu_group_manual'
                           AND requester_id = ?
                           AND mode IN ('rerun', 'debug')
                           AND created_at >= ?
                        """,
                        (manual.requester_id, window_start),
                    ).fetchone()[0]
                )
                if recent_operator_actions >= rate_limit:
                    raise ManualRcaAdmissionError(
                        "manual_operator_rate_limited"
                    )

            self._assert_manual_storage_capacity()
            if configured_policy is not None:
                policy = configured_policy
                active_policy_sha256 = _canonical_sha256(policy.to_dict())
            else:
                policy_row = conn.execute(
                    """
                    SELECT policy_sha256, policy_json
                      FROM rca_policy_snapshots WHERE active = 1
                    """
                ).fetchone()
                if policy_row is None:
                    raise ManualRcaAdmissionError("manual_active_policy_unavailable")
                policy = WorkflowEventPolicy.from_mapping(
                    json.loads(policy_row["policy_json"])
                )
                active_policy_sha256 = str(policy_row["policy_sha256"])
            if project_simple_name not in policy.project_simple_names:
                raise ManualRcaAdmissionError("manual_issue_project_not_allowed")
            if len(policy.project_keys) != 1 or len(policy.work_item_type_keys) != 1:
                raise ManualRcaAdmissionError("manual_policy_identity_ambiguous")
            project_key = next(iter(policy.project_keys))
            work_item_type_key = next(iter(policy.work_item_type_keys))
            base_admission = build_rca_admission(
                project_key=project_key,
                project_simple_name=project_simple_name,
                work_item_type_key=work_item_type_key,
                work_item_id=work_item_id,
                rule_version=policy.policy_version,
                trigger_kind="manual_issue_request",
            )
            latest, issue_scope_id, business_key_count = self._select_issue_scope_tx(
                conn,
                project_key=project_key,
                work_item_type_key=work_item_type_key,
                work_item_id=work_item_id,
            )
            if normalized_user_rerun is not None:
                if latest is None:
                    raise ManualRcaAdmissionError(
                        "group_user_rerun_existing_generation_required"
                    )
                if not self._execution_terminal_tx(conn, latest):
                    raise ManualRcaAdmissionError(
                        "group_user_rerun_terminal_generation_required"
                    )
            if normalized_historical_epoch_rerun is not None:
                if latest is None:
                    raise ManualRcaAdmissionError(
                        "historical_epoch_rerun_existing_generation_required"
                    )
                current_epoch = self._current_activation_epoch_tx(conn)
                if (
                    current_epoch is None
                    or str(current_epoch["state"] or "") != "steady_active"
                ):
                    raise ManualRcaAdmissionError(
                        "historical_epoch_rerun_current_epoch_not_steady"
                    )
                prior_epoch_id = str(latest["activation_epoch_id"] or "").strip()
                prior_ledger_id = latest["activation_ledger_id"]
                outbox_prior_epoch_id = str(
                    latest["outbox_activation_epoch_id"] or ""
                ).strip()
                outbox_prior_ledger_id = latest["outbox_activation_ledger_id"]
                target_epoch_id = str(current_epoch["epoch_id"])
                if (
                    prior_epoch_id != outbox_prior_epoch_id
                    or prior_ledger_id != outbox_prior_ledger_id
                    or prior_epoch_id == target_epoch_id
                ):
                    raise ManualRcaAdmissionError(
                        "historical_epoch_rerun_authority_mismatch"
                    )
                if prior_epoch_id:
                    old_binding = conn.execute(
                        """
                        SELECT 1
                          FROM rca_activation_admission_ledger AS ledger
                          JOIN rca_activation_epochs AS epoch
                            ON epoch.epoch_id = ledger.epoch_id
                         WHERE ledger.ledger_id = ? AND ledger.epoch_id = ?
                           AND ledger.business_key = ?
                           AND ledger.submission_key = ?
                           AND ledger.generation = ?
                           AND ledger.decision IN ('admit', 'shadow')
                           AND ledger.bound_at IS NOT NULL
                           AND epoch.is_current = 0
                        """,
                        (
                            prior_ledger_id,
                            prior_epoch_id,
                            str(latest["business_key"]),
                            str(latest["submission_key"]),
                            int(latest["generation"]),
                        ),
                    ).fetchone()
                    if prior_ledger_id is None or old_binding is None:
                        raise ManualRcaAdmissionError(
                            "historical_epoch_rerun_authority_mismatch"
                        )
                elif prior_ledger_id is not None:
                    raise ManualRcaAdmissionError(
                        "historical_epoch_rerun_authority_mismatch"
                    )
                expected_historical_epoch_rerun = (
                    build_historical_epoch_rerun_authority(
                        batch_id=normalized_historical_epoch_rerun["batch_id"],
                        queue_sha256=normalized_historical_epoch_rerun["queue_sha256"],
                        issue_id=work_item_id,
                        prior_submission_key=str(latest["submission_key"]),
                        prior_generation=int(latest["generation"]),
                        prior_activation_epoch_id=prior_epoch_id,
                        prior_activation_ledger_id=prior_ledger_id,
                        target_activation_epoch_id=target_epoch_id,
                        owner_receipt_path=normalized_historical_epoch_rerun[
                            "owner_receipt_path"
                        ],
                        owner_receipt_sha256=normalized_historical_epoch_rerun[
                            "owner_receipt_sha256"
                        ],
                        activation_required=True,
                        requester_id=manual.requester_id,
                        reason=manual.reason,
                    )
                )
                if normalized_historical_epoch_rerun != expected_historical_epoch_rerun:
                    raise ManualRcaAdmissionError(
                        "historical_epoch_rerun_authority_mismatch"
                    )
                ineligibility = self._historical_epoch_rerun_ineligibility_tx(
                    conn, latest, now=now
                )
                if ineligibility:
                    raise ManualRcaAdmissionError(ineligibility)
            if normalized_silent_rerun is not None:
                if latest is None or not self._silent_terminal_rerun_eligible_tx(
                    conn, latest
                ):
                    raise ManualRcaAdmissionError(
                        "silent_terminal_rerun_terminal_generation_required"
                    )
                expected_silent_rerun = build_silent_terminal_rerun_authority(
                    batch_id=normalized_silent_rerun["batch_id"],
                    queue_sha256=normalized_silent_rerun["queue_sha256"],
                    issue_id=work_item_id,
                    prior_submission_key=str(latest["submission_key"]),
                    prior_generation=int(latest["generation"]),
                    owner_receipt_sha256=normalized_silent_rerun[
                        "owner_receipt_sha256"
                    ],
                    owner_receipt_path=normalized_silent_rerun[
                        "owner_receipt_path"
                    ],
                    activation_required=normalized_silent_rerun[
                        "activation_required"
                    ],
                    requester_id=manual.requester_id,
                    reason=manual.reason,
                )
                if normalized_silent_rerun != expected_silent_rerun:
                    raise ManualRcaAdmissionError(
                        "silent_terminal_rerun_authority_mismatch"
                    )
            if normalized_batch_rerun is not None:
                if latest is None or not self._execution_terminal_tx(conn, latest):
                    raise ManualRcaAdmissionError(
                        "batch_terminal_rerun_terminal_generation_required"
                    )
                latest_watch = conn.execute(
                    """
                    SELECT state, delivery_id
                      FROM rca_execution_watch
                     WHERE submission_key = ?
                    """,
                    (str(latest["submission_key"]),),
                ).fetchone()
                if (
                    latest_watch is None
                    or str(latest_watch["state"] or "") != "delivery_created"
                    or not str(latest_watch["delivery_id"] or "").strip()
                    or str(latest_watch["delivery_id"])
                    != normalized_batch_rerun["prior_delivery_id"]
                    or normalized_batch_rerun["prior_submission_key"]
                    != str(latest["submission_key"])
                    or int(normalized_batch_rerun["prior_generation"])
                    != int(latest["generation"])
                    or normalized_batch_rerun["issue_id"] != work_item_id
                ):
                    raise ManualRcaAdmissionError(
                        "batch_terminal_rerun_authority_mismatch"
                    )
                batch_job = conn.execute(
                    """
                    SELECT delivery_id, status, outcome, terminal_error_code
                      FROM rca_delivery_jobs
                     WHERE business_key = ? AND generation = ?
                     LIMIT 1
                    """,
                    (str(latest["business_key"]), int(latest["generation"])),
                ).fetchone()
                if (
                    batch_job is None
                    or str(batch_job["delivery_id"] or "")
                    != normalized_batch_rerun["prior_delivery_id"]
                    or str(batch_job["status"] or "")
                    not in {"delivered", "partial", "quarantined"}
                ):
                    raise ManualRcaAdmissionError(
                        "batch_terminal_rerun_terminal_generation_required"
                    )
                batch_effects = conn.execute(
                    """
                    SELECT status
                      FROM rca_delivery_effects
                     WHERE delivery_id = ? AND required = 1
                    """,
                    (normalized_batch_rerun["prior_delivery_id"],),
                ).fetchall()
                if not batch_effects or any(
                    str(effect["status"] or "")
                    not in {"succeeded", "suppressed", "quarantined"}
                    for effect in batch_effects
                ):
                    raise ManualRcaAdmissionError(
                        "batch_terminal_rerun_terminal_generation_required"
                    )
            if business_key_count > 1:
                self._audit_issue_scope_conflict_tx(
                    conn,
                    event_uid=source_id,
                    operator=f"manual:{manual.requester_id}",
                    scope_id=issue_scope_id,
                    business_key_count=business_key_count,
                    current=current,
                )
                conn.commit()
                raise ManualRcaAdmissionError(
                    f"manual_{ISSUE_SCOPE_CONFLICT_REASON}"
                )
            if configured_policy is not None:
                active_policy_sha256 = self._register_policy_snapshot_tx(
                    conn, configured_policy, current
                )

            def existing_admission(
                *, generation: int
            ) -> RcaAdmission:
                if latest is None:
                    raise RuntimeError("manual_issue_scope_latest_missing")
                trigger_kind = "manual_retrigger"
                if generation == 1:
                    trigger_kind = (
                        "issue_created"
                        if str(latest["source_event_id"] or "").strip()
                        else "manual_issue_request"
                    )
                value = build_rca_admission(
                    project_key=str(latest["project_key"]),
                    project_simple_name=project_simple_name,
                    work_item_type_key=str(latest["work_item_type_key"]),
                    work_item_id=str(latest["work_item_id"]),
                    rule_version=str(latest["creation_rule_version"]),
                    trigger_kind=trigger_kind,
                    generation=generation,
                )
                if (
                    value.business_key != str(latest["business_key"])
                    or value.submission_key != str(latest["submission_key"])
                ):
                    raise ManualRcaAdmissionError(
                        "manual_issue_scope_chain_identity_invalid"
                    )
                return value

            created = False
            rearmed = False
            rearm_reason = ""
            needs_input_rearm = False
            if latest is None:
                self._assert_manual_dispatch_capacity_tx(
                    conn,
                    outbox_high_watermark=high_watermark,
                )
                admission = base_admission
                outcome = "created"
            elif normalized_historical_epoch_rerun is not None:
                self._assert_manual_dispatch_capacity_tx(
                    conn,
                    outbox_high_watermark=high_watermark,
                )
                admission = build_rca_admission(
                    project_key=str(latest["project_key"]),
                    project_simple_name=project_simple_name,
                    work_item_type_key=str(latest["work_item_type_key"]),
                    work_item_id=str(latest["work_item_id"]),
                    rule_version=str(latest["creation_rule_version"]),
                    trigger_kind="manual_retrigger",
                    generation=int(latest["generation"]) + 1,
                )
                outcome = "created"
            elif str(latest["outbox_status"] or "") == "shadow":
                raise ManualRcaAdmissionError(
                    "manual_historical_shadow_not_executable"
                )
            elif self._manual_input_wait_rearm_eligible(
                latest
            ) and not self._execution_watch_exists_tx(
                conn, str(latest["submission_key"])
            ):
                self._assert_manual_dispatch_capacity_tx(
                    conn,
                    outbox_high_watermark=high_watermark,
                )
                admission = existing_admission(
                    generation=int(latest["generation"])
                )
                outcome = "rearmed"
                rearmed = True
                rearm_reason = INPUT_WAIT_QUARANTINE_REARMED_REASON
                needs_input_rearm = True
            elif manual.mode in {"rerun", "debug"} and self._execution_terminal_tx(
                conn,
                latest,
                allow_silent_terminal=normalized_silent_rerun is not None,
            ):
                if (
                    manual.platform != "feishu"
                    or manual.mode != "rerun"
                ) and normalized_silent_rerun is None and normalized_batch_rerun is None:
                    raise ManualRcaAdmissionError(
                        "manual_generation_requires_explicit_user_rerun"
                    )
                self._assert_manual_dispatch_capacity_tx(
                    conn,
                    outbox_high_watermark=high_watermark,
                )
                admission = build_rca_admission(
                    project_key=str(latest["project_key"]),
                    project_simple_name=project_simple_name,
                    work_item_type_key=str(latest["work_item_type_key"]),
                    work_item_id=str(latest["work_item_id"]),
                    rule_version=str(latest["creation_rule_version"]),
                    trigger_kind="manual_retrigger",
                    generation=int(latest["generation"]) + 1,
                )
                outcome = "created"
            else:
                admission = existing_admission(
                    generation=int(latest["generation"]),
                )
                outcome = "joined"

            creates_generation = latest is None or admission.generation > int(
                latest["generation"] if latest is not None else 0
            )
            activation_decision = self.adjudicate_activation_tx(
                conn,
                entrypoint="manual_admit",
                source_kind="manual",
                source_identity=activation_source_identity,
                business_key=admission.business_key,
                submission_key=admission.submission_key,
                generation=admission.generation,
                new_execution=creates_generation,
                now=now,
            )
            if activation_decision.decision not in {"admit", "join"}:
                raise ManualRcaAdmissionError(activation_decision.reason)
            if needs_input_rearm:
                if latest is None or not self._manual_input_wait_rearm_tx(
                    conn, row=latest, current=current
                ):
                    raise RuntimeError("manual_input_wait_rearm_not_applied")

            trigger_context = build_rca_trigger_context(
                source_kind="feishu_group_manual",
                project_key=admission.source_refs.project_key,
                project_simple_name=project_simple_name,
                work_item_type_key=admission.source_refs.work_item_type_key,
                work_item_id=work_item_id,
                rule_version=admission.source_refs.rule_version,
                issue_url=manual.issue_url,
                title=(
                    str(w3_ticket_authority["ticket"]["title"])
                    if w3_ticket_authority is not None
                    else ""
                ),
            )
            conn.execute(
                """
                INSERT INTO rca_trigger_sources(
                    source_id, source_kind, source_dedupe_key, payload_sha256,
                    platform, chat_id, thread_id, message_id, requester_id,
                    kafka_event_uid, mode, outcome, created_at
                ) VALUES (?, 'feishu_group_manual', ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?)
                """,
                (
                    source_id,
                    source_dedupe_key,
                    payload_sha,
                    manual.platform,
                    manual.chat_id,
                    manual.thread_id,
                    manual.message_id,
                    manual.requester_id,
                    manual.mode,
                    outcome,
                    current,
                ),
            )
            if latest is None or admission.generation > int(latest["generation"]):
                inserted = conn.execute(
                    """
                    INSERT INTO business_triggers(
                        business_key, generation, submission_key, creation_rule_version,
                        work_item_id, project_key, work_item_type_key, origin_source_id,
                        source_event_id,
                        source_topic, source_partition, source_offset, normalized_json,
                        state, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, ?, 'pending', ?)
                    """,
                    (
                        admission.business_key,
                        admission.generation,
                        admission.submission_key,
                        admission.source_refs.rule_version,
                        work_item_id,
                        admission.source_refs.project_key,
                        admission.source_refs.work_item_type_key,
                        source_id,
                        _canonical_json(trigger_context.to_dict()),
                        current,
                    ),
                )
                if inserted.rowcount != 1:
                    raise ManualRcaAdmissionError("manual_trigger_create_conflict")
                payload_json = _manual_submission_outbox_payload(
                    admission=admission,
                    trigger_context=trigger_context,
                    origin_source_id=source_id,
                )
                conn.execute(
                    """
                    INSERT INTO rca_outbox(
                        action, business_key, submission_key, creation_rule_version,
                        generation, origin_source_id, source_event_id, source_topic, source_partition,
                        source_offset, payload_json, status, retry_window_started_at,
                        created_at, updated_at
                    ) VALUES (
                        'submit_rca_issue_intake', ?, ?, ?, ?, ?, NULL, NULL, NULL,
                        NULL, ?, 'pending', ?, ?, ?
                    )
                    """,
                    (
                        admission.business_key,
                        admission.submission_key,
                        admission.source_refs.rule_version,
                        admission.generation,
                        source_id,
                        payload_json,
                        current,
                        current,
                        current,
                    ),
                )
                created = True
                if (
                    activation_decision.decision == "admit"
                    and activation_decision.ledger_id is not None
                ):
                    self.bind_activation_admission_tx(
                        conn,
                        activation_decision,
                        business_key=admission.business_key,
                        submission_key=admission.submission_key,
                        generation=admission.generation,
                        now=now,
                    )

            bound_outbox = conn.execute(
                """
                SELECT outbox_id FROM rca_outbox
                 WHERE business_key = ? AND generation = ?
                """,
                (admission.business_key, admission.generation),
            ).fetchone()
            if bound_outbox is None:
                raise ManualRcaAdmissionError("manual_issue_scope_outbox_missing")
            if normalized_silent_rerun is not None:
                prior_generation = int(
                    normalized_silent_rerun["prior_generation"]
                )
                if not created or admission.generation != prior_generation + 1:
                    raise RuntimeError(
                        "silent_terminal_rerun_generation_not_created"
                    )
                self._insert_promotion_audit(
                    conn,
                    event_uid=source_id,
                    outbox_id=int(bound_outbox["outbox_id"]),
                    submission_key=admission.submission_key,
                    operator=f"manual:{manual.requester_id}",
                    reason="silent_terminal_explicit_batch_rerun",
                    outcome="silent_terminal_new_generation_created",
                    from_status=f"terminal_failed:g{prior_generation}",
                    to_status=f"pending:g{admission.generation}",
                    detail=_canonical_json(normalized_silent_rerun),
                    created_at=current,
                )
            if normalized_batch_rerun is not None:
                prior_generation = int(normalized_batch_rerun["prior_generation"])
                if not created or admission.generation != prior_generation + 1:
                    raise RuntimeError(
                        "batch_terminal_rerun_generation_not_created"
                    )
                self._insert_promotion_audit(
                    conn,
                    event_uid=source_id,
                    outbox_id=int(bound_outbox["outbox_id"]),
                    submission_key=admission.submission_key,
                    operator=f"manual:{manual.requester_id}",
                    reason="settled_delivery_correction_batch_rerun",
                    outcome="batch_terminal_rerun_new_generation_created",
                    from_status=f"delivery_created:g{prior_generation}",
                    to_status=f"pending:g{admission.generation}",
                    detail=_canonical_json(normalized_batch_rerun),
                    created_at=current,
                )
            if normalized_historical_epoch_rerun is not None:
                prior_generation = int(
                    normalized_historical_epoch_rerun["prior_generation"]
                )
                if not created or admission.generation != prior_generation + 1:
                    raise RuntimeError("historical_epoch_rerun_generation_not_created")
                prior_epoch = str(
                    normalized_historical_epoch_rerun["prior_activation_epoch_id"]
                    or "legacy_unconfigured"
                )
                self._insert_promotion_audit(
                    conn,
                    event_uid=source_id,
                    outbox_id=int(bound_outbox["outbox_id"]),
                    submission_key=admission.submission_key,
                    operator=f"manual:{manual.requester_id}",
                    reason="historical_epoch_explicit_batch_rerun",
                    outcome="historical_epoch_rerun_new_generation_created",
                    from_status=f"historical_epoch:{prior_epoch}:g{prior_generation}",
                    to_status=(
                        "current_epoch:"
                        f"{normalized_historical_epoch_rerun['target_activation_epoch_id']}:"
                        f"pending:g{admission.generation}"
                    ),
                    detail=_canonical_json(normalized_historical_epoch_rerun),
                    created_at=current,
                )
            self._audit_manual_policy_observation_tx(
                conn,
                source_id=source_id,
                outbox_id=int(bound_outbox["outbox_id"]),
                submission_key=admission.submission_key,
                requester_id=manual.requester_id,
                scope_id=issue_scope_id,
                active_policy_sha256=active_policy_sha256,
                active_policy_version=policy.policy_version,
                chain_rule_version=admission.source_refs.rule_version,
                current=current,
            )

            conn.execute(
                """
                INSERT INTO rca_trigger_bindings(
                    source_id, business_key, generation, role, bound_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    source_id,
                    admission.business_key,
                    admission.generation,
                    "origin" if created else "observer",
                    current,
                ),
            )
            if normalized_silent_rerun is not None:
                self._persist_terminal_rerun_delivery_authority_tx(
                    conn,
                    authority_kind="silent_terminal",
                    authority=normalized_silent_rerun,
                    source_id=source_id,
                    source_payload_sha256=payload_sha,
                    admission=admission,
                    current=current,
                )
            if normalized_batch_rerun is not None:
                self._persist_terminal_rerun_delivery_authority_tx(
                    conn,
                    authority_kind="batch_terminal",
                    authority=normalized_batch_rerun,
                    source_id=source_id,
                    source_payload_sha256=payload_sha,
                    admission=admission,
                    current=current,
                )
            if normalized_historical_epoch_rerun is not None:
                self._persist_historical_epoch_rerun_delivery_authority_tx(
                    conn,
                    authority=normalized_historical_epoch_rerun,
                    source_id=source_id,
                    source_payload_sha256=payload_sha,
                    admission=admission,
                    current=current,
                )
            learning_lane = (
                False
                if any(
                    authority is not None
                    for authority in (
                        normalized_user_rerun,
                        normalized_historical_epoch_rerun,
                        normalized_silent_rerun,
                        normalized_batch_rerun,
                    )
                )
                else self._ensure_learning_lane_admission_tx(
                    conn, admission=admission, current=current
                )
            )
            if w3_authority is not None:
                self.persist_w3_admission_snapshot_tx(
                    conn,
                    admission=admission,
                    trigger_context=trigger_context,
                    source_id=source_id,
                    snapshot_authority=w3_authority,
                    ticket_authority=w3_ticket_authority,
                    activation_decision=activation_decision,
                    manual_ingress_authority=w3_manual_authority,
                )
            issue_subscription_key = self._insert_issue_subscription_tx(
                conn, admission=admission, current=current
            )
            subscription_key = issue_subscription_key
            if not issue_only_operator:
                subscription_key, _subscription_created = (
                    self._insert_thread_subscription_tx(
                        conn,
                        admission=admission,
                        source_id=source_id,
                        request=manual,
                        current=current,
                    )
                )
            self._bind_source_subscription_tx(
                conn,
                source_id=source_id,
                subscription_key=issue_subscription_key,
                current=current,
            )
            late_catchup = False
            if not learning_lane and not issue_only_operator:
                self._bind_source_subscription_tx(
                    conn,
                    source_id=source_id,
                    subscription_key=subscription_key,
                    current=current,
                )
                late_catchup = self._mark_late_catchup_tx(
                    conn,
                    subscription_key=subscription_key,
                    business_key=admission.business_key,
                    generation=admission.generation,
                    current=current,
                )
            if late_catchup and outcome == "joined":
                outcome = "catchup_attached"
                conn.execute(
                    "UPDATE rca_trigger_sources SET outcome = ? WHERE source_id = ?",
                    (outcome, source_id),
                )
            state_row = conn.execute(
                """
                SELECT state FROM business_triggers
                 WHERE business_key = ? AND generation = ?
                """,
                (admission.business_key, admission.generation),
            ).fetchone()
            conn.commit()
            return ManualRcaAdmissionResult(
                schema_version=MANUAL_ADMISSION_RESULT_SCHEMA_VERSION,
                outcome=outcome,
                business_key=admission.business_key,
                submission_key=admission.submission_key,
                generation=admission.generation,
                source_id=source_id,
                subscription_key=subscription_key,
                state=str(state_row["state"] if state_row is not None else "pending"),
                reason=(
                    rearm_reason if rearmed else manual.reason
                ),
            )
        except Exception:
            if conn.in_transaction:
                conn.rollback()
            raise
        finally:
            conn.close()

    def process_event(
        self,
        event_uid: str,
        *,
        runtime_identity: Mapping[str, Any] | None = None,
        allow_terminal_duplicate_retrigger: bool = False,
        snapshot_authority: Any = None,
    ) -> IngestResult:
        """Classify one durable inbox row and atomically create trigger/outbox state."""
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM kafka_inbox WHERE event_uid = ?", (event_uid,)
            ).fetchone()
            if row is None:
                raise KeyError(f"unknown inbox event: {event_uid}")
            transport_duplicate = row["decision"] != "pending"
            terminal_duplicate_retrigger = (
                transport_duplicate
                and allow_terminal_duplicate_retrigger
                and self._terminal_duplicate_retrigger_eligible_tx(
                    conn,
                    inbox_row=row,
                )
            )
            if transport_duplicate and not terminal_duplicate_retrigger:
                conn.commit()
                return IngestResult(
                    event_uid=event_uid,
                    decision=row["decision"],
                    reason=row["reason"],
                    raw_inserted=False,
                    transport_duplicate=True,
                    business_key=row["business_key"] or "",
                    submission_key=row["submission_key"] or "",
                    generation=int(row["generation"] or 0),
                    outbox_rearmed=(
                        str(row["rearm_reason"] or "")
                        == INPUT_WAIT_QUARANTINE_REARMED_REASON
                    ),
                    rearm_reason=str(row["rearm_reason"] or ""),
                )

            persisted_policy = WorkflowEventPolicy.from_mapping(
                json.loads(row["policy_json"])
            )
            result = classify_workflow_event(
                topic=row["topic"], value=row["raw_value"], policy=persisted_policy
            )
            now_dt = _utc_datetime()
            now = _iso(now_dt)
            normalized_json = None
            business_key = ""
            submission_key = ""
            generation = 0
            decision = result.decision
            reason = result.reason
            trigger_created = False
            outbox_created = False
            creates_generation = False
            input_wait_terminal_generation_created = False
            w3_automatic_observation_joined = False
            outbox_rearmed = False
            rearm_reason = ""
            activation_decision: ActivationAdmissionDecision | None = None
            effective_submission_mode = str(row["submission_mode"])

            if result.normalized is not None:
                normalized = result.normalized
                normalized_json = _canonical_json(normalized.to_dict())
                candidate_admission = build_event_admission(
                    normalized,
                    topic=row["topic"],
                    partition=row["partition_id"],
                    offset=row["offset_id"],
                )
                latest, issue_scope_id, business_key_count = (
                    self._select_issue_scope_tx(
                        conn,
                        project_key=normalized.project_key,
                        work_item_type_key=normalized.work_item_type_key,
                        work_item_id=normalized.work_item_id,
                    )
                )
                if business_key_count > 1:
                    self._audit_issue_scope_conflict_tx(
                        conn,
                        event_uid=event_uid,
                        operator="kafka:process_event",
                        scope_id=issue_scope_id,
                        business_key_count=business_key_count,
                        current=now,
                    )
                    decision = "invalid"
                    reason = ISSUE_SCOPE_CONFLICT_REASON
                else:
                    binding_row = latest
                    if latest is not None:
                        # Manual generations do not retarget the Kafka chain.
                        # Query the latest immutable Kafka origin explicitly.
                        binding_row = self._select_latest_kafka_issue_generation_tx(
                            conn,
                            project_key=normalized.project_key,
                            work_item_type_key=normalized.work_item_type_key,
                            work_item_id=normalized.work_item_id,
                        )
                        if (
                            binding_row is not None
                            and not self._kafka_generation_origin_contract_valid(
                                binding_row
                            )
                        ):
                            decision = "invalid"
                            reason = LEGACY_KAFKA_GENERATION_REASON
                            binding_row = None
                        elif binding_row is None:
                            binding_row = self._select_issue_generation_tx(
                                conn,
                                project_key=normalized.project_key,
                                work_item_type_key=normalized.work_item_type_key,
                                work_item_id=normalized.work_item_id,
                                generation=1,
                            )
                        if binding_row is None and decision != "invalid":
                            decision = "invalid"
                            reason = "issue_scope_kafka_generation_missing"
                    if decision == "invalid":
                        admission = None
                    elif binding_row is None:
                        admission = candidate_admission
                        creates_generation = True
                    else:
                        bound_generation = int(binding_row["generation"])
                        trigger_kind = "kafka_retrigger"
                        if bound_generation == 1:
                            trigger_kind = (
                                "issue_created"
                                if str(binding_row["source_event_id"] or "").strip()
                                else "manual_issue_request"
                            )
                        admission_kwargs: dict[str, Any] = {}
                        if trigger_kind in RCA_KAFKA_TRIGGER_KINDS:
                            admission_kwargs = {
                                "topic": str(row["topic"]),
                                "partition": int(row["partition_id"]),
                                "offset": int(row["offset_id"]),
                            }
                        admission = build_rca_admission(
                            project_key=str(binding_row["project_key"]),
                            project_simple_name=normalized.project_simple_name,
                            work_item_type_key=str(binding_row["work_item_type_key"]),
                            work_item_id=str(binding_row["work_item_id"]),
                            rule_version=str(binding_row["creation_rule_version"]),
                            trigger_kind=trigger_kind,
                            generation=bound_generation,
                            **admission_kwargs,
                        )
                        if (
                            admission.business_key != str(binding_row["business_key"])
                            or admission.submission_key
                            != str(binding_row["submission_key"])
                        ):
                            raise RecordConflictError(
                                "issue scope chain admission identity changed"
                            )
                        same_rule_chain = (
                            candidate_admission.business_key
                            == admission.business_key
                        )
                        latest_is_bound_generation = (
                            latest is not None
                            and int(latest["generation"]) == bound_generation
                        )
                        current_profile_sha256 = (
                            self._business_profile_observation_sha256(
                                normalized.to_dict()
                            )
                        )
                        previous_profile_sha256 = (
                            self._latest_business_profile_observation_sha256_tx(
                                conn,
                                project_key=normalized.project_key,
                                work_item_type_key=normalized.work_item_type_key,
                                work_item_id=normalized.work_item_id,
                            )
                        )
                        profile_changed = bool(
                            current_profile_sha256
                            and current_profile_sha256 != previous_profile_sha256
                        )
                        if profile_changed:
                            if latest is None or not same_rule_chain:
                                decision = "invalid"
                                reason = "business_profile_change_chain_invalid"
                                admission = None
                            elif not self._execution_terminal_tx(conn, latest):
                                decision = "invalid"
                                reason = "business_profile_change_requires_terminal_generation"
                                admission = None
                            else:
                                prior_submission_key = str(latest["submission_key"])
                                admission = build_rca_admission(
                                    project_key=str(latest["project_key"]),
                                    project_simple_name=(
                                        normalized.project_simple_name
                                    ),
                                    work_item_type_key=str(
                                        latest["work_item_type_key"]
                                    ),
                                    work_item_id=str(latest["work_item_id"]),
                                    rule_version=str(
                                        latest["creation_rule_version"]
                                    ),
                                    trigger_kind="kafka_retrigger",
                                    generation=int(latest["generation"]) + 1,
                                    topic=str(row["topic"]),
                                    partition=int(row["partition_id"]),
                                    offset=int(row["offset_id"]),
                                )
                                if (
                                    admission.business_key
                                    != str(latest["business_key"])
                                    or admission.submission_key
                                    == prior_submission_key
                                ):
                                    raise RecordConflictError(
                                        "business profile next generation identity invalid"
                                    )
                                creates_generation = True
                                rearm_reason = "business_profile_observation_changed"
                        elif same_rule_chain and latest_is_bound_generation:
                            input_wait, _replacement_reason = (
                                self._input_wait_replacement_candidate(
                                    conn,
                                    inbox_row=row,
                                    admission=admission,
                                    normalized=normalized,
                                    allow_same_event=terminal_duplicate_retrigger,
                                )
                            )
                            if (
                                input_wait is not None
                                and self._execution_watch_exists_tx(
                                    conn, admission.submission_key
                                )
                                and self._execution_terminal_tx(conn, input_wait)
                            ):
                                prior_submission_key = admission.submission_key
                                admission = build_rca_admission(
                                    project_key=str(binding_row["project_key"]),
                                    project_simple_name=(
                                        normalized.project_simple_name
                                    ),
                                    work_item_type_key=str(
                                        binding_row["work_item_type_key"]
                                    ),
                                    work_item_id=str(binding_row["work_item_id"]),
                                    rule_version=str(
                                        binding_row["creation_rule_version"]
                                    ),
                                    trigger_kind="kafka_retrigger",
                                    generation=bound_generation + 1,
                                    topic=str(row["topic"]),
                                    partition=int(row["partition_id"]),
                                    offset=int(row["offset_id"]),
                                )
                                if (
                                    admission.business_key
                                    != str(binding_row["business_key"])
                                    or admission.submission_key
                                    == prior_submission_key
                                ):
                                    raise RecordConflictError(
                                        "input-wait next generation identity invalid"
                                    )
                                creates_generation = True
                                input_wait_terminal_generation_created = True
                                rearm_reason = (
                                    INPUT_WAIT_TERMINAL_NEW_GENERATION_REASON
                                )

                        if (
                            snapshot_authority is not None
                            and admission is not None
                            and admission.generation > 1
                            and creates_generation
                        ):
                            if latest is None:
                                raise RecordConflictError(
                                    "w3_automatic_generation_parent_missing"
                                )
                            observed_generation = int(latest["generation"])
                            observed_trigger_kind = (
                                "issue_created"
                                if observed_generation == 1
                                else "kafka_retrigger"
                            )
                            admission = build_rca_admission(
                                project_key=str(latest["project_key"]),
                                project_simple_name=normalized.project_simple_name,
                                work_item_type_key=str(
                                    latest["work_item_type_key"]
                                ),
                                work_item_id=str(latest["work_item_id"]),
                                rule_version=str(latest["creation_rule_version"]),
                                trigger_kind=observed_trigger_kind,
                                generation=observed_generation,
                                topic=str(row["topic"]),
                                partition=int(row["partition_id"]),
                                offset=int(row["offset_id"]),
                            )
                            if (
                                admission.business_key
                                != str(latest["business_key"])
                                or admission.submission_key
                                != str(latest["submission_key"])
                            ):
                                raise RecordConflictError(
                                    "w3_automatic_observation_identity_mismatch"
                                )
                            creates_generation = False
                            input_wait_terminal_generation_created = False
                            rearm_reason = ""
                            w3_automatic_observation_joined = True

                    parent_snapshot_row = None
                    if (
                        snapshot_authority is not None
                        and admission is not None
                        and not creates_generation
                    ):
                        parent_snapshot_row = conn.execute(
                            """
                            SELECT admission_snapshot_json
                              FROM rca_admission_snapshots
                             WHERE business_key = ? AND generation = ?
                            """,
                            (admission.business_key, admission.generation),
                        ).fetchone()
                    if (
                        snapshot_authority is not None
                        and admission is not None
                        and not creates_generation
                        and parent_snapshot_row is None
                    ):
                        decision = "invalid"
                        reason = W3_LEGACY_PARENT_SNAPSHOT_MISSING_REASON
                        admission = None
                        creates_generation = False
                        input_wait_terminal_generation_created = False
                        w3_automatic_observation_joined = False
                        rearm_reason = ""
                    elif (
                        w3_automatic_observation_joined
                        and parent_snapshot_row is not None
                    ):
                        try:
                            parent_snapshot = json.loads(
                                str(parent_snapshot_row["admission_snapshot_json"])
                            )
                            parent_ticket = parent_snapshot["canonical_request"][
                                "ticket"
                            ]
                        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                            raise RecordConflictError(
                                "w3_automatic_observation_parent_snapshot_invalid"
                            ) from exc
                        if not isinstance(parent_ticket, Mapping):
                            raise RecordConflictError(
                                "w3_automatic_observation_parent_snapshot_invalid"
                            )
                        observed_ticket = {
                            "project_key": normalized.project_key,
                            "project_simple_name": normalized.project_simple_name,
                            "work_item_type_key": normalized.work_item_type_key,
                            "work_item_id": normalized.work_item_id,
                            "issue_url": normalized.issue_url.rstrip("/"),
                            "title": normalized.title,
                        }
                        if any(
                            str(parent_ticket.get(name) or "") != value
                            for name, value in observed_ticket.items()
                        ):
                            decision = "invalid"
                            reason = (
                                W3_AUTOMATIC_OBSERVATION_SNAPSHOT_MISMATCH_REASON
                            )
                            admission = None
                            creates_generation = False
                            input_wait_terminal_generation_created = False
                            w3_automatic_observation_joined = False
                            rearm_reason = ""

                    if admission is None:
                        business_key = ""
                        generation = 0
                        submission_key = ""
                        kafka_source_id = ""
                        creates_generation = False
                    else:
                        business_key = admission.business_key
                        generation = admission.generation
                        submission_key = admission.submission_key
                        if int(row["submit_enabled_requested"] or 0) == 1:
                            captured_epoch = (
                                str(row["activation_epoch_id"])
                                if row["activation_epoch_id"] is not None
                                else (
                                    ""
                                    if int(row["activation_required"] or 0) == 1
                                    else None
                                )
                            )
                            activation_decision = self.adjudicate_activation_tx(
                                conn,
                                entrypoint="kafka_ingest",
                                source_kind="kafka",
                                source_identity={"event_uid": event_uid},
                                business_key=business_key,
                                submission_key=submission_key,
                                generation=generation,
                                new_execution=creates_generation,
                                ingress_epoch_id=captured_epoch,
                                now=now_dt,
                            )
                            if activation_decision.decision == "admit":
                                effective_submission_mode = "pending"
                        kafka_source_id = self._ensure_kafka_source_tx(
                            conn,
                            event_uid=event_uid,
                            raw_sha256=str(row["raw_sha256"]),
                            generation=admission.generation,
                            trigger_kind=(
                                admission.trigger_kind
                                if admission.trigger_kind in RCA_KAFKA_TRIGGER_KINDS
                                else "issue_created"
                            ),
                            current=now,
                        ) if admission is not None else ""
                    if admission is not None and creates_generation:
                        inserted = conn.execute(
                            """
                            INSERT INTO business_triggers(
                                business_key, generation, submission_key,
                                creation_rule_version, work_item_id, project_key,
                                work_item_type_key, origin_source_id, source_event_id,
                                source_topic, source_partition, source_offset,
                                normalized_json, state, created_at
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                business_key,
                                generation,
                                submission_key,
                                admission.source_refs.rule_version,
                                normalized.work_item_id,
                                normalized.project_key,
                                normalized.work_item_type_key,
                                kafka_source_id,
                                event_uid,
                                row["topic"],
                                row["partition_id"],
                                row["offset_id"],
                                normalized_json,
                                effective_submission_mode,
                                now,
                            ),
                        )
                        trigger_created = inserted.rowcount == 1
                    origin_row = conn.execute(
                        """
                        SELECT origin_source_id FROM business_triggers
                         WHERE business_key = ? AND generation = ?
                        """,
                        (business_key, generation),
                    ).fetchone() if admission is not None else None
                    origin_source_id = str(
                        origin_row["origin_source_id"]
                        if origin_row is not None
                        else ""
                    ).strip()
                    if admission is not None and not origin_source_id:
                        raise RuntimeError("trigger_origin_source_missing")
                    if admission is not None and trigger_created:
                        outbox_status = effective_submission_mode
                        payload_json = _submission_outbox_payload(
                            admission=admission,
                            normalized=normalized,
                            event_uid=event_uid,
                            topic=str(row["topic"]),
                            partition=int(row["partition_id"]),
                            offset=int(row["offset_id"]),
                            origin_source_id=origin_source_id,
                        )
                        outbox = conn.execute(
                            """
                            INSERT INTO rca_outbox(
                                action, business_key, submission_key,
                                creation_rule_version, generation, origin_source_id,
                                source_event_id, source_topic, source_partition,
                                source_offset, payload_json, status,
                                retry_window_started_at, created_at, updated_at
                            ) VALUES (
                                'submit_rca_issue_intake', ?, ?, ?, ?, ?, ?, ?, ?, ?,
                                ?, ?, ?, ?, ?
                            )
                            """,
                            (
                                business_key,
                                submission_key,
                                admission.source_refs.rule_version,
                                generation,
                                origin_source_id,
                                event_uid,
                                row["topic"],
                                row["partition_id"],
                                row["offset_id"],
                                payload_json,
                                outbox_status,
                                now,
                                now,
                                now,
                            ),
                        )
                        outbox_created = outbox.rowcount == 1
                        if input_wait_terminal_generation_created:
                            rearm_reason = INPUT_WAIT_TERMINAL_NEW_GENERATION_REASON
                            if effective_submission_mode == "pending":
                                reason = INPUT_WAIT_TERMINAL_NEW_GENERATION_REASON
                        if (
                            activation_decision is not None
                            and activation_decision.decision == "admit"
                            and activation_decision.ledger_id is not None
                        ):
                            self.bind_activation_admission_tx(
                                conn,
                                activation_decision,
                                business_key=business_key,
                                submission_key=submission_key,
                                generation=generation,
                                now=now_dt,
                            )
                    elif admission is not None:
                        decision = "deduped"
                        reason = "business_trigger_exists"
                        same_rule_chain = (
                            candidate_admission.business_key == admission.business_key
                        )
                        if same_rule_chain:
                            payload_json = _submission_outbox_payload(
                                admission=admission,
                                normalized=normalized,
                                event_uid=event_uid,
                                topic=str(row["topic"]),
                                partition=int(row["partition_id"]),
                                offset=int(row["offset_id"]),
                                origin_source_id=origin_source_id,
                            )
                            outbox_rearmed, rearm_reason = (
                                self._rearm_input_wait_quarantine(
                                    conn,
                                    inbox_row=row,
                                    event_uid=event_uid,
                                    admission=admission,
                                    normalized=normalized,
                                    normalized_json=normalized_json,
                                    payload_json=payload_json,
                                    current=now,
                                )
                            )
                            if outbox_rearmed:
                                reason = INPUT_WAIT_QUARANTINE_REARMED_REASON
                            elif w3_automatic_observation_joined:
                                reason = W3_KAFKA_OBSERVATION_JOIN_REASON

                    bound_source_id = self._bind_kafka_source_tx(
                        conn,
                        event_uid=event_uid,
                        raw_sha256=str(row["raw_sha256"]),
                        business_key=admission.business_key,
                        generation=admission.generation,
                        trigger_kind=(
                            admission.trigger_kind
                            if admission.trigger_kind in RCA_KAFKA_TRIGGER_KINDS
                            else "issue_created"
                        ),
                        current=now,
                    ) if admission is not None else ""
                    if admission is not None and trigger_created:
                        conn.execute(
                            """
                            UPDATE rca_trigger_bindings SET role = 'origin'
                             WHERE source_id = ?
                            """,
                            (bound_source_id,),
                        )
                    if admission is not None:
                        learning_lane = self._ensure_learning_lane_admission_tx(
                            conn, admission=admission, current=now
                        )
                        issue_subscription_key = self._insert_issue_subscription_tx(
                            conn, admission=admission, current=now
                        )
                        self._bind_source_subscription_tx(
                            conn,
                            source_id=bound_source_id,
                            subscription_key=issue_subscription_key,
                            current=now,
                        )
                        if (
                            snapshot_authority is not None
                            and activation_decision is not None
                        ):
                            snapshot_context = build_rca_trigger_context(
                                source_kind="kafka_workflow_event",
                                project_key=normalized.project_key,
                                project_simple_name=normalized.project_simple_name,
                                work_item_type_key=normalized.work_item_type_key,
                                work_item_id=normalized.work_item_id,
                                rule_version=admission.source_refs.rule_version,
                                issue_url=normalized.issue_url,
                                title=normalized.title,
                            )
                            self.persist_w3_admission_snapshot_tx(
                                conn,
                                admission=admission,
                                trigger_context=snapshot_context,
                                source_id=bound_source_id,
                                snapshot_authority=snapshot_authority,
                                ticket_authority=None,
                                activation_decision=activation_decision,
                            )

            if decision == "invalid":
                conn.execute(
                    """
                    INSERT OR IGNORE INTO kafka_dead_letters(
                        source_event_id, source_topic, source_partition, source_offset,
                        error_code, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event_uid,
                        row["topic"],
                        row["partition_id"],
                        row["offset_id"],
                        reason,
                        now,
                    ),
                )

            self._advance_partition_progress(conn, row, now)

            conn.execute(
                """
                UPDATE kafka_inbox
                   SET decision = ?, reason = ?, normalized_json = ?, business_key = ?,
                       submission_key = ?, generation = ?, rearm_reason = ?,
                       submission_mode = ?,
                       raw_value = CASE
                           WHEN (? = 'filtered' AND ? IN ('topic_not_allowed', 'unsupported_message_shape'))
                                OR ? = 'event_too_large' THEN X''
                           ELSE raw_value
                       END,
                       raw_pruned_at = CASE
                           WHEN (? = 'filtered' AND ? IN ('topic_not_allowed', 'unsupported_message_shape'))
                                OR ? = 'event_too_large' THEN ?
                           ELSE raw_pruned_at
                       END,
                       last_processing_error_code = '',
                       last_processing_error_detail = '', processed_at = ?
                 WHERE event_uid = ?
                """,
                (
                    decision,
                    reason,
                    normalized_json,
                    business_key or None,
                    submission_key or None,
                    generation or None,
                    rearm_reason,
                    effective_submission_mode,
                    decision,
                    reason,
                    reason,
                    decision,
                    reason,
                    reason,
                    now,
                    now,
                    event_uid,
                ),
            )
            if runtime_identity is not None and submission_key:
                insert_host_runtime_transition(
                    conn,
                    submission_key=submission_key,
                    business_key=business_key,
                    generation=generation,
                    service_label="local.pnc.rca-kafka-consumer",
                    transition_kind="kafka_ingested",
                    entity_key=event_uid,
                    runtime_identity=runtime_identity,
                    transitioned_at=now,
                )
            self._prune_replay_raw_tx(conn, now=now_dt)
            conn.commit()
            return IngestResult(
                event_uid=event_uid,
                decision=decision,
                reason=reason,
                raw_inserted=False,
                transport_duplicate=transport_duplicate,
                trigger_created=trigger_created,
                outbox_created=outbox_created,
                business_key=business_key,
                submission_key=submission_key,
                generation=generation,
                outbox_rearmed=outbox_rearmed,
                rearm_reason=rearm_reason,
            )
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def ingest_record(
        self,
        record: KafkaRecord,
        *,
        policy: WorkflowEventPolicy,
        submit_enabled: bool = False,
        activation_required: bool = False,
        runtime_identity: Mapping[str, Any] | None = None,
        after_raw_persisted: Callable[[RawPersistResult], None] | None = None,
        snapshot_authority: Any = None,
    ) -> IngestResult:
        """Persist raw, then classify; callers may commit only after this returns."""
        raw_result = self.persist_raw(
            record,
            policy=policy,
            submit_enabled=submit_enabled,
            activation_required=activation_required,
        )
        if after_raw_persisted is not None:
            after_raw_persisted(raw_result)
        result = self.process_event_resilient(
            raw_result.event_uid,
            runtime_identity=runtime_identity,
            allow_terminal_duplicate_retrigger=not raw_result.inserted,
            snapshot_authority=snapshot_authority,
        )
        return replace(result, raw_inserted=raw_result.inserted)

    def process_event_resilient(
        self,
        event_uid: str,
        *,
        runtime_identity: Mapping[str, Any] | None = None,
        allow_terminal_duplicate_retrigger: bool = False,
        snapshot_authority: Any = None,
    ) -> IngestResult:
        """Classify one event while keeping unknown failures unacknowledged."""
        try:
            return self.process_event(
                event_uid,
                runtime_identity=runtime_identity,
                allow_terminal_duplicate_retrigger=(
                    allow_terminal_duplicate_retrigger
                ),
                snapshot_authority=snapshot_authority,
            )
        except (sqlite3.Error, OSError):
            raise
        except Exception as exc:
            self._record_processing_failure(event_uid, exc)
            raise RecordProcessingBlockedError(event_uid) from exc

    def _record_processing_failure(
        self,
        event_uid: str,
        exc: Exception,
    ) -> None:
        error_type = type(exc).__name__[:100]
        error_code = f"record_processing_{error_type}"[:120]
        current = _now_iso()
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM kafka_inbox WHERE event_uid = ?", (event_uid,)
            ).fetchone()
            if row is None:
                raise KeyError(f"unknown inbox event: {event_uid}")
            if row["decision"] != "pending":
                conn.commit()
                return
            attempts = int(row["processing_attempts"] or 0) + 1
            conn.execute(
                """
                UPDATE kafka_inbox
                   SET processing_attempts = ?, last_processing_error_code = ?,
                       last_processing_error_detail = ?, processing_failed_at = ?
                 WHERE event_uid = ? AND decision = 'pending'
                """,
                (
                    attempts,
                    error_code,
                    error_type,
                    current,
                    event_uid,
                ),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    @staticmethod
    def _advance_partition_progress(
        conn: sqlite3.Connection,
        inbox_row: sqlite3.Row,
        updated_at: str,
    ) -> None:
        """Record local durability before the caller may commit the Kafka offset."""
        offset = int(inbox_row["offset_id"])
        conn.execute(
            """
            INSERT INTO kafka_partition_progress(
                topic, partition_id, first_offset, durable_next_offset,
                last_event_uid, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(topic, partition_id) DO UPDATE SET
                durable_next_offset = MAX(
                    kafka_partition_progress.durable_next_offset,
                    excluded.durable_next_offset
                ),
                last_event_uid = CASE
                    WHEN excluded.durable_next_offset >=
                         kafka_partition_progress.durable_next_offset
                    THEN excluded.last_event_uid
                    ELSE kafka_partition_progress.last_event_uid
                END,
                updated_at = CASE
                    WHEN excluded.durable_next_offset >=
                         kafka_partition_progress.durable_next_offset
                    THEN excluded.updated_at
                    ELSE kafka_partition_progress.updated_at
                END
            """,
            (
                inbox_row["topic"],
                inbox_row["partition_id"],
                offset,
                offset + 1,
                inbox_row["event_uid"],
                updated_at,
            ),
        )

    def partition_progress(
        self,
        *,
        topic: str,
        partitions: Iterable[int],
    ) -> dict[int, int]:
        """Return durable next offsets used to detect broker/DB split brain."""
        normalized = sorted({int(partition) for partition in partitions})
        if not normalized:
            return {}
        if any(partition < 0 for partition in normalized):
            raise ValueError("partitions must be non-negative")
        placeholders = ",".join("?" for _ in normalized)
        conn = self._connect()
        try:
            rows = conn.execute(
                f"""
                SELECT partition_id, durable_next_offset
                  FROM kafka_partition_progress
                 WHERE topic = ? AND partition_id IN ({placeholders})
                """,
                (str(topic), *normalized),
            ).fetchall()
            return {
                int(row["partition_id"]): int(row["durable_next_offset"])
                for row in rows
            }
        finally:
            conn.close()

    def activation_partition_start_fence(
        self,
        *,
        topic: str,
        partitions: Iterable[int],
    ) -> dict[int, int]:
        """Read the current activation epoch's immutable Kafka start fence.

        A bounded activation may intentionally begin after the durable local
        progress checkpoint because the pre-release cohort is sealed and held.
        Consumers need the exact epoch fence to distinguish that deliberate
        skip from an unexplained broker/DB split-brain.
        """
        normalized = sorted({int(partition) for partition in partitions})
        if any(partition < 0 for partition in normalized):
            raise ValueError("partitions must be non-negative")
        if not normalized:
            return {}
        conn = self._connect()
        try:
            conn.execute("BEGIN")
            row = self._current_activation_epoch_tx(conn)
            if row is None or str(row["state"]) != "steady_active":
                conn.commit()
                return {}
            try:
                start_fence = json.loads(str(row["partition_start_fence_json"]))
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise ActivationEpochError(
                    "activation_partition_start_fence_invalid"
                ) from exc
            topic_fence = start_fence.get(str(topic))
            if not isinstance(topic_fence, dict):
                raise ActivationEpochError(
                    "activation_partition_start_fence_missing"
                )
            result: dict[int, int] = {}
            for partition in normalized:
                raw_offset = topic_fence.get(str(partition))
                if (
                    isinstance(raw_offset, bool)
                    or not isinstance(raw_offset, int)
                    or raw_offset < 0
                ):
                    raise ActivationEpochError(
                        "activation_partition_start_fence_invalid"
                    )
                result[partition] = raw_offset
            conn.commit()
            return result
        except Exception:
            if conn.in_transaction:
                conn.rollback()
            raise
        finally:
            conn.close()

    def record_host_runtime_transition(
        self,
        *,
        submission_key: str,
        business_key: str,
        generation: int,
        service_label: str,
        transition_kind: str,
        entity_key: str,
        runtime_identity: Mapping[str, Any],
        now: datetime | None = None,
    ) -> dict[str, Any]:
        current = _iso(now)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            result = insert_host_runtime_transition(
                conn,
                submission_key=submission_key,
                business_key=business_key,
                generation=generation,
                service_label=service_label,
                transition_kind=transition_kind,
                entity_key=entity_key,
                runtime_identity=runtime_identity,
                transitioned_at=current,
            )
            conn.commit()
            return result
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def pending_event_uids(self, *, limit: int = 1000) -> list[str]:
        """Return durable raw events left between the raw and classification phases."""
        if limit < 1:
            return []
        conn = self._connect()
        try:
            rows = conn.execute(
                """
                SELECT event_uid
                  FROM kafka_inbox
                 WHERE decision = 'pending'
                 ORDER BY received_at, topic, partition_id, offset_id
                 LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [str(row["event_uid"]) for row in rows]
        finally:
            conn.close()

    def process_pending(
        self,
        *,
        limit: int = 1000,
        runtime_identity: Mapping[str, Any] | None = None,
        snapshot_authority: Any = None,
    ) -> list[IngestResult]:
        """Recover raw rows from a prior crash before polling new Kafka records."""
        return [
            self.process_event_resilient(
                event_uid,
                runtime_identity=runtime_identity,
                snapshot_authority=snapshot_authority,
            )
            for event_uid in self.pending_event_uids(limit=limit)
        ]

    @staticmethod
    def _insert_promotion_audit(
        conn: sqlite3.Connection,
        *,
        event_uid: str,
        outbox_id: int | None,
        submission_key: str,
        operator: str,
        reason: str,
        outcome: str,
        from_status: str,
        to_status: str,
        detail: str,
        created_at: str,
    ) -> int:
        cursor = conn.execute(
            """
            INSERT INTO rca_shadow_promotion_audit(
                event_uid, outbox_id, submission_key, operator, reason,
                outcome, from_status, to_status, detail, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_uid,
                outbox_id,
                submission_key,
                operator,
                reason,
                outcome,
                from_status,
                to_status,
                detail,
                created_at,
            ),
        )
        if cursor.lastrowid is None:
            raise RuntimeError("promotion audit insert returned no row id")
        return int(cursor.lastrowid)

    @classmethod
    def _activation_claim_predicate_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        alias: str = "o",
    ) -> tuple[str, tuple[Any, ...]]:
        epoch = cls._current_activation_epoch_tx(conn)
        if epoch is None or str(epoch["state"] or "") != "steady_active":
            return "0", ()
        epoch_id = str(epoch["epoch_id"])
        ledger_match = f"""
            {alias}.activation_epoch_id = ?
            AND {alias}.activation_ledger_id IS NOT NULL
            AND EXISTS (
                SELECT 1 FROM rca_activation_admission_ledger AS al
                 WHERE al.ledger_id = {alias}.activation_ledger_id
                   AND al.epoch_id = {alias}.activation_epoch_id
                   AND al.decision = 'admit'
                   AND al.bound_at IS NOT NULL
                   AND al.business_key = {alias}.business_key
                   AND al.submission_key = {alias}.submission_key
                   AND al.generation = {alias}.generation
            )
            AND EXISTS (
                SELECT 1 FROM business_triggers AS bt
                 WHERE bt.activation_epoch_id = {alias}.activation_epoch_id
                   AND bt.activation_ledger_id = {alias}.activation_ledger_id
                   AND bt.business_key = {alias}.business_key
                   AND bt.submission_key = {alias}.submission_key
                   AND bt.generation = {alias}.generation
            )
        """
        return f"({ledger_match})", (epoch_id,)

    def claim_outbox(
        self,
        *,
        lease_owner: str,
        lease_seconds: int = 180,
        max_age_seconds: int = 86_400,
        submission_key: str | None = None,
        now: datetime | None = None,
    ) -> OutboxClaim | None:
        """Atomically claim one due pending row or recover one expired lease.

        Shadow rows are intentionally absent from every eligibility predicate.
        ``lease_token`` and ``fence`` change on every claim, so a worker that
        wakes after losing its lease cannot complete or reschedule the row.
        """
        owner = str(lease_owner or "").strip()
        if not owner:
            raise ValueError("lease_owner must not be empty")
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be positive")
        if max_age_seconds < 1:
            raise ValueError("max_age_seconds must be positive")
        target_submission_key = None
        if submission_key is not None:
            target_submission_key = str(submission_key).strip()
            if (
                not target_submission_key
                or len(target_submission_key) > 256
                or any(ord(char) < 0x21 for char in target_submission_key)
            ):
                raise ValueError("submission_key filter is invalid")
        current = _utc_datetime(now)
        now_iso = _iso(current)
        expires_at = _iso(current + timedelta(seconds=lease_seconds))
        cutoff = _iso(current - timedelta(seconds=max_age_seconds))
        token = uuid.uuid4().hex

        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            activation_predicate, activation_parameters = (
                self._activation_claim_predicate_tx(conn)
            )
            target_clause = (
                " AND o.submission_key = ?" if target_submission_key else ""
            )
            target_parameters = (
                (target_submission_key,) if target_submission_key else ()
            )
            expired_rows = conn.execute(
                f"""
                SELECT o.outbox_id, o.business_key, o.generation
                  FROM rca_outbox AS o
                 WHERE ({activation_predicate})
                   {target_clause}
                   AND COALESCE(o.retry_window_started_at, o.created_at) <= ?
                   AND (
                        o.status = 'pending'
                        OR (
                            o.status = 'claimed'
                            AND o.lease_expires_at IS NOT NULL
                            AND o.lease_expires_at <= ?
                        )
                   )
                """,
                (*activation_parameters, *target_parameters, cutoff, now_iso),
            ).fetchall()
            if expired_rows:
                expired_ids = [int(row["outbox_id"]) for row in expired_rows]
                placeholders = ",".join("?" for _ in expired_ids)
                conn.execute(
                    f"""
                    UPDATE rca_outbox
                       SET status = 'quarantined', quarantined_at = ?,
                           lease_token = NULL, lease_owner = NULL,
                           lease_expires_at = NULL,
                           last_error_code = 'dispatch_age_exceeded',
                           last_error_detail = 'outbox row exceeded dispatch retry horizon',
                           updated_at = ?
                     WHERE outbox_id IN ({placeholders})
                    """,
                    (now_iso, now_iso, *expired_ids),
                )
                for row in expired_rows:
                    conn.execute(
                        """
                        UPDATE business_triggers
                           SET state = 'quarantined'
                         WHERE business_key = ? AND generation = ?
                        """,
                        (row["business_key"], row["generation"]),
                    )

            streak_row = conn.execute(
                "SELECT value FROM control_meta WHERE key = ?",
                (OUTBOX_KAFKA_CLAIM_STREAK_META_KEY,),
            ).fetchone()
            try:
                kafka_claim_streak = int(streak_row["value"])
            except (KeyError, TypeError, ValueError):
                kafka_claim_streak = 0
            kafka_claim_streak = max(
                0,
                min(
                    kafka_claim_streak,
                    OUTBOX_MAX_CONSECUTIVE_KAFKA_CLAIMS,
                ),
            )
            if kafka_claim_streak >= OUTBOX_MAX_CONSECUTIVE_KAFKA_CLAIMS:
                source_order = "CASE WHEN o.source_topic IS NULL THEN 0 ELSE 1 END"
            else:
                source_order = "CASE WHEN o.source_topic IS NULL THEN 1 ELSE 0 END"

            row = conn.execute(
                f"""
                SELECT o.*
                  FROM rca_outbox AS o
                 WHERE ({activation_predicate})
                   {target_clause}
                   AND ((
                        o.status = 'pending'
                        AND (o.next_attempt_at IS NULL OR o.next_attempt_at <= ?)
                       )
                    OR (
                        o.status = 'claimed'
                        AND o.lease_expires_at IS NOT NULL
                        AND o.lease_expires_at <= ?
                       ))
                 ORDER BY {source_order}, COALESCE(
                              o.next_attempt_at,
                              o.retry_window_started_at,
                              o.created_at
                          ), outbox_id
                 LIMIT 1
                """,
                (*activation_parameters, *target_parameters, now_iso, now_iso),
            ).fetchone()
            if row is None:
                conn.commit()
                return None

            updated = conn.execute(
                """
                UPDATE rca_outbox
                   SET status = 'claimed', attempt = attempt + 1,
                       fence = fence + 1, lease_token = ?, lease_owner = ?,
                       lease_expires_at = ?, claimed_at = ?, updated_at = ?
                 WHERE outbox_id = ?
                   AND (? IS NULL OR submission_key = ?)
                   AND (
                        (status = 'pending' AND (next_attempt_at IS NULL OR next_attempt_at <= ?))
                        OR (
                            status = 'claimed'
                            AND lease_expires_at IS NOT NULL
                            AND lease_expires_at <= ?
                        )
                   )
                """,
                (
                    token,
                    owner,
                    expires_at,
                    now_iso,
                    now_iso,
                    row["outbox_id"],
                    target_submission_key,
                    target_submission_key,
                    now_iso,
                    now_iso,
                ),
            )
            if updated.rowcount != 1:
                conn.rollback()
                return None
            claimed = conn.execute(
                "SELECT * FROM rca_outbox WHERE outbox_id = ?",
                (row["outbox_id"],),
            ).fetchone()
            if str(claimed["source_topic"] or "").strip():
                next_kafka_claim_streak = min(
                    OUTBOX_MAX_CONSECUTIVE_KAFKA_CLAIMS,
                    kafka_claim_streak + 1,
                )
            else:
                next_kafka_claim_streak = 0
            conn.execute(
                """
                INSERT INTO control_meta(key, value) VALUES(?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (
                    OUTBOX_KAFKA_CLAIM_STREAK_META_KEY,
                    str(next_kafka_claim_streak),
                ),
            )
            conn.execute(
                """
                UPDATE business_triggers
                   SET state = 'dispatching'
                 WHERE business_key = ? AND generation = ?
                """,
                (claimed["business_key"], claimed["generation"]),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        try:
            payload = json.loads(claimed["payload_json"])
        except (TypeError, json.JSONDecodeError):
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        return OutboxClaim(
            outbox_id=int(claimed["outbox_id"]),
            action=str(claimed["action"]),
            business_key=str(claimed["business_key"]),
            submission_key=str(claimed["submission_key"]),
            creation_rule_version=str(claimed["creation_rule_version"]),
            generation=int(claimed["generation"]),
            source_event_id=(
                str(claimed["source_event_id"])
                if claimed["source_event_id"] is not None
                else None
            ),
            source_topic=(
                str(claimed["source_topic"])
                if claimed["source_topic"] is not None
                else None
            ),
            source_partition=(
                int(claimed["source_partition"])
                if claimed["source_partition"] is not None
                else None
            ),
            source_offset=(
                int(claimed["source_offset"])
                if claimed["source_offset"] is not None
                else None
            ),
            payload=payload,
            attempt=int(claimed["attempt"]),
            fence=int(claimed["fence"]),
            lease_token=str(claimed["lease_token"]),
            lease_owner=str(claimed["lease_owner"]),
            lease_expires_at=str(claimed["lease_expires_at"]),
            created_at=str(claimed["created_at"]),
            origin_source_id=str(claimed["origin_source_id"] or ""),
        )

    def extend_outbox_lease(
        self,
        *,
        outbox_id: int,
        lease_token: str,
        lease_owner: str,
        lease_seconds: int,
        now: datetime | None = None,
    ) -> str:
        """Extend a live lease only while its activation binding remains eligible."""
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be positive")
        owner = str(lease_owner or "").strip()
        if not owner:
            raise ValueError("lease_owner must not be empty")
        current = _utc_datetime(now)
        current_iso = _iso(current)
        expires_at = _iso(current + timedelta(seconds=lease_seconds))
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            activation_predicate, activation_parameters = (
                self._activation_claim_predicate_tx(conn, alias="o")
            )
            updated = conn.execute(
                f"""
                UPDATE rca_outbox AS o
                   SET lease_expires_at = ?, updated_at = ?
                 WHERE o.outbox_id = ? AND o.status = 'claimed'
                   AND o.lease_token = ? AND o.lease_owner = ?
                   AND o.lease_expires_at IS NOT NULL
                   AND o.lease_expires_at > ?
                   AND ({activation_predicate})
                """,
                (
                    expires_at,
                    current_iso,
                    int(outbox_id),
                    str(lease_token or ""),
                    owner,
                    current_iso,
                    *activation_parameters,
                ),
            )
            if updated.rowcount != 1:
                raise StaleOutboxLeaseError(f"stale lease for outbox {outbox_id}")
            conn.commit()
            return expires_at
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def complete_outbox(
        self,
        *,
        outbox_id: int,
        lease_token: str,
        result: dict[str, Any],
        runtime_identity: Mapping[str, Any] | None = None,
        now: datetime | None = None,
    ) -> OutboxMutationResult:
        """Complete a claimed row only when the caller owns its current token."""
        current = _iso(now)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = self._current_lease_row(conn, outbox_id, lease_token, current)
            updated = conn.execute(
                """
                UPDATE rca_outbox
                   SET status = 'completed', completed_at = ?, result_json = ?,
                       next_attempt_at = NULL, lease_token = NULL,
                       lease_owner = NULL, lease_expires_at = NULL,
                       last_error_code = '', last_error_detail = '', updated_at = ?
                 WHERE outbox_id = ? AND status = 'claimed' AND lease_token = ?
                """,
                (current, _canonical_json(result), current, outbox_id, lease_token),
            )
            if updated.rowcount != 1:
                raise StaleOutboxLeaseError(f"stale lease for outbox {outbox_id}")
            conn.execute(
                """
                UPDATE business_triggers SET state = 'submitted'
                 WHERE business_key = ? AND generation = ?
                """,
                (row["business_key"], row["generation"]),
            )
            if runtime_identity is not None:
                insert_host_runtime_transition(
                    conn,
                    submission_key=str(row["submission_key"]),
                    business_key=str(row["business_key"]),
                    generation=int(row["generation"]),
                    service_label="local.pnc.rca-outbox-dispatcher",
                    transition_kind="outbox_completed",
                    entity_key=str(outbox_id),
                    runtime_identity=runtime_identity,
                    transitioned_at=current,
                )
            conn.commit()
            return OutboxMutationResult(outbox_id, "completed", int(row["attempt"]))
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def retry_outbox(
        self,
        *,
        outbox_id: int,
        lease_token: str,
        error_code: str,
        error_detail: str = "",
        delay_seconds: int,
        max_age_seconds: int = 86_400,
        now: datetime | None = None,
    ) -> OutboxMutationResult:
        """Reschedule a claimed row, or quarantine it after its retry horizon."""
        if delay_seconds < 0:
            raise ValueError("delay_seconds must be non-negative")
        if max_age_seconds < 1:
            raise ValueError("max_age_seconds must be positive")
        current = _utc_datetime(now)
        current_iso = _iso(current)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            mutation = self._retry_outbox_in_transaction(
                conn,
                outbox_id=outbox_id,
                lease_token=lease_token,
                error_code=error_code,
                error_detail=error_detail,
                delay_seconds=delay_seconds,
                max_age_seconds=max_age_seconds,
                current=current,
                current_iso=current_iso,
            )
            conn.commit()
            return mutation
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def retry_outbox_and_open_circuit(
        self,
        *,
        outbox_id: int,
        lease_token: str,
        error_code: str,
        error_detail: str = "",
        delay_seconds: int,
        max_age_seconds: int = 86_400,
        now: datetime | None = None,
    ) -> OutboxMutationResult:
        """Reschedule a fenced row and open the dispatcher circuit atomically."""
        if delay_seconds < 0:
            raise ValueError("delay_seconds must be non-negative")
        if max_age_seconds < 1:
            raise ValueError("max_age_seconds must be positive")
        current = _utc_datetime(now)
        current_iso = _iso(current)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            mutation = self._retry_outbox_in_transaction(
                conn,
                outbox_id=outbox_id,
                lease_token=lease_token,
                error_code=error_code,
                error_detail=error_detail,
                delay_seconds=delay_seconds,
                max_age_seconds=max_age_seconds,
                current=current,
                current_iso=current_iso,
            )
            self._open_dispatcher_circuit_in_transaction(
                conn,
                reason_code=error_code,
                reason_detail=error_detail,
                current=current_iso,
            )
            conn.commit()
            return mutation
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _retry_outbox_in_transaction(
        self,
        conn: sqlite3.Connection,
        *,
        outbox_id: int,
        lease_token: str,
        error_code: str,
        error_detail: str,
        delay_seconds: int,
        max_age_seconds: int,
        current: datetime,
        current_iso: str,
    ) -> OutboxMutationResult:
        row = self._current_lease_row(conn, outbox_id, lease_token, current_iso)
        normalized_error_code = str(error_code or "dispatch_failed")[:120]
        previous_error_code = str(row["last_error_code"] or "").strip()
        entering_input_wait = (
            normalized_error_code in INPUT_WAIT_RETRY_WINDOW_ERROR_CODES
            and previous_error_code not in INPUT_WAIT_RETRY_WINDOW_ERROR_CODES
        )
        restart_window = entering_input_wait
        window_started_at = (
            current
            if restart_window
            else _utc_datetime(
                datetime.fromisoformat(
                    str(row["retry_window_started_at"] or row["created_at"])
                )
            )
        )
        window_started_at_iso = (
            current_iso
            if restart_window
            else str(row["retry_window_started_at"] or row["created_at"])
        )
        deadline = window_started_at + timedelta(seconds=max_age_seconds)
        expired = current >= deadline
        status = "quarantined" if expired else "pending"
        next_attempt_at = (
            None
            if expired
            else _iso(min(current + timedelta(seconds=delay_seconds), deadline))
        )
        updated = conn.execute(
            """
            UPDATE rca_outbox
               SET status = ?, next_attempt_at = ?,
                   quarantined_at = CASE WHEN ? = 'quarantined' THEN ? ELSE NULL END,
                   lease_token = NULL, lease_owner = NULL,
                   lease_expires_at = NULL, last_error_code = ?,
                   last_error_detail = ?, retry_window_started_at = ?, updated_at = ?
             WHERE outbox_id = ? AND status = 'claimed' AND lease_token = ?
            """,
            (
                status,
                next_attempt_at,
                status,
                current_iso,
                normalized_error_code,
                str(error_detail or "")[:1000],
                window_started_at_iso,
                current_iso,
                outbox_id,
                lease_token,
            ),
        )
        if updated.rowcount != 1:
            raise StaleOutboxLeaseError(f"stale lease for outbox {outbox_id}")
        conn.execute(
            """
            UPDATE business_triggers SET state = ?
             WHERE business_key = ? AND generation = ?
            """,
            (status, row["business_key"], row["generation"]),
        )
        return OutboxMutationResult(
            outbox_id,
            status,
            int(row["attempt"]),
            next_attempt_at,
        )

    def quarantine_outbox(
        self,
        *,
        outbox_id: int,
        lease_token: str,
        error_code: str,
        error_detail: str = "",
        now: datetime | None = None,
    ) -> OutboxMutationResult:
        """Permanently quarantine a claimed poison row under its fencing token."""
        current = _iso(now)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = self._current_lease_row(conn, outbox_id, lease_token, current)
            updated = conn.execute(
                """
                UPDATE rca_outbox
                   SET status = 'quarantined', quarantined_at = ?,
                       next_attempt_at = NULL, lease_token = NULL,
                       lease_owner = NULL, lease_expires_at = NULL,
                       last_error_code = ?, last_error_detail = ?, updated_at = ?
                 WHERE outbox_id = ? AND status = 'claimed' AND lease_token = ?
                """,
                (
                    current,
                    str(error_code or "dispatch_quarantined")[:120],
                    str(error_detail or "")[:1000],
                    current,
                    outbox_id,
                    lease_token,
                ),
            )
            if updated.rowcount != 1:
                raise StaleOutboxLeaseError(f"stale lease for outbox {outbox_id}")
            conn.execute(
                """
                UPDATE business_triggers SET state = 'quarantined'
                 WHERE business_key = ? AND generation = ?
                """,
                (row["business_key"], row["generation"]),
            )
            conn.commit()
            return OutboxMutationResult(outbox_id, "quarantined", int(row["attempt"]))
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    @staticmethod
    def _current_lease_row(
        conn: sqlite3.Connection,
        outbox_id: int,
        lease_token: str,
        current: str,
    ) -> sqlite3.Row:
        row = conn.execute(
            """
            SELECT * FROM rca_outbox
             WHERE outbox_id = ? AND status = 'claimed' AND lease_token = ?
               AND lease_expires_at IS NOT NULL AND lease_expires_at > ?
            """,
            (int(outbox_id), str(lease_token or ""), current),
        ).fetchone()
        if row is None:
            raise StaleOutboxLeaseError(f"stale lease for outbox {outbox_id}")
        return row

    def preview_dispatchable(
        self,
        *,
        limit: int = 20,
        submission_key: str | None = None,
        now: datetime | None = None,
    ) -> list[dict[str, Any]]:
        """Read due rows without claiming or mutating them (used by dry-run)."""
        if limit < 1:
            return []
        target_submission_key = None
        if submission_key is not None:
            target_submission_key = str(submission_key).strip()
            if (
                not target_submission_key
                or len(target_submission_key) > 256
                or any(ord(char) < 0x21 for char in target_submission_key)
            ):
                raise ValueError("submission_key filter is invalid")
        current = _iso(now)
        conn = self._connect()
        try:
            conn.execute("BEGIN")
            activation_predicate, activation_parameters = (
                self._activation_claim_predicate_tx(conn)
            )
            rows = conn.execute(
                f"""
                SELECT o.outbox_id, o.action, o.submission_key, o.source_event_id,
                       o.source_topic, o.source_partition, o.source_offset, o.attempt,
                       o.status, o.next_attempt_at, o.lease_expires_at, o.created_at
                 FROM rca_outbox AS o
                 WHERE ({activation_predicate})
                   AND (? IS NULL OR o.submission_key = ?)
                   AND ((
                        o.status = 'pending'
                        AND (o.next_attempt_at IS NULL OR o.next_attempt_at <= ?)
                       )
                    OR (
                        o.status = 'claimed'
                        AND o.lease_expires_at IS NOT NULL
                        AND o.lease_expires_at <= ?
                       ))
                 ORDER BY COALESCE(o.next_attempt_at, o.created_at), o.outbox_id
                 LIMIT ?
                """,
                (
                    *activation_parameters,
                    target_submission_key,
                    target_submission_key,
                    current,
                    current,
                    limit,
                ),
            ).fetchall()
            value = [dict(row) for row in rows]
            conn.commit()
            return value
        except Exception:
            if conn.in_transaction:
                conn.rollback()
            raise
        finally:
            conn.close()

    def dispatcher_circuit(self) -> DispatcherCircuit:
        conn = self._connect()
        try:
            row = conn.execute(
                """
                SELECT state, reason_code, reason_detail, opened_at, updated_at
                  FROM rca_dispatcher_circuit WHERE circuit_name = 'submission'
                """
            ).fetchone()
            if row is None:
                return DispatcherCircuit(state="open", reason_code="circuit_state_missing")
            return DispatcherCircuit(**dict(row))
        finally:
            conn.close()

    def open_dispatcher_circuit(
        self,
        *,
        reason_code: str,
        reason_detail: str = "",
        now: datetime | None = None,
    ) -> DispatcherCircuit:
        current = _iso(now)
        conn = self._connect()
        try:
            self._open_dispatcher_circuit_in_transaction(
                conn,
                reason_code=reason_code,
                reason_detail=reason_detail,
                current=current,
            )
        finally:
            conn.close()
        return self.dispatcher_circuit()

    @staticmethod
    def _open_dispatcher_circuit_in_transaction(
        conn: sqlite3.Connection,
        *,
        reason_code: str,
        reason_detail: str,
        current: str,
    ) -> None:
        conn.execute(
            """
            INSERT INTO rca_dispatcher_circuit(
                circuit_name, state, reason_code, reason_detail,
                opened_at, updated_at
            ) VALUES('submission', 'open', ?, ?, ?, ?)
            ON CONFLICT(circuit_name) DO UPDATE SET
                state = 'open', reason_code = excluded.reason_code,
                reason_detail = excluded.reason_detail,
                opened_at = COALESCE(
                    rca_dispatcher_circuit.opened_at,
                    excluded.opened_at
                ),
                updated_at = excluded.updated_at
            """,
            (
                str(reason_code or "dispatcher_system_error")[:120],
                str(reason_detail or "")[:1000],
                current,
                current,
            ),
        )

    def close_dispatcher_circuit(
        self,
        *,
        now: datetime | None = None,
    ) -> DispatcherCircuit:
        """Explicit operator reset; dispatch never closes a circuit automatically."""
        current = _iso(now)
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO rca_dispatcher_circuit(
                    circuit_name, state, updated_at
                ) VALUES('submission', 'closed', ?)
                ON CONFLICT(circuit_name) DO UPDATE SET
                    state = 'closed', reason_code = '', reason_detail = '',
                    opened_at = NULL, updated_at = excluded.updated_at
                """,
                (current,),
            )
        finally:
            conn.close()
        return self.dispatcher_circuit()

    def close_dispatcher_circuit_with_audit(
        self,
        *,
        audit: Mapping[str, Any],
        now: datetime | None = None,
    ) -> tuple[DispatcherCircuit, DispatcherCircuit]:
        """Close the submission circuit and persist its operator receipt atomically.

        The receipt is stored in ``control_meta`` under a create-once key in the
        same transaction as the circuit transition.  The CLI may materialize
        that value to a filesystem receipt afterwards; the database entry is
        the authoritative recovery record if that materialization is interrupted.
        """
        payload = _validate_dispatcher_circuit_reset_audit(audit)
        reset_id = str(payload.get("reset_id") or "").strip()
        if not reset_id or any(char in reset_id for char in "\n\r\x00"):
            raise ValueError("dispatcher_circuit_reset_id_invalid")
        if len(reset_id) > 200:
            raise ValueError("dispatcher_circuit_reset_id_invalid")
        try:
            db_identity = self.db_path.expanduser().absolute().lstat()
        except OSError as exc:
            raise RuntimeError("dispatcher_circuit_reset_control_db_missing") from exc
        recorded_identity = payload["control_db_identity"]
        if (
            recorded_identity.get("path")
            != str(self.db_path.expanduser().absolute())
            or int(recorded_identity.get("device")) != int(db_identity.st_dev)
            or int(recorded_identity.get("inode")) != int(db_identity.st_ino)
        ):
            raise RuntimeError("dispatcher_circuit_reset_control_db_mismatch")
        before_value = payload.get("before")
        after_value = payload.get("after")
        if not isinstance(before_value, Mapping) or not isinstance(
            after_value, Mapping
        ):
            raise ValueError("dispatcher_circuit_reset_state_invalid")
        current = _iso(now)
        expected_before = DispatcherCircuit(
            state=str(before_value.get("state") or ""),
            reason_code=str(before_value.get("reason_code") or ""),
            reason_detail=str(before_value.get("reason_detail") or ""),
            opened_at=(
                str(before_value["opened_at"])
                if before_value.get("opened_at") is not None
                else None
            ),
            updated_at=(
                str(before_value["updated_at"])
                if before_value.get("updated_at") is not None
                else None
            ),
        )
        expected_after = DispatcherCircuit(
            state=str(after_value.get("state") or ""),
            reason_code=str(after_value.get("reason_code") or ""),
            reason_detail=str(after_value.get("reason_detail") or ""),
            opened_at=(
                str(after_value["opened_at"])
                if after_value.get("opened_at") is not None
                else None
            ),
            updated_at=(
                str(after_value["updated_at"])
                if after_value.get("updated_at") is not None
                else None
            ),
        )
        if expected_before.state != "open":
            raise RuntimeError("dispatcher_circuit_reset_requires_open_circuit")
        if expected_after != DispatcherCircuit(
            state="closed", updated_at=current
        ):
            raise ValueError("dispatcher_circuit_reset_post_state_invalid")
        payload = dict(payload)
        payload["before"] = {
            "state": expected_before.state,
            "reason_code": expected_before.reason_code,
            "reason_detail": expected_before.reason_detail,
            "opened_at": expected_before.opened_at,
            "updated_at": expected_before.updated_at,
        }
        payload["after"] = {
            "state": expected_after.state,
            "reason_code": expected_after.reason_code,
            "reason_detail": expected_after.reason_detail,
            "opened_at": expected_after.opened_at,
            "updated_at": expected_after.updated_at,
        }
        serialized = _canonical_json(payload)
        meta_key = f"{OUTBOX_CIRCUIT_RESET_META_PREFIX}{reset_id}"
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """
                SELECT state, reason_code, reason_detail, opened_at, updated_at
                  FROM rca_dispatcher_circuit
                 WHERE circuit_name = 'submission'
                """
            ).fetchone()
            if row is None:
                raise RuntimeError("dispatcher_circuit_reset_state_missing")
            observed_before = DispatcherCircuit(**dict(row))
            if observed_before != expected_before:
                raise RuntimeError("dispatcher_circuit_reset_state_changed")
            conn.execute(
                "INSERT INTO control_meta(key, value) VALUES(?, ?)",
                (meta_key, serialized),
            )
            updated = conn.execute(
                """
                UPDATE rca_dispatcher_circuit
                   SET state = 'closed', reason_code = '', reason_detail = '',
                       opened_at = NULL, updated_at = ?
                 WHERE circuit_name = 'submission'
                   AND state = 'open'
                   AND updated_at = ?
                """,
                (current, expected_before.updated_at),
            )
            if updated.rowcount != 1:
                raise RuntimeError("dispatcher_circuit_reset_state_changed")
            row = conn.execute(
                """
                SELECT state, reason_code, reason_detail, opened_at, updated_at
                  FROM rca_dispatcher_circuit
                 WHERE circuit_name = 'submission'
                """
            ).fetchone()
            if row is None:
                raise RuntimeError("dispatcher_circuit_reset_post_state_missing")
            observed_after = DispatcherCircuit(**dict(row))
            if observed_after != expected_after:
                raise RuntimeError("dispatcher_circuit_reset_post_state_changed")
            conn.commit()
        except Exception:
            if conn.in_transaction:
                conn.rollback()
            raise
        finally:
            conn.close()
        return observed_before, observed_after

    def dispatcher_circuit_reset_audit(
        self, reset_id: str
    ) -> dict[str, Any] | None:
        """Read an operator circuit-reset receipt from the durable metadata log."""
        normalized = str(reset_id or "").strip()
        if not normalized:
            raise ValueError("dispatcher_circuit_reset_id_invalid")
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT value FROM control_meta WHERE key = ?",
                (f"{OUTBOX_CIRCUIT_RESET_META_PREFIX}{normalized}",),
            ).fetchone()
            if row is None:
                return None
            raw = str(row["value"])
            value = json.loads(
                raw,
                parse_constant=_reject_dispatcher_reset_json_constant,
            )
            value = _validate_dispatcher_circuit_reset_audit(value)
            if (
                value.get("reset_id") != normalized
                or _canonical_json(value) != raw
            ):
                raise RuntimeError("dispatcher_circuit_reset_audit_tampered")
            try:
                observed_db = self.db_path.expanduser().absolute().lstat()
            except OSError as exc:
                raise RuntimeError("dispatcher_circuit_reset_control_db_missing") from exc
            identity = value["control_db_identity"]
            if (
                identity.get("path")
                != str(self.db_path.expanduser().absolute())
                or int(identity.get("device")) != int(observed_db.st_dev)
                or int(identity.get("inode")) != int(observed_db.st_ino)
            ):
                raise RuntimeError("dispatcher_circuit_reset_control_db_mismatch")
            return value
        except (json.JSONDecodeError, ValueError) as exc:
            raise RuntimeError("dispatcher_circuit_reset_audit_invalid") from exc
        finally:
            conn.close()

    def get_inbox(self, event_uid: str) -> dict[str, Any] | None:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM kafka_inbox WHERE event_uid = ?", (event_uid,)
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    @staticmethod
    def _prune_replay_raw_tx(
        conn: sqlite3.Connection,
        *,
        now: datetime,
        limit: int = REPLAY_RAW_PRUNE_BATCH,
    ) -> int:
        replay_cutoff = _iso(_utc_datetime(now) - REPLAY_RAW_RETENTION)
        processed_cutoff = _iso(_utc_datetime(now) - PROCESSED_RAW_RETENTION)
        current = _iso(now)
        result = conn.execute(
            """
            UPDATE kafka_inbox
               SET raw_value = X'', raw_pruned_at = ?
             WHERE event_uid IN (
                SELECT event_uid FROM kafka_inbox
                 WHERE length(raw_value) > 0
                   AND (
                        (decision IN ('filtered', 'deduped') AND processed_at < ?)
                        OR
                        (decision IN ('accepted', 'invalid') AND processed_at < ?)
                   )
                 ORDER BY processed_at, event_uid
                 LIMIT ?
             )
            """,
            (current, replay_cutoff, processed_cutoff, max(1, int(limit))),
        )
        return int(result.rowcount)

    def prune_replay_raw(self, *, now: datetime | None = None) -> int:
        """Expire replay-only raw bodies after the controlled retention window."""
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            count = self._prune_replay_raw_tx(conn, now=_utc_datetime(now))
            conn.commit()
            return count
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def list_rows(self, table: str) -> list[dict[str, Any]]:
        allowed = {
            "kafka_inbox",
            "business_triggers",
            "kafka_partition_progress",
            "rca_outbox",
            "kafka_dead_letters",
            "rca_dispatcher_circuit",
            "rca_shadow_promotion_audit",
            "rca_outbox_rearm_audit",
            "rca_policy_snapshots",
            "rca_trigger_sources",
            "rca_trigger_bindings",
            "rca_delivery_subscriptions",
            "rca_trigger_delivery_bindings",
            "rca_host_runtime_transitions",
            "rca_activation_epochs",
            "rca_activation_admission_ledger",
            "rca_activation_transition_audit",
            "rca_canonical_requests",
            "rca_admission_snapshots",
            "rca_source_authority_receipts",
            "rca_snapshot_source_envelopes",
            "rca_learning_lane_cohorts",
            "rca_learning_lane_stock_items",
            "rca_learning_lane_admissions",
            "rca_terminal_rerun_delivery_authorities",
            "rca_historical_epoch_rerun_delivery_authorities",
        }
        if table not in allowed:
            raise ValueError(f"unsupported table: {table}")
        conn = self._connect()
        try:
            rows = conn.execute(f"SELECT * FROM {table} ORDER BY rowid").fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def dispatch_backlog_count(self) -> int:
        """Count current steady work that can pressure submission."""
        conn = self._connect()
        try:
            conn.execute("BEGIN")
            predicate, parameters = self._activation_claim_predicate_tx(conn)
            count = int(
                conn.execute(
                    f"""
                    SELECT COUNT(*) FROM rca_outbox AS o
                     WHERE o.status IN ('pending', 'claimed')
                       AND ({predicate})
                    """,
                    parameters,
                ).fetchone()[0]
            )
            conn.commit()
            return count
        except Exception:
            if conn.in_transaction:
                conn.rollback()
            raise
        finally:
            conn.close()

    def health(self) -> dict[str, Any]:
        conn = self._connect()
        try:
            conn.execute("BEGIN")
            snapshot_at = _now_iso()
            current_epoch = self._current_activation_epoch_unchecked_tx(conn)
            activation_binding_valid = False
            activation_current = None
            if current_epoch is not None:
                try:
                    activation_schema_version = self._activation_schema_version_tx(conn)
                    self._activation_transition_binding_tx(
                        conn,
                        epoch=current_epoch,
                        schema_version=activation_schema_version,
                    )
                except ActivationEpochError:
                    activation_predicate, activation_parameters = "0", ()
                else:
                    activation_binding_valid = True
                    activation_current = self._public_activation_epoch(
                        current_epoch,
                        schema_version=activation_schema_version,
                    )
                    activation_predicate, activation_parameters = (
                        self._activation_claim_predicate_tx(conn)
                    )
            else:
                activation_predicate, activation_parameters = (
                    self._activation_claim_predicate_tx(conn)
                )
            inbox = {
                row["decision"]: int(row["count"])
                for row in conn.execute(
                    "SELECT decision, COUNT(*) AS count FROM kafka_inbox GROUP BY decision"
                ).fetchall()
            }
            outbox = {
                row["status"]: int(row["count"])
                for row in conn.execute(
                    "SELECT status, COUNT(*) AS count FROM rca_outbox GROUP BY status"
                ).fetchall()
            }
            oldest_pending = conn.execute(
                "SELECT MIN(received_at) FROM kafka_inbox WHERE decision = 'pending'"
            ).fetchone()[0]
            oldest_dispatchable = conn.execute(
                f"""
                SELECT MIN(COALESCE(o.retry_window_started_at, o.created_at))
                  FROM rca_outbox AS o
                 WHERE o.status IN ('pending', 'claimed')
                   AND ({activation_predicate})
                """,
                activation_parameters,
            ).fetchone()[0]
            expired_leases = int(
                conn.execute(
                    f"""
                    SELECT COUNT(*) FROM rca_outbox AS o
                     WHERE o.status = 'claimed' AND o.lease_expires_at <= ?
                       AND ({activation_predicate})
                    """,
                    (snapshot_at, *activation_parameters),
                ).fetchone()[0]
            )
            dispatchable_backlog = int(
                conn.execute(
                    f"""
                    SELECT COUNT(*) FROM rca_outbox AS o
                     WHERE o.status IN ('pending', 'claimed')
                       AND ({activation_predicate})
                    """,
                    activation_parameters,
                ).fetchone()[0]
            )
            circuit_row = conn.execute(
                """
                SELECT state, reason_code, reason_detail, opened_at, updated_at
                  FROM rca_dispatcher_circuit WHERE circuit_name = 'submission'
                """
            ).fetchone()
            rearm_count = int(
                conn.execute("SELECT COUNT(*) FROM rca_outbox_rearm_audit").fetchone()[0]
            )
            replay_raw = conn.execute(
                """
                SELECT COUNT(*) AS count, COALESCE(SUM(length(raw_value)), 0) AS bytes
                  FROM kafka_inbox
                 WHERE decision IN ('filtered', 'deduped') AND length(raw_value) > 0
                """
            ).fetchone()
            processed_raw = conn.execute(
                """
                SELECT COUNT(*) AS count, COALESCE(SUM(length(raw_value)), 0) AS bytes
                  FROM kafka_inbox
                 WHERE decision IN ('accepted', 'invalid') AND length(raw_value) > 0
                """
            ).fetchone()
            pruned_raw_count = int(
                conn.execute(
                    "SELECT COUNT(*) FROM kafka_inbox WHERE raw_pruned_at IS NOT NULL"
                ).fetchone()[0]
            )
            activation_ledger = {
                "admit": 0,
                "join": 0,
                "shadow": 0,
                "reject": 0,
            }
            pending_inbox = 0
            unbound_admissions = 0
            if current_epoch is not None:
                epoch_id = str(current_epoch["epoch_id"])
                for row in conn.execute(
                    """
                    SELECT decision, COUNT(*) AS count
                      FROM rca_activation_admission_ledger
                     WHERE epoch_id = ? GROUP BY decision
                    """,
                    (epoch_id,),
                ).fetchall():
                    activation_ledger[str(row["decision"])] = int(row["count"])
                pending_inbox = int(
                    conn.execute(
                        """
                        SELECT COUNT(*) FROM kafka_inbox
                         WHERE decision = 'pending' AND activation_epoch_id = ?
                        """,
                        (epoch_id,),
                    ).fetchone()[0]
                )
                unbound_admissions = int(
                    conn.execute(
                        """
                        SELECT COUNT(*)
                          FROM rca_activation_admission_ledger AS al
                         WHERE al.epoch_id = ? AND al.decision = 'admit'
                           AND (
                               al.bound_at IS NULL
                            OR NOT EXISTS (
                                SELECT 1 FROM business_triggers AS bt
                                 WHERE bt.activation_epoch_id = al.epoch_id
                                   AND bt.activation_ledger_id = al.ledger_id
                                   AND bt.business_key = al.business_key
                                   AND bt.submission_key = al.submission_key
                                   AND bt.generation = al.generation
                            )
                            OR NOT EXISTS (
                                SELECT 1 FROM rca_outbox AS o
                                 WHERE o.activation_epoch_id = al.epoch_id
                                   AND o.activation_ledger_id = al.ledger_id
                                   AND o.business_key = al.business_key
                                   AND o.submission_key = al.submission_key
                                   AND o.generation = al.generation
                            )
                           )
                        """,
                        (epoch_id,),
                    ).fetchone()[0]
                )
            sqlite_data_version = int(conn.execute("PRAGMA data_version").fetchone()[0])
            conn.commit()

            db_files = {
                "main": self.db_path,
                "wal": Path(f"{self.db_path}-wal"),
                "shm": Path(f"{self.db_path}-shm"),
            }
            db_file_sizes: dict[str, int] = {}
            db_size_error = ""
            try:
                db_file_sizes = {
                    name: path.stat().st_size if path.exists() else 0
                    for name, path in db_files.items()
                }
            except OSError as exc:
                db_size_error = f"{type(exc).__name__}: {exc}"
            filesystem: dict[str, Any] = {
                "path": str(self.db_path.parent),
                "total_bytes": None,
                "free_bytes": None,
                "available_bytes": None,
                "error": "",
            }
            try:
                statvfs = os.statvfs(self.db_path.parent)
                filesystem.update(
                    {
                        "total_bytes": int(statvfs.f_blocks * statvfs.f_frsize),
                        "free_bytes": int(statvfs.f_bfree * statvfs.f_frsize),
                        "available_bytes": int(statvfs.f_bavail * statvfs.f_frsize),
                    }
                )
            except OSError as exc:
                filesystem["error"] = f"{type(exc).__name__}: {exc}"
            capacity_ok = (
                not filesystem["error"]
                and isinstance(filesystem["available_bytes"], int)
                and filesystem["available_bytes"] >= CONTROL_DB_MIN_AVAILABLE_BYTES
            )
            activation_ok = current_epoch is None or activation_binding_valid
            schema_runtime_capability = self.schema_runtime_capability()
            process_healthy = capacity_ok and activation_ok
            business_ready = bool(
                process_healthy
                and schema_runtime_capability["write_enabled"]
                and schema_runtime_capability["work_admission_enabled"]
                and current_epoch is not None
                and activation_binding_valid
                and str(current_epoch["state"]) == "steady_active"
            )
            return {
                "ok": process_healthy,
                "process_healthy": process_healthy,
                "business_ready": business_ready,
                "schema_runtime_capability": schema_runtime_capability,
                "schema_version": self._observed_schema_version,
                "db_path": str(self.db_path),
                "snapshot_at": snapshot_at,
                "sqlite_data_version": sqlite_data_version,
                "inbox": inbox,
                "outbox": outbox,
                "oldest_pending_received_at": oldest_pending,
                "oldest_dispatchable_created_at": oldest_dispatchable,
                "expired_outbox_leases": expired_leases,
                "dispatcher_circuit": dict(circuit_row) if circuit_row else None,
                "input_wait_rearms": rearm_count,
                "replay_raw_retention_days": REPLAY_RAW_RETENTION.days,
                "replay_raw_retained_count": int(replay_raw["count"]),
                "replay_raw_retained_bytes": int(replay_raw["bytes"]),
                "processed_raw_retention_days": PROCESSED_RAW_RETENTION.days,
                "processed_raw_retained_count": int(processed_raw["count"]),
                "processed_raw_retained_bytes": int(processed_raw["bytes"]),
                "raw_pruned_count": pruned_raw_count,
                "activation": {
                    "mode": "steady_only",
                    "configured": current_epoch is not None,
                    "binding_valid": activation_binding_valid,
                    "production_active": business_ready,
                    "current_epoch": activation_current,
                    "ledger": activation_ledger,
                    "dispatchable_backlog": dispatchable_backlog,
                    "pending_inbox": pending_inbox,
                    "unbound_admissions": unbound_admissions,
                },
                "db_size_bytes": sum(db_file_sizes.values()),
                "db_file_sizes": db_file_sizes,
                "db_size_error": db_size_error,
                "filesystem": filesystem,
                "minimum_available_bytes": CONTROL_DB_MIN_AVAILABLE_BYTES,
            }
        except Exception:
            if conn.in_transaction:
                conn.rollback()
            raise
        finally:
            conn.close()
