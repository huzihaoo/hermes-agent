"""Durable SQLite inbox, trigger, outbox, and DLQ for Kafka-driven PNC RCA."""

from __future__ import annotations

import base64
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
import plistlib
from pathlib import Path
import re
import sqlite3
import stat
import subprocess
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
from gateway.pnc_rca_gray_samples import (
    GRAY_SAMPLE_DAILY_LIMIT,
    GRAY_SAMPLE_REQUESTER_ID,
    build_gray_sample_message_id,
    build_gray_sample_reason,
    gray_sample_issue_url,
    normalize_gray_sample_automation_authority,
)
from gateway.pnc_rca_runtime_transition import (
    ensure_host_runtime_transition_schema,
    insert_host_runtime_transition,
    validate_host_runtime_transition_schema,
)
from gateway.pnc_rca_requester_identity import validate_rca_requester


CONTROL_STORE_SCHEMA_VERSION = "pnc_rca_control_store_v13"
CONTROL_STORE_SCHEMA_PREDECESSOR_VERSION = "pnc_rca_control_store_v12"
SUPPORTED_CONTROL_STORE_SCHEMA_VERSIONS = frozenset(
    {
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
        CONTROL_STORE_SCHEMA_VERSION,
    }
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
EXACT_OUTBOX_HOLD_META_PREFIX = "rca_exact_outbox_hold:"
EXACT_OUTBOX_HOLD_SCHEMA_VERSION = "pnc_rca_exact_outbox_hold_v1"
EXACT_OUTBOX_HOLD_ROW_SCHEMA_VERSION = "pnc_rca_exact_outbox_hold_row_v1"
EXACT_OUTBOX_HOLD_SNAPSHOT_SCHEMA_VERSION = "pnc_rca_exact_outbox_hold_snapshot_v1"
EXACT_OUTBOX_HOLD_UNTIL = "9999-12-31T23:59:59.999999+00:00"
EXACT_OUTBOX_HOLD_MAX_AUDIT_BYTES = 512 * 1024
# A hold is an operational gate for a predecessor canary, not a generic retry
# delay.  Keep a fixed margin for the canary, collector, and manual review.
EXACT_OUTBOX_HOLD_MIN_REMAINING_SECONDS = 6 * 60 * 60
EXACT_OUTBOX_HOLD_RECORD_MAX_AGE_SECONDS = 60
EXACT_OUTBOX_HOLD_MAX_FUTURE_SKEW_SECONDS = 5
EXACT_OUTBOX_RUNTIME_PLIST_LABELS = (
    "local.pnc.rca-kafka-consumer",
    "local.pnc.rca-outbox-dispatcher",
    "local.pnc.rca-delivery-collector",
    "local.pnc.rca-delivery-dispatcher",
    "local.pnc.completion-notice-relay",
    "local.pnc.feishu-delivery-repair",
)
EXACT_OUTBOX_CONFIG_KEYS = frozenset(
    {
        "dispatch_enabled",
        "activation_required",
        "control_db_path",
        "delivery_db_path",
        "health_path",
        "service_id",
        "service_capability",
        "service_operation",
        "lease_seconds",
        "max_age_seconds",
        "input_wait_max_age_seconds",
        "poll_interval_seconds",
        "circuit_poll_interval_seconds",
        "batch_size",
        "data_access_mode",
        "allow_download",
        "allow_feishu_writeback",
        "group_response_cap",
        "translate_baseline",
        "translate_contract_path",
        "storage_admission_enabled",
        "storage_reservation_enabled",
        "derived_capacity_reservation_enabled",
        "delivery_backpressure_enabled",
        "delivery_high_watermark",
        "delivery_resume_watermark",
        "storage_concurrency_reserve_cases",
        "storage_cases_per_day",
        "storage_capacity_scope",
        "derived_capacity_atomic_reservation",
        "storage_expected_artifact_cache_bytes",
        "storage_reserve_percent",
        "storage_timeout_seconds",
        "derived_capacity_reservation_timeout_seconds",
        "capacity_mode",
        "release_id",
        "bootstrap_epoch_id",
        "active_release_binding_path",
        "live_env_path",
        "w3_snapshot_read",
    }
)
EXACT_OUTBOX_HOLD_REQUIRED_FIELDS = frozenset(
    {
        "schema_version",
        "command",
        "phase",
        "hold_id",
        "plan_id",
        "recorded_at",
        "operator",
        "reason",
        "target_outbox_id",
        "predecessor_outbox_id",
        "control_db_identity",
        "activation_required",
        "max_age_seconds",
        "active_activation",
        "active_release_binding",
        "config_binding",
        "config_binding_sha256",
        "tool_provenance",
        "tool_provenance_sha256",
        "resident_census",
        "destination_path",
        "destination_binding",
        "target_before",
        "target_after",
        "predecessor",
        "eligible_queue_before",
        "eligible_queue_after",
        "retry_horizon",
        "effect_delta",
        "receipt_fingerprint",
    }
)
EXACT_OUTBOX_HOLD_CONTROL_IDENTITY_FIELDS = frozenset(
    {
        "schema_version",
        "path",
        "present",
        "device",
        "inode",
        "size",
        "mtime_ns",
        "sha256",
        "wal",
        "shm",
        "logical_db_identity",
        "coordination_observation",
    }
)
EXACT_OUTBOX_HOLD_SOURCE_FILE_FIELDS = frozenset(
    {"present", "device", "inode", "size", "mtime_ns", "sha256"}
)
EXACT_OUTBOX_HOLD_LOGICAL_DB_FIELDS = frozenset({"database", "wal"})
EXACT_OUTBOX_HOLD_COORDINATION_FIELDS = frozenset({"shm"})
EXACT_OUTBOX_HOLD_W3_READ_DISABLED_FIELDS = frozenset({"enabled", "mode"})
EXACT_OUTBOX_HOLD_W3_READ_ENABLED_FIELDS = frozenset(
    {"enabled", "mode", "schema_version", "authority_sha256", "policy_sha256s"}
)
EXACT_OUTBOX_HOLD_W3_POLICY_FIELDS = frozenset(
    {
        "creation_policy",
        "business_profile",
        "execution_policy",
        "publication_policy",
        "correction_lineage_policy",
    }
)
EXACT_OUTBOX_HOLD_ACTIVE_BINDING_FIELDS = frozenset(
    {
        "path",
        "sha256",
        "release_id",
        "authority_sha256",
        "authority_epoch_id",
        "bootstrap_epoch_id",
        "release_bom_sha256",
        "candidate_env_sha256",
        "authorization_fingerprint",
        "authorization_receipt_sha256",
        "approval_evidence_sha256",
        "runtime_manifest_sha256",
        "runtime_release_target",
        "runtime_git_head",
        "runtime_git_tree",
        "raw_sha256",
        "live_env_path",
        "live_env_sha256",
    }
)
EXACT_OUTBOX_HOLD_TOOL_FIELDS = frozenset(
    {
        "entrypoint_path",
        "entrypoint_sha256",
        "control_store_path",
        "control_store_sha256",
        "bootstrap_path",
        "bootstrap_sha256",
        "git_head",
        "git_tree",
        "git_status_returncode",
        "git_clean",
        "runtime_provenance",
    }
)
EXACT_OUTBOX_HOLD_RUNTIME_FIELDS = frozenset(
    {
        "schema_version",
        "manifest",
        "manifest_runtime_root",
        "manifest_runtime_release_target",
        "manifest_gateway_release_target",
        "manifest_commit",
        "manifest_tree",
        "runtime_git_head",
        "runtime_git_tree",
        "release_bom_sha256",
        "plists",
        "stable_target_registry",
    }
)
EXACT_OUTBOX_HOLD_RUNTIME_FILE_FIELDS = frozenset(
    {"present", "path", "sha256", "mode", "uid", "nlink"}
)
EXACT_OUTBOX_HOLD_RUNTIME_PLIST_FIELDS = frozenset(
    {"present", "label", "path", "sha256", "mode", "uid", "nlink"}
)
EXACT_OUTBOX_HOLD_RESIDENT_FIELDS = frozenset(
    {
        "schema_version",
        "observed_at",
        "forbidden_labels",
        "observations",
        "loaded_labels",
        "loaded_count",
        "all_unloaded",
        "source_kind",
        "domain",
        "active_release_binding_path",
        "source_sha256",
    }
)
EXACT_OUTBOX_HOLD_RESIDENT_OBSERVATION_FIELDS = frozenset(
    {"label", "loaded", "returncode", "unloaded_proven", "output_sha256"}
)
EXACT_OUTBOX_HOLD_ACTIVATION_FIELDS = frozenset(
    {
        "configured",
        "epoch_id",
        "state",
        "is_current",
        "config_sha256",
        "db_logical_identity_sha256",
        "db_logical_identity",
        "preproduction_fingerprint",
        "preproduction_gate_receipt_sha256",
        "production_fingerprint",
        "production_gate_receipt_sha256",
        "sha256",
    }
)
EXACT_OUTBOX_HOLD_ACTIVATION_DB_FIELDS = frozenset(
    {"device", "inode", "logical_store_id"}
)
EXACT_OUTBOX_HOLD_ROW_FIELDS = frozenset(
    {
        "schema_version",
        "outbox_id",
        "submission_key",
        "business_key",
        "generation",
        "status",
        "attempt",
        "fence",
        "next_attempt_at",
        "retry_window_started_at",
        "lease_token",
        "lease_owner",
        "lease_expires_at",
        "claimed_at",
        "completed_at",
        "quarantined_at",
        "result_json",
        "updated_at",
        "activation_epoch_id",
        "activation_ledger_id",
        "row_sha256",
        "created_at",
    }
)
EXACT_OUTBOX_HOLD_QUEUE_FIELDS = frozenset(
    {"outbox_ids", "entries", "sha256"}
)
EXACT_OUTBOX_HOLD_QUEUE_ENTRY_FIELDS = frozenset({"outbox_id", "row_sha256"})
EXACT_OUTBOX_HOLD_RETRY_FIELDS = frozenset(
    {
        "target_outbox_id",
        "anchor",
        "expires_at",
        "min_remaining_seconds",
        "safety_headroom_seconds",
        "record_max_age_seconds",
        "plan_remaining_seconds",
        "apply_observed_at",
        "apply_remaining_seconds",
    }
)
EXACT_OUTBOX_HOLD_EFFECT_FIELDS = frozenset(
    {
        "external_writes",
        "external_effects_triggered",
        "target_rows_updated",
        "control_meta_inserted",
        "business_trigger_rows_updated",
        "mutation",
    }
)
EXACT_OUTBOX_HOLD_EFFECT_MUTATION_FIELDS = frozenset(
    {"next_attempt_at", "updated_at"}
)
DEFAULT_MANUAL_OPERATOR_RATE_LIMIT = 3
DEFAULT_MANUAL_OPERATOR_RATE_WINDOW_SECONDS = 600
GROUP_USER_RERUN_SCHEMA_VERSION = "pnc_rca_group_user_rerun_v1"
GROUP_USER_RERUN_DEDUPE_SECONDS = 600
REPLAY_RAW_RETENTION = timedelta(days=7)
PROCESSED_RAW_RETENTION = timedelta(days=30)
REPLAY_RAW_PRUNE_BATCH = 1000
INPUT_WAIT_QUARANTINE_REARMED_REASON = "input_wait_quarantine_rearmed"
INPUT_WAIT_TERMINAL_NEW_GENERATION_REASON = (
    "input_wait_terminal_new_generation_created"
)
INPUT_WAIT_EXECUTION_WATCH_PRESENT_REASON = "input_wait_execution_watch_present"
MANUAL_SHADOW_PROMOTED_REASON = "manual_shadow_promoted"
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
ACTIVATION_HISTORICAL_OUTBOX_HOLD_SCHEMA_VERSION = (
    "pnc_rca_activation_historical_outbox_hold_v1"
)
ACTIVATION_HISTORICAL_OUTBOX_ROW_SCHEMA_VERSION = (
    "pnc_rca_activation_historical_outbox_row_v1"
)
ACTIVATION_HISTORICAL_OUTBOX_DISPOSITION_SCHEMA_VERSION = (
    "pnc_rca_activation_historical_outbox_disposition_v1"
)
ACTIVATION_HISTORICAL_OUTBOX_DISPOSITION_ERROR_CODE = (
    "activation_historical_hold_owner_disposed"
)
ACTIVATION_HISTORICAL_OUTBOX_ROW_FIELDS = (
    "outbox_id",
    "action",
    "business_key",
    "submission_key",
    "creation_rule_version",
    "generation",
    "activation_epoch_id",
    "activation_ledger_id",
    "origin_source_id",
    "source_event_id",
    "source_topic",
    "source_partition",
    "source_offset",
    "payload_json",
    "status",
    "attempt",
    "next_attempt_at",
    "fence",
    "lease_token",
    "lease_owner",
    "lease_expires_at",
    "claimed_at",
    "completed_at",
    "quarantined_at",
    "last_error_code",
    "last_error_detail",
    "result_json",
    "retry_window_started_at",
    "created_at",
    "updated_at",
)
ACTIVATION_HISTORICAL_OUTBOX_DISPOSITION_MUTABLE_FIELDS = frozenset(
    {
        "status",
        "next_attempt_at",
        "quarantined_at",
        "last_error_code",
        "last_error_detail",
        "updated_at",
    }
)
ACTIVATION_HISTORICAL_OUTBOX_IMMUTABLE_ROW_FIELDS = tuple(
    field
    for field in ACTIVATION_HISTORICAL_OUTBOX_ROW_FIELDS
    if field not in ACTIVATION_HISTORICAL_OUTBOX_DISPOSITION_MUTABLE_FIELDS
)
# The W6 stock boundary is a release contract, not an operator-controlled flag.
STOCK_CUTOFF = "2026-07-25T10:15:43.473251+00:00"
LEARNING_LANE_ALLOWED_WRITE_KINDS = ("internal_alert", "vm_submit")
ACTIVATION_EPOCH_STATES = frozenset(
    {
        "safe_off",
        "preauthorized",
        "bounded_active",
        "confirmed",
        "steady_active",
        "aborted",
    }
)
ACTIVATION_SLOT_KINDS = (
    "kafka_success",
    "manual_success",
    "manual_terminal_failure",
)
# Kafka is a passive production ingress. Its broker/ACL/offset connectivity is
# sealed by the preauthorization gate; bounded activation actively exercises
# the two exact manual paths without fabricating an upstream Kafka event.
ACTIVATION_RELEASE_SLOT_KINDS = (
    "manual_success",
    "manual_terminal_failure",
)
ACTIVATION_KAFKA_PROOF_MODE = "passive_connectivity"
_ACTIVATION_RELEASE_SLOT_SQL = "'manual_success', 'manual_terminal_failure'"
ACTIVATION_ENTRYPOINTS = frozenset(
    {"kafka_ingest", "manual_admit", "shadow_promotion"}
)
ACTIVATION_SOURCE_KINDS = frozenset({"kafka", "manual"})
_ACTIVATION_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_ACTIVATION_EPOCH_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
CAPACITY_BOOTSTRAP_PRODUCTION = "BOOTSTRAP_PRODUCTION"
CAPACITY_STEADY_ACTIVE = "STEADY_ACTIVE"
CAPACITY_TRANSITION_STATES = frozenset(
    {CAPACITY_BOOTSTRAP_PRODUCTION, CAPACITY_STEADY_ACTIVE}
)
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


def _exact_canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    )


def _exact_canonical_sha256(value: Any) -> str:
    return hashlib.sha256(_exact_canonical_json(value).encode("utf-8")).hexdigest()


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


def _exact_outbox_hold_fingerprint(value: Mapping[str, Any]) -> str:
    payload = dict(value)
    payload.pop("receipt_fingerprint", None)
    return _exact_canonical_sha256(payload)


def _exact_outbox_hold_plan_id(value: Mapping[str, Any]) -> str:
    control_identity = dict(value["control_db_identity"])
    logical_identity = dict(control_identity.get("logical_db_identity") or {})
    wal = dict(logical_identity.get("wal") or {})
    if wal.get("present") is True and int(wal.get("size", 0)) == 0:
        wal = {"present": False}
    logical_identity["wal"] = wal
    control_identity = {
        "path": control_identity.get("path"),
        "logical_db_identity": logical_identity,
    }
    return _exact_canonical_sha256(
        {
            "command": "hold-exact-outbox",
            "operator": value["operator"],
            "reason": value["reason"],
            "target_outbox_id": value["target_outbox_id"],
            "predecessor_outbox_id": value["predecessor_outbox_id"],
            "activation_required": value["activation_required"],
            "max_age_seconds": value["max_age_seconds"],
            "active_activation": value["active_activation"],
            "active_release_binding": value["active_release_binding"],
            # WAL is durable logical state. SHM contains volatile lock bytes,
            # so bind only its presence while preserving its full hash in the
            # audit evidence outside the deterministic plan id.
            "control_db_identity": control_identity,
            "config_binding_sha256": value["config_binding_sha256"],
            "tool_provenance_sha256": value["tool_provenance_sha256"],
            "target_row_sha256": value["target_before"]["row_sha256"],
            "predecessor_row_sha256": value["predecessor"]["row_sha256"],
            "eligible_queue_sha256": value["eligible_queue_before"]["sha256"],
            # Remaining seconds and observation timestamps are evidence, not
            # plan identity.  Bind only the fixed policy and expiry anchor.
            "retry_horizon": {
                key: value["retry_horizon"][key]
                for key in (
                    "target_outbox_id",
                    "anchor",
                    "expires_at",
                    "min_remaining_seconds",
                    "safety_headroom_seconds",
                    "record_max_age_seconds",
                )
            },
            "destination_path": value["destination_path"],
            "destination_binding": value["destination_binding"],
            "resident_census_policy": {
                "schema_version": value["resident_census"]["schema_version"],
                "source_kind": value["resident_census"]["source_kind"],
                "domain": value["resident_census"]["domain"],
                "forbidden_labels": value["resident_census"]["forbidden_labels"],
                "all_unloaded": value["resident_census"]["all_unloaded"],
            },
        }
    )


def _exact_hold_require_mapping(
    value: Any,
    expected_fields: frozenset[str],
    error_code: str,
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != expected_fields:
        raise ValueError(error_code)
    return value


def _exact_hold_require_type(
    value: Any,
    expected_type: type[Any],
    error_code: str,
    *,
    nullable: bool = False,
) -> None:
    if nullable and value is None:
        return
    if type(value) is not expected_type:
        raise ValueError(error_code)


def _exact_hold_require_sha(value: Any, error_code: str) -> None:
    if (
        not isinstance(value, str)
        or _ACTIVATION_SHA256_RE.fullmatch(value) is None
        or value == "0" * 64
    ):
        raise ValueError(error_code)


def _validate_exact_outbox_hold_nested(value: Mapping[str, Any]) -> None:
    """Reject structural drift before any live binding or database work."""
    error = "exact_outbox_hold_nested_schema_invalid"

    def require_string(item: Any, code: str = error) -> None:
        _exact_hold_require_type(item, str, code)

    def require_int(item: Any, code: str = error) -> None:
        _exact_hold_require_type(item, int, code)

    def require_bool(item: Any, code: str = error) -> None:
        _exact_hold_require_type(item, bool, code)

    def require_nullable_string(item: Any, code: str = error) -> None:
        _exact_hold_require_type(item, str, code, nullable=True)

    def require_nullable_int(item: Any, code: str = error) -> None:
        _exact_hold_require_type(item, int, code, nullable=True)

    def require_path(item: Any, code: str = error) -> None:
        require_string(item, code)
        if not Path(item).is_absolute():
            raise ValueError(code)

    def require_source_file(item: Any, *, allow_label: bool = False) -> None:
        fields = (
            EXACT_OUTBOX_HOLD_RUNTIME_PLIST_FIELDS
            if allow_label
            else EXACT_OUTBOX_HOLD_RUNTIME_FILE_FIELDS
        )
        source = _exact_hold_require_mapping(item, fields, error)
        require_bool(source["present"])
        require_path(source["path"])
        _exact_hold_require_sha(source["sha256"], error)
        for field in ("mode", "uid", "nlink"):
            require_int(source[field])
        if source["nlink"] != 1:
            raise ValueError(error)
        if allow_label:
            require_string(source["label"])

    identity = _exact_hold_require_mapping(
        value["control_db_identity"],
        EXACT_OUTBOX_HOLD_CONTROL_IDENTITY_FIELDS,
        "exact_outbox_hold_control_db_identity_invalid",
    )
    require_string(identity["schema_version"])
    require_path(identity["path"])
    require_bool(identity["present"])
    for field in ("device", "inode", "size", "mtime_ns"):
        require_int(identity[field])
    _exact_hold_require_sha(identity["sha256"], error)

    def require_sidecar(item: Any) -> None:
        sidecar = _exact_hold_require_mapping(
            item,
            (
                frozenset({"present"})
                if isinstance(item, Mapping) and item.get("present") is False
                else EXACT_OUTBOX_HOLD_SOURCE_FILE_FIELDS
            ),
            "exact_outbox_hold_control_db_identity_invalid",
        )
        require_bool(sidecar["present"])
        if sidecar["present"]:
            for field in ("device", "inode", "size", "mtime_ns"):
                require_int(sidecar[field])
            _exact_hold_require_sha(sidecar["sha256"], error)

    require_sidecar(identity["wal"])
    require_sidecar(identity["shm"])
    logical_identity = _exact_hold_require_mapping(
        identity["logical_db_identity"],
        EXACT_OUTBOX_HOLD_LOGICAL_DB_FIELDS,
        "exact_outbox_hold_control_db_identity_invalid",
    )
    require_sidecar(logical_identity["database"])
    require_sidecar(logical_identity["wal"])
    coordination = _exact_hold_require_mapping(
        identity["coordination_observation"],
        EXACT_OUTBOX_HOLD_COORDINATION_FIELDS,
        "exact_outbox_hold_control_db_identity_invalid",
    )
    require_sidecar(coordination["shm"])

    config = _exact_hold_require_mapping(
        value["config_binding"],
        EXACT_OUTBOX_CONFIG_KEYS,
        "exact_outbox_hold_config_binding_invalid",
    )
    for field in (
        "dispatch_enabled",
        "activation_required",
        "allow_download",
        "allow_feishu_writeback",
        "storage_admission_enabled",
        "storage_reservation_enabled",
        "derived_capacity_reservation_enabled",
        "delivery_backpressure_enabled",
        "derived_capacity_atomic_reservation",
    ):
        require_bool(config[field])
    for field in (
        "lease_seconds",
        "max_age_seconds",
        "input_wait_max_age_seconds",
        "poll_interval_seconds",
        "circuit_poll_interval_seconds",
        "batch_size",
        "delivery_high_watermark",
        "delivery_resume_watermark",
        "storage_concurrency_reserve_cases",
        "storage_cases_per_day",
        "storage_expected_artifact_cache_bytes",
        "storage_reserve_percent",
        "storage_timeout_seconds",
        "derived_capacity_reservation_timeout_seconds",
    ):
        require_int(config[field])
    for field in (
        "control_db_path",
        "delivery_db_path",
        "health_path",
        "service_id",
        "service_capability",
        "service_operation",
        "data_access_mode",
        "group_response_cap",
        "translate_baseline",
        "translate_contract_path",
        "storage_capacity_scope",
        "capacity_mode",
        "release_id",
        "bootstrap_epoch_id",
        "active_release_binding_path",
        "live_env_path",
    ):
        require_string(config[field])
    w3_read = config["w3_snapshot_read"]
    if not isinstance(w3_read, Mapping):
        raise ValueError("exact_outbox_hold_config_binding_invalid")
    require_bool(w3_read.get("enabled"))
    require_string(w3_read.get("mode"))
    w3_fields = (
        EXACT_OUTBOX_HOLD_W3_READ_ENABLED_FIELDS
        if w3_read["enabled"]
        else EXACT_OUTBOX_HOLD_W3_READ_DISABLED_FIELDS
    )
    w3_read = _exact_hold_require_mapping(
        w3_read, w3_fields, "exact_outbox_hold_config_binding_invalid"
    )
    if w3_read["enabled"]:
        require_string(w3_read["schema_version"])
        _exact_hold_require_sha(w3_read["authority_sha256"], error)
        policy_sha256s = _exact_hold_require_mapping(
            w3_read["policy_sha256s"],
            EXACT_OUTBOX_HOLD_W3_POLICY_FIELDS,
            "exact_outbox_hold_config_binding_invalid",
        )
        for item in policy_sha256s.values():
            _exact_hold_require_sha(item, error)

    active_binding = _exact_hold_require_mapping(
        value["active_release_binding"],
        EXACT_OUTBOX_HOLD_ACTIVE_BINDING_FIELDS,
        "exact_outbox_hold_active_binding_invalid",
    )
    for field in (
        "path",
        "release_id",
        "authority_epoch_id",
        "bootstrap_epoch_id",
        "runtime_release_target",
        "live_env_path",
    ):
        require_string(active_binding[field])
    require_path(active_binding["path"])
    require_path(active_binding["live_env_path"])
    for field in (
        "sha256",
        "authority_sha256",
        "release_bom_sha256",
        "candidate_env_sha256",
        "authorization_fingerprint",
        "authorization_receipt_sha256",
        "approval_evidence_sha256",
        "runtime_manifest_sha256",
        "raw_sha256",
        "live_env_sha256",
    ):
        _exact_hold_require_sha(active_binding[field], error)
    for field in ("runtime_git_head", "runtime_git_tree"):
        if not isinstance(active_binding[field], str) or re.fullmatch(
            r"[0-9a-f]{40}", active_binding[field]
        ) is None:
            raise ValueError("exact_outbox_hold_active_binding_invalid")

    tool = _exact_hold_require_mapping(
        value["tool_provenance"],
        EXACT_OUTBOX_HOLD_TOOL_FIELDS,
        "exact_outbox_hold_tool_provenance_invalid",
    )
    for field in ("entrypoint_path", "control_store_path", "bootstrap_path"):
        require_path(tool[field])
    for field in (
        "entrypoint_sha256",
        "control_store_sha256",
        "bootstrap_sha256",
    ):
        _exact_hold_require_sha(tool[field], error)
    for field in ("git_head", "git_tree"):
        if not isinstance(tool[field], str) or re.fullmatch(
            r"[0-9a-f]{40}", tool[field]
        ) is None:
            raise ValueError("exact_outbox_hold_tool_provenance_invalid")
    require_int(tool["git_status_returncode"])
    require_bool(tool["git_clean"])
    runtime = _exact_hold_require_mapping(
        tool["runtime_provenance"],
        EXACT_OUTBOX_HOLD_RUNTIME_FIELDS,
        "exact_outbox_hold_runtime_provenance_invalid",
    )
    for field in (
        "schema_version",
        "manifest_runtime_root",
        "manifest_runtime_release_target",
        "manifest_gateway_release_target",
        "manifest_commit",
        "manifest_tree",
        "runtime_git_head",
        "runtime_git_tree",
    ):
        require_string(runtime[field])
    require_path(runtime["manifest_runtime_root"])
    for field in ("manifest_commit", "manifest_tree", "runtime_git_head", "runtime_git_tree"):
        if re.fullmatch(r"[0-9a-f]{40}", runtime[field]) is None:
            raise ValueError("exact_outbox_hold_runtime_provenance_invalid")
    _exact_hold_require_sha(runtime["release_bom_sha256"], error)
    require_source_file(runtime["manifest"])
    require_source_file(runtime["stable_target_registry"])
    plists = runtime["plists"]
    if type(plists) is not list or len(plists) != len(EXACT_OUTBOX_RUNTIME_PLIST_LABELS):
        raise ValueError("exact_outbox_hold_runtime_provenance_invalid")
    for label, plist in zip(EXACT_OUTBOX_RUNTIME_PLIST_LABELS, plists, strict=True):
        require_source_file(plist, allow_label=True)
        if plist["label"] != label:
            raise ValueError("exact_outbox_hold_runtime_provenance_invalid")

    resident = _exact_hold_require_mapping(
        value["resident_census"],
        EXACT_OUTBOX_HOLD_RESIDENT_FIELDS,
        "exact_outbox_hold_resident_census_invalid",
    )
    for field in ("schema_version", "observed_at", "source_kind", "domain"):
        require_string(resident[field])
    require_path(resident["active_release_binding_path"])
    require_bool(resident["all_unloaded"])
    require_int(resident["loaded_count"])
    if type(resident["forbidden_labels"]) is not list or type(
        resident["observations"]
    ) is not list or type(resident["loaded_labels"]) is not list:
        raise ValueError("exact_outbox_hold_resident_census_invalid")
    for label in resident["forbidden_labels"]:
        require_string(label)
    for label in resident["loaded_labels"]:
        require_string(label)
    observations = resident["observations"]
    if len(observations) != len(resident["forbidden_labels"]):
        raise ValueError("exact_outbox_hold_resident_census_invalid")
    for observation in observations:
        item = _exact_hold_require_mapping(
            observation,
            EXACT_OUTBOX_HOLD_RESIDENT_OBSERVATION_FIELDS,
            "exact_outbox_hold_resident_census_invalid",
        )
        require_string(item["label"])
        require_bool(item["loaded"])
        require_int(item["returncode"])
        require_bool(item["unloaded_proven"])
        _exact_hold_require_sha(item["output_sha256"], error)
    _exact_hold_require_sha(resident["source_sha256"], error)

    activation = _exact_hold_require_mapping(
        value["active_activation"],
        EXACT_OUTBOX_HOLD_ACTIVATION_FIELDS,
        "exact_outbox_hold_activation_invalid",
    )
    require_bool(activation["configured"])
    for field in ("epoch_id", "state"):
        require_string(activation[field])
    require_int(activation["is_current"])
    for field in (
        "config_sha256",
        "db_logical_identity_sha256",
        "preproduction_fingerprint",
        "preproduction_gate_receipt_sha256",
        "production_fingerprint",
        "production_gate_receipt_sha256",
    ):
        require_string(activation[field])
    db_identity = _exact_hold_require_mapping(
        activation["db_logical_identity"],
        EXACT_OUTBOX_HOLD_ACTIVATION_DB_FIELDS,
        "exact_outbox_hold_activation_invalid",
    )
    require_int(db_identity["device"])
    require_int(db_identity["inode"])
    require_string(db_identity["logical_store_id"])
    _exact_hold_require_sha(activation["sha256"], error)

    def require_row(item: Any) -> None:
        row = _exact_hold_require_mapping(
            item, EXACT_OUTBOX_HOLD_ROW_FIELDS, "exact_outbox_hold_row_binding_invalid"
        )
        require_string(row["schema_version"])
        require_int(row["outbox_id"])
        for field in (
            "submission_key",
            "business_key",
            "status",
            "created_at",
            "updated_at",
        ):
            require_string(row[field])
        for field in ("generation", "attempt", "fence"):
            require_int(row[field])
        for field in (
            "next_attempt_at",
            "retry_window_started_at",
            "lease_token",
            "lease_owner",
            "lease_expires_at",
            "claimed_at",
            "completed_at",
            "quarantined_at",
            "result_json",
            "activation_epoch_id",
        ):
            require_nullable_string(row[field])
        require_nullable_int(row["activation_ledger_id"])
        _exact_hold_require_sha(row["row_sha256"], error)

    for field in ("target_before", "target_after", "predecessor"):
        require_row(value[field])
    for field in ("eligible_queue_before", "eligible_queue_after"):
        queue = _exact_hold_require_mapping(
            value[field], EXACT_OUTBOX_HOLD_QUEUE_FIELDS, "exact_outbox_hold_queue_binding_invalid"
        )
        if type(queue["outbox_ids"]) is not list or type(queue["entries"]) is not list:
            raise ValueError("exact_outbox_hold_queue_binding_invalid")
        _exact_hold_require_sha(queue["sha256"], error)
        for outbox_id in queue["outbox_ids"]:
            require_int(outbox_id)
        for entry in queue["entries"]:
            queue_entry = _exact_hold_require_mapping(
                entry,
                EXACT_OUTBOX_HOLD_QUEUE_ENTRY_FIELDS,
                "exact_outbox_hold_queue_binding_invalid",
            )
            require_int(queue_entry["outbox_id"])
            _exact_hold_require_sha(queue_entry["row_sha256"], error)

    retry = _exact_hold_require_mapping(
        value["retry_horizon"],
        EXACT_OUTBOX_HOLD_RETRY_FIELDS,
        "exact_outbox_hold_retry_horizon_invalid",
    )
    require_int(retry["target_outbox_id"])
    for field in ("anchor", "expires_at"):
        require_string(retry[field])
    for field in (
        "min_remaining_seconds",
        "safety_headroom_seconds",
        "record_max_age_seconds",
        "plan_remaining_seconds",
    ):
        require_int(retry[field])
    require_nullable_string(retry["apply_observed_at"])
    require_nullable_int(retry["apply_remaining_seconds"])
    if (retry["apply_observed_at"] is None) != (
        retry["apply_remaining_seconds"] is None
    ):
        raise ValueError("exact_outbox_hold_retry_horizon_invalid")

    effect = _exact_hold_require_mapping(
        value["effect_delta"],
        EXACT_OUTBOX_HOLD_EFFECT_FIELDS,
        "exact_outbox_hold_effect_delta_invalid",
    )
    require_int(effect["external_writes"])
    require_bool(effect["external_effects_triggered"])
    for field in (
        "target_rows_updated",
        "control_meta_inserted",
        "business_trigger_rows_updated",
    ):
        require_int(effect[field])
    mutation = _exact_hold_require_mapping(
        effect["mutation"],
        EXACT_OUTBOX_HOLD_EFFECT_MUTATION_FIELDS,
        "exact_outbox_hold_effect_delta_invalid",
    )
    require_string(mutation["next_attempt_at"])
    require_string(mutation["updated_at"])


def _validate_exact_outbox_hold_audit(
    value: Mapping[str, Any],
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("exact_outbox_hold_audit_invalid")
    normalized = dict(value)
    if set(normalized) != EXACT_OUTBOX_HOLD_REQUIRED_FIELDS:
        raise ValueError("exact_outbox_hold_audit_fields_invalid")
    if (
        normalized.get("schema_version") != EXACT_OUTBOX_HOLD_SCHEMA_VERSION
        or normalized.get("command") != "hold-exact-outbox"
        or normalized.get("phase") != "hold"
    ):
        raise ValueError("exact_outbox_hold_audit_schema_invalid")
    for field in ("hold_id", "plan_id"):
        if (
            not isinstance(normalized.get(field), str)
            or _ACTIVATION_SHA256_RE.fullmatch(normalized[field]) is None
            or normalized[field] == "0" * 64
        ):
            raise ValueError(f"exact_outbox_hold_{field}_invalid")
    for field in ("operator", "reason"):
        limit = 200 if field == "operator" else 1000
        item = normalized.get(field)
        if (
            not isinstance(item, str)
            or not item
            or item != item.strip()
            or len(item.encode("utf-8")) > limit
            or any(char in item for char in "\n\r\x00")
        ):
            raise ValueError("exact_outbox_hold_audit_text_invalid")
    recorded_at = normalized.get("recorded_at")
    try:
        recorded = datetime.fromisoformat(str(recorded_at).replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise ValueError("exact_outbox_hold_timestamp_invalid") from exc
    if recorded.tzinfo is None or recorded.utcoffset() != timedelta(0):
        raise ValueError("exact_outbox_hold_timestamp_invalid")
    for field in ("target_outbox_id", "predecessor_outbox_id"):
        item = normalized.get(field)
        if isinstance(item, bool) or not isinstance(item, int) or item < 1:
            raise ValueError("exact_outbox_hold_identity_invalid")
    if normalized["target_outbox_id"] == normalized["predecessor_outbox_id"]:
        raise ValueError("exact_outbox_hold_identity_invalid")
    if normalized.get("activation_required") is not True:
        raise ValueError("exact_outbox_hold_activation_required_invalid")
    max_age_seconds = normalized.get("max_age_seconds")
    if (
        isinstance(max_age_seconds, bool)
        or not isinstance(max_age_seconds, int)
        or max_age_seconds < 1
    ):
        raise ValueError("exact_outbox_hold_retry_horizon_invalid")
    _validate_exact_outbox_hold_nested(normalized)
    identity = normalized.get("control_db_identity")
    if (
        not isinstance(identity, Mapping)
        or identity.get("schema_version")
        != "pnc_rca_control_store_source_snapshot_v1"
        or not isinstance(identity.get("path"), str)
        or not Path(identity["path"]).is_absolute()
        or identity.get("present") is not True
        or isinstance(identity.get("device"), bool)
        or not isinstance(identity.get("device"), int)
        or isinstance(identity.get("size"), bool)
        or not isinstance(identity.get("size"), int)
        or isinstance(identity.get("mtime_ns"), bool)
        or not isinstance(identity.get("mtime_ns"), int)
        or isinstance(identity.get("inode"), bool)
        or not isinstance(identity.get("inode"), int)
        or _ACTIVATION_SHA256_RE.fullmatch(str(identity.get("sha256") or ""))
        is None
    ):
        raise ValueError("exact_outbox_hold_control_db_identity_invalid")
    for sidecar in ("wal", "shm"):
        item = identity.get(sidecar)
        if not isinstance(item, Mapping) or not isinstance(
            item.get("present"), bool
        ):
            raise ValueError("exact_outbox_hold_control_db_identity_invalid")
        expected_keys = (
            {"present"}
            if item.get("present") is False
            else {"present", "device", "inode", "size", "mtime_ns", "sha256"}
        )
        if set(item) != expected_keys:
            raise ValueError("exact_outbox_hold_control_db_identity_invalid")
        if item["present"] and (
            _ACTIVATION_SHA256_RE.fullmatch(str(item.get("sha256") or "")) is None
            or any(
                isinstance(item.get(field), bool)
                or not isinstance(item.get(field), int)
                for field in ("device", "inode", "size", "mtime_ns")
            )
        ):
            raise ValueError("exact_outbox_hold_control_db_identity_invalid")
    logical_identity = identity.get("logical_db_identity")
    coordination = identity.get("coordination_observation")
    if (
        not isinstance(logical_identity, Mapping)
        or logical_identity.get("database") != {
            key: identity[key]
            for key in ("present", "device", "inode", "size", "mtime_ns", "sha256")
            if key in identity
        }
        or logical_identity.get("wal") != identity.get("wal")
        or not isinstance(coordination, Mapping)
        or coordination.get("shm") != identity.get("shm")
    ):
        raise ValueError("exact_outbox_hold_control_db_identity_invalid")
    for field in ("config_binding_sha256", "tool_provenance_sha256"):
        item = normalized.get(field)
        if (
            not isinstance(item, str)
            or _ACTIVATION_SHA256_RE.fullmatch(item) is None
            or item == "0" * 64
        ):
            raise ValueError("exact_outbox_hold_provenance_invalid")
    config_binding = normalized.get("config_binding")
    if (
        not isinstance(config_binding, Mapping)
        or set(config_binding) != EXACT_OUTBOX_CONFIG_KEYS
        or _exact_canonical_sha256(config_binding)
        != normalized["config_binding_sha256"]
    ):
        raise ValueError("exact_outbox_hold_config_binding_invalid")
    active_binding = normalized.get("active_release_binding")
    if (
        not isinstance(active_binding, Mapping)
        or not isinstance(active_binding.get("path"), str)
        or not Path(active_binding["path"]).is_absolute()
        or not isinstance(active_binding.get("live_env_path"), str)
        or not Path(active_binding["live_env_path"]).is_absolute()
        or _ACTIVATION_SHA256_RE.fullmatch(str(active_binding.get("raw_sha256") or ""))
        is None
        or _ACTIVATION_SHA256_RE.fullmatch(
            str(active_binding.get("live_env_sha256") or "")
        )
        is None
        or _ACTIVATION_SHA256_RE.fullmatch(
            str(active_binding.get("sha256") or "")
        )
        is None
        or active_binding.get("sha256") != active_binding.get("raw_sha256")
        or _ACTIVATION_SHA256_RE.fullmatch(
            str(active_binding.get("release_bom_sha256") or "")
        )
        is None
        or _ACTIVATION_SHA256_RE.fullmatch(
            str(active_binding.get("runtime_manifest_sha256") or "")
        )
        is None
        or any(
            _ACTIVATION_SHA256_RE.fullmatch(str(active_binding.get(field) or ""))
            is None
            for field in (
                "candidate_env_sha256",
                "authorization_fingerprint",
                "authorization_receipt_sha256",
                "approval_evidence_sha256",
            )
        )
        or not re.fullmatch(
            r"[0-9a-f]{40}", str(active_binding.get("runtime_git_head") or "")
        )
        or not re.fullmatch(
            r"[0-9a-f]{40}", str(active_binding.get("runtime_git_tree") or "")
        )
    ):
        raise ValueError("exact_outbox_hold_active_binding_invalid")
    tool_provenance = normalized.get("tool_provenance")
    if (
        not isinstance(tool_provenance, Mapping)
        or _exact_canonical_sha256(tool_provenance)
        != normalized["tool_provenance_sha256"]
    ):
        raise ValueError("exact_outbox_hold_tool_provenance_invalid")
    if (
        not isinstance(tool_provenance.get("git_clean"), bool)
        or tool_provenance.get("git_clean") is not True
        or not re.fullmatch(r"[0-9a-f]{40}", str(tool_provenance.get("git_head") or ""))
        or not re.fullmatch(r"[0-9a-f]{40}", str(tool_provenance.get("git_tree") or ""))
    ):
        raise ValueError("exact_outbox_hold_tool_provenance_invalid")
    runtime_provenance = tool_provenance.get("runtime_provenance")
    runtime_plists = (
        runtime_provenance.get("plists")
        if isinstance(runtime_provenance, Mapping)
        else None
    )

    def _valid_runtime_plists(value: Any) -> bool:
        if not isinstance(value, list) or len(value) != len(EXACT_OUTBOX_RUNTIME_PLIST_LABELS):
            return False
        prefix = Path.home() / "Library" / "LaunchAgents"
        for label, item in zip(EXACT_OUTBOX_RUNTIME_PLIST_LABELS, value, strict=True):
            if not isinstance(item, Mapping) or item.get("label") != label:
                return False
            expected_path = prefix / f"{label}.plist"
            if item.get("path") != str(expected_path):
                return False
            if (
                item.get("present") is not True
                or _ACTIVATION_SHA256_RE.fullmatch(str(item.get("sha256") or "")) is None
                or isinstance(item.get("mode"), bool)
                or not isinstance(item.get("mode"), int)
                or isinstance(item.get("uid"), bool)
                or not isinstance(item.get("uid"), int)
                or item.get("nlink") != 1
            ):
                return False
        return True

    if (
        not isinstance(runtime_provenance, Mapping)
        or runtime_provenance.get("schema_version")
        != "pnc_rca_exact_outbox_runtime_provenance_v1"
        or not isinstance(runtime_provenance.get("manifest"), Mapping)
        or runtime_provenance["manifest"].get("present") is not True
        or not isinstance(runtime_provenance["manifest"].get("path"), str)
        or not Path(runtime_provenance["manifest"]["path"]).is_absolute()
        or _ACTIVATION_SHA256_RE.fullmatch(
            str(runtime_provenance["manifest"].get("sha256") or "")
        )
        is None
        or isinstance(runtime_provenance["manifest"].get("mode"), bool)
        or not isinstance(runtime_provenance["manifest"].get("mode"), int)
        or isinstance(runtime_provenance["manifest"].get("uid"), bool)
        or not isinstance(runtime_provenance["manifest"].get("uid"), int)
        or runtime_provenance["manifest"].get("nlink") != 1
        or not isinstance(runtime_provenance.get("manifest_runtime_root"), str)
        or not Path(runtime_provenance["manifest_runtime_root"]).is_absolute()
        or not isinstance(runtime_provenance.get("manifest_runtime_release_target"), str)
        or not runtime_provenance["manifest_runtime_release_target"]
        or runtime_provenance.get("manifest_runtime_release_target")
        != runtime_provenance.get("manifest_gateway_release_target")
        or not re.fullmatch(
            r"[0-9a-f]{40}", str(runtime_provenance.get("runtime_git_head") or "")
        )
        or not re.fullmatch(
            r"[0-9a-f]{40}", str(runtime_provenance.get("runtime_git_tree") or "")
        )
        or runtime_provenance.get("runtime_git_head")
        != runtime_provenance.get("manifest_commit")
        or runtime_provenance.get("runtime_git_tree")
        != runtime_provenance.get("manifest_tree")
        or _ACTIVATION_SHA256_RE.fullmatch(
            str(runtime_provenance.get("release_bom_sha256") or "")
        )
        is None
        or not _valid_runtime_plists(runtime_plists)
        or not isinstance(runtime_provenance.get("stable_target_registry"), Mapping)
        or runtime_provenance["stable_target_registry"].get("present") is not True
        or not isinstance(runtime_provenance["stable_target_registry"].get("path"), str)
        or not Path(runtime_provenance["stable_target_registry"]["path"]).is_absolute()
        or _ACTIVATION_SHA256_RE.fullmatch(
            str(runtime_provenance["stable_target_registry"].get("sha256") or "")
        )
        is None
        or runtime_provenance["stable_target_registry"].get("nlink") != 1
    ):
        raise ValueError("exact_outbox_hold_runtime_provenance_invalid")
    if (
        active_binding.get("release_bom_sha256")
        != runtime_provenance.get("release_bom_sha256")
        or active_binding.get("runtime_manifest_sha256")
        != runtime_provenance["manifest"].get("sha256")
        or active_binding.get("runtime_git_head")
        != runtime_provenance.get("runtime_git_head")
        or active_binding.get("runtime_git_tree")
        != runtime_provenance.get("runtime_git_tree")
        or active_binding.get("runtime_release_target")
        != runtime_provenance.get("manifest_runtime_release_target")
    ):
        raise ValueError("exact_outbox_hold_active_binding_invalid")
    resident_census = normalized.get("resident_census")
    if (
        not isinstance(resident_census, Mapping)
        or resident_census.get("schema_version")
        != "pnc_rca_exact_outbox_resident_census_v1"
        or resident_census.get("all_unloaded") is not True
        or resident_census.get("loaded_count") != 0
        or resident_census.get("source_kind") != "launchctl_read_only_print"
        or resident_census.get("domain") != f"gui/{os.getuid()}"
        or not isinstance(resident_census.get("forbidden_labels"), list)
        or not isinstance(resident_census.get("observations"), list)
        or resident_census.get("forbidden_labels")
        != [
            "local.pnc.rca-kafka-consumer",
            "local.pnc.rca-outbox-dispatcher",
            "local.pnc.rca-delivery-collector",
            "local.pnc.rca-delivery-dispatcher",
            "local.pnc.completion-notice-relay",
            "local.pnc.feishu-delivery-repair",
        ]
        or len(resident_census.get("observations"))
        != len(resident_census.get("forbidden_labels"))
        or any(
            not isinstance(item, Mapping)
            or item.get("label") != resident_census["forbidden_labels"][index]
            or item.get("loaded") is not False
            or item.get("returncode") != 113
            or item.get("unloaded_proven") is not True
            for index, item in enumerate(resident_census.get("observations", []))
        )
        or resident_census.get("loaded_labels") != []
        or not isinstance(resident_census.get("source_sha256"), str)
        or _ACTIVATION_SHA256_RE.fullmatch(resident_census["source_sha256"]) is None
        or resident_census.get("source_sha256")
        != _exact_canonical_sha256(
            {
                key: item
                for key, item in resident_census.items()
                if key != "source_sha256"
            }
        )
    ):
        raise ValueError("exact_outbox_hold_resident_census_invalid")
    try:
        resident_observed_at = datetime.fromisoformat(
            str(resident_census["observed_at"]).replace("Z", "+00:00")
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("exact_outbox_hold_resident_census_invalid") from exc
    if (
        resident_observed_at.tzinfo is None
        or resident_observed_at.utcoffset() != timedelta(0)
        or _utc_datetime(resident_observed_at) < recorded
        or _utc_datetime(resident_observed_at)
        > datetime.now(timezone.utc)
        + timedelta(seconds=EXACT_OUTBOX_HOLD_MAX_FUTURE_SKEW_SECONDS)
    ):
        raise ValueError("exact_outbox_hold_resident_census_invalid")
    live_env_path = Path(active_binding["live_env_path"]).expanduser().absolute()
    canonical_binding_path = (
        live_env_path.parent
        / "runtime"
        / "pnc_agent"
        / "feishu_issue_kafka_rca"
        / "active-release-binding.json"
    )
    if (
        resident_census["active_release_binding_path"]
        != active_binding["path"]
        or Path(resident_census["active_release_binding_path"])
        .expanduser()
        .absolute()
        != canonical_binding_path
    ):
        raise ValueError("exact_outbox_hold_resident_census_invalid")
    active_activation = normalized.get("active_activation")
    if (
        not isinstance(active_activation, Mapping)
        or active_activation.get("configured") is not True
        or active_activation.get("state") != "bounded_active"
        or active_activation.get("is_current") != 1
        or _ACTIVATION_SHA256_RE.fullmatch(
            str(active_activation.get("sha256") or "")
        )
        is None
        or any(
            _ACTIVATION_SHA256_RE.fullmatch(str(active_activation.get(field) or ""))
            is None
            for field in (
                "config_sha256",
                "db_logical_identity_sha256",
                "preproduction_fingerprint",
                "preproduction_gate_receipt_sha256",
            )
        )
        or not isinstance(active_activation.get("db_logical_identity"), Mapping)
        or active_activation["db_logical_identity"].get("logical_store_id")
        != "rca-control-primary"
        or _exact_canonical_sha256(active_activation["db_logical_identity"])
        != active_activation.get("db_logical_identity_sha256")
        or active_activation.get("production_fingerprint") != ""
        or active_activation.get("production_gate_receipt_sha256") != ""
    ):
        raise ValueError("exact_outbox_hold_activation_invalid")
    if active_activation["sha256"] != _exact_canonical_sha256(
        {
            key: item
            for key, item in active_activation.items()
            if key != "sha256"
        }
    ):
        raise ValueError("exact_outbox_hold_activation_invalid")
    destination = normalized.get("destination_binding")
    destination_path = normalized.get("destination_path")
    if (
        not isinstance(destination_path, str)
        or not Path(destination_path).is_absolute()
        or destination_path != str(Path(destination_path).absolute())
        or hashlib.sha256(destination_path.encode("utf-8")).hexdigest()
        != str(destination.get("path_sha256") if isinstance(destination, Mapping) else "")
        or not isinstance(destination, Mapping)
        or set(destination) != {"path_sha256", "parent_device", "parent_inode"}
        or _ACTIVATION_SHA256_RE.fullmatch(str(destination.get("path_sha256") or ""))
        is None
        or isinstance(destination.get("parent_device"), bool)
        or not isinstance(destination.get("parent_device"), int)
        or isinstance(destination.get("parent_inode"), bool)
        or not isinstance(destination.get("parent_inode"), int)
        or destination.get("parent_device") < 1
        or destination.get("parent_inode") < 1
    ):
        raise ValueError("exact_outbox_hold_destination_invalid")
    for field, expected_id in (
        ("target_before", normalized["target_outbox_id"]),
        ("target_after", normalized["target_outbox_id"]),
        ("predecessor", normalized["predecessor_outbox_id"]),
    ):
        item = normalized.get(field)
        if (
            not isinstance(item, Mapping)
            or item.get("schema_version") != EXACT_OUTBOX_HOLD_ROW_SCHEMA_VERSION
            or item.get("outbox_id") != expected_id
            or _ACTIVATION_SHA256_RE.fullmatch(
                str(item.get("row_sha256") or "")
            )
            is None
        ):
            raise ValueError("exact_outbox_hold_row_binding_invalid")
    before = normalized["target_before"]
    after = normalized["target_after"]
    if (
        before.get("status") != "pending"
        or after.get("status") != "pending"
        or before.get("attempt") != 0
        or after.get("attempt") != 0
        or before.get("fence") != 0
        or after.get("fence") != 0
        or before.get("next_attempt_at") is not None
        or after.get("next_attempt_at") != EXACT_OUTBOX_HOLD_UNTIL
        or after.get("updated_at") != recorded_at
        or normalized["predecessor"].get("status") != "pending"
        or normalized["predecessor"].get("attempt") != 0
        or normalized["predecessor"].get("fence") != 0
        or normalized["predecessor"].get("next_attempt_at") is not None
        or before.get("activation_epoch_id") != active_activation.get("epoch_id")
        or after.get("activation_epoch_id") != active_activation.get("epoch_id")
        or normalized["predecessor"].get("activation_epoch_id")
        != active_activation.get("epoch_id")
    ):
        raise ValueError("exact_outbox_hold_row_state_invalid")
    for row in (before, after, normalized["predecessor"]):
        for field in ("created_at", "retry_window_started_at"):
            raw_timestamp = row.get(field)
            if raw_timestamp is None:
                if field == "created_at":
                    raise ValueError("exact_outbox_hold_row_state_invalid")
                continue
            try:
                parsed_timestamp = datetime.fromisoformat(
                    str(raw_timestamp).replace("Z", "+00:00")
                )
            except (TypeError, ValueError) as exc:
                raise ValueError("exact_outbox_hold_row_state_invalid") from exc
            if (
                parsed_timestamp.tzinfo is None
                or parsed_timestamp.utcoffset() != timedelta(0)
                or _utc_datetime(parsed_timestamp) > recorded
            ):
                raise ValueError("exact_outbox_hold_row_state_invalid")
    mutable_after_fields = {"next_attempt_at", "updated_at", "row_sha256"}
    if any(
        before.get(field) != after.get(field)
        for field in set(before) | set(after)
        if field not in mutable_after_fields
    ):
        raise ValueError("exact_outbox_hold_row_state_invalid")
    for field, expected_ids in (
        (
            "eligible_queue_before",
            sorted(
                [
                    normalized["predecessor_outbox_id"],
                    normalized["target_outbox_id"],
                ]
            ),
        ),
        ("eligible_queue_after", [normalized["predecessor_outbox_id"]]),
    ):
        item = normalized.get(field)
        entries = item.get("entries") if isinstance(item, Mapping) else None
        if (
            not isinstance(item, Mapping)
            or item.get("outbox_ids") != expected_ids
            or not isinstance(entries, list)
            or [entry.get("outbox_id") for entry in entries] != expected_ids
            or any(
                not isinstance(entry, Mapping)
                or set(entry) != {"outbox_id", "row_sha256"}
                or _ACTIVATION_SHA256_RE.fullmatch(
                    str(entry.get("row_sha256") or "")
                )
                is None
                for entry in entries
            )
            or _ACTIVATION_SHA256_RE.fullmatch(
                str(item.get("sha256") or "")
            )
            is None
            or item.get("sha256")
            != _exact_canonical_sha256(
                [
                    {
                        "outbox_id": int(entry["outbox_id"]),
                        "row_sha256": str(entry["row_sha256"]),
                    }
                    for entry in entries
                ]
            )
        ):
            raise ValueError("exact_outbox_hold_queue_binding_invalid")
    before_entries = normalized["eligible_queue_before"]["entries"]
    after_entries = normalized["eligible_queue_after"]["entries"]
    expected_before_sha = {
        normalized["predecessor_outbox_id"]: normalized["predecessor"]["row_sha256"],
        normalized["target_outbox_id"]: normalized["target_before"]["row_sha256"],
    }
    if (
        any(
            entry["row_sha256"] != expected_before_sha[entry["outbox_id"]]
            for entry in before_entries
        )
        or after_entries[0]["row_sha256"]
        != normalized["predecessor"]["row_sha256"]
    ):
        raise ValueError("exact_outbox_hold_queue_row_binding_invalid")
    retry_horizon = normalized.get("retry_horizon")
    if (
        not isinstance(retry_horizon, Mapping)
        or not isinstance(retry_horizon.get("anchor"), str)
        or not isinstance(retry_horizon.get("expires_at"), str)
        or retry_horizon.get("target_outbox_id")
        != normalized["target_outbox_id"]
        or retry_horizon.get("min_remaining_seconds")
        != EXACT_OUTBOX_HOLD_MIN_REMAINING_SECONDS
        or retry_horizon.get("safety_headroom_seconds")
        != EXACT_OUTBOX_HOLD_MIN_REMAINING_SECONDS
        or retry_horizon.get("record_max_age_seconds")
        != EXACT_OUTBOX_HOLD_RECORD_MAX_AGE_SECONDS
        or isinstance(retry_horizon.get("plan_remaining_seconds"), bool)
        or not isinstance(retry_horizon.get("plan_remaining_seconds"), int)
        or retry_horizon.get("plan_remaining_seconds")
        < EXACT_OUTBOX_HOLD_MIN_REMAINING_SECONDS
        or (
            retry_horizon.get("apply_remaining_seconds") is not None
            and (
                isinstance(retry_horizon.get("apply_remaining_seconds"), bool)
                or not isinstance(retry_horizon.get("apply_remaining_seconds"), int)
                or retry_horizon.get("apply_remaining_seconds")
                < EXACT_OUTBOX_HOLD_MIN_REMAINING_SECONDS
            )
        )
    ):
        raise ValueError("exact_outbox_hold_retry_horizon_invalid")
    try:
        anchor = datetime.fromisoformat(
            str(retry_horizon["anchor"]).replace("Z", "+00:00")
        )
        expires = datetime.fromisoformat(
            str(retry_horizon["expires_at"]).replace("Z", "+00:00")
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("exact_outbox_hold_retry_horizon_invalid") from exc
    if (
        anchor.tzinfo is None
        or expires.tzinfo is None
        or _utc_datetime(expires)
        != _utc_datetime(anchor) + timedelta(seconds=max_age_seconds)
        or _utc_datetime(expires) <= recorded
        or int((_utc_datetime(expires) - _utc_datetime(recorded)).total_seconds())
        < EXACT_OUTBOX_HOLD_MIN_REMAINING_SECONDS
        or retry_horizon["plan_remaining_seconds"]
        != int((_utc_datetime(expires) - _utc_datetime(recorded)).total_seconds())
    ):
        raise ValueError("exact_outbox_hold_retry_horizon_invalid")
    apply_observed_at = retry_horizon.get("apply_observed_at")
    if apply_observed_at is not None:
        try:
            apply_observed = datetime.fromisoformat(
                str(apply_observed_at).replace("Z", "+00:00")
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("exact_outbox_hold_retry_horizon_invalid") from exc
        if (
            apply_observed.tzinfo is None
            or _utc_datetime(apply_observed) < _utc_datetime(recorded)
            or int(
                (_utc_datetime(expires) - _utc_datetime(apply_observed)).total_seconds()
            )
            != retry_horizon.get("apply_remaining_seconds")
        ):
            raise ValueError("exact_outbox_hold_retry_horizon_invalid")
    effect_delta = normalized.get("effect_delta")
    if (
        not isinstance(effect_delta, Mapping)
        or effect_delta.get("external_writes") != 0
        or effect_delta.get("external_effects_triggered") is not False
        or effect_delta.get("target_rows_updated") != 1
        or effect_delta.get("control_meta_inserted") != 1
        or effect_delta.get("business_trigger_rows_updated") != 0
    ):
        raise ValueError("exact_outbox_hold_effect_delta_invalid")
    mutation = effect_delta["mutation"]
    if (
        mutation["next_attempt_at"] != EXACT_OUTBOX_HOLD_UNTIL
        or mutation["next_attempt_at"] != after["next_attempt_at"]
        or mutation["updated_at"] != recorded_at
        or mutation["updated_at"] != after["updated_at"]
    ):
        raise ValueError("exact_outbox_hold_effect_delta_invalid")
    fingerprint = normalized.get("receipt_fingerprint")
    expected_plan_id = _exact_outbox_hold_plan_id(normalized)
    expected_hold_id = _exact_canonical_sha256(
        {
            "plan_id": expected_plan_id,
            "recorded_at": recorded_at,
            "target_after_sha256": after["row_sha256"],
        }
    )
    if (
        normalized["plan_id"] != expected_plan_id
        or normalized["hold_id"] != expected_hold_id
        or not isinstance(fingerprint, str)
        or _ACTIVATION_SHA256_RE.fullmatch(fingerprint) is None
        or fingerprint != _exact_outbox_hold_fingerprint(normalized)
    ):
        raise ValueError("exact_outbox_hold_fingerprint_invalid")
    try:
        serialized = _exact_canonical_json(normalized)
    except (TypeError, ValueError) as exc:
        raise ValueError("exact_outbox_hold_audit_json_invalid") from exc
    if len(serialized.encode("utf-8")) > EXACT_OUTBOX_HOLD_MAX_AUDIT_BYTES:
        raise ValueError("exact_outbox_hold_audit_too_large")
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


ActivationEpochState = Literal[
    "safe_off",
    "preauthorized",
    "bounded_active",
    "confirmed",
    "steady_active",
    "aborted",
]
ActivationAdmissionOutcome = Literal["admit", "join", "shadow", "reject"]


@dataclass(frozen=True)
class ActivationAdmissionDecision:
    epoch_id: str
    epoch_state: str
    decision: ActivationAdmissionOutcome
    reason: str
    ledger_id: int | None = None
    slot_kind: str = ""
    consumed_slot: bool = False
    legacy_unconfigured: bool = False

    @property
    def creates_execution(self) -> bool:
        return self.decision == "admit"

    @property
    def creates_shadow(self) -> bool:
        return self.decision == "shadow"


class ActivationEpochError(RuntimeError):
    """The durable production activation state rejected an unsafe mutation."""


class CapacityTransitionStateError(RuntimeError):
    """The durable capacity latch rejected an unsafe or stale transition."""


class ManualRcaAdmissionError(ValueError):
    """A manual request failed closed before creating execution work."""


class RecordConflictError(RuntimeError):
    """The same Kafka coordinate was observed with different raw bytes."""


class RecordProcessingBlockedError(RuntimeError):
    """One durable record hit an unknown code path and must remain unacknowledged."""

    def __init__(self, event_uid: str):
        self.event_uid = str(event_uid)
        super().__init__(f"durable record processing blocked: {self.event_uid}")


class ActivationIngressDeferredError(
    ActivationEpochError, RecordProcessingBlockedError
):
    """Kafka must retain this exact offset until the activation fence advances."""

    def __init__(self, event_uid: str, reason_code: str):
        self.event_uid = str(event_uid)
        self.reason_code = str(reason_code)
        RuntimeError.__init__(
            self,
            f"durable record processing blocked: {self.event_uid}: {self.reason_code}",
        )


class StaleOutboxLeaseError(RuntimeError):
    """An outbox mutation was attempted without the current fencing token."""


class ShadowPromotionError(RuntimeError):
    """A shadow event could not be promoted under the single-event policy."""


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


@dataclass(frozen=True)
class ShadowPromotionResult:
    event_uid: str
    outbox_id: int
    submission_key: str
    status: str
    promoted: bool
    audit_id: int


@dataclass(frozen=True)
class ActivationDeferralResult:
    event_uid: str
    epoch_id: str
    outbox_id: int
    submission_key: str
    prior_status: str
    status: str
    audit_id: int


class RcaControlStore:
    """SQLite control plane with raw-first persistence and create-once triggers."""

    def __init__(
        self,
        db_path: str | Path,
        *,
        busy_timeout_ms: int = 5000,
        require_current: bool = False,
        read_only: bool = False,
    ):
        self.db_path = Path(db_path).expanduser()
        if not isinstance(require_current, bool):
            raise TypeError("require_current must be true or false")
        if not isinstance(read_only, bool):
            raise TypeError("read_only must be true or false")
        if read_only and not require_current:
            raise ValueError("read_only control store requires current schema")
        self.require_current = require_current
        self.read_only = read_only
        self._read_only_snapshot_dir: tempfile.TemporaryDirectory[str] | None = None
        self._read_only_db_path: Path | None = None
        self._read_only_source_identity: dict[str, Any] | None = None
        if require_current:
            self._validate_no_installation_marker()
            self._validate_existing_path()
        else:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.busy_timeout_ms = max(1, int(busy_timeout_ms))
        if self.read_only:
            self._create_read_only_snapshot()
        self._initialization_mode = "unknown"
        self._initialization_backfill_runs = 0
        self._initialize()

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
        descriptor = os.open(
            source,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
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

    def _create_read_only_snapshot(self) -> None:
        temporary = tempfile.TemporaryDirectory(prefix="pnc-rca-control-ro-")
        root = Path(temporary.name)
        os.chmod(root, 0o700)
        snapshot_db = root / "control.sqlite3"
        sources = {
            "database": (self.db_path, snapshot_db, True),
            "wal": (Path(f"{self.db_path}-wal"), Path(f"{snapshot_db}-wal"), False),
            "shm": (Path(f"{self.db_path}-shm"), None, False),
        }
        try:
            first = {
                name: self._snapshot_file(
                    source, destination=destination, required=required
                )
                for name, (source, destination, required) in sources.items()
            }
            second = {
                name: self._snapshot_file(source, destination=None, required=required)
                for name, (source, _destination, required) in sources.items()
            }
            if first != second:
                raise RuntimeError("rca_control_store_snapshot_source_changed")
            database = first["database"]
            self._read_only_snapshot_dir = temporary
            self._read_only_db_path = snapshot_db
            self._read_only_source_identity = {
                "schema_version": "pnc_rca_control_store_source_snapshot_v1",
                "path": str(self.db_path.expanduser().absolute()),
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
        except Exception:
            temporary.cleanup()
            raise

    def control_db_source_snapshot_identity(self) -> dict[str, Any]:
        if not self.read_only or self._read_only_source_identity is None:
            raise RuntimeError("rca_control_store_source_snapshot_unavailable")
        return json.loads(_canonical_json(self._read_only_source_identity))

    @property
    def _sqlite_path(self) -> Path:
        if self.read_only:
            if self._read_only_db_path is None:
                raise RuntimeError("rca_control_store_read_only_snapshot_missing")
            return self._read_only_db_path
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
        return conn

    def _initialize(self) -> None:
        marker_value = self._preflight_schema_version()
        if self.require_current and marker_value != CONTROL_STORE_SCHEMA_VERSION:
            raise RuntimeError("rca_control_store_schema_not_current")
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
            self._initialization_mode = "migration" if migrated else "steady"
            return

        if marker_value == "pnc_rca_control_store_v11":
            migrated = self._migrate_v11_to_v12()
            migrated = self._migrate_v12_to_v13() or migrated
            self._initialization_mode = "migration" if migrated else "steady"
            return

        if marker_value == "pnc_rca_control_store_v12":
            self._initialization_mode = (
                "migration" if self._migrate_v12_to_v13() else "steady"
            )
            return

        self._initialization_mode = "migration"
        conn = self._connect()
        try:
            if self._table_exists(conn, "kafka_inbox"):
                self._migrate_inbox_columns(conn)
            if self._table_exists(conn, "rca_outbox"):
                self._migrate_outbox_columns(conn)
            conn.execute("PRAGMA foreign_keys=OFF")
            self._migrate_source_neutral_parents(conn)
            conn.execute("PRAGMA foreign_keys=ON")
            conn.executescript(
                """
                BEGIN IMMEDIATE;

                CREATE TABLE IF NOT EXISTS control_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS rca_capacity_transition_state (
                    singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
                    release_id TEXT NOT NULL,
                    bootstrap_epoch_id TEXT NOT NULL,
                    state TEXT NOT NULL CHECK (state IN (
                        'BOOTSTRAP_PRODUCTION', 'STEADY_ACTIVE'
                    )),
                    generation INTEGER NOT NULL CHECK (generation >= 1),
                    final_ledger_sha256 TEXT,
                    transition_authorization_sha256 TEXT,
                    transition_authorization_fingerprint TEXT,
                    transition_receipt_sha256 TEXT,
                    transition_receipt_fingerprint TEXT,
                    commit_marker_sha256 TEXT,
                    commit_marker_fingerprint TEXT,
                    evidence_bundle_sha256 TEXT,
                    evidence_bundle_fingerprint TEXT,
                    authorization_issued_at TEXT,
                    authorization_expires_at TEXT,
                    receipt_created_at TEXT,
                    marker_committed_at TEXT,
                    bootstrap_initialized_at TEXT NOT NULL,
                    steady_activated_at TEXT,
                    updated_at TEXT NOT NULL,
                    CHECK (
                        (
                            state = 'BOOTSTRAP_PRODUCTION'
                            AND final_ledger_sha256 IS NULL
                            AND transition_authorization_sha256 IS NULL
                            AND transition_authorization_fingerprint IS NULL
                            AND transition_receipt_sha256 IS NULL
                            AND transition_receipt_fingerprint IS NULL
                            AND commit_marker_sha256 IS NULL
                            AND commit_marker_fingerprint IS NULL
                            AND evidence_bundle_sha256 IS NULL
                            AND evidence_bundle_fingerprint IS NULL
                            AND authorization_issued_at IS NULL
                            AND authorization_expires_at IS NULL
                            AND receipt_created_at IS NULL
                            AND marker_committed_at IS NULL
                            AND steady_activated_at IS NULL
                        ) OR (
                            state = 'STEADY_ACTIVE'
                            AND final_ledger_sha256 IS NOT NULL
                            AND transition_authorization_sha256 IS NOT NULL
                            AND transition_authorization_fingerprint IS NOT NULL
                            AND transition_receipt_sha256 IS NOT NULL
                            AND transition_receipt_fingerprint IS NOT NULL
                            AND commit_marker_sha256 IS NOT NULL
                            AND commit_marker_fingerprint IS NOT NULL
                            AND evidence_bundle_sha256 IS NOT NULL
                            AND evidence_bundle_fingerprint IS NOT NULL
                            AND authorization_issued_at IS NOT NULL
                            AND authorization_expires_at IS NOT NULL
                            AND receipt_created_at IS NOT NULL
                            AND marker_committed_at IS NOT NULL
                            AND steady_activated_at IS NOT NULL
                        )
                    )
                );

                CREATE TABLE IF NOT EXISTS rca_capacity_transition_audit (
                    audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    release_id TEXT NOT NULL,
                    bootstrap_epoch_id TEXT NOT NULL,
                    from_state TEXT NOT NULL CHECK (from_state IN (
                        'UNCONFIGURED', 'BOOTSTRAP_PRODUCTION'
                    )),
                    to_state TEXT NOT NULL CHECK (to_state IN (
                        'BOOTSTRAP_PRODUCTION', 'STEADY_ACTIVE'
                    )),
                    from_generation INTEGER NOT NULL CHECK (from_generation >= 0),
                    to_generation INTEGER NOT NULL CHECK (to_generation >= 1),
                    final_ledger_sha256 TEXT,
                    transition_authorization_sha256 TEXT,
                    transition_authorization_fingerprint TEXT,
                    transition_receipt_sha256 TEXT,
                    transition_receipt_fingerprint TEXT,
                    commit_marker_sha256 TEXT,
                    commit_marker_fingerprint TEXT,
                    evidence_bundle_sha256 TEXT,
                    evidence_bundle_fingerprint TEXT,
                    authorization_issued_at TEXT,
                    authorization_expires_at TEXT,
                    receipt_created_at TEXT,
                    marker_committed_at TEXT,
                    transitioned_at TEXT NOT NULL,
                    UNIQUE (release_id, bootstrap_epoch_id, to_generation),
                    CHECK (
                        (
                            to_state = 'BOOTSTRAP_PRODUCTION'
                            AND from_state = 'UNCONFIGURED'
                            AND from_generation = 0
                            AND to_generation = 1
                            AND final_ledger_sha256 IS NULL
                            AND transition_authorization_sha256 IS NULL
                            AND transition_authorization_fingerprint IS NULL
                            AND transition_receipt_sha256 IS NULL
                            AND transition_receipt_fingerprint IS NULL
                            AND commit_marker_sha256 IS NULL
                            AND commit_marker_fingerprint IS NULL
                            AND evidence_bundle_sha256 IS NULL
                            AND evidence_bundle_fingerprint IS NULL
                            AND authorization_issued_at IS NULL
                            AND authorization_expires_at IS NULL
                            AND receipt_created_at IS NULL
                            AND marker_committed_at IS NULL
                        ) OR (
                            to_state = 'STEADY_ACTIVE'
                            AND from_state = 'BOOTSTRAP_PRODUCTION'
                            AND to_generation = from_generation + 1
                            AND final_ledger_sha256 IS NOT NULL
                            AND transition_authorization_sha256 IS NOT NULL
                            AND transition_authorization_fingerprint IS NOT NULL
                            AND transition_receipt_sha256 IS NOT NULL
                            AND transition_receipt_fingerprint IS NOT NULL
                            AND commit_marker_sha256 IS NOT NULL
                            AND commit_marker_fingerprint IS NOT NULL
                            AND evidence_bundle_sha256 IS NOT NULL
                            AND evidence_bundle_fingerprint IS NOT NULL
                            AND authorization_issued_at IS NOT NULL
                            AND authorization_expires_at IS NOT NULL
                            AND receipt_created_at IS NOT NULL
                            AND marker_committed_at IS NOT NULL
                        )
                    )
                );

                CREATE INDEX IF NOT EXISTS idx_rca_capacity_transition_audit_time
                    ON rca_capacity_transition_audit(transitioned_at, audit_id);

                CREATE TRIGGER IF NOT EXISTS trg_rca_capacity_state_no_delete
                BEFORE DELETE ON rca_capacity_transition_state
                BEGIN
                    SELECT RAISE(ABORT, 'rca_capacity_state_delete_forbidden');
                END;

                CREATE TRIGGER IF NOT EXISTS trg_rca_capacity_state_no_replace
                BEFORE INSERT ON rca_capacity_transition_state
                WHEN EXISTS (SELECT 1 FROM rca_capacity_transition_state)
                BEGIN
                    SELECT RAISE(ABORT, 'rca_capacity_state_replace_forbidden');
                END;

                CREATE TRIGGER IF NOT EXISTS trg_rca_capacity_state_identity_immutable
                BEFORE UPDATE ON rca_capacity_transition_state
                WHEN NEW.release_id != OLD.release_id
                  OR NEW.bootstrap_epoch_id != OLD.bootstrap_epoch_id
                  OR NEW.singleton_id != OLD.singleton_id
                BEGIN
                    SELECT RAISE(ABORT, 'rca_capacity_state_identity_immutable');
                END;

                CREATE TRIGGER IF NOT EXISTS trg_rca_capacity_state_bootstrap_transition
                BEFORE UPDATE ON rca_capacity_transition_state
                WHEN OLD.state = 'BOOTSTRAP_PRODUCTION'
                 AND NOT (
                    NEW.state = 'STEADY_ACTIVE'
                    AND NEW.generation = OLD.generation + 1
                 )
                BEGIN
                    SELECT RAISE(ABORT, 'rca_capacity_state_transition_invalid');
                END;

                CREATE TRIGGER IF NOT EXISTS trg_rca_capacity_state_steady_immutable
                BEFORE UPDATE ON rca_capacity_transition_state
                WHEN OLD.state = 'STEADY_ACTIVE'
                BEGIN
                    SELECT RAISE(ABORT, 'rca_capacity_state_steady_immutable');
                END;

                CREATE TRIGGER IF NOT EXISTS trg_rca_capacity_audit_no_update
                BEFORE UPDATE ON rca_capacity_transition_audit
                BEGIN
                    SELECT RAISE(ABORT, 'rca_capacity_audit_update_forbidden');
                END;

                CREATE TRIGGER IF NOT EXISTS trg_rca_capacity_audit_no_delete
                BEFORE DELETE ON rca_capacity_transition_audit
                BEGIN
                    SELECT RAISE(ABORT, 'rca_capacity_audit_delete_forbidden');
                END;

                CREATE TRIGGER IF NOT EXISTS trg_rca_capacity_audit_no_replace
                BEFORE INSERT ON rca_capacity_transition_audit
                WHEN EXISTS (
                    SELECT 1 FROM rca_capacity_transition_audit
                     WHERE audit_id = NEW.audit_id
                        OR (
                            release_id = NEW.release_id
                            AND bootstrap_epoch_id = NEW.bootstrap_epoch_id
                            AND to_generation = NEW.to_generation
                        )
                )
                BEGIN
                    SELECT RAISE(ABORT, 'rca_capacity_audit_replace_forbidden');
                END;

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

                CREATE TABLE IF NOT EXISTS rca_activation_budget_slots (
                    epoch_id TEXT NOT NULL,
                    slot_kind TEXT NOT NULL CHECK (slot_kind IN (
                        'kafka_success', 'manual_success',
                        'manual_terminal_failure'
                    )),
                    authorized_source_kind TEXT CHECK (
                        authorized_source_kind IN ('kafka', 'manual')
                    ),
                    authorized_identity_sha256 TEXT,
                    authorized_at TEXT,
                    authorized_operator TEXT,
                    authorized_reason TEXT,
                    consumed_ledger_id INTEGER UNIQUE,
                    consumed_at TEXT,
                    PRIMARY KEY(epoch_id, slot_kind),
                    FOREIGN KEY(epoch_id) REFERENCES rca_activation_epochs(epoch_id),
                    FOREIGN KEY(consumed_ledger_id)
                        REFERENCES rca_activation_admission_ledger(ledger_id)
                );

                CREATE UNIQUE INDEX IF NOT EXISTS idx_rca_activation_slot_identity
                    ON rca_activation_budget_slots(
                        epoch_id, authorized_source_kind, authorized_identity_sha256
                    )
                    WHERE authorized_identity_sha256 IS NOT NULL;

                CREATE TABLE IF NOT EXISTS rca_activation_admission_ledger (
                    ledger_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    epoch_id TEXT NOT NULL,
                    admission_key TEXT NOT NULL,
                    entrypoint TEXT NOT NULL CHECK (entrypoint IN (
                        'kafka_ingest', 'manual_admit', 'shadow_promotion'
                    )),
                    source_kind TEXT NOT NULL CHECK (source_kind IN ('kafka', 'manual')),
                    source_identity_sha256 TEXT NOT NULL,
                    slot_kind TEXT CHECK (slot_kind IN (
                        'kafka_success', 'manual_success',
                        'manual_terminal_failure'
                    )),
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
                    activation_slot_kind TEXT,
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
            self._create_v13_historical_outbox_hold_schema(conn)
            if self._learning_delivery_schema_present(conn):
                self._ensure_learning_lane_cohort_tx(
                    conn, sealed_at=_now_iso()
                )
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
        """Install the immutable historical-outbox hold and disposition schema."""
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
            if marker_value != "pnc_rca_control_store_v12":
                raise RuntimeError("incompatible_control_store_schema:version_marker")
            self._validate_v12_learning_lane_schema(conn)
            self._drop_v13_historical_outbox_hold_triggers(conn)
            self._create_v13_historical_outbox_hold_schema(conn)
            self._validate_structural_contract(conn, integrity_check=True)
            updated = conn.execute(
                "UPDATE control_meta SET value = ? "
                "WHERE key = 'schema_version' AND value = ?",
                (CONTROL_STORE_SCHEMA_VERSION, "pnc_rca_control_store_v12"),
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
    def _v13_historical_outbox_hold_trigger_names() -> tuple[str, ...]:
        return (
            "trg_activation_historical_hold_no_update",
            "trg_activation_historical_hold_no_delete",
            "trg_activation_historical_hold_no_replace",
            "trg_activation_historical_hold_item_no_append",
            "trg_activation_historical_hold_item_no_update",
            "trg_activation_historical_hold_item_no_delete",
            "trg_activation_historical_disposition_no_update",
            "trg_activation_historical_disposition_no_delete",
            "trg_activation_historical_disposition_no_replace",
            "trg_activation_historical_disposition_item_no_append",
            "trg_activation_historical_disposition_item_no_update",
            "trg_activation_historical_disposition_item_no_delete",
            "trg_activation_historical_outbox_no_update",
            "trg_activation_historical_outbox_disposition_guard",
            "trg_activation_historical_outbox_no_delete",
        )

    @staticmethod
    def _v13_historical_outbox_hold_schema_statements() -> tuple[str, ...]:
        immutable_guard = " OR ".join(
            f"NEW.{field} IS NOT OLD.{field}"
            for field in ACTIVATION_HISTORICAL_OUTBOX_IMMUTABLE_ROW_FIELDS
        )
        return (
            f"""
            CREATE TABLE IF NOT EXISTS rca_activation_historical_outbox_holds (
                epoch_id TEXT PRIMARY KEY,
                schema_version TEXT NOT NULL CHECK(
                    schema_version =
                        '{ACTIVATION_HISTORICAL_OUTBOX_HOLD_SCHEMA_VERSION}'
                ),
                partition_start_fence_sha256 TEXT NOT NULL CHECK(
                    length(partition_start_fence_sha256) = 64
                    AND partition_start_fence_sha256 NOT GLOB '*[^0-9a-f]*'
                ),
                cohort_count INTEGER NOT NULL CHECK(cohort_count >= 0),
                cohort_sha256 TEXT NOT NULL CHECK(
                    length(cohort_sha256) = 64
                    AND cohort_sha256 NOT GLOB '*[^0-9a-f]*'
                ),
                sealed_at TEXT NOT NULL CHECK(length(trim(sealed_at)) > 0),
                FOREIGN KEY(epoch_id) REFERENCES rca_activation_epochs(epoch_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS
                rca_activation_historical_outbox_hold_items (
                    epoch_id TEXT NOT NULL,
                    outbox_id INTEGER NOT NULL,
                    row_sha256 TEXT NOT NULL CHECK(
                        length(row_sha256) = 64
                        AND row_sha256 NOT GLOB '*[^0-9a-f]*'
                    ),
                    immutable_row_sha256 TEXT NOT NULL CHECK(
                        length(immutable_row_sha256) = 64
                        AND immutable_row_sha256 NOT GLOB '*[^0-9a-f]*'
                    ),
                    PRIMARY KEY(epoch_id, outbox_id),
                    FOREIGN KEY(epoch_id)
                        REFERENCES rca_activation_historical_outbox_holds(epoch_id),
                    FOREIGN KEY(outbox_id) REFERENCES rca_outbox(outbox_id)
                )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS
                rca_activation_historical_outbox_dispositions (
                    disposition_id TEXT PRIMARY KEY CHECK(
                        disposition_id = 'rca-hold-disposition-v1-' || disposition_sha256
                    ),
                    epoch_id TEXT NOT NULL UNIQUE,
                    epoch_state TEXT NOT NULL CHECK(epoch_state = 'aborted'),
                    schema_version TEXT NOT NULL CHECK(
                        schema_version =
                            '{ACTIVATION_HISTORICAL_OUTBOX_DISPOSITION_SCHEMA_VERSION}'
                    ),
                    hold_schema_version TEXT NOT NULL CHECK(
                        hold_schema_version =
                            '{ACTIVATION_HISTORICAL_OUTBOX_HOLD_SCHEMA_VERSION}'
                    ),
                    row_schema_version TEXT NOT NULL CHECK(
                        row_schema_version =
                            '{ACTIVATION_HISTORICAL_OUTBOX_ROW_SCHEMA_VERSION}'
                    ),
                    cohort_count INTEGER NOT NULL CHECK(cohort_count >= 0),
                    cohort_sha256 TEXT NOT NULL CHECK(
                        length(cohort_sha256) = 64
                        AND cohort_sha256 NOT GLOB '*[^0-9a-f]*'
                    ),
                    owner_authorized INTEGER NOT NULL CHECK(owner_authorized = 1),
                    operator TEXT NOT NULL CHECK(length(trim(operator)) > 0),
                    reason TEXT NOT NULL CHECK(length(trim(reason)) > 0),
                    disposed_at TEXT NOT NULL CHECK(length(trim(disposed_at)) > 0),
                    disposition_sha256 TEXT NOT NULL UNIQUE CHECK(
                        length(disposition_sha256) = 64
                        AND disposition_sha256 NOT GLOB '*[^0-9a-f]*'
                    ),
                    FOREIGN KEY(epoch_id)
                        REFERENCES rca_activation_historical_outbox_holds(epoch_id)
                )
            """,
            """
            CREATE TABLE IF NOT EXISTS
                rca_activation_historical_outbox_disposition_items (
                    disposition_id TEXT NOT NULL,
                    outbox_id INTEGER NOT NULL,
                    row_sha256 TEXT NOT NULL CHECK(
                        length(row_sha256) = 64
                        AND row_sha256 NOT GLOB '*[^0-9a-f]*'
                    ),
                    immutable_row_sha256 TEXT NOT NULL CHECK(
                        length(immutable_row_sha256) = 64
                        AND immutable_row_sha256 NOT GLOB '*[^0-9a-f]*'
                    ),
                    PRIMARY KEY(disposition_id, outbox_id),
                    FOREIGN KEY(disposition_id) REFERENCES
                        rca_activation_historical_outbox_dispositions(disposition_id),
                    FOREIGN KEY(outbox_id) REFERENCES rca_outbox(outbox_id)
                )
            """,
            """
            CREATE TRIGGER IF NOT EXISTS trg_activation_historical_hold_no_update
            BEFORE UPDATE ON rca_activation_historical_outbox_holds
            BEGIN
                SELECT RAISE(
                    ABORT, 'activation_historical_hold_seal_update_forbidden'
                );
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS trg_activation_historical_hold_no_delete
            BEFORE DELETE ON rca_activation_historical_outbox_holds
            BEGIN
                SELECT RAISE(
                    ABORT, 'activation_historical_hold_seal_delete_forbidden'
                );
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS trg_activation_historical_hold_no_replace
            BEFORE INSERT ON rca_activation_historical_outbox_holds
            WHEN EXISTS (
                SELECT 1 FROM rca_activation_historical_outbox_holds
                 WHERE epoch_id = NEW.epoch_id
            )
            BEGIN
                SELECT RAISE(
                    ABORT, 'activation_historical_hold_seal_replace_forbidden'
                );
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS
                trg_activation_historical_hold_item_no_append
            BEFORE INSERT ON rca_activation_historical_outbox_hold_items
            WHEN (
                SELECT COUNT(*)
                  FROM rca_activation_historical_outbox_hold_items
                 WHERE epoch_id = NEW.epoch_id
            ) >= (
                SELECT cohort_count
                  FROM rca_activation_historical_outbox_holds
                 WHERE epoch_id = NEW.epoch_id
            )
            BEGIN
                SELECT RAISE(
                    ABORT, 'activation_historical_hold_item_append_forbidden'
                );
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS
                trg_activation_historical_hold_item_no_update
            BEFORE UPDATE ON rca_activation_historical_outbox_hold_items
            BEGIN
                SELECT RAISE(
                    ABORT, 'activation_historical_hold_item_update_forbidden'
                );
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS
                trg_activation_historical_hold_item_no_delete
            BEFORE DELETE ON rca_activation_historical_outbox_hold_items
            BEGIN
                SELECT RAISE(
                    ABORT, 'activation_historical_hold_item_delete_forbidden'
                );
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS
                trg_activation_historical_disposition_no_update
            BEFORE UPDATE ON rca_activation_historical_outbox_dispositions
            BEGIN
                SELECT RAISE(
                    ABORT, 'activation_historical_disposition_update_forbidden'
                );
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS
                trg_activation_historical_disposition_no_delete
            BEFORE DELETE ON rca_activation_historical_outbox_dispositions
            BEGIN
                SELECT RAISE(
                    ABORT, 'activation_historical_disposition_delete_forbidden'
                );
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS
                trg_activation_historical_disposition_no_replace
            BEFORE INSERT ON rca_activation_historical_outbox_dispositions
            WHEN EXISTS (
                SELECT 1 FROM rca_activation_historical_outbox_dispositions
                 WHERE disposition_id = NEW.disposition_id
                    OR epoch_id = NEW.epoch_id
                    OR disposition_sha256 = NEW.disposition_sha256
            )
            BEGIN
                SELECT RAISE(
                    ABORT, 'activation_historical_disposition_replace_forbidden'
                );
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS
                trg_activation_historical_disposition_item_no_append
            BEFORE INSERT ON rca_activation_historical_outbox_disposition_items
            WHEN (
                SELECT COUNT(*)
                  FROM rca_activation_historical_outbox_disposition_items
                 WHERE disposition_id = NEW.disposition_id
            ) >= (
                SELECT cohort_count
                  FROM rca_activation_historical_outbox_dispositions
                 WHERE disposition_id = NEW.disposition_id
            )
            BEGIN
                SELECT RAISE(
                    ABORT, 'activation_historical_disposition_item_append_forbidden'
                );
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS
                trg_activation_historical_disposition_item_no_update
            BEFORE UPDATE ON rca_activation_historical_outbox_disposition_items
            BEGIN
                SELECT RAISE(
                    ABORT, 'activation_historical_disposition_item_update_forbidden'
                );
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS
                trg_activation_historical_disposition_item_no_delete
            BEFORE DELETE ON rca_activation_historical_outbox_disposition_items
            BEGIN
                SELECT RAISE(
                    ABORT, 'activation_historical_disposition_item_delete_forbidden'
                );
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS trg_activation_historical_outbox_no_update
            BEFORE UPDATE ON rca_outbox
            WHEN EXISTS (
                SELECT 1
                  FROM rca_activation_historical_outbox_hold_items AS held
                 WHERE held.outbox_id = OLD.outbox_id
                   AND NOT EXISTS (
                       SELECT 1
                         FROM rca_activation_historical_outbox_disposition_items AS disposed
                        WHERE disposed.outbox_id = held.outbox_id
                   )
            )
            BEGIN
                SELECT RAISE(
                    ABORT, 'activation_historical_outbox_update_forbidden'
                );
            END
            """,
            f"""
            CREATE TRIGGER IF NOT EXISTS
                trg_activation_historical_outbox_disposition_guard
            BEFORE UPDATE ON rca_outbox
            WHEN EXISTS (
                SELECT 1
                  FROM rca_activation_historical_outbox_disposition_items AS disposed
                 WHERE disposed.outbox_id = OLD.outbox_id
            ) AND NOT (
                OLD.status = 'pending'
                AND NEW.status = 'quarantined'
                AND NEW.next_attempt_at IS NULL
                AND NEW.lease_token IS NULL
                AND NEW.lease_owner IS NULL
                AND NEW.lease_expires_at IS NULL
                AND NEW.claimed_at IS NULL
                AND NEW.completed_at IS NULL
                AND NEW.result_json IS NULL
                AND NEW.quarantined_at IS NOT NULL
                AND NEW.updated_at = NEW.quarantined_at
                AND NEW.last_error_code =
                    '{ACTIVATION_HISTORICAL_OUTBOX_DISPOSITION_ERROR_CODE}'
                AND NEW.last_error_detail = 'owner_audited_disposition'
                AND NOT ({immutable_guard})
                AND EXISTS (
                    SELECT 1
                      FROM rca_activation_historical_outbox_disposition_items AS disposed
                      JOIN rca_activation_historical_outbox_dispositions AS disposition
                        ON disposition.disposition_id = disposed.disposition_id
                      JOIN rca_activation_historical_outbox_hold_items AS held
                        ON held.epoch_id = disposition.epoch_id
                       AND held.outbox_id = disposed.outbox_id
                       AND held.row_sha256 = disposed.row_sha256
                       AND held.immutable_row_sha256 =
                           disposed.immutable_row_sha256
                     WHERE disposed.outbox_id = OLD.outbox_id
                       AND disposition.disposed_at = NEW.quarantined_at
                )
            )
            BEGIN
                SELECT RAISE(
                    ABORT, 'activation_historical_disposed_outbox_update_forbidden'
                );
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS trg_activation_historical_outbox_no_delete
            BEFORE DELETE ON rca_outbox
            WHEN EXISTS (
                SELECT 1
                  FROM rca_activation_historical_outbox_hold_items AS held
                 WHERE held.outbox_id = OLD.outbox_id
            )
            BEGIN
                SELECT RAISE(
                    ABORT, 'activation_historical_outbox_delete_forbidden'
                );
            END
            """,
        )

    @classmethod
    def _drop_v13_historical_outbox_hold_triggers(
        cls,
        conn: sqlite3.Connection,
    ) -> None:
        for name in cls._v13_historical_outbox_hold_trigger_names():
            conn.execute(f"DROP TRIGGER IF EXISTS {name}")

    @classmethod
    def _create_v13_historical_outbox_hold_schema(
        cls,
        conn: sqlite3.Connection,
    ) -> None:
        """Create immutable per-epoch seals and owner-audited disposition records."""
        for statement in cls._v13_historical_outbox_hold_schema_statements():
            conn.execute(statement)

    @classmethod
    def _validate_v13_historical_outbox_hold_schema(
        cls,
        conn: sqlite3.Connection,
    ) -> None:
        normalize_sql = lambda value: " ".join(str(value).split()).rstrip(";")
        statements = cls._v13_historical_outbox_hold_schema_statements()
        expected_tables: dict[str, str] = {}
        expected_triggers: dict[str, str] = {}
        for statement in statements:
            normalized = normalize_sql(statement)
            if normalized.startswith("CREATE TABLE IF NOT EXISTS "):
                name = normalized[len("CREATE TABLE IF NOT EXISTS ") :].split(" ", 1)[0]
                expected_tables[name] = normalized.replace(
                    "CREATE TABLE IF NOT EXISTS ", "CREATE TABLE ", 1
                )
            elif normalized.startswith("CREATE TRIGGER IF NOT EXISTS "):
                name = normalized[len("CREATE TRIGGER IF NOT EXISTS ") :].split(" ", 1)[0]
                expected_triggers[name] = normalized.replace(
                    "CREATE TRIGGER IF NOT EXISTS ", "CREATE TRIGGER ", 1
                )
        observed_tables = {
            str(row["name"]): normalize_sql(row["sql"] or "")
            for row in conn.execute(
                "SELECT name, sql FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
            if str(row["name"]) in expected_tables
        }
        historical_table_names = {
            str(row["name"])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' "
                "AND name LIKE 'rca_activation_historical_outbox_%'"
            ).fetchall()
        }
        if (
            observed_tables != expected_tables
            or historical_table_names != set(expected_tables)
        ):
            raise RuntimeError(
                "incompatible_control_store_schema:historical_outbox_hold_table_sql"
            )
        observed_triggers = {
            str(row["name"]): normalize_sql(row["sql"] or "")
            for row in conn.execute(
                "SELECT name, sql FROM sqlite_master WHERE type = 'trigger'"
            ).fetchall()
            if str(row["name"]) in expected_triggers
        }
        historical_trigger_names = {
            str(row["name"])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'trigger' "
                "AND name LIKE 'trg_activation_historical_%'"
            ).fetchall()
        }
        if (
            observed_triggers != expected_triggers
            or historical_trigger_names != set(expected_triggers)
        ):
            raise RuntimeError(
                "incompatible_control_store_schema:historical_outbox_hold_trigger_sql"
            )

        orphan = conn.execute(
            """
            SELECT 1
              FROM rca_activation_historical_outbox_dispositions AS disposition
         LEFT JOIN rca_activation_historical_outbox_holds AS held
                ON held.epoch_id = disposition.epoch_id
             WHERE held.epoch_id IS NULL
             LIMIT 1
            """
        ).fetchone()
        if orphan is not None:
            raise RuntimeError(
                "incompatible_control_store_schema:historical_outbox_disposition_orphan"
            )
        orphan_hold_item = conn.execute(
            """
            SELECT 1
              FROM rca_activation_historical_outbox_hold_items AS item
         LEFT JOIN rca_activation_historical_outbox_holds AS held
                ON held.epoch_id = item.epoch_id
         LEFT JOIN rca_outbox AS outbox
                ON outbox.outbox_id = item.outbox_id
             WHERE held.epoch_id IS NULL OR outbox.outbox_id IS NULL
             LIMIT 1
            """
        ).fetchone()
        if orphan_hold_item is not None:
            raise RuntimeError(
                "incompatible_control_store_schema:historical_outbox_hold_item_orphan"
            )
        orphan_disposition_item = conn.execute(
            """
            SELECT 1
              FROM rca_activation_historical_outbox_disposition_items AS item
         LEFT JOIN rca_activation_historical_outbox_dispositions AS disposition
                ON disposition.disposition_id = item.disposition_id
         LEFT JOIN rca_outbox AS outbox
                ON outbox.outbox_id = item.outbox_id
             WHERE disposition.disposition_id IS NULL
                OR outbox.outbox_id IS NULL
             LIMIT 1
            """
        ).fetchone()
        if orphan_disposition_item is not None:
            raise RuntimeError(
                "incompatible_control_store_schema:"
                "historical_outbox_disposition_item_orphan"
            )
        for hold in conn.execute(
            "SELECT * FROM rca_activation_historical_outbox_holds ORDER BY epoch_id"
        ).fetchall():
            epoch = conn.execute(
                "SELECT state, partition_start_fence_sha256 "
                "FROM rca_activation_epochs WHERE epoch_id = ?",
                (hold["epoch_id"],),
            ).fetchone()
            if (
                epoch is None
                or str(hold["schema_version"])
                != ACTIVATION_HISTORICAL_OUTBOX_HOLD_SCHEMA_VERSION
                or str(hold["partition_start_fence_sha256"])
                != str(epoch["partition_start_fence_sha256"])
            ):
                raise RuntimeError(
                    "incompatible_control_store_schema:historical_outbox_hold_binding"
                )
            items = [
                {
                    "outbox_id": int(row["outbox_id"]),
                    "row_sha256": str(row["row_sha256"]),
                    "immutable_row_sha256": str(row["immutable_row_sha256"]),
                }
                for row in conn.execute(
                    "SELECT outbox_id, row_sha256, immutable_row_sha256 "
                    "FROM rca_activation_historical_outbox_hold_items "
                    "WHERE epoch_id = ? ORDER BY outbox_id",
                    (hold["epoch_id"],),
                ).fetchall()
            ]
            if len(items) != int(hold["cohort_count"]) or _canonical_sha256(
                items
            ) != str(hold["cohort_sha256"]):
                raise RuntimeError(
                    "incompatible_control_store_schema:historical_outbox_hold_seal"
                )
            disposition = conn.execute(
                "SELECT * FROM rca_activation_historical_outbox_dispositions "
                "WHERE epoch_id = ?",
                (hold["epoch_id"],),
            ).fetchone()
            if disposition is None:
                for item in items:
                    outbox = conn.execute(
                        "SELECT * FROM rca_outbox WHERE outbox_id = ?",
                        (item["outbox_id"],),
                    ).fetchone()
                    if (
                        outbox is None
                        or cls._historical_outbox_row_sha256(outbox)
                        != item["row_sha256"]
                        or cls._historical_outbox_immutable_row_sha256(outbox)
                        != item["immutable_row_sha256"]
                    ):
                        raise RuntimeError(
                            "incompatible_control_store_schema:"
                            "historical_outbox_hold_row_binding"
                        )
                continue
            if str(epoch["state"]) != "aborted":
                raise RuntimeError(
                    "incompatible_control_store_schema:"
                    "historical_outbox_disposition_epoch_state"
                )
            disposed_items = [
                {
                    "outbox_id": int(row["outbox_id"]),
                    "row_sha256": str(row["row_sha256"]),
                    "immutable_row_sha256": str(row["immutable_row_sha256"]),
                }
                for row in conn.execute(
                    "SELECT outbox_id, row_sha256, immutable_row_sha256 "
                    "FROM rca_activation_historical_outbox_disposition_items "
                    "WHERE disposition_id = ? ORDER BY outbox_id",
                    (disposition["disposition_id"],),
                ).fetchall()
            ]
            binding = cls._historical_outbox_disposition_binding(
                epoch_id=str(disposition["epoch_id"]),
                cohort_count=int(disposition["cohort_count"]),
                cohort_sha256=str(disposition["cohort_sha256"]),
                operator=str(disposition["operator"]),
                reason=str(disposition["reason"]),
                disposed_at=str(disposition["disposed_at"]),
            )
            disposition_sha256 = _canonical_sha256(binding)
            if (
                str(disposition["schema_version"])
                != ACTIVATION_HISTORICAL_OUTBOX_DISPOSITION_SCHEMA_VERSION
                or str(disposition["epoch_id"]) != str(hold["epoch_id"])
                or str(disposition["epoch_state"]) != "aborted"
                or str(disposition["hold_schema_version"])
                != ACTIVATION_HISTORICAL_OUTBOX_HOLD_SCHEMA_VERSION
                or str(disposition["row_schema_version"])
                != ACTIVATION_HISTORICAL_OUTBOX_ROW_SCHEMA_VERSION
                or int(disposition["cohort_count"]) != int(hold["cohort_count"])
                or str(disposition["cohort_sha256"]) != str(hold["cohort_sha256"])
                or int(disposition["owner_authorized"]) != 1
                or disposed_items != items
                or str(disposition["disposition_sha256"]) != disposition_sha256
                or str(disposition["disposition_id"])
                != f"rca-hold-disposition-v1-{disposition_sha256}"
            ):
                raise RuntimeError(
                    "incompatible_control_store_schema:"
                    "historical_outbox_disposition_binding"
                )
            for item in disposed_items:
                outbox = conn.execute(
                    "SELECT * FROM rca_outbox WHERE outbox_id = ?",
                    (item["outbox_id"],),
                ).fetchone()
                if (
                    outbox is None
                    or cls._historical_outbox_immutable_row_sha256(outbox)
                    != item["immutable_row_sha256"]
                    or str(outbox["status"]) != "quarantined"
                    or outbox["next_attempt_at"] is not None
                    or str(outbox["quarantined_at"] or "")
                    != str(disposition["disposed_at"])
                    or str(outbox["last_error_code"] or "")
                    != ACTIVATION_HISTORICAL_OUTBOX_DISPOSITION_ERROR_CODE
                    or str(outbox["last_error_detail"] or "")
                    != "owner_audited_disposition"
                    or str(outbox["updated_at"] or "")
                    != str(disposition["disposed_at"])
                ):
                    raise RuntimeError(
                        "incompatible_control_store_schema:"
                        "historical_outbox_disposition_row_binding"
                    )

    def _preflight_schema_version(self) -> str | None:
        """Reject a future schema using a read-only connection before any pragma/DDL."""
        sqlite_path = self._sqlite_path
        if not sqlite_path.is_file() or sqlite_path.stat().st_size == 0:
            return None
        uri = f"{sqlite_path.resolve().as_uri()}?mode=ro"
        try:
            conn = sqlite3.connect(uri, uri=True)
            conn.row_factory = sqlite3.Row
            table = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'control_meta'"
            ).fetchone()
            if table is None:
                return None
            marker = conn.execute(
                "SELECT value FROM control_meta WHERE key = 'schema_version'"
            ).fetchone()
        except sqlite3.Error as exc:
            raise RuntimeError("incompatible_control_store_schema:preflight") from exc
        finally:
            if "conn" in locals():
                conn.close()
        if (
            marker is not None
            and str(marker["value"]) not in SUPPORTED_CONTROL_STORE_SCHEMA_VERSIONS
        ):
            raise RuntimeError("incompatible_control_store_schema:version")
        return str(marker["value"]) if marker is not None else None

    def _validate_current_schema_read_only(self) -> None:
        """Validate fixed-size schema metadata without taking a SQLite write lock."""
        uri = f"{self._sqlite_path.resolve().as_uri()}?mode=ro"
        conn = sqlite3.connect(uri, uri=True)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA recursive_triggers=ON")
            conn.execute("BEGIN")
            self._validate_structural_contract(conn, integrity_check=False)
            self._validate_v12_learning_lane_schema(conn)
            self._validate_v13_historical_outbox_hold_schema(conn)
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
    def _normalize_capacity_identity(value: str, field: str) -> str:
        normalized = str(value or "").strip()
        if _ACTIVATION_EPOCH_ID_RE.fullmatch(normalized) is None:
            raise CapacityTransitionStateError(f"capacity_{field}_invalid")
        return normalized

    @staticmethod
    def _normalize_capacity_sha256(value: str, field: str) -> str:
        normalized = str(value or "").strip().lower()
        if _ACTIVATION_SHA256_RE.fullmatch(normalized) is None:
            raise CapacityTransitionStateError(f"capacity_{field}_invalid")
        return normalized

    @staticmethod
    def _normalize_capacity_timestamp(value: str, field: str) -> str:
        text = str(value or "").strip()
        candidate = text[:-1] + "+00:00" if text.endswith("Z") else text
        try:
            parsed = datetime.fromisoformat(candidate)
        except (TypeError, ValueError, OverflowError) as exc:
            raise CapacityTransitionStateError(f"capacity_{field}_invalid") from exc
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise CapacityTransitionStateError(f"capacity_{field}_invalid")
        return parsed.astimezone(timezone.utc).isoformat()

    @staticmethod
    def _capacity_transition_state_tx(
        conn: sqlite3.Connection,
    ) -> sqlite3.Row | None:
        return conn.execute(
            "SELECT * FROM rca_capacity_transition_state WHERE singleton_id = 1"
        ).fetchone()

    @staticmethod
    def _public_capacity_transition_state(row: sqlite3.Row) -> dict[str, Any]:
        value = dict(row)
        value["singleton_id"] = int(value["singleton_id"])
        value["generation"] = int(value["generation"])
        return value

    @staticmethod
    def _capacity_transition_integrity_error_tx(conn: sqlite3.Connection) -> str:
        capacity_rows = conn.execute(
            "SELECT * FROM rca_capacity_transition_state"
        ).fetchall()
        total_audits = int(
            conn.execute("SELECT COUNT(*) FROM rca_capacity_transition_audit").fetchone()[
                0
            ]
        )
        if len(capacity_rows) > 1:
            return "capacity_transition_singleton"
        if not capacity_rows:
            return "capacity_transition_orphan_audit" if total_audits else ""

        capacity_row = capacity_rows[0]
        if int(capacity_row["singleton_id"]) != 1:
            return "capacity_transition_singleton"
        for field in ("release_id", "bootstrap_epoch_id"):
            raw_identity = str(capacity_row[field] or "")
            if (
                raw_identity != raw_identity.strip()
                or _ACTIVATION_EPOCH_ID_RE.fullmatch(raw_identity) is None
            ):
                return "capacity_transition_identity"

        durable_state = str(capacity_row["state"])
        durable_generation = int(capacity_row["generation"])
        expected_generation = (
            1
            if durable_state == CAPACITY_BOOTSTRAP_PRODUCTION
            else 2
            if durable_state == CAPACITY_STEADY_ACTIVE
            else -1
        )
        if durable_generation != expected_generation:
            return "capacity_transition_generation"

        evidence_fields = (
            "final_ledger_sha256",
            "transition_authorization_sha256",
            "transition_authorization_fingerprint",
            "transition_receipt_sha256",
            "transition_receipt_fingerprint",
            "commit_marker_sha256",
            "commit_marker_fingerprint",
            "evidence_bundle_sha256",
            "evidence_bundle_fingerprint",
        )
        transition_time_fields = (
            "authorization_issued_at",
            "authorization_expires_at",
            "receipt_created_at",
            "marker_committed_at",
        )

        def strict_timestamp(value: Any) -> datetime:
            text = str(value or "")
            parsed = datetime.fromisoformat(text)
            if (
                parsed.tzinfo is None
                or parsed.utcoffset() is None
                or text != parsed.astimezone(timezone.utc).isoformat()
            ):
                raise ValueError("timestamp is not canonical UTC")
            return parsed.astimezone(timezone.utc)

        try:
            initialized = strict_timestamp(capacity_row["bootstrap_initialized_at"])
            updated = strict_timestamp(capacity_row["updated_at"])
        except (TypeError, ValueError, OverflowError):
            return "capacity_transition_time"
        if initialized > updated:
            return "capacity_transition_time"

        audit_rows = conn.execute(
            """
            SELECT * FROM rca_capacity_transition_audit
             WHERE release_id = ? AND bootstrap_epoch_id = ?
             ORDER BY to_generation
            """,
            (
                capacity_row["release_id"],
                capacity_row["bootstrap_epoch_id"],
            ),
        ).fetchall()
        if (
            len(audit_rows) != durable_generation
            or str(audit_rows[0]["from_state"]) != "UNCONFIGURED"
            or str(audit_rows[0]["to_state"]) != CAPACITY_BOOTSTRAP_PRODUCTION
            or int(audit_rows[0]["from_generation"]) != 0
            or int(audit_rows[0]["to_generation"]) != 1
            or any(
                audit_rows[0][field] is not None
                for field in evidence_fields + transition_time_fields
            )
            or str(audit_rows[0]["transitioned_at"])
            != str(capacity_row["bootstrap_initialized_at"])
        ):
            return "capacity_transition_audit_chain"

        if durable_state == CAPACITY_BOOTSTRAP_PRODUCTION:
            if (
                any(capacity_row[field] is not None for field in evidence_fields)
                or any(
                    capacity_row[field] is not None
                    for field in transition_time_fields
                )
                or capacity_row["steady_activated_at"] is not None
                or initialized != updated
            ):
                return "capacity_transition_bootstrap_binding"
        else:
            if any(
                _ACTIVATION_SHA256_RE.fullmatch(str(capacity_row[field] or ""))
                is None
                for field in evidence_fields
            ):
                return "capacity_transition_evidence"
            try:
                issued = strict_timestamp(capacity_row["authorization_issued_at"])
                expires = strict_timestamp(capacity_row["authorization_expires_at"])
                receipt_created = strict_timestamp(capacity_row["receipt_created_at"])
                marker_committed = strict_timestamp(capacity_row["marker_committed_at"])
                activated = strict_timestamp(capacity_row["steady_activated_at"])
            except (TypeError, ValueError, OverflowError):
                return "capacity_transition_time"
            if not (
                initialized <= issued <= receipt_created <= marker_committed
                <= expires
                and marker_committed <= activated == updated
                and issued < expires
            ):
                return "capacity_transition_time"
            steady_audit = audit_rows[1]
            if (
                str(steady_audit["from_state"])
                != CAPACITY_BOOTSTRAP_PRODUCTION
                or str(steady_audit["to_state"]) != CAPACITY_STEADY_ACTIVE
                or int(steady_audit["from_generation"]) != 1
                or int(steady_audit["to_generation"]) != 2
                or any(
                    steady_audit[field] != capacity_row[field]
                    for field in evidence_fields + transition_time_fields
                )
                or str(steady_audit["transitioned_at"])
                != str(capacity_row["steady_activated_at"])
            ):
                return "capacity_transition_audit_binding"
        if total_audits != durable_generation:
            return "capacity_transition_audit_scope"
        return ""

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
        if not isinstance(value, Mapping) or not value:
            raise ActivationEpochError("activation_partition_fence_invalid")
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
            automation_authority = value.get("automation_authority")
            if automation_authority is not None:
                try:
                    normalized["automation_authority"] = (
                        normalize_gray_sample_automation_authority(
                            automation_authority
                        )
                    )
                except ValueError as exc:
                    raise ActivationEpochError(str(exc)) from exc
        canonical = _canonical_json(normalized)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest(), normalized

    @staticmethod
    def _historical_outbox_row_sha256(row: sqlite3.Row) -> str:
        """Hash the stable v1 outbox projection, ignoring later additive columns."""
        try:
            projection = {
                field: row[field] for field in ACTIVATION_HISTORICAL_OUTBOX_ROW_FIELDS
            }
        except (IndexError, KeyError) as exc:
            raise RuntimeError(
                "incompatible_control_store_schema:historical_outbox_row_projection"
            ) from exc
        return _canonical_sha256(projection)

    @staticmethod
    def _historical_outbox_immutable_row_sha256(row: sqlite3.Row) -> str:
        """Hash the v1 identity that must survive owner-audited disposition."""
        try:
            projection = {
                field: row[field]
                for field in ACTIVATION_HISTORICAL_OUTBOX_IMMUTABLE_ROW_FIELDS
            }
        except (IndexError, KeyError) as exc:
            raise RuntimeError(
                "incompatible_control_store_schema:"
                "historical_outbox_immutable_row_projection"
            ) from exc
        return _canonical_sha256(projection)

    @staticmethod
    def _historical_outbox_disposition_binding(
        *,
        epoch_id: str,
        cohort_count: int,
        cohort_sha256: str,
        operator: str,
        reason: str,
        disposed_at: str,
    ) -> dict[str, Any]:
        return {
            "schema_version": ACTIVATION_HISTORICAL_OUTBOX_DISPOSITION_SCHEMA_VERSION,
            "hold_schema_version": ACTIVATION_HISTORICAL_OUTBOX_HOLD_SCHEMA_VERSION,
            "row_schema_version": ACTIVATION_HISTORICAL_OUTBOX_ROW_SCHEMA_VERSION,
            "epoch_id": epoch_id,
            "epoch_state": "aborted",
            "cohort_count": cohort_count,
            "cohort_sha256": cohort_sha256,
            "owner_authorized": True,
            "operator": operator,
            "reason": reason,
            "disposed_at": disposed_at,
        }

    @classmethod
    def _historical_outbox_hold_snapshot_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        epoch: sqlite3.Row,
        allow_current_epoch: bool,
    ) -> list[dict[str, Any]]:
        epoch_id = str(epoch["epoch_id"])
        start_fence = json.loads(str(epoch["partition_start_fence_json"]))
        items: list[dict[str, Any]] = []
        for row in conn.execute(
            "SELECT * FROM rca_outbox "
            "WHERE status IN ('pending', 'claimed', 'shadow') ORDER BY outbox_id"
        ).fetchall():
            bound_epoch = row["activation_epoch_id"]
            bound_ledger = row["activation_ledger_id"]
            if bound_epoch is not None or bound_ledger is not None:
                if allow_current_epoch and str(bound_epoch or "") == epoch_id:
                    if bound_ledger is None:
                        raise ActivationEpochError(
                            "activation_historical_hold_current_epoch_binding_invalid"
                        )
                    binding = conn.execute(
                        """
                        SELECT ledger.decision, ledger.bound_at,
                               trigger.state AS trigger_state
                          FROM rca_activation_admission_ledger AS ledger
                          JOIN business_triggers AS trigger
                            ON trigger.activation_epoch_id = ledger.epoch_id
                           AND trigger.activation_ledger_id = ledger.ledger_id
                           AND trigger.business_key = ledger.business_key
                           AND trigger.submission_key = ledger.submission_key
                           AND trigger.generation = ledger.generation
                         WHERE ledger.epoch_id = ? AND ledger.ledger_id = ?
                           AND ledger.business_key = ?
                           AND ledger.submission_key = ?
                           AND ledger.generation = ?
                        """,
                        (
                            epoch_id,
                            bound_ledger,
                            row["business_key"],
                            row["submission_key"],
                            row["generation"],
                        ),
                    ).fetchone()
                    status = str(row["status"] or "")
                    expected = {
                        "pending": ("admit", "pending"),
                        "claimed": ("admit", "dispatching"),
                        "shadow": ("shadow", "shadow"),
                    }.get(status)
                    if (
                        binding is None
                        or expected is None
                        or not str(binding["bound_at"] or "")
                        or str(binding["decision"] or "") != expected[0]
                        or str(binding["trigger_state"] or "") != expected[1]
                    ):
                        raise ActivationEpochError(
                            "activation_historical_hold_current_epoch_binding_invalid"
                        )
                    continue
                raise ActivationEpochError(
                    "activation_historical_hold_outbox_activation_bound"
                )
            status = str(row["status"] or "")
            if status == "claimed":
                raise ActivationEpochError("activation_historical_hold_outbox_claimed")
            if status == "shadow":
                raise ActivationEpochError("activation_historical_hold_outbox_shadow")
            if status != "pending":
                raise ActivationEpochError(
                    "activation_historical_hold_outbox_status_invalid"
                )
            # A retry clears the active lease but intentionally preserves
            # claimed_at as immutable audit history. Only live lease fields
            # make a pending historical row unsafe to seal.
            if any(
                row[field] is not None
                for field in ("lease_token", "lease_owner", "lease_expires_at")
            ):
                raise ActivationEpochError("activation_historical_hold_outbox_leased")
            topic = str(row["source_topic"] or "")
            if not topic:
                raise ActivationEpochError("activation_historical_hold_outbox_manual")
            raw_partition = row["source_partition"]
            raw_offset = row["source_offset"]
            if raw_partition is None or raw_offset is None:
                raise ActivationEpochError("activation_historical_hold_outbox_unfenced")
            try:
                partition_value = int(raw_partition)
                offset = int(raw_offset)
            except (TypeError, ValueError, OverflowError) as exc:
                raise ActivationEpochError(
                    "activation_historical_hold_outbox_unfenced"
                ) from exc
            partition = str(partition_value)
            if (
                partition_value < 0
                or offset < 0
                or topic not in start_fence
                or partition not in start_fence[topic]
            ):
                raise ActivationEpochError("activation_historical_hold_outbox_unfenced")
            if offset >= int(start_fence[topic][partition]):
                raise ActivationEpochError(
                    "activation_historical_hold_outbox_at_or_after_start_fence"
                )
            items.append({
                "outbox_id": int(row["outbox_id"]),
                "row_sha256": cls._historical_outbox_row_sha256(row),
                "immutable_row_sha256": (
                    cls._historical_outbox_immutable_row_sha256(row)
                ),
            })
        return items

    @classmethod
    def _historical_outbox_hold_evidence_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        epoch: sqlite3.Row,
        allow_current_epoch: bool,
    ) -> dict[str, Any]:
        epoch_id = str(epoch["epoch_id"])
        hold = conn.execute(
            "SELECT * FROM rca_activation_historical_outbox_holds WHERE epoch_id = ?",
            (epoch_id,),
        ).fetchone()
        if hold is None:
            raise ActivationEpochError("activation_historical_hold_not_sealed")
        if str(
            hold["schema_version"]
        ) != ACTIVATION_HISTORICAL_OUTBOX_HOLD_SCHEMA_VERSION or str(
            hold["partition_start_fence_sha256"]
        ) != str(epoch["partition_start_fence_sha256"]):
            raise ActivationEpochError("activation_historical_hold_seal_invalid")
        sealed_items = [
            {
                "outbox_id": int(row["outbox_id"]),
                "row_sha256": str(row["row_sha256"]),
                "immutable_row_sha256": str(row["immutable_row_sha256"]),
            }
            for row in conn.execute(
                "SELECT outbox_id, row_sha256, immutable_row_sha256 "
                "FROM rca_activation_historical_outbox_hold_items "
                "WHERE epoch_id = ? ORDER BY outbox_id",
                (epoch_id,),
            ).fetchall()
        ]
        sealed_count = int(hold["cohort_count"])
        sealed_sha256 = str(hold["cohort_sha256"])
        if (
            len(sealed_items) != sealed_count
            or _canonical_sha256(sealed_items) != sealed_sha256
        ):
            raise ActivationEpochError("activation_historical_hold_seal_invalid")
        disposition = conn.execute(
            "SELECT disposition_id, disposition_sha256, disposed_at "
            "FROM rca_activation_historical_outbox_dispositions WHERE epoch_id = ?",
            (epoch_id,),
        ).fetchone()
        current_items = cls._historical_outbox_hold_snapshot_tx(
            conn,
            epoch=epoch,
            allow_current_epoch=allow_current_epoch,
        )
        current_count = len(current_items)
        current_sha256 = _canonical_sha256(current_items)
        return {
            "schema_version": ACTIVATION_HISTORICAL_OUTBOX_HOLD_SCHEMA_VERSION,
            "row_schema_version": ACTIVATION_HISTORICAL_OUTBOX_ROW_SCHEMA_VERSION,
            "epoch_id": epoch_id,
            "partition_start_fence_sha256": str(hold["partition_start_fence_sha256"]),
            "sealed_at": str(hold["sealed_at"]),
            "sealed_count": sealed_count,
            "sealed_sha256": sealed_sha256,
            "disposed": disposition is not None,
            "disposition_id": (
                str(disposition["disposition_id"]) if disposition is not None else ""
            ),
            "disposition_sha256": (
                str(disposition["disposition_sha256"])
                if disposition is not None
                else ""
            ),
            "disposed_at": (
                str(disposition["disposed_at"]) if disposition is not None else ""
            ),
            "current_count": current_count,
            "current_sha256": current_sha256,
            "matches": (
                sealed_count == current_count
                and sealed_sha256 == current_sha256
                and sealed_items == current_items
            ),
        }

    @classmethod
    def _require_historical_outbox_hold_match_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        epoch: sqlite3.Row,
        allow_current_epoch: bool = True,
    ) -> dict[str, Any]:
        evidence = cls._historical_outbox_hold_evidence_tx(
            conn,
            epoch=epoch,
            allow_current_epoch=allow_current_epoch,
        )
        if evidence["disposed"]:
            raise ActivationEpochError("activation_historical_hold_disposed")
        if not evidence["matches"]:
            raise ActivationEpochError("activation_historical_hold_cohort_changed")
        return evidence

    @classmethod
    def _seal_historical_outbox_hold_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        epoch: sqlite3.Row,
        sealed_at: str,
    ) -> dict[str, Any]:
        epoch_id = str(epoch["epoch_id"])
        existing = conn.execute(
            "SELECT 1 FROM rca_activation_historical_outbox_holds WHERE epoch_id = ?",
            (epoch_id,),
        ).fetchone()
        if existing is not None:
            return cls._require_historical_outbox_hold_match_tx(
                conn,
                epoch=epoch,
                allow_current_epoch=False,
            )
        items = cls._historical_outbox_hold_snapshot_tx(
            conn,
            epoch=epoch,
            allow_current_epoch=False,
        )
        cohort_sha256 = _canonical_sha256(items)
        conn.execute(
            """
            INSERT INTO rca_activation_historical_outbox_holds(
                epoch_id, schema_version, partition_start_fence_sha256,
                cohort_count, cohort_sha256, sealed_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                epoch_id,
                ACTIVATION_HISTORICAL_OUTBOX_HOLD_SCHEMA_VERSION,
                epoch["partition_start_fence_sha256"],
                len(items),
                cohort_sha256,
                sealed_at,
            ),
        )
        conn.executemany(
            """
            INSERT INTO rca_activation_historical_outbox_hold_items(
                epoch_id, outbox_id, row_sha256, immutable_row_sha256
            ) VALUES (?, ?, ?, ?)
            """,
            [
                (
                    epoch_id,
                    item["outbox_id"],
                    item["row_sha256"],
                    item["immutable_row_sha256"],
                )
                for item in items
            ],
        )
        return cls._require_historical_outbox_hold_match_tx(
            conn,
            epoch=epoch,
            allow_current_epoch=False,
        )

    @staticmethod
    def _current_activation_epoch_tx(conn: sqlite3.Connection) -> sqlite3.Row | None:
        return conn.execute(
            "SELECT * FROM rca_activation_epochs WHERE is_current = 1"
        ).fetchone()

    @staticmethod
    def _public_activation_epoch(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "epoch_id": str(row["epoch_id"]),
            "state": str(row["state"]),
            "preauthorization_fingerprint": str(
                row["preauthorization_fingerprint"]
            ),
            "preauthorization_gate_receipt_sha256": str(
                row["preauthorization_gate_receipt_sha256"]
            ),
            "preauthorization_capsule_sha256": str(
                row["preauthorization_capsule_sha256"]
            ),
            "preproduction_fingerprint": str(
                row["preproduction_fingerprint"] or ""
            ),
            "preproduction_gate_receipt_sha256": str(
                row["preproduction_gate_receipt_sha256"] or ""
            ),
            "preproduction_capsule_sha256": str(
                row["preproduction_capsule_sha256"] or ""
            ),
            "config_sha256": str(row["config_sha256"]),
            "db_logical_identity_sha256": str(row["db_logical_identity_sha256"]),
            "partition_start_fence_sha256": str(
                row["partition_start_fence_sha256"]
            ),
            "partition_end_fence_sha256": str(
                row["partition_end_fence_sha256"] or ""
            ),
            "production_fingerprint": str(row["production_fingerprint"] or ""),
            "production_gate_receipt_sha256": str(
                row["production_gate_receipt_sha256"] or ""
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

    @staticmethod
    def _insert_activation_transition_audit_tx(
        conn: sqlite3.Connection,
        *,
        epoch: sqlite3.Row,
        from_state: str,
        to_state: str,
        operator: str,
        reason: str,
        transitioned_at: str,
    ) -> int:
        slot_bindings = [
            {
                "authorized_identity_sha256": str(
                    row["authorized_identity_sha256"] or ""
                ),
                "authorized_operator": str(row["authorized_operator"] or ""),
                "authorized_reason": str(row["authorized_reason"] or ""),
                "authorized_source_kind": str(row["authorized_source_kind"] or ""),
                "consumed_ledger_id": int(row["consumed_ledger_id"] or 0),
                "slot_kind": str(row["slot_kind"]),
            }
            for row in conn.execute(
                """
                SELECT slot_kind, authorized_source_kind,
                       authorized_identity_sha256, authorized_operator,
                       authorized_reason, consumed_ledger_id
                  FROM rca_activation_budget_slots
                 WHERE epoch_id = ? ORDER BY slot_kind
                """,
                (epoch["epoch_id"],),
            ).fetchall()
        ]
        binding_fingerprint = _canonical_sha256(
            {
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
                "slot_bindings_sha256": _canonical_sha256(slot_bindings),
                "to_state": to_state,
            }
        )
        cursor = conn.execute(
            """
            INSERT INTO rca_activation_transition_audit(
                epoch_id, from_state, to_state, operator, reason,
                binding_fingerprint, transitioned_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                epoch["epoch_id"],
                from_state,
                to_state,
                operator,
                reason,
                binding_fingerprint,
                transitioned_at,
            ),
        )
        if cursor.lastrowid is None:
            raise ActivationEpochError("activation_transition_audit_failed")
        return int(cursor.lastrowid)

    def create_activation_epoch(
        self,
        *,
        epoch_id: str,
        preauthorization_fingerprint: str,
        preauthorization_gate_receipt_sha256: str,
        preauthorization_capsule_sha256: str,
        config_sha256: str,
        db_logical_identity: Mapping[str, Any],
        partition_start_fence: Mapping[str, Any],
        operator: str,
        reason: str,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        """Create one safe-off epoch from an immutable preauthorization capsule."""
        identity = self._normalize_activation_epoch_id(epoch_id)
        preauthorization_hash = self._normalize_activation_sha256(
            preauthorization_fingerprint, "preauthorization_fingerprint"
        )
        preauthorization_receipt_hash = self._normalize_activation_sha256(
            preauthorization_gate_receipt_sha256,
            "preauthorization_gate_receipt_sha256",
        )
        preauthorization_capsule_hash = self._normalize_activation_sha256(
            preauthorization_capsule_sha256,
            "preauthorization_capsule_sha256",
        )
        config_hash = self._normalize_activation_sha256(config_sha256, "config_sha256")
        db_identity_json = self._normalize_activation_db_identity(db_logical_identity)
        db_identity_sha = hashlib.sha256(db_identity_json.encode("utf-8")).hexdigest()
        start_fence_json = self._normalize_partition_fence(partition_start_fence)
        start_fence_sha = hashlib.sha256(start_fence_json.encode("utf-8")).hexdigest()
        actor, justification = self._normalize_activation_audit_text(operator, reason)
        current = _iso(now)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            existing = self._current_activation_epoch_tx(conn)
            if existing is not None and str(existing["epoch_id"]) == identity:
                expected = (
                    preauthorization_hash,
                    preauthorization_receipt_hash,
                    preauthorization_capsule_hash,
                    config_hash,
                    db_identity_sha,
                    start_fence_sha,
                )
                observed = (
                    str(existing["preauthorization_fingerprint"]),
                    str(existing["preauthorization_gate_receipt_sha256"]),
                    str(existing["preauthorization_capsule_sha256"]),
                    str(existing["config_sha256"]),
                    str(existing["db_logical_identity_sha256"]),
                    str(existing["partition_start_fence_sha256"]),
                )
                if observed != expected:
                    raise ActivationEpochError("activation_epoch_binding_conflict")
                conn.commit()
                return self._public_activation_epoch(existing)
            pending_inbox = int(
                conn.execute(
                    "SELECT COUNT(*) FROM kafka_inbox WHERE decision = 'pending'"
                ).fetchone()[0]
            )
            if pending_inbox:
                raise ActivationEpochError("activation_pending_inbox_not_drained")
            shadow_backlog = int(
                conn.execute(
                    "SELECT COUNT(*) FROM rca_outbox WHERE status = 'shadow'"
                ).fetchone()[0]
            )
            if shadow_backlog:
                raise ActivationEpochError(
                    "activation_shadow_backlog_not_disposed"
                )
            if existing is not None:
                if str(existing["state"]) != "aborted":
                    raise ActivationEpochError("activation_current_epoch_exists")
                conn.execute(
                    """
                    UPDATE rca_activation_epochs
                       SET is_current = 0, superseded_at = ?, updated_at = ?
                     WHERE epoch_id = ? AND is_current = 1 AND state = 'aborted'
                    """,
                    (current, current, existing["epoch_id"]),
                )
            conn.execute(
                """
                INSERT INTO rca_activation_epochs(
                    epoch_id, state, is_current,
                    preauthorization_fingerprint,
                    preauthorization_gate_receipt_sha256,
                    preauthorization_capsule_sha256,
                    config_sha256, db_logical_identity_json,
                    db_logical_identity_sha256, partition_start_fence_json,
                    partition_start_fence_sha256, created_at, updated_at
                ) VALUES (?, 'safe_off', 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    identity,
                    preauthorization_hash,
                    preauthorization_receipt_hash,
                    preauthorization_capsule_hash,
                    config_hash,
                    db_identity_json,
                    db_identity_sha,
                    start_fence_json,
                    start_fence_sha,
                    current,
                    current,
                ),
            )
            conn.executemany(
                """
                INSERT INTO rca_activation_budget_slots(epoch_id, slot_kind)
                VALUES (?, ?)
                """,
                [(identity, slot_kind) for slot_kind in ACTIVATION_SLOT_KINDS],
            )
            row = self._current_activation_epoch_tx(conn)
            if row is None:
                raise ActivationEpochError("activation_epoch_create_lost")
            self._insert_activation_transition_audit_tx(
                conn,
                epoch=row,
                from_state="none",
                to_state="safe_off",
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

    def preauthorize_activation_epoch(
        self,
        *,
        epoch_id: str,
        preproduction_fingerprint: str,
        preproduction_gate_receipt_sha256: str,
        preproduction_capsule_sha256: str,
        expected_preauthorization_fingerprint: str,
        expected_preauthorization_gate_receipt_sha256: str,
        expected_preauthorization_capsule_sha256: str,
        expected_config_sha256: str,
        expected_db_logical_identity_sha256: str,
        expected_partition_start_fence_sha256: str,
        operator: str,
        reason: str,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        """Consume one preproduction capsule and open the exact safe-off epoch."""
        identity = self._normalize_activation_epoch_id(epoch_id)
        preproduction_hash = self._normalize_activation_sha256(
            preproduction_fingerprint, "preproduction_fingerprint"
        )
        preproduction_receipt_hash = self._normalize_activation_sha256(
            preproduction_gate_receipt_sha256,
            "preproduction_gate_receipt_sha256",
        )
        preproduction_capsule_hash = self._normalize_activation_sha256(
            preproduction_capsule_sha256,
            "preproduction_capsule_sha256",
        )
        expected_bindings = {
            "preauthorization_fingerprint": self._normalize_activation_sha256(
                expected_preauthorization_fingerprint,
                "preauthorization_fingerprint",
            ),
            "preauthorization_gate_receipt_sha256": (
                self._normalize_activation_sha256(
                    expected_preauthorization_gate_receipt_sha256,
                    "preauthorization_gate_receipt_sha256",
                )
            ),
            "preauthorization_capsule_sha256": self._normalize_activation_sha256(
                expected_preauthorization_capsule_sha256,
                "preauthorization_capsule_sha256",
            ),
            "config_sha256": self._normalize_activation_sha256(
                expected_config_sha256, "config_sha256"
            ),
            "db_logical_identity_sha256": self._normalize_activation_sha256(
                expected_db_logical_identity_sha256,
                "db_logical_identity_sha256",
            ),
            "partition_start_fence_sha256": self._normalize_activation_sha256(
                expected_partition_start_fence_sha256,
                "partition_start_fence_sha256",
            ),
        }
        actor, justification = self._normalize_activation_audit_text(operator, reason)
        current = _iso(now)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            epoch = self._current_activation_epoch_tx(conn)
            if epoch is None or str(epoch["epoch_id"]) != identity:
                raise ActivationEpochError("activation_epoch_not_current")
            if any(str(epoch[field] or "") != value for field, value in expected_bindings.items()):
                raise ActivationEpochError(
                    "activation_preproduction_epoch_binding_changed"
                )
            prior = str(epoch["state"])
            expected_preproduction = (
                preproduction_hash,
                preproduction_receipt_hash,
                preproduction_capsule_hash,
            )
            observed_preproduction = (
                str(epoch["preproduction_fingerprint"] or ""),
                str(epoch["preproduction_gate_receipt_sha256"] or ""),
                str(epoch["preproduction_capsule_sha256"] or ""),
            )
            if prior == "preauthorized":
                if observed_preproduction != expected_preproduction:
                    raise ActivationEpochError(
                        "activation_preproduction_binding_conflict"
                    )
                self._require_historical_outbox_hold_match_tx(
                    conn,
                    epoch=epoch,
                )
                conn.commit()
                return self._public_activation_epoch(epoch)
            if prior != "safe_off":
                raise ActivationEpochError("activation_epoch_state_changed")
            if observed_preproduction != ("", "", ""):
                raise ActivationEpochError("activation_preproduction_binding_conflict")
            pending_inbox = int(
                conn.execute(
                    "SELECT COUNT(*) FROM kafka_inbox WHERE decision = 'pending'"
                ).fetchone()[0]
            )
            current_ledger = int(
                conn.execute(
                    "SELECT COUNT(*) FROM rca_activation_admission_ledger "
                    "WHERE epoch_id = ?",
                    (identity,),
                ).fetchone()[0]
            )
            if pending_inbox:
                raise ActivationEpochError("activation_historical_hold_pending_inbox")
            if current_ledger:
                raise ActivationEpochError("activation_historical_hold_current_ledger")
            self._seal_historical_outbox_hold_tx(
                conn,
                epoch=epoch,
                sealed_at=current,
            )
            updated = conn.execute(
                """
                UPDATE rca_activation_epochs
                   SET state = 'preauthorized',
                       preproduction_fingerprint = ?,
                       preproduction_gate_receipt_sha256 = ?,
                       preproduction_capsule_sha256 = ?,
                       updated_at = ?
                 WHERE epoch_id = ? AND is_current = 1 AND state = 'safe_off'
                   AND preproduction_fingerprint IS NULL
                   AND preproduction_gate_receipt_sha256 IS NULL
                   AND preproduction_capsule_sha256 IS NULL
                """,
                (
                    preproduction_hash,
                    preproduction_receipt_hash,
                    preproduction_capsule_hash,
                    current,
                    identity,
                ),
            )
            if updated.rowcount != 1:
                raise ActivationEpochError("activation_epoch_state_changed")
            row = self._current_activation_epoch_tx(conn)
            if row is None:
                raise ActivationEpochError("activation_epoch_transition_lost")
            self._insert_activation_transition_audit_tx(
                conn,
                epoch=row,
                from_state="safe_off",
                to_state="preauthorized",
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
            row = self._current_activation_epoch_tx(conn)
            return self._public_activation_epoch(row) if row is not None else None
        finally:
            conn.close()

    def activation_historical_outbox_hold_evidence(
        self,
        *,
        epoch_id: str,
    ) -> dict[str, Any]:
        """Return privacy-light sealed/current cohort evidence without writing."""
        identity = self._normalize_activation_epoch_id(epoch_id)
        conn = self._connect()
        try:
            conn.execute("BEGIN")
            epoch = self._current_activation_epoch_tx(conn)
            if epoch is None or str(epoch["epoch_id"]) != identity:
                raise ActivationEpochError("activation_epoch_not_current")
            evidence = self._historical_outbox_hold_evidence_tx(
                conn,
                epoch=epoch,
                allow_current_epoch=True,
            )
            conn.rollback()
            return evidence
        except Exception:
            if conn.in_transaction:
                conn.rollback()
            raise
        finally:
            conn.close()

    @staticmethod
    def _public_historical_outbox_disposition(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "schema_version": str(row["schema_version"]),
            "disposition_id": str(row["disposition_id"]),
            "disposition_sha256": str(row["disposition_sha256"]),
            "epoch_id": str(row["epoch_id"]),
            "epoch_state": str(row["epoch_state"]),
            "hold_schema_version": str(row["hold_schema_version"]),
            "row_schema_version": str(row["row_schema_version"]),
            "cohort_count": int(row["cohort_count"]),
            "cohort_sha256": str(row["cohort_sha256"]),
            "owner_authorized": bool(row["owner_authorized"]),
            "operator": str(row["operator"]),
            "reason": str(row["reason"]),
            "disposed_at": str(row["disposed_at"]),
            "outbox_status": "quarantined",
        }

    def dispose_activation_historical_outbox_hold(
        self,
        *,
        epoch_id: str,
        expected_cohort_count: int,
        expected_cohort_sha256: str,
        owner_authorized: bool,
        operator: str,
        reason: str,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        """Quarantine one exact aborted cohort under an immutable owner audit."""
        identity = self._normalize_activation_epoch_id(epoch_id)
        if (
            isinstance(expected_cohort_count, bool)
            or not isinstance(expected_cohort_count, int)
            or expected_cohort_count < 0
        ):
            raise ActivationEpochError(
                "activation_historical_disposition_cohort_count_invalid"
            )
        expected_sha256 = self._normalize_activation_sha256(
            expected_cohort_sha256,
            "historical_disposition_cohort_sha256",
        )
        if owner_authorized is not True:
            raise ActivationEpochError(
                "activation_historical_disposition_owner_authorization_required"
            )
        actor, justification = self._normalize_activation_audit_text(operator, reason)
        disposed_at = _iso(now)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            epoch = self._current_activation_epoch_tx(conn)
            if epoch is None or str(epoch["epoch_id"]) != identity:
                raise ActivationEpochError("activation_epoch_not_current")
            if str(epoch["state"]) != "aborted":
                raise ActivationEpochError(
                    "activation_historical_disposition_epoch_not_aborted"
                )
            hold = conn.execute(
                "SELECT * FROM rca_activation_historical_outbox_holds "
                "WHERE epoch_id = ?",
                (identity,),
            ).fetchone()
            if hold is None:
                raise ActivationEpochError("activation_historical_hold_not_sealed")
            if (
                int(hold["cohort_count"]) != expected_cohort_count
                or str(hold["cohort_sha256"]) != expected_sha256
            ):
                raise ActivationEpochError(
                    "activation_historical_disposition_cohort_binding_changed"
                )
            existing = conn.execute(
                "SELECT * FROM rca_activation_historical_outbox_dispositions "
                "WHERE epoch_id = ?",
                (identity,),
            ).fetchone()
            if existing is not None:
                if (
                    int(existing["cohort_count"]) != expected_cohort_count
                    or str(existing["cohort_sha256"]) != expected_sha256
                    or str(existing["operator"]) != actor
                    or str(existing["reason"]) != justification
                    or int(existing["owner_authorized"]) != 1
                ):
                    raise ActivationEpochError(
                        "activation_historical_disposition_binding_conflict"
                    )
                self._validate_v13_historical_outbox_hold_schema(conn)
                result = self._public_historical_outbox_disposition(existing)
                conn.commit()
                return result
            evidence = self._require_historical_outbox_hold_match_tx(
                conn,
                epoch=epoch,
                allow_current_epoch=True,
            )
            if (
                int(evidence["sealed_count"]) != expected_cohort_count
                or str(evidence["sealed_sha256"]) != expected_sha256
            ):
                raise ActivationEpochError(
                    "activation_historical_disposition_cohort_binding_changed"
                )
            items = [
                {
                    "outbox_id": int(row["outbox_id"]),
                    "row_sha256": str(row["row_sha256"]),
                    "immutable_row_sha256": str(row["immutable_row_sha256"]),
                }
                for row in conn.execute(
                    "SELECT outbox_id, row_sha256, immutable_row_sha256 "
                    "FROM rca_activation_historical_outbox_hold_items "
                    "WHERE epoch_id = ? ORDER BY outbox_id",
                    (identity,),
                ).fetchall()
            ]
            covered_holds = [
                {
                    "epoch_id": identity,
                    "state": str(epoch["state"]),
                    "cohort_count": int(hold["cohort_count"]),
                    "cohort_sha256": str(hold["cohort_sha256"]),
                }
            ]
            if items:
                outbox_ids = [item["outbox_id"] for item in items]
                placeholders = ",".join("?" for _ in outbox_ids)
                covered_holds = [
                    dict(row)
                    for row in conn.execute(
                        f"""
                        SELECT DISTINCT referenced_epoch.epoch_id,
                                        referenced_epoch.state,
                                        referenced_hold.cohort_count,
                                        referenced_hold.cohort_sha256
                          FROM rca_activation_historical_outbox_hold_items AS held
                          JOIN rca_activation_historical_outbox_holds
                               AS referenced_hold
                            ON referenced_hold.epoch_id = held.epoch_id
                          JOIN rca_activation_epochs AS referenced_epoch
                            ON referenced_epoch.epoch_id = held.epoch_id
                         WHERE held.outbox_id IN ({placeholders})
                         ORDER BY referenced_epoch.epoch_id
                        """,
                        tuple(outbox_ids),
                    ).fetchall()
                ]
                for covered_hold in covered_holds:
                    if str(covered_hold["state"]) != "aborted":
                        raise ActivationEpochError(
                            "activation_historical_disposition_active_epoch_reference"
                        )
                    referenced_items = [
                        {
                            "outbox_id": int(row["outbox_id"]),
                            "row_sha256": str(row["row_sha256"]),
                            "immutable_row_sha256": str(
                                row["immutable_row_sha256"]
                            ),
                        }
                        for row in conn.execute(
                            "SELECT outbox_id, row_sha256, immutable_row_sha256 "
                            "FROM rca_activation_historical_outbox_hold_items "
                            "WHERE epoch_id = ? ORDER BY outbox_id",
                            (covered_hold["epoch_id"],),
                        ).fetchall()
                    ]
                    if (
                        referenced_items != items
                        or int(covered_hold["cohort_count"])
                        != expected_cohort_count
                        or str(covered_hold["cohort_sha256"]) != expected_sha256
                    ):
                        raise ActivationEpochError(
                            "activation_historical_disposition_overlapping_cohort_changed"
                        )
            if identity not in {
                str(covered_hold["epoch_id"]) for covered_hold in covered_holds
            }:
                raise ActivationEpochError(
                    "activation_historical_disposition_current_hold_missing"
                )
            for covered_hold in covered_holds:
                overlapping = conn.execute(
                    "SELECT 1 FROM rca_activation_historical_outbox_dispositions "
                    "WHERE epoch_id = ?",
                    (covered_hold["epoch_id"],),
                ).fetchone()
                if overlapping is not None:
                    raise ActivationEpochError(
                        "activation_historical_disposition_overlapping_epoch_disposed"
                    )
            for item in items:
                parent = conn.execute(
                    """
                    SELECT trigger.state
                      FROM rca_outbox AS outbox
                      JOIN business_triggers AS trigger
                        ON trigger.business_key = outbox.business_key
                       AND trigger.generation = outbox.generation
                       AND trigger.submission_key = outbox.submission_key
                     WHERE outbox.outbox_id = ?
                    """,
                    (item["outbox_id"],),
                ).fetchone()
                if parent is None or str(parent["state"]) != "pending":
                    raise ActivationEpochError(
                        "activation_historical_disposition_parent_state_invalid"
                    )
            disposition_id = ""
            for covered_hold in covered_holds:
                covered_epoch_id = str(covered_hold["epoch_id"])
                binding = self._historical_outbox_disposition_binding(
                    epoch_id=covered_epoch_id,
                    cohort_count=expected_cohort_count,
                    cohort_sha256=expected_sha256,
                    operator=actor,
                    reason=justification,
                    disposed_at=disposed_at,
                )
                disposition_sha256 = _canonical_sha256(binding)
                covered_disposition_id = (
                    f"rca-hold-disposition-v1-{disposition_sha256}"
                )
                conn.execute(
                    """
                    INSERT INTO rca_activation_historical_outbox_dispositions(
                        disposition_id, epoch_id, epoch_state, schema_version,
                        hold_schema_version, row_schema_version,
                        cohort_count, cohort_sha256, owner_authorized,
                        operator, reason, disposed_at, disposition_sha256
                    ) VALUES (?, ?, 'aborted', ?, ?, ?, ?, ?, 1, ?, ?, ?, ?)
                    """,
                    (
                        covered_disposition_id,
                        covered_epoch_id,
                        ACTIVATION_HISTORICAL_OUTBOX_DISPOSITION_SCHEMA_VERSION,
                        ACTIVATION_HISTORICAL_OUTBOX_HOLD_SCHEMA_VERSION,
                        ACTIVATION_HISTORICAL_OUTBOX_ROW_SCHEMA_VERSION,
                        expected_cohort_count,
                        expected_sha256,
                        actor,
                        justification,
                        disposed_at,
                        disposition_sha256,
                    ),
                )
                conn.executemany(
                    """
                    INSERT INTO rca_activation_historical_outbox_disposition_items(
                        disposition_id, outbox_id, row_sha256,
                        immutable_row_sha256
                    ) VALUES (?, ?, ?, ?)
                    """,
                    [
                        (
                            covered_disposition_id,
                            item["outbox_id"],
                            item["row_sha256"],
                            item["immutable_row_sha256"],
                        )
                        for item in items
                    ],
                )
                if covered_epoch_id == identity:
                    disposition_id = covered_disposition_id
            if not disposition_id:
                raise ActivationEpochError(
                    "activation_historical_disposition_current_audit_missing"
                )
            for item in items:
                updated = conn.execute(
                    """
                    UPDATE rca_outbox
                       SET status = 'quarantined', next_attempt_at = NULL,
                           quarantined_at = ?,
                           last_error_code = ?,
                           last_error_detail = 'owner_audited_disposition',
                           updated_at = ?
                     WHERE outbox_id = ? AND status = 'pending'
                       AND activation_epoch_id IS NULL
                       AND activation_ledger_id IS NULL
                       AND lease_token IS NULL AND lease_owner IS NULL
                       AND lease_expires_at IS NULL AND claimed_at IS NULL
                       AND completed_at IS NULL AND result_json IS NULL
                    """,
                    (
                        disposed_at,
                        ACTIVATION_HISTORICAL_OUTBOX_DISPOSITION_ERROR_CODE,
                        disposed_at,
                        item["outbox_id"],
                    ),
                )
                if updated.rowcount != 1:
                    raise ActivationEpochError(
                        "activation_historical_disposition_outbox_state_changed"
                    )
                parent_updated = conn.execute(
                    """
                    UPDATE business_triggers
                       SET state = 'quarantined'
                     WHERE business_key = (
                               SELECT business_key FROM rca_outbox WHERE outbox_id = ?
                           )
                       AND generation = (
                               SELECT generation FROM rca_outbox WHERE outbox_id = ?
                           )
                       AND state = 'pending'
                    """,
                    (item["outbox_id"], item["outbox_id"]),
                )
                if parent_updated.rowcount != 1:
                    raise ActivationEpochError(
                        "activation_historical_disposition_parent_state_changed"
                    )
            self._validate_v13_historical_outbox_hold_schema(conn)
            disposition = conn.execute(
                "SELECT * FROM rca_activation_historical_outbox_dispositions "
                "WHERE disposition_id = ?",
                (disposition_id,),
            ).fetchone()
            if disposition is None:
                raise ActivationEpochError(
                    "activation_historical_disposition_commit_lost"
                )
            result = self._public_historical_outbox_disposition(disposition)
            conn.commit()
            return result
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
        if not all(expected_source.values()):
            raise RecordConflictError(
                "manual_external_write_source_identity_invalid"
            )

        conn = self._connect()
        try:
            conn.execute("BEGIN")
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
            if (
                str(source["source_kind"]) != "feishu_group_manual"
                or str(source["platform"]) != "feishu"
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
                "mode": str(source["mode"]),
            }
            source_identity_sha256 = _canonical_sha256(source_identity)
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
            if str(ledger["state"]) not in {
                "bounded_active",
                "steady_active",
            }:
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
            if source_row is None or (
                str(source_row["source_kind"] or "") != "feishu_group_manual"
                or str(source_row["platform"] or "") != "feishu"
            ):
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
                   AND source.platform = 'feishu'
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
                or not all(str(value).strip() for value in source_identity.values())
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
        current bounded/steady activation epoch.
        """
        epoch_id = str(fence.get("activation_epoch_id") or "").strip()
        ledger_id = fence.get("activation_ledger_id")
        admission_key = str(fence.get("admission_key") or "").strip()
        if not epoch_id or isinstance(ledger_id, bool) or not isinstance(ledger_id, int):
            raise RecordConflictError("external_write_fence_schema_invalid")
        conn = self._connect()
        try:
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
        finally:
            conn.close()
        if row is None or int(row["is_current"]) != 1:
            raise RecordConflictError("external_write_fence_epoch_not_current")
        if str(row["state"]) not in {"bounded_active", "steady_active"}:
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

    def capacity_transition_state(self) -> dict[str, Any] | None:
        """Return the durable capacity latch; derived readiness lives elsewhere."""

        conn = self._connect()
        try:
            conn.execute("BEGIN")
            integrity_error = self._capacity_transition_integrity_error_tx(conn)
            if integrity_error:
                raise RuntimeError(
                    f"rca_capacity_transition_integrity:{integrity_error}"
                )
            row = self._capacity_transition_state_tx(conn)
            result = (
                self._public_capacity_transition_state(row)
                if row is not None
                else None
            )
            conn.commit()
            return result
        except Exception:
            if conn.in_transaction:
                conn.rollback()
            raise
        finally:
            conn.close()

    def initialize_capacity_transition(
        self,
        *,
        release_id: str,
        bootstrap_epoch_id: str,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        """Create the explicit bootstrap latch once; missing state never auto-allows."""

        release = self._normalize_capacity_identity(release_id, "release_id")
        epoch = self._normalize_capacity_identity(
            bootstrap_epoch_id, "bootstrap_epoch_id"
        )
        current = _iso(now)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            integrity_error = self._capacity_transition_integrity_error_tx(conn)
            if integrity_error:
                raise CapacityTransitionStateError(
                    f"capacity_transition_integrity_invalid:{integrity_error}"
                )
            existing = self._capacity_transition_state_tx(conn)
            if existing is not None:
                if (
                    str(existing["release_id"]) != release
                    or str(existing["bootstrap_epoch_id"]) != epoch
                ):
                    raise CapacityTransitionStateError(
                        "capacity_transition_identity_conflict"
                    )
                conn.commit()
                return self._public_capacity_transition_state(existing)
            conn.execute(
                """
                INSERT INTO rca_capacity_transition_state(
                    singleton_id, release_id, bootstrap_epoch_id, state,
                    generation, bootstrap_initialized_at, updated_at
                ) VALUES(1, ?, ?, 'BOOTSTRAP_PRODUCTION', 1, ?, ?)
                """,
                (release, epoch, current, current),
            )
            conn.execute(
                """
                INSERT INTO rca_capacity_transition_audit(
                    release_id, bootstrap_epoch_id, from_state, to_state,
                    from_generation, to_generation, transitioned_at
                ) VALUES(?, ?, 'UNCONFIGURED', 'BOOTSTRAP_PRODUCTION', 0, 1, ?)
                """,
                (release, epoch, current),
            )
            row = self._capacity_transition_state_tx(conn)
            if row is None:
                raise CapacityTransitionStateError(
                    "capacity_transition_initialize_lost"
                )
            integrity_error = self._capacity_transition_integrity_error_tx(conn)
            if integrity_error:
                raise CapacityTransitionStateError(
                    f"capacity_transition_initialize_invalid:{integrity_error}"
                )
            conn.commit()
            return self._public_capacity_transition_state(row)
        except Exception:
            if conn.in_transaction:
                conn.rollback()
            raise
        finally:
            conn.close()

    def compare_and_set_capacity_steady(
        self,
        *,
        expected_generation: int,
        release_id: str,
        bootstrap_epoch_id: str,
        final_ledger_sha256: str,
        transition_authorization_sha256: str,
        transition_authorization_fingerprint: str,
        transition_receipt_sha256: str,
        transition_receipt_fingerprint: str,
        commit_marker_sha256: str,
        commit_marker_fingerprint: str,
        evidence_bundle_sha256: str,
        evidence_bundle_fingerprint: str,
        authorization_issued_at: str,
        authorization_expires_at: str,
        receipt_created_at: str,
        marker_committed_at: str,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        """Irreversibly CAS the release-bound bootstrap latch to steady."""

        if (
            isinstance(expected_generation, bool)
            or not isinstance(expected_generation, int)
            or expected_generation < 1
        ):
            raise CapacityTransitionStateError(
                "capacity_expected_generation_invalid"
            )
        release = self._normalize_capacity_identity(release_id, "release_id")
        epoch = self._normalize_capacity_identity(
            bootstrap_epoch_id, "bootstrap_epoch_id"
        )
        hashes = {
            field: self._normalize_capacity_sha256(value, field)
            for field, value in {
                "final_ledger_sha256": final_ledger_sha256,
                "transition_authorization_sha256": (
                    transition_authorization_sha256
                ),
                "transition_authorization_fingerprint": (
                    transition_authorization_fingerprint
                ),
                "transition_receipt_sha256": transition_receipt_sha256,
                "transition_receipt_fingerprint": (
                    transition_receipt_fingerprint
                ),
                "commit_marker_sha256": commit_marker_sha256,
                "commit_marker_fingerprint": commit_marker_fingerprint,
                "evidence_bundle_sha256": evidence_bundle_sha256,
                "evidence_bundle_fingerprint": evidence_bundle_fingerprint,
            }.items()
        }
        timestamps = {
            field: self._normalize_capacity_timestamp(value, field)
            for field, value in {
                "authorization_issued_at": authorization_issued_at,
                "authorization_expires_at": authorization_expires_at,
                "receipt_created_at": receipt_created_at,
                "marker_committed_at": marker_committed_at,
            }.items()
        }
        issued = datetime.fromisoformat(timestamps["authorization_issued_at"])
        expires = datetime.fromisoformat(timestamps["authorization_expires_at"])
        receipt_created = datetime.fromisoformat(timestamps["receipt_created_at"])
        marker_committed = datetime.fromisoformat(timestamps["marker_committed_at"])
        current_dt = _utc_datetime(now)
        if not (
            issued < expires
            and issued <= receipt_created <= marker_committed <= expires
            and marker_committed <= current_dt + timedelta(seconds=5)
        ):
            raise CapacityTransitionStateError(
                "capacity_transition_timestamp_order_invalid"
            )
        current = current_dt.isoformat()
        bindings = {**hashes, **timestamps}
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            integrity_error = self._capacity_transition_integrity_error_tx(conn)
            if integrity_error:
                raise CapacityTransitionStateError(
                    f"capacity_transition_integrity_invalid:{integrity_error}"
                )
            existing = self._capacity_transition_state_tx(conn)
            if existing is None:
                raise CapacityTransitionStateError(
                    "capacity_transition_unconfigured"
                )
            if (
                str(existing["release_id"]) != release
                or str(existing["bootstrap_epoch_id"]) != epoch
            ):
                raise CapacityTransitionStateError(
                    "capacity_transition_identity_conflict"
                )
            existing_state = str(existing["state"])
            if existing_state == CAPACITY_STEADY_ACTIVE:
                if int(existing["generation"]) != expected_generation + 1:
                    raise CapacityTransitionStateError(
                        "capacity_transition_generation_changed"
                    )
                if all(existing[field] == value for field, value in bindings.items()):
                    conn.commit()
                    return self._public_capacity_transition_state(existing)
                raise CapacityTransitionStateError(
                    "capacity_transition_steady_binding_conflict"
                )
            if existing_state != CAPACITY_BOOTSTRAP_PRODUCTION:
                raise CapacityTransitionStateError(
                    "capacity_transition_state_invalid"
                )
            if int(existing["generation"]) != expected_generation:
                raise CapacityTransitionStateError(
                    "capacity_transition_generation_changed"
                )
            next_generation = expected_generation + 1
            updated = conn.execute(
                """
                UPDATE rca_capacity_transition_state
                   SET state = 'STEADY_ACTIVE', generation = ?,
                       final_ledger_sha256 = ?,
                       transition_authorization_sha256 = ?,
                       transition_authorization_fingerprint = ?,
                       transition_receipt_sha256 = ?,
                       transition_receipt_fingerprint = ?,
                       commit_marker_sha256 = ?,
                       commit_marker_fingerprint = ?,
                       evidence_bundle_sha256 = ?,
                       evidence_bundle_fingerprint = ?,
                       authorization_issued_at = ?,
                       authorization_expires_at = ?,
                       receipt_created_at = ?, marker_committed_at = ?,
                       steady_activated_at = ?, updated_at = ?
                 WHERE singleton_id = 1
                   AND release_id = ? AND bootstrap_epoch_id = ?
                   AND state = 'BOOTSTRAP_PRODUCTION' AND generation = ?
                """,
                (
                    next_generation,
                    hashes["final_ledger_sha256"],
                    hashes["transition_authorization_sha256"],
                    hashes["transition_authorization_fingerprint"],
                    hashes["transition_receipt_sha256"],
                    hashes["transition_receipt_fingerprint"],
                    hashes["commit_marker_sha256"],
                    hashes["commit_marker_fingerprint"],
                    hashes["evidence_bundle_sha256"],
                    hashes["evidence_bundle_fingerprint"],
                    timestamps["authorization_issued_at"],
                    timestamps["authorization_expires_at"],
                    timestamps["receipt_created_at"],
                    timestamps["marker_committed_at"],
                    current,
                    current,
                    release,
                    epoch,
                    expected_generation,
                ),
            )
            if updated.rowcount != 1:
                raise CapacityTransitionStateError(
                    "capacity_transition_generation_changed"
                )
            conn.execute(
                """
                INSERT INTO rca_capacity_transition_audit(
                    release_id, bootstrap_epoch_id, from_state, to_state,
                    from_generation, to_generation,
                    final_ledger_sha256,
                    transition_authorization_sha256,
                    transition_authorization_fingerprint,
                    transition_receipt_sha256,
                    transition_receipt_fingerprint,
                    commit_marker_sha256, commit_marker_fingerprint,
                    evidence_bundle_sha256, evidence_bundle_fingerprint,
                    authorization_issued_at, authorization_expires_at,
                    receipt_created_at, marker_committed_at, transitioned_at
                ) VALUES(
                    ?, ?, 'BOOTSTRAP_PRODUCTION', 'STEADY_ACTIVE', ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                """,
                (
                    release,
                    epoch,
                    expected_generation,
                    next_generation,
                    hashes["final_ledger_sha256"],
                    hashes["transition_authorization_sha256"],
                    hashes["transition_authorization_fingerprint"],
                    hashes["transition_receipt_sha256"],
                    hashes["transition_receipt_fingerprint"],
                    hashes["commit_marker_sha256"],
                    hashes["commit_marker_fingerprint"],
                    hashes["evidence_bundle_sha256"],
                    hashes["evidence_bundle_fingerprint"],
                    timestamps["authorization_issued_at"],
                    timestamps["authorization_expires_at"],
                    timestamps["receipt_created_at"],
                    timestamps["marker_committed_at"],
                    current,
                ),
            )
            row = self._capacity_transition_state_tx(conn)
            if row is None or int(row["generation"]) != next_generation:
                raise CapacityTransitionStateError(
                    "capacity_transition_cas_lost"
                )
            integrity_error = self._capacity_transition_integrity_error_tx(conn)
            if integrity_error:
                raise CapacityTransitionStateError(
                    f"capacity_transition_cas_invalid:{integrity_error}"
                )
            conn.commit()
            return self._public_capacity_transition_state(row)
        except Exception:
            if conn.in_transaction:
                conn.rollback()
            raise
        finally:
            conn.close()

    def activation_slot_authorizations(
        self,
        *,
        epoch_id: str,
    ) -> dict[str, dict[str, str | None]]:
        """Return the immutable, payload-free slot authorization bindings."""
        identity = self._normalize_activation_epoch_id(epoch_id)
        conn = self._connect()
        try:
            epoch = self._current_activation_epoch_tx(conn)
            if epoch is None or str(epoch["epoch_id"]) != identity:
                raise ActivationEpochError("activation_epoch_not_current")
            rows = conn.execute(
                f"""
                SELECT slot_kind, authorized_source_kind,
                       authorized_identity_sha256
                  FROM rca_activation_budget_slots
                 WHERE epoch_id = ?
                   AND slot_kind IN ({_ACTIVATION_RELEASE_SLOT_SQL})
                 ORDER BY slot_kind
                """,
                (identity,),
            ).fetchall()
            if len(rows) != len(ACTIVATION_RELEASE_SLOT_KINDS) or {
                str(row["slot_kind"]) for row in rows
            } != set(ACTIVATION_RELEASE_SLOT_KINDS):
                raise ActivationEpochError("activation_slot_set_invalid")
            return {
                str(row["slot_kind"]): {
                    "source_kind": (
                        str(row["authorized_source_kind"])
                        if row["authorized_source_kind"] is not None
                        else None
                    ),
                    "source_identity_sha256": (
                        str(row["authorized_identity_sha256"])
                        if row["authorized_identity_sha256"] is not None
                        else None
                    ),
                }
                for row in rows
            }
        finally:
            conn.close()

    def authorize_activation_slot(
        self,
        *,
        epoch_id: str,
        slot_kind: str,
        source_kind: str,
        source_identity: Mapping[str, Any],
        operator: str,
        reason: str,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        """Bind one bounded slot to one exact, payload-free source identity hash."""
        identity = self._normalize_activation_epoch_id(epoch_id)
        slot = str(slot_kind or "").strip()
        if slot not in ACTIVATION_SLOT_KINDS:
            raise ActivationEpochError("activation_slot_kind_invalid")
        kind = str(source_kind or "").strip()
        expected_kind = "kafka" if slot == "kafka_success" else "manual"
        if kind != expected_kind:
            raise ActivationEpochError("activation_slot_source_kind_mismatch")
        source_sha, normalized_source = self._normalize_activation_source_identity(
            kind, source_identity
        )
        actor, justification = self._normalize_activation_audit_text(operator, reason)
        current = _iso(now)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            epoch = self._current_activation_epoch_tx(conn)
            if epoch is None or str(epoch["epoch_id"]) != identity:
                raise ActivationEpochError("activation_epoch_not_current")
            if str(epoch["state"]) != "preauthorized":
                raise ActivationEpochError("activation_slot_authorization_closed")
            if kind == "kafka":
                start_fence = json.loads(str(epoch["partition_start_fence_json"]))
                topic = str(normalized_source["topic"])
                partition = str(normalized_source["partition"])
                offset = int(normalized_source["offset"])
                if topic not in start_fence or partition not in start_fence[topic]:
                    raise ActivationEpochError("activation_kafka_partition_not_fenced")
                if offset < int(start_fence[topic][partition]):
                    raise ActivationEpochError("activation_kafka_before_start_fence")
            reused = conn.execute(
                """
                SELECT slot_kind FROM rca_activation_budget_slots
                 WHERE epoch_id = ? AND slot_kind != ?
                   AND authorized_source_kind = ?
                   AND authorized_identity_sha256 = ?
                """,
                (identity, slot, kind, source_sha),
            ).fetchone()
            if reused is not None:
                raise ActivationEpochError("activation_slot_identity_reused")
            row = conn.execute(
                """
                SELECT * FROM rca_activation_budget_slots
                 WHERE epoch_id = ? AND slot_kind = ?
                """,
                (identity, slot),
            ).fetchone()
            if row is None:
                raise ActivationEpochError("activation_slot_missing")
            if row["authorized_identity_sha256"] is not None:
                if (
                    str(row["authorized_source_kind"]) != kind
                    or str(row["authorized_identity_sha256"]) != source_sha
                    or str(row["authorized_operator"] or "") != actor
                    or str(row["authorized_reason"] or "") != justification
                ):
                    raise ActivationEpochError("activation_slot_identity_conflict")
            else:
                conn.execute(
                    """
                    UPDATE rca_activation_budget_slots
                       SET authorized_source_kind = ?,
                           authorized_identity_sha256 = ?, authorized_at = ?,
                           authorized_operator = ?, authorized_reason = ?
                     WHERE epoch_id = ? AND slot_kind = ?
                       AND authorized_identity_sha256 IS NULL
                    """,
                    (
                        kind,
                        source_sha,
                        current,
                        actor,
                        justification,
                        identity,
                        slot,
                    ),
                )
            conn.commit()
            return {
                "epoch_id": identity,
                "slot_kind": slot,
                "source_kind": kind,
                "source_identity_sha256": source_sha,
            }
        except Exception:
            if conn.in_transaction:
                conn.rollback()
            raise
        finally:
            conn.close()

    @staticmethod
    def _validate_partition_end_fence(start_json: str, end_json: str) -> None:
        start = json.loads(start_json)
        end = json.loads(end_json)
        if set(start) != set(end):
            raise ActivationEpochError("activation_partition_end_fence_keys_changed")
        for topic, start_partitions in start.items():
            end_partitions = end.get(topic)
            if not isinstance(end_partitions, dict) or set(start_partitions) != set(
                end_partitions
            ):
                raise ActivationEpochError(
                    "activation_partition_end_fence_keys_changed"
                )
            for partition, start_offset in start_partitions.items():
                if int(end_partitions[partition]) < int(start_offset):
                    raise ActivationEpochError(
                        "activation_partition_end_fence_regressed"
                    )

    @staticmethod
    def _activation_inflight_writes_tx(conn: sqlite3.Connection) -> int:
        total = int(
            conn.execute(
                "SELECT COUNT(*) FROM rca_outbox WHERE status = 'claimed'"
            ).fetchone()[0]
        )
        delivery_effects = conn.execute(
            """
            SELECT 1 FROM sqlite_master
             WHERE type = 'table' AND name = 'rca_delivery_effects'
            """
        ).fetchone()
        if delivery_effects is not None:
            total += int(
                conn.execute(
                    """
                    SELECT COUNT(*) FROM rca_delivery_effects
                     WHERE status = 'claimed'
                    """
                ).fetchone()[0]
            )
        return total

    @staticmethod
    def _activation_terminal_execution_complete_tx(
        conn: sqlite3.Connection,
        *,
        business_key: str,
        submission_key: str,
        generation: int,
    ) -> bool:
        """Require the terminal canary's durable delivery, not just its quarantine.

        A terminal canary is allowed to end at the submission boundary.  Its
        outbox row is then quarantined, while the execution watch and terminal
        delivery still have to settle through both required Feishu effects.
        Keeping this predicate in the control store makes ingress readiness and
        the release-binding validator use the same fail-closed contract.
        """
        required_tables = {
            "rca_execution_watch",
            "rca_delivery_jobs",
            "rca_delivery_effects",
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
                   w.last_error_code, j.status AS job_status,
                   j.delivery_id AS job_delivery_id, j.outcome AS job_outcome,
                   j.terminal_state,
                   j.terminal_error_code
              FROM rca_execution_watch AS w
              LEFT JOIN rca_delivery_jobs AS j
                ON j.delivery_id = w.delivery_id
             WHERE w.submission_key = ?
               AND w.business_key = ?
               AND w.generation = ?
            """,
            (submission_key, business_key, generation),
        ).fetchone()
        if watch is None or (
            str(watch["state"] or "") != "delivery_created"
            or watch["task_id"] is not None
            or not str(watch["terminal_at"] or "")
            or not str(watch["last_error_code"] or "")
            or not str(watch["delivery_id"] or "")
            or str(watch["job_status"] or "") != "delivered"
            or str(watch["job_outcome"] or "") not in {"terminal_failed", "quarantined"}
            or not str(watch["terminal_state"] or "")
            or not str(watch["terminal_error_code"] or "")
            or str(watch["delivery_id"] or "")
            != str(watch["job_delivery_id"] or "")
        ):
            return False
        effects = conn.execute(
            """
            SELECT s.effect_kind, s.required, s.status AS subscription_status,
                   s.delivery_id AS subscription_delivery_id,
                   s.effect_key AS subscription_effect_key,
                   e.status AS effect_status, e.outcome AS effect_outcome,
                   e.completed_at
              FROM rca_delivery_subscriptions AS s
              LEFT JOIN rca_delivery_effects AS e
                ON e.effect_key = s.effect_key
             WHERE s.business_key = ?
               AND s.generation = ?
               AND s.required = 1
            ORDER BY s.effect_kind
            """,
            (business_key, generation),
        ).fetchall()
        if len(effects) != 2 or {
            str(row["effect_kind"] or "") for row in effects
        } != {"feishu_issue_comment", "feishu_thread_reply"}:
            return False
        delivery_id = str(watch["delivery_id"])
        return all(
            int(row["required"] or 0) == 1
            and str(row["subscription_status"] or "") == "materialized"
            and str(row["subscription_delivery_id"] or "") == delivery_id
            and str(row["subscription_effect_key"] or "")
            and str(row["effect_status"] or "") == "succeeded"
            and str(row["effect_outcome"] or "") == str(watch["job_outcome"])
            and bool(str(row["completed_at"] or ""))
            for row in effects
        )

    @classmethod
    def _activation_completed_bound_slot_count_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        epoch_id: str,
    ) -> int:
        bound_slots = conn.execute(
            f"""
            SELECT s.slot_kind, al.business_key, al.submission_key,
                   al.generation, o.status AS outbox_status
              FROM rca_activation_budget_slots AS s
              JOIN rca_activation_admission_ledger AS al
                ON al.epoch_id = s.epoch_id
               AND al.ledger_id = s.consumed_ledger_id
               AND al.slot_kind = s.slot_kind
               AND al.source_identity_sha256 = s.authorized_identity_sha256
               AND al.decision = 'admit'
               AND al.bound_at IS NOT NULL
              JOIN business_triggers AS t
                ON t.activation_epoch_id = al.epoch_id
               AND t.activation_ledger_id = al.ledger_id
               AND t.business_key = al.business_key
               AND t.submission_key = al.submission_key
               AND t.generation = al.generation
              JOIN rca_outbox AS o
                ON o.activation_epoch_id = al.epoch_id
               AND o.activation_ledger_id = al.ledger_id
               AND o.business_key = al.business_key
               AND o.submission_key = al.submission_key
               AND o.generation = al.generation
             WHERE s.epoch_id = ?
               AND s.slot_kind IN ({_ACTIVATION_RELEASE_SLOT_SQL})
            """,
            (epoch_id,),
        ).fetchall()
        completed = 0
        for slot in bound_slots:
            slot_kind = str(slot["slot_kind"] or "")
            is_complete = (
                cls._activation_terminal_execution_complete_tx(
                    conn,
                    business_key=str(slot["business_key"] or ""),
                    submission_key=str(slot["submission_key"] or ""),
                    generation=int(slot["generation"] or 0),
                )
                if slot_kind == "manual_terminal_failure"
                else str(slot["outbox_status"] or "") == "completed"
            )
            if is_complete:
                completed += 1
        return completed

    @classmethod
    def _validate_consumed_activation_executions_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        epoch: sqlite3.Row,
        end_fence_json: str,
    ) -> str:
        epoch_id = str(epoch["epoch_id"])
        historical_hold = cls._require_historical_outbox_hold_match_tx(
            conn,
            epoch=epoch,
        )
        rows = conn.execute(
            f"""
            SELECT s.slot_kind, s.authorized_identity_sha256,
                   s.consumed_ledger_id, s.consumed_at,
                   al.ledger_id, al.epoch_id AS ledger_epoch_id,
                   al.source_identity_sha256, al.slot_kind AS ledger_slot_kind,
                   al.decision, al.bound_at, al.business_key,
                   al.submission_key, al.generation,
                   t.activation_epoch_id AS trigger_epoch_id,
                   t.activation_ledger_id AS trigger_ledger_id,
                   o.activation_epoch_id AS outbox_epoch_id,
                   o.activation_ledger_id AS outbox_ledger_id,
                   o.status AS outbox_status
             FROM rca_activation_budget_slots AS s
         LEFT JOIN rca_activation_admission_ledger AS al
                ON al.ledger_id = s.consumed_ledger_id
               AND al.epoch_id = s.epoch_id
         LEFT JOIN business_triggers AS t
                ON t.business_key = al.business_key
               AND t.submission_key = al.submission_key
               AND t.generation = al.generation
         LEFT JOIN rca_outbox AS o
                ON o.business_key = al.business_key
               AND o.submission_key = al.submission_key
               AND o.generation = al.generation
             WHERE s.epoch_id = ?
               AND s.slot_kind IN ({_ACTIVATION_RELEASE_SLOT_SQL})
             ORDER BY s.slot_kind
            """,
            (epoch_id,),
        ).fetchall()
        if len(rows) != len(ACTIVATION_RELEASE_SLOT_KINDS):
            raise ActivationEpochError("activation_bounded_execution_unbound")
        for row in rows:
            ledger_id = int(row["ledger_id"] or 0)
            if (
                row["consumed_ledger_id"] is None
                or not str(row["consumed_at"] or "")
                or ledger_id != int(row["consumed_ledger_id"] or 0)
                or str(row["ledger_epoch_id"] or "") != epoch_id
                or str(row["ledger_slot_kind"] or "") != str(row["slot_kind"])
                or str(row["decision"] or "") != "admit"
                or not str(row["bound_at"] or "")
                or str(row["source_identity_sha256"] or "")
                != str(row["authorized_identity_sha256"] or "")
                or str(row["trigger_epoch_id"] or "") != epoch_id
                or int(row["trigger_ledger_id"] or 0) != ledger_id
                or str(row["outbox_epoch_id"] or "") != epoch_id
                or int(row["outbox_ledger_id"] or 0) != ledger_id
                or (
                    str(row["slot_kind"] or "") == "manual_terminal_failure"
                    and not cls._activation_terminal_execution_complete_tx(
                        conn,
                        business_key=str(row["business_key"] or ""),
                        submission_key=str(row["submission_key"] or ""),
                        generation=int(row["generation"] or 0),
                    )
                )
                or (
                    str(row["slot_kind"] or "") != "manual_terminal_failure"
                    and str(row["outbox_status"] or "") != "completed"
                )
            ):
                raise ActivationEpochError("activation_bounded_execution_unbound")
        start_fence = json.loads(str(epoch["partition_start_fence_json"]))
        unexpected_admissions = int(
            conn.execute(
                """
                SELECT COUNT(*) FROM rca_activation_admission_ledger
                 WHERE epoch_id = ? AND decision IN ('admit', 'join')
                   AND ledger_id NOT IN (
                       SELECT consumed_ledger_id
                         FROM rca_activation_budget_slots
                        WHERE epoch_id = ? AND consumed_ledger_id IS NOT NULL
                   )
                """,
                (epoch_id, epoch_id),
            ).fetchone()[0]
        )
        admitted_ledgers = int(
            conn.execute(
                """
                SELECT COUNT(*) FROM rca_activation_admission_ledger
                 WHERE epoch_id = ? AND decision = 'admit'
                """,
                (epoch_id,),
            ).fetchone()[0]
        )
        unbound_ledger = int(
            conn.execute(
                """
                SELECT COUNT(*)
                  FROM rca_activation_admission_ledger AS al
                 WHERE al.epoch_id = ?
                   AND al.decision IN ('admit', 'shadow')
                   AND (
                       al.bound_at IS NULL
                    OR NOT EXISTS (
                        SELECT 1 FROM business_triggers AS t
                         WHERE t.activation_epoch_id = al.epoch_id
                           AND t.activation_ledger_id = al.ledger_id
                           AND t.business_key = al.business_key
                           AND t.submission_key = al.submission_key
                           AND t.generation = al.generation
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
        historical_blocked = int(
            conn.execute(
                """
                SELECT COUNT(*) FROM rca_outbox AS o
                 WHERE o.status IN ('pending', 'claimed')
                   AND NOT EXISTS (
                       SELECT 1
                         FROM rca_activation_historical_outbox_hold_items AS held
                        WHERE held.epoch_id = ?
                          AND held.outbox_id = o.outbox_id
                   )
                   AND (
                       o.activation_epoch_id IS NULL
                       OR o.activation_epoch_id != ?
                       OR o.activation_ledger_id IS NULL
                       OR NOT EXISTS (
                           SELECT 1
                             FROM rca_activation_admission_ledger AS al
                            WHERE al.ledger_id = o.activation_ledger_id
                              AND al.epoch_id = o.activation_epoch_id
                              AND al.decision = 'admit'
                       )
                   )
                """,
                (epoch_id, epoch_id),
            ).fetchone()[0]
        )
        historical_held = int(
            conn.execute(
                """
                SELECT COUNT(*) FROM rca_outbox AS o
                 WHERE o.status = 'shadow'
                   AND (
                       o.activation_epoch_id IS NULL
                       OR o.activation_epoch_id != ?
                   )
                """,
                (epoch_id,),
            ).fetchone()[0]
        )
        unadjudicated_shadow = int(
            conn.execute(
                """
                SELECT COUNT(*) FROM rca_outbox AS o
                 WHERE o.status = 'shadow'
                   AND (
                       o.activation_ledger_id IS NULL
                       OR NOT EXISTS (
                           SELECT 1
                             FROM rca_activation_admission_ledger AS al
                            WHERE al.ledger_id = o.activation_ledger_id
                              AND al.epoch_id = o.activation_epoch_id
                              AND al.business_key = o.business_key
                              AND al.submission_key = o.submission_key
                              AND al.generation = o.generation
                       )
                   )
                """
            ).fetchone()[0]
        )
        if (
            admitted_ledgers != len(ACTIVATION_RELEASE_SLOT_KINDS)
            or unexpected_admissions
        ):
            raise ActivationEpochError("activation_unexpected_admission")
        if unbound_ledger:
            raise ActivationEpochError("activation_bounded_execution_unbound")
        if historical_blocked or historical_held:
            raise ActivationEpochError("activation_historical_backlog_not_drained")
        if unadjudicated_shadow:
            raise ActivationEpochError("activation_shadow_binding_invalid")
        bindings = {
            str(row["slot_kind"]): {
                "business_key": str(row["business_key"] or ""),
                "submission_key": str(row["submission_key"] or ""),
                "generation": int(row["generation"] or 0),
                "ledger_id": int(row["ledger_id"]),
                "source_identity_sha256": str(
                    row["source_identity_sha256"]
                ),
            }
            for row in rows
        }
        release_binding = {
            "epoch_id": epoch_id,
            "state": "bounded_active",
            "preauthorization_fingerprint": str(epoch["preauthorization_fingerprint"]),
            "preauthorization_gate_receipt_sha256": str(
                epoch["preauthorization_gate_receipt_sha256"]
            ),
            "preauthorization_capsule_sha256": str(
                epoch["preauthorization_capsule_sha256"]
            ),
            "preproduction_fingerprint": str(epoch["preproduction_fingerprint"]),
            "preproduction_gate_receipt_sha256": str(
                epoch["preproduction_gate_receipt_sha256"]
            ),
            "preproduction_capsule_sha256": str(epoch["preproduction_capsule_sha256"]),
            "config_sha256": str(epoch["config_sha256"]),
            "db_logical_identity_sha256": str(epoch["db_logical_identity_sha256"]),
            "bounded_activated_at": str(epoch["bounded_activated_at"]),
            "partition_start_fence_sha256": str(epoch["partition_start_fence_sha256"]),
            "partition_start_fence": start_fence,
            "kafka_proof": {
                "mode": ACTIVATION_KAFKA_PROOF_MODE,
                "preauthorization_gate_receipt_sha256": str(
                    epoch["preauthorization_gate_receipt_sha256"]
                ),
                "partition_start_fence_sha256": str(
                    epoch["partition_start_fence_sha256"]
                ),
                "partition_end_fence_sha256": hashlib.sha256(
                    end_fence_json.encode("utf-8")
                ).hexdigest(),
            },
            "slot_bindings": bindings,
            "unexpected_admissions": 0,
            "historical_blocked": 0,
            "historical_held": 0,
            "historical_hold_count": historical_hold["sealed_count"],
            "historical_hold_sha256": historical_hold["sealed_sha256"],
            "historical_hold_row_schema_version": historical_hold[
                "row_schema_version"
            ],
            "pending_inbox": 0,
            "unbound_ledger": 0,
            "inflight_writes": 0,
        }
        return _canonical_sha256(release_binding)

    @staticmethod
    def _validate_current_activation_shadows_tx(
        conn: sqlite3.Connection,
        *,
        epoch: sqlite3.Row,
        end_fence_json: str,
    ) -> None:
        epoch_id = str(epoch["epoch_id"])
        start_fence = json.loads(str(epoch["partition_start_fence_json"]))
        end_fence = json.loads(end_fence_json)
        rows = conn.execute(
            """
            SELECT o.activation_ledger_id, o.business_key, o.submission_key,
                   o.generation, o.source_topic, o.source_partition,
                   o.source_offset, al.ledger_id, al.decision, al.bound_at
              FROM rca_outbox AS o
         LEFT JOIN rca_activation_admission_ledger AS al
                ON al.epoch_id = o.activation_epoch_id
               AND al.ledger_id = o.activation_ledger_id
               AND al.business_key = o.business_key
               AND al.submission_key = o.submission_key
               AND al.generation = o.generation
             WHERE o.activation_epoch_id = ? AND o.status = 'shadow'
            """,
            (epoch_id,),
        ).fetchall()
        for row in rows:
            if (
                row["activation_ledger_id"] is None
                or row["ledger_id"] is None
                or str(row["decision"] or "") != "shadow"
                or not str(row["bound_at"] or "")
                or row["source_partition"] is None
                or row["source_offset"] is None
            ):
                raise ActivationEpochError("activation_shadow_binding_invalid")
            topic = str(row["source_topic"] or "")
            partition = str(row["source_partition"])
            offset = int(row["source_offset"])
            if (
                topic not in start_fence
                or partition not in start_fence[topic]
                or topic not in end_fence
                or partition not in end_fence[topic]
                or offset < int(start_fence[topic][partition])
                or offset >= int(end_fence[topic][partition])
            ):
                raise ActivationEpochError("activation_shadow_outside_end_fence")

    def activation_release_binding_sha256(
        self,
        *,
        epoch_id: str,
        partition_end_fence: Mapping[str, Any],
    ) -> str:
        """Return the exact bounded release binding from one read snapshot."""
        identity = self._normalize_activation_epoch_id(epoch_id)
        end_fence_json = self._normalize_partition_fence(partition_end_fence)
        conn = self._connect()
        try:
            conn.execute("BEGIN")
            epoch = self._current_activation_epoch_tx(conn)
            if epoch is None or str(epoch["epoch_id"]) != identity:
                raise ActivationEpochError("activation_epoch_not_current")
            if str(epoch["state"]) not in {"bounded_active", "confirmed"}:
                raise ActivationEpochError("activation_epoch_state_changed")
            if int(
                conn.execute(
                    "SELECT COUNT(*) FROM kafka_inbox WHERE decision = 'pending'"
                ).fetchone()[0]
            ):
                raise ActivationEpochError("activation_pending_inbox_not_drained")
            if self._activation_inflight_writes_tx(conn):
                raise ActivationEpochError("activation_inflight_writes_not_drained")
            self._validate_partition_end_fence(
                str(epoch["partition_start_fence_json"]), end_fence_json
            )
            result = self._validate_consumed_activation_executions_tx(
                conn,
                epoch=epoch,
                end_fence_json=end_fence_json,
            )
            conn.rollback()
            return result
        except Exception:
            if conn.in_transaction:
                conn.rollback()
            raise
        finally:
            conn.close()

    def transition_activation_epoch(
        self,
        *,
        epoch_id: str,
        target_state: ActivationEpochState,
        expected_state: ActivationEpochState | None = None,
        partition_end_fence: Mapping[str, Any] | None = None,
        production_fingerprint: str = "",
        production_gate_receipt_sha256: str = "",
        expected_config_sha256: str = "",
        expected_db_logical_identity_sha256: str = "",
        expected_partition_start_fence_sha256: str = "",
        expected_release_binding_sha256: str = "",
        operator: str,
        reason: str,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        """Advance one current epoch through the fail-closed release state machine."""
        identity = self._normalize_activation_epoch_id(epoch_id)
        target = str(target_state or "").strip()
        if target not in ACTIVATION_EPOCH_STATES:
            raise ActivationEpochError("activation_target_state_invalid")
        if target == "preauthorized":
            raise ActivationEpochError(
                "activation_preproduction_capsule_required"
            )
        expected = str(expected_state or "").strip()
        if expected and expected not in ACTIVATION_EPOCH_STATES:
            raise ActivationEpochError("activation_expected_state_invalid")
        actor, justification = self._normalize_activation_audit_text(operator, reason)
        end_fence_json = (
            self._normalize_partition_fence(partition_end_fence)
            if partition_end_fence is not None
            else None
        )
        production_hash = str(production_fingerprint or "").strip().lower()
        receipt_hash = str(production_gate_receipt_sha256 or "").strip().lower()
        expected_confirmation_bindings = {
            "config_sha256": str(expected_config_sha256 or "").strip().lower(),
            "db_logical_identity_sha256": str(
                expected_db_logical_identity_sha256 or ""
            )
            .strip()
            .lower(),
            "partition_start_fence_sha256": str(
                expected_partition_start_fence_sha256 or ""
            )
            .strip()
            .lower(),
            "release_binding_sha256": str(
                expected_release_binding_sha256 or ""
            )
            .strip()
            .lower(),
        }
        for field, value in tuple(expected_confirmation_bindings.items()):
            if value:
                expected_confirmation_bindings[field] = (
                    self._normalize_activation_sha256(value, field)
                )
        if any(expected_confirmation_bindings.values()) and target != "confirmed":
            raise ActivationEpochError(
                "activation_confirmation_binding_only_allowed_on_confirm"
            )
        if target == "confirmed" and not all(
            expected_confirmation_bindings.values()
        ):
            raise ActivationEpochError(
                "activation_confirmation_preconditions_required"
            )
        if target == "confirmed":
            production_hash = self._normalize_activation_sha256(
                production_hash, "production_fingerprint"
            )
            receipt_hash = self._normalize_activation_sha256(
                receipt_hash, "production_gate_receipt_sha256"
            )
        elif production_hash or receipt_hash:
            raise ActivationEpochError(
                "activation_production_binding_only_allowed_on_confirm"
            )
        current = _iso(now)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            epoch = self._current_activation_epoch_tx(conn)
            if epoch is None or str(epoch["epoch_id"]) != identity:
                raise ActivationEpochError("activation_epoch_not_current")
            for field, value in expected_confirmation_bindings.items():
                if field == "release_binding_sha256":
                    continue
                if value and str(epoch[field] or "") != value:
                    raise ActivationEpochError(
                        "activation_confirmation_epoch_binding_changed"
                    )
            prior = str(epoch["state"])
            if expected and prior != expected:
                raise ActivationEpochError("activation_epoch_state_changed")
            if prior == target:
                if target in {"bounded_active", "steady_active"}:
                    self._require_historical_outbox_hold_match_tx(
                        conn,
                        epoch=epoch,
                    )
                if target == "confirmed":
                    if end_fence_json is None:
                        raise ActivationEpochError(
                            "activation_partition_end_fence_required"
                        )
                    observed_sha = str(epoch["partition_end_fence_sha256"] or "")
                    expected_sha = hashlib.sha256(
                        end_fence_json.encode("utf-8")
                    ).hexdigest()
                    if (
                        observed_sha != expected_sha
                        or str(epoch["production_fingerprint"] or "")
                        != production_hash
                        or str(epoch["production_gate_receipt_sha256"] or "")
                        != receipt_hash
                    ):
                        raise ActivationEpochError(
                            "activation_confirmation_binding_conflict"
                        )
                    release_binding_sha256 = (
                        self._validate_consumed_activation_executions_tx(
                            conn,
                            epoch=epoch,
                            end_fence_json=end_fence_json,
                        )
                    )
                    if (
                        release_binding_sha256
                        != expected_confirmation_bindings["release_binding_sha256"]
                    ):
                        raise ActivationEpochError(
                            "activation_confirmation_release_binding_changed"
                        )
                conn.commit()
                return self._public_activation_epoch(epoch)
            pending_inbox = int(
                conn.execute(
                    "SELECT COUNT(*) FROM kafka_inbox WHERE decision = 'pending'"
                ).fetchone()[0]
            )
            if pending_inbox:
                raise ActivationEpochError("activation_pending_inbox_not_drained")
            if target in {"confirmed", "steady_active", "aborted"}:
                if self._activation_inflight_writes_tx(conn):
                    raise ActivationEpochError("activation_inflight_writes_not_drained")
            allowed = {
                "safe_off": {"aborted"},
                "preauthorized": {"bounded_active", "aborted"},
                "bounded_active": {"confirmed", "aborted"},
                "confirmed": {"steady_active", "aborted"},
                "steady_active": {"aborted"},
                "aborted": set(),
            }
            if target not in allowed[prior]:
                raise ActivationEpochError("activation_state_transition_invalid")
            fields = ["state = ?", "updated_at = ?"]
            parameters: list[Any] = [target, current]
            if target == "bounded_active":
                self._require_historical_outbox_hold_match_tx(
                    conn,
                    epoch=epoch,
                )
                if any(
                    not str(epoch[field] or "")
                    for field in (
                        "preproduction_fingerprint",
                        "preproduction_gate_receipt_sha256",
                        "preproduction_capsule_sha256",
                    )
                ):
                    raise ActivationEpochError(
                        "activation_preproduction_binding_missing"
                    )
                authorized = int(
                    conn.execute(
                        f"""
                        SELECT COUNT(*) FROM rca_activation_budget_slots
                         WHERE epoch_id = ?
                           AND slot_kind IN ({_ACTIVATION_RELEASE_SLOT_SQL})
                           AND authorized_source_kind IS NOT NULL
                           AND authorized_identity_sha256 IS NOT NULL
                        """,
                        (identity,),
                    ).fetchone()[0]
                )
                optional_mutated = int(
                    conn.execute(
                        f"""
                        SELECT COUNT(*) FROM rca_activation_budget_slots
                         WHERE epoch_id = ?
                           AND slot_kind NOT IN ({_ACTIVATION_RELEASE_SLOT_SQL})
                           AND (
                               authorized_source_kind IS NOT NULL
                            OR authorized_identity_sha256 IS NOT NULL
                            OR consumed_ledger_id IS NOT NULL
                           )
                        """,
                        (identity,),
                    ).fetchone()[0]
                )
                if optional_mutated:
                    raise ActivationEpochError(
                        "activation_nonrelease_slot_mutated"
                    )
                if authorized != len(ACTIVATION_RELEASE_SLOT_KINDS):
                    raise ActivationEpochError("activation_slots_not_preauthorized")
                fields.append("bounded_activated_at = ?")
                parameters.append(current)
            elif target == "confirmed":
                consumed = int(
                    conn.execute(
                        f"""
                        SELECT COUNT(*) FROM rca_activation_budget_slots
                         WHERE epoch_id = ?
                           AND slot_kind IN ({_ACTIVATION_RELEASE_SLOT_SQL})
                           AND consumed_ledger_id IS NOT NULL
                        """,
                        (identity,),
                    ).fetchone()[0]
                )
                if consumed != len(ACTIVATION_RELEASE_SLOT_KINDS):
                    raise ActivationEpochError("activation_bounded_budget_incomplete")
                if end_fence_json is None:
                    raise ActivationEpochError("activation_partition_end_fence_required")
                self._validate_partition_end_fence(
                    str(epoch["partition_start_fence_json"]), end_fence_json
                )
                release_binding_sha256 = (
                    self._validate_consumed_activation_executions_tx(
                    conn,
                    epoch=epoch,
                    end_fence_json=end_fence_json,
                )
                )
                if release_binding_sha256 != expected_confirmation_bindings[
                    "release_binding_sha256"
                ]:
                    raise ActivationEpochError(
                        "activation_confirmation_release_binding_changed"
                    )
                self._validate_current_activation_shadows_tx(
                    conn,
                    epoch=epoch,
                    end_fence_json=end_fence_json,
                )
                end_sha = hashlib.sha256(end_fence_json.encode("utf-8")).hexdigest()
                fields.extend(
                    [
                        "partition_end_fence_json = ?",
                        "partition_end_fence_sha256 = ?",
                        "production_fingerprint = ?",
                        "production_gate_receipt_sha256 = ?",
                        "confirmed_at = ?",
                    ]
                )
                parameters.extend(
                    [
                        end_fence_json,
                        end_sha,
                        production_hash,
                        receipt_hash,
                        current,
                    ]
                )
            elif target == "steady_active":
                self._require_historical_outbox_hold_match_tx(
                    conn,
                    epoch=epoch,
                )
                if (
                    not str(epoch["production_fingerprint"] or "")
                    or not str(epoch["production_gate_receipt_sha256"] or "")
                    or not str(epoch["partition_end_fence_sha256"] or "")
                ):
                    raise ActivationEpochError(
                        "activation_production_confirmation_missing"
                    )
                shadow_backlog = int(
                    conn.execute(
                        "SELECT COUNT(*) FROM rca_outbox WHERE status = 'shadow'"
                    ).fetchone()[0]
                )
                if shadow_backlog:
                    raise ActivationEpochError(
                        "activation_shadow_backlog_not_drained"
                    )
                fields.append("steady_activated_at = ?")
                parameters.append(current)
            elif target == "aborted":
                fields.append("aborted_at = ?")
                parameters.append(current)
            parameters.extend([identity, prior])
            updated = conn.execute(
                f"""
                UPDATE rca_activation_epochs SET {', '.join(fields)}
                 WHERE epoch_id = ? AND is_current = 1 AND state = ?
                """,
                parameters,
            )
            if updated.rowcount != 1:
                raise ActivationEpochError("activation_epoch_state_changed")
            row = self._current_activation_epoch_tx(conn)
            if row is None:
                raise ActivationEpochError("activation_epoch_transition_lost")
            self._insert_activation_transition_audit_tx(
                conn,
                epoch=row,
                from_state=prior,
                to_state=target,
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

    @staticmethod
    def _activation_held_outcome(entrypoint: str, state: str) -> tuple[str, str]:
        if entrypoint == "kafka_ingest" and state != "aborted":
            return "shadow", f"activation_epoch_held_{state}"
        return "reject", f"activation_epoch_rejected_{state}"

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
        slot_kind: str,
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
                    source_identity_sha256, slot_kind, decision, reason,
                    business_key, submission_key, generation,
                    first_adjudicated_at, last_adjudicated_at, admitted_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    epoch_id,
                    admission_key,
                    entrypoint,
                    source_kind,
                    source_identity_sha256,
                    slot_kind or None,
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
               SET entrypoint = ?, slot_kind = ?, decision = ?, reason = ?,
                   adjudication_count = adjudication_count + 1,
                   last_adjudicated_at = ?,
                   admitted_at = CASE WHEN ? = 'admit' THEN ? ELSE admitted_at END
             WHERE ledger_id = ?
            """,
            (
                entrypoint,
                slot_kind or None,
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
        requested_slot_kind: str = "",
        activation_required: bool = False,
        ingress_epoch_id: str | None = None,
        ingress_state: str | None = None,
        now: datetime | None = None,
    ) -> ActivationAdmissionDecision:
        """Adjudicate and consume inside the caller's ``BEGIN IMMEDIATE``.

        The caller must create or mutate the generation and outbox before the
        same transaction commits, then call ``bind_activation_admission_tx``.
        A committed ledger reservation is intentionally never returned after a
        process crash; an exact retry reuses the same ledger id without another
        slot consumption.
        """
        if not conn.in_transaction:
            raise ActivationEpochError("activation_transaction_required")
        point = str(entrypoint or "").strip()
        kind = str(source_kind or "").strip()
        if point not in ACTIVATION_ENTRYPOINTS:
            raise ActivationEpochError("activation_entrypoint_invalid")
        expected_kind = "manual" if point == "manual_admit" else "kafka"
        if kind != expected_kind:
            raise ActivationEpochError("activation_entrypoint_source_mismatch")
        source_sha, normalized_source = cls._normalize_activation_source_identity(
            kind, source_identity
        )
        business = str(business_key or "").strip()
        submission = str(submission_key or "").strip()
        if not business or not submission or len(business) > 500 or len(submission) > 500:
            raise ActivationEpochError("activation_execution_identity_invalid")
        if isinstance(generation, bool):
            raise ActivationEpochError("activation_generation_invalid")
        try:
            generation_number = int(generation)
        except (TypeError, ValueError) as exc:
            raise ActivationEpochError("activation_generation_invalid") from exc
        if generation_number < 1:
            raise ActivationEpochError("activation_generation_invalid")
        if not isinstance(new_execution, bool) or not isinstance(
            activation_required, bool
        ):
            raise ActivationEpochError("activation_adjudication_flag_invalid")
        slot_kind = str(requested_slot_kind or "").strip()
        if slot_kind and slot_kind not in ACTIVATION_SLOT_KINDS:
            raise ActivationEpochError("activation_slot_kind_invalid")
        current = _iso(now)
        epoch = cls._current_activation_epoch_tx(conn)
        if epoch is None:
            if not activation_required and ingress_epoch_id is None:
                return ActivationAdmissionDecision(
                    epoch_id="",
                    epoch_state="legacy_unconfigured",
                    decision="admit",
                    reason="activation_legacy_unconfigured",
                    legacy_unconfigured=True,
                )
            decision, reason = cls._activation_held_outcome(point, "unconfigured")
            return ActivationAdmissionDecision(
                epoch_id="",
                epoch_state="unconfigured",
                decision=decision,  # type: ignore[arg-type]
                reason=reason,
            )

        epoch_id = str(epoch["epoch_id"])
        state = str(epoch["state"])
        if ingress_epoch_id is not None and str(ingress_epoch_id) != epoch_id:
            raise ActivationEpochError("activation_ingress_epoch_changed")
        admission_key = cls._activation_admission_key(
            source_kind=kind,
            source_identity_sha256=source_sha,
            business_key=business,
            submission_key=submission,
            generation=generation_number,
        )
        captured_state = str(ingress_state or "").strip()
        if (
            point == "kafka_ingest"
            and captured_state
            and captured_state != "legacy_unconfigured"
            and captured_state != state
        ):
            raise ActivationEpochError("activation_ingress_state_changed")
        if point == "kafka_ingest" and captured_state in {
            "safe_off",
            "preauthorized",
            "confirmed",
            "aborted",
        }:
            held_decision, held_reason = cls._activation_held_outcome(
                point, f"ingress_{captured_state}"
            )
            ledger_id, prior = cls._write_activation_ledger_tx(
                conn,
                epoch_id=epoch_id,
                admission_key=admission_key,
                entrypoint=point,
                source_kind=kind,
                source_identity_sha256=source_sha,
                slot_kind=slot_kind,
                decision=held_decision,
                reason=held_reason,
                business_key=business,
                submission_key=submission,
                generation=generation_number,
                current=current,
            )
            if prior == "admit":
                held_decision = "admit"
                held_reason = "activation_admission_idempotent"
            return ActivationAdmissionDecision(
                epoch_id=epoch_id,
                epoch_state=state,
                decision=held_decision,  # type: ignore[arg-type]
                reason=held_reason,
                ledger_id=ledger_id,
                slot_kind=slot_kind,
            )
        existing_trigger = conn.execute(
            """
            SELECT business_key, generation, source_event_id,
                   activation_epoch_id, activation_ledger_id
              FROM business_triggers WHERE submission_key = ?
            """,
            (submission,),
        ).fetchone()
        if existing_trigger is not None:
            if (
                str(existing_trigger["business_key"]) != business
                or int(existing_trigger["generation"]) != generation_number
            ):
                raise ActivationEpochError("activation_join_identity_conflict")
            prior_admission = conn.execute(
                """
                SELECT al.ledger_id, al.slot_kind
                  FROM rca_activation_admission_ledger AS al
                  JOIN rca_outbox AS o
                    ON o.activation_epoch_id = al.epoch_id
                   AND o.activation_ledger_id = al.ledger_id
                   AND o.business_key = al.business_key
                   AND o.submission_key = al.submission_key
                   AND o.generation = al.generation
                  JOIN rca_activation_budget_slots AS abs
                    ON abs.epoch_id = al.epoch_id
                   AND abs.consumed_ledger_id = al.ledger_id
                 WHERE al.epoch_id = ? AND al.admission_key = ?
                   AND al.decision = 'admit' AND al.bound_at IS NOT NULL
                """,
                (epoch_id, admission_key),
            ).fetchone()
            if point == "shadow_promotion" and state == "confirmed":
                if slot_kind:
                    raise ActivationEpochError(
                        "activation_confirmed_reconciliation_slot_forbidden"
                    )
                start_fence = json.loads(str(epoch["partition_start_fence_json"]))
                end_fence = json.loads(str(epoch["partition_end_fence_json"] or "{}"))
                topic = str(normalized_source["topic"])
                partition = str(normalized_source["partition"])
                offset = int(normalized_source["offset"])
                if (
                    topic not in start_fence
                    or partition not in start_fence[topic]
                    or topic not in end_fence
                    or partition not in end_fence[topic]
                    or offset < int(start_fence[topic][partition])
                    or offset >= int(end_fence[topic][partition])
                ):
                    raise ActivationEpochError(
                        "activation_confirmed_shadow_outside_fence"
                    )
                reconciliation = conn.execute(
                    """
                    SELECT al.ledger_id, al.decision, al.bound_at,
                           o.status AS outbox_status
                      FROM rca_activation_admission_ledger AS al
                      JOIN rca_outbox AS o
                        ON o.activation_epoch_id = al.epoch_id
                       AND o.activation_ledger_id = al.ledger_id
                       AND o.business_key = al.business_key
                       AND o.submission_key = al.submission_key
                       AND o.generation = al.generation
                     WHERE al.epoch_id = ? AND al.admission_key = ?
                       AND al.source_kind = 'kafka'
                       AND al.source_identity_sha256 = ?
                       AND al.business_key = ? AND al.submission_key = ?
                       AND al.generation = ? AND al.decision = 'shadow'
                       AND al.bound_at IS NOT NULL AND o.status = 'shadow'
                    """,
                    (
                        epoch_id,
                        admission_key,
                        source_sha,
                        business,
                        submission,
                        generation_number,
                    ),
                ).fetchone()
                if (
                    reconciliation is None
                    or str(existing_trigger["source_event_id"] or "")
                    != str(normalized_source["event_uid"])
                    or str(existing_trigger["activation_epoch_id"] or "") != epoch_id
                    or int(existing_trigger["activation_ledger_id"] or 0)
                    != int(reconciliation["ledger_id"])
                ):
                    raise ActivationEpochError(
                        "activation_confirmed_shadow_reconciliation_invalid"
                    )
                ledger_id, prior = cls._write_activation_ledger_tx(
                    conn,
                    epoch_id=epoch_id,
                    admission_key=admission_key,
                    entrypoint=point,
                    source_kind=kind,
                    source_identity_sha256=source_sha,
                    slot_kind="",
                    decision="admit",
                    reason="activation_confirmed_shadow_reconciliation",
                    business_key=business,
                    submission_key=submission,
                    generation=generation_number,
                    current=current,
                )
                if prior != "shadow" or ledger_id != int(reconciliation["ledger_id"]):
                    raise ActivationEpochError(
                        "activation_confirmed_shadow_reconciliation_lost"
                    )
                return ActivationAdmissionDecision(
                    epoch_id=epoch_id,
                    epoch_state=state,
                    decision="admit",
                    reason="activation_confirmed_shadow_reconciliation",
                    ledger_id=ledger_id,
                )
            if point == "manual_admit" and prior_admission is not None:
                conn.execute(
                    """
                    UPDATE rca_activation_admission_ledger
                       SET adjudication_count = adjudication_count + 1,
                           last_adjudicated_at = ?
                     WHERE ledger_id = ?
                    """,
                    (current, prior_admission["ledger_id"]),
                )
                return ActivationAdmissionDecision(
                    epoch_id=epoch_id,
                    epoch_state=state,
                    decision="join",
                    reason="activation_admission_idempotent",
                    ledger_id=int(prior_admission["ledger_id"]),
                    slot_kind=str(prior_admission["slot_kind"] or ""),
                )
            exact_kafka_replay = (
                point == "kafka_ingest"
                and str(existing_trigger["source_event_id"] or "")
                == str(normalized_source["event_uid"])
            )
            steady_join = state == "steady_active" and point in {
                "kafka_ingest",
                "manual_admit",
            }
            if point != "shadow_promotion" and (exact_kafka_replay or steady_join):
                ledger_id, _prior = cls._write_activation_ledger_tx(
                    conn,
                    epoch_id=epoch_id,
                    admission_key=admission_key,
                    entrypoint=point,
                    source_kind=kind,
                    source_identity_sha256=source_sha,
                    slot_kind="",
                    decision="join",
                    reason="activation_existing_generation_join",
                    business_key=business,
                    submission_key=submission,
                    generation=generation_number,
                    current=current,
                )
                return ActivationAdmissionDecision(
                    epoch_id=epoch_id,
                    epoch_state=state,
                    decision="join",
                    reason="activation_existing_generation_join",
                    ledger_id=ledger_id,
                )
            if point != "shadow_promotion":
                rejection_reason = (
                    f"activation_epoch_rejected_{state}"
                    if state in {
                        "safe_off",
                        "preauthorized",
                        "confirmed",
                        "aborted",
                    }
                    else "activation_existing_generation_not_eligible"
                )
                ledger_id, _prior = cls._write_activation_ledger_tx(
                    conn,
                    epoch_id=epoch_id,
                    admission_key=admission_key,
                    entrypoint=point,
                    source_kind=kind,
                    source_identity_sha256=source_sha,
                    slot_kind=slot_kind,
                    decision="reject",
                    reason=rejection_reason,
                    business_key=business,
                    submission_key=submission,
                    generation=generation_number,
                    current=current,
                )
                return ActivationAdmissionDecision(
                    epoch_id=epoch_id,
                    epoch_state=state,
                    decision="reject",
                    reason=rejection_reason,
                    ledger_id=ledger_id,
                    slot_kind=slot_kind,
                )
        if not new_execution:
            decision, reason = "reject", "activation_join_target_missing"
            ledger_id, _prior = cls._write_activation_ledger_tx(
                conn,
                epoch_id=epoch_id,
                admission_key=admission_key,
                entrypoint=point,
                source_kind=kind,
                source_identity_sha256=source_sha,
                slot_kind=slot_kind,
                decision=decision,
                reason=reason,
                business_key=business,
                submission_key=submission,
                generation=generation_number,
                current=current,
            )
            return ActivationAdmissionDecision(
                epoch_id=epoch_id,
                epoch_state=state,
                decision="reject",
                reason=reason,
                ledger_id=ledger_id,
                slot_kind=slot_kind,
            )

        def held(reason_code: str) -> ActivationAdmissionDecision:
            outcome, _state_reason = cls._activation_held_outcome(point, state)
            ledger_id, prior = cls._write_activation_ledger_tx(
                conn,
                epoch_id=epoch_id,
                admission_key=admission_key,
                entrypoint=point,
                source_kind=kind,
                source_identity_sha256=source_sha,
                slot_kind=slot_kind,
                decision=outcome,
                reason=reason_code,
                business_key=business,
                submission_key=submission,
                generation=generation_number,
                current=current,
            )
            if prior == "admit":
                return ActivationAdmissionDecision(
                    epoch_id=epoch_id,
                    epoch_state=state,
                    decision="admit",
                    reason="activation_admission_idempotent",
                    ledger_id=ledger_id,
                    slot_kind=slot_kind,
                )
            return ActivationAdmissionDecision(
                epoch_id=epoch_id,
                epoch_state=state,
                decision=outcome,  # type: ignore[arg-type]
                reason=reason_code,
                ledger_id=ledger_id,
                slot_kind=slot_kind,
            )

        if state != "bounded_active" and state != "steady_active":
            _outcome, held_reason = cls._activation_held_outcome(point, state)
            return held(held_reason)
        if state == "steady_active":
            ledger_id, prior = cls._write_activation_ledger_tx(
                conn,
                epoch_id=epoch_id,
                admission_key=admission_key,
                entrypoint=point,
                source_kind=kind,
                source_identity_sha256=source_sha,
                slot_kind="",
                decision="admit",
                reason="activation_steady_active",
                business_key=business,
                submission_key=submission,
                generation=generation_number,
                current=current,
            )
            return ActivationAdmissionDecision(
                epoch_id=epoch_id,
                epoch_state=state,
                decision="admit",
                reason=(
                    "activation_admission_idempotent"
                    if prior == "admit"
                    else "activation_steady_active"
                ),
                ledger_id=ledger_id,
            )

        if not slot_kind and kind == "manual":
            matching_slots = conn.execute(
                """
                SELECT slot_kind FROM rca_activation_budget_slots
                 WHERE epoch_id = ? AND authorized_source_kind = ?
                   AND authorized_identity_sha256 = ?
                 ORDER BY slot_kind
                """,
                (epoch_id, kind, source_sha),
            ).fetchall()
            if len(matching_slots) > 1:
                return held("activation_bounded_slot_ambiguous")
            if len(matching_slots) == 1:
                slot_kind = str(matching_slots[0]["slot_kind"])
        if not slot_kind:
            return held("activation_bounded_slot_required")
        slot = conn.execute(
            """
            SELECT * FROM rca_activation_budget_slots
             WHERE epoch_id = ? AND slot_kind = ?
            """,
            (epoch_id, slot_kind),
        ).fetchone()
        if slot is None:
            raise ActivationEpochError("activation_slot_missing")
        if (
            str(slot["authorized_source_kind"] or "") != kind
            or str(slot["authorized_identity_sha256"] or "") != source_sha
        ):
            return held("activation_bounded_identity_not_authorized")
        if kind == "kafka":
            start_fence = json.loads(str(epoch["partition_start_fence_json"]))
            topic = str(normalized_source["topic"])
            partition = str(normalized_source["partition"])
            offset = int(normalized_source["offset"])
            if topic not in start_fence or partition not in start_fence[topic]:
                return held("activation_kafka_partition_not_fenced")
            if offset < int(start_fence[topic][partition]):
                return held("activation_kafka_before_start_fence")
            if epoch["partition_end_fence_json"] is not None:
                end_fence = json.loads(str(epoch["partition_end_fence_json"]))
                if offset >= int(end_fence[topic][partition]):
                    return held("activation_kafka_at_or_after_end_fence")
        consumed_ledger_id = slot["consumed_ledger_id"]
        existing_ledger = conn.execute(
            """
            SELECT ledger_id, decision FROM rca_activation_admission_ledger
             WHERE epoch_id = ? AND admission_key = ?
            """,
            (epoch_id, admission_key),
        ).fetchone()
        if consumed_ledger_id is not None:
            if (
                existing_ledger is not None
                and int(consumed_ledger_id) == int(existing_ledger["ledger_id"])
                and str(existing_ledger["decision"]) == "admit"
            ):
                ledger_id, _prior = cls._write_activation_ledger_tx(
                    conn,
                    epoch_id=epoch_id,
                    admission_key=admission_key,
                    entrypoint=point,
                    source_kind=kind,
                    source_identity_sha256=source_sha,
                    slot_kind=slot_kind,
                    decision="admit",
                    reason="activation_admission_idempotent",
                    business_key=business,
                    submission_key=submission,
                    generation=generation_number,
                    current=current,
                )
                return ActivationAdmissionDecision(
                    epoch_id=epoch_id,
                    epoch_state=state,
                    decision="admit",
                    reason="activation_admission_idempotent",
                    ledger_id=ledger_id,
                    slot_kind=slot_kind,
                )
            return held("activation_bounded_slot_consumed")
        ledger_id, prior = cls._write_activation_ledger_tx(
            conn,
            epoch_id=epoch_id,
            admission_key=admission_key,
            entrypoint=point,
            source_kind=kind,
            source_identity_sha256=source_sha,
            slot_kind=slot_kind,
            decision="admit",
            reason="activation_bounded_slot_consumed",
            business_key=business,
            submission_key=submission,
            generation=generation_number,
            current=current,
        )
        consumed = conn.execute(
            """
            UPDATE rca_activation_budget_slots
               SET consumed_ledger_id = ?, consumed_at = ?
             WHERE epoch_id = ? AND slot_kind = ?
               AND consumed_ledger_id IS NULL
               AND authorized_source_kind = ?
               AND authorized_identity_sha256 = ?
            """,
            (ledger_id, current, epoch_id, slot_kind, kind, source_sha),
        )
        if consumed.rowcount != 1:
            raise ActivationEpochError("activation_slot_consume_lost")
        return ActivationAdmissionDecision(
            epoch_id=epoch_id,
            epoch_state=state,
            decision="admit",
            reason=(
                "activation_admission_idempotent"
                if prior == "admit"
                else "activation_bounded_slot_consumed"
            ),
            ledger_id=ledger_id,
            slot_kind=slot_kind,
            consumed_slot=prior != "admit",
        )

    @staticmethod
    def bind_activation_admission_tx(
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
        if decision.legacy_unconfigured:
            return
        if decision.decision not in {"admit", "shadow"}:
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
            or str(ledger["decision"]) != decision.decision
        ):
            raise ActivationEpochError("activation_binding_ledger_conflict")
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
                   o.lease_expires_at
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
            "activation_slot_kind": "TEXT",
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
        if RcaControlStore._table_exists(conn, "rca_activation_budget_slots"):
            slot_columns = {
                str(row["name"])
                for row in conn.execute(
                    "PRAGMA table_info(rca_activation_budget_slots)"
                ).fetchall()
            }
            for name in ("authorized_operator", "authorized_reason"):
                if name not in slot_columns:
                    conn.execute(
                        "ALTER TABLE rca_activation_budget_slots "
                        f"ADD COLUMN {name} TEXT"
                    )

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
        if marker_value in {
            "pnc_rca_control_store_v12",
            CONTROL_STORE_SCHEMA_VERSION,
        } or v12_tables_present:
            RcaControlStore._validate_v12_learning_lane_schema(conn)
        v13_tables_present = any(
            RcaControlStore._table_exists(conn, table)
            for table in (
                "rca_activation_historical_outbox_holds",
                "rca_activation_historical_outbox_hold_items",
                "rca_activation_historical_outbox_dispositions",
                "rca_activation_historical_outbox_disposition_items",
            )
        )
        if marker_value == CONTROL_STORE_SCHEMA_VERSION or v13_tables_present:
            RcaControlStore._validate_v13_historical_outbox_hold_schema(conn)

        def foreign_key_groups(table: str) -> dict[tuple[int, str], set[tuple[str, str]]]:
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
        slot_groups = foreign_key_groups("rca_activation_budget_slots")
        required_slot_foreign_keys = {
            ("rca_activation_epochs", "epoch_id", "epoch_id"),
            (
                "rca_activation_admission_ledger",
                "consumed_ledger_id",
                "ledger_id",
            ),
        }
        observed_slot_foreign_keys = {
            (parent, source, target)
            for (_identifier, parent), pairs in slot_groups.items()
            for source, target in pairs
        }
        if not required_slot_foreign_keys.issubset(observed_slot_foreign_keys):
            raise RuntimeError(
                "incompatible_control_store_schema:activation_slot_foreign_keys"
            )
        required_indexes = {
            "idx_business_triggers_issue_scope",
            "idx_rca_manual_operator_rate",
            "idx_rca_single_current_activation_epoch",
            "idx_rca_activation_slot_identity",
            "idx_rca_activation_ledger_submission",
            "idx_rca_activation_transition_epoch",
            "idx_rca_capacity_transition_audit_time",
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
        slot_identity_index = next(
            (
                row
                for row in conn.execute(
                    "PRAGMA index_list(rca_activation_budget_slots)"
                ).fetchall()
                if str(row["name"]) == "idx_rca_activation_slot_identity"
            ),
            None,
        )
        slot_identity_columns = [
            str(row["name"])
            for row in conn.execute(
                "PRAGMA index_info(idx_rca_activation_slot_identity)"
            ).fetchall()
        ]
        if (
            slot_identity_index is None
            or int(slot_identity_index["unique"]) != 1
            or int(slot_identity_index["partial"]) != 1
            or slot_identity_columns
            != [
                "epoch_id",
                "authorized_source_kind",
                "authorized_identity_sha256",
            ]
        ):
            raise RuntimeError(
                "incompatible_control_store_schema:activation_slot_identity_index"
            )
        required_activation_columns = {
            "kafka_inbox": {
                "activation_epoch_id",
                "activation_ingress_state",
                "activation_required",
                "activation_slot_kind",
                "activation_source_identity_sha256",
                "submit_enabled_requested",
            },
            "business_triggers": {"activation_epoch_id", "activation_ledger_id"},
            "rca_outbox": {"activation_epoch_id", "activation_ledger_id"},
            "rca_activation_epochs": {
                "preauthorization_fingerprint",
                "preauthorization_gate_receipt_sha256",
                "preauthorization_capsule_sha256",
                "preproduction_fingerprint",
                "preproduction_gate_receipt_sha256",
                "preproduction_capsule_sha256",
                "production_fingerprint",
                "production_gate_receipt_sha256",
            },
            "rca_activation_budget_slots": {
                "epoch_id",
                "slot_kind",
                "authorized_source_kind",
                "authorized_identity_sha256",
                "authorized_at",
                "authorized_operator",
                "authorized_reason",
                "consumed_ledger_id",
                "consumed_at",
            },
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
            "rca_activation_budget_slots",
            "rca_activation_admission_ledger",
            "rca_activation_transition_audit",
            "rca_capacity_transition_state",
            "rca_capacity_transition_audit",
        }
        present_tables = {
            str(row["name"])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        if not required_activation_tables.issubset(present_tables):
            raise RuntimeError("incompatible_control_store_schema:activation_tables")
        required_capacity_columns = {
            "singleton_id",
            "release_id",
            "bootstrap_epoch_id",
            "state",
            "generation",
            "final_ledger_sha256",
            "transition_authorization_sha256",
            "transition_authorization_fingerprint",
            "transition_receipt_sha256",
            "transition_receipt_fingerprint",
            "commit_marker_sha256",
            "commit_marker_fingerprint",
            "evidence_bundle_sha256",
            "evidence_bundle_fingerprint",
            "authorization_issued_at",
            "authorization_expires_at",
            "receipt_created_at",
            "marker_committed_at",
            "bootstrap_initialized_at",
            "steady_activated_at",
            "updated_at",
        }
        observed_capacity_columns = {
            str(row["name"])
            for row in conn.execute(
                "PRAGMA table_info(rca_capacity_transition_state)"
            ).fetchall()
        }
        if observed_capacity_columns != required_capacity_columns:
            raise RuntimeError(
                "incompatible_control_store_schema:capacity_transition_columns"
            )
        required_capacity_audit_columns = {
            "audit_id",
            "release_id",
            "bootstrap_epoch_id",
            "from_state",
            "to_state",
            "from_generation",
            "to_generation",
            "final_ledger_sha256",
            "transition_authorization_sha256",
            "transition_authorization_fingerprint",
            "transition_receipt_sha256",
            "transition_receipt_fingerprint",
            "commit_marker_sha256",
            "commit_marker_fingerprint",
            "evidence_bundle_sha256",
            "evidence_bundle_fingerprint",
            "authorization_issued_at",
            "authorization_expires_at",
            "receipt_created_at",
            "marker_committed_at",
            "transitioned_at",
        }
        observed_capacity_audit_columns = {
            str(row["name"])
            for row in conn.execute(
                "PRAGMA table_info(rca_capacity_transition_audit)"
            ).fetchall()
        }
        if observed_capacity_audit_columns != required_capacity_audit_columns:
            raise RuntimeError(
                "incompatible_control_store_schema:capacity_transition_audit_columns"
            )
        required_capacity_triggers = {
            "trg_rca_capacity_state_no_delete",
            "trg_rca_capacity_state_no_replace",
            "trg_rca_capacity_state_identity_immutable",
            "trg_rca_capacity_state_bootstrap_transition",
            "trg_rca_capacity_state_steady_immutable",
            "trg_rca_capacity_audit_no_update",
            "trg_rca_capacity_audit_no_delete",
            "trg_rca_capacity_audit_no_replace",
        }
        observed_capacity_triggers = {
            str(row["name"]): str(row["sql"] or "")
            for row in conn.execute(
                "SELECT name, sql FROM sqlite_master WHERE type = 'trigger'"
            ).fetchall()
        }
        if not required_capacity_triggers.issubset(observed_capacity_triggers):
            raise RuntimeError(
                "incompatible_control_store_schema:capacity_transition_triggers"
            )
        normalize_sql = lambda value: " ".join(str(value).split()).rstrip(";")
        expected_capacity_trigger_sql = {
            "trg_rca_capacity_state_no_delete": """
                CREATE TRIGGER trg_rca_capacity_state_no_delete
                BEFORE DELETE ON rca_capacity_transition_state
                BEGIN
                    SELECT RAISE(ABORT, 'rca_capacity_state_delete_forbidden');
                END
            """,
            "trg_rca_capacity_state_no_replace": """
                CREATE TRIGGER trg_rca_capacity_state_no_replace
                BEFORE INSERT ON rca_capacity_transition_state
                WHEN EXISTS (SELECT 1 FROM rca_capacity_transition_state)
                BEGIN
                    SELECT RAISE(ABORT, 'rca_capacity_state_replace_forbidden');
                END
            """,
            "trg_rca_capacity_state_identity_immutable": """
                CREATE TRIGGER trg_rca_capacity_state_identity_immutable
                BEFORE UPDATE ON rca_capacity_transition_state
                WHEN NEW.release_id != OLD.release_id
                  OR NEW.bootstrap_epoch_id != OLD.bootstrap_epoch_id
                  OR NEW.singleton_id != OLD.singleton_id
                BEGIN
                    SELECT RAISE(ABORT, 'rca_capacity_state_identity_immutable');
                END
            """,
            "trg_rca_capacity_state_bootstrap_transition": """
                CREATE TRIGGER trg_rca_capacity_state_bootstrap_transition
                BEFORE UPDATE ON rca_capacity_transition_state
                WHEN OLD.state = 'BOOTSTRAP_PRODUCTION'
                 AND NOT (
                    NEW.state = 'STEADY_ACTIVE'
                    AND NEW.generation = OLD.generation + 1
                 )
                BEGIN
                    SELECT RAISE(ABORT, 'rca_capacity_state_transition_invalid');
                END
            """,
            "trg_rca_capacity_state_steady_immutable": """
                CREATE TRIGGER trg_rca_capacity_state_steady_immutable
                BEFORE UPDATE ON rca_capacity_transition_state
                WHEN OLD.state = 'STEADY_ACTIVE'
                BEGIN
                    SELECT RAISE(ABORT, 'rca_capacity_state_steady_immutable');
                END
            """,
            "trg_rca_capacity_audit_no_update": """
                CREATE TRIGGER trg_rca_capacity_audit_no_update
                BEFORE UPDATE ON rca_capacity_transition_audit
                BEGIN
                    SELECT RAISE(ABORT, 'rca_capacity_audit_update_forbidden');
                END
            """,
            "trg_rca_capacity_audit_no_delete": """
                CREATE TRIGGER trg_rca_capacity_audit_no_delete
                BEFORE DELETE ON rca_capacity_transition_audit
                BEGIN
                    SELECT RAISE(ABORT, 'rca_capacity_audit_delete_forbidden');
                END
            """,
            "trg_rca_capacity_audit_no_replace": """
                CREATE TRIGGER trg_rca_capacity_audit_no_replace
                BEFORE INSERT ON rca_capacity_transition_audit
                WHEN EXISTS (
                    SELECT 1 FROM rca_capacity_transition_audit
                     WHERE audit_id = NEW.audit_id
                        OR (
                            release_id = NEW.release_id
                            AND bootstrap_epoch_id = NEW.bootstrap_epoch_id
                            AND to_generation = NEW.to_generation
                        )
                )
                BEGIN
                    SELECT RAISE(ABORT, 'rca_capacity_audit_replace_forbidden');
                END
            """,
        }
        if any(
            normalize_sql(observed_capacity_triggers[name])
            != normalize_sql(expected_capacity_trigger_sql[name])
            for name in required_capacity_triggers
        ):
            raise RuntimeError(
                "incompatible_control_store_schema:capacity_transition_trigger_sql"
            )
        capacity_integrity_error = (
            RcaControlStore._capacity_transition_integrity_error_tx(conn)
        )
        if capacity_integrity_error:
            raise RuntimeError(
                f"incompatible_control_store_schema:{capacity_integrity_error}"
            )
        dangling_slot = conn.execute(
            """
            SELECT 1 FROM rca_activation_budget_slots AS s
         LEFT JOIN rca_activation_admission_ledger AS al
                ON al.ledger_id = s.consumed_ledger_id
               AND al.epoch_id = s.epoch_id
             WHERE s.consumed_ledger_id IS NOT NULL AND al.ledger_id IS NULL
             LIMIT 1
            """
        ).fetchone()
        if dangling_slot is not None:
            raise RuntimeError(
                "incompatible_control_store_schema:activation_slot_binding"
            )
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
                    "shadow_promotion"
                    if core.execution_admission["reason"]
                    == "activation_confirmed_shadow_reconciliation"
                    else (
                        "manual_admit"
                        if activation_source_kind == "manual"
                        else "kafka_ingest"
                    )
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

    def persist_w3_admission_shadow_tx(
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
        """Project and persist one legacy admission through the W3 shadow oracle."""
        from gateway.pnc_rca_admission import validate_rca_trigger_context
        from gateway.pnc_rca_snapshot import (
            build_admission_snapshot,
            build_canonical_rca_request,
            build_snapshot_source_envelope,
            build_source_authority_receipt,
            compare_snapshot_shadow,
            compose_snapshot_projection,
            legacy_semantic_projection,
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
                execution_admission["decision"] == "shadow"
                and str(binding["outbox_status"]) == "pending"
            ):
                raise RecordConflictError("w3_snapshot_promotion_lineage_required")
        else:
            if binding_action == "join":
                raise RecordConflictError("w3_snapshot_creator_missing")
            if activation_decision is None or activation_decision.legacy_unconfigured:
                shadow_observation = str(binding["outbox_status"]) == "shadow"
                execution_admission = (
                    {
                        "activation_epoch_id": "",
                        "activation_ledger_id": None,
                        "decision": "shadow",
                        "reason": "activation_epoch_held_unconfigured",
                        "state": "unconfigured",
                        "legacy_unconfigured": False,
                    }
                    if shadow_observation
                    else {
                        "activation_epoch_id": "",
                        "activation_ledger_id": None,
                        "decision": "admit",
                        "reason": "activation_legacy_unconfigured",
                        "state": "legacy_unconfigured",
                        "legacy_unconfigured": True,
                    }
                )
            else:
                if activation_decision.decision not in {"admit", "shadow"}:
                    raise RecordConflictError("w3_snapshot_activation_decision_invalid")
                execution_admission = {
                    "activation_epoch_id": activation_decision.epoch_id,
                    "activation_ledger_id": activation_decision.ledger_id,
                    "decision": activation_decision.decision,
                    "reason": activation_decision.reason,
                    "state": activation_decision.epoch_state,
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

        legacy_snapshot_sha256 = snapshot.snapshot_sha256

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
            "requested_mode": (
                "pending"
                if snapshot.execution_admission["decision"] == "admit"
                else "shadow"
            ),
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
        legacy_projection = legacy_semantic_projection(
            admission=legacy_admission,
            trigger_context=context,
            creation_policy=policies["creation_policy"],
            business_profile=policies["business_profile"],
            execution_policy=policies["execution_policy"],
            publication_policy=policies["publication_policy"],
            correction_lineage_policy=policies["correction_lineage_policy"],
            execution_admission=execution_admission,
            source_id=str(source_id),
            source_metadata=source_metadata,
            anchor=anchor,
            ingress_decision=ingress_decision,
            expected_authorization_evidence_sha256=(
                authorization_evidence_sha256
            ),
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
            expected_source_payload_sha256=str(source["payload_sha256"]),
            expected_policy_sha256s=policy_sha256s,
            expected_source_authority=source_authority,
        )
        candidate_projection = compose_snapshot_projection(
            snapshot,
            envelope,
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
        comparison = compare_snapshot_shadow(
            legacy_projection,
            candidate_projection,
            expected_legacy_authorization_evidence_sha256=(
                authorization_evidence_sha256
            ),
            expected_candidate_authorization_evidence_sha256=(
                authorization_evidence_sha256
            ),
            expected_legacy_generation_authorization_evidence_sha256=(
                generation_evidence
            ),
            expected_candidate_generation_authorization_evidence_sha256=(
                generation_evidence
            ),
            expected_legacy_ticket_title_sha256=expected_title_sha256,
            expected_candidate_ticket_title_sha256=expected_title_sha256,
            expected_legacy_source_payload_sha256=str(source["payload_sha256"]),
            expected_candidate_source_payload_sha256=str(source["payload_sha256"]),
            expected_legacy_policy_sha256s=policy_sha256s,
            expected_candidate_policy_sha256s=policy_sha256s,
            expected_legacy_snapshot_sha256=legacy_snapshot_sha256,
            expected_candidate_snapshot_sha256=snapshot.snapshot_sha256,
            expected_legacy_source_authority=source_authority,
            expected_candidate_source_authority=source_authority,
        )
        if comparison["outcome"] != "match":
            raise RecordConflictError("w3_snapshot_shadow_mismatch")
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
            "shadow_comparison": comparison,
        }

    def persist_raw(
        self,
        record: KafkaRecord,
        *,
        policy: WorkflowEventPolicy,
        submit_enabled: bool = False,
        activation_required: bool = False,
        activation_slot_kind: str = "",
    ) -> RawPersistResult:
        """Durably persist raw bytes plus their immutable processing policy."""
        if not isinstance(activation_required, bool):
            raise ActivationEpochError("activation_adjudication_flag_invalid")
        slot_kind = str(activation_slot_kind or "").strip()
        if slot_kind and slot_kind not in ACTIVATION_SLOT_KINDS:
            raise ActivationEpochError("activation_slot_kind_invalid")
        source_sha, normalized_source = self._normalize_activation_source_identity(
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
            activation_epoch_id = str(epoch["epoch_id"]) if epoch is not None else None
            activation_ingress_state = (
                str(epoch["state"]) if epoch is not None else "legacy_unconfigured"
            )
            activation_enforced = activation_required or epoch is not None
            if activation_enforced and (
                epoch is None
                or not submit_enabled
                or activation_ingress_state
                in {"safe_off", "preauthorized", "aborted"}
            ):
                raise ActivationIngressDeferredError(
                    record.event_uid, "activation_ingress_unavailable"
                )
            if submit_enabled and activation_ingress_state == "confirmed":
                start_fence = json.loads(str(epoch["partition_start_fence_json"]))
                end_fence = json.loads(str(epoch["partition_end_fence_json"] or "{}"))
                topic = str(normalized_source["topic"])
                partition = str(normalized_source["partition"])
                offset = int(normalized_source["offset"])
                if (
                    topic not in start_fence
                    or partition not in start_fence[topic]
                    or topic not in end_fence
                    or partition not in end_fence[topic]
                    or offset < int(start_fence[topic][partition])
                    or offset >= int(end_fence[topic][partition])
                ):
                    raise ActivationIngressDeferredError(
                        record.event_uid,
                        "activation_confirmed_ingress_deferred",
                    )
            submission_mode = "pending" if submit_enabled else "shadow"
            if submit_enabled and epoch is not None:
                if str(epoch["state"]) == "steady_active":
                    submission_mode = "pending"
                elif str(epoch["state"]) == "bounded_active" and slot_kind:
                    slot = conn.execute(
                        """
                        SELECT authorized_source_kind, authorized_identity_sha256,
                               consumed_ledger_id
                          FROM rca_activation_budget_slots
                         WHERE epoch_id = ? AND slot_kind = ?
                        """,
                        (activation_epoch_id, slot_kind),
                    ).fetchone()
                    start_fence = json.loads(
                        str(epoch["partition_start_fence_json"])
                    )
                    topic = str(normalized_source["topic"])
                    partition = str(normalized_source["partition"])
                    offset = int(normalized_source["offset"])
                    exact_slot = (
                        slot is not None
                        and str(slot["authorized_source_kind"] or "") == "kafka"
                        and str(slot["authorized_identity_sha256"] or "") == source_sha
                        and slot["consumed_ledger_id"] is None
                    )
                    in_fence = (
                        topic in start_fence
                        and partition in start_fence[topic]
                        and offset >= int(start_fence[topic][partition])
                    )
                    submission_mode = "pending" if exact_slot and in_fence else "shadow"
                else:
                    submission_mode = "shadow"
            elif submit_enabled and activation_required:
                submission_mode = "shadow"
            self._register_policy_snapshot_tx(conn, policy, current)
            existing = conn.execute(
                """
                SELECT raw_sha256, policy_json, creation_rule_version,
                       submission_mode, submit_enabled_requested,
                       activation_epoch_id, activation_ingress_state,
                       activation_required, activation_slot_kind,
                       activation_source_identity_sha256
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
                    str(existing["activation_slot_kind"] or ""),
                    str(existing["activation_source_identity_sha256"] or ""),
                )
                requested_intent = (
                    policy_json,
                    policy.policy_version,
                    int(activation_required),
                    slot_kind,
                    source_sha,
                )
                if immutable_intent != requested_intent:
                    raise RecordConflictError(
                        f"Kafka coordinate {record.event_uid} changed ingress intent"
                    )
                submit_intent_changed = int(
                    existing["submit_enabled_requested"]
                ) != int(bool(submit_enabled))
                legacy_lineage = (
                    existing["activation_epoch_id"] is None
                    and str(existing["activation_ingress_state"])
                    == "legacy_unconfigured"
                    and int(existing["activation_required"]) == 0
                )
                if submit_intent_changed and not legacy_lineage:
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
                    activation_required, activation_slot_kind,
                    activation_source_identity_sha256
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    int(activation_required),
                    slot_kind or None,
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

    @staticmethod
    def _assert_manual_dispatch_capacity_tx(
        conn: sqlite3.Connection,
        *,
        outbox_high_watermark: int,
    ) -> None:
        backlog = int(
            conn.execute(
                """
                SELECT COUNT(*) FROM rca_outbox
                 WHERE status IN ('pending', 'claimed')
                """
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
                """
                SELECT COUNT(*)
                  FROM rca_outbox AS o
                 WHERE o.status IN ('pending', 'claimed')
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
                """
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
    def _manual_shadow_promote_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        row: sqlite3.Row,
        request: ManualRcaTriggerRequest,
        current: str,
    ) -> bool:
        """Promote one unsubmitted Kafka shadow without changing its lineage."""
        if str(row["outbox_status"] or "") != "shadow":
            return False
        if str(row["state"] or "") != "shadow":
            raise ManualRcaAdmissionError("manual_shadow_state_inconsistent")
        if conn.execute(
            """
            SELECT 1 FROM rca_admission_snapshots
             WHERE business_key = ? AND generation = ?
            """,
            (row["business_key"], row["generation"]),
        ).fetchone() is not None:
            raise ManualRcaAdmissionError(
                "w3_snapshot_promotion_lineage_required"
            )
        event_uid = str(row["source_event_id"] or "").strip()
        if not event_uid:
            raise ManualRcaAdmissionError("manual_shadow_source_missing")
        if (
            int(row["attempt"] or 0) != 0
            or row["completed_at"] is not None
            or row["result_json"] is not None
            or row["quarantined_at"] is not None
            or str(row["last_error_code"] or "")
            or any(
                row[name] is not None
                for name in ("lease_token", "lease_owner", "lease_expires_at")
            )
        ):
            raise ManualRcaAdmissionError("manual_shadow_already_submitted")
        inbox = conn.execute(
            """
            SELECT decision, submission_mode, business_key, submission_key, generation
              FROM kafka_inbox WHERE event_uid = ?
            """,
            (event_uid,),
        ).fetchone()
        if (
            inbox is None
            or str(inbox["decision"] or "") != "accepted"
            or str(inbox["submission_mode"] or "") != "shadow"
            or str(inbox["business_key"] or "") != str(row["business_key"])
            or str(inbox["submission_key"] or "") != str(row["submission_key"])
            or int(inbox["generation"] or 0) != int(row["generation"])
        ):
            raise ManualRcaAdmissionError("manual_shadow_inbox_inconsistent")

        inbox_update = conn.execute(
            """
            UPDATE kafka_inbox SET submission_mode = 'pending'
             WHERE event_uid = ? AND decision = 'accepted'
               AND submission_mode = 'shadow' AND business_key = ?
               AND submission_key = ? AND generation = ?
            """,
            (
                event_uid,
                row["business_key"],
                row["submission_key"],
                row["generation"],
            ),
        )
        outbox_update = conn.execute(
            """
            UPDATE rca_outbox
               SET status = 'pending', next_attempt_at = ?, updated_at = ?
             WHERE outbox_id = ? AND status = 'shadow' AND attempt = 0
               AND completed_at IS NULL AND result_json IS NULL
               AND quarantined_at IS NULL AND last_error_code = ''
               AND lease_token IS NULL AND lease_owner IS NULL
               AND lease_expires_at IS NULL
            """,
            (current, current, row["outbox_id"]),
        )
        trigger_update = conn.execute(
            """
            UPDATE business_triggers SET state = 'pending'
             WHERE business_key = ? AND generation = ? AND state = 'shadow'
               AND submission_key = ? AND source_event_id = ?
            """,
            (
                row["business_key"],
                row["generation"],
                row["submission_key"],
                event_uid,
            ),
        )
        if (
            inbox_update.rowcount != 1
            or outbox_update.rowcount != 1
            or trigger_update.rowcount != 1
        ):
            raise RuntimeError("manual_shadow_promotion_lost_atomic_guard")
        cls._insert_promotion_audit(
            conn,
            event_uid=event_uid,
            outbox_id=int(row["outbox_id"]),
            submission_key=str(row["submission_key"]),
            operator=f"manual:{request.requester_id}",
            reason=f"manual_{request.mode}",
            outcome=MANUAL_SHADOW_PROMOTED_REASON,
            from_status="shadow",
            to_status="pending",
            detail="authorized manual trigger promoted exact unsubmitted shadow",
            created_at=current,
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
        cls, conn: sqlite3.Connection, row: sqlite3.Row
    ) -> bool:
        if cls._table_exists(conn, "rca_execution_watch"):
            watch = conn.execute(
                "SELECT state FROM rca_execution_watch WHERE submission_key = ?",
                (row["submission_key"],),
            ).fetchone()
            if watch is not None:
                watch_state = str(watch["state"] or "")
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
        activation_slot_kind: str = "",
        automation_authority: Mapping[str, Any] | None = None,
        user_rerun_authority: Mapping[str, Any] | None = None,
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
        activation_slot = str(activation_slot_kind or "").strip()
        if activation_slot and activation_slot not in ACTIVATION_SLOT_KINDS:
            raise ManualRcaAdmissionError("manual_activation_slot_invalid")
        gray_sample_authority: dict[str, str] | None = None
        normalized_user_rerun: dict[str, str] | None = None
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
        if automation_authority is not None:
            try:
                gray_sample_authority = normalize_gray_sample_automation_authority(
                    automation_authority
                )
            except ValueError as exc:
                raise ManualRcaAdmissionError(str(exc)) from exc
            if (
                manual.platform != "operator"
                or manual.mode != "rerun"
                or manual.requester_id != GRAY_SAMPLE_REQUESTER_ID
                or activation_required is not True
                or activation_slot
                or operator_authorized is not True
                or snapshot_authority is not None
                or snapshot_ticket_authority is not None
                or snapshot_manual_ingress_authority is not None
            ):
                raise ManualRcaAdmissionError(
                    "gray_sample_automation_contract_invalid"
                )
            sample_id = gray_sample_authority["sample_id"]
            if (
                manual.issue_url != gray_sample_issue_url(sample_id)
                or manual.reason != build_gray_sample_reason(gray_sample_authority)
                or manual.message_id
                != build_gray_sample_message_id(gray_sample_authority)
            ):
                raise ManualRcaAdmissionError(
                    "gray_sample_automation_binding_mismatch"
                )
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
        if gray_sample_authority is not None:
            source_payload["automation_authority"] = gray_sample_authority
        if normalized_user_rerun is not None:
            source_payload["user_rerun_authority"] = normalized_user_rerun
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
        if gray_sample_authority is not None:
            activation_source_identity["automation_authority"] = (
                gray_sample_authority
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
                replay_activation = self.adjudicate_activation_tx(
                    conn,
                    entrypoint="manual_admit",
                    source_kind="manual",
                    source_identity=activation_source_identity,
                    business_key=str(binding["business_key"]),
                    submission_key=str(binding["submission_key"]),
                    generation=int(binding["generation"]),
                    new_execution=False,
                    requested_slot_kind=activation_slot,
                    activation_required=activation_required,
                    now=now,
                )
                if replay_activation.decision not in {"admit", "join"}:
                    raise ManualRcaAdmissionError(replay_activation.reason)
                w3_shadow_observation = bool(
                    w3_authority is not None
                    and manual.mode == "run_or_join"
                    and str(binding["outbox_status"] or "") == "shadow"
                )
                if (
                    str(binding["outbox_status"] or "") == "shadow"
                    and not w3_shadow_observation
                ):
                    self._assert_manual_storage_capacity()
                    self._assert_manual_dispatch_capacity_tx(
                        conn,
                        outbox_high_watermark=high_watermark,
                    )
                shadow_promoted = (
                    False
                    if w3_shadow_observation
                    else self._manual_shadow_promote_tx(
                        conn,
                        row=binding,
                        request=manual,
                        current=current,
                    )
                )
                replay_outcome = str(existing_source["outcome"] or "joined")
                replay_state = str(binding["state"])
                replay_reason = "idempotent_source_replay"
                if shadow_promoted:
                    replay_outcome = "rearmed"
                    replay_state = "pending"
                    replay_reason = MANUAL_SHADOW_PROMOTED_REASON
                    conn.execute(
                        "UPDATE rca_trigger_sources SET outcome = ? WHERE source_id = ?",
                        (replay_outcome, source_id),
                    )
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
                learning_lane = (
                    False
                    if normalized_user_rerun is not None
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
                    self.persist_w3_admission_shadow_tx(
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

            if gray_sample_authority is not None:
                day_start = _utc_datetime(now).replace(
                    hour=0, minute=0, second=0, microsecond=0
                )
                day_end = day_start + timedelta(days=1)
                started_today = int(
                    conn.execute(
                        """
                        SELECT COUNT(*) FROM rca_trigger_sources
                         WHERE source_kind = 'feishu_group_manual'
                           AND platform = 'operator'
                           AND requester_id = ? AND mode = 'rerun'
                           AND created_at >= ? AND created_at < ?
                        """,
                        (
                            GRAY_SAMPLE_REQUESTER_ID,
                            _iso(day_start),
                            _iso(day_end),
                        ),
                    ).fetchone()[0]
                )
                if started_today >= GRAY_SAMPLE_DAILY_LIMIT:
                    raise ManualRcaAdmissionError(
                        "gray_sample_daily_rate_limited"
                    )
            elif (
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
            if gray_sample_authority is not None and (
                latest is None or not self._execution_terminal_tx(conn, latest)
            ):
                raise ManualRcaAdmissionError(
                    "gray_sample_terminal_generation_required"
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
            needs_shadow_promotion = False
            needs_input_rearm = False
            if latest is None:
                self._assert_manual_dispatch_capacity_tx(
                    conn,
                    outbox_high_watermark=high_watermark,
                )
                admission = base_admission
                outcome = "created"
            elif (
                str(latest["outbox_status"] or "") == "shadow"
                and w3_authority is not None
                and manual.mode == "run_or_join"
            ):
                admission = existing_admission(
                    generation=int(latest["generation"])
                )
                outcome = "joined"
            elif str(latest["outbox_status"] or "") == "shadow":
                self._assert_manual_dispatch_capacity_tx(
                    conn,
                    outbox_high_watermark=high_watermark,
                )
                admission = existing_admission(
                    generation=int(latest["generation"])
                )
                outcome = "rearmed"
                rearmed = True
                rearm_reason = MANUAL_SHADOW_PROMOTED_REASON
                needs_shadow_promotion = True
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
                conn, latest
            ):
                if (
                    manual.platform != "feishu"
                    or manual.mode != "rerun"
                ) and gray_sample_authority is None:
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
                requested_slot_kind=activation_slot,
                activation_required=activation_required,
                now=now,
            )
            if activation_decision.decision not in {"admit", "join"}:
                raise ManualRcaAdmissionError(activation_decision.reason)
            if needs_shadow_promotion:
                if latest is None or not self._manual_shadow_promote_tx(
                    conn,
                    row=latest,
                    request=manual,
                    current=current,
                ):
                    raise RuntimeError("manual_shadow_promotion_not_applied")
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
                    and not activation_decision.legacy_unconfigured
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
            learning_lane = (
                False
                if normalized_user_rerun is not None
                else self._ensure_learning_lane_admission_tx(
                    conn, admission=admission, current=current
                )
            )
            if w3_authority is not None:
                self.persist_w3_admission_shadow_tx(
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
                                requested_slot_kind=str(
                                    row["activation_slot_kind"] or ""
                                ),
                                activation_required=bool(
                                    row["activation_required"]
                                ),
                                ingress_epoch_id=captured_epoch,
                                ingress_state=str(
                                    row["activation_ingress_state"] or ""
                                ),
                                now=now_dt,
                            )
                            if activation_decision.decision == "reject":
                                decision = "filtered"
                                reason = activation_decision.reason
                                admission = None
                                business_key = ""
                                generation = 0
                                submission_key = ""
                                kafka_source_id = ""
                                creates_generation = False
                            elif activation_decision.decision == "shadow":
                                effective_submission_mode = "shadow"
                                reason = activation_decision.reason
                            elif activation_decision.decision == "admit":
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
                            and activation_decision.decision in {"admit", "shadow"}
                            and not activation_decision.legacy_unconfigured
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
                        if snapshot_authority is not None:
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
                            self.persist_w3_admission_shadow_tx(
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
        activation_slot_kind: str = "",
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
            activation_slot_kind=activation_slot_kind,
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
            row = self._current_activation_epoch_tx(conn)
            if row is None or str(row["state"]) not in {
                "bounded_active",
                "confirmed",
                "steady_active",
            }:
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
            return result
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

    def promote_shadow_event(
        self,
        event_uid: str,
        *,
        operator: str,
        reason: str,
        expected_activation_epoch_id: str = "",
        activation_required: bool = False,
        activation_slot_kind: str = "",
        now: datetime | None = None,
    ) -> ShadowPromotionResult:
        """Promote exactly one accepted shadow event for an audited canary.

        This API deliberately accepts one exact transport identity and has no
        list, prefix, policy, or wildcard mode. Repeating a successful promotion
        is idempotent and creates a second audit entry without mutating state.
        """
        event_id = str(event_uid or "").strip()
        actor = str(operator or "").strip()
        justification = str(reason or "").strip()
        if not event_id or "\n" in event_id or "\r" in event_id:
            raise ValueError("event_uid must be one exact non-empty identity")
        if not actor:
            raise ValueError("operator must not be empty")
        if not justification:
            raise ValueError("reason must not be empty")
        if len(event_id) > 500 or len(actor) > 200 or len(justification) > 1000:
            raise ValueError("promotion audit fields exceed their size limits")
        if not isinstance(activation_required, bool):
            raise ActivationEpochError("activation_adjudication_flag_invalid")
        expected_epoch_id = str(expected_activation_epoch_id or "").strip()
        if expected_epoch_id:
            expected_epoch_id = self._normalize_activation_epoch_id(expected_epoch_id)
        activation_slot = str(activation_slot_kind or "").strip()
        if activation_slot and activation_slot not in ACTIVATION_SLOT_KINDS:
            raise ActivationEpochError("activation_slot_kind_invalid")
        current = _iso(now)
        conn = self._connect()
        denied: tuple[str, int, str, str] | None = None
        result: ShadowPromotionResult | None = None
        try:
            conn.execute("BEGIN IMMEDIATE")
            epoch = self._current_activation_epoch_tx(conn)
            if expected_epoch_id and (
                epoch is None or str(epoch["epoch_id"]) != expected_epoch_id
            ):
                raise ActivationEpochError("activation_epoch_not_current")
            row = conn.execute(
                """
                SELECT i.event_uid, i.decision, i.submission_mode,
                       o.outbox_id, o.submission_key, o.status AS outbox_status,
                       o.business_key, o.generation, o.activation_epoch_id,
                       o.activation_ledger_id,
                       EXISTS (
                           SELECT 1 FROM rca_admission_snapshots AS snapshot
                            WHERE snapshot.business_key = o.business_key
                              AND snapshot.generation = o.generation
                       ) AS has_w3_snapshot,
                       t.state AS trigger_state
                  FROM kafka_inbox AS i
             LEFT JOIN rca_outbox AS o ON o.source_event_id = i.event_uid
             LEFT JOIN business_triggers AS t
                    ON t.business_key = o.business_key
                   AND t.generation = o.generation
                 WHERE i.event_uid = ?
                """,
                (event_id,),
            ).fetchone()
            if row is None:
                denied = ("event_not_found", 0, "", "")
            elif row["decision"] != "accepted":
                denied = (
                    "event_not_accepted",
                    int(row["outbox_id"] or 0),
                    str(row["submission_key"] or ""),
                    str(row["outbox_status"] or row["submission_mode"] or ""),
                )
            elif row["outbox_id"] is None:
                denied = ("accepted_event_missing_outbox", 0, "", "")
            elif row["submission_mode"] == "shadow":
                if row["outbox_status"] != "shadow" or row["trigger_state"] != "shadow":
                    denied = (
                        "shadow_state_inconsistent_or_completed",
                        int(row["outbox_id"]),
                        str(row["submission_key"]),
                        str(row["outbox_status"] or ""),
                    )
                elif int(row["has_w3_snapshot"] or 0) == 1:
                    denied = (
                        "w3_snapshot_promotion_lineage_required",
                        int(row["outbox_id"]),
                        str(row["submission_key"]),
                        str(row["outbox_status"] or ""),
                    )
                else:
                    if epoch is not None and (
                        str(row["activation_epoch_id"] or "")
                        != str(epoch["epoch_id"])
                        or row["activation_ledger_id"] is None
                    ):
                        denied = (
                            "activation_shadow_epoch_mismatch",
                            int(row["outbox_id"]),
                            str(row["submission_key"]),
                            str(row["outbox_status"] or ""),
                        )
                    else:
                        activation_decision = self.adjudicate_activation_tx(
                            conn,
                            entrypoint="shadow_promotion",
                            source_kind="kafka",
                            source_identity={"event_uid": event_id},
                            business_key=str(row["business_key"]),
                            submission_key=str(row["submission_key"]),
                            generation=int(row["generation"]),
                            new_execution=True,
                            requested_slot_kind=activation_slot,
                            activation_required=activation_required,
                            now=now,
                        )
                        if activation_decision.decision != "admit":
                            denied = (
                                activation_decision.reason,
                                int(row["outbox_id"]),
                                str(row["submission_key"]),
                                str(row["outbox_status"] or ""),
                            )
                        elif epoch is not None and (
                            activation_decision.ledger_id
                            != int(row["activation_ledger_id"])
                            or activation_decision.epoch_id != str(epoch["epoch_id"])
                        ):
                            raise ActivationEpochError(
                                "activation_shadow_ledger_mismatch"
                            )
                    if denied is not None:
                        pass
                    else:
                        inbox_update = conn.execute(
                            """
                            UPDATE kafka_inbox SET submission_mode = 'pending'
                             WHERE event_uid = ? AND decision = 'accepted'
                               AND submission_mode = 'shadow'
                            """,
                            (event_id,),
                        )
                        outbox_update = conn.execute(
                            """
                            UPDATE rca_outbox SET status = 'pending', updated_at = ?
                             WHERE outbox_id = ? AND status = 'shadow'
                            """,
                            (current, row["outbox_id"]),
                        )
                        trigger_update = conn.execute(
                            """
                            UPDATE business_triggers SET state = 'pending'
                             WHERE business_key = ? AND generation = ?
                               AND state = 'shadow'
                            """,
                            (row["business_key"], row["generation"]),
                        )
                        if (
                            inbox_update.rowcount != 1
                            or outbox_update.rowcount != 1
                            or trigger_update.rowcount != 1
                        ):
                            raise RuntimeError(
                                "shadow promotion lost its atomic state guard"
                            )
                        audit_id = self._insert_promotion_audit(
                            conn,
                            event_uid=event_id,
                            outbox_id=int(row["outbox_id"]),
                            submission_key=str(row["submission_key"]),
                            operator=actor,
                            reason=justification,
                            outcome="promoted",
                            from_status="shadow",
                            to_status="pending",
                            detail="single accepted shadow event promoted for canary",
                            created_at=current,
                        )
                        result = ShadowPromotionResult(
                            event_uid=event_id,
                            outbox_id=int(row["outbox_id"]),
                            submission_key=str(row["submission_key"]),
                            status="pending",
                            promoted=True,
                            audit_id=audit_id,
                        )
            else:
                prior = conn.execute(
                    """
                    SELECT audit_id FROM rca_shadow_promotion_audit
                     WHERE event_uid = ? AND outcome = 'promoted'
                     ORDER BY audit_id LIMIT 1
                    """,
                    (event_id,),
                ).fetchone()
                if prior is None:
                    denied = (
                        "event_was_not_promoted_from_shadow",
                        int(row["outbox_id"]),
                        str(row["submission_key"]),
                        str(row["outbox_status"] or ""),
                    )
                else:
                    if epoch is not None:
                        eligible = conn.execute(
                            """
                            SELECT al.bound_at, al.reason,
                                   EXISTS (
                                       SELECT 1
                                         FROM rca_activation_budget_slots AS abs
                                        WHERE abs.epoch_id = al.epoch_id
                                          AND abs.consumed_ledger_id = al.ledger_id
                                          AND abs.slot_kind = 'kafka_success'
                                   ) AS is_kafka_canary
                              FROM rca_activation_admission_ledger AS al
                             WHERE al.ledger_id = ? AND al.epoch_id = ?
                               AND al.decision = 'admit'
                               AND al.business_key = ?
                               AND al.submission_key = ?
                               AND al.generation = ?
                            """,
                            (
                                row["activation_ledger_id"],
                                epoch["epoch_id"],
                                row["business_key"],
                                row["submission_key"],
                                row["generation"],
                            ),
                        ).fetchone()
                        epoch_state = str(epoch["state"])
                        replay_allowed = bool(
                            eligible is not None
                            and str(eligible["bound_at"] or "")
                            and (
                                epoch_state == "steady_active"
                                or (
                                    epoch_state == "bounded_active"
                                    and int(eligible["is_kafka_canary"] or 0) == 1
                                )
                                or (
                                    epoch_state == "confirmed"
                                    and (
                                        int(eligible["is_kafka_canary"] or 0) == 1
                                        or str(eligible["reason"] or "")
                                        == "activation_confirmed_shadow_reconciliation"
                                    )
                                )
                            )
                        )
                        if (
                            str(row["activation_epoch_id"] or "")
                            != str(epoch["epoch_id"])
                            or not replay_allowed
                        ):
                            denied = (
                                "activation_promotion_replay_not_eligible",
                                int(row["outbox_id"]),
                                str(row["submission_key"]),
                                str(row["outbox_status"] or ""),
                            )
                    elif activation_required:
                        denied = (
                            "activation_epoch_rejected_unconfigured",
                            int(row["outbox_id"]),
                            str(row["submission_key"]),
                            str(row["outbox_status"] or ""),
                        )
                    if denied is not None:
                        pass
                    else:
                        audit_id = self._insert_promotion_audit(
                            conn,
                            event_uid=event_id,
                            outbox_id=int(row["outbox_id"]),
                            submission_key=str(row["submission_key"]),
                            operator=actor,
                            reason=justification,
                            outcome="already_promoted",
                            from_status=str(row["outbox_status"] or ""),
                            to_status=str(row["outbox_status"] or ""),
                            detail="idempotent repeat of an audited promotion",
                            created_at=current,
                        )
                        result = ShadowPromotionResult(
                            event_uid=event_id,
                            outbox_id=int(row["outbox_id"]),
                            submission_key=str(row["submission_key"]),
                            status=str(row["outbox_status"] or ""),
                            promoted=False,
                            audit_id=audit_id,
                        )

            if denied is not None:
                code, outbox_id, submission_key, from_status = denied
                self._insert_promotion_audit(
                    conn,
                    event_uid=event_id,
                    outbox_id=outbox_id or None,
                    submission_key=submission_key,
                    operator=actor,
                    reason=justification,
                    outcome="denied",
                    from_status=from_status,
                    to_status=from_status,
                    detail=code,
                    created_at=current,
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        if denied is not None:
            raise ShadowPromotionError(
                f"shadow promotion denied for {event_id}: {denied[0]}"
            )
        if result is None:
            raise RuntimeError("shadow promotion produced no result")
        return result

    def defer_activation_event(
        self,
        event_uid: str,
        *,
        expected_activation_epoch_id: str,
        operator: str,
        reason: str,
        now: datetime | None = None,
    ) -> ActivationDeferralResult:
        """Quarantine one exact unexecuted Kafka item with an immutable audit trail."""
        event_id = str(event_uid or "").strip()
        if not event_id or "\n" in event_id or "\r" in event_id or len(event_id) > 500:
            raise ActivationEpochError("activation_deferred_event_uid_invalid")
        epoch_id = self._normalize_activation_epoch_id(expected_activation_epoch_id)
        actor, justification = self._normalize_activation_audit_text(operator, reason)
        current = _iso(now)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            epoch = self._current_activation_epoch_tx(conn)
            if epoch is None or str(epoch["epoch_id"]) != epoch_id:
                raise ActivationEpochError("activation_epoch_not_current")
            if str(epoch["state"]) == "steady_active":
                raise ActivationEpochError("activation_deferral_closed")
            row = conn.execute(
                """
                SELECT i.decision AS inbox_decision,
                       o.outbox_id, o.business_key, o.submission_key, o.generation,
                       o.status AS outbox_status, o.last_error_code,
                       o.activation_epoch_id, o.activation_ledger_id,
                       t.state AS trigger_state,
                       al.decision AS ledger_decision, al.bound_at
                  FROM kafka_inbox AS i
                  JOIN rca_outbox AS o ON o.source_event_id = i.event_uid
                  JOIN business_triggers AS t
                    ON t.business_key = o.business_key
                   AND t.submission_key = o.submission_key
                   AND t.generation = o.generation
                  JOIN rca_activation_admission_ledger AS al
                    ON al.epoch_id = o.activation_epoch_id
                   AND al.ledger_id = o.activation_ledger_id
                   AND al.business_key = o.business_key
                   AND al.submission_key = o.submission_key
                   AND al.generation = o.generation
                 WHERE i.event_uid = ?
                """,
                (event_id,),
            ).fetchone()
            if row is None:
                raise ActivationEpochError("activation_deferred_event_not_bound")
            if (
                str(row["inbox_decision"]) != "accepted"
                or str(row["activation_epoch_id"] or "") != epoch_id
                or row["activation_ledger_id"] is None
                or not str(row["bound_at"] or "")
            ):
                raise ActivationEpochError("activation_deferred_binding_invalid")
            prior_status = str(row["outbox_status"] or "")
            ledger_decision = str(row["ledger_decision"] or "")
            already_deferred = (
                prior_status == "quarantined"
                and str(row["last_error_code"] or "")
                == "activation_epoch_deferred"
                and str(row["trigger_state"] or "") == "quarantined"
            )
            if not already_deferred and (
                prior_status not in {"shadow", "pending"}
                or (prior_status == "shadow" and ledger_decision != "shadow")
                or (prior_status == "pending" and ledger_decision != "admit")
                or str(row["trigger_state"] or "") != prior_status
            ):
                raise ActivationEpochError("activation_deferred_state_invalid")
            invalid_subscription = conn.execute(
                """
                SELECT 1 FROM rca_delivery_subscriptions
                 WHERE business_key = ? AND generation = ?
                   AND status NOT IN ('pending', 'quarantined')
                 LIMIT 1
                """,
                (row["business_key"], row["generation"]),
            ).fetchone()
            if invalid_subscription is not None:
                raise ActivationEpochError(
                    "activation_deferred_delivery_already_materialized"
                )
            if not already_deferred:
                outbox_update = conn.execute(
                    """
                    UPDATE rca_outbox
                       SET status = 'quarantined', quarantined_at = ?,
                           next_attempt_at = NULL, lease_token = NULL,
                           lease_owner = NULL, lease_expires_at = NULL,
                           last_error_code = 'activation_epoch_deferred',
                           last_error_detail = 'exact operator-reviewed activation deferral',
                           updated_at = ?
                     WHERE outbox_id = ? AND status IN ('shadow', 'pending')
                    """,
                    (current, current, row["outbox_id"]),
                )
                trigger_update = conn.execute(
                    """
                    UPDATE business_triggers SET state = 'quarantined'
                     WHERE business_key = ? AND generation = ? AND state = ?
                    """,
                    (
                        row["business_key"],
                        row["generation"],
                        prior_status,
                    ),
                )
                if outbox_update.rowcount != 1 or trigger_update.rowcount != 1:
                    raise ActivationEpochError("activation_deferred_state_changed")
                conn.execute(
                    """
                    UPDATE rca_delivery_subscriptions
                       SET status = 'quarantined',
                           reason = 'activation_epoch_deferred', updated_at = ?
                     WHERE business_key = ? AND generation = ? AND status = 'pending'
                    """,
                    (current, row["business_key"], row["generation"]),
                )
            audit_id = self._insert_promotion_audit(
                conn,
                event_uid=event_id,
                outbox_id=int(row["outbox_id"]),
                submission_key=str(row["submission_key"]),
                operator=actor,
                reason=justification,
                outcome="already_deferred" if already_deferred else "deferred",
                from_status=prior_status,
                to_status="quarantined",
                detail="exact activation item deferred for reviewed manual recovery",
                created_at=current,
            )
            conn.commit()
            return ActivationDeferralResult(
                event_uid=event_id,
                epoch_id=epoch_id,
                outbox_id=int(row["outbox_id"]),
                submission_key=str(row["submission_key"]),
                prior_status=prior_status,
                status="quarantined",
                audit_id=audit_id,
            )
        except Exception:
            if conn.in_transaction:
                conn.rollback()
            raise
        finally:
            conn.close()

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
        activation_required: bool,
        historical_submission_allowlist: Iterable[str],
        alias: str = "o",
    ) -> tuple[str, tuple[Any, ...]]:
        if not isinstance(activation_required, bool):
            raise ActivationEpochError("activation_adjudication_flag_invalid")
        allowlist = tuple(
            dict.fromkeys(
                str(item or "").strip() for item in historical_submission_allowlist
            )
        )
        if any(not item or len(item) > 500 for item in allowlist) or len(allowlist) > 100:
            raise ActivationEpochError("activation_historical_allowlist_invalid")
        epoch = cls._current_activation_epoch_tx(conn)
        if epoch is None:
            return ("0", ()) if activation_required else ("1", ())
        epoch_id = str(epoch["epoch_id"])
        state = str(epoch["state"])
        if state not in {"bounded_active", "steady_active"}:
            return "0", ()
        ledger_match = f"""
            {alias}.activation_epoch_id = ?
            AND {alias}.activation_ledger_id IS NOT NULL
            AND EXISTS (
                SELECT 1 FROM rca_activation_admission_ledger AS al
                 WHERE al.ledger_id = {alias}.activation_ledger_id
                   AND al.epoch_id = {alias}.activation_epoch_id
                   AND al.decision = 'admit'
                   AND al.business_key = {alias}.business_key
                   AND al.submission_key = {alias}.submission_key
                   AND al.generation = {alias}.generation
            )
        """
        parameters: list[Any] = [epoch_id]
        if state == "bounded_active":
            ledger_match += f"""
                AND EXISTS (
                    SELECT 1 FROM rca_activation_budget_slots AS abs
                     WHERE abs.epoch_id = {alias}.activation_epoch_id
                       AND abs.consumed_ledger_id = {alias}.activation_ledger_id
                )
            """
        elif allowlist:
            placeholders = ",".join("?" for _ in allowlist)
            ledger_match = (
                f"(({ledger_match}) OR ("
                f"{alias}.activation_epoch_id IS NULL "
                f"AND {alias}.activation_ledger_id IS NULL "
                f"AND {alias}.submission_key IN ({placeholders}) "
                "AND NOT EXISTS ("
                "SELECT 1 "
                "FROM rca_activation_historical_outbox_hold_items AS held "
                f"WHERE held.outbox_id = {alias}.outbox_id)))"
            )
            parameters.extend(allowlist)
        return f"({ledger_match})", tuple(parameters)

    def claim_outbox(
        self,
        *,
        lease_owner: str,
        lease_seconds: int = 180,
        max_age_seconds: int = 86_400,
        activation_required: bool = False,
        historical_submission_allowlist: Iterable[str] = (),
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
        current = _utc_datetime(now)
        now_iso = _iso(current)
        expires_at = _iso(current + timedelta(seconds=lease_seconds))
        cutoff = _iso(current - timedelta(seconds=max_age_seconds))
        token = uuid.uuid4().hex

        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            activation_predicate, activation_parameters = (
                self._activation_claim_predicate_tx(
                    conn,
                    activation_required=activation_required,
                    historical_submission_allowlist=historical_submission_allowlist,
                )
            )
            expired_rows = conn.execute(
                f"""
                SELECT o.outbox_id, o.business_key, o.generation
                  FROM rca_outbox AS o
                 WHERE ({activation_predicate})
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
                (*activation_parameters, cutoff, now_iso),
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
                (*activation_parameters, now_iso, now_iso),
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
        activation_required: bool = False,
        historical_submission_allowlist: Iterable[str] = (),
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
                self._activation_claim_predicate_tx(
                    conn,
                    activation_required=activation_required,
                    historical_submission_allowlist=historical_submission_allowlist,
                    alias="o",
                )
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
        window_started_at = _utc_datetime(
            datetime.fromisoformat(
                str(row["retry_window_started_at"] or row["created_at"])
            )
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
                   last_error_detail = ?, updated_at = ?
             WHERE outbox_id = ? AND status = 'claimed' AND lease_token = ?
            """,
            (
                status,
                next_attempt_at,
                status,
                current_iso,
                str(error_code or "dispatch_failed")[:120],
                str(error_detail or "")[:1000],
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
        activation_required: bool = False,
        historical_submission_allowlist: Iterable[str] = (),
        now: datetime | None = None,
    ) -> list[dict[str, Any]]:
        """Read due rows without claiming or mutating them (used by dry-run)."""
        if limit < 1:
            return []
        current = _iso(now)
        conn = self._connect()
        try:
            activation_predicate, activation_parameters = (
                self._activation_claim_predicate_tx(
                    conn,
                    activation_required=activation_required,
                    historical_submission_allowlist=historical_submission_allowlist,
                )
            )
            rows = conn.execute(
                f"""
                SELECT o.outbox_id, o.action, o.submission_key, o.source_event_id,
                       o.source_topic, o.source_partition, o.source_offset, o.attempt,
                       o.status, o.next_attempt_at, o.lease_expires_at, o.created_at
                  FROM rca_outbox AS o
                 WHERE ({activation_predicate})
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
                (*activation_parameters, current, current, limit),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    @staticmethod
    def _exact_outbox_row_binding(row: sqlite3.Row) -> dict[str, Any]:
        """Return the small stable row binding used by the operator CAS."""
        projection = {
            field: row[field] for field in ACTIVATION_HISTORICAL_OUTBOX_ROW_FIELDS
        }
        return {
            "schema_version": EXACT_OUTBOX_HOLD_ROW_SCHEMA_VERSION,
            "outbox_id": int(row["outbox_id"]),
            "submission_key": str(row["submission_key"] or ""),
            "business_key": str(row["business_key"] or ""),
            "generation": int(row["generation"]),
            "status": str(row["status"] or ""),
            "attempt": int(row["attempt"]),
            "fence": int(row["fence"]),
            "next_attempt_at": (
                str(row["next_attempt_at"])
                if row["next_attempt_at"] is not None
                else None
            ),
            "retry_window_started_at": (
                str(row["retry_window_started_at"])
                if row["retry_window_started_at"] is not None
                else None
            ),
            "lease_token": row["lease_token"],
            "lease_owner": row["lease_owner"],
            "lease_expires_at": row["lease_expires_at"],
            "claimed_at": row["claimed_at"],
            "completed_at": row["completed_at"],
            "quarantined_at": row["quarantined_at"],
            "result_json": row["result_json"],
            "created_at": str(row["created_at"] or ""),
            "updated_at": str(row["updated_at"] or ""),
            "activation_epoch_id": row["activation_epoch_id"],
            "activation_ledger_id": row["activation_ledger_id"],
            "row_sha256": _exact_canonical_sha256(projection),
            "_row_projection": projection,
        }

    @staticmethod
    def _exact_bound_file_bytes(
        path: str | Path,
    ) -> tuple[str, bytes, os.stat_result]:
        lexical = Path(path).expanduser().absolute()
        try:
            lexical_stat = lexical.lstat()
        except OSError as exc:
            raise RuntimeError("exact_outbox_hold_bound_file_missing") from exc
        if (
            stat.S_ISLNK(lexical_stat.st_mode)
            or not stat.S_ISREG(lexical_stat.st_mode)
            or lexical_stat.st_nlink != 1
            or lexical_stat.st_uid != os.getuid()
            or stat.S_IMODE(lexical_stat.st_mode) & 0o022
        ):
            raise RuntimeError("exact_outbox_hold_bound_file_invalid")
        descriptor = os.open(
            lexical,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            before = os.fstat(descriptor)
            if (before.st_dev, before.st_ino) != (
                lexical_stat.st_dev,
                lexical_stat.st_ino,
            ):
                raise RuntimeError("exact_outbox_hold_bound_file_changed")
            digest = hashlib.sha256()
            chunks: list[bytes] = []
            while True:
                chunk = os.read(descriptor, 1024 * 1024)
                if not chunk:
                    break
                chunks.append(chunk)
                digest.update(chunk)
            after = os.fstat(descriptor)
            if (
                before.st_dev,
                before.st_ino,
                before.st_size,
                before.st_mtime_ns,
            ) != (
                after.st_dev,
                after.st_ino,
                after.st_size,
                after.st_mtime_ns,
            ):
                raise RuntimeError("exact_outbox_hold_bound_file_changed")
            return digest.hexdigest(), b"".join(chunks), before
        finally:
            os.close(descriptor)

    @staticmethod
    def _exact_bound_file_sha256(path: str | Path) -> str:
        digest, _raw, _identity = RcaControlStore._exact_bound_file_bytes(path)
        return digest

    @staticmethod
    def _exact_logical_source_identity(identity: Mapping[str, Any]) -> dict[str, Any]:
        logical = dict(identity.get("logical_db_identity") or {})
        wal = dict(logical.get("wal") or {})
        if wal.get("present") is True and int(wal.get("size", 0)) == 0:
            wal = {"present": False}
        logical["wal"] = wal
        return {
            "path": identity.get("path"),
            "logical_db_identity": logical,
        }

    @staticmethod
    def _exact_destination_parent_live(payload: Mapping[str, Any]) -> None:
        path = Path(str(payload["destination_path"])).expanduser().absolute()
        parent = path.parent
        try:
            observed = parent.lstat()
        except OSError as exc:
            raise RuntimeError("exact_outbox_hold_destination_parent_invalid") from exc
        binding = payload["destination_binding"]
        if (
            stat.S_ISLNK(observed.st_mode)
            or not stat.S_ISDIR(observed.st_mode)
            or observed.st_uid != os.getuid()
            or stat.S_IMODE(observed.st_mode) & 0o022
            or observed.st_dev != int(binding["parent_device"])
            or observed.st_ino != int(binding["parent_inode"])
            or Path(os.path.realpath(parent)) != parent
        ):
            raise RuntimeError("exact_outbox_hold_destination_parent_changed")

    @staticmethod
    def _exact_resident_census_live(payload: Mapping[str, Any]) -> None:
        census = payload["resident_census"]
        uid = str(os.getuid())
        expected_labels = list(EXACT_OUTBOX_RUNTIME_PLIST_LABELS)
        try:
            observed_at = datetime.fromisoformat(
                str(census["observed_at"]).replace("Z", "+00:00")
            )
            recorded_at = datetime.fromisoformat(
                str(payload["recorded_at"]).replace("Z", "+00:00")
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError("exact_outbox_hold_resident_census_invalid") from exc
        if (
            observed_at.tzinfo is None
            or observed_at.utcoffset() != timedelta(0)
            or recorded_at.tzinfo is None
            or recorded_at.utcoffset() != timedelta(0)
        ):
            raise RuntimeError("exact_outbox_hold_resident_census_invalid")
        current = datetime.now(timezone.utc)
        age = (current - observed_at.astimezone(timezone.utc)).total_seconds()
        if (
            observed_at.astimezone(timezone.utc)
            < recorded_at.astimezone(timezone.utc)
            or age > EXACT_OUTBOX_HOLD_RECORD_MAX_AGE_SECONDS
            or age < -EXACT_OUTBOX_HOLD_MAX_FUTURE_SKEW_SECONDS
        ):
            raise RuntimeError("exact_outbox_hold_resident_census_invalid")
        live_env_path = Path(
            str(payload["active_release_binding"]["live_env_path"])
        ).expanduser().absolute()
        canonical_binding_path = (
            live_env_path.parent
            / "runtime"
            / "pnc_agent"
            / "feishu_issue_kafka_rca"
            / "active-release-binding.json"
        )
        census_binding_path = Path(
            str(census["active_release_binding_path"])
        ).expanduser().absolute()
        if (
            census_binding_path != canonical_binding_path
            or str(census_binding_path)
            != str(payload["active_release_binding"]["path"])
        ):
            raise RuntimeError("exact_outbox_hold_resident_census_invalid")
        if census.get("forbidden_labels") != expected_labels:
            raise RuntimeError("exact_outbox_hold_resident_census_invalid")
        observations = census.get("observations")
        if not isinstance(observations, list) or len(observations) != len(expected_labels):
            raise RuntimeError("exact_outbox_hold_resident_census_invalid")
        for expected_label, expected in zip(expected_labels, observations, strict=True):
            try:
                result = subprocess.run(
                    ["launchctl", "print", f"gui/{uid}/{expected_label}"],
                    capture_output=True,
                    text=True,
                    check=False,
                    timeout=3,
                )
            except (OSError, subprocess.SubprocessError) as exc:
                raise RuntimeError("exact_outbox_hold_resident_census_unavailable") from exc
            combined = (result.stdout or "") + (result.stderr or "")
            unloaded_proven = bool(
                result.returncode == 113
                and re.fullmatch(
                    rf"Bad request\.\nCould not find service \"{re.escape(expected_label)}\" in domain for user gui: {re.escape(uid)}\n?",
                    combined,
                )
            )
            if result.returncode == 0 or not unloaded_proven:
                raise RuntimeError("exact_outbox_hold_forbidden_resident_loaded")
            observed_sha = hashlib.sha256(combined.encode("utf-8")).hexdigest()
            if (
                not isinstance(expected, Mapping)
                or expected.get("label") != expected_label
                or expected.get("returncode") != result.returncode
                or expected.get("unloaded_proven") is not True
                or expected.get("output_sha256") != observed_sha
            ):
                raise RuntimeError("exact_outbox_hold_resident_census_changed")

    def _validate_exact_hold_external_bindings(
        self,
        payload: Mapping[str, Any],
        *,
        include_control_db_identity: bool = True,
    ) -> None:
        self._exact_destination_parent_live(payload)
        self._exact_resident_census_live(payload)
        config_binding = payload["config_binding"]
        if config_binding.get("control_db_path") != str(self.db_path.expanduser().absolute()):
            raise RuntimeError("exact_outbox_hold_config_changed")
        live_path = Path(str(payload["active_release_binding"]["live_env_path"])).expanduser().absolute()
        hermes_home = live_path.parent
        canonical_db_path = (
            hermes_home
            / "runtime"
            / "pnc_agent"
            / "feishu_issue_kafka_rca"
            / "control.sqlite3"
        )
        canonical_binding_path = self.db_path.expanduser().absolute().parent / "active-release-binding.json"
        if (
            self.db_path.expanduser().absolute() != canonical_db_path
            or Path(str(payload["active_release_binding"]["path"])).expanduser().absolute()
            != canonical_binding_path
            or live_path != hermes_home / ".env"
        ):
            raise RuntimeError("exact_outbox_hold_config_changed")
        if (
            config_binding.get("active_release_binding_path")
            != payload["active_release_binding"]["path"]
            or config_binding.get("live_env_path")
            != payload["active_release_binding"]["live_env_path"]
            or config_binding.get("activation_required") is not True
        ):
            raise RuntimeError("exact_outbox_hold_config_changed")
        try:
            from scripts.pnc_rca_outbox_dispatcher import (
                DispatcherConfig,
                _exact_outbox_canonical_env_config,
            )

            canonical_config = _exact_outbox_canonical_env_config(
                payload["active_release_binding"]["live_env_path"]
            )
            process_config = DispatcherConfig.from_env()
            canonical_config_binding = canonical_config.public_dict()
            live_config_binding = process_config.public_dict()
        except Exception as exc:
            raise RuntimeError("exact_outbox_hold_config_unavailable") from exc
        if (
            canonical_config_binding != dict(config_binding)
            or live_config_binding != dict(config_binding)
        ):
            raise RuntimeError("exact_outbox_hold_config_changed")
        expected_max_age = canonical_config.max_age_seconds
        if (
            config_binding.get("max_age_seconds") != expected_max_age
            or payload.get("max_age_seconds") != expected_max_age
            or config_binding.get("release_id")
            != payload["active_release_binding"].get("release_id")
            or config_binding.get("bootstrap_epoch_id")
            != payload["active_release_binding"].get("bootstrap_epoch_id")
            or config_binding.get("allow_download") is not False
            or config_binding.get("allow_feishu_writeback") is not False
            or config_binding.get("data_access_mode") != "remote_read"
        ):
            raise RuntimeError("exact_outbox_hold_config_changed")
        if include_control_db_identity:
            snapshot_store = RcaControlStore(
                self.db_path,
                require_current=True,
                read_only=True,
            )
            current_identity = snapshot_store.control_db_source_snapshot_identity()
            if self._exact_logical_source_identity(current_identity) != self._exact_logical_source_identity(
                payload["control_db_identity"]
            ):
                raise RuntimeError("exact_outbox_hold_control_db_provenance_changed")
        binding = payload["active_release_binding"]
        active_digest, raw_binding, _active_identity = self._exact_bound_file_bytes(
            binding["path"]
        )
        if active_digest != binding["raw_sha256"]:
            raise RuntimeError("exact_outbox_hold_active_binding_changed")
        live_env_digest = self._exact_bound_file_sha256(binding["live_env_path"])
        if (
            live_env_digest != binding["live_env_sha256"]
            or live_env_digest != binding["candidate_env_sha256"]
        ):
            raise RuntimeError("exact_outbox_hold_live_env_changed")
        try:
            parsed_binding = json.loads(
                raw_binding.decode("utf-8"),
                parse_constant=lambda _value: (_ for _ in ()).throw(ValueError()),
            )
        except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise RuntimeError("exact_outbox_hold_active_binding_changed") from exc
        if not isinstance(parsed_binding, Mapping):
            raise RuntimeError("exact_outbox_hold_active_binding_changed")
        try:
            from gateway.pnc_rca_prod_bootstrap import load_active_release_binding

            live_binding = load_active_release_binding(
                path=Path(binding["path"]),
                live_env_path=Path(binding["live_env_path"]),
                expected_release_id=str(config_binding["release_id"]),
                expected_epoch_id=str(config_binding["bootstrap_epoch_id"]),
            )
        except Exception as exc:
            raise RuntimeError("exact_outbox_hold_active_binding_changed") from exc
        live_binding_projection = {
            "sha256": str(live_binding["binding_receipt_sha256"]),
            "release_id": str(live_binding["release_id"]),
            "authority_sha256": str(live_binding["authority_sha256"]),
            "authority_epoch_id": str(live_binding["authority_epoch_id"]),
            "bootstrap_epoch_id": str(live_binding["bootstrap_epoch_id"]),
            "release_bom_sha256": str(live_binding["release_bom_sha256"]),
            "candidate_env_sha256": str(live_binding["candidate_env_sha256"]),
            "authorization_fingerprint": str(
                live_binding["authorization_fingerprint"]
            ),
            "authorization_receipt_sha256": str(
                live_binding["authorization_receipt_sha256"]
            ),
            "approval_evidence_sha256": str(
                live_binding["approval_evidence_sha256"]
            ),
        }
        if (
            any(binding.get(key) != value for key, value in live_binding_projection.items())
            or self._exact_bound_file_sha256(binding["path"]) != active_digest
        ):
            raise RuntimeError("exact_outbox_hold_active_binding_changed")
        provenance = payload["tool_provenance"]
        for key, value in provenance.items():
            if not key.endswith("_path"):
                continue
            digest_key = key.removesuffix("_path") + "_sha256"
            if digest_key not in provenance:
                raise RuntimeError("exact_outbox_hold_tool_provenance_invalid")
            if self._exact_bound_file_sha256(value) != provenance[digest_key]:
                raise RuntimeError("exact_outbox_hold_tool_provenance_changed")
        entrypoint = Path(provenance["entrypoint_path"]).expanduser().absolute()
        control_source = Path(provenance["control_store_path"]).expanduser().absolute()
        bootstrap_source = Path(provenance["bootstrap_path"]).expanduser().absolute()
        module_source = Path(__file__).expanduser().absolute()
        module_root = module_source.parent.parent
        if (
            Path(os.path.realpath(module_source)) != module_source
            or Path(os.path.realpath(entrypoint)) != entrypoint
            or Path(os.path.realpath(control_source)) != control_source
            or Path(os.path.realpath(bootstrap_source)) != bootstrap_source
            or entrypoint != module_root / "scripts" / "pnc_rca_outbox_dispatcher.py"
            or control_source != module_source
            or bootstrap_source != module_root / "gateway" / "pnc_rca_prod_bootstrap.py"
        ):
            raise RuntimeError("exact_outbox_hold_tool_provenance_invalid")
        tool_root = module_root
        tool_head = subprocess.run(
            ["git", "-C", str(tool_root), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
        tool_tree = subprocess.run(
            ["git", "-C", str(tool_root), "rev-parse", "HEAD^{tree}"],
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
        tool_status = subprocess.run(
            ["git", "-C", str(tool_root), "status", "--porcelain", "--untracked-files=all"],
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
        if (
            tool_head.returncode != 0
            or tool_tree.returncode != 0
            or tool_status.returncode != 0
            or tool_head.stdout.strip() != provenance["git_head"]
            or tool_tree.stdout.strip() != provenance["git_tree"]
            or tool_status.stdout.strip()
            or provenance.get("git_status_returncode") != 0
            or provenance.get("git_clean") is not True
        ):
            raise RuntimeError("exact_outbox_hold_tool_provenance_changed")
        runtime = provenance["runtime_provenance"]
        runtime_files = [runtime["manifest"], *runtime["plists"], runtime["stable_target_registry"]]
        runtime_raw: dict[str, bytes] = {}
        for file_binding in runtime_files:
            digest, raw, _identity = self._exact_bound_file_bytes(file_binding["path"])
            if digest != file_binding["sha256"]:
                raise RuntimeError("exact_outbox_hold_runtime_provenance_changed")
            runtime_raw[str(Path(file_binding["path"]).expanduser().absolute())] = raw
        manifest_path = Path(runtime["manifest"]["path"]).expanduser().absolute()
        hermes_home = Path(
            payload["active_release_binding"]["live_env_path"]
        ).expanduser().absolute().parent
        if (
            manifest_path != hermes_home / "runtime" / "LIVE_MANIFEST.json"
            or Path(os.path.realpath(manifest_path)) != manifest_path
        ):
            raise RuntimeError("exact_outbox_hold_runtime_provenance_changed")
        try:
            manifest = json.loads(
                runtime_raw[str(manifest_path)].decode("utf-8"),
                parse_constant=lambda _value: (_ for _ in ()).throw(ValueError()),
            )
        except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise RuntimeError("exact_outbox_hold_runtime_provenance_changed") from exc
        release_binding = manifest.get("gateway_release_binding") if isinstance(manifest, Mapping) else None
        capacity = release_binding.get("capacity_admission") if isinstance(release_binding, Mapping) else None
        if (
            not isinstance(manifest, Mapping)
            or manifest.get("runtime_root") != runtime["manifest_runtime_root"]
            or manifest.get("runtime_release_target") != runtime["manifest_runtime_release_target"]
            or manifest.get("gateway_release_target") != runtime["manifest_gateway_release_target"]
            or not isinstance(release_binding, Mapping)
            or release_binding.get("commit") != runtime["manifest_commit"]
            or release_binding.get("tree") != runtime["manifest_tree"]
            or not isinstance(capacity, Mapping)
            or capacity.get("release_bom_sha256") != runtime["release_bom_sha256"]
            or payload["active_release_binding"].get("release_bom_sha256")
            != runtime["release_bom_sha256"]
        ):
            raise RuntimeError("exact_outbox_hold_runtime_provenance_changed")
        runtime_root = Path(runtime["manifest_runtime_root"]).expanduser().absolute()
        if (
            runtime_root != Path(runtime["manifest_runtime_root"])
            or Path(os.path.realpath(runtime_root)) != runtime_root
            or runtime_root.parent
            != hermes_home / "runtime" / "releases"
        ):
            raise RuntimeError("exact_outbox_hold_runtime_provenance_changed")
        try:
            runtime_root_stat = runtime_root.lstat()
        except OSError as exc:
            raise RuntimeError("exact_outbox_hold_runtime_provenance_changed") from exc
        if (
            stat.S_ISLNK(runtime_root_stat.st_mode)
            or not stat.S_ISDIR(runtime_root_stat.st_mode)
            or runtime_root_stat.st_uid != os.getuid()
            or stat.S_IMODE(runtime_root_stat.st_mode) & 0o022
        ):
            raise RuntimeError("exact_outbox_hold_runtime_provenance_changed")
        expected_plist_prefix = Path.home() / "Library" / "LaunchAgents"
        expected_plist_paths = [
            expected_plist_prefix / f"{label}.plist"
            for label in EXACT_OUTBOX_RUNTIME_PLIST_LABELS
        ]
        actual_plist_paths = [
            Path(item["path"]).expanduser().absolute() for item in runtime["plists"]
        ]
        if actual_plist_paths != expected_plist_paths:
            raise RuntimeError("exact_outbox_hold_runtime_provenance_changed")
        for label, item in zip(EXACT_OUTBOX_RUNTIME_PLIST_LABELS, runtime["plists"], strict=True):
            plist_path = Path(item["path"])
            observed = plist_path.lstat()
            if Path(os.path.realpath(plist_path)) != plist_path:
                raise RuntimeError("exact_outbox_hold_runtime_provenance_changed")
            if (
                observed.st_uid != int(item["uid"])
                or observed.st_nlink != int(item["nlink"])
                or stat.S_IMODE(observed.st_mode) != int(item["mode"])
            ):
                raise RuntimeError("exact_outbox_hold_runtime_provenance_changed")
            try:
                from scripts.pnc_rca_release_transaction import _validate_plist

                _validate_plist(
                    runtime_raw[str(plist_path)],
                    label=label,
                    hermes_home=hermes_home,
                )
            except Exception as exc:
                raise RuntimeError("exact_outbox_hold_runtime_provenance_changed") from exc
        registry_path = Path(runtime["stable_target_registry"]["path"]).expanduser().absolute()
        if (
            registry_path
            != runtime_root / "gateway" / "assets" / "pnc_stable_target_registry_v1.json"
            or Path(os.path.realpath(registry_path)) != registry_path
        ):
            raise RuntimeError("exact_outbox_hold_runtime_provenance_changed")
        try:
            from scripts.pnc_live_exec import SERVICE_TARGETS, _stable_target_registry

            registered_targets = _stable_target_registry(runtime_root)
            for label, (target_kind, relative_target) in SERVICE_TARGETS.items():
                if target_kind not in {"governance_tool", "runtime_file"}:
                    continue
                target_base = (
                    hermes_home / "runtime" / "governance-tools"
                    if target_kind == "governance_tool"
                    else hermes_home / "runtime"
                )
                target_path = (target_base / relative_target).absolute()
                expected = registered_targets[label]
                digest, raw, _identity = self._exact_bound_file_bytes(target_path)
                if (
                    Path(os.path.realpath(target_path)) != target_path
                    or len(raw) != expected["size"]
                    or digest != expected["sha256"]
                ):
                    raise RuntimeError("exact_outbox_hold_runtime_target_changed")
        except Exception as exc:
            if isinstance(exc, RuntimeError) and str(exc) == "exact_outbox_hold_runtime_target_changed":
                raise
            raise RuntimeError("exact_outbox_hold_runtime_provenance_changed") from exc
        git_head = subprocess.run(
            ["git", "-C", str(runtime_root), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
        git_tree = subprocess.run(
            ["git", "-C", str(runtime_root), "rev-parse", "HEAD^{tree}"],
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
        git_status = subprocess.run(
            ["git", "-C", str(runtime_root), "status", "--porcelain", "--untracked-files=all"],
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
        if (
            git_head.returncode != 0
            or git_tree.returncode != 0
            or git_status.returncode != 0
            or git_head.stdout.strip() != runtime["runtime_git_head"]
            or git_tree.stdout.strip() != runtime["runtime_git_tree"]
            or git_status.stdout.strip()
        ):
            raise RuntimeError("exact_outbox_hold_runtime_provenance_changed")

    @staticmethod
    def _exact_hold_freshness(
        payload: Mapping[str, Any], fresh_now: datetime
    ) -> tuple[datetime, datetime, int]:
        try:
            recorded = datetime.fromisoformat(
                str(payload["recorded_at"]).replace("Z", "+00:00")
            )
            horizon = payload["retry_horizon"]
            expires = datetime.fromisoformat(
                str(horizon["expires_at"]).replace("Z", "+00:00")
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError("exact_outbox_hold_retry_horizon_invalid") from exc
        if recorded.tzinfo is None or expires.tzinfo is None:
            raise RuntimeError("exact_outbox_hold_retry_horizon_invalid")
        fresh = _utc_datetime(fresh_now)
        age = (fresh - _utc_datetime(recorded)).total_seconds()
        if age < -5 or age > EXACT_OUTBOX_HOLD_RECORD_MAX_AGE_SECONDS:
            raise RuntimeError("exact_outbox_hold_recorded_at_stale")
        remaining = (_utc_datetime(expires) - fresh).total_seconds()
        if remaining < EXACT_OUTBOX_HOLD_MIN_REMAINING_SECONDS:
            raise RuntimeError("exact_outbox_hold_retry_horizon_headroom_insufficient")
        return recorded, expires, int(remaining)

    @staticmethod
    def _exact_hold_with_apply_observation(
        payload: Mapping[str, Any], fresh_now: datetime, remaining: int
    ) -> dict[str, Any]:
        effective = dict(payload)
        horizon = dict(payload["retry_horizon"])
        horizon["apply_observed_at"] = _iso(fresh_now)
        horizon["apply_remaining_seconds"] = int(remaining)
        effective["retry_horizon"] = horizon
        effective["receipt_fingerprint"] = _exact_outbox_hold_fingerprint(effective)
        return _validate_exact_outbox_hold_audit(effective)

    @staticmethod
    def _exact_outbox_queue_sha256(entries: Iterable[Mapping[str, Any]]) -> str:
        return _exact_canonical_sha256(
            [
                {
                    "outbox_id": int(item["outbox_id"]),
                    "row_sha256": str(item["row_sha256"]),
                }
                for item in entries
            ]
        )

    @classmethod
    def _exact_outbox_activation_binding_tx(
        cls, conn: sqlite3.Connection
    ) -> dict[str, Any]:
        current_rows = conn.execute(
            "SELECT * FROM rca_activation_epochs WHERE is_current = 1"
        ).fetchall()
        if len(current_rows) > 1:
            raise RuntimeError("exact_outbox_hold_activation_not_unique")
        epoch = current_rows[0] if current_rows else None
        if epoch is None:
            value: dict[str, Any] = {"configured": False, "epoch_id": ""}
        else:
            db_identity_json = str(epoch["db_logical_identity_json"])
            try:
                db_identity = json.loads(
                    db_identity_json,
                    parse_constant=lambda _value: (_ for _ in ()).throw(ValueError()),
                )
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise RuntimeError(
                    "exact_outbox_hold_activation_db_binding_invalid"
                ) from exc
            if (
                not isinstance(db_identity, Mapping)
                or _exact_canonical_json(db_identity) != db_identity_json
                or hashlib.sha256(db_identity_json.encode("utf-8")).hexdigest()
                != str(epoch["db_logical_identity_sha256"])
            ):
                raise RuntimeError("exact_outbox_hold_activation_db_binding_invalid")
            value = {
                "configured": True,
                "epoch_id": str(epoch["epoch_id"]),
                "state": str(epoch["state"]),
                "is_current": int(epoch["is_current"]),
                "config_sha256": str(epoch["config_sha256"]),
                "db_logical_identity_sha256": str(
                    epoch["db_logical_identity_sha256"]
                ),
                "db_logical_identity": dict(db_identity),
                "preproduction_fingerprint": str(
                    epoch["preproduction_fingerprint"] or ""
                ),
                "preproduction_gate_receipt_sha256": str(
                    epoch["preproduction_gate_receipt_sha256"] or ""
                ),
                "production_fingerprint": str(epoch["production_fingerprint"] or ""),
                "production_gate_receipt_sha256": str(
                    epoch["production_gate_receipt_sha256"] or ""
                ),
            }
        value["sha256"] = _exact_canonical_sha256(
            {key: item for key, item in value.items() if key != "sha256"}
        )
        return value

    @classmethod
    def _exact_outbox_hold_role_binding_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        row: Mapping[str, Any],
        epoch_id: str,
        expected_slot_kind: str,
    ) -> None:
        """Bind each hold row to its exact consumed manual activation slot."""
        binding = conn.execute(
            """
            SELECT s.slot_kind, s.authorized_source_kind,
                   s.authorized_identity_sha256, s.consumed_ledger_id,
                   s.consumed_at, al.ledger_id, al.epoch_id AS ledger_epoch_id,
                   al.entrypoint, al.source_kind, al.source_identity_sha256,
                   al.slot_kind AS ledger_slot_kind, al.decision, al.bound_at,
                   al.business_key, al.submission_key, al.generation,
                   t.activation_epoch_id AS trigger_epoch_id,
                   t.activation_ledger_id AS trigger_ledger_id
              FROM rca_activation_budget_slots AS s
              JOIN rca_activation_admission_ledger AS al
                ON al.epoch_id = s.epoch_id
               AND al.ledger_id = s.consumed_ledger_id
              JOIN business_triggers AS t
                ON t.activation_epoch_id = al.epoch_id
               AND t.activation_ledger_id = al.ledger_id
               AND t.business_key = al.business_key
               AND t.submission_key = al.submission_key
               AND t.generation = al.generation
             WHERE s.epoch_id = ?
               AND s.slot_kind = ?
               AND s.consumed_ledger_id = ?
               AND t.business_key = ?
               AND t.submission_key = ?
               AND t.generation = ?
            """,
            (
                str(epoch_id),
                str(expected_slot_kind),
                row["activation_ledger_id"],
                row["business_key"],
                row["submission_key"],
                row["generation"],
            ),
        ).fetchone()
        if (
            binding is None
            or str(binding["slot_kind"] or "") != expected_slot_kind
            or str(binding["authorized_source_kind"] or "") != "manual"
            or not str(binding["authorized_identity_sha256"] or "")
            or binding["consumed_ledger_id"] is None
            or not str(binding["consumed_at"] or "")
            or int(binding["ledger_id"] or 0)
            != int(binding["consumed_ledger_id"] or 0)
            or str(binding["ledger_epoch_id"] or "") != str(epoch_id)
            or str(binding["entrypoint"] or "") != "manual_admit"
            or str(binding["source_kind"] or "") != "manual"
            or str(binding["source_identity_sha256"] or "")
            != str(binding["authorized_identity_sha256"] or "")
            or str(binding["ledger_slot_kind"] or "") != expected_slot_kind
            or str(binding["decision"] or "") != "admit"
            or not str(binding["bound_at"] or "")
            or str(binding["business_key"] or "") != str(row["business_key"])
            or str(binding["submission_key"] or "")
            != str(row["submission_key"])
            or int(binding["generation"] or 0) != int(row["generation"])
            or str(binding["trigger_epoch_id"] or "") != str(epoch_id)
            or int(binding["trigger_ledger_id"] or 0)
            != int(binding["ledger_id"] or 0)
        ):
            raise RuntimeError("exact_outbox_hold_activation_role_binding_invalid")

    @classmethod
    def _exact_outbox_hold_snapshot_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        target_outbox_id: int,
        predecessor_outbox_id: int,
        activation_required: bool,
        max_age_seconds: int,
        now: datetime,
        require_exact_queue: bool = True,
        include_private_projection: bool = False,
    ) -> dict[str, Any]:
        if isinstance(target_outbox_id, bool) or int(target_outbox_id) < 1:
            raise ValueError("exact_outbox_hold_target_id_invalid")
        if isinstance(predecessor_outbox_id, bool) or int(predecessor_outbox_id) < 1:
            raise ValueError("exact_outbox_hold_predecessor_id_invalid")
        if int(target_outbox_id) == int(predecessor_outbox_id):
            raise ValueError("exact_outbox_hold_identity_invalid")
        if not isinstance(max_age_seconds, int) or isinstance(max_age_seconds, bool):
            raise ValueError("exact_outbox_hold_max_age_invalid")
        if max_age_seconds < 1:
            raise ValueError("exact_outbox_hold_max_age_invalid")
        current = _utc_datetime(now)
        current_iso = _iso(current)
        cutoff_iso = _iso(current - timedelta(seconds=max_age_seconds))
        predicate, parameters = cls._activation_claim_predicate_tx(
            conn,
            activation_required=activation_required,
            historical_submission_allowlist=(),
        )
        target = conn.execute(
            "SELECT * FROM rca_outbox WHERE outbox_id = ?",
            (int(target_outbox_id),),
        ).fetchone()
        predecessor = conn.execute(
            "SELECT * FROM rca_outbox WHERE outbox_id = ?",
            (int(predecessor_outbox_id),),
        ).fetchone()
        if target is None:
            raise RuntimeError("exact_outbox_hold_target_missing")
        if predecessor is None:
            raise RuntimeError("exact_outbox_hold_predecessor_missing")
        for row in (target, predecessor):
            for field in ("created_at", "retry_window_started_at"):
                raw_timestamp = row[field]
                if raw_timestamp is None:
                    if field == "created_at":
                        raise RuntimeError("exact_outbox_hold_retry_window_invalid")
                    continue
                try:
                    parsed_timestamp = datetime.fromisoformat(
                        str(raw_timestamp).replace("Z", "+00:00")
                    )
                except (TypeError, ValueError) as exc:
                    raise RuntimeError(
                        "exact_outbox_hold_retry_window_invalid"
                    ) from exc
                if (
                    parsed_timestamp.tzinfo is None
                    or parsed_timestamp.utcoffset() != timedelta(0)
                    or _utc_datetime(parsed_timestamp) > current
                ):
                    raise RuntimeError("exact_outbox_hold_retry_window_invalid")
        rows = conn.execute(
            f"""
            SELECT o.*
              FROM rca_outbox AS o
             WHERE ({predicate})
               AND COALESCE(o.retry_window_started_at, o.created_at) > ?
               AND (
                    (o.status = 'pending'
                     AND (o.next_attempt_at IS NULL OR o.next_attempt_at <= ?))
                    OR (o.status = 'claimed'
                        AND o.lease_expires_at IS NOT NULL
                        AND o.lease_expires_at <= ?)
               )
             ORDER BY o.outbox_id
            """,
            (*parameters, cutoff_iso, current_iso, current_iso),
        ).fetchall()
        bindings = [cls._exact_outbox_row_binding(row) for row in rows]
        queue_entries = [
            {"outbox_id": item["outbox_id"], "row_sha256": item["row_sha256"]}
            for item in bindings
        ]
        queue_ids = [int(item["outbox_id"]) for item in queue_entries]
        expected_ids = sorted(
            [int(predecessor_outbox_id), int(target_outbox_id)]
        )
        if require_exact_queue and queue_ids != expected_ids:
            raise RuntimeError(
                "exact_outbox_hold_eligible_queue_changed:"
                f"observed={queue_ids}:expected={expected_ids}"
            )
        target_binding = cls._exact_outbox_row_binding(target)
        predecessor_binding = cls._exact_outbox_row_binding(predecessor)
        active_activation = cls._exact_outbox_activation_binding_tx(conn)
        if (
            activation_required is not True
            or active_activation.get("configured") is not True
            or active_activation.get("state") != "bounded_active"
        ):
            raise RuntimeError("exact_outbox_hold_bounded_activation_required")
        epoch_id = str(active_activation.get("epoch_id") or "")
        cls._exact_outbox_hold_role_binding_tx(
            conn,
            row=predecessor,
            epoch_id=epoch_id,
            expected_slot_kind="manual_success",
        )
        cls._exact_outbox_hold_role_binding_tx(
            conn,
            row=target,
            epoch_id=epoch_id,
            expected_slot_kind="manual_terminal_failure",
        )
        for role, binding in (
            ("target", target_binding),
            ("predecessor", predecessor_binding),
        ):
            if (
                binding["status"] != "pending"
                or binding["attempt"] != 0
                or binding["fence"] != 0
                or (
                    binding["next_attempt_at"] is not None
                    and (require_exact_queue or role == "predecessor")
                )
                or any(
                    binding[field] is not None
                    for field in (
                        "lease_token",
                        "lease_owner",
                        "lease_expires_at",
                        "claimed_at",
                        "completed_at",
                        "quarantined_at",
                        "result_json",
                    )
                )
            ):
                raise RuntimeError(f"exact_outbox_hold_{role}_baseline_invalid")
        target_anchor = target["retry_window_started_at"] or target["created_at"]
        try:
            target_anchor_dt = _utc_datetime(
                datetime.fromisoformat(str(target_anchor).replace("Z", "+00:00"))
            )
        except (TypeError, ValueError) as exc:
            raise RuntimeError("exact_outbox_hold_retry_window_invalid") from exc
        expires_at = target_anchor_dt + timedelta(seconds=max_age_seconds)
        remaining = (expires_at - current).total_seconds()
        if remaining <= 0:
            raise RuntimeError("exact_outbox_hold_retry_horizon_expired")
        snapshot = {
            "schema_version": EXACT_OUTBOX_HOLD_SNAPSHOT_SCHEMA_VERSION,
            "observed_at": current_iso,
            "target_outbox_id": int(target_outbox_id),
            "predecessor_outbox_id": int(predecessor_outbox_id),
            "activation_required": activation_required,
            "max_age_seconds": max_age_seconds,
            "active_activation": active_activation,
            "target": target_binding,
            "predecessor": predecessor_binding,
            "eligible_queue": {
                "outbox_ids": queue_ids,
                "entries": queue_entries,
                "sha256": cls._exact_outbox_queue_sha256(queue_entries),
            },
            "retry_horizon": {
                "target_outbox_id": int(target_outbox_id),
                "anchor": str(target_anchor),
                "expires_at": _iso(expires_at),
                "cutoff_at": cutoff_iso,
                "remaining_seconds": remaining,
            },
        }
        if not include_private_projection:
            snapshot["target"].pop("_row_projection", None)
            snapshot["predecessor"].pop("_row_projection", None)
        return snapshot

    def exact_outbox_hold_snapshot(
        self,
        *,
        target_outbox_id: int,
        predecessor_outbox_id: int,
        activation_required: bool = True,
        max_age_seconds: int = 86_400,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        """Read the exact bounded queue without taking a write lock."""
        conn = self._connect()
        try:
            return self._exact_outbox_hold_snapshot_tx(
                conn,
                target_outbox_id=target_outbox_id,
                predecessor_outbox_id=predecessor_outbox_id,
                activation_required=activation_required,
                max_age_seconds=max_age_seconds,
                now=_utc_datetime(now),
            )
        finally:
            conn.close()

    def _exact_outbox_hold_private_snapshot(
        self,
        *,
        target_outbox_id: int,
        predecessor_outbox_id: int,
        activation_required: bool,
        max_age_seconds: int,
        now: datetime,
    ) -> dict[str, Any]:
        conn = self._connect()
        try:
            return self._exact_outbox_hold_snapshot_tx(
                conn,
                target_outbox_id=target_outbox_id,
                predecessor_outbox_id=predecessor_outbox_id,
                activation_required=activation_required,
                max_age_seconds=max_age_seconds,
                now=now,
                include_private_projection=True,
            )
        finally:
            conn.close()

    def hold_exact_outbox_with_audit(
        self,
        *,
        audit: Mapping[str, Any],
        now: datetime | None = None,
    ) -> dict[str, Any]:
        """CAS-hold exactly one pending outbox row and durably audit it."""
        if self.read_only:
            raise RuntimeError("exact_outbox_hold_mutation_requires_read_write_store")
        payload = _validate_exact_outbox_hold_audit(audit)
        # The caller's timestamp is only an audit anchor.  Fresh wall-clock
        # reads govern admission and are repeated after taking the write lock.
        fresh_before = _utc_datetime()
        self._exact_hold_freshness(payload, fresh_before)
        current = str(payload["recorded_at"])
        try:
            db_identity = self.db_path.expanduser().absolute().lstat()
        except OSError as exc:
            raise RuntimeError("exact_outbox_hold_control_db_missing") from exc
        recorded_identity = payload["control_db_identity"]
        if (
            recorded_identity.get("path") != str(self.db_path.expanduser().absolute())
            or int(recorded_identity.get("device")) != int(db_identity.st_dev)
            or int(recorded_identity.get("inode")) != int(db_identity.st_ino)
        ):
            raise RuntimeError("exact_outbox_hold_control_db_changed")
        self._validate_exact_hold_external_bindings(payload)
        meta_key = f"{EXACT_OUTBOX_HOLD_META_PREFIX}{payload['hold_id']}"
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            fresh_inside = _utc_datetime()
            self._exact_hold_freshness(payload, fresh_inside)
            self._validate_exact_hold_external_bindings(payload)
            changes_before = conn.total_changes
            existing = conn.execute(
                "SELECT value FROM control_meta WHERE key = ?", (meta_key,)
            ).fetchone()
            if existing is not None:
                if str(existing["value"]) == _exact_canonical_json(payload):
                    raise RuntimeError("exact_outbox_hold_already_applied")
                raise RuntimeError("exact_outbox_hold_audit_key_conflict")
            snapshot = self._exact_outbox_hold_snapshot_tx(
                conn,
                target_outbox_id=int(payload["target_outbox_id"]),
                predecessor_outbox_id=int(payload["predecessor_outbox_id"]),
                activation_required=bool(payload["activation_required"]),
                max_age_seconds=int(payload["max_age_seconds"]),
                now=fresh_inside,
            )
            target = snapshot["target"]
            predecessor = snapshot["predecessor"]
            payload_horizon = payload["retry_horizon"]
            snapshot_horizon = snapshot["retry_horizon"]
            actual_remaining = snapshot_horizon["remaining_seconds"]
            if (
                payload_horizon["target_outbox_id"]
                != snapshot_horizon["target_outbox_id"]
                or payload_horizon["anchor"] != snapshot_horizon["anchor"]
                or payload_horizon["expires_at"]
                != snapshot_horizon["expires_at"]
                or isinstance(actual_remaining, bool)
                or not isinstance(actual_remaining, (int, float))
                or actual_remaining < EXACT_OUTBOX_HOLD_MIN_REMAINING_SECONDS
            ):
                raise RuntimeError("exact_outbox_hold_retry_horizon_changed")
            remaining_inside = int(actual_remaining)
            if (
                dict(target) != dict(payload["target_before"])
                or dict(predecessor) != dict(payload["predecessor"])
                or snapshot["eligible_queue"]["sha256"]
                != payload["eligible_queue_before"]["sha256"]
                or dict(snapshot["active_activation"])
                != dict(payload["active_activation"])
                or snapshot["active_activation"].get("configured") is not True
                or snapshot["active_activation"].get("state") != "bounded_active"
                or target["status"] != "pending"
                or target["attempt"] != 0
                or target["fence"] != 0
                or target["next_attempt_at"] is not None
                or any(
                    target[field] is not None
                    for field in (
                        "lease_token",
                        "lease_owner",
                        "lease_expires_at",
                        "claimed_at",
                        "completed_at",
                        "quarantined_at",
                        "result_json",
                    )
                )
            ):
                raise RuntimeError("exact_outbox_hold_target_changed")
            if (
                predecessor["status"] != "pending"
                or predecessor["attempt"] != 0
                or predecessor["fence"] != 0
                or predecessor["next_attempt_at"] is not None
                or any(
                    predecessor[field] is not None
                    for field in (
                        "lease_token",
                        "lease_owner",
                        "lease_expires_at",
                        "claimed_at",
                        "completed_at",
                        "quarantined_at",
                        "result_json",
                    )
                )
            ):
                raise RuntimeError("exact_outbox_hold_predecessor_not_pending")
            effective_payload = self._exact_hold_with_apply_observation(
                payload, fresh_inside, remaining_inside
            )
            conn.execute(
                "INSERT INTO control_meta(key, value) VALUES(?, ?)",
                (meta_key, _exact_canonical_json(effective_payload)),
            )
            updated = conn.execute(
                """
                UPDATE rca_outbox
                   SET next_attempt_at = ?, updated_at = ?
                 WHERE outbox_id = ? AND status = 'pending'
                   AND attempt = 0 AND fence = ?
                   AND submission_key = ? AND business_key = ? AND generation = ?
                   AND next_attempt_at IS ? AND updated_at = ?
                   AND lease_token IS NULL AND lease_owner IS NULL
                   AND lease_expires_at IS NULL AND claimed_at IS NULL
                   AND completed_at IS NULL AND quarantined_at IS NULL
                   AND result_json IS NULL
                """,
                (
                    EXACT_OUTBOX_HOLD_UNTIL,
                    current,
                    int(payload["target_outbox_id"]),
                    int(target["fence"]),
                    target["submission_key"],
                    target["business_key"],
                    int(target["generation"]),
                    target["next_attempt_at"],
                    target["updated_at"],
                ),
            )
            if updated.rowcount != 1:
                raise RuntimeError("exact_outbox_hold_target_cas_lost")
            held = conn.execute(
                "SELECT * FROM rca_outbox WHERE outbox_id = ?",
                (int(payload["target_outbox_id"]),),
            ).fetchone()
            if held is None:
                raise RuntimeError("exact_outbox_hold_post_row_missing")
            held_binding = self._exact_outbox_row_binding(held)
            public_held_binding = {
                key: item
                for key, item in held_binding.items()
                if not key.startswith("_")
            }
            if (
                public_held_binding != dict(payload["target_after"])
                or public_held_binding["next_attempt_at"] != EXACT_OUTBOX_HOLD_UNTIL
                or public_held_binding["updated_at"] != current
            ):
                raise RuntimeError("exact_outbox_hold_post_row_changed")
            after_snapshot = self._exact_outbox_hold_snapshot_tx(
                conn,
                target_outbox_id=int(payload["target_outbox_id"]),
                predecessor_outbox_id=int(payload["predecessor_outbox_id"]),
                activation_required=bool(payload["activation_required"]),
                max_age_seconds=int(payload["max_age_seconds"]),
                now=fresh_inside,
                require_exact_queue=False,
            )
            if (
                after_snapshot["eligible_queue"]["outbox_ids"]
                != [int(payload["predecessor_outbox_id"])]
                or after_snapshot["eligible_queue"]["sha256"]
                != payload["eligible_queue_after"]["sha256"]
                or conn.total_changes - changes_before != 2
            ):
                raise RuntimeError("exact_outbox_hold_effect_delta_changed")
            self._validate_exact_hold_external_bindings(
                effective_payload,
                include_control_db_identity=False,
            )
            self._exact_hold_freshness(payload, _utc_datetime())
            conn.commit()
            return dict(effective_payload)
        except Exception:
            if conn.in_transaction:
                conn.rollback()
            raise
        finally:
            conn.close()

    def exact_outbox_hold_audit(self, hold_id: str) -> dict[str, Any] | None:
        normalized = str(hold_id or "").strip()
        if not normalized:
            raise ValueError("exact_outbox_hold_id_invalid")
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT value FROM control_meta WHERE key = ?",
                (f"{EXACT_OUTBOX_HOLD_META_PREFIX}{normalized}",),
            ).fetchone()
            if row is None:
                return None
            raw = str(row["value"])
            try:
                value = json.loads(raw, parse_constant=lambda _v: (_ for _ in ()).throw(ValueError()))
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise RuntimeError("exact_outbox_hold_audit_invalid") from exc
            value = _validate_exact_outbox_hold_audit(value)
            if (
                value["hold_id"] != normalized
                or _exact_canonical_json(value) != raw
            ):
                raise RuntimeError("exact_outbox_hold_audit_tampered")
            identity = value["control_db_identity"]
            observed = self.db_path.expanduser().absolute().lstat()
            if (
                identity.get("path") != str(self.db_path.expanduser().absolute())
                or int(identity.get("device")) != int(observed.st_dev)
                or int(identity.get("inode")) != int(observed.st_ino)
            ):
                raise RuntimeError("exact_outbox_hold_control_db_changed")
            return value
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
            "rca_activation_budget_slots",
            "rca_activation_admission_ledger",
            "rca_activation_transition_audit",
            "rca_capacity_transition_state",
            "rca_capacity_transition_audit",
            "rca_canonical_requests",
            "rca_admission_snapshots",
            "rca_source_authority_receipts",
            "rca_snapshot_source_envelopes",
            "rca_learning_lane_cohorts",
            "rca_learning_lane_stock_items",
            "rca_learning_lane_admissions",
            "rca_activation_historical_outbox_holds",
            "rca_activation_historical_outbox_hold_items",
            "rca_activation_historical_outbox_dispositions",
            "rca_activation_historical_outbox_disposition_items",
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
        """Count only work that can pressure the external submission path."""
        conn = self._connect()
        try:
            return int(
                conn.execute(
                    """
                    SELECT COUNT(*) FROM rca_outbox
                     WHERE status IN ('pending', 'claimed')
                       AND NOT EXISTS (
                           SELECT 1
                             FROM rca_activation_historical_outbox_hold_items AS held
                            WHERE held.outbox_id = rca_outbox.outbox_id
                       )
                    """
                ).fetchone()[0]
            )
        finally:
            conn.close()

    def activation_ingress_freeze_readiness(self) -> dict[str, Any]:
        """Read only the bounded-ingress fence fields needed by the Kafka loop."""
        conn = self._connect()
        try:
            conn.execute("BEGIN")
            epoch = self._current_activation_epoch_tx(conn)
            epoch_id = str(epoch["epoch_id"]) if epoch is not None else ""
            state = str(epoch["state"]) if epoch is not None else "unconfigured"
            consumed_slot_count = 0
            completed_bound_slot_count = 0
            pending_inbox = 0
            unbound_ledger = 0
            inflight_writes = 0
            reason = (
                "activation_epoch_unconfigured"
                if epoch is None
                else "activation_epoch_not_bounded"
            )

            # The full health snapshot scans retention and capacity state. Kafka
            # needs the narrower execution proof only during the short bounded
            # window; steady-state polls stay an indexed epoch lookup.
            if state == "bounded_active":
                consumed_slot_count = int(
                    conn.execute(
                        f"""
                        SELECT COUNT(*) FROM rca_activation_budget_slots
                         WHERE epoch_id = ?
                           AND slot_kind IN ({_ACTIVATION_RELEASE_SLOT_SQL})
                           AND consumed_ledger_id IS NOT NULL
                        """,
                        (epoch_id,),
                    ).fetchone()[0]
                )
                completed_bound_slot_count = (
                    self._activation_completed_bound_slot_count_tx(
                        conn,
                        epoch_id=epoch_id,
                    )
                )
                pending_inbox = int(
                    conn.execute(
                        "SELECT COUNT(*) FROM kafka_inbox WHERE decision = 'pending'"
                    ).fetchone()[0]
                )
                unbound_ledger = int(
                    conn.execute(
                        """
                        SELECT COUNT(*)
                          FROM rca_activation_admission_ledger AS al
                         WHERE al.epoch_id = ?
                           AND al.decision IN ('admit', 'shadow')
                           AND (
                               al.bound_at IS NULL
                            OR NOT EXISTS (
                                SELECT 1 FROM business_triggers AS t
                                 WHERE t.activation_epoch_id = al.epoch_id
                                   AND t.activation_ledger_id = al.ledger_id
                                   AND t.business_key = al.business_key
                                   AND t.submission_key = al.submission_key
                                   AND t.generation = al.generation
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
                inflight_writes = self._activation_inflight_writes_tx(conn)
                if consumed_slot_count != len(ACTIVATION_RELEASE_SLOT_KINDS):
                    reason = "activation_slots_incomplete"
                elif completed_bound_slot_count != len(
                    ACTIVATION_RELEASE_SLOT_KINDS
                ):
                    reason = "activation_canary_executions_incomplete"
                elif pending_inbox:
                    reason = "activation_pending_inbox_not_drained"
                elif unbound_ledger:
                    reason = "activation_unbound_ledger"
                elif inflight_writes:
                    reason = "activation_inflight_writes_not_drained"
                else:
                    reason = "ready"

            result = {
                "epoch_id": epoch_id,
                "state": state,
                "ready": reason == "ready",
                "reason": reason,
                "required_slot_count": len(ACTIVATION_RELEASE_SLOT_KINDS),
                "consumed_slot_count": consumed_slot_count,
                "completed_bound_slot_count": completed_bound_slot_count,
                "pending_inbox": pending_inbox,
                "unbound_ledger": unbound_ledger,
                "inflight_writes": inflight_writes,
            }
            conn.commit()
            return result
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
                """
                SELECT MIN(COALESCE(retry_window_started_at, created_at))
                  FROM rca_outbox
                 WHERE status IN ('pending', 'claimed')
                """
            ).fetchone()[0]
            expired_leases = int(
                conn.execute(
                    """
                    SELECT COUNT(*) FROM rca_outbox
                     WHERE status = 'claimed' AND lease_expires_at <= ?
                    """,
                    (_now_iso(),),
                ).fetchone()[0]
            )
            circuit_row = conn.execute(
                """
                SELECT state, reason_code, reason_detail, opened_at, updated_at
                  FROM rca_dispatcher_circuit WHERE circuit_name = 'submission'
                """
            ).fetchone()
            promotion_counts = {
                row["outcome"]: int(row["count"])
                for row in conn.execute(
                    """
                    SELECT outcome, COUNT(*) AS count
                      FROM rca_shadow_promotion_audit GROUP BY outcome
                    """
                ).fetchall()
            }
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
            current_epoch = self._current_activation_epoch_tx(conn)
            capacity_transition_row = self._capacity_transition_state_tx(conn)
            capacity_transition_audit_count = int(
                conn.execute(
                    "SELECT COUNT(*) FROM rca_capacity_transition_audit"
                ).fetchone()[0]
            )
            capacity_transition_integrity_error = (
                self._capacity_transition_integrity_error_tx(conn)
            )
            capacity_transition_integrity_ok = not bool(
                capacity_transition_integrity_error
            )
            activation_slots = {
                slot_kind: {"authorized": False, "consumed": False}
                for slot_kind in ACTIVATION_SLOT_KINDS
            }
            activation_ledger = {
                "admit": 0,
                "join": 0,
                "shadow": 0,
                "reject": 0,
            }
            activation_backlog = {
                "current_admitted": 0,
                "current_held": 0,
                "unadjudicated_shadow": 0,
                "historical_blocked": 0,
                "historical_held": 0,
                "deferred_quarantined": 0,
                "pending_inbox": int(inbox.get("pending", 0)),
                "unbound_ledger": 0,
                "historical_unbound_ledger": 0,
            }
            activation_current = None
            bounded_canaries_completed_count = 0
            historical_hold_count = 0
            if current_epoch is not None:
                epoch_id = str(current_epoch["epoch_id"])
                activation_current = self._public_activation_epoch(current_epoch)
                historical_hold = conn.execute(
                    "SELECT cohort_count "
                    "FROM rca_activation_historical_outbox_holds "
                    "WHERE epoch_id = ? AND NOT EXISTS ("
                    "SELECT 1 FROM rca_activation_historical_outbox_dispositions "
                    "WHERE epoch_id = ?)",
                    (epoch_id, epoch_id),
                ).fetchone()
                historical_hold_count = (
                    int(historical_hold["cohort_count"])
                    if historical_hold is not None
                    else 0
                )
                for slot_row in conn.execute(
                    """
                    SELECT slot_kind, authorized_identity_sha256,
                           consumed_ledger_id
                      FROM rca_activation_budget_slots WHERE epoch_id = ?
                    """,
                    (epoch_id,),
                ).fetchall():
                    activation_slots[str(slot_row["slot_kind"])] = {
                        "authorized": bool(
                            str(slot_row["authorized_identity_sha256"] or "")
                        ),
                        "consumed": slot_row["consumed_ledger_id"] is not None,
                    }
                for ledger_row in conn.execute(
                    """
                    SELECT decision, COUNT(*) AS count
                      FROM rca_activation_admission_ledger
                     WHERE epoch_id = ? GROUP BY decision
                    """,
                    (epoch_id,),
                ).fetchall():
                    activation_ledger[str(ledger_row["decision"])] = int(
                        ledger_row["count"]
                    )
                bounded_canaries_completed_count = (
                    self._activation_completed_bound_slot_count_tx(
                        conn,
                        epoch_id=epoch_id,
                    )
                )
                activation_backlog["current_admitted"] = int(
                    conn.execute(
                        """
                        SELECT COUNT(*) FROM rca_outbox AS o
                         WHERE o.activation_epoch_id = ?
                           AND o.status IN ('pending', 'claimed')
                           AND EXISTS (
                               SELECT 1
                                 FROM rca_activation_admission_ledger AS al
                                WHERE al.ledger_id = o.activation_ledger_id
                                  AND al.epoch_id = o.activation_epoch_id
                                  AND al.decision = 'admit'
                                  AND al.business_key = o.business_key
                                  AND al.submission_key = o.submission_key
                                  AND al.generation = o.generation
                           )
                        """,
                        (epoch_id,),
                    ).fetchone()[0]
                )
                activation_backlog["current_held"] = int(
                    conn.execute(
                        """
                        SELECT COUNT(*) FROM rca_outbox AS o
                         WHERE o.activation_epoch_id = ? AND o.status = 'shadow'
                           AND EXISTS (
                               SELECT 1
                                 FROM rca_activation_admission_ledger AS al
                                WHERE al.ledger_id = o.activation_ledger_id
                                  AND al.epoch_id = o.activation_epoch_id
                                  AND al.decision IN ('shadow', 'reject')
                           )
                        """,
                        (epoch_id,),
                    ).fetchone()[0]
                )
                activation_backlog["historical_blocked"] = int(
                    conn.execute(
                        """
                        SELECT COUNT(*) FROM rca_outbox AS o
                         WHERE o.status IN ('pending', 'claimed')
                           AND NOT EXISTS (
                               SELECT 1
                                 FROM rca_activation_historical_outbox_hold_items AS held
                                WHERE held.epoch_id = ?
                                  AND held.outbox_id = o.outbox_id
                           )
                           AND (
                               o.activation_epoch_id IS NULL
                               OR o.activation_epoch_id != ?
                               OR o.activation_ledger_id IS NULL
                               OR NOT EXISTS (
                                   SELECT 1
                                     FROM rca_activation_admission_ledger AS al
                                    WHERE al.ledger_id = o.activation_ledger_id
                                      AND al.epoch_id = o.activation_epoch_id
                                      AND al.decision = 'admit'
                               )
                           )
                        """,
                        (epoch_id, epoch_id),
                    ).fetchone()[0]
                )
                activation_backlog["historical_held"] = (
                    int(
                        conn.execute(
                            """
                        SELECT COUNT(*) FROM rca_outbox AS o
                         WHERE o.status = 'shadow'
                           AND (
                               o.activation_epoch_id IS NULL
                               OR o.activation_epoch_id != ?
                           )
                        """,
                            (epoch_id,),
                        ).fetchone()[0]
                    )
                    + historical_hold_count
                )
            else:
                activation_backlog["historical_blocked"] = int(
                    conn.execute(
                        """
                        SELECT COUNT(*) FROM rca_outbox
                         WHERE status IN ('pending', 'claimed')
                        """
                    ).fetchone()[0]
                )
                activation_backlog["historical_held"] = int(
                    conn.execute(
                        "SELECT COUNT(*) FROM rca_outbox WHERE status = 'shadow'"
                    ).fetchone()[0]
                )
            activation_backlog["unadjudicated_shadow"] = int(
                conn.execute(
                    """
                    SELECT COUNT(*) FROM rca_outbox AS o
                     WHERE o.status = 'shadow'
                       AND (
                           o.activation_ledger_id IS NULL
                           OR NOT EXISTS (
                               SELECT 1
                                 FROM rca_activation_admission_ledger AS al
                                WHERE al.ledger_id = o.activation_ledger_id
                                  AND al.epoch_id = o.activation_epoch_id
                                  AND al.business_key = o.business_key
                                  AND al.submission_key = o.submission_key
                                  AND al.generation = o.generation
                           )
                       )
                    """
                ).fetchone()[0]
            )
            activation_backlog["deferred_quarantined"] = int(
                conn.execute(
                    """
                    SELECT COUNT(*) FROM rca_outbox
                     WHERE status = 'quarantined'
                       AND last_error_code = 'activation_epoch_deferred'
                    """
                ).fetchone()[0]
            )
            def unbound_ledger_count(
                epoch_predicate: str, parameters: tuple[Any, ...]
            ) -> int:
                return int(
                    conn.execute(
                        f"""
                    SELECT COUNT(*)
                      FROM rca_activation_admission_ledger AS al
                     WHERE {epoch_predicate}
                       AND al.decision IN ('admit', 'shadow')
                       AND (
                           al.bound_at IS NULL
                        OR NOT EXISTS (
                            SELECT 1 FROM business_triggers AS t
                             WHERE t.activation_epoch_id = al.epoch_id
                               AND t.activation_ledger_id = al.ledger_id
                               AND t.business_key = al.business_key
                               AND t.submission_key = al.submission_key
                               AND t.generation = al.generation
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
                        parameters,
                    ).fetchone()[0]
                )

            if current_epoch is not None:
                current_epoch_id = str(current_epoch["epoch_id"])
                activation_backlog["unbound_ledger"] = unbound_ledger_count(
                    "al.epoch_id = ?", (current_epoch_id,)
                )
                activation_backlog["historical_unbound_ledger"] = (
                    unbound_ledger_count(
                        "al.epoch_id != ?", (current_epoch_id,)
                    )
                )
            else:
                activation_backlog["historical_unbound_ledger"] = (
                    unbound_ledger_count("1", ())
                )
            consumed_slot_count = sum(
                1
                for slot_kind, slot in activation_slots.items()
                if slot_kind in ACTIVATION_RELEASE_SLOT_KINDS and slot["consumed"]
            )
            inflight_writes = self._activation_inflight_writes_tx(conn)
            freeze_state = (
                str(current_epoch["state"])
                if current_epoch is not None
                else "unconfigured"
            )
            freeze_reason = "ready"
            if current_epoch is None:
                freeze_reason = "activation_epoch_unconfigured"
            elif freeze_state != "bounded_active":
                freeze_reason = "activation_epoch_not_bounded"
            elif consumed_slot_count != len(ACTIVATION_RELEASE_SLOT_KINDS):
                freeze_reason = "activation_slots_incomplete"
            elif bounded_canaries_completed_count != len(
                ACTIVATION_RELEASE_SLOT_KINDS
            ):
                freeze_reason = "activation_canary_executions_incomplete"
            elif activation_backlog["pending_inbox"]:
                freeze_reason = "activation_pending_inbox_not_drained"
            elif activation_backlog["unbound_ledger"]:
                freeze_reason = "activation_unbound_ledger"
            elif inflight_writes:
                freeze_reason = "activation_inflight_writes_not_drained"
            ingress_freeze_readiness = {
                "epoch_id": (
                    str(current_epoch["epoch_id"])
                    if current_epoch is not None
                    else ""
                ),
                "state": freeze_state,
                "ready": freeze_reason == "ready",
                "reason": freeze_reason,
                "required_slot_count": len(ACTIVATION_RELEASE_SLOT_KINDS),
                "consumed_slot_count": consumed_slot_count,
                "completed_bound_slot_count": bounded_canaries_completed_count,
                "pending_inbox": activation_backlog["pending_inbox"],
                "unbound_ledger": activation_backlog["unbound_ledger"],
                "inflight_writes": inflight_writes,
            }
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
                stat = os.statvfs(self.db_path.parent)
                filesystem.update(
                    {
                        "total_bytes": int(stat.f_blocks * stat.f_frsize),
                        "free_bytes": int(stat.f_bfree * stat.f_frsize),
                        "available_bytes": int(stat.f_bavail * stat.f_frsize),
                    }
                )
            except OSError as exc:
                filesystem["error"] = f"{type(exc).__name__}: {exc}"
            capacity_ok = (
                not filesystem["error"]
                and isinstance(filesystem["available_bytes"], int)
                and filesystem["available_bytes"] >= CONTROL_DB_MIN_AVAILABLE_BYTES
            )
            return {
                "ok": capacity_ok and capacity_transition_integrity_ok,
                "schema_version": CONTROL_STORE_SCHEMA_VERSION,
                "db_path": str(self.db_path),
                "snapshot_at": snapshot_at,
                "sqlite_data_version": sqlite_data_version,
                "inbox": inbox,
                "outbox": outbox,
                "oldest_pending_received_at": oldest_pending,
                "oldest_dispatchable_created_at": oldest_dispatchable,
                "expired_outbox_leases": expired_leases,
                "dispatcher_circuit": dict(circuit_row) if circuit_row else None,
                "shadow_promotions": promotion_counts,
                "input_wait_rearms": rearm_count,
                "replay_raw_retention_days": REPLAY_RAW_RETENTION.days,
                "replay_raw_retained_count": int(replay_raw["count"]),
                "replay_raw_retained_bytes": int(replay_raw["bytes"]),
                "processed_raw_retention_days": PROCESSED_RAW_RETENTION.days,
                "processed_raw_retained_count": int(processed_raw["count"]),
                "processed_raw_retained_bytes": int(processed_raw["bytes"]),
                "raw_pruned_count": pruned_raw_count,
                "activation": {
                    "configured": current_epoch is not None,
                    "bounded_canaries_completed": (
                        bounded_canaries_completed_count
                        == len(ACTIVATION_RELEASE_SLOT_KINDS)
                    ),
                    "bounded_canaries_completed_count": (
                        bounded_canaries_completed_count
                    ),
                    "ingress_freeze_readiness": ingress_freeze_readiness,
                    "production_active": bool(
                        current_epoch is not None
                        and str(current_epoch["state"]) == "steady_active"
                    ),
                    "current_epoch": activation_current,
                    "slots": activation_slots,
                    "ledger": activation_ledger,
                    "backlog": activation_backlog,
                },
                "capacity_transition": {
                    "configured": capacity_transition_row is not None,
                    "durable_capacity_mode": (
                        "blocked"
                        if not capacity_transition_integrity_ok
                        else (
                            "steady"
                            if capacity_transition_row is not None
                            and str(capacity_transition_row["state"])
                            == CAPACITY_STEADY_ACTIVE
                            else "bootstrap"
                            if capacity_transition_row is not None
                            else None
                        )
                    ),
                    "generation": (
                        int(capacity_transition_row["generation"])
                        if capacity_transition_row is not None
                        else None
                    ),
                    "irreversible": bool(
                        capacity_transition_row is not None
                        and str(capacity_transition_row["state"])
                        == CAPACITY_STEADY_ACTIVE
                    ),
                    "state": (
                        self._public_capacity_transition_state(
                            capacity_transition_row
                        )
                        if capacity_transition_row is not None
                        else None
                    ),
                    "audit_count": capacity_transition_audit_count,
                    "integrity_ok": capacity_transition_integrity_ok,
                    "integrity_error": capacity_transition_integrity_error,
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
