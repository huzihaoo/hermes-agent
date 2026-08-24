"""Durable execution watches and delivery records for Kafka-triggered RCA."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import stat
from typing import Any, Literal, Mapping, Sequence
import uuid

from gateway.pnc_rca_delivery_contract import (
    DELIVERY_CARD_PATCH_EFFECT_KIND,
    DELIVERY_EFFECT_KIND,
    DELIVERY_EFFECT_KINDS,
    DELIVERY_EFFECT_SCHEMA_VERSION,
    DELIVERY_THREAD_EFFECT_KIND,
    RCA_REPORT_FIELD_KEY,
    RCA_RESULT_FIELD_KEY,
    TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION,
    TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION,
    TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION_COMMENT_ONLY,
    TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION_COMMENT_ONLY,
    TERMINAL_DELIVERY_OUTCOMES,
    DeliveryContractError,
    VerifiedDelivery,
    VerifiedTerminalDelivery,
    build_terminal_delivery,
    build_terminal_thread_reply_effect,
    build_thread_reply_effect,
    compute_delivery_effect_key,
    compute_delivery_effect_payload_sha256,
    delivery_effect_marker,
    validate_card_patch_effect_payload,
    validate_delivery_subscription_target,
)
from gateway.pnc_rca_delivery_observability import validate_delivery_observation
from gateway.pnc_rca_business_profiles import (
    G1Q3_KAFKA_SCOPE_ERROR_CODE,
    is_g1q3_kafka_profile_resolution,
)
from gateway.pnc_rca_kafka_contract import G1Q3_KAFKA_POLICY_VERSION
from gateway.pnc_rca_control_store import (
    CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION,
    CONTROL_STORE_SCHEMA_VERSION,
    OUTBOX_CIRCUIT_RESET_SCHEMA_VERSION,
    ActivationEpochError,
    ControlStoreSchemaSnapshot,
    RcaControlStore,
    _validate_dispatcher_circuit_reset_audit,
)
from gateway.pnc_rca_conclusion_adjudication import (
    ADJUDICATION_EFFECT_SCHEMA_VERSION,
    ConclusionAdjudicationError,
    ConclusionAdjudicationResult,
    ConclusionReviewQueueItem,
    ensure_conclusion_adjudication_schema,
    identifies_adjudication_effect,
    list_conclusion_review_queue_tx,
    record_conclusion_adjudication_tx,
    validate_conclusion_adjudication_artifact_receipt,
    validate_adjudication_effect_ledger_binding,
    validate_conclusion_adjudication_schema,
)
from gateway.pnc_rca_runtime_transition import (
    ensure_host_runtime_transition_schema,
    insert_host_runtime_transition,
    validate_host_runtime_transition_schema,
)
from gateway.pnc_rca_failure_route_schema import (
    FAILURE_ROUTE_FOREIGN_KEY_CONTRACT as _FAILURE_ROUTE_FOREIGN_KEY_CONTRACT,
    FAILURE_ROUTE_INDEX_CONTRACT as _FAILURE_ROUTE_INDEX_CONTRACT,
    FAILURE_ROUTE_REQUIRED_CHECKS as _FAILURE_ROUTE_REQUIRED_CHECKS,
    FAILURE_ROUTE_REQUIRED_COLUMNS as _FAILURE_ROUTE_REQUIRED_COLUMNS,
    FAILURE_ROUTE_TABLE_INFO_CONTRACT as _FAILURE_ROUTE_TABLE_INFO_CONTRACT,
    failure_route_schema_errors,
)
from gateway.pnc_rca_write_fence import (
    ExternalWriteFenceError,
    snapshot_core_sha256 as _snapshot_core_sha256,
    validate_write_fence,
    validate_write_fence_source_binding,
)


DELIVERY_STORE_SCHEMA_VERSION = "pnc_rca_delivery_store_v12"
DELIVERY_STORE_SUCCESSOR_READ_ONLY_ERROR = (
    "rca_delivery_store_successor_read_only"
)
W5_EXTERNAL_WRITE_FENCE_CUTOFF_META_KEY = "w5_external_write_fence_cutoff"
# Persisted once at delivery-store initialization; never controlled by env.
W5_EXTERNAL_WRITE_FENCE_CUTOFF = "2026-07-25T00:00:00+00:00"
DELIVERY_STORE_SCHEMA_PREDECESSOR_VERSION = "pnc_rca_delivery_store_v7"
DELIVERY_STORE_W2_SCHEMA_VERSION = "pnc_rca_delivery_store_v8"
DELIVERY_STORE_W6_PREDECESSOR_VERSION = "pnc_rca_delivery_store_v9"
DELIVERY_STORE_OBSERVABILITY_PREDECESSOR_VERSION = "pnc_rca_delivery_store_v10"
DELIVERY_STORE_TERMINAL_RERUN_PREDECESSOR_VERSION = "pnc_rca_delivery_store_v11"
_DELIVERY_OBSERVATION_OUTBOX_SCHEMA_OBJECTS = {
    "rca_delivery_observation_outbox": (
        "table",
        "rca_delivery_observation_outbox",
        "56ac3767f64f26666da68a7eff78e04e81dc94ad49c73ffde2ce7408cf4a7e0e",
    ),
    "idx_delivery_observation_outbox_status": (
        "index",
        "rca_delivery_observation_outbox",
        "d5be40072c9c9edba2fa7b0602daa22d7545c5e68d1e2e49745149a283c50637",
    ),
}
DELIVERY_BACKPRESSURE_SNAPSHOT_SCHEMA_VERSION = (
    "pnc_rca_delivery_backpressure_snapshot_v2"
)
CANONICAL_CANARY_READBACK_SCHEMA_VERSION = "pnc_rca_canonical_canary_readback_v1"
DELIVERY_OUTCOME_SLO_SCHEMA_VERSION = "pnc_rca_delivery_outcome_slo_v2"
DELIVERY_OUTCOME_SLO_SUCCESS_STATUSES = ("delivered", "partial")
DELIVERY_OUTCOME_SLO_FAILURE_STATUSES = ("quarantined",)
DELIVERY_OUTCOME_SLO_WINDOWS = (
    ("5m", 300, 3, 0.5),
    ("15m", 900, 5, 0.4),
    ("60m", 3600, 8, 0.3),
)
DELIVERY_OUTCOME_CONSECUTIVE_WINDOW_SECONDS = 3600
DELIVERY_OUTCOME_CONSECUTIVE_FAILURE_THRESHOLD = 3
OUTBOX_QUARANTINED_TERMINAL_STATE = "submission_quarantined"
OUTBOX_QUARANTINED_PUBLIC_ERROR_CODE = "outbox_submission_quarantined"
OUTBOX_PRE_W3_QUARANTINE_POLICY = "silent_internal_alert_only"
OUTBOX_PRE_W3_QUARANTINE_MISSING_CODE = "w3_execution_snapshot_missing"
OUTBOX_PRE_W3_QUARANTINE_INVALID_CODE = "w3_execution_snapshot_invalid"
OUTBOX_MANUAL_ACTIVATION_BINDING_INVALID_CODE = "manual_activation_binding_invalid"
OUTBOX_PROFILE_TERMINAL_BINDING_INVALID_CODE = (
    "profile_terminal_binding_invalid"
)
OUTBOX_PROFILE_TERMINAL_WRITE_ERROR_CODES = frozenset({
    "business_profile_unsupported",
    "business_profile_conflict",
    "business_profile_adapter_not_ready",
})
OUTBOX_SILENT_PROFILE_TERMINAL_ERROR_CODES = frozenset({
    "business_profile_unsupported",
    "business_profile_conflict",
})
OUTBOX_PUBLIC_PROFILE_ERROR_CODES = frozenset({
    "business_profile_unresolved",
    *OUTBOX_PROFILE_TERMINAL_WRITE_ERROR_CODES,
})
DELIVERY_WATCH_SLA_SECONDS = 86_400
PERMANENT_FAILURE_CIRCUIT_THRESHOLD = 2
_PERMANENT_FAILURE_STREAK_META_KEY = "permanent_failure_streak"
_PERMANENT_FAILURE_LAST_META_KEY = "permanent_failure_last"
DELIVERY_CIRCUIT_RESET_META_PREFIX = "rca_delivery_dispatcher_circuit_reset:"
DELIVERY_CIRCUIT_RESET_SCHEMA_VERSION = OUTBOX_CIRCUIT_RESET_SCHEMA_VERSION
DELIVERY_CIRCUIT_RESET_REQUIRED_FIELDS = frozenset(
    {
        "plan_id",
        "before_state_sha256",
        "destination_binding",
        "circuit_scope",
        "effect_kind",
        "release_binding",
        "tool_provenance",
        "permanent_failure_before",
        "permanent_failure_after",
    }
)
_NON_PIPELINE_QUARANTINE_CODES = frozenset({
    "feishu_work_item_not_found",
    G1Q3_KAFKA_SCOPE_ERROR_CODE,
    *OUTBOX_SILENT_PROFILE_TERMINAL_ERROR_CODES,
})
LEARNING_LANE_EXTERNAL_EFFECT_ERROR = "learning_lane_external_effect_forbidden"
LEARNING_LANE_ADMISSION_MISSING_ERROR = "learning_lane_admission_missing"
_W6_STOCK_CUTOFF = "2026-07-25T10:15:43.473251+00:00"
_TERMINAL_EFFECT_SCHEMA_VERSIONS = frozenset({
    "pnc_rca_terminal_delivery_effect_v1",
    "pnc_rca_terminal_delivery_effect_v2",
    "pnc_rca_terminal_delivery_effect_v3",
    "pnc_rca_terminal_delivery_effect_v4",
    "pnc_rca_terminal_delivery_effect_v5",
})
_ISSUE_OPERATIONS = frozenset({
    DELIVERY_EFFECT_KIND,
    "feishu_issue_field_update",
})


def _terminal_rerun_payload_identity_matches(
    payload: Mapping[str, Any],
    *,
    effect_key: str,
    submission_key: str,
    generation: int,
    expected_payload_sha256: str,
) -> bool:
    schema_version = str(payload.get("schema_version") or "")
    try:
        if schema_version == DELIVERY_EFFECT_SCHEMA_VERSION:
            return (
                str(payload.get("effect_key") or "") == effect_key
                and str(payload.get("semantic_payload_sha256") or "")
                == expected_payload_sha256
                and compute_delivery_effect_payload_sha256(
                    payload, DELIVERY_EFFECT_KIND
                )
                == expected_payload_sha256
            )
        return (
            schema_version in _TERMINAL_EFFECT_SCHEMA_VERSIONS
            and str(payload.get("submission_key") or "") == submission_key
            and int(payload.get("generation") or 0) == generation
            and hashlib.sha256(
                _canonical_json(payload).encode("utf-8")
            ).hexdigest()
            == expected_payload_sha256
        )
    except (DeliveryContractError, TypeError, ValueError):
        return False
_LEARNING_ADJUDICATION_SCHEMAS = frozenset({
    "pnc_rca_conclusion_adjudication_effect_v1",
    "pnc_rca_conclusion_adjudication_effect_v2",
})
_ADJUDICATION_TARGET_PREFIX = "g1q3-rca-adjudication-target-v1-"
_OPEN_ID_RE = re.compile(r"^ou_[0-9a-f]{32}$")
_FEISHU_ISSUE_URL_RE = re.compile(
    r"^https://project\.feishu\.cn/([A-Za-z0-9._-]+)/issue/detail/([0-9]+)/*$"
)
COMMENT_SLOT_SCHEMA_VERSION = "pnc_rca_comment_slot_v1"
COMMENT_SLOT_KINDS = frozenset({"conclusion", "correction"})
ACCIDENT_SAMPLE_BUDGET_EXEMPT_ISSUE_IDS = frozenset(
    {
        "7058246921",
        "7058307180",
        "7058335096",
        "7058336194",
        "7058457524",
        "7058462331",
        "7058500122",
        "7058503076",
        "7058537483",
    }
)
SUPPORTED_DELIVERY_STORE_SCHEMA_VERSIONS = frozenset({
    "pnc_rca_delivery_store_v1",
    "pnc_rca_delivery_store_v2",
    "pnc_rca_delivery_store_v3",
    "pnc_rca_delivery_store_v4",
    "pnc_rca_delivery_store_v5",
    "pnc_rca_delivery_store_v6",
    DELIVERY_STORE_SCHEMA_PREDECESSOR_VERSION,
    DELIVERY_STORE_W2_SCHEMA_VERSION,
    DELIVERY_STORE_W6_PREDECESSOR_VERSION,
    DELIVERY_STORE_OBSERVABILITY_PREDECESSOR_VERSION,
    DELIVERY_STORE_TERMINAL_RERUN_PREDECESSOR_VERSION,
    DELIVERY_STORE_SCHEMA_VERSION,
})
WATCH_ACTIVE_STATES = frozenset({"pending", "running"})
WATCH_TERMINAL_STATES = frozenset({
    "terminal_failed",
    "quarantined",
    "delivery_created",
})
DELIVERY_EFFECT_STATES = frozenset({
    "pending",
    "claimed",
    "retry_wait",
    "uncertain",
    "succeeded",
    "quarantined",
    "suppressed",
})
DELIVERY_EFFECT_UNRESOLVED_STATES = (
    "pending",
    "claimed",
    "retry_wait",
    "uncertain",
)
DELIVERY_WATCH_STATES = WATCH_ACTIVE_STATES | WATCH_TERMINAL_STATES
REQUIRED_DELIVERY_EFFECT_KINDS = tuple(sorted(DELIVERY_EFFECT_KINDS))
ACTIVATION_DELIVERY_STATES = frozenset({"steady_active"})
_ACTIVATION_REQUIRED_TABLES = frozenset({
    "rca_activation_epochs",
    "rca_activation_admission_ledger",
    "rca_activation_transition_audit",
    "business_triggers",
    "rca_outbox",
})
_ACTIVATION_EXECUTION_ELIGIBLE_SQL = """
EXISTS (
    SELECT 1
      FROM rca_activation_epochs AS activation_epoch
      JOIN rca_activation_admission_ledger AS activation_ledger
        ON activation_ledger.epoch_id = activation_epoch.epoch_id
       AND activation_ledger.ledger_id = o.activation_ledger_id
     WHERE activation_epoch.is_current = 1
       AND activation_epoch.state = 'steady_active'
       AND o.activation_epoch_id = activation_epoch.epoch_id
       AND t.activation_epoch_id = activation_epoch.epoch_id
       AND t.activation_ledger_id = o.activation_ledger_id
       AND activation_ledger.decision = 'admit'
       AND activation_ledger.bound_at IS NOT NULL
       AND activation_ledger.business_key = o.business_key
       AND activation_ledger.submission_key = o.submission_key
       AND activation_ledger.generation = o.generation
)
"""
_ACTIVATION_CURRENT_BINDING_SQL = """
EXISTS (
    SELECT 1
      FROM rca_activation_epochs AS activation_epoch
     WHERE activation_epoch.is_current = 1
       AND o.activation_epoch_id = activation_epoch.epoch_id
       AND t.activation_epoch_id = activation_epoch.epoch_id
       AND o.activation_ledger_id IS NOT NULL
       AND t.activation_ledger_id = o.activation_ledger_id
)
"""
_ADJUDICATION_ACTIVATION_ELIGIBLE_SQL = """
EXISTS (
    SELECT 1
      FROM rca_conclusion_adjudications AS adjudication
      JOIN rca_activation_epochs AS adjudication_epoch
        ON adjudication_epoch.epoch_id = adjudication.activation_epoch_id
       AND adjudication_epoch.is_current = 1
     WHERE adjudication.correction_effect_key = e.effect_key
       AND adjudication_epoch.state = 'steady_active'
)
"""
_CARD_PATCH_ACTIVATION_ELIGIBLE_SQL = """
EXISTS (
    SELECT 1
      FROM rca_conclusion_adjudications AS card_adjudication
      JOIN rca_activation_epochs AS card_epoch
        ON card_epoch.epoch_id = card_adjudication.activation_epoch_id
      JOIN rca_delivery_effects AS card_correction
        ON card_correction.effect_key = card_adjudication.correction_effect_key
     WHERE e.effect_kind = 'feishu_card_patch'
       AND card_adjudication.original_delivery_id = e.delivery_id
       AND card_correction.status = 'succeeded'
       AND card_correction.write_phase = 'settled'
       AND (
            (
                card_epoch.is_current = 1
                AND card_epoch.state = 'steady_active'
            )
            OR e.write_phase = 'write_started'
       )
)
"""


class StaleDeliveryWatchLeaseError(RuntimeError):
    """A watch mutation used an expired or superseded fencing token."""


class DeliveryRecordConflictError(RuntimeError):
    """A submission was already bound to different immutable delivery bytes."""


class StaleDeliveryEffectLeaseError(RuntimeError):
    """An effect mutation used an expired or superseded fencing token."""


@dataclass(frozen=True)
class ExecutionWatchClaim:
    submission_key: str
    submission_outbox_id: int
    business_key: str
    generation: int
    project_key: str
    work_item_type_key: str
    work_item_id: str
    task_id: str
    state: str
    poll_attempt: int
    fence: int
    lease_token: str
    lease_owner: str
    lease_expires_at: str
    work_started_at: str
    terminal_first_seen_at: str | None
    submission_payload: dict[str, Any]
    submission_result: dict[str, Any]
    origin_source_id: str = ""
    trigger_origin_source_id: str = ""


@dataclass(frozen=True)
class DeliveryCreateResult:
    delivery_id: str
    effect_key: str
    created: bool


@dataclass(frozen=True)
class FailureRouteMutation:
    route_key: str
    created: bool
    status: str
    owner: str
    remediation_attempt_count: int


@dataclass(frozen=True)
class SubscriptionMaterializationResult:
    materialized: int = 0
    quarantined: int = 0


@dataclass(frozen=True)
class DeliveryEffectClaim:
    effect_key: str
    delivery_id: str
    effect_kind: str
    required: bool
    target_key: str
    payload: dict[str, Any]
    payload_sha256: str
    previous_status: str
    attempt: int
    fence: int
    request_id: str
    lease_token: str
    lease_owner: str
    lease_expires_at: str
    effect_created_at: str
    business_accepted_at: str
    artifact_set_id: str
    project_key: str
    work_item_type_key: str
    work_item_id: str
    issue_url: str
    report_url: str
    manifest: dict[str, Any]
    artifacts: list[dict[str, Any]]
    contract: dict[str, Any]
    write_phase: str = "prewrite"
    write_started_at: str | None = None
    reconciliation_miss_count: int = 0
    recovery_write_count: int = 0
    last_recovery_write_at: str | None = None
    outcome: str = "success"
    terminal_state: str = ""
    terminal_error_code: str = ""
    outcome_key: str = ""
    business_key: str = ""
    submission_key: str = ""
    generation: int = 0
    adjudication_comment_attempt_count: int = 0
    adjudication_comment_attempted_at: str | None = None


@dataclass(frozen=True)
class DeliveryObservationIntent:
    observation_id: str
    effect_key: str
    payload: dict[str, Any]
    payload_sha256: str
    created_at: str
    status: str = "pending"


@dataclass(frozen=True)
class DeliveryEffectMutation:
    effect_key: str
    delivery_id: str
    effect_status: str
    job_status: str
    next_attempt_at: str | None = None


@dataclass(frozen=True)
class DeliveryReconciliationState:
    missing_read_count: int
    recovery_write_count: int
    visibility_grace_elapsed: bool
    recovery_interval_elapsed: bool
    recovery_eligible: bool
    recovery_limit_exceeded: bool


@dataclass(frozen=True)
class DeliveryDispatcherCircuit:
    state: str
    reason_code: str = ""
    reason_detail: str = ""
    opened_at: str | None = None
    updated_at: str = ""

    @property
    def is_open(self) -> bool:
        return self.state == "open"


@dataclass(frozen=True)
class DeliveryBackpressureSnapshot:
    schema_version: str
    observed_at: str
    db_path: str
    pending: int
    claimed: int
    retry_wait: int
    uncertain: int
    unresolved_effects: int
    untracked_completed_submissions: int
    pending_watches: int
    running_watches: int
    unresolved_work: int
    outcome_slo: dict[str, Any]
    circuit: DeliveryDispatcherCircuit
    circuits: dict[str, DeliveryDispatcherCircuit]

    def public_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "observed_at": self.observed_at,
            "db_path": self.db_path,
            "effect_counts": {
                "pending": self.pending,
                "claimed": self.claimed,
                "retry_wait": self.retry_wait,
                "uncertain": self.uncertain,
            },
            "unresolved_effects": self.unresolved_effects,
            "pipeline_counts": {
                "untracked_completed_submissions": (
                    self.untracked_completed_submissions
                ),
                "pending_watches": self.pending_watches,
                "running_watches": self.running_watches,
            },
            "unresolved_work": self.unresolved_work,
            "delivery_outcome_slo": self.outcome_slo,
            "delivery_dispatcher_circuit": {
                "state": self.circuit.state,
                "reason_code": self.circuit.reason_code,
                "reason_detail": self.circuit.reason_detail,
                "opened_at": self.circuit.opened_at,
                "updated_at": self.circuit.updated_at,
            },
            "delivery_dispatcher_circuits": {
                name: {
                    "state": item.state,
                    "reason_code": item.reason_code,
                    "reason_detail": item.reason_detail,
                    "opened_at": item.opened_at,
                    "updated_at": item.updated_at,
                }
                for name, item in sorted(self.circuits.items())
            },
        }


def _utc_datetime(value: datetime | None = None) -> datetime:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None:
        raise ValueError("datetime values must be timezone-aware")
    return current.astimezone(timezone.utc)


def _iso(value: datetime | None = None) -> str:
    return _utc_datetime(value).isoformat()


def _parse_iso(value: str) -> datetime:
    parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("stored datetime must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _reject_delivery_circuit_reset_json_constant(_value: str) -> None:
    raise ValueError("delivery_circuit_reset_non_finite_json")


def _validate_delivery_circuit_reset_audit(
    value: Mapping[str, Any],
) -> dict[str, Any]:
    normalized = _validate_dispatcher_circuit_reset_audit(value)
    if not DELIVERY_CIRCUIT_RESET_REQUIRED_FIELDS.issubset(normalized):
        raise ValueError("delivery_circuit_reset_audit_fields_invalid")
    if (
        re.fullmatch(r"[0-9a-f]{64}", str(normalized.get("plan_id") or "")) is None
        or re.fullmatch(
            r"[0-9a-f]{64}", str(normalized.get("before_state_sha256") or "")
        )
        is None
        or normalized.get("before_state_sha256")
        != hashlib.sha256(_canonical_json(normalized["before"]).encode()).hexdigest()
    ):
        raise ValueError("delivery_circuit_reset_plan_binding_invalid")
    destination = normalized.get("destination_binding")
    if (
        not isinstance(destination, Mapping)
        or set(destination) != {"path_sha256", "parent_device", "parent_inode"}
        or re.fullmatch(
            r"[0-9a-f]{64}", str(destination.get("path_sha256") or "")
        )
        is None
        or isinstance(destination.get("parent_device"), bool)
        or not isinstance(destination.get("parent_device"), int)
        or destination.get("parent_device") < 0
        or isinstance(destination.get("parent_inode"), bool)
        or not isinstance(destination.get("parent_inode"), int)
        or destination.get("parent_inode") < 0
    ):
        raise ValueError("delivery_circuit_reset_destination_binding_invalid")
    if (
        normalized.get("circuit_scope") != "delivery"
        or normalized.get("effect_kind") not in DELIVERY_EFFECT_KINDS
    ):
        raise ValueError("delivery_circuit_reset_audit_schema_invalid")
    release = normalized.get("release_binding")
    if (
        not isinstance(release, Mapping)
        or re.fullmatch(
            r"[0-9a-f]{64}", str(release.get("release_fingerprint_sha256") or "")
        )
        is None
        or re.fullmatch(
            r"[0-9a-f]{64}", str(release.get("release_note_sha256") or "")
        )
        is None
        or not str(release.get("release_id") or "").strip()
        or not str(release.get("epoch_id") or "").strip()
        or not isinstance(release.get("release_note_path"), str)
        or not Path(release["release_note_path"]).is_absolute()
    ):
        raise ValueError("delivery_circuit_reset_release_binding_invalid")
    provenance = normalized.get("tool_provenance")
    if not isinstance(provenance, Mapping) or any(
        not isinstance(provenance.get(path_name), str)
        or not Path(provenance[path_name]).is_absolute()
        or re.fullmatch(r"[0-9a-f]{64}", str(provenance.get(sha_name) or ""))
        is None
        for path_name, sha_name in (
            ("entrypoint_path", "entrypoint_sha256"),
            ("delivery_store_path", "delivery_store_sha256"),
            ("receipt_helper_path", "receipt_helper_sha256"),
            ("control_store_path", "control_store_sha256"),
        )
    ):
        raise ValueError("delivery_circuit_reset_tool_provenance_invalid")
    before_failure = normalized.get("permanent_failure_before")
    after_failure = normalized.get("permanent_failure_after")
    if (
        not isinstance(before_failure, Mapping)
        or before_failure.get("threshold") != PERMANENT_FAILURE_CIRCUIT_THRESHOLD
        or isinstance(before_failure.get("consecutive_failures"), bool)
        or not isinstance(before_failure.get("consecutive_failures"), int)
        or type(before_failure.get("last_failure_present")) is not bool
        or not isinstance(before_failure.get("last_failure"), Mapping)
        or after_failure
        != {
            "threshold": PERMANENT_FAILURE_CIRCUIT_THRESHOLD,
            "consecutive_failures": 0,
            "last_failure": {},
            "last_failure_present": False,
        }
    ):
        raise ValueError("delivery_circuit_reset_failure_state_invalid")
    effect_delta = normalized.get("effect_delta")
    expected_total = 3 + int(before_failure["last_failure_present"])
    if (
        not isinstance(effect_delta, Mapping)
        or effect_delta.get("external_effects_triggered") is not False
        or effect_delta.get("delivery_effect_rows") != 0
        or effect_delta.get("database_rows")
        != {
            "circuit_updated": 1,
            "control_meta_inserted": 1,
            "permanent_failure_streak_upserted": 1,
            "permanent_failure_last_deleted": expected_total - 3,
            "total": expected_total,
        }
    ):
        raise ValueError("delivery_circuit_reset_effect_delta_invalid")
    return normalized


def _execute_schema_script_in_transaction(
    conn: sqlite3.Connection,
    script: str,
) -> None:
    """Execute complete DDL statements without sqlite3's implicit COMMIT."""

    pending = ""
    for line in script.splitlines(keepends=True):
        pending += line
        if sqlite3.complete_statement(pending):
            statement = pending.strip()
            if statement:
                conn.execute(statement)
            pending = ""
    if pending.strip():
        raise RuntimeError("incomplete_delivery_store_schema_script")


def _json_object(value: Any) -> dict[str, Any]:
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _card_patch_success_receipt(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "remote_id": str(payload.get("message_id") or ""),
        "source": "relay_card_patch",
        "render_hash": str(payload.get("render_hash") or ""),
        "adjudication_id": str(payload.get("adjudication_id") or ""),
        "conclusion_state": str(payload.get("conclusion_state") or ""),
        "correction_effect_key": str(payload.get("correction_effect_key") or ""),
    }


def _card_patch_suppression_receipt(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "source": "card_message_expired",
        "message_id": str(payload.get("message_id") or ""),
        "render_hash": str(payload.get("render_hash") or ""),
        "adjudication_id": str(payload.get("adjudication_id") or ""),
        "conclusion_state": str(payload.get("conclusion_state") or ""),
        "correction_effect_key": str(payload.get("correction_effect_key") or ""),
        "error_code": "feishu_card_patch_message_expired",
    }


def validate_delivery_outcome_slo(
    value: Mapping[str, Any], *, expected_observed_at: str | None = None
) -> bool:
    """Validate the fixed outcome-SLO policy and all derived counters."""
    expected_keys = {
        "schema_version",
        "observed_at",
        "success_delivery_statuses",
        "failure_delivery_statuses",
        "windows",
        "consecutive_failure_window_seconds",
        "consecutive_failure_threshold",
        "consecutive_failure_count",
        "consecutive_failure_breached",
        "contract_valid",
        "healthy",
    }
    if set(value) != expected_keys:
        raise ValueError("delivery outcome SLO fields are invalid")
    if value.get("schema_version") != DELIVERY_OUTCOME_SLO_SCHEMA_VERSION:
        raise ValueError("delivery outcome SLO schema is invalid")
    observed_at = str(value.get("observed_at") or "")
    _parse_iso(observed_at)
    if expected_observed_at is not None and observed_at != expected_observed_at:
        raise ValueError("delivery outcome SLO observation is inconsistent")
    if value.get("success_delivery_statuses") != sorted(
        DELIVERY_OUTCOME_SLO_SUCCESS_STATUSES
    ):
        raise ValueError("delivery outcome SLO success statuses are invalid")
    if value.get("failure_delivery_statuses") != sorted(
        DELIVERY_OUTCOME_SLO_FAILURE_STATUSES
    ):
        raise ValueError("delivery outcome SLO failure statuses are invalid")
    windows = value.get("windows")
    if not isinstance(windows, Mapping) or set(windows) != {
        name for name, *_policy in DELIVERY_OUTCOME_SLO_WINDOWS
    }:
        raise ValueError("delivery outcome SLO windows are invalid")
    any_window_breached = False
    window_keys = {
        "window_seconds",
        "min_samples",
        "max_failure_rate",
        "sample_count",
        "failure_count",
        "failure_rate",
        "breached",
    }
    for name, window_seconds, min_samples, max_failure_rate in DELIVERY_OUTCOME_SLO_WINDOWS:
        window = windows.get(name)
        if not isinstance(window, Mapping) or set(window) != window_keys:
            raise ValueError("delivery outcome SLO window contract is invalid")
        integer_values = (
            window.get("window_seconds"),
            window.get("min_samples"),
            window.get("sample_count"),
            window.get("failure_count"),
        )
        if any(type(item) is not int or item < 0 for item in integer_values):
            raise ValueError("delivery outcome SLO counters are invalid")
        if (
            window["window_seconds"] != window_seconds
            or window["min_samples"] != min_samples
            or isinstance(window.get("max_failure_rate"), bool)
            or float(window.get("max_failure_rate", -1)) != max_failure_rate
            or window["failure_count"] > window["sample_count"]
        ):
            raise ValueError("delivery outcome SLO policy is invalid")
        expected_rate = (
            window["failure_count"] / window["sample_count"]
            if window["sample_count"]
            else 0.0
        )
        if (
            isinstance(window.get("failure_rate"), bool)
            or abs(float(window.get("failure_rate", -1)) - expected_rate) > 1e-12
        ):
            raise ValueError("delivery outcome SLO failure rate is invalid")
        expected_breached = (
            window["sample_count"] >= min_samples and expected_rate > max_failure_rate
        )
        if type(window.get("breached")) is not bool or (
            window["breached"] is not expected_breached
        ):
            raise ValueError("delivery outcome SLO breach state is invalid")
        any_window_breached = any_window_breached or expected_breached
    count = value.get("consecutive_failure_count")
    if type(count) is not int or count < 0:
        raise ValueError("delivery outcome consecutive failure count is invalid")
    if (
        value.get("consecutive_failure_window_seconds")
        != DELIVERY_OUTCOME_CONSECUTIVE_WINDOW_SECONDS
        or value.get("consecutive_failure_threshold")
        != DELIVERY_OUTCOME_CONSECUTIVE_FAILURE_THRESHOLD
    ):
        raise ValueError("delivery outcome consecutive failure policy is invalid")
    consecutive_breached = count >= DELIVERY_OUTCOME_CONSECUTIVE_FAILURE_THRESHOLD
    if type(value.get("consecutive_failure_breached")) is not bool or (
        value["consecutive_failure_breached"] is not consecutive_breached
    ):
        raise ValueError("delivery outcome consecutive breach state is invalid")
    if type(value.get("contract_valid")) is not bool:
        raise ValueError("delivery outcome contract validity is invalid")
    expected_healthy = (
        value["contract_valid"] and not any_window_breached and not consecutive_breached
    )
    if type(value.get("healthy")) is not bool or value["healthy"] is not expected_healthy:
        raise ValueError("delivery outcome SLO health is invalid")
    return expected_healthy


class RcaDeliveryStore:
    """Own delivery-only tables in the existing RCA control SQLite database."""

    def __init__(
        self,
        db_path: str | Path,
        *,
        busy_timeout_ms: int = 5000,
        require_current: bool = False,
        read_only: bool = False,
        ensure_current_rows: bool = True,
        allow_successor_read_only: bool = False,
        allow_successor_write: bool = False,
    ):
        self.db_path = Path(db_path).expanduser()
        if not isinstance(require_current, bool):
            raise TypeError("require_current must be true or false")
        if not isinstance(read_only, bool):
            raise TypeError("read_only must be true or false")
        if not isinstance(ensure_current_rows, bool):
            raise TypeError("ensure_current_rows must be true or false")
        if not isinstance(allow_successor_read_only, bool):
            raise TypeError("allow_successor_read_only must be true or false")
        if not isinstance(allow_successor_write, bool):
            raise TypeError("allow_successor_write must be true or false")
        if read_only and not require_current:
            raise ValueError("read_only delivery store requires current schema")
        if allow_successor_read_only and not require_current:
            raise ValueError(
                "successor read-only delivery store requires current schema"
            )
        if allow_successor_write and not require_current:
            raise ValueError("successor-write delivery store requires current schema")
        if allow_successor_write and (read_only or allow_successor_read_only):
            raise ValueError(
                "successor-write delivery store cannot also be read-only"
            )
        self.require_current = require_current
        self.requested_read_only = read_only
        self.read_only = read_only
        self.ensure_current_rows = ensure_current_rows
        self.allow_successor_read_only = allow_successor_read_only
        self.allow_successor_write = allow_successor_write
        self._binary_write_schema_version = CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION
        self._connection_write_schema_version = (
            CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION
            if allow_successor_write
            else CONTROL_STORE_SCHEMA_VERSION
        )
        self._successor_read_only = False
        self._enforce_binary_write_schema = False
        self._schema_probe_snapshot: ControlStoreSchemaSnapshot | None = None
        self._observed_delivery_schema_version: str | None = None
        self._observed_control_schema_version: str | None = None
        if require_current:
            self._validate_runtime_fences()
            self._validate_existing_path()
        else:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.busy_timeout_ms = max(1, int(busy_timeout_ms))
        source_snapshot_verified = False
        try:
            if self.db_path.is_file() and self.db_path.stat().st_size > 0:
                if self.requested_read_only:
                    self._schema_probe_snapshot = (
                        RcaControlStore.create_schema_probe_snapshot(
                            self.db_path,
                            allow_successor_read_only=True,
                        )
                    )
                    source_control_schema_version = (
                        self._schema_probe_snapshot.schema_version
                    )
                else:
                    (
                        source_control_schema_version,
                        self._schema_probe_snapshot,
                    ) = RcaControlStore.probe_writable_schema_source(
                        self.db_path,
                        expected_write_schema_version=(
                            self._connection_write_schema_version
                            if self.allow_successor_write
                            else None
                        ),
                    )
                if (
                    source_control_schema_version
                    == CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION
                    and not (
                        require_current
                        and (allow_successor_read_only or allow_successor_write)
                    )
                ):
                    raise RuntimeError(
                        "rca_delivery_store_control_schema_not_current"
                    )
                if (
                    source_control_schema_version
                    == CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION
                    and self._schema_probe_snapshot is None
                    and not self.allow_successor_write
                ):
                    self._schema_probe_snapshot = (
                        RcaControlStore.create_schema_probe_snapshot(
                            self.db_path,
                            allow_successor_read_only=True,
                        )
                    )
                    if (
                        self._schema_probe_snapshot.schema_version
                        != CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION
                    ):
                        raise RuntimeError(
                            "rca_delivery_store_control_schema_source_changed"
                        )
                if self.allow_successor_write:
                    self.read_only = False
            elif self.allow_successor_write:
                raise RuntimeError(
                    "rca_delivery_store_successor_write_schema_required"
                )
            self._initialize()
            if not require_current:
                self._observed_delivery_schema_version = (
                    self._preflight_schema_version()
                )
                self._observed_control_schema_version = (
                    self._preflight_control_schema_version()
                )
            if self._schema_probe_snapshot is not None:
                RcaControlStore._verify_schema_probe_source_unchanged(
                    self._schema_probe_snapshot
                )
                source_snapshot_verified = True
            if not self.read_only:
                self._discard_schema_probe_snapshot()
        except Exception:
            self._discard_schema_probe_snapshot()
            raise
        self._enforce_binary_write_schema = True
        if (
            not self.read_only
            and not source_snapshot_verified
            and self._observed_control_schema_version
            == self._connection_write_schema_version
        ):
            conn = self._connect_read_only()
            conn.close()
        if require_current:
            self._validate_runtime_fences()

    def _discard_schema_probe_snapshot(self) -> None:
        if self._schema_probe_snapshot is not None:
            self._schema_probe_snapshot.close()
        self._schema_probe_snapshot = None

    @property
    def _read_db_path(self) -> Path:
        if self.read_only:
            if self._schema_probe_snapshot is None:
                raise RuntimeError("rca_delivery_store_read_only_snapshot_missing")
            return self._schema_probe_snapshot.db_path
        return self.db_path

    @property
    def _schema_probe_db_path(self) -> Path:
        if self._schema_probe_snapshot is not None:
            return self._schema_probe_snapshot.db_path
        return self.db_path

    def schema_runtime_capability(self) -> dict[str, Any]:
        """Report the exact schema mode this binary may use."""

        control_schema_version = str(
            self._observed_control_schema_version or ""
        )
        delivery_schema_version = str(
            self._observed_delivery_schema_version or ""
        )
        read_supported = (
            delivery_schema_version == DELIVERY_STORE_SCHEMA_VERSION
            and control_schema_version
            in {
                CONTROL_STORE_SCHEMA_VERSION,
                CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION,
            }
        )
        write_enabled = (
            read_supported
            and control_schema_version == self._connection_write_schema_version
            and not self.read_only
        )
        if self._successor_read_only:
            mode = "successor_read_only"
        elif self.read_only:
            mode = "explicit_read_only"
        else:
            mode = "current_write"
        return {
            "observed_control_schema_version": control_schema_version,
            "binary_write_schema_version": self._binary_write_schema_version,
            "mode": mode,
            "read_supported": read_supported,
            "write_enabled": write_enabled,
            "work_admission_enabled": write_enabled,
            "lease_acquisition_enabled": write_enabled,
            "external_effect_enabled": write_enabled,
        }

    def _validate_runtime_fences(self) -> None:
        for suffix in (".pnc-rca-maintenance", ".pnc-rca-tombstone"):
            marker = Path(f"{self.db_path}{suffix}")
            try:
                observed = marker.lstat()
            except FileNotFoundError:
                continue
            except OSError as exc:
                raise RuntimeError("rca_delivery_store_runtime_fence_invalid") from exc
            if stat.S_ISLNK(observed.st_mode) or not stat.S_ISREG(observed.st_mode):
                raise RuntimeError("rca_delivery_store_runtime_fence_invalid")
            raise RuntimeError("rca_delivery_store_runtime_fenced")

    def _validate_existing_path(self) -> None:
        if not self.db_path.is_absolute():
            raise RuntimeError("rca_delivery_store_existing_path_not_absolute")
        try:
            observed = self.db_path.lstat()
        except OSError as exc:
            raise RuntimeError("rca_delivery_store_existing_path_missing") from exc
        if (
            stat.S_ISLNK(observed.st_mode)
            or not stat.S_ISREG(observed.st_mode)
            or observed.st_nlink != 1
            or observed.st_size <= 0
        ):
            raise RuntimeError("rca_delivery_store_existing_path_invalid")

    def _validate_binary_write_schema_tx(
        self,
        conn: sqlite3.Connection,
    ) -> None:
        if (
            self.read_only
            or not self._enforce_binary_write_schema
            or self._observed_control_schema_version
            not in {
                CONTROL_STORE_SCHEMA_VERSION,
                CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION,
            }
        ):
            return
        try:
            observed = RcaControlStore._activation_schema_version_tx(conn)
        except (RuntimeError, sqlite3.Error) as exc:
            raise RuntimeError(
                "incompatible_control_store_schema:write_marker"
            ) from exc
        if observed != self._connection_write_schema_version:
            raise RuntimeError("incompatible_control_store_schema:write_marker")
        if observed == CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION:
            try:
                RcaControlStore._validate_v15_activation_schema_tx(conn)
            except (RuntimeError, sqlite3.Error) as exc:
                raise RuntimeError(
                    "incompatible_control_store_schema:write_marker"
                ) from exc

    def _connect(self) -> sqlite3.Connection:
        if self._successor_read_only:
            raise RuntimeError(DELIVERY_STORE_SUCCESSOR_READ_ONLY_ERROR)
        if self.require_current:
            self._validate_runtime_fences()
        if self.read_only:
            conn = sqlite3.connect(
                f"{self._read_db_path.resolve().as_uri()}?mode=ro",
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
        try:
            self._validate_binary_write_schema_tx(conn)
        except Exception:
            conn.close()
            raise
        conn.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
        conn.execute("PRAGMA foreign_keys=ON")
        if self.read_only:
            conn.execute("PRAGMA query_only=ON")
        else:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=FULL")
            if (
                self._enforce_binary_write_schema
                and self._observed_control_schema_version
                == self._connection_write_schema_version
            ):
                try:
                    conn.execute("BEGIN")
                    RcaControlStore._install_connection_write_guards_tx(
                        conn,
                        expected_schema_version=(
                            self._connection_write_schema_version
                        ),
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
        if self.require_current:
            try:
                self._validate_runtime_fences()
            except RuntimeError:
                conn.close()
                raise
        return conn

    def _connect_read_only(self) -> sqlite3.Connection:
        """Open a lightweight live read without mutating SQLite journal state."""

        if self.require_current:
            self._validate_runtime_fences()
        conn = sqlite3.connect(
            f"{self._read_db_path.resolve().as_uri()}?mode=ro",
            uri=True,
            timeout=self.busy_timeout_ms / 1000,
            isolation_level=None,
        )
        conn.row_factory = sqlite3.Row
        try:
            self._validate_binary_write_schema_tx(conn)
        except Exception:
            conn.close()
            raise
        conn.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA query_only=ON")
        if self.require_current:
            try:
                self._validate_runtime_fences()
            except RuntimeError:
                conn.close()
                raise
        return conn

    def _validate_current_activation_binding_tx(
        self,
        conn: sqlite3.Connection,
    ) -> sqlite3.Row:
        """Fail provider writes closed on current epoch/audit drift."""

        epoch = RcaControlStore._current_activation_epoch_unchecked_tx(conn)
        if epoch is None:
            raise RuntimeError("external_write_fence_epoch_not_current")
        try:
            RcaControlStore.validate_current_activation_binding_tx(
                conn,
                schema_version=self._connection_write_schema_version,
            )
        except (ActivationEpochError, RuntimeError) as exc:
            raise RuntimeError("external_write_fence_epoch_not_current") from exc
        return epoch

    def validate_external_write_fence_binding(
        self,
        fence: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Validate the exact current activation ledger for a delivery fence."""
        if self._successor_read_only:
            raise RuntimeError(DELIVERY_STORE_SUCCESSOR_READ_ONLY_ERROR)
        epoch_id = str(fence.get("activation_epoch_id") or "").strip()
        ledger_id = fence.get("activation_ledger_id")
        admission_key = str(fence.get("admission_key") or "").strip()
        if not epoch_id or isinstance(ledger_id, bool) or not isinstance(ledger_id, int) or not admission_key:
            raise RuntimeError("external_write_fence_schema_invalid")
        conn = self._connect_read_only()
        try:
            conn.execute("BEGIN")
            self._validate_current_activation_binding_tx(conn)
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
                 WHERE epoch.epoch_id = ? AND ledger.admission_key = ?
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
            raise RuntimeError("external_write_fence_epoch_not_current")
        if str(row["state"]) != "steady_active":
            raise RuntimeError("external_write_fence_epoch_not_current")
        if str(row["decision"]) != "admit" or not row["bound_at"]:
            raise RuntimeError("external_write_fence_operation_denied")
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
            raise RuntimeError(exc.code) from exc
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise RuntimeError("external_write_fence_schema_invalid") from exc
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

    def validate_profile_terminal_external_write_binding(
        self,
        *,
        effect_key: str,
        delivery_id: str,
        lease_token: str,
        lease_fence: int,
        operation: str,
        issue_url: str,
        target_key: str,
        business_key: str,
        submission_key: str,
        generation: int,
        require_write_started: bool,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        """Reopen the exact Kafka profile-terminal lease for one issue comment."""
        if self._successor_read_only:
            raise RuntimeError(DELIVERY_STORE_SUCCESSOR_READ_ONLY_ERROR)
        text_values = (
            effect_key,
            delivery_id,
            lease_token,
            issue_url,
            target_key,
            business_key,
            submission_key,
        )
        if (
            not all(isinstance(value, str) and value.strip() for value in text_values)
            or isinstance(lease_fence, bool)
            or not isinstance(lease_fence, int)
            or lease_fence < 1
            or isinstance(generation, bool)
            or not isinstance(generation, int)
            or generation < 1
            or not isinstance(require_write_started, bool)
        ):
            raise RuntimeError("external_write_fence_schema_invalid")
        if operation != "feishu_issue_comment":
            raise RuntimeError("external_write_fence_operation_denied")
        current = _utc_datetime(now)
        conn = self._connect_read_only()
        try:
            conn.execute("BEGIN")
            self._validate_current_activation_binding_tx(conn)
            row = conn.execute(
                """
                SELECT outbox.*,
                       effect.effect_key AS delivery_effect_key,
                       effect.delivery_id AS effect_delivery_id,
                       effect.effect_kind AS delivery_effect_kind,
                       effect.required AS delivery_effect_required,
                       effect.target_key AS delivery_effect_target_key,
                       effect.outcome AS delivery_effect_outcome,
                       effect.status AS delivery_effect_status,
                       effect.write_phase AS delivery_effect_write_phase,
                       effect.lease_token AS delivery_effect_lease_token,
                       effect.lease_expires_at AS delivery_effect_lease_expires_at,
                       effect.fence AS delivery_effect_fence,
                       job.issue_url AS delivery_issue_url,
                       job.outcome AS delivery_job_outcome,
                       job.terminal_state AS delivery_job_terminal_state,
                       job.terminal_error_code AS delivery_job_terminal_error_code,
                       job.business_key AS delivery_job_business_key,
                       job.submission_key AS delivery_job_submission_key,
                       job.generation AS delivery_job_generation,
                       job.contract_json AS delivery_job_contract_json,
                       watch.state AS delivery_watch_state,
                       trigger.project_key AS project_key,
                       trigger.work_item_type_key AS work_item_type_key,
                       trigger.work_item_id AS work_item_id,
                       trigger.normalized_json AS trigger_normalized_json,
                       source.source_id AS kafka_origin_source_id,
                       source.source_dedupe_key AS kafka_source_dedupe_key,
                       source.kafka_event_uid AS kafka_event_uid,
                       source.mode AS kafka_source_mode,
                       source.payload_sha256 AS kafka_source_payload_sha256,
                       outbox.origin_source_id AS outbox_origin_source_id,
                       outbox.source_event_id AS outbox_source_event_id,
                       outbox.source_topic AS outbox_source_topic,
                       outbox.source_partition AS outbox_source_partition,
                       outbox.source_offset AS outbox_source_offset,
                       outbox.payload_json AS outbox_payload_json,
                       inbox.normalized_json AS inbox_normalized_json,
                       inbox.raw_sha256 AS inbox_raw_sha256
                  FROM rca_delivery_effects AS effect
                  JOIN rca_delivery_jobs AS job
                    ON job.delivery_id = effect.delivery_id
                  JOIN rca_execution_watch AS watch
                    ON watch.delivery_id = job.delivery_id
                   AND watch.submission_key = job.submission_key
                  JOIN rca_outbox AS outbox
                    ON outbox.outbox_id = watch.submission_outbox_id
                   AND outbox.business_key = job.business_key
                   AND outbox.submission_key = job.submission_key
                   AND outbox.generation = job.generation
                  JOIN business_triggers AS trigger
                    ON trigger.business_key = outbox.business_key
                   AND trigger.generation = outbox.generation
                  JOIN rca_trigger_sources AS source
                    ON source.source_id = trigger.origin_source_id
                  JOIN kafka_inbox AS inbox
                    ON inbox.event_uid = outbox.source_event_id
                 WHERE effect.effect_key = ?
                   AND effect.delivery_id = ?
                   AND effect.lease_token = ?
                   AND effect.fence = ?
                   AND effect.status = 'claimed'
                """,
                (effect_key, delivery_id, lease_token, lease_fence),
            ).fetchone()
            if row is None:
                raise RuntimeError("external_write_fence_operation_denied")
            try:
                lease_expires_at = _parse_iso(
                    str(row["delivery_effect_lease_expires_at"] or "")
                )
                contract = _json_object(row["delivery_job_contract_json"])
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise RuntimeError("external_write_fence_schema_invalid") from exc
            write_phase = str(row["delivery_effect_write_phase"] or "")
            if (
                lease_expires_at <= current
                or write_phase not in {"prewrite", "write_started"}
                or (require_write_started and write_phase != "write_started")
            ):
                raise RuntimeError("external_write_fence_operation_denied")
            epoch = conn.execute(
                "SELECT state, is_current FROM rca_activation_epochs "
                "WHERE epoch_id = ?",
                (str(row["activation_epoch_id"] or ""),),
            ).fetchone()
            if (
                epoch is None
                or int(epoch["is_current"] or 0) != 1
                or str(epoch["state"] or "") not in ACTIVATION_DELIVERY_STATES
            ):
                raise RuntimeError("external_write_fence_epoch_not_current")
            (
                is_profile_terminal,
                observed_issue_url,
                observed_project_simple_name,
                source_error_code,
            ) = (
                self._stored_profile_terminal_issue_target_for_quarantined_outbox_tx(
                    conn,
                    row=row,
                )
            )
            if not RcaControlStore._kafka_generation_contract_valid(row):
                raise RuntimeError("external_write_fence_identity_mismatch")
            if not RcaControlStore._business_profile_observation_sha256(
                row["trigger_normalized_json"]
            ):
                raise RuntimeError("external_write_fence_identity_mismatch")
            if (
                str(row["trigger_normalized_json"] or "")
                != str(row["inbox_normalized_json"] or "")
                or str(row["kafka_source_payload_sha256"] or "")
                != str(row["inbox_raw_sha256"] or "")
            ):
                raise RuntimeError("external_write_fence_identity_mismatch")
            activation_ledger_id = row["activation_ledger_id"]
            if (
                not is_profile_terminal
                or not observed_issue_url
                or not observed_project_simple_name
                or source_error_code not in OUTBOX_PROFILE_TERMINAL_WRITE_ERROR_CODES
                or isinstance(activation_ledger_id, bool)
                or not isinstance(activation_ledger_id, int)
                or activation_ledger_id < 1
                or str(row["delivery_effect_key"] or "") != effect_key
                or str(row["effect_delivery_id"] or "") != delivery_id
                or str(row["delivery_effect_kind"] or "")
                != "feishu_issue_comment"
                or int(row["delivery_effect_required"] or 0) != 1
                or str(row["delivery_effect_target_key"] or "") != target_key
                or str(row["delivery_effect_outcome"] or "") != "quarantined"
                or str(row["delivery_effect_status"] or "") != "claimed"
                or str(row["delivery_effect_lease_token"] or "") != lease_token
                or int(row["delivery_effect_fence"] or 0) != lease_fence
                or str(row["delivery_job_outcome"] or "") != "quarantined"
                or str(row["delivery_job_terminal_state"] or "")
                != OUTBOX_QUARANTINED_TERMINAL_STATE
                or str(row["delivery_job_terminal_error_code"] or "")
                != source_error_code
                or str(row["delivery_job_business_key"] or "") != business_key
                or str(row["delivery_job_submission_key"] or "")
                != submission_key
                or int(row["delivery_job_generation"] or 0) != generation
                or str(row["delivery_watch_state"] or "") != "delivery_created"
                or str(row["status"] or "") != "quarantined"
                or str(row["business_key"] or "") != business_key
                or str(row["submission_key"] or "") != submission_key
                or int(row["generation"] or 0) != generation
                or "w3_execution_snapshot" in contract
            ):
                raise RuntimeError("external_write_fence_identity_mismatch")
            if (
                str(row["delivery_issue_url"] or "").rstrip("/")
                != issue_url.rstrip("/")
                or observed_issue_url.rstrip("/") != issue_url.rstrip("/")
            ):
                raise RuntimeError("external_write_fence_target_mismatch")
            return {
                "epoch_id": str(row["activation_epoch_id"]),
                "activation_ledger_id": activation_ledger_id,
                "effect_key": effect_key,
                "delivery_id": delivery_id,
                "lease_token": lease_token,
                "lease_fence": lease_fence,
                "operation": "feishu_issue_comment",
                "issue_url": observed_issue_url,
                "project_key": str(row["project_key"]),
                "project_simple_name": observed_project_simple_name,
                "target_key": target_key,
                "business_key": business_key,
                "submission_key": submission_key,
                "generation": generation,
                "source_error_code": source_error_code,
                "write_phase": write_phase,
            }
        finally:
            if conn.in_transaction:
                conn.rollback()
            conn.close()

    def validate_terminal_rerun_external_write_binding(
        self,
        *,
        effect_key: str,
        delivery_id: str,
        lease_token: str,
        lease_fence: int,
        operation: str,
        issue_url: str,
        target_key: str,
        business_key: str,
        submission_key: str,
        generation: int,
        require_write_started: bool,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        """Reopen exact authority-bound terminal correction issue writes."""
        if self._successor_read_only:
            raise RuntimeError(DELIVERY_STORE_SUCCESSOR_READ_ONLY_ERROR)
        text_values = (
            effect_key,
            delivery_id,
            lease_token,
            issue_url,
            target_key,
            business_key,
            submission_key,
        )
        if (
            not all(isinstance(value, str) and value.strip() for value in text_values)
            or isinstance(lease_fence, bool)
            or not isinstance(lease_fence, int)
            or lease_fence < 1
            or isinstance(generation, bool)
            or not isinstance(generation, int)
            or generation < 2
            or not isinstance(require_write_started, bool)
        ):
            raise RuntimeError("external_write_fence_schema_invalid")
        current = _utc_datetime(now)
        conn = self._connect_read_only()
        try:
            conn.execute("BEGIN")
            self._validate_current_activation_binding_tx(conn)
            authority = self._owner_authorized_rerun_authority_tx(
                conn,
                business_key=business_key,
                generation=generation,
            )
            if authority is None:
                raise RuntimeError("external_write_fence_identity_mismatch")
            if operation not in _ISSUE_OPERATIONS:
                raise RuntimeError("external_write_fence_operation_denied")
            row = conn.execute(
                """
                SELECT effect.effect_kind, effect.required, effect.target_key,
                       effect.payload_json, effect.payload_sha256,
                       effect.status, effect.write_phase, effect.lease_token,
                       effect.lease_expires_at, effect.fence,
                       job.issue_url, job.target_key AS job_target_key,
                       job.project_key, job.work_item_type_key, job.work_item_id,
                       job.business_key, job.submission_key, job.generation,
                       watch.state AS watch_state
                  FROM rca_delivery_effects AS effect
                  JOIN rca_delivery_jobs AS job
                    ON job.delivery_id = effect.delivery_id
                  JOIN rca_execution_watch AS watch
                    ON watch.delivery_id = job.delivery_id
                   AND watch.submission_key = job.submission_key
                 WHERE effect.effect_key = ?
                   AND effect.delivery_id = ?
                   AND effect.lease_token = ?
                   AND effect.fence = ?
                   AND effect.status = 'claimed'
                """,
                (effect_key, delivery_id, lease_token, lease_fence),
            ).fetchone()
            if row is None:
                raise RuntimeError("external_write_fence_operation_denied")
            try:
                expires_at = _parse_iso(str(row["lease_expires_at"] or ""))
                payload = _json_object(row["payload_json"])
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise RuntimeError("external_write_fence_schema_invalid") from exc
            write_phase = str(row["write_phase"] or "")
            if (
                expires_at <= current
                or write_phase not in {"prewrite", "write_started"}
                or (require_write_started and write_phase != "write_started")
            ):
                raise RuntimeError("external_write_fence_operation_denied")
            expected_target = (
                f"feishu_project:{row['project_key']}:"
                f"{row['work_item_type_key']}:{row['work_item_id']}"
            )
            issue_match = _FEISHU_ISSUE_URL_RE.fullmatch(issue_url.rstrip("/"))
            if (
                issue_match is None
                or issue_match.group(1) != str(authority["project_simple_name"])
                or issue_match.group(2) != str(row["work_item_id"])
                or str(row["issue_url"] or "").rstrip("/") != issue_url.rstrip("/")
                or str(row["target_key"] or "") != target_key
                or str(row["job_target_key"] or "") != expected_target
                or target_key != expected_target
            ):
                raise RuntimeError("external_write_fence_target_mismatch")
            if (
                str(row["effect_kind"] or "") != DELIVERY_EFFECT_KIND
                or int(row["required"] or 0) != 1
                or str(row["status"] or "") != "claimed"
                or str(row["lease_token"] or "") != lease_token
                or int(row["fence"] or 0) != lease_fence
                or str(row["business_key"] or "") != business_key
                or str(row["submission_key"] or "") != submission_key
                or int(row["generation"] or 0) != generation
                or str(row["watch_state"] or "") != "delivery_created"
                or str(authority["submission_key"]) != submission_key
                or str(authority["project_key"]) != str(row["project_key"])
                or str(authority["work_item_type_key"]) !=
                    str(row["work_item_type_key"])
                or str(authority["issue_id"]) != str(row["work_item_id"])
                or str(payload.get("delivery_id") or "") != delivery_id
                or str(payload.get("effect_kind") or "") != DELIVERY_EFFECT_KIND
                or str(payload.get("target_key") or "") != target_key
                or str(payload.get("project_key") or "") != str(row["project_key"])
                or str(payload.get("work_item_type_key") or "") !=
                    str(row["work_item_type_key"])
                or str(payload.get("work_item_id") or "") !=
                    str(row["work_item_id"])
                or not _terminal_rerun_payload_identity_matches(
                    payload,
                    effect_key=effect_key,
                    submission_key=submission_key,
                    generation=generation,
                    expected_payload_sha256=str(row["payload_sha256"] or ""),
                )
            ):
                raise RuntimeError("external_write_fence_identity_mismatch")
            return {
                "authority_sha256": str(authority["authority_sha256"]),
                "outbox_id": int(authority["outbox_id"]),
                "epoch_id": str(authority["activation_epoch_id"]),
                "activation_ledger_id": int(authority["activation_ledger_id"]),
                "effect_key": effect_key,
                "delivery_id": delivery_id,
                "lease_token": lease_token,
                "lease_fence": lease_fence,
                "operation": DELIVERY_EFFECT_KIND,
                "issue_url": issue_url.rstrip("/"),
                "target_key": target_key,
                "business_key": business_key,
                "submission_key": submission_key,
                "generation": generation,
                "project_key": str(row["project_key"]),
                "project_simple_name": str(authority["project_simple_name"]),
                "work_item_type_key": str(row["work_item_type_key"]),
                "work_item_id": str(row["work_item_id"]),
                "write_phase": write_phase,
            }
        finally:
            if conn.in_transaction:
                conn.rollback()
            conn.close()

    def is_historical_external_write_effect(self, created_at: str) -> bool:
        """Grandfather only effects predating the durable W5 rollout marker."""
        conn = self._connect()
        try:
            marker = conn.execute(
                "SELECT value FROM rca_delivery_meta WHERE key = ?",
                (W5_EXTERNAL_WRITE_FENCE_CUTOFF_META_KEY,),
            ).fetchone()
        finally:
            conn.close()
        cutoff = (
            str(marker["value"])
            if marker is not None
            else W5_EXTERNAL_WRITE_FENCE_CUTOFF
        )
        try:
            observed = datetime.fromisoformat(
                str(created_at).replace("Z", "+00:00")
            )
            boundary = datetime.fromisoformat(cutoff.replace("Z", "+00:00"))
        except (TypeError, ValueError, OverflowError):
            return False
        if observed.tzinfo is None or observed.utcoffset() is None:
            return False
        if boundary.tzinfo is None or boundary.utcoffset() is None:
            return False
        return observed.astimezone(timezone.utc) < boundary.astimezone(timezone.utc)

    def activation_epoch(self) -> dict[str, Any] | None:
        """Return the current epoch and its minimal production release binding."""

        conn = self._connect_read_only()
        try:
            conn.execute("BEGIN")
            if not self._table_exists(conn, "rca_activation_epochs"):
                conn.commit()
                return None
            row = conn.execute(
                "SELECT * FROM rca_activation_epochs WHERE is_current = 1"
            ).fetchone()
            if row is None:
                conn.commit()
                return None
            schema_version = RcaControlStore._activation_schema_version_tx(conn)
            RcaControlStore._activation_transition_binding_tx(
                conn,
                epoch=row,
                schema_version=schema_version,
            )
            binding = RcaControlStore._activation_release_epoch_projection(
                row,
                schema_version=schema_version,
            )
            value = {
                "epoch_id": str(row["epoch_id"]),
                "state": str(row["state"]),
                "config_sha256": str(row["config_sha256"] or ""),
                "release_fingerprint_sha256": binding[
                    "release_fingerprint_sha256"
                ],
                "release_note_sha256": binding["release_note_sha256"],
            }
            conn.commit()
            return value
        except Exception:
            if conn.in_transaction:
                conn.rollback()
            raise
        finally:
            conn.close()

    def validate_learning_lane_external_operation(
        self, *, business_key: str, generation: int, operation: str
    ) -> None:
        """Reject every provider-side Feishu operation for a learning admission."""
        conn = self._connect()
        try:
            if not str(operation or "").strip().startswith("feishu_"):
                return
            state = self._learning_lane_guard_state_tx(
                conn, business_key=business_key, generation=generation
            )
        finally:
            conn.close()
        if state == "admitted" or state == "unknown":
            raise RuntimeError(LEARNING_LANE_EXTERNAL_EFFECT_ERROR)
        if (
            state == "terminal_rerun_authorized"
            and str(operation) not in _ISSUE_OPERATIONS
        ):
            raise RuntimeError(LEARNING_LANE_EXTERNAL_EFFECT_ERROR)
        if state == "admission_missing":
            raise RuntimeError(LEARNING_LANE_ADMISSION_MISSING_ERROR)

    @staticmethod
    def _validate_activation_required(activation_required: bool) -> None:
        if not isinstance(activation_required, bool):
            raise ValueError("activation_required must be true or false")

    @classmethod
    def _activation_enforced_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        activation_required: bool,
    ) -> bool:
        """Fail closed once a current activation epoch exists.

        ``activation_required=False`` is retained only for legacy databases that
        have no current epoch. The decision must be made inside the caller's
        transaction so epoch creation and delivery work have a clear order.
        """
        cls._validate_activation_required(activation_required)
        if activation_required:
            return True
        epoch_table = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' "
            "AND name = 'rca_activation_epochs'"
        ).fetchone()
        if epoch_table is None:
            return False
        epoch_columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(rca_activation_epochs)")
        }
        if not {"epoch_id", "state", "is_current"}.issubset(epoch_columns):
            return True
        return (
            conn.execute(
                "SELECT 1 FROM rca_activation_epochs WHERE is_current = 1 LIMIT 1"
            ).fetchone()
            is not None
        )

    @classmethod
    def _activation_schema_ready(
        cls,
        conn: sqlite3.Connection,
        *,
        schema_version: str | None = None,
    ) -> bool:
        present = {
            str(row["name"])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        if not _ACTIVATION_REQUIRED_TABLES.issubset(present):
            return False
        try:
            observed_schema_version = (
                RcaControlStore._activation_schema_version_tx(conn)
            )
        except (ActivationEpochError, RuntimeError):
            return False
        if (
            schema_version is not None
            and schema_version != observed_schema_version
        ):
            return False
        required_columns = {
            "rca_activation_admission_ledger": {
                "ledger_id",
                "epoch_id",
                "decision",
                "bound_at",
                "business_key",
                "submission_key",
                "generation",
            },
            "business_triggers": {"activation_epoch_id", "activation_ledger_id"},
            "rca_outbox": {"activation_epoch_id", "activation_ledger_id"},
        }
        return all(
            columns.issubset({
                str(row["name"])
                for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
            })
            for table, columns in required_columns.items()
        )

    @classmethod
    def _require_activation_schema(cls, conn: sqlite3.Connection) -> None:
        if not cls._activation_schema_ready(conn):
            raise RuntimeError("delivery_activation_schema_unavailable")

    @classmethod
    def _execution_activation_eligible_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        submission_key: str,
    ) -> bool:
        cls._require_activation_schema(conn)
        return (
            conn.execute(
                f"""
                SELECT 1
                  FROM rca_execution_watch AS w
                  JOIN rca_outbox AS o ON o.outbox_id = w.submission_outbox_id
                  JOIN business_triggers AS t
                    ON t.business_key = o.business_key
                   AND t.generation = o.generation
                 WHERE w.submission_key = ?
                   AND {_ACTIVATION_EXECUTION_ELIGIBLE_SQL}
                 LIMIT 1
                """,
                (submission_key,),
            ).fetchone()
            is not None
        )

    @classmethod
    def _effect_activation_eligible_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        effect_key: str,
        submission_key: str,
    ) -> bool:
        row = conn.execute(
            "SELECT payload_json, target_key, effect_kind FROM rca_delivery_effects "
            "WHERE effect_key = ?",
            (effect_key,),
        ).fetchone()
        if row is None:
            return False
        try:
            payload = json.loads(str(row["payload_json"] or ""))
        except (TypeError, json.JSONDecodeError):
            return False
        if isinstance(payload, Mapping) and identifies_adjudication_effect(
            payload, target_key=str(row["target_key"] or "")
        ):
            return (
                conn.execute(
                    f"""
                    SELECT 1
                      FROM rca_delivery_effects AS e
                     WHERE e.effect_key = ?
                       AND {_ADJUDICATION_ACTIVATION_ELIGIBLE_SQL}
                     LIMIT 1
                    """,
                    (effect_key,),
                ).fetchone()
                is not None
            )
        if str(row["effect_kind"] or "") == DELIVERY_CARD_PATCH_EFFECT_KIND:
            return (
                conn.execute(
                    f"""
                    SELECT 1
                      FROM rca_delivery_effects AS e
                     WHERE e.effect_key = ?
                       AND {_CARD_PATCH_ACTIVATION_ELIGIBLE_SQL}
                     LIMIT 1
                    """,
                    (effect_key,),
                ).fetchone()
                is not None
            )
        return cls._execution_activation_eligible_tx(
            conn, submission_key=submission_key
        )

    @staticmethod
    def _validate_failure_route_schema(conn: sqlite3.Connection) -> None:
        errors = failure_route_schema_errors(conn)
        if errors:
            if any(error.startswith("missing_columns:") for error in errors):
                raise RuntimeError("incompatible_delivery_store_schema:failure_routes")
            raise RuntimeError(
                "incompatible_delivery_store_schema:failure_routes_contract"
            )

    @staticmethod
    def _validate_subscription_observability_schema(
        conn: sqlite3.Connection,
    ) -> None:
        if not RcaDeliveryStore._table_exists(
            conn, "rca_delivery_subscriptions"
        ):
            return
        columns = RcaDeliveryStore._table_columns_tx(
            conn, "rca_delivery_subscriptions"
        )
        if "reason" not in columns or not RcaDeliveryStore._table_exists(
            conn, "rca_delivery_subscription_events"
        ):
            raise RuntimeError(
                "incompatible_delivery_store_schema:subscription_observability"
            )
        triggers = {
            str(row["name"])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'trigger' "
                "AND name LIKE 'trg_rca_delivery_subscription_%'"
            ).fetchall()
        }
        if not {
            "trg_rca_delivery_subscription_reason_required",
            "trg_rca_delivery_subscription_event_insert",
            "trg_rca_delivery_subscription_event_update",
        }.issubset(triggers):
            raise RuntimeError(
                "incompatible_delivery_store_schema:subscription_observability"
            )

    @staticmethod
    def _validate_delivery_observation_outbox_schema(
        conn: sqlite3.Connection,
    ) -> None:
        rows = conn.execute(
            "SELECT type, name, tbl_name, sql FROM sqlite_master "
            "WHERE tbl_name = 'rca_delivery_observation_outbox' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY type, name"
        ).fetchall()
        observed = {
            str(row["name"]): {
                "type": str(row["type"]),
                "table": str(row["tbl_name"]),
                "sql_sha256": hashlib.sha256(
                    "".join(str(row["sql"] or "").split()).lower().encode()
                ).hexdigest(),
            }
            for row in rows
        }
        if set(observed) != set(_DELIVERY_OBSERVATION_OUTBOX_SCHEMA_OBJECTS):
            raise RuntimeError(
                "incompatible_delivery_store_schema:delivery_observation_outbox"
            )
        for name, (expected_type, expected_table, expected_sha256) in (
            _DELIVERY_OBSERVATION_OUTBOX_SCHEMA_OBJECTS.items()
        ):
            item = observed[name]
            if (
                item["type"] != expected_type
                or item["table"] != expected_table
                or item["sql_sha256"] != expected_sha256
            ):
                raise RuntimeError(
                    "incompatible_delivery_store_schema:delivery_observation_outbox"
                )

    @staticmethod
    def _validate_v9_predecessor_variant(
        conn: sqlite3.Connection,
        *,
        schema_version: str,
    ) -> None:
        tables = {
            str(row[0])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        effect_columns = {
            str(row[1])
            for row in conn.execute("PRAGMA table_info(rca_delivery_effects)")
        }
        attempt_columns = {
            "adjudication_comment_attempt_count",
            "adjudication_comment_attempted_at",
        }
        adjudication_table = "rca_conclusion_adjudications"
        repair_table = "rca_conclusion_adjudication_repairs"
        failure_table = "rca_failure_routes"
        if schema_version == DELIVERY_STORE_SCHEMA_PREDECESSOR_VERSION:
            if (
                {adjudication_table, repair_table, failure_table} & tables
                or attempt_columns & effect_columns
            ):
                raise RuntimeError(
                    "incompatible_delivery_store_schema:"
                    "pre_v9_source_variant_operator_remediation"
                )
            return
        if schema_version != DELIVERY_STORE_W2_SCHEMA_VERSION:
            return
        if repair_table in tables:
            repair_columns = {
                str(row[1])
                for row in conn.execute(
                    "PRAGMA table_info(rca_conclusion_adjudication_repairs)"
                )
            }
            if "status" in repair_columns and conn.execute(
                "SELECT 1 FROM rca_conclusion_adjudication_repairs "
                "WHERE status = 'succeeded' LIMIT 1"
            ).fetchone() is not None:
                raise RuntimeError(
                    "incompatible_delivery_store_schema:"
                    "legacy_adjudication_receipt_operator_remediation"
                )
            raise RuntimeError(
                "incompatible_delivery_store_schema:"
                "pre_v9_source_variant_operator_remediation"
            )
        if (
            adjudication_table not in tables
            or failure_table not in tables
            or attempt_columns & effect_columns
        ):
            raise RuntimeError(
                "incompatible_delivery_store_schema:"
                "pre_v9_source_variant_operator_remediation"
            )
        adjudication_columns = {
            str(row[1])
            for row in conn.execute(
                "PRAGMA table_info(rca_conclusion_adjudications)"
            )
        }
        if "activation_epoch_id" not in adjudication_columns:
            raise RuntimeError(
                "incompatible_delivery_store_schema:"
                "pre_v9_source_variant_operator_remediation"
            )
        if conn.execute(
            "SELECT 1 FROM rca_conclusion_adjudications "
            "WHERE TRIM(activation_epoch_id) = '' LIMIT 1"
        ).fetchone() is not None:
            raise RuntimeError(
                "incompatible_delivery_store_schema:"
                "legacy_adjudication_activation_operator_remediation"
            )

    def _initialize(self) -> None:
        marker_value = self._preflight_schema_version()
        control_marker_value = self._preflight_control_schema_version()
        self._observed_delivery_schema_version = marker_value
        self._observed_control_schema_version = control_marker_value
        if control_marker_value not in {
            None,
            CONTROL_STORE_SCHEMA_VERSION,
            CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION,
        }:
            raise RuntimeError(
                "rca_delivery_store_control_schema_not_current"
            )
        if control_marker_value == CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION:
            if not self.require_current:
                raise RuntimeError(
                    "rca_delivery_store_control_schema_not_current"
                )
            if self.allow_successor_write and not self.read_only:
                pass
            elif self.allow_successor_read_only:
                self._successor_read_only = True
                self.read_only = True
            else:
                raise RuntimeError(
                    "rca_delivery_store_control_schema_not_current"
                )
        if self.require_current:
            if marker_value != DELIVERY_STORE_SCHEMA_VERSION:
                raise RuntimeError("rca_delivery_store_schema_not_current")
            if control_marker_value not in {
                None,
                CONTROL_STORE_SCHEMA_VERSION,
                CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION,
            }:
                raise RuntimeError(
                    "rca_delivery_store_control_schema_not_current"
                )
            uri = f"{self._schema_probe_db_path.resolve().as_uri()}?mode=ro"
            conn = sqlite3.connect(uri, uri=True)
            conn.row_factory = sqlite3.Row
            conn.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("PRAGMA query_only=ON")
            try:
                if control_marker_value is not None:
                    observed_activation_schema = (
                        RcaControlStore._activation_schema_version_tx(conn)
                    )
                    if observed_activation_schema != control_marker_value:
                        raise RuntimeError(
                            "rca_delivery_store_control_schema_not_current"
                        )
                    if (
                        observed_activation_schema
                        == CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION
                    ):
                        RcaControlStore._validate_v15_activation_schema_tx(conn)
                validate_conclusion_adjudication_schema(conn)
                self._validate_failure_route_schema(conn)
                self._validate_comment_slot_schema(conn)
                self._validate_subscription_observability_schema(conn)
                self._validate_delivery_observation_outbox_schema(conn)
                self._validate_w6_effect_guards(conn)
            finally:
                conn.close()
            if self.ensure_current_rows and not self.read_only:
                if self._schema_probe_snapshot is not None:
                    RcaControlStore._verify_schema_probe_source_unchanged(
                        self._schema_probe_snapshot
                    )
                self._discard_schema_probe_snapshot()
                self._ensure_card_patch_circuit_row()
            return
        # Legacy schema initialization is serialized by SQLite and may race with
        # another writer after probing. The probe still prevents a successor
        # schema from entering this path; require_current snapshot admission
        # retains the stricter source-identity check above.
        self._discard_schema_probe_snapshot()
        conn = self._connect()
        try:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS rca_delivery_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS rca_execution_watch (
                    submission_key TEXT PRIMARY KEY,
                    submission_outbox_id INTEGER NOT NULL UNIQUE,
                    business_key TEXT NOT NULL,
                    generation INTEGER NOT NULL CHECK (generation >= 1),
                    project_key TEXT NOT NULL,
                    work_item_type_key TEXT NOT NULL,
                    work_item_id TEXT NOT NULL,
                    task_id TEXT UNIQUE,
                    state TEXT NOT NULL CHECK (
                        state IN (
                            'pending', 'running', 'terminal_failed',
                            'quarantined', 'delivery_created'
                        )
                    ),
                    poll_attempt INTEGER NOT NULL DEFAULT 0 CHECK (poll_attempt >= 0),
                    next_poll_at TEXT NOT NULL,
                    last_observed_at TEXT,
                    terminal_at TEXT,
                    terminal_first_seen_at TEXT,
                    fence INTEGER NOT NULL DEFAULT 0 CHECK (fence >= 0),
                    lease_token TEXT,
                    lease_owner TEXT,
                    lease_expires_at TEXT,
                    last_status_json TEXT,
                    last_error_code TEXT NOT NULL DEFAULT '',
                    last_error_detail TEXT NOT NULL DEFAULT '',
                    delivery_id TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(business_key, generation),
                    FOREIGN KEY(submission_outbox_id) REFERENCES rca_outbox(outbox_id)
                );

                CREATE TABLE IF NOT EXISTS rca_delivery_jobs (
                    delivery_id TEXT PRIMARY KEY,
                    submission_key TEXT NOT NULL UNIQUE,
                    business_key TEXT NOT NULL,
                    generation INTEGER NOT NULL CHECK (generation >= 1),
                    artifact_set_id TEXT NOT NULL UNIQUE,
                    project_key TEXT NOT NULL,
                    work_item_type_key TEXT NOT NULL,
                    work_item_id TEXT NOT NULL,
                    target_key TEXT NOT NULL,
                    issue_url TEXT NOT NULL,
                    report_url TEXT NOT NULL,
                    outcome TEXT NOT NULL DEFAULT 'success',
                    outcome_key TEXT NOT NULL DEFAULT '',
                    terminal_state TEXT NOT NULL DEFAULT '',
                    terminal_error_code TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL CHECK (
                        status IN ('ready', 'partial', 'delivered', 'quarantined')
                    ),
                    manifest_json TEXT NOT NULL,
                    contract_json TEXT NOT NULL,
                    artifacts_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(submission_key)
                        REFERENCES rca_execution_watch(submission_key)
                );

                CREATE TABLE IF NOT EXISTS rca_delivery_effects (
                    effect_key TEXT PRIMARY KEY,
                    delivery_id TEXT NOT NULL,
                    effect_kind TEXT NOT NULL CHECK (
                        effect_kind IN (
                            'feishu_issue_comment', 'feishu_card_patch',
                            'feishu_thread_reply', 'feishu_attachment_upload',
                            'feishu_field_update'
                        )
                    ),
                    required INTEGER NOT NULL CHECK (required IN (0, 1)),
                    target_key TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    payload_sha256 TEXT NOT NULL,
                    outcome TEXT NOT NULL DEFAULT 'success',
                    write_phase TEXT NOT NULL DEFAULT 'prewrite' CHECK (
                        write_phase IN ('prewrite', 'write_started', 'settled')
                    ),
                    write_started_at TEXT,
                    reconciliation_miss_count INTEGER NOT NULL DEFAULT 0 CHECK (
                        reconciliation_miss_count >= 0
                    ),
                    recovery_write_count INTEGER NOT NULL DEFAULT 0 CHECK (
                        recovery_write_count >= 0
                    ),
                    last_recovery_write_at TEXT,
                    adjudication_comment_attempt_count INTEGER NOT NULL DEFAULT 0
                        CHECK (adjudication_comment_attempt_count IN (0, 1)),
                    adjudication_comment_attempted_at TEXT,
                    comment_slot_schema_version TEXT NOT NULL DEFAULT '' CHECK (
                        comment_slot_schema_version IN (
                            '', 'pnc_rca_comment_slot_v1'
                        )
                    ),
                    comment_slot_key TEXT NOT NULL DEFAULT '',
                    comment_slot_kind TEXT NOT NULL DEFAULT '' CHECK (
                        comment_slot_kind IN ('', 'conclusion', 'correction')
                    ),
                    comment_slot_generation INTEGER CHECK (
                        comment_slot_generation IS NULL
                        OR comment_slot_generation >= 1
                    ),
                    comment_slot_revision INTEGER CHECK (
                        comment_slot_revision IS NULL
                        OR comment_slot_revision >= 1
                    ),
                    comment_slot_budget_exempt INTEGER NOT NULL DEFAULT 0 CHECK (
                        comment_slot_budget_exempt IN (0, 1)
                    ),
                    status TEXT NOT NULL CHECK (
                        status IN (
                            'pending', 'claimed', 'retry_wait', 'uncertain',
                            'succeeded', 'quarantined', 'suppressed'
                        )
                    ),
                    attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),
                    next_attempt_at TEXT,
                    fence INTEGER NOT NULL DEFAULT 0 CHECK (fence >= 0),
                    lease_token TEXT,
                    lease_owner TEXT,
                    lease_expires_at TEXT,
                    remote_receipt_json TEXT,
                    last_error_code TEXT NOT NULL DEFAULT '',
                    last_error_detail TEXT NOT NULL DEFAULT '',
                    completed_at TEXT,
                    quarantined_at TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(delivery_id, effect_kind, target_key),
                    FOREIGN KEY(delivery_id) REFERENCES rca_delivery_jobs(delivery_id)
                );

                CREATE TABLE IF NOT EXISTS rca_delivery_attempts (
                    attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    effect_key TEXT NOT NULL,
                    attempt_no INTEGER NOT NULL CHECK (attempt_no >= 1),
                    event_seq INTEGER NOT NULL CHECK (event_seq >= 1),
                    fence INTEGER NOT NULL CHECK (fence >= 0),
                    request_id TEXT NOT NULL,
                    outcome TEXT NOT NULL CHECK (
                        outcome IN (
                            'started', 'ack', 'nack', 'unknown',
                            'reconciled', 'quarantined'
                        )
                    ),
                    remote_id TEXT NOT NULL DEFAULT '',
                    error_code TEXT NOT NULL DEFAULT '',
                    detail TEXT NOT NULL DEFAULT '',
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    UNIQUE(effect_key, attempt_no, event_seq),
                    FOREIGN KEY(effect_key) REFERENCES rca_delivery_effects(effect_key)
                );

                CREATE TABLE IF NOT EXISTS rca_delivery_dispatcher_circuit (
                    circuit_name TEXT PRIMARY KEY,
                    state TEXT NOT NULL CHECK (state IN ('closed', 'open')),
                    reason_code TEXT NOT NULL DEFAULT '',
                    reason_detail TEXT NOT NULL DEFAULT '',
                    opened_at TEXT,
                    updated_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_execution_watch_due
                    ON rca_execution_watch(state, next_poll_at, lease_expires_at);
                CREATE INDEX IF NOT EXISTS idx_delivery_jobs_status
                    ON rca_delivery_jobs(status, created_at);
                CREATE INDEX IF NOT EXISTS idx_delivery_jobs_updated_at
                    ON rca_delivery_jobs(updated_at DESC, status);
                CREATE INDEX IF NOT EXISTS idx_delivery_effects_due
                    ON rca_delivery_effects(status, next_attempt_at, lease_expires_at);
                """
            )
            ensure_host_runtime_transition_schema(conn)
            self._migrate_schema(conn)
            validate_host_runtime_transition_schema(
                conn,
                error_prefix="incompatible_delivery_store_schema",
            )
            validate_conclusion_adjudication_schema(conn)
            self._validate_failure_route_schema(conn)
            marker = conn.execute(
                "SELECT value FROM rca_delivery_meta WHERE key = 'schema_version'"
            ).fetchone()
            if (
                marker is not None
                and str(marker["value"]) not in SUPPORTED_DELIVERY_STORE_SCHEMA_VERSIONS
            ):
                raise RuntimeError("incompatible_delivery_store_schema:version")
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_delivery_attempts_effect
                    ON rca_delivery_attempts(effect_key, attempt_no, event_seq)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_delivery_attempts_request
                    ON rca_delivery_attempts(request_id)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_delivery_jobs_updated_at
                    ON rca_delivery_jobs(updated_at DESC, status)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_delivery_attempts_outcome_started
                    ON rca_delivery_attempts(outcome, started_at)
                """
            )
            conn.execute(
                """
                INSERT INTO rca_delivery_meta(key, value)
                VALUES('schema_version', ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value
                """,
                (DELIVERY_STORE_SCHEMA_VERSION,),
            )
            conn.execute(
                """
                INSERT INTO rca_delivery_meta(key, value)
                VALUES(?, ?)
                ON CONFLICT(key) DO NOTHING
                """,
                (
                    W5_EXTERNAL_WRITE_FENCE_CUTOFF_META_KEY,
                    W5_EXTERNAL_WRITE_FENCE_CUTOFF,
                ),
            )
            conn.execute(
                """
                INSERT INTO rca_delivery_dispatcher_circuit(
                    circuit_name, state, updated_at
                ) VALUES('feishu_issue_comment', 'closed', ?)
                ON CONFLICT(circuit_name) DO NOTHING
                """,
                (_iso(),),
            )
            conn.execute(
                """
                INSERT INTO rca_delivery_dispatcher_circuit(
                    circuit_name, state, updated_at
                ) VALUES('feishu_thread_reply', 'closed', ?)
                ON CONFLICT(circuit_name) DO NOTHING
                """,
                (_iso(),),
            )
            conn.execute(
                """
                INSERT INTO rca_delivery_dispatcher_circuit(
                    circuit_name, state, updated_at
                ) VALUES('feishu_card_patch', 'closed', ?)
                ON CONFLICT(circuit_name) DO NOTHING
                """,
                (_iso(),),
            )
        finally:
            conn.close()

    def _ensure_card_patch_circuit_row(self) -> None:
        """Bootstrap only the additive B10 circuit on an existing current DB."""

        self._validate_runtime_fences()
        self._ensure_card_patch_circuit_row_at_path(
            self.db_path,
            busy_timeout_ms=self.busy_timeout_ms,
            expected_control_schema_version=(
                self._connection_write_schema_version
            ),
        )
        self._validate_runtime_fences()

    @classmethod
    def _ensure_card_patch_circuit_row_at_path(
        cls,
        db_path: str | Path,
        *,
        busy_timeout_ms: int,
        expected_control_schema_version: str,
    ) -> None:
        if expected_control_schema_version not in {
            CONTROL_STORE_SCHEMA_VERSION,
            CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION,
        }:
            raise ValueError("delivery_store_expected_control_schema_invalid")
        path = Path(db_path).expanduser()
        for suffix in (".pnc-rca-maintenance", ".pnc-rca-tombstone"):
            try:
                Path(f"{path}{suffix}").lstat()
            except FileNotFoundError:
                continue
            except OSError as exc:
                raise RuntimeError(
                    "rca_delivery_store_runtime_fence_invalid"
                ) from exc
            raise RuntimeError("rca_delivery_store_runtime_fenced")
        uri = f"{path.resolve().as_uri()}?mode=ro"
        read_conn = sqlite3.connect(uri, uri=True)
        try:
            existing = read_conn.execute(
                "SELECT 1 FROM rca_delivery_dispatcher_circuit "
                "WHERE circuit_name = ?",
                (DELIVERY_CARD_PATCH_EFFECT_KIND,),
            ).fetchone()
        finally:
            read_conn.close()
        if existing is not None:
            return
        conn = sqlite3.connect(
            path,
            timeout=max(1, int(busy_timeout_ms)) / 1000,
            isolation_level=None,
        )
        conn.row_factory = sqlite3.Row
        try:
            conn.execute(f"PRAGMA busy_timeout={max(1, int(busy_timeout_ms))}")
            conn.execute("BEGIN IMMEDIATE")
            try:
                control_schema_version = (
                    RcaControlStore._activation_schema_version_tx(conn)
                )
            except (RuntimeError, sqlite3.Error) as exc:
                raise RuntimeError(
                    "incompatible_control_store_schema:write_marker"
                ) from exc
            if control_schema_version != expected_control_schema_version:
                raise RuntimeError(
                    "incompatible_control_store_schema:write_marker"
                )
            if control_schema_version == CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION:
                RcaControlStore._validate_v15_activation_schema_tx(conn)
            marker = conn.execute(
                "SELECT value FROM rca_delivery_meta WHERE key = 'schema_version'"
            ).fetchone()
            if marker is None or str(marker[0]) != DELIVERY_STORE_SCHEMA_VERSION:
                raise RuntimeError("rca_delivery_store_schema_not_current")
            conn.execute(
                """
                INSERT INTO rca_delivery_dispatcher_circuit(
                    circuit_name, state, updated_at
                ) VALUES(?, 'closed', ?)
                ON CONFLICT(circuit_name) DO NOTHING
                """,
                (DELIVERY_CARD_PATCH_EFFECT_KIND, _iso()),
            )
            conn.commit()
        except Exception:
            if conn.in_transaction:
                conn.rollback()
            raise
        finally:
            conn.close()

    def _preflight_schema_version(self) -> str | None:
        probe_path = self._schema_probe_db_path
        if not probe_path.is_file() or probe_path.stat().st_size == 0:
            return None
        uri = f"{probe_path.resolve().as_uri()}?mode=ro"
        try:
            conn = sqlite3.connect(uri, uri=True)
            conn.row_factory = sqlite3.Row
            table = conn.execute(
                "SELECT 1 FROM sqlite_master "
                "WHERE type = 'table' AND name = 'rca_delivery_meta'"
            ).fetchone()
            if table is None:
                return None
            marker = conn.execute(
                "SELECT value FROM rca_delivery_meta WHERE key = 'schema_version'"
            ).fetchone()
        except sqlite3.Error as exc:
            raise RuntimeError("incompatible_delivery_store_schema:preflight") from exc
        finally:
            if "conn" in locals():
                conn.close()
        if (
            marker is not None
            and str(marker["value"]) not in SUPPORTED_DELIVERY_STORE_SCHEMA_VERSIONS
        ):
            raise RuntimeError("incompatible_delivery_store_schema:version")
        return str(marker["value"]) if marker is not None else None

    def _preflight_control_schema_version(self) -> str | None:
        if self._schema_probe_snapshot is not None:
            return self._schema_probe_snapshot.schema_version
        if not self.db_path.is_file() or self.db_path.stat().st_size == 0:
            return None
        uri = f"{self.db_path.resolve().as_uri()}?mode=ro"
        try:
            conn = sqlite3.connect(uri, uri=True)
            conn.row_factory = sqlite3.Row
            table = conn.execute(
                "SELECT 1 FROM sqlite_master "
                "WHERE type = 'table' AND name = 'control_meta'"
            ).fetchone()
            if table is None:
                return None
            marker = conn.execute(
                "SELECT value FROM control_meta WHERE key = 'schema_version'"
            ).fetchone()
        except sqlite3.Error as exc:
            raise RuntimeError(
                "incompatible_delivery_store_schema:control_preflight"
            ) from exc
        finally:
            if "conn" in locals():
                conn.close()
        return str(marker["value"]) if marker is not None else None

    @staticmethod
    def _migrate_schema(conn: sqlite3.Connection) -> None:
        # Serialize schema inspection and migration across resident processes.
        marker = conn.execute(
            "SELECT value FROM rca_delivery_meta WHERE key = 'schema_version'"
        ).fetchone()
        initial_schema_version = str(marker["value"]) if marker is not None else ""
        initial_watch_info = list(
            conn.execute("PRAGMA table_info(rca_execution_watch)")
        )
        relax_task_id = any(
            str(row["name"]) == "task_id" and int(row["notnull"]) == 1
            for row in initial_watch_info
        )
        if relax_task_id:
            conn.execute("PRAGMA foreign_keys=OFF")
            conn.execute("PRAGMA legacy_alter_table=ON")
        conn.execute("BEGIN IMMEDIATE")
        RcaDeliveryStore._validate_v9_predecessor_variant(
            conn,
            schema_version=initial_schema_version,
        )
        if RcaDeliveryStore._table_exists(
            conn, "rca_delivery_dispatcher_circuit"
        ):
            card_patch_row = conn.execute(
                "SELECT 1 FROM rca_delivery_dispatcher_circuit "
                "WHERE circuit_name = 'feishu_card_patch'"
            ).fetchone()
            if card_patch_row is None:
                deterministic_updated_at = conn.execute(
                    "SELECT COALESCE(MAX(updated_at), ?) "
                    "FROM rca_delivery_dispatcher_circuit",
                    (W5_EXTERNAL_WRITE_FENCE_CUTOFF,),
                ).fetchone()[0]
                conn.execute(
                    """
                    INSERT INTO rca_delivery_dispatcher_circuit(
                        circuit_name, state, updated_at
                    ) VALUES('feishu_card_patch', 'closed', ?)
                    """,
                    (deterministic_updated_at,),
                )
        if RcaDeliveryStore._table_exists(conn, "rca_delivery_subscriptions"):
            subscription_columns = {
                str(row["name"])
                for row in conn.execute(
                    "PRAGMA table_info(rca_delivery_subscriptions)"
                )
            }
            reason_added = "reason" not in subscription_columns
            if reason_added:
                conn.execute(
                    "ALTER TABLE rca_delivery_subscriptions ADD COLUMN reason "
                    "TEXT NOT NULL DEFAULT 'awaiting_delivery_materialization'"
                )
            reason_update = """
                UPDATE rca_delivery_subscriptions
                   SET reason = CASE status
                       WHEN 'pending' THEN 'awaiting_delivery_materialization'
                       WHEN 'materialized' THEN 'delivery_effect_materialized'
                       WHEN 'suppressed' THEN 'legacy_suppression_reason_unknown'
                       WHEN 'quarantined' THEN 'legacy_quarantine_reason_unknown'
                       ELSE 'legacy_subscription_state_unknown'
                   END
            """
            if reason_added:
                conn.execute(reason_update)
            else:
                conn.execute(reason_update + " WHERE TRIM(reason) = ''")
            _execute_schema_script_in_transaction(
                conn,
                """
                CREATE TABLE IF NOT EXISTS rca_delivery_subscription_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    subscription_key TEXT NOT NULL,
                    old_status TEXT NOT NULL DEFAULT '',
                    new_status TEXT NOT NULL,
                    reason TEXT NOT NULL CHECK(length(trim(reason)) > 0),
                    observed_at TEXT NOT NULL,
                    FOREIGN KEY(subscription_key)
                        REFERENCES rca_delivery_subscriptions(subscription_key)
                );

                CREATE INDEX IF NOT EXISTS idx_rca_delivery_subscription_events
                    ON rca_delivery_subscription_events(
                        subscription_key, event_id
                    );

                CREATE TRIGGER IF NOT EXISTS
                    trg_rca_delivery_subscription_reason_required
                BEFORE UPDATE OF status ON rca_delivery_subscriptions
                WHEN OLD.status != NEW.status
                 AND (
                     length(trim(NEW.reason)) = 0
                     OR NEW.reason = OLD.reason
                 )
                BEGIN
                    SELECT RAISE(
                        ABORT,
                        'rca_delivery_subscription_transition_reason_required'
                    );
                END;

                CREATE TRIGGER IF NOT EXISTS
                    trg_rca_delivery_subscription_event_insert
                AFTER INSERT ON rca_delivery_subscriptions
                BEGIN
                    INSERT INTO rca_delivery_subscription_events(
                        subscription_key, old_status, new_status, reason,
                        observed_at
                    ) VALUES (
                        NEW.subscription_key, '', NEW.status, NEW.reason,
                        NEW.updated_at
                    );
                END;

                CREATE TRIGGER IF NOT EXISTS
                    trg_rca_delivery_subscription_event_update
                AFTER UPDATE OF status, reason ON rca_delivery_subscriptions
                WHEN OLD.status != NEW.status OR OLD.reason != NEW.reason
                BEGIN
                    INSERT INTO rca_delivery_subscription_events(
                        subscription_key, old_status, new_status, reason,
                        observed_at
                    ) VALUES (
                        NEW.subscription_key, OLD.status, NEW.status,
                        NEW.reason, NEW.updated_at
                    );
                END;
                """,
            )
            conn.execute(
                """
                INSERT INTO rca_delivery_subscription_events(
                    subscription_key, old_status, new_status, reason,
                    observed_at
                )
                SELECT subscription_key, '', status, reason, updated_at
                  FROM rca_delivery_subscriptions AS subscription
                 WHERE NOT EXISTS (
                     SELECT 1
                       FROM rca_delivery_subscription_events AS event
                      WHERE event.subscription_key = subscription.subscription_key
                 )
                 ORDER BY subscription.subscription_key
                """
            )
        watch_columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(rca_execution_watch)")
        }
        if "terminal_first_seen_at" not in watch_columns:
            conn.execute(
                "ALTER TABLE rca_execution_watch ADD COLUMN terminal_first_seen_at TEXT"
            )
        current_watch_info = list(
            conn.execute("PRAGMA table_info(rca_execution_watch)")
        )
        if any(
            str(row["name"]) == "task_id" and int(row["notnull"]) == 1
            for row in current_watch_info
        ):
            conn.execute(
                "ALTER TABLE rca_execution_watch RENAME TO rca_execution_watch_v4"
            )
            conn.execute(
                """
                CREATE TABLE rca_execution_watch (
                    submission_key TEXT PRIMARY KEY,
                    submission_outbox_id INTEGER NOT NULL UNIQUE,
                    business_key TEXT NOT NULL,
                    generation INTEGER NOT NULL CHECK (generation >= 1),
                    project_key TEXT NOT NULL,
                    work_item_type_key TEXT NOT NULL,
                    work_item_id TEXT NOT NULL,
                    task_id TEXT UNIQUE,
                    state TEXT NOT NULL CHECK (
                        state IN (
                            'pending', 'running', 'terminal_failed',
                            'quarantined', 'delivery_created'
                        )
                    ),
                    poll_attempt INTEGER NOT NULL DEFAULT 0 CHECK (poll_attempt >= 0),
                    next_poll_at TEXT NOT NULL,
                    last_observed_at TEXT,
                    terminal_at TEXT,
                    terminal_first_seen_at TEXT,
                    fence INTEGER NOT NULL DEFAULT 0 CHECK (fence >= 0),
                    lease_token TEXT,
                    lease_owner TEXT,
                    lease_expires_at TEXT,
                    last_status_json TEXT,
                    last_error_code TEXT NOT NULL DEFAULT '',
                    last_error_detail TEXT NOT NULL DEFAULT '',
                    delivery_id TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(business_key, generation),
                    FOREIGN KEY(submission_outbox_id) REFERENCES rca_outbox(outbox_id)
                )
                """
            )
            conn.execute(
                """
                INSERT INTO rca_execution_watch(
                    submission_key, submission_outbox_id, business_key, generation,
                    project_key, work_item_type_key, work_item_id, task_id, state,
                    poll_attempt, next_poll_at, last_observed_at, terminal_at,
                    terminal_first_seen_at, fence, lease_token, lease_owner,
                    lease_expires_at, last_status_json, last_error_code,
                    last_error_detail, delivery_id, created_at, updated_at
                )
                SELECT submission_key, submission_outbox_id, business_key, generation,
                       project_key, work_item_type_key, work_item_id, task_id, state,
                       poll_attempt, next_poll_at, last_observed_at, terminal_at,
                       terminal_first_seen_at, fence, lease_token, lease_owner,
                       lease_expires_at, last_status_json, last_error_code,
                       last_error_detail, delivery_id, created_at, updated_at
                  FROM rca_execution_watch_v4
                """
            )
            conn.execute("DROP TABLE rca_execution_watch_v4")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_execution_watch_due "
                "ON rca_execution_watch(state, next_poll_at, lease_expires_at)"
            )

        job_columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(rca_delivery_jobs)")
        }
        for name, definition in {
            "outcome": "TEXT NOT NULL DEFAULT 'success'",
            "outcome_key": "TEXT NOT NULL DEFAULT ''",
            "terminal_state": "TEXT NOT NULL DEFAULT ''",
            "terminal_error_code": "TEXT NOT NULL DEFAULT ''",
        }.items():
            if name not in job_columns:
                conn.execute(
                    f"ALTER TABLE rca_delivery_jobs ADD COLUMN {name} {definition}"
                )

        effect_columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(rca_delivery_effects)")
        }
        if "outcome" not in effect_columns:
            conn.execute(
                "ALTER TABLE rca_delivery_effects "
                "ADD COLUMN outcome TEXT NOT NULL DEFAULT 'success'"
            )
        if "write_phase" not in effect_columns:
            conn.execute(
                "ALTER TABLE rca_delivery_effects ADD COLUMN write_phase TEXT "
                "NOT NULL DEFAULT 'prewrite' CHECK (write_phase IN "
                "('prewrite', 'write_started', 'settled'))"
            )
            conn.execute(
                "UPDATE rca_delivery_effects SET write_phase = 'write_started' "
                "WHERE status IN ('claimed', 'uncertain')"
            )
            conn.execute(
                "UPDATE rca_delivery_effects SET write_phase = 'settled' "
                "WHERE status IN ('succeeded', 'quarantined', 'suppressed')"
            )
        for name, definition in {
            "write_started_at": "TEXT",
            "reconciliation_miss_count": (
                "INTEGER NOT NULL DEFAULT 0 CHECK (reconciliation_miss_count >= 0)"
            ),
            "recovery_write_count": (
                "INTEGER NOT NULL DEFAULT 0 CHECK (recovery_write_count >= 0)"
            ),
            "last_recovery_write_at": "TEXT",
            "adjudication_comment_attempt_count": (
                "INTEGER NOT NULL DEFAULT 0 CHECK "
                "(adjudication_comment_attempt_count IN (0, 1))"
            ),
            "adjudication_comment_attempted_at": "TEXT",
            "comment_slot_schema_version": (
                "TEXT NOT NULL DEFAULT '' CHECK "
                "(comment_slot_schema_version IN "
                "('', 'pnc_rca_comment_slot_v1'))"
            ),
            "comment_slot_key": "TEXT NOT NULL DEFAULT ''",
            "comment_slot_kind": (
                "TEXT NOT NULL DEFAULT '' CHECK "
                "(comment_slot_kind IN ('', 'conclusion', 'correction'))"
            ),
            "comment_slot_generation": (
                "INTEGER CHECK (comment_slot_generation IS NULL "
                "OR comment_slot_generation >= 1)"
            ),
            "comment_slot_revision": (
                "INTEGER CHECK (comment_slot_revision IS NULL "
                "OR comment_slot_revision >= 1)"
            ),
            "comment_slot_budget_exempt": (
                "INTEGER NOT NULL DEFAULT 0 CHECK "
                "(comment_slot_budget_exempt IN (0, 1))"
            ),
        }.items():
            if name not in effect_columns:
                conn.execute(
                    f"ALTER TABLE rca_delivery_effects ADD COLUMN {name} {definition}"
                )
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_delivery_effects_comment_slot "
            "ON rca_delivery_effects(comment_slot_key) "
            "WHERE comment_slot_key != ''"
        )
        conn.execute(
            "UPDATE rca_delivery_effects "
            "SET write_started_at = COALESCE(write_started_at, updated_at, created_at) "
            "WHERE write_phase = 'write_started' AND write_started_at IS NULL"
        )
        ensure_conclusion_adjudication_schema(conn)

        attempt_columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(rca_delivery_attempts)")
        }
        if attempt_columns and "event_seq" not in attempt_columns:
            conn.execute(
                "ALTER TABLE rca_delivery_attempts RENAME TO rca_delivery_attempts_v1"
            )
            conn.execute(
                """
                CREATE TABLE rca_delivery_attempts (
                    attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    effect_key TEXT NOT NULL,
                    attempt_no INTEGER NOT NULL CHECK (attempt_no >= 1),
                    event_seq INTEGER NOT NULL CHECK (event_seq >= 1),
                    fence INTEGER NOT NULL CHECK (fence >= 0),
                    request_id TEXT NOT NULL,
                    outcome TEXT NOT NULL CHECK (
                        outcome IN (
                            'started', 'ack', 'nack', 'unknown',
                            'reconciled', 'quarantined'
                        )
                    ),
                    remote_id TEXT NOT NULL DEFAULT '',
                    error_code TEXT NOT NULL DEFAULT '',
                    detail TEXT NOT NULL DEFAULT '',
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    UNIQUE(effect_key, attempt_no, event_seq),
                    FOREIGN KEY(effect_key) REFERENCES rca_delivery_effects(effect_key)
                )
                """
            )
            conn.execute(
                """
                INSERT INTO rca_delivery_attempts(
                    attempt_id, effect_key, attempt_no, event_seq, fence,
                    request_id, outcome, remote_id, error_code, detail,
                    started_at, finished_at
                )
                SELECT attempt_id, effect_key, attempt_no, 1, fence,
                       request_id, outcome, remote_id, error_code, detail,
                       started_at, finished_at
                  FROM rca_delivery_attempts_v1
                """
            )
            conn.execute("DROP TABLE rca_delivery_attempts_v1")
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_delivery_attempts_effect
                    ON rca_delivery_attempts(effect_key, attempt_no, event_seq)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_delivery_attempts_request
                    ON rca_delivery_attempts(request_id)
                """
            )
        _execute_schema_script_in_transaction(
            conn,
            """
            CREATE TABLE IF NOT EXISTS rca_delivery_quarantine_mutation_audit (
                audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_kind TEXT NOT NULL CHECK (
                    entity_kind IN ('job', 'effect', 'subscription')
                ),
                entity_key TEXT NOT NULL,
                operation TEXT NOT NULL CHECK (
                    operation IN ('migration_observed', 'entered', 'left', 'deleted')
                ),
                old_status TEXT NOT NULL DEFAULT '',
                new_status TEXT NOT NULL DEFAULT '',
                observed_at TEXT NOT NULL
            );

            CREATE TRIGGER IF NOT EXISTS trg_rca_quarantine_audit_no_update
            BEFORE UPDATE ON rca_delivery_quarantine_mutation_audit
            BEGIN
                SELECT RAISE(ABORT, 'rca_delivery_quarantine_audit_immutable');
            END;

            CREATE TRIGGER IF NOT EXISTS trg_rca_quarantine_audit_no_delete
            BEFORE DELETE ON rca_delivery_quarantine_mutation_audit
            BEGIN
                SELECT RAISE(ABORT, 'rca_delivery_quarantine_audit_immutable');
            END;

            CREATE TRIGGER IF NOT EXISTS trg_rca_delivery_job_quarantine_insert
            AFTER INSERT ON rca_delivery_jobs
            WHEN NEW.status = 'quarantined'
            BEGIN
                INSERT INTO rca_delivery_quarantine_mutation_audit(
                    entity_kind, entity_key, operation, old_status,
                    new_status, observed_at
                ) VALUES (
                    'job', NEW.delivery_id, 'entered', '', NEW.status,
                    strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                );
            END;

            CREATE TRIGGER IF NOT EXISTS trg_rca_delivery_job_quarantine_update
            AFTER UPDATE OF status ON rca_delivery_jobs
            WHEN OLD.status != NEW.status
             AND (OLD.status = 'quarantined' OR NEW.status = 'quarantined')
            BEGIN
                INSERT INTO rca_delivery_quarantine_mutation_audit(
                    entity_kind, entity_key, operation, old_status,
                    new_status, observed_at
                ) VALUES (
                    'job', NEW.delivery_id,
                    CASE WHEN NEW.status = 'quarantined' THEN 'entered' ELSE 'left' END,
                    OLD.status, NEW.status,
                    strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                );
            END;

            CREATE TRIGGER IF NOT EXISTS trg_rca_delivery_job_quarantine_delete
            AFTER DELETE ON rca_delivery_jobs
            WHEN OLD.status = 'quarantined'
            BEGIN
                INSERT INTO rca_delivery_quarantine_mutation_audit(
                    entity_kind, entity_key, operation, old_status,
                    new_status, observed_at
                ) VALUES (
                    'job', OLD.delivery_id, 'deleted', OLD.status, '',
                    strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                );
            END;

            CREATE TRIGGER IF NOT EXISTS trg_rca_delivery_effect_quarantine_insert
            AFTER INSERT ON rca_delivery_effects
            WHEN NEW.status = 'quarantined'
            BEGIN
                INSERT INTO rca_delivery_quarantine_mutation_audit(
                    entity_kind, entity_key, operation, old_status,
                    new_status, observed_at
                ) VALUES (
                    'effect', NEW.effect_key, 'entered', '', NEW.status,
                    strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                );
            END;

            CREATE TRIGGER IF NOT EXISTS trg_rca_delivery_effect_quarantine_update
            AFTER UPDATE OF status ON rca_delivery_effects
            WHEN OLD.status != NEW.status
             AND (OLD.status = 'quarantined' OR NEW.status = 'quarantined')
            BEGIN
                INSERT INTO rca_delivery_quarantine_mutation_audit(
                    entity_kind, entity_key, operation, old_status,
                    new_status, observed_at
                ) VALUES (
                    'effect', NEW.effect_key,
                    CASE WHEN NEW.status = 'quarantined' THEN 'entered' ELSE 'left' END,
                    OLD.status, NEW.status,
                    strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                );
            END;

            CREATE TRIGGER IF NOT EXISTS trg_rca_delivery_effect_quarantine_delete
            AFTER DELETE ON rca_delivery_effects
            WHEN OLD.status = 'quarantined'
            BEGIN
                INSERT INTO rca_delivery_quarantine_mutation_audit(
                    entity_kind, entity_key, operation, old_status,
                    new_status, observed_at
                ) VALUES (
                    'effect', OLD.effect_key, 'deleted', OLD.status, '',
                    strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                );
            END;

            """,
        )
        subscriptions_ready = (
            conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' "
                "AND name = 'rca_delivery_subscriptions'"
            ).fetchone()
            is not None
        )
        if subscriptions_ready:
            _execute_schema_script_in_transaction(
                conn,
                """
                CREATE TRIGGER IF NOT EXISTS trg_rca_delivery_subscription_quarantine_insert
                AFTER INSERT ON rca_delivery_subscriptions
                WHEN NEW.status = 'quarantined'
                BEGIN
                    INSERT INTO rca_delivery_quarantine_mutation_audit(
                        entity_kind, entity_key, operation, old_status,
                        new_status, observed_at
                    ) VALUES (
                        'subscription', NEW.subscription_key, 'entered', '', NEW.status,
                        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                    );
                END;

                CREATE TRIGGER IF NOT EXISTS trg_rca_delivery_subscription_quarantine_update
                AFTER UPDATE OF status ON rca_delivery_subscriptions
                WHEN OLD.status != NEW.status
                 AND (OLD.status = 'quarantined' OR NEW.status = 'quarantined')
                BEGIN
                    INSERT INTO rca_delivery_quarantine_mutation_audit(
                        entity_kind, entity_key, operation, old_status,
                        new_status, observed_at
                    ) VALUES (
                        'subscription', NEW.subscription_key,
                        CASE WHEN NEW.status = 'quarantined' THEN 'entered' ELSE 'left' END,
                        OLD.status, NEW.status,
                        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                    );
                END;

                CREATE TRIGGER IF NOT EXISTS trg_rca_delivery_subscription_quarantine_delete
                AFTER DELETE ON rca_delivery_subscriptions
                WHEN OLD.status = 'quarantined'
                BEGIN
                    INSERT INTO rca_delivery_quarantine_mutation_audit(
                        entity_kind, entity_key, operation, old_status,
                        new_status, observed_at
                    ) VALUES (
                        'subscription', OLD.subscription_key, 'deleted', OLD.status, '',
                        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                    );
                END;
                """,
            )
        if (
            conn.execute(
                "SELECT 1 FROM rca_delivery_quarantine_mutation_audit LIMIT 1"
            ).fetchone()
            is None
        ):
            for entity_kind, table, key in (
                ("job", "rca_delivery_jobs", "delivery_id"),
                ("effect", "rca_delivery_effects", "effect_key"),
                *(
                    (
                        (
                            "subscription",
                            "rca_delivery_subscriptions",
                            "subscription_key",
                        ),
                    )
                    if subscriptions_ready
                    else ()
                ),
            ):
                conn.execute(
                    "INSERT INTO rca_delivery_quarantine_mutation_audit("
                    "entity_kind, entity_key, operation, old_status, new_status, "
                    "observed_at) SELECT ?, " + key + ", 'migration_observed', '', "
                    "'quarantined', updated_at FROM " + table + " "
                    "WHERE status = 'quarantined' ORDER BY " + key,
                    (entity_kind,),
                )
        _execute_schema_script_in_transaction(
            conn,
            """
            CREATE TABLE IF NOT EXISTS rca_failure_routes (
                route_key TEXT PRIMARY KEY,
                dedupe_key TEXT NOT NULL UNIQUE,
                submission_key TEXT NOT NULL,
                business_key TEXT NOT NULL,
                generation INTEGER NOT NULL CHECK (generation >= 1),
                task_id TEXT NOT NULL,
                terminal_error_code TEXT NOT NULL,
                lane TEXT NOT NULL CHECK (
                    lane IN (
                        'infra_self_healable', 'needs_human_input',
                        'hard_defect'
                    )
                ),
                route_kind TEXT NOT NULL CHECK (
                    route_kind IN (
                        'infra_remediation_hold', 'internal_backlog',
                        'internal_alert'
                    )
                ),
                owner TEXT NOT NULL,
                status TEXT NOT NULL CHECK (
                    status IN (
                        'remediation_pending', 'remediation_started',
                        'remediation_succeeded', 'remediation_held',
                        'backlog_pending', 'alert_pending',
                        'terminal_fallback', 'resolved'
                    )
                ),
                work_started_at TEXT NOT NULL,
                deadline_at TEXT NOT NULL,
                first_observed_at TEXT NOT NULL,
                last_observed_at TEXT NOT NULL,
                remediation_attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (
                    remediation_attempt_count BETWEEN 0 AND 1
                ),
                observation_count INTEGER NOT NULL DEFAULT 1 CHECK (
                    observation_count >= 1
                ),
                retry_count INTEGER NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
                remediation_attempted_at TEXT,
                remediation_result_json TEXT NOT NULL DEFAULT '{}',
                next_retry_at TEXT,
                retry_exhausted INTEGER NOT NULL DEFAULT 0 CHECK (
                    retry_exhausted IN (0, 1)
                ),
                audit_json TEXT NOT NULL,
                route_payload_json TEXT NOT NULL,
                completed_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(submission_key)
                    REFERENCES rca_execution_watch(submission_key)
            );

            CREATE INDEX IF NOT EXISTS idx_failure_routes_status
                ON rca_failure_routes(status, owner, deadline_at);
            CREATE INDEX IF NOT EXISTS idx_failure_routes_submission
                ON rca_failure_routes(submission_key, created_at);
            """,
        )
        _execute_schema_script_in_transaction(
            conn,
            """
            CREATE TABLE IF NOT EXISTS rca_delivery_observation_outbox (
                observation_id TEXT PRIMARY KEY CHECK (
                    length(observation_id) = 64
                    AND observation_id NOT GLOB '*[^0-9a-f]*'
                ),
                effect_key TEXT NOT NULL UNIQUE,
                payload_json TEXT NOT NULL,
                payload_sha256 TEXT NOT NULL CHECK (
                    length(payload_sha256) = 64
                    AND payload_sha256 NOT GLOB '*[^0-9a-f]*'
                ),
                status TEXT NOT NULL CHECK (status IN ('pending', 'appended')),
                created_at TEXT NOT NULL,
                appended_at TEXT,
                CHECK (
                    (status = 'pending' AND appended_at IS NULL)
                    OR (status = 'appended' AND appended_at IS NOT NULL)
                ),
                FOREIGN KEY(effect_key)
                    REFERENCES rca_delivery_effects(effect_key)
            );

            CREATE INDEX IF NOT EXISTS idx_delivery_observation_outbox_status
                ON rca_delivery_observation_outbox(status, created_at);
            """,
        )
        validate_conclusion_adjudication_schema(conn)
        RcaDeliveryStore._validate_failure_route_schema(conn)
        RcaDeliveryStore._validate_comment_slot_schema(conn)
        RcaDeliveryStore._validate_subscription_observability_schema(conn)
        RcaDeliveryStore._validate_delivery_observation_outbox_schema(conn)
        # Recreate the exact W6 objects even when the marker already says v12.
        # The historical-epoch authority contract changes trigger bodies, and
        # SQLite's CREATE TRIGGER IF NOT EXISTS does not replace an older body.
        # The Python transaction guards remain authoritative when the control
        # tables are not present in an isolated delivery fixture.
        RcaDeliveryStore._drop_w6_stock_effect_guards(conn)
        RcaDeliveryStore._install_w6_effect_guards(conn)
        RcaDeliveryStore._validate_w6_effect_guards(conn)
        quick_check = conn.execute("PRAGMA quick_check").fetchone()
        if quick_check is None or str(quick_check[0]).lower() != "ok":
            raise RuntimeError("incompatible_delivery_store_schema:quick_check")
        if initial_schema_version in {
            "pnc_rca_delivery_store_v6",
            DELIVERY_STORE_SCHEMA_PREDECESSOR_VERSION,
            DELIVERY_STORE_W2_SCHEMA_VERSION,
            DELIVERY_STORE_W6_PREDECESSOR_VERSION,
            DELIVERY_STORE_OBSERVABILITY_PREDECESSOR_VERSION,
            DELIVERY_STORE_TERMINAL_RERUN_PREDECESSOR_VERSION,
        }:
            conn.execute(
                "UPDATE rca_delivery_meta SET value = ? WHERE key = 'schema_version'",
                (DELIVERY_STORE_SCHEMA_VERSION,),
            )
        conn.commit()
        if relax_task_id:
            conn.execute("PRAGMA legacy_alter_table=OFF")
            conn.execute("PRAGMA foreign_keys=ON")
            if conn.execute("PRAGMA foreign_key_check").fetchone() is not None:
                raise RuntimeError("incompatible_delivery_store_schema:foreign_keys")

    @staticmethod
    def _drop_w6_stock_effect_guards(conn: sqlite3.Connection) -> None:
        for name in (
            "trg_learning_lane_stock_effect_insert_forbidden",
            "trg_learning_lane_stock_subscription_insert_forbidden",
            "trg_learning_lane_stock_subscription_update_forbidden",
        ):
            conn.execute(f"DROP TRIGGER IF EXISTS {name}")

    @staticmethod
    def _w6_effect_guard_statements(conn: sqlite3.Connection) -> tuple[str, ...]:
        statements: list[str] = []
        admissions_ready = RcaDeliveryStore._table_exists(
            conn, "rca_learning_lane_admissions"
        )
        if admissions_ready:
            statements.append(
                """
                CREATE TRIGGER IF NOT EXISTS trg_learning_lane_effect_insert_forbidden
                BEFORE INSERT ON rca_delivery_effects
                WHEN NEW.effect_kind LIKE 'feishu_%'
                 AND EXISTS (
                     SELECT 1 FROM rca_learning_lane_admissions AS admission
                      WHERE admission.business_key = (
                          SELECT business_key FROM rca_delivery_jobs
                           WHERE delivery_id = NEW.delivery_id
                      )
                        AND admission.generation = (
                          SELECT generation FROM rca_delivery_jobs
                           WHERE delivery_id = NEW.delivery_id
                        )
                        AND admission.lane = 'learning'
                        AND admission.external_write_allowed = 0
                 )
                BEGIN
                    SELECT RAISE(ABORT, 'learning_lane_external_effect_forbidden');
                END
                """
            )

        cohort_ready = all(
            RcaDeliveryStore._table_exists(conn, table)
            for table in (
                "business_triggers",
                "rca_learning_lane_cohorts",
                "rca_learning_lane_stock_items",
            )
        )
        if not (cohort_ready and admissions_ready):
            return tuple(statements)
        authority_schema_ready = (
            all(
                RcaDeliveryStore._table_exists(conn, table)
                for table in (
                    "rca_terminal_rerun_delivery_authorities",
                    "rca_historical_epoch_rerun_delivery_authorities",
                )
            )
            and conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='view' "
                "AND name='rca_owner_authorized_rerun_delivery_authorities'"
            ).fetchone()
            is not None
        )
        if not authority_schema_ready:
            raise RuntimeError(
                "incompatible_delivery_store_schema:"
                "terminal_rerun_authority_table_missing"
            )
        try:
            control_schema_version = (
                RcaControlStore._activation_schema_version_tx(conn)
            )
        except RuntimeError as exc:
            raise RuntimeError(
                "incompatible_delivery_store_schema:"
                "terminal_rerun_authority_control_schema"
            ) from exc
        if control_schema_version not in {
            CONTROL_STORE_SCHEMA_VERSION,
            CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION,
        }:
            raise RuntimeError(
                "incompatible_delivery_store_schema:"
                "terminal_rerun_authority_control_schema"
            )
        RcaControlStore._validate_v14_terminal_rerun_delivery_authority_schema(conn)
        RcaControlStore._validate_historical_epoch_rerun_delivery_authority_schema(conn)
        statements.append(
            """
            CREATE TRIGGER IF NOT EXISTS
                trg_learning_lane_stock_effect_insert_forbidden
            BEFORE INSERT ON rca_delivery_effects
            WHEN NEW.effect_kind LIKE 'feishu_%'
             AND EXISTS (
                 SELECT 1
                   FROM rca_delivery_jobs AS job
                   JOIN business_triggers AS bt
                     ON bt.business_key = job.business_key
                    AND bt.generation = job.generation
                   JOIN rca_learning_lane_cohorts AS cohort
                   JOIN rca_learning_lane_stock_items AS item
                     ON item.cohort_id = cohort.cohort_id
                    AND item.work_item_id = job.work_item_id
                  WHERE job.delivery_id = NEW.delivery_id
                    AND (
                        julianday(bt.created_at) IS NULL
                        OR julianday(cohort.stock_cutoff) IS NULL
                        OR julianday(bt.created_at) > julianday(cohort.stock_cutoff)
                    )
                    AND NOT EXISTS (
                        SELECT 1
                          FROM rca_learning_lane_admissions AS admission
                         WHERE admission.business_key = job.business_key
                           AND admission.generation = job.generation
                           AND admission.lane = 'learning'
                           AND admission.external_write_allowed = 0
                    )
                    AND NOT (
                        NEW.effect_kind = 'feishu_issue_comment'
                        AND NEW.target_key = job.target_key
                        AND job.target_key =
                            'feishu_project:' || bt.project_key || ':' ||
                            bt.work_item_type_key || ':' || bt.work_item_id
                        AND json_extract(NEW.payload_json, '$.delivery_id') =
                            NEW.delivery_id
                        AND json_extract(NEW.payload_json, '$.effect_kind') =
                            NEW.effect_kind
                        AND json_extract(NEW.payload_json, '$.target_key') =
                            NEW.target_key
                        AND json_extract(NEW.payload_json, '$.project_key') =
                            bt.project_key
                        AND json_extract(
                            NEW.payload_json, '$.work_item_type_key'
                        ) = bt.work_item_type_key
                        AND json_extract(NEW.payload_json, '$.work_item_id') =
                            bt.work_item_id
                        AND (
                            (
                                json_extract(
                                    NEW.payload_json, '$.schema_version'
                                ) = 'pnc_rca_delivery_effect_v4'
                                AND json_extract(
                                    NEW.payload_json, '$.effect_key'
                                ) = NEW.effect_key
                                AND json_extract(
                                    NEW.payload_json,
                                    '$.semantic_payload_sha256'
                                ) = NEW.payload_sha256
                            )
                            OR (
                                json_extract(
                                    NEW.payload_json, '$.schema_version'
                                ) IN (
                                    'pnc_rca_terminal_delivery_effect_v1',
                                    'pnc_rca_terminal_delivery_effect_v2',
                                    'pnc_rca_terminal_delivery_effect_v3',
                                    'pnc_rca_terminal_delivery_effect_v4',
                                    'pnc_rca_terminal_delivery_effect_v5'
                                )
                                AND json_extract(
                                    NEW.payload_json, '$.submission_key'
                                ) = job.submission_key
                                AND json_extract(
                                    NEW.payload_json, '$.generation'
                                ) = job.generation
                            )
                        )
                        AND EXISTS (
                            SELECT 1
                              FROM rca_owner_authorized_rerun_delivery_authorities AS authority
                              JOIN rca_execution_watch AS authority_watch
                                ON authority_watch.submission_key =
                                   job.submission_key
                              JOIN rca_outbox AS authority_outbox
                                ON authority_outbox.outbox_id =
                                   authority_watch.submission_outbox_id
                               AND authority_outbox.business_key =
                                   job.business_key
                               AND authority_outbox.generation = job.generation
                               AND authority_outbox.submission_key =
                                   job.submission_key
                             WHERE authority.business_key = job.business_key
                               AND authority.generation = job.generation
                               AND authority.submission_key = job.submission_key
                               AND authority.outbox_id =
                                   authority_outbox.outbox_id
                               AND authority.activation_epoch_id =
                                   authority_outbox.activation_epoch_id
                               AND authority.activation_ledger_id =
                                   authority_outbox.activation_ledger_id
                               AND authority.project_key = bt.project_key
                               AND authority.work_item_type_key =
                                   bt.work_item_type_key
                               AND authority.project_simple_name = json_extract(
                                   bt.normalized_json, '$.project_simple_name'
                               )
                               AND authority.issue_id = job.work_item_id
                               AND authority.effect_kind = NEW.effect_kind
                               AND authority.activation_required = 1
                        )
                    )
             )
            BEGIN
                SELECT RAISE(ABORT, 'learning_lane_admission_missing');
            END
            """
        )
        if RcaDeliveryStore._table_exists(conn, "rca_delivery_subscriptions"):
            statements.extend(
                (
                    """
                    CREATE TRIGGER IF NOT EXISTS
                        trg_learning_lane_stock_subscription_insert_forbidden
                    BEFORE INSERT ON rca_delivery_subscriptions
                    WHEN NEW.effect_kind LIKE 'feishu_%'
                     AND EXISTS (
                         SELECT 1
                           FROM business_triggers AS bt
                           JOIN rca_learning_lane_cohorts AS cohort
                           JOIN rca_learning_lane_stock_items AS item
                             ON item.cohort_id = cohort.cohort_id
                            AND item.work_item_id = bt.work_item_id
                          WHERE bt.business_key = NEW.business_key
                            AND bt.generation = NEW.generation
                            AND (
                                julianday(bt.created_at) IS NULL
                                OR julianday(cohort.stock_cutoff) IS NULL
                                OR julianday(bt.created_at) >
                                   julianday(cohort.stock_cutoff)
                            )
                            AND NOT EXISTS (
                                SELECT 1
                                  FROM rca_learning_lane_admissions AS admission
                                 WHERE admission.business_key = NEW.business_key
                                   AND admission.generation = NEW.generation
                                   AND admission.lane = 'learning'
                                   AND admission.external_write_allowed = 0
                            )
                            AND NOT (
                                NEW.effect_kind = 'feishu_issue_comment'
                                AND NEW.target_key =
                                    'feishu_project:' || bt.project_key || ':' ||
                                    bt.work_item_type_key || ':' || bt.work_item_id
                                AND json_extract(
                                    NEW.target_json, '$.platform'
                                ) = 'feishu_project'
                                AND json_extract(
                                    NEW.target_json, '$.project_key'
                                ) = bt.project_key
                                AND json_extract(
                                    NEW.target_json, '$.work_item_type_key'
                                ) = bt.work_item_type_key
                                AND json_extract(
                                    NEW.target_json, '$.work_item_id'
                                ) = bt.work_item_id
                                AND EXISTS (
                                    SELECT 1
                                      FROM rca_owner_authorized_rerun_delivery_authorities
                                           AS authority
                                      JOIN rca_outbox AS authority_outbox
                                        ON authority_outbox.outbox_id =
                                           authority.outbox_id
                                       AND authority_outbox.business_key =
                                           authority.business_key
                                       AND authority_outbox.generation =
                                           authority.generation
                                       AND authority_outbox.submission_key =
                                           authority.submission_key
                                       AND authority_outbox.activation_epoch_id =
                                           authority.activation_epoch_id
                                       AND authority_outbox.activation_ledger_id =
                                           authority.activation_ledger_id
                                     WHERE authority.business_key = NEW.business_key
                                       AND authority.generation = NEW.generation
                                       AND authority.submission_key =
                                           bt.submission_key
                                       AND authority.project_key = bt.project_key
                                       AND authority.work_item_type_key =
                                           bt.work_item_type_key
                                       AND authority.project_simple_name =
                                           json_extract(
                                               bt.normalized_json,
                                               '$.project_simple_name'
                                           )
                                       AND authority.issue_id = bt.work_item_id
                                       AND authority.effect_kind = NEW.effect_kind
                                       AND authority.activation_required = 1
                                )
                            )
                     )
                    BEGIN
                        SELECT RAISE(ABORT, 'learning_lane_admission_missing');
                    END
                    """,
                    """
                    CREATE TRIGGER IF NOT EXISTS
                        trg_learning_lane_stock_subscription_update_forbidden
                    BEFORE UPDATE OF business_key, generation, effect_kind
                        ON rca_delivery_subscriptions
                    WHEN NEW.effect_kind LIKE 'feishu_%'
                     AND EXISTS (
                         SELECT 1
                           FROM business_triggers AS bt
                           JOIN rca_learning_lane_cohorts AS cohort
                           JOIN rca_learning_lane_stock_items AS item
                             ON item.cohort_id = cohort.cohort_id
                            AND item.work_item_id = bt.work_item_id
                          WHERE bt.business_key = NEW.business_key
                            AND bt.generation = NEW.generation
                            AND (
                                julianday(bt.created_at) IS NULL
                                OR julianday(cohort.stock_cutoff) IS NULL
                                OR julianday(bt.created_at) >
                                   julianday(cohort.stock_cutoff)
                            )
                            AND NOT EXISTS (
                                SELECT 1
                                  FROM rca_learning_lane_admissions AS admission
                                 WHERE admission.business_key = NEW.business_key
                                   AND admission.generation = NEW.generation
                                   AND admission.lane = 'learning'
                                   AND admission.external_write_allowed = 0
                            )
                            AND NOT (
                                NEW.effect_kind = 'feishu_issue_comment'
                                AND NEW.target_key =
                                    'feishu_project:' || bt.project_key || ':' ||
                                    bt.work_item_type_key || ':' || bt.work_item_id
                                AND json_extract(
                                    NEW.target_json, '$.platform'
                                ) = 'feishu_project'
                                AND json_extract(
                                    NEW.target_json, '$.project_key'
                                ) = bt.project_key
                                AND json_extract(
                                    NEW.target_json, '$.work_item_type_key'
                                ) = bt.work_item_type_key
                                AND json_extract(
                                    NEW.target_json, '$.work_item_id'
                                ) = bt.work_item_id
                                AND EXISTS (
                                    SELECT 1
                                      FROM rca_owner_authorized_rerun_delivery_authorities
                                           AS authority
                                      JOIN rca_outbox AS authority_outbox
                                        ON authority_outbox.outbox_id =
                                           authority.outbox_id
                                       AND authority_outbox.business_key =
                                           authority.business_key
                                       AND authority_outbox.generation =
                                           authority.generation
                                       AND authority_outbox.submission_key =
                                           authority.submission_key
                                       AND authority_outbox.activation_epoch_id =
                                           authority.activation_epoch_id
                                       AND authority_outbox.activation_ledger_id =
                                           authority.activation_ledger_id
                                     WHERE authority.business_key = NEW.business_key
                                       AND authority.generation = NEW.generation
                                       AND authority.submission_key =
                                           bt.submission_key
                                       AND authority.project_key = bt.project_key
                                       AND authority.work_item_type_key =
                                           bt.work_item_type_key
                                       AND authority.project_simple_name =
                                           json_extract(
                                               bt.normalized_json,
                                               '$.project_simple_name'
                                           )
                                       AND authority.issue_id = bt.work_item_id
                                       AND authority.effect_kind = NEW.effect_kind
                                       AND authority.activation_required = 1
                                )
                            )
                     )
                    BEGIN
                        SELECT RAISE(ABORT, 'learning_lane_admission_missing');
                    END
                    """,
                )
            )
        return tuple(statements)

    @staticmethod
    def _install_w6_effect_guards(conn: sqlite3.Connection) -> None:
        """Install exact DB-level W6 backstops for the shared control schema."""
        for statement in RcaDeliveryStore._w6_effect_guard_statements(conn):
            conn.execute(statement)

    @staticmethod
    def _validate_comment_slot_schema(conn: sqlite3.Connection) -> None:
        required_columns = {
            "comment_slot_budget_exempt",
            "comment_slot_generation",
            "comment_slot_key",
            "comment_slot_kind",
            "comment_slot_revision",
            "comment_slot_schema_version",
        }
        columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(rca_delivery_effects)")
        }
        if not required_columns.issubset(columns):
            raise RuntimeError(
                "incompatible_delivery_store_schema:comment_slot_columns"
            )
        index = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='index' "
            "AND name='idx_delivery_effects_comment_slot'"
        ).fetchone()
        normalized_index = " ".join(str(index["sql"] or "").lower().split()) if index else ""
        if (
            "unique index" not in normalized_index
            or "comment_slot_key" not in normalized_index
            or "where comment_slot_key != ''" not in normalized_index
        ):
            raise RuntimeError(
                "incompatible_delivery_store_schema:comment_slot_index"
            )
        incoherent = conn.execute(
            """
            SELECT 1 FROM rca_delivery_effects
             WHERE (
                    comment_slot_schema_version = ''
                AND (
                       comment_slot_key != '' OR comment_slot_kind != ''
                    OR comment_slot_generation IS NOT NULL
                    OR comment_slot_revision IS NOT NULL
                    OR comment_slot_budget_exempt != 0
                )
             ) OR (
                    comment_slot_schema_version = ?
                AND (
                       effect_kind != 'feishu_issue_comment'
                    OR comment_slot_key = ''
                    OR comment_slot_kind NOT IN ('conclusion', 'correction')
                    OR comment_slot_generation IS NULL
                    OR comment_slot_revision IS NULL
                )
             ) OR comment_slot_schema_version NOT IN ('', ?)
             LIMIT 1
            """,
            (COMMENT_SLOT_SCHEMA_VERSION, COMMENT_SLOT_SCHEMA_VERSION),
        ).fetchone()
        if incoherent is not None:
            raise RuntimeError(
                "incompatible_delivery_store_schema:comment_slot_rows"
            )

    @staticmethod
    def _validate_w6_effect_guards(conn: sqlite3.Connection) -> None:
        """Require the exact W6 trigger bodies, including correction authority."""
        w6_authority_tables = {
            "rca_learning_lane_cohorts",
            "rca_learning_lane_stock_items",
            "rca_learning_lane_admissions",
        }
        present = {
            table
            for table in w6_authority_tables
            if RcaDeliveryStore._table_exists(conn, table)
        }
        # Delivery-only test/migration databases predate W6 entirely.
        if not present:
            return
        if present != w6_authority_tables or not RcaDeliveryStore._table_exists(
            conn, "business_triggers"
        ):
            raise RuntimeError(
                "incompatible_delivery_store_schema:w6_authority_tables"
            )
        normalize_sql = lambda value: " ".join(str(value).lower().split()).rstrip(
            ";"
        )
        expected: dict[str, str] = {}
        for statement in RcaDeliveryStore._w6_effect_guard_statements(conn):
            normalized = normalize_sql(statement)
            prefix = "create trigger if not exists "
            if not normalized.startswith(prefix):
                raise RuntimeError(
                    "incompatible_delivery_store_schema:w6_trigger_definition"
                )
            name = normalized[len(prefix) :].split(" ", 1)[0]
            expected[name] = normalized.replace(
                prefix, "create trigger ", 1
            )
        observed = {
            str(row["name"]): normalize_sql(row["sql"] or "")
            for row in conn.execute(
                "SELECT name, sql FROM sqlite_master WHERE type = 'trigger'"
            ).fetchall()
            if str(row["name"]) in expected
        }
        if observed != expected:
            mismatched = sorted(
                name
                for name in set(expected) | set(observed)
                if expected.get(name) != observed.get(name)
            )
            detail = mismatched[0] if mismatched else "unknown"
            raise RuntimeError(
                f"incompatible_delivery_store_schema:w6_trigger:{detail}"
            )

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

    def record_conclusion_adjudication(
        self,
        *,
        work_item_id: str,
        action: Literal["retract", "recognize"],
        reason: str,
        actor_id: str,
        actor_name: str = "",
        source: Mapping[str, Any],
        replacement_conclusion: str = "",
        original_effect_key: str = "",
        now: datetime | None = None,
    ) -> ConclusionAdjudicationResult:
        """Atomically invalidate/recognize a conclusion and enqueue its comment."""
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            result = record_conclusion_adjudication_tx(
                conn,
                work_item_id=work_item_id,
                action=action,
                reason=reason,
                actor_id=actor_id,
                actor_name=actor_name,
                source=source,
                replacement_conclusion=replacement_conclusion,
                original_effect_key=original_effect_key,
                now=now,
            )
            conn.commit()
            return result
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def list_conclusion_review_queue(
        self,
        *,
        limit: int = 20,
    ) -> tuple[ConclusionReviewQueueItem, ...]:
        """List recomputed medium-confidence conclusions awaiting owner review."""

        conn = self._connect_read_only()
        try:
            return list_conclusion_review_queue_tx(conn, limit=limit)
        finally:
            conn.close()

    def record_conclusion_adjudications(
        self,
        *,
        work_item_ids: Sequence[str],
        action: Literal["retract", "recognize"],
        reason: str,
        actor_id: str,
        actor_name: str = "",
        source: Mapping[str, Any],
        require_medium_candidate: bool = True,
        now: datetime | None = None,
    ) -> tuple[ConclusionAdjudicationResult, ...]:
        """Atomically record one bounded batch of owner adjudications."""

        issue_ids = tuple(str(item or "").strip() for item in work_item_ids)
        if (
            not issue_ids
            or len(issue_ids) > 50
            or len(set(issue_ids)) != len(issue_ids)
        ):
            raise ConclusionAdjudicationError(
                "conclusion_adjudication_batch_invalid"
            )
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            results = tuple(
                record_conclusion_adjudication_tx(
                    conn,
                    work_item_id=work_item_id,
                    action=action,
                    reason=reason,
                    actor_id=actor_id,
                    actor_name=actor_name,
                    source=source,
                    require_medium_candidate=require_medium_candidate,
                    now=now,
                )
                for work_item_id in issue_ids
            )
            conn.commit()
            return results
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    @staticmethod
    def _card_patch_binding_row_tx(
        conn: sqlite3.Connection,
        *,
        adjudication_id: str,
    ) -> sqlite3.Row:
        row = conn.execute(
            """
            SELECT a.*,
                   original.delivery_id AS original_effect_delivery_id,
                   original.effect_kind AS original_effect_kind,
                   original.required AS original_effect_required,
                   original.status AS original_effect_status,
                   original.write_phase AS original_effect_write_phase,
                   correction.delivery_id AS correction_effect_delivery_id,
                   correction.effect_kind AS correction_effect_kind,
                   correction.required AS correction_effect_required,
                   correction.status AS correction_effect_status,
                   correction.write_phase AS correction_effect_write_phase,
                   job.business_key AS job_business_key,
                   job.submission_key AS job_submission_key,
                   job.generation AS job_generation,
                   job.project_key AS job_project_key,
                   job.work_item_type_key AS job_work_item_type_key,
                   job.work_item_id AS job_work_item_id,
                   epoch.state AS activation_state,
                   epoch.is_current AS activation_is_current
              FROM rca_conclusion_adjudications AS a
              JOIN rca_delivery_effects AS original
                ON original.effect_key = a.original_effect_key
              JOIN rca_delivery_effects AS correction
                ON correction.effect_key = a.correction_effect_key
              JOIN rca_delivery_jobs AS job
                ON job.delivery_id = a.original_delivery_id
         LEFT JOIN rca_activation_epochs AS epoch
                ON epoch.epoch_id = a.activation_epoch_id
             WHERE a.adjudication_id = ?
            """,
            (adjudication_id,),
        ).fetchone()
        if row is None:
            raise DeliveryRecordConflictError(
                "delivery_card_patch_adjudication_binding_missing"
            )
        return row

    @staticmethod
    def _validate_card_patch_binding_row(
        row: sqlite3.Row,
        *,
        payload: Mapping[str, Any],
        require_current_activation: bool = True,
    ) -> None:
        expected = {
            "business_key": payload.get("business_key"),
            "generation": payload.get("generation"),
            "project_key": payload.get("project_key"),
            "work_item_type_key": payload.get("work_item_type_key"),
            "work_item_id": payload.get("work_item_id"),
            "action": payload.get("action"),
            "conclusion_state": payload.get("conclusion_state"),
            "original_delivery_id": payload.get("delivery_id"),
            "original_effect_key": payload.get("original_effect_key"),
            "correction_effect_key": payload.get("correction_effect_key"),
        }
        if any(row[key] != value for key, value in expected.items()) or any(
            row[key] != value
            for key, value in {
                "original_effect_delivery_id": payload.get("delivery_id"),
                "correction_effect_delivery_id": payload.get("delivery_id"),
                "job_business_key": payload.get("business_key"),
                "job_submission_key": payload.get("submission_key"),
                "job_generation": payload.get("generation"),
                "job_project_key": payload.get("project_key"),
                "job_work_item_type_key": payload.get("work_item_type_key"),
                "job_work_item_id": payload.get("work_item_id"),
            }.items()
        ):
            raise DeliveryRecordConflictError(
                "delivery_card_patch_adjudication_binding_invalid"
            )
        if (
            row["original_effect_kind"] != DELIVERY_EFFECT_KIND
            or int(row["original_effect_required"]) != 1
            or row["original_effect_status"] != "succeeded"
            or row["original_effect_write_phase"] != "settled"
            or row["correction_effect_kind"] != DELIVERY_EFFECT_KIND
            or int(row["correction_effect_required"]) != 1
            or row["correction_effect_status"] != "succeeded"
            or row["correction_effect_write_phase"] != "settled"
        ):
            raise DeliveryRecordConflictError(
                "delivery_card_patch_correction_not_settled"
            )
        if require_current_activation and (
            row["activation_is_current"] != 1
            or row["activation_state"] != "steady_active"
        ):
            raise DeliveryRecordConflictError(
                "delivery_card_patch_activation_stale"
            )

    @classmethod
    def _validate_card_patch_binding_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        payload: Mapping[str, Any],
        require_current_activation: bool = True,
    ) -> None:
        row = cls._card_patch_binding_row_tx(
            conn,
            adjudication_id=str(payload.get("adjudication_id") or ""),
        )
        cls._validate_card_patch_binding_row(
            row,
            payload=payload,
            require_current_activation=require_current_activation,
        )

    def card_patch_materialization_binding(
        self,
        *,
        adjudication_id: str,
        action: str,
        conclusion_state: str,
        business_key: str,
        submission_key: str,
        generation: int,
        work_item_id: str,
        original_effect_key: str,
        correction_effect_key: str,
        require_current_activation: bool = True,
    ) -> dict[str, Any]:
        """Resolve one sidecar adjudication to immutable delivery identity."""

        values = {
            "adjudication_id": str(adjudication_id or "").strip(),
            "action": str(action or "").strip(),
            "conclusion_state": str(conclusion_state or "").strip(),
            "business_key": str(business_key or "").strip(),
            "submission_key": str(submission_key or "").strip(),
            "generation": generation,
            "work_item_id": str(work_item_id or "").strip(),
            "original_effect_key": str(original_effect_key or "").strip(),
            "correction_effect_key": str(correction_effect_key or "").strip(),
        }
        if (
            not all(value for key, value in values.items() if key != "generation")
            or isinstance(generation, bool)
            or not isinstance(generation, int)
            or generation < 1
            or {"recognize": "recognized", "retract": "invalidated"}.get(
                values["action"]
            )
            != values["conclusion_state"]
        ):
            raise DeliveryRecordConflictError(
                "delivery_card_patch_materialization_identity_invalid"
            )
        conn = self._connect()
        try:
            conn.execute("BEGIN")
            row = self._card_patch_binding_row_tx(
                conn,
                adjudication_id=values["adjudication_id"],
            )
            payload = {
                **values,
                "delivery_id": str(row["original_delivery_id"]),
                "project_key": str(row["job_project_key"]),
                "work_item_type_key": str(row["job_work_item_type_key"]),
            }
            self._validate_card_patch_binding_row(
                row,
                payload=payload,
                require_current_activation=require_current_activation,
            )
            conn.commit()
            return payload
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def card_patch_effect_state(
        self,
        *,
        delivery_id: str,
        target_key: str,
        adjudication_id: str,
    ) -> dict[str, Any] | None:
        """Read and validate the one durable card-effect slot for a target."""

        conn = self._connect()
        try:
            conn.execute("BEGIN")
            row = conn.execute(
                """
                SELECT effect_key, delivery_id, effect_kind, required, target_key,
                       payload_json, payload_sha256, outcome, status, write_phase,
                       remote_receipt_json, completed_at
                  FROM rca_delivery_effects
                 WHERE delivery_id = ? AND effect_kind = ? AND target_key = ?
                 LIMIT 1
                """,
                (
                    str(delivery_id or "").strip(),
                    DELIVERY_CARD_PATCH_EFFECT_KIND,
                    str(target_key or "").strip(),
                ),
            ).fetchone()
            if row is None:
                conn.commit()
                return None
            try:
                payload = json.loads(str(row["payload_json"] or ""))
            except (TypeError, json.JSONDecodeError) as exc:
                raise DeliveryRecordConflictError(
                    "delivery_card_patch_effect_state_invalid"
                ) from exc
            validated = validate_card_patch_effect_payload(payload)
            if (
                str(row["effect_key"]) != validated["effect_key"]
                or str(row["delivery_id"]) != validated["delivery_id"]
                or str(row["effect_kind"]) != DELIVERY_CARD_PATCH_EFFECT_KIND
                or int(row["required"]) != 1
                or str(row["target_key"]) != validated["target_key"]
                or str(row["payload_json"]) != _canonical_json(validated)
                or str(row["payload_sha256"])
                != validated["semantic_payload_sha256"]
                or str(row["outcome"] or "success") != "success"
                or validated["adjudication_id"]
                != str(adjudication_id or "").strip()
            ):
                raise DeliveryRecordConflictError(
                    "delivery_card_patch_effect_state_invalid"
                )
            receipt = _json_object(row["remote_receipt_json"])
            status = str(row["status"] or "")
            write_phase = str(row["write_phase"] or "")
            self._validate_card_patch_binding_tx(
                conn,
                payload=validated,
                require_current_activation=status
                not in {"succeeded", "suppressed", "quarantined"},
            )
            if status == "succeeded" and (
                write_phase != "settled"
                or not row["completed_at"]
                or receipt != _card_patch_success_receipt(validated)
            ):
                raise DeliveryRecordConflictError(
                    "delivery_card_patch_effect_receipt_invalid"
                )
            if status == "suppressed" and (
                write_phase != "settled"
                or not row["completed_at"]
                or receipt != _card_patch_suppression_receipt(validated)
            ):
                raise DeliveryRecordConflictError(
                    "delivery_card_patch_effect_receipt_invalid"
                )
            conn.commit()
            return {
                "effect_key": validated["effect_key"],
                "status": status,
                "write_phase": write_phase,
                "completed_at": (
                    str(row["completed_at"]) if row["completed_at"] else None
                ),
                "payload": validated,
                "remote_receipt": receipt,
            }
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def enqueue_card_patch_effect(
        self,
        *,
        payload: Mapping[str, Any],
        now: datetime | None = None,
    ) -> DeliveryCreateResult:
        """Enqueue one exact card patch after its correction is settled."""

        validated = validate_card_patch_effect_payload(payload)
        current = _iso(now)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            self._validate_card_patch_binding_tx(conn, payload=validated)
            self._require_learning_lane_guard_tx(
                conn,
                business_key=str(validated["business_key"]),
                generation=int(validated["generation"]),
                work_item_id=str(validated["work_item_id"]),
                effect_kind=DELIVERY_CARD_PATCH_EFFECT_KIND,
            )
            existing = conn.execute(
                """
                SELECT effect_key, delivery_id, effect_kind, required,
                       target_key, payload_json, payload_sha256, outcome
                 FROM rca_delivery_effects
                 WHERE effect_key = ?
                    OR (delivery_id = ? AND effect_kind = ?)
                 LIMIT 1
                """,
                (
                    validated["effect_key"],
                    validated["delivery_id"],
                    DELIVERY_CARD_PATCH_EFFECT_KIND,
                ),
            ).fetchone()
            canonical_payload = _canonical_json(validated)
            if existing is not None:
                if (
                    str(existing["effect_key"]) != validated["effect_key"]
                    or str(existing["delivery_id"]) != validated["delivery_id"]
                    or str(existing["effect_kind"])
                    != DELIVERY_CARD_PATCH_EFFECT_KIND
                    or int(existing["required"]) != 1
                    or str(existing["target_key"]) != validated["target_key"]
                    or str(existing["payload_json"]) != canonical_payload
                    or str(existing["payload_sha256"])
                    != validated["semantic_payload_sha256"]
                    or str(existing["outcome"] or "success") != "success"
                ):
                    raise DeliveryRecordConflictError(
                        "delivery_card_patch_effect_conflict"
                    )
                conn.commit()
                return DeliveryCreateResult(
                    delivery_id=str(validated["delivery_id"]),
                    effect_key=str(validated["effect_key"]),
                    created=False,
                )
            conn.execute(
                """
                INSERT INTO rca_delivery_effects(
                    effect_key, delivery_id, effect_kind, required, target_key,
                    payload_json, payload_sha256, outcome, status,
                    created_at, updated_at
                ) VALUES (?, ?, ?, 1, ?, ?, ?, 'success', 'pending', ?, ?)
                """,
                (
                    validated["effect_key"],
                    validated["delivery_id"],
                    DELIVERY_CARD_PATCH_EFFECT_KIND,
                    validated["target_key"],
                    canonical_payload,
                    validated["semantic_payload_sha256"],
                    current,
                    current,
                ),
            )
            self._aggregate_job_status(
                conn,
                str(validated["delivery_id"]),
                current,
            )
            conn.commit()
            return DeliveryCreateResult(
                delivery_id=str(validated["delivery_id"]),
                effect_key=str(validated["effect_key"]),
                created=True,
            )
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def validate_card_patch_effect_binding(
        self,
        *,
        claim: DeliveryEffectClaim,
        now: datetime | None = None,
    ) -> None:
        if claim.effect_kind != DELIVERY_CARD_PATCH_EFFECT_KIND:
            raise DeliveryRecordConflictError(
                "delivery_card_patch_effect_kind_invalid"
            )
        current = _iso(now)
        conn = self._connect()
        try:
            conn.execute("BEGIN")
            self._current_effect_claim(
                conn,
                claim=claim,
                current=current,
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def validate_adjudication_effect_binding(
        self,
        *,
        claim: DeliveryEffectClaim,
        now: datetime | None = None,
    ) -> None:
        """Verify an adjudication claim against its immutable ledger and epoch."""

        current = _iso(now)
        conn = self._connect()
        try:
            conn.execute("BEGIN")
            self._current_effect_claim(
                conn,
                claim=claim,
                current=current,
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def mark_conclusion_adjudication_artifact_repair(
        self,
        *,
        adjudication_id: str,
        succeeded: bool,
        receipt_binding: Mapping[str, Any] | None = None,
        error_code: str = "",
        error_detail: str = "",
        now: datetime | None = None,
    ) -> str:
        current = _iso(now)
        status = "succeeded" if succeeded else "pending"
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            repair_row = conn.execute(
                "SELECT status FROM rca_conclusion_adjudication_repairs "
                "WHERE adjudication_id = ?",
                (adjudication_id,),
            ).fetchone()
            if repair_row is None:
                raise DeliveryRecordConflictError(
                    "conclusion adjudication repair row is missing"
                )
            if not succeeded and str(repair_row["status"] or "") == "succeeded":
                conn.commit()
                return "succeeded"
            normalized_receipt: dict[str, Any] | None = None
            if succeeded:
                adjudication = conn.execute(
                    "SELECT * FROM rca_conclusion_adjudications "
                    "WHERE adjudication_id = ?",
                    (adjudication_id,),
                ).fetchone()
                if adjudication is None:
                    raise DeliveryRecordConflictError(
                        "conclusion adjudication repair row is missing"
                    )
                if receipt_binding is None:
                    raise DeliveryRecordConflictError(
                        "conclusion adjudication artifact receipt is required"
                    )
                normalized_receipt = (
                    validate_conclusion_adjudication_artifact_receipt(
                        receipt_binding,
                        adjudication=dict(adjudication),
                    )
                )
            updated = conn.execute(
                """
                UPDATE rca_conclusion_adjudication_repairs
                   SET status = ?, attempt_count = attempt_count + 1,
                       last_error_code = ?, last_error_detail = ?,
                       receipt_schema_version = ?, receipt_path = ?,
                       receipt_offset = ?, receipt_length = ?,
                       receipt_sha256 = ?, receipt_device = ?,
                       receipt_inode = ?, receipt_event_id = ?,
                       completed_at = CASE WHEN ? = 'succeeded' THEN ? ELSE NULL END,
                       updated_at = ?
                 WHERE adjudication_id = ?
                """,
                (
                    status,
                    "" if succeeded else str(error_code or "artifact_repair_failed")[:120],
                    "" if succeeded else str(error_detail or "")[:1000],
                    normalized_receipt["schema_version"] if normalized_receipt else "",
                    normalized_receipt["path"] if normalized_receipt else "",
                    normalized_receipt["offset"] if normalized_receipt else -1,
                    normalized_receipt["length"] if normalized_receipt else 0,
                    normalized_receipt["sha256"] if normalized_receipt else "",
                    normalized_receipt["device"] if normalized_receipt else 0,
                    normalized_receipt["inode"] if normalized_receipt else 0,
                    normalized_receipt["review_event_id"] if normalized_receipt else "",
                    status,
                    current,
                    current,
                    adjudication_id,
                ),
            )
            if updated.rowcount != 1:
                raise DeliveryRecordConflictError(
                    "conclusion adjudication repair row is missing"
                )
            conn.commit()
            return status
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def conclusion_adjudication_artifact_repair(
        self, adjudication_id: str
    ) -> dict[str, Any] | None:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM rca_conclusion_adjudication_repairs "
                "WHERE adjudication_id = ?",
                (adjudication_id,),
            ).fetchone()
            return dict(row) if row is not None else None
        finally:
            conn.close()

    @classmethod
    def _issued_w3_binding_for_quarantined_outbox_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        row: sqlite3.Row,
        current: str,
    ) -> tuple[dict[str, Any] | None, str, str]:
        """Return a live-bound W3 fence, or a fail-closed disposition code.

        A quarantined outbox can predate W3 and therefore have no immutable
        source snapshot at all.  Once an activation epoch is enforced, that
        history must never be upgraded into a public effect.  The check stays
        inside the caller's write transaction and only accepts the immutable
        snapshot/envelope pair whose issued fence is bound to this exact
        outbox identity and current epoch ledger.
        """
        required_tables = {
            "rca_admission_snapshots",
            "rca_snapshot_source_envelopes",
        }
        present_tables = {
            str(item["name"])
            for item in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        if not required_tables.issubset(present_tables):
            return None, OUTBOX_PRE_W3_QUARANTINE_MISSING_CODE, ""
        snapshot_row = conn.execute(
            """
            SELECT snapshot.*, envelope.source_envelope_json
              FROM rca_admission_snapshots AS snapshot
              JOIN rca_snapshot_source_envelopes AS envelope
                ON envelope.source_envelope_sha256 =
                   snapshot.creator_source_envelope_sha256
               AND envelope.source_authority_sha256 =
                   snapshot.creator_authority_sha256
               AND envelope.source_id = snapshot.creator_source_id
             WHERE snapshot.submission_key = ?
             LIMIT 1
            """,
            (str(row["submission_key"]),),
        ).fetchone()
        if snapshot_row is None:
            return None, OUTBOX_PRE_W3_QUARANTINE_MISSING_CODE, ""
        try:
            snapshot = json.loads(str(snapshot_row["admission_snapshot_json"] or ""))
            envelope = json.loads(str(snapshot_row["source_envelope_json"] or ""))
            if not isinstance(snapshot, Mapping) or not isinstance(envelope, Mapping):
                raise ValueError("w3_snapshot_mapping_invalid")
            resolved = snapshot.get("resolved_admission")
            if not isinstance(resolved, Mapping):
                raise TypeError("w3_snapshot_resolved_admission_invalid")
            if (
                snapshot.get("snapshot_sha256") != snapshot_row["snapshot_sha256"]
                or snapshot.get("snapshot_id") != snapshot_row["snapshot_id"]
                or snapshot.get("request_sha256") != snapshot_row["request_sha256"]
                or resolved.get("business_key") != row["business_key"]
                or resolved.get("submission_key") != row["submission_key"]
                or resolved.get("generation") != row["generation"]
                or snapshot_row["business_key"] != row["business_key"]
                or snapshot_row["submission_key"] != row["submission_key"]
                or int(snapshot_row["generation"]) != int(row["generation"])
                or snapshot_row["activation_epoch_id"]
                != row["activation_epoch_id"]
                or snapshot_row["activation_ledger_id"]
                != row["activation_ledger_id"]
                or snapshot_row["execution_decision"] != "admit"
                or int(snapshot_row["legacy_unconfigured"]) != 0
            ):
                raise ValueError("w3_snapshot_identity_mismatch")
            execution = snapshot.get("execution_admission")
            fence = snapshot.get("write_fence")
            request = snapshot.get("canonical_request")
            ticket = request.get("ticket") if isinstance(request, Mapping) else None
            if (
                not isinstance(execution, Mapping)
                or not isinstance(fence, Mapping)
                or not isinstance(ticket, Mapping)
            ):
                raise ValueError("w3_snapshot_fence_missing")
            if (
                execution.get("decision") != "admit"
                or execution.get("legacy_unconfigured") is True
                or execution.get("activation_epoch_id")
                != row["activation_epoch_id"]
                or execution.get("activation_ledger_id")
                != row["activation_ledger_id"]
                or fence.get("state") != "issued"
                or ticket.get("project_key") != row["project_key"]
                or ticket.get("work_item_type_key") != row["work_item_type_key"]
                or ticket.get("work_item_id") != row["work_item_id"]
            ):
                raise ValueError("w3_snapshot_fence_unissued")
            targets = validate_write_fence_source_binding(
                fence,
                snapshot=snapshot,
                source_envelope=envelope,
            )
            if not isinstance(targets, Mapping):
                raise TypeError("w3_snapshot_targets_invalid")
            issue_target = str(targets.get("issue_target") or "").strip()
            target_set_sha256 = str(
                targets.get("target_set_sha256") or ""
            ).strip()
            if not issue_target or not target_set_sha256:
                raise ValueError("w3_snapshot_target_mismatch")
            validate_write_fence(
                fence,
                snapshot=snapshot,
                operation="feishu_issue_comment",
                target=issue_target,
                expected_epoch_id=str(row["activation_epoch_id"]),
                expected_ledger_id=int(row["activation_ledger_id"]),
                expected_business_key=str(row["business_key"]),
                expected_submission_key=str(row["submission_key"]),
                expected_generation=int(row["generation"]),
                expected_issue_target=issue_target,
                expected_target_set_sha256=target_set_sha256,
                now=_parse_iso(current),
            )
            return {
                "write_fence": dict(fence),
                "snapshot_core_sha256": _snapshot_core_sha256(snapshot),
            }, "", issue_target
        except ExternalWriteFenceError:
            return None, OUTBOX_PRE_W3_QUARANTINE_INVALID_CODE, ""
        except (TypeError, ValueError, OverflowError):
            return None, OUTBOX_PRE_W3_QUARANTINE_INVALID_CODE, ""

    @classmethod
    def _manual_activation_issue_target_for_quarantined_outbox_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        row: sqlite3.Row,
    ) -> tuple[bool, str]:
        """Recognize manual admission authority without requiring a W3 snapshot.

        Manual Feishu executions intentionally use their immutable source and
        admission-ledger chain as the write authority. Kafka executions have no
        equivalent exception and must continue through the issued W3 fence path.
        """
        source = conn.execute(
            """
            SELECT source.source_kind, source.platform,
                   source.chat_id, source.thread_id, source.message_id,
                   source.requester_id, source.mode,
                   trigger.origin_source_id, trigger.normalized_json,
                   trigger.activation_epoch_id AS trigger_epoch_id,
                   trigger.activation_ledger_id AS trigger_ledger_id,
                   binding.business_key AS binding_business_key,
                   binding.generation AS binding_generation,
                   epoch.epoch_id, epoch.state AS epoch_state, epoch.is_current,
                   ledger.entrypoint, ledger.source_kind AS ledger_source_kind,
                   ledger.source_identity_sha256, ledger.decision,
                   ledger.bound_at, ledger.business_key AS ledger_business_key,
                   ledger.submission_key AS ledger_submission_key,
                   ledger.generation AS ledger_generation
              FROM rca_outbox AS outbox
              JOIN business_triggers AS trigger
                ON trigger.business_key = outbox.business_key
               AND trigger.submission_key = outbox.submission_key
               AND trigger.generation = outbox.generation
              JOIN rca_trigger_sources AS source
                ON source.source_id = trigger.origin_source_id
              JOIN rca_trigger_bindings AS binding
                ON binding.source_id = source.source_id
               AND binding.business_key = trigger.business_key
               AND binding.generation = trigger.generation
              JOIN rca_activation_epochs AS epoch
                ON epoch.is_current = 1
               AND epoch.epoch_id = outbox.activation_epoch_id
              JOIN rca_activation_admission_ledger AS ledger
                ON ledger.epoch_id = epoch.epoch_id
               AND ledger.ledger_id = outbox.activation_ledger_id
               AND ledger.business_key = outbox.business_key
               AND ledger.submission_key = outbox.submission_key
               AND ledger.generation = outbox.generation
             WHERE outbox.outbox_id = ?
            """,
            (int(row["outbox_id"]),),
        ).fetchone()
        if source is None:
            return False, ""
        if (
            str(source["source_kind"] or "") != "feishu_group_manual"
            or str(source["platform"] or "") != "feishu"
        ):
            return False, ""
        try:
            normalized = json.loads(str(source["normalized_json"] or ""))
            if not isinstance(normalized, Mapping):
                raise ValueError("manual_trigger_context_invalid")
            issue_url = str(normalized.get("issue_url") or "").strip()
            if not issue_url:
                raise ValueError("manual_trigger_issue_url_missing")
            source_identity = {
                "chat_id": str(source["chat_id"] or "").strip(),
                "thread_id": str(source["thread_id"] or "").strip(),
                "message_id": str(source["message_id"] or "").strip(),
                "requester_id": str(source["requester_id"] or "").strip(),
                "issue_url": issue_url,
                "mode": str(source["mode"] or "").strip(),
            }
            if not all(source_identity.values()):
                raise ValueError("manual_source_identity_invalid")
            if (
                str(source["trigger_epoch_id"] or "") != str(row["activation_epoch_id"] or "")
                or int(source["trigger_ledger_id"] or 0)
                != int(row["activation_ledger_id"] or 0)
                or str(source["epoch_state"] or "")
                not in ACTIVATION_DELIVERY_STATES
                or int(source["is_current"] or 0) != 1
                or str(source["entrypoint"] or "") != "manual_admit"
                or str(source["ledger_source_kind"] or "") != "manual"
                or str(source["decision"] or "") not in {"admit", "join"}
                or not str(source["bound_at"] or "").strip()
                or str(source["binding_business_key"] or "") != str(row["business_key"])
                or int(source["binding_generation"] or 0) != int(row["generation"])
                or str(source["ledger_business_key"] or "") != str(row["business_key"])
                or str(source["ledger_submission_key"] or "") != str(row["submission_key"])
                or int(source["ledger_generation"] or 0) != int(row["generation"])
                or hashlib.sha256(_canonical_json(source_identity).encode("utf-8")).hexdigest()
                != str(source["source_identity_sha256"] or "")
            ):
                raise ValueError("manual_activation_binding_mismatch")
            return True, issue_url
        except (TypeError, ValueError, OverflowError, json.JSONDecodeError):
            return True, ""

    @classmethod
    def _stored_profile_terminal_issue_target_for_quarantined_outbox_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        row: sqlite3.Row,
    ) -> tuple[bool, str, str, str]:
        """Validate a Kafka profile terminal without a W3 execution snapshot.

        Snapshot-only profile observations are sufficient for a neutral
        out-of-scope terminal.  They are never sufficient for VM submission
        or a successful external write.  The source/ledger joins keep this
        exception bound to the exact current activation admission.
        """
        source_error = str(row["last_error_code"] or "").strip()
        if source_error not in OUTBOX_PROFILE_TERMINAL_WRITE_ERROR_CODES:
            return False, "", "", ""
        source = conn.execute(
            """
            SELECT source.source_id, source.source_kind, source.kafka_event_uid,
                   source.mode, source.payload_sha256,
                   trigger.normalized_json, inbox.normalized_json AS inbox_normalized_json,
                   inbox.raw_sha256 AS inbox_raw_sha256,
                   trigger.activation_epoch_id AS trigger_epoch_id,
                   trigger.activation_ledger_id AS trigger_ledger_id,
                   binding.business_key AS binding_business_key,
                   binding.generation AS binding_generation,
                   epoch.epoch_id, epoch.state AS epoch_state, epoch.is_current,
                   ledger.entrypoint, ledger.source_kind AS ledger_source_kind,
                   ledger.decision, ledger.bound_at,
                   ledger.business_key AS ledger_business_key,
                   ledger.submission_key AS ledger_submission_key,
                   ledger.generation AS ledger_generation
              FROM rca_outbox AS outbox
              JOIN business_triggers AS trigger
                ON trigger.business_key = outbox.business_key
               AND trigger.generation = outbox.generation
              JOIN rca_trigger_sources AS source
                ON source.source_id = trigger.origin_source_id
              JOIN kafka_inbox AS inbox
                ON inbox.event_uid = outbox.source_event_id
              JOIN rca_trigger_bindings AS binding
                ON binding.source_id = source.source_id
               AND binding.business_key = trigger.business_key
               AND binding.generation = trigger.generation
              JOIN rca_activation_epochs AS epoch
                ON epoch.is_current = 1
               AND epoch.epoch_id = outbox.activation_epoch_id
              JOIN rca_activation_admission_ledger AS ledger
                ON ledger.epoch_id = epoch.epoch_id
               AND ledger.ledger_id = outbox.activation_ledger_id
               AND ledger.business_key = outbox.business_key
               AND ledger.submission_key = outbox.submission_key
               AND ledger.generation = outbox.generation
             WHERE outbox.outbox_id = ?
            """,
            (int(row["outbox_id"]),),
        ).fetchone()
        if source is None:
            return True, "", "", OUTBOX_PROFILE_TERMINAL_BINDING_INVALID_CODE
        try:
            normalized = json.loads(str(source["normalized_json"] or ""))
            resolution = normalized["business_profile_resolution"]
            if not isinstance(normalized, Mapping) or not isinstance(
                resolution, Mapping
            ):
                raise ValueError("profile_terminal_observation_invalid")
            status = str(resolution.get("status") or "").strip()
            if source_error == "business_profile_adapter_not_ready":
                if (
                    status != "matched"
                    or str(resolution.get("execution_readiness") or "").strip()
                    != "input_adapter_pending"
                    or not str(resolution.get("profile_id") or "").strip()
                ):
                    raise ValueError("profile_terminal_adapter_contract_invalid")
            elif f"business_profile_{status}" != source_error:
                raise ValueError("profile_terminal_error_code_mismatch")
            issue_url = str(normalized.get("issue_url") or "").strip()
            project_simple_name = str(
                normalized.get("project_simple_name") or ""
            ).strip()
            work_item_id = str(normalized.get("work_item_id") or "").strip()
            expected_issue_url = (
                f"https://project.feishu.cn/{project_simple_name}/issue/detail/"
                f"{work_item_id}"
            )
            option_ids = resolution.get("project_option_ids")
            if (
                not isinstance(option_ids, list)
                or not option_ids
                or any(
                    not isinstance(option_id, str) or not option_id.strip()
                    for option_id in option_ids
                )
                or len(set(option_ids)) != len(option_ids)
                or option_ids != sorted(option_ids)
            ):
                raise ValueError("profile_terminal_project_options_missing")
            if (
                normalized.get("schema_version")
                != "pnc_rca_workflow_event_v1"
                or str(normalized.get("creation_rule_version") or "")
                != str(row["creation_rule_version"] or "")
                or normalized.get("business_profile_observed") is not True
                or resolution.get("routing_field_key") != "field_052f23"
                or resolution.get("registry_version") != "rca_business_profiles_v1"
                or str(resolution.get("project_key") or "")
                != str(normalized.get("project_key") or "")
                or str(resolution.get("work_item_type_key") or "")
                != str(normalized.get("work_item_type_key") or "")
                or str(resolution.get("project_key") or "")
                != str(row["project_key"] or "")
                or str(resolution.get("work_item_type_key") or "")
                != str(row["work_item_type_key"] or "")
                or str(normalized.get("project_key") or "")
                != str(row["project_key"] or "")
                or str(normalized.get("work_item_type_key") or "")
                != str(row["work_item_type_key"] or "")
                or work_item_id != str(row["work_item_id"] or "")
                or not _FEISHU_ISSUE_URL_RE.fullmatch(issue_url)
                or issue_url.rstrip("/") != expected_issue_url.rstrip("/")
                or str(source["source_kind"] or "")
                != "kafka_workflow_event"
                or str(source["payload_sha256"] or "")
                != str(source["inbox_raw_sha256"] or "")
                or str(source["normalized_json"] or "")
                != str(source["inbox_normalized_json"] or "")
                or str(source["source_id"] or "")
                != str(row["origin_source_id"] or "")
                or (
                    int(row["generation"]) == 1
                    and str(source["kafka_event_uid"] or "")
                    != str(row["source_event_id"] or "")
                )
                or (
                    int(row["generation"]) >= 2
                    and str(source["kafka_event_uid"] or "")
                )
                or str(source["mode"] or "")
                != ("issue_created" if int(row["generation"]) == 1 else "kafka_retrigger")
                or str(source["trigger_epoch_id"] or "")
                != str(row["activation_epoch_id"] or "")
                or int(source["trigger_ledger_id"] or 0)
                != int(row["activation_ledger_id"] or 0)
                or str(source["epoch_state"] or "")
                not in ACTIVATION_DELIVERY_STATES
                or int(source["is_current"] or 0) != 1
                or str(source["entrypoint"] or "") != "kafka_ingest"
                or str(source["ledger_source_kind"] or "") != "kafka"
                or str(source["decision"] or "") not in {"admit", "join"}
                or not str(source["bound_at"] or "").strip()
                or str(source["binding_business_key"] or "")
                != str(row["business_key"] or "")
                or int(source["binding_generation"] or 0)
                != int(row["generation"])
                or str(source["ledger_business_key"] or "")
                != str(row["business_key"] or "")
                or str(source["ledger_submission_key"] or "")
                != str(row["submission_key"] or "")
                or int(source["ledger_generation"] or 0)
                != int(row["generation"])
            ):
                raise ValueError("profile_terminal_binding_mismatch")
            return True, issue_url, project_simple_name, source_error
        except (KeyError, TypeError, ValueError, OverflowError, json.JSONDecodeError):
            return True, "", "", OUTBOX_PROFILE_TERMINAL_BINDING_INVALID_CODE

    def kafka_profile_scope_error_for_lineage(
        self,
        *,
        business_key: str,
        generation: int,
        submission_key: str = "",
    ) -> str | None:
        """Fence historical non-G1Q3 effects at the provider boundary."""
        conn = self._connect_read_only()
        try:
            row = conn.execute(
                """
                SELECT source.source_kind, source.payload_sha256,
                       source.kafka_event_uid,
                       trigger.normalized_json,
                       trigger.creation_rule_version,
                       trigger.source_event_id, trigger.source_topic,
                       trigger.source_partition, trigger.source_offset,
                       inbox.event_uid, inbox.topic,
                       inbox.partition_id, inbox.offset_id,
                       inbox.raw_sha256,
                       inbox.normalized_json AS inbox_normalized_json,
                       inbox.creation_rule_version AS inbox_rule_version
                  FROM business_triggers AS trigger
                  JOIN rca_trigger_sources AS source
                    ON source.source_id = trigger.origin_source_id
                  LEFT JOIN kafka_inbox AS inbox
                    ON inbox.event_uid = trigger.source_event_id
                 WHERE trigger.business_key = ?
                   AND trigger.generation = ?
                   AND (? = '' OR trigger.submission_key = ?)
                 LIMIT 1
                """,
                (
                    str(business_key),
                    int(generation),
                    str(submission_key or ""),
                    str(submission_key or ""),
                ),
            ).fetchone()
            if row is None or str(row["source_kind"] or "") != "kafka_workflow_event":
                return None
            try:
                normalized = json.loads(str(row["normalized_json"] or ""))
            except (TypeError, ValueError, json.JSONDecodeError):
                return G1Q3_KAFKA_SCOPE_ERROR_CODE
            if not isinstance(normalized, Mapping):
                return G1Q3_KAFKA_SCOPE_ERROR_CODE
            rules = (
                str(row["creation_rule_version"] or ""),
                str(row["inbox_rule_version"] or ""),
                str(normalized.get("creation_rule_version") or ""),
            )
            binding_valid = (
                str(row["source_event_id"] or "")
                == str(row["event_uid"] or "")
                and str(row["kafka_event_uid"] or "")
                == str(row["event_uid"] or "")
                and str(row["source_topic"] or "") == str(row["topic"] or "")
                and row["source_partition"] == row["partition_id"]
                and row["source_offset"] == row["offset_id"]
                and str(row["payload_sha256"] or "")
                == str(row["raw_sha256"] or "")
                and str(row["normalized_json"] or "")
                == str(row["inbox_normalized_json"] or "")
            )
            if G1Q3_KAFKA_POLICY_VERSION not in rules:
                return (
                    None
                    if binding_valid and bool(rules[0]) and len(set(rules)) == 1
                    else G1Q3_KAFKA_SCOPE_ERROR_CODE
                )
            if not binding_valid or any(
                rule != G1Q3_KAFKA_POLICY_VERSION for rule in rules
            ):
                return G1Q3_KAFKA_SCOPE_ERROR_CODE
            resolution = normalized.get("business_profile_resolution")
            return (
                None
                if is_g1q3_kafka_profile_resolution(resolution)
                else G1Q3_KAFKA_SCOPE_ERROR_CODE
            )
        finally:
            conn.close()

    @staticmethod
    def _materialize_silent_quarantined_outbox_in_transaction(
        conn: sqlite3.Connection,
        *,
        row: sqlite3.Row,
        current: str,
        disposition_code: str,
    ) -> None:
        """Close pre-W3 quarantine history without creating an outward effect."""
        status = {
            "success": False,
            "state": "quarantined",
            "error_code": disposition_code,
            "external_writes": False,
            "terminal_delivery_policy": OUTBOX_PRE_W3_QUARANTINE_POLICY,
        }
        inserted = conn.execute(
            """
            INSERT INTO rca_execution_watch(
                submission_key, submission_outbox_id, business_key,
                generation, project_key, work_item_type_key, work_item_id,
                task_id, state, next_poll_at, last_observed_at, terminal_at,
                last_status_json, last_error_code, last_error_detail,
                delivery_id, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, 'quarantined', ?, ?, ?,
                      ?, ?, ?, NULL, ?, ?)
            """,
            (
                row["submission_key"],
                row["outbox_id"],
                row["business_key"],
                row["generation"],
                row["project_key"],
                row["work_item_type_key"],
                row["work_item_id"],
                current,
                current,
                str(row["quarantined_at"] or current),
                _canonical_json(status),
                disposition_code,
                "pre-W3 quarantined outbox retained as an internal terminal disposition",
                str(row["outbox_created_at"] or current),
                current,
            ),
        )
        if inserted.rowcount != 1:
            raise DeliveryRecordConflictError(
                "pre-W3 quarantined outbox watch was not created exactly once"
            )
        subscription_status = (
            "suppressed"
            if disposition_code in OUTBOX_SILENT_PROFILE_TERMINAL_ERROR_CODES
            else "quarantined"
        )
        conn.execute(
            """
            UPDATE rca_delivery_subscriptions
               SET status = ?, delivery_id = NULL, effect_key = NULL,
                   reason = ?, updated_at = ?
             WHERE business_key = ? AND generation = ?
               AND required = 1 AND status = 'pending'
            """,
            (
                subscription_status,
                disposition_code,
                current,
                row["business_key"],
                row["generation"],
            ),
        )

    def _materialize_quarantined_outbox_in_transaction(
        self,
        conn: sqlite3.Connection,
        *,
        row: sqlite3.Row,
        current: str,
        activation_enforced: bool = False,
    ) -> None:
        source_error_code = str(row["last_error_code"] or "").strip()
        if source_error_code in OUTBOX_SILENT_PROFILE_TERMINAL_ERROR_CODES:
            self._materialize_silent_quarantined_outbox_in_transaction(
                conn,
                row=row,
                current=current,
                disposition_code=source_error_code,
            )
            return
        w3_binding: dict[str, Any] | None = None
        w3_issue_target = ""
        if activation_enforced:
            w3_binding, w3_disposition_code, w3_issue_target = (
                self._issued_w3_binding_for_quarantined_outbox_tx(
                    conn,
                    row=row,
                    current=current,
                )
            )
            if w3_binding is not None:
                pass
            else:
                is_manual, manual_issue_target = (
                    self._manual_activation_issue_target_for_quarantined_outbox_tx(
                        conn,
                        row=row,
                    )
                )
                (
                    is_profile_terminal,
                    profile_issue_target,
                    _profile_project_simple_name,
                    profile_code,
                ) = self._stored_profile_terminal_issue_target_for_quarantined_outbox_tx(
                    conn,
                    row=row,
                )
                if is_manual:
                    if not manual_issue_target:
                        self._materialize_silent_quarantined_outbox_in_transaction(
                            conn,
                            row=row,
                            current=current,
                            disposition_code=OUTBOX_MANUAL_ACTIVATION_BINDING_INVALID_CODE,
                        )
                        return
                    w3_issue_target = manual_issue_target
                elif is_profile_terminal:
                    if not profile_issue_target:
                        self._materialize_silent_quarantined_outbox_in_transaction(
                            conn,
                            row=row,
                            current=current,
                            disposition_code=profile_code,
                        )
                        return
                    w3_issue_target = profile_issue_target
                else:
                    self._materialize_silent_quarantined_outbox_in_transaction(
                        conn,
                        row=row,
                        current=current,
                        disposition_code=w3_disposition_code,
                    )
                    return
        public_error_code = (
            source_error_code
            if source_error_code in OUTBOX_PUBLIC_PROFILE_ERROR_CODES
            else OUTBOX_QUARANTINED_PUBLIC_ERROR_CODE
        )
        delivery = build_terminal_delivery(
            business_key=str(row["business_key"]),
            submission_key=str(row["submission_key"]),
            generation=int(row["generation"]),
            project_key=str(row["project_key"]),
            work_item_type_key=str(row["work_item_type_key"]),
            work_item_id=str(row["work_item_id"]),
            outcome="quarantined",
            terminal_state=OUTBOX_QUARANTINED_TERMINAL_STATE,
            error_code=public_error_code,
            source_error_code=source_error_code,
            diagnostic_detail=(
                str(row["last_error_detail"] or "")
                if source_error_code in OUTBOX_PUBLIC_PROFILE_ERROR_CODES
                else ""
            ),
            schema_version=TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION_COMMENT_ONLY,
        )
        if w3_binding is not None:
            delivery.contract["w3_execution_snapshot"] = w3_binding
        terminal_at = str(row["quarantined_at"] or current)
        status = {
            "success": False,
            "state": OUTBOX_QUARANTINED_TERMINAL_STATE,
            "error_code": public_error_code,
        }
        inserted = conn.execute(
            """
            INSERT INTO rca_execution_watch(
                submission_key, submission_outbox_id, business_key,
                generation, project_key, work_item_type_key, work_item_id,
                task_id, state, next_poll_at, last_observed_at, terminal_at,
                last_status_json, last_error_code, last_error_detail,
                delivery_id, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, 'delivery_created', ?, ?, ?,
                      ?, ?, ?, ?, ?, ?)
            """,
            (
                row["submission_key"],
                row["outbox_id"],
                row["business_key"],
                row["generation"],
                row["project_key"],
                row["work_item_type_key"],
                row["work_item_id"],
                current,
                current,
                terminal_at,
                _canonical_json(status),
                str(row["last_error_code"] or "dispatch_quarantined")[:120],
                str(row["last_error_detail"] or "")[:1000],
                delivery.delivery_id,
                str(row["outbox_created_at"] or current),
                current,
            ),
        )
        if inserted.rowcount != 1:
            raise DeliveryRecordConflictError(
                "quarantined outbox watch was not created exactly once"
            )
        self._ensure_terminal_delivery_in_transaction(
            conn,
            delivery=delivery,
            current=current,
            materialize_subscriptions=(
                self._materialize_delivery_subscriptions_in_transaction
            ),
            issue_url=w3_issue_target,
        )

    def backfill_completed_submissions(
        self,
        *,
        limit: int = 1000,
        now: datetime | None = None,
        activation_required: bool = False,
    ) -> int:
        """Create watches and public terminal deliveries for durable outbox rows."""
        self._validate_activation_required(activation_required)
        if limit < 1:
            return 0
        current = _iso(now)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            activation_enforced = self._activation_enforced_tx(
                conn,
                activation_required=activation_required,
            )
            if activation_enforced:
                self._require_activation_schema(conn)
            activation_filter = (
                f"AND {_ACTIVATION_EXECUTION_ELIGIBLE_SQL}"
                if activation_enforced
                else ""
            )
            rows = conn.execute(
                f"""
                SELECT o.outbox_id, o.submission_key, o.business_key, o.generation,
                       o.activation_epoch_id, o.activation_ledger_id,
                       o.origin_source_id, o.source_event_id,
                       o.status AS outbox_status, o.created_at AS outbox_created_at,
                       o.quarantined_at, o.last_error_code, o.last_error_detail,
                       t.creation_rule_version, t.project_key,
                       t.work_item_type_key, t.work_item_id
                  FROM rca_outbox AS o
                  JOIN business_triggers AS t
                    ON t.business_key = o.business_key
                   AND t.generation = o.generation
             LEFT JOIN rca_execution_watch AS w
                    ON w.submission_key = o.submission_key
                 WHERE (
                        (o.status = 'completed' AND o.result_json IS NOT NULL)
                        OR o.status = 'quarantined'
                   )
                   -- Activation deferrals are terminal control-plane
                   -- dispositions.  They must never be re-materialized into a
                   -- delivery job after the writer has been fenced.
                   AND NOT (
                       o.status = 'quarantined'
                       AND o.last_error_code = 'activation_epoch_deferred'
                       AND o.activation_epoch_id IS NOT NULL
                       AND o.activation_ledger_id IS NOT NULL
                       AND EXISTS (
                           SELECT 1
                             FROM rca_activation_admission_ledger AS al
                            WHERE al.ledger_id = o.activation_ledger_id
                              AND al.epoch_id = o.activation_epoch_id
                              AND al.business_key = o.business_key
                              AND al.submission_key = o.submission_key
                              AND al.generation = o.generation
                              AND al.decision IN ('admit', 'shadow')
                              AND al.bound_at IS NOT NULL
                       )
                       AND EXISTS (
                           SELECT 1
                             FROM rca_shadow_promotion_audit AS audit
                            WHERE audit.outbox_id = o.outbox_id
                              AND audit.submission_key = o.submission_key
                              AND audit.outcome = 'deferred'
                              AND audit.to_status = 'quarantined'
                              AND audit.detail =
                                  'exact activation item deferred for reviewed manual recovery'
                              AND (
                                  audit.event_uid = o.source_event_id
                                  OR EXISTS (
                                      SELECT 1
                                        FROM rca_trigger_sources AS source
                                        JOIN rca_trigger_bindings AS origin
                                          ON origin.source_id = source.source_id
                                         AND origin.business_key = o.business_key
                                         AND origin.generation = o.generation
                                         AND origin.role = 'origin'
                                       WHERE source.source_id = o.origin_source_id
                                         AND source.source_kind =
                                             'feishu_group_manual'
                                         AND source.message_id = audit.event_uid
                                         AND source.source_dedupe_key =
                                             'feishu:' || source.message_id
                                  )
                              )
                       )
                   )
                   AND w.submission_key IS NULL
                   {activation_filter}
                 ORDER BY COALESCE(o.completed_at, o.quarantined_at, o.updated_at),
                          o.outbox_id
                 LIMIT ?
                """,
                (limit,),
            ).fetchall()
            inserted = 0
            for row in rows:
                if str(row["outbox_status"]) == "quarantined":
                    self._materialize_quarantined_outbox_in_transaction(
                        conn,
                        row=row,
                        current=current,
                        activation_enforced=activation_enforced,
                    )
                    inserted += 1
                else:
                    result = conn.execute(
                        """
                        INSERT OR IGNORE INTO rca_execution_watch(
                            submission_key, submission_outbox_id, business_key,
                            generation, project_key, work_item_type_key, work_item_id,
                            task_id, state, next_poll_at, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)
                        """,
                        (
                            row["submission_key"],
                            row["outbox_id"],
                            row["business_key"],
                            row["generation"],
                            row["project_key"],
                            row["work_item_type_key"],
                            row["work_item_id"],
                            row["submission_key"],
                            current,
                            current,
                            current,
                        ),
                    )
                    inserted += result.rowcount
            conn.commit()
            return inserted
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def preview_unwatched_completed(
        self,
        *,
        limit: int = 1000,
        activation_required: bool = False,
    ) -> list[dict[str, Any]]:
        self._validate_activation_required(activation_required)
        if limit < 1:
            return []
        conn = self._connect()
        try:
            conn.execute("BEGIN")
            activation_enforced = self._activation_enforced_tx(
                conn,
                activation_required=activation_required,
            )
            if activation_enforced:
                self._require_activation_schema(conn)
            activation_filter = (
                f"AND {_ACTIVATION_EXECUTION_ELIGIBLE_SQL}"
                if activation_enforced
                else ""
            )
            rows = conn.execute(
                f"""
                SELECT o.outbox_id AS submission_outbox_id, o.submission_key,
                       o.business_key, o.generation, t.project_key,
                       t.work_item_type_key, t.work_item_id, o.payload_json,
                       o.result_json
                  FROM rca_outbox AS o
                  JOIN business_triggers AS t
                    ON t.business_key = o.business_key
                   AND t.generation = o.generation
             LEFT JOIN rca_execution_watch AS w
                    ON w.submission_key = o.submission_key
                 WHERE o.status = 'completed'
                   AND o.result_json IS NOT NULL
                   AND w.submission_key IS NULL
                   {activation_filter}
                 ORDER BY o.completed_at, o.outbox_id
                 LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def claim_due_watch(
        self,
        *,
        lease_owner: str,
        lease_seconds: int = 120,
        now: datetime | None = None,
        activation_required: bool = False,
    ) -> ExecutionWatchClaim | None:
        self._validate_activation_required(activation_required)
        owner = str(lease_owner or "").strip()
        if not owner:
            raise ValueError("lease_owner is required")
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be positive")
        current_dt = _utc_datetime(now)
        current = _iso(current_dt)
        expires = _iso(current_dt + timedelta(seconds=lease_seconds))
        token = uuid.uuid4().hex
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            activation_enforced = self._activation_enforced_tx(
                conn,
                activation_required=activation_required,
            )
            if activation_enforced:
                self._require_activation_schema(conn)
            activation_filter = (
                f"AND {_ACTIVATION_EXECUTION_ELIGIBLE_SQL}"
                if activation_enforced
                else ""
            )
            activation_update_filter = (
                f"""
                AND EXISTS (
                    SELECT 1
                      FROM rca_outbox AS o
                      JOIN business_triggers AS t
                        ON t.business_key = o.business_key
                       AND t.generation = o.generation
                     WHERE o.outbox_id = rca_execution_watch.submission_outbox_id
                       AND {_ACTIVATION_EXECUTION_ELIGIBLE_SQL}
                )
                """
                if activation_enforced
                else ""
            )
            row = conn.execute(
                f"""
                SELECT w.*, o.payload_json AS submission_payload_json,
                       o.result_json AS submission_result_json,
                       o.origin_source_id AS origin_source_id,
                       t.origin_source_id AS trigger_origin_source_id,
                       o.completed_at AS work_started_at
                  FROM rca_execution_watch AS w
                  JOIN rca_outbox AS o ON o.outbox_id = w.submission_outbox_id
                  JOIN business_triggers AS t
                    ON t.business_key = o.business_key
                   AND t.generation = o.generation
                 WHERE w.state IN ('pending', 'running')
                   AND w.next_poll_at <= ?
                   AND (
                        w.lease_token IS NULL
                        OR w.lease_expires_at IS NULL
                        OR w.lease_expires_at <= ?
                   )
                   {activation_filter}
                 ORDER BY w.next_poll_at, w.created_at, w.submission_key
                 LIMIT 1
                """,
                (current, current),
            ).fetchone()
            if row is None:
                conn.commit()
                return None
            updated = conn.execute(
                f"""
                UPDATE rca_execution_watch
                   SET poll_attempt = poll_attempt + 1, fence = fence + 1,
                       lease_token = ?, lease_owner = ?, lease_expires_at = ?,
                       updated_at = ?
                 WHERE submission_key = ?
                   AND state IN ('pending', 'running')
                   AND next_poll_at <= ?
                   AND (
                        lease_token IS NULL OR lease_expires_at IS NULL
                        OR lease_expires_at <= ?
                   )
                   {activation_update_filter}
                """,
                (
                    token,
                    owner,
                    expires,
                    current,
                    row["submission_key"],
                    current,
                    current,
                ),
            )
            if updated.rowcount != 1:
                conn.rollback()
                return None
            claimed = conn.execute(
                """
                SELECT w.*, o.payload_json AS submission_payload_json,
                       o.result_json AS submission_result_json,
                       o.origin_source_id AS origin_source_id,
                       t.origin_source_id AS trigger_origin_source_id,
                       o.completed_at AS work_started_at
                  FROM rca_execution_watch AS w
                  JOIN rca_outbox AS o ON o.outbox_id = w.submission_outbox_id
                  JOIN business_triggers AS t
                    ON t.business_key = o.business_key
                   AND t.generation = o.generation
                 WHERE w.submission_key = ?
                """,
                (row["submission_key"],),
            ).fetchone()
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        return ExecutionWatchClaim(
            submission_key=str(claimed["submission_key"]),
            submission_outbox_id=int(claimed["submission_outbox_id"]),
            business_key=str(claimed["business_key"]),
            generation=int(claimed["generation"]),
            project_key=str(claimed["project_key"]),
            work_item_type_key=str(claimed["work_item_type_key"]),
            work_item_id=str(claimed["work_item_id"]),
            task_id=str(claimed["task_id"]),
            state=str(claimed["state"]),
            poll_attempt=int(claimed["poll_attempt"]),
            fence=int(claimed["fence"]),
            lease_token=str(claimed["lease_token"]),
            lease_owner=str(claimed["lease_owner"]),
            lease_expires_at=str(claimed["lease_expires_at"]),
            work_started_at=str(claimed["work_started_at"]),
            terminal_first_seen_at=(
                str(claimed["terminal_first_seen_at"])
                if claimed["terminal_first_seen_at"]
                else None
            ),
            submission_payload=_json_object(claimed["submission_payload_json"]),
            submission_result=_json_object(claimed["submission_result_json"]),
            origin_source_id=str(claimed["origin_source_id"] or ""),
            trigger_origin_source_id=str(claimed["trigger_origin_source_id"] or ""),
        )

    @staticmethod
    def _current_claim(
        conn: sqlite3.Connection,
        submission_key: str,
        lease_token: str,
        current: str,
    ) -> sqlite3.Row:
        row = conn.execute(
            """
            SELECT * FROM rca_execution_watch
             WHERE submission_key = ? AND lease_token = ?
               AND state IN ('pending', 'running')
               AND lease_expires_at IS NOT NULL AND lease_expires_at > ?
            """,
            (submission_key, lease_token, current),
        ).fetchone()
        if row is None:
            raise StaleDeliveryWatchLeaseError(
                f"stale execution-watch lease for {submission_key}"
            )
        return row

    def reschedule_watch(
        self,
        *,
        submission_key: str,
        lease_token: str,
        observed_state: str,
        status: dict[str, Any],
        next_poll_at: datetime,
        error_code: str = "",
        error_detail: str = "",
        now: datetime | None = None,
    ) -> None:
        state = "running" if observed_state not in {"", "pending"} else "pending"
        current = _iso(now)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            self._current_claim(conn, submission_key, lease_token, current)
            updated = conn.execute(
                """
                UPDATE rca_execution_watch
                   SET state = ?, next_poll_at = ?, last_observed_at = ?,
                       last_status_json = ?, last_error_code = ?,
                       last_error_detail = ?,
                       terminal_first_seen_at = CASE
                           WHEN ? = '' THEN NULL
                           ELSE COALESCE(terminal_first_seen_at, ?)
                       END,
                       lease_token = NULL,
                       lease_owner = NULL, lease_expires_at = NULL, updated_at = ?
                 WHERE submission_key = ? AND lease_token = ?
                """,
                (
                    state,
                    _iso(next_poll_at),
                    current,
                    _canonical_json(status),
                    str(error_code or "")[:120],
                    str(error_detail or "")[:1000],
                    str(error_code or "")[:120],
                    current,
                    current,
                    submission_key,
                    lease_token,
                ),
            )
            if updated.rowcount != 1:
                raise StaleDeliveryWatchLeaseError(submission_key)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    @staticmethod
    def _failure_route_identity(
        *,
        submission_key: str,
        terminal_error_code: str,
        lane: str,
        route_kind: str,
    ) -> tuple[str, str]:
        material = "\0".join((
            submission_key,
            terminal_error_code,
            lane,
            route_kind,
        )).encode("utf-8")
        digest = hashlib.sha256(material).hexdigest()
        return f"rca-failure-route-{digest}", digest

    def upsert_failure_route(
        self,
        *,
        claim: ExecutionWatchClaim,
        terminal_error_code: str,
        lane: str,
        route_kind: str,
        owner: str,
        work_started_at: str,
        deadline_at: str,
        audit: Mapping[str, Any],
        route_payload: Mapping[str, Any],
        now: datetime | None = None,
    ) -> FailureRouteMutation:
        allowed_lanes = {
            "infra_self_healable",
            "needs_human_input",
            "hard_defect",
        }
        initial_status = {
            "infra_remediation_hold": "remediation_pending",
            "internal_backlog": "backlog_pending",
            "internal_alert": "alert_pending",
        }.get(route_kind)
        code = str(terminal_error_code or "").strip()
        selected_owner = str(owner or "").strip()
        if lane not in allowed_lanes or initial_status is None:
            raise ValueError("failure route lane/kind is invalid")
        if not code or len(code.encode("utf-8")) > 120:
            raise ValueError("failure route error code is invalid")
        if not selected_owner or len(selected_owner.encode("utf-8")) > 120:
            raise ValueError("failure route owner is invalid")
        audit_json = _canonical_json(dict(audit))
        payload_json = _canonical_json(dict(route_payload))
        if len(audit_json.encode("utf-8")) > 65_536:
            raise ValueError("failure route audit is too large")
        if len(payload_json.encode("utf-8")) > 65_536:
            raise ValueError("failure route payload is too large")
        route_key, dedupe_key = self._failure_route_identity(
            submission_key=claim.submission_key,
            terminal_error_code=code,
            lane=lane,
            route_kind=route_kind,
        )
        current = _iso(now)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            self._current_claim(conn, claim.submission_key, claim.lease_token, current)
            marker_updated = conn.execute(
                """
                UPDATE rca_execution_watch
                   SET terminal_first_seen_at = COALESCE(terminal_first_seen_at, ?)
                 WHERE submission_key = ? AND lease_token = ?
                """,
                (current, claim.submission_key, claim.lease_token),
            )
            if marker_updated.rowcount != 1:
                raise StaleDeliveryWatchLeaseError(claim.submission_key)
            existing = conn.execute(
                "SELECT route_key FROM rca_failure_routes WHERE route_key = ?",
                (route_key,),
            ).fetchone()
            conn.execute(
                """
                INSERT INTO rca_failure_routes(
                    route_key, dedupe_key, submission_key, business_key,
                    generation, task_id, terminal_error_code, lane, route_kind,
                    owner, status, work_started_at, deadline_at,
                    first_observed_at, last_observed_at, audit_json,
                    route_payload_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(route_key) DO UPDATE SET
                    owner = excluded.owner,
                    work_started_at = excluded.work_started_at,
                    deadline_at = excluded.deadline_at,
                    last_observed_at = excluded.last_observed_at,
                    observation_count = rca_failure_routes.observation_count + 1,
                    audit_json = excluded.audit_json,
                    route_payload_json = excluded.route_payload_json,
                    updated_at = excluded.updated_at
                """,
                (
                    route_key,
                    dedupe_key,
                    claim.submission_key,
                    claim.business_key,
                    claim.generation,
                    claim.task_id,
                    code,
                    lane,
                    route_kind,
                    selected_owner,
                    initial_status,
                    work_started_at,
                    deadline_at,
                    current,
                    current,
                    audit_json,
                    payload_json,
                    current,
                    current,
                ),
            )
            row = conn.execute(
                """
                SELECT route_key, status, owner, remediation_attempt_count
                  FROM rca_failure_routes WHERE route_key = ?
                """,
                (route_key,),
            ).fetchone()
            conn.commit()
            if row is None:
                raise RuntimeError("failure route was not persisted")
            return FailureRouteMutation(
                route_key=str(row["route_key"]),
                created=existing is None,
                status=str(row["status"]),
                owner=str(row["owner"]),
                remediation_attempt_count=int(row["remediation_attempt_count"]),
            )
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def reschedule_failure_route(
        self,
        *,
        claim: ExecutionWatchClaim,
        route_key: str,
        next_retry_at: datetime,
        now: datetime | None = None,
    ) -> None:
        current = _iso(now)
        retry_at = _iso(next_retry_at)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            self._current_claim(conn, claim.submission_key, claim.lease_token, current)
            updated = conn.execute(
                """
                UPDATE rca_failure_routes
                   SET next_retry_at = ?, retry_count = retry_count + 1,
                       last_observed_at = ?, updated_at = ?
                 WHERE route_key = ? AND submission_key = ?
                   AND status != 'terminal_fallback'
                   AND status != 'resolved'
                """,
                (
                    retry_at,
                    current,
                    current,
                    route_key,
                    claim.submission_key,
                ),
            )
            if updated.rowcount != 1:
                raise StaleDeliveryWatchLeaseError(
                    f"failure route state changed for {route_key}"
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def failure_route_for_deadline(
        self,
        *,
        claim: ExecutionWatchClaim,
        now: datetime | None = None,
    ) -> dict[str, Any] | None:
        current = _iso(now)
        conn = self._connect()
        try:
            conn.execute("BEGIN")
            self._current_claim(conn, claim.submission_key, claim.lease_token, current)
            row = conn.execute(
                """
                SELECT route_key, terminal_error_code, lane, route_kind,
                       owner, status, audit_json, route_payload_json
                  FROM rca_failure_routes
                 WHERE submission_key = ?
                   AND status NOT IN ('terminal_fallback', 'resolved')
                 ORDER BY last_observed_at DESC, created_at DESC, route_key DESC
                 LIMIT 1
                """,
                (claim.submission_key,),
            ).fetchone()
            conn.commit()
            if row is None:
                return None
            return {
                "route_key": str(row["route_key"]),
                "terminal_error_code": str(row["terminal_error_code"]),
                "lane": str(row["lane"]),
                "route_kind": str(row["route_kind"]),
                "owner": str(row["owner"]),
                "status": str(row["status"]),
                "audit": json.loads(str(row["audit_json"])),
                "route_payload": json.loads(str(row["route_payload_json"])),
            }
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def claim_failure_remediation(
        self,
        *,
        claim: ExecutionWatchClaim,
        route_key: str,
        now: datetime | None = None,
    ) -> bool:
        current = _iso(now)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            self._current_claim(conn, claim.submission_key, claim.lease_token, current)
            updated = conn.execute(
                """
                UPDATE rca_failure_routes
                   SET status = 'remediation_started',
                       remediation_attempt_count = 1,
                       remediation_attempted_at = ?, updated_at = ?
                 WHERE route_key = ? AND submission_key = ?
                   AND status = 'remediation_pending'
                   AND remediation_attempt_count = 0
                """,
                (current, current, route_key, claim.submission_key),
            )
            conn.commit()
            return updated.rowcount == 1
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def finish_failure_remediation(
        self,
        *,
        claim: ExecutionWatchClaim,
        route_key: str,
        succeeded: bool,
        result: Mapping[str, Any],
        now: datetime | None = None,
    ) -> None:
        current = _iso(now)
        result_json = _canonical_json(dict(result))
        if len(result_json.encode("utf-8")) > 65_536:
            raise ValueError("failure remediation result is too large")
        status = "remediation_succeeded" if succeeded else "remediation_held"
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            self._current_claim(conn, claim.submission_key, claim.lease_token, current)
            updated = conn.execute(
                """
                UPDATE rca_failure_routes
                   SET status = ?, remediation_result_json = ?,
                       retry_exhausted = ?, next_retry_at = NULL,
                       updated_at = ?
                 WHERE route_key = ? AND submission_key = ?
                   AND status = 'remediation_started'
                   AND remediation_attempt_count = 1
                """,
                (
                    status,
                    result_json,
                    0 if succeeded else 1,
                    current,
                    route_key,
                    claim.submission_key,
                ),
            )
            if updated.rowcount != 1:
                raise StaleDeliveryWatchLeaseError(
                    f"failure remediation state changed for {route_key}"
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def terminal_failure(
        self,
        *,
        submission_key: str,
        lease_token: str,
        status: dict[str, Any],
        error_code: str,
        error_detail: str,
        now: datetime | None = None,
    ) -> None:
        self._finish_watch(
            submission_key=submission_key,
            lease_token=lease_token,
            state="terminal_failed",
            status=status,
            error_code=error_code,
            error_detail=error_detail,
            now=now,
        )

    def quarantine_watch(
        self,
        *,
        submission_key: str,
        lease_token: str,
        status: dict[str, Any],
        error_code: str,
        error_detail: str,
        now: datetime | None = None,
    ) -> None:
        self._finish_watch(
            submission_key=submission_key,
            lease_token=lease_token,
            state="quarantined",
            status=status,
            error_code=error_code,
            error_detail=error_detail,
            now=now,
        )

    def _finish_watch(
        self,
        *,
        submission_key: str,
        lease_token: str,
        state: str,
        status: dict[str, Any],
        error_code: str,
        error_detail: str,
        now: datetime | None,
    ) -> None:
        if state not in {"terminal_failed", "quarantined"}:
            raise ValueError(f"unsupported terminal watch state: {state}")
        current = _iso(now)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            self._current_claim(conn, submission_key, lease_token, current)
            updated = conn.execute(
                """
                UPDATE rca_execution_watch
                   SET state = ?, terminal_at = ?, last_observed_at = ?,
                       last_status_json = ?, last_error_code = ?,
                       last_error_detail = ?, lease_token = NULL,
                       lease_owner = NULL, lease_expires_at = NULL, updated_at = ?
                 WHERE submission_key = ? AND lease_token = ?
                """,
                (
                    state,
                    current,
                    current,
                    _canonical_json(status),
                    str(error_code or "")[:120],
                    str(error_detail or "")[:1000],
                    current,
                    submission_key,
                    lease_token,
                ),
            )
            if updated.rowcount != 1:
                raise StaleDeliveryWatchLeaseError(submission_key)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    @staticmethod
    def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table_name,),
        ).fetchone()
        return row is not None

    @classmethod
    def _learning_lane_row_tx(
        cls, conn: sqlite3.Connection, *, business_key: str, generation: int
    ) -> sqlite3.Row | None:
        if not cls._table_exists(conn, "rca_learning_lane_admissions"):
            return None
        return conn.execute(
            "SELECT * FROM rca_learning_lane_admissions "
            "WHERE business_key = ? AND generation = ?",
            (business_key, generation),
        ).fetchone()

    @staticmethod
    def _table_columns_tx(
        conn: sqlite3.Connection, table_name: str
    ) -> set[str]:
        return {
            str(row[1])
            for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        }

    @classmethod
    def _terminal_rerun_authority_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        business_key: str,
        generation: int,
        work_item_id: str | None = None,
    ) -> sqlite3.Row | None:
        required_tables = (
            "rca_terminal_rerun_delivery_authorities",
            "business_triggers",
            "rca_outbox",
            "rca_trigger_bindings",
            "rca_trigger_sources",
            "rca_activation_admission_ledger",
            "rca_activation_epochs",
        )
        if not all(cls._table_exists(conn, table) for table in required_tables):
            return None
        row = conn.execute(
            """
            SELECT authority.*,
                   trigger.work_item_id AS bound_work_item_id,
                   trigger.activation_epoch_id AS bound_epoch_id,
                   trigger.activation_ledger_id AS bound_ledger_id
              FROM rca_terminal_rerun_delivery_authorities AS authority
              JOIN business_triggers AS trigger
                ON trigger.business_key = authority.business_key
               AND trigger.generation = authority.generation
               AND trigger.submission_key = authority.submission_key
               AND trigger.project_key = authority.project_key
               AND trigger.work_item_type_key = authority.work_item_type_key
               AND trigger.work_item_id = authority.issue_id
               AND json_extract(
                   trigger.normalized_json, '$.project_simple_name'
               ) = authority.project_simple_name
               AND trigger.activation_epoch_id = authority.activation_epoch_id
               AND trigger.activation_ledger_id = authority.activation_ledger_id
              JOIN rca_trigger_bindings AS binding
                ON binding.source_id = authority.source_id
               AND binding.business_key = authority.business_key
               AND binding.generation = authority.generation
               AND binding.role = 'origin'
              JOIN rca_trigger_sources AS source
                ON source.source_id = binding.source_id
               AND source.payload_sha256 = authority.source_payload_sha256
               AND source.source_kind = 'feishu_group_manual'
               AND source.platform = 'operator'
               AND source.chat_id = ''
               AND source.thread_id = ''
               AND source.mode = 'rerun'
               AND source.outcome = 'created'
               AND source.requester_id = authority.requester_id
              JOIN rca_outbox AS outbox
                ON outbox.outbox_id = authority.outbox_id
               AND outbox.business_key = authority.business_key
               AND outbox.generation = authority.generation
               AND outbox.submission_key = authority.submission_key
               AND outbox.origin_source_id = authority.source_id
               AND outbox.action = 'submit_rca_issue_intake'
               AND outbox.activation_epoch_id = authority.activation_epoch_id
               AND outbox.activation_ledger_id = authority.activation_ledger_id
              JOIN rca_activation_admission_ledger AS ledger
                ON ledger.ledger_id = authority.activation_ledger_id
               AND ledger.epoch_id = authority.activation_epoch_id
               AND ledger.entrypoint = 'manual_admit'
               AND ledger.source_kind = 'manual'
               AND ledger.decision = 'admit'
               AND ledger.business_key = authority.business_key
               AND ledger.submission_key = authority.submission_key
               AND ledger.generation = authority.generation
               AND ledger.bound_at IS NOT NULL
              JOIN rca_activation_epochs AS epoch
                ON epoch.epoch_id = ledger.epoch_id
               AND epoch.is_current = 1
               AND epoch.state = 'steady_active'
             WHERE authority.business_key = ?
               AND authority.generation = ?
               AND authority.effect_kind = 'feishu_issue_comment'
               AND authority.activation_required = 1
             LIMIT 1
            """,
            (str(business_key), int(generation)),
        ).fetchone()
        if row is None or (
            work_item_id is not None
            and str(row["issue_id"]) != str(work_item_id)
        ):
            return None
        try:
            from gateway.pnc_rca_control_store import (
                TERMINAL_RERUN_DELIVERY_AUTHORITY_SCHEMA_VERSION,
                build_batch_terminal_rerun_authority,
                build_silent_terminal_rerun_authority,
            )

            authority = json.loads(str(row["authority_json"]))
            if not isinstance(authority, dict):
                return None
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
                expected_prior_delivery_id = ""
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
                expected_prior_delivery_id = str(expected["prior_delivery_id"])
            else:
                return None
        except (TypeError, ValueError, json.JSONDecodeError):
            return None
        expected_projection = {
            "authority_sha256": str(expected["selection_sha256"]),
            "schema_version": TERMINAL_RERUN_DELIVERY_AUTHORITY_SCHEMA_VERSION,
            "issue_id": str(expected["issue_id"]),
            "batch_id": str(expected["batch_id"]),
            "prior_submission_key": str(expected["prior_submission_key"]),
            "prior_generation": int(expected["prior_generation"]),
            "prior_delivery_id": expected_prior_delivery_id,
            "queue_sha256": str(expected["queue_sha256"]),
            "owner_receipt_path": str(expected["owner_receipt_path"]),
            "owner_receipt_sha256": str(expected["owner_receipt_sha256"]),
            "requester_id": str(expected["requester_id"]),
            "reason": str(expected["reason"]),
            "authority_json": _canonical_json(expected),
        }
        if any(row[name] != value for name, value in expected_projection.items()):
            return None
        return row

    @classmethod
    def _owner_authorized_rerun_authority_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        business_key: str,
        generation: int,
        work_item_id: str | None = None,
    ) -> sqlite3.Row | None:
        required = (
            "business_triggers",
            "rca_outbox",
            "rca_trigger_bindings",
            "rca_trigger_sources",
            "rca_activation_admission_ledger",
            "rca_activation_epochs",
        )
        if not all(cls._table_exists(conn, table) for table in required):
            return None
        if (
            conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='view' "
                "AND name='rca_owner_authorized_rerun_delivery_authorities'"
            ).fetchone()
            is None
        ):
            return None
        row = conn.execute(
            """
            SELECT authority.*
              FROM rca_owner_authorized_rerun_delivery_authorities AS authority
              JOIN business_triggers AS trigger
                ON trigger.business_key = authority.business_key
               AND trigger.generation = authority.generation
               AND trigger.submission_key = authority.submission_key
               AND trigger.project_key = authority.project_key
               AND trigger.work_item_type_key = authority.work_item_type_key
               AND trigger.work_item_id = authority.issue_id
               AND json_extract(
                   trigger.normalized_json, '$.project_simple_name'
               ) = authority.project_simple_name
               AND trigger.activation_epoch_id = authority.activation_epoch_id
               AND trigger.activation_ledger_id = authority.activation_ledger_id
              JOIN rca_trigger_bindings AS binding
                ON binding.source_id = authority.source_id
               AND binding.business_key = authority.business_key
               AND binding.generation = authority.generation
               AND binding.role = 'origin'
              JOIN rca_trigger_sources AS source
                ON source.source_id = binding.source_id
               AND source.payload_sha256 = authority.source_payload_sha256
               AND source.source_kind = 'feishu_group_manual'
               AND source.platform = 'operator'
               AND source.chat_id = ''
               AND source.thread_id = ''
               AND source.mode = 'rerun'
               AND source.outcome = 'created'
               AND source.requester_id = authority.requester_id
              JOIN rca_outbox AS outbox
                ON outbox.outbox_id = authority.outbox_id
               AND outbox.business_key = authority.business_key
               AND outbox.generation = authority.generation
               AND outbox.submission_key = authority.submission_key
               AND outbox.origin_source_id = authority.source_id
               AND outbox.action = 'submit_rca_issue_intake'
               AND outbox.activation_epoch_id = authority.activation_epoch_id
               AND outbox.activation_ledger_id = authority.activation_ledger_id
              JOIN rca_activation_admission_ledger AS ledger
                ON ledger.ledger_id = authority.activation_ledger_id
               AND ledger.epoch_id = authority.activation_epoch_id
               AND ledger.entrypoint = 'manual_admit'
               AND ledger.source_kind = 'manual'
               AND ledger.decision = 'admit'
               AND ledger.business_key = authority.business_key
               AND ledger.submission_key = authority.submission_key
               AND ledger.generation = authority.generation
               AND ledger.bound_at IS NOT NULL
              JOIN rca_activation_epochs AS epoch
                ON epoch.epoch_id = ledger.epoch_id
               AND epoch.is_current = 1
               AND epoch.state = 'steady_active'
             WHERE authority.business_key = ?
               AND authority.generation = ?
               AND authority.effect_kind = 'feishu_issue_comment'
               AND authority.activation_required = 1
             LIMIT 1
            """,
            (str(business_key), int(generation)),
        ).fetchone()
        if row is None or (
            work_item_id is not None and str(row["issue_id"]) != str(work_item_id)
        ):
            return None
        return row

    @classmethod
    def _explicit_user_rerun_keys_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        business_key: str,
        work_item_id: str,
    ) -> set[tuple[str, int]] | None:
        """Return exact human-origin reruns, or None when authority is unavailable."""
        required_tables = (
            "business_triggers",
            "rca_trigger_bindings",
            "rca_trigger_sources",
        )
        if not all(cls._table_exists(conn, table) for table in required_tables):
            return None
        required_columns = {
            "business_triggers": {
                "business_key",
                "generation",
                "work_item_id",
                "origin_source_id",
            },
            "rca_trigger_bindings": {
                "source_id",
                "business_key",
                "generation",
                "role",
            },
            "rca_trigger_sources": {
                "source_id",
                "source_kind",
                "platform",
                "chat_id",
                "thread_id",
                "message_id",
                "requester_id",
                "mode",
            },
        }
        if any(
            not required.issubset(cls._table_columns_tx(conn, table))
            for table, required in required_columns.items()
        ):
            return None
        rows = conn.execute(
            """
            SELECT b.business_key, b.generation, s.requester_id
              FROM rca_trigger_bindings AS b
              JOIN business_triggers AS t
                ON t.business_key = b.business_key
               AND t.generation = b.generation
               AND t.origin_source_id = b.source_id
              JOIN rca_trigger_sources AS s ON s.source_id = b.source_id
             WHERE b.business_key = ?
               AND t.work_item_id = ?
               AND b.role = 'origin'
               AND s.source_kind = 'feishu_group_manual'
               AND s.platform = 'feishu'
               AND s.mode = 'rerun'
               AND TRIM(s.chat_id) != ''
               AND TRIM(s.thread_id) != ''
               AND TRIM(s.message_id) != ''
            """,
            (business_key, work_item_id),
        ).fetchall()
        return {
            (str(row["business_key"]), int(row["generation"]))
            for row in rows
            if _OPEN_ID_RE.fullmatch(str(row["requester_id"] or ""))
        }

    @classmethod
    def is_learning_lane_tx(
        cls, conn: sqlite3.Connection, *, business_key: str, generation: int
    ) -> bool:
        return (
            cls._learning_lane_guard_state_tx(
                conn,
                business_key=business_key,
                generation=generation,
            )
            == "admitted"
        )

    @classmethod
    def _learning_lane_guard_state_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        business_key: str,
        generation: int,
        work_item_id: str | None = None,
    ) -> Literal[
        "admitted",
        "terminal_rerun_authorized",
        "admission_missing",
        "not_learning",
        "unknown",
    ]:
        """Resolve the W6 lane from immutable admission and stock authority.

        A stock member is learning even when its admission row is missing.  The
        latter must fail closed instead of falling through to ordinary delivery.
        Legacy isolated delivery fixtures without the control schema remain
        non-learning for compatibility.
        """
        admission = cls._learning_lane_row_tx(
            conn, business_key=business_key, generation=generation
        )
        if admission is not None:
            admission_columns = cls._table_columns_tx(
                conn, "rca_learning_lane_admissions"
            )
            required_admission_columns = {
                "business_key",
                "generation",
                "work_item_id",
                "lane",
                "external_write_allowed",
                "cohort_id",
                "stock_cutoff",
                "stock_ids_sha256",
                "admitted_at",
            }
            if not required_admission_columns.issubset(admission_columns):
                return "unknown"
            if (
                str(admission["lane"] or "") != "learning"
                or int(
                    admission["external_write_allowed"]
                    if admission["external_write_allowed"] is not None
                    else 1
                )
                != 0
            ):
                return "unknown"
            if not all(
                cls._table_exists(conn, table)
                for table in (
                    "business_triggers",
                    "rca_learning_lane_cohorts",
                    "rca_learning_lane_stock_items",
                )
            ):
                return "unknown"
            trigger = conn.execute(
                "SELECT work_item_id, created_at FROM business_triggers "
                "WHERE business_key = ? AND generation = ?",
                (business_key, generation),
            ).fetchone()
            cohort = conn.execute(
                "SELECT stock_cutoff, stock_ids_sha256 FROM "
                "rca_learning_lane_cohorts WHERE cohort_id = ?",
                (str(admission["cohort_id"] or ""),),
            ).fetchone()
            if trigger is None or cohort is None:
                return "unknown"
            try:
                trigger_created = _parse_iso(str(trigger["created_at"]))
                stock_cutoff = _parse_iso(str(cohort["stock_cutoff"]))
                admitted_at = _parse_iso(str(admission["admitted_at"]))
            except (TypeError, ValueError, OverflowError):
                return "unknown"
            if (
                str(trigger["work_item_id"] or "")
                != str(admission["work_item_id"] or "")
                or str(admission["stock_cutoff"] or "")
                != str(cohort["stock_cutoff"] or "")
                or str(admission["stock_ids_sha256"] or "")
                != str(cohort["stock_ids_sha256"] or "")
                or admitted_at <= stock_cutoff
                or trigger_created <= stock_cutoff
            ):
                return "unknown"
            member = conn.execute(
                "SELECT 1 FROM rca_learning_lane_stock_items "
                "WHERE cohort_id = ? AND work_item_id = ?",
                (str(admission["cohort_id"]), str(admission["work_item_id"])),
            ).fetchone()
            return "admitted" if member is not None else "unknown"

        cohort_tables = {
            "rca_learning_lane_cohorts",
            "rca_learning_lane_stock_items",
        }
        present = {
            table
            for table in cohort_tables
            if cls._table_exists(conn, table)
        }
        # A standalone delivery database has no W6 authority at all.
        if not present and not cls._table_exists(
            conn, "rca_learning_lane_admissions"
        ):
            return "not_learning"
        if present != cohort_tables:
            return "unknown"
        trigger_tables = {"business_triggers"}
        if not all(cls._table_exists(conn, table) for table in trigger_tables):
            return "unknown"
        trigger_columns = cls._table_columns_tx(conn, "business_triggers")
        if not {"business_key", "generation", "work_item_id", "created_at"}.issubset(
            trigger_columns
        ):
            return "unknown"
        trigger = conn.execute(
            "SELECT work_item_id, created_at FROM business_triggers "
            "WHERE business_key = ? AND generation = ?",
            (business_key, generation),
        ).fetchone()
        if trigger is None:
            return "unknown"
        item_id = str(work_item_id or trigger["work_item_id"] or "").strip()
        if not item_id:
            return "unknown"
        cohort = conn.execute(
            "SELECT cohort_id, stock_cutoff FROM rca_learning_lane_cohorts "
            "ORDER BY sealed_at, cohort_id LIMIT 1"
        ).fetchone()
        if cohort is None:
            return "not_learning"
        try:
            created_at = _parse_iso(str(trigger["created_at"]))
            stock_cutoff = _parse_iso(str(cohort["stock_cutoff"]))
        except (TypeError, ValueError, OverflowError):
            return "unknown"
        if created_at <= stock_cutoff:
            return "not_learning"
        member = conn.execute(
            "SELECT 1 FROM rca_learning_lane_stock_items "
            "WHERE cohort_id = ? AND work_item_id = ?",
            (str(cohort["cohort_id"]), item_id),
        ).fetchone()
        if member is None:
            return "not_learning"
        if (
            cls._owner_authorized_rerun_authority_tx(
                conn,
                business_key=str(business_key),
                generation=int(generation),
                work_item_id=item_id,
            )
            is not None
        ):
            return "terminal_rerun_authorized"
        return "admission_missing"

    @classmethod
    def _require_learning_lane_guard_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        business_key: str,
        generation: int,
        work_item_id: str | None = None,
        effect_kind: str,
    ) -> Literal[
        "terminal_rerun_authorized", "not_learning"
    ]:
        state = cls._learning_lane_guard_state_tx(
            conn,
            business_key=business_key,
            generation=generation,
            work_item_id=work_item_id,
        )
        if state == "admitted":
            raise DeliveryContractError(LEARNING_LANE_EXTERNAL_EFFECT_ERROR)
        if state == "terminal_rerun_authorized":
            if str(effect_kind) != DELIVERY_EFFECT_KIND:
                raise DeliveryContractError(LEARNING_LANE_EXTERNAL_EFFECT_ERROR)
            return state
        if state == "admission_missing":
            raise DeliveryContractError(LEARNING_LANE_ADMISSION_MISSING_ERROR)
        if state == "unknown":
            raise DeliveryContractError(LEARNING_LANE_EXTERNAL_EFFECT_ERROR)
        return "not_learning"

    @staticmethod
    def _is_adjudication_comment_payload(
        payload: Mapping[str, Any], target_key: str
    ) -> bool:
        return (
            str(payload.get("schema_version") or "") in _LEARNING_ADJUDICATION_SCHEMAS
            or str(target_key or "").startswith(_ADJUDICATION_TARGET_PREFIX)
        )

    @classmethod
    def _is_pre_w6_terminal_generation_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        business_key: str,
        generation: int,
    ) -> bool:
        """Recognize immutable pre-cutover terminal history only.

        Legacy Kafka generations created before the W6 cutoff may still need a
        single terminal comment.  New/post-cutover generations remain subject
        to the explicit-human-rerun authority check below.
        """
        if not cls._table_exists(conn, "business_triggers"):
            return False
        row = conn.execute(
            "SELECT created_at FROM business_triggers "
            "WHERE business_key = ? AND generation = ?",
            (business_key, generation),
        ).fetchone()
        if row is None:
            return False
        try:
            return _parse_iso(str(row["created_at"])) <= _parse_iso(_W6_STOCK_CUTOFF)
        except (TypeError, ValueError, OverflowError):
            return False

    @classmethod
    def _is_automatic_kafka_terminal_generation_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        business_key: str,
        generation: int,
    ) -> bool:
        """Allow one terminal result for a durable Kafka retrigger lineage.

        Kafka re-observation is an existing execution lineage (not a user
        correction).  It may publish its one terminal result, while ordinary
        post-cutover comments still require the explicit Feishu rerun binding.
        """
        required = (
            "business_triggers",
            "rca_trigger_bindings",
            "rca_trigger_sources",
        )
        if not all(cls._table_exists(conn, table) for table in required):
            return False
        row = conn.execute(
            """
            SELECT s.source_kind, s.mode
              FROM business_triggers AS t
              JOIN rca_trigger_bindings AS b
                ON b.business_key = t.business_key
               AND b.generation = t.generation
               AND b.source_id = t.origin_source_id
              JOIN rca_trigger_sources AS s ON s.source_id = b.source_id
             WHERE t.business_key = ?
               AND t.generation = ?
               AND b.role = 'origin'
             LIMIT 1
            """,
            (business_key, generation),
        ).fetchone()
        return row is not None and (
            str(row["source_kind"] or "") == "kafka_workflow_event"
            or str(row["mode"] or "") == "kafka_retrigger"
        )

    @classmethod
    def enforce_issue_comment_budget_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        delivery_id: str,
        business_key: str,
        generation: int,
        target_key: str,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Reserve one outward issue-comment slot inside the caller transaction."""
        job = conn.execute(
            "SELECT business_key, generation, work_item_id, target_key "
            "FROM rca_delivery_jobs WHERE delivery_id = ?",
            (delivery_id,),
        ).fetchone()
        if job is None:
            raise DeliveryRecordConflictError("delivery_comment_budget_job_missing")
        if (
            str(job["business_key"]) != str(business_key)
            or int(job["generation"]) != int(generation)
        ):
            raise DeliveryRecordConflictError(
                "delivery_comment_budget_job_identity_mismatch"
            )
        guard_state = cls._require_learning_lane_guard_tx(
            conn,
            business_key=str(business_key),
            generation=int(generation),
            work_item_id=str(job["work_item_id"]),
            effect_kind=DELIVERY_EFFECT_KIND,
        )
        job_target = str(job["target_key"] or "")
        is_adjudication = cls._is_adjudication_comment_payload(payload, target_key)
        if is_adjudication and guard_state == "terminal_rerun_authorized":
            raise DeliveryContractError(LEARNING_LANE_EXTERNAL_EFFECT_ERROR)
        if not is_adjudication and str(target_key) != job_target:
            raise DeliveryContractError("delivery_comment_budget_unclassified")
        if not is_adjudication and generation > 1:
            is_terminal = (
                str(payload.get("schema_version") or "")
                in _TERMINAL_EFFECT_SCHEMA_VERSIONS
            )
            legacy_terminal_exception = is_terminal and (
                cls._is_pre_w6_terminal_generation_tx(
                    conn,
                    business_key=str(business_key),
                    generation=int(generation),
                )
                or cls._is_automatic_kafka_terminal_generation_tx(
                    conn,
                    business_key=str(business_key),
                    generation=int(generation),
                )
            )
            owner_authorized_rerun_exception = (
                guard_state == "terminal_rerun_authorized"
                or cls._owner_authorized_rerun_authority_tx(
                    conn,
                    business_key=str(business_key),
                    generation=int(generation),
                    work_item_id=str(job["work_item_id"]),
                )
                is not None
            )
            if (
                not legacy_terminal_exception
                and not owner_authorized_rerun_exception
            ):
                explicit_keys = cls._explicit_user_rerun_keys_tx(
                    conn,
                    business_key=str(job["business_key"]),
                    work_item_id=str(job["work_item_id"]),
                )
                if explicit_keys is None or (
                    str(business_key), int(generation)
                ) not in explicit_keys:
                    raise DeliveryContractError(
                        "delivery_comment_budget_generation_not_user_rerun"
                    )

        slot_kind = "correction" if is_adjudication else "conclusion"
        slot_key = "g1q3-rca-comment-slot-v1-" + hashlib.sha256(
            _canonical_json(
                {
                    "business_key": str(business_key),
                    "generation": int(generation),
                    "slot_kind": slot_kind,
                }
            ).encode("utf-8")
        ).hexdigest()
        existing = conn.execute(
            "SELECT effect_key FROM rca_delivery_effects "
            "WHERE comment_slot_key = ? LIMIT 1",
            (slot_key,),
        ).fetchone()
        if existing is not None:
            raise DeliveryContractError(
                "conclusion_adjudication_comment_budget_exhausted"
                if is_adjudication
                else "delivery_comment_budget_exhausted"
            )
        return {
            "comment_slot_budget_exempt": int(
                str(job["work_item_id"])
                in ACCIDENT_SAMPLE_BUDGET_EXEMPT_ISSUE_IDS
            ),
            "comment_slot_generation": int(generation),
            "comment_slot_key": slot_key,
            "comment_slot_kind": slot_kind,
            "comment_slot_revision": 1,
            "comment_slot_schema_version": COMMENT_SLOT_SCHEMA_VERSION,
        }

    @classmethod
    def _quarantine_subscription_in_transaction(
        cls,
        conn: sqlite3.Connection,
        *,
        subscription_key: str,
        delivery_id: str,
        current: str,
        reason: str,
    ) -> None:
        conn.execute(
            """
            UPDATE rca_delivery_subscriptions
               SET status = 'quarantined', delivery_id = ?, reason = ?,
                   updated_at = ?
             WHERE subscription_key = ? AND status = 'pending'
            """,
            (
                delivery_id,
                str(reason or "subscription_materialization_failed")[:240],
                current,
                subscription_key,
            ),
        )
        conn.execute(
            "UPDATE rca_delivery_jobs SET status = 'quarantined', updated_at = ? "
            "WHERE delivery_id = ?",
            (current, delivery_id),
        )

    @classmethod
    def _materialize_subscription_in_transaction(
        cls,
        conn: sqlite3.Connection,
        *,
        subscription: sqlite3.Row,
        job: sqlite3.Row,
        current: str,
    ) -> None:
        subscription_key = str(subscription["subscription_key"])
        effect_kind = str(subscription["effect_kind"])
        target_key = str(subscription["target_key"])
        delivery_id = str(job["delivery_id"])
        cls._require_learning_lane_guard_tx(
            conn,
            business_key=str(job["business_key"]),
            generation=int(job["generation"]),
            work_item_id=str(job["work_item_id"]),
            effect_kind=effect_kind,
        )
        if int(subscription["required"]) != 1:
            raise DeliveryContractError("delivery_subscription_required_invalid")
        try:
            target = json.loads(str(subscription["target_json"] or ""))
        except (TypeError, json.JSONDecodeError) as exc:
            raise DeliveryContractError("delivery_subscription_target_invalid") from exc
        if not isinstance(target, dict):
            raise DeliveryContractError("delivery_subscription_target_invalid")
        validate_delivery_subscription_target(
            effect_kind=effect_kind,
            target_key=target_key,
            target=target,
            project_key=str(job["project_key"]),
            work_item_type_key=str(job["work_item_type_key"]),
            work_item_id=str(job["work_item_id"]),
        )
        issue_effect = conn.execute(
            """
            SELECT effect_key, target_key, payload_json, payload_sha256, required
              FROM rca_delivery_effects
             WHERE delivery_id = ? AND effect_kind = 'feishu_issue_comment'
               AND target_key = ?
            """,
            (delivery_id, job["target_key"]),
        ).fetchone()
        if issue_effect is None or int(issue_effect["required"]) != 1:
            raise DeliveryContractError("delivery_primary_effect_missing")

        if effect_kind == DELIVERY_EFFECT_KIND:
            if target_key != str(issue_effect["target_key"]) or target_key != str(
                job["target_key"]
            ):
                raise DeliveryContractError("delivery_primary_effect_invalid")
            effect_key = str(issue_effect["effect_key"])
        elif effect_kind == DELIVERY_THREAD_EFFECT_KIND:
            try:
                issue_payload = json.loads(str(issue_effect["payload_json"] or ""))
            except (TypeError, json.JSONDecodeError) as exc:
                raise DeliveryContractError("delivery_primary_effect_invalid") from exc
            if not isinstance(issue_payload, dict):
                raise DeliveryContractError("delivery_primary_effect_invalid")
            outcome = str(job["outcome"] or "success")
            if outcome == "success":
                effect_key, payload_sha256, payload = build_thread_reply_effect(
                    issue_effect_payload=issue_payload,
                    target_key=target_key,
                    target=target,
                )
            else:
                effect_key, payload_sha256, payload = (
                    build_terminal_thread_reply_effect(
                        issue_effect_payload=issue_payload,
                        target_key=target_key,
                        target=target,
                    )
                )
            conn.execute(
                """
                INSERT INTO rca_delivery_effects(
                    effect_key, delivery_id, effect_kind, required,
                    target_key, payload_json, payload_sha256, outcome, status,
                    created_at, updated_at
                ) VALUES (?, ?, 'feishu_thread_reply', 1, ?, ?, ?, ?, 'pending', ?, ?)
                ON CONFLICT(delivery_id, effect_kind, target_key) DO NOTHING
                """,
                (
                    effect_key,
                    delivery_id,
                    target_key,
                    _canonical_json(payload),
                    payload_sha256,
                    outcome,
                    current,
                    current,
                ),
            )
            existing = conn.execute(
                """
                SELECT effect_key, payload_sha256, required
                  FROM rca_delivery_effects
                 WHERE delivery_id = ? AND effect_kind = 'feishu_thread_reply'
                   AND target_key = ?
                """,
                (delivery_id, target_key),
            ).fetchone()
            if (
                existing is None
                or str(existing["effect_key"]) != effect_key
                or str(existing["payload_sha256"]) != payload_sha256
                or int(existing["required"]) != 1
            ):
                raise DeliveryRecordConflictError(
                    "delivery subscription is bound to a different thread effect"
                )
        else:
            raise DeliveryContractError("delivery_effect_kind_unsupported")

        updated = conn.execute(
            """
            UPDATE rca_delivery_subscriptions
               SET status = 'materialized', delivery_id = ?, effect_key = ?,
                   reason = 'delivery_effect_materialized',
                   materialized_at = ?, updated_at = ?
             WHERE subscription_key = ? AND status = 'pending'
            """,
            (delivery_id, effect_key, current, current, subscription_key),
        )
        if updated.rowcount != 1:
            raise DeliveryRecordConflictError(
                "delivery subscription changed during materialization"
            )
        cls._aggregate_job_status(conn, delivery_id, current)

    @classmethod
    def _materialize_delivery_subscriptions_in_transaction(
        cls,
        conn: sqlite3.Connection,
        *,
        delivery_id: str,
        current: str,
        limit: int | None = None,
        activation_required: bool = False,
    ) -> SubscriptionMaterializationResult:
        cls._validate_activation_required(activation_required)
        activation_enforced = cls._activation_enforced_tx(
            conn,
            activation_required=activation_required,
        )
        if activation_enforced:
            cls._require_activation_schema(conn)
        if not cls._table_exists(conn, "rca_delivery_subscriptions"):
            return SubscriptionMaterializationResult()
        job = conn.execute(
            "SELECT * FROM rca_delivery_jobs WHERE delivery_id = ?",
            (delivery_id,),
        ).fetchone()
        if job is None:
            raise DeliveryRecordConflictError("delivery job disappeared")
        if activation_enforced and not cls._execution_activation_eligible_tx(
            conn,
            submission_key=str(job["submission_key"]),
        ):
            raise StaleDeliveryWatchLeaseError(
                f"delivery activation changed for {job['submission_key']}"
            )
        sql = (
            "SELECT * FROM rca_delivery_subscriptions "
            "WHERE business_key = ? AND generation = ? AND status = 'pending' "
            "ORDER BY created_at, subscription_key"
        )
        params: list[Any] = [job["business_key"], job["generation"]]
        if limit is not None:
            sql += " LIMIT ?"
            params.append(max(1, int(limit)))
        materialized = 0
        quarantined = 0
        for subscription in conn.execute(sql, tuple(params)).fetchall():
            try:
                cls._materialize_subscription_in_transaction(
                    conn,
                    subscription=subscription,
                    job=job,
                    current=current,
                )
                materialized += 1
            except (DeliveryContractError, DeliveryRecordConflictError) as exc:
                cls._quarantine_subscription_in_transaction(
                    conn,
                    subscription_key=str(subscription["subscription_key"]),
                    delivery_id=delivery_id,
                    current=current,
                    reason=str(exc) or type(exc).__name__,
                )
                quarantined += 1
        return SubscriptionMaterializationResult(materialized, quarantined)

    def materialize_pending_subscriptions(
        self,
        *,
        limit: int = 100,
        now: datetime | None = None,
        activation_required: bool = False,
    ) -> SubscriptionMaterializationResult:
        self._validate_activation_required(activation_required)
        if limit < 1:
            return SubscriptionMaterializationResult()
        current = _iso(now)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            activation_enforced = self._activation_enforced_tx(
                conn,
                activation_required=activation_required,
            )
            if activation_enforced:
                self._require_activation_schema(conn)
            if not self._table_exists(conn, "rca_delivery_subscriptions"):
                conn.commit()
                return SubscriptionMaterializationResult()
            activation_joins = (
                """
             LEFT JOIN rca_execution_watch AS w
                    ON w.submission_key = j.submission_key
             LEFT JOIN rca_outbox AS o
                    ON o.outbox_id = w.submission_outbox_id
             LEFT JOIN business_triggers AS t
                    ON t.business_key = o.business_key
                   AND t.generation = o.generation
                """
                if activation_enforced
                else ""
            )
            activation_filter = (
                f"AND {_ACTIVATION_EXECUTION_ELIGIBLE_SQL}"
                if activation_enforced
                else ""
            )
            rows = conn.execute(
                f"""
                SELECT DISTINCT j.delivery_id
                  FROM rca_delivery_subscriptions AS s
                  JOIN rca_delivery_jobs AS j
                    ON j.business_key = s.business_key
                   AND j.generation = s.generation
                  {activation_joins}
                 WHERE s.status = 'pending'
                   {activation_filter}
                 ORDER BY s.catchup_requested_at IS NULL,
                          s.catchup_requested_at, s.created_at
                 LIMIT ?
                """,
                (limit,),
            ).fetchall()
            materialized = 0
            quarantined = 0
            remaining = limit
            for row in rows:
                result = self._materialize_delivery_subscriptions_in_transaction(
                    conn,
                    delivery_id=str(row["delivery_id"]),
                    current=current,
                    limit=remaining,
                    activation_required=activation_enforced,
                )
                materialized += result.materialized
                quarantined += result.quarantined
                remaining -= result.materialized + result.quarantined
                if remaining <= 0:
                    break
            conn.commit()
            return SubscriptionMaterializationResult(materialized, quarantined)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def create_delivery(
        self,
        *,
        claim: ExecutionWatchClaim,
        delivery: VerifiedDelivery,
        status: dict[str, Any],
        runtime_identity: Mapping[str, Any] | None = None,
        now: datetime | None = None,
        activation_required: bool = False,
    ) -> DeliveryCreateResult:
        self._validate_activation_required(activation_required)
        if (
            delivery.submission_key != claim.submission_key
            or delivery.business_key != claim.business_key
            or delivery.generation != claim.generation
            or delivery.project_key != claim.project_key
            or delivery.work_item_type_key != claim.work_item_type_key
            or delivery.work_item_id != claim.work_item_id
        ):
            raise DeliveryRecordConflictError(
                "verified delivery identity does not match claimed execution watch"
            )
        current = _iso(now)
        job = delivery.job_payload()
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            self._current_claim(conn, claim.submission_key, claim.lease_token, current)
            activation_enforced = self._activation_enforced_tx(
                conn,
                activation_required=activation_required,
            )
            if activation_enforced:
                self._require_activation_schema(conn)
            if activation_enforced and not self._execution_activation_eligible_tx(
                conn,
                submission_key=claim.submission_key,
            ):
                raise StaleDeliveryWatchLeaseError(
                    f"delivery activation changed for {claim.submission_key}"
                )
            existing = conn.execute(
                "SELECT delivery_id, artifact_set_id FROM rca_delivery_jobs WHERE submission_key = ?",
                (claim.submission_key,),
            ).fetchone()
            if existing:
                if (
                    existing["delivery_id"] != delivery.delivery_id
                    or existing["artifact_set_id"] != delivery.artifact_set_id
                ):
                    raise DeliveryRecordConflictError(
                        "submission is already bound to a different artifact set"
                    )
                effect = conn.execute(
                    "SELECT effect_key FROM rca_delivery_effects "
                    "WHERE delivery_id = ? AND effect_kind = 'feishu_issue_comment' "
                    "AND target_key = ?",
                    (delivery.delivery_id, delivery.target_key),
                ).fetchone()
                if effect is None or effect["effect_key"] != delivery.effect_key:
                    raise DeliveryRecordConflictError(
                        "delivery is already bound to a different effect"
                    )
                created = False
            else:
                conn.execute(
                    """
                    INSERT INTO rca_delivery_jobs(
                        delivery_id, submission_key, business_key, generation,
                        artifact_set_id, project_key, work_item_type_key,
                        work_item_id, target_key, issue_url, report_url, status,
                        manifest_json, contract_json, artifacts_json,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ready', ?, ?, ?, ?, ?)
                    """,
                    (
                        delivery.delivery_id,
                        delivery.submission_key,
                        delivery.business_key,
                        delivery.generation,
                        delivery.artifact_set_id,
                        delivery.project_key,
                        delivery.work_item_type_key,
                        delivery.work_item_id,
                        delivery.target_key,
                        delivery.issue_url,
                        delivery.report_url,
                        _canonical_json(delivery.manifest),
                        _canonical_json(delivery.contract),
                        _canonical_json(job["artifacts"]),
                        current,
                        current,
                    ),
                )
                comment_slot = self.enforce_issue_comment_budget_tx(
                    conn,
                    delivery_id=delivery.delivery_id,
                    business_key=delivery.business_key,
                    generation=delivery.generation,
                    target_key=delivery.target_key,
                    payload=delivery.effect_payload,
                )
                conn.execute(
                    """
                    INSERT INTO rca_delivery_effects(
                        effect_key, delivery_id, effect_kind, required,
                        target_key, payload_json, payload_sha256, status,
                        comment_slot_schema_version, comment_slot_key,
                        comment_slot_kind, comment_slot_generation,
                        comment_slot_revision, comment_slot_budget_exempt,
                        created_at, updated_at
                    ) VALUES (?, ?, 'feishu_issue_comment', 1, ?, ?, ?, 'pending',
                              ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        delivery.effect_key,
                        delivery.delivery_id,
                        delivery.target_key,
                        _canonical_json(delivery.effect_payload),
                        delivery.semantic_payload_sha256,
                        comment_slot["comment_slot_schema_version"],
                        comment_slot["comment_slot_key"],
                        comment_slot["comment_slot_kind"],
                        comment_slot["comment_slot_generation"],
                        comment_slot["comment_slot_revision"],
                        comment_slot["comment_slot_budget_exempt"],
                        current,
                        current,
                    ),
                )
                created = True
            self._materialize_delivery_subscriptions_in_transaction(
                conn,
                delivery_id=delivery.delivery_id,
                current=current,
                activation_required=activation_enforced,
            )
            updated = conn.execute(
                """
                UPDATE rca_execution_watch
                   SET state = 'delivery_created', delivery_id = ?, terminal_at = ?,
                       last_observed_at = ?, last_status_json = ?,
                       last_error_code = '', last_error_detail = '',
                       terminal_first_seen_at = NULL,
                       lease_token = NULL, lease_owner = NULL,
                       lease_expires_at = NULL, updated_at = ?
                 WHERE submission_key = ? AND lease_token = ?
                """,
                (
                    delivery.delivery_id,
                    current,
                    current,
                    _canonical_json(status),
                    current,
                    claim.submission_key,
                    claim.lease_token,
                ),
            )
            if updated.rowcount != 1:
                raise StaleDeliveryWatchLeaseError(claim.submission_key)
            if runtime_identity is not None:
                insert_host_runtime_transition(
                    conn,
                    submission_key=claim.submission_key,
                    business_key=claim.business_key,
                    generation=claim.generation,
                    service_label="local.pnc.rca-delivery-collector",
                    transition_kind="delivery_created",
                    entity_key=delivery.delivery_id,
                    runtime_identity=runtime_identity,
                    transitioned_at=current,
                )
            conn.commit()
            return DeliveryCreateResult(
                delivery.delivery_id, delivery.effect_key, created
            )
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    @classmethod
    def _ensure_terminal_delivery_in_transaction(
        cls,
        conn: sqlite3.Connection,
        *,
        delivery: VerifiedTerminalDelivery,
        current: str,
        materialize_subscriptions: Any = None,
        activation_required: bool = False,
        issue_url: str = "",
    ) -> bool:
        terminal_issue_url = str(issue_url or "").strip()
        existing = conn.execute(
            """
            SELECT delivery_id, outcome_key, outcome, terminal_state,
                   terminal_error_code, issue_url, report_url, contract_json
              FROM rca_delivery_jobs WHERE submission_key = ?
            """,
            (delivery.submission_key,),
        ).fetchone()
        if existing is not None:
            expected = (
                delivery.delivery_id,
                delivery.outcome_key,
                delivery.outcome,
                delivery.terminal_state,
                delivery.error_code,
                terminal_issue_url,
                "",
                _canonical_json(delivery.contract),
            )
            observed = tuple(
                existing[key]
                for key in (
                    "delivery_id",
                    "outcome_key",
                    "outcome",
                    "terminal_state",
                    "terminal_error_code",
                    "issue_url",
                    "report_url",
                    "contract_json",
                )
            )
            if observed != expected:
                raise DeliveryRecordConflictError(
                    "submission is already bound to a different terminal outcome"
                )
            effect = conn.execute(
                """
                SELECT effect_key FROM rca_delivery_effects
                 WHERE delivery_id = ? AND effect_kind = 'feishu_issue_comment'
                   AND target_key = ?
                """,
                (delivery.delivery_id, delivery.target_key),
            ).fetchone()
            if effect is None or effect["effect_key"] != delivery.effect_key:
                raise DeliveryRecordConflictError(
                    "terminal delivery is bound to a different issue effect"
                )
            created = False
        else:
            conn.execute(
                """
                INSERT INTO rca_delivery_jobs(
                    delivery_id, submission_key, business_key, generation,
                    artifact_set_id, project_key, work_item_type_key,
                    work_item_id, target_key, issue_url, report_url,
                    outcome, outcome_key, terminal_state,
                    terminal_error_code, status, manifest_json,
                    contract_json, artifacts_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', ?, ?, ?, ?,
                          'ready', '{}', ?, '[]', ?, ?)
                """,
                (
                    delivery.delivery_id,
                    delivery.submission_key,
                    delivery.business_key,
                    delivery.generation,
                    delivery.outcome_key,
                    delivery.project_key,
                    delivery.work_item_type_key,
                    delivery.work_item_id,
                    delivery.target_key,
                    terminal_issue_url,
                    delivery.outcome,
                    delivery.outcome_key,
                    delivery.terminal_state,
                    delivery.error_code,
                    _canonical_json(delivery.contract),
                    current,
                    current,
                ),
            )
            comment_slot = cls.enforce_issue_comment_budget_tx(
                conn,
                delivery_id=delivery.delivery_id,
                business_key=delivery.business_key,
                generation=delivery.generation,
                target_key=delivery.target_key,
                payload=delivery.effect_payload,
            )
            conn.execute(
                """
                INSERT INTO rca_delivery_effects(
                    effect_key, delivery_id, effect_kind, required,
                    target_key, payload_json, payload_sha256, outcome,
                    comment_slot_schema_version, comment_slot_key,
                    comment_slot_kind, comment_slot_generation,
                    comment_slot_revision, comment_slot_budget_exempt,
                    status, created_at, updated_at
                ) VALUES (?, ?, 'feishu_issue_comment', 1, ?, ?, ?, ?,
                          ?, ?, ?, ?, ?, ?, 'pending', ?, ?)
                """,
                (
                    delivery.effect_key,
                    delivery.delivery_id,
                    delivery.target_key,
                    _canonical_json(delivery.effect_payload),
                    delivery.semantic_payload_sha256,
                    delivery.outcome,
                    comment_slot["comment_slot_schema_version"],
                    comment_slot["comment_slot_key"],
                    comment_slot["comment_slot_kind"],
                    comment_slot["comment_slot_generation"],
                    comment_slot["comment_slot_revision"],
                    comment_slot["comment_slot_budget_exempt"],
                    current,
                    current,
                ),
            )
            created = True
        materializer = (
            materialize_subscriptions
            or cls._materialize_delivery_subscriptions_in_transaction
        )
        materializer(
            conn,
            delivery_id=delivery.delivery_id,
            current=current,
            activation_required=activation_required,
        )
        return created

    def create_terminal_delivery(
        self,
        *,
        claim: ExecutionWatchClaim,
        status: dict[str, Any],
        outcome: str,
        terminal_state: str,
        error_code: str,
        error_detail: str,
        terminal_fallback: Mapping[str, Any] | None = None,
        runtime_identity: Mapping[str, Any] | None = None,
        now: datetime | None = None,
        activation_required: bool = False,
    ) -> DeliveryCreateResult:
        """Atomically turn a failed execution into durable required effects."""
        self._validate_activation_required(activation_required)
        delivery: VerifiedTerminalDelivery = build_terminal_delivery(
            business_key=claim.business_key,
            submission_key=claim.submission_key,
            generation=claim.generation,
            project_key=claim.project_key,
            work_item_type_key=claim.work_item_type_key,
            work_item_id=claim.work_item_id,
            outcome=outcome,
            terminal_state=terminal_state,
            error_code=error_code,
            terminal_fallback=terminal_fallback,
            schema_version=(
                TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION_COMMENT_ONLY
                if terminal_fallback is not None
                else TERMINAL_DELIVERY_EFFECT_SCHEMA_VERSION_COMMENT_ONLY
            ),
        )
        # Preserve the immutable W3/W5 lineage on terminal effects as well as
        # successful report deliveries.  The contract JSON is already copied
        # into every materialized subscription, so no parallel authority column
        # is needed in this candidate schema.
        submission_binding = claim.submission_result.get("w3_execution_snapshot")
        if isinstance(submission_binding, Mapping):
            delivery.contract["w3_execution_snapshot"] = dict(submission_binding)
        current = _iso(now)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            self._current_claim(conn, claim.submission_key, claim.lease_token, current)
            activation_enforced = self._activation_enforced_tx(
                conn,
                activation_required=activation_required,
            )
            if activation_enforced:
                self._require_activation_schema(conn)
            if activation_enforced and not self._execution_activation_eligible_tx(
                conn,
                submission_key=claim.submission_key,
            ):
                raise StaleDeliveryWatchLeaseError(
                    f"delivery activation changed for {claim.submission_key}"
                )
            created = self._ensure_terminal_delivery_in_transaction(
                conn,
                delivery=delivery,
                current=current,
                materialize_subscriptions=(
                    self._materialize_delivery_subscriptions_in_transaction
                ),
                activation_required=activation_enforced,
            )
            updated = conn.execute(
                """
                UPDATE rca_execution_watch
                   SET state = 'delivery_created', delivery_id = ?, terminal_at = ?,
                       last_observed_at = ?, last_status_json = ?,
                       last_error_code = ?, last_error_detail = ?,
                       lease_token = NULL, lease_owner = NULL,
                       lease_expires_at = NULL, updated_at = ?
                 WHERE submission_key = ? AND lease_token = ?
                """,
                (
                    delivery.delivery_id,
                    current,
                    current,
                    _canonical_json(status),
                    delivery.error_code,
                    str(error_detail or "")[:1000],
                    current,
                    claim.submission_key,
                    claim.lease_token,
                ),
            )
            if updated.rowcount != 1:
                raise StaleDeliveryWatchLeaseError(claim.submission_key)
            if terminal_fallback is not None:
                fallback = dict(terminal_fallback)
                route_updated = conn.execute(
                    """
                    UPDATE rca_failure_routes
                       SET status = 'terminal_fallback', completed_at = ?,
                           next_retry_at = NULL, updated_at = ?
                     WHERE route_key = ? AND submission_key = ?
                       AND terminal_error_code = ? AND route_kind = ?
                       AND owner = ? AND work_started_at = ? AND deadline_at = ?
                       AND status != 'resolved'
                    """,
                    (
                        current,
                        current,
                        fallback.get("route_key"),
                        claim.submission_key,
                        delivery.error_code,
                        fallback.get("route_kind"),
                        fallback.get("route_owner"),
                        fallback.get("work_started_at"),
                        fallback.get("deadline_at"),
                    ),
                )
                if route_updated.rowcount != 1:
                    raise StaleDeliveryWatchLeaseError(
                        f"failure route changed for {claim.submission_key}"
                    )
            if runtime_identity is not None:
                insert_host_runtime_transition(
                    conn,
                    submission_key=claim.submission_key,
                    business_key=claim.business_key,
                    generation=claim.generation,
                    service_label="local.pnc.rca-delivery-collector",
                    transition_kind="delivery_created",
                    entity_key=delivery.delivery_id,
                    runtime_identity=runtime_identity,
                    transitioned_at=current,
                )
            conn.commit()
            return DeliveryCreateResult(
                delivery.delivery_id, delivery.effect_key, created
            )
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    @staticmethod
    def _effect_request_id(effect_key: str, fence: int) -> str:
        return f"{effect_key}:fence:{fence}"

    @staticmethod
    def _append_attempt_event(
        conn: sqlite3.Connection,
        *,
        effect_key: str,
        attempt_no: int,
        fence: int,
        request_id: str,
        outcome: str,
        current: str,
        remote_id: str = "",
        error_code: str = "",
        detail: str = "",
    ) -> None:
        event_seq = int(
            conn.execute(
                """
                SELECT COALESCE(MAX(event_seq), 0) + 1
                  FROM rca_delivery_attempts
                 WHERE effect_key = ? AND attempt_no = ?
                """,
                (effect_key, attempt_no),
            ).fetchone()[0]
        )
        conn.execute(
            """
            INSERT INTO rca_delivery_attempts(
                effect_key, attempt_no, event_seq, fence, request_id,
                outcome, remote_id, error_code, detail, started_at, finished_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                effect_key,
                max(1, int(attempt_no)),
                event_seq,
                max(0, int(fence)),
                str(request_id or "system"),
                outcome,
                str(remote_id or "")[:256],
                str(error_code or "")[:120],
                str(detail or "")[:1000],
                current,
                None if outcome == "started" else current,
            ),
        )

    @staticmethod
    def _aggregate_job_status(
        conn: sqlite3.Connection, delivery_id: str, current: str
    ) -> str:
        rows = conn.execute(
            """
            SELECT required, status FROM rca_delivery_effects
             WHERE delivery_id = ?
            """,
            (delivery_id,),
        ).fetchall()
        subscription_states: list[str] = []
        if RcaDeliveryStore._table_exists(conn, "rca_delivery_subscriptions"):
            job_identity = conn.execute(
                "SELECT business_key, generation FROM rca_delivery_jobs "
                "WHERE delivery_id = ?",
                (delivery_id,),
            ).fetchone()
            if job_identity is not None:
                subscription_states = [
                    str(row["status"])
                    for row in conn.execute(
                        """
                        SELECT status FROM rca_delivery_subscriptions
                         WHERE business_key = ? AND generation = ? AND required = 1
                        """,
                        (job_identity["business_key"], job_identity["generation"]),
                    ).fetchall()
                ]
        required = [row for row in rows if int(row["required"]) == 1]
        optional = [row for row in rows if int(row["required"]) == 0]
        if (
            any(row["status"] == "quarantined" for row in required)
            or "quarantined" in subscription_states
        ):
            status = "quarantined"
        elif (
            required
            and all(row["status"] in {"succeeded", "suppressed"} for row in required)
            and all(
                state in {"materialized", "suppressed"} for state in subscription_states
            )
        ):
            status = (
                "partial"
                if any(row["status"] == "suppressed" for row in required)
                or "suppressed" in subscription_states
                or (optional and any(row["status"] != "succeeded" for row in optional))
                else "delivered"
            )
        else:
            status = "ready"
        conn.execute(
            "UPDATE rca_delivery_jobs SET status = ?, updated_at = ? WHERE delivery_id = ?",
            (status, current, delivery_id),
        )
        return status

    def reconcile_delivery_job_status(
        self,
        *,
        delivery_id: str,
        now: datetime | None = None,
    ) -> str:
        """Recompute one job status from its durable effects and subscriptions."""
        exact_delivery_id = str(delivery_id or "").strip()
        if not exact_delivery_id:
            raise ValueError("delivery_id is required")
        current = _iso(now)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            exists = conn.execute(
                "SELECT 1 FROM rca_delivery_jobs WHERE delivery_id = ?",
                (exact_delivery_id,),
            ).fetchone()
            if exists is None:
                raise DeliveryRecordConflictError("delivery job is missing")
            status = self._aggregate_job_status(conn, exact_delivery_id, current)
            conn.commit()
            return status
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    @staticmethod
    def _validate_current_adjudication_binding_tx(
        conn: sqlite3.Connection,
        *,
        claim: DeliveryEffectClaim,
        require_immutable_binding: bool = True,
    ) -> None:
        validate_conclusion_adjudication_schema(conn)
        activation_columns = {
            str(row["name"])
            for row in conn.execute(
                "PRAGMA table_info(rca_activation_epochs)"
            ).fetchall()
        }
        if not {"epoch_id", "state", "is_current"}.issubset(activation_columns):
            raise ConclusionAdjudicationError(
                "conclusion_adjudication_effect_activation_stale"
            )
        row = conn.execute(
            """
            SELECT a.*,
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
                   epoch.is_current AS activation_is_current
              FROM rca_conclusion_adjudications AS a
              JOIN rca_delivery_effects AS original
                ON original.effect_key = a.original_effect_key
              JOIN rca_delivery_jobs AS original_job
                ON original_job.delivery_id = a.original_delivery_id
              LEFT JOIN rca_activation_epochs AS epoch
                ON epoch.epoch_id = a.activation_epoch_id
             WHERE a.correction_effect_key = ?
            """,
            (claim.effect_key,),
        ).fetchone()
        if row is None:
            if not require_immutable_binding and isinstance(claim.payload, Mapping):
                payload_epoch_id = str(
                    claim.payload.get("activation_epoch_id") or ""
                )
                active_payload_epoch = conn.execute(
                    "SELECT 1 FROM rca_activation_epochs "
                    "WHERE epoch_id = ? AND is_current = 1 "
                    "AND state = 'steady_active'",
                    (payload_epoch_id,),
                ).fetchone()
                if active_payload_epoch is not None:
                    return
                raise ConclusionAdjudicationError(
                    "conclusion_adjudication_effect_activation_stale"
                )
            raise ConclusionAdjudicationError(
                "conclusion_adjudication_effect_ledger_missing"
            )
        if (
            row["activation_is_current"] != 1
            or row["activation_state"] != "steady_active"
        ):
            raise ConclusionAdjudicationError(
                "conclusion_adjudication_effect_activation_stale"
            )
        if require_immutable_binding:
            validate_adjudication_effect_ledger_binding(claim, dict(row))

    @classmethod
    def _current_effect_claim(
        cls,
        conn: sqlite3.Connection,
        *,
        claim: DeliveryEffectClaim,
        current: str,
        allow_invalid_adjudication: bool = False,
        allow_inactive_card_patch_after_write: bool = False,
    ) -> sqlite3.Row:
        row = conn.execute(
            """
            SELECT e.*, j.created_at AS job_created_at,
                   j.submission_key AS job_submission_key
              FROM rca_delivery_effects AS e
              JOIN rca_delivery_jobs AS j ON j.delivery_id = e.delivery_id
             WHERE e.effect_key = ? AND e.lease_token = ? AND e.fence = ?
               AND e.status = 'claimed'
               AND e.lease_expires_at IS NOT NULL AND e.lease_expires_at > ?
            """,
            (claim.effect_key, claim.lease_token, claim.fence, current),
        ).fetchone()
        if row is None:
            raise StaleDeliveryEffectLeaseError(
                f"stale delivery-effect lease for {claim.effect_key} "
                f"fence {claim.fence}"
            )
        try:
            current_payload = json.loads(str(row["payload_json"] or ""))
        except (TypeError, json.JSONDecodeError):
            current_payload = {}
        claim_is_adjudication = isinstance(
            claim.payload, Mapping
        ) and identifies_adjudication_effect(
            claim.payload,
            target_key=claim.target_key,
        )
        row_is_adjudication = isinstance(
            current_payload, Mapping
        ) and identifies_adjudication_effect(
            current_payload,
            target_key=str(row["target_key"] or ""),
        )
        claim_is_card_patch = claim.effect_kind == DELIVERY_CARD_PATCH_EFFECT_KIND
        row_is_card_patch = (
            str(row["effect_kind"] or "") == DELIVERY_CARD_PATCH_EFFECT_KIND
        )
        if claim_is_card_patch or row_is_card_patch:
            if not allow_invalid_adjudication and (
                current_payload != claim.payload
                or str(row["delivery_id"] or "") != claim.delivery_id
                or str(row["effect_kind"] or "") != claim.effect_kind
                or int(row["required"]) != int(claim.required)
                or str(row["target_key"] or "") != claim.target_key
                or str(row["payload_sha256"] or "") != claim.payload_sha256
            ):
                raise DeliveryRecordConflictError(
                    "delivery_card_patch_effect_claim_mismatch"
                )
            if not allow_invalid_adjudication:
                validated = validate_card_patch_effect_payload(current_payload)
                if validated != current_payload:
                    raise DeliveryRecordConflictError(
                        "delivery_card_patch_effect_payload_mismatch"
                    )
                cls._validate_card_patch_binding_tx(
                    conn,
                    payload=validated,
                    require_current_activation=not (
                        allow_inactive_card_patch_after_write
                        and str(row["write_phase"] or "") == "write_started"
                    ),
                )
        if claim_is_adjudication or row_is_adjudication:
            if not allow_invalid_adjudication and (
                current_payload != claim.payload
                or str(row["delivery_id"] or "") != claim.delivery_id
                or str(row["effect_kind"] or "") != claim.effect_kind
                or int(row["required"]) != int(claim.required)
                or str(row["target_key"] or "") != claim.target_key
                or str(row["payload_sha256"] or "") != claim.payload_sha256
            ):
                raise ConclusionAdjudicationError(
                    "conclusion_adjudication_effect_ledger_mismatch"
                )
            cls._validate_current_adjudication_binding_tx(
                conn,
                claim=claim,
                require_immutable_binding=not allow_invalid_adjudication,
            )
        return row

    def extend_effect_lease(
        self,
        *,
        claim: DeliveryEffectClaim,
        lease_seconds: int,
        now: datetime | None = None,
    ) -> str:
        """Extend only the live fenced lease owned by this exact worker claim."""
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be positive")
        owner = str(claim.lease_owner or "").strip()
        if not owner:
            raise ValueError("claim lease_owner is required")
        current_dt = _utc_datetime(now)
        current = _iso(current_dt)
        expires = _iso(current_dt + timedelta(seconds=lease_seconds))
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            self._current_effect_claim(
                conn,
                claim=claim,
                current=current,
                allow_inactive_card_patch_after_write=(
                    claim.effect_kind == DELIVERY_CARD_PATCH_EFFECT_KIND
                ),
            )
            updated = conn.execute(
                """
                UPDATE rca_delivery_effects
                   SET lease_expires_at = ?, updated_at = ?
                 WHERE effect_key = ? AND status = 'claimed'
                   AND lease_token = ? AND fence = ? AND lease_owner = ?
                   AND lease_expires_at IS NOT NULL
                   AND lease_expires_at > ?
                """,
                (
                    expires,
                    current,
                    claim.effect_key,
                    claim.lease_token,
                    claim.fence,
                    owner,
                    current,
                ),
            )
            if updated.rowcount != 1:
                raise StaleDeliveryEffectLeaseError(
                    f"stale delivery-effect lease for {claim.effect_key} "
                    f"fence {claim.fence}"
                )
            conn.commit()
            return expires
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    @staticmethod
    def _payload_updates_issue_fields(payload_json: Any) -> bool:
        try:
            payload = json.loads(str(payload_json or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            return False
        updates = payload.get("field_updates") if isinstance(payload, dict) else None
        if not isinstance(updates, list):
            return False
        field_keys = {
            str(update.get("field_key") or "").strip()
            for update in updates
            if isinstance(update, dict)
        }
        return bool(field_keys & {RCA_RESULT_FIELD_KEY, RCA_REPORT_FIELD_KEY})

    @classmethod
    def _newer_settled_issue_field_effect_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        delivery_id: str,
    ) -> sqlite3.Row | None:
        rows = conn.execute(
            """
            SELECT newer.delivery_id, newer.generation, newer.outcome,
                   newer_effect.effect_key, newer_effect.payload_json
              FROM rca_delivery_jobs AS current_job
              JOIN rca_delivery_jobs AS newer
                ON newer.business_key = current_job.business_key
               AND newer.generation > current_job.generation
              JOIN rca_delivery_effects AS newer_effect
                ON newer_effect.delivery_id = newer.delivery_id
               AND newer_effect.effect_kind = 'feishu_issue_comment'
               AND newer_effect.required = 1
               AND newer_effect.status = 'succeeded'
               AND newer_effect.write_phase = 'settled'
             WHERE current_job.delivery_id = ?
             ORDER BY newer.generation DESC, newer.delivery_id,
                      newer_effect.effect_key
            """,
            (delivery_id,),
        ).fetchall()
        return next(
            (
                row
                for row in rows
                if cls._payload_updates_issue_fields(row["payload_json"])
            ),
            None,
        )

    @classmethod
    def _suppress_terminal_effect_if_newer_settled_fields_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        claim: DeliveryEffectClaim,
        row: sqlite3.Row,
        current: str,
    ) -> DeliveryEffectMutation | None:
        if claim.outcome not in TERMINAL_DELIVERY_OUTCOMES:
            return None
        newer = cls._newer_settled_issue_field_effect_tx(
            conn,
            delivery_id=claim.delivery_id,
        )
        if newer is None:
            return None
        reason = "delivery_effect_superseded_by_newer_settled_fields"
        detail = "newer generation already confirmed its issue fields"
        receipt = {
            "source": reason,
            "superseding_delivery_id": str(newer["delivery_id"]),
            "superseding_effect_key": str(newer["effect_key"]),
            "superseding_generation": int(newer["generation"]),
            "superseding_outcome": str(newer["outcome"]),
        }
        cls._append_attempt_event(
            conn,
            effect_key=claim.effect_key,
            attempt_no=claim.attempt,
            fence=claim.fence,
            request_id=claim.request_id,
            outcome="reconciled",
            current=current,
            error_code=reason,
            detail=detail,
        )
        updated = conn.execute(
            """
            UPDATE rca_delivery_effects
               SET status = 'suppressed', write_phase = 'settled',
                   next_attempt_at = NULL, remote_receipt_json = ?,
                   completed_at = ?, last_error_code = ?,
                   last_error_detail = ?, lease_token = NULL,
                   lease_owner = NULL, lease_expires_at = NULL, updated_at = ?
             WHERE effect_key = ? AND lease_token = ? AND fence = ?
               AND status = 'claimed'
            """,
            (
                _canonical_json(receipt),
                current,
                reason,
                detail,
                current,
                claim.effect_key,
                claim.lease_token,
                claim.fence,
            ),
        )
        if updated.rowcount != 1:
            raise StaleDeliveryEffectLeaseError(
                f"stale superseded effect claim for {claim.effect_key}"
            )
        job_status = cls._aggregate_job_status(conn, str(row["delivery_id"]), current)
        return DeliveryEffectMutation(
            claim.effect_key,
            claim.delivery_id,
            "suppressed",
            job_status,
        )

    def mark_effect_write_started(
        self,
        *,
        claim: DeliveryEffectClaim,
        now: datetime | None = None,
        activation_required: bool = False,
    ) -> DeliveryEffectMutation | None:
        """Persist and revalidate the fenced remote-write ambiguity boundary."""
        self._validate_activation_required(activation_required)
        current = _iso(now)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = self._current_effect_claim(
                conn,
                claim=claim,
                current=current,
            )
            activation_enforced = self._activation_enforced_tx(
                conn,
                activation_required=activation_required,
            )
            if activation_enforced:
                self._require_activation_schema(conn)
            if activation_enforced and not self._effect_activation_eligible_tx(
                conn,
                effect_key=claim.effect_key,
                submission_key=str(row["job_submission_key"]),
            ):
                raise StaleDeliveryEffectLeaseError(
                    f"delivery activation changed for {claim.effect_key}"
                )
            suppressed = self._suppress_terminal_effect_if_newer_settled_fields_tx(
                conn,
                claim=claim,
                row=row,
                current=current,
            )
            if suppressed is not None:
                conn.commit()
                return suppressed
            if str(row["write_phase"] or "") != "prewrite":
                raise DeliveryRecordConflictError(
                    "delivery effect crossed its write boundary more than once"
                )
            updated = conn.execute(
                """
                UPDATE rca_delivery_effects
                   SET write_phase = 'write_started', write_started_at = ?,
                       reconciliation_miss_count = 0, updated_at = ?
                 WHERE effect_key = ? AND lease_token = ? AND fence = ?
                   AND status = 'claimed' AND write_phase = 'prewrite'
                """,
                (
                    current,
                    current,
                    claim.effect_key,
                    claim.lease_token,
                    claim.fence,
                ),
            )
            if updated.rowcount != 1:
                raise StaleDeliveryEffectLeaseError(
                    f"stale delivery-effect write boundary for {claim.effect_key}"
                )
            conn.commit()
            return None
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def suppress_terminal_effect_if_newer_settled_fields(
        self,
        *,
        claim: DeliveryEffectClaim,
        now: datetime | None = None,
    ) -> DeliveryEffectMutation | None:
        """Settle a stale terminal before it can overwrite newer settled fields."""

        if claim.outcome not in TERMINAL_DELIVERY_OUTCOMES:
            return None
        current = _iso(now)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = self._current_effect_claim(
                conn,
                claim=claim,
                current=current,
            )
            suppressed = self._suppress_terminal_effect_if_newer_settled_fields_tx(
                conn,
                claim=claim,
                row=row,
                current=current,
            )
            conn.commit()
            return suppressed
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def suppress_effect_for_quality_regression(
        self,
        *,
        claim: DeliveryEffectClaim,
        error_code: str,
        error_detail: str,
        receipt: dict[str, Any],
        now: datetime | None = None,
    ) -> DeliveryEffectMutation:
        """Settle a pre-write effect that would degrade an existing RCA result.

        This is deliberately a separate terminal state from ``succeeded``: no
        Feishu write happened, so the delivery cannot be represented as an
        acknowledged external update.  The suppressed effect remains visible in
        the attempt/effect receipt for operators while the already-published
        higher-quality result stays authoritative.
        """
        current = _iso(now)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = self._current_effect_claim(
                conn,
                claim=claim,
                current=current,
            )
            if str(row["write_phase"] or "") != "prewrite":
                raise DeliveryRecordConflictError(
                    "quality regression guard crossed the remote-write boundary"
                )
            self._append_attempt_event(
                conn,
                effect_key=claim.effect_key,
                attempt_no=claim.attempt,
                fence=claim.fence,
                request_id=claim.request_id,
                outcome="reconciled",
                current=current,
                error_code=error_code,
                detail=error_detail,
            )
            updated = conn.execute(
                """
                UPDATE rca_delivery_effects
                   SET status = 'suppressed', write_phase = 'settled',
                       next_attempt_at = NULL, remote_receipt_json = ?,
                       completed_at = ?, last_error_code = ?,
                       last_error_detail = ?, lease_token = NULL,
                       lease_owner = NULL, lease_expires_at = NULL, updated_at = ?
                 WHERE effect_key = ? AND lease_token = ? AND fence = ?
                   AND status = 'claimed' AND write_phase = 'prewrite'
                """,
                (
                    _canonical_json(receipt),
                    current,
                    str(error_code or "")[:120],
                    str(error_detail or "")[:1000],
                    current,
                    claim.effect_key,
                    claim.lease_token,
                    claim.fence,
                ),
            )
            if updated.rowcount != 1:
                raise StaleDeliveryEffectLeaseError(
                    f"stale quality-regression effect claim for {claim.effect_key}"
                )
            job_status = self._aggregate_job_status(
                conn, str(row["delivery_id"]), current
            )
            conn.commit()
            return DeliveryEffectMutation(
                claim.effect_key,
                claim.delivery_id,
                "suppressed",
                job_status,
            )
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def suppress_expired_card_patch(
        self,
        *,
        claim: DeliveryEffectClaim,
        error_detail: str,
        receipt: Mapping[str, Any],
        now: datetime | None = None,
    ) -> DeliveryEffectMutation:
        """Settle a deterministic Feishu fourteen-day PATCH refusal."""

        if claim.effect_kind != DELIVERY_CARD_PATCH_EFFECT_KIND:
            raise DeliveryRecordConflictError(
                "delivery_card_patch_effect_kind_invalid"
            )
        error_code = "feishu_card_patch_message_expired"
        current = _iso(now)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = self._current_effect_claim(
                conn,
                claim=claim,
                current=current,
                allow_inactive_card_patch_after_write=True,
            )
            validated = validate_card_patch_effect_payload(claim.payload)
            if dict(receipt) != _card_patch_suppression_receipt(validated):
                raise DeliveryRecordConflictError(
                    "delivery_card_patch_effect_receipt_invalid"
                )
            if str(row["write_phase"] or "") != "write_started":
                raise DeliveryRecordConflictError(
                    "delivery_card_patch_suppression_before_write"
                )
            self._append_attempt_event(
                conn,
                effect_key=claim.effect_key,
                attempt_no=claim.attempt,
                fence=claim.fence,
                request_id=claim.request_id,
                outcome="reconciled",
                current=current,
                error_code=error_code,
                detail=error_detail,
            )
            updated = conn.execute(
                """
                UPDATE rca_delivery_effects
                   SET status = 'suppressed', write_phase = 'settled',
                       next_attempt_at = NULL, remote_receipt_json = ?,
                       completed_at = ?, last_error_code = ?,
                       last_error_detail = ?, lease_token = NULL,
                       lease_owner = NULL, lease_expires_at = NULL, updated_at = ?
                 WHERE effect_key = ? AND lease_token = ? AND fence = ?
                   AND status = 'claimed' AND write_phase = 'write_started'
                """,
                (
                    _canonical_json(dict(receipt)),
                    current,
                    error_code,
                    str(error_detail or "")[:1000],
                    current,
                    claim.effect_key,
                    claim.lease_token,
                    claim.fence,
                ),
            )
            if updated.rowcount != 1:
                raise StaleDeliveryEffectLeaseError(
                    f"stale expired card-patch claim for {claim.effect_key}"
                )
            job_status = self._aggregate_job_status(
                conn, str(row["delivery_id"]), current
            )
            conn.commit()
            return DeliveryEffectMutation(
                claim.effect_key,
                claim.delivery_id,
                "suppressed",
                job_status,
            )
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def record_effect_reconciliation_miss(
        self,
        *,
        claim: DeliveryEffectClaim,
        visibility_grace_seconds: int,
        minimum_missing_reads: int,
        recovery_interval_seconds: int,
        max_recovery_writes: int,
        now: datetime | None = None,
    ) -> DeliveryReconciliationState:
        """Record a strict marker miss and evaluate bounded recovery eligibility."""
        if (
            visibility_grace_seconds < 1
            or minimum_missing_reads < 2
            or recovery_interval_seconds < 1
            or max_recovery_writes < 1
        ):
            raise ValueError("delivery reconciliation policy is invalid")
        current_dt = _utc_datetime(now)
        current = _iso(current_dt)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = self._current_effect_claim(
                conn,
                claim=claim,
                current=current,
            )
            if str(row["write_phase"] or "") != "write_started":
                raise DeliveryRecordConflictError(
                    "delivery reconciliation requires an entered write boundary"
                )
            try:
                write_started_at = _parse_iso(str(row["write_started_at"] or ""))
                last_recovery_at = (
                    _parse_iso(str(row["last_recovery_write_at"]))
                    if row["last_recovery_write_at"]
                    else None
                )
            except ValueError as exc:
                raise DeliveryRecordConflictError(
                    "delivery reconciliation timestamps are invalid"
                ) from exc
            missing_reads = int(row["reconciliation_miss_count"]) + 1
            visibility_anchor = last_recovery_at or write_started_at
            grace_elapsed = (
                current_dt - visibility_anchor
            ).total_seconds() >= visibility_grace_seconds
            interval_elapsed = (
                last_recovery_at is None
                or (current_dt - last_recovery_at).total_seconds()
                >= recovery_interval_seconds
            )
            updated = conn.execute(
                """
                UPDATE rca_delivery_effects
                   SET reconciliation_miss_count = ?, updated_at = ?
                 WHERE effect_key = ? AND lease_token = ? AND fence = ?
                   AND status = 'claimed' AND write_phase = 'write_started'
                """,
                (
                    missing_reads,
                    current,
                    claim.effect_key,
                    claim.lease_token,
                    claim.fence,
                ),
            )
            if updated.rowcount != 1:
                raise StaleDeliveryEffectLeaseError(
                    f"stale delivery-effect reconciliation for {claim.effect_key}"
                )
            conn.commit()
            recovery_write_count = int(row["recovery_write_count"])
            recovery_limit_reached = recovery_write_count >= max_recovery_writes
            return DeliveryReconciliationState(
                missing_read_count=missing_reads,
                recovery_write_count=recovery_write_count,
                visibility_grace_elapsed=grace_elapsed,
                recovery_interval_elapsed=interval_elapsed,
                recovery_eligible=(
                    grace_elapsed
                    and interval_elapsed
                    and missing_reads >= minimum_missing_reads
                    and not recovery_limit_reached
                ),
                recovery_limit_exceeded=(
                    grace_elapsed
                    and missing_reads >= minimum_missing_reads
                    and recovery_limit_reached
                ),
            )
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def authorize_adjudication_comment_attempt(
        self,
        *,
        claim: DeliveryEffectClaim,
        now: datetime | None = None,
        activation_required: bool = False,
    ) -> bool:
        """Consume the correction's single remote comment-attempt token."""

        self._validate_activation_required(activation_required)
        current = _iso(now)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = self._current_effect_claim(
                conn,
                claim=claim,
                current=current,
            )
            try:
                payload = json.loads(str(row["payload_json"] or ""))
            except (TypeError, json.JSONDecodeError) as exc:
                raise DeliveryRecordConflictError(
                    "adjudication effect payload is invalid"
                ) from exc
            if (
                not isinstance(payload, Mapping)
                or payload.get("schema_version")
                != ADJUDICATION_EFFECT_SCHEMA_VERSION
            ):
                raise DeliveryRecordConflictError(
                    "comment-attempt token is only valid for adjudication effects"
                )
            activation_enforced = self._activation_enforced_tx(
                conn,
                activation_required=activation_required,
            )
            if not activation_enforced:
                raise StaleDeliveryEffectLeaseError(
                    f"delivery activation unavailable for {claim.effect_key}"
                )
            self._require_activation_schema(conn)
            if not self._effect_activation_eligible_tx(
                conn,
                effect_key=claim.effect_key,
                submission_key=str(row["job_submission_key"]),
            ):
                raise StaleDeliveryEffectLeaseError(
                    f"delivery activation changed for {claim.effect_key}"
                )
            if str(row["write_phase"] or "") != "write_started":
                raise DeliveryRecordConflictError(
                    "adjudication comment requires an entered write boundary"
                )
            if int(row["adjudication_comment_attempt_count"]) != 0:
                conn.commit()
                return False
            updated = conn.execute(
                """
                UPDATE rca_delivery_effects
                   SET adjudication_comment_attempt_count = 1,
                       adjudication_comment_attempted_at = ?, updated_at = ?
                 WHERE effect_key = ? AND lease_token = ? AND fence = ?
                   AND status = 'claimed'
                   AND write_phase = 'write_started'
                   AND adjudication_comment_attempt_count = 0
                """,
                (
                    current,
                    current,
                    claim.effect_key,
                    claim.lease_token,
                    claim.fence,
                ),
            )
            if updated.rowcount != 1:
                raise StaleDeliveryEffectLeaseError(
                    f"stale adjudication comment attempt for {claim.effect_key}"
                )
            conn.commit()
            return True
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def authorize_effect_recovery_write(
        self,
        *,
        claim: DeliveryEffectClaim,
        visibility_grace_seconds: int,
        minimum_missing_reads: int,
        recovery_interval_seconds: int,
        max_recovery_writes: int,
        now: datetime | None = None,
        activation_required: bool = False,
    ) -> int | None:
        """Atomically consume one rate-limited recovery-write authorization."""
        self._validate_activation_required(activation_required)
        if (
            visibility_grace_seconds < 1
            or minimum_missing_reads < 2
            or recovery_interval_seconds < 1
            or max_recovery_writes < 1
        ):
            raise ValueError("delivery reconciliation policy is invalid")
        current_dt = _utc_datetime(now)
        current = _iso(current_dt)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = self._current_effect_claim(
                conn,
                claim=claim,
                current=current,
            )
            activation_enforced = self._activation_enforced_tx(
                conn,
                activation_required=activation_required,
            )
            if activation_enforced:
                self._require_activation_schema(conn)
            if activation_enforced and not self._effect_activation_eligible_tx(
                conn,
                effect_key=claim.effect_key,
                submission_key=str(row["job_submission_key"]),
            ):
                raise StaleDeliveryEffectLeaseError(
                    f"delivery activation changed for {claim.effect_key}"
                )
            if str(row["write_phase"] or "") != "write_started":
                raise DeliveryRecordConflictError(
                    "delivery recovery requires an entered write boundary"
                )
            try:
                write_started_at = _parse_iso(str(row["write_started_at"] or ""))
                last_recovery_at = (
                    _parse_iso(str(row["last_recovery_write_at"]))
                    if row["last_recovery_write_at"]
                    else None
                )
            except ValueError as exc:
                raise DeliveryRecordConflictError(
                    "delivery recovery timestamps are invalid"
                ) from exc
            missing_reads = int(row["reconciliation_miss_count"])
            recovery_write_count = int(row["recovery_write_count"])
            grace_elapsed = (
                current_dt - (last_recovery_at or write_started_at)
            ).total_seconds() >= visibility_grace_seconds
            interval_elapsed = (
                last_recovery_at is None
                or (current_dt - last_recovery_at).total_seconds()
                >= recovery_interval_seconds
            )
            if (
                missing_reads < minimum_missing_reads
                or not grace_elapsed
                or not interval_elapsed
                or recovery_write_count >= max_recovery_writes
            ):
                conn.commit()
                return None
            recovery_write_count += 1
            updated = conn.execute(
                """
                UPDATE rca_delivery_effects
                   SET recovery_write_count = ?, last_recovery_write_at = ?,
                       reconciliation_miss_count = 0,
                       last_error_code = 'delivery_effect_recovery_write_authorized',
                       last_error_detail = ?, updated_at = ?
                 WHERE effect_key = ? AND lease_token = ? AND fence = ?
                   AND status = 'claimed' AND write_phase = 'write_started'
                """,
                (
                    recovery_write_count,
                    current,
                    (
                        "controlled recovery write authorized after "
                        f"{missing_reads} strict marker misses"
                    ),
                    current,
                    claim.effect_key,
                    claim.lease_token,
                    claim.fence,
                ),
            )
            if updated.rowcount != 1:
                raise StaleDeliveryEffectLeaseError(
                    f"stale delivery-effect recovery write for {claim.effect_key}"
                )
            conn.commit()
            return recovery_write_count
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def claim_due_effect(
        self,
        *,
        lease_owner: str,
        lease_seconds: int = 60,
        max_age_seconds: int = 86_400,
        now: datetime | None = None,
        activation_required: bool = False,
    ) -> DeliveryEffectClaim | None:
        self._validate_activation_required(activation_required)
        self.materialize_pending_subscriptions(
            limit=100,
            now=now,
            activation_required=activation_required,
        )
        owner = str(lease_owner or "").strip()
        if not owner:
            raise ValueError("lease_owner is required")
        if lease_seconds < 1 or max_age_seconds < 1:
            raise ValueError("lease and max age must be positive")
        current_dt = _utc_datetime(now)
        current = _iso(current_dt)
        expires = _iso(current_dt + timedelta(seconds=lease_seconds))
        cutoff = _iso(current_dt - timedelta(seconds=max_age_seconds))
        token = uuid.uuid4().hex
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            activation_enforced = self._activation_enforced_tx(
                conn,
                activation_required=activation_required,
            )
            if activation_enforced:
                self._require_activation_schema(conn)
            acceptance_ready = {
                "business_key",
                "generation",
                "created_at",
            }.issubset(self._table_columns_tx(conn, "business_triggers"))
            acceptance_join = (
                """
             LEFT JOIN business_triggers AS accepted_trigger
                    ON accepted_trigger.business_key = j.business_key
                   AND accepted_trigger.generation = j.generation
                """
                if acceptance_ready
                else ""
            )
            acceptance_select = (
                "accepted_trigger.created_at AS business_accepted_at"
                if acceptance_ready
                else "e.created_at AS business_accepted_at"
            )
            activation_joins = (
                """
             LEFT JOIN rca_execution_watch AS w
                    ON w.submission_key = j.submission_key
             LEFT JOIN rca_outbox AS o
                    ON o.outbox_id = w.submission_outbox_id
             LEFT JOIN business_triggers AS t
                    ON t.business_key = o.business_key
                   AND t.generation = o.generation
                """
                if activation_enforced
                else ""
            )
            activation_filter = (
                "AND ("
                f"({_ACTIVATION_EXECUTION_ELIGIBLE_SQL}) OR "
                f"({_ADJUDICATION_ACTIVATION_ELIGIBLE_SQL}) OR "
                f"({_CARD_PATCH_ACTIVATION_ELIGIBLE_SQL})"
                ")"
                if activation_enforced
                else ""
            )
            expiration_activation_filter = (
                "AND (("
                f"({_ACTIVATION_EXECUTION_ELIGIBLE_SQL}) OR "
                f"({_ADJUDICATION_ACTIVATION_ELIGIBLE_SQL}) OR "
                f"({_CARD_PATCH_ACTIVATION_ELIGIBLE_SQL})"
                ") OR (e.effect_kind = 'feishu_card_patch' "
                "AND e.write_phase = 'prewrite'))"
                if activation_enforced
                else ""
            )
            expired = conn.execute(
                f"""
                SELECT e.effect_key, e.delivery_id, e.effect_kind,
                       e.attempt, e.fence
                  FROM rca_delivery_effects AS e
                  JOIN rca_delivery_jobs AS j ON j.delivery_id = e.delivery_id
                  {activation_joins}
                 WHERE e.created_at <= ?
                   AND (
                        e.status IN ('pending', 'retry_wait', 'uncertain')
                        OR (
                            e.status = 'claimed' AND e.lease_expires_at IS NOT NULL
                            AND e.lease_expires_at <= ?
                        )
                   )
                   {expiration_activation_filter}
                """,
                (cutoff, current),
            ).fetchall()
            for row in expired:
                attempt_no = max(1, int(row["attempt"]))
                fence = int(row["fence"])
                self._append_attempt_event(
                    conn,
                    effect_key=str(row["effect_key"]),
                    attempt_no=attempt_no,
                    fence=fence,
                    request_id=self._effect_request_id(str(row["effect_key"]), fence),
                    outcome="quarantined",
                    current=current,
                    error_code="delivery_effect_age_exceeded",
                    detail="delivery effect exceeded the 24 hour retry horizon",
                )
                conn.execute(
                    """
                    UPDATE rca_delivery_effects
                       SET status = 'quarantined', quarantined_at = ?,
                           write_phase = 'settled',
                           next_attempt_at = NULL, lease_token = NULL,
                           lease_owner = NULL, lease_expires_at = NULL,
                           last_error_code = 'delivery_effect_age_exceeded',
                           last_error_detail = 'delivery effect exceeded the retry horizon',
                           updated_at = ?
                     WHERE effect_key = ?
                    """,
                    (current, current, row["effect_key"]),
                )
                self._aggregate_job_status(conn, str(row["delivery_id"]), current)
                self._record_permanent_failure_in_transaction(
                    conn,
                    circuit_name=str(row["effect_kind"]),
                    subject_key=str(row["effect_key"]),
                    failure_state="quarantined",
                    error_code="delivery_effect_age_exceeded",
                    error_detail="delivery effect exceeded the retry horizon",
                    current=current,
                )

            row = conn.execute(
                f"""
                SELECT e.*, j.artifact_set_id, j.project_key,
                       j.work_item_type_key, j.work_item_id, j.issue_url,
                       j.report_url, j.outcome AS job_outcome,
                       j.outcome_key AS job_outcome_key,
                       j.business_key AS job_business_key,
                       j.submission_key AS job_submission_key,
                       j.generation AS job_generation,
                       j.terminal_state AS job_terminal_state,
                       j.terminal_error_code AS job_terminal_error_code,
                       j.manifest_json, j.contract_json, j.artifacts_json,
                       {acceptance_select}
                  FROM rca_delivery_effects AS e
                  JOIN rca_delivery_jobs AS j ON j.delivery_id = e.delivery_id
                  JOIN rca_delivery_dispatcher_circuit AS circuit
                    ON circuit.circuit_name = e.effect_kind
                   AND circuit.state = 'closed'
                  {acceptance_join}
                  {activation_joins}
                 WHERE (
                        (
                            e.status IN ('pending', 'retry_wait', 'uncertain')
                            AND (e.next_attempt_at IS NULL OR e.next_attempt_at <= ?)
                        )
                        OR (
                            e.status = 'claimed' AND e.lease_expires_at IS NOT NULL
                            AND e.lease_expires_at <= ?
                        )
                 )
                   {activation_filter}
                 ORDER BY COALESCE(e.next_attempt_at, e.created_at), e.created_at,
                          e.effect_key
                 LIMIT 1
                """,
                (current, current),
            ).fetchone()
            if row is None:
                conn.commit()
                return None
            business_accepted_at = str(row["business_accepted_at"] or "").strip()
            if acceptance_ready:
                try:
                    business_accepted_at = _parse_iso(
                        business_accepted_at
                    ).isoformat()
                except (TypeError, ValueError) as exc:
                    raise DeliveryRecordConflictError(
                        "delivery_business_acceptance_timestamp_invalid"
                    ) from exc
            previous_status = str(row["status"])
            previous_write_phase = str(row["write_phase"] or "")
            if previous_write_phase not in {"prewrite", "write_started"}:
                raise DeliveryRecordConflictError(
                    "dispatchable delivery effect has an invalid write phase"
                )
            if previous_status == "claimed":
                old_fence = int(row["fence"])
                write_was_started = previous_write_phase == "write_started"
                self._append_attempt_event(
                    conn,
                    effect_key=str(row["effect_key"]),
                    attempt_no=int(row["attempt"]),
                    fence=old_fence,
                    request_id=self._effect_request_id(
                        str(row["effect_key"]), old_fence
                    ),
                    outcome="unknown" if write_was_started else "nack",
                    current=current,
                    error_code=(
                        "delivery_effect_write_outcome_unknown"
                        if write_was_started
                        else "delivery_effect_prewrite_lease_expired"
                    ),
                    detail=(
                        "worker lease expired after entering the remote write boundary"
                        if write_was_started
                        else "worker lease expired before entering the remote write boundary"
                    ),
                )
                previous_status = "uncertain" if write_was_started else "retry_wait"
            elif (
                previous_status == "uncertain"
                and previous_write_phase != "write_started"
            ):
                raise DeliveryRecordConflictError(
                    "uncertain delivery effect is missing its write boundary"
                )
            next_write_phase = (
                "write_started" if previous_status == "uncertain" else "prewrite"
            )
            next_write_started_at = (
                str(row["write_started_at"])
                if next_write_phase == "write_started" and row["write_started_at"]
                else None
            )
            next_reconciliation_miss_count = (
                int(row["reconciliation_miss_count"])
                if next_write_phase == "write_started"
                else 0
            )
            next_attempt = int(row["attempt"]) + 1
            next_fence = int(row["fence"]) + 1
            request_id = self._effect_request_id(str(row["effect_key"]), next_fence)
            updated = conn.execute(
                """
                UPDATE rca_delivery_effects
                   SET status = 'claimed', attempt = ?, fence = ?,
                       lease_token = ?, lease_owner = ?, lease_expires_at = ?,
                       write_phase = ?, write_started_at = ?,
                       reconciliation_miss_count = ?,
                       next_attempt_at = NULL, updated_at = ?
                 WHERE effect_key = ?
                   AND (
                        status IN ('pending', 'retry_wait', 'uncertain')
                        OR (
                            status = 'claimed' AND lease_expires_at IS NOT NULL
                            AND lease_expires_at <= ?
                        )
                   )
                """,
                (
                    next_attempt,
                    next_fence,
                    token,
                    owner,
                    expires,
                    next_write_phase,
                    next_write_started_at,
                    next_reconciliation_miss_count,
                    current,
                    row["effect_key"],
                    current,
                ),
            )
            if updated.rowcount != 1:
                conn.rollback()
                return None
            self._append_attempt_event(
                conn,
                effect_key=str(row["effect_key"]),
                attempt_no=next_attempt,
                fence=next_fence,
                request_id=request_id,
                outcome="started",
                current=current,
            )
            conn.commit()
            artifacts = json.loads(str(row["artifacts_json"] or "[]"))
            if not isinstance(artifacts, list):
                artifacts = []
            if str(row["outcome"] or "success") != str(row["job_outcome"] or "success"):
                raise DeliveryRecordConflictError(
                    "delivery effect outcome does not match its job"
                )
            return DeliveryEffectClaim(
                effect_key=str(row["effect_key"]),
                delivery_id=str(row["delivery_id"]),
                effect_kind=str(row["effect_kind"]),
                required=bool(row["required"]),
                target_key=str(row["target_key"]),
                payload=_json_object(row["payload_json"]),
                payload_sha256=str(row["payload_sha256"]),
                previous_status=previous_status,
                attempt=next_attempt,
                fence=next_fence,
                request_id=request_id,
                lease_token=token,
                lease_owner=owner,
                lease_expires_at=expires,
                effect_created_at=str(row["created_at"]),
                business_accepted_at=business_accepted_at,
                artifact_set_id=str(row["artifact_set_id"]),
                project_key=str(row["project_key"]),
                work_item_type_key=str(row["work_item_type_key"]),
                work_item_id=str(row["work_item_id"]),
                issue_url=str(row["issue_url"]),
                report_url=str(row["report_url"]),
                manifest=_json_object(row["manifest_json"]),
                artifacts=artifacts,
                contract=_json_object(row["contract_json"]),
                write_phase=next_write_phase,
                write_started_at=next_write_started_at,
                reconciliation_miss_count=next_reconciliation_miss_count,
                recovery_write_count=int(row["recovery_write_count"]),
                last_recovery_write_at=(
                    str(row["last_recovery_write_at"])
                    if row["last_recovery_write_at"]
                    else None
                ),
                outcome=str(row["job_outcome"] or "success"),
                terminal_state=str(row["job_terminal_state"] or ""),
                terminal_error_code=str(row["job_terminal_error_code"] or ""),
                outcome_key=str(row["job_outcome_key"] or ""),
                business_key=str(row["job_business_key"] or ""),
                submission_key=str(row["job_submission_key"] or ""),
                generation=int(row["job_generation"]),
                adjudication_comment_attempt_count=int(
                    row["adjudication_comment_attempt_count"]
                ),
                adjudication_comment_attempted_at=(
                    str(row["adjudication_comment_attempted_at"])
                    if row["adjudication_comment_attempted_at"]
                    else None
                ),
            )
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def complete_effect(
        self,
        *,
        claim: DeliveryEffectClaim,
        outcome: str,
        remote_id: str,
        receipt: dict[str, Any],
        observation: Mapping[str, Any] | None = None,
        runtime_identity: Mapping[str, Any] | None = None,
        now: datetime | None = None,
    ) -> DeliveryEffectMutation:
        if outcome not in {"ack", "reconciled"}:
            raise ValueError("successful effect outcome must be ack or reconciled")
        if not str(remote_id or "").strip():
            raise ValueError("remote_id is required for a successful effect")
        current = _iso(now)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = self._current_effect_claim(
                conn,
                claim=claim,
                current=current,
                allow_inactive_card_patch_after_write=(
                    claim.effect_kind == DELIVERY_CARD_PATCH_EFFECT_KIND
                ),
            )
            if claim.effect_kind == DELIVERY_CARD_PATCH_EFFECT_KIND:
                validated = validate_card_patch_effect_payload(claim.payload)
                if (
                    str(remote_id) != validated["message_id"]
                    or dict(receipt) != _card_patch_success_receipt(validated)
                ):
                    raise DeliveryRecordConflictError(
                        "delivery_card_patch_effect_receipt_invalid"
                    )
            normalized_observation = self._bound_delivery_observation_intent(
                claim=claim,
                remote_id=remote_id,
                observation=observation,
            )
            self._append_attempt_event(
                conn,
                effect_key=claim.effect_key,
                attempt_no=claim.attempt,
                fence=claim.fence,
                request_id=claim.request_id,
                outcome=outcome,
                current=current,
                remote_id=remote_id,
            )
            conn.execute(
                """
                UPDATE rca_delivery_effects
                   SET status = 'succeeded', next_attempt_at = NULL,
                       write_phase = 'settled',
                       remote_receipt_json = ?, completed_at = ?,
                       last_error_code = '', last_error_detail = '',
                       lease_token = NULL, lease_owner = NULL,
                       lease_expires_at = NULL, updated_at = ?
                 WHERE effect_key = ? AND lease_token = ? AND fence = ?
                """,
                (
                    _canonical_json(receipt),
                    current,
                    current,
                    claim.effect_key,
                    claim.lease_token,
                    claim.fence,
                ),
            )
            job_status = self._aggregate_job_status(
                conn, str(row["delivery_id"]), current
            )
            if job_status in {"delivered", "partial"}:
                self._reset_permanent_failure_streak_in_transaction(
                    conn,
                    circuit_name=claim.effect_kind,
                    require_closed_circuit=True,
                )
            if runtime_identity is not None:
                job_identity = conn.execute(
                    """
                    SELECT submission_key, business_key, generation
                      FROM rca_delivery_jobs WHERE delivery_id = ?
                    """,
                    (claim.delivery_id,),
                ).fetchone()
                if job_identity is None:
                    raise RuntimeError("delivery runtime transition job is missing")
                insert_host_runtime_transition(
                    conn,
                    submission_key=str(job_identity["submission_key"]),
                    business_key=str(job_identity["business_key"]),
                    generation=int(job_identity["generation"]),
                    service_label="local.pnc.rca-delivery-dispatcher",
                    transition_kind="effect_succeeded",
                    entity_key=claim.effect_key,
                    runtime_identity=runtime_identity,
                    transitioned_at=current,
                )
            observation_id, payload_json, payload_sha256 = normalized_observation
            try:
                conn.execute(
                    """
                    INSERT INTO rca_delivery_observation_outbox(
                        observation_id, effect_key, payload_json,
                        payload_sha256, status, created_at, appended_at
                    ) VALUES(?, ?, ?, ?, 'pending', ?, NULL)
                    """,
                    (
                        observation_id,
                        claim.effect_key,
                        payload_json,
                        payload_sha256,
                        current,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise DeliveryRecordConflictError(
                    "delivery_observation_intent_conflict"
                ) from exc
            conn.commit()
            return DeliveryEffectMutation(
                claim.effect_key, claim.delivery_id, "succeeded", job_status
            )
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    @staticmethod
    def _effect_observation_content_sha256(claim: DeliveryEffectClaim) -> str:
        if claim.effect_kind == DELIVERY_CARD_PATCH_EFFECT_KIND:
            validated = validate_card_patch_effect_payload(claim.payload)
            content = _canonical_json(validated["card_payload"])
        elif claim.effect_kind == DELIVERY_EFFECT_KIND:
            content = str(claim.payload.get("comment_content") or "")
        elif claim.effect_kind == DELIVERY_THREAD_EFFECT_KIND:
            content = str(claim.payload.get("message_content") or "")
        else:
            raise DeliveryRecordConflictError(
                "delivery_observation_effect_kind_invalid"
            )
        if not content:
            raise DeliveryRecordConflictError(
                "delivery_observation_effect_content_missing"
            )
        return hashlib.sha256(content.encode("utf-8")).hexdigest()

    @classmethod
    def _bound_delivery_observation_intent(
        cls,
        *,
        claim: DeliveryEffectClaim,
        remote_id: str,
        observation: Mapping[str, Any] | None,
    ) -> tuple[str, str, str]:
        if observation is None:
            raise DeliveryRecordConflictError("delivery_observation_required")
        payload = validate_delivery_observation(observation)
        expected = {
            "remote_receipt_id": str(remote_id),
            "work_item_id": claim.work_item_id,
            "case_key": claim.submission_key or claim.business_key,
            "outcome_content_sha256": cls._effect_observation_content_sha256(
                claim
            ),
        }
        for field, expected_value in expected.items():
            if payload.get(field) != expected_value:
                raise DeliveryRecordConflictError(
                    f"delivery_observation_{field}_mismatch"
                )
        payload_json = _canonical_json(payload)
        return (
            payload["observation_id"],
            payload_json,
            hashlib.sha256(payload_json.encode("utf-8")).hexdigest(),
        )

    def record_postwrite_delivery_observation(
        self,
        *,
        claim: DeliveryEffectClaim,
        remote_id: str,
        observation: Mapping[str, Any],
        now: datetime | None = None,
    ) -> bool:
        """Persist the exact observation after a provider success lost settlement.

        This path never repeats or settles the external effect. It is limited to
        card PATCH writes, whose provider has no exact readback reconciliation.
        """
        if claim.effect_kind != DELIVERY_CARD_PATCH_EFFECT_KIND:
            raise DeliveryRecordConflictError(
                "delivery_postwrite_observation_effect_kind_invalid"
            )
        validated = validate_card_patch_effect_payload(claim.payload)
        if str(remote_id or "") != validated["message_id"]:
            raise DeliveryRecordConflictError(
                "delivery_card_patch_effect_receipt_invalid"
            )
        observation_id, payload_json, payload_sha256 = (
            self._bound_delivery_observation_intent(
                claim=claim,
                remote_id=remote_id,
                observation=observation,
            )
        )
        current = _iso(now)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """
                SELECT e.delivery_id, e.effect_kind, e.target_key,
                       e.payload_json, e.payload_sha256, e.write_phase,
                       j.business_key, j.submission_key, j.work_item_id
                  FROM rca_delivery_effects AS e
                  JOIN rca_delivery_jobs AS j
                    ON j.delivery_id = e.delivery_id
                 WHERE e.effect_key = ?
                """,
                (claim.effect_key,),
            ).fetchone()
            if row is None:
                raise DeliveryRecordConflictError(
                    "delivery_postwrite_observation_effect_missing"
                )
            expected_identity = {
                "delivery_id": claim.delivery_id,
                "effect_kind": claim.effect_kind,
                "target_key": claim.target_key,
                "payload_json": _canonical_json(claim.payload),
                "payload_sha256": claim.payload_sha256,
                "business_key": claim.business_key,
                "submission_key": claim.submission_key,
                "work_item_id": claim.work_item_id,
            }
            if any(
                str(row[field]) != str(expected)
                for field, expected in expected_identity.items()
            ) or str(row["write_phase"]) not in {"write_started", "settled"}:
                raise DeliveryRecordConflictError(
                    "delivery_postwrite_observation_effect_identity_invalid"
                )
            existing = conn.execute(
                """
                SELECT observation_id, payload_json, payload_sha256
                  FROM rca_delivery_observation_outbox
                 WHERE effect_key = ?
                """,
                (claim.effect_key,),
            ).fetchone()
            if existing is not None:
                if (
                    str(existing["observation_id"]) != observation_id
                    or str(existing["payload_json"]) != payload_json
                    or str(existing["payload_sha256"]) != payload_sha256
                ):
                    raise DeliveryRecordConflictError(
                        "delivery_observation_intent_conflict"
                    )
                conn.commit()
                return False
            conn.execute(
                """
                INSERT INTO rca_delivery_observation_outbox(
                    observation_id, effect_key, payload_json,
                    payload_sha256, status, created_at, appended_at
                ) VALUES(?, ?, ?, ?, 'pending', ?, NULL)
                """,
                (
                    observation_id,
                    claim.effect_key,
                    payload_json,
                    payload_sha256,
                    current,
                ),
            )
            conn.commit()
            return True
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def list_delivery_observations(
        self,
        *,
        status: str | None = None,
        limit: int | None = None,
    ) -> list[DeliveryObservationIntent]:
        if status not in {None, "pending", "appended"}:
            raise ValueError("delivery observation status is invalid")
        if limit is not None and (
            isinstance(limit, bool) or not isinstance(limit, int) or limit < 1
        ):
            raise ValueError("delivery observation limit must be positive")
        conn = self._connect()
        try:
            where = "" if status is None else " WHERE status = ?"
            bounded = "" if limit is None else " LIMIT ?"
            parameters: tuple[Any, ...] = (() if status is None else (status,))
            if limit is not None:
                parameters += (limit,)
            rows = conn.execute(
                "SELECT observation_id, effect_key, payload_json, payload_sha256, "
                "created_at, status FROM rca_delivery_observation_outbox"
                f"{where} ORDER BY created_at, observation_id{bounded}",
                parameters,
            ).fetchall()
            intents: list[DeliveryObservationIntent] = []
            for row in rows:
                payload_json = str(row["payload_json"])
                payload_sha256 = hashlib.sha256(
                    payload_json.encode("utf-8")
                ).hexdigest()
                if payload_sha256 != str(row["payload_sha256"]):
                    raise RuntimeError("delivery_observation_intent_hash_mismatch")
                try:
                    payload = json.loads(payload_json)
                except json.JSONDecodeError as exc:
                    raise RuntimeError(
                        "delivery_observation_intent_payload_invalid"
                    ) from exc
                if (
                    not isinstance(payload, dict)
                    or payload.get("observation_id") != row["observation_id"]
                ):
                    raise RuntimeError(
                        "delivery_observation_intent_identity_mismatch"
                    )
                intents.append(
                    DeliveryObservationIntent(
                        observation_id=str(row["observation_id"]),
                        effect_key=str(row["effect_key"]),
                        payload=payload,
                        payload_sha256=payload_sha256,
                        created_at=str(row["created_at"]),
                        status=str(row["status"]),
                    )
                )
            return intents
        finally:
            conn.close()

    def list_pending_delivery_observations(
        self,
        *,
        limit: int = 100,
    ) -> list[DeliveryObservationIntent]:
        return self.list_delivery_observations(status="pending", limit=limit)

    def list_appended_delivery_observations(self) -> list[DeliveryObservationIntent]:
        return self.list_delivery_observations(status="appended")

    def pending_delivery_observation_count(self) -> int:
        conn = self._connect()
        try:
            return int(
                conn.execute(
                    "SELECT COUNT(*) FROM rca_delivery_observation_outbox "
                    "WHERE status = 'pending'"
                ).fetchone()[0]
            )
        finally:
            conn.close()

    def mark_delivery_observation_appended(
        self,
        *,
        observation_id: str,
        payload_sha256: str,
        now: datetime | None = None,
    ) -> bool:
        if re.fullmatch(r"[0-9a-f]{64}", str(observation_id or "")) is None:
            raise ValueError("delivery observation id is invalid")
        if re.fullmatch(r"[0-9a-f]{64}", str(payload_sha256 or "")) is None:
            raise ValueError("delivery observation payload hash is invalid")
        current = _iso(now)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT status, payload_sha256 FROM "
                "rca_delivery_observation_outbox WHERE observation_id = ?",
                (observation_id,),
            ).fetchone()
            if row is None or str(row["payload_sha256"]) != payload_sha256:
                raise DeliveryRecordConflictError(
                    "delivery_observation_intent_receipt_mismatch"
                )
            if str(row["status"]) == "appended":
                conn.commit()
                return False
            updated = conn.execute(
                """
                UPDATE rca_delivery_observation_outbox
                   SET status = 'appended', appended_at = ?
                 WHERE observation_id = ? AND status = 'pending'
                   AND payload_sha256 = ?
                """,
                (current, observation_id, payload_sha256),
            )
            if updated.rowcount != 1:
                raise DeliveryRecordConflictError(
                    "delivery_observation_intent_receipt_conflict"
                )
            conn.commit()
            return True
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def requeue_delivery_observations(
        self,
        *,
        observations: Sequence[tuple[str, str]],
    ) -> int:
        """Undo only exact acknowledgements whose live receipt proof was lost."""
        normalized = tuple((str(item[0]), str(item[1])) for item in observations)
        if not normalized or len(set(normalized)) != len(normalized):
            raise ValueError("delivery observation requeue set is invalid")
        for observation_id, payload_sha256 in normalized:
            if re.fullmatch(r"[0-9a-f]{64}", observation_id) is None:
                raise ValueError("delivery observation id is invalid")
            if re.fullmatch(r"[0-9a-f]{64}", payload_sha256) is None:
                raise ValueError("delivery observation payload hash is invalid")

        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            for observation_id, payload_sha256 in normalized:
                row = conn.execute(
                    "SELECT status, payload_sha256 FROM "
                    "rca_delivery_observation_outbox WHERE observation_id = ?",
                    (observation_id,),
                ).fetchone()
                if (
                    row is None
                    or str(row["status"]) != "appended"
                    or str(row["payload_sha256"]) != payload_sha256
                ):
                    raise DeliveryRecordConflictError(
                        "delivery_observation_requeue_identity_mismatch"
                    )
                updated = conn.execute(
                    """
                    UPDATE rca_delivery_observation_outbox
                       SET status = 'pending', appended_at = NULL
                     WHERE observation_id = ? AND status = 'appended'
                       AND payload_sha256 = ?
                    """,
                    (observation_id, payload_sha256),
                )
                if updated.rowcount != 1:
                    raise DeliveryRecordConflictError(
                        "delivery_observation_requeue_conflict"
                    )
            conn.commit()
            return len(normalized)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def reschedule_effect(
        self,
        *,
        claim: DeliveryEffectClaim,
        error_code: str,
        error_detail: str,
        delay_seconds: int,
        uncertain: bool,
        max_age_seconds: int = 86_400,
        now: datetime | None = None,
    ) -> DeliveryEffectMutation:
        if delay_seconds < 0 or max_age_seconds < 1:
            raise ValueError("retry delay and max age are invalid")
        current_dt = _utc_datetime(now)
        current = _iso(current_dt)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            mutation = self._reschedule_effect_in_transaction(
                conn,
                claim=claim,
                error_code=error_code,
                error_detail=error_detail,
                delay_seconds=delay_seconds,
                uncertain=uncertain,
                max_age_seconds=max_age_seconds,
                current_dt=current_dt,
                current=current,
            )
            conn.commit()
            return mutation
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def reschedule_effect_and_open_circuit(
        self,
        *,
        claim: DeliveryEffectClaim,
        error_code: str,
        error_detail: str,
        delay_seconds: int,
        uncertain: bool,
        max_age_seconds: int = 86_400,
        now: datetime | None = None,
    ) -> DeliveryEffectMutation:
        """Reschedule a fenced effect and open its dispatcher circuit atomically."""
        if delay_seconds < 0 or max_age_seconds < 1:
            raise ValueError("retry delay and max age are invalid")
        current_dt = _utc_datetime(now)
        current = _iso(current_dt)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            mutation = self._reschedule_effect_in_transaction(
                conn,
                claim=claim,
                error_code=error_code,
                error_detail=error_detail,
                delay_seconds=delay_seconds,
                uncertain=uncertain,
                max_age_seconds=max_age_seconds,
                current_dt=current_dt,
                current=current,
            )
            self._open_delivery_dispatcher_circuit_in_transaction(
                conn,
                circuit_name=claim.effect_kind,
                reason_code=error_code,
                reason_detail=error_detail,
                current=current,
            )
            conn.commit()
            return mutation
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _reschedule_effect_in_transaction(
        self,
        conn: sqlite3.Connection,
        *,
        claim: DeliveryEffectClaim,
        error_code: str,
        error_detail: str,
        delay_seconds: int,
        uncertain: bool,
        max_age_seconds: int,
        current_dt: datetime,
        current: str,
    ) -> DeliveryEffectMutation:
        row = self._current_effect_claim(
            conn,
            claim=claim,
            current=current,
            allow_inactive_card_patch_after_write=(
                claim.effect_kind == DELIVERY_CARD_PATCH_EFFECT_KIND
            ),
        )
        age = (current_dt - _parse_iso(str(row["created_at"]))).total_seconds()
        if age >= max_age_seconds:
            effect_status = "quarantined"
            event_outcome = "quarantined"
            next_attempt_at = None
            write_phase = "settled"
            final_code = "delivery_effect_age_exceeded"
            final_detail = "delivery effect exceeded the 24 hour retry horizon"
        else:
            effect_status = "uncertain" if uncertain else "retry_wait"
            event_outcome = "unknown" if uncertain else "nack"
            next_attempt_at = _iso(current_dt + timedelta(seconds=delay_seconds))
            write_phase = "write_started" if uncertain else "prewrite"
            final_code = error_code
            final_detail = error_detail
        self._append_attempt_event(
            conn,
            effect_key=claim.effect_key,
            attempt_no=claim.attempt,
            fence=claim.fence,
            request_id=claim.request_id,
            outcome=event_outcome,
            current=current,
            error_code=final_code,
            detail=final_detail,
        )
        conn.execute(
            """
            UPDATE rca_delivery_effects
               SET status = ?, next_attempt_at = ?,
                   write_phase = ?,
                   write_started_at = CASE WHEN ? = 'prewrite' THEN NULL
                                           ELSE write_started_at END,
                   reconciliation_miss_count = CASE WHEN ? = 'prewrite' THEN 0
                                                    ELSE reconciliation_miss_count END,
                   quarantined_at = CASE WHEN ? = 'quarantined' THEN ? ELSE NULL END,
                   last_error_code = ?, last_error_detail = ?,
                   lease_token = NULL, lease_owner = NULL,
                   lease_expires_at = NULL, updated_at = ?
             WHERE effect_key = ? AND lease_token = ? AND fence = ?
            """,
            (
                effect_status,
                next_attempt_at,
                write_phase,
                write_phase,
                write_phase,
                effect_status,
                current,
                str(final_code or "")[:120],
                str(final_detail or "")[:1000],
                current,
                claim.effect_key,
                claim.lease_token,
                claim.fence,
            ),
        )
        job_status = self._aggregate_job_status(conn, str(row["delivery_id"]), current)
        if effect_status == "quarantined":
            self._record_permanent_failure_in_transaction(
                conn,
                circuit_name=claim.effect_kind,
                subject_key=claim.effect_key,
                failure_state="quarantined",
                error_code=final_code,
                error_detail=final_detail,
                current=current,
            )
        return DeliveryEffectMutation(
            claim.effect_key,
            claim.delivery_id,
            effect_status,
            job_status,
            next_attempt_at,
        )

    def quarantine_effect(
        self,
        *,
        claim: DeliveryEffectClaim,
        error_code: str,
        error_detail: str,
        now: datetime | None = None,
    ) -> DeliveryEffectMutation:
        current = _iso(now)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            mutation = self._quarantine_effect_in_transaction(
                conn,
                claim=claim,
                error_code=error_code,
                error_detail=error_detail,
                current=current,
            )
            conn.commit()
            return mutation
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def quarantine_effect_and_open_circuit(
        self,
        *,
        claim: DeliveryEffectClaim,
        error_code: str,
        error_detail: str,
        now: datetime | None = None,
    ) -> DeliveryEffectMutation:
        """Atomically quarantine an effect and stop writes to its boundary."""
        current = _iso(now)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            mutation = self._quarantine_effect_in_transaction(
                conn,
                claim=claim,
                error_code=error_code,
                error_detail=error_detail,
                current=current,
            )
            self._open_delivery_dispatcher_circuit_in_transaction(
                conn,
                circuit_name=claim.effect_kind,
                reason_code=error_code,
                reason_detail=error_detail,
                current=current,
            )
            conn.commit()
            return mutation
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _quarantine_effect_in_transaction(
        self,
        conn: sqlite3.Connection,
        *,
        claim: DeliveryEffectClaim,
        error_code: str,
        error_detail: str,
        current: str,
    ) -> DeliveryEffectMutation:
        row = self._current_effect_claim(
            conn,
            claim=claim,
            current=current,
            allow_invalid_adjudication=True,
        )
        self._append_attempt_event(
            conn,
            effect_key=claim.effect_key,
            attempt_no=claim.attempt,
            fence=claim.fence,
            request_id=claim.request_id,
            outcome="quarantined",
            current=current,
            error_code=error_code,
            detail=error_detail,
        )
        conn.execute(
            """
            UPDATE rca_delivery_effects
               SET status = 'quarantined', quarantined_at = ?,
                   write_phase = 'settled',
                   next_attempt_at = NULL, last_error_code = ?,
                   last_error_detail = ?, lease_token = NULL,
                   lease_owner = NULL, lease_expires_at = NULL, updated_at = ?
             WHERE effect_key = ? AND lease_token = ? AND fence = ?
            """,
            (
                current,
                str(error_code or "")[:120],
                str(error_detail or "")[:1000],
                current,
                claim.effect_key,
                claim.lease_token,
                claim.fence,
            ),
        )
        job_status = self._aggregate_job_status(conn, str(row["delivery_id"]), current)
        if error_code in _NON_PIPELINE_QUARANTINE_CODES:
            self._reset_permanent_failure_streak_in_transaction(
                conn,
                circuit_name=claim.effect_kind,
                require_closed_circuit=True,
            )
        else:
            self._record_permanent_failure_in_transaction(
                conn,
                circuit_name=claim.effect_kind,
                subject_key=claim.effect_key,
                failure_state="quarantined",
                error_code=error_code,
                error_detail=error_detail,
                current=current,
            )
        return DeliveryEffectMutation(
            claim.effect_key, claim.delivery_id, "quarantined", job_status
        )

    @staticmethod
    def _delivery_circuit_reset_state_in_transaction(
        conn: sqlite3.Connection,
        *,
        effect_kind: str,
    ) -> dict[str, Any] | None:
        row = conn.execute(
            """
            SELECT state, reason_code, reason_detail, opened_at, updated_at
              FROM rca_delivery_dispatcher_circuit
             WHERE circuit_name = ?
            """,
            (effect_kind,),
        ).fetchone()
        if row is None:
            return None
        streak_key = (
            _PERMANENT_FAILURE_STREAK_META_KEY
            if effect_kind == DELIVERY_EFFECT_KIND
            else f"{_PERMANENT_FAILURE_STREAK_META_KEY}:{effect_kind}"
        )
        last_key = (
            _PERMANENT_FAILURE_LAST_META_KEY
            if effect_kind == DELIVERY_EFFECT_KIND
            else f"{_PERMANENT_FAILURE_LAST_META_KEY}:{effect_kind}"
        )
        streak_row = conn.execute(
            "SELECT value FROM rca_delivery_meta WHERE key = ?",
            (streak_key,),
        ).fetchone()
        last_row = conn.execute(
            "SELECT value FROM rca_delivery_meta WHERE key = ?",
            (last_key,),
        ).fetchone()
        if streak_row is None:
            streak = 0
        else:
            try:
                streak = int(str(streak_row["value"]))
            except (TypeError, ValueError):
                streak = PERMANENT_FAILURE_CIRCUIT_THRESHOLD
            if streak < 0:
                streak = PERMANENT_FAILURE_CIRCUIT_THRESHOLD
        return {
            "circuit": dict(row),
            "permanent_failure": {
                "threshold": PERMANENT_FAILURE_CIRCUIT_THRESHOLD,
                "consecutive_failures": streak,
                "last_failure": (
                    _json_object(last_row["value"]) if last_row is not None else {}
                ),
                "last_failure_present": last_row is not None,
            },
        }

    def delivery_dispatcher_circuit_reset_state(
        self,
        effect_kind: str = DELIVERY_EFFECT_KIND,
    ) -> dict[str, Any] | None:
        """Return the exact circuit and streak state used by a reset plan."""
        if effect_kind not in DELIVERY_EFFECT_KINDS:
            raise ValueError("unsupported delivery circuit")
        conn = self._connect_read_only()
        try:
            return self._delivery_circuit_reset_state_in_transaction(
                conn,
                effect_kind=effect_kind,
            )
        finally:
            conn.close()

    def delivery_dispatcher_circuit(
        self, effect_kind: str = DELIVERY_EFFECT_KIND
    ) -> DeliveryDispatcherCircuit:
        if effect_kind not in DELIVERY_EFFECT_KINDS:
            raise ValueError("unsupported delivery circuit")
        conn = self._connect()
        try:
            row = conn.execute(
                """
                SELECT state, reason_code, reason_detail, opened_at, updated_at
                  FROM rca_delivery_dispatcher_circuit
                 WHERE circuit_name = ?
                """,
                (effect_kind,),
            ).fetchone()
            if row is None:
                return DeliveryDispatcherCircuit(
                    state="open", reason_code="delivery_circuit_state_missing"
                )
            return DeliveryDispatcherCircuit(**dict(row))
        finally:
            conn.close()

    def delivery_dispatcher_circuits(self) -> dict[str, DeliveryDispatcherCircuit]:
        return {
            effect_kind: self.delivery_dispatcher_circuit(effect_kind)
            for effect_kind in REQUIRED_DELIVERY_EFFECT_KINDS
        }

    def open_delivery_dispatcher_circuit(
        self,
        *,
        effect_kind: str = DELIVERY_EFFECT_KIND,
        reason_code: str,
        reason_detail: str = "",
        now: datetime | None = None,
    ) -> DeliveryDispatcherCircuit:
        if effect_kind not in DELIVERY_EFFECT_KINDS:
            raise ValueError("unsupported delivery circuit")
        current = _iso(now)
        conn = self._connect()
        try:
            self._open_delivery_dispatcher_circuit_in_transaction(
                conn,
                circuit_name=effect_kind,
                reason_code=reason_code,
                reason_detail=reason_detail,
                current=current,
            )
        finally:
            conn.close()
        return self.delivery_dispatcher_circuit(effect_kind)

    @staticmethod
    def _permanent_failure_streak_in_transaction(
        conn: sqlite3.Connection,
        circuit_name: str = DELIVERY_EFFECT_KIND,
    ) -> int:
        streak_key = (
            _PERMANENT_FAILURE_STREAK_META_KEY
            if circuit_name == DELIVERY_EFFECT_KIND
            else f"{_PERMANENT_FAILURE_STREAK_META_KEY}:{circuit_name}"
        )
        row = conn.execute(
            "SELECT value FROM rca_delivery_meta WHERE key = ?",
            (streak_key,),
        ).fetchone()
        if row is None:
            return 0
        try:
            streak = int(str(row["value"]))
        except (TypeError, ValueError):
            return PERMANENT_FAILURE_CIRCUIT_THRESHOLD
        if streak < 0:
            return PERMANENT_FAILURE_CIRCUIT_THRESHOLD
        return streak

    def _record_permanent_failure_in_transaction(
        self,
        conn: sqlite3.Connection,
        *,
        circuit_name: str = DELIVERY_EFFECT_KIND,
        subject_key: str,
        failure_state: str,
        error_code: str,
        error_detail: str,
        current: str,
    ) -> int:
        streak_key = (
            _PERMANENT_FAILURE_STREAK_META_KEY
            if circuit_name == DELIVERY_EFFECT_KIND
            else f"{_PERMANENT_FAILURE_STREAK_META_KEY}:{circuit_name}"
        )
        last_key = (
            _PERMANENT_FAILURE_LAST_META_KEY
            if circuit_name == DELIVERY_EFFECT_KIND
            else f"{_PERMANENT_FAILURE_LAST_META_KEY}:{circuit_name}"
        )
        streak = self._permanent_failure_streak_in_transaction(conn, circuit_name) + 1
        last_failure = {
            "subject_key": str(subject_key or "")[:256],
            "state": str(failure_state or "")[:120],
            "error_code": str(error_code or "")[:120],
            "error_detail": str(error_detail or "")[:1000],
            "observed_at": current,
        }
        conn.execute(
            """
            INSERT INTO rca_delivery_meta(key, value) VALUES(?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (streak_key, str(streak)),
        )
        conn.execute(
            """
            INSERT INTO rca_delivery_meta(key, value) VALUES(?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (last_key, _canonical_json(last_failure)),
        )
        if streak >= PERMANENT_FAILURE_CIRCUIT_THRESHOLD:
            self._open_delivery_dispatcher_circuit_in_transaction(
                conn,
                circuit_name=circuit_name,
                reason_code="delivery_permanent_failure_streak_exceeded",
                reason_detail=(
                    f"{streak} consecutive permanent RCA pipeline failures; "
                    f"last={last_failure['state']}:{last_failure['error_code']}; "
                    f"subject={last_failure['subject_key']}"
                ),
                current=current,
            )
        return streak

    @staticmethod
    def _reset_permanent_failure_streak_in_transaction(
        conn: sqlite3.Connection,
        *,
        circuit_name: str = DELIVERY_EFFECT_KIND,
        require_closed_circuit: bool,
    ) -> bool:
        streak_key = (
            _PERMANENT_FAILURE_STREAK_META_KEY
            if circuit_name == DELIVERY_EFFECT_KIND
            else f"{_PERMANENT_FAILURE_STREAK_META_KEY}:{circuit_name}"
        )
        last_key = (
            _PERMANENT_FAILURE_LAST_META_KEY
            if circuit_name == DELIVERY_EFFECT_KIND
            else f"{_PERMANENT_FAILURE_LAST_META_KEY}:{circuit_name}"
        )
        if require_closed_circuit:
            circuit = conn.execute(
                "SELECT state FROM rca_delivery_dispatcher_circuit "
                "WHERE circuit_name = ?",
                (circuit_name,),
            ).fetchone()
            if circuit is None or str(circuit["state"]) != "closed":
                return False
        conn.execute(
            """
            INSERT INTO rca_delivery_meta(key, value) VALUES(?, '0')
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (streak_key,),
        )
        conn.execute(
            "DELETE FROM rca_delivery_meta WHERE key = ?",
            (last_key,),
        )
        return True

    def permanent_failure_circuit_state(
        self, effect_kind: str = DELIVERY_EFFECT_KIND
    ) -> dict[str, Any]:
        if effect_kind not in DELIVERY_EFFECT_KINDS:
            raise ValueError("unsupported delivery circuit")
        last_key = (
            _PERMANENT_FAILURE_LAST_META_KEY
            if effect_kind == DELIVERY_EFFECT_KIND
            else f"{_PERMANENT_FAILURE_LAST_META_KEY}:{effect_kind}"
        )
        conn = self._connect()
        try:
            streak = self._permanent_failure_streak_in_transaction(conn, effect_kind)
            row = conn.execute(
                "SELECT value FROM rca_delivery_meta WHERE key = ?",
                (last_key,),
            ).fetchone()
            last_failure = _json_object(row["value"]) if row is not None else {}
            return {
                "threshold": PERMANENT_FAILURE_CIRCUIT_THRESHOLD,
                "consecutive_failures": streak,
                "last_failure": last_failure,
            }
        finally:
            conn.close()

    @staticmethod
    def _open_delivery_dispatcher_circuit_in_transaction(
        conn: sqlite3.Connection,
        *,
        circuit_name: str = DELIVERY_EFFECT_KIND,
        reason_code: str,
        reason_detail: str,
        current: str,
    ) -> None:
        conn.execute(
            """
            INSERT INTO rca_delivery_dispatcher_circuit(
                circuit_name, state, reason_code, reason_detail,
                opened_at, updated_at
            ) VALUES(?, 'open', ?, ?, ?, ?)
            ON CONFLICT(circuit_name) DO UPDATE SET
                state = 'open', reason_code = excluded.reason_code,
                reason_detail = excluded.reason_detail,
                opened_at = COALESCE(
                    rca_delivery_dispatcher_circuit.opened_at,
                    excluded.opened_at
                ),
                updated_at = excluded.updated_at
            """,
            (
                circuit_name,
                str(reason_code or "delivery_dispatcher_system_error")[:120],
                str(reason_detail or "")[:1000],
                current,
                current,
            ),
        )

    def close_delivery_dispatcher_circuit(
        self,
        *,
        effect_kind: str = DELIVERY_EFFECT_KIND,
        now: datetime | None = None,
    ) -> DeliveryDispatcherCircuit:
        if effect_kind not in DELIVERY_EFFECT_KINDS:
            raise ValueError("unsupported delivery circuit")
        current = _iso(now)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                INSERT INTO rca_delivery_dispatcher_circuit(
                    circuit_name, state, updated_at
                ) VALUES(?, 'closed', ?)
                ON CONFLICT(circuit_name) DO UPDATE SET
                    state = 'closed', reason_code = '', reason_detail = '',
                    opened_at = NULL, updated_at = excluded.updated_at
                """,
                (effect_kind, current),
            )
            self._reset_permanent_failure_streak_in_transaction(
                conn,
                circuit_name=effect_kind,
                require_closed_circuit=False,
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        return self.delivery_dispatcher_circuit(effect_kind)

    def close_delivery_dispatcher_circuit_with_audit(
        self,
        *,
        effect_kind: str,
        audit: Mapping[str, Any],
        now: datetime | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """Atomically persist an operator audit and close one exact circuit."""
        if effect_kind not in DELIVERY_EFFECT_KINDS:
            raise ValueError("unsupported delivery circuit")
        payload = _validate_delivery_circuit_reset_audit(audit)
        if payload["effect_kind"] != effect_kind:
            raise ValueError("delivery_circuit_reset_effect_kind_mismatch")
        current = _iso(now)
        if payload["recorded_at"] != current:
            raise ValueError("delivery_circuit_reset_timestamp_mismatch")
        try:
            observed_db = self.db_path.expanduser().absolute().lstat()
        except OSError as exc:
            raise RuntimeError("delivery_circuit_reset_control_db_missing") from exc
        identity = payload["control_db_identity"]
        observed_identity = {
            "path": str(self.db_path.expanduser().absolute()),
            "device": int(observed_db.st_dev),
            "inode": int(observed_db.st_ino),
            "size": int(observed_db.st_size),
            "mtime_ns": int(observed_db.st_mtime_ns),
        }
        if identity != observed_identity:
            raise RuntimeError("delivery_circuit_reset_control_db_changed")
        expected_before = {
            "circuit": dict(payload["before"]),
            "permanent_failure": dict(payload["permanent_failure_before"]),
        }
        expected_after = {
            "circuit": dict(payload["after"]),
            "permanent_failure": dict(payload["permanent_failure_after"]),
        }
        expected_rows = dict(payload["effect_delta"]["database_rows"])
        serialized = _canonical_json(payload)
        meta_key = f"{DELIVERY_CIRCUIT_RESET_META_PREFIX}{payload['reset_id']}"
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            if conn.execute(
                "SELECT 1 FROM sqlite_master "
                "WHERE type = 'table' AND name = 'control_meta'"
            ).fetchone() is None:
                raise RuntimeError("delivery_circuit_reset_audit_store_missing")
            observed_before = self._delivery_circuit_reset_state_in_transaction(
                conn,
                effect_kind=effect_kind,
            )
            if observed_before is None:
                raise RuntimeError("delivery_circuit_reset_state_missing")
            if observed_before != expected_before:
                raise RuntimeError("delivery_circuit_reset_state_changed")
            if observed_before["circuit"]["state"] != "open":
                raise RuntimeError("delivery_circuit_reset_requires_open_circuit")
            changes_before = conn.total_changes
            try:
                conn.execute(
                    "INSERT INTO control_meta(key, value) VALUES(?, ?)",
                    (meta_key, serialized),
                )
            except sqlite3.IntegrityError as exc:
                raise RuntimeError(
                    "delivery_circuit_reset_audit_already_exists"
                ) from exc
            updated = conn.execute(
                """
                UPDATE rca_delivery_dispatcher_circuit
                   SET state = 'closed', reason_code = '', reason_detail = '',
                       opened_at = NULL, updated_at = ?
                 WHERE circuit_name = ? AND state = 'open' AND updated_at = ?
                """,
                (
                    current,
                    effect_kind,
                    observed_before["circuit"]["updated_at"],
                ),
            )
            if updated.rowcount != 1:
                raise RuntimeError("delivery_circuit_reset_state_changed")
            self._reset_permanent_failure_streak_in_transaction(
                conn,
                circuit_name=effect_kind,
                require_closed_circuit=True,
            )
            observed_after = self._delivery_circuit_reset_state_in_transaction(
                conn,
                effect_kind=effect_kind,
            )
            if observed_after != expected_after:
                raise RuntimeError("delivery_circuit_reset_post_state_changed")
            actual_rows = conn.total_changes - changes_before
            if actual_rows != expected_rows["total"]:
                raise RuntimeError("delivery_circuit_reset_row_delta_changed")
            conn.commit()
        except Exception:
            if conn.in_transaction:
                conn.rollback()
            raise
        finally:
            conn.close()
        return observed_before, observed_after

    def delivery_dispatcher_circuit_reset_audit(
        self,
        reset_id: str,
        *,
        effect_kind: str | None = None,
    ) -> dict[str, Any] | None:
        """Read and verify a durable delivery-circuit reset audit."""
        normalized = str(reset_id or "").strip()
        if re.fullmatch(r"[0-9a-f]{64}", normalized) is None:
            raise ValueError("delivery_circuit_reset_id_invalid")
        if effect_kind is not None and effect_kind not in DELIVERY_EFFECT_KINDS:
            raise ValueError("unsupported delivery circuit")
        conn = self._connect_read_only()
        try:
            row = conn.execute(
                "SELECT value FROM control_meta WHERE key = ?",
                (f"{DELIVERY_CIRCUIT_RESET_META_PREFIX}{normalized}",),
            ).fetchone()
            if row is None:
                return None
            raw = str(row["value"])
            value = json.loads(
                raw,
                parse_constant=_reject_delivery_circuit_reset_json_constant,
            )
            value = _validate_delivery_circuit_reset_audit(value)
            if (
                value["reset_id"] != normalized
                or _canonical_json(value) != raw
            ):
                raise RuntimeError("delivery_circuit_reset_audit_tampered")
            if effect_kind is not None and value["effect_kind"] != effect_kind:
                raise RuntimeError("delivery_circuit_reset_effect_kind_mismatch")
            try:
                observed_db = self.db_path.expanduser().absolute().lstat()
            except OSError as exc:
                raise RuntimeError(
                    "delivery_circuit_reset_control_db_missing"
                ) from exc
            identity = value["control_db_identity"]
            if (
                identity["path"] != str(self.db_path.expanduser().absolute())
                or int(identity["device"]) != int(observed_db.st_dev)
                or int(identity["inode"]) != int(observed_db.st_ino)
            ):
                raise RuntimeError("delivery_circuit_reset_control_db_mismatch")
            return value
        except (json.JSONDecodeError, ValueError) as exc:
            raise RuntimeError("delivery_circuit_reset_audit_invalid") from exc
        finally:
            conn.close()

    @staticmethod
    def _delivery_outcome_slo_in_transaction(
        conn: sqlite3.Connection, *, current_dt: datetime
    ) -> dict[str, Any]:
        observed_at = _iso(current_dt)
        cutoff = _iso(
            current_dt - timedelta(seconds=DELIVERY_OUTCOME_CONSECUTIVE_WINDOW_SECONDS)
        )
        rows = conn.execute(
            """
            SELECT status, updated_at
              FROM rca_delivery_jobs
             WHERE updated_at >= ?
             ORDER BY updated_at DESC, rowid DESC
            """,
            (cutoff,),
        ).fetchall()
        recent: list[tuple[bool, datetime]] = []
        contract_valid = True
        success_statuses = set(DELIVERY_OUTCOME_SLO_SUCCESS_STATUSES)
        failure_statuses = set(DELIVERY_OUTCOME_SLO_FAILURE_STATUSES)
        allowed_statuses = {"ready", *success_statuses, *failure_statuses}
        for row in rows:
            status = str(row["status"] or "")
            try:
                updated_at = _parse_iso(str(row["updated_at"] or ""))
            except ValueError:
                contract_valid = False
                continue
            if status not in allowed_statuses:
                contract_valid = False
                continue
            if status == "ready":
                continue
            recent.append((status in failure_statuses, updated_at))

        windows: dict[str, dict[str, Any]] = {}
        for (
            name,
            window_seconds,
            min_samples,
            max_failure_rate,
        ) in DELIVERY_OUTCOME_SLO_WINDOWS:
            window_cutoff = current_dt - timedelta(seconds=window_seconds)
            delivery_failures = [
                failed for failed, updated_at in recent if updated_at >= window_cutoff
            ]
            failures = sum(delivery_failures)
            failure_rate = (
                failures / len(delivery_failures) if delivery_failures else 0.0
            )
            windows[name] = {
                "window_seconds": window_seconds,
                "min_samples": min_samples,
                "max_failure_rate": max_failure_rate,
                "sample_count": len(delivery_failures),
                "failure_count": failures,
                "failure_rate": failure_rate,
                "breached": (
                    len(delivery_failures) >= min_samples
                    and failure_rate > max_failure_rate
                ),
            }

        consecutive_failures = 0
        for failed, _updated_at in recent:
            if not failed:
                break
            consecutive_failures += 1
        consecutive_breached = (
            consecutive_failures >= DELIVERY_OUTCOME_CONSECUTIVE_FAILURE_THRESHOLD
        )
        outcome_slo = {
            "schema_version": DELIVERY_OUTCOME_SLO_SCHEMA_VERSION,
            "observed_at": observed_at,
            "success_delivery_statuses": sorted(DELIVERY_OUTCOME_SLO_SUCCESS_STATUSES),
            "failure_delivery_statuses": sorted(DELIVERY_OUTCOME_SLO_FAILURE_STATUSES),
            "windows": windows,
            "consecutive_failure_window_seconds": (
                DELIVERY_OUTCOME_CONSECUTIVE_WINDOW_SECONDS
            ),
            "consecutive_failure_threshold": (
                DELIVERY_OUTCOME_CONSECUTIVE_FAILURE_THRESHOLD
            ),
            "consecutive_failure_count": consecutive_failures,
            "consecutive_failure_breached": consecutive_breached,
            "contract_valid": contract_valid,
            "healthy": (
                contract_valid
                and not consecutive_breached
                and not any(window["breached"] for window in windows.values())
            ),
        }
        validate_delivery_outcome_slo(outcome_slo, expected_observed_at=observed_at)
        return outcome_slo

    @classmethod
    def read_existing_backpressure_snapshot(
        cls,
        db_path: str | Path,
        *,
        now: datetime | None = None,
        busy_timeout_ms: int = 5000,
        activation_required: bool = False,
        expected_control_schema_version: str = CONTROL_STORE_SCHEMA_VERSION,
    ) -> DeliveryBackpressureSnapshot:
        """Read the delivery backlog and circuit from one live read transaction."""
        cls._validate_activation_required(activation_required)
        if expected_control_schema_version not in {
            CONTROL_STORE_SCHEMA_VERSION,
            CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION,
        }:
            raise ValueError("delivery_backpressure_control_schema_invalid")
        path = Path(db_path).expanduser()
        if not path.is_file():
            raise RuntimeError("delivery_backpressure_store_unavailable:file_missing")
        current_dt = _utc_datetime(now)
        current = _iso(current_dt)
        if not Path(f"{path}-wal").is_file():
            try:
                preflight_schema_version = (
                    RcaControlStore._preflight_schema_version_at(
                        path,
                        allow_successor_read_only=True,
                        immutable=True,
                    )
                )
            except RuntimeError as exc:
                raise RuntimeError(
                    "delivery_backpressure_contract_invalid:control_schema_version"
                ) from exc
            if preflight_schema_version != expected_control_schema_version:
                raise RuntimeError(
                    "delivery_backpressure_contract_invalid:control_schema_version"
                )
        uri = f"{path.resolve().as_uri()}?mode=ro"
        try:
            conn = sqlite3.connect(
                uri,
                uri=True,
                timeout=busy_timeout_ms / 1000,
                isolation_level=None,
            )
            conn.row_factory = sqlite3.Row
            conn.execute(f"PRAGMA busy_timeout={busy_timeout_ms}")
            conn.execute("PRAGMA query_only=ON")
            conn.execute("BEGIN")
            try:
                control_schema_version = (
                    RcaControlStore._activation_schema_version_tx(conn)
                )
            except (RuntimeError, sqlite3.Error) as exc:
                raise RuntimeError(
                    "delivery_backpressure_contract_invalid:control_schema_version"
                ) from exc
            if control_schema_version != expected_control_schema_version:
                raise RuntimeError(
                    "delivery_backpressure_contract_invalid:control_schema_version"
                )
            if control_schema_version == CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION:
                RcaControlStore._validate_v15_activation_schema_tx(conn)
            marker = conn.execute(
                "SELECT value FROM rca_delivery_meta WHERE key = 'schema_version'"
            ).fetchone()
            if (
                marker is None
                or str(marker["value"]) not in SUPPORTED_DELIVERY_STORE_SCHEMA_VERSIONS
            ):
                raise RuntimeError(
                    "delivery_backpressure_contract_invalid:schema_version"
                )
            activation_enforced = cls._activation_enforced_tx(
                conn,
                activation_required=activation_required,
            )
            if activation_enforced:
                cls._require_activation_schema(conn)
            all_effect_rows = conn.execute(
                "SELECT status, COUNT(*) AS count "
                "FROM rca_delivery_effects GROUP BY status"
            ).fetchall()
            all_effect_counts = {
                str(row["status"]): int(row["count"]) for row in all_effect_rows
            }
            unknown_states = set(all_effect_counts) - DELIVERY_EFFECT_STATES
            if unknown_states or any(
                value < 0 for value in all_effect_counts.values()
            ):
                raise RuntimeError(
                    "delivery_backpressure_contract_invalid:effect_status"
                )
            effect_joins = (
                """
                  JOIN rca_delivery_jobs AS j ON j.delivery_id = e.delivery_id
             LEFT JOIN rca_execution_watch AS w
                    ON w.submission_key = j.submission_key
             LEFT JOIN rca_outbox AS o
                    ON o.outbox_id = w.submission_outbox_id
             LEFT JOIN business_triggers AS t
                    ON t.business_key = o.business_key
                   AND t.generation = o.generation
                """
                if activation_enforced
                else ""
            )
            effect_filter = (
                "WHERE ("
                f"({_ACTIVATION_EXECUTION_ELIGIBLE_SQL}) OR "
                f"({_ADJUDICATION_ACTIVATION_ELIGIBLE_SQL}) OR "
                f"({_CARD_PATCH_ACTIVATION_ELIGIBLE_SQL})"
                ")"
                if activation_enforced
                else ""
            )
            if activation_enforced:
                rows = conn.execute(
                    f"SELECT e.status, COUNT(*) AS count "
                    f"FROM rca_delivery_effects AS e {effect_joins} "
                    f"{effect_filter} GROUP BY e.status"
                ).fetchall()
                counts = {
                    str(row["status"]): int(row["count"]) for row in rows
                }
            else:
                counts = all_effect_counts
            all_watch_rows = conn.execute(
                "SELECT state, COUNT(*) AS count "
                "FROM rca_execution_watch GROUP BY state"
            ).fetchall()
            all_watch_counts = {
                str(row["state"]): int(row["count"]) for row in all_watch_rows
            }
            if set(all_watch_counts) - DELIVERY_WATCH_STATES or any(
                value < 0 for value in all_watch_counts.values()
            ):
                raise RuntimeError("delivery_backpressure_contract_invalid:watch_state")
            watch_joins = (
                """
                  JOIN rca_outbox AS o ON o.outbox_id = w.submission_outbox_id
                  JOIN business_triggers AS t
                    ON t.business_key = o.business_key
                   AND t.generation = o.generation
                """
                if activation_enforced
                else ""
            )
            watch_filter = (
                f"WHERE {_ACTIVATION_EXECUTION_ELIGIBLE_SQL}"
                if activation_enforced
                else ""
            )
            if activation_enforced:
                watch_rows = conn.execute(
                    f"SELECT w.state, COUNT(*) AS count "
                    f"FROM rca_execution_watch AS w {watch_joins} "
                    f"{watch_filter} GROUP BY w.state"
                ).fetchall()
                watch_counts = {
                    str(row["state"]): int(row["count"]) for row in watch_rows
                }
            else:
                watch_counts = all_watch_counts
            untracked_activation_join = (
                """
                  JOIN business_triggers AS t
                    ON t.business_key = o.business_key
                   AND t.generation = o.generation
                """
                if activation_enforced
                else ""
            )
            untracked_activation_filter = (
                f"AND {_ACTIVATION_EXECUTION_ELIGIBLE_SQL}"
                if activation_enforced
                else ""
            )
            untracked_completed = int(
                conn.execute(
                    f"""
                    SELECT COUNT(*)
                      FROM rca_outbox AS o
                      {untracked_activation_join}
                      LEFT JOIN rca_execution_watch AS w
                        ON w.submission_outbox_id = o.outbox_id
                     WHERE o.status IN ('completed', 'quarantined')
                       AND w.submission_outbox_id IS NULL
                       {untracked_activation_filter}
                    """
                ).fetchone()[0]
            )
            if untracked_completed < 0:
                raise RuntimeError(
                    "delivery_backpressure_contract_invalid:untracked_completed"
                )
            circuit_rows = conn.execute(
                """
                SELECT circuit_name, state, reason_code, reason_detail,
                       opened_at, updated_at
                  FROM rca_delivery_dispatcher_circuit
                 WHERE circuit_name IN (
                       'feishu_issue_comment', 'feishu_thread_reply',
                       'feishu_card_patch'
                 )
                """
            ).fetchall()
            rows_by_name = {str(item["circuit_name"]): item for item in circuit_rows}
            circuits: dict[str, DeliveryDispatcherCircuit] = {}
            for circuit_name in REQUIRED_DELIVERY_EFFECT_KINDS:
                circuit_row = rows_by_name.get(circuit_name)
                if circuit_row is None:
                    if (
                        circuit_name == DELIVERY_CARD_PATCH_EFFECT_KIND
                        and str(marker["value"]) != DELIVERY_STORE_SCHEMA_VERSION
                    ):
                        # Older compatible snapshots predate B10. Keep this API
                        # read-only and synthesize only their additive circuit;
                        # a missing row in the current schema still fails closed.
                        circuits[circuit_name] = DeliveryDispatcherCircuit(
                            state="closed",
                            updated_at=current,
                        )
                        continue
                    circuits[circuit_name] = DeliveryDispatcherCircuit(
                        state="open",
                        reason_code="delivery_circuit_state_missing",
                        reason_detail=circuit_name,
                        updated_at=current,
                    )
                    continue
                circuit_body = dict(circuit_row)
                circuit_body.pop("circuit_name", None)
                item = DeliveryDispatcherCircuit(**circuit_body)
                if item.state not in {"closed", "open"}:
                    raise RuntimeError(
                        "delivery_backpressure_contract_invalid:circuit_state"
                    )
                circuits[circuit_name] = item
            open_names = sorted(name for name, item in circuits.items() if item.is_open)
            circuit = (
                circuits[open_names[0]]
                if open_names
                else circuits[DELIVERY_EFFECT_KIND]
            )
            outcome_slo = cls._delivery_outcome_slo_in_transaction(
                conn, current_dt=current_dt
            )
            conn.commit()
        except sqlite3.Error as exc:
            if "conn" in locals():
                conn.rollback()
            raise RuntimeError(
                "delivery_backpressure_store_unavailable:sqlite"
            ) from exc
        except Exception:
            if "conn" in locals():
                conn.rollback()
            raise
        finally:
            if "conn" in locals():
                conn.close()

        unresolved = {
            state: counts.get(state, 0) for state in DELIVERY_EFFECT_UNRESOLVED_STATES
        }
        unresolved_effects = sum(unresolved.values())
        pending_watches = watch_counts.get("pending", 0)
        running_watches = watch_counts.get("running", 0)
        return DeliveryBackpressureSnapshot(
            schema_version=DELIVERY_BACKPRESSURE_SNAPSHOT_SCHEMA_VERSION,
            observed_at=current,
            db_path=str(path),
            pending=unresolved["pending"],
            claimed=unresolved["claimed"],
            retry_wait=unresolved["retry_wait"],
            uncertain=unresolved["uncertain"],
            unresolved_effects=unresolved_effects,
            untracked_completed_submissions=untracked_completed,
            pending_watches=pending_watches,
            running_watches=running_watches,
            unresolved_work=(
                unresolved_effects
                + untracked_completed
                + pending_watches
                + running_watches
            ),
            outcome_slo=outcome_slo,
            circuit=circuit,
            circuits=circuits,
        )

    def backpressure_snapshot(
        self,
        *,
        now: datetime | None = None,
        activation_required: bool = False,
    ) -> DeliveryBackpressureSnapshot:
        return self.read_existing_backpressure_snapshot(
            self.db_path,
            now=now,
            busy_timeout_ms=self.busy_timeout_ms,
            activation_required=activation_required,
            expected_control_schema_version=self._connection_write_schema_version,
        )

    def preview_dispatchable_effects(
        self,
        *,
        limit: int = 100,
        activation_required: bool = False,
    ) -> list[dict[str, Any]]:
        self._validate_activation_required(activation_required)
        if limit < 1:
            return []
        conn = self._connect()
        try:
            conn.execute("BEGIN")
            activation_enforced = self._activation_enforced_tx(
                conn,
                activation_required=activation_required,
            )
            if activation_enforced:
                self._require_activation_schema(conn)
            activation_joins = (
                """
             LEFT JOIN rca_execution_watch AS w
                    ON w.submission_key = j.submission_key
             LEFT JOIN rca_outbox AS o
                    ON o.outbox_id = w.submission_outbox_id
             LEFT JOIN business_triggers AS t
                    ON t.business_key = o.business_key
                   AND t.generation = o.generation
                """
                if activation_enforced
                else ""
            )
            activation_filter = (
                "AND ("
                f"({_ACTIVATION_EXECUTION_ELIGIBLE_SQL}) OR "
                f"({_ADJUDICATION_ACTIVATION_ELIGIBLE_SQL}) OR "
                f"({_CARD_PATCH_ACTIVATION_ELIGIBLE_SQL})"
                ")"
                if activation_enforced
                else ""
            )
            rows = conn.execute(
                f"""
                SELECT e.effect_key, e.delivery_id, e.effect_kind, e.required,
                       e.status, e.attempt, e.next_attempt_at, e.created_at,
                       j.project_key, j.work_item_id, j.report_url, j.outcome
                  FROM rca_delivery_effects AS e
                  JOIN rca_delivery_jobs AS j ON j.delivery_id = e.delivery_id
                  {activation_joins}
                 WHERE e.status IN ('pending', 'retry_wait', 'uncertain', 'claimed')
                   {activation_filter}
                 ORDER BY COALESCE(e.next_attempt_at, e.created_at), e.effect_key
                 LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def list_rows(self, table: str) -> list[dict[str, Any]]:
        allowed = {
            "rca_execution_watch",
            "rca_delivery_jobs",
            "rca_delivery_effects",
            "rca_delivery_attempts",
            "rca_delivery_dispatcher_circuit",
            "rca_conclusion_adjudications",
            "rca_delivery_subscriptions",
            "rca_delivery_subscription_events",
            "rca_delivery_observation_outbox",
            "rca_failure_routes",
        }
        if table not in allowed:
            raise ValueError(f"unsupported delivery table: {table}")
        conn = self._connect()
        try:
            rows = conn.execute(f"SELECT * FROM {table} ORDER BY rowid").fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    @staticmethod
    def _activation_pipeline_count_tx(
        conn: sqlite3.Connection,
        *,
        from_sql: str,
        where_sql: str,
    ) -> dict[str, int]:
        row = conn.execute(
            f"""
            SELECT COUNT(*) AS total,
                   COALESCE(SUM(CASE
                       WHEN {_ACTIVATION_CURRENT_BINDING_SQL} THEN 1 ELSE 0
                   END), 0) AS current_bound,
                   COALESCE(SUM(CASE
                       WHEN {_ACTIVATION_EXECUTION_ELIGIBLE_SQL} THEN 1 ELSE 0
                   END), 0) AS eligible
              {from_sql}
             WHERE {where_sql}
            """
        ).fetchone()
        total = int(row["total"])
        current_bound = int(row["current_bound"])
        eligible = int(row["eligible"])
        return {
            "eligible": eligible,
            "held_current": max(0, current_bound - eligible),
            "blocked_historical": max(0, total - current_bound),
        }

    @classmethod
    def _activation_health_tx(
        cls,
        conn: sqlite3.Connection,
        *,
        activation_required: bool,
        successor_read_only: bool = False,
    ) -> dict[str, Any]:
        activation_enforced = cls._activation_enforced_tx(
            conn,
            activation_required=activation_required,
        )
        try:
            schema_version = RcaControlStore._activation_schema_version_tx(conn)
        except (ActivationEpochError, RuntimeError):
            schema_version = ""
        schema_ready = cls._activation_schema_ready(
            conn,
            schema_version=schema_version,
        )
        empty = {
            "completed_submissions": 0,
            "active_watches": 0,
            "pending_subscriptions": 0,
            "dispatchable_effects": 0,
        }
        if not schema_ready:
            return {
                "required": activation_enforced,
                "schema_ready": False,
                "schema_version": schema_version,
                "current_epoch_id": "",
                "current_epoch_state": "unconfigured",
                "binding_valid": False,
                "release_fingerprint_sha256": "",
                "release_note_sha256": "",
                "production_ready": False,
                "processing_enabled": (
                    not activation_enforced and not successor_read_only
                ),
                "eligible_counts": dict(empty),
                "held_current_counts": dict(empty),
                "blocked_historical_counts": dict(empty),
            }
        epoch = conn.execute(
            "SELECT * FROM rca_activation_epochs WHERE is_current = 1"
        ).fetchone()
        state = str(epoch["state"]) if epoch is not None else "unconfigured"
        binding_valid = False
        release_binding = {
            "release_fingerprint_sha256": "",
            "release_note_sha256": "",
        }
        if epoch is not None:
            try:
                RcaControlStore._activation_transition_binding_tx(
                    conn,
                    epoch=epoch,
                    schema_version=schema_version,
                )
            except ActivationEpochError:
                pass
            else:
                binding_valid = True
                release_binding = RcaControlStore._activation_release_epoch_projection(
                    epoch,
                    schema_version=schema_version,
                )
        release_fingerprint_sha256 = release_binding[
            "release_fingerprint_sha256"
        ]
        release_fingerprint_valid = (
            re.fullmatch(r"[0-9a-f]{64}", release_fingerprint_sha256) is not None
            and release_fingerprint_sha256 != "0" * 64
        )
        release_note_sha256 = release_binding["release_note_sha256"]
        release_note_valid = (
            re.fullmatch(r"[0-9a-f]{64}", release_note_sha256) is not None
            and release_note_sha256 != "0" * 64
        )
        production_ready = (
            binding_valid
            and state == "steady_active"
            and release_fingerprint_valid
            and release_note_valid
            and not successor_read_only
        )
        if not activation_enforced:
            return {
                "required": False,
                "schema_ready": True,
                "schema_version": schema_version,
                "current_epoch_id": "",
                "current_epoch_state": state,
                "binding_valid": binding_valid,
                "release_fingerprint_sha256": release_fingerprint_sha256,
                "release_note_sha256": release_note_sha256,
                "production_ready": production_ready,
                "processing_enabled": not successor_read_only,
                "eligible_counts": dict(empty),
                "held_current_counts": dict(empty),
                "blocked_historical_counts": dict(empty),
            }
        rows: dict[str, dict[str, int]] = {
            "completed_submissions": cls._activation_pipeline_count_tx(
                conn,
                from_sql="""
                  FROM rca_outbox AS o
                  JOIN business_triggers AS t
                    ON t.business_key = o.business_key
                   AND t.generation = o.generation
             LEFT JOIN rca_execution_watch AS w
                    ON w.submission_key = o.submission_key
                """,
                where_sql="""
                ((o.status = 'completed' AND o.result_json IS NOT NULL)
                 OR o.status = 'quarantined')
                AND w.submission_key IS NULL
                """,
            ),
            "active_watches": cls._activation_pipeline_count_tx(
                conn,
                from_sql="""
                  FROM rca_execution_watch AS w
                  JOIN rca_outbox AS o ON o.outbox_id = w.submission_outbox_id
                  JOIN business_triggers AS t
                    ON t.business_key = o.business_key
                   AND t.generation = o.generation
                """,
                where_sql="w.state IN ('pending', 'running')",
            ),
            "dispatchable_effects": cls._activation_pipeline_count_tx(
                conn,
                from_sql="""
                  FROM rca_delivery_effects AS e
                  JOIN rca_delivery_jobs AS j ON j.delivery_id = e.delivery_id
                  JOIN rca_execution_watch AS w
                    ON w.submission_key = j.submission_key
                  JOIN rca_outbox AS o
                    ON o.outbox_id = w.submission_outbox_id
                  JOIN business_triggers AS t
                    ON t.business_key = o.business_key
                   AND t.generation = o.generation
                """,
                where_sql="""
                e.status IN ('pending', 'claimed', 'retry_wait', 'uncertain')
                """,
            ),
        }
        if cls._table_exists(conn, "rca_delivery_subscriptions"):
            rows["pending_subscriptions"] = cls._activation_pipeline_count_tx(
                conn,
                from_sql="""
                  FROM rca_delivery_subscriptions AS s
                  JOIN rca_delivery_jobs AS j
                    ON j.business_key = s.business_key
                   AND j.generation = s.generation
                  JOIN rca_execution_watch AS w
                    ON w.submission_key = j.submission_key
                  JOIN rca_outbox AS o
                    ON o.outbox_id = w.submission_outbox_id
                  JOIN business_triggers AS t
                    ON t.business_key = o.business_key
                   AND t.generation = o.generation
                """,
                where_sql="s.status = 'pending'",
            )
        else:
            rows["pending_subscriptions"] = {
                "eligible": 0,
                "held_current": 0,
                "blocked_historical": 0,
            }
        return {
            "required": activation_enforced,
            "schema_ready": True,
            "schema_version": schema_version,
            "current_epoch_id": str(epoch["epoch_id"]) if epoch is not None else "",
            "current_epoch_state": state,
            "binding_valid": binding_valid,
            "release_fingerprint_sha256": release_fingerprint_sha256,
            "release_note_sha256": release_note_sha256,
            "production_ready": production_ready,
            "processing_enabled": (
                binding_valid and state in ACTIVATION_DELIVERY_STATES
                and not successor_read_only
            ),
            "eligible_counts": {
                name: values["eligible"] for name, values in sorted(rows.items())
            },
            "held_current_counts": {
                name: values["held_current"] for name, values in sorted(rows.items())
            },
            "blocked_historical_counts": {
                name: values["blocked_historical"]
                for name, values in sorted(rows.items())
            },
        }

    def canonical_canary_readback(
        self,
        *,
        batch_id: str,
        issue_id: str,
        submission_key: str,
        activation_epoch_id: str,
    ) -> dict[str, Any]:
        """Project one delivered canary from canonical rows in one transaction."""

        batch = str(batch_id or "").strip()
        issue = str(issue_id or "").strip()
        submission = str(submission_key or "").strip()
        epoch_id = str(activation_epoch_id or "").strip()
        if (
            re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{2,127}", batch) is None
            or re.fullmatch(r"[0-9]{6,24}", issue) is None
            or re.fullmatch(r"g1q3-rca-s1-[0-9a-f]{64}", submission) is None
            or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{5,127}", epoch_id)
            is None
        ):
            raise ValueError("canonical_canary_readback_identity_invalid")

        def invalid(detail: str) -> None:
            raise DeliveryRecordConflictError(
                f"canonical_canary_readback_invalid:{detail}"
            )

        def exact_object(raw: Any, detail: str) -> dict[str, Any]:
            if not isinstance(raw, str) or not raw:
                invalid(detail)
            try:
                value = json.loads(raw)
            except (TypeError, json.JSONDecodeError):
                invalid(detail)
            if not isinstance(value, dict) or _canonical_json(value) != raw:
                invalid(detail)
            return value

        conn = self._connect()
        try:
            conn.execute("BEGIN")
            epochs = conn.execute(
                "SELECT epoch_id, state FROM rca_activation_epochs "
                "WHERE is_current = 1"
            ).fetchall()
            if (
                len(epochs) != 1
                or str(epochs[0]["epoch_id"]) != epoch_id
                or str(epochs[0]["state"]) != "steady_active"
            ):
                invalid("activation_not_current")

            core_rows = conn.execute(
                """
                SELECT t.business_key, t.generation,
                       t.project_key, t.work_item_type_key, t.work_item_id,
                       t.state AS trigger_state,
                       t.activation_epoch_id AS trigger_epoch_id,
                       t.activation_ledger_id AS trigger_ledger_id,
                       t.origin_source_id AS trigger_source_id,
                       o.outbox_id, o.status AS outbox_status,
                       o.activation_epoch_id AS outbox_epoch_id,
                       o.activation_ledger_id AS outbox_ledger_id,
                       o.origin_source_id AS outbox_source_id,
                       o.result_json AS outbox_result_json,
                       w.submission_outbox_id, w.business_key AS watch_business_key,
                       w.generation AS watch_generation,
                       w.project_key AS watch_project_key,
                       w.work_item_type_key AS watch_work_item_type_key,
                       w.work_item_id AS watch_work_item_id,
                       w.task_id, w.state AS watch_state,
                       w.last_status_json, w.delivery_id AS watch_delivery_id,
                       b.role AS source_role,
                       s.source_id, s.source_kind, s.platform, s.chat_id,
                       s.thread_id, s.message_id, s.requester_id,
                       s.mode AS source_mode, s.outcome AS source_outcome,
                       l.epoch_id AS ledger_epoch_id,
                       l.business_key AS ledger_business_key,
                       l.submission_key AS ledger_submission_key,
                       l.generation AS ledger_generation,
                       l.decision AS ledger_decision, l.bound_at AS ledger_bound_at
                  FROM business_triggers AS t
                  JOIN rca_outbox AS o
                    ON o.business_key = t.business_key
                   AND o.generation = t.generation
                   AND o.submission_key = t.submission_key
                  JOIN rca_execution_watch AS w
                    ON w.submission_key = t.submission_key
                   AND w.submission_outbox_id = o.outbox_id
                  JOIN rca_trigger_bindings AS b
                    ON b.source_id = t.origin_source_id
                   AND b.business_key = t.business_key
                   AND b.generation = t.generation
                  JOIN rca_trigger_sources AS s
                    ON s.source_id = b.source_id
                  JOIN rca_activation_admission_ledger AS l
                    ON l.ledger_id = t.activation_ledger_id
                 WHERE t.submission_key = ? AND t.work_item_id = ?
                """,
                (submission, issue),
            ).fetchall()
            if len(core_rows) != 1:
                invalid("execution_lineage_missing")
            core = core_rows[0]
            generation = int(core["generation"])
            business_key = str(core["business_key"])
            source_id = str(core["source_id"])
            expected_message_prefix = f"{batch}-{issue}-try-"
            if (
                generation < 1
                or str(core["trigger_state"]) != "submitted"
                or str(core["outbox_status"]) != "completed"
                or str(core["trigger_epoch_id"] or "") != epoch_id
                or str(core["outbox_epoch_id"] or "") != epoch_id
                or core["trigger_ledger_id"] is None
                or core["trigger_ledger_id"] != core["outbox_ledger_id"]
                or str(core["ledger_epoch_id"] or "") != epoch_id
                or str(core["ledger_business_key"]) != business_key
                or str(core["ledger_submission_key"]) != submission
                or int(core["ledger_generation"]) != generation
                or str(core["ledger_decision"]) != "admit"
                or not str(core["ledger_bound_at"] or "")
                or str(core["trigger_source_id"] or "") != source_id
                or str(core["outbox_source_id"] or "") != source_id
                or str(core["source_role"]) != "origin"
                or str(core["source_kind"]) != "feishu_group_manual"
                or str(core["platform"]) != "operator"
                or str(core["chat_id"] or "")
                or str(core["thread_id"] or "")
                or str(core["source_mode"]) != "rerun"
                or str(core["source_outcome"]) != "created"
                or not str(core["requester_id"] or "").startswith("automation:")
                or re.fullmatch(
                    re.escape(expected_message_prefix) + r"[1-9][0-9]*",
                    str(core["message_id"] or ""),
                )
                is None
                or int(core["submission_outbox_id"]) != int(core["outbox_id"])
                or str(core["watch_business_key"]) != business_key
                or int(core["watch_generation"]) != generation
                or str(core["watch_project_key"]) != str(core["project_key"])
                or str(core["watch_work_item_type_key"])
                != str(core["work_item_type_key"])
                or str(core["watch_work_item_id"]) != issue
                or str(core["task_id"] or "") != submission
                or str(core["watch_state"]) != "delivery_created"
                or not str(core["watch_delivery_id"] or "")
            ):
                invalid("execution_lineage_mismatch")

            outbox_result = exact_object(
                core["outbox_result_json"], "outbox_result_invalid"
            )
            if (
                outbox_result.get("success") is not True
                or outbox_result.get("submission_key") != submission
                or outbox_result.get("task_id") != submission
            ):
                invalid("outbox_result_mismatch")
            watch_status = exact_object(
                core["last_status_json"], "watch_status_invalid"
            )
            execution_readback = watch_status.get("execution_identity_readback")
            if (
                watch_status.get("success") is not True
                or watch_status.get("state") != "completed"
                or not isinstance(execution_readback, dict)
            ):
                invalid("watch_status_mismatch")

            if generation <= 1:
                invalid("rerun_authority_missing")
            authorities = conn.execute(
                """
                SELECT authority_family, outbox_id, business_key, generation,
                       batch_id, issue_id, submission_key, activation_epoch_id,
                       activation_ledger_id, source_id, effect_kind,
                       project_key, work_item_type_key
                  FROM rca_owner_authorized_rerun_delivery_authorities
                 WHERE submission_key = ?
                """,
                (submission,),
            ).fetchall()
            if len(authorities) != 1:
                invalid("rerun_authority_missing")
            authority = authorities[0]
            if (
                str(authority["authority_family"])
                not in {"terminal_rerun", "historical_epoch_rerun"}
                or int(authority["outbox_id"]) != int(core["outbox_id"])
                or str(authority["business_key"]) != business_key
                or int(authority["generation"]) != generation
                or str(authority["batch_id"]) != batch
                or str(authority["issue_id"]) != issue
                or str(authority["submission_key"]) != submission
                or str(authority["activation_epoch_id"]) != epoch_id
                or authority["activation_ledger_id"] != core["trigger_ledger_id"]
                or str(authority["source_id"]) != source_id
                or str(authority["effect_kind"]) != "feishu_issue_comment"
                or str(authority["project_key"]) != str(core["project_key"])
                or str(authority["work_item_type_key"])
                != str(core["work_item_type_key"])
            ):
                invalid("rerun_authority_mismatch")

            jobs = conn.execute(
                "SELECT * FROM rca_delivery_jobs WHERE submission_key = ?",
                (submission,),
            ).fetchall()
            if len(jobs) != 1:
                invalid("delivery_job_missing")
            job = jobs[0]
            delivery_id = str(job["delivery_id"])
            if (
                delivery_id != str(core["watch_delivery_id"])
                or str(job["business_key"]) != business_key
                or int(job["generation"]) != generation
                or str(job["project_key"]) != str(core["project_key"])
                or str(job["work_item_type_key"])
                != str(core["work_item_type_key"])
                or str(job["work_item_id"]) != issue
                or str(job["status"]) != "delivered"
                or str(job["outcome"]) != "success"
            ):
                invalid("delivery_job_mismatch")

            effects = conn.execute(
                "SELECT * FROM rca_delivery_effects "
                "WHERE delivery_id = ? AND required = 1 "
                "ORDER BY effect_kind, effect_key",
                (delivery_id,),
            ).fetchall()
            if (
                len(effects) != 1
                or str(effects[0]["effect_kind"]) != "feishu_issue_comment"
                or str(effects[0]["status"]) != "succeeded"
                or str(effects[0]["write_phase"]) != "settled"
                or not str(effects[0]["completed_at"] or "")
            ):
                invalid("required_effects_mismatch")
            effect = effects[0]
            effect_key = str(effect["effect_key"])
            effect_payload = exact_object(
                effect["payload_json"], "effect_payload_invalid"
            )
            field_updates = effect_payload.get("field_updates")
            expected_target = str(job["target_key"])
            expected_report_url = str(job["report_url"])
            expected_result_value = effect_payload.get("result_field_value")
            expected_field_updates = [
                {
                    "field_key": RCA_RESULT_FIELD_KEY,
                    "field_value": expected_result_value,
                },
                {
                    "field_key": RCA_REPORT_FIELD_KEY,
                    "field_value": expected_report_url,
                },
            ]
            try:
                computed_payload_sha256 = compute_delivery_effect_payload_sha256(
                    effect_payload, DELIVERY_EFFECT_KIND
                )
                computed_effect_key = compute_delivery_effect_key(
                    delivery_id=delivery_id,
                    effect_kind=DELIVERY_EFFECT_KIND,
                    target_key=expected_target,
                    semantic_payload_sha256=computed_payload_sha256,
                )
                expected_marker = delivery_effect_marker(
                    computed_effect_key, str(job["artifact_set_id"])
                )
            except (DeliveryContractError, TypeError, ValueError):
                invalid("effect_payload_invalid")
            if (
                str(effect["target_key"]) != expected_target
                or effect_payload.get("schema_version")
                != DELIVERY_EFFECT_SCHEMA_VERSION
                or effect_payload.get("delivery_id") != delivery_id
                or effect_payload.get("effect_kind") != DELIVERY_EFFECT_KIND
                or effect_payload.get("target_key") != expected_target
                or effect_payload.get("project_key") != str(core["project_key"])
                or effect_payload.get("work_item_type_key")
                != str(core["work_item_type_key"])
                or effect_payload.get("work_item_id") != issue
                or effect_payload.get("issue_url") != str(job["issue_url"])
                or effect_payload.get("artifact_set_id")
                != str(job["artifact_set_id"])
                or not expected_report_url
                or effect_payload.get("report_url") != expected_report_url
                or not isinstance(expected_result_value, str)
                or not expected_result_value.strip()
                or field_updates != expected_field_updates
                or str(effect["payload_sha256"]) != computed_payload_sha256
                or effect_payload.get("semantic_payload_sha256")
                != computed_payload_sha256
                or effect_key != computed_effect_key
                or effect_payload.get("effect_key") != computed_effect_key
                or effect_payload.get("marker") != expected_marker
            ):
                invalid("effect_payload_mismatch")
            receipt = exact_object(
                effect["remote_receipt_json"], "remote_receipt_invalid"
            )
            remote_id = str(receipt.get("remote_id") or "")
            source = str(receipt.get("source") or "")
            fields = receipt.get("confirmed_field_keys")
            canonical_fields = (
                sorted(fields)
                if isinstance(fields, list)
                and all(isinstance(value, str) for value in fields)
                else []
            )
            content = str(effect_payload.get("comment_content") or "")
            if (
                not remote_id
                or source not in {"read_after_write", "read_after_recovery_write"}
                or canonical_fields != ["field_8c912e", "field_9193cb"]
                or receipt.get("confirmed_report_url") != str(job["report_url"])
                or receipt.get("marker") != effect_payload.get("marker")
                or not content
                or receipt.get("confirmed_content_sha256")
                != hashlib.sha256(content.encode("utf-8")).hexdigest()
            ):
                invalid("remote_receipt_mismatch")

            attempts = conn.execute(
                "SELECT remote_id FROM rca_delivery_attempts "
                "WHERE effect_key = ? AND outcome IN ('ack', 'reconciled') "
                "AND finished_at IS NOT NULL ORDER BY attempt_id",
                (effect_key,),
            ).fetchall()
            if len(attempts) != 1 or str(attempts[0]["remote_id"] or "") != remote_id:
                invalid("delivery_attempt_mismatch")

            observations = conn.execute(
                "SELECT * FROM rca_delivery_observation_outbox WHERE effect_key = ?",
                (effect_key,),
            ).fetchall()
            if (
                len(observations) != 1
                or str(observations[0]["status"]) != "appended"
                or not str(observations[0]["appended_at"] or "")
            ):
                invalid("delivery_observation_missing")
            observation_row = observations[0]
            observation = exact_object(
                observation_row["payload_json"], "delivery_observation_invalid"
            )
            try:
                validated_observation = validate_delivery_observation(observation)
            except (TypeError, ValueError) as exc:
                raise DeliveryRecordConflictError(
                    "canonical_canary_readback_invalid:delivery_observation_invalid"
                ) from exc
            if (
                str(observation_row["payload_sha256"])
                != hashlib.sha256(_canonical_json(observation).encode()).hexdigest()
                or str(validated_observation["observation_id"])
                != str(observation_row["observation_id"])
                or str(validated_observation["work_item_id"]) != issue
                or str(validated_observation["case_key"]) != submission
                or str(validated_observation["remote_receipt_id"]) != remote_id
                or str(validated_observation["release_id"])
                != str(execution_readback.get("release_id") or "")
                or str(validated_observation["outcome_content_sha256"])
                != hashlib.sha256(content.encode("utf-8")).hexdigest()
            ):
                invalid("delivery_observation_mismatch")

            result = {
                "schema_version": CANONICAL_CANARY_READBACK_SCHEMA_VERSION,
                "batch_id": batch,
                "issue_id": issue,
                "submission_key": submission,
                "activation_epoch_id": epoch_id,
                "trigger": {
                    "business_key": business_key,
                    "generation": generation,
                    "state": str(core["trigger_state"]),
                    "source_id": source_id,
                },
                "outbox": {
                    "outbox_id": int(core["outbox_id"]),
                    "status": str(core["outbox_status"]),
                },
                "watch": {
                    "state": str(core["watch_state"]),
                    "task_id": str(core["task_id"]),
                    "delivery_id": delivery_id,
                },
                "delivery_job": {
                    "delivery_id": delivery_id,
                    "status": str(job["status"]),
                    "outcome": str(job["outcome"]),
                },
                "required_effects": [
                    {
                        "effect_key": effect_key,
                        "effect_kind": str(effect["effect_kind"]),
                        "status": str(effect["status"]),
                        "write_phase": str(effect["write_phase"]),
                        "remote_id": remote_id,
                        "observation_id": str(observation_row["observation_id"]),
                    }
                ],
                "transport": {
                    "status": "pass",
                    "official_comment_id": remote_id,
                    "official_field_keys": canonical_fields,
                    "official_readback_source": source,
                },
                "execution_identity_readback": dict(execution_readback),
            }
            conn.commit()
            return result
        except Exception:
            if conn.in_transaction:
                conn.rollback()
            raise
        finally:
            conn.close()

    def health(
        self,
        *,
        now: datetime | None = None,
        activation_required: bool = False,
    ) -> dict[str, Any]:
        self._validate_activation_required(activation_required)
        current_dt = _utc_datetime(now)
        current = _iso(current_dt)
        stalled_cutoff = _iso(
            current_dt - timedelta(seconds=DELIVERY_WATCH_SLA_SECONDS)
        )
        conn = self._connect_read_only()
        try:
            conn.execute("BEGIN")
            schema_runtime_capability = self.schema_runtime_capability()
            activation = self._activation_health_tx(
                conn,
                activation_required=activation_required,
                successor_read_only=(
                    schema_runtime_capability["mode"]
                    == "successor_read_only"
                ),
            )
            watch = {
                row["state"]: int(row["count"])
                for row in conn.execute(
                    "SELECT state, COUNT(*) AS count FROM rca_execution_watch GROUP BY state"
                ).fetchall()
            }
            jobs = {
                row["status"]: int(row["count"])
                for row in conn.execute(
                    "SELECT status, COUNT(*) AS count FROM rca_delivery_jobs GROUP BY status"
                ).fetchall()
            }
            job_outcomes = {
                row["outcome"]: int(row["count"])
                for row in conn.execute(
                    "SELECT outcome, COUNT(*) AS count "
                    "FROM rca_delivery_jobs GROUP BY outcome"
                ).fetchall()
            }
            outcome_slo = self._delivery_outcome_slo_in_transaction(
                conn, current_dt=current_dt
            )
            effects = {
                row["status"]: int(row["count"])
                for row in conn.execute(
                    "SELECT status, COUNT(*) AS count FROM rca_delivery_effects GROUP BY status"
                ).fetchall()
            }
            uncertain_effect_blockers = int(effects.get("uncertain", 0))
            current_epoch_id = str(activation["current_epoch_id"] or "")
            if (
                activation["required"]
                and activation["schema_ready"]
                and current_epoch_id
            ):
                uncertain_activation_counts = self._activation_pipeline_count_tx(
                    conn,
                    from_sql="""
                      FROM rca_delivery_effects AS e
                      JOIN rca_delivery_jobs AS j ON j.delivery_id = e.delivery_id
                      JOIN rca_execution_watch AS w
                        ON w.submission_key = j.submission_key
                      JOIN rca_outbox AS o
                        ON o.outbox_id = w.submission_outbox_id
                      JOIN business_triggers AS t
                        ON t.business_key = o.business_key
                       AND t.generation = o.generation
                    """,
                    where_sql="e.status = 'uncertain'",
                )
                uncertain_effect_blockers = sum(
                    uncertain_activation_counts[key]
                    for key in ("eligible", "held_current")
                )
            attempts = {
                row["outcome"]: int(row["count"])
                for row in conn.execute(
                    "SELECT outcome, COUNT(*) AS count FROM rca_delivery_attempts GROUP BY outcome"
                ).fetchall()
            }
            observation_outbox = {
                str(row["status"]): int(row["count"])
                for row in conn.execute(
                    "SELECT status, COUNT(*) AS count "
                    "FROM rca_delivery_observation_outbox GROUP BY status"
                ).fetchall()
            }
            circuit_rows = conn.execute(
                """
                SELECT circuit_name, state, reason_code, reason_detail,
                       opened_at, updated_at
                  FROM rca_delivery_dispatcher_circuit
                """
            ).fetchall()
            circuits = {
                str(row["circuit_name"]): {
                    key: row[key]
                    for key in (
                        "state",
                        "reason_code",
                        "reason_detail",
                        "opened_at",
                        "updated_at",
                    )
                }
                for row in circuit_rows
            }
            subscriptions: dict[str, int] = {}
            subscription_reasons: list[dict[str, Any]] = []
            subscription_events: dict[str, int] = {}
            pending_required_subscriptions = 0
            if self._table_exists(conn, "rca_delivery_subscriptions"):
                subscriptions = {
                    str(row["status"]): int(row["count"])
                    for row in conn.execute(
                        "SELECT status, COUNT(*) AS count "
                        "FROM rca_delivery_subscriptions GROUP BY status"
                    ).fetchall()
                }
                pending_required_subscriptions = int(
                    conn.execute(
                        "SELECT COUNT(*) FROM rca_delivery_subscriptions "
                        "WHERE required = 1 AND status = 'pending'"
                    ).fetchone()[0]
                )
                if "reason" in self._table_columns_tx(
                    conn, "rca_delivery_subscriptions"
                ):
                    subscription_reasons = [
                        {
                            "status": str(row["status"]),
                            "reason": str(row["reason"]),
                            "count": int(row["count"]),
                        }
                        for row in conn.execute(
                            "SELECT status, reason, COUNT(*) AS count "
                            "FROM rca_delivery_subscriptions "
                            "GROUP BY status, reason ORDER BY status, reason"
                        ).fetchall()
                    ]
                if self._table_exists(
                    conn, "rca_delivery_subscription_events"
                ):
                    subscription_events = {
                        str(row["new_status"]): int(row["count"])
                        for row in conn.execute(
                            "SELECT new_status, COUNT(*) AS count "
                            "FROM rca_delivery_subscription_events "
                            "GROUP BY new_status"
                        ).fetchall()
                    }
            unresolved_required_effects = int(
                conn.execute(
                    "SELECT COUNT(*) FROM rca_delivery_effects "
                    "WHERE required = 1 AND status IN "
                    "('pending', 'claimed', 'retry_wait', 'uncertain')"
                ).fetchone()[0]
            )
            oldest_due = conn.execute(
                """
                SELECT MIN(next_poll_at) FROM rca_execution_watch
                 WHERE state IN ('pending', 'running')
                """
            ).fetchone()[0]
            expired = int(
                conn.execute(
                    """
                    SELECT COUNT(*) FROM rca_execution_watch
                     WHERE state IN ('pending', 'running')
                       AND lease_token IS NOT NULL AND lease_expires_at <= ?
                    """,
                    (current,),
                ).fetchone()[0]
            )
            oldest_active_created_at = conn.execute(
                """
                SELECT MIN(created_at) FROM rca_execution_watch
                 WHERE state IN ('pending', 'running')
                """
            ).fetchone()[0]
            stalled_watch_count = int(
                conn.execute(
                    """
                    SELECT COUNT(*) FROM rca_execution_watch
                     WHERE state IN ('pending', 'running') AND created_at <= ?
                    """,
                    (stalled_cutoff,),
                ).fetchone()[0]
            )
            business_blockers = {
                "schema_successor_read_only": int(
                    schema_runtime_capability["mode"]
                    == "successor_read_only"
                ),
                "activation_schema_unavailable": int(
                    activation["required"] and not activation["schema_ready"]
                ),
                "stalled_watches": stalled_watch_count,
                "terminal_failed_watches": int(watch.get("terminal_failed", 0)),
                "quarantined_watches": int(watch.get("quarantined", 0)),
                "quarantined_jobs": int(jobs.get("quarantined", 0)),
                "uncertain_effects": uncertain_effect_blockers,
                "quarantined_effects": int(effects.get("quarantined", 0)),
                "quarantined_subscriptions": int(subscriptions.get("quarantined", 0)),
                "pending_required_subscriptions": pending_required_subscriptions,
                "unresolved_required_effects": unresolved_required_effects,
                "outcome_slo_breached": int(not outcome_slo["healthy"]),
                "pending_delivery_observations": int(
                    observation_outbox.get("pending", 0)
                ),
            }
            # Keep ordinary backlog and historical outcome counts visible without
            # turning them into admission gates.
            production_blockers = {
                "schema_successor_read_only": business_blockers[
                    "schema_successor_read_only"
                ],
                "activation_schema_unavailable": business_blockers[
                    "activation_schema_unavailable"
                ],
                "activation_epoch_not_steady": int(
                    activation["required"]
                    and activation["current_epoch_state"] != "steady_active"
                ),
                "activation_binding_invalid": int(
                    activation["required"]
                    and bool(activation["current_epoch_id"])
                    and not activation["binding_valid"]
                ),
                "release_fingerprint_invalid": int(
                    activation["required"]
                    and not (
                        re.fullmatch(
                            r"[0-9a-f]{64}",
                            str(activation["release_fingerprint_sha256"]),
                        )
                        and activation["release_fingerprint_sha256"] != "0" * 64
                    )
                ),
                "release_note_invalid": int(
                    activation["required"]
                    and not (
                        re.fullmatch(
                            r"[0-9a-f]{64}",
                            str(activation["release_note_sha256"]),
                        )
                        and activation["release_note_sha256"] != "0" * 64
                    )
                ),
                "uncertain_effects": business_blockers["uncertain_effects"],
                "pending_delivery_observations": business_blockers[
                    "pending_delivery_observations"
                ],
            }
            required_circuits = set(REQUIRED_DELIVERY_EFFECT_KINDS)
            circuits_ready = all(
                circuits.get(name, {}).get("state") == "closed"
                for name in required_circuits
            )
            circuit = circuits.get(DELIVERY_EFFECT_KIND, {"state": "missing"})
            ready = not any(production_blockers.values()) and circuits_ready
            permanent_failure_circuit = {
                "threshold": PERMANENT_FAILURE_CIRCUIT_THRESHOLD,
                "consecutive_failures": (
                    self._permanent_failure_streak_in_transaction(conn)
                ),
            }
            return {
                "ok": ready,
                "process_healthy": True,
                "business_ready": ready,
                "schema_version": DELIVERY_STORE_SCHEMA_VERSION,
                "schema_runtime_capability": schema_runtime_capability,
                "db_path": str(self.db_path),
                "execution_watch": watch,
                "delivery_jobs": jobs,
                "delivery_job_outcomes": job_outcomes,
                "delivery_outcome_slo": outcome_slo,
                "delivery_effects": effects,
                "delivery_attempts": attempts,
                "delivery_observation_outbox": observation_outbox,
                "delivery_subscriptions": subscriptions,
                "delivery_subscription_reasons": subscription_reasons,
                "delivery_subscription_events": subscription_events,
                "delivery_dispatcher_circuit": circuit,
                "delivery_dispatcher_circuits": circuits,
                "activation": activation,
                "permanent_failure_circuit": permanent_failure_circuit,
                "oldest_watch_next_poll_at": oldest_due,
                "oldest_active_watch_created_at": oldest_active_created_at,
                "watch_sla_seconds": DELIVERY_WATCH_SLA_SECONDS,
                "business_blockers": business_blockers,
                "production_blockers": production_blockers,
                "expired_watch_leases": expired,
            }
        finally:
            conn.close()
