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


CONTROL_STORE_SCHEMA_VERSION = "pnc_rca_control_store_v11"
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
        CONTROL_STORE_SCHEMA_VERSION,
    }
)
CONTROL_DB_MIN_AVAILABLE_BYTES = 1024 * 1024 * 1024
DEFAULT_OUTBOX_HIGH_WATERMARK = 100
MANUAL_OUTBOX_SHARE_NUMERATOR = 4
MANUAL_OUTBOX_SHARE_DENOMINATOR = 5
OUTBOX_MAX_CONSECUTIVE_KAFKA_CLAIMS = 3
OUTBOX_KAFKA_CLAIM_STREAK_META_KEY = "outbox_kafka_claim_streak"
DEFAULT_MANUAL_OPERATOR_RATE_LIMIT = 3
DEFAULT_MANUAL_OPERATOR_RATE_WINDOW_SECONDS = 600
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
OUTBOX_PAYLOAD_SCHEMA_VERSION = "pnc_rca_submission_outbox_v2"
DELIVERY_TARGET_SCHEMA_VERSION = "pnc_rca_delivery_target_v1"
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
    ):
        self.db_path = Path(db_path).expanduser()
        if not isinstance(require_current, bool):
            raise TypeError("require_current must be true or false")
        self.require_current = require_current
        if require_current:
            self._validate_no_installation_marker()
            self._validate_existing_path()
        else:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.busy_timeout_ms = max(1, int(busy_timeout_ms))
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

    def _connect(self) -> sqlite3.Connection:
        if self.require_current:
            self._validate_no_installation_marker()
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
            self._initialization_mode = (
                "migration" if self._migrate_v10_to_v11() else "steady"
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
                    CONTROL_STORE_SCHEMA_VERSION,
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

    def _preflight_schema_version(self) -> str | None:
        """Reject a future schema using a read-only connection before any pragma/DDL."""
        if not self.db_path.is_file() or self.db_path.stat().st_size == 0:
            return None
        uri = f"{self.db_path.resolve().as_uri()}?mode=ro"
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
        uri = f"{self.db_path.resolve().as_uri()}?mode=ro"
        conn = sqlite3.connect(uri, uri=True)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA recursive_triggers=ON")
            conn.execute("BEGIN")
            self._validate_structural_contract(conn, integrity_check=False)
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
        canonical = _canonical_json(normalized)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest(), normalized

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
            active_outbox = int(
                conn.execute(
                    "SELECT COUNT(*) FROM rca_outbox "
                    "WHERE status IN ('pending', 'claimed', 'shadow')"
                ).fetchone()[0]
            )
            current_ledger = int(
                conn.execute(
                    "SELECT COUNT(*) FROM rca_activation_admission_ledger "
                    "WHERE epoch_id = ?",
                    (identity,),
                ).fetchone()[0]
            )
            if pending_inbox or active_outbox or current_ledger:
                raise ActivationEpochError(
                    "activation_preproduction_effects_not_held"
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
                       ledger.generation, ledger.decision, ledger.bound_at
                  FROM rca_activation_epochs AS epoch
                  JOIN rca_activation_admission_ledger AS ledger
                    ON ledger.epoch_id = epoch.epoch_id
                   AND ledger.ledger_id = ?
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
        return {
            "epoch_id": str(row["epoch_id"]),
            "state": str(row["state"]),
            "ledger_id": int(row["ledger_id"]),
            "admission_key": str(row["admission_key"]),
            "business_key": str(row["business_key"]),
            "submission_key": str(row["submission_key"]),
            "generation": int(row["generation"]),
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
                """
                SELECT slot_kind, authorized_source_kind,
                       authorized_identity_sha256
                  FROM rca_activation_budget_slots
                 WHERE epoch_id = ? ORDER BY slot_kind
                """,
                (identity,),
            ).fetchall()
            if len(rows) != len(ACTIVATION_SLOT_KINDS) or {
                str(row["slot_kind"]) for row in rows
            } != set(ACTIVATION_SLOT_KINDS):
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
    def _validate_consumed_activation_executions_tx(
        conn: sqlite3.Connection,
        *,
        epoch: sqlite3.Row,
        end_fence_json: str,
    ) -> str:
        epoch_id = str(epoch["epoch_id"])
        rows = conn.execute(
            """
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
                   o.status AS outbox_status, o.source_topic,
                   o.source_partition, o.source_offset
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
             ORDER BY s.slot_kind
            """,
            (epoch_id,),
        ).fetchall()
        if len(rows) != len(ACTIVATION_SLOT_KINDS):
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
                or str(row["outbox_status"] or "") != "completed"
            ):
                raise ActivationEpochError("activation_bounded_execution_unbound")
        kafka = next(
            row for row in rows if str(row["slot_kind"]) == "kafka_success"
        )
        start_fence = json.loads(str(epoch["partition_start_fence_json"]))
        end_fence = json.loads(end_fence_json)
        topic = str(kafka["source_topic"] or "")
        partition = str(kafka["source_partition"])
        raw_offset = kafka["source_offset"]
        offset = int(raw_offset) if raw_offset is not None else -1
        if (
            topic not in start_fence
            or partition not in start_fence[topic]
            or topic not in end_fence
            or partition not in end_fence[topic]
            or offset < int(start_fence[topic][partition])
            or offset >= int(end_fence[topic][partition])
        ):
            raise ActivationEpochError("activation_kafka_canary_outside_end_fence")
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
                (epoch_id,),
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
            admitted_ledgers != len(ACTIVATION_SLOT_KINDS)
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
            "preauthorization_fingerprint": str(
                epoch["preauthorization_fingerprint"]
            ),
            "preauthorization_gate_receipt_sha256": str(
                epoch["preauthorization_gate_receipt_sha256"]
            ),
            "preauthorization_capsule_sha256": str(
                epoch["preauthorization_capsule_sha256"]
            ),
            "preproduction_fingerprint": str(
                epoch["preproduction_fingerprint"]
            ),
            "preproduction_gate_receipt_sha256": str(
                epoch["preproduction_gate_receipt_sha256"]
            ),
            "preproduction_capsule_sha256": str(
                epoch["preproduction_capsule_sha256"]
            ),
            "config_sha256": str(epoch["config_sha256"]),
            "db_logical_identity_sha256": str(
                epoch["db_logical_identity_sha256"]
            ),
            "bounded_activated_at": str(epoch["bounded_activated_at"]),
            "partition_start_fence_sha256": str(
                epoch["partition_start_fence_sha256"]
            ),
            "partition_start_fence": start_fence,
            "kafka_coordinate": {
                "topic": topic,
                "partition": int(partition),
                "offset": offset,
            },
            "slot_bindings": bindings,
            "unexpected_admissions": 0,
            "historical_blocked": 0,
            "historical_held": 0,
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
                        """
                        SELECT COUNT(*) FROM rca_activation_budget_slots
                         WHERE epoch_id = ?
                           AND authorized_source_kind IS NOT NULL
                           AND authorized_identity_sha256 IS NOT NULL
                        """,
                        (identity,),
                    ).fetchone()[0]
                )
                if authorized != len(ACTIVATION_SLOT_KINDS):
                    raise ActivationEpochError("activation_slots_not_preauthorized")
                fields.append("bounded_activated_at = ?")
                parameters.append(current)
            elif target == "confirmed":
                consumed = int(
                    conn.execute(
                        """
                        SELECT COUNT(*) FROM rca_activation_budget_slots
                         WHERE epoch_id = ? AND consumed_ledger_id IS NOT NULL
                        """,
                        (identity,),
                    ).fetchone()[0]
                )
                if consumed != len(ACTIVATION_SLOT_KINDS):
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
                effect_kind, target_key, target_json, required, status,
                created_at, updated_at
            ) VALUES (?, ?, ?, NULL, 'feishu_issue_comment', ?, ?, 1, 'pending', ?, ?)
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
                effect_kind, target_key, target_json, required, status,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, 'feishu_thread_reply', ?, ?, 1, 'pending', ?, ?)
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
               SET delivery_id = ?, catchup_requested_at = ?, updated_at = ?
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
                    snapshot = issue_snapshot_write_fence(
                        snapshot,
                        activation_epoch_id=str(activation_decision.epoch_id),
                        activation_ledger_id=int(activation_decision.ledger_id),
                        admission_key=admission_key,
                        target_set=target_set,
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
        allowed = {str(item or "").strip() for item in allowed_chat_ids}
        if not submit_enabled:
            raise ManualRcaAdmissionError("manual_intake_disabled")
        if not issue_only_operator and manual.chat_id not in allowed:
            raise ManualRcaAdmissionError("manual_intake_chat_not_allowed")
        operator_requested = manual.mode in {"rerun", "debug"}
        if (operator_requested or issue_only_operator) and not operator_authorized:
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
        payload_sha = _canonical_sha256(manual.to_dict())
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
                required_subscriptions = conn.execute(
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
                subscriptions_by_kind = {
                    str(row["effect_kind"]): str(row["subscription_key"])
                    for row in required_subscriptions
                }
                expected_kinds = {"feishu_issue_comment"}
                subscription_key = subscriptions_by_kind.get(
                    "feishu_issue_comment", ""
                )
                if not issue_only_operator:
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
                if not subscription_key or (
                    not issue_only_operator
                    and subscriptions_by_kind.get("feishu_thread_reply")
                    != subscription_key
                ):
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

            if operator_requested and not issue_only_operator:
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
            if not issue_only_operator:
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
                       SET status = 'quarantined', updated_at = ?
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
                f"AND {alias}.submission_key IN ({placeholders})))"
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
                        """
                        SELECT COUNT(*) FROM rca_activation_budget_slots
                         WHERE epoch_id = ? AND consumed_ledger_id IS NOT NULL
                        """,
                        (epoch_id,),
                    ).fetchone()[0]
                )
                completed_bound_slot_count = int(
                    conn.execute(
                        """
                        SELECT COUNT(*)
                          FROM rca_activation_budget_slots AS s
                          JOIN rca_activation_admission_ledger AS al
                            ON al.epoch_id = s.epoch_id
                           AND al.ledger_id = s.consumed_ledger_id
                           AND al.slot_kind = s.slot_kind
                           AND al.source_identity_sha256 =
                               s.authorized_identity_sha256
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
                           AND o.status = 'completed'
                         WHERE s.epoch_id = ?
                        """,
                        (epoch_id,),
                    ).fetchone()[0]
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
                if consumed_slot_count != len(ACTIVATION_SLOT_KINDS):
                    reason = "activation_slots_incomplete"
                elif completed_bound_slot_count != len(ACTIVATION_SLOT_KINDS):
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
                "required_slot_count": len(ACTIVATION_SLOT_KINDS),
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
            if current_epoch is not None:
                epoch_id = str(current_epoch["epoch_id"])
                activation_current = self._public_activation_epoch(current_epoch)
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
                bounded_canaries_completed_count = int(
                    conn.execute(
                        """
                        SELECT COUNT(*)
                          FROM rca_activation_budget_slots AS s
                          JOIN rca_activation_admission_ledger AS al
                            ON al.epoch_id = s.epoch_id
                           AND al.ledger_id = s.consumed_ledger_id
                           AND al.slot_kind = s.slot_kind
                           AND al.source_identity_sha256 =
                               s.authorized_identity_sha256
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
                           AND o.status = 'completed'
                         WHERE s.epoch_id = ?
                        """,
                        (epoch_id,),
                    ).fetchone()[0]
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
                        (epoch_id,),
                    ).fetchone()[0]
                )
                activation_backlog["historical_held"] = int(
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
                1 for slot in activation_slots.values() if slot["consumed"]
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
            elif consumed_slot_count != len(ACTIVATION_SLOT_KINDS):
                freeze_reason = "activation_slots_incomplete"
            elif bounded_canaries_completed_count != len(ACTIVATION_SLOT_KINDS):
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
                "required_slot_count": len(ACTIVATION_SLOT_KINDS),
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
                        == len(ACTIVATION_SLOT_KINDS)
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
