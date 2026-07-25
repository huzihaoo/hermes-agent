"""Shared, strict environment loader for the RCA workflow creation policy."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
import re
from types import MappingProxyType
from typing import Any, Mapping

from gateway.pnc_rca_kafka_contract import (
    WorkflowEventPolicy,
    WorkflowTransition,
)


ENV_PREFIX = "HERMES_RCA_KAFKA_"
MAX_POLICY_JSON_NESTING = 32
W3_SNAPSHOT_AUTHORITY_SCHEMA_VERSION = "pnc_rca_w3_snapshot_authority_v1"
W3_SNAPSHOT_READ_MODE_ENV = "HERMES_RCA_W3_SNAPSHOT_READ_MODE"
W3_SNAPSHOT_READ_MODES = frozenset({"legacy", "snapshot_required"})
W3_SNAPSHOT_POLICY_NAMES = (
    "creation_policy",
    "business_profile",
    "execution_policy",
    "publication_policy",
    "correction_lineage_policy",
)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def _canonical_sha256(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        dict(value),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _freeze_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _freeze_json(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_json(item) for item in value)
    return value


def _thaw_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _thaw_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_thaw_json(item) for item in value]
    return value


@dataclass(frozen=True)
class W3SnapshotAuthority:
    """Externally pinned policy authority used by both W3 intake paths."""

    authority_sha256: str
    policies: Mapping[str, Mapping[str, Any]]

    def __post_init__(self) -> None:
        if _SHA256_RE.fullmatch(self.authority_sha256) is None:
            raise ValueError("W3 snapshot authority SHA-256 is invalid")
        if set(self.policies) != set(W3_SNAPSHOT_POLICY_NAMES):
            raise ValueError("W3 snapshot authority policies are incomplete")
        frozen = {
            name: _freeze_json(self.policies[name])
            for name in W3_SNAPSHOT_POLICY_NAMES
        }
        object.__setattr__(self, "policies", MappingProxyType(frozen))

    @property
    def policy_sha256s(self) -> dict[str, str]:
        return {
            name: str(self.policies[name]["sha256"])
            for name in W3_SNAPSHOT_POLICY_NAMES
        }

    def to_runtime_dict(self) -> dict[str, Any]:
        return {
            "schema_version": W3_SNAPSHOT_AUTHORITY_SCHEMA_VERSION,
            "authority_sha256": self.authority_sha256,
            "policies": {
                name: _thaw_json(self.policies[name])
                for name in W3_SNAPSHOT_POLICY_NAMES
            },
        }

    def to_public_dict(self) -> dict[str, Any]:
        return {
            "enabled": True,
            "schema_version": W3_SNAPSHOT_AUTHORITY_SCHEMA_VERSION,
            "authority_sha256": self.authority_sha256,
            "policy_sha256s": self.policy_sha256s,
        }


@dataclass(frozen=True)
class ManualRcaAdmissionRuntimeConfig:
    """One immutable policy snapshot shared by Gateway identity and admission."""

    active_policy: WorkflowEventPolicy
    outbox_high_watermark: int
    w3_snapshot_authority: W3SnapshotAuthority | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.active_policy, WorkflowEventPolicy):
            raise TypeError("active_policy must be WorkflowEventPolicy")
        if (
            isinstance(self.outbox_high_watermark, bool)
            or not isinstance(self.outbox_high_watermark, int)
            or self.outbox_high_watermark < 1
        ):
            raise ValueError("outbox_high_watermark must be a positive integer")
        if self.w3_snapshot_authority is not None and not isinstance(
            self.w3_snapshot_authority, W3SnapshotAuthority
        ):
            raise TypeError("w3_snapshot_authority must be a W3SnapshotAuthority")

    def to_public_dict(self) -> dict[str, Any]:
        policy = self.active_policy.to_dict()
        return {
            "workflow_event_policy": policy,
            "workflow_event_policy_sha256": _canonical_sha256(policy),
            "creation_rule_version": self.active_policy.policy_version,
            "kafka_outbox_high_watermark": self.outbox_high_watermark,
            "w3_snapshot_shadow": (
                self.w3_snapshot_authority.to_public_dict()
                if self.w3_snapshot_authority is not None
                else {"enabled": False}
            ),
        }


def _required(env: Mapping[str, str], name: str) -> str:
    value = str(env.get(name, "")).strip()
    if not value:
        raise ValueError(f"missing required environment variable: {name}")
    return value


def _csv(env: Mapping[str, str], name: str) -> frozenset[str]:
    values = frozenset(
        part.strip() for part in _required(env, name).split(",") if part.strip()
    )
    if not values:
        raise ValueError(f"{name} must contain at least one exact value")
    return values


def _optional_csv(env: Mapping[str, str], name: str) -> frozenset[str]:
    return frozenset(
        part.strip()
        for part in str(env.get(name, "")).split(",")
        if part.strip()
    )


def _strict_json(raw: str, name: str) -> Any:
    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate_json_key")
            result[key] = value
        return result

    def reject_constant(_value: str) -> None:
        raise ValueError("invalid_json_constant")

    try:
        value = json.loads(
            raw,
            object_pairs_hook=unique_object,
            parse_constant=reject_constant,
        )
    except (json.JSONDecodeError, RecursionError, ValueError) as exc:
        raise ValueError(f"{name} must be strict JSON") from exc
    stack: list[tuple[Any, int]] = [(value, 1)]
    while stack:
        item, depth = stack.pop()
        if depth > MAX_POLICY_JSON_NESTING:
            raise ValueError(f"{name} exceeds maximum JSON nesting")
        if isinstance(item, dict):
            stack.extend((child, depth + 1) for child in item.values())
        elif isinstance(item, list):
            stack.extend((child, depth + 1) for child in item)
    return value


def workflow_policy_from_env(
    env: Mapping[str, str] | None = None,
) -> WorkflowEventPolicy:
    """Build the exact policy consumed by Kafka and manual RCA admission."""
    source = os.environ if env is None else env
    transitions_name = f"{ENV_PREFIX}STATE_TRANSITIONS_JSON"
    raw_transitions = str(source.get(transitions_name, "[]")).strip() or "[]"
    transitions = _strict_json(raw_transitions, transitions_name)
    if not isinstance(transitions, list):
        raise ValueError(f"{transitions_name} must be a JSON array")
    if not all(isinstance(item, dict) for item in transitions):
        raise ValueError(f"{transitions_name} entries must be JSON objects")
    return WorkflowEventPolicy(
        topic=_required(source, f"{ENV_PREFIX}TOPIC"),
        policy_version=_required(source, f"{ENV_PREFIX}CREATION_RULE_VERSION"),
        project_keys=_csv(source, f"{ENV_PREFIX}PROJECT_KEYS"),
        project_simple_names=_csv(source, f"{ENV_PREFIX}PROJECT_SIMPLE_NAMES"),
        work_item_type_keys=_csv(source, f"{ENV_PREFIX}WORK_ITEM_TYPE_KEYS"),
        status_change_types=_optional_csv(
            source, f"{ENV_PREFIX}STATUS_CHANGE_TYPES"
        ),
        transitions=tuple(
            WorkflowTransition.from_mapping(item) for item in transitions
        ),
        snapshot_patterns=_optional_csv(
            source, f"{ENV_PREFIX}SNAPSHOT_PATTERNS"
        ),
        snapshot_sub_stages=_optional_csv(
            source, f"{ENV_PREFIX}SNAPSHOT_SUB_STAGES"
        ),
    )


def w3_snapshot_authority_from_env(
    env: Mapping[str, str] | None = None,
    *,
    active_policy: WorkflowEventPolicy,
) -> W3SnapshotAuthority | None:
    """Load a separately hashed W3 authority; the feature is off by default."""
    from gateway.pnc_rca_snapshot import (
        canonical_json_sha256,
        validate_snapshot_policy_authority,
    )

    source = os.environ if env is None else env
    enabled_name = "HERMES_RCA_W3_SNAPSHOT_SHADOW_ENABLED"
    enabled_raw = str(source.get(enabled_name, "false")).strip().lower()
    if enabled_raw not in {"true", "false"}:
        raise ValueError(f"{enabled_name} must be exactly true or false")
    if enabled_raw == "false":
        return None

    authority_name = "HERMES_RCA_W3_SNAPSHOT_AUTHORITY_JSON"
    authority_sha_name = "HERMES_RCA_W3_SNAPSHOT_AUTHORITY_SHA256"
    raw = _required(source, authority_name)
    expected_authority_sha256 = _required(source, authority_sha_name)
    if _SHA256_RE.fullmatch(expected_authority_sha256) is None:
        raise ValueError(f"{authority_sha_name} must be a lowercase SHA-256")
    value = _strict_json(raw, authority_name)
    if not isinstance(value, dict) or set(value) != {"schema_version", "policies"}:
        raise ValueError(f"{authority_name} has an invalid schema")
    if value.get("schema_version") != W3_SNAPSHOT_AUTHORITY_SCHEMA_VERSION:
        raise ValueError(f"{authority_name} has an invalid schema version")
    if canonical_json_sha256(value) != expected_authority_sha256:
        raise ValueError(f"{authority_name} does not match its pinned SHA-256")
    raw_policies = value.get("policies")
    if not isinstance(raw_policies, dict) or set(raw_policies) != set(
        W3_SNAPSHOT_POLICY_NAMES
    ):
        raise ValueError(f"{authority_name} must bind all W3 policies exactly")

    policies: dict[str, dict[str, Any]] = {}
    for name in W3_SNAPSHOT_POLICY_NAMES:
        policy = raw_policies[name]
        if not isinstance(policy, dict) or set(policy) != {"version", "sha256", "value"}:
            raise ValueError(f"{authority_name} has an invalid {name}")
        version = str(policy.get("version") or "").strip()
        policy_sha256 = str(policy.get("sha256") or "").strip()
        policy_value = policy.get("value")
        if (
            not version
            or _SHA256_RE.fullmatch(policy_sha256) is None
            or not isinstance(policy_value, dict)
            or not policy_value
            or policy_value.get("state") == "unbound"
        ):
            raise ValueError(f"{authority_name} has an invalid {name}")
        normalized = {
            "version": version,
            "sha256": policy_sha256,
            "value": dict(policy_value),
        }
        if canonical_json_sha256(
            {"version": version, "value": normalized["value"]}
        ) != policy_sha256:
            raise ValueError(f"{authority_name} has an invalid {name} digest")
        policies[name] = normalized

    try:
        policies = validate_snapshot_policy_authority(policies)
    except Exception as exc:
        raise ValueError(f"{authority_name} has an invalid policy projection") from exc

    creation_value = policies["creation_policy"]["value"]
    active_policy_sha256 = _canonical_sha256(active_policy.to_dict())
    if (
        creation_value.get("rule_version") != active_policy.policy_version
        or creation_value.get("workflow_event_policy_sha256")
        != active_policy_sha256
    ):
        raise ValueError(
            f"{authority_name} creation policy does not match the active workflow policy"
        )
    return W3SnapshotAuthority(
        authority_sha256=expected_authority_sha256,
        policies=policies,
    )


def w3_snapshot_read_config_from_env(
    env: Mapping[str, str] | None = None,
) -> tuple[str, W3SnapshotAuthority | None]:
    """Load the independent consumer cutover and its approved policy root."""
    source = os.environ if env is None else env
    mode = str(source.get(W3_SNAPSHOT_READ_MODE_ENV, "legacy")).strip()
    if mode not in W3_SNAPSHOT_READ_MODES:
        raise ValueError(
            f"{W3_SNAPSHOT_READ_MODE_ENV} must be exactly legacy or snapshot_required"
        )
    if mode == "legacy":
        return mode, None
    active_policy = workflow_policy_from_env(source)
    authority = w3_snapshot_authority_from_env(
        source,
        active_policy=active_policy,
    )
    if authority is None:
        raise ValueError(
            f"{W3_SNAPSHOT_READ_MODE_ENV}=snapshot_required requires the approved "
            "W3 snapshot authority"
        )
    return mode, authority


def manual_rca_admission_runtime_config_from_env(
    env: Mapping[str, str] | None = None,
) -> ManualRcaAdmissionRuntimeConfig:
    """Load the exact policy and outbox bound consumed by manual admission."""
    from gateway.pnc_rca_control_store import DEFAULT_OUTBOX_HIGH_WATERMARK

    source = os.environ if env is None else env
    high_watermark_name = f"{ENV_PREFIX}OUTBOX_HIGH_WATERMARK"
    raw_high_watermark = source.get(
        high_watermark_name,
        str(DEFAULT_OUTBOX_HIGH_WATERMARK),
    )
    try:
        outbox_high_watermark = int(str(raw_high_watermark).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"{high_watermark_name} must be a positive integer"
        ) from exc
    if outbox_high_watermark < 1:
        raise ValueError(
            f"{high_watermark_name} must be a positive integer"
        )
    active_policy = workflow_policy_from_env(source)
    return ManualRcaAdmissionRuntimeConfig(
        active_policy=active_policy,
        outbox_high_watermark=outbox_high_watermark,
        w3_snapshot_authority=w3_snapshot_authority_from_env(
            source,
            active_policy=active_policy,
        ),
    )
