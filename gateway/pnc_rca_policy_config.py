"""Shared, strict environment loader for the RCA workflow creation policy."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from typing import Any, Mapping

from gateway.pnc_rca_kafka_contract import (
    WorkflowEventPolicy,
    WorkflowTransition,
)


ENV_PREFIX = "HERMES_RCA_KAFKA_"
MAX_POLICY_JSON_NESTING = 32


def _canonical_sha256(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        dict(value),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class ManualRcaAdmissionRuntimeConfig:
    """One immutable policy snapshot shared by Gateway identity and admission."""

    active_policy: WorkflowEventPolicy
    outbox_high_watermark: int

    def __post_init__(self) -> None:
        if not isinstance(self.active_policy, WorkflowEventPolicy):
            raise TypeError("active_policy must be WorkflowEventPolicy")
        if (
            isinstance(self.outbox_high_watermark, bool)
            or not isinstance(self.outbox_high_watermark, int)
            or self.outbox_high_watermark < 1
        ):
            raise ValueError("outbox_high_watermark must be a positive integer")

    def to_public_dict(self) -> dict[str, Any]:
        policy = self.active_policy.to_dict()
        return {
            "workflow_event_policy": policy,
            "workflow_event_policy_sha256": _canonical_sha256(policy),
            "creation_rule_version": self.active_policy.policy_version,
            "kafka_outbox_high_watermark": self.outbox_high_watermark,
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
    transitions = _strict_json(_required(source, transitions_name), transitions_name)
    if not isinstance(transitions, list) or not transitions:
        raise ValueError(f"{transitions_name} must be a non-empty JSON array")
    if not all(isinstance(item, dict) for item in transitions):
        raise ValueError(f"{transitions_name} entries must be JSON objects")
    return WorkflowEventPolicy(
        topic=_required(source, f"{ENV_PREFIX}TOPIC"),
        policy_version=_required(source, f"{ENV_PREFIX}CREATION_RULE_VERSION"),
        project_keys=_csv(source, f"{ENV_PREFIX}PROJECT_KEYS"),
        project_simple_names=_csv(source, f"{ENV_PREFIX}PROJECT_SIMPLE_NAMES"),
        work_item_type_keys=_csv(source, f"{ENV_PREFIX}WORK_ITEM_TYPE_KEYS"),
        status_change_types=_csv(source, f"{ENV_PREFIX}STATUS_CHANGE_TYPES"),
        transitions=tuple(
            WorkflowTransition.from_mapping(item) for item in transitions
        ),
    )


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
    return ManualRcaAdmissionRuntimeConfig(
        active_policy=workflow_policy_from_env(source),
        outbox_high_watermark=outbox_high_watermark,
    )
