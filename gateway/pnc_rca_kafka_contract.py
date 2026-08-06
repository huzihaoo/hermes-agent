"""Fail-closed contract for Feishu workflow events that may trigger PNC RCA."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
import json
from typing import Any, Literal, Mapping

from gateway.pnc_rca_admission import RcaAdmission, build_rca_admission
from gateway.pnc_rca_business_profiles import resolve_business_profile


NORMALIZED_EVENT_SCHEMA_VERSION = "pnc_rca_workflow_event_v1"
MAX_WORKFLOW_EVENT_BYTES = 2 * 1024 * 1024
MAX_WORKFLOW_NODES = 100
MAX_TITLE_LENGTH = 2_000
MAX_IDENTITY_LENGTH = 256
BASE_WORK_ITEM_REQUIRED_FIELDS = frozenset({
    "id",
    "name",
    "project_key",
    "project_simple_name",
    "updated_at",
    "work_item_type_key",
})
WORKFLOW_REQUIRED_FIELDS = BASE_WORK_ITEM_REQUIRED_FIELDS | frozenset({
    "nodes",
    "status_change_type",
})
SNAPSHOT_REQUIRED_FIELDS = BASE_WORK_ITEM_REQUIRED_FIELDS | frozenset({
    "created_at",
    "fields",
    "pattern",
    "sub_stage",
    "work_item_status",
})
_WORKFLOW_MARKERS = frozenset({
    "created_at",
    "fields",
    "nodes",
    "pattern",
    "project_key",
    "status_change_type",
    "sub_stage",
    "work_item_status",
    "work_item_type_key",
})
_SNAPSHOT_MARKERS = frozenset({
    "created_at",
    "fields",
    "pattern",
    "sub_stage",
    "work_item_status",
})
MAX_SNAPSHOT_FIELDS = 1_000

Decision = Literal["accepted", "filtered", "invalid"]
StatusValue = str | int


def _required_text(value: Any, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field} must not be empty")
    return text


def _string_set(value: Any, field: str) -> frozenset[str]:
    if isinstance(value, str):
        items = value.split(",")
    else:
        items = value or ()
    result = frozenset(str(item).strip() for item in items if str(item).strip())
    if not result:
        raise ValueError(f"{field} must contain at least one exact value")
    return result


def _optional_string_set(value: Any) -> frozenset[str]:
    if isinstance(value, str):
        items = value.split(",")
    else:
        items = value or ()
    return frozenset(str(item).strip() for item in items if str(item).strip())


def _status_value(value: Any, field: str) -> StatusValue:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise ValueError(f"{field} must be a string or integer")
    if isinstance(value, str) and not value.strip():
        raise ValueError(f"{field} must not be empty")
    return value


@dataclass(frozen=True)
class WorkflowTransition:
    """One exact workflow-node transition allowed to create an RCA trigger."""

    state_key: str
    pre_status: StatusValue
    cur_status: StatusValue

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "state_key", _required_text(self.state_key, "state_key")
        )
        object.__setattr__(
            self, "pre_status", _status_value(self.pre_status, "pre_status")
        )
        object.__setattr__(
            self, "cur_status", _status_value(self.cur_status, "cur_status")
        )

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "WorkflowTransition":
        return cls(
            state_key=value.get("state_key", ""),
            pre_status=_status_value(value.get("pre_status"), "pre_status"),
            cur_status=_status_value(value.get("cur_status"), "cur_status"),
        )


@dataclass(frozen=True)
class WorkflowEventPolicy:
    """Versioned, fully explicit allowlist for creation-like workflow events."""

    topic: str
    policy_version: str
    project_keys: frozenset[str]
    project_simple_names: frozenset[str]
    work_item_type_keys: frozenset[str]
    status_change_types: frozenset[str] = frozenset()
    transitions: tuple[WorkflowTransition, ...] = ()
    snapshot_patterns: frozenset[str] = frozenset()
    snapshot_sub_stages: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        topic = _required_text(self.topic, "topic")
        if "," in topic or "\n" in topic or "\r" in topic:
            raise ValueError("topic must be one exact Kafka topic")
        object.__setattr__(self, "topic", topic)
        object.__setattr__(
            self,
            "policy_version",
            _required_text(self.policy_version, "policy_version"),
        )
        object.__setattr__(
            self, "project_keys", _string_set(self.project_keys, "project_keys")
        )
        object.__setattr__(
            self,
            "project_simple_names",
            _string_set(self.project_simple_names, "project_simple_names"),
        )
        object.__setattr__(
            self,
            "work_item_type_keys",
            _string_set(self.work_item_type_keys, "work_item_type_keys"),
        )
        object.__setattr__(
            self,
            "status_change_types",
            _optional_string_set(self.status_change_types),
        )
        transitions = tuple(self.transitions or ())
        if not all(isinstance(item, WorkflowTransition) for item in transitions):
            raise ValueError("transitions must contain WorkflowTransition values")
        object.__setattr__(self, "transitions", transitions)
        object.__setattr__(
            self,
            "snapshot_patterns",
            _optional_string_set(self.snapshot_patterns),
        )
        object.__setattr__(
            self,
            "snapshot_sub_stages",
            _optional_string_set(self.snapshot_sub_stages),
        )
        transition_mode = bool(self.status_change_types or transitions)
        if bool(self.status_change_types) != bool(transitions):
            raise ValueError(
                "status_change_types and transitions must be configured together"
            )
        snapshot_mode = bool(self.snapshot_patterns or self.snapshot_sub_stages)
        if bool(self.snapshot_patterns) != bool(self.snapshot_sub_stages):
            raise ValueError(
                "snapshot_patterns and snapshot_sub_stages must be configured together"
            )
        if not transition_mode and not snapshot_mode:
            raise ValueError("at least one exact creation policy mode is required")

    def to_dict(self) -> dict[str, Any]:
        return {
            "topic": self.topic,
            "policy_version": self.policy_version,
            "project_keys": sorted(self.project_keys),
            "project_simple_names": sorted(self.project_simple_names),
            "work_item_type_keys": sorted(self.work_item_type_keys),
            "status_change_types": sorted(self.status_change_types),
            "transitions": [asdict(item) for item in self.transitions],
            "snapshot_patterns": sorted(self.snapshot_patterns),
            "snapshot_sub_stages": sorted(self.snapshot_sub_stages),
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "WorkflowEventPolicy":
        transitions = value.get("transitions") or ()
        return cls(
            topic=value.get("topic", ""),
            policy_version=value.get("policy_version", ""),
            project_keys=_string_set(value.get("project_keys"), "project_keys"),
            project_simple_names=_string_set(
                value.get("project_simple_names"), "project_simple_names"
            ),
            work_item_type_keys=_string_set(
                value.get("work_item_type_keys"), "work_item_type_keys"
            ),
            status_change_types=_optional_string_set(
                value.get("status_change_types")
            ),
            transitions=tuple(
                item
                if isinstance(item, WorkflowTransition)
                else WorkflowTransition.from_mapping(item)
                for item in transitions
            ),
            snapshot_patterns=_optional_string_set(
                value.get("snapshot_patterns")
            ),
            snapshot_sub_stages=_optional_string_set(
                value.get("snapshot_sub_stages")
            ),
        )


@dataclass(frozen=True)
class NormalizedWorkflowNode:
    state_key: str
    pre_status: StatusValue
    cur_status: StatusValue
    node_name: str = ""


@dataclass(frozen=True)
class NormalizedWorkflowEvent:
    schema_version: str
    creation_rule_version: str
    work_item_id: str
    title: str
    project_key: str
    project_simple_name: str
    work_item_type_key: str
    status_change_type: str
    updated_at_ms: int
    issue_url: str
    nodes: tuple[NormalizedWorkflowNode, ...]
    matched_nodes: tuple[NormalizedWorkflowNode, ...]
    business_profile_observed: bool = False
    business_profile_resolution: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ClassificationResult:
    decision: Decision
    reason: str
    normalized: NormalizedWorkflowEvent | None = None


def decode_json_object(
    value: bytes | bytearray | memoryview | str | Mapping[str, Any],
) -> dict[str, Any]:
    """Decode a Kafka value while requiring a JSON object at the boundary."""
    if isinstance(value, Mapping):
        return {str(key): item for key, item in value.items()}
    if isinstance(value, (bytes, bytearray, memoryview)):
        if len(value) > MAX_WORKFLOW_EVENT_BYTES:
            raise ValueError("event_too_large")
        try:
            text = bytes(value).decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError("invalid_utf8") from exc
    elif isinstance(value, str):
        if len(value.encode("utf-8")) > MAX_WORKFLOW_EVENT_BYTES:
            raise ValueError("event_too_large")
        text = value
    else:
        raise ValueError("invalid_value_type")
    try:
        payload = json.loads(
            text,
            object_pairs_hook=_unique_json_object,
            parse_constant=_reject_json_constant,
        )
    except json.JSONDecodeError as exc:
        raise ValueError("invalid_json") from exc
    except RecursionError as exc:
        raise ValueError("json_nesting_too_deep") from exc
    if not isinstance(payload, dict):
        raise ValueError("invalid_json_object")
    return payload


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate_json_key")
        result[key] = value
    return result


def _reject_json_constant(_value: str) -> None:
    raise ValueError("invalid_json_constant")


def _work_item_id(value: Any) -> str:
    if isinstance(value, bool):
        raise ValueError("invalid_work_item_id")
    text = str(value).strip()
    if not text.isdigit() or len(text) > 32 or int(text) <= 0:
        raise ValueError("invalid_work_item_id")
    return text


def _timestamp_ms(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise ValueError("invalid_updated_at")
    text = str(value).strip()
    if not text.isdigit() or (len(text) > 1 and text.startswith("0")):
        raise ValueError("invalid_updated_at")
    timestamp = int(text)
    if timestamp <= 0 or timestamp > 9_223_372_036_854_775_807:
        raise ValueError("invalid_updated_at")
    return timestamp


def _normalize_node(value: Any) -> NormalizedWorkflowNode:
    if not isinstance(value, dict):
        raise ValueError("invalid_node")
    try:
        state_key = _required_text(value.get("state_key"), "state_key")
        node_name = str(value.get("node_name") or "").strip()
        if len(state_key) > MAX_IDENTITY_LENGTH or len(node_name) > MAX_TITLE_LENGTH:
            raise ValueError("node_field_too_long")
        return NormalizedWorkflowNode(
            state_key=state_key,
            pre_status=_status_value(value.get("pre_status"), "pre_status"),
            cur_status=_status_value(value.get("cur_status"), "cur_status"),
            node_name=node_name,
        )
    except ValueError as exc:
        raise ValueError("invalid_node") from exc


def _matches(node: NormalizedWorkflowNode, transition: WorkflowTransition) -> bool:
    return (
        node.state_key == transition.state_key
        and node.pre_status == transition.pre_status
        and node.cur_status == transition.cur_status
    )


def build_event_admission(
    event: NormalizedWorkflowEvent,
    *,
    topic: str,
    partition: int,
    offset: int,
) -> RcaAdmission:
    """Build the canonical source-neutral admission for one normalized event."""
    return build_rca_admission(
        project_key=event.project_key,
        project_simple_name=event.project_simple_name,
        work_item_type_key=event.work_item_type_key,
        work_item_id=event.work_item_id,
        rule_version=event.creation_rule_version,
        trigger_kind="issue_created",
        topic=topic,
        partition=partition,
        offset=offset,
    )


def classify_workflow_event(
    *,
    topic: str,
    value: bytes | bytearray | memoryview | str | Mapping[str, Any],
    policy: WorkflowEventPolicy,
) -> ClassificationResult:
    """Normalize and classify one message without performing any side effect."""
    if topic != policy.topic:
        return ClassificationResult("filtered", "topic_not_allowed")
    try:
        payload = decode_json_object(value)
    except ValueError as exc:
        return ClassificationResult("invalid", str(exc))

    snapshot_shape = len(_SNAPSHOT_MARKERS & payload.keys()) >= 2
    required_fields = SNAPSHOT_REQUIRED_FIELDS if snapshot_shape else WORKFLOW_REQUIRED_FIELDS
    missing = required_fields - payload.keys()
    if missing:
        if len(_WORKFLOW_MARKERS & payload.keys()) < 2:
            return ClassificationResult("filtered", "unsupported_message_shape")
        reason = (
            "missing_creation_snapshot_fields"
            if snapshot_shape
            else "missing_workflow_fields"
        )
        return ClassificationResult("invalid", reason)

    try:
        work_item_id = _work_item_id(payload.get("id"))
        title = _required_text(payload.get("name"), "name")
        project_key = _required_text(payload.get("project_key"), "project_key")
        project_simple_name = _required_text(
            payload.get("project_simple_name"), "project_simple_name"
        )
        work_item_type_key = _required_text(
            payload.get("work_item_type_key"), "work_item_type_key"
        )
        updated_at_ms = _timestamp_ms(payload.get("updated_at"))
        if len(title) > MAX_TITLE_LENGTH or any(
            len(value) > MAX_IDENTITY_LENGTH
            for value in (
                project_key,
                project_simple_name,
                work_item_type_key,
            )
        ):
            raise ValueError("workflow_field_too_long")
    except ValueError as exc:
        return ClassificationResult("invalid", str(exc))

    if project_key not in policy.project_keys:
        return ClassificationResult("filtered", "project_key_not_allowed")
    if project_simple_name not in policy.project_simple_names:
        return ClassificationResult("filtered", "project_simple_name_not_allowed")
    if work_item_type_key not in policy.work_item_type_keys:
        return ClassificationResult("filtered", "work_item_type_not_allowed")
    if snapshot_shape:
        try:
            pattern = _required_text(payload.get("pattern"), "pattern")
            sub_stage = _required_text(payload.get("sub_stage"), "sub_stage")
            _timestamp_ms(payload.get("created_at"))
            raw_fields = payload.get("fields")
            if not isinstance(raw_fields, list) or len(raw_fields) > MAX_SNAPSHOT_FIELDS:
                raise ValueError("invalid_snapshot_fields")
            if not isinstance(payload.get("work_item_status"), dict):
                raise ValueError("invalid_work_item_status")
            if "nodes" in payload or "status_change_type" in payload:
                raise ValueError("ambiguous_creation_snapshot")
            if any(
                len(value) > MAX_IDENTITY_LENGTH for value in (pattern, sub_stage)
            ):
                raise ValueError("snapshot_field_too_long")
        except ValueError as exc:
            return ClassificationResult("invalid", str(exc))
        if not policy.snapshot_patterns:
            return ClassificationResult("filtered", "creation_snapshot_not_allowed")
        if pattern not in policy.snapshot_patterns:
            return ClassificationResult("filtered", "snapshot_pattern_not_allowed")
        if sub_stage not in policy.snapshot_sub_stages:
            return ClassificationResult("filtered", "snapshot_sub_stage_not_allowed")
        profile_resolution = resolve_business_profile(
            project_key=project_key,
            work_item_type_key=work_item_type_key,
            work_item_brief=payload,
        )
        normalized = NormalizedWorkflowEvent(
            schema_version=NORMALIZED_EVENT_SCHEMA_VERSION,
            creation_rule_version=policy.policy_version,
            work_item_id=work_item_id,
            title=title,
            project_key=project_key,
            project_simple_name=project_simple_name,
            work_item_type_key=work_item_type_key,
            status_change_type=pattern,
            updated_at_ms=updated_at_ms,
            issue_url=(
                f"https://project.feishu.cn/{project_simple_name}/issue/detail/"
                f"{work_item_id}"
            ),
            nodes=(),
            matched_nodes=(),
            business_profile_observed=True,
            business_profile_resolution=profile_resolution.to_dict(),
        )
        return ClassificationResult(
            "accepted", "creation_snapshot_policy_matched", normalized
        )

    # A workflow transition must not silently discard snapshot fields.  A
    # hybrid envelope has two competing creation contracts and is rejected
    # before profile routing or admission identity is built.
    if _SNAPSHOT_MARKERS & payload.keys():
        return ClassificationResult("invalid", "ambiguous_creation_snapshot")

    try:
        status_change_type = _required_text(
            payload.get("status_change_type"), "status_change_type"
        )
        raw_nodes = payload.get("nodes")
        if (
            not isinstance(raw_nodes, list)
            or not raw_nodes
            or len(raw_nodes) > MAX_WORKFLOW_NODES
        ):
            raise ValueError("invalid_nodes")
        nodes = tuple(_normalize_node(item) for item in raw_nodes)
    except ValueError as exc:
        return ClassificationResult("invalid", str(exc))
    if status_change_type not in policy.status_change_types:
        return ClassificationResult("filtered", "status_change_type_not_allowed")

    matched = tuple(
        node
        for node in nodes
        if any(_matches(node, transition) for transition in policy.transitions)
    )
    if not matched:
        return ClassificationResult("filtered", "state_transition_not_allowed")

    normalized = NormalizedWorkflowEvent(
        schema_version=NORMALIZED_EVENT_SCHEMA_VERSION,
        creation_rule_version=policy.policy_version,
        work_item_id=work_item_id,
        title=title,
        project_key=project_key,
        project_simple_name=project_simple_name,
        work_item_type_key=work_item_type_key,
        status_change_type=status_change_type,
        updated_at_ms=updated_at_ms,
        issue_url=(
            f"https://project.feishu.cn/{project_simple_name}/issue/detail/{work_item_id}"
        ),
        nodes=nodes,
        matched_nodes=matched,
        business_profile_observed=False,
        business_profile_resolution=resolve_business_profile(
            project_key=project_key,
            work_item_type_key=work_item_type_key,
            work_item_brief={},
        ).to_dict(),
    )
    return ClassificationResult("accepted", "creation_policy_matched", normalized)
