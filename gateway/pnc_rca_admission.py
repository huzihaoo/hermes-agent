"""Pure admission contract for source-neutral G1Q3 RCA submissions.

Transport coordinates are retained for auditability, but they are deliberately
excluded from the business and submission keys.  Re-delivery of the same issue
from another Kafka offset therefore resolves to the same create-once key.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
from typing import Any, Literal, Mapping


RCA_ADMISSION_SCHEMA_VERSION_V1 = "pnc_rca_admission_v1"
RCA_ADMISSION_SCHEMA_VERSION = "pnc_rca_admission_v2"
RCA_ADMISSION_KEY_VERSION = "v1"
RCA_ISSUE_SCOPE_KEY_VERSION = "v1"
RCA_TRIGGER_CONTEXT_SCHEMA_VERSION = "pnc_rca_trigger_context_v1"

TriggerKind = Literal["issue_created", "manual_issue_request", "manual_retrigger"]


class RcaAdmissionError(ValueError):
    """Raised when an admission request is incomplete or internally inconsistent."""


@dataclass(frozen=True)
class RcaSourceRefs:
    """Source-neutral issue identity plus optional Kafka audit coordinates."""

    project_key: str
    project_simple_name: str
    work_item_type_key: str
    work_item_id: str
    rule_version: str
    topic: str = ""
    partition: int | None = None
    offset: int | None = None


@dataclass(frozen=True)
class RcaAdmission:
    """Deterministic create-once admission passed to a durable control store."""

    schema_version: str
    key_version: str
    trigger_kind: TriggerKind
    generation: int
    create_once: bool
    dedupe_scope: str
    business_key: str
    submission_key: str
    source_refs: RcaSourceRefs

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        if self.schema_version == RCA_ADMISSION_SCHEMA_VERSION_V1:
            payload["source_refs"].pop("project_simple_name", None)
        return payload


@dataclass(frozen=True)
class RcaTriggerContext:
    """Source-neutral issue identity consumed by the submission dispatcher."""

    schema_version: str
    source_kind: str
    creation_rule_version: str
    project_key: str
    project_simple_name: str
    work_item_type_key: str
    work_item_id: str
    issue_url: str
    title: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _required_text(name: str, value: Any) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise RcaAdmissionError(f"{name} is required")
    return normalized


def _optional_non_negative_int(name: str, value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise RcaAdmissionError(f"{name} must be a non-negative integer")
    return value


def _stable_key(prefix: str, material: Mapping[str, Any]) -> str:
    canonical = json.dumps(material, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return f"{prefix}-{hashlib.sha256(canonical.encode('utf-8')).hexdigest()}"


def build_rca_issue_scope_key(
    *,
    project_key: str,
    work_item_type_key: str,
    work_item_id: str,
) -> str:
    """Build the rule-neutral identity used to serialize one issue's generations."""
    return _stable_key(
        "g1q3-rca-issue-v1",
        {
            "key_version": RCA_ISSUE_SCOPE_KEY_VERSION,
            "project_key": _required_text("project_key", project_key),
            "work_item_type_key": _required_text(
                "work_item_type_key", work_item_type_key
            ),
            "work_item_id": _required_text("work_item_id", work_item_id),
        },
    )


def build_rca_admission(
    *,
    project_key: str,
    project_simple_name: str = "",
    work_item_type_key: str,
    work_item_id: str,
    rule_version: str,
    trigger_kind: TriggerKind = "issue_created",
    generation: int | None = None,
    topic: str = "",
    partition: int | None = None,
    offset: int | None = None,
    _schema_version: str = RCA_ADMISSION_SCHEMA_VERSION,
) -> RcaAdmission:
    """Build stable business/submission identities for one RCA generation.

    ``issue_created`` is always generation 1.  A deliberate rerun must use
    ``manual_retrigger`` and explicitly select a generation greater than 1.
    Kafka coordinates are all-or-none and never participate in deduplication.
    """

    project = _required_text("project_key", project_key)
    project_simple = str(project_simple_name or "").strip()
    item_type = _required_text("work_item_type_key", work_item_type_key)
    item_id = _required_text("work_item_id", work_item_id)
    rule = _required_text("rule_version", rule_version)

    normalized_trigger = str(trigger_kind or "").strip()
    if normalized_trigger not in {
        "issue_created",
        "manual_issue_request",
        "manual_retrigger",
    }:
        raise RcaAdmissionError(f"unsupported trigger_kind: {normalized_trigger!r}")
    if normalized_trigger in {"issue_created", "manual_issue_request"}:
        if generation is not None and (isinstance(generation, bool) or not isinstance(generation, int) or generation != 1):
            raise RcaAdmissionError(f"{normalized_trigger} is fixed at generation 1")
        normalized_generation = 1
    else:
        if isinstance(generation, bool) or not isinstance(generation, int) or generation < 2:
            raise RcaAdmissionError("manual_retrigger requires an explicit generation >= 2")
        normalized_generation = generation

    normalized_topic = str(topic or "").strip()
    normalized_partition = _optional_non_negative_int("partition", partition)
    normalized_offset = _optional_non_negative_int("offset", offset)
    transport_parts = (bool(normalized_topic), normalized_partition is not None, normalized_offset is not None)
    if any(transport_parts) and not all(transport_parts):
        raise RcaAdmissionError("topic, partition, and offset must be provided together")

    refs = RcaSourceRefs(
        project_key=project,
        project_simple_name=project_simple,
        work_item_type_key=item_type,
        work_item_id=item_id,
        rule_version=rule,
        topic=normalized_topic,
        partition=normalized_partition,
        offset=normalized_offset,
    )
    business_key = _stable_key(
        "g1q3-rca-b1",
        {
            "key_version": RCA_ADMISSION_KEY_VERSION,
            "project_key": project,
            "work_item_type_key": item_type,
            "work_item_id": item_id,
            "rule_version": rule,
        },
    )
    submission_key = _stable_key(
        "g1q3-rca-s1",
        {
            "key_version": RCA_ADMISSION_KEY_VERSION,
            "business_key": business_key,
            "generation": normalized_generation,
        },
    )
    return RcaAdmission(
        schema_version=_schema_version,
        key_version=RCA_ADMISSION_KEY_VERSION,
        trigger_kind=normalized_trigger,  # type: ignore[arg-type]
        generation=normalized_generation,
        create_once=True,
        dedupe_scope="submission_key",
        business_key=business_key,
        submission_key=submission_key,
        source_refs=refs,
    )


def validate_rca_admission(value: RcaAdmission | Mapping[str, Any]) -> RcaAdmission:
    """Validate and re-derive an admission so forged keys fail closed."""

    if isinstance(value, RcaAdmission):
        payload = value.to_dict()
    elif isinstance(value, Mapping):
        payload = dict(value)
    else:
        raise RcaAdmissionError("admission must be an RcaAdmission or mapping")

    schema_version = str(payload.get("schema_version") or "")
    if schema_version not in {
        RCA_ADMISSION_SCHEMA_VERSION_V1,
        RCA_ADMISSION_SCHEMA_VERSION,
    }:
        raise RcaAdmissionError("unsupported admission schema_version")
    if payload.get("key_version") != RCA_ADMISSION_KEY_VERSION:
        raise RcaAdmissionError("unsupported admission key_version")
    if payload.get("create_once") is not True or payload.get("dedupe_scope") != "submission_key":
        raise RcaAdmissionError("admission must use create-once submission_key semantics")
    refs = payload.get("source_refs")
    if not isinstance(refs, Mapping):
        raise RcaAdmissionError("source_refs must be an object")

    expected = build_rca_admission(
        project_key=refs.get("project_key", ""),
        project_simple_name=refs.get("project_simple_name", ""),
        work_item_type_key=refs.get("work_item_type_key", ""),
        work_item_id=refs.get("work_item_id", ""),
        rule_version=refs.get("rule_version", ""),
        trigger_kind=payload.get("trigger_kind", ""),  # type: ignore[arg-type]
        generation=payload.get("generation"),
        topic=refs.get("topic", ""),
        partition=refs.get("partition"),
        offset=refs.get("offset"),
        _schema_version=schema_version,
    )
    if payload != expected.to_dict():
        raise RcaAdmissionError("admission payload does not match its derived keys or contract")
    return expected


def build_rca_trigger_context(
    *,
    source_kind: str,
    project_key: str,
    project_simple_name: str,
    work_item_type_key: str,
    work_item_id: str,
    rule_version: str,
    issue_url: str,
    title: str = "",
) -> RcaTriggerContext:
    source = _required_text("source_kind", source_kind)
    if source not in {"kafka_workflow_event", "feishu_group_manual"}:
        raise RcaAdmissionError(f"unsupported source_kind: {source!r}")
    project = _required_text("project_key", project_key)
    project_simple = _required_text("project_simple_name", project_simple_name)
    item_type = _required_text("work_item_type_key", work_item_type_key)
    item_id = _required_text("work_item_id", work_item_id)
    rule = _required_text("rule_version", rule_version)
    url = _required_text("issue_url", issue_url)
    expected_url = f"https://project.feishu.cn/{project_simple}/issue/detail/{item_id}"
    if url.rstrip("/") != expected_url:
        raise RcaAdmissionError("issue_url does not match trigger identity")
    normalized_title = str(title or "").strip()
    if len(normalized_title) > 500:
        raise RcaAdmissionError("title is too long")
    return RcaTriggerContext(
        schema_version=RCA_TRIGGER_CONTEXT_SCHEMA_VERSION,
        source_kind=source,
        creation_rule_version=rule,
        project_key=project,
        project_simple_name=project_simple,
        work_item_type_key=item_type,
        work_item_id=item_id,
        issue_url=expected_url,
        title=normalized_title,
    )


def validate_rca_trigger_context(
    value: RcaTriggerContext | Mapping[str, Any],
) -> RcaTriggerContext:
    payload = value.to_dict() if isinstance(value, RcaTriggerContext) else dict(value)
    if payload.get("schema_version") != RCA_TRIGGER_CONTEXT_SCHEMA_VERSION:
        raise RcaAdmissionError("unsupported trigger context schema_version")
    expected = build_rca_trigger_context(
        source_kind=payload.get("source_kind", ""),
        project_key=payload.get("project_key", ""),
        project_simple_name=payload.get("project_simple_name", ""),
        work_item_type_key=payload.get("work_item_type_key", ""),
        work_item_id=payload.get("work_item_id", ""),
        rule_version=payload.get("creation_rule_version", ""),
        issue_url=payload.get("issue_url", ""),
        title=payload.get("title", ""),
    )
    if payload != expected.to_dict():
        raise RcaAdmissionError("trigger context does not match its canonical identity")
    return expected
