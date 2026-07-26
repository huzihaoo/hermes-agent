"""Strict source-neutral W3 request and admission snapshot contracts."""

from __future__ import annotations

from dataclasses import InitVar, dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
import re
from types import MappingProxyType
from typing import Any, Mapping

from gateway.pnc_rca_admission import (
    RCA_ADMISSION_KEY_VERSION,
    RcaAdmission,
    RcaAdmissionError,
    RcaTriggerContext,
    build_rca_admission,
    build_rca_trigger_context,
    validate_rca_admission,
    validate_rca_trigger_context,
)
from gateway.pnc_rca_requester_identity import validate_rca_requester


CANONICAL_RCA_REQUEST_SCHEMA_VERSION = "pnc_rca_canonical_request_v1"
ADMISSION_SNAPSHOT_SCHEMA_VERSION = "pnc_rca_admission_snapshot_v1"
SOURCE_ENVELOPE_SCHEMA_VERSION = "pnc_rca_snapshot_source_envelope_v1"
SOURCE_AUTHORITY_SCHEMA_VERSION = "pnc_rca_source_authority_receipt_v1"
EXECUTION_SNAPSHOT_BUNDLE_SCHEMA_VERSION = "pnc_rca_execution_snapshot_bundle_v1"

_SNAPSHOT_ID_PREFIX = "pnc-rca-snapshot-v1-"
_SOURCE_ENVELOPE_ID_PREFIX = "pnc-rca-source-envelope-v1-"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_RERUN_REASONS = frozenset({"explicit_user_rerun"})
_SOURCE_ENVELOPE_AUTHORITY = object()

UNISSUED_WRITE_FENCE: Mapping[str, Any] = MappingProxyType(
    {
        "schema_version": "pnc_rca_write_fence_slot_v1",
        "state": "unissued",
    }
)


def _unissued_write_fence() -> dict[str, str]:
    return dict(UNISSUED_WRITE_FENCE)

_REQUEST_FIELDS = frozenset(
    {
        "schema_version",
        "ticket",
        "execution_intent",
        "creation_policy",
        "business_profile",
        "execution_policy",
        "publication_policy",
        "correction_lineage_policy",
    }
)
_TICKET_FIELDS = frozenset(
    {
        "project_key",
        "project_simple_name",
        "work_item_type_key",
        "work_item_id",
        "issue_url",
        "title",
        "title_sha256",
    }
)
_INTENT_FIELDS = frozenset(
    {"kind", "generation_reason", "generation_authorization_evidence_sha256"}
)
_POLICY_FIELDS = frozenset({"version", "sha256", "value"})
_EXECUTION_REQUEST_POLICY_VALUE_FIELDS = frozenset(
    {
        "request_schema",
        "data_access_mode",
        "allow_download",
        "input_materialization",
        "derived_artifacts_allowed",
        "allow_feishu_writeback",
        "group_response_cap",
        "translate_baseline",
        "translate_contract_path",
    }
)
_POLICY_NAMES = (
    "creation_policy",
    "business_profile",
    "execution_policy",
    "publication_policy",
    "correction_lineage_policy",
)
_EXPECTED_POLICY_SHA256_FIELDS = frozenset(_POLICY_NAMES)
_SNAPSHOT_FIELDS = frozenset(
    {
        "schema_version",
        "snapshot_id",
        "snapshot_sha256",
        "request_sha256",
        "canonical_request",
        "resolved_admission",
        "execution_admission",
        "write_fence",
    }
)
_RESOLVED_ADMISSION_FIELDS = frozenset(
    {
        "key_version",
        "creation_rule_version",
        "business_key",
        "submission_key",
        "generation",
        "create_once",
        "dedupe_scope",
    }
)
_EXECUTION_ADMISSION_FIELDS = frozenset(
    {
        "activation_epoch_id",
        "activation_ledger_id",
        "decision",
        "reason",
        "state",
        "legacy_unconfigured",
    }
)
_SOURCE_ENVELOPE_FIELDS = frozenset(
    {
        "schema_version",
        "source_envelope_id",
        "source_envelope_sha256",
        "source_authority_sha256",
        "snapshot_id",
        "snapshot_sha256",
        "submission_key",
        "source_id",
        "source_kind",
        "ingress_decision",
        "source_metadata",
        "anchor",
    }
)
_SOURCE_AUTHORITY_FIELDS = frozenset(
    {
        "schema_version",
        "authority_sha256",
        "source_id",
        "source_kind",
        "source_metadata_sha256",
        "anchor_sha256",
        "ingress_decision_sha256",
    }
)
_EXECUTION_SNAPSHOT_BUNDLE_FIELDS = frozenset(
    {
        "schema_version",
        "bundle_sha256",
        "snapshot_authority_sha256",
        "snapshot",
        "creator_source_envelope",
        "creator_source_authority",
    }
)
_INGRESS_DECISION_FIELDS = frozenset(
    {
        "requested_mode",
        "binding_action",
        "decision",
        "authorization_evidence_sha256",
    }
)
_ANCHOR_FIELDS = frozenset({"issue_target", "thread_target"})
_KAFKA_METADATA_FIELDS = frozenset(
    {
        "source_kind",
        "event_uid",
        "topic",
        "partition",
        "offset",
        "payload_sha256",
        "observed_at",
    }
)
_MANUAL_METADATA_FIELDS = frozenset(
    {
        "source_kind",
        "platform",
        "chat_id",
        "thread_id",
        "message_id",
        "requester_id",
        "mode",
        "payload_sha256",
        "observed_at",
    }
)
_PROJECTION_FIELDS = frozenset({"snapshot_core", "source_metadata", "anchor"})
_ACTIVATION_STATES = frozenset(
    {
        "legacy_unconfigured",
        "unconfigured",
        "safe_off",
        "preauthorized",
        "bounded_active",
        "confirmed",
        "steady_active",
        "aborted",
    }
)
_PROJECTION_SOURCE_FIELDS = frozenset(
    {
        "schema_version",
        "source_envelope_id",
        "source_envelope_sha256",
        "source_authority_sha256",
        "snapshot_id",
        "snapshot_sha256",
        "submission_key",
        "source_id",
        "source_kind",
        "ingress_decision",
        "transport",
    }
)


def _thaw(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _thaw(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw(item) for item in value]
    return value


def _freeze(value: Any, *, path: str = "$") -> Any:
    if isinstance(value, Mapping):
        frozen: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise RcaAdmissionError(f"w3_non_string_key:{path}")
            frozen[key] = _freeze(item, path=f"{path}.{key}")
        return MappingProxyType(frozen)
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item, path=f"{path}[]") for item in value)
    if isinstance(value, bool) or value is None or isinstance(value, str):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise RcaAdmissionError(f"w3_non_finite_number:{path}")
        return value
    raise RcaAdmissionError(f"w3_unsupported_value:{path}")


def canonical_json_bytes(value: Any) -> bytes:
    """Return canonical finite JSON bytes without coercing keys or values."""
    try:
        return json.dumps(
            _thaw(_freeze(value)),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except RcaAdmissionError:
        raise
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise RcaAdmissionError("w3_contract_not_canonical") from exc


def canonical_json_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def canonical_ticket_title_sha256(title: str) -> str:
    normalized = _text("ticket_title", title)
    return canonical_json_sha256({"title": normalized})


def strict_canonical_json_loads(raw: str) -> Any:
    """Parse exact canonical JSON and reject duplicate keys or alternate encodings."""
    if not isinstance(raw, str) or raw.startswith("\ufeff"):
        raise RcaAdmissionError("w3_json_invalid")

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, item in pairs:
            if key in result:
                raise RcaAdmissionError("w3_duplicate_json_key")
            result[key] = item
        return result

    def reject_constant(value: str) -> Any:
        raise RcaAdmissionError(f"w3_non_finite_number:{value}")

    try:
        parsed = json.loads(
            raw,
            object_pairs_hook=unique_object,
            parse_constant=reject_constant,
        )
    except RcaAdmissionError:
        raise
    except (TypeError, ValueError, RecursionError) as exc:
        raise RcaAdmissionError("w3_json_invalid") from exc
    if canonical_json_bytes(parsed) != raw.encode("utf-8"):
        raise RcaAdmissionError("w3_json_not_canonical")
    return parsed


def _exact_mapping(
    name: str,
    value: Any,
    fields: frozenset[str],
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != fields:
        raise RcaAdmissionError(f"w3_{name}_exact_fields_invalid")
    return value


def _text(name: str, value: Any, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str):
        raise RcaAdmissionError(f"w3_{name}_type_invalid")
    normalized = value.strip()
    if not normalized and not allow_empty:
        raise RcaAdmissionError(f"w3_{name}_required")
    return normalized


def _sha256(name: str, value: Any) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise RcaAdmissionError(f"w3_{name}_sha256_invalid")
    return value


def _observed_at(value: Any) -> str:
    observed = _text("source_observed_at", value)
    try:
        parsed = datetime.fromisoformat(observed.replace("Z", "+00:00"))
    except ValueError as exc:
        raise RcaAdmissionError("w3_source_observed_at_invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise RcaAdmissionError("w3_source_observed_at_timezone_missing")
    return parsed.astimezone(timezone.utc).isoformat()


def _policy(name: str, value: Any) -> dict[str, Any]:
    if value is None:
        raise RcaAdmissionError(f"w3_{name}_unbound")
    envelope = _exact_mapping(name, value, _POLICY_FIELDS)
    version = _text(f"{name}_version", envelope.get("version"))
    observed = envelope.get("value")
    if not isinstance(observed, Mapping):
        raise RcaAdmissionError(f"w3_{name}_value_invalid")
    digest = _sha256(name, envelope.get("sha256"))
    expected = canonical_json_sha256({"version": version, "value": observed})
    if digest != expected:
        raise RcaAdmissionError(f"w3_{name}_digest_mismatch")
    normalized = {
        "version": version,
        "sha256": digest,
        "value": _thaw(_freeze(observed)),
    }
    if name == "creation_policy":
        _creation_rule(normalized)
    elif observed.get("state") != "unbound" and name == "business_profile":
        _business_profile_value(normalized["value"])
    elif observed.get("state") != "unbound" and name == "execution_policy":
        _execution_request_policy_value(normalized["value"])
    return normalized


def _business_profile_value(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise RcaAdmissionError("w3_business_profile_value_invalid")
    profile = _thaw(_freeze(value))
    required = (
        "profile_id",
        "artifact_kind",
        "artifact_namespace",
    )
    if (
        profile.get("status") != "matched"
        or profile.get("execution_readiness") != "ready"
        or profile.get("resource_class") != "rca_prod"
        or any(not _text(f"business_profile_{key}", profile.get(key)) for key in required)
    ):
        raise RcaAdmissionError("w3_business_profile_not_execution_ready")
    return profile


def _execution_request_policy_value(value: Any) -> dict[str, Any]:
    policy = _exact_mapping(
        "execution_request_policy_value",
        value,
        _EXECUTION_REQUEST_POLICY_VALUE_FIELDS,
    )
    normalized = {
        "request_schema": _text(
            "execution_request_schema", policy.get("request_schema")
        ),
        "data_access_mode": _text(
            "execution_data_access_mode", policy.get("data_access_mode")
        ),
        "allow_download": policy.get("allow_download"),
        "input_materialization": _text(
            "execution_input_materialization",
            policy.get("input_materialization"),
        ),
        "derived_artifacts_allowed": policy.get("derived_artifacts_allowed"),
        "allow_feishu_writeback": policy.get("allow_feishu_writeback"),
        "group_response_cap": _text(
            "execution_group_response_cap", policy.get("group_response_cap")
        ),
        "translate_baseline": _text(
            "execution_translate_baseline", policy.get("translate_baseline")
        ),
        "translate_contract_path": _text(
            "execution_translate_contract_path",
            policy.get("translate_contract_path"),
            allow_empty=True,
        ),
    }
    if (
        normalized["request_schema"] != "g1q3_rca_execution_request_v2"
        or normalized["data_access_mode"] != "remote_read"
        or normalized["allow_download"] is not False
        or normalized["input_materialization"] != "forbidden"
        or normalized["derived_artifacts_allowed"] is not True
        or normalized["allow_feishu_writeback"] is not False
        or normalized["group_response_cap"] not in {"L0", "L1"}
    ):
        raise RcaAdmissionError("w3_execution_request_policy_invalid")
    if normalized != dict(policy):
        raise RcaAdmissionError("w3_execution_request_policy_not_canonical")
    return normalized


def _ticket(value: Any) -> dict[str, Any]:
    ticket = _exact_mapping("ticket", value, _TICKET_FIELDS)
    title = _text("ticket_title", ticket.get("title"))
    title_sha256 = _sha256("ticket_title", ticket.get("title_sha256"))
    if title_sha256 != canonical_ticket_title_sha256(title):
        raise RcaAdmissionError("w3_ticket_title_hash_mismatch")
    result = {
        "project_key": _text("ticket_project_key", ticket.get("project_key")),
        "project_simple_name": _text(
            "ticket_project_simple_name", ticket.get("project_simple_name")
        ),
        "work_item_type_key": _text(
            "ticket_work_item_type_key", ticket.get("work_item_type_key")
        ),
        "work_item_id": _text("ticket_work_item_id", ticket.get("work_item_id")),
        "issue_url": _text("ticket_issue_url", ticket.get("issue_url")),
        "title": title,
        "title_sha256": title_sha256,
    }
    expected_url = (
        f"https://project.feishu.cn/{result['project_simple_name']}/issue/detail/"
        f"{result['work_item_id']}"
    )
    if result["issue_url"].rstrip("/") != expected_url:
        raise RcaAdmissionError("w3_ticket_issue_url_invalid")
    if len(result["title"]) > 500:
        raise RcaAdmissionError("w3_ticket_title_invalid")
    result["issue_url"] = expected_url
    return result


def _execution_intent(value: Any) -> dict[str, Any]:
    intent = _exact_mapping("execution_intent", value, _INTENT_FIELDS)
    kind = _text("intent_kind", intent.get("kind"))
    reason = _text("generation_reason", intent.get("generation_reason"))
    if kind != "analyze_ticket" or reason not in ({"initial"} | set(_RERUN_REASONS)):
        raise RcaAdmissionError("w3_execution_intent_invalid")
    evidence = intent.get("generation_authorization_evidence_sha256")
    if reason == "initial":
        if evidence is not None:
            raise RcaAdmissionError("w3_initial_generation_authorization_forbidden")
        normalized_evidence = None
    else:
        normalized_evidence = _sha256("generation_authorization_evidence", evidence)
        if normalized_evidence == "0" * 64:
            raise RcaAdmissionError("w3_generation_authorization_evidence_unbound")
    return {
        "kind": kind,
        "generation_reason": reason,
        "generation_authorization_evidence_sha256": normalized_evidence,
    }


def _validate_expected_generation_authorization(
    execution_intent: Mapping[str, Any],
    expected_generation_authorization_evidence_sha256: str | None,
) -> None:
    observed = execution_intent["generation_authorization_evidence_sha256"]
    if execution_intent["generation_reason"] == "initial":
        if expected_generation_authorization_evidence_sha256 is not None:
            raise RcaAdmissionError("w3_initial_generation_authorization_forbidden")
        return
    expected = _sha256(
        "expected_generation_authorization_evidence",
        expected_generation_authorization_evidence_sha256,
    )
    if expected == "0" * 64:
        raise RcaAdmissionError(
            "w3_expected_generation_authorization_evidence_unbound"
        )
    if observed != expected:
        raise RcaAdmissionError("w3_generation_authorization_evidence_mismatch")


def _validate_expected_ticket_title(
    ticket: Mapping[str, Any],
    expected_ticket_title_sha256: str,
) -> None:
    expected = _sha256("expected_ticket_title", expected_ticket_title_sha256)
    if ticket["title_sha256"] != expected:
        raise RcaAdmissionError("w3_ticket_title_authority_mismatch")


def _resolved_admission(value: Any) -> dict[str, Any]:
    admission = _exact_mapping(
        "resolved_admission", value, _RESOLVED_ADMISSION_FIELDS
    )
    generation = admission.get("generation")
    if isinstance(generation, bool) or not isinstance(generation, int) or generation < 1:
        raise RcaAdmissionError("w3_resolved_admission_generation_invalid")
    if (
        admission.get("create_once") is not True
        or admission.get("dedupe_scope") != "submission_key"
    ):
        raise RcaAdmissionError("w3_resolved_admission_semantics_invalid")
    return {
        "key_version": _text(
            "resolved_key_version", admission.get("key_version")
        ),
        "creation_rule_version": _text(
            "resolved_creation_rule_version",
            admission.get("creation_rule_version"),
        ),
        "business_key": _text(
            "resolved_business_key", admission.get("business_key")
        ),
        "submission_key": _text(
            "resolved_submission_key", admission.get("submission_key")
        ),
        "generation": generation,
        "create_once": True,
        "dedupe_scope": "submission_key",
    }


def _resolved_admission_from_legacy(
    value: RcaAdmission | Mapping[str, Any],
) -> dict[str, Any]:
    admission = validate_rca_admission(value)
    return {
        "key_version": admission.key_version,
        "creation_rule_version": admission.source_refs.rule_version,
        "business_key": admission.business_key,
        "submission_key": admission.submission_key,
        "generation": admission.generation,
        "create_once": admission.create_once,
        "dedupe_scope": admission.dedupe_scope,
    }


def _execution_admission(value: Any) -> dict[str, Any]:
    admission = _exact_mapping(
        "execution_admission", value, _EXECUTION_ADMISSION_FIELDS
    )
    epoch_id = _text(
        "execution_epoch_id",
        admission.get("activation_epoch_id"),
        allow_empty=True,
    )
    ledger_id = admission.get("activation_ledger_id")
    if ledger_id is not None and (
        isinstance(ledger_id, bool)
        or not isinstance(ledger_id, int)
        or ledger_id < 1
    ):
        raise RcaAdmissionError("w3_execution_ledger_id_invalid")
    decision = _text("execution_decision", admission.get("decision"))
    if decision not in {"admit", "shadow"}:
        raise RcaAdmissionError("w3_execution_decision_invalid")
    state = _text("execution_state", admission.get("state"))
    if state not in _ACTIVATION_STATES:
        raise RcaAdmissionError("w3_execution_state_invalid")
    legacy = admission.get("legacy_unconfigured")
    reason = _text("execution_reason", admission.get("reason"))
    if not isinstance(legacy, bool):
        raise RcaAdmissionError("w3_execution_legacy_flag_invalid")
    if legacy != (state == "legacy_unconfigured"):
        raise RcaAdmissionError("w3_execution_legacy_state_invalid")
    if legacy:
        if epoch_id or ledger_id is not None:
            raise RcaAdmissionError("w3_execution_legacy_binding_invalid")
        if decision != "admit" or reason != "activation_legacy_unconfigured":
            raise RcaAdmissionError("w3_execution_legacy_decision_invalid")
    elif state == "unconfigured":
        if epoch_id or ledger_id is not None:
            raise RcaAdmissionError("w3_execution_unconfigured_binding_invalid")
        if decision != "shadow" or reason != "activation_epoch_held_unconfigured":
            raise RcaAdmissionError("w3_execution_unconfigured_decision_invalid")
    else:
        if not epoch_id or ledger_id is None:
            raise RcaAdmissionError("w3_execution_binding_missing")
        allowed_decisions = {
            "safe_off": {"shadow"},
            "preauthorized": {"shadow"},
            "bounded_active": {"admit", "shadow"},
            "confirmed": {"admit", "shadow"},
            "steady_active": {"admit"},
            "aborted": {"shadow"},
        }
        if decision not in allowed_decisions[state]:
            raise RcaAdmissionError("w3_execution_state_decision_mismatch")
        admit_reasons = {
            "bounded_active": {
                "activation_bounded_slot_consumed",
                "activation_admission_idempotent",
            },
            "confirmed": {
                "activation_confirmed_shadow_reconciliation",
                "activation_admission_idempotent",
            },
            "steady_active": {
                "activation_steady_active",
                "activation_admission_idempotent",
            },
        }
        if decision == "admit" and reason not in admit_reasons[state]:
            raise RcaAdmissionError("w3_execution_admit_reason_invalid")
        shadow_reasons = {
            "safe_off": {
                "activation_epoch_held_safe_off",
                "activation_epoch_held_ingress_safe_off",
            },
            "preauthorized": {
                "activation_epoch_held_preauthorized",
                "activation_epoch_held_ingress_preauthorized",
            },
            "bounded_active": {
                "activation_bounded_slot_required",
                "activation_bounded_identity_not_authorized",
                "activation_kafka_partition_not_fenced",
                "activation_kafka_before_start_fence",
                "activation_kafka_at_or_after_end_fence",
                "activation_bounded_slot_consumed",
            },
            "confirmed": {
                "activation_epoch_held_confirmed",
                "activation_epoch_held_ingress_confirmed",
            },
            "aborted": {"activation_epoch_held_ingress_aborted"},
        }
        if decision == "shadow" and reason not in shadow_reasons[state]:
            raise RcaAdmissionError("w3_execution_shadow_reason_invalid")
    return {
        "activation_epoch_id": epoch_id,
        "activation_ledger_id": ledger_id,
        "decision": decision,
        "reason": reason,
        "state": state,
        "legacy_unconfigured": legacy,
    }


def _validate_legacy_identity(
    admission: RcaAdmission,
    context: RcaTriggerContext,
) -> str:
    refs = admission.source_refs
    expected = {
        "project_key": refs.project_key,
        "work_item_type_key": refs.work_item_type_key,
        "work_item_id": refs.work_item_id,
        "rule_version": refs.rule_version,
    }
    observed = {
        "project_key": context.project_key,
        "work_item_type_key": context.work_item_type_key,
        "work_item_id": context.work_item_id,
        "rule_version": context.creation_rule_version,
    }
    if expected != observed:
        raise RcaAdmissionError("w3_admission_context_identity_mismatch")
    if refs.project_simple_name and refs.project_simple_name != context.project_simple_name:
        raise RcaAdmissionError("w3_admission_context_project_mismatch")
    return refs.project_simple_name or context.project_simple_name


def _validate_request_against_legacy(
    request: CanonicalRcaRequest,
    admission: RcaAdmission,
) -> None:
    refs = admission.source_refs
    ticket = request.ticket
    expected_ticket = {
        "project_key": refs.project_key,
        "project_simple_name": refs.project_simple_name
        or str(ticket["project_simple_name"]),
        "work_item_type_key": refs.work_item_type_key,
        "work_item_id": refs.work_item_id,
    }
    if any(ticket[key] != value for key, value in expected_ticket.items()):
        raise RcaAdmissionError("w3_snapshot_request_admission_ticket_mismatch")
    reason = str(request.execution_intent["generation_reason"])
    if (admission.generation == 1) != (reason == "initial"):
        raise RcaAdmissionError("w3_snapshot_request_admission_intent_mismatch")
    creation_rule = _creation_rule(request.creation_policy)
    if creation_rule != refs.rule_version:
        raise RcaAdmissionError("w3_snapshot_creation_policy_admission_mismatch")


def _creation_rule(policy: Mapping[str, Any]) -> str:
    value = policy["value"]
    rule = value.get("rule")
    rule_version = value.get("rule_version")
    if rule is None and rule_version is None:
        raise RcaAdmissionError("w3_creation_policy_rule_unbound")
    normalized_rule = (
        _text("creation_policy_rule", rule) if rule is not None else None
    )
    normalized_version = (
        _text("creation_policy_rule_version", rule_version)
        if rule_version is not None
        else None
    )
    if (
        normalized_rule is not None
        and normalized_version is not None
        and normalized_rule != normalized_version
    ):
        raise RcaAdmissionError("w3_creation_policy_rule_conflict")
    return str(normalized_rule or normalized_version)


def _validate_resolved_identity(
    request: CanonicalRcaRequest,
    resolved: Mapping[str, Any],
) -> None:
    if resolved["key_version"] != RCA_ADMISSION_KEY_VERSION:
        raise RcaAdmissionError("w3_resolved_key_version_invalid")
    if resolved["creation_rule_version"] != _creation_rule(
        request.creation_policy
    ):
        raise RcaAdmissionError("w3_resolved_creation_policy_mismatch")
    generation = int(resolved["generation"])
    reason = str(request.execution_intent["generation_reason"])
    if (generation == 1) != (reason == "initial"):
        raise RcaAdmissionError("w3_resolved_generation_intent_mismatch")
    trigger_kind = "manual_issue_request" if generation == 1 else "manual_retrigger"
    expected = build_rca_admission(
        project_key=str(request.ticket["project_key"]),
        project_simple_name=str(request.ticket["project_simple_name"]),
        work_item_type_key=str(request.ticket["work_item_type_key"]),
        work_item_id=str(request.ticket["work_item_id"]),
        rule_version=str(resolved["creation_rule_version"]),
        trigger_kind=trigger_kind,
        generation=generation,
    )
    if (
        expected.business_key != resolved["business_key"]
        or expected.submission_key != resolved["submission_key"]
    ):
        raise RcaAdmissionError("w3_resolved_admission_identity_invalid")


@dataclass(frozen=True)
class CanonicalRcaRequest:
    schema_version: str
    ticket: Mapping[str, Any]
    execution_intent: Mapping[str, Any]
    creation_policy: Mapping[str, Any]
    business_profile: Mapping[str, Any]
    execution_policy: Mapping[str, Any]
    publication_policy: Mapping[str, Any]
    correction_lineage_policy: Mapping[str, Any]

    def __post_init__(self) -> None:
        if self.schema_version != CANONICAL_RCA_REQUEST_SCHEMA_VERSION:
            raise RcaAdmissionError("w3_request_schema_version_invalid")
        object.__setattr__(self, "ticket", _freeze(_ticket(self.ticket)))
        object.__setattr__(
            self,
            "execution_intent",
            _freeze(_execution_intent(self.execution_intent)),
        )
        for name in (
            "creation_policy",
            "business_profile",
            "execution_policy",
            "publication_policy",
            "correction_lineage_policy",
        ):
            object.__setattr__(self, name, _freeze(_policy(name, getattr(self, name))))

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "ticket": _thaw(self.ticket),
            "execution_intent": _thaw(self.execution_intent),
            "creation_policy": _thaw(self.creation_policy),
            "business_profile": _thaw(self.business_profile),
            "execution_policy": _thaw(self.execution_policy),
            "publication_policy": _thaw(self.publication_policy),
            "correction_lineage_policy": _thaw(self.correction_lineage_policy),
        }

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_json_bytes(self.to_dict())

    @property
    def request_sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> CanonicalRcaRequest:
        mapping = _exact_mapping("request", value, _REQUEST_FIELDS)
        return cls(**{field: mapping[field] for field in _REQUEST_FIELDS})


def _assert_policy_authority(request: CanonicalRcaRequest) -> None:
    for policy_name in _POLICY_NAMES:
        policy = getattr(request, policy_name)
        if not policy["value"] or policy["value"].get("state") == "unbound":
            raise RcaAdmissionError(f"w3_{policy_name}_not_switch_ready")


def validate_snapshot_policy_authority(
    value: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    """Validate the exact switch-ready policy set before intake starts."""
    policies = _exact_mapping(
        "snapshot_policy_authority",
        value,
        frozenset(_POLICY_NAMES),
    )
    normalized = {name: _policy(name, policies[name]) for name in _POLICY_NAMES}
    if any(
        not normalized[name]["value"]
        or normalized[name]["value"].get("state") == "unbound"
        for name in _POLICY_NAMES
    ):
        raise RcaAdmissionError("w3_snapshot_policy_authority_not_switch_ready")
    return normalized


def _validate_expected_policy_sha256s(
    request: CanonicalRcaRequest | Mapping[str, Any],
    expected_policy_sha256s: Mapping[str, Any],
) -> None:
    expected = _exact_mapping(
        "expected_policy_sha256s",
        expected_policy_sha256s,
        _EXPECTED_POLICY_SHA256_FIELDS,
    )
    for policy_name in _POLICY_NAMES:
        expected_sha256 = _sha256(
            f"expected_{policy_name}",
            expected[policy_name],
        )
        if expected_sha256 == "0" * 64:
            raise RcaAdmissionError(f"w3_expected_{policy_name}_unbound")
        observed_policy = (
            getattr(request, policy_name)
            if isinstance(request, CanonicalRcaRequest)
            else request[policy_name]
        )
        if observed_policy["sha256"] != expected_sha256:
            raise RcaAdmissionError(f"w3_{policy_name}_authority_mismatch")


def validate_canonical_rca_request(
    value: CanonicalRcaRequest | Mapping[str, Any],
) -> CanonicalRcaRequest:
    if not isinstance(value, (CanonicalRcaRequest, Mapping)):
        raise RcaAdmissionError("w3_request_type_invalid")
    original = value.to_dict() if isinstance(value, CanonicalRcaRequest) else dict(value)
    request = CanonicalRcaRequest.from_mapping(original)
    if request.to_dict() != original:
        raise RcaAdmissionError("w3_request_not_canonical")
    return request


def build_canonical_rca_request(
    *,
    admission: RcaAdmission | Mapping[str, Any],
    trigger_context: RcaTriggerContext | Mapping[str, Any],
    creation_policy: Mapping[str, Any] | None = None,
    business_profile: Mapping[str, Any] | None = None,
    execution_policy: Mapping[str, Any] | None = None,
    publication_policy: Mapping[str, Any] | None = None,
    correction_lineage_policy: Mapping[str, Any] | None = None,
    generation_reason: str | None = None,
    generation_authorization_evidence_sha256: str | None = None,
    expected_generation_authorization_evidence_sha256: str | None = None,
    expected_ticket_title_sha256: str,
    expected_policy_sha256s: Mapping[str, Any],
) -> CanonicalRcaRequest:
    legacy = validate_rca_admission(admission)
    context = validate_rca_trigger_context(trigger_context)
    project_simple_name = _validate_legacy_identity(legacy, context)
    if legacy.generation == 1:
        if generation_reason not in {None, "initial"}:
            raise RcaAdmissionError("w3_initial_generation_reason_invalid")
        if generation_authorization_evidence_sha256 is not None:
            raise RcaAdmissionError("w3_initial_generation_authorization_forbidden")
        if expected_generation_authorization_evidence_sha256 is not None:
            raise RcaAdmissionError("w3_initial_generation_authorization_forbidden")
        reason = "initial"
        generation_authorization = None
    else:
        if generation_reason is None:
            raise RcaAdmissionError("w3_rerun_generation_reason_unbound")
        reason = _text("rerun_generation_reason", generation_reason)
        if reason not in _RERUN_REASONS:
            raise RcaAdmissionError("w3_rerun_generation_reason_invalid")
        generation_authorization = _sha256(
            "generation_authorization_evidence",
            generation_authorization_evidence_sha256,
        )
        if generation_authorization == "0" * 64:
            raise RcaAdmissionError("w3_generation_authorization_evidence_unbound")
        expected_generation_authorization = _sha256(
            "expected_generation_authorization_evidence",
            expected_generation_authorization_evidence_sha256,
        )
        if expected_generation_authorization == "0" * 64:
            raise RcaAdmissionError(
                "w3_expected_generation_authorization_evidence_unbound"
            )
        if generation_authorization != expected_generation_authorization:
            raise RcaAdmissionError("w3_generation_authorization_evidence_mismatch")
    creation = _policy("creation_policy", creation_policy)
    creation_rule = _creation_rule(creation)
    if creation_rule != legacy.source_refs.rule_version:
        raise RcaAdmissionError("w3_creation_policy_admission_mismatch")
    title = _text("ticket_title", context.title)
    title_sha256 = canonical_ticket_title_sha256(title)
    expected_title_sha256 = _sha256(
        "expected_ticket_title",
        expected_ticket_title_sha256,
    )
    if title_sha256 != expected_title_sha256:
        raise RcaAdmissionError("w3_ticket_title_authority_mismatch")
    request = CanonicalRcaRequest(
        schema_version=CANONICAL_RCA_REQUEST_SCHEMA_VERSION,
        ticket={
            "project_key": legacy.source_refs.project_key,
            "project_simple_name": project_simple_name,
            "work_item_type_key": legacy.source_refs.work_item_type_key,
            "work_item_id": legacy.source_refs.work_item_id,
            "issue_url": context.issue_url,
            "title": title,
            "title_sha256": title_sha256,
        },
        execution_intent={
            "kind": "analyze_ticket",
            "generation_reason": reason,
            "generation_authorization_evidence_sha256": (
                generation_authorization
            ),
        },
        creation_policy=creation,
        business_profile=_policy("business_profile", business_profile),
        execution_policy=_policy("execution_policy", execution_policy),
        publication_policy=_policy("publication_policy", publication_policy),
        correction_lineage_policy=_policy(
            "correction_lineage_policy", correction_lineage_policy
        ),
    )
    _validate_expected_policy_sha256s(request, expected_policy_sha256s)
    return request


@dataclass(frozen=True)
class AdmissionSnapshot:
    schema_version: str
    snapshot_id: str
    snapshot_sha256: str
    request_sha256: str
    canonical_request: CanonicalRcaRequest
    resolved_admission: Mapping[str, Any]
    execution_admission: Mapping[str, Any]
    write_fence: Mapping[str, Any]

    def __post_init__(self) -> None:
        if self.schema_version != ADMISSION_SNAPSHOT_SCHEMA_VERSION:
            raise RcaAdmissionError("w3_snapshot_schema_version_invalid")
        request = validate_canonical_rca_request(self.canonical_request)
        object.__setattr__(self, "canonical_request", request)
        if self.request_sha256 != request.request_sha256:
            raise RcaAdmissionError("w3_snapshot_request_hash_mismatch")
        object.__setattr__(
            self,
            "resolved_admission",
            _freeze(_resolved_admission(self.resolved_admission)),
        )
        _validate_resolved_identity(request, self.resolved_admission)
        object.__setattr__(
            self,
            "execution_admission",
            _freeze(_execution_admission(self.execution_admission)),
        )
        observed_fence = _thaw(self.write_fence)
        if observed_fence == _unissued_write_fence():
            object.__setattr__(self, "write_fence", _freeze(_unissued_write_fence()))
        else:
            # W3 creates an unissued slot; W5 may replace it exactly once with
            # an immutable issued fence after activation admission.  Keep the
            # detailed schema/hash checks in the dedicated fence module.
            try:
                from gateway.pnc_rca_write_fence import validate_write_fence

                validate_write_fence(
                    observed_fence,
                    snapshot_core_sha256_value=self.snapshot_core_sha256,
                    # Snapshot construction validates immutable shape/hash only;
                    # live expiry and epoch checks belong to the provider fence
                    # boundary where the current clock is available.
                    now=datetime.fromisoformat(
                        str(observed_fence["issued_at"]).replace("Z", "+00:00")
                    ),
                )
            except Exception as exc:
                if isinstance(exc, RcaAdmissionError):
                    raise
                raise RcaAdmissionError(
                    getattr(exc, "code", "w3_write_fence_invalid")
                ) from exc
            object.__setattr__(self, "write_fence", _freeze(observed_fence))
        expected_sha = canonical_json_sha256(self.identity_payload)
        if self.snapshot_sha256 != expected_sha:
            raise RcaAdmissionError("w3_snapshot_hash_mismatch")
        if self.snapshot_id != f"{_SNAPSHOT_ID_PREFIX}{expected_sha}":
            raise RcaAdmissionError("w3_snapshot_id_mismatch")

    @property
    def identity_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "request_sha256": self.request_sha256,
            "canonical_request": self.canonical_request.to_dict(),
            "resolved_admission": _thaw(self.resolved_admission),
            "execution_admission": _thaw(self.execution_admission),
            "write_fence": _thaw(self.write_fence),
        }

    @property
    def core_payload(self) -> dict[str, Any]:
        """Snapshot identity excluding the mutable W5 issuance slot."""
        return {
            "schema_version": self.schema_version,
            "request_sha256": self.request_sha256,
            "canonical_request": self.canonical_request.to_dict(),
            "resolved_admission": _thaw(self.resolved_admission),
            "execution_admission": _thaw(self.execution_admission),
        }

    @property
    def snapshot_core_sha256(self) -> str:
        return canonical_json_sha256(self.core_payload)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "snapshot_id": self.snapshot_id,
            "snapshot_sha256": self.snapshot_sha256,
            **self.identity_payload,
        }


def build_admission_snapshot(
    *,
    request: CanonicalRcaRequest | Mapping[str, Any],
    admission: RcaAdmission | Mapping[str, Any],
    execution_admission: Mapping[str, Any] | None = None,
    expected_generation_authorization_evidence_sha256: str | None = None,
    allow_unbound_policies_for_shadow: bool = False,
    expected_ticket_title_sha256: str,
    expected_policy_sha256s: Mapping[str, Any],
) -> AdmissionSnapshot:
    canonical_request = validate_canonical_rca_request(request)
    _validate_expected_generation_authorization(
        canonical_request.execution_intent,
        expected_generation_authorization_evidence_sha256,
    )
    _validate_expected_ticket_title(
        canonical_request.ticket,
        expected_ticket_title_sha256,
    )
    _validate_expected_policy_sha256s(
        canonical_request,
        expected_policy_sha256s,
    )
    if not isinstance(allow_unbound_policies_for_shadow, bool):
        raise RcaAdmissionError("w3_shadow_policy_opt_in_invalid")
    if not allow_unbound_policies_for_shadow:
        _assert_policy_authority(canonical_request)
    legacy = validate_rca_admission(admission)
    _validate_request_against_legacy(canonical_request, legacy)
    resolved = _resolved_admission_from_legacy(legacy)
    if execution_admission is None:
        raise RcaAdmissionError("w3_execution_admission_unbound")
    execution = _execution_admission(execution_admission)
    payload = {
        "schema_version": ADMISSION_SNAPSHOT_SCHEMA_VERSION,
        "request_sha256": canonical_request.request_sha256,
        "canonical_request": canonical_request.to_dict(),
        "resolved_admission": resolved,
        "execution_admission": execution,
        "write_fence": _unissued_write_fence(),
    }
    digest = canonical_json_sha256(payload)
    return AdmissionSnapshot(
        schema_version=ADMISSION_SNAPSHOT_SCHEMA_VERSION,
        snapshot_id=f"{_SNAPSHOT_ID_PREFIX}{digest}",
        snapshot_sha256=digest,
        request_sha256=canonical_request.request_sha256,
        canonical_request=canonical_request,
        resolved_admission=resolved,
        execution_admission=execution,
        write_fence=_unissued_write_fence(),
    )


def validate_admission_snapshot(
    value: AdmissionSnapshot | Mapping[str, Any],
    *,
    expected_snapshot_sha256: str,
    expected_generation_authorization_evidence_sha256: str | None = None,
    allow_unbound_policies_for_shadow: bool = False,
    expected_ticket_title_sha256: str,
    expected_policy_sha256s: Mapping[str, Any],
) -> AdmissionSnapshot:
    if not isinstance(value, (AdmissionSnapshot, Mapping)):
        raise RcaAdmissionError("w3_snapshot_type_invalid")
    original = value.to_dict() if isinstance(value, AdmissionSnapshot) else dict(value)
    mapping = _exact_mapping("snapshot", original, _SNAPSHOT_FIELDS)
    snapshot = AdmissionSnapshot(
        schema_version=mapping["schema_version"],
        snapshot_id=mapping["snapshot_id"],
        snapshot_sha256=mapping["snapshot_sha256"],
        request_sha256=mapping["request_sha256"],
        canonical_request=CanonicalRcaRequest.from_mapping(mapping["canonical_request"]),
        resolved_admission=mapping["resolved_admission"],
        execution_admission=mapping["execution_admission"],
        write_fence=mapping["write_fence"],
    )
    _validate_expected_generation_authorization(
        snapshot.canonical_request.execution_intent,
        expected_generation_authorization_evidence_sha256,
    )
    _validate_expected_ticket_title(
        snapshot.canonical_request.ticket,
        expected_ticket_title_sha256,
    )
    _validate_expected_policy_sha256s(
        snapshot.canonical_request,
        expected_policy_sha256s,
    )
    if not isinstance(allow_unbound_policies_for_shadow, bool):
        raise RcaAdmissionError("w3_shadow_policy_opt_in_invalid")
    if not allow_unbound_policies_for_shadow:
        _assert_policy_authority(snapshot.canonical_request)
    expected = _sha256("expected_snapshot", expected_snapshot_sha256)
    if snapshot.snapshot_sha256 != expected:
        raise RcaAdmissionError("w3_snapshot_expected_hash_mismatch")
    if snapshot.to_dict() != original:
        raise RcaAdmissionError("w3_snapshot_not_canonical")
    return snapshot


def _source_metadata(source_kind: str, value: Any) -> dict[str, Any]:
    if source_kind not in {"kafka_workflow_event", "feishu_group_manual"}:
        raise RcaAdmissionError("w3_source_metadata_kind_invalid")
    expected_fields = (
        _KAFKA_METADATA_FIELDS
        if source_kind == "kafka_workflow_event"
        else _MANUAL_METADATA_FIELDS
    )
    metadata = _exact_mapping("source_metadata", value, expected_fields)
    if metadata.get("source_kind") != source_kind:
        raise RcaAdmissionError("w3_source_metadata_kind_mismatch")
    normalized = {"source_kind": source_kind}
    for key in sorted(expected_fields - {"source_kind"}):
        item = metadata.get(key)
        if key in {"partition", "offset"}:
            if isinstance(item, bool) or not isinstance(item, int) or item < 0:
                raise RcaAdmissionError(f"w3_source_metadata_{key}_invalid")
            normalized[key] = item
        elif key == "payload_sha256":
            normalized[key] = _sha256("source_payload", item)
            if normalized[key] == "0" * 64:
                raise RcaAdmissionError("w3_source_payload_unbound")
        elif key == "observed_at":
            normalized[key] = _observed_at(item)
        else:
            may_be_empty = source_kind == "feishu_group_manual" and key in {
                "chat_id",
                "thread_id",
            }
            normalized[key] = _text(
                f"source_metadata_{key}", item, allow_empty=may_be_empty
            )
    if source_kind == "feishu_group_manual":
        platform = normalized["platform"]
        if platform not in {"feishu", "operator"}:
            raise RcaAdmissionError("w3_source_metadata_platform_invalid")
        mode = normalized["mode"]
        if mode not in {"run_or_join", "rerun", "debug"}:
            raise RcaAdmissionError("w3_source_metadata_mode_invalid")
        try:
            validate_rca_requester(
                platform=platform,
                requester_id=normalized["requester_id"],
            )
        except ValueError as exc:
            raise RcaAdmissionError(str(exc)) from exc
        if platform == "feishu":
            if not normalized["chat_id"]:
                raise RcaAdmissionError("w3_source_metadata_chat_id_required")
            thread_id = normalized["thread_id"]
            if not thread_id.startswith("topic:"):
                raise RcaAdmissionError("w3_source_metadata_thread_id_invalid")
            root = thread_id.removeprefix("topic:")
            if re.fullmatch(r"[A-Za-z0-9_-]{3,200}", root) is None:
                raise RcaAdmissionError("w3_source_metadata_thread_id_invalid")
        else:
            if mode not in {"rerun", "debug"}:
                raise RcaAdmissionError("w3_source_metadata_operator_mode_invalid")
            if normalized["chat_id"] or normalized["thread_id"]:
                raise RcaAdmissionError("w3_source_metadata_operator_transport_invalid")
    elif normalized["event_uid"] != (
        f"{normalized['topic']}:{normalized['partition']}:{normalized['offset']}"
    ):
        raise RcaAdmissionError("w3_source_metadata_event_uid_mismatch")
    return normalized


def _anchor(value: Any, *, expected_issue_target: str) -> dict[str, Any]:
    anchor = _exact_mapping("anchor", value, _ANCHOR_FIELDS)
    issue_target = _text("anchor_issue_target", anchor.get("issue_target"))
    if issue_target.rstrip("/") != expected_issue_target.rstrip("/"):
        raise RcaAdmissionError("w3_anchor_issue_target_mismatch")
    thread_target = anchor.get("thread_target")
    if thread_target is not None:
        thread_target = _text("anchor_thread_target", thread_target)
    return {"issue_target": expected_issue_target, "thread_target": thread_target}


def _validate_source_anchor(
    source_kind: str,
    metadata: Mapping[str, Any],
    anchor: Mapping[str, Any],
) -> None:
    if source_kind != "feishu_group_manual":
        if anchor["thread_target"] is not None:
            raise RcaAdmissionError("w3_source_anchor_kafka_thread_invalid")
        return
    thread_target = anchor["thread_target"]
    if metadata["platform"] == "feishu":
        if thread_target != metadata["thread_id"]:
            raise RcaAdmissionError("w3_source_anchor_thread_mismatch")
    elif thread_target is not None:
        raise RcaAdmissionError("w3_source_anchor_operator_thread_invalid")


def _validate_source_payload(
    metadata: Mapping[str, Any],
    expected_source_payload_sha256: str,
) -> None:
    expected = _sha256("expected_source_payload", expected_source_payload_sha256)
    if expected == "0" * 64:
        raise RcaAdmissionError("w3_expected_source_payload_unbound")
    if metadata["payload_sha256"] != expected:
        raise RcaAdmissionError("w3_source_payload_mismatch")


def _ingress_decision(value: Any) -> dict[str, Any]:
    decision = _exact_mapping(
        "ingress_decision", value, _INGRESS_DECISION_FIELDS
    )
    requested_mode = _text("ingress_requested_mode", decision.get("requested_mode"))
    if requested_mode not in {"pending", "shadow"}:
        raise RcaAdmissionError("w3_ingress_requested_mode_invalid")
    binding_action = _text("ingress_binding_action", decision.get("binding_action"))
    if binding_action not in {"create", "join"}:
        raise RcaAdmissionError("w3_ingress_binding_action_invalid")
    outcome = _text("ingress_decision", decision.get("decision"))
    if outcome not in {"admit", "shadow"}:
        raise RcaAdmissionError("w3_ingress_decision_invalid")
    if (requested_mode, outcome) not in {
        ("pending", "admit"),
        ("shadow", "shadow"),
    }:
        raise RcaAdmissionError("w3_ingress_decision_mode_mismatch")
    evidence_sha256 = _sha256(
        "authorization_evidence",
        decision.get("authorization_evidence_sha256"),
    )
    if evidence_sha256 == "0" * 64:
        raise RcaAdmissionError("w3_authorization_evidence_unbound")
    return {
        "requested_mode": requested_mode,
        "binding_action": binding_action,
        "decision": outcome,
        "authorization_evidence_sha256": evidence_sha256,
    }


def _validate_authorization_evidence(
    ingress_decision: Mapping[str, Any],
    expected_authorization_evidence_sha256: str,
) -> None:
    expected = _sha256(
        "expected_authorization_evidence",
        expected_authorization_evidence_sha256,
    )
    if expected == "0" * 64:
        raise RcaAdmissionError("w3_expected_authorization_evidence_unbound")
    if ingress_decision["authorization_evidence_sha256"] != expected:
        raise RcaAdmissionError("w3_authorization_evidence_mismatch")


def build_source_authority_receipt(
    *,
    source_id: str,
    source_kind: str,
    source_metadata: Mapping[str, Any],
    anchor: Mapping[str, Any],
    ingress_decision: Mapping[str, Any],
    expected_issue_target: str,
) -> dict[str, Any]:
    normalized_source_id = _text("source_authority_source_id", source_id)
    metadata = _source_metadata(source_kind, source_metadata)
    normalized_anchor = _anchor(
        anchor,
        expected_issue_target=expected_issue_target,
    )
    _validate_source_anchor(source_kind, metadata, normalized_anchor)
    ingress = _ingress_decision(ingress_decision)
    identity = {
        "schema_version": SOURCE_AUTHORITY_SCHEMA_VERSION,
        "source_id": normalized_source_id,
        "source_kind": source_kind,
        "source_metadata_sha256": canonical_json_sha256(metadata),
        "anchor_sha256": canonical_json_sha256(normalized_anchor),
        "ingress_decision_sha256": canonical_json_sha256(ingress),
    }
    return {
        **identity,
        "authority_sha256": canonical_json_sha256(identity),
    }


def _validate_expected_source_authority(
    *,
    source_id: str,
    source_kind: str,
    source_metadata: Mapping[str, Any],
    anchor: Mapping[str, Any],
    ingress_decision: Mapping[str, Any],
    expected_source_authority: Mapping[str, Any],
) -> str:
    receipt = _exact_mapping(
        "expected_source_authority",
        expected_source_authority,
        _SOURCE_AUTHORITY_FIELDS,
    )
    if receipt["schema_version"] != SOURCE_AUTHORITY_SCHEMA_VERSION:
        raise RcaAdmissionError("w3_source_authority_schema_version_invalid")
    identity = {
        key: receipt[key]
        for key in _SOURCE_AUTHORITY_FIELDS
        if key != "authority_sha256"
    }
    authority_sha256 = _sha256(
        "source_authority",
        receipt["authority_sha256"],
    )
    if authority_sha256 != canonical_json_sha256(identity):
        raise RcaAdmissionError("w3_source_authority_hash_mismatch")
    observed = {
        "schema_version": SOURCE_AUTHORITY_SCHEMA_VERSION,
        "source_id": source_id,
        "source_kind": source_kind,
        "source_metadata_sha256": canonical_json_sha256(source_metadata),
        "anchor_sha256": canonical_json_sha256(anchor),
        "ingress_decision_sha256": canonical_json_sha256(ingress_decision),
    }
    if identity != observed:
        raise RcaAdmissionError("w3_source_authority_mismatch")
    return authority_sha256


def _ingress_matches_execution(ingress_decision: str, execution_decision: str) -> bool:
    return ingress_decision == execution_decision


def _validate_rerun_source_authority(
    snapshot: AdmissionSnapshot,
    *,
    source_kind: str,
    source_metadata: Mapping[str, Any],
    ingress_decision: Mapping[str, Any],
) -> None:
    if snapshot.resolved_admission["generation"] == 1:
        return
    if snapshot.canonical_request.execution_intent["generation_reason"] != (
        "explicit_user_rerun"
    ):
        raise RcaAdmissionError("w3_rerun_intent_invalid")
    if ingress_decision["binding_action"] != "create":
        return
    if ingress_decision["authorization_evidence_sha256"] != (
        snapshot.canonical_request.execution_intent[
            "generation_authorization_evidence_sha256"
        ]
    ):
        raise RcaAdmissionError("w3_rerun_creator_evidence_mismatch")
    if (
        source_kind != "feishu_group_manual"
        or source_metadata["platform"] != "feishu"
        or source_metadata["mode"] != "rerun"
    ):
        raise RcaAdmissionError("w3_explicit_rerun_authority_invalid")


@dataclass(frozen=True)
class AdmissionSnapshotSourceEnvelope:
    schema_version: str
    source_envelope_id: str
    source_envelope_sha256: str
    source_authority_sha256: str
    snapshot_id: str
    snapshot_sha256: str
    submission_key: str
    source_id: str
    source_kind: str
    ingress_decision: Mapping[str, Any]
    source_metadata: Mapping[str, Any]
    anchor: Mapping[str, Any]
    _authority: InitVar[object | None] = None

    def __post_init__(self, _authority: object | None) -> None:
        if _authority is not _SOURCE_ENVELOPE_AUTHORITY:
            raise RcaAdmissionError("w3_source_envelope_external_authority_required")
        if self.schema_version != SOURCE_ENVELOPE_SCHEMA_VERSION:
            raise RcaAdmissionError("w3_source_envelope_schema_version_invalid")
        if self.source_kind not in {"kafka_workflow_event", "feishu_group_manual"}:
            raise RcaAdmissionError("w3_source_envelope_kind_invalid")
        for name in ("snapshot_id", "submission_key", "source_id"):
            object.__setattr__(self, name, _text(name, getattr(self, name)))
        _sha256("source_envelope_snapshot", self.snapshot_sha256)
        _sha256("source_authority", self.source_authority_sha256)
        if self.snapshot_id != f"{_SNAPSHOT_ID_PREFIX}{self.snapshot_sha256}":
            raise RcaAdmissionError("w3_source_envelope_snapshot_identity_invalid")
        object.__setattr__(
            self,
            "ingress_decision",
            _freeze(_ingress_decision(self.ingress_decision)),
        )
        object.__setattr__(
            self,
            "source_metadata",
            _freeze(_source_metadata(self.source_kind, self.source_metadata)),
        )
        anchor = _exact_mapping("anchor", self.anchor, _ANCHOR_FIELDS)
        issue_target = _text("anchor_issue_target", anchor.get("issue_target"))
        thread_target = anchor.get("thread_target")
        if thread_target is not None:
            thread_target = _text("anchor_thread_target", thread_target)
        object.__setattr__(
            self,
            "anchor",
            _freeze({"issue_target": issue_target, "thread_target": thread_target}),
        )
        _validate_source_anchor(self.source_kind, self.source_metadata, self.anchor)
        expected = canonical_json_sha256(self.identity_payload)
        if self.source_envelope_sha256 != expected:
            raise RcaAdmissionError("w3_source_envelope_hash_mismatch")
        if self.source_envelope_id != f"{_SOURCE_ENVELOPE_ID_PREFIX}{expected}":
            raise RcaAdmissionError("w3_source_envelope_id_mismatch")

    @property
    def identity_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "source_authority_sha256": self.source_authority_sha256,
            "snapshot_id": self.snapshot_id,
            "snapshot_sha256": self.snapshot_sha256,
            "submission_key": self.submission_key,
            "source_id": self.source_id,
            "source_kind": self.source_kind,
            "ingress_decision": _thaw(self.ingress_decision),
            "source_metadata": _thaw(self.source_metadata),
            "anchor": _thaw(self.anchor),
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "source_envelope_id": self.source_envelope_id,
            "source_envelope_sha256": self.source_envelope_sha256,
            **self.identity_payload,
        }


def build_snapshot_source_envelope(
    *,
    snapshot: AdmissionSnapshot | Mapping[str, Any],
    source_id: str,
    source_kind: str,
    source_metadata: Mapping[str, Any],
    anchor: Mapping[str, Any],
    ingress_decision: Mapping[str, Any],
    expected_authorization_evidence_sha256: str,
    expected_generation_authorization_evidence_sha256: str | None = None,
    allow_unbound_policies_for_shadow: bool = False,
    expected_ticket_title_sha256: str,
    expected_source_payload_sha256: str,
    expected_policy_sha256s: Mapping[str, Any],
    expected_snapshot_sha256: str,
    expected_source_authority: Mapping[str, Any],
) -> AdmissionSnapshotSourceEnvelope:
    """Bind a source using an evidence hash supplied by the durable authority."""
    core = validate_admission_snapshot(
        snapshot,
        expected_snapshot_sha256=expected_snapshot_sha256,
        expected_generation_authorization_evidence_sha256=(
            expected_generation_authorization_evidence_sha256
        ),
        allow_unbound_policies_for_shadow=allow_unbound_policies_for_shadow,
        expected_ticket_title_sha256=expected_ticket_title_sha256,
        expected_policy_sha256s=expected_policy_sha256s,
    )
    metadata = _source_metadata(source_kind, source_metadata)
    _validate_source_payload(metadata, expected_source_payload_sha256)
    normalized_anchor = _anchor(
        anchor,
        expected_issue_target=str(core.canonical_request.ticket["issue_url"]),
    )
    _validate_source_anchor(source_kind, metadata, normalized_anchor)
    ingress = _ingress_decision(ingress_decision)
    _validate_authorization_evidence(
        ingress,
        expected_authorization_evidence_sha256,
    )
    normalized_source_id = _text("source_id", source_id)
    source_authority_sha256 = _validate_expected_source_authority(
        source_id=normalized_source_id,
        source_kind=source_kind,
        source_metadata=metadata,
        anchor=normalized_anchor,
        ingress_decision=ingress,
        expected_source_authority=expected_source_authority,
    )
    if not _ingress_matches_execution(
        str(ingress["decision"]), str(core.execution_admission["decision"])
    ):
        raise RcaAdmissionError("w3_source_envelope_decision_mismatch")
    _validate_rerun_source_authority(
        core,
        source_kind=source_kind,
        source_metadata=metadata,
        ingress_decision=ingress,
    )
    payload = {
        "schema_version": SOURCE_ENVELOPE_SCHEMA_VERSION,
        "source_authority_sha256": source_authority_sha256,
        "snapshot_id": core.snapshot_id,
        "snapshot_sha256": core.snapshot_sha256,
        "submission_key": str(core.resolved_admission["submission_key"]),
        "source_id": normalized_source_id,
        "source_kind": source_kind,
        "ingress_decision": ingress,
        "source_metadata": metadata,
        "anchor": normalized_anchor,
    }
    digest = canonical_json_sha256(payload)
    return AdmissionSnapshotSourceEnvelope(
        source_envelope_id=f"{_SOURCE_ENVELOPE_ID_PREFIX}{digest}",
        source_envelope_sha256=digest,
        _authority=_SOURCE_ENVELOPE_AUTHORITY,
        **payload,
    )


def validate_snapshot_source_envelope(
    value: AdmissionSnapshotSourceEnvelope | Mapping[str, Any],
    *,
    expected_snapshot: AdmissionSnapshot | Mapping[str, Any],
    expected_authorization_evidence_sha256: str,
    expected_generation_authorization_evidence_sha256: str | None = None,
    allow_unbound_policies_for_shadow: bool = False,
    expected_ticket_title_sha256: str,
    expected_source_payload_sha256: str,
    expected_policy_sha256s: Mapping[str, Any],
    expected_snapshot_sha256: str,
    expected_source_authority: Mapping[str, Any],
) -> AdmissionSnapshotSourceEnvelope:
    if not isinstance(value, (AdmissionSnapshotSourceEnvelope, Mapping)):
        raise RcaAdmissionError("w3_source_envelope_type_invalid")
    original = (
        value.to_dict()
        if isinstance(value, AdmissionSnapshotSourceEnvelope)
        else dict(value)
    )
    mapping = _exact_mapping("source_envelope", original, _SOURCE_ENVELOPE_FIELDS)
    envelope = AdmissionSnapshotSourceEnvelope(
        _authority=_SOURCE_ENVELOPE_AUTHORITY,
        **{field: mapping[field] for field in _SOURCE_ENVELOPE_FIELDS}
    )
    _validate_source_payload(
        envelope.source_metadata,
        expected_source_payload_sha256,
    )
    snapshot = validate_admission_snapshot(
        expected_snapshot,
        expected_snapshot_sha256=expected_snapshot_sha256,
        expected_generation_authorization_evidence_sha256=(
            expected_generation_authorization_evidence_sha256
        ),
        allow_unbound_policies_for_shadow=allow_unbound_policies_for_shadow,
        expected_ticket_title_sha256=expected_ticket_title_sha256,
        expected_policy_sha256s=expected_policy_sha256s,
    )
    _validate_authorization_evidence(
        envelope.ingress_decision,
        expected_authorization_evidence_sha256,
    )
    source_authority_sha256 = _validate_expected_source_authority(
        source_id=envelope.source_id,
        source_kind=envelope.source_kind,
        source_metadata=envelope.source_metadata,
        anchor=envelope.anchor,
        ingress_decision=envelope.ingress_decision,
        expected_source_authority=expected_source_authority,
    )
    if envelope.source_authority_sha256 != source_authority_sha256:
        raise RcaAdmissionError("w3_source_authority_reference_mismatch")
    if (
        envelope.snapshot_id != snapshot.snapshot_id
        or envelope.snapshot_sha256 != snapshot.snapshot_sha256
        or envelope.submission_key != snapshot.resolved_admission["submission_key"]
        or envelope.anchor["issue_target"]
        != snapshot.canonical_request.ticket["issue_url"]
        or not _ingress_matches_execution(
            str(envelope.ingress_decision["decision"]),
            str(snapshot.execution_admission["decision"]),
        )
    ):
        raise RcaAdmissionError("w3_source_envelope_snapshot_mismatch")
    _validate_rerun_source_authority(
        snapshot,
        source_kind=envelope.source_kind,
        source_metadata=envelope.source_metadata,
        ingress_decision=envelope.ingress_decision,
    )
    if envelope.to_dict() != original:
        raise RcaAdmissionError("w3_source_envelope_not_canonical")
    return envelope


@dataclass(frozen=True)
class AdmissionSnapshotExecutionBundle:
    """Content-addressed W3 authority passed through execution and readback."""

    schema_version: str
    bundle_sha256: str
    snapshot_authority_sha256: str
    snapshot: AdmissionSnapshot
    creator_source_envelope: AdmissionSnapshotSourceEnvelope
    creator_source_authority: Mapping[str, Any]

    def __post_init__(self) -> None:
        if self.schema_version != EXECUTION_SNAPSHOT_BUNDLE_SCHEMA_VERSION:
            raise RcaAdmissionError("w3_execution_bundle_schema_version_invalid")
        if not isinstance(self.snapshot, AdmissionSnapshot):
            raise RcaAdmissionError("w3_execution_bundle_snapshot_invalid")
        authority_body = {
            "schema_version": "pnc_rca_w3_snapshot_authority_v1",
            "policies": {
                name: _thaw(getattr(self.snapshot.canonical_request, name))
                for name in _POLICY_NAMES
            },
        }
        if self.snapshot_authority_sha256 != canonical_json_sha256(authority_body):
            raise RcaAdmissionError("w3_execution_bundle_snapshot_authority_mismatch")
        if not isinstance(
            self.creator_source_envelope,
            AdmissionSnapshotSourceEnvelope,
        ):
            raise RcaAdmissionError("w3_execution_bundle_envelope_invalid")
        envelope = self.creator_source_envelope
        receipt = _exact_mapping(
            "execution_bundle_source_authority",
            self.creator_source_authority,
            _SOURCE_AUTHORITY_FIELDS,
        )
        authority_sha256 = _validate_expected_source_authority(
            source_id=envelope.source_id,
            source_kind=envelope.source_kind,
            source_metadata=envelope.source_metadata,
            anchor=envelope.anchor,
            ingress_decision=envelope.ingress_decision,
            expected_source_authority=receipt,
        )
        if (
            envelope.source_authority_sha256 != authority_sha256
            or envelope.snapshot_id != self.snapshot.snapshot_id
            or envelope.snapshot_sha256 != self.snapshot.snapshot_sha256
            or envelope.submission_key
            != self.snapshot.resolved_admission["submission_key"]
        ):
            raise RcaAdmissionError("w3_execution_bundle_binding_mismatch")
        if envelope.ingress_decision["binding_action"] != "create":
            raise RcaAdmissionError("w3_execution_bundle_creator_required")
        if (
            envelope.ingress_decision["decision"] != "admit"
            or self.snapshot.execution_admission["decision"] != "admit"
        ):
            raise RcaAdmissionError("w3_execution_bundle_not_admitted")
        object.__setattr__(
            self,
            "creator_source_authority",
            _freeze(dict(receipt)),
        )
        expected_sha256 = canonical_json_sha256(self.identity_payload)
        if self.bundle_sha256 != expected_sha256:
            raise RcaAdmissionError("w3_execution_bundle_hash_mismatch")

    @property
    def identity_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "snapshot_authority_sha256": self.snapshot_authority_sha256,
            "snapshot": self.snapshot.to_dict(),
            "creator_source_envelope": self.creator_source_envelope.to_dict(),
            "creator_source_authority": _thaw(self.creator_source_authority),
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "bundle_sha256": self.bundle_sha256,
            "snapshot_authority_sha256": self.snapshot_authority_sha256,
            "snapshot": self.snapshot.to_dict(),
            "creator_source_envelope": self.creator_source_envelope.to_dict(),
            "creator_source_authority": _thaw(self.creator_source_authority),
        }


def build_snapshot_execution_bundle(
    *,
    snapshot: AdmissionSnapshot,
    snapshot_authority_sha256: str,
    creator_source_envelope: AdmissionSnapshotSourceEnvelope,
    creator_source_authority: Mapping[str, Any],
) -> AdmissionSnapshotExecutionBundle:
    payload = {
        "schema_version": EXECUTION_SNAPSHOT_BUNDLE_SCHEMA_VERSION,
        "snapshot_authority_sha256": snapshot_authority_sha256,
        "snapshot": snapshot.to_dict(),
        "creator_source_envelope": creator_source_envelope.to_dict(),
        "creator_source_authority": dict(creator_source_authority),
    }
    return AdmissionSnapshotExecutionBundle(
        bundle_sha256=canonical_json_sha256(payload),
        snapshot_authority_sha256=snapshot_authority_sha256,
        snapshot=snapshot,
        creator_source_envelope=creator_source_envelope,
        creator_source_authority=creator_source_authority,
        schema_version=EXECUTION_SNAPSHOT_BUNDLE_SCHEMA_VERSION,
    )


def validate_snapshot_execution_bundle(
    value: AdmissionSnapshotExecutionBundle | Mapping[str, Any],
) -> AdmissionSnapshotExecutionBundle:
    if not isinstance(value, (AdmissionSnapshotExecutionBundle, Mapping)):
        raise RcaAdmissionError("w3_execution_bundle_type_invalid")
    original = (
        value.to_dict()
        if isinstance(value, AdmissionSnapshotExecutionBundle)
        else dict(value)
    )
    bundle_mapping = _exact_mapping(
        "execution_bundle",
        original,
        _EXECUTION_SNAPSHOT_BUNDLE_FIELDS,
    )
    snapshot_mapping = _exact_mapping(
        "execution_bundle_snapshot",
        bundle_mapping["snapshot"],
        _SNAPSHOT_FIELDS,
    )
    request_mapping = _exact_mapping(
        "execution_bundle_request",
        snapshot_mapping["canonical_request"],
        _REQUEST_FIELDS,
    )
    ticket = _ticket(request_mapping["ticket"])
    intent = _execution_intent(request_mapping["execution_intent"])
    policies = {
        name: _policy(name, request_mapping[name]) for name in _POLICY_NAMES
    }
    policy_sha256s = {name: str(policies[name]["sha256"]) for name in _POLICY_NAMES}
    snapshot = validate_admission_snapshot(
        snapshot_mapping,
        expected_snapshot_sha256=str(snapshot_mapping["snapshot_sha256"]),
        expected_generation_authorization_evidence_sha256=intent[
            "generation_authorization_evidence_sha256"
        ],
        expected_ticket_title_sha256=str(ticket["title_sha256"]),
        expected_policy_sha256s=policy_sha256s,
    )

    envelope_mapping = _exact_mapping(
        "execution_bundle_creator_envelope",
        bundle_mapping["creator_source_envelope"],
        _SOURCE_ENVELOPE_FIELDS,
    )
    source_kind = _text(
        "execution_bundle_source_kind",
        envelope_mapping["source_kind"],
    )
    metadata = _source_metadata(source_kind, envelope_mapping["source_metadata"])
    ingress = _ingress_decision(envelope_mapping["ingress_decision"])
    authority = _exact_mapping(
        "execution_bundle_source_authority",
        bundle_mapping["creator_source_authority"],
        _SOURCE_AUTHORITY_FIELDS,
    )
    envelope = validate_snapshot_source_envelope(
        envelope_mapping,
        expected_snapshot=snapshot,
        expected_authorization_evidence_sha256=str(
            ingress["authorization_evidence_sha256"]
        ),
        expected_generation_authorization_evidence_sha256=intent[
            "generation_authorization_evidence_sha256"
        ],
        expected_ticket_title_sha256=str(ticket["title_sha256"]),
        expected_source_payload_sha256=str(metadata["payload_sha256"]),
        expected_policy_sha256s=policy_sha256s,
        expected_snapshot_sha256=snapshot.snapshot_sha256,
        expected_source_authority=authority,
    )
    bundle = AdmissionSnapshotExecutionBundle(
        schema_version=bundle_mapping["schema_version"],
        bundle_sha256=bundle_mapping["bundle_sha256"],
        snapshot_authority_sha256=bundle_mapping["snapshot_authority_sha256"],
        snapshot=snapshot,
        creator_source_envelope=envelope,
        creator_source_authority=authority,
    )
    if bundle.to_dict() != original:
        raise RcaAdmissionError("w3_execution_bundle_not_canonical")
    return bundle


def snapshot_execution_request_inputs(
    value: AdmissionSnapshotExecutionBundle | Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return the frozen business profile and stable VM policy projection."""
    bundle = validate_snapshot_execution_bundle(value)
    request = bundle.snapshot.canonical_request
    return (
        _business_profile_value(request.business_profile["value"]),
        _execution_request_policy_value(request.execution_policy["value"]),
    )


def snapshot_execution_inputs(
    value: AdmissionSnapshotExecutionBundle | Mapping[str, Any],
) -> tuple[RcaAdmission, RcaTriggerContext]:
    """Derive the compatibility admission and ticket input only from W3 bytes."""
    bundle = validate_snapshot_execution_bundle(value)
    snapshot = bundle.snapshot
    envelope = bundle.creator_source_envelope
    ticket = snapshot.canonical_request.ticket
    resolved = snapshot.resolved_admission
    generation = int(resolved["generation"])
    if envelope.source_kind == "kafka_workflow_event":
        trigger_kind = "issue_created" if generation == 1 else "kafka_retrigger"
        transport = {
            "topic": str(envelope.source_metadata["topic"]),
            "partition": int(envelope.source_metadata["partition"]),
            "offset": int(envelope.source_metadata["offset"]),
        }
    elif envelope.source_kind == "feishu_group_manual":
        trigger_kind = (
            "manual_issue_request" if generation == 1 else "manual_retrigger"
        )
        transport = {}
    else:  # pragma: no cover - guarded by the envelope validator
        raise RcaAdmissionError("w3_execution_bundle_source_kind_invalid")
    admission = build_rca_admission(
        project_key=str(ticket["project_key"]),
        project_simple_name=str(ticket["project_simple_name"]),
        work_item_type_key=str(ticket["work_item_type_key"]),
        work_item_id=str(ticket["work_item_id"]),
        rule_version=str(resolved["creation_rule_version"]),
        trigger_kind=trigger_kind,
        generation=generation,
        **transport,
    )
    expected_resolved = {
        "key_version": admission.key_version,
        "creation_rule_version": admission.source_refs.rule_version,
        "business_key": admission.business_key,
        "submission_key": admission.submission_key,
        "generation": admission.generation,
        "create_once": admission.create_once,
        "dedupe_scope": admission.dedupe_scope,
    }
    if expected_resolved != dict(resolved):
        raise RcaAdmissionError("w3_execution_bundle_admission_mismatch")
    context = build_rca_trigger_context(
        source_kind=envelope.source_kind,
        project_key=str(ticket["project_key"]),
        project_simple_name=str(ticket["project_simple_name"]),
        work_item_type_key=str(ticket["work_item_type_key"]),
        work_item_id=str(ticket["work_item_id"]),
        rule_version=str(resolved["creation_rule_version"]),
        issue_url=str(ticket["issue_url"]),
        title=str(ticket["title"]),
    )
    return admission, context


def _projection_source(
    envelope: AdmissionSnapshotSourceEnvelope,
) -> dict[str, Any]:
    return {
        "schema_version": envelope.schema_version,
        "source_envelope_id": envelope.source_envelope_id,
        "source_envelope_sha256": envelope.source_envelope_sha256,
        "source_authority_sha256": envelope.source_authority_sha256,
        "snapshot_id": envelope.snapshot_id,
        "snapshot_sha256": envelope.snapshot_sha256,
        "submission_key": envelope.submission_key,
        "source_id": envelope.source_id,
        "source_kind": envelope.source_kind,
        "ingress_decision": _thaw(envelope.ingress_decision),
        "transport": _thaw(envelope.source_metadata),
    }


def compose_snapshot_projection(
    snapshot: AdmissionSnapshot | Mapping[str, Any],
    envelope: AdmissionSnapshotSourceEnvelope | Mapping[str, Any],
    *,
    expected_authorization_evidence_sha256: str,
    expected_generation_authorization_evidence_sha256: str | None = None,
    allow_unbound_policies_for_shadow: bool = False,
    expected_ticket_title_sha256: str,
    expected_source_payload_sha256: str,
    expected_policy_sha256s: Mapping[str, Any],
    expected_snapshot_sha256: str,
    expected_source_authority: Mapping[str, Any],
) -> dict[str, Any]:
    core = validate_admission_snapshot(
        snapshot,
        expected_snapshot_sha256=expected_snapshot_sha256,
        expected_generation_authorization_evidence_sha256=(
            expected_generation_authorization_evidence_sha256
        ),
        allow_unbound_policies_for_shadow=allow_unbound_policies_for_shadow,
        expected_ticket_title_sha256=expected_ticket_title_sha256,
        expected_policy_sha256s=expected_policy_sha256s,
    )
    source = validate_snapshot_source_envelope(
        envelope,
        expected_snapshot=core,
        expected_authorization_evidence_sha256=(
            expected_authorization_evidence_sha256
        ),
        expected_generation_authorization_evidence_sha256=(
            expected_generation_authorization_evidence_sha256
        ),
        allow_unbound_policies_for_shadow=allow_unbound_policies_for_shadow,
        expected_ticket_title_sha256=expected_ticket_title_sha256,
        expected_source_payload_sha256=expected_source_payload_sha256,
        expected_policy_sha256s=expected_policy_sha256s,
        expected_snapshot_sha256=expected_snapshot_sha256,
        expected_source_authority=expected_source_authority,
    )
    return {
        "snapshot_core": core.to_dict(),
        "source_metadata": _projection_source(source),
        "anchor": _thaw(source.anchor),
    }


def _validate_projection(
    name: str,
    value: Mapping[str, Any],
    *,
    expected_authorization_evidence_sha256: str,
    expected_generation_authorization_evidence_sha256: str | None,
    expected_ticket_title_sha256: str,
    expected_source_payload_sha256: str,
    expected_policy_sha256s: Mapping[str, Any],
    expected_snapshot_sha256: str,
    expected_source_authority: Mapping[str, Any],
) -> dict[str, Any]:
    projection = _exact_mapping(name, value, _PROJECTION_FIELDS)
    snapshot = validate_admission_snapshot(
        projection["snapshot_core"],
        expected_snapshot_sha256=expected_snapshot_sha256,
        expected_generation_authorization_evidence_sha256=(
            expected_generation_authorization_evidence_sha256
        ),
        allow_unbound_policies_for_shadow=True,
        expected_ticket_title_sha256=expected_ticket_title_sha256,
        expected_policy_sha256s=expected_policy_sha256s,
    )
    _assert_policy_authority(snapshot.canonical_request)
    source = _exact_mapping(
        f"{name}_source_metadata",
        projection["source_metadata"],
        _PROJECTION_SOURCE_FIELDS,
    )
    envelope_payload = {
        "schema_version": source["schema_version"],
        "source_envelope_id": source["source_envelope_id"],
        "source_envelope_sha256": source["source_envelope_sha256"],
        "source_authority_sha256": source["source_authority_sha256"],
        "snapshot_id": source["snapshot_id"],
        "snapshot_sha256": source["snapshot_sha256"],
        "submission_key": source["submission_key"],
        "source_id": source["source_id"],
        "source_kind": source["source_kind"],
        "ingress_decision": source["ingress_decision"],
        "source_metadata": source["transport"],
        "anchor": projection["anchor"],
    }
    envelope = validate_snapshot_source_envelope(
        envelope_payload,
        expected_snapshot=snapshot,
        expected_authorization_evidence_sha256=(
            expected_authorization_evidence_sha256
        ),
        expected_generation_authorization_evidence_sha256=(
            expected_generation_authorization_evidence_sha256
        ),
        allow_unbound_policies_for_shadow=True,
        expected_ticket_title_sha256=expected_ticket_title_sha256,
        expected_source_payload_sha256=expected_source_payload_sha256,
        expected_policy_sha256s=expected_policy_sha256s,
        expected_snapshot_sha256=expected_snapshot_sha256,
        expected_source_authority=expected_source_authority,
    )
    normalized = {
        "snapshot_core": snapshot.to_dict(),
        "source_metadata": _projection_source(envelope),
        "anchor": _thaw(envelope.anchor),
    }
    if normalized != dict(projection):
        raise RcaAdmissionError(f"w3_{name}_not_canonical")
    return normalized


def _diff_paths(
    left: Any,
    right: Any,
    *,
    path: tuple[str, ...] = (),
) -> list[tuple[str, ...]]:
    if isinstance(left, Mapping) and isinstance(right, Mapping):
        paths: list[tuple[str, ...]] = []
        for key in sorted(set(left) | set(right)):
            if not isinstance(key, str):
                raise RcaAdmissionError("w3_diff_non_string_key")
            child = path + (key,)
            if key not in left or key not in right:
                paths.append(child)
            else:
                paths.extend(_diff_paths(left[key], right[key], path=child))
        return paths
    if isinstance(left, (list, tuple)) and isinstance(right, (list, tuple)):
        paths = []
        for index in range(max(len(left), len(right))):
            child = path + (str(index),)
            if index >= len(left) or index >= len(right):
                paths.append(child)
            else:
                paths.extend(_diff_paths(left[index], right[index], path=child))
        return paths
    if type(left) is not type(right) or left != right:
        return [path or ("$",)]
    return []


def _json_pointer(path: tuple[str, ...]) -> str:
    return "/" + "/".join(
        part.replace("~", "~0").replace("/", "~1") for part in path
    )


def compare_snapshot_shadow(
    legacy_projection: Mapping[str, Any],
    candidate_projection: Mapping[str, Any],
    *,
    expected_legacy_authorization_evidence_sha256: str,
    expected_candidate_authorization_evidence_sha256: str,
    expected_legacy_generation_authorization_evidence_sha256: str | None = None,
    expected_candidate_generation_authorization_evidence_sha256: str | None = None,
    expected_legacy_ticket_title_sha256: str,
    expected_candidate_ticket_title_sha256: str,
    expected_legacy_source_payload_sha256: str,
    expected_candidate_source_payload_sha256: str,
    expected_legacy_policy_sha256s: Mapping[str, Any],
    expected_candidate_policy_sha256s: Mapping[str, Any],
    expected_legacy_snapshot_sha256: str,
    expected_candidate_snapshot_sha256: str,
    expected_legacy_source_authority: Mapping[str, Any],
    expected_candidate_source_authority: Mapping[str, Any],
) -> dict[str, Any]:
    legacy = _exact_mapping("legacy_projection", legacy_projection, _PROJECTION_FIELDS)
    candidate = _exact_mapping(
        "candidate_projection", candidate_projection, _PROJECTION_FIELDS
    )
    paths = sorted(set(_diff_paths(legacy, candidate)))
    validation_errors: dict[str, str] = {}
    expected_evidence = {
        "legacy": expected_legacy_authorization_evidence_sha256,
        "candidate": expected_candidate_authorization_evidence_sha256,
    }
    expected_generation_evidence = {
        "legacy": expected_legacy_generation_authorization_evidence_sha256,
        "candidate": expected_candidate_generation_authorization_evidence_sha256,
    }
    expected_title = {
        "legacy": expected_legacy_ticket_title_sha256,
        "candidate": expected_candidate_ticket_title_sha256,
    }
    expected_payload = {
        "legacy": expected_legacy_source_payload_sha256,
        "candidate": expected_candidate_source_payload_sha256,
    }
    expected_policies = {
        "legacy": expected_legacy_policy_sha256s,
        "candidate": expected_candidate_policy_sha256s,
    }
    expected_snapshots = {
        "legacy": expected_legacy_snapshot_sha256,
        "candidate": expected_candidate_snapshot_sha256,
    }
    expected_source_authorities = {
        "legacy": expected_legacy_source_authority,
        "candidate": expected_candidate_source_authority,
    }
    for name, projection in (("legacy", legacy), ("candidate", candidate)):
        try:
            _validate_projection(
                f"{name}_projection",
                projection,
                expected_authorization_evidence_sha256=expected_evidence[name],
                expected_generation_authorization_evidence_sha256=(
                    expected_generation_evidence[name]
                ),
                expected_ticket_title_sha256=expected_title[name],
                expected_source_payload_sha256=expected_payload[name],
                expected_policy_sha256s=expected_policies[name],
                expected_snapshot_sha256=expected_snapshots[name],
                expected_source_authority=expected_source_authorities[name],
            )
        except (RcaAdmissionError, TypeError, ValueError) as exc:
            validation_errors[name] = str(exc)
    if validation_errors:
        allowed_segments: list[tuple[str, ...]] = []
        forbidden_segments = list(paths)
        forbidden_segments.extend((f"${name}",) for name in validation_errors)
        forbidden_segments = sorted(set(forbidden_segments))
    else:
        allowed_segments = [
            path
            for path in paths
            if path
            and (
                path[0] in {"source_metadata", "anchor"}
                or (
                    path[0] == "snapshot_core"
                    and len(path) > 1
                    and path[1] in {"snapshot_id", "snapshot_sha256", "write_fence"}
                )
            )
        ]
        forbidden_segments = [path for path in paths if path not in allowed_segments]

    def _shadow_core(value: Mapping[str, Any]) -> dict[str, Any]:
        snapshot = dict(value)
        return {
            key: snapshot[key]
            for key in (
                "schema_version",
                "request_sha256",
                "canonical_request",
                "resolved_admission",
                "execution_admission",
            )
        }

    legacy_core_sha = canonical_json_sha256(_shadow_core(legacy["snapshot_core"]))
    candidate_core_sha = canonical_json_sha256(_shadow_core(candidate["snapshot_core"]))
    matches = (
        not validation_errors
        and not forbidden_segments
        and legacy_core_sha == candidate_core_sha
    )
    return {
        "match": matches,
        "outcome": "match" if matches else "mismatch",
        "legacy_full_sha256": canonical_json_sha256(legacy),
        "candidate_full_sha256": canonical_json_sha256(candidate),
        "legacy_semantic_sha256": legacy_core_sha,
        "candidate_semantic_sha256": candidate_core_sha,
        "allowed_diff_paths": [_json_pointer(path) for path in allowed_segments],
        "forbidden_diff_paths": [
            _json_pointer(path) for path in forbidden_segments
        ],
        "validation_errors": validation_errors,
    }


def _legacy_policy(name: str, value: Any) -> dict[str, Any]:
    """Independent legacy-side policy projection used only by the shadow oracle."""
    if value is None or not isinstance(value, Mapping) or set(value) != _POLICY_FIELDS:
        raise RcaAdmissionError(f"w3_legacy_{name}_unbound")
    version = _text(f"legacy_{name}_version", value.get("version"))
    observed = value.get("value")
    if not isinstance(observed, Mapping):
        raise RcaAdmissionError(f"w3_legacy_{name}_value_invalid")
    digest = _sha256(f"legacy_{name}", value.get("sha256"))
    if digest != canonical_json_sha256({"version": version, "value": observed}):
        raise RcaAdmissionError(f"w3_legacy_{name}_digest_mismatch")
    return {"version": version, "sha256": digest, "value": _thaw(_freeze(observed))}


def legacy_semantic_projection(
    *,
    admission: RcaAdmission | Mapping[str, Any],
    trigger_context: RcaTriggerContext | Mapping[str, Any],
    creation_policy: Mapping[str, Any],
    business_profile: Mapping[str, Any],
    execution_policy: Mapping[str, Any],
    publication_policy: Mapping[str, Any],
    correction_lineage_policy: Mapping[str, Any],
    execution_admission: Mapping[str, Any],
    source_id: str,
    source_metadata: Mapping[str, Any],
    anchor: Mapping[str, Any],
    ingress_decision: Mapping[str, Any],
    expected_authorization_evidence_sha256: str,
    generation_reason: str | None = None,
    generation_authorization_evidence_sha256: str | None = None,
    expected_generation_authorization_evidence_sha256: str | None = None,
    expected_ticket_title_sha256: str,
    expected_source_payload_sha256: str,
    expected_policy_sha256s: Mapping[str, Any],
    expected_source_authority: Mapping[str, Any],
) -> dict[str, Any]:
    """Independently project durable legacy values without candidate builders."""
    legacy = validate_rca_admission(admission)
    context = validate_rca_trigger_context(trigger_context)
    project_simple_name = _validate_legacy_identity(legacy, context)
    if legacy.generation == 1:
        if generation_reason not in {None, "initial"}:
            raise RcaAdmissionError("w3_legacy_initial_generation_reason_invalid")
        if generation_authorization_evidence_sha256 is not None:
            raise RcaAdmissionError(
                "w3_legacy_initial_generation_authorization_forbidden"
            )
        if expected_generation_authorization_evidence_sha256 is not None:
            raise RcaAdmissionError(
                "w3_legacy_initial_generation_authorization_forbidden"
            )
        reason = "initial"
        generation_authorization = None
    else:
        if generation_reason is None:
            raise RcaAdmissionError("w3_legacy_rerun_generation_reason_unbound")
        reason = _text("legacy_rerun_generation_reason", generation_reason)
        if reason not in _RERUN_REASONS:
            raise RcaAdmissionError("w3_legacy_rerun_generation_reason_invalid")
        generation_authorization = _sha256(
            "legacy_generation_authorization_evidence",
            generation_authorization_evidence_sha256,
        )
        if generation_authorization == "0" * 64:
            raise RcaAdmissionError(
                "w3_legacy_generation_authorization_evidence_unbound"
            )
        expected_generation_authorization = _sha256(
            "legacy_expected_generation_authorization_evidence",
            expected_generation_authorization_evidence_sha256,
        )
        if expected_generation_authorization == "0" * 64:
            raise RcaAdmissionError(
                "w3_legacy_expected_generation_authorization_evidence_unbound"
            )
        if generation_authorization != expected_generation_authorization:
            raise RcaAdmissionError(
                "w3_legacy_generation_authorization_evidence_mismatch"
            )
    legacy_creation = _legacy_policy("creation_policy", creation_policy)
    creation_rule = _creation_rule(legacy_creation)
    if creation_rule != legacy.source_refs.rule_version:
        raise RcaAdmissionError("w3_legacy_creation_policy_admission_mismatch")
    title = _text("legacy_ticket_title", context.title)
    title_sha256 = canonical_ticket_title_sha256(title)
    expected_title_sha256 = _sha256(
        "legacy_expected_ticket_title",
        expected_ticket_title_sha256,
    )
    if title_sha256 != expected_title_sha256:
        raise RcaAdmissionError("w3_legacy_ticket_title_authority_mismatch")
    request_dict = {
        "schema_version": CANONICAL_RCA_REQUEST_SCHEMA_VERSION,
        "ticket": {
            "project_key": legacy.source_refs.project_key,
            "project_simple_name": project_simple_name,
            "work_item_type_key": legacy.source_refs.work_item_type_key,
            "work_item_id": legacy.source_refs.work_item_id,
            "issue_url": context.issue_url,
            "title": title,
            "title_sha256": title_sha256,
        },
        "execution_intent": {
            "kind": "analyze_ticket",
            "generation_reason": reason,
            "generation_authorization_evidence_sha256": (
                generation_authorization
            ),
        },
        "creation_policy": legacy_creation,
        "business_profile": _legacy_policy("business_profile", business_profile),
        "execution_policy": _legacy_policy("execution_policy", execution_policy),
        "publication_policy": _legacy_policy(
            "publication_policy", publication_policy
        ),
        "correction_lineage_policy": _legacy_policy(
            "correction_lineage_policy", correction_lineage_policy
        ),
    }
    _validate_expected_policy_sha256s(request_dict, expected_policy_sha256s)
    request_sha = canonical_json_sha256(request_dict)
    resolved = _resolved_admission_from_legacy(legacy)
    execution = _execution_admission(execution_admission)
    snapshot_payload = {
        "schema_version": ADMISSION_SNAPSHOT_SCHEMA_VERSION,
        "request_sha256": request_sha,
        "canonical_request": request_dict,
        "resolved_admission": resolved,
        "execution_admission": execution,
        "write_fence": _unissued_write_fence(),
    }
    snapshot_sha = canonical_json_sha256(snapshot_payload)
    snapshot_core = {
        "schema_version": ADMISSION_SNAPSHOT_SCHEMA_VERSION,
        "snapshot_id": f"{_SNAPSHOT_ID_PREFIX}{snapshot_sha}",
        "snapshot_sha256": snapshot_sha,
        **snapshot_payload,
    }
    metadata = _source_metadata(context.source_kind, source_metadata)
    _validate_source_payload(metadata, expected_source_payload_sha256)
    normalized_anchor = _anchor(
        anchor,
        expected_issue_target=str(request_dict["ticket"]["issue_url"]),
    )
    _validate_source_anchor(context.source_kind, metadata, normalized_anchor)
    ingress = _ingress_decision(ingress_decision)
    _validate_authorization_evidence(
        ingress,
        expected_authorization_evidence_sha256,
    )
    normalized_source_id = _text("legacy_source_id", source_id)
    source_authority_sha256 = _validate_expected_source_authority(
        source_id=normalized_source_id,
        source_kind=context.source_kind,
        source_metadata=metadata,
        anchor=normalized_anchor,
        ingress_decision=ingress,
        expected_source_authority=expected_source_authority,
    )
    if not _ingress_matches_execution(
        str(ingress["decision"]), str(execution["decision"])
    ):
        raise RcaAdmissionError("w3_legacy_ingress_decision_mismatch")
    if legacy.generation > 1 and ingress["binding_action"] == "create":
        if ingress["authorization_evidence_sha256"] != generation_authorization:
            raise RcaAdmissionError("w3_legacy_rerun_creator_evidence_mismatch")
        if (
            context.source_kind != "feishu_group_manual"
            or metadata["platform"] != "feishu"
            or metadata["mode"] != "rerun"
        ):
            raise RcaAdmissionError("w3_legacy_explicit_rerun_authority_invalid")
    envelope_payload = {
        "schema_version": SOURCE_ENVELOPE_SCHEMA_VERSION,
        "source_authority_sha256": source_authority_sha256,
        "snapshot_id": snapshot_core["snapshot_id"],
        "snapshot_sha256": snapshot_core["snapshot_sha256"],
        "submission_key": resolved["submission_key"],
        "source_id": normalized_source_id,
        "source_kind": context.source_kind,
        "ingress_decision": ingress,
        "source_metadata": metadata,
        "anchor": normalized_anchor,
    }
    envelope_sha = canonical_json_sha256(envelope_payload)
    return {
        "snapshot_core": snapshot_core,
        "source_metadata": {
            "schema_version": SOURCE_ENVELOPE_SCHEMA_VERSION,
            "source_envelope_id": f"{_SOURCE_ENVELOPE_ID_PREFIX}{envelope_sha}",
            "source_envelope_sha256": envelope_sha,
            "source_authority_sha256": source_authority_sha256,
            "snapshot_id": snapshot_core["snapshot_id"],
            "snapshot_sha256": snapshot_core["snapshot_sha256"],
            "submission_key": resolved["submission_key"],
            "source_id": normalized_source_id,
            "source_kind": context.source_kind,
            "ingress_decision": ingress,
            "transport": metadata,
        },
        "anchor": normalized_anchor,
    }
