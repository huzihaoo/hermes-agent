"""Fail-closed RCA terminal-failure taxonomy.

Every VM blocker is projected into exactly one operational lane.  The
projection keeps the producer's named code intact; an unknown code becomes a
``taxonomy_gap:<raw>`` hard defect instead of being guessed from ``retryable``
or collapsed to ``*_unclassified``.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
import re
from typing import Any, Mapping, TypeGuard


INFRA_SELF_HEALABLE = "infra_self_healable"
NEEDS_HUMAN_INPUT = "needs_human_input"
HARD_DEFECT = "hard_defect"

FAULT_CLASSES = frozenset({INFRA_SELF_HEALABLE, NEEDS_HUMAN_INPUT, HARD_DEFECT})

INFRA_REMEDIATION_HOLD = "infra_remediation_hold"
INTERNAL_BACKLOG = "internal_backlog"
INTERNAL_ALERT = "internal_alert"

TERMINAL_FALLBACK_SECONDS = 30 * 60

# Environment, ownership, and bounded service faults.  Retrying these must not
# mint a new RCA generation or ask the issue originator to repair VM state.
INFRA_SELF_HEALABLE_KINDS = frozenset({
    "translate_workdir_permission",
    "translate_service_unavailable",
    "mcap_chown_required",
    "workdir_permission",
    "permission_denied",
    "case_dir_permission",
    "datapipe_timeout",
    "remote_reader_timeout",
    "remote_reader_read_failed_exhausted",
    "vm_status_reader_unavailable",
    "vm_status_missing",
    "vm_status_unavailable",
    "artifact_reader_unavailable",
    "artifact_bundle_unavailable",
    "delivery_contract_missing",
    "delivery_manifest_missing",
    "report_data_missing",
    "artifact_missing",
    "html_dependency_missing",
    "html_dependency_changed_during_read",
    "required_html_artifact_missing",
    "html_css_parser_dependency_missing",
    "html_css_parser_version_mismatch",
    "viz_publication_missing",
    "viz_publication_path_invalid",
    "timeout",
})

# Source/evidence boundaries that can only be resolved outside the automated
# execution.  They go to an internal backlog; the public result must not turn
# them into a question or assign work to the reporter.
NEEDS_HUMAN_INPUT_KINDS = frozenset({
    "need_source_or_evidence",
    "need_evidence",
    "need_data",
    "missing_frame_id",
    "frame_id_missing",
    "frame_id_unresolved_after_auto_discovery",
    "front_camera_frame_not_found",
    "front_camera_frame_outside_tolerance",
    "data_address_missing",
    "source_unreadable",
    "source_quality_insufficient",
    "remote_event_not_found",
    "remote_read_completeness_not_proven",
    "issue_field_missing_remote_data_reference",
    "issue_field_invalid_remote_data_reference",
    "issue_field_invalid_frame_reference",
    "missing_or_invalid_remote_data_access",
    "unsupported_function_domain",
    "out_of_scope",
    "need_keyframe",
    "need_key_frame",
    "required_input",
    "missing_required_input",
    "evidence_not_ready",
})

# Code, provenance, lineage, and publication-contract defects.  These are
# operator-facing alerts and never requests to the issue originator.
HARD_DEFECT_KINDS = frozenset({
    "remote_evidence_domain_unsupported",
    "service_provenance_unavailable",
    "delivery_lineage_unavailable",
    "viz_mcap_build_failed",
    "html_capability_payload_mismatch",
    "translate_tool_missing",
    "reader_topic_mismatch",
    "alignment_failed",
    "invalid_schema_version",
    "missing_request",
    "request_missing",
    "request_not_visible_on_vm",
    "schema_mismatch",
    "request_contract_drift",
    "service_pipeline_runner_failed",
    "service_execution_request_invalid",
    "service_worker_attestation_unavailable",
    "service_worker_attestation_timeout",
    "rca_execution_attestation_unavailable",
    "rca_prod_capacity_attestation_unavailable",
    "rca_work_deadline_exceeded",
    "vm_status_unknown",
    "failure_receipt_missing",
    "failure_receipt_reader_unavailable",
    "failure_receipt_response_invalid",
    "submission_admission_invalid",
    "submission_outbox_contract_invalid",
    "submission_receipt_identity_mismatch",
    "submission_watch_identity_mismatch",
    "delivery_record_conflict",
    "failure_receipt_unavailable",
    "failure_receipt_json_invalid",
    "failure_receipt_size_invalid",
    "failure_receipt_shape_invalid",
    "failure_receipt_identity_invalid",
    "failure_receipt_blocker_too_large",
    "failure_receipt_file_invalid",
    "failure_receipt_parent_invalid",
    "artifact_reader_response_invalid",
    "artifact_verifier_unavailable",
    "artifact_bundle_too_large",
    "artifact_path_invalid",
    "artifact_path_outside_root",
    "delivery_manifest_artifacts_invalid",
    "delivery_manifest_duplicate_artifact",
    "html_active_content_unsupported",
    "html_active_navigation_unsupported",
    "html_base_url_unsupported",
    "html_comments_unsupported",
    "html_css_dynamic_resource_unsupported",
    "html_css_parser_probe_invalid",
    "html_css_parser_probe_root_invalid",
    "html_css_parser_probe_unavailable",
    "html_css_syntax_invalid",
    "html_declaration_unsupported",
    "html_delivery_mcap_forbidden",
    "html_dependency_not_manifested",
    "html_dependency_text_invalid",
    "html_dependency_text_total_too_large",
    "html_duplicate_attribute_unsupported",
    "html_dynamic_dependency_unsupported",
    "html_embedded_data_dependency_unsupported",
    "html_external_active_document_unsupported",
    "html_external_dependency_unsupported",
    "html_markup_invalid",
    "html_navigation_scheme_unsupported",
    "html_processing_instruction_unsupported",
    "html_script_execution_unsupported",
    "html_srcdoc_nesting_too_deep",
    "html_srcset_syntax_unsupported",
    "public_artifact_banned_phrase",
    "viz_publication_manifest_json_invalid",
    "viz_publication_manifest_mismatch",
    "viz_publication_size_mismatch",
    "artifact_hash_invalid",
    "artifact_hash_mismatch",
    "artifact_not_regular_file",
    "artifact_root_invalid",
    "artifact_set_id_mismatch",
    "artifact_set_reference_mismatch",
    "artifact_size_mismatch",
    "consumer_capability_applicability_invalid",
    "consumer_capability_evaluator_invalid",
    "consumer_capability_evidence_invalid",
    "consumer_capability_false_applied",
    "consumer_capability_field_lineage_invalid",
    "consumer_capability_invalid",
    "consumer_capability_inventory_invalid",
    "consumer_capability_reason_missing",
    "consumer_capability_schema_unsupported",
    "consumer_capability_unused_reason_missing",
    "consumer_capability_viz_lineage_invalid",
    "delivery_artifact_bundle_too_large",
    "delivery_artifact_inventory_invalid",
    "delivery_artifact_inventory_mismatch",
    "delivery_business_state_not_ready",
    "delivery_comment_too_large",
    "delivery_contract_schema_unsupported",
    "delivery_dependencies_incomplete",
    "delivery_field_invalid",
    "delivery_field_missing",
    "delivery_kind_unsupported",
    "delivery_manifest_not_sealed",
    "delivery_manifest_schema_unsupported",
    "delivery_manifest_sealed_at_invalid",
    "delivery_manifest_shape_invalid",
    "delivery_manifest_store_hash_mismatch",
    "delivery_project_simple_name_invalid",
    "delivery_project_simple_name_missing",
    "delivery_report_cifs_path_invalid",
    "delivery_report_not_deliverable",
    "foxglove_url_invalid",
    "html_delivery_must_not_claim_viz",
    "html_validation_blocked",
    "html_validation_fidelity_failed",
    "html_validation_missing",
    "html_validation_shape_invalid",
    "html_validation_state_invalid",
    "report_path_identity_mismatch",
    "report_public_origin_invalid",
    "report_url_identity_invalid",
    "report_url_invalid",
    "required_html_artifact_invalid",
    "required_report_data_artifact_invalid",
    "viz_publication_identity_mismatch",
    "viz_publication_manifest_observation_mismatch",
    "viz_publication_manifest_path_invalid",
    "viz_publication_observation_mismatch",
    "viz_publication_shape_invalid",
    "viz_publication_source_hash_mismatch",
    "viz_publication_source_invalid",
})

_KIND_TO_LANE = {
    **{kind: INFRA_SELF_HEALABLE for kind in INFRA_SELF_HEALABLE_KINDS},
    **{kind: NEEDS_HUMAN_INPUT for kind in NEEDS_HUMAN_INPUT_KINDS},
    **{kind: HARD_DEFECT for kind in HARD_DEFECT_KINDS},
}

_INFRA_KIND_PREFIXES = ("artifact_missing_", "html_dependency_missing_")

_GATE_TO_KIND = {
    "ready_to_download": "need_source_or_evidence",
    "need_evidence": "need_evidence",
    "need_source_or_evidence": "need_source_or_evidence",
    "requires_download": "need_source_or_evidence",
    "need_download": "need_source_or_evidence",
    "need_user_data": "need_source_or_evidence",
    "need_input": "need_source_or_evidence",
    "blocked_need_keyframe": "need_keyframe",
    "need_keyframe": "need_keyframe",
    "need_pipeline_fix": "service_pipeline_runner_failed",
    "needs_fix": "service_pipeline_runner_failed",
    "technical_failure": "service_pipeline_runner_failed",
}

_SAFE_GAP_RE = re.compile(r"[^a-z0-9_.-]+")


@dataclass(frozen=True)
class FailureDecision:
    raw_code: str
    terminal_error_code: str
    lane: str
    internal_route: str
    known: bool
    retryable: bool
    external_comment_policy: str
    terminal_fallback_seconds: int
    audit: dict[str, Any]
    contract_errors: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["contract_errors"] = list(self.contract_errors)
        return value


class TaxonomyContractError(RuntimeError):
    def __init__(self, decision: FailureDecision):
        self.decision = decision
        super().__init__(decision.terminal_error_code)


def _as_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        low = value.strip().lower()
        if low in {"1", "true", "yes", "on"}:
            return True
        if low in {"0", "false", "no", "off"}:
            return False
    return None


def _normalized_kind(value: Any) -> str:
    return str(value or "").strip().lower()


def _taxonomy_gap(raw: str) -> str:
    safe = _SAFE_GAP_RE.sub("_", _normalized_kind(raw)).strip("_.-")
    return f"taxonomy_gap:{(safe or 'missing_blocker')[:96]}"


def blocker_kind(blocker: Any) -> str:
    if not isinstance(blocker, Mapping):
        return ""
    return _normalized_kind(blocker.get("kind"))


def is_retryable(blocker: Any) -> bool:
    if not isinstance(blocker, Mapping):
        return False
    return _as_bool(blocker.get("retryable")) is True


_REMOTE_AUDIT_FIELD_ORDER = ("parse_attempts", "data_sources", "results")
_REMOTE_AUDIT_FIELDS = frozenset(_REMOTE_AUDIT_FIELD_ORDER)
_REMOTE_ATTEMPT_FIELDS = frozenset({
    "attempt_id",
    "parser",
    "status",
    "reference_sha256",
})
_REMOTE_SOURCE_FIELDS = frozenset({
    "source_id",
    "source_kind",
    "status",
    "reference_sha256",
})
_REMOTE_RESULT_FIELDS = frozenset({
    "attempt_id",
    "source_id",
    "status",
    "returned_count",
    "reference_sha256",
})
_REMOTE_AUDIT_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def _valid_reference_sha256(value: Any) -> TypeGuard[str]:
    return (
        isinstance(value, str)
        and _REMOTE_AUDIT_SHA256_RE.fullmatch(value) is not None
        and value != "0" * 64
    )


def _remote_event_audit(
    blocker: Mapping[str, Any],
) -> tuple[dict[str, Any], tuple[str, ...]]:
    raw_audit = blocker.get("audit")
    audit: dict[str, Any] = {}
    errors: list[str] = []
    if not isinstance(raw_audit, Mapping):
        return audit, ("remote_event_not_found_audit_missing",)
    if set(raw_audit) != _REMOTE_AUDIT_FIELDS:
        return audit, ("remote_event_not_found_audit_keys_invalid",)

    source = raw_audit
    normalized_fields: dict[str, list[dict[str, Any]]] = {}
    for field in _REMOTE_AUDIT_FIELD_ORDER:
        value = source.get(field)
        if not isinstance(value, list) or not value:
            errors.append(f"remote_event_not_found_{field}_missing")
            continue
        if len(value) > 64 or not all(isinstance(item, Mapping) for item in value):
            errors.append(f"remote_event_not_found_{field}_invalid")
            continue
        # JSON round-trip guarantees the durable projection contains only
        # ordinary JSON values, never producer-side object instances.
        try:
            normalized = json.loads(
                json.dumps(value, ensure_ascii=False, allow_nan=False)
            )
        except (TypeError, ValueError):
            errors.append(f"remote_event_not_found_{field}_invalid")
            continue
        if len(json.dumps(normalized, ensure_ascii=False).encode("utf-8")) > 16_384:
            errors.append(f"remote_event_not_found_{field}_too_large")
            continue
        normalized_fields[field] = normalized
    if errors:
        return audit, tuple(errors)

    attempts = normalized_fields["parse_attempts"]
    sources = normalized_fields["data_sources"]
    results = normalized_fields["results"]
    attempt_records: dict[str, dict[str, Any]] = {}
    source_records: dict[str, dict[str, Any]] = {}
    reference_hashes: set[str] = set()
    for index, item in enumerate(attempts, start=1):
        attempt_id = item.get("attempt_id")
        parser = item.get("parser")
        status = item.get("status")
        reference_sha256 = item.get("reference_sha256")
        if (
            set(item) != _REMOTE_ATTEMPT_FIELDS
            or not isinstance(attempt_id, str)
            or attempt_id != f"parse-attempt-{index}"
            or parser != "remote_event_reader"
            or status != "parsed"
            or not _valid_reference_sha256(reference_sha256)
        ):
            errors.append("remote_event_not_found_parse_attempts_invalid")
            continue
        attempt_records[attempt_id] = item
        reference_hashes.add(reference_sha256)
    for index, item in enumerate(sources, start=1):
        source_id = item.get("source_id")
        source_kind = item.get("source_kind")
        status = item.get("status")
        reference_sha256 = item.get("reference_sha256")
        if (
            set(item) != _REMOTE_SOURCE_FIELDS
            or not isinstance(source_id, str)
            or source_id != f"data-source-{index}"
            or source_kind != "pdcl_event"
            or status != "not_found"
            or not _valid_reference_sha256(reference_sha256)
        ):
            errors.append("remote_event_not_found_data_sources_invalid")
            continue
        source_records[source_id] = item
        reference_hashes.add(reference_sha256)
    correlated_attempts: set[str] = set()
    correlated_sources: set[str] = set()
    result_identities: set[tuple[str, str]] = set()
    for item in results:
        attempt_id = item.get("attempt_id")
        source_id = item.get("source_id")
        status = item.get("status")
        returned_count = item.get("returned_count")
        reference_sha256 = item.get("reference_sha256")
        if (
            set(item) != _REMOTE_RESULT_FIELDS
            or not isinstance(attempt_id, str)
            or not isinstance(source_id, str)
            or status != "not_found"
            or type(returned_count) is not int
            or returned_count != 0
            or not _valid_reference_sha256(reference_sha256)
        ):
            errors.append("remote_event_not_found_results_invalid")
            continue
        result_identity = (attempt_id, source_id)
        attempt = attempt_records.get(attempt_id)
        data_source = source_records.get(source_id)
        if (
            attempt is None
            or data_source is None
            or result_identity in result_identities
        ):
            errors.append("remote_event_not_found_results_invalid")
            continue
        if (
            reference_sha256 != attempt["reference_sha256"]
            or reference_sha256 != data_source["reference_sha256"]
        ):
            errors.append("remote_event_not_found_reference_sha256_mismatch")
            continue
        result_identities.add(result_identity)
        correlated_attempts.add(attempt_id)
        correlated_sources.add(source_id)
        reference_hashes.add(reference_sha256)
    if correlated_attempts != set(attempt_records) or correlated_sources != set(
        source_records
    ):
        errors.append("remote_event_not_found_audit_uncorrelated")
    if len(reference_hashes) != 1:
        errors.append("remote_event_not_found_reference_sha256_mismatch")
    if not errors:
        audit.update(normalized_fields)
    return audit, tuple(errors)


def decide_failure(
    blocker: Any,
    *,
    gate_decision: str = "",
    require_complete: bool = False,
) -> FailureDecision:
    """Return the durable three-lane decision for a producer blocker.

    Unknown kinds and producer classification conflicts are hard defects with a
    ``taxonomy_gap:<raw>`` code.  ``require_complete`` turns any such contract
    gap (including missing event-not-found audit) into a non-zero gate failure.
    """

    item = dict(blocker) if isinstance(blocker, Mapping) else {}
    raw_code = blocker_kind(item)
    if not raw_code:
        raw_code = _GATE_TO_KIND.get(_normalized_kind(gate_decision), "")
    expected_lane = _KIND_TO_LANE.get(raw_code)
    if expected_lane is None and raw_code.startswith(_INFRA_KIND_PREFIXES):
        expected_lane = INFRA_SELF_HEALABLE
    explicit_lane = _normalized_kind(item.get("fault_class"))
    errors: list[str] = []

    if expected_lane is None:
        lane = HARD_DEFECT
        terminal_error_code = _taxonomy_gap(raw_code)
        errors.append("unknown_blocker_kind")
    elif explicit_lane and explicit_lane not in FAULT_CLASSES:
        lane = HARD_DEFECT
        terminal_error_code = _taxonomy_gap(raw_code)
        errors.append("invalid_explicit_fault_class")
    elif explicit_lane and explicit_lane != expected_lane:
        lane = HARD_DEFECT
        terminal_error_code = _taxonomy_gap(raw_code)
        errors.append("explicit_fault_class_conflict")
    else:
        lane = expected_lane
        terminal_error_code = raw_code

    audit: dict[str, Any] = {}
    if raw_code == "remote_event_not_found":
        audit, audit_errors = _remote_event_audit(item)
        errors.extend(audit_errors)
        if audit_errors:
            lane = HARD_DEFECT

    retryable = lane == INFRA_SELF_HEALABLE
    advertised_retryable = _as_bool(item.get("retryable"))
    if expected_lane is not None and advertised_retryable is not None:
        if advertised_retryable is not retryable:
            errors.append("retryable_contract_conflict")
            lane = HARD_DEFECT
            retryable = False

    if lane == INFRA_SELF_HEALABLE:
        route = INFRA_REMEDIATION_HOLD
        comment_policy = "suppress_until_terminal_fallback"
    elif lane == NEEDS_HUMAN_INPUT:
        route = INTERNAL_BACKLOG
        comment_policy = "honest_non_attribution_only"
    else:
        route = INTERNAL_ALERT
        comment_policy = "honest_non_attribution_only"

    decision = FailureDecision(
        raw_code=raw_code or "missing_blocker",
        terminal_error_code=terminal_error_code,
        lane=lane,
        internal_route=route,
        known=expected_lane is not None and not errors,
        retryable=retryable,
        external_comment_policy=comment_policy,
        terminal_fallback_seconds=TERMINAL_FALLBACK_SECONDS,
        audit=audit,
        contract_errors=tuple(errors),
    )
    if require_complete and not decision.known:
        raise TaxonomyContractError(decision)
    return decision


def classify_blocker(blocker: Any, *, gate_decision: str = "") -> str:
    return decide_failure(blocker, gate_decision=gate_decision).lane


def is_self_healable(blocker: Any, *, gate_decision: str = "") -> bool:
    return classify_blocker(blocker, gate_decision=gate_decision) == INFRA_SELF_HEALABLE


def needs_human_input(blocker: Any, *, gate_decision: str = "") -> bool:
    return classify_blocker(blocker, gate_decision=gate_decision) == NEEDS_HUMAN_INPUT


def is_hard_defect(blocker: Any, *, gate_decision: str = "") -> bool:
    return classify_blocker(blocker, gate_decision=gate_decision) == HARD_DEFECT


_REMEDIATION = {
    "translate_workdir_permission": {
        "op": "normalize_workdir_ownership",
        "detail": "normalize the pipeline-owned translate workdir, then retry",
        "resume_from_stage": "s3b_translate",
    },
    "mcap_chown_required": {
        "op": "normalize_workdir_ownership",
        "detail": "normalize the MCAP case workdir ownership, then retry",
        "resume_from_stage": "s3b_translate",
    },
    "translate_service_unavailable": {
        "op": "wait_and_retry",
        "detail": "wait for the bounded translate service and retry",
        "resume_from_stage": "s3b_translate",
    },
    "remote_reader_timeout": {
        "op": "bounded_retry",
        "detail": "retry the remote reader within the same generation",
        "resume_from_stage": "s2_remote_read",
    },
}


def remediation_for(blocker: Any) -> dict[str, Any] | None:
    if not isinstance(blocker, Mapping):
        return None
    explicit = blocker.get("remediation")
    if isinstance(explicit, Mapping) and explicit:
        return dict(explicit)
    return _REMEDIATION.get(blocker_kind(blocker))
