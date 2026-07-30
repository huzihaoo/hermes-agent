"""Fail-closed, side-effect-free RCA evidence projections for B5 delivery."""

from __future__ import annotations

import json
from typing import Any, Mapping, Sequence


MATERIALIZED_EVALUATOR_PROJECTION_SCHEMA_VERSION = (
    "pnc_rca_materialized_evaluator_projection_v1"
)
UNMATERIALIZED_CASE_PROJECTION_SCHEMA_VERSION = (
    "pnc_rca_unmaterialized_case_projection_v1"
)
EVALUATOR_STATUSES = frozenset({
    "need_fields",
    "not_applicable",
    "refuted",
    "supported",
    "unknown",
})
UNMATERIALIZED_FAILURE_MESSAGES = {
    "unsupported_function_domain": (
        "该问题不在当前已接入的功能证据范围内；本次未取得可用于归因的分析数据。"
    ),
    "remote_event_not_found": (
        "当前数据源未找到对应事件；本次未取得可用于归因的分析数据。"
    ),
    "remote_read_completeness_not_proven": (
        "当前无法证明远程读取完整；本次未取得可用于归因的分析数据。"
    ),
}
_MATERIALIZED_ENTRY_FIELDS = ("key", "domain", "pattern", "status")
_FORBIDDEN_UNMATERIALIZED_FIELD_TOKENS = (
    "url",
    "foxglove",
    "mcap",
    "report",
    "replay",
    "conclusion",
    "confidence",
)


class RcaEvidenceProjectionError(ValueError):
    """Raised when B5 evidence would cross a materialization boundary."""

    def __init__(self, code: str, detail: str = ""):
        self.code = str(code or "rca_evidence_projection_invalid")[:120]
        self.detail = str(detail or self.code)[:500]
        super().__init__(self.detail)


def _json_copy(value: Any, *, code: str) -> Any:
    """Return a detached JSON value or reject a non-JSON evidence value."""
    try:
        return json.loads(json.dumps(value, ensure_ascii=False, sort_keys=True))
    except (TypeError, ValueError) as exc:
        raise RcaEvidenceProjectionError(code) from exc


def _nonempty_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    return normalized or None


def _project_checks(value: Any) -> list[dict[str, Any]]:
    if value is None:
        return []
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise RcaEvidenceProjectionError("evaluator_checks_invalid")
    projected: list[dict[str, Any]] = []
    for check in value:
        if not isinstance(check, Mapping):
            raise RcaEvidenceProjectionError("evaluator_check_invalid")
        entry: dict[str, Any] = {}
        thresholds = check.get("thresholds")
        if thresholds is not None:
            if not isinstance(thresholds, Mapping):
                raise RcaEvidenceProjectionError("evaluator_check_thresholds_invalid")
            entry["thresholds"] = _json_copy(
                thresholds, code="evaluator_check_thresholds_not_json"
            )
        evidence = check.get("evidence")
        if evidence is not None:
            if not isinstance(evidence, Mapping):
                raise RcaEvidenceProjectionError("evaluator_check_evidence_invalid")
            fields = evidence.get("fields")
            if fields is not None:
                if not isinstance(fields, Sequence) or isinstance(fields, (str, bytes)):
                    raise RcaEvidenceProjectionError(
                        "evaluator_evidence_fields_invalid"
                    )
                if any(not _nonempty_text(field) for field in fields):
                    raise RcaEvidenceProjectionError("evaluator_evidence_field_invalid")
                entry["evidence_fields"] = [str(field).strip() for field in fields]
        if entry:
            projected.append(entry)
    return projected


def _project_evidence_refs(value: Any) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise RcaEvidenceProjectionError("evaluator_evidence_refs_invalid")
    projected: list[str] = []
    for reference in value:
        if not isinstance(reference, Mapping):
            raise RcaEvidenceProjectionError("evaluator_evidence_ref_invalid")
        evidence = _nonempty_text(reference.get("evidence"))
        if evidence is not None:
            projected.append(evidence)
    return projected


def project_materialized_evaluator_evidence(
    report_data: Mapping[str, Any],
) -> dict[str, Any]:
    """Project existing evaluator facts without evaluating, writing, or promoting them."""
    if not isinstance(report_data, Mapping):
        raise RcaEvidenceProjectionError("report_data_invalid")
    evaluators = report_data.get("rca_evaluators")
    if not isinstance(evaluators, Sequence) or isinstance(evaluators, (str, bytes)):
        raise RcaEvidenceProjectionError("materialized_evaluators_missing")
    if not evaluators:
        raise RcaEvidenceProjectionError("materialized_evaluators_empty")

    projected_evaluators: list[dict[str, Any]] = []
    for evaluator in evaluators:
        if not isinstance(evaluator, Mapping):
            raise RcaEvidenceProjectionError("materialized_evaluator_invalid")
        status = _nonempty_text(evaluator.get("status"))
        if status not in EVALUATOR_STATUSES:
            raise RcaEvidenceProjectionError("materialized_evaluator_status_invalid")
        entry: dict[str, Any] = {"status": status}
        absent: list[str] = []
        for field in _MATERIALIZED_ENTRY_FIELDS[:-1]:
            value = _nonempty_text(evaluator.get(field))
            if value is None:
                absent.append(field)
            else:
                entry[field] = value
        window = evaluator.get("window")
        if window is not None:
            if not isinstance(window, Mapping):
                raise RcaEvidenceProjectionError("evaluator_window_invalid")
            entry["window"] = _json_copy(window, code="evaluator_window_not_json")
        missing_fields = evaluator.get("missing_fields")
        if missing_fields is not None:
            if not isinstance(missing_fields, Sequence) or isinstance(
                missing_fields, (str, bytes)
            ):
                raise RcaEvidenceProjectionError("evaluator_missing_fields_invalid")
            if any(not _nonempty_text(field) for field in missing_fields):
                raise RcaEvidenceProjectionError("evaluator_missing_field_invalid")
            entry["missing_fields"] = [str(field).strip() for field in missing_fields]
        checks = _project_checks(evaluator.get("checks"))
        if checks:
            entry["checks"] = checks
        evidence = _project_evidence_refs(evaluator.get("evidence_refs"))
        if evidence:
            entry["evidence"] = evidence
        if absent:
            entry["source_field_absent"] = absent
        projected_evaluators.append(entry)

    return {
        "schema_version": MATERIALIZED_EVALUATOR_PROJECTION_SCHEMA_VERSION,
        "input_materialized": True,
        "evaluators": projected_evaluators,
    }


def _reject_unmaterialized_disclosure(value: Any) -> None:
    if isinstance(value, Mapping):
        for key, nested in value.items():
            normalized_key = str(key).lower()
            if any(
                token in normalized_key
                for token in _FORBIDDEN_UNMATERIALIZED_FIELD_TOKENS
            ):
                raise RcaEvidenceProjectionError("unmaterialized_disclosure_forbidden")
            _reject_unmaterialized_disclosure(nested)
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        for nested in value:
            _reject_unmaterialized_disclosure(nested)
    elif isinstance(value, str) and value.strip().lower().startswith((
        "http://",
        "https://",
    )):
        raise RcaEvidenceProjectionError("unmaterialized_disclosure_forbidden")


def project_unmaterialized_case_anchor(case: Mapping[str, Any]) -> dict[str, Any]:
    """Render only the fixed abstention sentence and already-known case anchors."""
    if not isinstance(case, Mapping):
        raise RcaEvidenceProjectionError("unmaterialized_case_invalid")
    if case.get("input_materialized") is not False:
        raise RcaEvidenceProjectionError("unmaterialized_input_required")
    if case.get("rca_evaluators"):
        raise RcaEvidenceProjectionError("unmaterialized_evaluator_present")
    _reject_unmaterialized_disclosure(case)

    failure_class = _nonempty_text(case.get("failure_class"))
    message = UNMATERIALIZED_FAILURE_MESSAGES.get(failure_class or "")
    if message is None:
        raise RcaEvidenceProjectionError("unmaterialized_failure_class_invalid")

    anchors: dict[str, Any] = {}
    frame_lookup = case.get("frame_lookup")
    if frame_lookup is not None:
        if not isinstance(frame_lookup, Mapping):
            raise RcaEvidenceProjectionError("unmaterialized_frame_lookup_invalid")
        management_timestamp = frame_lookup.get("management_timestamp")
        if management_timestamp is not None:
            if isinstance(management_timestamp, bool) or not isinstance(
                management_timestamp, (int, float, str)
            ):
                raise RcaEvidenceProjectionError(
                    "unmaterialized_management_timestamp_invalid"
                )
            anchors["management_timestamp"] = _json_copy(
                management_timestamp,
                code="unmaterialized_management_timestamp_not_json",
            )
    for name in ("marker_time", "event_uuid"):
        value = _nonempty_text(case.get(name))
        if value is not None:
            anchors[name] = value

    return {
        "schema_version": UNMATERIALIZED_CASE_PROJECTION_SCHEMA_VERSION,
        "input_materialized": False,
        "failure_class": failure_class,
        "message": message,
        "anchors": anchors,
    }
