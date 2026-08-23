"""Fail-closed, side-effect-free RCA evidence projections for B5 delivery."""

from __future__ import annotations

import json
import math
from typing import Any, Mapping, Sequence


MATERIALIZED_EVALUATOR_PROJECTION_SCHEMA_VERSION = (
    "pnc_rca_materialized_evaluator_projection_v1"
)
UNMATERIALIZED_CASE_PROJECTION_SCHEMA_VERSION = (
    "pnc_rca_unmaterialized_case_projection_v1"
)
GATE_A_PROJECTION_SCHEMA_VERSION = "pnc_rca_gate_a_projection_v2"
GATE_A_IDENTIFIER_BINDING_SCHEMA_VERSION = "pnc_rca_gate_a_identifier_binding_v1"
GATE_A_PROJECTION_MODE = "observed_facts_only"
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
    "viz_mcap_build_failed": (
        "可视化证据构建未完成；本次未取得可用于归因的完整分析证据。"
    ),
}
_MATERIALIZED_ENTRY_FIELDS = ("key", "domain", "pattern", "status")
MAX_PUBLIC_GATE_A_OBSERVATIONS = 8
_PUBLIC_GATE_A_STATUSES = frozenset({"supported", "refuted"})
_EVIDENCE_REF_IDENTIFIER_FIELDS = ("field", "signal")
_PUBLIC_IDENTIFIER_PUNCTUATION = frozenset("_./:-*[]()")
_EVIDENCE_REF_NUMERIC_FIELDS = (
    "count",
    "delta_m",
    "duration_s",
    "frame_match_count",
    "id_switch_count",
    "j2_problem_count",
    "line_match_count",
    "max",
    "max_abs",
    "max_abs_lat_m",
    "max_adjacent_delta_m",
    "max_adjacent_delta_mps",
    "max_delta",
    "min",
    "min_long_m",
    "residual_std_m",
    "temporal_problem_count",
    "threshold",
    "threshold_m",
    "threshold_mps",
    "threshold_mps2",
)
_FORBIDDEN_PUBLIC_EVIDENCE_TEXT = (
    "http://",
    "https://",
    "file://",
    "/mnt/",
    "//hfs",
    "candidate",
    "responsibility",
    "attribution",
    "候选",
    "责任",
    "归因",
    "因果",
    "导致",
    "责任方",
    "责任归因",
)
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


def _safe_public_identifier(value: Any) -> str | None:
    text = _nonempty_text(value)
    if text is None:
        return None
    lowered = text.lower()
    if (
        len(text) > 160
        or any(character.isspace() for character in text)
        or any(
            not character.isalnum() and character not in _PUBLIC_IDENTIFIER_PUNCTUATION
            for character in text
        )
        or any(token in lowered for token in _FORBIDDEN_PUBLIC_EVIDENCE_TEXT)
    ):
        raise RcaEvidenceProjectionError("evaluator_public_identifier_forbidden")
    return text


def _safe_public_evidence_identifier(value: Any) -> str | None:
    try:
        return _safe_public_identifier(value)
    except RcaEvidenceProjectionError as exc:
        raise RcaEvidenceProjectionError("evaluator_evidence_text_forbidden") from exc


def _identifier_inventory(value: Any, *, field: str) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise RcaEvidenceProjectionError("gate_a_identifier_binding_invalid", field)
    normalized = [_safe_public_identifier(item) for item in value]
    if (
        any(item is None for item in normalized)
        or len(set(normalized)) != len(normalized)
    ):
        raise RcaEvidenceProjectionError("gate_a_identifier_binding_invalid", field)
    return sorted(normalized)


def build_gate_a_identifier_binding(
    consumer_capability: Mapping[str, Any],
) -> dict[str, Any]:
    """Derive the only identifiers L1 may expose from the sealed capability row."""
    if not isinstance(consumer_capability, Mapping):
        raise RcaEvidenceProjectionError("gate_a_identifier_binding_invalid")
    raw_evaluators = consumer_capability.get("actual_evaluators")
    if not isinstance(raw_evaluators, Sequence) or isinstance(
        raw_evaluators, (str, bytes)
    ):
        raise RcaEvidenceProjectionError("gate_a_identifier_binding_invalid")
    evaluator_statuses: dict[str, str] = {}
    for item in raw_evaluators:
        if not isinstance(item, Mapping):
            raise RcaEvidenceProjectionError("gate_a_identifier_binding_invalid")
        evaluator_id = _safe_public_identifier(item.get("evaluator_id"))
        status = _nonempty_text(item.get("status"))
        if (
            evaluator_id is None
            or evaluator_id in evaluator_statuses
            or status not in EVALUATOR_STATUSES
        ):
            raise RcaEvidenceProjectionError("gate_a_identifier_binding_invalid")
        evaluator_statuses[evaluator_id] = status
    return {
        "schema_version": GATE_A_IDENTIFIER_BINDING_SCHEMA_VERSION,
        "evaluator_statuses": dict(sorted(evaluator_statuses.items())),
        "signals": _identifier_inventory(
            consumer_capability.get("actual_signals"), field="actual_signals"
        ),
        "fields": _identifier_inventory(
            consumer_capability.get("actual_fields"), field="actual_fields"
        ),
    }


def _validate_gate_a_identifier_binding(value: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != {
        "schema_version",
        "evaluator_statuses",
        "signals",
        "fields",
    }:
        raise RcaEvidenceProjectionError("gate_a_identifier_binding_invalid")
    evaluator_statuses = value.get("evaluator_statuses")
    if not isinstance(evaluator_statuses, Mapping):
        raise RcaEvidenceProjectionError("gate_a_identifier_binding_invalid")
    capability = {
        "actual_evaluators": [
            {"evaluator_id": key, "status": status}
            for key, status in evaluator_statuses.items()
        ],
        "actual_signals": value.get("signals"),
        "actual_fields": value.get("fields"),
    }
    normalized = build_gate_a_identifier_binding(capability)
    if (
        value.get("schema_version") != GATE_A_IDENTIFIER_BINDING_SCHEMA_VERSION
        or dict(value) != normalized
    ):
        raise RcaEvidenceProjectionError("gate_a_identifier_binding_invalid")
    return normalized


def _require_gate_a_identifier_binding(
    projection: Mapping[str, Any],
    binding: Mapping[str, Any],
) -> None:
    evaluator_statuses = binding["evaluator_statuses"]
    signals = set(binding["signals"])
    fields = set(binding["fields"])
    for evaluator in projection.get("evaluators") or []:
        key = evaluator.get("key")
        status = evaluator.get("status")
        if evaluator_statuses.get(key) != status:
            raise RcaEvidenceProjectionError("gate_a_identifier_binding_mismatch")
        for field in evaluator.get("missing_fields") or []:
            if field not in fields and field not in signals:
                raise RcaEvidenceProjectionError("gate_a_identifier_binding_mismatch")
        for check in evaluator.get("checks") or []:
            for field in check.get("evidence_fields") or []:
                if field not in fields and field not in signals:
                    raise RcaEvidenceProjectionError("gate_a_identifier_binding_mismatch")
        for reference in evaluator.get("evidence_refs") or []:
            if "signal" in reference and reference["signal"] not in signals:
                raise RcaEvidenceProjectionError("gate_a_identifier_binding_mismatch")
            if "field" in reference and reference["field"] not in fields:
                raise RcaEvidenceProjectionError("gate_a_identifier_binding_mismatch")
            for field in reference.get("fields") or []:
                if field not in fields and field not in signals:
                    raise RcaEvidenceProjectionError("gate_a_identifier_binding_mismatch")


def _project_numeric_facts(
    value: Any,
    *,
    code: str,
    ignore_bool: bool = False,
    ignore_numeric_sequences: bool = False,
) -> dict[str, int | float]:
    if not isinstance(value, Mapping):
        raise RcaEvidenceProjectionError(code)
    projected: dict[str, int | float] = {}
    for raw_key, raw_value in value.items():
        key = _safe_public_evidence_identifier(raw_key)
        if key is None:
            raise RcaEvidenceProjectionError(code)
        if isinstance(raw_value, bool):
            if ignore_bool:
                continue
            raise RcaEvidenceProjectionError(code)
        elif isinstance(raw_value, (int, float)):
            if not math.isfinite(float(raw_value)):
                raise RcaEvidenceProjectionError(code)
        elif (
            ignore_numeric_sequences
            and isinstance(raw_value, Sequence)
            and not isinstance(raw_value, (str, bytes))
            and 1 <= len(raw_value) <= 8
        ):
            if any(
                isinstance(item, bool)
                or not isinstance(item, (int, float))
                or not math.isfinite(float(item))
                for item in raw_value
            ):
                raise RcaEvidenceProjectionError(code)
            continue
        else:
            raise RcaEvidenceProjectionError(code)
        projected[key] = raw_value
    return projected


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
            entry["thresholds"] = _project_numeric_facts(
                thresholds,
                code="evaluator_check_thresholds_invalid",
                ignore_bool=True,
                ignore_numeric_sequences=True,
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
                normalized_fields = [
                    _safe_public_evidence_identifier(field) for field in fields
                ]
                if any(field is None for field in normalized_fields):
                    raise RcaEvidenceProjectionError("evaluator_evidence_field_invalid")
                entry["evidence_fields"] = normalized_fields
        if entry:
            projected.append(entry)
    return projected


def _project_evidence_ref_details(value: Any) -> list[dict[str, Any]]:
    """Keep only signal/window facts needed by the public L1 projection."""
    if value is None:
        return []
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise RcaEvidenceProjectionError("evaluator_evidence_refs_invalid")
    projected: list[dict[str, Any]] = []
    for reference in value:
        if not isinstance(reference, Mapping):
            raise RcaEvidenceProjectionError("evaluator_evidence_ref_invalid")
        entry: dict[str, Any] = {}
        for field in _EVIDENCE_REF_IDENTIFIER_FIELDS:
            text = _safe_public_evidence_identifier(reference.get(field))
            if text is not None:
                entry[field] = text
        fields = reference.get("fields")
        if fields is not None:
            if not isinstance(fields, Sequence) or isinstance(fields, (str, bytes)):
                raise RcaEvidenceProjectionError("evaluator_evidence_fields_invalid")
            normalized_fields = []
            for field in fields:
                normalized = _safe_public_evidence_identifier(field)
                if normalized is None:
                    raise RcaEvidenceProjectionError(
                        "evaluator_evidence_text_forbidden"
                    )
                normalized_fields.append(normalized)
            entry["fields"] = [field[:160] for field in normalized_fields]
        window = reference.get("window")
        if window is not None:
            if (
                not isinstance(window, Sequence)
                or isinstance(window, (str, bytes))
                or len(window) != 2
                or any(
                    isinstance(item, bool)
                    or not isinstance(item, (int, float))
                    or not math.isfinite(float(item))
                    for item in window
                )
            ):
                raise RcaEvidenceProjectionError("evaluator_evidence_window_invalid")
            entry["window"] = [float(window[0]), float(window[1])]
        metrics: dict[str, int | float] = {}
        for field in _EVIDENCE_REF_NUMERIC_FIELDS:
            metric = reference.get(field)
            if metric is None:
                continue
            if (
                isinstance(metric, bool)
                or not isinstance(metric, (int, float))
                or not math.isfinite(float(metric))
            ):
                raise RcaEvidenceProjectionError("evaluator_evidence_metric_invalid")
            metrics[field] = metric
        if metrics:
            entry["metrics"] = metrics
        if entry:
            projected.append(entry)
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
            value = _safe_public_identifier(evaluator.get(field))
            if value is None:
                absent.append(field)
            else:
                entry[field] = value
        window = evaluator.get("window")
        if window is not None:
            entry["window"] = _project_numeric_facts(
                window,
                code="evaluator_window_invalid",
            )
        missing_fields = evaluator.get("missing_fields")
        if missing_fields is not None:
            if not isinstance(missing_fields, Sequence) or isinstance(
                missing_fields, (str, bytes)
            ):
                raise RcaEvidenceProjectionError("evaluator_missing_fields_invalid")
            normalized_missing_fields = [
                _safe_public_evidence_identifier(field) for field in missing_fields
            ]
            if any(field is None for field in normalized_missing_fields):
                raise RcaEvidenceProjectionError("evaluator_missing_field_invalid")
            entry["missing_fields"] = normalized_missing_fields
        checks = _project_checks(evaluator.get("checks"))
        if checks:
            entry["checks"] = checks
        evidence_refs = _project_evidence_ref_details(evaluator.get("evidence_refs"))
        if evidence_refs:
            entry["evidence_refs"] = evidence_refs
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


def project_gate_a_report(
    source: Mapping[str, Any],
    *,
    identifier_binding: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the Gate A receipt from source facts without evaluating them.

    Gate A may publish only an honest abstention or already-materialized
    evaluator observations.  Responsibility, confidence, and causal claims
    are intentionally absent from this receipt; those belong to Gate B.
    """
    if not isinstance(source, Mapping):
        raise RcaEvidenceProjectionError("gate_a_source_invalid")
    evaluators = source.get("rca_evaluators")
    if evaluators:
        if source.get("input_materialized") is False:
            raise RcaEvidenceProjectionError("unmaterialized_evaluator_present")
        if (
            source.get("input_materialized") is not True
            and source.get("materialization_attested") is not True
        ):
            raise RcaEvidenceProjectionError("gate_a_materialization_state_missing")
        if identifier_binding is None:
            raise RcaEvidenceProjectionError("gate_a_identifier_binding_missing")
        normalized_binding = _validate_gate_a_identifier_binding(identifier_binding)
        if not isinstance(evaluators, Sequence) or isinstance(
            evaluators, (str, bytes)
        ):
            project_materialized_evaluator_evidence({"rca_evaluators": evaluators})
            raise AssertionError("invalid evaluator collection was not rejected")
        public_evaluators: list[dict[str, Any]] = []
        first_error: RcaEvidenceProjectionError | None = None
        for evaluator in evaluators:
            status = (
                _nonempty_text(evaluator.get("status"))
                if isinstance(evaluator, Mapping)
                else None
            )
            if status in EVALUATOR_STATUSES - _PUBLIC_GATE_A_STATUSES:
                continue
            try:
                materialized = project_materialized_evaluator_evidence(
                    {"rca_evaluators": [evaluator]}
                )
                projected = materialized["evaluators"][0]
                if not projected.get("evidence_refs"):
                    continue
                candidate_projection = {
                    **materialized,
                    "evaluators": [projected],
                }
                _require_gate_a_identifier_binding(
                    candidate_projection, normalized_binding
                )
            except RcaEvidenceProjectionError as exc:
                if first_error is None:
                    first_error = exc
                continue
            public_evaluators.append(projected)
        if not public_evaluators:
            if first_error is not None:
                raise first_error
            raise RcaEvidenceProjectionError("gate_a_observation_evidence_missing")
        evaluator_projection = {
            "schema_version": MATERIALIZED_EVALUATOR_PROJECTION_SCHEMA_VERSION,
            "input_materialized": True,
            "evaluators": public_evaluators,
        }
        return {
            "schema_version": GATE_A_PROJECTION_SCHEMA_VERSION,
            "mode": GATE_A_PROJECTION_MODE,
            "level": "L1_observation",
            "input_materialized": True,
            "evaluator_projection": evaluator_projection,
            "abstention": None,
            "identifier_binding": normalized_binding,
        }
    if source.get("input_materialized") is not False:
        raise RcaEvidenceProjectionError("gate_a_materialization_state_missing")
    anchor = project_unmaterialized_case_anchor(source)
    return {
        "schema_version": GATE_A_PROJECTION_SCHEMA_VERSION,
        "mode": GATE_A_PROJECTION_MODE,
        "level": "L0_abstain",
        "input_materialized": False,
        "evaluator_projection": None,
        "abstention": anchor,
        "identifier_binding": None,
    }


def _reproject_detached_evaluator_projection(
    projection: Mapping[str, Any],
) -> dict[str, Any]:
    """Rebuild source-shaped evaluators so canonical validation runs again."""
    evaluators = projection.get("evaluators")
    if not isinstance(evaluators, list) or not evaluators:
        raise RcaEvidenceProjectionError("gate_a_projection_invalid")
    allowed_entry_fields = {
        "status",
        "key",
        "domain",
        "pattern",
        "window",
        "missing_fields",
        "checks",
        "evidence_refs",
        "source_field_absent",
    }
    source_evaluators: list[dict[str, Any]] = []
    for entry in evaluators:
        if not isinstance(entry, Mapping) or not set(entry).issubset(
            allowed_entry_fields
        ):
            raise RcaEvidenceProjectionError("gate_a_projection_invalid")
        source: dict[str, Any] = {}
        for field in ("status", "key", "domain", "pattern", "window", "missing_fields"):
            if field in entry:
                source[field] = entry[field]
        if "checks" in entry:
            checks = entry["checks"]
            if not isinstance(checks, list):
                raise RcaEvidenceProjectionError("gate_a_projection_invalid")
            source_checks = []
            for check in checks:
                if not isinstance(check, Mapping) or not set(check).issubset({
                    "thresholds",
                    "evidence_fields",
                }):
                    raise RcaEvidenceProjectionError("gate_a_projection_invalid")
                source_check: dict[str, Any] = {}
                if "thresholds" in check:
                    source_check["thresholds"] = check["thresholds"]
                if "evidence_fields" in check:
                    source_check["evidence"] = {"fields": check["evidence_fields"]}
                source_checks.append(source_check)
            source["checks"] = source_checks
        if "evidence_refs" in entry:
            evidence_refs = entry["evidence_refs"]
            if not isinstance(evidence_refs, list):
                raise RcaEvidenceProjectionError("gate_a_projection_invalid")
            source_refs = []
            for reference in evidence_refs:
                if not isinstance(reference, Mapping):
                    raise RcaEvidenceProjectionError("gate_a_projection_invalid")
                source_reference = dict(reference)
                metrics = source_reference.pop("metrics", None)
                if metrics is not None:
                    if not isinstance(metrics, Mapping):
                        raise RcaEvidenceProjectionError("gate_a_projection_invalid")
                    source_reference.update(metrics)
                source_refs.append(source_reference)
            source["evidence_refs"] = source_refs
        source_evaluators.append(source)
    try:
        return project_materialized_evaluator_evidence({
            "rca_evaluators": source_evaluators,
        })
    except RcaEvidenceProjectionError as exc:
        raise RcaEvidenceProjectionError("gate_a_projection_invalid") from exc


def _reproject_detached_abstention(anchor: Mapping[str, Any]) -> dict[str, Any]:
    """Rebuild the L0 source so its fixed sentence and anchors are canonical."""
    anchors = anchor.get("anchors")
    if not isinstance(anchors, Mapping) or not set(anchors).issubset({
        "management_timestamp",
        "marker_time",
        "event_uuid",
    }):
        raise RcaEvidenceProjectionError("gate_a_projection_invalid")
    source: dict[str, Any] = {
        "input_materialized": False,
        "failure_class": anchor.get("failure_class"),
    }
    if "management_timestamp" in anchors:
        source["frame_lookup"] = {
            "management_timestamp": anchors["management_timestamp"],
        }
    for field in ("marker_time", "event_uuid"):
        if field in anchors:
            source[field] = anchors[field]
    try:
        return project_unmaterialized_case_anchor(source)
    except RcaEvidenceProjectionError as exc:
        raise RcaEvidenceProjectionError("gate_a_projection_invalid") from exc


def validate_gate_a_projection(
    value: Mapping[str, Any],
    *,
    identifier_binding: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Validate a detached Gate A receipt before it reaches rendering."""
    if not isinstance(value, Mapping):
        raise RcaEvidenceProjectionError("gate_a_projection_invalid")
    expected = {
        "schema_version",
        "mode",
        "level",
        "input_materialized",
        "evaluator_projection",
        "abstention",
        "identifier_binding",
    }
    if set(value) != expected or (
        value.get("schema_version") != GATE_A_PROJECTION_SCHEMA_VERSION
        or value.get("mode") != GATE_A_PROJECTION_MODE
        or not isinstance(value.get("input_materialized"), bool)
    ):
        raise RcaEvidenceProjectionError("gate_a_projection_invalid")
    level = value.get("level")
    if level == "L1_observation":
        projection = value.get("evaluator_projection")
        if not isinstance(projection, Mapping) or value.get("abstention") is not None:
            raise RcaEvidenceProjectionError("gate_a_projection_invalid")
        if set(projection) != {
            "schema_version",
            "input_materialized",
            "evaluators",
        } or (
            value.get("input_materialized") is not True
            or projection.get("schema_version")
            != MATERIALIZED_EVALUATOR_PROJECTION_SCHEMA_VERSION
            or projection.get("input_materialized") is not True
            or not isinstance(projection.get("evaluators"), list)
            or not projection["evaluators"]
        ):
            raise RcaEvidenceProjectionError("gate_a_projection_invalid")
        if dict(projection) != _reproject_detached_evaluator_projection(projection):
            raise RcaEvidenceProjectionError("gate_a_projection_invalid")
        embedded_binding = _validate_gate_a_identifier_binding(
            value.get("identifier_binding")
        )
        _require_gate_a_identifier_binding(projection, embedded_binding)
        if identifier_binding is not None and embedded_binding != (
            _validate_gate_a_identifier_binding(identifier_binding)
        ):
            raise RcaEvidenceProjectionError("gate_a_identifier_binding_mismatch")
        normalized = _json_copy(value, code="gate_a_projection_invalid")
        return normalized
    if level == "L0_abstain":
        anchor = value.get("abstention")
        if (
            value.get("evaluator_projection") is not None
            or value.get("input_materialized") is not False
            or not isinstance(anchor, Mapping)
            or value.get("identifier_binding") is not None
        ):
            raise RcaEvidenceProjectionError("gate_a_projection_invalid")
        if set(anchor) != {
            "schema_version",
            "input_materialized",
            "failure_class",
            "message",
            "anchors",
        }:
            raise RcaEvidenceProjectionError("gate_a_projection_invalid")
        if dict(anchor) != _reproject_detached_abstention(anchor):
            raise RcaEvidenceProjectionError("gate_a_projection_invalid")
        normalized = _json_copy(value, code="gate_a_projection_invalid")
        return normalized
    raise RcaEvidenceProjectionError("gate_a_projection_invalid")


def build_gate_a_public_result(value: Mapping[str, Any]) -> dict[str, Any]:
    """Render only L0/L1 facts for the Gate A public contract."""
    projection = validate_gate_a_projection(value)
    level = projection["level"]
    if level == "L1_observation":
        evaluators = [
            evaluator
            for evaluator in projection["evaluator_projection"]["evaluators"]
            if evaluator.get("status") in _PUBLIC_GATE_A_STATUSES
            and evaluator.get("evidence_refs")
        ]
        if not evaluators:
            raise RcaEvidenceProjectionError("gate_a_observation_evidence_missing")
        evaluators = sorted(
            evaluators,
            key=lambda item: 0 if item.get("status") == "supported" else 1,
        )
        visible = evaluators[:MAX_PUBLIC_GATE_A_OBSERVATIONS]
        refs = []
        narrative = []
        observations: list[dict[str, Any]] = []
        for evaluator in visible:
            key = str(evaluator.get("key") or "unknown evaluator")
            status = str(evaluator.get("status") or "unknown")
            details = evaluator.get("evidence_refs")
            fields: list[str] = []
            windows: list[str] = []
            metrics: dict[str, int | float] = {}
            for detail in details if isinstance(details, list) else []:
                if not isinstance(detail, Mapping):
                    continue
                for field in ("field", "signal"):
                    text = _nonempty_text(detail.get(field))
                    if text is not None and text not in fields:
                        fields.append(text)
                for field in detail.get("fields") or []:
                    text = _nonempty_text(field)
                    if text is not None and text not in fields:
                        fields.append(text)
                window = detail.get("window")
                if isinstance(window, list) and len(window) == 2:
                    label = f"{window[0]:g}~{window[1]:g}s"
                    if label not in windows:
                        windows.append(label)
                detail_metrics = detail.get("metrics")
                if isinstance(detail_metrics, Mapping):
                    for metric_key, metric_value in detail_metrics.items():
                        if metric_key not in metrics:
                            metrics[metric_key] = metric_value
            qualifiers = []
            if fields:
                qualifiers.append("信号 " + "、".join(fields[:4]))
            if windows:
                qualifiers.append("窗口 " + "、".join(windows[:2]))
            if metrics:
                metric_text = []
                for metric_key, metric_value in list(metrics.items())[:4]:
                    if isinstance(metric_value, float):
                        rendered_value = f"{metric_value:g}"
                    else:
                        rendered_value = str(metric_value)
                    metric_text.append(f"{metric_key}={rendered_value}")
                qualifiers.append("指标 " + "、".join(metric_text))
            if status == "supported":
                observed = f"已观测到评测项 {key} 的支持证据"
            else:
                observed = f"现有证据不支持评测项 {key}"
            if qualifiers:
                observed += "（" + "；".join(qualifiers) + "）"
            observed += "。"
            refs.append({"summary": observed})
            narrative.append({"role": "证据", "text": observed})
            observation: dict[str, Any] = {
                "key": key,
                "evaluator_key": key,
                "observation_kind": (
                    "支持性事实" if status == "supported" else "反驳性事实"
                ),
                "text": observed,
            }
            if fields:
                observation["signals"] = fields[:4]
            if windows:
                observation["windows"] = windows[:2]
            if metrics:
                observation["metrics"] = dict(list(metrics.items())[:6])
            # Keep the machine-readable observation separate from the
            # materialized evaluator projection; never expose checks/status
            # enums or missing-field internals on the public surface.
            observations.append(observation)
        omitted = len(evaluators) - len(visible)
        if omitted:
            omitted_text = f"另有 {omitted} 项证据观测保留在报告中，本条不展开。"
            refs.append({"summary": omitted_text})
            narrative.append({"role": "证据", "text": omitted_text})
        summary = {
            "short_conclusion": (
                f"自动RCA未归因：已投影 {len(evaluators)} 项证据观测；"
                "本次不输出责任归因。"
            )
        }
        boundary = [
            "Gate A 仅发布已物化的观测事实；域 golden 未绑定，责任归因留待 Gate B。"
        ]
        return {
            "summary": summary,
            "responsibility": {
                "status": "not_attributed",
                "candidate": "暂无法判断",
            },
            "causal_chain": {"narrative": narrative},
            "evidence_summary": {"refs": refs, "missing_evidence": boundary},
            "evidence_boundary": boundary,
            "evaluator_observations": observations,
            "evaluator_observation_count": len(evaluators),
            "evaluator_observation_omitted_count": omitted,
            "gate_a_level": level,
        }
    anchor = projection["abstention"]
    boundary = [str(anchor["message"])]
    return {
        "summary": {"short_conclusion": anchor["message"]},
        "responsibility": {
            "status": "not_attributed",
            "candidate": "暂无法判断",
        },
        "causal_chain": {"narrative": []},
        "evidence_summary": {"refs": [], "missing_evidence": boundary},
        "evidence_boundary": boundary,
        "evaluator_observations": [],
        "gate_a_level": level,
    }
