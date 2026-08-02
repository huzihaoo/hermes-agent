"""Strict cross-plane release authority contracts for the PNC RCA path."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sqlite3
from typing import Any, Mapping, Sequence
from urllib.parse import urlparse


AUTHORITY_SCHEMA_VERSION = "pnc_rca_release_authority_v1"
POINTER_SCHEMA_VERSION = "pnc_rca_active_release_pointer_v1"
STAGE_STATUS_SCHEMA_VERSION = "pnc_rca_release_stage_status_v1"
COMPONENT_HEALTH_SCHEMA_VERSION = "pnc_rca_component_health_v1"
PROJECTION_AUDIT_SCHEMA_VERSION = "pnc_rca_release_projection_audit_v1"

AUTHORITY_STATUSES = frozenset({"candidate_only", "approved_for_activation"})
POINTER_STATES = frozenset({"candidate", "active"})
SIDE_EFFECT_MODES = frozenset({"disabled", "shadow", "canary", "enabled"})
STAGE_STATES = frozenset({"pass", "fail", "not_measured", "not_applicable"})
RELEASE_STAGES = (
    "candidate_validated",
    "installed",
    "resident_loaded",
    "control_plane_compatible",
    "execution_ready",
    "delivery_ready",
    "live_write_observed",
    "remote_receipt_proven",
)
FACE_NAMES = (
    "host_runtime",
    "vm_worker_state",
    "g1q3_rca_pipeline",
    "mcap_data_translate",
)
FACE_PROJECTIONS = {
    "host_runtime": ("runtime_engine", "gateway_runtime"),
    "vm_worker_state": ("vm_worker_state",),
    "g1q3_rca_pipeline": ("g1q3_rca_pipeline",),
    "mcap_data_translate": ("mcap_data_translate",),
}

_HEX40_RE = re.compile(r"^[0-9a-f]{40}$")
_HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$")
_COMPONENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,159}$")


class ReleaseAuthorityError(ValueError):
    """Stable validation failure for release authority artifacts."""

    def __init__(self, code: str, detail: str = ""):
        self.code = str(code or "rca_release_authority_invalid")[:160]
        self.detail = str(detail or self.code)[:1000]
        super().__init__(self.detail)


def canonical_json_bytes(value: Mapping[str, Any]) -> bytes:
    try:
        return json.dumps(
            dict(value),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ReleaseAuthorityError(
            "rca_release_authority_json_invalid", "artifact is not canonical JSON"
        ) from exc


def canonical_json_sha256(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _mapping(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ReleaseAuthorityError(
            "rca_release_authority_shape_invalid", f"{field} must be an object"
        )
    return value


def _exact_keys(value: Mapping[str, Any], expected: set[str], field: str) -> None:
    observed = set(value)
    if observed != expected:
        raise ReleaseAuthorityError(
            "rca_release_authority_fields_invalid",
            f"{field} fields differ: missing={sorted(expected - observed)} "
            f"extra={sorted(observed - expected)}",
        )


def _text(value: Any, field: str, *, pattern: re.Pattern[str] | None = None) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ReleaseAuthorityError(
            "rca_release_authority_field_invalid", f"{field} must be non-empty"
        )
    result = value.strip()
    if pattern is not None and pattern.fullmatch(result) is None:
        raise ReleaseAuthorityError(
            "rca_release_authority_field_invalid", f"{field} has invalid syntax"
        )
    return result


def _optional_hex(value: Any, field: str) -> str | None:
    if value is None:
        return None
    return _text(value, field, pattern=_HEX64_RE)


def _timestamp(value: Any, field: str) -> datetime:
    text = _text(value, field)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ReleaseAuthorityError(
            "rca_release_authority_time_invalid", f"{field} is not ISO-8601"
        ) from exc
    if parsed.tzinfo is None:
        raise ReleaseAuthorityError(
            "rca_release_authority_time_invalid", f"{field} lacks timezone"
        )
    return parsed.astimezone(timezone.utc)


def _absolute_path(value: Any, field: str) -> str:
    if isinstance(value, Path):
        value = str(value)
    text = _text(value, field)
    if not Path(text).is_absolute():
        raise ReleaseAuthorityError(
            "rca_release_authority_path_invalid", f"{field} must be absolute"
        )
    return text


def _validate_face(value: Any, field: str, *, mcap: bool = False) -> None:
    face = _mapping(value, field)
    expected = {"commit", "tree", "root"}
    if mcap:
        expected.add("contract_sha256")
    _exact_keys(face, expected, field)
    _text(face.get("commit"), f"{field}.commit", pattern=_HEX40_RE)
    _text(face.get("tree"), f"{field}.tree", pattern=_HEX40_RE)
    _absolute_path(face.get("root"), f"{field}.root")
    if mcap:
        _text(
            face.get("contract_sha256"),
            f"{field}.contract_sha256",
            pattern=_HEX64_RE,
        )


def _validate_candidate_measurement(
    *,
    status: str,
    values: Sequence[tuple[Any, str]],
    reason: Any,
    field: str,
) -> None:
    missing = [name for value, name in values if value is None]
    reason_text = str(reason or "").strip()
    if status == "approved_for_activation":
        if missing or reason_text:
            raise ReleaseAuthorityError(
                "rca_release_authority_approval_incomplete",
                f"{field} is incomplete for activation: {missing}",
            )
    elif missing and not reason_text:
        raise ReleaseAuthorityError(
            "rca_release_authority_measurement_reason_missing",
            f"{field} missing values require not_measured_reason",
        )


def validate_release_authority(value: Mapping[str, Any]) -> None:
    authority = _mapping(value, "authority")
    _exact_keys(
        authority,
        {
            "schema_version",
            "release_id",
            "authority_epoch_id",
            "created_at",
            "status",
            "supersedes_authority_sha256",
            "faces",
            "control_store",
            "quarantine_baseline",
            "side_effect_policy",
            "report_publication",
            "feishu_capability",
        },
        "authority",
    )
    if authority.get("schema_version") != AUTHORITY_SCHEMA_VERSION:
        raise ReleaseAuthorityError(
            "rca_release_authority_schema_invalid", "authority schema mismatch"
        )
    _text(authority.get("release_id"), "release_id", pattern=_IDENTIFIER_RE)
    _text(
        authority.get("authority_epoch_id"),
        "authority_epoch_id",
        pattern=_IDENTIFIER_RE,
    )
    _timestamp(authority.get("created_at"), "created_at")
    status = _text(authority.get("status"), "status")
    if status not in AUTHORITY_STATUSES:
        raise ReleaseAuthorityError(
            "rca_release_authority_status_invalid", f"unsupported status: {status}"
        )
    supersedes = authority.get("supersedes_authority_sha256")
    if supersedes not in {None, ""}:
        _text(supersedes, "supersedes_authority_sha256", pattern=_HEX64_RE)

    faces = _mapping(authority.get("faces"), "faces")
    _exact_keys(faces, set(FACE_NAMES), "faces")
    for name in FACE_NAMES:
        _validate_face(faces.get(name), f"faces.{name}", mcap=name == "mcap_data_translate")

    control = _mapping(authority.get("control_store"), "control_store")
    _exact_keys(
        control,
        {
            "schema_version",
            "database_instance_id",
            "schema_fingerprint_sha256",
            "backup_receipt_sha256",
            "not_measured_reason",
        },
        "control_store",
    )
    _text(control.get("schema_version"), "control_store.schema_version")
    _text(
        control.get("database_instance_id"),
        "control_store.database_instance_id",
        pattern=_IDENTIFIER_RE,
    )
    schema_fingerprint = _optional_hex(
        control.get("schema_fingerprint_sha256"),
        "control_store.schema_fingerprint_sha256",
    )
    backup_receipt = _optional_hex(
        control.get("backup_receipt_sha256"),
        "control_store.backup_receipt_sha256",
    )
    _validate_candidate_measurement(
        status=status,
        values=(
            (schema_fingerprint, "schema_fingerprint_sha256"),
            (backup_receipt, "backup_receipt_sha256"),
        ),
        reason=control.get("not_measured_reason"),
        field="control_store",
    )

    baseline = _mapping(authority.get("quarantine_baseline"), "quarantine_baseline")
    _exact_keys(
        baseline,
        {
            "state",
            "required",
            "schema_version",
            "baseline_sha256",
            "not_measured_reason",
        },
        "quarantine_baseline",
    )
    baseline_state = _text(baseline.get("state"), "quarantine_baseline.state")
    if baseline_state not in {"ready", "not_applicable", "not_measured"}:
        raise ReleaseAuthorityError(
            "rca_release_authority_baseline_invalid",
            f"unsupported baseline state: {baseline_state}",
        )
    required = baseline.get("required")
    if required is not None and not isinstance(required, bool):
        raise ReleaseAuthorityError(
            "rca_release_authority_baseline_invalid", "baseline.required is invalid"
        )
    baseline_schema = baseline.get("schema_version")
    baseline_sha = baseline.get("baseline_sha256")
    reason = str(baseline.get("not_measured_reason") or "").strip()
    if baseline_state == "ready":
        if required is not True or reason:
            raise ReleaseAuthorityError(
                "rca_release_authority_baseline_invalid",
                "ready baseline must be required and fully measured",
            )
        _text(baseline_schema, "quarantine_baseline.schema_version")
        _text(
            baseline_sha,
            "quarantine_baseline.baseline_sha256",
            pattern=_HEX64_RE,
        )
    elif baseline_state == "not_applicable":
        if required is not False or baseline_sha not in {None, ""} or reason:
            raise ReleaseAuthorityError(
                "rca_release_authority_baseline_invalid",
                "not-applicable baseline must be explicitly not required",
            )
        if baseline_schema not in {None, ""}:
            _text(baseline_schema, "quarantine_baseline.schema_version")
    else:
        if required is not None or baseline_schema is not None or baseline_sha is not None:
            raise ReleaseAuthorityError(
                "rca_release_authority_baseline_invalid",
                "unmeasured baseline cannot assert identity or applicability",
            )
        if not reason:
            raise ReleaseAuthorityError(
                "rca_release_authority_measurement_reason_missing",
                "unmeasured baseline requires a reason",
            )
    if status == "approved_for_activation" and baseline_state == "not_measured":
        raise ReleaseAuthorityError(
            "rca_release_authority_approval_incomplete",
            "approved authority cannot have an unmeasured baseline",
        )

    side_effect = _mapping(authority.get("side_effect_policy"), "side_effect_policy")
    _exact_keys(
        side_effect,
        {
            "mode",
            "single_active_writer",
            "allow_historical_requeue",
            "allowed_effect_kinds",
        },
        "side_effect_policy",
    )
    mode = _text(side_effect.get("mode"), "side_effect_policy.mode")
    if mode not in SIDE_EFFECT_MODES:
        raise ReleaseAuthorityError(
            "rca_release_authority_side_effect_invalid", f"unsupported mode: {mode}"
        )
    if side_effect.get("single_active_writer") is not True:
        raise ReleaseAuthorityError(
            "rca_release_authority_side_effect_invalid",
            "single_active_writer must be true",
        )
    if side_effect.get("allow_historical_requeue") is not False:
        raise ReleaseAuthorityError(
            "rca_release_authority_side_effect_invalid",
            "historical requeue must remain false",
        )
    kinds = side_effect.get("allowed_effect_kinds")
    if not isinstance(kinds, list) or not kinds or len(set(kinds)) != len(kinds):
        raise ReleaseAuthorityError(
            "rca_release_authority_side_effect_invalid",
            "allowed effect kinds must be a unique non-empty list",
        )
    for index, kind in enumerate(kinds):
        _text(kind, f"side_effect_policy.allowed_effect_kinds[{index}]", pattern=_IDENTIFIER_RE)

    publication = _mapping(authority.get("report_publication"), "report_publication")
    _exact_keys(
        publication,
        {"canonical_base_url", "root", "manifest_schema_version"},
        "report_publication",
    )
    base_url = _text(
        publication.get("canonical_base_url"),
        "report_publication.canonical_base_url",
    )
    parsed_url = urlparse(base_url)
    if parsed_url.scheme not in {"http", "https"} or not parsed_url.netloc:
        raise ReleaseAuthorityError(
            "rca_release_authority_report_invalid", "canonical report URL is invalid"
        )
    _absolute_path(publication.get("root"), "report_publication.root")
    _text(
        publication.get("manifest_schema_version"),
        "report_publication.manifest_schema_version",
        pattern=_IDENTIFIER_RE,
    )

    feishu = _mapping(authority.get("feishu_capability"), "feishu_capability")
    _exact_keys(
        feishu,
        {"required_surfaces", "capability_profile_sha256", "not_measured_reason"},
        "feishu_capability",
    )
    surfaces = feishu.get("required_surfaces")
    if not isinstance(surfaces, list) or not surfaces or len(set(surfaces)) != len(surfaces):
        raise ReleaseAuthorityError(
            "rca_release_authority_feishu_invalid",
            "required surfaces must be a unique non-empty list",
        )
    for index, surface in enumerate(surfaces):
        _text(surface, f"feishu_capability.required_surfaces[{index}]", pattern=_IDENTIFIER_RE)
    profile_sha = _optional_hex(
        feishu.get("capability_profile_sha256"),
        "feishu_capability.capability_profile_sha256",
    )
    _validate_candidate_measurement(
        status=status,
        values=((profile_sha, "capability_profile_sha256"),),
        reason=feishu.get("not_measured_reason"),
        field="feishu_capability",
    )


def build_active_pointer(
    authority: Mapping[str, Any],
    *,
    authority_path: str | Path,
    state: str,
    activated_at: str,
    previous_authority_sha256: str | None = None,
) -> dict[str, Any]:
    validate_release_authority(authority)
    selected_state = _text(state, "pointer.state")
    if selected_state not in POINTER_STATES:
        raise ReleaseAuthorityError(
            "rca_release_pointer_state_invalid", f"unsupported state: {selected_state}"
        )
    if (
        selected_state == "active"
        and authority.get("status") != "approved_for_activation"
    ):
        raise ReleaseAuthorityError(
            "rca_release_pointer_activation_invalid",
            "candidate-only authority cannot be activated",
        )
    _timestamp(activated_at, "pointer.activated_at")
    authority_path_text = _absolute_path(authority_path, "pointer.authority_path")
    previous = previous_authority_sha256 or None
    if previous is not None:
        _text(previous, "pointer.previous_authority_sha256", pattern=_HEX64_RE)
    return {
        "schema_version": POINTER_SCHEMA_VERSION,
        "state": selected_state,
        "release_id": str(authority["release_id"]),
        "authority_path": authority_path_text,
        "authority_sha256": canonical_json_sha256(authority),
        "activated_at": activated_at,
        "previous_authority_sha256": previous,
    }


def validate_active_pointer(
    pointer: Mapping[str, Any],
    authority: Mapping[str, Any],
    *,
    expected_authority_path: str | Path | None = None,
) -> None:
    validate_release_authority(authority)
    selected = _mapping(pointer, "pointer")
    _exact_keys(
        selected,
        {
            "schema_version",
            "state",
            "release_id",
            "authority_path",
            "authority_sha256",
            "activated_at",
            "previous_authority_sha256",
        },
        "pointer",
    )
    if selected.get("schema_version") != POINTER_SCHEMA_VERSION:
        raise ReleaseAuthorityError(
            "rca_release_pointer_schema_invalid", "pointer schema mismatch"
        )
    state = _text(selected.get("state"), "pointer.state")
    if state not in POINTER_STATES:
        raise ReleaseAuthorityError(
            "rca_release_pointer_state_invalid", f"unsupported state: {state}"
        )
    if state == "active" and authority.get("status") != "approved_for_activation":
        raise ReleaseAuthorityError(
            "rca_release_pointer_activation_invalid",
            "candidate-only authority cannot be active",
        )
    if selected.get("release_id") != authority.get("release_id"):
        raise ReleaseAuthorityError(
            "rca_release_pointer_release_mismatch", "pointer release differs"
        )
    if selected.get("authority_sha256") != canonical_json_sha256(authority):
        raise ReleaseAuthorityError(
            "rca_release_pointer_digest_mismatch", "pointer digest differs"
        )
    authority_path = _absolute_path(
        selected.get("authority_path"), "pointer.authority_path"
    )
    if expected_authority_path is not None and Path(authority_path) != Path(
        expected_authority_path
    ).expanduser().absolute():
        raise ReleaseAuthorityError(
            "rca_release_pointer_path_mismatch", "pointer authority path differs"
        )
    _timestamp(selected.get("activated_at"), "pointer.activated_at")
    previous = selected.get("previous_authority_sha256")
    if previous is not None:
        _text(previous, "pointer.previous_authority_sha256", pattern=_HEX64_RE)


def _validate_stage_item(value: Any, field: str) -> None:
    item = _mapping(value, field)
    _exact_keys(item, {"status", "observed_at", "evidence_sha256", "reason"}, field)
    status = _text(item.get("status"), f"{field}.status")
    if status not in STAGE_STATES:
        raise ReleaseAuthorityError(
            "rca_release_stage_status_invalid", f"unsupported stage status: {status}"
        )
    _timestamp(item.get("observed_at"), f"{field}.observed_at")
    evidence = item.get("evidence_sha256")
    reason = str(item.get("reason") or "").strip()
    if status in {"pass", "fail"}:
        _text(evidence, f"{field}.evidence_sha256", pattern=_HEX64_RE)
        if status == "fail" and not reason:
            raise ReleaseAuthorityError(
                "rca_release_stage_reason_missing", f"{field} failure lacks reason"
            )
        if status == "pass" and reason:
            raise ReleaseAuthorityError(
                "rca_release_stage_reason_invalid", f"{field} pass cannot have reason"
            )
    elif evidence is not None or not reason:
        raise ReleaseAuthorityError(
            "rca_release_stage_measurement_invalid",
            f"{field} unmeasured/not-applicable state is incomplete",
        )


def validate_stage_status(value: Mapping[str, Any]) -> None:
    status = _mapping(value, "stage_status")
    _exact_keys(
        status,
        {"schema_version", "authority_sha256", "observed_at", "stages"},
        "stage_status",
    )
    if status.get("schema_version") != STAGE_STATUS_SCHEMA_VERSION:
        raise ReleaseAuthorityError(
            "rca_release_stage_schema_invalid", "stage status schema mismatch"
        )
    _text(status.get("authority_sha256"), "authority_sha256", pattern=_HEX64_RE)
    _timestamp(status.get("observed_at"), "observed_at")
    stages = _mapping(status.get("stages"), "stages")
    _exact_keys(stages, set(RELEASE_STAGES), "stages")
    for name in RELEASE_STAGES:
        _validate_stage_item(stages.get(name), f"stages.{name}")


def production_completion_proven(value: Mapping[str, Any]) -> bool:
    validate_stage_status(value)
    stages = _mapping(value.get("stages"), "stages")
    return all(_mapping(stages[name], name).get("status") == "pass" for name in RELEASE_STAGES)


def _validate_health_dimension(value: Any, field: str) -> str:
    item = _mapping(value, field)
    _exact_keys(item, {"status", "evidence_sha256", "reason"}, field)
    status = _text(item.get("status"), f"{field}.status")
    if status not in STAGE_STATES:
        raise ReleaseAuthorityError(
            "rca_component_health_status_invalid", f"unsupported status: {status}"
        )
    evidence = item.get("evidence_sha256")
    reason = str(item.get("reason") or "").strip()
    if status in {"pass", "fail"}:
        _text(evidence, f"{field}.evidence_sha256", pattern=_HEX64_RE)
        if status == "fail" and not reason:
            raise ReleaseAuthorityError(
                "rca_component_health_reason_missing", f"{field} failure lacks reason"
            )
        if status == "pass" and reason:
            raise ReleaseAuthorityError(
                "rca_component_health_reason_invalid", f"{field} pass cannot have reason"
            )
    elif evidence is not None or not reason:
        raise ReleaseAuthorityError(
            "rca_component_health_measurement_invalid",
            f"{field} unmeasured/not-applicable state is incomplete",
        )
    return status


def validate_component_health(
    value: Mapping[str, Any], *, now: datetime | None = None
) -> None:
    health = _mapping(value, "component_health")
    _exact_keys(
        health,
        {
            "schema_version",
            "component",
            "authority_sha256",
            "observed_at",
            "freshness_ttl_seconds",
            "pid",
            "started_at",
            "executable",
            "executable_sha256",
            "process_health",
            "dependency_health",
            "readiness",
            "side_effect_mode",
        },
        "component_health",
    )
    if health.get("schema_version") != COMPONENT_HEALTH_SCHEMA_VERSION:
        raise ReleaseAuthorityError(
            "rca_component_health_schema_invalid", "component health schema mismatch"
        )
    _text(health.get("component"), "component", pattern=_COMPONENT_RE)
    _text(health.get("authority_sha256"), "authority_sha256", pattern=_HEX64_RE)
    observed = _timestamp(health.get("observed_at"), "observed_at")
    ttl = health.get("freshness_ttl_seconds")
    if isinstance(ttl, bool) or not isinstance(ttl, int) or not 1 <= ttl <= 3600:
        raise ReleaseAuthorityError(
            "rca_component_health_ttl_invalid", "freshness TTL is invalid"
        )
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    age = (current - observed).total_seconds()
    if age < -5 or age > ttl:
        raise ReleaseAuthorityError(
            "rca_component_health_stale", f"health age {age:.3f}s exceeds TTL"
        )
    process_status = _validate_health_dimension(
        health.get("process_health"), "process_health"
    )
    dependency_status = _validate_health_dimension(
        health.get("dependency_health"), "dependency_health"
    )
    readiness = _validate_health_dimension(health.get("readiness"), "readiness")
    side_effect_mode = _text(health.get("side_effect_mode"), "side_effect_mode")
    if side_effect_mode not in SIDE_EFFECT_MODES:
        raise ReleaseAuthorityError(
            "rca_component_health_side_effect_invalid",
            f"unsupported side-effect mode: {side_effect_mode}",
        )
    pid = health.get("pid")
    if process_status == "pass":
        if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
            raise ReleaseAuthorityError(
                "rca_component_health_process_invalid", "passing process lacks PID"
            )
        _timestamp(health.get("started_at"), "started_at")
        _absolute_path(health.get("executable"), "executable")
        _text(
            health.get("executable_sha256"),
            "executable_sha256",
            pattern=_HEX64_RE,
        )
    elif any(
        health.get(name) is not None
        for name in ("pid", "started_at", "executable", "executable_sha256")
    ):
        raise ReleaseAuthorityError(
            "rca_component_health_process_invalid",
            "non-passing process cannot assert resident identity",
        )
    if readiness == "pass" and (
        process_status != "pass"
        or dependency_status != "pass"
        or side_effect_mode == "disabled"
    ):
        raise ReleaseAuthorityError(
            "rca_component_health_readiness_invalid",
            "readiness pass disagrees with process, dependencies, or disabled mode",
        )


def component_ready(value: Mapping[str, Any], *, now: datetime | None = None) -> bool:
    validate_component_health(value, now=now)
    return all(
        _mapping(value[name], name).get("status") == "pass"
        for name in ("process_health", "dependency_health", "readiness")
    ) and value.get("side_effect_mode") != "disabled"


def read_control_store_schema(path: str | Path) -> str:
    selected = Path(path).expanduser().absolute()
    try:
        connection = sqlite3.connect(f"{selected.as_uri()}?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        row = connection.execute(
            "SELECT value FROM control_meta WHERE key='schema_version'"
        ).fetchone()
    except (OSError, sqlite3.Error) as exc:
        raise ReleaseAuthorityError(
            "rca_release_control_store_unavailable", "control store cannot be read"
        ) from exc
    finally:
        if "connection" in locals():
            connection.close()
    if row is None:
        raise ReleaseAuthorityError(
            "rca_release_control_store_schema_missing", "schema marker is missing"
        )
    return _text(row["value"], "control_store.schema_version")


def audit_release_projections(
    authority: Mapping[str, Any],
    *,
    pointer: Mapping[str, Any] | None = None,
    authority_path: str | Path | None = None,
    live_manifest: Mapping[str, Any] | None = None,
    active_binding: Mapping[str, Any] | None = None,
    control_store_path: str | Path | None = None,
    health_artifacts: Sequence[Mapping[str, Any]] = (),
    now: datetime | None = None,
) -> dict[str, Any]:
    validate_release_authority(authority)
    authority_sha = canonical_json_sha256(authority)
    errors: list[dict[str, str]] = []
    checks: dict[str, Any] = {}

    def record_error(code: str, detail: str) -> None:
        errors.append({"code": code, "detail": detail})

    if pointer is not None:
        try:
            validate_active_pointer(
                pointer,
                authority,
                expected_authority_path=authority_path,
            )
            checks["pointer"] = "pass"
        except ReleaseAuthorityError as exc:
            checks["pointer"] = "fail"
            record_error(exc.code, exc.detail)

    if live_manifest is not None:
        manifest = _mapping(live_manifest, "live_manifest")
        reference = manifest.get("rca_release_authority")
        if not isinstance(reference, Mapping):
            record_error(
                "rca_release_live_manifest_authority_missing",
                "LIVE_MANIFEST lacks rca_release_authority",
            )
        elif (
            reference.get("release_id") != authority.get("release_id")
            or reference.get("authority_sha256") != authority_sha
        ):
            record_error(
                "rca_release_live_manifest_authority_mismatch",
                "LIVE_MANIFEST authority reference differs",
            )
        manifest_faces = manifest.get("face_git_bindings")
        if not isinstance(manifest_faces, Mapping):
            record_error(
                "rca_release_live_manifest_faces_missing",
                "LIVE_MANIFEST face_git_bindings is missing",
            )
        else:
            authority_faces = _mapping(authority.get("faces"), "faces")
            for face_name, projection_names in FACE_PROJECTIONS.items():
                projected = next(
                    (
                        manifest_faces[name]
                        for name in projection_names
                        if isinstance(manifest_faces.get(name), Mapping)
                    ),
                    None,
                )
                if projected is None:
                    record_error(
                        "rca_release_live_manifest_face_missing",
                        f"LIVE_MANIFEST lacks {face_name}",
                    )
                    continue
                expected = _mapping(authority_faces.get(face_name), face_name)
                if (
                    projected.get("commit") != expected.get("commit")
                    or projected.get("tree") != expected.get("tree")
                    or projected.get("repo") != expected.get("root")
                ):
                    record_error(
                        "rca_release_live_manifest_face_mismatch",
                        f"LIVE_MANIFEST {face_name} differs",
                    )
            host = _mapping(authority_faces.get("host_runtime"), "host_runtime")
            if manifest.get("runtime_root") != host.get("root"):
                record_error(
                    "rca_release_live_manifest_runtime_root_mismatch",
                    "LIVE_MANIFEST runtime_root differs",
                )
        checks["live_manifest"] = "pass" if not any(
            item["code"].startswith("rca_release_live_manifest") for item in errors
        ) else "fail"

    if active_binding is not None:
        binding = _mapping(active_binding, "active_binding")
        if (
            binding.get("release_id") != authority.get("release_id")
            or binding.get("authority_sha256") != authority_sha
            or binding.get("authority_epoch_id")
            != authority.get("authority_epoch_id")
        ):
            record_error(
                "rca_release_active_binding_authority_mismatch",
                "active binding does not reference the exact authority",
            )
            checks["active_binding"] = "fail"
        else:
            checks["active_binding"] = "pass"

    if control_store_path is not None:
        expected_schema = _mapping(
            authority.get("control_store"), "control_store"
        ).get("schema_version")
        try:
            observed_schema = read_control_store_schema(control_store_path)
            checks["control_store"] = {
                "status": "pass" if observed_schema == expected_schema else "fail",
                "expected_schema_version": expected_schema,
                "observed_schema_version": observed_schema,
            }
            if observed_schema != expected_schema:
                record_error(
                    "rca_release_control_store_schema_mismatch",
                    "control store schema differs from authority",
                )
        except ReleaseAuthorityError as exc:
            checks["control_store"] = "fail"
            record_error(exc.code, exc.detail)

    health_checks: list[dict[str, Any]] = []
    for index, health in enumerate(health_artifacts):
        component = str(health.get("component") or f"health[{index}]")
        try:
            validate_component_health(health, now=now)
            if health.get("authority_sha256") != authority_sha:
                raise ReleaseAuthorityError(
                    "rca_component_health_authority_mismatch",
                    f"{component} authority differs",
                )
            health_checks.append({"component": component, "status": "pass"})
        except ReleaseAuthorityError as exc:
            health_checks.append(
                {"component": component, "status": "fail", "code": exc.code}
            )
            record_error(exc.code, exc.detail)
    if health_artifacts:
        checks["component_health"] = health_checks

    return {
        "schema_version": PROJECTION_AUDIT_SCHEMA_VERSION,
        "ok": not errors,
        "release_id": authority["release_id"],
        "authority_sha256": authority_sha,
        "checks": checks,
        "errors": errors,
        "read_only_attestation": {
            "database_open_mode": "mode=ro + PRAGMA query_only=ON",
            "production_mutation_performed": False,
            "external_effects_triggered": False,
        },
    }
