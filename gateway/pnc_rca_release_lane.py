"""Fail-closed release-lane classification for the RCA production path."""

from __future__ import annotations

import hashlib
import json
from pathlib import PurePosixPath
import re
from typing import Any, Mapping, Sequence


RELEASE_LANE_SCHEMA_VERSION = "pnc_rca_release_lane_decision_v1"
RELEASE_LANES = frozenset(
    {"critical_full", "resident_targeted", "vm_task_fast", "noncritical_offline"}
)
CAPACITY_PROFILE = "rca_prod_80pct"
_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER = re.compile(r"^[a-z0-9][a-z0-9._-]{5,127}$")
_DECISION_FIELDS = frozenset(
    {
        "schema_version",
        "release_lane",
        "changed_paths",
        "dependency_closure",
        "affected_faces",
        "restart_targets",
        "criticality",
        "dependency_closure_sha256",
        "validation_manifest_sha256",
        "rollback_release_id",
        "rollback_release_note_sha256",
        "capacity_profile",
        "max_concurrency",
        "import_closure_complete",
        "classification_reason",
    }
)

_CRITICAL_PREFIXES = (
    "gateway/pnc_rca_control_store.py",
    "gateway/pnc_rca_delivery_store.py",
    "gateway/pnc_rca_prod_admission.py",
    "gateway/pnc_rca_vm_release_binding.py",
    "gateway/pnc_rca_write_fence.py",
    "scripts/pnc_fault_taxonomy.py",
    "scripts/pnc_rca_delivery_collector.py",
    "scripts/pnc_rca_delivery_dispatcher.py",
    "scripts/pnc_rca_kafka_consumer.py",
    "scripts/pnc_rca_minimal_release.py",
    "scripts/pnc_rca_outbox_dispatcher.py",
    "tools/vm_task_tool.py",
)
_VM_TASK_FAST_PREFIXES = (
    "api/g1q3_rca/evaluators/",
    "api/g1q3_rca/report/",
    "api/g1q3_rca/data_adapters/",
    "g1q3_rca/evaluators/",
    "g1q3_rca/report/",
    "g1q3_rca/data_adapters/",
)
_NONCRITICAL_PREFIXES = ("docs/", "fixtures/", "tests/")
_RESIDENT_TARGETED_PREFIXES = (
    "scripts/pnc_rca_requester_identity_report.py",
    "scripts/pnc_rca_status_report.py",
)
_FULL_RESTART_TARGETS = (
    "ai.hermes.gateway",
    "local.pnc.rca-outbox-dispatcher",
    "local.pnc.rca-delivery-collector",
    "local.pnc.rca-delivery-dispatcher",
)


class ReleaseLaneError(ValueError):
    def __init__(self, code: str, detail: str = ""):
        self.code = str(code or "release_lane_invalid")[:120]
        self.detail = str(detail or self.code)[:1000]
        super().__init__(self.detail)


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    ).encode("ascii")


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _paths(values: Sequence[str], label: str) -> list[str]:
    result: list[str] = []
    for raw in values:
        value = str(raw or "").strip()
        path = PurePosixPath(value)
        if (
            not value
            or path.is_absolute()
            or ".." in path.parts
            or value.startswith("./")
        ):
            raise ReleaseLaneError(f"{label}_invalid", value)
        result.append(value)
    if not result or len(result) != len(set(result)):
        raise ReleaseLaneError(f"{label}_invalid")
    return sorted(result)


def dependency_closure_sha256(paths: Sequence[str]) -> str:
    return _sha256(canonical_bytes(_paths(paths, "dependency_closure")))


def _matches(path: str, prefixes: Sequence[str]) -> bool:
    return any(path == prefix or path.startswith(prefix) for prefix in prefixes)


def _automatic_lane(
    changed_paths: Sequence[str], dependency_closure: Sequence[str], complete: bool
) -> tuple[str, str]:
    all_paths = set(changed_paths) | set(dependency_closure)
    if not complete:
        return "critical_full", "import_closure_incomplete_auto_upgrade"
    if any(_matches(path, _CRITICAL_PREFIXES) for path in all_paths):
        return "critical_full", "critical_identity_or_delivery_closure"
    if all(_matches(path, _VM_TASK_FAST_PREFIXES) for path in all_paths):
        return "vm_task_fast", "vm_subprocess_only_closure"
    if all(_matches(path, _NONCRITICAL_PREFIXES) for path in all_paths):
        return "noncritical_offline", "offline_only_closure"
    if all(_matches(path, _RESIDENT_TARGETED_PREFIXES) for path in all_paths):
        return "resident_targeted", "single_resident_face_closure"
    return "critical_full", "unknown_closure_auto_upgrade"


def _faces_and_restarts(lane: str, paths: Sequence[str]) -> tuple[list[str], list[str]]:
    if lane == "vm_task_fast":
        return ["vm_task_subprocess"], []
    if lane == "noncritical_offline":
        return ["offline_validation"], []
    if lane == "resident_targeted":
        return ["host_status_reporter"], ["local.pnc.rca-status-report"]
    faces: set[str] = set()
    for path in paths:
        if path in {
            "gateway/pnc_rca_vm_release_binding.py",
            "scripts/pnc_rca_outbox_dispatcher.py",
            "tools/vm_task_tool.py",
        }:
            faces.add("host_outbox_dispatcher")
        if path in {
            "gateway/pnc_rca_vm_release_binding.py",
            "scripts/pnc_fault_taxonomy.py",
            "scripts/pnc_rca_delivery_collector.py",
        }:
            faces.add("host_delivery_collector")
        if path == "scripts/pnc_rca_delivery_dispatcher.py":
            faces.add("host_delivery_dispatcher")
        if path == "scripts/pnc_rca_kafka_consumer.py":
            faces.add("host_kafka_consumer")
        if path.startswith("gateway/") or path == "scripts/pnc_rca_minimal_release.py":
            faces.add("host_release_contract")
    if not faces:
        faces.add("host_critical_runtime")
    return sorted(faces), list(_FULL_RESTART_TARGETS)


def build_release_lane_decision(
    *,
    changed_paths: Sequence[str],
    dependency_closure: Sequence[str],
    validation_manifest_sha256: str,
    rollback_release_id: str,
    rollback_release_note_sha256: str,
    import_closure_complete: bool,
    max_concurrency: int = 1,
) -> dict[str, Any]:
    changed = _paths(changed_paths, "changed_paths")
    closure = _paths(dependency_closure, "dependency_closure")
    if not set(changed).issubset(closure):
        raise ReleaseLaneError("dependency_closure_missing_changed_path")
    if not isinstance(import_closure_complete, bool):
        raise ReleaseLaneError("import_closure_complete_invalid")
    if (
        _HEX64.fullmatch(str(validation_manifest_sha256 or "")) is None
        or _HEX64.fullmatch(str(rollback_release_note_sha256 or "")) is None
        or _IDENTIFIER.fullmatch(str(rollback_release_id or "")) is None
        or isinstance(max_concurrency, bool)
        or not isinstance(max_concurrency, int)
        or max_concurrency < 1
        or max_concurrency > 4
    ):
        raise ReleaseLaneError("release_lane_binding_invalid")
    lane, reason = _automatic_lane(changed, closure, import_closure_complete)
    faces, restart_targets = _faces_and_restarts(lane, closure)
    decision = {
        "schema_version": RELEASE_LANE_SCHEMA_VERSION,
        "release_lane": lane,
        "changed_paths": changed,
        "dependency_closure": closure,
        "affected_faces": faces,
        "restart_targets": restart_targets,
        "criticality": (
            "critical"
            if lane == "critical_full"
            else "resident"
            if lane == "resident_targeted"
            else "vm_task"
            if lane == "vm_task_fast"
            else "offline"
        ),
        "dependency_closure_sha256": dependency_closure_sha256(closure),
        "validation_manifest_sha256": validation_manifest_sha256,
        "rollback_release_id": rollback_release_id,
        "rollback_release_note_sha256": rollback_release_note_sha256,
        "capacity_profile": CAPACITY_PROFILE,
        "max_concurrency": max_concurrency,
        "import_closure_complete": import_closure_complete,
        "classification_reason": reason,
    }
    return validate_release_lane_decision(decision)


def validate_release_lane_decision(value: object) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != _DECISION_FIELDS:
        raise ReleaseLaneError("release_lane_contract_invalid")
    decision = dict(value)
    try:
        changed = _paths(decision["changed_paths"], "changed_paths")
        closure = _paths(decision["dependency_closure"], "dependency_closure")
    except (TypeError, ReleaseLaneError) as exc:
        if isinstance(exc, ReleaseLaneError):
            raise
        raise ReleaseLaneError("release_lane_contract_invalid") from exc
    if (
        decision.get("schema_version") != RELEASE_LANE_SCHEMA_VERSION
        or decision.get("release_lane") not in RELEASE_LANES
        or changed != decision.get("changed_paths")
        or closure != decision.get("dependency_closure")
        or not set(changed).issubset(closure)
        or decision.get("dependency_closure_sha256")
        != dependency_closure_sha256(closure)
        or _HEX64.fullmatch(str(decision.get("validation_manifest_sha256") or ""))
        is None
        or _IDENTIFIER.fullmatch(str(decision.get("rollback_release_id") or ""))
        is None
        or _HEX64.fullmatch(
            str(decision.get("rollback_release_note_sha256") or "")
        )
        is None
        or decision.get("capacity_profile") != CAPACITY_PROFILE
        or isinstance(decision.get("max_concurrency"), bool)
        or not isinstance(decision.get("max_concurrency"), int)
        or not 1 <= decision["max_concurrency"] <= 4
        or not isinstance(decision.get("import_closure_complete"), bool)
        or not isinstance(decision.get("affected_faces"), list)
        or not decision["affected_faces"]
        or len(decision["affected_faces"]) != len(set(decision["affected_faces"]))
        or not isinstance(decision.get("restart_targets"), list)
        or len(decision["restart_targets"]) != len(set(decision["restart_targets"]))
        or not str(decision.get("criticality") or "")
        or not str(decision.get("classification_reason") or "")
    ):
        raise ReleaseLaneError("release_lane_contract_invalid")
    expected_lane, expected_reason = _automatic_lane(
        changed, closure, decision["import_closure_complete"]
    )
    expected_faces, expected_targets = _faces_and_restarts(expected_lane, closure)
    if (
        decision["release_lane"] != expected_lane
        or decision["classification_reason"] != expected_reason
        or decision["affected_faces"] != expected_faces
        or decision["restart_targets"] != expected_targets
    ):
        raise ReleaseLaneError("release_lane_classification_mismatch")
    return decision
