#!/usr/bin/env python3
"""Fail-closed apply gate for the exact controlled RCA gray contract."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import stat
import sys
from typing import Any, Callable, Mapping, Sequence


REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import pnc_rca_controlled_gray as gray


EXECUTION_RESULT_SCHEMA_VERSION = "pnc_rca_controlled_gray_execution_result_v1"
AUTHORIZATION_SCHEMA_VERSION = "pnc_rca_controlled_gray_authorization_v1"
AUTHORIZATION_DECISION = "authorize_exact_controlled_gray_apply"
MAX_PLAN_AGE = timedelta(minutes=10)
MAX_AUTHORIZATION_TTL = timedelta(minutes=5)
MAX_FUTURE_SKEW = timedelta(seconds=5)
MAX_AUTHORIZATION_BYTES = 128 * 1024
IDENTITY_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
NONCE_RE = re.compile(r"[0-9a-f]{64}\Z")

PLAN_FIELDS = {
    "schema_version",
    "observed_at",
    "decision",
    "status",
    "tool_mode",
    "bom",
    "bom_core_sha256",
    "capacity_gate",
    "admission_contract",
    "blockers",
    "production_effects",
}
AUTHORIZATION_FIELDS = {
    "schema_version",
    "authorization_id",
    "decision",
    "issued_at",
    "expires_at",
    "release_id",
    "plan_sha256",
    "bom_core_sha256",
    "executor_sha256",
    "required_primitive_contract_sha256",
    "target",
    "slots",
    "policy",
    "authorized_by",
    "authorized_role",
    "nonce",
    "authorization_fingerprint",
}
TARGET_FIELDS = {
    "api_project_key",
    "project_simple_name",
    "work_item_type_key",
    "work_item_id",
    "issue_url",
}
POLICY_FIELDS = {
    "max_concurrency",
    "max_slots",
    "stop_on_first_failure",
    "suppress_later_writes_after_failure",
    "resource_class",
    "capacity_mode",
    "queue_if_blocked",
    "bypass_allowed",
    "direct_database_write_allowed",
    "direct_meegle_write_allowed",
    "dispatcher_payload_override_allowed",
    "official_field_readback_required",
    "official_full_comment_readback_required",
    "authorization_create_once_required",
}

PRODUCTION_PRIMITIVE_REQUIREMENTS: dict[str, Any] = {
    "schema_version": "pnc_rca_controlled_gray_primitive_contract_v1",
    "exact_issue": {
        "work_item_id": gray.TARGET_WORK_ITEM_ID,
        "api_project_key": gray.TARGET_API_PROJECT_KEY,
        "project_simple_name": gray.TARGET_PROJECT_SIMPLE_NAME,
        "existing_governed_recovery_primitive_required": True,
        "real_rca_only": True,
        "dispatcher_payload_override_forbidden": True,
    },
    "natural_kafka": {
        "count": 1,
        "ordinary_ingest_only": True,
        "manual_seek_forbidden": True,
        "manual_commit_forbidden": True,
        "operator_recovery_forbidden": True,
    },
    "completion": {
        "official_field_readback": gray.OFFICIAL_FIELD_READBACK_ADAPTER,
        "official_comment_readback": gray.OFFICIAL_COMMENT_READBACK_ADAPTER,
        "canonical_full_comment_required": True,
        "receipt_before_readback_forbidden": True,
    },
}
PRODUCTION_PRIMITIVE_CONTRACT_SHA256 = gray._sha256_value(
    PRODUCTION_PRIMITIVE_REQUIREMENTS
)
PRODUCTION_PRIMITIVE_GAPS = (
    "controlled_gray_exact_issue_governed_primitive_absent",
    "controlled_gray_first_natural_kafka_primitive_absent",
    "controlled_gray_official_readback_closeout_primitive_absent",
)

ResourceProbe = Callable[[], Mapping[str, Any]]


class ControlledGrayExecutionError(ValueError):
    def __init__(self, code: str, detail: str = ""):
        self.code = str(code or "controlled_gray_execution_invalid")[:160]
        self.detail = str(detail or self.code)[:500]
        super().__init__(self.code)


@dataclass(frozen=True)
class OwnedDocument:
    path: Path
    raw: bytes
    sha256: str
    body: Mapping[str, Any]


def _effects() -> dict[str, Any]:
    return {
        "authorization_claimed": False,
        "production_mutation": False,
        "production_write_attempts": 0,
        "vm_tasks_submitted": 0,
        "kafka_seek_calls": 0,
        "kafka_commit_calls": 0,
        "direct_database_writes": 0,
        "direct_meegle_writes": 0,
        "dispatcher_payload_overrides": 0,
        "completion_receipts_written": 0,
    }


def _now(value: datetime | None = None) -> datetime:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None or current.utcoffset() is None:
        raise ControlledGrayExecutionError(
            "controlled_gray_execution_time_invalid"
        )
    return current.astimezone(timezone.utc)


def _timestamp(value: Any, *, field: str) -> datetime:
    try:
        return gray._timestamp(value, field=field)
    except gray.ControlledGrayError as exc:
        raise ControlledGrayExecutionError(
            f"controlled_gray_execution_{field}_invalid"
        ) from exc


def _read_owned_json(
    path: Path, *, artifact: str, maximum: int
) -> OwnedDocument:
    selected = gray._absolute_path(path, field=f"execution_{artifact}_path")
    try:
        info = os.lstat(selected)
    except OSError as exc:
        raise ControlledGrayExecutionError(
            f"controlled_gray_execution_{artifact}_unavailable"
        ) from exc
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.geteuid()
        or info.st_nlink != 1
        or stat.S_IMODE(info.st_mode) != 0o600
    ):
        raise ControlledGrayExecutionError(
            f"controlled_gray_execution_{artifact}_not_owner_only"
        )
    try:
        raw, digest = gray._read_stable_file(
            selected,
            artifact=f"execution_{artifact}",
            maximum=maximum,
        )
        body = gray._strict_json(raw, artifact=f"execution_{artifact}")
    except gray.ControlledGrayError as exc:
        raise ControlledGrayExecutionError(
            f"controlled_gray_execution_{artifact}_invalid"
        ) from exc
    return OwnedDocument(selected, raw, digest, body)


def _expected_target() -> dict[str, Any]:
    return {
        "api_project_key": gray.TARGET_API_PROJECT_KEY,
        "project_simple_name": gray.TARGET_PROJECT_SIMPLE_NAME,
        "work_item_type_key": gray.TARGET_WORK_ITEM_TYPE_KEY,
        "work_item_id": gray.TARGET_WORK_ITEM_ID,
        "issue_url": gray.TARGET_ISSUE_URL,
    }


def _expected_slots() -> list[dict[str, Any]]:
    return [
        {
            "slot": 0,
            "kind": "exact_issue",
            "work_item_id": gray.TARGET_WORK_ITEM_ID,
            "real_rca_required": True,
            "write_via_resident_dispatcher_only": True,
        },
        {
            "slot": 1,
            "kind": "first_natural_kafka_canary_after_target_readback",
            "count": 1,
            "ordinary_kafka_ingest_only": True,
            "manual_seek_allowed": False,
            "manual_commit_allowed": False,
            "manual_trigger_allowed": False,
            "operator_recovery_allowed": False,
        },
    ]


def _expected_policy() -> dict[str, Any]:
    return {
        "max_concurrency": 1,
        "max_slots": 2,
        "stop_on_first_failure": True,
        "suppress_later_writes_after_failure": True,
        "resource_class": gray.RESOURCE_CLASS,
        "capacity_mode": gray.CAPACITY_MODE,
        "queue_if_blocked": False,
        "bypass_allowed": False,
        "direct_database_write_allowed": False,
        "direct_meegle_write_allowed": False,
        "dispatcher_payload_override_allowed": False,
        "official_field_readback_required": True,
        "official_full_comment_readback_required": True,
        "authorization_create_once_required": True,
    }


def _current_source_sha256(path: Path, *, artifact: str) -> str:
    try:
        _raw, digest = gray._read_stable_file(
            path.resolve(), artifact=artifact, maximum=gray.MAX_SOURCE_BYTES
        )
    except gray.ControlledGrayError as exc:
        raise ControlledGrayExecutionError(
            f"controlled_gray_execution_{artifact}_invalid"
        ) from exc
    return digest


def _validate_plan(owned: OwnedDocument, *, now: datetime) -> Mapping[str, Any]:
    body = owned.body
    if (
        set(body) != PLAN_FIELDS
        or body.get("schema_version") != gray.PLAN_SCHEMA_VERSION
        or body.get("decision") != "GO"
        or body.get("status") != "GO_FOR_CONTROLLED_GRAY_SUBMISSION"
        or body.get("tool_mode") != "validate_plan_only"
        or body.get("blockers") != []
    ):
        raise ControlledGrayExecutionError(
            "controlled_gray_execution_plan_not_applyable"
        )
    observed_at = _timestamp(body.get("observed_at"), field="plan_observed_at")
    if observed_at > now + MAX_FUTURE_SKEW or now - observed_at > MAX_PLAN_AGE:
        raise ControlledGrayExecutionError(
            "controlled_gray_execution_plan_stale"
        )
    if body.get("production_effects") != {
        "production_mutation": False,
        "production_write_attempts": 0,
        "kafka_offset_commits": 0,
        "feishu_writes": 0,
        "service_restarts": 0,
        "vm_tasks_submitted": 0,
    }:
        raise ControlledGrayExecutionError(
            "controlled_gray_execution_plan_effects_invalid"
        )
    bom = body.get("bom")
    if not isinstance(bom, Mapping):
        raise ControlledGrayExecutionError(
            "controlled_gray_execution_bom_invalid"
        )
    bom_without_hash = dict(bom)
    claimed_bom_sha = bom_without_hash.pop("bom_core_sha256", None)
    if (
        claimed_bom_sha != body.get("bom_core_sha256")
        or claimed_bom_sha != gray._sha256_value(bom_without_hash)
        or bom.get("schema_version") != gray.BOM_SCHEMA_VERSION
    ):
        raise ControlledGrayExecutionError(
            "controlled_gray_execution_bom_hash_invalid"
        )
    components = bom.get("components")
    host = components.get("host") if isinstance(components, Mapping) else None
    vm = components.get("vm") if isinstance(components, Mapping) else None
    vm_receipt = (
        vm.get("independent_go_receipt") if isinstance(vm, Mapping) else None
    )
    if (
        not isinstance(host, Mapping)
        or host.get("root") != gray.EXPECTED_HOST_ROOT
        or host.get("commit") != gray.EXPECTED_HOST_COMMIT
        or host.get("tree") != gray.EXPECTED_HOST_TREE
        or host.get("runtime_manifest_sha256")
        != gray._sha256_value(host.get("runtime_manifest"))
        or not isinstance(vm, Mapping)
        or vm.get("root") != gray.EXPECTED_VM_ROOT
        or vm.get("commit") != gray.EXPECTED_VM_COMMIT
        or vm.get("tree") != gray.EXPECTED_VM_TREE
        or not isinstance(vm_receipt, Mapping)
        or vm_receipt.get("sha256") != gray.EXPECTED_VM_GO_RECEIPT_SHA256
        or vm_receipt.get("verdict") != "GO"
        or bom.get("execution_contract") != gray._execution_contract()
    ):
        raise ControlledGrayExecutionError(
            "controlled_gray_execution_component_binding_invalid"
        )
    capabilities = host.get("delivery_capabilities")
    if (
        not isinstance(capabilities, Mapping)
        or capabilities.get("delivery_effect_schema_version")
        != "pnc_rca_delivery_effect_v2"
        or capabilities.get("report_link_kind") != "manifest_html"
        or capabilities.get("legacy_v1_success_effect_rejected") is not True
        or capabilities.get("canonical_content_reconstruction") is not True
        or capabilities.get("api_project_key_and_url_slug_separated") is not True
        or capabilities.get("official_field_adapter")
        != gray.OFFICIAL_FIELD_READBACK_ADAPTER
        or capabilities.get("official_comment_adapter")
        != gray.OFFICIAL_COMMENT_READBACK_ADAPTER
        or capabilities.get("http_artifact_verification_precedes_remote_boundary")
        is not True
    ):
        raise ControlledGrayExecutionError(
            "controlled_gray_execution_host_capability_invalid"
        )
    tooling = bom.get("tooling")
    if not isinstance(tooling, Mapping):
        raise ControlledGrayExecutionError(
            "controlled_gray_execution_tooling_invalid"
        )
    planner_path = Path(gray.__file__).resolve()
    if (
        tooling.get("path") != str(planner_path)
        or tooling.get("sha256")
        != _current_source_sha256(planner_path, artifact="planner_source")
    ):
        raise ControlledGrayExecutionError(
            "controlled_gray_execution_planner_drift"
        )
    admission = body.get("admission_contract")
    if not isinstance(admission, Mapping):
        raise ControlledGrayExecutionError(
            "controlled_gray_execution_admission_contract_invalid"
        )
    admission_without_hash = dict(admission)
    admission_sha = admission_without_hash.pop("contract_sha256", None)
    if (
        admission_sha != gray._sha256_value(admission_without_hash)
        or admission.get("schema_version")
        != gray.ADMISSION_CONTRACT_SCHEMA_VERSION
        or admission.get("bom_core_sha256") != claimed_bom_sha
        or admission.get("resource_class") != gray.RESOURCE_CLASS
        or admission.get("capacity_mode") != gray.CAPACITY_MODE
        or admission.get("queue_if_blocked") is not False
        or admission.get("bypass_allowed") is not False
        or admission.get("signed_rca_prod_admission_required_just_in_time")
        is not True
        or admission.get("production_effects_authorized_by_this_plan") is not False
    ):
        raise ControlledGrayExecutionError(
            "controlled_gray_execution_admission_contract_invalid"
        )
    return body


def _revalidate_bom(plan: Mapping[str, Any], *, now: datetime) -> None:
    bom = plan["bom"]
    components = bom["components"]
    host = components["host"]
    vm = components["vm"]
    vm_receipt = vm["independent_go_receipt"]
    spec = {
        "schema_version": gray.SPEC_SCHEMA_VERSION,
        "release_id": bom["release_id"],
        "host_candidate": {
            "root": host["root"],
            "commit": host["commit"],
            "tree": host["tree"],
        },
        "vm_candidate": {
            "root": vm["root"],
            "commit": vm["commit"],
            "tree": vm["tree"],
            "independent_go_receipt_path": vm_receipt["observed_path"],
            "independent_go_receipt_sha256": vm_receipt["sha256"],
        },
    }
    try:
        observed = gray._build_bom(spec, now=now)
    except gray.ControlledGrayError as exc:
        raise ControlledGrayExecutionError(
            "controlled_gray_execution_bom_revalidation_failed", exc.code
        ) from exc
    if observed != bom:
        raise ControlledGrayExecutionError(
            "controlled_gray_execution_bom_drift"
        )


def _authorization_fingerprint(value: Mapping[str, Any]) -> str:
    return gray._sha256_value(
        {
            key: item
            for key, item in value.items()
            if key != "authorization_fingerprint"
        }
    )


def _validate_authorization(
    owned: OwnedDocument,
    *,
    plan: Mapping[str, Any],
    plan_sha256: str,
    now: datetime,
) -> Mapping[str, Any]:
    body = owned.body
    if set(body) != AUTHORIZATION_FIELDS:
        raise ControlledGrayExecutionError(
            "controlled_gray_execution_authorization_shape_invalid"
        )
    authorization_id = str(body.get("authorization_id") or "")
    authorized_by = str(body.get("authorized_by") or "")
    nonce = str(body.get("nonce") or "")
    if (
        body.get("schema_version") != AUTHORIZATION_SCHEMA_VERSION
        or body.get("decision") != AUTHORIZATION_DECISION
        or IDENTITY_RE.fullmatch(authorization_id) is None
        or IDENTITY_RE.fullmatch(authorized_by) is None
        or body.get("authorized_role") != "owner"
        or NONCE_RE.fullmatch(nonce) is None
        or body.get("release_id") != plan["bom"]["release_id"]
        or body.get("plan_sha256") != plan_sha256
        or body.get("bom_core_sha256") != plan["bom_core_sha256"]
        or body.get("target") != _expected_target()
        or body.get("slots") != _expected_slots()
        or not isinstance(body.get("policy"), Mapping)
        or set(body["policy"]) != POLICY_FIELDS
        or body.get("policy") != _expected_policy()
        or body.get("required_primitive_contract_sha256")
        != PRODUCTION_PRIMITIVE_CONTRACT_SHA256
        or body.get("executor_sha256")
        != _current_source_sha256(Path(__file__), artifact="executor_source")
        or body.get("authorization_fingerprint")
        != _authorization_fingerprint(body)
    ):
        raise ControlledGrayExecutionError(
            "controlled_gray_execution_authorization_invalid"
        )
    issued_at = _timestamp(body.get("issued_at"), field="authorization_issued_at")
    expires_at = _timestamp(
        body.get("expires_at"), field="authorization_expires_at"
    )
    if (
        expires_at <= issued_at
        or expires_at - issued_at > MAX_AUTHORIZATION_TTL
        or issued_at > now + MAX_FUTURE_SKEW
        or now < issued_at - MAX_FUTURE_SKEW
        or now >= expires_at
    ):
        raise ControlledGrayExecutionError(
            "controlled_gray_execution_authorization_expired"
        )
    return body


def _primitive_gaps() -> tuple[str, ...]:
    """Return reviewed missing boundaries; never substitute the formal draft pack."""

    return PRODUCTION_PRIMITIVE_GAPS


def evaluate(
    *,
    mode: str,
    plan_path: Path,
    authorization_path: Path,
    now: datetime | None = None,
    resource_probe: ResourceProbe = gray._default_resource_probe,
) -> Mapping[str, Any]:
    current = _now(now)
    effects = _effects()
    base = {
        "schema_version": EXECUTION_RESULT_SCHEMA_VERSION,
        "mode": mode,
        "observed_at": current.isoformat(),
        "production_closed": False,
        "completion_receipt": None,
        "authorization_claim": None,
        "production_effects": effects,
    }
    if mode not in {"validate", "apply"}:
        return {
            **base,
            "decision": "NO_GO",
            "status": "NO_GO_MODE_INVALID",
            "blockers": ["controlled_gray_execution_mode_invalid"],
        }
    try:
        plan_owned = _read_owned_json(
            plan_path, artifact="plan", maximum=gray.MAX_JSON_BYTES
        )
        plan = _validate_plan(plan_owned, now=current)
        if mode == "apply":
            _revalidate_bom(plan, now=current)
        authorization_owned = _read_owned_json(
            authorization_path,
            artifact="authorization",
            maximum=MAX_AUTHORIZATION_BYTES,
        )
        _authorization = _validate_authorization(
            authorization_owned,
            plan=plan,
            plan_sha256=plan_owned.sha256,
            now=current,
        )
    except ControlledGrayExecutionError as exc:
        return {
            **base,
            "decision": "NO_GO",
            "status": "NO_GO_INPUT_OR_AUTHORIZATION",
            "blockers": [exc.code],
        }
    try:
        capacity_report = resource_probe()
        capacity = gray._validate_regular_capacity(capacity_report, now=current)
    except Exception as exc:
        code = getattr(exc, "code", "controlled_gray_execution_capacity_probe_failed")
        return {
            **base,
            "decision": "NO_GO",
            "status": "NO_GO_REGULAR_RCA_PROD_CAPACITY",
            "blockers": [str(code)],
        }
    gaps = _primitive_gaps()
    if gaps:
        return {
            **base,
            "decision": "NO_GO",
            "status": "NO_GO_PRODUCTION_PRIMITIVE_GAP",
            "blockers": list(gaps),
            "capacity_gate": {
                "status": capacity["status"],
                "resource_class": capacity["resource_class"],
                "capacity_mode": capacity["capacity_mode"],
                "authorization_receipt_sha256": capacity["authorization"][
                    "authorization_receipt_sha256"
                ],
                "snapshot_sha256": capacity["snapshot_sha256"],
            },
            "required_primitive_contract": PRODUCTION_PRIMITIVE_REQUIREMENTS,
            "required_primitive_contract_sha256": (
                PRODUCTION_PRIMITIVE_CONTRACT_SHA256
            ),
        }
    return {
        **base,
        "decision": "NO_GO",
        "status": "NO_GO_EXECUTION_ADAPTER_NOT_IMPLEMENTED",
        "blockers": ["controlled_gray_reviewed_execution_adapter_absent"],
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="mode", required=True)
    for name in ("validate", "apply"):
        command = subparsers.add_parser(name)
        command.add_argument("--plan", type=Path, required=True)
        command.add_argument("--authorization", type=Path, required=True)
        command.add_argument("--output", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    result = evaluate(
        mode=args.mode,
        plan_path=args.plan,
        authorization_path=args.authorization,
    )
    if args.output is not None:
        try:
            gray._write_create_once(args.output, result)
        except gray.ControlledGrayError as exc:
            result = {
                **result,
                "decision": "NO_GO",
                "status": "NO_GO_OUTPUT_FAILURE",
                "blockers": [exc.code],
            }
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
