#!/usr/bin/env python3
"""BOM-bound controller for transactional VM and worker source promotion."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import pnc_rca_production_cutover as cutover
from scripts import pnc_rca_release_gate as release_gate
from scripts import pnc_rca_release_prepare as release_prepare
from scripts import pnc_rca_vm_promotion_remote as remote_helper


MANIFEST_SCHEMA_VERSION = "pnc_rca_vm_promotion_manifest_v1"
PLAN_SCHEMA_VERSION = "pnc_rca_vm_promotion_plan_v1"
APPROVAL_SCHEMA_VERSION = "pnc_rca_vm_promotion_approval_v1"
RECEIPT_SCHEMA_VERSION = "pnc_rca_vm_promotion_receipt_v1"
ROLLBACK_RECEIPT_SCHEMA_VERSION = "pnc_rca_vm_promotion_rollback_receipt_v1"
AUTO_ROLLBACK_RECEIPT_SCHEMA_VERSION = (
    "pnc_rca_vm_promotion_auto_rollback_receipt_v1"
)
AUTHORIZATION_DECISION = "authorize_exact_rca_vm_and_worker_promotion"
ACTION_SET = (
    "stop_vm_worker_scheduler",
    "verify_no_active_rca_or_worker_child",
    "snapshot_vm_and_worker_tracked_closure",
    "promote_vm_candidate_commit",
    "promote_vm_worker_candidate_commit",
    "verify_entrypoints_and_topic_extractor",
    "restore_vm_worker_scheduler",
    "rollback_both_components_on_failure",
)
CANONICAL_VM_ROOT = PurePosixPath("/home/mini/data3/yj-evaluation-server")
CANONICAL_WORKER_ROOT = PurePosixPath("/home/mini/.hermes/worker-state")
VM_ENTRYPOINT = "api/g1q3_rca/scripts/run_rca_service_request.py"
WORKER_ENTRYPOINT = "vm_coding_worker_v2.py"
VM_TOPIC_EXTRACTOR = (
    "third_party/mcap_data_translate/build/bin/mcap_topic_extract"
)
DEFAULT_SSH_MINI_AGENT = Path.home() / ".local/bin/ssh-mini-agent"
REMOTE_HELPER_PATH = Path(__file__).with_name("pnc_rca_vm_promotion_remote.py")
MAX_REMOTE_OUTPUT_BYTES = 2 * 1024 * 1024
REMOTE_RUNNER_TIMEOUT_SECONDS = 180
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_RELEASE_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{7,127}\Z")


class VmPromotionError(ValueError):
    def __init__(self, code: str):
        self.code = str(code or "vm_promotion_invalid")[:120]
        super().__init__(self.code)


@dataclass(frozen=True)
class VmPromotionInputs:
    release_prepare_manifest: Path
    release_approval_receipt: Path
    vm_candidate_root: str
    worker_candidate_root: str
    vm_topic_extractor_sha256: str
    vm_topic_extractor_size: int
    plan_path: Path
    promotion_approval_receipt: Path
    receipt_path: Path
    rollback_receipt_path: Path
    remote_work_root: str
    remote_lock_path: str


ReleaseBindingProvider = Callable[
    [VmPromotionInputs, datetime], Mapping[str, Any]
]
RemoteRunner = Callable[[Mapping[str, Any]], Mapping[str, Any]]


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(remote_helper._canonical_json(value)).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sha256(value: Any, *, field: str) -> str:
    text = str(value or "").strip().lower()
    if _SHA256_RE.fullmatch(text) is None:
        raise VmPromotionError(f"vm_promotion_{field}_invalid")
    return text


def _absolute_local(value: Any, *, field: str) -> Path:
    text = str(value or "").strip()
    path = Path(text).expanduser()
    if not text or not path.is_absolute() or ".." in path.parts or "\x00" in text:
        raise VmPromotionError(f"vm_promotion_{field}_invalid")
    return path.absolute()


def _absolute_remote(value: Any, *, field: str) -> str:
    text = str(value or "").strip()
    path = PurePosixPath(text)
    if not text or not path.is_absolute() or ".." in path.parts or "\x00" in text:
        raise VmPromotionError(f"vm_promotion_{field}_invalid")
    return str(path)


def _time(value: Any, *, field: str) -> datetime:
    text = str(value or "")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise VmPromotionError(f"vm_promotion_{field}_invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise VmPromotionError(f"vm_promotion_{field}_invalid")
    return parsed.astimezone(timezone.utc)


def _owner_parent(path: Path, *, field: str) -> None:
    parent = path.parent
    if parent.is_symlink() or not parent.is_dir():
        raise VmPromotionError(f"vm_promotion_{field}_parent_invalid")
    observed = parent.stat()
    if observed.st_uid != os.getuid() or observed.st_mode & 0o077:
        raise VmPromotionError(f"vm_promotion_{field}_parent_invalid")


def load_manifest(path: Path) -> VmPromotionInputs:
    body = cutover._read_owned_json(
        _absolute_local(path, field="manifest"), artifact="vm_promotion_manifest"
    ).body
    expected = {
        "schema_version",
        "release_prepare_manifest",
        "release_approval_receipt",
        "vm_candidate_root",
        "worker_candidate_root",
        "vm_topic_extractor_sha256",
        "vm_topic_extractor_size",
        "plan_path",
        "promotion_approval_receipt",
        "receipt_path",
        "rollback_receipt_path",
        "remote_work_root",
        "remote_lock_path",
    }
    if set(body) != expected or body.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        raise VmPromotionError("vm_promotion_manifest_shape_invalid")
    extractor_size = body.get("vm_topic_extractor_size")
    if (
        isinstance(extractor_size, bool)
        or not isinstance(extractor_size, int)
        or extractor_size <= 0
    ):
        raise VmPromotionError("vm_promotion_topic_extractor_size_invalid")
    inputs = VmPromotionInputs(
        release_prepare_manifest=_absolute_local(
            body.get("release_prepare_manifest"), field="release_prepare_manifest"
        ),
        release_approval_receipt=_absolute_local(
            body.get("release_approval_receipt"), field="release_approval_receipt"
        ),
        vm_candidate_root=_absolute_remote(
            body.get("vm_candidate_root"), field="vm_candidate_root"
        ),
        worker_candidate_root=_absolute_remote(
            body.get("worker_candidate_root"), field="worker_candidate_root"
        ),
        vm_topic_extractor_sha256=_sha256(
            body.get("vm_topic_extractor_sha256"), field="topic_extractor_sha256"
        ),
        vm_topic_extractor_size=extractor_size,
        plan_path=_absolute_local(body.get("plan_path"), field="plan_path"),
        promotion_approval_receipt=_absolute_local(
            body.get("promotion_approval_receipt"), field="promotion_approval_receipt"
        ),
        receipt_path=_absolute_local(body.get("receipt_path"), field="receipt_path"),
        rollback_receipt_path=_absolute_local(
            body.get("rollback_receipt_path"), field="rollback_receipt_path"
        ),
        remote_work_root=_absolute_remote(
            body.get("remote_work_root"), field="remote_work_root"
        ),
        remote_lock_path=_absolute_remote(
            body.get("remote_lock_path"), field="remote_lock_path"
        ),
    )
    if (
        inputs.vm_candidate_root == str(CANONICAL_VM_ROOT)
        or inputs.worker_candidate_root == str(CANONICAL_WORKER_ROOT)
    ):
        raise VmPromotionError("vm_promotion_candidate_target_alias")
    for field, output in (
        ("plan", inputs.plan_path),
        ("promotion_approval", inputs.promotion_approval_receipt),
        ("receipt", inputs.receipt_path),
        ("rollback_receipt", inputs.rollback_receipt_path),
    ):
        _owner_parent(output, field=field)
    return inputs


def _machine_identity() -> Mapping[str, Any]:
    try:
        return release_gate._observe_release_approval_machine_identity()
    except Exception as exc:
        raise VmPromotionError("vm_promotion_machine_identity_unavailable") from exc


def _default_release_binding(
    inputs: VmPromotionInputs, now: datetime
) -> Mapping[str, Any]:
    manifest_owned = cutover._read_owned_json(
        inputs.release_prepare_manifest, artifact="vm_promotion_release_manifest"
    )
    manifest = manifest_owned.body
    if (
        manifest.get("schema_version")
        != release_prepare.RELEASE_PREPARE_MANIFEST_SCHEMA_VERSION
        or manifest.get("complete") is not True
        or manifest.get("plan_only") is not True
    ):
        raise VmPromotionError("vm_promotion_release_manifest_invalid")
    request_owned = cutover._read_owned_json(
        manifest_owned.path.parent / release_prepare.APPROVAL_REQUEST_FILENAME,
        artifact="vm_promotion_release_request",
    )
    approval_owned = cutover._read_owned_json(
        inputs.release_approval_receipt, artifact="vm_promotion_release_approval"
    )
    machine = _machine_identity()
    try:
        release_gate.validate_release_prepare_approval_binding(
            approval_request=request_owned.body,
            approval_request_sha256=request_owned.sha256,
            approval_receipt=approval_owned.body,
            approval_receipt_sha256=approval_owned.sha256,
            final_manifest_schema_version=(
                release_gate.RELEASE_PREPARE_FINAL_MANIFEST_SCHEMA_VERSION
            ),
            require_fresh_request=False,
            now=now,
            machine_identity_observer=lambda: machine,
        )
    except release_gate.EvidenceError as exc:
        raise VmPromotionError("vm_promotion_release_approval_invalid") from exc
    bindings = request_owned.body.get("bindings")
    bom = bindings.get("release_bom") if isinstance(bindings, Mapping) else None
    if not isinstance(bom, Mapping):
        raise VmPromotionError("vm_promotion_release_bom_missing")
    release_id = str(manifest.get("release_id") or "")
    release_bom_sha256 = _sha256(
        manifest.get("release_bom_sha256"), field="release_bom_sha256"
    )
    if (
        _RELEASE_ID_RE.fullmatch(release_id) is None
        or approval_owned.sha256 != manifest.get("approval_receipt_sha256")
        or release_bom_sha256 != bindings.get("release_bom_sha256")
        or release_gate._sha256_json(bom) != release_bom_sha256
    ):
        raise VmPromotionError("vm_promotion_release_binding_invalid")
    return {
        "release_id": release_id,
        "release_bom": bom,
        "release_bom_sha256": release_bom_sha256,
        "release_approval_receipt_sha256": approval_owned.sha256,
        "release_approval_expires_at": str(approval_owned.body.get("expires_at") or ""),
        "machine_identity": machine,
        "machine_identity_sha256": _sha256_json(machine),
    }


def _component_from_bom(
    *,
    name: str,
    component: Mapping[str, Any],
    candidate_root: str,
    target_root: PurePosixPath,
    entrypoint_relative: str,
    runtime_artifacts: list[Mapping[str, Any]],
) -> Mapping[str, Any]:
    expected_fields = {
        "source",
        "repo_root",
        "commit",
        "tree_clean",
        "status_sha256",
        "tree",
        "entrypoint_path",
        "entrypoint_sha256",
        "entrypoint_committed_sha256",
        "entrypoint_git_mode",
        "entrypoint_blob",
    }
    if set(component) != expected_fields:
        raise VmPromotionError(f"vm_promotion_{name}_bom_shape_invalid")
    commit = str(component.get("commit") or "").lower()
    tree = str(component.get("tree") or "").lower()
    if (
        component.get("source") != "ssh-mini-agent"
        or component.get("repo_root") != candidate_root
        or component.get("tree_clean") is not True
        or component.get("status_sha256") != release_gate.EMPTY_GIT_STATUS_SHA256
        or component.get("entrypoint_path")
        != str(PurePosixPath(candidate_root) / entrypoint_relative)
        or component.get("entrypoint_committed_sha256")
        != component.get("entrypoint_sha256")
        or re.fullmatch(r"[0-9a-f]{40,64}", commit) is None
        or re.fullmatch(r"[0-9a-f]{40,64}", tree) is None
    ):
        raise VmPromotionError(f"vm_promotion_{name}_bom_invalid")
    return {
        "name": name,
        "candidate_root": candidate_root,
        "target_root": str(target_root),
        "desired_commit": commit,
        "desired_tree": tree,
        "entrypoint_relative": entrypoint_relative,
        "entrypoint_sha256": _sha256(
            component.get("entrypoint_sha256"), field=f"{name}_entrypoint_sha256"
        ),
        "runtime_artifacts": runtime_artifacts,
    }


def _remote_program(request: Mapping[str, Any]) -> str:
    source = REMOTE_HELPER_PATH.read_text(encoding="utf-8")
    marker = '\nif __name__ == "__main__":\n'
    if source.count(marker) != 1:
        raise VmPromotionError("vm_promotion_remote_helper_shape_invalid")
    library = source.split(marker, 1)[0]
    request_json = json.dumps(
        request, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    return (
        library
        + "\n_REQUEST = json.loads("
        + repr(request_json)
        + ")\nprint(json.dumps(execute(_REQUEST), ensure_ascii=False, sort_keys=True))\n"
    )


def _default_remote_runner(request: Mapping[str, Any]) -> Mapping[str, Any]:
    program = _remote_program(request)
    environment = os.environ.copy()
    environment["SSH_MINI_AGENT_TIMEOUT"] = str(REMOTE_RUNNER_TIMEOUT_SECONDS)
    try:
        result = subprocess.run(
            [str(DEFAULT_SSH_MINI_AGENT), "run_py_json"],
            input=program,
            check=False,
            capture_output=True,
            text=True,
            timeout=REMOTE_RUNNER_TIMEOUT_SECONDS + 20,
            env=environment,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise VmPromotionError("vm_promotion_remote_unavailable") from exc
    if result.returncode != 0:
        raise VmPromotionError("vm_promotion_remote_failed")
    raw = str(result.stdout or "")
    if len(raw.encode("utf-8")) > MAX_REMOTE_OUTPUT_BYTES:
        raise VmPromotionError("vm_promotion_remote_output_too_large")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise VmPromotionError("vm_promotion_remote_output_invalid") from exc
    if not isinstance(payload, Mapping):
        raise VmPromotionError("vm_promotion_remote_output_invalid")
    return payload


def _build_remote_request(
    inputs: VmPromotionInputs, binding: Mapping[str, Any]
) -> Mapping[str, Any]:
    bom = binding.get("release_bom")
    components = bom.get("components") if isinstance(bom, Mapping) else None
    if not isinstance(components, Mapping) or set(components) != {
        "host",
        "workspace",
        "vm",
        "vm_worker",
    }:
        raise VmPromotionError("vm_promotion_release_components_invalid")
    vm = _component_from_bom(
        name="vm",
        component=components["vm"],
        candidate_root=inputs.vm_candidate_root,
        target_root=CANONICAL_VM_ROOT,
        entrypoint_relative=VM_ENTRYPOINT,
        runtime_artifacts=[
            {
                "relative_path": VM_TOPIC_EXTRACTOR,
                "sha256": inputs.vm_topic_extractor_sha256,
                "size": inputs.vm_topic_extractor_size,
            }
        ],
    )
    worker = _component_from_bom(
        name="vm_worker",
        component=components["vm_worker"],
        candidate_root=inputs.worker_candidate_root,
        target_root=CANONICAL_WORKER_ROOT,
        entrypoint_relative=WORKER_ENTRYPOINT,
        runtime_artifacts=[],
    )
    return {
        "schema_version": remote_helper.REQUEST_SCHEMA_VERSION,
        "mode": "observe",
        "release_id": binding["release_id"],
        "components": [vm, worker],
        "service_mode": "systemd_user",
        "remote_work_root": inputs.remote_work_root,
        "lock_path": inputs.remote_lock_path,
    }


def _validate_observation(
    observation: Mapping[str, Any], request: Mapping[str, Any]
) -> None:
    if (
        observation.get("schema_version") != remote_helper.OBSERVATION_SCHEMA_VERSION
        or observation.get("release_id") != request.get("release_id")
        or not isinstance(observation.get("components"), Mapping)
        or set(observation["components"]) != {"vm", "vm_worker"}
        or not isinstance(observation.get("service"), Mapping)
        or observation["service"].get("mode") != "systemd_user"
    ):
        raise VmPromotionError("vm_promotion_observation_invalid")
    for spec in request["components"]:
        observed = observation["components"].get(spec["name"])
        if not isinstance(observed, Mapping):
            raise VmPromotionError("vm_promotion_observation_invalid")
        candidate = observed.get("candidate")
        target = observed.get("target")
        if (
            not isinstance(candidate, Mapping)
            or not isinstance(target, Mapping)
            or candidate.get("root") != spec["candidate_root"]
            or candidate.get("head") != spec["desired_commit"]
            or candidate.get("tree") != spec["desired_tree"]
            or candidate.get("tree_clean") is not True
            or candidate.get("entrypoint", {}).get("sha256")
            != spec["entrypoint_sha256"]
            or target.get("root") != spec["target_root"]
        ):
            raise VmPromotionError("vm_promotion_observation_candidate_drift")
        runtime = observed.get("candidate_runtime_artifacts")
        if not isinstance(runtime, list) or len(runtime) != len(
            spec["runtime_artifacts"]
        ):
            raise VmPromotionError("vm_promotion_observation_runtime_invalid")
        if any(
            item.get("observed", {}).get("sha256") != item.get("expected_sha256")
            or item.get("observed", {}).get("size") != item.get("expected_size")
            for item in runtime
        ):
            raise VmPromotionError("vm_promotion_observation_runtime_drift")


def build_plan(
    inputs: VmPromotionInputs,
    *,
    release_binding_provider: ReleaseBindingProvider = _default_release_binding,
    remote_runner: RemoteRunner = _default_remote_runner,
    now: datetime | None = None,
) -> Mapping[str, Any]:
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    binding = release_binding_provider(inputs, current)
    request = _build_remote_request(inputs, binding)
    observation = remote_runner(request)
    _validate_observation(observation, request)
    helper_sha = _sha256_file(REMOTE_HELPER_PATH)
    plan = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "release_id": binding["release_id"],
        "created_at": current.isoformat(),
        "release_bom_sha256": binding["release_bom_sha256"],
        "release_approval_receipt_sha256": binding[
            "release_approval_receipt_sha256"
        ],
        "release_approval_expires_at": binding["release_approval_expires_at"],
        "machine_identity_sha256": binding["machine_identity_sha256"],
        "remote_helper_sha256": helper_sha,
        "action_set": list(ACTION_SET),
        "action_set_sha256": _sha256_json(list(ACTION_SET)),
        "remote_request": request,
        "prestate": observation,
        "prestate_sha256": _sha256_json(observation),
        "production_effects_executed": False,
    }
    cutover._publish_no_clobber(inputs.plan_path, plan)
    return plan


def _validate_promotion_approval(
    *,
    inputs: VmPromotionInputs,
    plan: Mapping[str, Any],
    binding: Mapping[str, Any],
    now: datetime,
) -> Mapping[str, Any]:
    approval_owned = cutover._read_owned_json(
        inputs.promotion_approval_receipt,
        artifact="vm_promotion_specific_approval",
    )
    approval = approval_owned.body
    expected = {
        "schema_version",
        "release_id",
        "decision",
        "created_at",
        "expires_at",
        "plan_sha256",
        "release_bom_sha256",
        "release_approval_receipt_sha256",
        "remote_helper_sha256",
        "machine_identity_sha256",
        "action_set",
        "action_set_sha256",
        "operator",
        "reason",
    }
    if set(approval) != expected or approval.get("schema_version") != APPROVAL_SCHEMA_VERSION:
        raise VmPromotionError("vm_promotion_approval_shape_invalid")
    created = _time(approval.get("created_at"), field="approval_created_at")
    expires = _time(approval.get("expires_at"), field="approval_expires_at")
    release_expires = _time(
        binding.get("release_approval_expires_at"), field="release_approval_expires_at"
    )
    validity = (expires - created).total_seconds()
    if (
        approval.get("release_id") != plan.get("release_id")
        or approval.get("decision") != AUTHORIZATION_DECISION
        or approval.get("plan_sha256") != _sha256_json(plan)
        or approval.get("release_bom_sha256") != plan.get("release_bom_sha256")
        or approval.get("release_approval_receipt_sha256")
        != plan.get("release_approval_receipt_sha256")
        or approval.get("remote_helper_sha256") != plan.get("remote_helper_sha256")
        or approval.get("machine_identity_sha256")
        != binding.get("machine_identity_sha256")
        or approval.get("action_set") != list(ACTION_SET)
        or approval.get("action_set_sha256") != _sha256_json(list(ACTION_SET))
        or not str(approval.get("operator") or "").strip()
        or not str(approval.get("reason") or "").strip()
        or created > now + timedelta(minutes=5)
        or validity <= 0
        or validity > 2 * 60 * 60
        or now >= expires
        or expires > release_expires
    ):
        raise VmPromotionError("vm_promotion_approval_invalid")
    return {**approval, "receipt_sha256": approval_owned.sha256}


def apply_promotion(
    inputs: VmPromotionInputs,
    *,
    authorization_decision: str,
    release_binding_provider: ReleaseBindingProvider = _default_release_binding,
    remote_runner: RemoteRunner = _default_remote_runner,
    now: datetime | None = None,
) -> Mapping[str, Any]:
    if authorization_decision != AUTHORIZATION_DECISION:
        raise VmPromotionError("vm_promotion_authorization_decision_invalid")
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    plan = cutover._read_owned_json(
        inputs.plan_path, artifact="vm_promotion_plan"
    ).body
    if (
        plan.get("schema_version") != PLAN_SCHEMA_VERSION
        or plan.get("production_effects_executed") is not False
        or plan.get("remote_helper_sha256") != _sha256_file(REMOTE_HELPER_PATH)
    ):
        raise VmPromotionError("vm_promotion_plan_invalid")
    binding = release_binding_provider(inputs, current)
    if (
        binding.get("release_id") != plan.get("release_id")
        or binding.get("release_bom_sha256") != plan.get("release_bom_sha256")
        or binding.get("release_approval_receipt_sha256")
        != plan.get("release_approval_receipt_sha256")
        or binding.get("machine_identity_sha256")
        != plan.get("machine_identity_sha256")
    ):
        raise VmPromotionError("vm_promotion_release_binding_drift")
    approval = _validate_promotion_approval(
        inputs=inputs, plan=plan, binding=binding, now=current
    )
    request = dict(plan.get("remote_request") or {})
    request["mode"] = "apply"
    request["expected_observation_sha256"] = plan.get("prestate_sha256")
    result = remote_runner(request)
    if (
        result.get("schema_version") != remote_helper.RECEIPT_SCHEMA_VERSION
        or result.get("ok") is not True
        or result.get("release_id") != plan.get("release_id")
        or result.get("expected_observation_sha256") != plan.get("prestate_sha256")
        or result.get("production_effects_executed") is not True
        or not isinstance(result.get("components"), list)
        or {item.get("name") for item in result["components"]} != {"vm", "vm_worker"}
        or result.get("service_before") != plan.get("prestate", {}).get("service")
        or result.get("snapshot_root")
        != str(PurePosixPath(inputs.remote_work_root) / "snapshot")
        or result.get("receipt_path")
        != str(PurePosixPath(inputs.remote_work_root) / "remote-receipt.json")
        or _SHA256_RE.fullmatch(str(result.get("receipt_sha256") or "")) is None
    ):
        raise VmPromotionError("vm_promotion_remote_receipt_invalid")
    receipt = {
        "schema_version": RECEIPT_SCHEMA_VERSION,
        "ok": True,
        "release_id": plan["release_id"],
        "completed_at": current.isoformat(),
        "authorization_decision": authorization_decision,
        "promotion_approval_receipt_sha256": approval["receipt_sha256"],
        "plan_sha256": _sha256_json(plan),
        "release_bom_sha256": plan["release_bom_sha256"],
        "remote_helper_sha256": plan["remote_helper_sha256"],
        "remote_receipt": result,
        "production_effects_executed": True,
    }
    try:
        cutover._publish_no_clobber(inputs.receipt_path, receipt)
    except Exception as publish_exc:
        rollback_request = dict(plan.get("remote_request") or {})
        rollback_request.update(
            mode="rollback",
            remote_receipt_path=result.get("receipt_path"),
            remote_receipt_sha256=result.get("receipt_sha256"),
        )
        try:
            remote_rollback = remote_runner(rollback_request)
            _validate_remote_rollback_result(
                result=remote_rollback,
                plan=plan,
                inputs=inputs,
                remote_receipt=result,
            )
        except Exception as rollback_exc:
            raise VmPromotionError(
                "vm_promotion_local_receipt_publish_and_remote_rollback_failed"
            ) from rollback_exc
        auto_rollback = {
            "schema_version": AUTO_ROLLBACK_RECEIPT_SCHEMA_VERSION,
            "ok": True,
            "release_id": plan["release_id"],
            "completed_at": current.isoformat(),
            "authorization_decision": authorization_decision,
            "promotion_approval_receipt_sha256": approval["receipt_sha256"],
            "plan_sha256": _sha256_json(plan),
            "remote_promotion_receipt_sha256": result["receipt_sha256"],
            "remote_rollback_receipt": remote_rollback,
            "rollback_complete": True,
            "trigger": "local_promotion_receipt_publish_failure",
            "production_effects_executed": True,
        }
        try:
            cutover._publish_no_clobber(inputs.rollback_receipt_path, auto_rollback)
        except Exception as rollback_publish_exc:
            raise VmPromotionError(
                "vm_promotion_local_receipts_publish_failed_after_remote_rollback"
            ) from rollback_publish_exc
        raise VmPromotionError(
            "vm_promotion_local_receipt_publish_failed_remote_rolled_back"
        ) from publish_exc
    return receipt


def _validate_remote_rollback_result(
    *,
    result: Mapping[str, Any],
    plan: Mapping[str, Any],
    inputs: VmPromotionInputs,
    remote_receipt: Mapping[str, Any],
) -> None:
    expected_service = plan.get("prestate", {}).get("service")
    restored_service = result.get("service_restored")
    service_matches = (
        isinstance(expected_service, Mapping)
        and isinstance(restored_service, Mapping)
        and set(restored_service) == set(expected_service)
        and {
            key: value
            for key, value in restored_service.items()
            if key != "main_pid"
        }
        == {
            key: value
            for key, value in expected_service.items()
            if key != "main_pid"
        }
        and not isinstance(restored_service.get("main_pid"), bool)
        and isinstance(restored_service.get("main_pid"), int)
        and (
            restored_service["main_pid"] > 0
            if restored_service.get("active") is True
            else restored_service["main_pid"] == 0
        )
    )
    if (
        result.get("schema_version")
        != remote_helper.ROLLBACK_RECEIPT_SCHEMA_VERSION
        or result.get("ok") is not True
        or result.get("release_id") != plan.get("release_id")
        or result.get("promotion_receipt_sha256")
        != remote_receipt.get("receipt_sha256")
        or result.get("rollback_complete") is not True
        or result.get("production_effects_executed") is not True
        or not isinstance(result.get("components"), list)
        or {item.get("name") for item in result["components"]}
        != {"vm", "vm_worker"}
        or not service_matches
        or result.get("receipt_path")
        != str(
            PurePosixPath(inputs.remote_work_root) / "remote-rollback-receipt.json"
        )
        or _SHA256_RE.fullmatch(str(result.get("receipt_sha256") or "")) is None
    ):
        raise VmPromotionError("vm_promotion_remote_rollback_receipt_invalid")


def verify_promotion(
    inputs: VmPromotionInputs,
    *,
    release_binding_provider: ReleaseBindingProvider = _default_release_binding,
    remote_runner: RemoteRunner = _default_remote_runner,
    now: datetime | None = None,
) -> Mapping[str, Any]:
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    plan = cutover._read_owned_json(
        inputs.plan_path, artifact="vm_promotion_verify_plan"
    ).body
    receipt_owned = cutover._read_owned_json(
        inputs.receipt_path, artifact="vm_promotion_verify_receipt"
    )
    receipt = receipt_owned.body
    if (
        plan.get("schema_version") != PLAN_SCHEMA_VERSION
        or receipt.get("schema_version") != RECEIPT_SCHEMA_VERSION
        or receipt.get("ok") is not True
        or receipt.get("plan_sha256") != _sha256_json(plan)
        or receipt.get("release_id") != plan.get("release_id")
        or plan.get("remote_helper_sha256") != _sha256_file(REMOTE_HELPER_PATH)
    ):
        raise VmPromotionError("vm_promotion_verify_receipt_invalid")
    binding = release_binding_provider(inputs, current)
    if (
        binding.get("release_id") != plan.get("release_id")
        or binding.get("release_bom_sha256") != plan.get("release_bom_sha256")
        or binding.get("machine_identity_sha256")
        != plan.get("machine_identity_sha256")
    ):
        raise VmPromotionError("vm_promotion_release_binding_drift")
    _validate_promotion_approval(
        inputs=inputs, plan=plan, binding=binding, now=current
    )
    request = dict(plan.get("remote_request") or {})
    request["mode"] = "observe"
    observation = remote_runner(request)
    _validate_observation(observation, request)
    for spec in request["components"]:
        target = observation["components"][spec["name"]]["target"]
        if (
            target.get("head") != spec["desired_commit"]
            or target.get("tree") != spec["desired_tree"]
            or target.get("tree_clean") is not True
            or target.get("entrypoint", {}).get("sha256")
            != spec["entrypoint_sha256"]
        ):
            raise VmPromotionError("vm_promotion_verify_target_drift")
        runtime = observation["components"][spec["name"]].get(
            "target_runtime_artifacts"
        )
        if not isinstance(runtime, list) or len(runtime) != len(
            spec["runtime_artifacts"]
        ) or any(
            item.get("observed", {}).get("sha256") != item.get("expected_sha256")
            or item.get("observed", {}).get("size") != item.get("expected_size")
            for item in runtime
        ):
            raise VmPromotionError("vm_promotion_verify_runtime_drift")
    remote_receipt = receipt.get("remote_receipt")
    if (
        not isinstance(remote_receipt, Mapping)
        or observation.get("service") != remote_receipt.get("service_after")
    ):
        raise VmPromotionError("vm_promotion_verify_service_drift")
    return {
        "schema_version": "pnc_rca_vm_promotion_verification_v1",
        "ok": True,
        "release_id": plan["release_id"],
        "promotion_receipt_sha256": receipt_owned.sha256,
        "live_observation": observation,
        "production_effects_executed": False,
    }


def rollback_promotion(
    inputs: VmPromotionInputs,
    *,
    authorization_decision: str,
    release_binding_provider: ReleaseBindingProvider = _default_release_binding,
    remote_runner: RemoteRunner = _default_remote_runner,
    now: datetime | None = None,
) -> Mapping[str, Any]:
    if authorization_decision != AUTHORIZATION_DECISION:
        raise VmPromotionError("vm_promotion_authorization_decision_invalid")
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    plan = cutover._read_owned_json(
        inputs.plan_path, artifact="vm_promotion_rollback_plan"
    ).body
    receipt_owned = cutover._read_owned_json(
        inputs.receipt_path, artifact="vm_promotion_applied_receipt"
    )
    receipt = receipt_owned.body
    if (
        plan.get("schema_version") != PLAN_SCHEMA_VERSION
        or receipt.get("schema_version") != RECEIPT_SCHEMA_VERSION
        or receipt.get("ok") is not True
        or receipt.get("plan_sha256") != _sha256_json(plan)
        or receipt.get("release_id") != plan.get("release_id")
    ):
        raise VmPromotionError("vm_promotion_rollback_receipt_invalid")
    applied_at = _time(receipt.get("completed_at"), field="promotion_completed_at")
    binding = release_binding_provider(inputs, applied_at)
    if (
        binding.get("release_id") != plan.get("release_id")
        or binding.get("release_bom_sha256") != plan.get("release_bom_sha256")
        or binding.get("machine_identity_sha256")
        != plan.get("machine_identity_sha256")
    ):
        raise VmPromotionError("vm_promotion_release_binding_drift")
    approval = _validate_promotion_approval(
        inputs=inputs, plan=plan, binding=binding, now=applied_at
    )
    remote_receipt = receipt.get("remote_receipt")
    if not isinstance(remote_receipt, Mapping):
        raise VmPromotionError("vm_promotion_rollback_receipt_invalid")
    request = dict(plan.get("remote_request") or {})
    request.update(
        mode="rollback",
        remote_receipt_path=remote_receipt.get("receipt_path"),
        remote_receipt_sha256=remote_receipt.get("receipt_sha256"),
    )
    result = remote_runner(request)
    _validate_remote_rollback_result(
        result=result,
        plan=plan,
        inputs=inputs,
        remote_receipt=remote_receipt,
    )
    rollback_receipt = {
        "schema_version": ROLLBACK_RECEIPT_SCHEMA_VERSION,
        "ok": True,
        "release_id": plan["release_id"],
        "completed_at": current.isoformat(),
        "authorization_decision": authorization_decision,
        "promotion_approval_receipt_sha256": approval["receipt_sha256"],
        "promotion_receipt_sha256": receipt_owned.sha256,
        "remote_rollback_receipt": result,
        "rollback_complete": True,
        "production_effects_executed": True,
    }
    cutover._publish_no_clobber(inputs.rollback_receipt_path, rollback_receipt)
    return rollback_receipt


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    for name in ("validate-manifest", "plan", "apply", "rollback"):
        command = subparsers.add_parser(name)
        command.add_argument("--manifest", type=Path, required=True)
        if name in {"apply", "rollback"}:
            command.add_argument("--authorization-decision", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    inputs = load_manifest(args.manifest)
    if args.command == "validate-manifest":
        result = {"ok": True, "production_effects_executed": False}
    elif args.command == "plan":
        result = build_plan(inputs)
    elif args.command == "apply":
        result = apply_promotion(
            inputs, authorization_decision=args.authorization_decision
        )
    else:
        result = rollback_promotion(
            inputs, authorization_decision=args.authorization_decision
        )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
