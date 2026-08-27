"""Host boundary for one authorized RCA same-task resume canary."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import subprocess
import time
from pathlib import Path
from typing import Any, Callable, Mapping

from gateway.pnc_rca_prod_admission import issue_rca_prod_admission
from gateway.pnc_rca_vm_release_binding import RCA_PROD_VM_RELEASE_ROOT


INFRA_REMEDIATION_SCHEMA_VERSION = "pnc_rca_infra_remediation_receipt_v1"
VM_REQUEST_SCHEMA_VERSION = "vm_rca_same_task_resume_request_v1"
VM_PREFLIGHT_SCHEMA_VERSION = "vm_rca_same_task_resume_preflight_v1"
VM_RECEIPT_SCHEMA_VERSION = "vm_rca_same_task_resume_receipt_v1"
VM_TOOL_PATH = Path("/home/mini/.hermes/worker-state/vm_rca_same_task_resume.py")
DEFAULT_SSH_MINI_AGENT = Path.home() / ".local/bin/ssh-mini-agent"
MAX_REMOTE_OUTPUT_BYTES = 4 * 1024 * 1024
SUPPORTED_BLOCKER = "remote_reader_timeout"
SUPPORTED_OPERATION = "bounded_retry"

# This release carries a one-item canary authorization, not a fleet-wide retry.
AUTHORIZED_TASK_ID = (
    "g1q3-rca-s1-"
    "259bfb7f9e2f93101d6fc6fc135ff2a4502f04520a033973570b660a073eeddb"
)
AUTHORIZED_BUSINESS_KEY = (
    "g1q3-rca-b1-"
    "ee987f00eb8fb740d115224dcfff9022be4be867d716bcbad7601bd5afd9768f"
)
AUTHORIZED_GENERATION = 7
AUTHORIZED_ISSUE_ID = "7068819154"
HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
ADMISSION_META_KEYS = frozenset(
    {
        "resource_class",
        "lane",
        "queue_if_blocked",
        "resource_gate_bypass",
        "rca_prod_capacity_mode",
        "rca_prod_attempt_id",
        "reservation_id",
        "reservation_fence",
        "reservation_contract_sha256",
        "rca_prod_goal_sha256",
        "rca_prod_command_sha256",
        "rca_prod_contract_sha256",
        "rca_prod_admission_receipt",
        "rca_prod_admission_key_fingerprint",
    }
)


class SameTaskResumeError(RuntimeError):
    def __init__(self, code: str):
        self.code = str(code)
        super().__init__(self.code)


RemoteCall = Callable[[str, Mapping[str, Any], int], Mapping[str, Any]]
AdmissionIssuer = Callable[..., Any]


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _remote_script(action: str, payload: Mapping[str, Any]) -> str:
    encoded = base64.b64encode(_canonical_bytes(dict(payload))).decode("ascii")
    return f'''import base64
import importlib.util
import json
from pathlib import Path

module_path = Path({str(VM_TOOL_PATH)!r})
spec = importlib.util.spec_from_file_location("vm_rca_same_task_resume_remote", module_path)
if spec is None or spec.loader is None:
    raise RuntimeError("same_task_resume_module_unavailable")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
payload = json.loads(base64.b64decode({encoded!r}).decode("utf-8"))
if {action!r} == "preflight":
    result = module.preflight(**payload)
elif {action!r} == "apply":
    result = module.apply_request(payload)
else:
    raise RuntimeError("same_task_resume_action_invalid")
print(json.dumps(result, ensure_ascii=False, sort_keys=True))
'''


def remote_call(
    action: str,
    payload: Mapping[str, Any],
    timeout_seconds: int,
    *,
    ssh_agent: Path = DEFAULT_SSH_MINI_AGENT,
    run_func: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> Mapping[str, Any]:
    if action not in {"preflight", "apply"} or timeout_seconds < 1:
        raise SameTaskResumeError("remote_resume_call_invalid")
    env = dict(os.environ)
    env.pop("HERMES_RCA_PROD_ADMISSION_HMAC_KEY", None)
    try:
        completed = run_func(
            [str(ssh_agent), "run_py_json"],
            input=_remote_script(action, payload),
            text=True,
            capture_output=True,
            timeout=timeout_seconds,
            env=env,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise SameTaskResumeError("remote_resume_timeout") from exc
    except OSError as exc:
        raise SameTaskResumeError("remote_resume_unavailable") from exc
    stdout = str(completed.stdout or "")
    if (
        completed.returncode != 0
        or not stdout
        or len(stdout.encode("utf-8", errors="replace")) > MAX_REMOTE_OUTPUT_BYTES
    ):
        raise SameTaskResumeError("remote_resume_failed")
    try:
        result = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise SameTaskResumeError("remote_resume_output_invalid") from exc
    if not isinstance(result, Mapping):
        raise SameTaskResumeError("remote_resume_output_invalid")
    return dict(result)


def _blocker_kind(blocker: Mapping[str, Any]) -> str:
    return str(blocker.get("kind") or blocker.get("error_code") or "").strip()


def _operation(remediation: Mapping[str, Any]) -> str:
    return str(remediation.get("op") or "").strip()


def _authorized(claim: Any, blocker: Mapping[str, Any], remediation: Mapping[str, Any]) -> bool:
    return bool(
        str(getattr(claim, "task_id", "")) == AUTHORIZED_TASK_ID
        and str(getattr(claim, "submission_key", "")) == AUTHORIZED_TASK_ID
        and str(getattr(claim, "business_key", "")) == AUTHORIZED_BUSINESS_KEY
        and getattr(claim, "generation", None) == AUTHORIZED_GENERATION
        and str(getattr(claim, "work_item_id", "")) == AUTHORIZED_ISSUE_ID
        and _blocker_kind(blocker) == SUPPORTED_BLOCKER
        and _operation(remediation) == SUPPORTED_OPERATION
    )


def _collector_result(
    claim: Any,
    blocker: Mapping[str, Any],
    remediation: Mapping[str, Any],
    timeout_seconds: int,
    *,
    success: bool,
    status: str,
    error_code: str,
) -> dict[str, Any]:
    return {
        "schema_version": INFRA_REMEDIATION_SCHEMA_VERSION,
        "success": success,
        "status": status,
        "submission_key": str(getattr(claim, "submission_key", "")),
        "business_key": str(getattr(claim, "business_key", "")),
        "generation": getattr(claim, "generation", None),
        "task_id": str(getattr(claim, "task_id", "")),
        "operation": _operation(remediation) or "unavailable",
        "blocker_kind": _blocker_kind(blocker),
        "resumed_same_task": success,
        "external_writes": False,
        "timeout_seconds": timeout_seconds,
        "error_code": error_code,
    }


def _validate_preflight(result: Mapping[str, Any], claim: Any) -> dict[str, Any]:
    value = dict(result)
    stable = value.get("stable_bindings")
    if (
        value.get("schema_version") != VM_PREFLIGHT_SCHEMA_VERSION
        or value.get("ok") is not True
        or value.get("task_id") != AUTHORIZED_TASK_ID
        or value.get("submission_key") != AUTHORIZED_TASK_ID
        or value.get("business_key") != AUTHORIZED_BUSINESS_KEY
        or value.get("generation") != AUTHORIZED_GENERATION
        or value.get("issue_id") != AUTHORIZED_ISSUE_ID
        or value.get("task_id") != getattr(claim, "task_id", None)
        or value.get("target_runtime_root") != RCA_PROD_VM_RELEASE_ROOT
        or not isinstance(value.get("goal_text"), str)
        or not value["goal_text"]
        or not HEX64_RE.fullmatch(str(value.get("goal_sha256") or ""))
        or not HEX64_RE.fullmatch(str(value.get("preflight_fingerprint") or ""))
        or not HEX64_RE.fullmatch(str(value.get("target_cli_sha256") or ""))
        or not isinstance(stable, Mapping)
        or set(stable)
        != {
            "contract_sha256",
            "reservation_id",
            "reservation_fence",
            "reservation_contract_sha256",
        }
        or not HEX64_RE.fullmatch(str(stable.get("contract_sha256") or ""))
        or not str(stable.get("reservation_id") or "")
        or not str(stable.get("reservation_fence") or "")
        or not HEX64_RE.fullmatch(
            str(stable.get("reservation_contract_sha256") or "")
        )
    ):
        raise SameTaskResumeError("remote_resume_preflight_invalid")
    if hashlib.sha256(value["goal_text"].encode("utf-8")).hexdigest() != value["goal_sha256"]:
        raise SameTaskResumeError("remote_resume_goal_hash_invalid")
    return value


def _validate_vm_apply(result: Mapping[str, Any], claim: Any) -> None:
    value = dict(result)
    if (
        value.get("schema_version") != VM_RECEIPT_SCHEMA_VERSION
        or value.get("success") is not True
        or value.get("status") != "applied"
        or value.get("task_id") != AUTHORIZED_TASK_ID
        or value.get("submission_key") != AUTHORIZED_TASK_ID
        or value.get("business_key") != AUTHORIZED_BUSINESS_KEY
        or value.get("generation") != AUTHORIZED_GENERATION
        or value.get("issue_id") != AUTHORIZED_ISSUE_ID
        or value.get("task_id") != getattr(claim, "task_id", None)
        or value.get("blocker_kind") != SUPPORTED_BLOCKER
        or value.get("operation") != SUPPORTED_OPERATION
        or value.get("resumed_same_task") is not True
        or value.get("business_external_writes") is not False
        or value.get("created_task_ids") != []
        or value.get("target_runtime_root") != RCA_PROD_VM_RELEASE_ROOT
    ):
        raise SameTaskResumeError("remote_resume_apply_invalid")


def resume_same_task(
    claim: Any,
    blocker: Mapping[str, Any],
    remediation: Mapping[str, Any],
    timeout_seconds: int,
    *,
    remote: RemoteCall = remote_call,
    issuer: AdmissionIssuer = issue_rca_prod_admission,
) -> Mapping[str, Any]:
    if not _authorized(claim, blocker, remediation):
        return _collector_result(
            claim,
            blocker,
            remediation,
            timeout_seconds,
            success=False,
            status="held",
            error_code="infra_remediation_scope_not_authorized",
        )
    started = time.monotonic()

    def remaining() -> int:
        value = timeout_seconds - int(time.monotonic() - started)
        if value < 1:
            raise SameTaskResumeError("same_task_resume_deadline_exceeded")
        return value

    try:
        preflight_payload = {
            "task_id": AUTHORIZED_TASK_ID,
            "submission_key": AUTHORIZED_TASK_ID,
            "business_key": AUTHORIZED_BUSINESS_KEY,
            "generation": AUTHORIZED_GENERATION,
            "issue_id": AUTHORIZED_ISSUE_ID,
            "blocker_kind": SUPPORTED_BLOCKER,
            "operation": SUPPORTED_OPERATION,
            "target_runtime_root": RCA_PROD_VM_RELEASE_ROOT,
        }
        checked = _validate_preflight(
            remote("preflight", preflight_payload, min(remaining(), 30)), claim
        )
        stable = checked["stable_bindings"]
        issued = issuer(
            task_id=AUTHORIZED_TASK_ID,
            submission_key=AUTHORIZED_TASK_ID,
            goal=checked["goal_text"],
            contract_sha256=stable["contract_sha256"],
            reservation_id=stable["reservation_id"],
            reservation_fence=stable["reservation_fence"],
            reservation_contract_sha256=stable["reservation_contract_sha256"],
        )
        admission_meta = dict(getattr(issued, "meta", {}))
        if set(admission_meta) != ADMISSION_META_KEYS:
            raise SameTaskResumeError("host_resume_admission_invalid")
        request = {
            "schema_version": VM_REQUEST_SCHEMA_VERSION,
            "task_id": AUTHORIZED_TASK_ID,
            "submission_key": AUTHORIZED_TASK_ID,
            "business_key": AUTHORIZED_BUSINESS_KEY,
            "generation": AUTHORIZED_GENERATION,
            "issue_id": AUTHORIZED_ISSUE_ID,
            "blocker_kind": SUPPORTED_BLOCKER,
            "operation": SUPPORTED_OPERATION,
            "preflight_fingerprint": checked["preflight_fingerprint"],
            "target_runtime_root": checked["target_runtime_root"],
            "target_cli_sha256": checked["target_cli_sha256"],
            "admission_meta": admission_meta,
        }
        applied = remote("apply", request, min(remaining(), 45))
        _validate_vm_apply(applied, claim)
        return _collector_result(
            claim,
            blocker,
            remediation,
            timeout_seconds,
            success=True,
            status="succeeded",
            error_code="",
        )
    except SameTaskResumeError as exc:
        error_code = exc.code
    except Exception as exc:
        error_code = f"same_task_resume_failed:{type(exc).__name__}"
    return _collector_result(
        claim,
        blocker,
        remediation,
        timeout_seconds,
        success=False,
        status="failed",
        error_code=error_code[:120],
    )
