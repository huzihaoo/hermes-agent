"""Pure status-first, create-once facade for direct RCA VM submissions.

The module defines a deliberately small transport boundary.  It does not know
how VM status is read or how a task is created; callers inject both operations.
Only a proven-missing status may cross the create boundary, and every create
attempt is followed by a fresh status read so ambiguous transport results are
never reported as successful creates.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
import hashlib
import json
import math
import posixpath
import re
from typing import Any, Literal

from gateway.pnc_rca_data_access import (
    RemoteDataAccessError,
    validate_remote_data_access,
)
from gateway.pnc_rca_schema import (
    RCA_EXECUTION_REQUEST_SCHEMA_VERSION,
    validate_vm_execution_request_envelope,
)


DIRECT_VM_SUBMIT_SCHEMA_VERSION = "g1q3_rca_direct_vm_submit_envelope_v1"
DIRECT_VM_MAX_JSON_BYTES = 1024 * 1024
DIRECT_VM_MAX_JSON_DEPTH = 32
DIRECT_VM_MAX_JSON_NODES = 50_000

_SAFE_TASK_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_DOWNLOAD_COMMAND_RE = re.compile(
    r"\b(?:mdi|pdcl)(?:\s+|[-_])(?:download|refresh(?:2)?)\b",
    re.IGNORECASE,
)

_REQUIRED_ENVELOPE_FIELDS = frozenset({
    "schema_version",
    "task_id",
    "submission_key",
    "identity_sha256",
    "contract_sha256",
    "create_once",
    "allow_download",
    "auth",
    "source_refs",
    "execution_request",
})
_OPTIONAL_ENVELOPE_FIELDS = frozenset({"artifact_root", "artifact_cifs_root"})
_AUTH_FIELDS = frozenset({"principal", "capability"})
_SOURCE_REF_FIELDS = frozenset({
    "origin_source_id",
    "source_event_id",
    "generation",
    "business_key",
    "submission_key",
})
_STATUS_FIELDS = frozenset({"state", "task_id", "submission_key", "identity_sha256"})
_FORBIDDEN_EXACT_KEYS = frozenset({
    "activation_epoch",
    "activation_epoch_id",
    "capacity",
    "capacity_mode",
    "derived_capacity_reservation",
    "epoch",
    "epoch_id",
    "live_write_fence_authority",
    "lane",
    "prod_receipt",
    "provider_fence",
    "queue_if_blocked",
    "rca_prod",
    "rca_prod_receipt",
    "release",
    "release_binding",
    "release_id",
    "resource_gate_bypass",
    "resource_class",
    "bypass",
    "risk_class",
    "runtime_release",
    "w3",
    "w3_execution_snapshot",
    "w3_snapshot",
    "workspace",
    "workspace_runtime",
    "workspace_runtime_identity",
    "write_fence",
})
_FORBIDDEN_KEY_PREFIXES = (
    "activation_epoch_",
    "derived_capacity_",
    "rca_prod_",
    "release_binding_",
    "w3_",
)
_FORBIDDEN_EXACT_VALUES = frozenset({"rca_prod"})
_FORBIDDEN_DOWNLOAD_KEYS = frozenset({
    "download_command",
    "download_url",
    "mdi_download_cmd",
    "pdcl_download_cmd",
})
_EXECUTION_REQUEST_REQUIRED_FIELDS = frozenset({
    "schema_version",
    "request_kind",
    "work_item",
    "data",
    "execution_policy",
    "source_refs",
})

# ``existing`` represents a known non-terminal shared-state task (pending,
# claimed, or running).  It is intentionally distinct from ``unknown``:
# known identity suppresses create, while an unreadable/ambiguous status must
# retry without crossing the create boundary.
DirectVmObservedState = Literal["missing", "unknown", "existing", "completed", "failed"]
DirectVmSubmitOutcomeName = Literal[
    "deduplicated", "reconciled", "retry", "permanent_conflict"
]
StatusTransport = Callable[[str], Mapping[str, Any]]
CreateTransport = Callable[[Mapping[str, Any]], Mapping[str, Any]]


class DirectVmSubmitError(ValueError):
    """Raised before transport when a direct submit contract is invalid."""


@dataclass(frozen=True)
class DirectVmSubmitRequest:
    """Canonical request passed to one injected VM create transport."""

    schema_version: str
    task_id: str
    submission_key: str
    identity_sha256: str
    contract_sha256: str
    create_once: bool
    allow_download: bool
    auth: Mapping[str, Any]
    source_refs: Mapping[str, Any]
    execution_request: Mapping[str, Any]
    artifact_root: str = ""
    artifact_cifs_root: str = ""

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "schema_version": self.schema_version,
            "task_id": self.task_id,
            "submission_key": self.submission_key,
            "identity_sha256": self.identity_sha256,
            "contract_sha256": self.contract_sha256,
            "create_once": self.create_once,
            "allow_download": self.allow_download,
            "auth": _json_copy(self.auth),
            "source_refs": _json_copy(self.source_refs),
            "execution_request": _json_copy(self.execution_request),
        }
        if self.artifact_root:
            payload["artifact_root"] = self.artifact_root
        if self.artifact_cifs_root:
            payload["artifact_cifs_root"] = self.artifact_cifs_root
        return payload


@dataclass(frozen=True)
class DirectVmStatus:
    """Exact read-only status returned by the injected status transport."""

    state: DirectVmObservedState
    task_id: str
    submission_key: str
    identity_sha256: str

    def to_dict(self) -> dict[str, str]:
        return {
            "state": self.state,
            "task_id": self.task_id,
            "submission_key": self.submission_key,
            "identity_sha256": self.identity_sha256,
        }


@dataclass(frozen=True)
class DirectVmSubmitOutcome:
    """Transport-neutral decision from :func:`status_first_submit`."""

    outcome: DirectVmSubmitOutcomeName
    task_id: str
    submission_key: str
    identity_sha256: str
    observed_state: str
    reason: str
    retryable: bool
    create_attempted: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "outcome": self.outcome,
            "task_id": self.task_id,
            "submission_key": self.submission_key,
            "identity_sha256": self.identity_sha256,
            "observed_state": self.observed_state,
            "reason": self.reason,
            "retryable": self.retryable,
            "create_attempted": self.create_attempted,
        }


def _fail(code: str) -> DirectVmSubmitError:
    return DirectVmSubmitError(code)


def _normalized_key(value: str) -> str:
    camel_split = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", value)
    return re.sub(r"[^a-z0-9]+", "_", camel_split.lower()).strip("_")


def _normalize_json(value: Any) -> Any:
    nodes = 0

    def visit(item: Any, *, depth: int) -> Any:
        nonlocal nodes
        nodes += 1
        if nodes > DIRECT_VM_MAX_JSON_NODES or depth > DIRECT_VM_MAX_JSON_DEPTH:
            raise _fail("direct_vm_json_shape_exceeded")
        if isinstance(item, Mapping):
            result: dict[str, Any] = {}
            for key, child in item.items():
                if not isinstance(key, str) or not key:
                    raise _fail("direct_vm_json_key_invalid")
                result[key] = visit(child, depth=depth + 1)
            return result
        if isinstance(item, list):
            return [visit(child, depth=depth + 1) for child in item]
        if item is None or type(item) in {bool, int, str}:
            return item
        if type(item) is float and math.isfinite(item):
            return item
        raise _fail("direct_vm_json_value_invalid")

    normalized = visit(value, depth=1)
    encoded = _canonical_json(normalized).encode("utf-8")
    if len(encoded) > DIRECT_VM_MAX_JSON_BYTES:
        raise _fail("direct_vm_json_bytes_exceeded")
    return normalized


def _canonical_json(value: Any) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise _fail("direct_vm_json_invalid") from exc


def _json_copy(value: Any) -> Any:
    return json.loads(_canonical_json(value))


def _canonical_sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _required_text(value: Any, field: str, *, maximum: int = 512) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise _fail(f"direct_vm_{field}_invalid")
    if len(value) > maximum or any(ord(character) < 32 for character in value):
        raise _fail(f"direct_vm_{field}_invalid")
    return value


def _task_id(value: Any, field: str) -> str:
    text = _required_text(value, field, maximum=128)
    if not _SAFE_TASK_ID_RE.fullmatch(text):
        raise _fail(f"direct_vm_{field}_invalid")
    return text


def _sha256(value: Any, field: str) -> str:
    if not isinstance(value, str) or not _SHA256_RE.fullmatch(value):
        raise _fail(f"direct_vm_{field}_invalid")
    return value


def _scan_forbidden_contract(value: Any) -> None:
    stack: list[tuple[str, Any]] = [("$", value)]
    while stack:
        path, item = stack.pop()
        if isinstance(item, dict):
            for key, child in item.items():
                normalized = _normalized_key(key)
                if normalized in _FORBIDDEN_EXACT_KEYS or normalized.startswith(
                    _FORBIDDEN_KEY_PREFIXES
                ):
                    raise _fail("direct_vm_forbidden_field")
                if normalized == "allow_download":
                    if child is not False:
                        raise _fail("direct_vm_download_not_disabled")
                elif normalized in _FORBIDDEN_DOWNLOAD_KEYS:
                    raise _fail("direct_vm_download_field_forbidden")
                if normalized == "input_materialization" and child != "forbidden":
                    raise _fail("direct_vm_input_materialization_not_forbidden")
                if normalized == "data_access_mode" and child != "remote_read":
                    raise _fail("direct_vm_data_access_mode_invalid")
                stack.append((f"{path}.{key}", child))
        elif isinstance(item, list):
            stack.extend(
                (f"{path}[{index}]", child) for index, child in enumerate(item)
            )
        elif isinstance(item, str):
            normalized = _normalized_key(item)
            if normalized in _FORBIDDEN_EXACT_VALUES:
                raise _fail("direct_vm_forbidden_value")
            if normalized in {"minimal_download", "full_download", "mdi_download"}:
                raise _fail("direct_vm_download_value_forbidden")
            if _DOWNLOAD_COMMAND_RE.search(item):
                raise _fail("direct_vm_download_command_forbidden")


def _execution_request(value: Any) -> dict[str, Any]:
    try:
        payload = validate_vm_execution_request_envelope(
            value,
            max_bytes=DIRECT_VM_MAX_JSON_BYTES,
        )
    except (TypeError, ValueError) as exc:
        raise _fail("direct_vm_execution_request_invalid") from exc
    if not isinstance(payload, dict) or not payload:
        raise _fail("direct_vm_execution_request_invalid")
    if payload.get("schema_version") != RCA_EXECUTION_REQUEST_SCHEMA_VERSION:
        raise _fail("direct_vm_execution_request_schema_invalid")
    if not _EXECUTION_REQUEST_REQUIRED_FIELDS <= frozenset(payload):
        raise _fail("direct_vm_execution_request_fields_invalid")
    if payload.get("request_kind") != "issue_intake":
        raise _fail("direct_vm_execution_request_kind_invalid")
    for field in ("work_item", "data", "execution_policy", "source_refs"):
        if not isinstance(payload.get(field), dict):
            raise _fail("direct_vm_execution_request_fields_invalid")
    work_item = payload["work_item"]
    for field in ("project_key", "work_item_type", "work_item_id"):
        _required_text(work_item.get(field), f"work_item_{field}", maximum=256)
    execution_policy = payload["execution_policy"]
    if execution_policy.get("data_access_mode") != "remote_read":
        raise _fail("direct_vm_data_access_mode_invalid")
    if execution_policy.get("allow_download") is not False:
        raise _fail("direct_vm_download_not_disabled")
    if execution_policy.get("input_materialization") != "forbidden":
        raise _fail("direct_vm_input_materialization_not_forbidden")
    try:
        payload["data"]["data_access"] = validate_remote_data_access(
            payload["data"].get("data_access")
        )
    except (RemoteDataAccessError, TypeError, ValueError) as exc:
        raise _fail("direct_vm_remote_data_access_invalid") from exc
    return _normalize_json(payload)


def _artifact_root(value: Any, task_id: str) -> str:
    raw = _required_text(value, "artifact_root", maximum=1024)
    if not raw.startswith("/") or raw.startswith("//") or "\\" in raw or "\x00" in raw:
        raise _fail("direct_vm_artifact_root_invalid")
    normalized = posixpath.normpath(raw)
    canonical = normalized.rstrip("/") + "/"
    if raw != canonical or normalized != f"/mnt/tmp/{task_id}":
        raise _fail("direct_vm_artifact_root_invalid")
    return canonical


def _artifact_cifs_root(value: Any, task_id: str) -> str:
    raw = _required_text(value, "artifact_cifs_root", maximum=2048)
    if (
        not raw.startswith("//")
        or raw.startswith("///")
        or "\\" in raw
        or "\x00" in raw
    ):
        raise _fail("direct_vm_artifact_cifs_root_invalid")
    normalized = posixpath.normpath(raw)
    canonical = normalized.rstrip("/") + "/"
    components = normalized[2:].split("/")
    if (
        raw != canonical
        or len(components) < 3
        or any(component in {"", ".", ".."} for component in components)
        or components[-1] != task_id
    ):
        raise _fail("direct_vm_artifact_cifs_root_invalid")
    return canonical


def _identity_material(payload: Mapping[str, Any]) -> dict[str, Any]:
    material = dict(payload)
    material.pop("identity_sha256", None)
    return material


def build_direct_vm_request(
    *,
    task_id: str,
    submission_key: str,
    auth: Mapping[str, Any],
    source_refs: Mapping[str, Any],
    execution_request: Mapping[str, Any],
    artifact_root: str = "",
    artifact_cifs_root: str = "",
) -> DirectVmSubmitRequest:
    """Build and then strictly validate one canonical direct submit envelope."""

    normalized_execution = _execution_request(execution_request)
    payload: dict[str, Any] = {
        "schema_version": DIRECT_VM_SUBMIT_SCHEMA_VERSION,
        "task_id": task_id,
        "submission_key": submission_key,
        "contract_sha256": _canonical_sha256(normalized_execution),
        "create_once": True,
        "allow_download": False,
        "auth": dict(auth),
        "source_refs": dict(source_refs),
        "execution_request": normalized_execution,
    }
    if artifact_root:
        payload["artifact_root"] = artifact_root
    if artifact_cifs_root:
        payload["artifact_cifs_root"] = artifact_cifs_root
    payload["identity_sha256"] = _canonical_sha256(_identity_material(payload))
    return validate_direct_vm_request(payload)


def validate_direct_vm_request(
    value: DirectVmSubmitRequest | Mapping[str, Any],
) -> DirectVmSubmitRequest:
    """Validate and re-derive the strict direct envelope and both hashes."""

    if isinstance(value, DirectVmSubmitRequest):
        raw: Any = value.to_dict()
    elif isinstance(value, Mapping):
        raw = dict(value)
    else:
        raise _fail("direct_vm_envelope_invalid")
    payload = _normalize_json(raw)
    if not isinstance(payload, dict):
        raise _fail("direct_vm_envelope_invalid")
    fields = frozenset(payload)
    if not _REQUIRED_ENVELOPE_FIELDS <= fields:
        raise _fail("direct_vm_envelope_missing_field")
    if fields - _REQUIRED_ENVELOPE_FIELDS - _OPTIONAL_ENVELOPE_FIELDS:
        raise _fail("direct_vm_envelope_unknown_field")
    if payload["schema_version"] != DIRECT_VM_SUBMIT_SCHEMA_VERSION:
        raise _fail("direct_vm_schema_version_invalid")

    task_id = _task_id(payload["task_id"], "task_id")
    submission_key = _task_id(payload["submission_key"], "submission_key")
    if task_id != submission_key:
        raise _fail("direct_vm_task_identity_mismatch")
    if payload["create_once"] is not True:
        raise _fail("direct_vm_create_once_required")
    if payload["allow_download"] is not False:
        raise _fail("direct_vm_download_not_disabled")

    auth = payload["auth"]
    if not isinstance(auth, dict) or frozenset(auth) != _AUTH_FIELDS:
        raise _fail("direct_vm_auth_invalid")
    principal = _required_text(auth.get("principal"), "auth_principal", maximum=256)
    capability = _required_text(auth.get("capability"), "auth_capability", maximum=256)
    auth = {"principal": principal, "capability": capability}

    source_refs = payload["source_refs"]
    if (
        not isinstance(source_refs, dict)
        or frozenset(source_refs) != _SOURCE_REF_FIELDS
    ):
        raise _fail("direct_vm_source_refs_invalid")
    generation = source_refs.get("generation")
    if (
        isinstance(generation, bool)
        or not isinstance(generation, int)
        or generation < 1
    ):
        raise _fail("direct_vm_source_generation_invalid")
    normalized_source_refs = {
        "origin_source_id": _required_text(
            source_refs.get("origin_source_id"), "origin_source_id", maximum=256
        ),
        "source_event_id": _required_text(
            source_refs.get("source_event_id"), "source_event_id", maximum=512
        ),
        "generation": generation,
        "business_key": _required_text(
            source_refs.get("business_key"), "business_key", maximum=256
        ),
        "submission_key": _task_id(
            source_refs.get("submission_key"), "source_submission_key"
        ),
    }
    if normalized_source_refs["submission_key"] != submission_key:
        raise _fail("direct_vm_source_identity_mismatch")

    execution_request = _execution_request(payload["execution_request"])
    artifact_root = ""
    if "artifact_root" in payload:
        artifact_root = _artifact_root(payload["artifact_root"], task_id)
    artifact_cifs_root = ""
    if "artifact_cifs_root" in payload:
        artifact_cifs_root = _artifact_cifs_root(payload["artifact_cifs_root"], task_id)

    nested_source_refs = execution_request.get("source_refs")
    if nested_source_refs is not None and nested_source_refs != normalized_source_refs:
        raise _fail("direct_vm_execution_source_refs_mismatch")
    data = execution_request.get("data")
    data = data if isinstance(data, dict) else {}
    policy = execution_request.get("execution_policy")
    policy = policy if isinstance(policy, dict) else {}
    nested_artifact_roots = [
        item
        for item in (data.get("artifact_root"), policy.get("artifact_root"))
        if item not in {None, ""}
    ]
    if nested_artifact_roots and (
        not artifact_root
        or any(item != artifact_root for item in nested_artifact_roots)
    ):
        raise _fail("direct_vm_execution_artifact_root_mismatch")
    nested_cifs_root = data.get("artifact_cifs_root")
    if nested_cifs_root not in {None, ""} and nested_cifs_root != artifact_cifs_root:
        raise _fail("direct_vm_execution_artifact_cifs_root_mismatch")

    canonical_payload: dict[str, Any] = {
        "schema_version": DIRECT_VM_SUBMIT_SCHEMA_VERSION,
        "task_id": task_id,
        "submission_key": submission_key,
        "identity_sha256": _sha256(payload["identity_sha256"], "identity_sha256"),
        "contract_sha256": _sha256(payload["contract_sha256"], "contract_sha256"),
        "create_once": True,
        "allow_download": False,
        "auth": auth,
        "source_refs": normalized_source_refs,
        "execution_request": execution_request,
    }
    if artifact_root:
        canonical_payload["artifact_root"] = artifact_root
    if artifact_cifs_root:
        canonical_payload["artifact_cifs_root"] = artifact_cifs_root

    _scan_forbidden_contract(canonical_payload)
    if canonical_payload["contract_sha256"] != _canonical_sha256(execution_request):
        raise _fail("direct_vm_contract_hash_mismatch")
    if canonical_payload["identity_sha256"] != _canonical_sha256(
        _identity_material(canonical_payload)
    ):
        raise _fail("direct_vm_identity_hash_mismatch")

    return DirectVmSubmitRequest(
        schema_version=DIRECT_VM_SUBMIT_SCHEMA_VERSION,
        task_id=task_id,
        submission_key=submission_key,
        identity_sha256=canonical_payload["identity_sha256"],
        contract_sha256=canonical_payload["contract_sha256"],
        create_once=True,
        allow_download=False,
        auth=auth,
        source_refs=normalized_source_refs,
        execution_request=execution_request,
        artifact_root=artifact_root,
        artifact_cifs_root=artifact_cifs_root,
    )


def _validate_status(value: Mapping[str, Any]) -> DirectVmStatus:
    payload = _normalize_json(value)
    if not isinstance(payload, dict) or frozenset(payload) != _STATUS_FIELDS:
        raise _fail("direct_vm_status_invalid")
    state = payload.get("state")
    if state not in {"missing", "unknown", "existing", "completed", "failed"}:
        raise _fail("direct_vm_status_invalid")
    task_id = _task_id(payload.get("task_id"), "status_task_id")
    submission_key = payload.get("submission_key")
    identity_sha256 = payload.get("identity_sha256")
    if state == "missing":
        if submission_key != "" or identity_sha256 != "":
            raise _fail("direct_vm_status_invalid")
    elif state == "unknown":
        empty_identity = submission_key == "" and identity_sha256 == ""
        complete_identity = (
            isinstance(submission_key, str)
            and _SAFE_TASK_ID_RE.fullmatch(submission_key) is not None
            and isinstance(identity_sha256, str)
            and _SHA256_RE.fullmatch(identity_sha256) is not None
        )
        if not empty_identity and not complete_identity:
            raise _fail("direct_vm_status_invalid")
    else:
        submission_key = _task_id(submission_key, "status_submission_key")
        identity_sha256 = _sha256(identity_sha256, "status_identity_sha256")
    return DirectVmStatus(
        state=state,
        task_id=task_id,
        submission_key=str(submission_key),
        identity_sha256=str(identity_sha256),
    )


def _read_status(
    transport: StatusTransport, task_id: str
) -> tuple[DirectVmStatus | None, str]:
    try:
        raw = transport(task_id)
    except Exception:
        return None, "status_unavailable"
    if not isinstance(raw, Mapping):
        return None, "status_unavailable"
    try:
        return _validate_status(raw), ""
    except DirectVmSubmitError:
        return None, "status_invalid"


def _matches(request: DirectVmSubmitRequest, status: DirectVmStatus) -> bool:
    return (
        status.task_id == request.task_id
        and status.submission_key == request.submission_key
        and status.identity_sha256 == request.identity_sha256
    )


def _outcome(
    request: DirectVmSubmitRequest,
    outcome: DirectVmSubmitOutcomeName,
    *,
    observed_state: str,
    reason: str,
    create_attempted: bool,
) -> DirectVmSubmitOutcome:
    return DirectVmSubmitOutcome(
        outcome=outcome,
        task_id=request.task_id,
        submission_key=request.submission_key,
        identity_sha256=request.identity_sha256,
        observed_state=observed_state,
        reason=reason,
        retryable=outcome == "retry",
        create_attempted=create_attempted,
    )


def status_first_submit(
    request: DirectVmSubmitRequest | Mapping[str, Any],
    status_transport: StatusTransport,
    create_transport: CreateTransport,
) -> DirectVmSubmitOutcome:
    """Read status, create only on proven absence, then read status again."""

    validated = validate_direct_vm_request(request)
    if not callable(status_transport) or not callable(create_transport):
        raise TypeError("direct VM status and create transports must be callable")

    initial, status_error = _read_status(status_transport, validated.task_id)
    if initial is None:
        return _outcome(
            validated,
            "retry",
            observed_state="unavailable",
            reason=f"pre_{status_error}",
            create_attempted=False,
        )
    if initial.state == "unknown":
        return _outcome(
            validated,
            "retry",
            observed_state="unknown",
            reason="pre_status_unknown",
            create_attempted=False,
        )
    if initial.state in {"existing", "completed", "failed"}:
        if _matches(validated, initial):
            return _outcome(
                validated,
                "deduplicated",
                observed_state=initial.state,
                reason="pre_status_identity_match",
                create_attempted=False,
            )
        return _outcome(
            validated,
            "permanent_conflict",
            observed_state=initial.state,
            reason="pre_status_identity_mismatch",
            create_attempted=False,
        )
    if initial.task_id != validated.task_id:
        return _outcome(
            validated,
            "retry",
            observed_state="missing",
            reason="pre_missing_target_mismatch",
            create_attempted=False,
        )

    create_observation = "create_acknowledged"
    try:
        create_result = create_transport(validated.to_dict())
        if not isinstance(create_result, Mapping):
            create_observation = "create_response_invalid"
    except Exception:
        create_observation = "create_unavailable"

    observed, status_error = _read_status(status_transport, validated.task_id)
    if observed is None:
        return _outcome(
            validated,
            "retry",
            observed_state="unavailable",
            reason=f"post_{status_error}:{create_observation}",
            create_attempted=True,
        )
    if observed.state == "unknown":
        return _outcome(
            validated,
            "retry",
            observed_state="unknown",
            reason=f"post_status_unknown:{create_observation}",
            create_attempted=True,
        )
    if observed.state == "missing":
        return _outcome(
            validated,
            "retry",
            observed_state="missing",
            reason=f"post_status_missing:{create_observation}",
            create_attempted=True,
        )
    if _matches(validated, observed):
        return _outcome(
            validated,
            "reconciled",
            observed_state=observed.state,
            reason="post_status_identity_match",
            create_attempted=True,
        )
    return _outcome(
        validated,
        "permanent_conflict",
        observed_state=observed.state,
        reason="post_status_identity_mismatch",
        create_attempted=True,
    )


__all__ = [
    "DIRECT_VM_SUBMIT_SCHEMA_VERSION",
    "DirectVmStatus",
    "DirectVmSubmitError",
    "DirectVmSubmitOutcome",
    "DirectVmSubmitRequest",
    "build_direct_vm_request",
    "status_first_submit",
    "validate_direct_vm_request",
]
