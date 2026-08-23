"""Self-contained validator for the direct RCA VM envelope.

The VM-side creator loads this file as a pinned, regular file.  It deliberately
has no ``gateway`` imports: the Host owns request construction, while the VM
must independently re-check the complete envelope before touching shared-state.
Keep this module limited to JSON/schema validation and deterministic hashing;
it must not read credentials, network state, or production configuration.
"""

from __future__ import annotations

from collections.abc import Mapping
import hashlib
import json
import math
import posixpath
import re
from typing import Any


DIRECT_VM_VALIDATOR_SCHEMA_VERSION = "g1q3_rca_direct_vm_validator_v1"
DIRECT_VM_SUBMIT_SCHEMA_VERSION = "g1q3_rca_direct_vm_submit_envelope_v1"
RCA_EXECUTION_REQUEST_SCHEMA_VERSION = "g1q3_rca_execution_request_v2"
RCA_REMOTE_DATA_ACCESS_SCHEMA_VERSION = "g1q3_rca_remote_data_access_v1"
DIRECT_VM_MAX_JSON_BYTES = 1024 * 1024
DIRECT_VM_MAX_JSON_DEPTH = 32
DIRECT_VM_MAX_JSON_NODES = 50_000
MAX_REMOTE_REFERENCES = 16
MAX_REMOTE_REFERENCE_LENGTH = 512

_SAFE_TASK_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_DOWNLOAD_COMMAND_RE = re.compile(
    r"\b(?:mdi|pdcl)(?:\s+|[-_])(?:download|refresh(?:2)?)\b",
    re.IGNORECASE,
)
_PLACEHOLDER_ID_RE = re.compile(
    r"^(?:default|fallback|placeholder|unknown|unset|missing|none|null|todo|tbd)"
    r"(?:[-_:].*)?$",
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
_EXECUTION_REQUEST_REQUIRED_FIELDS = frozenset({
    "schema_version",
    "request_kind",
    "work_item",
    "data",
    "execution_policy",
    "source_refs",
})
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
_SENSITIVE_KEYS = frozenset({
    "raw",
    "raw_payload",
    "raw_feishu_payload",
    "full_payload",
    "secret",
    "token",
})
_EXECUTION_REQUEST_FIELDS = _EXECUTION_REQUEST_REQUIRED_FIELDS


class DirectVmValidatorError(ValueError):
    """Typed validation failure returned by the pinned VM validator."""

    def __init__(self, code: str, detail: str = "") -> None:
        self.code = str(code)
        self.detail = str(detail)
        super().__init__(f"{self.code}: {self.detail}" if detail else self.code)


def _fail(code: str, detail: str = "") -> DirectVmValidatorError:
    return DirectVmValidatorError(code, detail)


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


def _normalize_json(value: Any) -> Any:
    """Normalize JSON and enforce the same bounded shape as the Host."""

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
    if len(_canonical_json(normalized).encode("utf-8")) > DIRECT_VM_MAX_JSON_BYTES:
        raise _fail("direct_vm_json_bytes_exceeded")
    return normalized


def _required_text(value: Any, field: str, *, maximum: int = 512) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise _fail(f"direct_vm_{field}_invalid")
    if len(value) > maximum or any(ord(character) < 32 for character in value):
        raise _fail(f"direct_vm_{field}_invalid")
    return value


def _task_id(value: Any, field: str) -> str:
    text = _required_text(value, field, maximum=128)
    if _SAFE_TASK_ID_RE.fullmatch(text) is None:
        raise _fail(f"direct_vm_{field}_invalid")
    return text


def _sha256(value: Any, field: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise _fail(f"direct_vm_{field}_invalid")
    return value


def _is_placeholder_reference(value: str) -> bool:
    text = str(value or "").strip()
    compact = re.sub(r"[-_:]", "", text)
    return bool(
        not text
        or _PLACEHOLDER_ID_RE.fullmatch(text)
        or (compact and set(compact) == {"0"})
    )


def _scan_forbidden_contract(value: Any) -> None:
    stack: list[Any] = [value]
    while stack:
        item = stack.pop()
        if isinstance(item, Mapping):
            for key, child in item.items():
                normalized = _normalized_key(key)
                if normalized in _SENSITIVE_KEYS:
                    raise _fail("direct_vm_sensitive_field_forbidden")
                if normalized in _FORBIDDEN_EXACT_KEYS or normalized.startswith(
                    _FORBIDDEN_KEY_PREFIXES
                ):
                    raise _fail("direct_vm_forbidden_field")
                if normalized == "allow_download" and child is not False:
                    raise _fail("direct_vm_download_not_disabled")
                if normalized in _FORBIDDEN_DOWNLOAD_KEYS:
                    raise _fail("direct_vm_download_field_forbidden")
                if normalized == "input_materialization" and child != "forbidden":
                    raise _fail("direct_vm_input_materialization_not_forbidden")
                if normalized == "data_access_mode" and child != "remote_read":
                    raise _fail("direct_vm_data_access_mode_invalid")
                stack.append(child)
        elif isinstance(item, list):
            stack.extend(item)
        elif isinstance(item, str):
            normalized = _normalized_key(item)
            if normalized in _FORBIDDEN_EXACT_VALUES:
                raise _fail("direct_vm_forbidden_value")
            if normalized in {"minimal_download", "full_download", "mdi_download"}:
                raise _fail("direct_vm_download_value_forbidden")
            if _DOWNLOAD_COMMAND_RE.search(item):
                raise _fail("direct_vm_download_command_forbidden")


def _normalized_key(value: str) -> str:
    camel_split = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", value)
    return re.sub(r"[^a-z0-9]+", "_", camel_split.lower()).strip("_")


def _validate_remote_data_access(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise _fail("direct_vm_remote_data_access_invalid")
    access = dict(value)
    if set(access) != {
        "schema_version",
        "mode",
        "transport",
        "references",
        "source",
        "reader_contract",
    }:
        raise _fail("direct_vm_remote_data_access_invalid")
    if access.get("schema_version") != RCA_REMOTE_DATA_ACCESS_SCHEMA_VERSION:
        raise _fail("direct_vm_remote_data_access_invalid")
    if access.get("mode") != "remote_read" or access.get("transport") != "pdcl_pyclip":
        raise _fail("direct_vm_remote_data_access_invalid")
    source = access.get("source")
    if not isinstance(source, Mapping) or set(source) != {"field", "value_sha256"}:
        raise _fail("direct_vm_remote_data_access_invalid")
    if (
        source.get("field") != "问题数据地址_PDCL"
        or _SHA256_RE.fullmatch(str(source.get("value_sha256") or "")) is None
    ):
        raise _fail("direct_vm_remote_data_access_invalid")
    expected_reader_contract = {
        "distribution": "pdcl_pyclip",
        "required_version": "0.1.6+rca.2",
        "mdi_download_allowed": False,
        "fallback": "forbidden",
        "completeness": "full_requested_scope",
    }
    if access.get("reader_contract") != expected_reader_contract:
        raise _fail("direct_vm_remote_data_access_invalid")
    references = access.get("references")
    if (
        not isinstance(references, list)
        or not 1 <= len(references) <= MAX_REMOTE_REFERENCES
    ):
        raise _fail("direct_vm_remote_data_access_invalid")
    normalized: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for item in references:
        if not isinstance(item, Mapping):
            raise _fail("direct_vm_remote_data_access_invalid")
        kind = str(item.get("kind") or "")
        if kind == "event":
            locator_key, reader_class = "event_uuid", "RemoteEventReader"
        elif kind == "clip":
            locator_key, reader_class = "clip_uuid", "RemoteClipReader"
        else:
            raise _fail("direct_vm_remote_data_access_invalid")
        if set(item) != {"kind", locator_key, "reader_class"}:
            raise _fail("direct_vm_remote_data_access_invalid")
        locator = item.get(locator_key)
        if (
            not isinstance(locator, str)
            or not locator
            or locator != locator.strip()
            or len(locator) > MAX_REMOTE_REFERENCE_LENGTH
            or "\x00" in locator
            or _is_placeholder_reference(locator)
            or item.get("reader_class") != reader_class
        ):
            raise _fail("direct_vm_remote_data_access_invalid")
        identity = (kind, locator)
        if identity in seen:
            raise _fail("direct_vm_remote_data_access_invalid")
        seen.add(identity)
        normalized.append({
            "kind": kind,
            locator_key: locator,
            "reader_class": reader_class,
        })
    return {
        **access,
        "source": dict(source),
        "references": normalized,
        "reader_contract": dict(expected_reader_contract),
    }


def _validate_execution_request(value: Any) -> dict[str, Any]:
    payload = _normalize_json(value)
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
    policy = payload["execution_policy"]
    if policy.get("data_access_mode") != "remote_read":
        raise _fail("direct_vm_data_access_mode_invalid")
    if policy.get("allow_download") is not False:
        raise _fail("direct_vm_download_not_disabled")
    if policy.get("input_materialization") != "forbidden":
        raise _fail("direct_vm_input_materialization_not_forbidden")
    data = payload["data"]
    data["data_access"] = _validate_remote_data_access(data.get("data_access"))
    return payload


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


def validate_direct_vm_request(value: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and normalize one direct VM envelope without importing Host code."""

    if not isinstance(value, Mapping):
        raise _fail("direct_vm_envelope_invalid")
    payload = _normalize_json(dict(value))
    if not isinstance(payload, dict):  # pragma: no cover - guarded above
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
    auth = {
        "principal": _required_text(
            auth.get("principal"), "auth_principal", maximum=256
        ),
        "capability": _required_text(
            auth.get("capability"), "auth_capability", maximum=256
        ),
    }

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

    execution_request = _validate_execution_request(payload["execution_request"])
    artifact_root = (
        _artifact_root(payload["artifact_root"], task_id)
        if "artifact_root" in payload
        else ""
    )
    artifact_cifs_root = (
        _artifact_cifs_root(payload["artifact_cifs_root"], task_id)
        if "artifact_cifs_root" in payload
        else ""
    )

    nested_source_refs = execution_request.get("source_refs")
    if nested_source_refs is not None and nested_source_refs != normalized_source_refs:
        raise _fail("direct_vm_execution_source_refs_mismatch")
    data = execution_request.get("data") or {}
    policy = execution_request.get("execution_policy") or {}
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
    if (
        canonical_payload["contract_sha256"]
        != hashlib.sha256(
            _canonical_json(execution_request).encode("utf-8")
        ).hexdigest()
    ):
        raise _fail("direct_vm_contract_hash_mismatch")
    if (
        canonical_payload["identity_sha256"]
        != hashlib.sha256(
            _canonical_json(_identity_material(canonical_payload)).encode("utf-8")
        ).hexdigest()
    ):
        raise _fail("direct_vm_identity_hash_mismatch")
    return canonical_payload


__all__ = [
    "DIRECT_VM_VALIDATOR_SCHEMA_VERSION",
    "DIRECT_VM_SUBMIT_SCHEMA_VERSION",
    "DirectVmValidatorError",
    "validate_direct_vm_request",
]
