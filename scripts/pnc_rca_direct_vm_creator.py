"""Independent VM-side creator/status protocol for direct RCA tasks.

This module is intentionally usable as a pinned remote helper.  It only
touches the canonical shared-state root when ``create_direct_vm_task`` is
called after the Host status-first check.  Status reads never create a root,
database, WAL, queue, or sidecar.
"""

from __future__ import annotations

from collections.abc import Mapping
import hashlib
import importlib.util
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import sys
from typing import Any


DIRECT_VM_CREATOR_SCHEMA_VERSION = "g1q3_rca_direct_vm_creator_v1"
DIRECT_VM_TRANSPORT_PROTOCOL_VERSION = "g1q3_rca_direct_vm_transport_v1"
DIRECT_VM_SUBMIT_SCHEMA_VERSION = "g1q3_rca_direct_vm_submit_envelope_v1"
DIRECT_VM_VALIDATOR_SCHEMA_VERSION = "g1q3_rca_direct_vm_validator_v1"
DIRECT_VM_HUMANIZER_MODULE_NAME = "vm_feishu_humanizer"
MAX_ENVELOPE_BYTES = 1024 * 1024
MAX_METADATA_BYTES = 1024 * 1024
MAX_JSON_DEPTH = 32
MAX_JSON_NODES = 50_000
DIRECT_VM_AUTH_PRINCIPAL = "pnc-rca-direct-outbox"
DIRECT_VM_AUTH_CAPABILITY = "g1q3_rca_direct_vm_submit"
_TASK_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_STATE_FIELDS = frozenset({"state", "task_id", "submission_key", "identity_sha256"})
_TERMINAL_SUCCESS = frozenset({"completed", "succeeded", "success", "done"})
_TERMINAL_FAILURE = frozenset({
    "failed",
    "abandoned",
    "cancelled",
    "canceled",
    "timeout",
})
_ACTIVE = frozenset({
    "pending",
    "queued",
    "claimed",
    "in_progress",
    "running",
    "waiting",
    "blocked",
    "accepted",
    "created",
    "submitted",
})
_DISPATCH_BUCKETS = ("pending", "claimed", "done", "failed")
_FORBIDDEN_KEYS = frozenset({
    "activation_epoch",
    "capacity",
    "capacity_mode",
    "epoch",
    "epoch_id",
    "lane",
    "prod_receipt",
    "release",
    "release_binding",
    "release_id",
    "resource_class",
    "risk_class",
    "runtime_release",
    "w3",
    "w3_snapshot",
    "workspace",
    "workspace_runtime",
    "write_fence",
    "queue_if_blocked",
    "rca_prod",
})
_FORBIDDEN_KEY_PREFIXES = (
    "activation_epoch_",
    "derived_capacity_",
    "release_binding_",
    "rca_prod_",
    "w3_",
)
_FORBIDDEN_DOWNLOAD_KEYS = frozenset({
    "download_command",
    "download_url",
    "mdi_download_cmd",
    "pdcl_download_cmd",
})


class DirectVmCreatorError(RuntimeError):
    """Typed helper failure; the Host maps it to an unknown/retry state."""

    def __init__(self, code: str, detail: str = "") -> None:
        self.code = str(code)
        self.detail = str(detail)
        super().__init__(f"{self.code}: {self.detail}" if self.detail else self.code)


def _text(value: Any) -> str:
    return str(value or "").strip()


def _safe_task_id(value: Any) -> str:
    task_id = _text(value)
    if _TASK_ID_RE.fullmatch(task_id) is None:
        raise DirectVmCreatorError("direct_vm_task_id_invalid")
    return task_id


def _safe_root(value: Any) -> Path:
    raw = _text(value)
    pure = PurePosixPath(raw)
    if (
        not raw.startswith("/")
        or "//" in raw
        or "\\" in raw
        or "\x00" in raw
        or ".." in pure.parts
        or raw.rstrip("/") != raw
    ):
        raise DirectVmCreatorError("direct_vm_shared_state_root_invalid")
    if not (raw.startswith("/home/mini/") or raw.startswith("/mnt/tmp/")):
        raise DirectVmCreatorError("direct_vm_shared_state_root_forbidden")
    if len(pure.parts) < 4:
        raise DirectVmCreatorError("direct_vm_shared_state_root_invalid")
    return Path(raw)


def _safe_child(root: Path, *parts: str) -> Path:
    if any(
        not part or part in {".", ".."} or "/" in part or "\\" in part or "\x00" in part
        for part in parts
    ):
        raise DirectVmCreatorError("direct_vm_path_segment_invalid")
    return root.joinpath(*parts)


def _validate_create_root(root: Path) -> None:
    """Validate existing ancestors while allowing the final root to be new."""

    current = Path(root.parts[0]) if root.parts else Path()
    components = root.parts[1:]
    for index, component in enumerate(components):
        current /= component
        try:
            observed = current.lstat()
        except FileNotFoundError:
            # No later component can exist once an ancestor is absent.  The
            # canonical creator may create the remaining directories.
            return
        except OSError as exc:
            raise DirectVmCreatorError("direct_vm_root_stat_failed") from exc
        if stat.S_ISLNK(observed.st_mode):
            raise DirectVmCreatorError("direct_vm_path_symlink_forbidden")
        if index < len(components) - 1 and not stat.S_ISDIR(observed.st_mode):
            raise DirectVmCreatorError("direct_vm_path_component_invalid")
        if index == len(components) - 1 and not stat.S_ISDIR(observed.st_mode):
            raise DirectVmCreatorError("direct_vm_root_not_directory")


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    """Write metadata without following a symlink or leaving partial JSON."""

    if not isinstance(payload, Mapping):
        raise DirectVmCreatorError("direct_vm_metadata_invalid")
    parent = path.parent
    _reject_symlink_components(parent)
    if path.exists() or path.is_symlink():
        _lstat_regular(path, max_bytes=MAX_METADATA_BYTES)
    encoded = (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    ).encode("utf-8")
    if len(encoded) > MAX_METADATA_BYTES:
        raise DirectVmCreatorError("direct_vm_metadata_too_large")
    temporary = parent / f".{path.name}.{os.getpid()}.direct-vm.tmp"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    try:
        fd = os.open(temporary, flags, 0o600)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
        except Exception:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass
            raise
    except FileExistsError as exc:
        raise DirectVmCreatorError("direct_vm_metadata_write_busy") from exc


def _acquire_create_lock(root: Path, task_id: str) -> tuple[Path, int]:
    runtime = _safe_child(root, ".runtime")
    try:
        runtime_stat = runtime.lstat()
    except FileNotFoundError:
        runtime.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise DirectVmCreatorError("direct_vm_runtime_stat_failed") from exc
    else:
        if stat.S_ISLNK(runtime_stat.st_mode) or not stat.S_ISDIR(runtime_stat.st_mode):
            raise DirectVmCreatorError("direct_vm_runtime_invalid")
    _reject_symlink_components(runtime)
    lock_path = _safe_child(runtime, f"direct-vm-create.{task_id}.lock")
    try:
        fd = os.open(lock_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError as exc:
        raise DirectVmCreatorError("direct_vm_create_lock_busy") from exc
    try:
        os.write(fd, f"pid={os.getpid()}\n".encode("ascii"))
    except OSError:
        os.close(fd)
        lock_path.unlink(missing_ok=True)
        raise
    return lock_path, fd


def _release_create_lock(lock_path: Path, fd: int) -> None:
    try:
        os.close(fd)
    finally:
        try:
            lock_path.unlink()
        except FileNotFoundError:
            pass


def _lstat_regular(path: Path, *, max_bytes: int | None = None) -> os.stat_result:
    try:
        observed = path.lstat()
    except FileNotFoundError as exc:
        raise DirectVmCreatorError("direct_vm_file_missing") from exc
    except OSError as exc:
        raise DirectVmCreatorError("direct_vm_file_stat_failed") from exc
    if not stat.S_ISREG(observed.st_mode):
        raise DirectVmCreatorError("direct_vm_file_not_regular")
    if observed.st_nlink != 1:
        raise DirectVmCreatorError("direct_vm_file_hardlink_forbidden")
    if max_bytes is not None and observed.st_size > max_bytes:
        raise DirectVmCreatorError("direct_vm_file_too_large")
    return observed


def _read_json(path: Path, *, max_bytes: int = MAX_METADATA_BYTES) -> Any:
    _lstat_regular(path, max_bytes=max_bytes)
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise DirectVmCreatorError("direct_vm_json_read_failed") from exc


def _reject_symlink_components(path: Path) -> None:
    """Reject directory components that could redirect a safe root."""

    current = Path(path.parts[0]) if path.parts else Path()
    for component in path.parts[1:]:
        current /= component
        try:
            observed = current.lstat()
        except FileNotFoundError as exc:
            raise DirectVmCreatorError("direct_vm_root_missing") from exc
        except OSError as exc:
            raise DirectVmCreatorError("direct_vm_root_stat_failed") from exc
        if stat.S_ISLNK(observed.st_mode):
            raise DirectVmCreatorError("direct_vm_path_symlink_forbidden")
        if not stat.S_ISDIR(observed.st_mode):
            raise DirectVmCreatorError("direct_vm_path_component_invalid")


def _root_ready(root: Path) -> None:
    _reject_symlink_components(root)
    try:
        root_stat = root.lstat()
    except FileNotFoundError as exc:
        raise DirectVmCreatorError("direct_vm_root_missing") from exc
    except OSError as exc:
        raise DirectVmCreatorError("direct_vm_root_stat_failed") from exc
    if not stat.S_ISDIR(root_stat.st_mode):
        raise DirectVmCreatorError("direct_vm_root_not_directory")
    # A missing tasks/dispatch directory is an initialization/read race, not
    # proof that a requested task is absent.
    for name in ("tasks", "dispatch"):
        child = root / name
        try:
            child_stat = child.lstat()
        except FileNotFoundError as exc:
            raise DirectVmCreatorError("direct_vm_root_layout_unknown") from exc
        except OSError as exc:
            raise DirectVmCreatorError("direct_vm_root_layout_stat_failed") from exc
        if not stat.S_ISDIR(child_stat.st_mode):
            raise DirectVmCreatorError("direct_vm_root_layout_invalid")
        if child_stat.st_nlink < 1:
            raise DirectVmCreatorError("direct_vm_root_layout_invalid")
    if root_stat.st_nlink < 1:
        raise DirectVmCreatorError("direct_vm_root_invalid")
    for bucket in _DISPATCH_BUCKETS:
        child = root / "dispatch" / bucket
        try:
            observed = child.lstat()
        except FileNotFoundError as exc:
            raise DirectVmCreatorError("direct_vm_root_layout_unknown") from exc
        except OSError as exc:
            raise DirectVmCreatorError("direct_vm_root_layout_stat_failed") from exc
        if not stat.S_ISDIR(observed.st_mode) or observed.st_nlink < 1:
            raise DirectVmCreatorError("direct_vm_root_layout_invalid")


def _identity_from_meta(meta: Mapping[str, Any], task_id: str) -> tuple[str, str]:
    containers: list[Mapping[str, Any]] = [meta]
    for key in ("metadata", "meta"):
        nested = meta.get(key)
        if isinstance(nested, Mapping):
            containers.append(nested)
    envelopes = [
        container.get("direct_vm_envelope")
        for container in containers
        if isinstance(container.get("direct_vm_envelope"), Mapping)
    ]
    if not envelopes:
        raise DirectVmCreatorError("direct_vm_identity_envelope_missing")
    for envelope in envelopes:
        _validate_envelope(envelope)
    submission_values = [
        _text(container.get(key))
        for container in containers
        for key in ("direct_vm_submission_key", "submission_key")
        if container.get(key) not in (None, "")
    ]
    submission_values.extend(
        _text(envelope.get("submission_key"))
        for envelope in envelopes
        if envelope.get("submission_key") not in (None, "")
    )
    identity_values = [
        _text(container.get(key))
        for container in containers
        for key in ("direct_vm_identity_sha256", "identity_sha256")
        if container.get(key) not in (None, "")
    ]
    identity_values.extend(
        _text(envelope.get("identity_sha256"))
        for envelope in envelopes
        if envelope.get("identity_sha256") not in (None, "")
    )
    if len(set(submission_values)) > 1 or len(set(identity_values)) > 1:
        raise DirectVmCreatorError("direct_vm_identity_sources_disagree")
    submission = submission_values[0] if submission_values else ""
    identity = identity_values[0] if identity_values else ""
    if not submission and not identity:
        return "", ""
    if _TASK_ID_RE.fullmatch(submission) is None or not _SHA256_RE.fullmatch(identity):
        raise DirectVmCreatorError("direct_vm_identity_corrupt")
    if submission != task_id:
        raise DirectVmCreatorError("direct_vm_identity_task_mismatch")
    return submission, identity


def _state_from_task(root: Path, task_id: str, meta: Mapping[str, Any]) -> str:
    candidates: list[str] = []
    task_dir = _safe_child(root, "tasks", task_id)
    status_path = task_dir / "status.md"
    try:
        _lstat_regular(status_path, max_bytes=128 * 1024)
        text = status_path.read_text(encoding="utf-8")
    except DirectVmCreatorError as exc:
        if exc.code != "direct_vm_file_missing":
            raise
        text = ""
    for line in text.splitlines():
        key, separator, value = line.partition(":")
        normalized_key = key.strip().lstrip("-").strip().lower()
        if separator and normalized_key in {"state", "status"}:
            candidates.append(value.strip().lower().replace(" ", "_"))
    for container in (meta, meta.get("metadata"), meta.get("meta")):
        if isinstance(container, Mapping):
            raw_meta_state = _text(container.get("state"))
            if raw_meta_state:
                candidates.append(raw_meta_state.lower().replace(" ", "_"))
    dispatch_root = root / "dispatch"
    for bucket in _DISPATCH_BUCKETS:
        candidate = dispatch_root / bucket / f"{task_id}.json"
        try:
            dispatch = _read_json(candidate, max_bytes=256 * 1024)
        except DirectVmCreatorError as exc:
            if exc.code == "direct_vm_file_missing":
                continue
            raise
        if isinstance(dispatch, Mapping):
            candidates.insert(0, _text(dispatch.get("state")) or bucket)
            break
    for state in candidates:
        if state in _TERMINAL_SUCCESS:
            return "completed"
        if state in _TERMINAL_FAILURE:
            return "failed"
        if state in _ACTIVE:
            return "existing"
    return "unknown"


def read_direct_vm_status(
    root_value: str,
    task_id_value: str,
    _validator_module_path: str = "",
) -> dict[str, str]:
    """Return a four-field status, distinguishing absence from read failure."""

    task_id = _safe_task_id(task_id_value)
    root = _safe_root(root_value)
    try:
        _root_ready(root)
    except DirectVmCreatorError:
        # Root/layout/read errors are deliberately never reported as missing.
        return {
            "state": "unknown",
            "task_id": task_id,
            "submission_key": "",
            "identity_sha256": "",
        }
    task_dir = _safe_child(root, "tasks", task_id)
    dispatch_candidates = [
        _safe_child(root, "dispatch", bucket, f"{task_id}.json")
        for bucket in _DISPATCH_BUCKETS
    ]
    task_exists = False
    try:
        task_stat = task_dir.lstat()
        if (
            stat.S_ISLNK(task_stat.st_mode)
            or not stat.S_ISDIR(task_stat.st_mode)
            or task_stat.st_nlink < 1
        ):
            return {
                "state": "unknown",
                "task_id": task_id,
                "submission_key": "",
                "identity_sha256": "",
            }
        task_exists = True
    except FileNotFoundError:
        task_exists = False
    except OSError:
        task_exists = True

    if not task_exists:
        for candidate in dispatch_candidates:
            try:
                observed = candidate.lstat()
            except FileNotFoundError:
                continue
            except OSError:
                return {
                    "state": "unknown",
                    "task_id": task_id,
                    "submission_key": "",
                    "identity_sha256": "",
                }
            if (
                stat.S_ISLNK(observed.st_mode)
                or not stat.S_ISREG(observed.st_mode)
                or observed.st_nlink != 1
            ):
                return {
                    "state": "unknown",
                    "task_id": task_id,
                    "submission_key": "",
                    "identity_sha256": "",
                }
            task_exists = True
            break
    if not task_exists:
        # Both canonical task and dispatch namespaces were read successfully;
        # this is the only point at which absence is proven.
        return {
            "state": "missing",
            "task_id": task_id,
            "submission_key": "",
            "identity_sha256": "",
        }

    meta_path = task_dir / "meta.json"
    try:
        meta = _read_json(meta_path)
    except DirectVmCreatorError:
        return {
            "state": "unknown",
            "task_id": task_id,
            "submission_key": "",
            "identity_sha256": "",
        }
    if not isinstance(meta, Mapping):
        return {
            "state": "unknown",
            "task_id": task_id,
            "submission_key": "",
            "identity_sha256": "",
        }
    try:
        submission, identity = _identity_from_meta(meta, task_id)
        if not submission or not identity:
            return {
                "state": "unknown",
                "task_id": task_id,
                "submission_key": "",
                "identity_sha256": "",
            }
        state = _state_from_task(root, task_id, meta)
    except DirectVmCreatorError:
        return {
            "state": "unknown",
            "task_id": task_id,
            "submission_key": "",
            "identity_sha256": "",
        }
    if state not in {"existing", "completed", "failed", "unknown"}:
        state = "unknown"
    return {
        "state": state,
        "task_id": task_id,
        "submission_key": submission,
        "identity_sha256": identity,
    }


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
        raise DirectVmCreatorError("direct_vm_envelope_json_invalid") from exc


def _validate_json_shape(value: Any) -> None:
    """Bound standalone helper input before recursive contract inspection."""

    nodes = 0
    stack: list[tuple[Any, int]] = [(value, 1)]
    while stack:
        item, depth = stack.pop()
        nodes += 1
        if nodes > MAX_JSON_NODES or depth > MAX_JSON_DEPTH:
            raise DirectVmCreatorError("direct_vm_json_shape_exceeded")
        if isinstance(item, Mapping):
            stack.extend((child, depth + 1) for child in item.values())
        elif isinstance(item, list):
            stack.extend((child, depth + 1) for child in item)


def _normalized_key(value: str) -> str:
    camel = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", value)
    return re.sub(r"[^a-z0-9]+", "_", camel.lower()).strip("_")


def _scan_contract(value: Any) -> None:
    stack = [value]
    while stack:
        item = stack.pop()
        if isinstance(item, Mapping):
            for key, child in item.items():
                if not isinstance(key, str):
                    raise DirectVmCreatorError("direct_vm_envelope_key_invalid")
                normalized = _normalized_key(key)
                if normalized in _FORBIDDEN_KEYS or normalized.startswith(
                    _FORBIDDEN_KEY_PREFIXES
                ):
                    raise DirectVmCreatorError("direct_vm_forbidden_field")
                if normalized == "allow_download" and child is not False:
                    raise DirectVmCreatorError("direct_vm_download_not_disabled")
                if normalized == "input_materialization" and child != "forbidden":
                    raise DirectVmCreatorError(
                        "direct_vm_input_materialization_not_forbidden"
                    )
                if normalized == "data_access_mode" and child != "remote_read":
                    raise DirectVmCreatorError("direct_vm_data_access_mode_invalid")
                if normalized in _FORBIDDEN_DOWNLOAD_KEYS:
                    raise DirectVmCreatorError("direct_vm_download_field_forbidden")
                stack.append(child)
        elif isinstance(item, list):
            stack.extend(item)
        elif isinstance(item, str) and _normalized_key(item) == "rca_prod":
            raise DirectVmCreatorError("direct_vm_forbidden_value")


def _validate_envelope(envelope: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(envelope, Mapping):
        raise DirectVmCreatorError("direct_vm_envelope_invalid")
    encoded = _canonical_json(dict(envelope)).encode("utf-8")
    if len(encoded) > MAX_ENVELOPE_BYTES:
        raise DirectVmCreatorError("direct_vm_envelope_too_large")
    payload = json.loads(encoded)
    _validate_json_shape(payload)
    _scan_contract(payload)
    required = {
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
    }
    if set(payload) - required - {
        "artifact_root",
        "artifact_cifs_root",
    } or not required <= set(payload):
        raise DirectVmCreatorError("direct_vm_envelope_fields_invalid")
    if payload.get("schema_version") != DIRECT_VM_SUBMIT_SCHEMA_VERSION:
        raise DirectVmCreatorError("direct_vm_envelope_schema_invalid")
    if (
        payload.get("create_once") is not True
        or payload.get("allow_download") is not False
    ):
        raise DirectVmCreatorError("direct_vm_envelope_create_once_invalid")
    task_id = _safe_task_id(payload.get("task_id"))
    if payload.get("submission_key") != task_id:
        raise DirectVmCreatorError("direct_vm_envelope_identity_invalid")
    if not _SHA256_RE.fullmatch(
        str(payload.get("identity_sha256") or "")
    ) or not _SHA256_RE.fullmatch(str(payload.get("contract_sha256") or "")):
        raise DirectVmCreatorError("direct_vm_envelope_hash_invalid")
    material = dict(payload)
    identity = material.pop("identity_sha256")
    expected_identity = hashlib.sha256(
        _canonical_json(material).encode("utf-8")
    ).hexdigest()
    if identity != expected_identity:
        raise DirectVmCreatorError("direct_vm_envelope_identity_hash_mismatch")
    expected_contract = hashlib.sha256(
        _canonical_json(payload["execution_request"]).encode("utf-8")
    ).hexdigest()
    if payload["contract_sha256"] != expected_contract:
        raise DirectVmCreatorError("direct_vm_envelope_contract_hash_mismatch")
    auth = payload.get("auth")
    if (
        not isinstance(auth, Mapping)
        or set(auth) != {"principal", "capability"}
        or not _text(auth.get("principal"))
        or not _text(auth.get("capability"))
    ):
        raise DirectVmCreatorError("direct_vm_envelope_auth_invalid")
    if (
        auth.get("principal") != DIRECT_VM_AUTH_PRINCIPAL
        or auth.get("capability") != DIRECT_VM_AUTH_CAPABILITY
    ):
        raise DirectVmCreatorError("direct_vm_envelope_auth_mismatch")
    if not isinstance(payload.get("source_refs"), Mapping) or not isinstance(
        payload.get("execution_request"), Mapping
    ):
        raise DirectVmCreatorError("direct_vm_envelope_contract_invalid")
    return payload


def _load_module(
    path_value: str,
    name: str,
    *,
    expected_sha256: str = "",
    expected_mode: int | None = None,
) -> Any:
    path = Path(path_value)
    if not path.is_absolute() or ".." in PurePosixPath(path).parts:
        raise DirectVmCreatorError("direct_vm_module_path_invalid")
    try:
        _reject_symlink_components(path.parent)
    except DirectVmCreatorError as exc:
        raise DirectVmCreatorError("direct_vm_module_path_invalid") from exc
    try:
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise DirectVmCreatorError("direct_vm_module_missing") from exc
    try:
        observed = os.fstat(descriptor)
        if (
            not stat.S_ISREG(observed.st_mode)
            or observed.st_nlink != 1
            or int(observed.st_uid) != os.geteuid()
            or stat.S_IMODE(observed.st_mode) & 0o022
            or (
                expected_mode is not None
                and stat.S_IMODE(observed.st_mode) != expected_mode
            )
            or observed.st_size > MAX_METADATA_BYTES * 16
        ):
            raise DirectVmCreatorError("direct_vm_module_permissions_invalid")
        chunks: list[bytes] = []
        remaining = observed.st_size
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                raise DirectVmCreatorError("direct_vm_module_unstable")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise DirectVmCreatorError("direct_vm_module_unstable")
        after = os.fstat(descriptor)
        lexical = path.lstat()

        # Reading a resident module may update atime.  Compare only the
        # content/identity fields that can indicate replacement or mutation,
        # rather than the complete stat_result (which includes atime).
        def _fingerprint(info: os.stat_result) -> tuple[int, ...]:
            return (
                info.st_dev,
                info.st_ino,
                info.st_mode,
                info.st_nlink,
                info.st_uid,
                info.st_gid,
                info.st_size,
                info.st_mtime_ns,
                info.st_ctime_ns,
            )

        if (
            stat.S_ISLNK(lexical.st_mode)
            or _fingerprint(observed) != _fingerprint(after)
            or (
                lexical.st_dev,
                lexical.st_ino,
                lexical.st_size,
                lexical.st_mtime_ns,
                lexical.st_ctime_ns,
            )
            != (
                observed.st_dev,
                observed.st_ino,
                observed.st_size,
                observed.st_mtime_ns,
                observed.st_ctime_ns,
            )
        ):
            raise DirectVmCreatorError("direct_vm_module_unstable")
        raw = b"".join(chunks)
        if expected_sha256 and hashlib.sha256(raw).hexdigest() != expected_sha256:
            raise DirectVmCreatorError("direct_vm_module_hash_mismatch")
    except OSError as exc:
        raise DirectVmCreatorError("direct_vm_module_unstable") from exc
    finally:
        os.close(descriptor)
    spec = importlib.util.spec_from_file_location(name, str(path))
    if spec is None or spec.loader is None:
        raise DirectVmCreatorError("direct_vm_module_unloadable")
    module = importlib.util.module_from_spec(spec)
    # ``dataclasses`` and a few runtime helpers resolve their defining module
    # through ``sys.modules`` while executing.  Keep the pinned module visible
    # for both import-time and subsequent type/ABI checks.
    previous = sys.modules.get(name)
    sys.modules[name] = module
    module_dirs = [path.parent]
    if path.parent.name == "gateway":
        module_dirs.append(path.parent.parent)
    for module_dir_path in module_dirs:
        module_dir = str(module_dir_path)
        if module_dir not in sys.path:
            sys.path.insert(0, module_dir)
    try:
        module.__file__ = str(path)
        exec(compile(raw, str(path), "exec"), module.__dict__)
    except Exception as exc:
        if previous is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = previous
        raise DirectVmCreatorError("direct_vm_module_import_failed") from exc
    return module


def _task_title_and_goal(envelope: Mapping[str, Any]) -> tuple[str, str]:
    execution = envelope.get("execution_request")
    execution = execution if isinstance(execution, Mapping) else {}
    work_item = execution.get("work_item")
    work_item = work_item if isinstance(work_item, Mapping) else {}
    project = _text(work_item.get("project_key")) or "rca"
    item_id = _text(work_item.get("work_item_id")) or envelope["task_id"]
    title = f"RCA direct VM task: {project}/{item_id}"[:256]
    goal = _canonical_json(execution)
    return title, goal


def _status_payload(
    state: str,
    task_id: str,
    submission_key: str = "",
    identity_sha256: str = "",
) -> dict[str, str]:
    return {
        "state": state,
        "task_id": task_id,
        "submission_key": submission_key,
        "identity_sha256": identity_sha256,
    }


def _create_result(
    *,
    accepted: bool,
    created: bool,
    task_id: str,
    identity_sha256: str,
    **flags: bool,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "protocol_version": DIRECT_VM_TRANSPORT_PROTOCOL_VERSION,
        "accepted": accepted,
        "created": created,
        "task_id": task_id,
        "submission_key": task_id,
        "identity_sha256": identity_sha256,
    }
    result.update(flags)
    return result


def _reconcile_existing(
    root: Path,
    task_id: str,
    identity_sha256: str,
) -> dict[str, Any] | None:
    observed = read_direct_vm_status(str(root), task_id)
    if observed["state"] == "unknown":
        raise DirectVmCreatorError("direct_vm_status_unknown")
    if observed["state"] == "missing":
        return None
    if observed["state"] not in {"existing", "completed", "failed"}:
        return None
    if (
        observed.get("submission_key") == task_id
        and observed.get("identity_sha256") == identity_sha256
    ):
        return _create_result(
            accepted=True,
            created=False,
            task_id=task_id,
            identity_sha256=identity_sha256,
            deduplicated=True,
        )
    return _create_result(
        accepted=False,
        created=False,
        task_id=task_id,
        identity_sha256=identity_sha256,
        conflict=True,
    )


def _ensure_creator_root(creator: Any, root: Path) -> None:
    ensure_canonical_root = getattr(creator, "ensure_canonical_root", None)
    if callable(ensure_canonical_root):
        try:
            resolved = Path(ensure_canonical_root(str(root)))
        except Exception as exc:
            raise DirectVmCreatorError(
                "direct_vm_shared_state_root_create_failed"
            ) from exc
        if resolved.absolute() != root.absolute():
            raise DirectVmCreatorError("direct_vm_shared_state_root_identity_mismatch")
    else:
        build_paths = getattr(creator, "build_paths", None)
        ensure_layout = getattr(creator, "ensure_layout", None)
        if not callable(build_paths) or not callable(ensure_layout):
            raise DirectVmCreatorError("direct_vm_shared_state_creator_abi_invalid")
        try:
            paths = build_paths(str(root))
            ensure_layout(paths)
        except Exception as exc:
            raise DirectVmCreatorError(
                "direct_vm_shared_state_root_create_failed"
            ) from exc
    _root_ready(root)


def _attach_direct_metadata(
    root: Path,
    task_id: str,
    payload: Mapping[str, Any],
) -> None:
    task_dir = _safe_child(root, "tasks", task_id)
    try:
        task_stat = task_dir.lstat()
    except OSError as exc:
        raise DirectVmCreatorError("direct_vm_task_directory_missing") from exc
    if not stat.S_ISDIR(task_stat.st_mode) or stat.S_ISLNK(task_stat.st_mode):
        raise DirectVmCreatorError("direct_vm_task_directory_invalid")
    meta_path = _safe_child(task_dir, "meta.json")
    meta = _read_json(meta_path)
    if not isinstance(meta, Mapping):
        raise DirectVmCreatorError("direct_vm_task_metadata_invalid")
    direct_meta: dict[str, Any] = {
        "schema_version": DIRECT_VM_CREATOR_SCHEMA_VERSION,
        "direct_vm_envelope": dict(payload),
        "direct_vm_submission_key": payload["submission_key"],
        "direct_vm_identity_sha256": payload["identity_sha256"],
        "direct_vm_contract_sha256": payload["contract_sha256"],
        "direct_vm_create_once": True,
        "direct_vm_allow_download": False,
    }
    for key in ("artifact_root", "artifact_cifs_root"):
        if payload.get(key):
            direct_meta[key] = payload[key]
    merged = dict(meta)
    metadata = merged.get("metadata")
    metadata = dict(metadata) if isinstance(metadata, Mapping) else {}
    metadata.update(direct_meta)
    merged["metadata"] = metadata
    _atomic_write_json(meta_path, merged)

    dispatch_path = _safe_child(root, "dispatch", "pending", f"{task_id}.json")
    try:
        dispatch = _read_json(dispatch_path, max_bytes=MAX_METADATA_BYTES)
    except DirectVmCreatorError as exc:
        if exc.code == "direct_vm_file_missing":
            return
        raise
    if not isinstance(dispatch, Mapping):
        raise DirectVmCreatorError("direct_vm_dispatch_metadata_invalid")
    dispatch_merged = dict(dispatch)
    dispatch_meta = dispatch_merged.get("meta")
    dispatch_meta = dict(dispatch_meta) if isinstance(dispatch_meta, Mapping) else {}
    dispatch_meta.update(direct_meta)
    dispatch_merged["meta"] = dispatch_meta
    _atomic_write_json(dispatch_path, dispatch_merged)


def create_direct_vm_task(
    root_value: str,
    envelope: Mapping[str, Any],
    *,
    shared_state_module_path: str,
    validator_module_path: str = "",
    shared_state_sha256: str = "",
    validator_sha256: str = "",
    humanizer_module_path: str = "",
    humanizer_sha256: str = "",
    humanizer_mode: int = 0o600,
    # Backwards-compatible keyword for older Host callers.  It is treated as
    # the validator path; no legacy gateway submit module is loaded remotely.
    submit_module_path: str = "",
) -> dict[str, Any]:
    """Create one pending shared-state task through the canonical VM ABI."""

    payload = _validate_envelope(envelope)
    if len(_canonical_json(payload).encode("utf-8")) > MAX_ENVELOPE_BYTES:
        raise DirectVmCreatorError("direct_vm_envelope_too_large")
    root = _safe_root(root_value)
    _validate_create_root(root)
    validator_path = validator_module_path or submit_module_path
    if not validator_path:
        raise DirectVmCreatorError("direct_vm_submit_contract_unavailable")
    # Validate the self-contained contract before loading any shared-state
    # dependency so a missing/invalid validator has deterministic precedence.
    try:
        validator_module = _load_module(
            validator_path,
            "pnc_rca_direct_vm_validator_remote",
            expected_sha256=validator_sha256,
        )
    except DirectVmCreatorError as exc:
        raise DirectVmCreatorError("direct_vm_submit_contract_unavailable") from exc
    if getattr(validator_module, "DIRECT_VM_VALIDATOR_SCHEMA_VERSION", "") != (
        DIRECT_VM_VALIDATOR_SCHEMA_VERSION
    ):
        raise DirectVmCreatorError("direct_vm_submit_contract_unavailable")
    validate_request = getattr(validator_module, "validate_direct_vm_request", None)
    if not callable(validate_request):
        raise DirectVmCreatorError("direct_vm_submit_contract_unavailable")
    try:
        validated = validate_request(payload)
        if not isinstance(validated, Mapping):
            raise TypeError("validator must return a mapping")
        payload = dict(validated)
    except Exception as exc:
        raise DirectVmCreatorError("direct_vm_submit_contract_invalid") from exc

    if not humanizer_module_path or not humanizer_sha256:
        raise DirectVmCreatorError("direct_vm_humanizer_unavailable")
    # shared_state_v2 imports this module at top level.  Load the exact,
    # content-addressed dependency under its import name before loading
    # shared_state_v2 so Python cannot resolve an unpinned sibling or PATH
    # module.  The loader also enforces owner, mode, regular-file, and stable
    # descriptor identity checks.
    try:
        humanizer_module = _load_module(
            humanizer_module_path,
            DIRECT_VM_HUMANIZER_MODULE_NAME,
            expected_sha256=humanizer_sha256,
            expected_mode=humanizer_mode,
        )
    except DirectVmCreatorError as exc:
        raise DirectVmCreatorError("direct_vm_humanizer_unavailable") from exc
    if not callable(getattr(humanizer_module, "build_task_state_notification", None)):
        raise DirectVmCreatorError("direct_vm_humanizer_abi_invalid")
    try:
        creator = _load_module(
            shared_state_module_path,
            "pnc_rca_shared_state_v2_direct",
            expected_sha256=shared_state_sha256,
        )
    except DirectVmCreatorError as exc:
        raise DirectVmCreatorError(
            "direct_vm_shared_state_creator_unavailable"
        ) from exc

    if not callable(getattr(creator, "create_task", None)):
        raise DirectVmCreatorError("direct_vm_shared_state_creator_abi_invalid")
    task_id = payload["task_id"]
    title, goal = _task_title_and_goal(payload)
    lock_path: Path | None = None
    lock_fd: int | None = None
    try:
        _ensure_creator_root(creator, root)
        lock_path, lock_fd = _acquire_create_lock(root, task_id)
        existing = _reconcile_existing(root, task_id, payload["identity_sha256"])
        if existing is not None:
            return existing
        try:
            result = creator.create_task(
                root=str(root),
                title=title,
                goal_text=goal,
                task_id=task_id,
                owner=payload["auth"]["principal"],
                requester_session_key=f"direct-vm:{task_id}",
                coding_session_key="direct-vm",
            )
        except FileExistsError:
            existing = _reconcile_existing(root, task_id, payload["identity_sha256"])
            if existing is not None:
                return existing
            raise DirectVmCreatorError("direct_vm_create_race_unknown")
        except TypeError as exc:
            raise DirectVmCreatorError(
                "direct_vm_shared_state_creator_abi_invalid"
            ) from exc
        if not isinstance(result, Mapping) or result.get("task_id") != task_id:
            raise DirectVmCreatorError("direct_vm_create_response_invalid")
        _attach_direct_metadata(root, task_id, payload)
        return {
            **_create_result(
                accepted=True,
                created=True,
                task_id=task_id,
                identity_sha256=payload["identity_sha256"],
            ),
            "state": "pending",
        }
    except DirectVmCreatorError:
        raise
    except Exception as exc:
        raise DirectVmCreatorError("direct_vm_shared_state_create_failed") from exc
    finally:
        if lock_path is not None and lock_fd is not None:
            _release_create_lock(lock_path, lock_fd)


__all__ = [
    "DIRECT_VM_CREATOR_SCHEMA_VERSION",
    "DIRECT_VM_VALIDATOR_SCHEMA_VERSION",
    "DIRECT_VM_HUMANIZER_MODULE_NAME",
    "DirectVmCreatorError",
    "create_direct_vm_task",
    "read_direct_vm_status",
]
