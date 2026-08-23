"""Fail-closed Host/VM transport for the direct RCA task envelope.

The direct submit facade owns the status-first state machine.  This module is
the concrete boundary used by that facade: it talks to one pinned
``ssh-mini-agent`` wrapper and a pinned VM-side helper.  It carries only the
direct envelope and never imports release or admission machinery.

The default is read-only status observation.  Creation must be explicitly
enabled by configuration and is still suppressed when the remote helper,
shared-state root, or protocol response cannot be proven safe.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
import hashlib
import json
import os
from pathlib import PurePosixPath
import re
import subprocess
from typing import Any

from gateway.pnc_rca_direct_vm_submit import (
    DirectVmSubmitError,
    DirectVmSubmitOutcome,
    DirectVmSubmitRequest,
    status_first_submit,
    validate_direct_vm_request,
)


DIRECT_VM_TRANSPORT_PROTOCOL_VERSION = "g1q3_rca_direct_vm_transport_v1"
REVIEWED_SSH_MINI_AGENT = "/Users/songying/.local/bin/ssh-mini-agent"
DEFAULT_VM_SHARED_STATE_ROOT = "/home/mini/.hermes/shared-state"
DEFAULT_REMOTE_CREATOR_PATH = (
    "/home/mini/.hermes/worker-state/pnc_rca_direct_vm_creator.py"
)
DEFAULT_REMOTE_SHARED_STATE_MODULE_PATH = (
    "/home/mini/.hermes/worker-state/shared_state_v2.py"
)
DEFAULT_REMOTE_VALIDATOR_MODULE_PATH = (
    "/home/mini/.hermes/worker-state/pnc_rca_direct_vm_validator.py"
)
DEFAULT_REMOTE_HUMANIZER_MODULE_PATH = (
    "/home/mini/.hermes/worker-state/vm_feishu_humanizer.py"
)
# Compatibility alias for pre-validator configuration names.  The path now
# points at the self-contained validator; no gateway submit module is loaded on
# the VM.
DEFAULT_REMOTE_SUBMIT_MODULE_PATH = DEFAULT_REMOTE_VALIDATOR_MODULE_PATH
# These hashes are the reviewed Host creator/validator bytes and the current
# live VM shared-state ABI.  A mismatch is an unknown/retryable transport
# result, never a proven absence.
DEFAULT_REMOTE_CREATOR_SHA256 = (
    "baf9bcaf86d0fcb50cde6856f83bc2eb4548c27e56e4e90663aef45b61aec318"
)
DEFAULT_REMOTE_VALIDATOR_SHA256 = (
    "4175ffb3405210e0504f9882e1f70013f5ce20791240728583d5c92070a935ef"
)
DEFAULT_REMOTE_HUMANIZER_SHA256 = (
    "3f6551c1e0e36e8cee21b50983338474724963da73aa6dc0304490e65c962bee"
)
DEFAULT_REMOTE_HUMANIZER_MODE = 0o600
DEFAULT_REMOTE_HUMANIZER_BASELINE_COMMIT = "fec0a86c169fc71d8dca48a2732dc1cd3b52db99"
DEFAULT_REMOTE_HUMANIZER_BASELINE_TREE = "1c8cb3832275abf18e31d14b56501cf31080f201"
REVIEWED_REMOTE_GIT = "/usr/bin/git"
DEFAULT_REMOTE_SHARED_STATE_SHA256 = (
    "a6a893d3773ef4f54e44f3f0a2224f32e86de9851214a92df59baf3f18d7ec22"
)
DEFAULT_COMMAND_TIMEOUT_SECONDS = 20.0
DEFAULT_MAX_RESPONSE_BYTES = 1024 * 1024

_TASK_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_ABSOLUTE_SAFE_ROOT_PREFIXES = ("/home/mini/", "/mnt/tmp/")
_ALLOWED_ROOTS = frozenset({"/home/mini/.hermes/shared-state"})
_VM_WORKER_STATE_PREFIX = "/home/mini/.hermes/worker-state/"
_SAFE_REMOTE_PATH_RE = re.compile(r"^/[A-Za-z0-9._/@+-]+$")
_STATUS_FIELDS = frozenset({"state", "task_id", "submission_key", "identity_sha256"})
_CREATE_FIELDS = frozenset({
    "protocol_version",
    "accepted",
    "created",
    "deduplicated",
    "conflict",
    "task_id",
    "submission_key",
    "identity_sha256",
    "state",
})
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class DirectVmTransportError(RuntimeError):
    """A transport boundary failure; callers must treat it as retryable."""

    def __init__(self, code: str, detail: str = "") -> None:
        self.code = str(code)
        self.detail = str(detail)
        super().__init__(f"{self.code}: {self.detail}" if self.detail else self.code)


CommandRunner = Callable[..., subprocess.CompletedProcess[str]]


def _text(value: Any) -> str:
    return str(value or "").strip()


def _safe_remote_path(value: Any, field: str) -> str:
    path = _text(value)
    if (
        not path
        or not path.startswith("/")
        or "//" in path
        or "\\" in path
        or "\x00" in path
        or ".." in PurePosixPath(path).parts
        or not _SAFE_REMOTE_PATH_RE.fullmatch(path)
    ):
        raise ValueError(f"{field}_must_be_absolute_safe_path")
    return path.rstrip("/") or "/"


def _safe_shared_state_root(value: Any) -> str:
    root = _safe_remote_path(value, "shared_state_root")
    if root not in _ALLOWED_ROOTS and not any(
        root.startswith(prefix) and len(PurePosixPath(root).parts) >= 4
        for prefix in _ABSOLUTE_SAFE_ROOT_PREFIXES
    ):
        raise ValueError("shared_state_root_outside_allowed_vm_namespace")
    return root


def _safe_worker_module_path(value: Any, field: str) -> str:
    path = _safe_remote_path(value, field)
    if not path.startswith(_VM_WORKER_STATE_PREFIX) or not path.endswith(".py"):
        raise ValueError(f"{field}_outside_pinned_worker_state")
    relative = path[len(_VM_WORKER_STATE_PREFIX) :]
    if not relative or "/" in relative or relative in {".", ".."}:
        raise ValueError(f"{field}_outside_pinned_worker_state")
    return path


def _safe_task_id(value: Any) -> str:
    task_id = _text(value)
    if _TASK_ID_RE.fullmatch(task_id) is None:
        raise DirectVmTransportError("direct_vm_task_id_invalid")
    return task_id


@dataclass(frozen=True)
class DirectVmTransportConfig:
    """Pinned, non-secret configuration for the Host/VM boundary."""

    ssh_mini_agent: str = ""
    shared_state_root: str = DEFAULT_VM_SHARED_STATE_ROOT
    remote_creator_path: str = DEFAULT_REMOTE_CREATOR_PATH
    remote_shared_state_module_path: str = DEFAULT_REMOTE_SHARED_STATE_MODULE_PATH
    remote_validator_module_path: str = DEFAULT_REMOTE_VALIDATOR_MODULE_PATH
    remote_humanizer_module_path: str = DEFAULT_REMOTE_HUMANIZER_MODULE_PATH
    # Deprecated input alias retained for old callers/config files.
    remote_submit_module_path: str = ""
    remote_creator_sha256: str = DEFAULT_REMOTE_CREATOR_SHA256
    remote_validator_sha256: str = DEFAULT_REMOTE_VALIDATOR_SHA256
    remote_humanizer_sha256: str = DEFAULT_REMOTE_HUMANIZER_SHA256
    remote_humanizer_mode: int = DEFAULT_REMOTE_HUMANIZER_MODE
    remote_humanizer_baseline_commit: str = DEFAULT_REMOTE_HUMANIZER_BASELINE_COMMIT
    remote_humanizer_baseline_tree: str = DEFAULT_REMOTE_HUMANIZER_BASELINE_TREE
    remote_shared_state_sha256: str = DEFAULT_REMOTE_SHARED_STATE_SHA256
    create_enabled: bool = False
    timeout_seconds: float = DEFAULT_COMMAND_TIMEOUT_SECONDS
    max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES

    def normalized(self) -> "DirectVmTransportConfig":
        agent = _text(self.ssh_mini_agent) or REVIEWED_SSH_MINI_AGENT
        agent = _safe_remote_path(agent, "ssh_mini_agent")
        root = _safe_shared_state_root(self.shared_state_root)
        creator = _safe_worker_module_path(
            self.remote_creator_path, "remote_creator_path"
        )
        shared = _safe_worker_module_path(
            self.remote_shared_state_module_path,
            "remote_shared_state_module_path",
        )
        validator_value = self.remote_validator_module_path
        if self.remote_submit_module_path:
            if (
                validator_value != DEFAULT_REMOTE_VALIDATOR_MODULE_PATH
                and validator_value != self.remote_submit_module_path
            ):
                raise ValueError("remote_validator_module_path_alias_conflict")
            validator_value = self.remote_submit_module_path
        validator = _safe_worker_module_path(
            validator_value,
            "remote_validator_module_path",
        )
        humanizer = _safe_worker_module_path(
            self.remote_humanizer_module_path,
            "remote_humanizer_module_path",
        )
        hashes = {
            "remote_creator_sha256": self.remote_creator_sha256,
            "remote_validator_sha256": self.remote_validator_sha256,
            "remote_humanizer_sha256": self.remote_humanizer_sha256,
            "remote_shared_state_sha256": self.remote_shared_state_sha256,
        }
        for field, value in hashes.items():
            if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
                raise ValueError(f"{field}_invalid")
        for field, value in {
            "remote_humanizer_baseline_commit": self.remote_humanizer_baseline_commit,
            "remote_humanizer_baseline_tree": self.remote_humanizer_baseline_tree,
        }.items():
            if (
                not isinstance(value, str)
                or re.fullmatch(r"[0-9a-f]{40}", value) is None
            ):
                raise ValueError(f"{field}_invalid")
        if self.remote_humanizer_mode != DEFAULT_REMOTE_HUMANIZER_MODE:
            raise ValueError("remote_humanizer_mode_must_be_0600")
        if isinstance(self.create_enabled, bool) is False:
            raise ValueError("create_enabled_must_be_boolean")
        try:
            timeout = float(self.timeout_seconds)
        except (TypeError, ValueError) as exc:
            raise ValueError("timeout_seconds_invalid") from exc
        if not 0.1 <= timeout <= 120.0:
            raise ValueError("timeout_seconds_out_of_range")
        if (
            isinstance(self.max_response_bytes, bool)
            or not isinstance(self.max_response_bytes, int)
            or not 1024 <= self.max_response_bytes <= 4 * 1024 * 1024
        ):
            raise ValueError("max_response_bytes_out_of_range")
        return replace(
            self,
            ssh_mini_agent=agent,
            shared_state_root=root,
            remote_creator_path=creator,
            remote_shared_state_module_path=shared,
            remote_validator_module_path=validator,
            remote_humanizer_module_path=humanizer,
            remote_submit_module_path="",
            timeout_seconds=timeout,
        )

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "DirectVmTransportConfig":
        if not isinstance(value, Mapping):
            raise TypeError("direct VM transport config must be a mapping")
        aliases = {
            "vm_shared_state_root": "shared_state_root",
            "creator_path": "remote_creator_path",
            "shared_state_module_path": "remote_shared_state_module_path",
            "submit_module_path": "remote_validator_module_path",
            "remote_submit_module_path": "remote_validator_module_path",
            "humanizer_module_path": "remote_humanizer_module_path",
            "remote_humanizer_path": "remote_humanizer_module_path",
            "enabled": "create_enabled",
        }
        fields = {
            "ssh_mini_agent",
            "shared_state_root",
            "remote_creator_path",
            "remote_shared_state_module_path",
            "remote_validator_module_path",
            "remote_submit_module_path",
            "remote_humanizer_module_path",
            "remote_creator_sha256",
            "remote_validator_sha256",
            "remote_humanizer_sha256",
            "remote_humanizer_mode",
            "remote_humanizer_baseline_commit",
            "remote_humanizer_baseline_tree",
            "remote_shared_state_sha256",
            "create_enabled",
            "timeout_seconds",
            "max_response_bytes",
        }
        values: dict[str, Any] = {}
        for key, item in value.items():
            normalized_key = aliases.get(str(key), str(key))
            if normalized_key in fields:
                values[normalized_key] = item
        return cls(**values).normalized()

    def public_dict(self) -> dict[str, Any]:
        normalized = self.normalized()
        return {
            "protocol_version": DIRECT_VM_TRANSPORT_PROTOCOL_VERSION,
            "ssh_mini_agent": normalized.ssh_mini_agent,
            "shared_state_root": normalized.shared_state_root,
            "remote_creator_path": normalized.remote_creator_path,
            "remote_shared_state_module_path": normalized.remote_shared_state_module_path,
            "remote_validator_module_path": normalized.remote_validator_module_path,
            "remote_submit_module_path": normalized.remote_validator_module_path,
            "remote_humanizer_module_path": normalized.remote_humanizer_module_path,
            "remote_creator_sha256": normalized.remote_creator_sha256,
            "remote_validator_sha256": normalized.remote_validator_sha256,
            "remote_humanizer_sha256": normalized.remote_humanizer_sha256,
            "remote_humanizer_mode": normalized.remote_humanizer_mode,
            "remote_humanizer_baseline_commit": normalized.remote_humanizer_baseline_commit,
            "remote_humanizer_baseline_tree": normalized.remote_humanizer_baseline_tree,
            "remote_shared_state_sha256": normalized.remote_shared_state_sha256,
            "create_enabled": normalized.create_enabled,
            "timeout_seconds": normalized.timeout_seconds,
            "max_response_bytes": normalized.max_response_bytes,
        }


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


def _validate_wire_status(value: Any, task_id: str) -> dict[str, str]:
    if not isinstance(value, Mapping) or frozenset(value) != _STATUS_FIELDS:
        raise DirectVmTransportError("direct_vm_status_response_invalid")
    if value.get("task_id") != task_id:
        raise DirectVmTransportError("direct_vm_status_task_id_mismatch")
    state = value.get("state")
    if state not in {"missing", "unknown", "completed", "failed", "existing"}:
        raise DirectVmTransportError("direct_vm_status_state_invalid")
    submission = value.get("submission_key")
    identity = value.get("identity_sha256")
    if not isinstance(submission, str) or not isinstance(identity, str):
        raise DirectVmTransportError("direct_vm_status_identity_invalid")
    if state == "missing":
        if submission or identity:
            raise DirectVmTransportError("direct_vm_status_missing_identity_nonempty")
    elif state == "unknown":
        if submission or identity:
            if _TASK_ID_RE.fullmatch(submission) is None or not _SHA256_RE.fullmatch(
                identity
            ):
                raise DirectVmTransportError("direct_vm_status_identity_invalid")
    elif _TASK_ID_RE.fullmatch(submission) is None or not _SHA256_RE.fullmatch(
        identity
    ):
        raise DirectVmTransportError("direct_vm_status_identity_invalid")
    return dict(value)


def _remote_script(
    *,
    helper_path: str,
    shared_state_root: str,
    shared_state_module_path: str,
    validator_module_path: str,
    humanizer_module_path: str,
    creator_sha256: str,
    validator_sha256: str,
    humanizer_sha256: str,
    humanizer_mode: int,
    humanizer_baseline_commit: str,
    humanizer_baseline_tree: str,
    shared_state_sha256: str,
    operation: str,
    task_id: str = "",
    envelope: Mapping[str, Any] | None = None,
) -> str:
    request_json = json.dumps(
        dict(envelope) if isinstance(envelope, Mapping) else {},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    # Values are literals in the generated script; no shell interpolation or
    # user-controlled argv is used by ssh-mini-agent.
    return (
        "import hashlib, json, os, stat, subprocess, sys, types\n"
        f"HELPER_PATH = {helper_path!r}\n"
        f"ROOT = {shared_state_root!r}\n"
        f"SHARED_STATE_MODULE = {shared_state_module_path!r}\n"
        f"VALIDATOR_MODULE = {validator_module_path!r}\n"
        f"HUMANIZER_MODULE = {humanizer_module_path!r}\n"
        f"CREATOR_SHA256 = {creator_sha256!r}\n"
        f"VALIDATOR_SHA256 = {validator_sha256!r}\n"
        f"HUMANIZER_SHA256 = {humanizer_sha256!r}\n"
        f"HUMANIZER_MODE = {humanizer_mode!r}\n"
        f"HUMANIZER_BASELINE_COMMIT = {humanizer_baseline_commit!r}\n"
        f"HUMANIZER_BASELINE_TREE = {humanizer_baseline_tree!r}\n"
        f"GIT_PATH = {REVIEWED_REMOTE_GIT!r}\n"
        f"SHARED_STATE_SHA256 = {shared_state_sha256!r}\n"
        f"OPERATION = {operation!r}\n"
        f"TASK_ID = {task_id!r}\n"
        f"ENVELOPE = json.loads({request_json!r})\n"
        "MAX_MODULE_BYTES = 16 * 1024 * 1024\n"
        "def _check_worker_state_baseline(humanizer_raw):\n"
        "    worker_state = os.path.dirname(HELPER_PATH)\n"
        "    relative = os.path.relpath(HUMANIZER_MODULE, worker_state)\n"
        "    if relative != 'vm_feishu_humanizer.py':\n"
        "        raise RuntimeError('direct_vm_humanizer_provenance_mismatch')\n"
        "    try:\n"
        "        commit = subprocess.run([GIT_PATH, '-C', worker_state, 'rev-parse', 'HEAD'], text=True, capture_output=True, check=False, timeout=2.0)\n"
        "        tree = subprocess.run([GIT_PATH, '-C', worker_state, 'rev-parse', 'HEAD^{tree}'], text=True, capture_output=True, check=False, timeout=2.0)\n"
        "        tracked = subprocess.run([GIT_PATH, '-C', worker_state, 'ls-files', '--error-unmatch', relative], text=True, capture_output=True, check=False, timeout=2.0)\n"
        "        blob = subprocess.run([GIT_PATH, '-C', worker_state, 'rev-parse', f'{HUMANIZER_BASELINE_COMMIT}:{relative}'], text=True, capture_output=True, check=False, timeout=2.0)\n"
        "    except (OSError, subprocess.SubprocessError):\n"
        "        raise RuntimeError('direct_vm_humanizer_provenance_mismatch')\n"
        "    blob_material = b'blob ' + str(len(humanizer_raw)).encode('ascii') + b'\\0' + humanizer_raw\n"
        "    expected_blob = hashlib.sha1(blob_material).hexdigest()\n"
        "    if (commit.returncode != 0 or tree.returncode != 0 or tracked.returncode != 0 or blob.returncode != 0 or commit.stdout.strip() != HUMANIZER_BASELINE_COMMIT or tree.stdout.strip() != HUMANIZER_BASELINE_TREE or tracked.stdout.strip() != relative or blob.stdout.strip() != expected_blob):\n"
        "        raise RuntimeError('direct_vm_humanizer_provenance_mismatch')\n"
        "def _check_pinned_parent(path):\n"
        "    current = os.path.sep\n"
        "    parts = [part for part in path.split(os.path.sep) if part]\n"
        "    for index, part in enumerate(parts):\n"
        "        current = os.path.join(current, part)\n"
        "        info = os.lstat(current)\n"
        "        if index < len(parts) - 1:\n"
        "            if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):\n"
        "                raise RuntimeError('direct_vm_module_parent_invalid')\n"
        "            if info.st_uid not in {0, os.geteuid()}:\n"
        "                raise RuntimeError('direct_vm_module_parent_permissions_invalid')\n"
        "            if info.st_uid == 0 and stat.S_IMODE(info.st_mode) & 0o022:\n"
        "                raise RuntimeError('direct_vm_module_parent_permissions_invalid')\n"
        "            if info.st_uid == os.geteuid() and stat.S_IMODE(info.st_mode) & 0o002:\n"
        "                raise RuntimeError('direct_vm_module_parent_permissions_invalid')\n"
        "def _fingerprint(info):\n"
        "    return (info.st_dev, info.st_ino, info.st_mode, info.st_nlink, info.st_uid, info.st_gid, info.st_size, info.st_mtime_ns, info.st_ctime_ns)\n"
        "def _stable_module_bytes(path, expected, required_mode=None):\n"
        "    _check_pinned_parent(path)\n"
        "    flags = os.O_RDONLY | getattr(os, 'O_CLOEXEC', 0) | getattr(os, 'O_NOFOLLOW', 0)\n"
        "    try:\n"
        "        descriptor = os.open(path, flags)\n"
        "    except OSError as exc:\n"
        "        raise RuntimeError('direct_vm_module_missing') from exc\n"
        "    try:\n"
        "        before = os.fstat(descriptor)\n"
        "        if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_uid != os.geteuid() or stat.S_IMODE(before.st_mode) & 0o022 or (required_mode is not None and stat.S_IMODE(before.st_mode) != required_mode) or before.st_size > MAX_MODULE_BYTES):\n"
        "            raise RuntimeError('direct_vm_module_permissions_invalid')\n"
        "        chunks = []\n"
        "        remaining = before.st_size\n"
        "        while remaining:\n"
        "            chunk = os.read(descriptor, min(1024 * 1024, remaining))\n"
        "            if not chunk:\n"
        "                raise RuntimeError('direct_vm_module_unstable')\n"
        "            chunks.append(chunk)\n"
        "            remaining -= len(chunk)\n"
        "        if os.read(descriptor, 1):\n"
        "            raise RuntimeError('direct_vm_module_unstable')\n"
        "        after = os.fstat(descriptor)\n"
        "        lexical = os.lstat(path)\n"
        "        if (stat.S_ISLNK(lexical.st_mode) or _fingerprint(before) != _fingerprint(after) or (lexical.st_dev, lexical.st_ino, lexical.st_size, lexical.st_mtime_ns, lexical.st_ctime_ns) != (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns)):\n"
        "            raise RuntimeError('direct_vm_module_unstable')\n"
        "        raw = b''.join(chunks)\n"
        "        if hashlib.sha256(raw).hexdigest() != expected:\n"
        "            raise RuntimeError('direct_vm_module_hash_mismatch')\n"
        "        return raw\n"
        "    except OSError as exc:\n"
        "        raise RuntimeError('direct_vm_module_unstable') from exc\n"
        "    finally:\n"
        "        os.close(descriptor)\n"
        "def _module_from_bytes(name, path, raw):\n"
        "    module = types.ModuleType(name)\n"
        "    module.__file__ = path\n"
        "    module.__package__ = ''\n"
        "    previous = sys.modules.get(name)\n"
        "    sys.modules[name] = module\n"
        "    try:\n"
        "        exec(compile(raw, path, 'exec'), module.__dict__)\n"
        "    except BaseException as exc:\n"
        "        if previous is None:\n"
        "            sys.modules.pop(name, None)\n"
        "        else:\n"
        "            sys.modules[name] = previous\n"
        "        raise RuntimeError('direct_vm_module_import_failed') from exc\n"
        "    return module\n"
        "creator_raw = _stable_module_bytes(HELPER_PATH, CREATOR_SHA256)\n"
        "validator_raw = _stable_module_bytes(VALIDATOR_MODULE, VALIDATOR_SHA256)\n"
        "shared_state_raw = _stable_module_bytes(SHARED_STATE_MODULE, SHARED_STATE_SHA256)\n"
        "validator = _module_from_bytes('pnc_rca_direct_vm_validator_probe', VALIDATOR_MODULE, validator_raw)\n"
        "if getattr(validator, 'DIRECT_VM_VALIDATOR_SCHEMA_VERSION', '') != 'g1q3_rca_direct_vm_validator_v1':\n"
        "    raise RuntimeError('direct_vm_submit_contract_unavailable')\n"
        "if not callable(getattr(validator, 'validate_direct_vm_request', None)):\n"
        "    raise RuntimeError('direct_vm_submit_contract_unavailable')\n"
        "module = _module_from_bytes('pnc_rca_direct_vm_creator_remote', HELPER_PATH, creator_raw)\n"
        "if getattr(module, 'DIRECT_VM_CREATOR_SCHEMA_VERSION', '') != 'g1q3_rca_direct_vm_creator_v1':\n"
        "    raise RuntimeError('direct_vm_creator_protocol_mismatch')\n"
        "humanizer_raw = _stable_module_bytes(HUMANIZER_MODULE, HUMANIZER_SHA256, HUMANIZER_MODE)\n"
        "if HUMANIZER_BASELINE_COMMIT or HUMANIZER_BASELINE_TREE:\n"
        "    _check_worker_state_baseline(humanizer_raw)\n"
        "if OPERATION == 'status':\n"
        "    result = module.read_direct_vm_status(ROOT, TASK_ID, VALIDATOR_MODULE)\n"
        "else:\n"
        "    humanizer = _module_from_bytes('vm_feishu_humanizer', HUMANIZER_MODULE, humanizer_raw)\n"
        "    if not callable(getattr(humanizer, 'build_task_state_notification', None)):\n"
        "        raise RuntimeError('direct_vm_humanizer_abi_invalid')\n"
        "    result = module.create_direct_vm_task(ROOT, ENVELOPE, shared_state_module_path=SHARED_STATE_MODULE, validator_module_path=VALIDATOR_MODULE, shared_state_sha256=SHARED_STATE_SHA256, validator_sha256=VALIDATOR_SHA256, humanizer_module_path=HUMANIZER_MODULE, humanizer_sha256=HUMANIZER_SHA256, humanizer_mode=HUMANIZER_MODE)\n"
        "print(json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(',', ':')))\n"
    )


class DirectVmTransport:
    """Concrete status/create transport for :func:`status_first_submit`."""

    def __init__(
        self,
        config: DirectVmTransportConfig | Mapping[str, Any] | None = None,
        *,
        command_runner: CommandRunner | None = None,
    ) -> None:
        if config is None:
            normalized = DirectVmTransportConfig().normalized()
        elif isinstance(config, DirectVmTransportConfig):
            normalized = config.normalized()
        else:
            normalized = DirectVmTransportConfig.from_mapping(config)
        self.config = normalized
        self._command_runner = command_runner or subprocess.run
        self.last_error: str = ""
        self.last_error_detail: str = ""

    def _invoke(
        self,
        *,
        operation: str,
        task_id: str = "",
        envelope: Mapping[str, Any] | None = None,
    ) -> Mapping[str, Any]:
        if operation not in {"status", "create"}:
            raise DirectVmTransportError("direct_vm_operation_invalid")
        if operation == "create" and not self.config.create_enabled:
            raise DirectVmTransportError(
                "direct_vm_transport_unavailable", "create_disabled"
            )
        script = _remote_script(
            helper_path=self.config.remote_creator_path,
            shared_state_root=self.config.shared_state_root,
            shared_state_module_path=self.config.remote_shared_state_module_path,
            validator_module_path=self.config.remote_validator_module_path,
            humanizer_module_path=self.config.remote_humanizer_module_path,
            creator_sha256=self.config.remote_creator_sha256,
            validator_sha256=self.config.remote_validator_sha256,
            humanizer_sha256=self.config.remote_humanizer_sha256,
            humanizer_mode=self.config.remote_humanizer_mode,
            humanizer_baseline_commit=self.config.remote_humanizer_baseline_commit,
            humanizer_baseline_tree=self.config.remote_humanizer_baseline_tree,
            shared_state_sha256=self.config.remote_shared_state_sha256,
            operation=operation,
            task_id=task_id,
            envelope=envelope,
        )
        command = [self.config.ssh_mini_agent, "run_py_json"]
        try:
            completed = self._command_runner(
                command,
                input=script,
                text=True,
                capture_output=True,
                timeout=self.config.timeout_seconds,
                check=False,
                env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
            )
        except subprocess.TimeoutExpired as exc:
            raise DirectVmTransportError("direct_vm_transport_timeout") from exc
        except OSError as exc:
            raise DirectVmTransportError("direct_vm_transport_unavailable") from exc
        if completed.returncode != 0:
            detail = _text(getattr(completed, "stderr", ""))[-400:]
            raise DirectVmTransportError(
                "direct_vm_remote_helper_failed",
                detail or f"rc={completed.returncode}",
            )
        raw = getattr(completed, "stdout", "") or ""
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="strict")
        if not isinstance(raw, str):
            raise DirectVmTransportError("direct_vm_transport_response_invalid")
        if len(raw.encode("utf-8")) > self.config.max_response_bytes:
            raise DirectVmTransportError("direct_vm_transport_response_too_large")
        try:
            payload = json.loads(raw)
        except (TypeError, ValueError) as exc:
            raise DirectVmTransportError(
                "direct_vm_transport_response_invalid"
            ) from exc
        if not isinstance(payload, Mapping):
            raise DirectVmTransportError("direct_vm_transport_response_invalid")
        return payload

    def status(self, task_id: str) -> Mapping[str, Any]:
        """Read one exact VM status; all transport failures become ``unknown``."""

        task_id = _safe_task_id(task_id)
        try:
            payload = self._invoke(operation="status", task_id=task_id)
            status = _validate_wire_status(payload, task_id)
            self.last_error = ""
            self.last_error_detail = ""
            return status
        except (DirectVmTransportError, ValueError) as exc:
            self.last_error = getattr(exc, "code", type(exc).__name__)
            self.last_error_detail = _text(getattr(exc, "detail", ""))
            return _status_payload("unknown", task_id)

    def create(self, envelope: Mapping[str, Any]) -> Mapping[str, Any]:
        """Create exactly one validated envelope through the pinned helper."""

        try:
            request = validate_direct_vm_request(envelope)
        except DirectVmSubmitError as exc:
            raise DirectVmTransportError("direct_vm_request_invalid", str(exc)) from exc
        payload = self._invoke(
            operation="create",
            task_id=request.task_id,
            envelope=request.to_dict(),
        )
        if payload.get("protocol_version") not in {
            None,
            DIRECT_VM_TRANSPORT_PROTOCOL_VERSION,
        }:
            raise DirectVmTransportError("direct_vm_create_protocol_mismatch")
        if payload.get("task_id") not in {None, request.task_id}:
            raise DirectVmTransportError("direct_vm_create_task_id_mismatch")
        if payload.get("identity_sha256") not in {None, request.identity_sha256}:
            raise DirectVmTransportError("direct_vm_create_identity_mismatch")
        if frozenset(payload) - _CREATE_FIELDS:
            raise DirectVmTransportError("direct_vm_create_response_invalid")
        for field in ("accepted", "created", "deduplicated", "conflict"):
            if field in payload and not isinstance(payload[field], bool):
                raise DirectVmTransportError("direct_vm_create_response_invalid")
        if "state" in payload and payload["state"] not in {
            "pending",
            "claimed",
            "existing",
            "completed",
            "failed",
        }:
            raise DirectVmTransportError("direct_vm_create_state_invalid")
        self.last_error = ""
        self.last_error_detail = ""
        return dict(payload)

    def submit(
        self,
        request: DirectVmSubmitRequest | Mapping[str, Any],
    ) -> DirectVmSubmitOutcome:
        """Run the facade state machine against this concrete transport."""

        return status_first_submit(request, self.status, self.create)


def build_direct_vm_transport(
    config: DirectVmTransportConfig | Mapping[str, Any] | None = None,
    *,
    command_runner: CommandRunner | None = None,
    test_only: bool = False,
) -> DirectVmTransport:
    """Build a concrete transport; production uses the reviewed agent path.

    ``test_only`` is intentionally explicit so unit tests can inject a fake
    command runner and executable without weakening the resident dispatcher.
    """

    transport = DirectVmTransport(config, command_runner=command_runner)
    if not test_only:
        reviewed_fields = {
            "ssh_mini_agent": REVIEWED_SSH_MINI_AGENT,
            "shared_state_root": DEFAULT_VM_SHARED_STATE_ROOT,
            "remote_creator_path": DEFAULT_REMOTE_CREATOR_PATH,
            "remote_shared_state_module_path": DEFAULT_REMOTE_SHARED_STATE_MODULE_PATH,
            "remote_validator_module_path": DEFAULT_REMOTE_VALIDATOR_MODULE_PATH,
            "remote_humanizer_module_path": DEFAULT_REMOTE_HUMANIZER_MODULE_PATH,
            "remote_creator_sha256": DEFAULT_REMOTE_CREATOR_SHA256,
            "remote_validator_sha256": DEFAULT_REMOTE_VALIDATOR_SHA256,
            "remote_humanizer_sha256": DEFAULT_REMOTE_HUMANIZER_SHA256,
            "remote_humanizer_mode": DEFAULT_REMOTE_HUMANIZER_MODE,
            "remote_humanizer_baseline_commit": DEFAULT_REMOTE_HUMANIZER_BASELINE_COMMIT,
            "remote_humanizer_baseline_tree": DEFAULT_REMOTE_HUMANIZER_BASELINE_TREE,
            "remote_shared_state_sha256": DEFAULT_REMOTE_SHARED_STATE_SHA256,
        }
        for field, expected in reviewed_fields.items():
            if getattr(transport.config, field) != expected:
                raise ValueError(f"{field}_must_use_reviewed_path")
    return transport


__all__ = [
    "DEFAULT_REMOTE_CREATOR_PATH",
    "DEFAULT_REMOTE_SHARED_STATE_MODULE_PATH",
    "DEFAULT_REMOTE_SUBMIT_MODULE_PATH",
    "DEFAULT_REMOTE_VALIDATOR_MODULE_PATH",
    "DEFAULT_REMOTE_HUMANIZER_MODULE_PATH",
    "DEFAULT_REMOTE_CREATOR_SHA256",
    "DEFAULT_REMOTE_VALIDATOR_SHA256",
    "DEFAULT_REMOTE_HUMANIZER_SHA256",
    "DEFAULT_REMOTE_HUMANIZER_MODE",
    "DEFAULT_REMOTE_HUMANIZER_BASELINE_COMMIT",
    "DEFAULT_REMOTE_HUMANIZER_BASELINE_TREE",
    "DEFAULT_REMOTE_SHARED_STATE_SHA256",
    "DEFAULT_VM_SHARED_STATE_ROOT",
    "DIRECT_VM_TRANSPORT_PROTOCOL_VERSION",
    "REVIEWED_SSH_MINI_AGENT",
    "DirectVmTransport",
    "DirectVmTransportConfig",
    "DirectVmTransportError",
    "build_direct_vm_transport",
]
