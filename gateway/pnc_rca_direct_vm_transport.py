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
import json
import os
from pathlib import Path, PurePosixPath
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
DEFAULT_VM_SHARED_STATE_ROOT = "/home/mini/.hermes/shared-state"
DEFAULT_REMOTE_CREATOR_PATH = (
    "/home/mini/.hermes/worker-state/pnc_rca_direct_vm_creator.py"
)
DEFAULT_REMOTE_SHARED_STATE_MODULE_PATH = (
    "/home/mini/.hermes/worker-state/shared_state_v2.py"
)
DEFAULT_REMOTE_SUBMIT_MODULE_PATH = (
    "/home/mini/.hermes/worker-state/pnc_rca_direct_vm_submit.py"
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
    remote_submit_module_path: str = DEFAULT_REMOTE_SUBMIT_MODULE_PATH
    create_enabled: bool = False
    timeout_seconds: float = DEFAULT_COMMAND_TIMEOUT_SECONDS
    max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES

    def normalized(self) -> "DirectVmTransportConfig":
        agent = _text(self.ssh_mini_agent) or str(
            Path.home() / ".local" / "bin" / "ssh-mini-agent"
        )
        agent = _safe_remote_path(agent, "ssh_mini_agent")
        root = _safe_shared_state_root(self.shared_state_root)
        creator = _safe_worker_module_path(
            self.remote_creator_path, "remote_creator_path"
        )
        shared = _safe_worker_module_path(
            self.remote_shared_state_module_path,
            "remote_shared_state_module_path",
        )
        submit = _safe_worker_module_path(
            self.remote_submit_module_path,
            "remote_submit_module_path",
        )
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
            remote_submit_module_path=submit,
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
            "submit_module_path": "remote_submit_module_path",
            "enabled": "create_enabled",
        }
        fields = {
            "ssh_mini_agent",
            "shared_state_root",
            "remote_creator_path",
            "remote_shared_state_module_path",
            "remote_submit_module_path",
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
            "remote_submit_module_path": normalized.remote_submit_module_path,
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
    submit_module_path: str,
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
        "import importlib.util, json, os, stat\n"
        f"HELPER_PATH = {helper_path!r}\n"
        f"ROOT = {shared_state_root!r}\n"
        f"SHARED_STATE_MODULE = {shared_state_module_path!r}\n"
        f"SUBMIT_MODULE = {submit_module_path!r}\n"
        f"OPERATION = {operation!r}\n"
        f"TASK_ID = {task_id!r}\n"
        f"ENVELOPE = json.loads({request_json!r})\n"
        "helper_stat = os.lstat(HELPER_PATH)\n"
        "if stat.S_ISLNK(helper_stat.st_mode) or not stat.S_ISREG(helper_stat.st_mode) or helper_stat.st_nlink != 1:\n"
        "    raise RuntimeError('direct_vm_creator_helper_not_regular')\n"
        "spec = importlib.util.spec_from_file_location('pnc_rca_direct_vm_creator_remote', HELPER_PATH)\n"
        "if spec is None or spec.loader is None:\n"
        "    raise RuntimeError('direct_vm_creator_helper_unloadable')\n"
        "module = importlib.util.module_from_spec(spec)\n"
        "spec.loader.exec_module(module)\n"
        "if getattr(module, 'DIRECT_VM_CREATOR_SCHEMA_VERSION', '') != 'g1q3_rca_direct_vm_creator_v1':\n"
        "    raise RuntimeError('direct_vm_creator_protocol_mismatch')\n"
        "if OPERATION == 'status':\n"
        "    result = module.read_direct_vm_status(ROOT, TASK_ID, SUBMIT_MODULE)\n"
        "else:\n"
        "    result = module.create_direct_vm_task(ROOT, ENVELOPE, shared_state_module_path=SHARED_STATE_MODULE, submit_module_path=SUBMIT_MODULE)\n"
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
            submit_module_path=self.config.remote_submit_module_path,
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
) -> DirectVmTransport:
    """Build a concrete transport; invalid config fails closed before I/O."""

    return DirectVmTransport(config, command_runner=command_runner)


__all__ = [
    "DEFAULT_REMOTE_CREATOR_PATH",
    "DEFAULT_REMOTE_SHARED_STATE_MODULE_PATH",
    "DEFAULT_REMOTE_SUBMIT_MODULE_PATH",
    "DEFAULT_VM_SHARED_STATE_ROOT",
    "DIRECT_VM_TRANSPORT_PROTOCOL_VERSION",
    "DirectVmTransport",
    "DirectVmTransportConfig",
    "DirectVmTransportError",
    "build_direct_vm_transport",
]
