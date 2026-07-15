#!/usr/bin/env python3
"""Prepare an immutable RCA production cutover plan without touching live state."""

from __future__ import annotations

import argparse
import contextlib
import errno
import fcntl
import hashlib
import json
import os
import platform
import pwd
import re
import stat
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dotenv import dotenv_values

from gateway import pnc_rca_workspace_runtime as workspace_runtime
from scripts import pnc_rca_release_gate as release_gate
from scripts import pnc_rca_runtime_stage as runtime_stage


RELEASE_PREPARE_SCHEMA_VERSION = "pnc_rca_release_prepare_plan_v1"
RELEASE_PREPARE_MANIFEST_SCHEMA_VERSION = "pnc_rca_release_prepare_manifest_v1"
RELEASE_PREPARE_RUN_IDENTITY_SCHEMA_VERSION = "pnc_rca_release_prepare_run_identity_v1"
RELEASE_APPROVAL_REQUEST_SCHEMA_VERSION = "pnc_rca_release_approval_request_v1"
RELEASE_APPROVAL_SCHEMA_VERSION = "pnc_rca_release_approval_v1"
RELEASE_APPROVAL_IDENTITY_SCHEMA_VERSION = "pnc_rca_release_approval_identity_v1"
RELEASE_APPROVAL_BINDING_VALIDATION_SCHEMA_VERSION = (
    "pnc_rca_release_approval_binding_validation_v1"
)
ROLLBACK_CONFIG_SCHEMA_VERSION = "pnc_rca_rollback_config_v1"
T0_BINDING_SCHEMA_VERSION = "pnc_rca_release_t0_binding_v1"

APPROVAL_DECISION = "authorize_rca_production_cutover_plan"
APPROVAL_IDENTITY_METHOD = "kernel_owner_and_machine_binding"
MAX_JSON_BYTES = 256 * 1024
MAX_APPROVAL_VALIDITY_SECONDS = 24 * 60 * 60
MAX_FUTURE_SKEW_SECONDS = 300
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
RELEASE_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{7,127}\Z")
NONCE_PATTERN = re.compile(r"[A-Za-z0-9_-]{16,128}\Z")
SENSITIVE_KEY_PATTERN = re.compile(
    r"(?:password|passwd|secret|token|credential|private[_-]?key|sasl[_-]?password)",
    re.IGNORECASE,
)

PRODUCTION_ACTION_SET = (
    "promote_host_candidate",
    "promote_scoped_workspace_closure",
    "promote_vm_candidate",
    "promote_vm_worker_candidate",
    "install_candidate_launchd_plists",
    "apply_runtime_configuration",
    "migrate_rca_stores",
    "start_rca_resident_services",
    "run_bounded_kafka_canary",
    "run_manual_success_canary",
    "run_manual_terminal_failure_canary",
    "confirm_rca_production",
    "transition_rca_steady",
    "rollback_rca_release",
)

EXPECTED_CANDIDATE_PLISTS = tuple(sorted(release_gate.FUTURE_RUNTIME_PLIST_FILENAMES))
RUN_ARTIFACT_ORDER = (
    "build_manifest.json",
    "cutover_plan.json",
    "release_plan.json",
)
RUN_IDENTITY_FILENAME = "run_identity.json"
APPROVAL_REQUEST_FILENAME = "approval_request.json"
RUN_MANIFEST_FILENAME = "release_prepare_manifest.json"
RUN_LOCK_FILENAME = ".prepare.lock"


class ReleasePrepareError(ValueError):
    """A preparation input or publication invariant failed closed."""

    def __init__(self, code: str, detail: str = ""):
        super().__init__(detail or code)
        self.code = code
        self.detail = detail or code


@dataclass(frozen=True)
class PrepareInputs:
    env_file: Path
    host_candidate: Path
    workspace_candidate: Path
    runtime_staging_root: Path
    runtime_stage_manifest: Path
    future_live_root: Path
    workspace_runtime_root: Path
    workspace_runtime_manifest: Path
    vm_candidate: str
    vm_worker_candidate: str
    candidate_plists: tuple[Path, ...]
    release_id: str
    approval_receipt: Path | None
    rollback_config: Path
    run_root: Path
    host_contract: Path
    vm_contract: Path


@dataclass(frozen=True)
class PreparedRelease:
    run_root: Path
    manifest: Mapping[str, Any]
    resumed: bool
    phase: str


@dataclass(frozen=True)
class _OwnedFile:
    path: Path
    raw: bytes
    stat_result: os.stat_result

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.raw).hexdigest()


@dataclass(frozen=True)
class _OwnedJson(_OwnedFile):
    body: Mapping[str, Any]


def _canonical_json(value: Any) -> bytes:
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ReleasePrepareError("release_prepare_json_invalid") from exc
    return encoded + b"\n"


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).rstrip(b"\n")).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise ReleasePrepareError(
            "release_prepare_input_unreadable", str(path)
        ) from exc
    return digest.hexdigest()


def _strict_json(
    raw: bytes,
    *,
    artifact: str,
    max_bytes: int = MAX_JSON_BYTES,
) -> Mapping[str, Any]:
    if not raw or len(raw) > max_bytes:
        raise ReleasePrepareError(f"{artifact}_size_invalid")

    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise ReleasePrepareError(f"{artifact}_duplicate_key")
            result[key] = value
        return result

    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=pairs,
            parse_constant=lambda _value: (_ for _ in ()).throw(
                ReleasePrepareError(f"{artifact}_number_invalid")
            ),
        )
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ReleasePrepareError(f"{artifact}_json_invalid") from exc
    if not isinstance(value, dict):
        raise ReleasePrepareError(f"{artifact}_shape_invalid")
    return value


def _read_owned_file(
    path: Path,
    *,
    artifact: str,
    max_bytes: int = MAX_JSON_BYTES,
) -> _OwnedFile:
    candidate = path.expanduser().absolute()
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(candidate, flags)
    except OSError as exc:
        raise ReleasePrepareError(f"{artifact}_unavailable") from exc
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or stat.S_IMODE(before.st_mode) != 0o600
            or before.st_uid != os.geteuid()
            or before.st_nlink != 1
        ):
            raise ReleasePrepareError(f"{artifact}_not_owner_only")
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(descriptor, min(64 * 1024, max_bytes + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > max_bytes:
                raise ReleasePrepareError(f"{artifact}_size_invalid")
        after = os.fstat(descriptor)
        try:
            lexical = os.lstat(candidate)
        except OSError as exc:
            raise ReleasePrepareError(f"{artifact}_unstable") from exc
        if (
            (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
            != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
            or (after.st_dev, after.st_ino) != (lexical.st_dev, lexical.st_ino)
            or stat.S_ISLNK(lexical.st_mode)
        ):
            raise ReleasePrepareError(f"{artifact}_unstable")
        raw = b"".join(chunks)
        return _OwnedFile(
            path=candidate,
            raw=raw,
            stat_result=after,
        )
    finally:
        os.close(descriptor)


def _read_owned_json(
    path: Path,
    *,
    artifact: str,
    max_bytes: int = MAX_JSON_BYTES,
) -> _OwnedJson:
    owned = _read_owned_file(path, artifact=artifact, max_bytes=max_bytes)
    return _OwnedJson(
        path=owned.path,
        raw=owned.raw,
        stat_result=owned.stat_result,
        body=_strict_json(owned.raw, artifact=artifact, max_bytes=max_bytes),
    )


def _parse_timestamp(value: Any, *, field: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ReleasePrepareError("release_approval_timestamp_invalid", field)
    try:
        observed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ReleasePrepareError("release_approval_timestamp_invalid", field) from exc
    if observed.tzinfo is None or observed.utcoffset() is None:
        raise ReleasePrepareError("release_approval_timestamp_invalid", field)
    return observed.astimezone(timezone.utc)


def _require_exact_keys(
    value: Mapping[str, Any], expected: set[str], *, code: str
) -> None:
    if set(value) != expected:
        raise ReleasePrepareError(code)


def _require_sha256(value: Any, *, code: str) -> str:
    if not isinstance(value, str) or SHA256_PATTERN.fullmatch(value) is None:
        raise ReleasePrepareError(code)
    return value


def _safe_remote_root(value: str, *, field: str) -> str:
    raw = str(value or "").strip()
    path = PurePosixPath(raw)
    if (
        not raw
        or not path.is_absolute()
        or ".." in path.parts
        or any(character in raw for character in ("\x00", "\n", "\r"))
    ):
        raise ReleasePrepareError("release_prepare_remote_path_invalid", field)
    return str(path)


def observe_machine_identity(
    *, runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run
) -> Mapping[str, str]:
    """Return a non-secret digest independently bound to the current machine."""
    for source, candidate in (
        ("etc_machine_id", Path("/etc/machine-id")),
        ("dbus_machine_id", Path("/var/lib/dbus/machine-id")),
    ):
        try:
            info = candidate.lstat()
            if stat.S_ISREG(info.st_mode) and not candidate.is_symlink():
                value = candidate.read_text(encoding="ascii").strip()
                if re.fullmatch(r"[A-Za-z0-9-]{16,128}", value):
                    return {
                        "source": source,
                        "sha256": hashlib.sha256(
                            f"{source}\0{value}".encode()
                        ).hexdigest(),
                    }
        except (OSError, UnicodeError):
            pass
    if platform.system() == "Darwin":
        try:
            result = runner(
                ["/usr/sbin/ioreg", "-rd1", "-c", "IOPlatformExpertDevice"],
                check=False,
                capture_output=True,
                text=True,
                timeout=3,
            )
        except (OSError, subprocess.SubprocessError):
            result = None
        if result is not None and result.returncode == 0:
            match = re.search(
                r'"IOPlatformUUID"\s*=\s*"([A-Fa-f0-9-]{16,64})"', result.stdout
            )
            if match:
                source = "darwin_ioplatformuuid"
                return {
                    "source": source,
                    "sha256": hashlib.sha256(
                        f"{source}\0{match.group(1).lower()}".encode()
                    ).hexdigest(),
                }
    raise ReleasePrepareError("release_approval_machine_identity_unavailable")


def _validated_rollback(owned: _OwnedJson) -> Mapping[str, Any]:
    body = owned.body
    _require_exact_keys(
        body,
        {
            "schema_version",
            "owner",
            "procedure",
            "max_restore_seconds",
            "rollback_window_seconds",
        },
        code="rollback_config_shape_invalid",
    )
    if body.get("schema_version") != ROLLBACK_CONFIG_SCHEMA_VERSION:
        raise ReleasePrepareError("rollback_config_schema_invalid")
    owner = body.get("owner")
    procedure = body.get("procedure")
    max_restore = body.get("max_restore_seconds")
    window = body.get("rollback_window_seconds")
    if not isinstance(owner, str) or not owner.strip() or len(owner) > 128:
        raise ReleasePrepareError("rollback_config_owner_invalid")
    if not isinstance(procedure, str) or not procedure.strip() or len(procedure) > 4096:
        raise ReleasePrepareError("rollback_config_procedure_invalid")
    if (
        isinstance(max_restore, bool)
        or not isinstance(max_restore, int)
        or max_restore < 1
        or max_restore > 3600
    ):
        raise ReleasePrepareError("rollback_config_restore_seconds_invalid")
    if (
        isinstance(window, bool)
        or not isinstance(window, int)
        or window < max_restore
        or window > 24 * 60 * 60
    ):
        raise ReleasePrepareError("rollback_config_window_invalid")
    return {
        "schema_version": ROLLBACK_CONFIG_SCHEMA_VERSION,
        "owner": owner.strip(),
        "procedure": procedure.strip(),
        "max_restore_seconds": max_restore,
        "rollback_window_seconds": window,
    }


def _approval_identity_requirement(
    machine_identity: Mapping[str, str],
) -> Mapping[str, Any]:
    source = machine_identity.get("source")
    digest = machine_identity.get("sha256")
    if (
        not isinstance(source, str)
        or re.fullmatch(r"[a-z][a-z0-9_]{2,63}", source) is None
    ):
        raise ReleasePrepareError("release_approval_machine_identity_invalid")
    _require_sha256(digest, code="release_approval_machine_identity_invalid")
    try:
        username = pwd.getpwuid(os.geteuid()).pw_name
    except KeyError as exc:
        raise ReleasePrepareError(
            "release_approval_local_identity_unavailable"
        ) from exc
    return {
        "schema_version": RELEASE_APPROVAL_IDENTITY_SCHEMA_VERSION,
        "method": APPROVAL_IDENTITY_METHOD,
        "uid": os.geteuid(),
        "username": username,
        "machine_identity_source": source,
        "machine_identity_sha256": digest,
    }


def _validate_approval(
    owned: _OwnedJson,
    *,
    release_id: str,
    approval_request_sha256: str,
    release_bom_sha256: str,
    workspace_runtime_sha256: str,
    future_runtime_sha256: str,
    runtime_config_sha256: str,
    t0_sha256: str,
    rollback_config_sha256: str,
    rollback_window_seconds: int,
    machine_identity: Mapping[str, str],
    now: datetime,
) -> Mapping[str, Any]:
    body = owned.body
    _require_exact_keys(
        body,
        {
            "schema_version",
            "release_id",
            "decision",
            "created_at",
            "expires_at",
            "nonce",
            "action_set",
            "action_set_sha256",
            "approval_request_sha256",
            "release_bom_sha256",
            "workspace_runtime_sha256",
            "future_runtime_sha256",
            "runtime_config_sha256",
            "t0_sha256",
            "rollback_config_sha256",
            "rollback_window_seconds",
            "identity",
        },
        code="release_approval_shape_invalid",
    )
    if body.get("schema_version") != RELEASE_APPROVAL_SCHEMA_VERSION:
        raise ReleasePrepareError("release_approval_schema_invalid")
    if "approved" in body or body.get("decision") != APPROVAL_DECISION:
        raise ReleasePrepareError("release_approval_decision_invalid")
    if body.get("release_id") != release_id:
        raise ReleasePrepareError("release_approval_release_id_mismatch")
    nonce = body.get("nonce")
    if not isinstance(nonce, str) or NONCE_PATTERN.fullmatch(nonce) is None:
        raise ReleasePrepareError("release_approval_nonce_invalid")
    action_set = body.get("action_set")
    if action_set != list(PRODUCTION_ACTION_SET):
        raise ReleasePrepareError("release_approval_action_set_mismatch")
    action_set_sha256 = _require_sha256(
        body.get("action_set_sha256"), code="release_approval_action_hash_invalid"
    )
    if action_set_sha256 != _sha256_json(list(PRODUCTION_ACTION_SET)):
        raise ReleasePrepareError("release_approval_action_hash_mismatch")
    if (
        _require_sha256(
            body.get("approval_request_sha256"),
            code="release_approval_request_hash_invalid",
        )
        != approval_request_sha256
    ):
        raise ReleasePrepareError("release_approval_request_hash_mismatch")
    expected_hashes = {
        "release_bom_sha256": release_bom_sha256,
        "workspace_runtime_sha256": workspace_runtime_sha256,
        "future_runtime_sha256": future_runtime_sha256,
        "runtime_config_sha256": runtime_config_sha256,
        "t0_sha256": t0_sha256,
        "rollback_config_sha256": rollback_config_sha256,
    }
    for field, expected in expected_hashes.items():
        if (
            _require_sha256(body.get(field), code=f"release_approval_{field}_invalid")
            != expected
        ):
            raise ReleasePrepareError(f"release_approval_{field}_mismatch")
    if body.get("rollback_window_seconds") != rollback_window_seconds:
        raise ReleasePrepareError("release_approval_rollback_window_mismatch")

    created_at = _parse_timestamp(body.get("created_at"), field="created_at")
    expires_at = _parse_timestamp(body.get("expires_at"), field="expires_at")
    current = now.astimezone(timezone.utc)
    validity = (expires_at - created_at).total_seconds()
    if validity <= 0 or validity > MAX_APPROVAL_VALIDITY_SECONDS:
        raise ReleasePrepareError("release_approval_validity_invalid")
    if created_at - current > timedelta(seconds=MAX_FUTURE_SKEW_SECONDS):
        raise ReleasePrepareError("release_approval_from_future")
    if current >= expires_at:
        raise ReleasePrepareError("release_approval_expired")

    identity = body.get("identity")
    if not isinstance(identity, dict):
        raise ReleasePrepareError("release_approval_identity_invalid")
    _require_exact_keys(
        identity,
        {
            "schema_version",
            "method",
            "uid",
            "username",
            "machine_identity_source",
            "machine_identity_sha256",
        },
        code="release_approval_identity_shape_invalid",
    )
    requirement = _approval_identity_requirement(machine_identity)
    username = str(requirement["username"])
    if identity != requirement or owned.stat_result.st_uid != identity.get("uid"):
        raise ReleasePrepareError("release_approval_identity_mismatch")
    _require_sha256(
        identity.get("machine_identity_sha256"),
        code="release_approval_machine_identity_invalid",
    )
    return {
        "schema_version": RELEASE_APPROVAL_SCHEMA_VERSION,
        "decision": APPROVAL_DECISION,
        "created_at": created_at.isoformat(),
        "expires_at": expires_at.isoformat(),
        "nonce_sha256": hashlib.sha256(nonce.encode()).hexdigest(),
        "identity": {
            "schema_version": RELEASE_APPROVAL_IDENTITY_SCHEMA_VERSION,
            "method": APPROVAL_IDENTITY_METHOD,
            "uid": os.geteuid(),
            "username": username,
            "machine_identity_source": machine_identity["source"],
            "machine_identity_sha256": machine_identity["sha256"],
        },
        "receipt_sha256": owned.sha256,
    }


def _validate_candidate_plists(
    staging_root: Path,
    paths: Sequence[Path],
    runtime_detail: Mapping[str, Any],
) -> Mapping[str, str]:
    root = staging_root.expanduser().absolute()
    by_name: dict[str, Path] = {}
    for raw in paths:
        candidate = raw.expanduser().absolute()
        name = candidate.name
        if name in by_name:
            raise ReleasePrepareError("candidate_plist_duplicate")
        by_name[name] = candidate
    if set(by_name) != set(EXPECTED_CANDIDATE_PLISTS):
        raise ReleasePrepareError("candidate_plist_set_mismatch")
    render_manifest = runtime_detail.get("render_manifest")
    if not isinstance(render_manifest, Mapping):
        raise ReleasePrepareError("future_runtime_render_manifest_missing")
    observed = render_manifest.get("candidate_plists")
    if not isinstance(observed, Mapping) or set(observed) != set(
        EXPECTED_CANDIDATE_PLISTS
    ):
        raise ReleasePrepareError("candidate_runtime_plist_projection_missing")
    result: dict[str, str] = {}
    for filename in EXPECTED_CANDIDATE_PLISTS:
        path = by_name[filename]
        expected_path = root / filename
        if path.is_symlink() or path.resolve() != expected_path or not path.is_file():
            raise ReleasePrepareError("candidate_plist_path_mismatch", filename)
        digest = _sha256_file(path)
        descriptor = observed.get(filename)
        if (
            not isinstance(descriptor, Mapping)
            or descriptor.get("staging_sha256") != digest
        ):
            raise ReleasePrepareError("candidate_plist_runtime_hash_mismatch", filename)
        result[filename] = digest
    return result


def _validate_runtime_stage_identity(
    inputs: PrepareInputs,
    *,
    validator: Callable[[Path], Mapping[str, Any]],
) -> Mapping[str, Any]:
    root = inputs.runtime_staging_root.expanduser().absolute()
    manifest_path = inputs.runtime_stage_manifest.expanduser().absolute()
    if manifest_path != root / runtime_stage.MANIFEST_FILENAME:
        raise ReleasePrepareError("runtime_stage_manifest_path_mismatch")
    owned = _read_owned_json(
        manifest_path,
        artifact="runtime_stage_manifest",
        max_bytes=runtime_stage.MAX_JSON_BYTES,
    )
    try:
        validated = dict(validator(root))
    except runtime_stage.RuntimeStageError as exc:
        raise ReleasePrepareError(exc.code, exc.detail) from exc
    except ReleasePrepareError:
        raise
    except Exception as exc:
        raise ReleasePrepareError("runtime_stage_validation_failed") from exc
    if validated != owned.body:
        raise ReleasePrepareError("runtime_stage_manifest_validation_mismatch")
    projection = validated.get("future_canonical_projection")
    content = validated.get("content")
    if not isinstance(projection, Mapping) or not isinstance(content, Mapping):
        raise ReleasePrepareError("runtime_stage_manifest_invalid")
    source = content.get("source")
    runtime_files = source.get("runtime_files") if isinstance(source, Mapping) else None
    if not isinstance(runtime_files, Mapping) or not runtime_files:
        raise ReleasePrepareError("runtime_stage_runtime_files_missing")
    runtime_file_descriptors: dict[str, dict[str, Any]] = {}
    for raw_path, raw_descriptor in sorted(runtime_files.items()):
        if not isinstance(raw_path, str) or not isinstance(raw_descriptor, Mapping):
            raise ReleasePrepareError("runtime_stage_runtime_files_invalid")
        runtime_file_descriptors[raw_path] = dict(raw_descriptor)
    runtime_file_sha256 = {
        path: descriptor.get("sha256")
        for path, descriptor in runtime_file_descriptors.items()
    }
    runtime_files_sha256 = projection.get("runtime_files_sha256")
    if runtime_files_sha256 != _sha256_json(runtime_file_sha256):
        raise ReleasePrepareError("runtime_stage_runtime_files_hash_mismatch")
    if (
        validated.get("staging_root") != str(root)
        or projection.get("canonical_live_root") != str(inputs.future_live_root)
    ):
        raise ReleasePrepareError("runtime_stage_projection_mismatch")
    return {
        "schema_version": runtime_stage.MANIFEST_SCHEMA_VERSION,
        "staging_root": str(root),
        "manifest_path": str(manifest_path),
        "manifest_sha256": owned.sha256,
        "plan_sha256": validated.get("plan_sha256"),
        "content_sha256": validated.get("content_sha256"),
        "source_commit": projection.get("source_commit"),
        "source_tree": projection.get("source_tree"),
        "canonical_live_root": projection.get("canonical_live_root"),
        "candidate_plist_sha256": projection.get("candidate_plist_sha256"),
        "runtime_file_descriptors": runtime_file_descriptors,
        "runtime_files_sha256": runtime_files_sha256,
    }


def _validate_workspace_runtime_identity(
    inputs: PrepareInputs,
    *,
    workspace_component: Mapping[str, Any],
    validator: Callable[[Path], Any],
) -> Mapping[str, Any]:
    root = inputs.workspace_runtime_root.expanduser().absolute()
    manifest_path = inputs.workspace_runtime_manifest.expanduser().absolute()
    if manifest_path != root / workspace_runtime.WORKSPACE_RUNTIME_MANIFEST_NAME:
        raise ReleasePrepareError("workspace_runtime_manifest_path_mismatch")
    owned = _read_owned_json(manifest_path, artifact="workspace_runtime_manifest")
    try:
        observed = validator(root)
        identity = observed.to_dict() if hasattr(observed, "to_dict") else dict(observed)
    except workspace_runtime.WorkspaceRuntimeError as exc:
        raise ReleasePrepareError(exc.code, exc.detail) from exc
    except ReleasePrepareError:
        raise
    except Exception as exc:
        raise ReleasePrepareError("workspace_runtime_validation_failed") from exc
    expected_keys = {
        "schema_version",
        "root",
        "manifest_path",
        "creator_path",
        "manifest_sha256",
        "closure_sha256",
        "source_commit",
        "file_sha256",
    }
    files = identity.get("file_sha256")
    if set(identity) != expected_keys or not isinstance(files, Mapping):
        raise ReleasePrepareError("workspace_runtime_identity_shape_invalid")
    if (
        identity.get("schema_version")
        != workspace_runtime.WORKSPACE_RUNTIME_IDENTITY_SCHEMA_VERSION
        or identity.get("root") != str(root)
        or identity.get("manifest_path") != str(manifest_path)
        or identity.get("manifest_sha256") != owned.sha256
        or identity.get("source_commit") != workspace_component.get("commit")
        or set(files) != set(workspace_runtime.WORKSPACE_RUNTIME_FILES)
    ):
        raise ReleasePrepareError("workspace_runtime_identity_mismatch")
    closure = workspace_component.get("execution_closure")
    closure_files = closure.get("files") if isinstance(closure, Mapping) else None
    if not isinstance(closure_files, Mapping):
        raise ReleasePrepareError("workspace_runtime_workspace_closure_missing")
    for relative in workspace_runtime.WORKSPACE_RUNTIME_FILES:
        descriptor = closure_files.get(relative)
        if (
            not isinstance(descriptor, Mapping)
            or descriptor.get("sha256") != files.get(relative)
        ):
            raise ReleasePrepareError("workspace_runtime_workspace_file_mismatch", relative)
    return {
        **identity,
        "identity_sha256": _sha256_json(identity),
    }


def _validate_future_runtime_projection(
    inputs: PrepareInputs,
    *,
    runtime_detail: Mapping[str, Any],
    runtime_stage_identity: Mapping[str, Any],
    host_commit: str,
) -> None:
    projection = runtime_detail.get("future_runtime_projection")
    render_manifest = runtime_detail.get("render_manifest")
    render_manifest_sha256 = runtime_detail.get("render_manifest_sha256")
    if not isinstance(projection, Mapping) or not isinstance(render_manifest, Mapping):
        raise ReleasePrepareError("future_runtime_projection_missing")
    if (
        set(projection)
        != {
            "schema_version",
            "ok",
            "source_commit",
            "staging_root",
            "canonical_live_root",
            "render_manifest_sha256",
        }
        or projection.get("schema_version")
        != release_gate.FUTURE_RUNTIME_PROJECTION_SCHEMA_VERSION
        or projection.get("ok") is not True
        or projection.get("source_commit") != host_commit
        or projection.get("source_commit")
        != runtime_stage_identity.get("source_commit")
        or projection.get("staging_root")
        != str(inputs.runtime_staging_root.expanduser().absolute())
        or projection.get("canonical_live_root") != str(inputs.future_live_root)
        or projection.get("render_manifest_sha256") != render_manifest_sha256
        or _sha256_json(render_manifest) != render_manifest_sha256
    ):
        raise ReleasePrepareError("future_runtime_projection_mismatch")
    if (
        render_manifest.get("schema_version")
        != release_gate.FUTURE_RUNTIME_RENDER_MANIFEST_SCHEMA_VERSION
        or render_manifest.get("source_commit") != host_commit
        or render_manifest.get("staging_root") != projection.get("staging_root")
        or render_manifest.get("canonical_live_root")
        != projection.get("canonical_live_root")
    ):
        raise ReleasePrepareError("future_runtime_render_manifest_mismatch")
    staged_plists = runtime_stage_identity.get("candidate_plist_sha256")
    rendered_plists = render_manifest.get("candidate_plists")
    if not isinstance(staged_plists, Mapping) or not isinstance(
        rendered_plists, Mapping
    ):
        raise ReleasePrepareError("future_runtime_plist_binding_missing")
    for filename in EXPECTED_CANDIDATE_PLISTS:
        descriptor = rendered_plists.get(filename)
        if (
            not isinstance(descriptor, Mapping)
            or descriptor.get("source_sha256") != staged_plists.get(filename)
        ):
            raise ReleasePrepareError("future_runtime_plist_binding_mismatch", filename)


def _future_runtime_release_binding(
    *,
    runtime_stage_manifest_identity: Mapping[str, Any],
    runtime_detail: Mapping[str, Any],
) -> Mapping[str, Any]:
    runtime_stage_identity = runtime_detail.get("runtime_stage_identity")
    projection = runtime_detail.get("future_runtime_projection")
    render_manifest = runtime_detail.get("render_manifest")
    render_manifest_sha256 = runtime_detail.get("render_manifest_sha256")
    if not all(
        isinstance(value, Mapping)
        for value in (runtime_stage_identity, projection, render_manifest)
    ):
        raise ReleasePrepareError("future_runtime_release_binding_missing")
    if _sha256_json(render_manifest) != render_manifest_sha256:
        raise ReleasePrepareError("future_runtime_render_manifest_hash_mismatch")
    return {
        "schema_version": release_gate.FUTURE_RUNTIME_RELEASE_BINDING_SCHEMA_VERSION,
        "runtime_stage_manifest_identity": dict(runtime_stage_manifest_identity),
        "runtime_stage_manifest_identity_sha256": _sha256_json(
            runtime_stage_manifest_identity
        ),
        "runtime_stage_identity": dict(runtime_stage_identity),
        "runtime_stage_identity_sha256": _sha256_json(runtime_stage_identity),
        "future_runtime_projection": dict(projection),
        "future_runtime_projection_sha256": _sha256_json(projection),
        "render_manifest": dict(render_manifest),
        "render_manifest_sha256": render_manifest_sha256,
    }


def _critical_file_hashes(host_candidate: Path) -> Mapping[str, str]:
    root = host_candidate.resolve()
    try:
        required = release_gate._required_critical_files(root)
    except Exception as exc:
        raise ReleasePrepareError("release_gate_critical_file_api_failed") from exc
    result: dict[str, str] = {}
    for relative in sorted(required):
        path = root / relative
        if path.is_symlink() or not path.is_file():
            raise ReleasePrepareError("build_manifest_critical_file_missing", relative)
        result[relative] = _sha256_file(path)
    return result


def _workspace_component_projection(
    provenance: Mapping[str, Any],
) -> Mapping[str, Any]:
    value = provenance.get("workspace")
    if not isinstance(value, Mapping):
        raise ReleasePrepareError("build_provenance_workspace_missing")
    required = {
        "source",
        "repo_root",
        "commit",
        "execution_closure",
        "execution_closure_sha256",
    }
    if not required.issubset(value):
        raise ReleasePrepareError("build_provenance_workspace_closure_missing")
    if value.get("source") != "local_git_scoped_closure":
        raise ReleasePrepareError("build_provenance_workspace_source_invalid")
    try:
        closure = release_gate._normalize_workspace_execution_closure(
            value.get("execution_closure"),
            repo_root=str(value.get("repo_root") or ""),
            commit=str(value.get("commit") or ""),
            field="release_prepare.workspace.execution_closure",
        )
    except release_gate.EvidenceError as exc:
        raise ReleasePrepareError(exc.code, exc.detail) from exc
    closure_sha256 = str(value.get("execution_closure_sha256") or "")
    if closure_sha256 != release_gate._sha256_json(closure):
        raise ReleasePrepareError("build_manifest_workspace_closure_hash_mismatch")
    return {
        "source": "local_git_scoped_closure",
        "repo_root": value["repo_root"],
        "commit": value["commit"],
        "execution_closure": closure,
        "execution_closure_sha256": closure_sha256,
    }


def _workspace_runtime_release_binding(
    identity: Mapping[str, Any],
) -> Mapping[str, Any]:
    staged_identity = {
        key: value for key, value in identity.items() if key != "identity_sha256"
    }
    staged_root = Path(str(staged_identity["root"])).expanduser().absolute()
    canonical_root = release_gate.CANONICAL_WORKSPACE_RUNTIME_ROOT
    if staged_root == canonical_root:
        raise ReleasePrepareError("workspace_runtime_staging_root_is_live")
    canonical_identity = {
        **staged_identity,
        "root": str(canonical_root),
        "manifest_path": str(
            canonical_root / workspace_runtime.WORKSPACE_RUNTIME_MANIFEST_NAME
        ),
        "creator_path": str(canonical_root / "bin" / "create_task_v2.py"),
    }
    return {
        "schema_version": (
            release_gate.WORKSPACE_RUNTIME_RELEASE_BINDING_SCHEMA_VERSION
        ),
        "staged_identity": staged_identity,
        "staged_identity_sha256": _sha256_json(staged_identity),
        "canonical_identity": canonical_identity,
        "canonical_identity_sha256": _sha256_json(canonical_identity),
    }


def _build_manifest(
    *,
    inputs: PrepareInputs,
    observed_at: str,
    consumer: Any,
    dispatcher: Any,
    cutover: Any,
    runtime_detail: Mapping[str, Any],
    provenance: Mapping[str, Any],
    workspace_component: Mapping[str, Any],
    workspace_runtime_binding: Mapping[str, Any],
    future_runtime_binding: Mapping[str, Any],
    critical_files: Mapping[str, str],
) -> tuple[Mapping[str, Any], Mapping[str, Any], str]:
    del consumer, dispatcher, cutover
    components: dict[str, Any] = {}
    for component in ("host", "vm", "vm_worker"):
        value = provenance.get(component)
        if not isinstance(value, Mapping):
            raise ReleasePrepareError("build_provenance_component_missing", component)
        if component in {"vm", "vm_worker"}:
            expected = {
                "source",
                "repo_root",
                "commit",
                "tree_clean",
                "status_sha256",
                "stable",
                "tree",
                "entrypoint_path",
                "entrypoint_sha256",
                "entrypoint_committed_sha256",
                "entrypoint_git_mode",
                "entrypoint_blob",
            }
            if set(value) != expected:
                raise ReleasePrepareError(
                    "build_provenance_vm_component_shape_invalid", component
                )
        projected = {
            key: value[key]
            for key in (
                "source",
                "repo_root",
                "commit",
                "tree_clean",
                "status_sha256",
            )
            if key in value
        }
        if component in {"vm", "vm_worker"}:
            projected.update({
                key: value[key]
                for key in (
                    "tree",
                    "entrypoint_path",
                    "entrypoint_sha256",
                    "entrypoint_committed_sha256",
                    "entrypoint_git_mode",
                    "entrypoint_blob",
                )
            })
        components[component] = projected
    components["workspace"] = dict(workspace_component)

    external_value = provenance.get("external_dependencies")
    try:
        external_dependencies = release_gate._normalize_external_dependencies(
            external_value,
            field="release_prepare.external_dependencies",
        )
    except release_gate.EvidenceError as exc:
        raise ReleasePrepareError(exc.code, exc.detail) from exc

    runtime_config_sha256 = str(runtime_detail.get("runtime_config_sha256") or "")
    launchd_config_sha256 = str(runtime_detail.get("launchd_config_sha256") or "")
    _require_sha256(runtime_config_sha256, code="runtime_config_fingerprint_invalid")
    _require_sha256(launchd_config_sha256, code="launchd_config_fingerprint_invalid")
    release_bom = {
        "schema_version": release_gate.RELEASE_BOM_SCHEMA_VERSION,
        "components": components,
        "workspace_runtime": dict(workspace_runtime_binding),
        "future_runtime": dict(future_runtime_binding),
        "runtime_config_sha256": runtime_config_sha256,
        "launchd_config_sha256": launchd_config_sha256,
        "critical_files_sha256": _sha256_json(dict(sorted(critical_files.items()))),
        "external_dependencies": external_dependencies,
    }
    manifest: dict[str, Any] = {
        "schema_version": release_gate.BUILD_MANIFEST_SCHEMA_VERSION,
        "observed_at": observed_at,
        "release_bom": release_bom,
        "release_bom_sha256": _sha256_json(release_bom),
        "critical_files": dict(sorted(critical_files.items())),
        "dependency_versions": dict(release_gate.EXPECTED_DEPENDENCY_VERSIONS),
    }
    return manifest, release_bom, manifest["release_bom_sha256"]


def _validate_build_manifest_with_gate(
    manifest: Mapping[str, Any],
    *,
    inputs: PrepareInputs,
    consumer: Any,
    runtime_config_sha256: str,
    launchd_config_sha256: str,
    provenance: Mapping[str, Any],
    provenance_verifier: Callable[[Any], Mapping[str, Any]],
    now: datetime,
) -> Mapping[str, Any]:
    settings = release_gate.ReleaseGateSettings(
        mode="preauthorization",
        evidence_dir=inputs.run_root,
        expected_topic=consumer.topic,
        expected_rule_version=consumer.policy.policy_version,
        host_contract_path=inputs.host_contract,
        vm_contract_path=inputs.vm_contract,
        kafka_env_file=inputs.env_file,
        host_repo_root=inputs.host_candidate,
        workspace_repo_root=inputs.workspace_candidate,
        vm_repo_root=inputs.vm_candidate,
        vm_worker_repo_root=inputs.vm_worker_candidate,
    )

    def revalidate(live_settings: Any) -> Mapping[str, Any]:
        refreshed = dict(provenance_verifier(live_settings))
        if refreshed != provenance:
            raise release_gate.EvidenceError("build_manifest_provenance_changed")
        return refreshed

    try:
        detail = release_gate._check_build_manifest(
            manifest,
            settings=settings,
            now=now,
            max_age_seconds=release_gate.DEFAULT_EVIDENCE_MAX_AGE_SECONDS,
            expected_runtime_config_sha256=runtime_config_sha256,
            expected_launchd_config_sha256=launchd_config_sha256,
            provenance_verifier=revalidate,
        )
    except release_gate.EvidenceError as exc:
        raise ReleasePrepareError(exc.code, exc.detail) from exc
    return dict(detail)


def _validated_workspace_governance(
    build_detail: Mapping[str, Any],
    *,
    workspace_component: Mapping[str, Any],
    provenance: Mapping[str, Any],
) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    governance = build_detail.get("workspace_governance")
    if not isinstance(governance, Mapping) or set(governance) != {
        "schema_version",
        "execution_closure",
        "unscoped_drift",
    }:
        raise ReleasePrepareError("release_gate_workspace_governance_missing")
    execution = governance.get("execution_closure")
    if not isinstance(execution, Mapping) or set(execution) != {
        "ok",
        "hash",
        "required_paths",
        "commit",
        "files",
    }:
        raise ReleasePrepareError("release_gate_workspace_governance_invalid")
    closure = workspace_component["execution_closure"]
    expected_files = {
        relative: item["sha256"] for relative, item in closure["files"].items()
    }
    if (
        governance.get("schema_version")
        != release_gate.WORKSPACE_GOVERNANCE_SCHEMA_VERSION
        or execution.get("ok") is not True
        or execution.get("hash") != workspace_component["execution_closure_sha256"]
        or execution.get("required_paths")
        != list(release_gate.WORKSPACE_EXECUTION_CLOSURE_RELATIVE_PATHS)
        or execution.get("commit") != workspace_component["commit"]
        or execution.get("files") != expected_files
    ):
        raise ReleasePrepareError("release_gate_workspace_governance_invalid")
    unscoped = governance.get("unscoped_drift")
    live_workspace = provenance.get("workspace")
    if (
        not isinstance(unscoped, Mapping)
        or not isinstance(live_workspace, Mapping)
        or unscoped != live_workspace.get("unscoped_drift")
        or unscoped.get("classification") != "DRIFT-PREEXISTING"
        or unscoped.get("blocking") is not False
    ):
        raise ReleasePrepareError("release_gate_workspace_unscoped_drift_invalid")
    dependencies = build_detail.get("external_dependencies")
    try:
        normalized_dependencies = release_gate._normalize_external_dependencies(
            dependencies,
            field="release_prepare.build_detail.external_dependencies",
        )
        live_dependencies = release_gate._normalize_external_dependencies(
            provenance.get("external_dependencies"),
            field="release_prepare.provenance.external_dependencies",
        )
    except release_gate.EvidenceError as exc:
        raise ReleasePrepareError(exc.code, exc.detail) from exc
    if normalized_dependencies != live_dependencies:
        raise ReleasePrepareError("release_gate_external_dependency_drift")
    return dict(governance), normalized_dependencies


def _cutover_plan(
    *, observed_at: str, rollback: Mapping[str, Any]
) -> Mapping[str, Any]:
    return {
        "schema_version": release_gate.CUTOVER_PLAN_SCHEMA_VERSION,
        "observed_at": observed_at,
        # This projection is emitted only after validating a separate, authenticated
        # approval receipt. The gate v1 schema still requires this legacy boolean.
        "approved": True,
        "legacy_entry_mode": "read_only",
        "legacy_auto_execution_disabled": True,
        "legacy_daily_quota": 0,
        "legacy_governance_download_enabled": False,
        "data_access_mode": release_gate.FIXED_REMOTE_ACCESS_CONTRACT["mode"],
        "mdi_download_allowed": False,
        "input_materialization": "forbidden",
        "legacy_storage_reservation_enabled": False,
        "derived_capacity_reservation_enabled": True,
        "derived_capacity_atomic_reservation": True,
        "delivery_collector_enabled": True,
        "delivery_dispatcher_enabled": True,
        "rollback": {
            "owner": rollback["owner"],
            "procedure": rollback["procedure"],
            "max_restore_seconds": rollback["max_restore_seconds"],
        },
    }


def _validate_cutover_with_gate(
    body: Mapping[str, Any], *, cutover: Any, now: datetime
) -> Mapping[str, Any]:
    try:
        return dict(
            release_gate._check_cutover_plan(
                body,
                cutover=cutover,
                now=now,
                max_age_seconds=release_gate.DEFAULT_EVIDENCE_MAX_AGE_SECONDS,
            )
        )
    except release_gate.EvidenceError as exc:
        raise ReleasePrepareError(exc.code, exc.detail) from exc


def _validate_downstream_approval_binding(
    *,
    approval_request: Mapping[str, Any],
    approval_request_sha256: str,
    approval_receipt: _OwnedJson,
    machine_identity: Mapping[str, str],
    now: datetime,
) -> Mapping[str, Any]:
    validator = getattr(
        release_gate,
        "validate_release_prepare_approval_binding",
        None,
    )
    if not callable(validator):
        raise ReleasePrepareError(
            "release_gate_authenticated_approval_binding_unsupported"
        )
    try:
        result = validator(
            approval_request=approval_request,
            approval_request_sha256=approval_request_sha256,
            approval_receipt=approval_receipt.body,
            approval_receipt_sha256=approval_receipt.sha256,
            final_manifest_schema_version=(RELEASE_PREPARE_MANIFEST_SCHEMA_VERSION),
            now=now,
            machine_identity_observer=lambda: machine_identity,
        )
    except release_gate.EvidenceError as exc:
        raise ReleasePrepareError(exc.code, exc.detail) from exc
    except Exception as exc:
        raise ReleasePrepareError(
            "release_gate_authenticated_approval_binding_invalid"
        ) from exc
    expected = {
        "schema_version": RELEASE_APPROVAL_BINDING_VALIDATION_SCHEMA_VERSION,
        "ok": True,
        "approval_request_sha256": approval_request_sha256,
        "approval_receipt_sha256": approval_receipt.sha256,
        "cutover_plan_schema_version": release_gate.CUTOVER_PLAN_SCHEMA_VERSION,
        "final_manifest_schema_version": RELEASE_PREPARE_MANIFEST_SCHEMA_VERSION,
    }
    if result != expected:
        raise ReleasePrepareError("release_gate_authenticated_approval_binding_invalid")
    return expected


def _sensitive_env_values(path: Path, *, raw: bytes | None = None) -> tuple[str, ...]:
    try:
        if raw is None:
            values = dotenv_values(path, interpolate=False)
        else:
            from io import StringIO

            values = dotenv_values(
                stream=StringIO(raw.decode("utf-8")), interpolate=False
            )
    except Exception as exc:
        raise ReleasePrepareError("release_prepare_env_invalid") from exc
    result = []
    for key, value in values.items():
        if value and len(value) >= 4 and SENSITIVE_KEY_PATTERN.search(str(key)):
            result.append(str(value))
    return tuple(sorted(set(result), key=len, reverse=True))


def _assert_redacted(value: Any, *, sensitive_values: Sequence[str]) -> None:
    def visit(item: Any, path: tuple[str, ...]) -> None:
        if isinstance(item, Mapping):
            for raw_key, child in item.items():
                key = str(raw_key)
                if SENSITIVE_KEY_PATTERN.search(key) and not (
                    key.endswith("_sha256")
                    or key.endswith("_configured")
                    or key.endswith("_count")
                    or key in {"authentication_method"}
                ):
                    raise ReleasePrepareError(
                        "release_prepare_sensitive_key_present", ".".join((*path, key))
                    )
                visit(child, (*path, key))
        elif isinstance(item, (list, tuple)):
            for index, child in enumerate(item):
                visit(child, (*path, str(index)))

    visit(value, ())
    raw = _canonical_json(value)
    for secret in sensitive_values:
        if secret.encode("utf-8") in raw:
            raise ReleasePrepareError("release_prepare_credential_leak_detected")


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _read_existing_exact(path: Path) -> bytes:
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags)
    try:
        info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(info.st_mode)
            or stat.S_IMODE(info.st_mode) != 0o600
            or info.st_uid != os.geteuid()
        ):
            raise ReleasePrepareError("release_prepare_artifact_ownership_invalid")
        chunks = []
        while True:
            chunk = os.read(descriptor, 64 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
            if sum(map(len, chunks)) > MAX_JSON_BYTES:
                raise ReleasePrepareError("release_prepare_artifact_size_invalid")
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def _publish_no_clobber(path: Path, body: Mapping[str, Any]) -> bool:
    """Publish through a deterministic fsynced inode; return True on resume."""
    payload = _canonical_json(body)
    digest = hashlib.sha256(payload).hexdigest()
    temporary = path.parent / f".{path.name}.{digest}.tmp"
    if path.exists():
        if _read_existing_exact(path) != payload:
            raise ReleasePrepareError("release_prepare_artifact_conflict", path.name)
        if temporary.exists():
            if _read_existing_exact(temporary) != payload:
                raise ReleasePrepareError(
                    "release_prepare_temporary_conflict", path.name
                )
            temporary.unlink()
            _fsync_directory(path.parent)
        return True
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(temporary, flags, 0o600)
    except FileExistsError:
        if _read_existing_exact(temporary) != payload:
            raise ReleasePrepareError("release_prepare_temporary_conflict", path.name)
    else:
        try:
            view = memoryview(payload)
            while view:
                written = os.write(descriptor, view)
                view = view[written:]
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        _fsync_directory(path.parent)
    try:
        os.link(temporary, path, follow_symlinks=False)
    except FileExistsError:
        if _read_existing_exact(path) != payload:
            raise ReleasePrepareError("release_prepare_artifact_conflict", path.name)
    _fsync_directory(path.parent)
    temporary.unlink()
    _fsync_directory(path.parent)
    return False


def _ensure_run_root(path: Path) -> tuple[Path, bool]:
    root = path.expanduser().absolute()
    parent = root.parent
    if not parent.is_dir() or parent.is_symlink():
        raise ReleasePrepareError("release_prepare_run_parent_invalid")
    try:
        os.mkdir(root, mode=0o700)
        created = True
        _fsync_directory(parent)
    except FileExistsError:
        created = False
    try:
        info = root.lstat()
    except OSError as exc:
        raise ReleasePrepareError("release_prepare_run_root_unavailable") from exc
    if (
        not stat.S_ISDIR(info.st_mode)
        or stat.S_IMODE(info.st_mode) != 0o700
        or info.st_uid != os.geteuid()
        or root.is_symlink()
    ):
        raise ReleasePrepareError("release_prepare_run_root_not_owner_only")
    return root, created


@contextlib.contextmanager
def _run_lock(run_root: Path):
    path = run_root / RUN_LOCK_FILENAME
    flags = os.O_RDWR | os.O_CREAT
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags, 0o600)
    try:
        info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(info.st_mode)
            or stat.S_IMODE(info.st_mode) != 0o600
            or info.st_uid != os.geteuid()
            or info.st_nlink != 1
        ):
            raise ReleasePrepareError("release_prepare_lock_invalid")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ReleasePrepareError("release_prepare_run_in_progress") from exc
        yield
    finally:
        with contextlib.suppress(OSError):
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _input_identity(inputs: PrepareInputs, *, created_at: str) -> Mapping[str, Any]:
    return {
        "schema_version": RELEASE_PREPARE_RUN_IDENTITY_SCHEMA_VERSION,
        "created_at": created_at,
        "release_id": inputs.release_id,
        "plan_only": True,
        "inputs": {
            "env_file": {
                "path": str(inputs.env_file.expanduser().absolute()),
                "sha256": _sha256_file(inputs.env_file),
            },
            "host_candidate": str(inputs.host_candidate.resolve()),
            "workspace_candidate": str(inputs.workspace_candidate.resolve()),
            "runtime_staging_root": str(
                inputs.runtime_staging_root.expanduser().absolute()
            ),
            "runtime_stage_manifest": {
                "path": str(inputs.runtime_stage_manifest.expanduser().absolute()),
                "sha256": _sha256_file(inputs.runtime_stage_manifest),
            },
            "future_live_root": str(inputs.future_live_root.expanduser().absolute()),
            "workspace_runtime_root": str(
                inputs.workspace_runtime_root.expanduser().absolute()
            ),
            "workspace_runtime_manifest": {
                "path": str(inputs.workspace_runtime_manifest.expanduser().absolute()),
                "sha256": _sha256_file(inputs.workspace_runtime_manifest),
            },
            "vm_candidate": inputs.vm_candidate,
            "vm_worker_candidate": inputs.vm_worker_candidate,
            "candidate_plists": {
                path.name: {
                    "path": str(path.expanduser().absolute()),
                    "sha256": _sha256_file(path),
                }
                for path in sorted(inputs.candidate_plists, key=lambda item: item.name)
            },
            "rollback_config": {
                "path": str(inputs.rollback_config.expanduser().absolute()),
                "sha256": _sha256_file(inputs.rollback_config),
            },
            "host_contract": {
                "path": str(inputs.host_contract.expanduser().absolute()),
                "sha256": _sha256_file(inputs.host_contract),
            },
            "vm_contract": {
                "path": str(inputs.vm_contract.expanduser().absolute()),
                "sha256": _sha256_file(inputs.vm_contract),
            },
        },
        "side_effect_contract": {
            "live_files_written": False,
            "launchctl_invoked": False,
            "kafka_consumer_created": False,
            "kafka_offsets_mutated": False,
            "feishu_writes": False,
            "vm_files_written": False,
            "output_scope": "unique_owner_only_run_root",
        },
    }


def _validate_inputs(inputs: PrepareInputs, *, phase: str) -> None:
    if phase not in {"request", "finalize"}:
        raise ReleasePrepareError("release_prepare_phase_invalid")
    if phase == "finalize" and inputs.approval_receipt is None:
        raise ReleasePrepareError("release_approval_receipt_required")
    if RELEASE_ID_PATTERN.fullmatch(inputs.release_id) is None:
        raise ReleasePrepareError("release_prepare_release_id_invalid")
    host = inputs.host_candidate.expanduser().resolve()
    workspace = inputs.workspace_candidate.expanduser().resolve()
    if not host.is_dir() or not workspace.is_dir():
        raise ReleasePrepareError("release_prepare_candidate_repo_missing")
    _safe_remote_root(inputs.vm_candidate, field="vm_candidate")
    _safe_remote_root(inputs.vm_worker_candidate, field="vm_worker_candidate")
    if host == workspace:
        raise ReleasePrepareError("release_prepare_candidate_repo_alias")
    stage = inputs.runtime_staging_root.expanduser().absolute()
    workspace_stage = inputs.workspace_runtime_root.expanduser().absolute()
    future_root = inputs.future_live_root.expanduser().absolute()
    if future_root != release_gate.CANONICAL_FUTURE_RUNTIME_ROOT:
        raise ReleasePrepareError("future_runtime_canonical_root_invalid")
    if stage in {host, workspace, future_root} or workspace_stage in {
        host,
        workspace,
        future_root,
        workspace_runtime.canonical_workspace_runtime_root(),
        release_gate.CANONICAL_WORKSPACE_RUNTIME_ROOT,
    }:
        raise ReleasePrepareError("release_prepare_staging_root_alias")
    if inputs.runtime_stage_manifest.expanduser().absolute() != (
        stage / runtime_stage.MANIFEST_FILENAME
    ):
        raise ReleasePrepareError("runtime_stage_manifest_path_mismatch")
    if inputs.workspace_runtime_manifest.expanduser().absolute() != (
        workspace_stage / workspace_runtime.WORKSPACE_RUNTIME_MANIFEST_NAME
    ):
        raise ReleasePrepareError("workspace_runtime_manifest_path_mismatch")
    run_root = inputs.run_root.expanduser().absolute()
    for candidate in (host, workspace, stage, workspace_stage):
        try:
            run_root.relative_to(candidate)
        except ValueError:
            pass
        else:
            raise ReleasePrepareError("release_prepare_run_root_inside_candidate")


def prepare_release(
    inputs: PrepareInputs,
    *,
    phase: str = "request",
    now: datetime | None = None,
    machine_identity_observer: Callable[
        [], Mapping[str, str]
    ] = observe_machine_identity,
    provenance_verifier: Callable[[Any], Mapping[str, Any]] | None = None,
    runtime_verifier: Callable[[Path], Mapping[str, Any]] | None = None,
    runtime_projector: Callable[..., Mapping[str, Any]] | None = None,
    runtime_stage_validator: Callable[[Path], Mapping[str, Any]] | None = None,
    workspace_runtime_validator: Callable[[Path], Any] | None = None,
) -> PreparedRelease:
    """Build and atomically publish a release plan; never execute it."""
    _validate_inputs(inputs, phase=phase)
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    env_owned = _read_owned_file(inputs.env_file, artifact="release_prepare_env")
    rollback_owned = _read_owned_json(
        inputs.rollback_config, artifact="rollback_config"
    )
    if (
        phase == "finalize"
        and not (
            inputs.run_root.expanduser().absolute() / APPROVAL_REQUEST_FILENAME
        ).is_file()
    ):
        raise ReleasePrepareError("release_approval_request_required")
    run_root, created = _ensure_run_root(inputs.run_root)
    resumed_any = not created
    with _run_lock(run_root):
        identity_path = run_root / RUN_IDENTITY_FILENAME
        if identity_path.exists():
            identity_owned = _read_owned_json(
                identity_path, artifact="release_prepare_run_identity"
            )
            identity = identity_owned.body
            created_at = str(identity.get("created_at") or "")
            expected_identity = _input_identity(inputs, created_at=created_at)
            if identity != expected_identity:
                raise ReleasePrepareError("release_prepare_run_identity_conflict")
            resumed_any = True
        else:
            unexpected = {
                path.name
                for path in run_root.iterdir()
                if path.name != RUN_LOCK_FILENAME
                and not path.name.startswith(f".{RUN_IDENTITY_FILENAME}.")
            }
            if unexpected:
                raise ReleasePrepareError("release_prepare_run_root_not_empty")
            created_at = current.isoformat()
            identity = _input_identity(inputs, created_at=created_at)
            resumed_any |= _publish_no_clobber(identity_path, identity)

        identity_inputs = identity.get("inputs")
        if not isinstance(identity_inputs, Mapping):
            raise ReleasePrepareError("release_prepare_run_identity_invalid")
        stable_input_hashes = {
            "env_file": env_owned.sha256,
            "rollback_config": rollback_owned.sha256,
        }
        for name, digest in stable_input_hashes.items():
            descriptor = identity_inputs.get(name)
            if (
                not isinstance(descriptor, Mapping)
                or descriptor.get("sha256") != digest
            ):
                raise ReleasePrepareError("release_prepare_input_changed", name)

        observed_at = _parse_timestamp(created_at, field="run_identity.created_at")
        if current - observed_at > timedelta(
            seconds=release_gate.DEFAULT_EVIDENCE_MAX_AGE_SECONDS
        ):
            raise ReleasePrepareError("release_prepare_run_identity_stale")

        rollback = _validated_rollback(rollback_owned)
        consumer, dispatcher = release_gate.load_redacted_configs(
            inputs.env_file,
            environment={},
            hermes_home=inputs.env_file.expanduser().absolute().parent,
        )
        cutover = release_gate.load_cutover_config(
            inputs.env_file,
            environment={},
        )
        if (
            _read_owned_file(inputs.env_file, artifact="release_prepare_env").raw
            != env_owned.raw
        ):
            raise ReleasePrepareError("release_prepare_env_changed")
        public_config = release_gate._public_config(consumer, dispatcher, cutover)
        runtime_config_sha256 = release_gate._sha256_json(public_config)
        runtime_config_environment = {
            key: value
            for key, value in release_gate._load_env_source(
                inputs.env_file,
                environment={},
            ).items()
            if key.startswith("HERMES_RCA_")
            or key.startswith("HERMES_G1Q3_")
            or key == "G1Q3_GOVERNANCE_DOWNLOAD_ENABLED"
        }
        settings = release_gate.ReleaseGateSettings(
            mode="preauthorization",
            evidence_dir=run_root,
            expected_topic=consumer.topic,
            expected_rule_version=consumer.policy.policy_version,
            host_contract_path=inputs.host_contract,
            vm_contract_path=inputs.vm_contract,
            kafka_env_file=inputs.env_file,
            host_repo_root=inputs.host_candidate,
            workspace_repo_root=inputs.workspace_candidate,
            vm_repo_root=inputs.vm_candidate,
            vm_worker_repo_root=inputs.vm_worker_candidate,
        )
        provenance_callable = (
            provenance_verifier or release_gate.verify_live_build_provenance
        )
        projector_callable = (
            runtime_projector or release_gate.project_future_candidate_runtime
        )
        stage_validator_callable = (
            runtime_stage_validator or runtime_stage.validate_staged_runtime
        )
        workspace_runtime_validator_callable = (
            workspace_runtime_validator
            or workspace_runtime.validate_staged_workspace_runtime
        )
        try:
            provenance = dict(provenance_callable(settings))
        except release_gate.EvidenceError as exc:
            raise ReleasePrepareError(exc.code, exc.detail) from exc
        workspace_component = _workspace_component_projection(provenance)
        runtime_stage_identity = _validate_runtime_stage_identity(
            inputs,
            validator=stage_validator_callable,
        )
        workspace_runtime_identity = _validate_workspace_runtime_identity(
            inputs,
            workspace_component=workspace_component,
            validator=workspace_runtime_validator_callable,
        )
        try:
            runtime_detail = dict(projector_callable(
                inputs.host_candidate,
                inputs.runtime_staging_root,
                candidate_plists=inputs.candidate_plists,
                canonical_live_root=inputs.future_live_root,
                runtime_verifier=runtime_verifier,
                runtime_config_environment=runtime_config_environment,
                vm_worker_candidate_root=inputs.vm_worker_candidate,
            ))
        except release_gate.EvidenceError as exc:
            raise ReleasePrepareError(exc.code, exc.detail) from exc
        host_provenance = provenance.get("host")
        if not isinstance(host_provenance, Mapping):
            raise ReleasePrepareError("build_provenance_component_missing", "host")
        host_commit = str(host_provenance.get("commit") or "")
        _validate_future_runtime_projection(
            inputs,
            runtime_detail=runtime_detail,
            runtime_stage_identity=runtime_stage_identity,
            host_commit=host_commit,
        )
        runtime_detail["runtime_config_sha256"] = runtime_config_sha256
        plist_hashes = _validate_candidate_plists(
            inputs.runtime_staging_root,
            inputs.candidate_plists,
            runtime_detail,
        )
        workspace_runtime_binding = _workspace_runtime_release_binding(
            workspace_runtime_identity
        )
        future_runtime_binding = _future_runtime_release_binding(
            runtime_stage_manifest_identity=runtime_stage_identity,
            runtime_detail=runtime_detail,
        )
        workspace_runtime_sha256 = _sha256_json(workspace_runtime_binding)
        future_runtime_sha256 = _sha256_json(future_runtime_binding)
        critical_files = _critical_file_hashes(inputs.host_candidate)
        build_manifest, release_bom, release_bom_sha256 = _build_manifest(
            inputs=inputs,
            observed_at=created_at,
            consumer=consumer,
            dispatcher=dispatcher,
            cutover=cutover,
            runtime_detail=runtime_detail,
            provenance=provenance,
            workspace_component=workspace_component,
            workspace_runtime_binding=workspace_runtime_binding,
            future_runtime_binding=future_runtime_binding,
            critical_files=critical_files,
        )
        build_detail = _validate_build_manifest_with_gate(
            build_manifest,
            inputs=inputs,
            consumer=consumer,
            runtime_config_sha256=runtime_config_sha256,
            launchd_config_sha256=str(runtime_detail["launchd_config_sha256"]),
            provenance=provenance,
            provenance_verifier=provenance_callable,
            now=current,
        )
        workspace_governance, external_dependencies = _validated_workspace_governance(
            build_detail,
            workspace_component=workspace_component,
            provenance=provenance,
        )
        t0_binding = {
            "schema_version": T0_BINDING_SCHEMA_VERSION,
            "topic": consumer.topic,
            "group_id": consumer.group_id,
            "initial_offsets": {
                str(partition): offset for partition, offset in consumer.initial_offsets
            },
        }
        t0_sha256 = _sha256_json(t0_binding)
        runtime_stage_recheck = _validate_runtime_stage_identity(
            inputs,
            validator=stage_validator_callable,
        )
        workspace_runtime_recheck = _validate_workspace_runtime_identity(
            inputs,
            workspace_component=workspace_component,
            validator=workspace_runtime_validator_callable,
        )
        try:
            runtime_recheck = dict(projector_callable(
                inputs.host_candidate,
                inputs.runtime_staging_root,
                candidate_plists=inputs.candidate_plists,
                canonical_live_root=inputs.future_live_root,
                runtime_verifier=runtime_verifier,
                runtime_config_environment=runtime_config_environment,
                vm_worker_candidate_root=inputs.vm_worker_candidate,
            ))
        except release_gate.EvidenceError as exc:
            raise ReleasePrepareError(exc.code, exc.detail) from exc
        _validate_future_runtime_projection(
            inputs,
            runtime_detail=runtime_recheck,
            runtime_stage_identity=runtime_stage_recheck,
            host_commit=host_commit,
        )
        runtime_recheck["runtime_config_sha256"] = runtime_config_sha256
        if (
            runtime_recheck != runtime_detail
            or runtime_stage_recheck != runtime_stage_identity
            or workspace_runtime_recheck != workspace_runtime_identity
        ):
            raise ReleasePrepareError("candidate_runtime_changed_during_prepare")
        if (
            _validate_candidate_plists(
                inputs.runtime_staging_root,
                inputs.candidate_plists,
                runtime_recheck,
            )
            != plist_hashes
        ):
            raise ReleasePrepareError("candidate_plist_changed_during_prepare")
        machine_identity = dict(machine_identity_observer())
        identity_requirement = _approval_identity_requirement(machine_identity)
        approval_request = {
            "schema_version": RELEASE_APPROVAL_REQUEST_SCHEMA_VERSION,
            "release_id": inputs.release_id,
            "created_at": created_at,
            "production_effects_executed": False,
            "approval_required_for_finalize": True,
            "approval_identity_requirement": identity_requirement,
            "action_set": list(PRODUCTION_ACTION_SET),
            "action_set_sha256": _sha256_json(list(PRODUCTION_ACTION_SET)),
            "bindings": {
                "release_bom": release_bom,
                "release_bom_sha256": release_bom_sha256,
                "build_manifest_sha256": hashlib.sha256(
                    _canonical_json(build_manifest)
                ).hexdigest(),
                "runtime_config_sha256": runtime_config_sha256,
                "launchd_config_sha256": runtime_detail["launchd_config_sha256"],
                "t0": t0_binding,
                "t0_sha256": t0_sha256,
                "rollback_config_sha256": _sha256_json(rollback),
                "rollback_window_seconds": rollback["rollback_window_seconds"],
                "workspace_closure_sha256": workspace_component[
                    "execution_closure_sha256"
                ],
                "workspace_runtime_sha256": workspace_runtime_sha256,
                "future_runtime_sha256": future_runtime_sha256,
            },
            "candidate_plist_sha256": plist_hashes,
            "workspace_governance": workspace_governance,
            "external_dependencies": external_dependencies,
            "rollback": rollback,
            "side_effect_contract": identity["side_effect_contract"],
        }
        sensitive_values = _sensitive_env_values(inputs.env_file, raw=env_owned.raw)
        _assert_redacted(approval_request, sensitive_values=sensitive_values)
        if phase == "request":
            if (
                _read_owned_file(inputs.env_file, artifact="release_prepare_env").raw
                != env_owned.raw
                or _read_owned_file(
                    inputs.rollback_config, artifact="rollback_config"
                ).raw
                != rollback_owned.raw
            ):
                raise ReleasePrepareError("release_prepare_input_changed")
            resumed_any |= _publish_no_clobber(
                run_root / APPROVAL_REQUEST_FILENAME,
                approval_request,
            )
            return PreparedRelease(
                run_root=run_root,
                manifest=approval_request,
                resumed=resumed_any,
                phase="request",
            )

        request_owned = _read_owned_json(
            run_root / APPROVAL_REQUEST_FILENAME,
            artifact="release_approval_request",
        )
        if request_owned.body != approval_request:
            raise ReleasePrepareError("release_approval_request_drift")
        if inputs.approval_receipt is None:
            raise ReleasePrepareError("release_approval_receipt_required")
        approval_owned = _read_owned_json(
            inputs.approval_receipt, artifact="release_approval"
        )
        approval = _validate_approval(
            approval_owned,
            release_id=inputs.release_id,
            approval_request_sha256=request_owned.sha256,
            release_bom_sha256=release_bom_sha256,
            workspace_runtime_sha256=workspace_runtime_sha256,
            future_runtime_sha256=future_runtime_sha256,
            runtime_config_sha256=runtime_config_sha256,
            t0_sha256=t0_sha256,
            rollback_config_sha256=_sha256_json(rollback),
            rollback_window_seconds=int(rollback["rollback_window_seconds"]),
            machine_identity=machine_identity,
            now=current,
        )
        approval_binding_validation = _validate_downstream_approval_binding(
            approval_request=approval_request,
            approval_request_sha256=request_owned.sha256,
            approval_receipt=approval_owned,
            machine_identity=machine_identity,
            now=current,
        )
        cutover_plan = _cutover_plan(observed_at=created_at, rollback=rollback)
        cutover_detail = _validate_cutover_with_gate(
            cutover_plan, cutover=cutover, now=current
        )
        release_plan = {
            "schema_version": RELEASE_PREPARE_SCHEMA_VERSION,
            "release_id": inputs.release_id,
            "created_at": created_at,
            "mode": "plan_only",
            "executed": False,
            "approval": approval,
            "bindings": {
                "approval_request_sha256": request_owned.sha256,
                "release_bom_sha256": release_bom_sha256,
                "runtime_config_sha256": runtime_config_sha256,
                "launchd_config_sha256": runtime_detail["launchd_config_sha256"],
                "t0": t0_binding,
                "t0_sha256": t0_sha256,
                "rollback_config_sha256": _sha256_json(rollback),
                "rollback_window_seconds": rollback["rollback_window_seconds"],
                "workspace_closure_sha256": workspace_component[
                    "execution_closure_sha256"
                ],
                "workspace_runtime_sha256": workspace_runtime_sha256,
                "future_runtime_sha256": future_runtime_sha256,
            },
            "candidate_plist_sha256": plist_hashes,
            "workspace_governance": workspace_governance,
            "external_dependencies": external_dependencies,
            "action_set": list(PRODUCTION_ACTION_SET),
            "action_set_sha256": _sha256_json(list(PRODUCTION_ACTION_SET)),
            "gate_validation": {
                "approval_binding": approval_binding_validation,
                "build_manifest": build_detail,
                "cutover_plan": cutover_detail,
            },
            "rollback": rollback,
            "side_effect_contract": identity["side_effect_contract"],
        }
        final_owned_inputs = {
            "release_prepare_env": (inputs.env_file, env_owned.raw),
            "release_approval": (inputs.approval_receipt, approval_owned.raw),
            "rollback_config": (inputs.rollback_config, rollback_owned.raw),
        }
        for artifact, (path, expected_raw) in final_owned_inputs.items():
            if _read_owned_file(path, artifact=artifact).raw != expected_raw:
                raise ReleasePrepareError("release_prepare_input_changed", artifact)
        for artifact in (build_manifest, cutover_plan, release_plan):
            _assert_redacted(artifact, sensitive_values=sensitive_values)
        artifacts = {
            "build_manifest.json": build_manifest,
            "cutover_plan.json": cutover_plan,
            "release_plan.json": release_plan,
        }
        for filename in RUN_ARTIFACT_ORDER:
            resumed_any |= _publish_no_clobber(run_root / filename, artifacts[filename])
        manifest = {
            "schema_version": RELEASE_PREPARE_MANIFEST_SCHEMA_VERSION,
            "release_id": inputs.release_id,
            "created_at": created_at,
            "complete": True,
            "plan_only": True,
            "run_identity": {
                "filename": RUN_IDENTITY_FILENAME,
                "sha256": hashlib.sha256(_canonical_json(identity)).hexdigest(),
            },
            "artifacts": {
                filename: {
                    "sha256": hashlib.sha256(
                        _canonical_json(artifacts[filename])
                    ).hexdigest(),
                    "size_bytes": len(_canonical_json(artifacts[filename])),
                    "schema_version": artifacts[filename]["schema_version"],
                }
                for filename in RUN_ARTIFACT_ORDER
            },
            "approval_receipt_sha256": approval_owned.sha256,
            "approval_request_sha256": request_owned.sha256,
            "release_bom_sha256": release_bom_sha256,
            "workspace_runtime_sha256": workspace_runtime_sha256,
            "future_runtime_sha256": future_runtime_sha256,
            "action_set_sha256": _sha256_json(list(PRODUCTION_ACTION_SET)),
            "side_effect_contract": identity["side_effect_contract"],
        }
        _assert_redacted(manifest, sensitive_values=sensitive_values)
        resumed_any |= _publish_no_clobber(run_root / RUN_MANIFEST_FILENAME, manifest)
        return PreparedRelease(
            run_root=run_root,
            manifest=manifest,
            resumed=resumed_any,
            phase="finalize",
        )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=("request", "finalize"), default="request")
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--host-candidate", type=Path, required=True)
    parser.add_argument("--workspace-candidate", type=Path, required=True)
    parser.add_argument("--runtime-staging-root", type=Path, required=True)
    parser.add_argument("--runtime-stage-manifest", type=Path, required=True)
    parser.add_argument("--future-live-root", type=Path, required=True)
    parser.add_argument("--workspace-runtime-root", type=Path, required=True)
    parser.add_argument("--workspace-runtime-manifest", type=Path, required=True)
    parser.add_argument("--vm-candidate", required=True)
    parser.add_argument("--vm-worker-candidate", required=True)
    parser.add_argument("--candidate-plist", type=Path, action="append", required=True)
    parser.add_argument("--release-id", required=True)
    parser.add_argument("--approval-receipt", type=Path)
    parser.add_argument("--rollback-config", type=Path, required=True)
    parser.add_argument("--run-root", type=Path, required=True)
    parser.add_argument("--host-contract", type=Path, required=True)
    parser.add_argument("--vm-contract", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    inputs = PrepareInputs(
        env_file=args.env_file,
        host_candidate=args.host_candidate,
        workspace_candidate=args.workspace_candidate,
        runtime_staging_root=args.runtime_staging_root,
        runtime_stage_manifest=args.runtime_stage_manifest,
        future_live_root=args.future_live_root,
        workspace_runtime_root=args.workspace_runtime_root,
        workspace_runtime_manifest=args.workspace_runtime_manifest,
        vm_candidate=args.vm_candidate,
        vm_worker_candidate=args.vm_worker_candidate,
        candidate_plists=tuple(args.candidate_plist),
        release_id=args.release_id,
        approval_receipt=args.approval_receipt,
        rollback_config=args.rollback_config,
        run_root=args.run_root,
        host_contract=args.host_contract,
        vm_contract=args.vm_contract,
    )
    try:
        result = prepare_release(inputs, phase=args.phase)
    except (OSError, ValueError, ReleasePrepareError) as exc:
        code = (
            exc.code
            if isinstance(exc, ReleasePrepareError)
            else "release_prepare_failed"
        )
        print(
            json.dumps(
                {"ok": False, "code": code, "detail": type(exc).__name__},
                ensure_ascii=True,
                sort_keys=True,
            )
        )
        return 2
    print(
        json.dumps(
            {
                "ok": True,
                "mode": "plan_only",
                "phase": result.phase,
                "run_root": str(result.run_root),
                "artifact": str(
                    result.run_root
                    / (
                        APPROVAL_REQUEST_FILENAME
                        if result.phase == "request"
                        else RUN_MANIFEST_FILENAME
                    )
                ),
                "resumed": result.resumed,
            },
            ensure_ascii=True,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
