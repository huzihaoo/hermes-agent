#!/usr/bin/env python3
"""Fail-closed system adapter candidate for the RCA production cutover.

The executor owns authorization, journaling, and step ordering.  This module
owns the narrow operating-system boundary.  It deliberately has no ambient
production factory and its CLI exposes no mutation command.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import secrets
import shutil
import stat
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol, Sequence

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import pnc_rca_production_cutover as cutover


ADAPTER_AUTHORITY_SCHEMA_VERSION = "pnc_rca_cutover_adapter_authority_v1"
ADAPTER_SNAPSHOT_COMPONENT_SCHEMA_VERSION = (
    "pnc_rca_cutover_adapter_snapshot_component_v1"
)
ADAPTER_TRANSACTION_SCHEMA_VERSION = "pnc_rca_cutover_adapter_transaction_v1"
FORWARD_AUTHORITY_MODE = "forward"
RECOVERY_AUTHORITY_MODE = "rollback_only"
MAX_OWNER_FILE_BYTES = 256 * 1024 * 1024
MAX_TREE_FILES = 100_000
RUNTIME_STAGE_MANIFEST_NAME = "runtime-stage-manifest.json"
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_SAFE_LABEL_RE = re.compile(r"(?:ai\.hermes\.gateway|local\.pnc\.[a-z0-9-]+)\Z")
_TRANSACTION_ID_RE = re.compile(r"[A-Za-z0-9._-]{1,200}\Z")


class CutoverAdapterError(ValueError):
    """The operating-system adapter rejected an unsafe or drifting action."""

    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class CommandResult:
    argv: tuple[str, ...]
    returncode: int
    stdout: str = ""
    stderr: str = ""


class CommandRunner(Protocol):
    def run(self, argv: Sequence[str]) -> CommandResult: ...


class ServiceController(Protocol):
    def capture_state(self, labels: Sequence[str]) -> Mapping[str, Any]: ...

    def stop_writers(
        self,
        labels: Sequence[str],
        *,
        lease_fingerprint: str,
        lease_token: str,
    ) -> Mapping[str, Any]: ...

    def verify(
        self, labels: Sequence[str], *, runtime_sha256: str
    ) -> Mapping[str, Any]: ...

    def restore_state(self, state: Mapping[str, Any]) -> None: ...


class SubprocessArgvRunner:
    """A shell-free runner suitable for explicit future production injection."""

    def run(self, argv: Sequence[str]) -> CommandResult:
        normalized = _validate_argv(argv)
        completed = subprocess.run(  # noqa: S603 - exact argv is adapter-validated
            normalized,
            check=False,
            capture_output=True,
            text=True,
            shell=False,
        )
        return CommandResult(
            argv=tuple(normalized),
            returncode=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
        )


@dataclass(frozen=True)
class PathProjection:
    """Map declared absolute paths onto either an explicit fake root or live."""

    fake_root: Path | None

    @classmethod
    def fake(cls, root: Path) -> PathProjection:
        selected = root.expanduser().absolute()
        _require_owner_directory(selected, exact_mode=0o700)
        _ancestor_identities(selected / ".projection-boundary")
        return cls(fake_root=selected)

    @classmethod
    def production(cls, *, explicit: bool = False) -> PathProjection:
        if explicit is not True:
            raise CutoverAdapterError("cutover_adapter_production_projection_not_explicit")
        return cls(fake_root=None)

    @property
    def production_enabled(self) -> bool:
        return self.fake_root is None

    def physical(self, declared: str | Path) -> Path:
        logical = Path(declared).expanduser()
        if not logical.is_absolute() or ".." in logical.parts:
            raise CutoverAdapterError("cutover_adapter_path_not_absolute")
        logical = logical.absolute()
        if self.fake_root is None:
            return logical
        return self.fake_root.joinpath(*logical.parts[1:])


@dataclass(frozen=True)
class AdapterMutationAuthority:
    schema_version: str
    mode: str
    plan_sha256: str
    gate_binding_sha256: str
    authorization_receipt_sha256: str
    authorization_summary_sha256: str
    machine_identity_sha256: str
    rollback_target_identity_sha256: str
    forward_lease_fingerprint: str
    lease_fingerprint: str
    lease_token_sha256: str

    @classmethod
    def bind(
        cls,
        *,
        plan: Mapping[str, Any],
        gate_binding: Mapping[str, Any],
        validated_authorization: Mapping[str, Any],
        machine_identity_sha256: str,
        lease_fingerprint: str,
        lease_token: str,
    ) -> AdapterMutationAuthority:
        _require_sha256(lease_fingerprint, "cutover_adapter_lease_fingerprint_invalid")
        _require_lease_token(lease_token)
        machine = _require_sha256(
            machine_identity_sha256, "cutover_adapter_machine_identity_invalid"
        )
        authorization_keys = {
            "release_id",
            "receipt_sha256",
            "bindings",
            "expires_at",
            "nonce",
            "machine_identity_sha256",
        }
        authorization_bindings = validated_authorization.get("bindings")
        if (
            plan.get("schema_version") != cutover.PLAN_SCHEMA_VERSION
            or gate_binding != plan.get("bindings")
            or set(validated_authorization) != authorization_keys
            or not isinstance(authorization_bindings, Mapping)
            or validated_authorization.get("release_id") != plan.get("release_id")
            or validated_authorization.get("machine_identity_sha256") != machine
            or plan.get("authorization_machine_identity_sha256") != machine
            or validated_authorization.get("receipt_sha256")
            != gate_binding.get("cutover_authorization_receipt_sha256")
            or authorization_bindings.get("cutover_lease_fingerprint")
            != lease_fingerprint
            or gate_binding.get("cutover_lease_fingerprint") != lease_fingerprint
        ):
            raise CutoverAdapterError("cutover_adapter_gate_plan_binding_mismatch")
        authorization_receipt_sha256 = _require_sha256(
            validated_authorization.get("receipt_sha256"),
            "cutover_adapter_authorization_receipt_invalid",
        )
        return cls(
            schema_version=ADAPTER_AUTHORITY_SCHEMA_VERSION,
            mode=FORWARD_AUTHORITY_MODE,
            plan_sha256=_sha256_json(plan),
            gate_binding_sha256=_sha256_json(gate_binding),
            authorization_receipt_sha256=authorization_receipt_sha256,
            authorization_summary_sha256=_sha256_json(validated_authorization),
            machine_identity_sha256=machine,
            rollback_target_identity_sha256=_require_sha256(
                gate_binding.get("rollback_live_identity_sha256"),
                "cutover_adapter_rollback_identity_invalid",
            ),
            forward_lease_fingerprint=lease_fingerprint,
            lease_fingerprint=lease_fingerprint,
            lease_token_sha256=hashlib.sha256(lease_token.encode("utf-8")).hexdigest(),
        )

    @classmethod
    def bind_recovery(
        cls,
        *,
        historical_plan: Mapping[str, Any],
        historical_gate_binding: Mapping[str, Any],
        historical_run_identity: Mapping[str, Any],
        historical_run_identity_raw_sha256: str,
        historical_snapshot: Mapping[str, Any],
        journal_root: Path,
        recovery_lease_identity: Mapping[str, Any],
        validated_recovery_authorization: Mapping[str, Any],
        recovery_authorization_raw_sha256: str,
        validated_recovery_authorization_summary_sha256: str,
        machine_identity_sha256: str,
        recovery_lease_token: str,
    ) -> AdapterMutationAuthority:
        _require_lease_token(recovery_lease_token)
        machine = _require_sha256(
            machine_identity_sha256, "cutover_adapter_machine_identity_invalid"
        )
        summary_keys = {
            "release_id",
            "receipt_sha256",
            "nonce",
            "expires_at",
            "bindings",
            "machine_identity_sha256",
        }
        binding_keys = {
            "original_plan_sha256",
            "journal_root",
            "run_identity_sha256",
            "snapshot_sha256",
            "rollback_target_identity_sha256",
            "forward_lease_fingerprint",
            "forward_lease_token_sha256",
            "forward_holder_sha256",
            "recovery_lease_fingerprint",
            "recovery_lease_token_sha256",
            "recovery_holder_sha256",
            "recovery_pid",
            "machine_identity_sha256",
        }
        bindings = validated_recovery_authorization.get("bindings")
        raw_receipt_sha256 = _require_sha256(
            recovery_authorization_raw_sha256,
            "cutover_adapter_recovery_authorization_receipt_invalid",
        )
        summary_sha256 = _require_sha256(
            validated_recovery_authorization_summary_sha256,
            "cutover_adapter_recovery_authorization_summary_invalid",
        )
        plan_sha256 = _sha256_json(historical_plan)
        run_identity_sha256 = _require_sha256(
            historical_run_identity_raw_sha256,
            "cutover_adapter_recovery_run_identity_invalid",
        )
        forward_lease_identity = historical_run_identity.get("forward_lease")
        forward_holder = (
            forward_lease_identity.get("holder")
            if isinstance(forward_lease_identity, Mapping)
            else None
        )
        recovery_holder = recovery_lease_identity.get("holder")
        if (
            not isinstance(forward_lease_identity, Mapping)
            or not isinstance(forward_holder, Mapping)
            or not isinstance(recovery_holder, Mapping)
        ):
            raise CutoverAdapterError("cutover_adapter_recovery_lease_identity_invalid")
        recovery_lease = _require_sha256(
            recovery_lease_identity.get("fingerprint"),
            "cutover_adapter_recovery_lease_fingerprint_invalid",
        )
        recovery_token_sha256 = hashlib.sha256(
            recovery_lease_token.encode("utf-8")
        ).hexdigest()
        expected_lease_keys = {"fingerprint", "token_sha256", "holder", "holder_sha256"}
        expected_bindings = {
            "original_plan_sha256": plan_sha256,
            "journal_root": str(journal_root.expanduser().resolve()),
            "run_identity_sha256": run_identity_sha256,
            "snapshot_sha256": _sha256_json(historical_snapshot),
            "rollback_target_identity_sha256": historical_gate_binding.get(
                "rollback_live_identity_sha256"
            ),
            "forward_lease_fingerprint": forward_lease_identity.get("fingerprint"),
            "forward_lease_token_sha256": forward_lease_identity.get("token_sha256"),
            "forward_holder_sha256": forward_lease_identity.get("holder_sha256"),
            "recovery_lease_fingerprint": recovery_lease,
            "recovery_lease_token_sha256": recovery_lease_identity.get("token_sha256"),
            "recovery_holder_sha256": recovery_lease_identity.get("holder_sha256"),
            "recovery_pid": recovery_holder.get("pid"),
            "machine_identity_sha256": machine,
        }
        if (
            historical_plan.get("schema_version") != cutover.PLAN_SCHEMA_VERSION
            or historical_gate_binding != historical_plan.get("bindings")
            or set(historical_run_identity)
            != {
                "schema_version",
                "release_id",
                "plan_sha256",
                "authorization_receipt_sha256",
                "expected_live_identity_sha256",
                "target_live_identity_sha256",
                "rollback_live_identity_sha256",
                "forward_lease",
            }
            or hashlib.sha256(_canonical_json(historical_run_identity)).hexdigest()
            != run_identity_sha256
            or historical_run_identity.get("schema_version")
            != cutover.JOURNAL_IDENTITY_SCHEMA_VERSION
            or historical_run_identity.get("release_id")
            != historical_plan.get("release_id")
            or historical_run_identity.get("plan_sha256") != plan_sha256
            or historical_run_identity.get("rollback_live_identity_sha256")
            != historical_gate_binding.get("rollback_live_identity_sha256")
            or historical_snapshot.get("schema_version")
            != cutover.SNAPSHOT_SCHEMA_VERSION
            or historical_snapshot.get("rollback_target_identity_sha256")
            != historical_gate_binding.get("rollback_live_identity_sha256")
            or not journal_root.expanduser().is_absolute()
            or set(forward_lease_identity) != expected_lease_keys
            or set(recovery_lease_identity) != expected_lease_keys
            or set(validated_recovery_authorization) != summary_keys
            or _sha256_json(validated_recovery_authorization) != summary_sha256
            or validated_recovery_authorization.get("receipt_sha256")
            != raw_receipt_sha256
            or not isinstance(bindings, Mapping)
            or set(bindings) != binding_keys
            or dict(bindings) != expected_bindings
            or validated_recovery_authorization.get("release_id")
            != historical_plan.get("release_id")
            or validated_recovery_authorization.get("machine_identity_sha256") != machine
            or historical_plan.get("authorization_machine_identity_sha256") != machine
            or forward_lease_identity.get("fingerprint")
            != historical_gate_binding.get("cutover_lease_fingerprint")
            or recovery_lease_identity.get("token_sha256") != recovery_token_sha256
            or forward_lease_identity.get("holder_sha256")
            != _sha256_json(forward_holder)
            or recovery_lease_identity.get("holder_sha256")
            != _sha256_json(recovery_holder)
            or bindings.get("machine_identity_sha256") != machine
            or bindings.get("forward_lease_fingerprint") == recovery_lease
            or bindings.get("forward_lease_token_sha256") == recovery_token_sha256
            or isinstance(bindings.get("recovery_pid"), bool)
            or not isinstance(bindings.get("recovery_pid"), int)
            or bindings["recovery_pid"] <= 0
        ):
            raise CutoverAdapterError("cutover_adapter_recovery_authority_binding_mismatch")
        for field in (
            "run_identity_sha256",
            "snapshot_sha256",
            "rollback_target_identity_sha256",
            "forward_lease_fingerprint",
            "forward_lease_token_sha256",
            "forward_holder_sha256",
            "recovery_lease_fingerprint",
            "recovery_lease_token_sha256",
            "recovery_holder_sha256",
            "machine_identity_sha256",
        ):
            _require_sha256(
                bindings.get(field), "cutover_adapter_recovery_authority_hash_invalid"
            )
        receipt_sha256 = _require_sha256(
            validated_recovery_authorization.get("receipt_sha256"),
            "cutover_adapter_recovery_authorization_receipt_invalid",
        )
        return cls(
            schema_version=ADAPTER_AUTHORITY_SCHEMA_VERSION,
            mode=RECOVERY_AUTHORITY_MODE,
            plan_sha256=plan_sha256,
            gate_binding_sha256=_sha256_json(historical_gate_binding),
            authorization_receipt_sha256=receipt_sha256,
            authorization_summary_sha256=summary_sha256,
            machine_identity_sha256=machine,
            rollback_target_identity_sha256=bindings[
                "rollback_target_identity_sha256"
            ],
            forward_lease_fingerprint=bindings["forward_lease_fingerprint"],
            lease_fingerprint=recovery_lease,
            lease_token_sha256=recovery_token_sha256,
        )


@dataclass(frozen=True)
class _StableFile:
    raw: bytes
    identity: Mapping[str, int]
    mode: int

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.raw).hexdigest()


@dataclass(frozen=True)
class _StagedReplacement:
    target: Path
    staged: Path | None
    displaced: Path | None


def _canonical_json(value: Any) -> bytes:
    try:
        return (
            json.dumps(
                value,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise CutoverAdapterError("cutover_adapter_json_invalid") from exc


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).rstrip(b"\n")).hexdigest()


def _require_sha256(value: Any, code: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise CutoverAdapterError(code)
    return value


def _require_lease_token(value: Any) -> str:
    if (
        not isinstance(value, str)
        or len(value) < 16
        or len(value) > 4096
        or any(ord(character) < 0x21 or ord(character) > 0x7E for character in value)
    ):
        raise CutoverAdapterError("cutover_adapter_lease_token_invalid")
    return value


def _require_mode(value: Any, code: str) -> int:
    if (
        not isinstance(value, str)
        or re.fullmatch(r"[0-7]{4}", value) is None
        or int(value, 8) & 0o022
    ):
        raise CutoverAdapterError(code)
    return int(value, 8)


def _stat_fields(value: os.stat_result) -> dict[str, int]:
    return {
        field: int(getattr(value, field))
        for field in (
            "st_dev",
            "st_ino",
            "st_mode",
            "st_uid",
            "st_nlink",
            "st_size",
            "st_mtime_ns",
            "st_ctime_ns",
        )
    }


def _require_owner_directory(path: Path, *, exact_mode: int | None = None) -> None:
    try:
        before = path.lstat()
        names = tuple(sorted(os.listdir(path)))
        after = path.lstat()
    except OSError as exc:
        raise CutoverAdapterError("cutover_adapter_directory_unavailable") from exc
    del names
    if (
        stat.S_ISLNK(before.st_mode)
        or not stat.S_ISDIR(before.st_mode)
        or before.st_uid != os.geteuid()
        or stat.S_IMODE(before.st_mode) & 0o022
        or (exact_mode is not None and stat.S_IMODE(before.st_mode) != exact_mode)
        or _stat_fields(before) != _stat_fields(after)
    ):
        raise CutoverAdapterError("cutover_adapter_directory_identity_invalid")


def _ensure_owner_directory(path: Path, *, mode: int = 0o700) -> None:
    if path.exists() or path.is_symlink():
        _require_owner_directory(path)
        return
    parent = path.parent
    _require_owner_directory(parent)
    try:
        path.mkdir(mode=mode)
    except OSError as exc:
        raise CutoverAdapterError("cutover_adapter_directory_create_failed") from exc
    _fsync_directory(parent)
    _require_owner_directory(path)


def _fsync_directory(path: Path) -> None:
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    except OSError as exc:
        raise CutoverAdapterError("cutover_adapter_directory_fsync_failed") from exc


def _ancestor_identities(path: Path) -> tuple[tuple[str, tuple[int, ...]], ...]:
    """Bind an open to the complete lexical directory chain, not only its leaf."""
    selected = path.absolute()
    directories = list(reversed(selected.parent.parents)) + [selected.parent]
    identities: list[tuple[str, tuple[int, ...]]] = []
    for directory in directories:
        try:
            info = directory.lstat()
        except OSError as exc:
            raise CutoverAdapterError("cutover_adapter_ancestor_unavailable") from exc
        mode = stat.S_IMODE(info.st_mode)
        if (
            stat.S_ISLNK(info.st_mode)
            or not stat.S_ISDIR(info.st_mode)
            or info.st_uid not in {0, os.geteuid()}
            or (mode & 0o022 and not (info.st_uid == 0 and mode & stat.S_ISVTX))
        ):
            raise CutoverAdapterError("cutover_adapter_ancestor_identity_invalid")
        identities.append((
            str(directory),
            (
                int(info.st_dev),
                int(info.st_ino),
                int(info.st_mode),
                int(info.st_uid),
            ),
        ))
    return tuple(identities)


def _read_stable_owner_file(
    path: Path,
    *,
    limit: int = MAX_OWNER_FILE_BYTES,
    io_hook: Callable[[str, Path], None] | None = None,
) -> _StableFile:
    if not hasattr(os, "O_NOFOLLOW"):
        raise CutoverAdapterError("cutover_adapter_no_follow_unavailable")
    ancestors_before = _ancestor_identities(path)
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW,
        )
    except OSError as exc:
        raise CutoverAdapterError("cutover_adapter_owner_file_unavailable") from exc
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.geteuid()
            or before.st_nlink != 1
            or before.st_size < 0
            or before.st_size > limit
            or stat.S_IMODE(before.st_mode) & 0o022
        ):
            raise CutoverAdapterError("cutover_adapter_owner_file_identity_invalid")
        chunks: list[bytes] = []
        remaining = before.st_size
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                raise CutoverAdapterError("cutover_adapter_owner_file_unstable")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise CutoverAdapterError("cutover_adapter_owner_file_unstable")
        if io_hook is not None:
            io_hook("after_same_fd_read", path)
        after = os.fstat(descriptor)
        lexical = path.lstat()
        ancestors_after = _ancestor_identities(path)
        identity = _stat_fields(before)
        if (
            stat.S_ISLNK(lexical.st_mode)
            or identity != _stat_fields(after)
            or identity != _stat_fields(lexical)
            or ancestors_before != ancestors_after
        ):
            raise CutoverAdapterError("cutover_adapter_owner_file_unstable")
        return _StableFile(
            raw=b"".join(chunks),
            identity=identity,
            mode=stat.S_IMODE(before.st_mode),
        )
    except OSError as exc:
        raise CutoverAdapterError("cutover_adapter_owner_file_unstable") from exc
    finally:
        os.close(descriptor)


def _write_new_file(path: Path, raw: bytes, *, mode: int) -> None:
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        descriptor = os.open(path, flags, mode)
        try:
            view = memoryview(raw)
            while view:
                written = os.write(descriptor, view)
                if written <= 0:
                    raise CutoverAdapterError("cutover_adapter_file_write_failed")
                view = view[written:]
            os.fchmod(descriptor, mode)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    except CutoverAdapterError:
        raise
    except OSError as exc:
        raise CutoverAdapterError("cutover_adapter_file_write_failed") from exc


def _validate_argv(argv: Sequence[str]) -> list[str]:
    if isinstance(argv, (str, bytes)) or not argv:
        raise CutoverAdapterError("cutover_adapter_argv_invalid")
    normalized: list[str] = []
    for argument in argv:
        if (
            not isinstance(argument, str)
            or not argument
            or "\x00" in argument
            or "\n" in argument
            or "\r" in argument
        ):
            raise CutoverAdapterError("cutover_adapter_argv_invalid")
        normalized.append(argument)
    return normalized


def _validate_logical_commands(
    step: str, commands: Sequence[Sequence[str]], plan: Mapping[str, Any]
) -> list[list[str]]:
    normalized = [_validate_argv(command) for command in commands]
    try:
        expected = cutover._expected_commands_for_step(step, plan)
    except Exception as exc:
        raise CutoverAdapterError("cutover_adapter_step_unknown") from exc
    if normalized != expected:
        raise CutoverAdapterError("cutover_adapter_command_not_allowlisted")
    domain = f"gui/{os.geteuid()}"
    for command in normalized:
        executable = command[0]
        if executable == cutover.CUTOVER_ADAPTER_EXECUTABLE:
            subcommand = command[1] if len(command) > 1 else ""
            arity = {
                "stop-writers": 2 + len(cutover.WRITER_LABELS),
                "install-owner-file": 5,
                "install-retained-tree": 5,
                "transition-bounded-activation": 3,
                "restore-exact-snapshot": 4,
            }.get(subcommand)
            if arity is None or len(command) != arity:
                raise CutoverAdapterError("cutover_adapter_subcommand_invalid")
            if subcommand == "stop-writers" and tuple(command[2:]) != tuple(
                cutover.WRITER_LABELS
            ):
                raise CutoverAdapterError("cutover_adapter_writer_labels_invalid")
            if subcommand in {"install-owner-file", "install-retained-tree"}:
                if not Path(command[2]).is_absolute() or not Path(command[3]).is_absolute():
                    raise CutoverAdapterError("cutover_adapter_command_path_invalid")
                _require_sha256(command[4], "cutover_adapter_command_hash_invalid")
            if subcommand in {
                "transition-bounded-activation",
                "restore-exact-snapshot",
            }:
                for value in command[2:]:
                    _require_sha256(value, "cutover_adapter_command_hash_invalid")
        elif executable == "/bin/launchctl":
            if command[1:3] == ["kickstart", "-k"]:
                if len(command) != 4 or not command[3].startswith(f"{domain}/"):
                    raise CutoverAdapterError("cutover_adapter_launchctl_argv_invalid")
                label = command[3].removeprefix(f"{domain}/")
            elif command[1] == "bootstrap":
                if len(command) != 4 or command[2] != domain:
                    raise CutoverAdapterError("cutover_adapter_launchctl_argv_invalid")
                label = Path(command[3]).stem
            else:
                raise CutoverAdapterError("cutover_adapter_launchctl_argv_invalid")
            if _SAFE_LABEL_RE.fullmatch(label) is None or label not in cutover.SERVICE_LABELS:
                raise CutoverAdapterError("cutover_adapter_launchctl_label_invalid")
        else:
            raise CutoverAdapterError("cutover_adapter_executable_invalid")
    return normalized


def _atomic_publish_json(path: Path, body: Mapping[str, Any]) -> str:
    raw = _canonical_json(body)
    _ensure_owner_directory(path.parent)
    if path.exists() or path.is_symlink():
        observed = _read_stable_owner_file(path, limit=len(raw) + 1)
        if observed.raw != raw or observed.mode != 0o600:
            raise CutoverAdapterError("cutover_adapter_receipt_conflict")
        return observed.sha256
    temporary = path.parent / f".{path.name}.{secrets.token_hex(8)}.tmp"
    try:
        _write_new_file(temporary, raw, mode=0o600)
        try:
            os.link(temporary, path, follow_symlinks=False)
        except FileExistsError:
            observed = _read_stable_owner_file(path, limit=len(raw) + 1)
            if observed.raw != raw:
                raise CutoverAdapterError("cutover_adapter_receipt_conflict")
        _fsync_directory(path.parent)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
    return hashlib.sha256(raw).hexdigest()


def _replace_json(path: Path, body: Mapping[str, Any]) -> None:
    """Durably replace mutable adapter state without exposing partial JSON."""
    raw = _canonical_json(body)
    _ensure_owner_directory(path.parent)
    temporary = path.parent / f".{path.name}.{secrets.token_hex(8)}.tmp"
    try:
        _write_new_file(temporary, raw, mode=0o600)
        os.rename(temporary, path)
        _fsync_directory(path.parent)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _read_json_mapping(path: Path) -> Mapping[str, Any]:
    observed = _read_stable_owner_file(path)
    try:
        value = json.loads(observed.raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise CutoverAdapterError("cutover_adapter_transaction_invalid") from exc
    if not isinstance(value, Mapping):
        raise CutoverAdapterError("cutover_adapter_transaction_invalid")
    return value


class ProductionCutoverAdapter:
    """Explicitly injected implementation of ``CutoverSystemAdapter``."""

    def __init__(
        self,
        *,
        projection: PathProjection,
        identity_observer: Callable[[], Mapping[str, Any]],
        snapshot_root: Path,
        runner: CommandRunner | None = None,
        service_controller: ServiceController | None = None,
        authority: AdapterMutationAuthority | None = None,
        io_hook: Callable[[str, Path], None] | None = None,
    ):
        if projection.production_enabled and authority is None:
            raise CutoverAdapterError("cutover_adapter_production_authority_required")
        self._projection = projection
        self._observer = identity_observer
        self._snapshot_root = projection.physical(snapshot_root)
        self._transaction_root = self._snapshot_root / "transactions"
        self._runner = runner
        self._services = service_controller
        self._authority = authority
        self._io_hook = io_hook

    def observe_live_identity(self) -> Mapping[str, Any]:
        value = self._observer()
        if not isinstance(value, Mapping):
            raise CutoverAdapterError("cutover_adapter_live_identity_invalid")
        body = dict(value)
        _sha256_json(body)
        return body

    def _require_authority(
        self,
        *,
        plan: Mapping[str, Any],
        lease_fingerprint: str,
        lease_token: str,
        operation: str,
    ) -> AdapterMutationAuthority:
        authority = self._authority
        if authority is None:
            raise CutoverAdapterError("cutover_adapter_mutation_authority_required")
        common_invalid = (
            authority.schema_version != ADAPTER_AUTHORITY_SCHEMA_VERSION
            or authority.mode not in {FORWARD_AUTHORITY_MODE, RECOVERY_AUTHORITY_MODE}
            or authority.plan_sha256 != _sha256_json(plan)
            or authority.gate_binding_sha256 != _sha256_json(plan.get("bindings"))
            or authority.machine_identity_sha256
            != plan.get("authorization_machine_identity_sha256")
            or authority.rollback_target_identity_sha256
            != plan.get("bindings", {}).get("rollback_live_identity_sha256")
            or authority.forward_lease_fingerprint
            != plan.get("bindings", {}).get("cutover_lease_fingerprint")
            or authority.lease_fingerprint != lease_fingerprint
            or authority.lease_token_sha256
            != hashlib.sha256(_require_lease_token(lease_token).encode("utf-8")).hexdigest()
        )
        forward_invalid = authority.mode == FORWARD_AUTHORITY_MODE and (
            authority.authorization_receipt_sha256
            != plan.get("bindings", {}).get("cutover_authorization_receipt_sha256")
            or authority.lease_fingerprint != authority.forward_lease_fingerprint
        )
        operation_invalid = operation not in {"forward", "rollback"} or (
            operation == "forward" and authority.mode != FORWARD_AUTHORITY_MODE
        )
        if common_invalid or forward_invalid or operation_invalid:
            raise CutoverAdapterError("cutover_adapter_mutation_authority_mismatch")
        return authority

    def _assert_live_identity(self, expected: str) -> None:
        _require_sha256(expected, "cutover_adapter_expected_identity_invalid")
        if _sha256_json(self.observe_live_identity()) != expected:
            raise CutoverAdapterError("cutover_adapter_live_identity_drift")

    def preflight_step(
        self,
        step: str,
        *,
        expected_identity_sha256: str,
        plan: Mapping[str, Any],
        payload_descriptors: Mapping[str, Any],
        lease_fingerprint: str,
        lease_token: str,
    ) -> Mapping[str, Any]:
        _require_sha256(lease_fingerprint, "cutover_adapter_lease_fingerprint_invalid")
        _require_lease_token(lease_token)
        self._assert_live_identity(expected_identity_sha256)
        commands = _validate_logical_commands(
            step, cutover._expected_commands_for_step(step, plan), plan
        )
        self._validate_payloads(step, payload_descriptors, commands, plan)
        if self._authority is not None:
            self._require_authority(
                plan=plan,
                lease_fingerprint=lease_fingerprint,
                lease_token=lease_token,
                operation="rollback" if step == "rollback" else "forward",
            )
        return {
            "schema_version": cutover.COMMAND_PREFLIGHT_SCHEMA_VERSION,
            "step": step,
            "expected_identity_sha256": expected_identity_sha256,
            "commands": commands,
            "payload_descriptors": payload_descriptors,
            "lease_fingerprint": lease_fingerprint,
        }

    def _regular_descriptor(self, descriptor: Mapping[str, Any]) -> _StableFile:
        if (
            descriptor.get("schema_version") != cutover.PAYLOAD_DESCRIPTOR_SCHEMA_VERSION
            or descriptor.get("kind") != "regular_file"
            or not isinstance(descriptor.get("path"), str)
            or descriptor.get("binding_sha256") != descriptor.get("physical_sha256")
        ):
            raise CutoverAdapterError("cutover_adapter_payload_descriptor_invalid")
        observed = _read_stable_owner_file(
            self._projection.physical(descriptor["path"]), io_hook=self._io_hook
        )
        if (
            observed.sha256 != descriptor.get("physical_sha256")
            or len(observed.raw) != descriptor.get("size_bytes")
            or observed.identity != descriptor.get("identity")
        ):
            raise CutoverAdapterError("cutover_adapter_payload_drift")
        return observed

    def _tree_expected_files(
        self, descriptor: Mapping[str, Any]
    ) -> Mapping[str, Mapping[str, Any]]:
        kind = descriptor.get("kind")
        if kind == "runtime_tree":
            files = descriptor.get("files")
            if not isinstance(files, Mapping):
                raise CutoverAdapterError("cutover_adapter_tree_descriptor_invalid")
            return files
        if kind == "workspace_tree":
            identity = descriptor.get("identity")
            if not isinstance(identity, Mapping) or not isinstance(
                identity.get("file_sha256"), Mapping
            ):
                raise CutoverAdapterError("cutover_adapter_tree_descriptor_invalid")
            root = Path(str(descriptor.get("path") or ""))
            manifest = Path(str(identity.get("manifest_path") or ""))
            try:
                manifest_relative = manifest.relative_to(root).as_posix()
            except ValueError as exc:
                raise CutoverAdapterError(
                    "cutover_adapter_tree_descriptor_invalid"
                ) from exc
            files: dict[str, Mapping[str, Any]] = {
                str(name): {"sha256": sha}
                for name, sha in identity["file_sha256"].items()
            }
            files[manifest_relative] = {"sha256": identity.get("manifest_sha256")}
            return files
        raise CutoverAdapterError("cutover_adapter_tree_descriptor_invalid")

    def _scan_tree(
        self,
        root: Path,
        *,
        destination: Path | None = None,
        expected: Mapping[str, Mapping[str, Any]] | None = None,
        expected_directories: Mapping[str, Mapping[str, Any]] | None = None,
        expected_root_mode: str | None = None,
        directories_out: dict[str, Mapping[str, Any]] | None = None,
        excluded: frozenset[str] = frozenset(),
    ) -> Mapping[str, Mapping[str, Any]]:
        _require_owner_directory(root)
        root_before = _stat_fields(root.lstat())
        root_mode = f"{stat.S_IMODE(root_before['st_mode']):04o}"
        if expected_root_mode is not None and root_mode != expected_root_mode:
            raise CutoverAdapterError("cutover_adapter_tree_root_mode_drift")
        observed: dict[str, Mapping[str, Any]] = {}
        observed_directories: dict[str, Mapping[str, Any]] = {}
        paths = sorted(root.rglob("*"), key=lambda item: item.as_posix())
        if len(paths) > MAX_TREE_FILES:
            raise CutoverAdapterError("cutover_adapter_tree_too_large")
        if destination is not None:
            destination.mkdir(mode=0o700)
            destination.chmod(int(root_mode, 8))
        for path in paths:
            relative = path.relative_to(root).as_posix()
            if relative in excluded:
                continue
            info = path.lstat()
            if stat.S_ISLNK(info.st_mode):
                raise CutoverAdapterError("cutover_adapter_tree_symlink_forbidden")
            target = destination / relative if destination is not None else None
            if stat.S_ISDIR(info.st_mode):
                if info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) & 0o022:
                    raise CutoverAdapterError("cutover_adapter_tree_directory_invalid")
                directory_entry = {"mode": f"{stat.S_IMODE(info.st_mode):04o}"}
                observed_directories[relative] = directory_entry
                if target is not None:
                    target.mkdir(mode=stat.S_IMODE(info.st_mode), parents=True)
                    target.chmod(stat.S_IMODE(info.st_mode))
                continue
            if not stat.S_ISREG(info.st_mode):
                raise CutoverAdapterError("cutover_adapter_tree_special_file_forbidden")
            stable = _read_stable_owner_file(path, io_hook=self._io_hook)
            entry = {
                "sha256": stable.sha256,
                "size_bytes": len(stable.raw),
                "mode": f"{stable.mode:04o}",
            }
            observed[relative] = entry
            if target is not None:
                target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                _write_new_file(target, stable.raw, mode=stable.mode)
        if expected is not None:
            if set(observed) != set(expected):
                raise CutoverAdapterError("cutover_adapter_tree_layout_drift")
            for relative, expected_entry in expected.items():
                actual = observed[relative]
                if actual["sha256"] != expected_entry.get("sha256"):
                    raise CutoverAdapterError("cutover_adapter_tree_hash_drift")
                if "size_bytes" in expected_entry and (
                    actual["size_bytes"] != expected_entry.get("size_bytes")
                ):
                    raise CutoverAdapterError("cutover_adapter_tree_size_drift")
                if "mode" in expected_entry and actual["mode"] != expected_entry.get("mode"):
                    raise CutoverAdapterError("cutover_adapter_tree_mode_drift")
                identity = expected_entry.get("identity")
                if isinstance(identity, Mapping):
                    physical = _read_stable_owner_file(
                        root / relative, io_hook=self._io_hook
                    )
                    if physical.identity != identity:
                        raise CutoverAdapterError("cutover_adapter_tree_identity_drift")
        if expected_directories is not None and observed_directories != dict(
            expected_directories
        ):
            raise CutoverAdapterError("cutover_adapter_tree_directory_drift")
        if directories_out is not None:
            directories_out.update(observed_directories)
        if destination is not None:
            for directory in sorted(
                (path for path in destination.rglob("*") if path.is_dir()),
                key=lambda item: len(item.parts),
                reverse=True,
            ):
                _fsync_directory(directory)
            _fsync_directory(destination)
        if _stat_fields(root.lstat()) != root_before:
            raise CutoverAdapterError("cutover_adapter_tree_root_drift")
        return observed

    def _tree_semantic(self, root: Path) -> Mapping[str, Any]:
        before = _stat_fields(root.lstat())
        directories: dict[str, Mapping[str, Any]] = {}
        root_mode = f"{stat.S_IMODE(before['st_mode']):04o}"
        files = self._scan_tree(
            root,
            expected_root_mode=root_mode,
            directories_out=directories,
        )
        if _stat_fields(root.lstat()) != before:
            raise CutoverAdapterError("cutover_adapter_tree_root_drift")
        return {
            "root_mode": root_mode,
            "directories": directories,
            "files": files,
        }

    def _validate_payloads(
        self,
        step: str,
        payloads: Mapping[str, Any],
        commands: Sequence[Sequence[str]],
        plan: Mapping[str, Any],
    ) -> None:
        expected_names = {
            "install_feishu_sidecar": {"feishu_sidecar"},
            "install_runtime": {"runtime"},
            "install_workspace": {"workspace"},
            "install_environment": {"candidate_environment", "active_release_binding"},
            "install_plists": {"runtime"},
        }.get(step, set())
        if set(payloads) != expected_names:
            raise CutoverAdapterError("cutover_adapter_payload_set_invalid")
        if step in {"install_feishu_sidecar", "install_environment"}:
            for descriptor in payloads.values():
                self._regular_descriptor(descriptor)
        elif step in {"install_runtime", "install_workspace", "install_plists"}:
            descriptor = payloads[next(iter(payloads))]
            if (
                not isinstance(descriptor, Mapping)
                or descriptor.get("schema_version")
                != cutover.PAYLOAD_DESCRIPTOR_SCHEMA_VERSION
                or not isinstance(descriptor.get("path"), str)
            ):
                raise CutoverAdapterError("cutover_adapter_tree_descriptor_invalid")
            root = self._projection.physical(descriptor["path"])
            expected_files = self._tree_expected_files(descriptor)
            if descriptor.get("kind") == "runtime_tree":
                physical = {
                    relative: {
                        "sha256": entry.get("sha256"),
                        "size_bytes": entry.get("size_bytes"),
                        "identity": entry.get("identity"),
                    }
                    for relative, entry in expected_files.items()
                }
                if (
                    _stat_fields(root.lstat()) != descriptor.get("root_identity")
                    or _sha256_json(physical) != descriptor.get("physical_sha256")
                ):
                    raise CutoverAdapterError(
                        "cutover_adapter_tree_descriptor_identity_invalid"
                    )
            elif descriptor.get("kind") == "workspace_tree":
                identity = descriptor.get("identity")
                if (
                    not isinstance(identity, Mapping)
                    or descriptor.get("binding_sha256")
                    != identity.get("closure_sha256")
                    or descriptor.get("physical_sha256") != _sha256_json(identity)
                ):
                    raise CutoverAdapterError(
                        "cutover_adapter_tree_descriptor_identity_invalid"
                    )
            excluded = frozenset()
            if descriptor.get("kind") == "runtime_tree":
                excluded = frozenset({RUNTIME_STAGE_MANIFEST_NAME})
                manifest = _read_stable_owner_file(
                    root / RUNTIME_STAGE_MANIFEST_NAME, io_hook=self._io_hook
                )
                expected_manifest_sha = plan.get("bindings", {}).get(
                    "runtime_stage_manifest_sha256"
                )
                if expected_manifest_sha is not None and (
                    manifest.sha256 != expected_manifest_sha
                ):
                    raise CutoverAdapterError(
                        "cutover_adapter_runtime_manifest_drift"
                    )
            self._scan_tree(root, expected=expected_files, excluded=excluded)
            if step == "install_plists":
                install_plists = descriptor.get("install_plists")
                expected_sources = {command[2] for command in commands}
                if (
                    not isinstance(install_plists, Mapping)
                    or set(install_plists) != expected_sources
                ):
                    raise CutoverAdapterError(
                        "cutover_adapter_plist_payload_invalid"
                    )
                for command in commands:
                    entry = install_plists.get(command[2])
                    if not isinstance(entry, Mapping):
                        raise CutoverAdapterError(
                            "cutover_adapter_plist_payload_invalid"
                        )
                    mode = entry.get("mode")
                    size = entry.get("size_bytes")
                    identity = entry.get("identity")
                    if (
                        entry.get("sha256") != command[4]
                        or mode != "0644"
                        or isinstance(size, bool)
                        or not isinstance(size, int)
                        or size < 0
                        or not isinstance(identity, Mapping)
                    ):
                        raise CutoverAdapterError(
                            "cutover_adapter_plist_payload_invalid"
                        )
                    observed = _read_stable_owner_file(
                        self._projection.physical(command[2]),
                        io_hook=self._io_hook,
                    )
                    if (
                        observed.sha256 != command[4]
                        or len(observed.raw) != size
                        or observed.mode != int(mode, 8)
                        or observed.identity != identity
                    ):
                        raise CutoverAdapterError(
                            "cutover_adapter_plist_payload_drift"
                        )

    def _stage_owner_file(
        self,
        source: Path,
        target: Path,
        *,
        expected_sha256: str,
        expected_identity: Mapping[str, Any] | None = None,
        expected_mode: int | None = None,
    ) -> Path:
        _ensure_owner_directory(target.parent)
        _ancestor_identities(target)
        observed = _read_stable_owner_file(source, io_hook=self._io_hook)
        if observed.sha256 != expected_sha256 or (
            expected_identity is not None and observed.identity != expected_identity
        ) or (expected_mode is not None and observed.mode != expected_mode):
            raise CutoverAdapterError("cutover_adapter_install_source_drift")
        staged = target.parent / f".{target.name}.{secrets.token_hex(8)}.install"
        _write_new_file(staged, observed.raw, mode=observed.mode)
        installed = _read_stable_owner_file(staged)
        if installed.sha256 != expected_sha256:
            raise CutoverAdapterError("cutover_adapter_install_stage_drift")
        return staged

    def _commit_files_transaction(
        self,
        staged: Sequence[tuple[Path, Path]],
        *,
        transaction_id: str,
    ) -> None:
        self._commit_replacements_transaction(
            [(temporary, target, "file") for temporary, target in staged],
            transaction_id=transaction_id,
        )

    def _path_semantic(self, path: Path, *, kind: str) -> Mapping[str, Any]:
        if not path.exists() and not path.is_symlink():
            return {"kind": "absent"}
        info = path.lstat()
        if stat.S_ISLNK(info.st_mode):
            raise CutoverAdapterError("cutover_adapter_transaction_symlink_forbidden")
        if kind == "file":
            observed = _read_stable_owner_file(path, io_hook=self._io_hook)
            return {
                "kind": "file",
                "sha256": observed.sha256,
                "size_bytes": len(observed.raw),
                "mode": f"{observed.mode:04o}",
                "uid": int(info.st_uid),
                "nlink": int(info.st_nlink),
            }
        if kind == "tree":
            if not stat.S_ISDIR(info.st_mode):
                raise CutoverAdapterError("cutover_adapter_transaction_kind_drift")
            closure = self._tree_semantic(path)
            return {
                "kind": "tree",
                "closure": closure,
                "closure_sha256": _sha256_json(closure),
            }
        raise CutoverAdapterError("cutover_adapter_transaction_kind_invalid")

    @staticmethod
    def _semantic_matches(actual: Mapping[str, Any], expected: Mapping[str, Any]) -> bool:
        return dict(actual) == dict(expected)

    def _transaction_path(self, transaction_id: str) -> Path:
        if (
            not isinstance(transaction_id, str)
            or _TRANSACTION_ID_RE.fullmatch(transaction_id) is None
        ):
            raise CutoverAdapterError("cutover_adapter_transaction_id_invalid")
        _ensure_owner_directory(self._snapshot_root)
        _ensure_owner_directory(self._transaction_root)
        return self._transaction_root / f"{hashlib.sha256(transaction_id.encode()).hexdigest()}.json"

    def _load_or_create_transaction(
        self,
        replacements: Sequence[tuple[Path, Path, str]],
        *,
        transaction_id: str,
    ) -> tuple[Path, dict[str, Any]]:
        path = self._transaction_path(transaction_id)
        supplied: list[dict[str, Any]] = []
        seen_targets: set[str] = set()
        for index, (temporary, target, kind) in enumerate(replacements):
            if str(target) in seen_targets:
                raise CutoverAdapterError("cutover_adapter_transaction_target_duplicate")
            seen_targets.add(str(target))
            desired = self._path_semantic(temporary, kind=kind)
            if desired.get("kind") != kind:
                raise CutoverAdapterError("cutover_adapter_transaction_stage_invalid")
            supplied.append({
                "index": index,
                "kind": kind,
                "target": str(target),
                "staged": str(temporary),
                "displaced": str(
                    target.parent / f".{target.name}.precutover.{transaction_id}.{index}"
                ),
                "desired": desired,
            })
        if path.exists() or path.is_symlink():
            loaded = dict(_read_json_mapping(path))
            entries = loaded.get("entries")
            if (
                loaded.get("schema_version") != ADAPTER_TRANSACTION_SCHEMA_VERSION
                or loaded.get("transaction_id") != transaction_id
                or loaded.get("status") not in {"prepared", "committing", "committed"}
                or not isinstance(entries, list)
                or len(entries) != len(supplied)
            ):
                raise CutoverAdapterError("cutover_adapter_transaction_invalid")
            changed = False
            for current, candidate in zip(entries, supplied, strict=True):
                if (
                    not isinstance(current, dict)
                    or current.get("index") != candidate["index"]
                    or current.get("kind") != candidate["kind"]
                    or current.get("target") != candidate["target"]
                    or current.get("displaced") != candidate["displaced"]
                    or current.get("desired") != candidate["desired"]
                    or not isinstance(current.get("original"), Mapping)
                ):
                    raise CutoverAdapterError("cutover_adapter_transaction_conflict")
                previous_stage = Path(str(current.get("staged") or ""))
                if not previous_stage.exists() and not previous_stage.is_symlink():
                    current["staged"] = candidate["staged"]
                    changed = True
            if changed:
                _replace_json(path, loaded)
            return path, loaded
        entries = []
        for candidate in supplied:
            target = Path(candidate["target"])
            displaced = Path(candidate["displaced"])
            if displaced.exists() or displaced.is_symlink():
                raise CutoverAdapterError("cutover_adapter_retained_target_conflict")
            entries.append({
                **candidate,
                "original": self._path_semantic(target, kind=candidate["kind"]),
                "phase": "pending",
            })
        body = {
            "schema_version": ADAPTER_TRANSACTION_SCHEMA_VERSION,
            "transaction_id": transaction_id,
            "status": "prepared",
            "entries": entries,
        }
        _replace_json(path, body)
        return path, body

    def _classify_transaction_entry(self, entry: Mapping[str, Any]) -> str:
        kind = str(entry["kind"])
        target = Path(str(entry["target"]))
        staged = Path(str(entry["staged"]))
        displaced = Path(str(entry["displaced"]))
        target_state = self._path_semantic(target, kind=kind)
        staged_state = self._path_semantic(staged, kind=kind)
        displaced_state = self._path_semantic(displaced, kind=kind)
        desired = entry["desired"]
        original = entry["original"]
        absent = {"kind": "absent"}
        if self._semantic_matches(target_state, desired) and (
            (original == absent and displaced_state == absent)
            or self._semantic_matches(displaced_state, original)
        ):
            return "installed"
        if (
            self._semantic_matches(target_state, original)
            and self._semantic_matches(staged_state, desired)
            and displaced_state == absent
        ):
            return "pending"
        if (
            target_state == absent
            and self._semantic_matches(staged_state, desired)
            and (
                (original == absent and displaced_state == absent)
                or self._semantic_matches(displaced_state, original)
            )
        ):
            return "displaced"
        raise CutoverAdapterError("cutover_adapter_transaction_state_unknown")

    def _abort_transaction(self, path: Path, body: dict[str, Any]) -> None:
        body["status"] = "aborting"
        _replace_json(path, body)
        for entry in reversed(body["entries"]):
            state = self._classify_transaction_entry(entry)
            target = Path(entry["target"])
            displaced = Path(entry["displaced"])
            original = entry["original"]
            if state == "installed":
                failed = target.parent / (
                    f".{target.name}.failed.{body['transaction_id']}.{secrets.token_hex(4)}"
                )
                os.rename(target, failed)
                _fsync_directory(target.parent)
                if original != {"kind": "absent"}:
                    os.rename(displaced, target)
                    _fsync_directory(target.parent)
            elif state == "displaced" and original != {"kind": "absent"}:
                os.rename(displaced, target)
                _fsync_directory(target.parent)
            entry["phase"] = "aborted"
            _replace_json(path, body)
        body["status"] = "aborted"
        _replace_json(path, body)

    def _commit_replacements_transaction(
        self,
        replacements: Sequence[tuple[Path, Path, str]],
        *,
        transaction_id: str,
    ) -> None:
        path, body = self._load_or_create_transaction(
            replacements, transaction_id=transaction_id
        )
        if body.get("status") == "committed":
            for entry in body["entries"]:
                if self._classify_transaction_entry(entry) != "installed":
                    raise CutoverAdapterError("cutover_adapter_transaction_committed_drift")
            for temporary, _target, _kind in replacements:
                if temporary.exists() or temporary.is_symlink():
                    if temporary.is_dir() and not temporary.is_symlink():
                        shutil.rmtree(temporary)
                    else:
                        temporary.unlink()
            return
        body["status"] = "committing"
        _replace_json(path, body)
        try:
            for index, entry in enumerate(body["entries"]):
                state = self._classify_transaction_entry(entry)
                if state == "installed":
                    entry["phase"] = "installed"
                    _replace_json(path, body)
                    continue
                temporary = Path(entry["staged"])
                target = Path(entry["target"])
                displaced = Path(entry["displaced"])
                _ancestor_identities(target)
                if self._io_hook is not None:
                    self._io_hook(f"before_file_commit_{index}", target)
                if state == "pending" and entry["original"] != {"kind": "absent"}:
                    os.rename(target, displaced)
                    _fsync_directory(target.parent)
                    entry["phase"] = "displaced"
                    _replace_json(path, body)
                    if self._io_hook is not None:
                        self._io_hook(f"after_file_displace_{index}", target)
                os.rename(temporary, target)
                _fsync_directory(target.parent)
                if self._io_hook is not None:
                    self._io_hook(f"after_file_install_{index}", target)
                if self._classify_transaction_entry(entry) != "installed":
                    raise CutoverAdapterError("cutover_adapter_transaction_postcheck_failed")
                entry["phase"] = "installed"
                _replace_json(path, body)
        except Exception:
            self._abort_transaction(path, body)
            raise
        body["status"] = "committed"
        _replace_json(path, body)
        for temporary, _target, _kind in replacements:
            if temporary.exists() or temporary.is_symlink():
                if temporary.is_dir() and not temporary.is_symlink():
                    shutil.rmtree(temporary)
                else:
                    temporary.unlink()

    def _install_owner_commands(
        self,
        commands: Sequence[Sequence[str]],
        payloads: Mapping[str, Any],
        *,
        transaction_id: str,
    ) -> None:
        descriptor_by_path: dict[str, Mapping[str, Any]] = {}
        for descriptor in payloads.values():
            if descriptor.get("kind") == "regular_file":
                descriptor_by_path[str(descriptor["path"])] = descriptor
            else:
                for relative, entry in self._tree_expected_files(descriptor).items():
                    descriptor_by_path[str(Path(descriptor["path"]) / relative)] = entry
                install_plists = descriptor.get("install_plists")
                if isinstance(install_plists, Mapping):
                    descriptor_by_path.update(
                        {
                            str(source): entry
                            for source, entry in install_plists.items()
                            if isinstance(entry, Mapping)
                        }
                    )
        staged: list[tuple[Path, Path]] = []
        try:
            for command in commands:
                source_logical, target_logical, expected_sha = command[2:5]
                source = self._projection.physical(source_logical)
                target = self._projection.physical(target_logical)
                descriptor = descriptor_by_path.get(source_logical)
                if descriptor is None:
                    raise CutoverAdapterError(
                        "cutover_adapter_install_descriptor_missing"
                    )
                staged.append((
                    self._stage_owner_file(
                        source,
                        target,
                        expected_sha256=expected_sha,
                        expected_identity=descriptor.get("identity"),
                    ),
                    target,
                ))
        except BaseException:
            for temporary, _target in staged:
                try:
                    temporary.unlink()
                except FileNotFoundError:
                    pass
            raise
        self._commit_files_transaction(staged, transaction_id=transaction_id)

    def _install_tree(
        self,
        command: Sequence[str],
        descriptor: Mapping[str, Any],
        *,
        transaction_id: str,
        source_is_physical: bool = False,
    ) -> None:
        source = Path(command[2]) if source_is_physical else self._projection.physical(command[2])
        target = self._projection.physical(command[3])
        if command[4] != descriptor.get("binding_sha256"):
            raise CutoverAdapterError("cutover_adapter_tree_binding_drift")
        _ensure_owner_directory(target.parent)
        _ancestor_identities(target)
        temporary = target.parent / f".{target.name}.{secrets.token_hex(8)}.install"
        directories: dict[str, Mapping[str, Any]] = {}
        source_root_mode = f"{stat.S_IMODE(source.lstat().st_mode):04o}"
        expected_directories = descriptor.get("snapshot_directories")
        expected_root_mode = descriptor.get("snapshot_root_mode")
        copied_files = self._scan_tree(
            source,
            destination=temporary,
            expected=self._tree_expected_files(descriptor),
            expected_directories=(
                expected_directories
                if isinstance(expected_directories, Mapping)
                else None
            ),
            expected_root_mode=(
                expected_root_mode if isinstance(expected_root_mode, str) else None
            ),
            directories_out=directories,
            excluded=(
                frozenset({RUNTIME_STAGE_MANIFEST_NAME})
                if descriptor.get("kind") == "runtime_tree"
                and not source_is_physical
                else frozenset()
            ),
        )
        self._scan_tree(
            temporary,
            expected=copied_files,
            expected_directories=directories,
            expected_root_mode=source_root_mode,
        )
        self._commit_replacements_transaction(
            [(temporary, target, "tree")], transaction_id=transaction_id
        )

    def _snapshot_path_component(
        self, logical_path: str, component_root: Path
    ) -> Mapping[str, Any]:
        source = self._projection.physical(logical_path)
        body: dict[str, Any] = {
            "schema_version": ADAPTER_SNAPSHOT_COMPONENT_SCHEMA_VERSION,
            "declared_path": logical_path,
        }
        if not source.exists() and not source.is_symlink():
            body["kind"] = "absent"
        else:
            info = source.lstat()
            if stat.S_ISLNK(info.st_mode):
                raise CutoverAdapterError("cutover_adapter_snapshot_symlink_forbidden")
            if stat.S_ISREG(info.st_mode):
                stable = _read_stable_owner_file(source, io_hook=self._io_hook)
                payload = component_root / "payload"
                _write_new_file(payload, stable.raw, mode=stable.mode)
                body.update({
                    "kind": "file",
                    "sha256": stable.sha256,
                    "mode": f"{stable.mode:04o}",
                })
            elif stat.S_ISDIR(info.st_mode):
                payload = component_root / "payload"
                directories: dict[str, Mapping[str, Any]] = {}
                files = self._scan_tree(
                    source,
                    destination=payload,
                    directories_out=directories,
                )
                root_mode = f"{stat.S_IMODE(info.st_mode):04o}"
                closure = {
                    "root_mode": root_mode,
                    "directories": directories,
                    "files": files,
                }
                body.update({
                    "kind": "tree",
                    **closure,
                    "closure_sha256": _sha256_json(closure),
                })
            else:
                raise CutoverAdapterError("cutover_adapter_snapshot_special_forbidden")
        _write_new_file(component_root / "component.json", _canonical_json(body), mode=0o600)
        _fsync_directory(component_root)
        return body

    def _snapshot_plists_component(
        self, plan: Mapping[str, Any], component_root: Path
    ) -> Mapping[str, Any]:
        entries: dict[str, Any] = {}
        payload_root = component_root / "payload"
        payload_root.mkdir(mode=0o700)
        labels = tuple(
            candidate.removesuffix(".candidate.plist")
            for candidate in cutover.CANDIDATE_PLISTS
        )
        for label in labels:
            logical = str(cutover.CANONICAL_LAUNCH_AGENTS_ROOT / f"{label}.plist")
            source = self._projection.physical(logical)
            if not source.exists() and not source.is_symlink():
                entries[label] = {"declared_path": logical, "kind": "absent"}
                continue
            stable = _read_stable_owner_file(source, io_hook=self._io_hook)
            _write_new_file(payload_root / f"{label}.plist", stable.raw, mode=stable.mode)
            entries[label] = {
                "declared_path": logical,
                "kind": "file",
                "sha256": stable.sha256,
                "mode": f"{stable.mode:04o}",
            }
        body = {
            "schema_version": ADAPTER_SNAPSHOT_COMPONENT_SCHEMA_VERSION,
            "kind": "file_set",
            "entries": entries,
            "plan_sha256": _sha256_json(plan),
        }
        _write_new_file(component_root / "component.json", _canonical_json(body), mode=0o600)
        _fsync_directory(payload_root)
        _fsync_directory(component_root)
        return body

    def _snapshot_services_component(self, component_root: Path) -> Mapping[str, Any]:
        if self._services is None:
            raise CutoverAdapterError("cutover_adapter_service_controller_required")
        state = self._services.capture_state(cutover.SERVICE_LABELS)
        if not isinstance(state, Mapping):
            raise CutoverAdapterError("cutover_adapter_service_snapshot_invalid")
        body = {
            "schema_version": ADAPTER_SNAPSHOT_COMPONENT_SCHEMA_VERSION,
            "kind": "services",
            "state": dict(state),
        }
        _write_new_file(component_root / "component.json", _canonical_json(body), mode=0o600)
        _fsync_directory(component_root)
        return body

    def _snapshot(self, plan: Mapping[str, Any], before: str) -> Mapping[str, Any]:
        _ensure_owner_directory(self._snapshot_root)
        _ancestor_identities(self._snapshot_root / ".snapshot-boundary")
        snapshot_id = f"snap-{_sha256_json(plan)[:16]}-{before[:16]}"
        final = self._snapshot_root / snapshot_id
        if final.exists() or final.is_symlink():
            return self._existing_snapshot(final, plan=plan, before=before)
        temporary = self._snapshot_root / f".{snapshot_id}.{secrets.token_hex(8)}.tmp"
        temporary.mkdir(mode=0o700)
        paths = plan["payload_bindings"]
        logical = {
            "runtime": paths["runtime"]["canonical_path"],
            "workspace": paths["workspace"]["canonical_path"],
            "environment": paths["candidate_environment"]["canonical_path"],
            "feishu_sidecar": paths["feishu_sidecar"]["canonical_path"],
            "active_release_binding": paths["active_release_binding"]["canonical_path"],
        }
        component_bodies: dict[str, Mapping[str, Any]] = {}
        try:
            for name, path in logical.items():
                root = temporary / name
                root.mkdir(mode=0o700)
                component_bodies[name] = self._snapshot_path_component(path, root)
            plists_root = temporary / "plists"
            plists_root.mkdir(mode=0o700)
            component_bodies["plists"] = self._snapshot_plists_component(
                plan, plists_root
            )
            services_root = temporary / "services"
            services_root.mkdir(mode=0o700)
            component_bodies["services"] = self._snapshot_services_component(
                services_root
            )
            manifest = {
                "schema_version": cutover.SNAPSHOT_SCHEMA_VERSION,
                "snapshot_id": snapshot_id,
                "before_live_identity_sha256": before,
                "rollback_target_identity_sha256": plan["bindings"][
                    "rollback_live_identity_sha256"
                ],
                "plan_sha256": _sha256_json(plan),
                "components": {
                    name: _sha256_json(body)
                    for name, body in sorted(component_bodies.items())
                },
            }
            _write_new_file(
                temporary / "snapshot-manifest.json", _canonical_json(manifest), mode=0o600
            )
            _fsync_directory(temporary)
            if final.exists() or final.is_symlink():
                existing = self._existing_snapshot(final, plan=plan, before=before)
                shutil.rmtree(temporary)
                return existing
            if self._io_hook is not None:
                self._io_hook("before_snapshot_publish", final)
            os.rename(temporary, final)
            _fsync_directory(self._snapshot_root)
            if self._io_hook is not None:
                self._io_hook("after_snapshot_publish", final)
        except Exception:
            if temporary.exists():
                shutil.rmtree(temporary)
            raise
        return {
            "schema_version": cutover.SNAPSHOT_SCHEMA_VERSION,
            "snapshot_id": snapshot_id,
            "before_live_identity_sha256": before,
            "rollback_target_identity_sha256": plan["bindings"][
                "rollback_live_identity_sha256"
            ],
            "components": {
                name: {
                    "sha256": _sha256_json(body),
                    "restore_ref": str(final / name),
                }
                for name, body in component_bodies.items()
            },
            "old_runtime_retained": True,
        }

    def _existing_snapshot(
        self, final: Path, *, plan: Mapping[str, Any], before: str
    ) -> Mapping[str, Any]:
        _require_owner_directory(final, exact_mode=0o700)
        raw = _read_stable_owner_file(final / "snapshot-manifest.json")
        try:
            manifest = json.loads(raw.raw.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise CutoverAdapterError("cutover_adapter_snapshot_no_clobber") from exc
        names = {
            "runtime",
            "workspace",
            "environment",
            "plists",
            "services",
            "feishu_sidecar",
            "active_release_binding",
        }
        if (
            not isinstance(manifest, Mapping)
            or manifest.get("schema_version") != cutover.SNAPSHOT_SCHEMA_VERSION
            or manifest.get("snapshot_id") != final.name
            or manifest.get("before_live_identity_sha256") != before
            or manifest.get("rollback_target_identity_sha256")
            != plan["bindings"]["rollback_live_identity_sha256"]
            or manifest.get("plan_sha256") != _sha256_json(plan)
            or not isinstance(manifest.get("components"), Mapping)
            or set(manifest["components"]) != names
            or {child.name for child in final.iterdir()}
            != names | {"snapshot-manifest.json"}
        ):
            raise CutoverAdapterError("cutover_adapter_snapshot_no_clobber")
        components: dict[str, Mapping[str, str]] = {}
        for name in names:
            component = final / name
            _require_owner_directory(component, exact_mode=0o700)
            observed = _read_stable_owner_file(component / "component.json")
            try:
                body = json.loads(observed.raw.decode("utf-8"))
            except (UnicodeError, json.JSONDecodeError) as exc:
                raise CutoverAdapterError("cutover_adapter_snapshot_no_clobber") from exc
            if _sha256_json(body) != manifest["components"][name]:
                raise CutoverAdapterError("cutover_adapter_snapshot_no_clobber")
            components[name] = {
                "sha256": manifest["components"][name],
                "restore_ref": str(component),
            }
        return {
            "schema_version": cutover.SNAPSHOT_SCHEMA_VERSION,
            "snapshot_id": final.name,
            "before_live_identity_sha256": before,
            "rollback_target_identity_sha256": plan["bindings"][
                "rollback_live_identity_sha256"
            ],
            "components": components,
            "old_runtime_retained": True,
        }

    def _read_snapshot_component(
        self, descriptor: Mapping[str, Any]
    ) -> tuple[Path, Mapping[str, Any]]:
        restore_ref = Path(str(descriptor.get("restore_ref") or ""))
        if not restore_ref.is_absolute() or ".." in restore_ref.parts:
            raise CutoverAdapterError("cutover_adapter_restore_ref_invalid")
        try:
            restore_ref.relative_to(self._snapshot_root)
        except ValueError as exc:
            raise CutoverAdapterError("cutover_adapter_restore_ref_invalid") from exc
        _require_owner_directory(restore_ref, exact_mode=0o700)
        raw = _read_stable_owner_file(restore_ref / "component.json")
        try:
            body = json.loads(raw.raw.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise CutoverAdapterError("cutover_adapter_snapshot_component_invalid") from exc
        if (
            not isinstance(body, Mapping)
            or body.get("schema_version") != ADAPTER_SNAPSHOT_COMPONENT_SCHEMA_VERSION
            or _sha256_json(body) != descriptor.get("sha256")
        ):
            raise CutoverAdapterError("cutover_adapter_snapshot_component_drift")
        return restore_ref, body

    def _restore_path_component(
        self,
        restore_ref: Path,
        body: Mapping[str, Any],
        *,
        transaction_id: str,
    ) -> None:
        target = self._projection.physical(str(body.get("declared_path") or ""))
        kind = body.get("kind")
        if kind == "absent":
            if target.exists() or target.is_symlink():
                retained = target.parent / f".{target.name}.rollback-displaced.{transaction_id}"
                if retained.exists() or retained.is_symlink():
                    raise CutoverAdapterError("cutover_adapter_rollback_no_clobber")
                os.rename(target, retained)
                _fsync_directory(target.parent)
            return
        if kind == "file":
            staged = self._stage_owner_file(
                restore_ref / "payload",
                target,
                expected_sha256=_require_sha256(
                    body.get("sha256"), "cutover_adapter_snapshot_hash_invalid"
                ),
                expected_mode=_require_mode(
                    body.get("mode"), "cutover_adapter_snapshot_mode_invalid"
                ),
            )
            self._commit_files_transaction(
                [(staged, target)], transaction_id=transaction_id
            )
            return
        if kind == "tree":
            files = body.get("files")
            directories = body.get("directories")
            root_mode = body.get("root_mode")
            closure = {
                "root_mode": root_mode,
                "directories": directories,
                "files": files,
            }
            if (
                not isinstance(files, Mapping)
                or not isinstance(directories, Mapping)
                or not isinstance(root_mode, str)
                or body.get("closure_sha256") != _sha256_json(closure)
            ):
                raise CutoverAdapterError("cutover_adapter_snapshot_tree_invalid")
            command = [
                cutover.CUTOVER_ADAPTER_EXECUTABLE,
                "install-retained-tree",
                str(restore_ref / "payload"),
                str(body["declared_path"]),
                "0" * 64,
            ]
            descriptor = {
                "kind": "runtime_tree",
                "binding_sha256": "0" * 64,
                "files": files,
                "snapshot_directories": directories,
                "snapshot_root_mode": root_mode,
            }
            self._install_tree(
                command,
                descriptor,
                transaction_id=transaction_id,
                source_is_physical=True,
            )
            return
        raise CutoverAdapterError("cutover_adapter_snapshot_component_kind_invalid")

    def _restore_snapshot(
        self, snapshot: Mapping[str, Any], plan: Mapping[str, Any]
    ) -> None:
        components = snapshot.get("components")
        if not isinstance(components, Mapping) or set(components) != {
            "runtime",
            "workspace",
            "environment",
            "plists",
            "services",
            "feishu_sidecar",
            "active_release_binding",
        }:
            raise CutoverAdapterError("cutover_adapter_snapshot_shape_invalid")
        transaction_id = f"restore-{_sha256_json(plan)[:12]}"
        loaded = {
            name: self._read_snapshot_component(descriptor)
            for name, descriptor in components.items()
        }
        expected_paths = {
            "runtime": plan["payload_bindings"]["runtime"]["canonical_path"],
            "workspace": plan["payload_bindings"]["workspace"]["canonical_path"],
            "environment": plan["payload_bindings"]["candidate_environment"][
                "canonical_path"
            ],
            "feishu_sidecar": plan["payload_bindings"]["feishu_sidecar"][
                "canonical_path"
            ],
            "active_release_binding": plan["payload_bindings"][
                "active_release_binding"
            ]["canonical_path"],
        }
        for name, expected_path in expected_paths.items():
            if loaded[name][1].get("declared_path") != expected_path:
                raise CutoverAdapterError("cutover_adapter_snapshot_target_drift")
        plist_body = loaded["plists"][1]
        expected_plist_paths = {
            candidate.removesuffix(".candidate.plist"): str(
                cutover.CANONICAL_LAUNCH_AGENTS_ROOT
                / candidate.replace(".candidate.plist", ".plist")
            )
            for candidate in cutover.CANDIDATE_PLISTS
        }
        entries = plist_body.get("entries")
        if (
            plist_body.get("kind") != "file_set"
            or not isinstance(entries, Mapping)
            or set(entries) != set(expected_plist_paths)
            or any(
                entries[label].get("declared_path") != expected
                for label, expected in expected_plist_paths.items()
            )
        ):
            raise CutoverAdapterError("cutover_adapter_snapshot_plists_invalid")
        for name, (restore_ref, body) in loaded.items():
            kind = body.get("kind")
            if kind == "file":
                observed = _read_stable_owner_file(restore_ref / "payload")
                if (
                    observed.sha256 != body.get("sha256")
                    or observed.mode
                    != _require_mode(
                        body.get("mode"), "cutover_adapter_snapshot_mode_invalid"
                    )
                ):
                    raise CutoverAdapterError("cutover_adapter_snapshot_payload_drift")
            elif kind == "tree":
                files = body.get("files")
                directories = body.get("directories")
                root_mode = body.get("root_mode")
                closure = {
                    "root_mode": root_mode,
                    "directories": directories,
                    "files": files,
                }
                if (
                    not isinstance(files, Mapping)
                    or not isinstance(directories, Mapping)
                    or not isinstance(root_mode, str)
                    or body.get("closure_sha256") != _sha256_json(closure)
                ):
                    raise CutoverAdapterError("cutover_adapter_snapshot_tree_invalid")
                self._scan_tree(
                    restore_ref / "payload",
                    expected=files,
                    expected_directories=directories,
                    expected_root_mode=root_mode,
                )
            elif kind == "file_set":
                for label, entry in entries.items():
                    if entry.get("kind") == "file":
                        observed = _read_stable_owner_file(
                            restore_ref / "payload" / f"{label}.plist"
                        )
                        if (
                            observed.sha256 != entry.get("sha256")
                            or observed.mode
                            != _require_mode(
                                entry.get("mode"),
                                "cutover_adapter_snapshot_mode_invalid",
                            )
                        ):
                            raise CutoverAdapterError(
                                "cutover_adapter_snapshot_payload_drift"
                            )
                    elif entry.get("kind") != "absent":
                        raise CutoverAdapterError(
                            "cutover_adapter_snapshot_plists_invalid"
                        )
            elif kind not in {"absent", "services"}:
                raise CutoverAdapterError("cutover_adapter_snapshot_component_kind_invalid")
        for name in (
            "runtime",
            "workspace",
            "environment",
            "plists",
            "feishu_sidecar",
            "active_release_binding",
        ):
            restore_ref, body = loaded[name]
            if name == "plists":
                entries = body.get("entries")
                if body.get("kind") != "file_set" or not isinstance(entries, Mapping):
                    raise CutoverAdapterError("cutover_adapter_snapshot_plists_invalid")
                for label, entry in entries.items():
                    if entry.get("kind") == "file":
                        source = restore_ref / "payload" / f"{label}.plist"
                        target = self._projection.physical(entry["declared_path"])
                        staged = self._stage_owner_file(
                            source,
                            target,
                            expected_sha256=entry["sha256"],
                            expected_mode=_require_mode(
                                entry.get("mode"),
                                "cutover_adapter_snapshot_mode_invalid",
                            ),
                        )
                        self._commit_files_transaction(
                            [(staged, target)], transaction_id=f"{transaction_id}-{label}"
                        )
                    elif entry.get("kind") == "absent":
                        target = self._projection.physical(entry["declared_path"])
                        if target.exists() or target.is_symlink():
                            retained = target.parent / (
                                f".{target.name}.rollback-displaced.{transaction_id}"
                            )
                            if retained.exists() or retained.is_symlink():
                                raise CutoverAdapterError(
                                    "cutover_adapter_rollback_no_clobber"
                                )
                            os.rename(target, retained)
                    else:
                        raise CutoverAdapterError(
                            "cutover_adapter_snapshot_plists_invalid"
                        )
            else:
                self._restore_path_component(
                    restore_ref, body, transaction_id=f"{transaction_id}-{name}"
                )
        services_ref, services_body = loaded["services"]
        del services_ref
        if self._services is None or services_body.get("kind") != "services":
            raise CutoverAdapterError("cutover_adapter_service_controller_required")
        state = services_body.get("state")
        if not isinstance(state, Mapping):
            raise CutoverAdapterError("cutover_adapter_service_snapshot_invalid")
        self._services.restore_state(state)

    def _base_result(
        self,
        *,
        step: str,
        before: str,
        commands: Sequence[Sequence[str]],
        snapshot: Mapping[str, Any] | None = None,
        services: Mapping[str, Any] | None = None,
        evidence: Mapping[str, Any] | None = None,
        started_labels: Sequence[str] = (),
    ) -> Mapping[str, Any]:
        return {
            "schema_version": cutover.STEP_RESULT_SCHEMA_VERSION,
            "step": step,
            "before_identity_sha256": before,
            "after_identity_sha256": _sha256_json(self.observe_live_identity()),
            "commands": [list(command) for command in commands],
            "old_runtime_retained": True,
            "snapshot": snapshot,
            "services": dict(services or {}),
            "evidence": dict(evidence or {}),
            "started_labels": list(started_labels),
        }

    def execute_step(
        self,
        step: str,
        *,
        expected_identity_sha256: str,
        plan: Mapping[str, Any],
        planned_commands: Sequence[Sequence[str]],
        payload_descriptors: Mapping[str, Any],
        lease_fingerprint: str,
        lease_token: str,
    ) -> Mapping[str, Any]:
        self._require_authority(
            plan=plan,
            lease_fingerprint=lease_fingerprint,
            lease_token=lease_token,
            operation="forward",
        )
        self._assert_live_identity(expected_identity_sha256)
        commands = _validate_logical_commands(step, planned_commands, plan)
        self._validate_payloads(step, payload_descriptors, commands, plan)
        transaction_id = f"{_sha256_json(plan)[:12]}-{step}"
        evidence: Mapping[str, Any] = {}
        services: Mapping[str, Any] = {}
        started: Sequence[str] = ()
        snapshot = None
        if step == "snapshot_live":
            snapshot = self._snapshot(plan, expected_identity_sha256)
        elif step == "stop_writers":
            if self._services is None:
                raise CutoverAdapterError("cutover_adapter_service_controller_required")
            evidence = self._services.stop_writers(
                cutover.WRITER_LABELS,
                lease_fingerprint=lease_fingerprint,
                lease_token=lease_token,
            )
        elif step in {"install_feishu_sidecar", "install_environment", "install_plists"}:
            self._install_owner_commands(
                commands, payload_descriptors, transaction_id=transaction_id
            )
            installed = {
                "install_feishu_sidecar": plan["bindings"]["feishu_sidecar_sha256"],
                "install_environment": plan["bindings"]["candidate_env_sha256"],
                "install_plists": plan["bindings"]["candidate_plist_set_sha256"],
            }[step]
            evidence = {"installed_sha256": installed, "post_install_verified": True}
            if step == "install_environment":
                environment = plan["payload_bindings"]["candidate_environment"]
                active = plan["payload_bindings"]["active_release_binding"]
                evidence = {
                    **evidence,
                    "live_environment": {
                        "canonical_path": environment["canonical_path"],
                        "installed_sha256": environment["sha256"],
                        "mode": "0600",
                        "uid": os.geteuid(),
                        "nlink": 1,
                        "post_install_verified": True,
                    },
                    "active_release_binding": {
                        "canonical_path": active["canonical_path"],
                        "installed_sha256": active["sha256"],
                        "mode": "0600",
                        "uid": os.geteuid(),
                        "nlink": 1,
                        "post_install_verified": True,
                    },
                }
        elif step in {"install_runtime", "install_workspace"}:
            descriptor = next(iter(payload_descriptors.values()))
            self._install_tree(commands[0], descriptor, transaction_id=transaction_id)
            binding = (
                plan["bindings"]["runtime_content_sha256"]
                if step == "install_runtime"
                else plan["bindings"]["workspace_runtime_sha256"]
            )
            evidence = {"installed_sha256": binding, "post_install_verified": True}
        elif step == "start_gateway_aux":
            if self._runner is None:
                raise CutoverAdapterError("cutover_adapter_command_runner_required")
            for command in commands:
                result = self._runner.run(command)
                if tuple(command) != result.argv or result.returncode != 0:
                    raise CutoverAdapterError("cutover_adapter_launchctl_failed")
            started = plan["gateway_aux_start_order"]
        elif step == "verify_gateway_aux":
            if self._services is None:
                raise CutoverAdapterError("cutover_adapter_service_controller_required")
            services = self._services.verify(
                cutover.GATEWAY_AUX_LABELS,
                runtime_sha256=plan["bindings"]["runtime_content_sha256"],
            )
        else:
            raise CutoverAdapterError("cutover_adapter_step_unknown")
        return self._base_result(
            step=step,
            before=expected_identity_sha256,
            commands=commands,
            snapshot=snapshot,
            services=services,
            evidence=evidence,
            started_labels=started,
        )

    def rollback(
        self,
        *,
        snapshot: Mapping[str, Any],
        expected_identity_sha256: str,
        plan: Mapping[str, Any],
        planned_commands: Sequence[Sequence[str]],
        lease_fingerprint: str,
        lease_token: str,
    ) -> Mapping[str, Any]:
        self._require_authority(
            plan=plan,
            lease_fingerprint=lease_fingerprint,
            lease_token=lease_token,
            operation="rollback",
        )
        self._assert_live_identity(expected_identity_sha256)
        commands = _validate_logical_commands("rollback", planned_commands, plan)
        if (
            snapshot.get("schema_version") != cutover.SNAPSHOT_SCHEMA_VERSION
            or snapshot.get("rollback_target_identity_sha256")
            != plan["bindings"]["rollback_live_identity_sha256"]
        ):
            raise CutoverAdapterError("cutover_adapter_snapshot_binding_invalid")
        self._restore_snapshot(snapshot, plan)
        return self._base_result(
            step="rollback",
            before=expected_identity_sha256,
            commands=commands,
        )


def build_production_adapter(
    *,
    authority: AdapterMutationAuthority | None = None,
    identity_observer: Callable[[], Mapping[str, Any]] | None = None,
    snapshot_root: Path | None = None,
    runner: CommandRunner | None = None,
    service_controller: ServiceController | None = None,
    io_hook: Callable[[str, Path], None] | None = None,
) -> ProductionCutoverAdapter:
    """Build the live projection only when every real boundary is injected."""
    if (
        authority is None
        or authority.schema_version != ADAPTER_AUTHORITY_SCHEMA_VERSION
        or authority.mode not in {FORWARD_AUTHORITY_MODE, RECOVERY_AUTHORITY_MODE}
        or identity_observer is None
        or not callable(identity_observer)
        or snapshot_root is None
        or not snapshot_root.expanduser().is_absolute()
        or runner is None
        or service_controller is None
    ):
        raise CutoverAdapterError("cutover_adapter_production_dependencies_unavailable")
    for value in (
        authority.plan_sha256,
        authority.gate_binding_sha256,
        authority.authorization_receipt_sha256,
        authority.authorization_summary_sha256,
        authority.machine_identity_sha256,
        authority.rollback_target_identity_sha256,
        authority.forward_lease_fingerprint,
        authority.lease_fingerprint,
        authority.lease_token_sha256,
    ):
        _require_sha256(value, "cutover_adapter_production_authority_invalid")
    return ProductionCutoverAdapter(
        projection=PathProjection.production(explicit=True),
        identity_observer=identity_observer,
        snapshot_root=snapshot_root,
        runner=runner,
        service_controller=service_controller,
        authority=authority,
        io_hook=io_hook,
    )


def _cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="RCA cutover adapter safe surface")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("schema", help="print the non-mutating adapter contract")
    observe = commands.add_parser("observe", help="observe an explicit fake root only")
    observe.add_argument("--fake-root", type=Path, required=True)
    observe.add_argument("--identity-file", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _cli_parser().parse_args(argv)
    if args.command == "schema":
        print(json.dumps({
            "schema_version": ADAPTER_AUTHORITY_SCHEMA_VERSION,
            "cli_mutation_supported": False,
            "programmatic_mutation_requires_authority": True,
            "production_projection_is_ambient": False,
        }, sort_keys=True))
        return 0
    projection = PathProjection.fake(args.fake_root)
    identity_path = projection.physical(args.identity_file)
    observed = _read_stable_owner_file(identity_path)
    try:
        body = json.loads(observed.raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise CutoverAdapterError("cutover_adapter_identity_json_invalid") from exc
    if not isinstance(body, Mapping):
        raise CutoverAdapterError("cutover_adapter_live_identity_invalid")
    print(json.dumps(body, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
