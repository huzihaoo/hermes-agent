#!/usr/bin/env python3
"""Atomically install or roll back the fixed RCA workspace runtime bundle."""

from __future__ import annotations

import argparse
import ctypes
import ctypes.util
from datetime import datetime, timezone
import errno
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import sys
from typing import Any, Callable, Mapping, Sequence
import uuid

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from gateway import pnc_rca_workspace_runtime as workspace_runtime
from scripts import pnc_rca_release_transaction as base
from scripts import pnc_rca_steady_release_transaction as steady


PLAN_SCHEMA_VERSION = "pnc_rca_workspace_release_transaction_plan_v1"
RECEIPT_SCHEMA_VERSION = "pnc_rca_workspace_release_transaction_receipt_v1"
ROLLBACK_SCHEMA_VERSION = "pnc_rca_workspace_release_transaction_rollback_v1"
CLI_SCHEMA_VERSION = "pnc_rca_workspace_release_transaction_cli_v1"

IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
HEX40_RE = re.compile(r"^[0-9a-f]{40}$")
HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
FILE_OBSERVATION_FIELDS = frozenset(
    {"sha256", "mode", "size_bytes", "device", "inode", "mtime_ns", "ctime_ns"}
)
IDENTITY_FIELDS = frozenset(
    {"source_commit", "manifest_sha256", "closure_sha256", "file_sha256"}
)
MANIFEST_BINDING_FIELDS = frozenset({"path", "observation", "workspace_identity"})
PLAN_FIELDS = frozenset(
    {
        "schema_version",
        "cli_schema_version",
        "transaction_id",
        "planned_at",
        "candidate_root",
        "candidate_workspace_root",
        "candidate_manifest",
        "hermes_home",
        "control_db",
        "state_root",
        "evidence_root",
        "transaction_dir",
        "prepared_workspace_root",
        "target_workspace_root",
        "live_manifest",
        "activation_binding",
        "successor_identity",
        "predecessor_identity",
        "filesystem_device",
        "atomic_swap",
        "mutation_performed",
    }
)
RECEIPT_FIELDS = frozenset(
    {
        "schema_version",
        "transaction_id",
        "completed_at",
        "plan_path",
        "plan_raw_sha256",
        "activation_binding",
        "candidate_manifest",
        "live_manifest",
        "successor_identity",
        "predecessor_identity",
        "target_after",
        "prepared_after",
        "atomic_swap",
        "mutation_performed",
        "rollback_performed",
        "production_effects",
        "verification",
    }
)
ACTIVATION_FIELDS = steady.ACTIVATION_BINDING_FIELDS
_AT_FDCWD = -2
_RENAME_SWAP = 0x00000002


class WorkspaceReleaseTransactionError(RuntimeError):
    def __init__(self, code: str) -> None:
        self.code = str(code or "pnc_workspace_release_transaction_invalid")[:160]
        super().__init__(self.code)


def _fail(code: str, exc: BaseException | None = None) -> None:
    error = WorkspaceReleaseTransactionError(code)
    if exc is None:
        raise error
    raise error from exc


def _effects() -> dict[str, bool]:
    return {
        "database_mutation": False,
        "task_submission": False,
        "kafka_consume": False,
        "feishu_write": False,
        "resident_restart": False,
    }


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _valid_timestamp(value: Any) -> bool:
    if not isinstance(value, str) or not value:
        return False
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return False
    return parsed.tzinfo is not None


def _identity(value: workspace_runtime.WorkspaceRuntimeIdentity) -> dict[str, Any]:
    return {
        "source_commit": value.source_commit,
        "manifest_sha256": value.manifest_sha256,
        "closure_sha256": value.closure_sha256,
        "file_sha256": dict(sorted(value.file_sha256.items())),
    }


def _valid_identity(value: Any) -> bool:
    if not isinstance(value, Mapping) or set(value) != IDENTITY_FIELDS:
        return False
    files = value.get("file_sha256")
    return (
        isinstance(value.get("source_commit"), str)
        and HEX40_RE.fullmatch(value["source_commit"]) is not None
        and isinstance(value.get("manifest_sha256"), str)
        and HEX64_RE.fullmatch(value["manifest_sha256"]) is not None
        and isinstance(value.get("closure_sha256"), str)
        and HEX64_RE.fullmatch(value["closure_sha256"]) is not None
        and isinstance(files, Mapping)
        and set(files) == set(workspace_runtime.WORKSPACE_RUNTIME_FILES)
        and all(
            isinstance(files[path], str) and HEX64_RE.fullmatch(files[path]) is not None
            for path in workspace_runtime.WORKSPACE_RUNTIME_FILES
        )
    )


def _valid_file_observation(value: Any) -> bool:
    return (
        isinstance(value, Mapping)
        and set(value) == FILE_OBSERVATION_FIELDS
        and isinstance(value.get("sha256"), str)
        and HEX64_RE.fullmatch(value["sha256"]) is not None
        and value.get("mode") == "0600"
        and all(
            isinstance(value.get(field), int) and not isinstance(value.get(field), bool)
            for field in ("size_bytes", "device", "inode", "mtime_ns", "ctime_ns")
        )
        and value["size_bytes"] > 0
    )


def _valid_manifest_binding(value: Any, *, expected_path: Path) -> bool:
    return (
        isinstance(value, Mapping)
        and set(value) == MANIFEST_BINDING_FIELDS
        and Path(str(value.get("path") or "")) == expected_path
        and _valid_file_observation(value.get("observation"))
        and _valid_identity(value.get("workspace_identity"))
    )


def _workspace_manifest_binding(
    path: Path,
    *,
    expected_identity: Mapping[str, Any],
    code: str,
) -> dict[str, Any]:
    try:
        raw, observation = base._read_file(path, code=code, required_mode=0o600)
        manifest = base._json(raw, code=code)
        gateway_binding = manifest.get("gateway_release_binding")
        branch_bindings = manifest.get("production_branch_bindings")
        branch_workspace = (
            branch_bindings.get("workspace_runtime")
            if isinstance(branch_bindings, Mapping)
            else None
        )
        if not isinstance(gateway_binding, Mapping) or not isinstance(
            branch_workspace, Mapping
        ):
            _fail(code)
        commit = str(branch_workspace.get("commit") or "").strip()
        gateway_commit = str(
            gateway_binding.get("workspace_runtime_source_commit") or ""
        ).strip()
        manifest_sha256 = str(
            gateway_binding.get("workspace_runtime_manifest_sha256") or ""
        ).strip()
        closure_sha256 = str(
            gateway_binding.get("workspace_runtime_closure_sha256") or ""
        ).strip()
        if (
            commit != expected_identity.get("source_commit")
            or gateway_commit != expected_identity.get("source_commit")
            or manifest_sha256 != expected_identity.get("manifest_sha256")
            or closure_sha256 != expected_identity.get("closure_sha256")
        ):
            _fail(code)
        return {
            "path": str(path),
            "observation": dict(observation),
            "workspace_identity": dict(expected_identity),
        }
    except WorkspaceReleaseTransactionError:
        raise
    except (base.ReleaseTransactionError, KeyError, TypeError, ValueError) as exc:
        _fail(code, exc)


def _activation_binding(control_db: Path) -> dict[str, Any]:
    try:
        return steady._read_activation_binding(control_db)
    except (steady.SteadyReleaseTransactionError, base.ReleaseTransactionError) as exc:
        _fail("pnc_workspace_release_transaction_activation_invalid", exc)


def _validate_control_db(control_db: Path, *, state_root: Path) -> Path:
    if control_db != state_root / "control.sqlite3":
        _fail("pnc_workspace_release_transaction_control_db_invalid")
    try:
        return steady._validate_db_path(control_db)
    except steady.SteadyReleaseTransactionError as exc:
        _fail("pnc_workspace_release_transaction_control_db_invalid", exc)


def _validate_staged(path: Path, *, code: str) -> dict[str, Any]:
    try:
        return _identity(workspace_runtime.validate_staged_workspace_runtime(path))
    except workspace_runtime.WorkspaceRuntimeError as exc:
        _fail(code, exc)


def _validate_live(hermes_home: Path, *, code: str) -> dict[str, Any]:
    try:
        return _identity(workspace_runtime.validate_workspace_runtime(hermes_home=hermes_home))
    except workspace_runtime.WorkspaceRuntimeError as exc:
        _fail(code, exc)


def _sync_directory(path: Path) -> None:
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _sync_bundle(root: Path) -> None:
    for relative in (*workspace_runtime.WORKSPACE_RUNTIME_FILES, "manifest.json"):
        descriptor = os.open(
            root / relative,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    _sync_directory(root / "bin")
    _sync_directory(root)


def _copy_candidate(source: Path, destination: Path) -> None:
    try:
        destination.lstat()
    except FileNotFoundError:
        pass
    else:
        _fail("pnc_workspace_release_transaction_prepared_exists")
    try:
        shutil.copytree(source, destination, symlinks=True, copy_function=shutil.copy2)
        _sync_bundle(destination)
        _sync_directory(destination.parent)
    except WorkspaceReleaseTransactionError:
        raise
    except OSError as exc:
        _fail("pnc_workspace_release_transaction_prepare_failed", exc)


def _rename_swap(left: Path, right: Path) -> None:
    """Exchange two existing directory entries without an overwrite window."""
    if sys.platform != "darwin":
        _fail("pnc_workspace_release_transaction_atomic_swap_unsupported")
    library = ctypes.util.find_library("System")
    if not library:
        _fail("pnc_workspace_release_transaction_atomic_swap_unsupported")
    try:
        system = ctypes.CDLL(library, use_errno=True)
        renameatx_np = system.renameatx_np
    except (OSError, AttributeError) as exc:
        _fail("pnc_workspace_release_transaction_atomic_swap_unsupported", exc)
    renameatx_np.argtypes = (
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    )
    renameatx_np.restype = ctypes.c_int
    ctypes.set_errno(0)
    result = renameatx_np(
        _AT_FDCWD,
        os.fsencode(left),
        _AT_FDCWD,
        os.fsencode(right),
        _RENAME_SWAP,
    )
    if result != 0:
        observed_errno = ctypes.get_errno() or errno.EIO
        _fail(
            "pnc_workspace_release_transaction_atomic_swap_failed",
            OSError(observed_errno, os.strerror(observed_errno)),
        )


def _swap(
    left: Path,
    right: Path,
    *,
    swap_func: Callable[[Path, Path], None],
) -> None:
    swap_func(left, right)


def _sync_swap_parents(left: Path, right: Path) -> None:
    _sync_directory(left.parent)
    if right.parent != left.parent:
        _sync_directory(right.parent)


def _atomic_swap_descriptor() -> dict[str, Any]:
    return {
        "platform": "darwin",
        "primitive": "renameatx_np",
        "flag": "RENAME_SWAP",
        "no_overwrite_window": True,
    }


def _valid_atomic_swap(value: Any) -> bool:
    return value == _atomic_swap_descriptor()


def _paths_overlap(left: Path, right: Path) -> bool:
    return left == right or left in right.parents or right in left.parents


def _read_plan(path: Path) -> tuple[bytes, dict[str, Any]]:
    try:
        raw, _observation = base._read_file(
            path,
            code="pnc_workspace_release_transaction_plan_invalid",
            required_mode=0o600,
        )
        value = base._json(raw, code="pnc_workspace_release_transaction_plan_invalid")
    except base.ReleaseTransactionError as exc:
        _fail("pnc_workspace_release_transaction_plan_invalid", exc)
    if raw != base._pretty(value):
        _fail("pnc_workspace_release_transaction_plan_invalid")
    _validate_plan(value)
    if path != Path(value["transaction_dir"]) / "plan.json":
        _fail("pnc_workspace_release_transaction_plan_invalid")
    return raw, value


def _validate_plan(plan: Mapping[str, Any]) -> None:
    if (
        set(plan) != PLAN_FIELDS
        or plan.get("schema_version") != PLAN_SCHEMA_VERSION
        or plan.get("cli_schema_version") != CLI_SCHEMA_VERSION
        or plan.get("mutation_performed") is not False
        or IDENTIFIER_RE.fullmatch(str(plan.get("transaction_id") or "")) is None
        or not _valid_timestamp(plan.get("planned_at"))
        or not isinstance(plan.get("filesystem_device"), int)
        or isinstance(plan.get("filesystem_device"), bool)
        or plan["filesystem_device"] < 0
        or not _valid_atomic_swap(plan.get("atomic_swap"))
        or not _valid_identity(plan.get("successor_identity"))
        or not _valid_identity(plan.get("predecessor_identity"))
    ):
        _fail("pnc_workspace_release_transaction_plan_invalid")

    paths = {
        name: Path(str(plan[name]))
        for name in (
            "candidate_root",
            "candidate_workspace_root",
            "hermes_home",
            "control_db",
            "state_root",
            "evidence_root",
            "transaction_dir",
            "prepared_workspace_root",
            "target_workspace_root",
        )
    }
    if any(not path.is_absolute() or path.absolute() != path for path in paths.values()):
        _fail("pnc_workspace_release_transaction_plan_invalid")
    transaction_id = str(plan["transaction_id"])
    expected_state = paths["hermes_home"] / "runtime/pnc_agent/feishu_issue_kafka_rca"
    if (
        paths["candidate_workspace_root"] != paths["candidate_root"] / "workspace-runtime"
        or paths["control_db"] != expected_state / "control.sqlite3"
        or paths["state_root"] != expected_state
        or paths["transaction_dir"] != paths["evidence_root"] / transaction_id
        or paths["prepared_workspace_root"]
        != paths["transaction_dir"] / "prepared/workspace-runtime"
        or paths["target_workspace_root"]
        != workspace_runtime.canonical_workspace_runtime_root(paths["hermes_home"])
        or _paths_overlap(
            paths["target_workspace_root"], paths["candidate_workspace_root"]
        )
        or _paths_overlap(
            paths["target_workspace_root"], paths["prepared_workspace_root"]
        )
        or not _valid_manifest_binding(
            plan.get("candidate_manifest"),
            expected_path=paths["candidate_root"] / "LIVE_MANIFEST.json",
        )
        or not _valid_manifest_binding(
            plan.get("live_manifest"),
            expected_path=paths["hermes_home"] / "runtime/LIVE_MANIFEST.json",
        )
        or plan["candidate_manifest"]["workspace_identity"]
        != plan["successor_identity"]
        or plan["live_manifest"]["workspace_identity"]
        != plan["predecessor_identity"]
    ):
        _fail("pnc_workspace_release_transaction_plan_invalid")
    activation = plan.get("activation_binding")
    if (
        not isinstance(activation, Mapping)
        or set(activation) != ACTIVATION_FIELDS
        or activation.get("state") != "aborted"
        or IDENTIFIER_RE.fullmatch(str(activation.get("epoch_id") or "")) is None
        or HEX64_RE.fullmatch(str(activation.get("binding_fingerprint") or ""))
        is None
        or not isinstance(activation.get("transition_audit_id"), int)
        or isinstance(activation.get("transition_audit_id"), bool)
        or activation["transition_audit_id"] < 1
        or not _valid_timestamp(activation.get("transitioned_at"))
    ):
        _fail("pnc_workspace_release_transaction_plan_invalid")


def build_plan(
    *,
    candidate_root: Path,
    hermes_home: Path,
    control_db: Path,
    evidence_root: Path,
    transaction_id: str | None = None,
) -> tuple[dict[str, Any], Path]:
    candidate_root = base._directory(candidate_root)
    hermes_home = base._directory(hermes_home)
    state_root = base._directory(
        hermes_home / "runtime/pnc_agent/feishu_issue_kafka_rca"
    )
    control_db = _validate_control_db(control_db, state_root=state_root)
    evidence_root = base._directory(evidence_root, create=True)
    candidate_workspace_root = candidate_root / "workspace-runtime"
    target_workspace_root = workspace_runtime.canonical_workspace_runtime_root(hermes_home)

    successor_identity = _validate_staged(
        candidate_workspace_root,
        code="pnc_workspace_release_transaction_candidate_invalid",
    )
    predecessor_identity = _validate_live(
        hermes_home,
        code="pnc_workspace_release_transaction_predecessor_invalid",
    )
    candidate_manifest = _workspace_manifest_binding(
        candidate_root / "LIVE_MANIFEST.json",
        expected_identity=successor_identity,
        code="pnc_workspace_release_transaction_candidate_manifest_invalid",
    )
    live_manifest = _workspace_manifest_binding(
        hermes_home / "runtime/LIVE_MANIFEST.json",
        expected_identity=predecessor_identity,
        code="pnc_workspace_release_transaction_live_manifest_invalid",
    )
    activation_binding = _activation_binding(control_db)

    target_device = target_workspace_root.lstat().st_dev
    if evidence_root.lstat().st_dev != target_device:
        _fail("pnc_workspace_release_transaction_filesystem_mismatch")

    selected_id = transaction_id or (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        + "-"
        + uuid.uuid4().hex[:12]
    )
    if IDENTIFIER_RE.fullmatch(selected_id) is None:
        _fail("pnc_workspace_release_transaction_id_invalid")
    transaction_dir = evidence_root / selected_id
    try:
        transaction_dir.mkdir(mode=0o700)
        prepared_parent = transaction_dir / "prepared"
        prepared_parent.mkdir(mode=0o700)
    except FileExistsError as exc:
        _fail("pnc_workspace_release_transaction_output_exists", exc)
    except OSError as exc:
        _fail("pnc_workspace_release_transaction_output_invalid", exc)
    prepared_workspace_root = prepared_parent / "workspace-runtime"
    _copy_candidate(candidate_workspace_root, prepared_workspace_root)
    prepared_identity = _validate_staged(
        prepared_workspace_root,
        code="pnc_workspace_release_transaction_prepared_invalid",
    )
    if prepared_identity != successor_identity:
        _fail("pnc_workspace_release_transaction_prepared_changed")
    if _validate_staged(
        candidate_workspace_root,
        code="pnc_workspace_release_transaction_candidate_invalid",
    ) != successor_identity:
        _fail("pnc_workspace_release_transaction_candidate_changed")
    if _validate_live(
        hermes_home,
        code="pnc_workspace_release_transaction_predecessor_invalid",
    ) != predecessor_identity:
        _fail("pnc_workspace_release_transaction_predecessor_changed")
    if _workspace_manifest_binding(
        candidate_root / "LIVE_MANIFEST.json",
        expected_identity=successor_identity,
        code="pnc_workspace_release_transaction_candidate_manifest_invalid",
    ) != candidate_manifest:
        _fail("pnc_workspace_release_transaction_candidate_manifest_changed")
    if _workspace_manifest_binding(
        hermes_home / "runtime/LIVE_MANIFEST.json",
        expected_identity=predecessor_identity,
        code="pnc_workspace_release_transaction_live_manifest_invalid",
    ) != live_manifest:
        _fail("pnc_workspace_release_transaction_live_manifest_changed")
    if _activation_binding(control_db) != activation_binding:
        _fail("pnc_workspace_release_transaction_activation_changed")

    prepared_device = prepared_workspace_root.lstat().st_dev
    if target_device != prepared_device:
        _fail("pnc_workspace_release_transaction_filesystem_mismatch")
    plan = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "cli_schema_version": CLI_SCHEMA_VERSION,
        "transaction_id": selected_id,
        "planned_at": _now(),
        "candidate_root": str(candidate_root),
        "candidate_workspace_root": str(candidate_workspace_root),
        "candidate_manifest": candidate_manifest,
        "hermes_home": str(hermes_home),
        "control_db": str(control_db),
        "state_root": str(state_root),
        "evidence_root": str(evidence_root),
        "transaction_dir": str(transaction_dir),
        "prepared_workspace_root": str(prepared_workspace_root),
        "target_workspace_root": str(target_workspace_root),
        "live_manifest": live_manifest,
        "activation_binding": activation_binding,
        "successor_identity": successor_identity,
        "predecessor_identity": predecessor_identity,
        "filesystem_device": target_device,
        "atomic_swap": _atomic_swap_descriptor(),
        "mutation_performed": False,
    }
    _validate_plan(plan)
    plan_path = transaction_dir / "plan.json"
    try:
        base._write_new(plan_path, base._pretty(plan), mode=0o600)
    except base.ReleaseTransactionError as exc:
        _fail("pnc_workspace_release_transaction_output_invalid", exc)
    return plan, plan_path


def _revalidate_plan_inputs(plan: Mapping[str, Any]) -> None:
    candidate_root = Path(plan["candidate_root"])
    hermes_home = Path(plan["hermes_home"])
    if _workspace_manifest_binding(
        candidate_root / "LIVE_MANIFEST.json",
        expected_identity=plan["successor_identity"],
        code="pnc_workspace_release_transaction_candidate_manifest_invalid",
    ) != plan["candidate_manifest"]:
        _fail("pnc_workspace_release_transaction_candidate_manifest_changed")
    if _workspace_manifest_binding(
        hermes_home / "runtime/LIVE_MANIFEST.json",
        expected_identity=plan["predecessor_identity"],
        code="pnc_workspace_release_transaction_live_manifest_invalid",
    ) != plan["live_manifest"]:
        _fail("pnc_workspace_release_transaction_live_manifest_changed")
    if _activation_binding(Path(plan["control_db"])) != plan["activation_binding"]:
        _fail("pnc_workspace_release_transaction_activation_changed")


def _validate_positions(
    plan: Mapping[str, Any],
    *,
    target_identity: Mapping[str, Any],
    prepared_identity: Mapping[str, Any],
) -> None:
    hermes_home = Path(plan["hermes_home"])
    target = Path(plan["target_workspace_root"])
    prepared = Path(plan["prepared_workspace_root"])
    if (
        target.lstat().st_dev != plan["filesystem_device"]
        or prepared.lstat().st_dev != plan["filesystem_device"]
    ):
        _fail("pnc_workspace_release_transaction_filesystem_changed")
    if _validate_live(
        hermes_home,
        code="pnc_workspace_release_transaction_target_invalid",
    ) != dict(target_identity):
        _fail("pnc_workspace_release_transaction_target_changed")
    if _validate_staged(
        prepared,
        code="pnc_workspace_release_transaction_prepared_invalid",
    ) != dict(prepared_identity):
        _fail("pnc_workspace_release_transaction_prepared_changed")


def _swap_back_if_exact(
    plan: Mapping[str, Any],
    *,
    target_before: Mapping[str, Any],
    prepared_before: Mapping[str, Any],
    target_after: Mapping[str, Any],
    prepared_after: Mapping[str, Any],
    swap_func: Callable[[Path, Path], None],
) -> bool:
    try:
        _validate_positions(
            plan,
            target_identity=target_before,
            prepared_identity=prepared_before,
        )
        _swap(
            Path(plan["target_workspace_root"]),
            Path(plan["prepared_workspace_root"]),
            swap_func=swap_func,
        )
        _sync_swap_parents(
            Path(plan["target_workspace_root"]),
            Path(plan["prepared_workspace_root"]),
        )
        _validate_positions(
            plan,
            target_identity=target_after,
            prepared_identity=prepared_after,
        )
        return True
    except (WorkspaceReleaseTransactionError, OSError):
        return False


def apply_plan(
    plan: Mapping[str, Any],
    *,
    plan_path: Path,
    swap_func: Callable[[Path, Path], None] | None = None,
) -> dict[str, Any]:
    _validate_plan(plan)
    plan_raw, on_disk_plan = _read_plan(plan_path)
    if dict(plan) != on_disk_plan:
        _fail("pnc_workspace_release_transaction_plan_changed")
    plan = on_disk_plan
    selected_swap = swap_func or _rename_swap
    state_root = base._directory(Path(plan["state_root"]))
    lock, lock_fd = steady._acquire_lock(state_root, str(plan["transaction_id"]))
    swapped = False
    try:
        locked_raw, locked_plan = _read_plan(plan_path)
        if locked_raw != plan_raw or locked_plan != plan:
            _fail("pnc_workspace_release_transaction_plan_changed")
        _revalidate_plan_inputs(plan)
        _validate_positions(
            plan,
            target_identity=plan["predecessor_identity"],
            prepared_identity=plan["successor_identity"],
        )
        _swap(
            Path(plan["target_workspace_root"]),
            Path(plan["prepared_workspace_root"]),
            swap_func=selected_swap,
        )
        swapped = True
        _sync_swap_parents(
            Path(plan["target_workspace_root"]),
            Path(plan["prepared_workspace_root"]),
        )
        _validate_positions(
            plan,
            target_identity=plan["successor_identity"],
            prepared_identity=plan["predecessor_identity"],
        )
        if _activation_binding(Path(plan["control_db"])) != plan["activation_binding"]:
            _fail("pnc_workspace_release_transaction_activation_changed")
        receipt = {
            "schema_version": RECEIPT_SCHEMA_VERSION,
            "transaction_id": plan["transaction_id"],
            "completed_at": _now(),
            "plan_path": str(plan_path),
            "plan_raw_sha256": hashlib.sha256(plan_raw).hexdigest(),
            "activation_binding": dict(plan["activation_binding"]),
            "candidate_manifest": dict(plan["candidate_manifest"]),
            "live_manifest": dict(plan["live_manifest"]),
            "successor_identity": dict(plan["successor_identity"]),
            "predecessor_identity": dict(plan["predecessor_identity"]),
            "target_after": dict(plan["successor_identity"]),
            "prepared_after": dict(plan["predecessor_identity"]),
            "atomic_swap": dict(plan["atomic_swap"]),
            "mutation_performed": True,
            "rollback_performed": False,
            "production_effects": _effects(),
            "verification": "pass",
        }
        receipt_path = Path(plan["transaction_dir"]) / "receipt.json"
        try:
            base._write_new(receipt_path, base._pretty(receipt), mode=0o600)
        except base.ReleaseTransactionError as exc:
            _fail("pnc_workspace_release_transaction_receipt_write_failed", exc)
        receipt["receipt_path"] = str(receipt_path)
        receipt["receipt_raw_sha256"] = hashlib.sha256(receipt_path.read_bytes()).hexdigest()
        return receipt
    except BaseException as original:
        if swapped:
            restored = _swap_back_if_exact(
                plan,
                target_before=plan["successor_identity"],
                prepared_before=plan["predecessor_identity"],
                target_after=plan["predecessor_identity"],
                prepared_after=plan["successor_identity"],
                swap_func=selected_swap,
            )
            automatic = {
                "schema_version": ROLLBACK_SCHEMA_VERSION,
                "transaction_id": plan["transaction_id"],
                "rolled_back_at": _now(),
                "restored_to_pre_transaction": restored,
                "original_error": getattr(
                    original, "code", "pnc_workspace_release_transaction_apply_failed"
                ),
                "production_effects": _effects(),
            }
            try:
                base._write_new(
                    Path(plan["transaction_dir"]) / "automatic-rollback.json",
                    base._pretty(automatic),
                    mode=0o600,
                )
            except base.ReleaseTransactionError as exc:
                if restored:
                    _fail("pnc_workspace_release_transaction_rollback_receipt_failed", exc)
            if not restored:
                _fail("pnc_workspace_release_transaction_automatic_rollback_incomplete", original)
        raise
    finally:
        steady._release_lock(lock, lock_fd)


def _validate_receipt(value: Mapping[str, Any]) -> None:
    if (
        set(value) != RECEIPT_FIELDS
        or value.get("schema_version") != RECEIPT_SCHEMA_VERSION
        or IDENTIFIER_RE.fullmatch(str(value.get("transaction_id") or "")) is None
        or not _valid_timestamp(value.get("completed_at"))
        or not isinstance(value.get("plan_path"), str)
        or not Path(value["plan_path"]).is_absolute()
        or HEX64_RE.fullmatch(str(value.get("plan_raw_sha256") or "")) is None
        or value.get("mutation_performed") is not True
        or value.get("rollback_performed") is not False
        or value.get("production_effects") != _effects()
        or value.get("verification") != "pass"
        or not _valid_atomic_swap(value.get("atomic_swap"))
        or not _valid_identity(value.get("successor_identity"))
        or not _valid_identity(value.get("predecessor_identity"))
        or value.get("target_after") != value.get("successor_identity")
        or value.get("prepared_after") != value.get("predecessor_identity")
    ):
        _fail("pnc_workspace_release_transaction_receipt_invalid")
    activation = value.get("activation_binding")
    if not isinstance(activation, Mapping) or set(activation) != ACTIVATION_FIELDS:
        _fail("pnc_workspace_release_transaction_receipt_invalid")


def _read_receipt(path: Path) -> tuple[bytes, dict[str, Any]]:
    try:
        raw, _observation = base._read_file(
            path,
            code="pnc_workspace_release_transaction_receipt_invalid",
            required_mode=0o600,
        )
        value = base._json(raw, code="pnc_workspace_release_transaction_receipt_invalid")
    except base.ReleaseTransactionError as exc:
        _fail("pnc_workspace_release_transaction_receipt_invalid", exc)
    if raw != base._pretty(value):
        _fail("pnc_workspace_release_transaction_receipt_invalid")
    _validate_receipt(value)
    return raw, value


def rollback_transaction(
    receipt_path: Path,
    *,
    output_path: Path,
    swap_func: Callable[[Path, Path], None] | None = None,
) -> dict[str, Any]:
    if not output_path.is_absolute() or output_path.absolute() != output_path:
        _fail("pnc_workspace_release_transaction_rollback_output_invalid")
    try:
        output_path.lstat()
    except FileNotFoundError:
        pass
    else:
        _fail("pnc_workspace_release_transaction_rollback_output_exists")
    receipt_raw, receipt = _read_receipt(receipt_path)
    plan_raw, plan = _read_plan(Path(receipt["plan_path"]))
    if (
        receipt_path != Path(plan["transaction_dir"]) / "receipt.json"
        or receipt["transaction_id"] != plan["transaction_id"]
        or receipt["plan_raw_sha256"] != hashlib.sha256(plan_raw).hexdigest()
        or receipt["activation_binding"] != plan["activation_binding"]
        or receipt["candidate_manifest"] != plan["candidate_manifest"]
        or receipt["live_manifest"] != plan["live_manifest"]
        or receipt["successor_identity"] != plan["successor_identity"]
        or receipt["predecessor_identity"] != plan["predecessor_identity"]
        or receipt["atomic_swap"] != plan["atomic_swap"]
    ):
        _fail("pnc_workspace_release_transaction_receipt_binding_invalid")
    base._directory(output_path.parent, create=True)

    selected_swap = swap_func or _rename_swap
    state_root = base._directory(Path(plan["state_root"]))
    lock, lock_fd = steady._acquire_lock(state_root, str(plan["transaction_id"]))
    swapped = False
    try:
        locked_receipt_raw, locked_receipt = _read_receipt(receipt_path)
        locked_plan_raw, locked_plan = _read_plan(Path(receipt["plan_path"]))
        if (
            locked_receipt_raw != receipt_raw
            or locked_receipt != receipt
            or locked_plan_raw != plan_raw
            or locked_plan != plan
        ):
            _fail("pnc_workspace_release_transaction_receipt_binding_invalid")
        if _activation_binding(Path(plan["control_db"])) != plan["activation_binding"]:
            _fail("pnc_workspace_release_transaction_activation_changed")
        _validate_positions(
            plan,
            target_identity=receipt["target_after"],
            prepared_identity=receipt["prepared_after"],
        )
        _swap(
            Path(plan["target_workspace_root"]),
            Path(plan["prepared_workspace_root"]),
            swap_func=selected_swap,
        )
        swapped = True
        _sync_swap_parents(
            Path(plan["target_workspace_root"]),
            Path(plan["prepared_workspace_root"]),
        )
        _validate_positions(
            plan,
            target_identity=plan["predecessor_identity"],
            prepared_identity=plan["successor_identity"],
        )
        result = {
            "schema_version": ROLLBACK_SCHEMA_VERSION,
            "transaction_id": plan["transaction_id"],
            "rolled_back_at": _now(),
            "receipt_path": str(receipt_path),
            "receipt_raw_sha256": hashlib.sha256(receipt_raw).hexdigest(),
            "restored_to_pre_transaction": True,
            "target_after": dict(plan["predecessor_identity"]),
            "prepared_after": dict(plan["successor_identity"]),
            "mutation_performed": True,
            "production_effects": _effects(),
            "verification": "pass",
        }
        try:
            base._write_new(output_path, base._pretty(result), mode=0o600)
        except base.ReleaseTransactionError as exc:
            _fail("pnc_workspace_release_transaction_rollback_output_invalid", exc)
        return result
    except BaseException as original:
        if swapped:
            restored = _swap_back_if_exact(
                plan,
                target_before=plan["predecessor_identity"],
                prepared_before=plan["successor_identity"],
                target_after=plan["successor_identity"],
                prepared_after=plan["predecessor_identity"],
                swap_func=selected_swap,
            )
            if not restored:
                _fail("pnc_workspace_release_transaction_manual_rollback_incomplete", original)
        raise
    finally:
        steady._release_lock(lock, lock_fd)


def _arguments(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    plan = commands.add_parser("plan")
    plan.add_argument("--candidate-root", type=Path, required=True)
    plan.add_argument("--hermes-home", type=Path, required=True)
    plan.add_argument("--control-db", type=Path, required=True)
    plan.add_argument("--evidence-root", type=Path, required=True)
    plan.add_argument("--transaction-id")
    apply = commands.add_parser("apply")
    apply.add_argument("--plan", type=Path, required=True)
    rollback = commands.add_parser("rollback")
    rollback.add_argument("--receipt", type=Path, required=True)
    rollback.add_argument("--output", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = _arguments(argv)
        if args.command == "plan":
            plan, plan_path = build_plan(
                candidate_root=args.candidate_root.expanduser().absolute(),
                hermes_home=args.hermes_home.expanduser().absolute(),
                control_db=args.control_db.expanduser().absolute(),
                evidence_root=args.evidence_root.expanduser().absolute(),
                transaction_id=args.transaction_id,
            )
            print(json.dumps({"ok": True, "plan_path": str(plan_path), **plan}, sort_keys=True))
            return 0
        if args.command == "apply":
            plan_path = args.plan.expanduser().absolute()
            _raw, plan = _read_plan(plan_path)
            result = apply_plan(plan, plan_path=plan_path)
            print(json.dumps({"ok": True, **result}, sort_keys=True))
            return 0
        result = rollback_transaction(
            args.receipt.expanduser().absolute(),
            output_path=args.output.expanduser().absolute(),
        )
        print(json.dumps({"ok": True, **result}, sort_keys=True))
        return 0
    except (
        WorkspaceReleaseTransactionError,
        steady.SteadyReleaseTransactionError,
        base.ReleaseTransactionError,
        OSError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    "code": getattr(
                        exc, "code", "pnc_workspace_release_transaction_invalid"
                    ),
                },
                sort_keys=True,
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
