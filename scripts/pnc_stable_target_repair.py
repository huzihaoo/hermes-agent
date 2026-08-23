#!/usr/bin/env python3
"""Build and verify an offline stable-target repair candidate.

The module has no production defaults and intentionally exposes no installer,
stage, backup, restore, or apply operation. Every runtime path is observed
read-only; only an explicitly supplied evidence directory receives a sealed
candidate manifest and plan.
"""

from __future__ import annotations

from datetime import datetime, timezone
import errno
import hashlib
import json
import os
from pathlib import Path
import re
import stat
from typing import Any, Mapping, Sequence
import uuid


PLAN_SCHEMA_VERSION = "pnc_stable_target_repair_plan_v2"
CANDIDATE_MANIFEST_SCHEMA_VERSION = "pnc_stable_target_candidate_manifest_v1"
STABLE_TARGET_SCHEMA_VERSION = "pnc_stable_target_registry_v1"
SAFE_WORKTREE_LABEL = "local.pnc.safe-worktree-remove"
SAFE_WORKTREE_RELATIVE_PATH = "hermes_safe_worktree_remove.py"
MAX_ARTIFACT_BYTES = 4 * 1024 * 1024
_TRANSACTION_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


class StableTargetRepairError(RuntimeError):
    """Fail-closed offline candidate error."""

    def __init__(self, code: str, detail: str = ""):
        self.code = str(code or "stable_target_repair_invalid")[:160]
        self.detail = str(detail or self.code)[:1000]
        super().__init__(self.code)


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _absolute(path: Path, code: str) -> Path:
    selected = path.expanduser()
    if not selected.is_absolute() or selected.absolute() != selected:
        raise StableTargetRepairError(code, str(selected))
    return selected


def _directory(path: Path, *, create: bool = False) -> Path:
    selected = _absolute(path, "stable_target_directory_invalid")
    if create:
        try:
            selected.mkdir(mode=0o700, parents=False, exist_ok=False)
        except OSError as exc:
            raise StableTargetRepairError(
                "stable_target_directory_invalid", str(selected)
            ) from exc
    try:
        observed = selected.lstat()
        resolved = selected.resolve(strict=True)
    except OSError as exc:
        raise StableTargetRepairError(
            "stable_target_directory_invalid", str(selected)
        ) from exc
    if (
        resolved != selected
        or stat.S_ISLNK(observed.st_mode)
        or not stat.S_ISDIR(observed.st_mode)
        or observed.st_uid != os.geteuid()
        or stat.S_IMODE(observed.st_mode) & 0o022
    ):
        raise StableTargetRepairError("stable_target_directory_invalid", str(selected))
    return selected


def _stable_read(path: Path) -> tuple[bytes, os.stat_result]:
    """Read one owner file while checking lstat -> fstat -> read -> fstat -> lstat."""
    selected = _absolute(path, "stable_target_path_invalid")
    try:
        before = selected.lstat()
    except FileNotFoundError as exc:
        raise StableTargetRepairError(
            "stable_target_file_missing", str(selected)
        ) from exc
    except OSError as exc:
        if exc.errno in {errno.ELOOP, errno.EMLINK}:
            raise StableTargetRepairError(
                "stable_target_file_invalid", str(selected)
            ) from exc
        raise StableTargetRepairError(
            "stable_target_file_unavailable", str(selected)
        ) from exc
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = -1
    try:
        descriptor = os.open(selected, flags)
        opened = os.fstat(descriptor)
        if (
            before.st_dev != opened.st_dev
            or before.st_ino != opened.st_ino
            or before.st_mode != opened.st_mode
            or before.st_uid != opened.st_uid
            or before.st_nlink != opened.st_nlink
            or before.st_size != opened.st_size
        ):
            raise StableTargetRepairError("stable_target_file_changed", str(selected))
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            total += len(chunk)
            if total > MAX_ARTIFACT_BYTES:
                raise StableTargetRepairError(
                    "stable_target_file_too_large", str(selected)
                )
            chunks.append(chunk)
        after = os.fstat(descriptor)
    except StableTargetRepairError:
        raise
    except OSError as exc:
        if exc.errno in {errno.ELOOP, errno.EMLINK}:
            raise StableTargetRepairError(
                "stable_target_file_invalid", str(selected)
            ) from exc
        raise StableTargetRepairError(
            "stable_target_file_unavailable", str(selected)
        ) from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    try:
        final = selected.lstat()
    except OSError as exc:
        raise StableTargetRepairError(
            "stable_target_file_changed", str(selected)
        ) from exc
    if (
        before.st_dev != after.st_dev
        or before.st_ino != after.st_ino
        or before.st_mode != after.st_mode
        or before.st_uid != after.st_uid
        or before.st_nlink != after.st_nlink
        or before.st_size != after.st_size
        or before.st_mtime_ns != after.st_mtime_ns
        or before.st_ctime_ns != after.st_ctime_ns
        or final.st_dev != after.st_dev
        or final.st_ino != after.st_ino
        or final.st_mode != after.st_mode
        or final.st_uid != after.st_uid
        or final.st_nlink != after.st_nlink
        or final.st_size != after.st_size
        or final.st_mtime_ns != after.st_mtime_ns
        or final.st_ctime_ns != after.st_ctime_ns
    ):
        raise StableTargetRepairError("stable_target_file_changed", str(selected))
    return b"".join(chunks), after


def _observe_file(
    path: Path,
    *,
    required: bool,
    allowed_modes: Sequence[int],
) -> dict[str, Any]:
    selected = _absolute(path, "stable_target_path_invalid")
    try:
        raw, observed = _stable_read(selected)
    except StableTargetRepairError as exc:
        if not required and exc.code == "stable_target_file_missing":
            return {
                "exists": False,
                "sha256": None,
                "size": 0,
                "mode": None,
                "uid": None,
                "nlink": None,
            }
        raise
    mode = stat.S_IMODE(observed.st_mode)
    if (
        not stat.S_ISREG(observed.st_mode)
        or observed.st_uid != os.geteuid()
        or observed.st_nlink != 1
        or observed.st_size <= 0
        or observed.st_size > MAX_ARTIFACT_BYTES
        or mode not in allowed_modes
        or len(raw) != observed.st_size
    ):
        raise StableTargetRepairError("stable_target_file_invalid", str(selected))
    return {
        "exists": True,
        "sha256": _sha256(raw),
        "size": observed.st_size,
        "mode": format(mode, "04o"),
        "uid": observed.st_uid,
        "nlink": observed.st_nlink,
        "dev": observed.st_dev,
        "ino": observed.st_ino,
        "mtime_ns": observed.st_mtime_ns,
        "ctime_ns": observed.st_ctime_ns,
    }


def _read_json(
    path: Path, *, allowed_modes: Sequence[int]
) -> tuple[bytes, dict[str, Any], dict[str, Any]]:
    observation = _observe_file(path, required=True, allowed_modes=allowed_modes)
    raw, observed_again = _stable_read(path)
    if _sha256(raw) != observation["sha256"] or len(raw) != observation["size"]:
        raise StableTargetRepairError("stable_target_file_changed", str(path))
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise StableTargetRepairError("stable_target_json_invalid", str(path)) from exc
    if not isinstance(value, dict):
        raise StableTargetRepairError("stable_target_json_invalid", str(path))
    if observed_again.st_ino != observation["ino"]:
        raise StableTargetRepairError("stable_target_file_changed", str(path))
    return raw, value, observation


def _pretty(value: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(
            dict(value), ensure_ascii=True, indent=2, sort_keys=True, allow_nan=False
        )
        + "\n"
    ).encode("utf-8")


def _write_evidence_new(path: Path, raw: bytes, *, mode: int) -> dict[str, Any]:
    """Create one evidence artifact with O_EXCL, fsync, and post-write CAS."""
    descriptor = -1
    try:
        descriptor = os.open(
            path,
            os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_CLOEXEC", 0),
            mode,
        )
        written = 0
        while written < len(raw):
            count = os.write(descriptor, raw[written:])
            if count <= 0:
                raise OSError("short write")
            written += count
        os.fsync(descriptor)
    except FileExistsError as exc:
        raise StableTargetRepairError(
            "stable_target_evidence_exists", str(path)
        ) from exc
    except OSError as exc:
        raise StableTargetRepairError(
            "stable_target_evidence_write_failed", str(path)
        ) from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    observed = _observe_file(path, required=True, allowed_modes=(mode,))
    if observed["sha256"] != _sha256(raw) or observed["size"] != len(raw):
        raise StableTargetRepairError("stable_target_evidence_changed", str(path))
    return observed


def _validate_registry_shape(raw: bytes) -> None:
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise StableTargetRepairError("stable_target_registry_invalid") from exc
    targets = value.get("targets") if isinstance(value, dict) else None
    item = targets.get(SAFE_WORKTREE_LABEL) if isinstance(targets, dict) else None
    if (
        not isinstance(value, dict)
        or value.get("schema_version") != STABLE_TARGET_SCHEMA_VERSION
        or not isinstance(item, dict)
        or set(item) != {"target_kind", "relative_path", "sha256", "size"}
        or item.get("target_kind") != "governance_tool"
        or item.get("relative_path") != SAFE_WORKTREE_RELATIVE_PATH
        or not isinstance(item.get("sha256"), str)
        or len(item["sha256"]) != 64
        or any(char not in "0123456789abcdef" for char in item["sha256"])
        or type(item.get("size")) is not int
        or item["size"] <= 0
    ):
        raise StableTargetRepairError("stable_target_registry_invalid")


def _candidate_registry(raw: bytes, *, helper_sha256: str, helper_size: int) -> bytes:
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise StableTargetRepairError("stable_target_registry_invalid") from exc
    _validate_registry_shape(raw)
    targets = dict(value["targets"])
    targets[SAFE_WORKTREE_LABEL] = {
        **dict(targets[SAFE_WORKTREE_LABEL]),
        "sha256": helper_sha256,
        "size": helper_size,
    }
    return _pretty({**value, "targets": targets})


def _candidate_manifest(raw: bytes, *, helper_sha256: str) -> bytes:
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise StableTargetRepairError("stable_target_manifest_invalid") from exc
    hashes = value.get("governance_tool_sha256") if isinstance(value, dict) else None
    if (
        not isinstance(value, dict)
        or value.get("schema_version") != 1
        or not isinstance(hashes, dict)
        or not isinstance(hashes.get(SAFE_WORKTREE_RELATIVE_PATH), str)
    ):
        raise StableTargetRepairError("stable_target_manifest_invalid")
    candidate = dict(value)
    candidate["governance_tool_sha256"] = {
        **hashes,
        SAFE_WORKTREE_RELATIVE_PATH: helper_sha256,
    }
    return _pretty(candidate)


def _entry(
    name: str, path: Path, observation: dict[str, Any], *, role: str
) -> dict[str, Any]:
    return {
        "name": name,
        "role": role,
        "path": str(path),
        "observed": observation,
    }


def build_plan(
    *,
    helper_source: Path,
    registry_source: Path,
    installed_helper: Path,
    runtime_registry: Path,
    live_manifest: Path,
    evidence_root: Path,
    transaction_id: str | None = None,
) -> tuple[dict[str, Any], Path]:
    """Read all preimages and emit only evidence-root candidate artifacts."""
    paths = {
        "helper_source": _absolute(helper_source, "stable_target_source_path_invalid"),
        "registry_source": _absolute(
            registry_source, "stable_target_source_path_invalid"
        ),
        "installed_helper": _absolute(
            installed_helper, "stable_target_target_path_invalid"
        ),
        "runtime_registry": _absolute(
            runtime_registry, "stable_target_target_path_invalid"
        ),
        "live_manifest": _absolute(live_manifest, "stable_target_target_path_invalid"),
    }
    evidence = _directory(evidence_root)
    for path in (
        paths["installed_helper"],
        paths["runtime_registry"],
        paths["live_manifest"],
    ):
        _directory(path.parent)
    selected_id = transaction_id or (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        + "-"
        + uuid.uuid4().hex[:12]
    )
    if _TRANSACTION_ID_RE.fullmatch(selected_id) is None:
        raise StableTargetRepairError("stable_target_transaction_id_invalid")
    transaction_dir = evidence / selected_id
    if transaction_dir.exists() or transaction_dir.is_symlink():
        raise StableTargetRepairError(
            "stable_target_evidence_exists", str(transaction_dir)
        )
    _directory(transaction_dir, create=True)

    _helper_raw, helper_stat = _stable_read(paths["helper_source"])
    helper = _observe_file(
        paths["helper_source"], required=True, allowed_modes=(0o755,)
    )
    if helper_stat.st_ino != helper["ino"]:
        raise StableTargetRepairError(
            "stable_target_file_changed", str(paths["helper_source"])
        )
    registry_raw, _registry, registry_observation = _read_json(
        paths["registry_source"], allowed_modes=(0o644,)
    )
    _validate_registry_shape(registry_raw)
    manifest_raw, _manifest, manifest_observation = _read_json(
        paths["live_manifest"], allowed_modes=(0o600,)
    )
    candidate_registry_raw = _candidate_registry(
        registry_raw,
        helper_sha256=str(helper["sha256"]),
        helper_size=int(helper["size"]),
    )
    candidate_registry_path = (
        transaction_dir / "pnc_stable_target_registry_v1.candidate.json"
    )
    candidate_registry_observation = _write_evidence_new(
        candidate_registry_path, candidate_registry_raw, mode=0o644
    )
    candidate_raw = _candidate_manifest(
        manifest_raw, helper_sha256=str(helper["sha256"])
    )
    candidate_manifest_path = transaction_dir / "LIVE_MANIFEST.candidate.json"
    candidate_manifest_observation = _write_evidence_new(
        candidate_manifest_path, candidate_raw, mode=0o600
    )
    installed_observation = _observe_file(
        paths["installed_helper"], required=True, allowed_modes=(0o755,)
    )
    runtime_registry_observation = _observe_file(
        paths["runtime_registry"], required=True, allowed_modes=(0o644,)
    )
    entries = [
        _entry("helper_source", paths["helper_source"], helper, role="source"),
        _entry(
            "registry_source",
            paths["registry_source"],
            registry_observation,
            role="source",
        ),
        _entry(
            "installed_helper_preimage",
            paths["installed_helper"],
            installed_observation,
            role="target_preimage",
        ),
        _entry(
            "runtime_registry_preimage",
            paths["runtime_registry"],
            runtime_registry_observation,
            role="target_preimage",
        ),
        _entry(
            "live_manifest_preimage",
            paths["live_manifest"],
            manifest_observation,
            role="target_preimage",
        ),
        _entry(
            "candidate_registry",
            candidate_registry_path,
            candidate_registry_observation,
            role="candidate",
        ),
        _entry(
            "candidate_manifest",
            candidate_manifest_path,
            candidate_manifest_observation,
            role="candidate",
        ),
    ]
    plan = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "candidate_manifest_schema_version": CANDIDATE_MANIFEST_SCHEMA_VERSION,
        "transaction_id": selected_id,
        "planned_at": datetime.now(timezone.utc).isoformat(),
        "candidate_manifest_path": str(candidate_manifest_path),
        "candidate_manifest_sha256": _sha256(candidate_raw),
        "candidate_manifest_size": len(candidate_raw),
        "candidate_registry_path": str(candidate_registry_path),
        "candidate_registry_sha256": _sha256(candidate_registry_raw),
        "candidate_registry_size": len(candidate_registry_raw),
        "helper_sha256": helper["sha256"],
        "helper_size": helper["size"],
        "registry_source_sha256": registry_observation["sha256"],
        "manifest_source_sha256": manifest_observation["sha256"],
        "entries": entries,
        "mutation_performed": False,
        "production_apply_available": False,
    }
    plan_raw = _pretty(plan)
    plan_path = transaction_dir / "plan.json"
    _write_evidence_new(plan_path, plan_raw, mode=0o600)
    return plan, plan_path


def _load_persisted_plan(
    plan: Mapping[str, Any], plan_path: Path
) -> tuple[dict[str, Any], dict[str, Any]]:
    selected = _absolute(plan_path, "stable_target_plan_path_invalid")
    _raw, persisted, observation = _read_json(selected, allowed_modes=(0o600,))
    if persisted != dict(plan):
        raise StableTargetRepairError("stable_target_plan_binding_invalid")
    return persisted, observation


def verify_plan(plan: Mapping[str, Any], *, plan_path: Path) -> dict[str, Any]:
    """Verify a candidate plan and all current preimages without mutation."""
    persisted, plan_observation = _load_persisted_plan(plan, plan_path)
    entries = persisted.get("entries")
    if (
        not isinstance(entries, list)
        or persisted.get("production_apply_available") is not False
    ):
        raise StableTargetRepairError("stable_target_plan_invalid")
    expected_names = {
        "helper_source",
        "registry_source",
        "installed_helper_preimage",
        "runtime_registry_preimage",
        "live_manifest_preimage",
        "candidate_registry",
        "candidate_manifest",
    }
    entries_by_name = {
        str(entry.get("name") or ""): entry
        for entry in entries
        if isinstance(entry, dict)
    }
    if set(entries_by_name) != expected_names or len(entries_by_name) != len(entries):
        raise StableTargetRepairError("stable_target_plan_invalid")
    checked: list[dict[str, Any]] = []
    for entry in entries:
        if not isinstance(entry, dict) or entry.get("role") not in {
            "source",
            "target_preimage",
            "candidate",
        }:
            raise StableTargetRepairError("stable_target_plan_invalid")
        path = Path(str(entry.get("path") or ""))
        if entry["name"] in {"helper_source", "installed_helper_preimage"}:
            allowed_modes = (0o755,)
        elif entry["name"] in {"live_manifest_preimage", "candidate_manifest"}:
            allowed_modes = (0o600,)
        else:
            allowed_modes = (0o644,)
        observed = _observe_file(path, required=True, allowed_modes=allowed_modes)
        if observed != entry.get("observed"):
            raise StableTargetRepairError(
                "stable_target_preimage_changed", entry["name"]
            )
        checked.append({
            "name": entry["name"],
            "sha256": observed["sha256"],
            "size": observed["size"],
        })

    helper_observation = entries_by_name["helper_source"]["observed"]
    registry_source_observation = entries_by_name["registry_source"]["observed"]
    manifest_source_observation = entries_by_name["live_manifest_preimage"]["observed"]
    candidate_registry_observation = entries_by_name["candidate_registry"]["observed"]
    candidate_manifest_observation = entries_by_name["candidate_manifest"]["observed"]
    if (
        persisted.get("helper_sha256") != helper_observation.get("sha256")
        or persisted.get("helper_size") != helper_observation.get("size")
        or persisted.get("registry_source_sha256")
        != registry_source_observation.get("sha256")
        or persisted.get("manifest_source_sha256")
        != manifest_source_observation.get("sha256")
        or persisted.get("candidate_registry_sha256")
        != candidate_registry_observation.get("sha256")
        or persisted.get("candidate_registry_size")
        != candidate_registry_observation.get("size")
        or persisted.get("candidate_manifest_sha256")
        != candidate_manifest_observation.get("sha256")
        or persisted.get("candidate_manifest_size")
        != candidate_manifest_observation.get("size")
    ):
        raise StableTargetRepairError("stable_target_plan_binding_invalid")

    candidate_registry_path = Path(str(persisted["candidate_registry_path"]))
    candidate_registry_raw, candidate_registry, _registry_observation = _read_json(
        candidate_registry_path, allowed_modes=(0o644,)
    )
    _validate_registry_shape(candidate_registry_raw)
    candidate_target = candidate_registry["targets"][SAFE_WORKTREE_LABEL]
    candidate_manifest_path = Path(str(persisted["candidate_manifest_path"]))
    _candidate_manifest_raw, candidate_manifest, _manifest_observation = _read_json(
        candidate_manifest_path, allowed_modes=(0o600,)
    )
    candidate_hashes = candidate_manifest.get("governance_tool_sha256")
    if (
        candidate_target.get("sha256") != persisted["helper_sha256"]
        or candidate_target.get("size") != persisted["helper_size"]
        or not isinstance(candidate_hashes, dict)
        or candidate_hashes.get(SAFE_WORKTREE_RELATIVE_PATH)
        != persisted["helper_sha256"]
    ):
        raise StableTargetRepairError("stable_target_plan_binding_invalid")
    return {
        "schema_version": PLAN_SCHEMA_VERSION,
        "transaction_id": persisted["transaction_id"],
        "plan_sha256": plan_observation["sha256"],
        "checked": checked,
        "mutation_performed": False,
        "production_apply_available": False,
        "verification": "pass",
    }
