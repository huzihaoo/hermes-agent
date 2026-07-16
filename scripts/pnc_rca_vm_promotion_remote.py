#!/usr/bin/env python3
"""Transactional VM-side Git promotion helper for the RCA release controller."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import stat
import subprocess
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Mapping, Sequence


REQUEST_SCHEMA_VERSION = "pnc_rca_vm_promotion_remote_request_v1"
OBSERVATION_SCHEMA_VERSION = "pnc_rca_vm_promotion_observation_v1"
RECEIPT_SCHEMA_VERSION = "pnc_rca_vm_promotion_remote_receipt_v1"
ROLLBACK_RECEIPT_SCHEMA_VERSION = "pnc_rca_vm_promotion_remote_rollback_v1"
SNAPSHOT_SCHEMA_VERSION = "pnc_rca_vm_promotion_snapshot_v1"
SERVICE_UNIT = "hermes-vm-coding-worker-daemon.service"
MAX_COMPONENTS = 2
MAX_DIRTY_PATHS = 128
MAX_AFFECTED_PATHS = 20_000
MAX_SNAPSHOT_BYTES = 512 * 1024 * 1024
_COMMIT_RE = re.compile(r"[0-9a-f]{40,64}\Z")
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_COMPONENT_RE = re.compile(r"[a-z][a-z0-9_]{1,31}\Z")


class VmPromotionRemoteError(ValueError):
    def __init__(self, code: str):
        self.code = str(code or "vm_promotion_remote_invalid")[:120]
        super().__init__(self.code)


def _canonical_json(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _absolute(value: Any, *, field: str) -> Path:
    text = str(value or "").strip()
    path = Path(text).expanduser()
    if not text or not path.is_absolute() or ".." in path.parts or "\x00" in text:
        raise VmPromotionRemoteError(f"vm_promotion_{field}_invalid")
    return path.absolute()


def _relative(value: Any, *, field: str) -> str:
    text = str(value or "").strip()
    path = Path(text)
    if (
        not text
        or path.is_absolute()
        or ".." in path.parts
        or "." in path.parts
        or "\x00" in text
    ):
        raise VmPromotionRemoteError(f"vm_promotion_{field}_invalid")
    return path.as_posix()


def _run(
    argv: Sequence[str],
    *,
    check: bool = True,
    timeout: int = 30,
    text: bool = True,
) -> subprocess.CompletedProcess[Any]:
    completed = subprocess.run(
        list(argv),
        check=False,
        capture_output=True,
        text=text,
        shell=False,
        timeout=timeout,
    )
    if check and completed.returncode != 0:
        raise VmPromotionRemoteError("vm_promotion_command_failed")
    return completed


def _git(root: Path, *arguments: str, check: bool = True) -> str:
    result = _run(
        ["git", "-c", "core.fileMode=false", "-C", str(root), *arguments],
        check=check,
        timeout=60,
    )
    return str(result.stdout or "").rstrip("\n")


def _git_bytes(root: Path, *arguments: str) -> bytes:
    result = _run(
        ["git", "-c", "core.fileMode=false", "-C", str(root), *arguments],
        timeout=60,
        text=False,
    )
    return bytes(result.stdout or b"")


def _commit(value: Any, *, field: str) -> str:
    text = str(value or "").strip().lower()
    if _COMMIT_RE.fullmatch(text) is None:
        raise VmPromotionRemoteError(f"vm_promotion_{field}_invalid")
    return text


def _sha256(value: Any, *, field: str) -> str:
    text = str(value or "").strip().lower()
    if _SHA256_RE.fullmatch(text) is None:
        raise VmPromotionRemoteError(f"vm_promotion_{field}_invalid")
    return text


def _status(root: Path) -> tuple[str, list[str]]:
    raw = _git(root, "status", "--porcelain=v1", "--untracked-files=all")
    names_raw = _git_bytes(
        root,
        "ls-files",
        "-z",
        "--modified",
        "--deleted",
        "--others",
        "--exclude-standard",
    )
    names = sorted(
        {
            item.decode("utf-8", errors="strict")
            for item in names_raw.split(b"\x00")
            if item
        }
    )
    if len(names) > MAX_DIRTY_PATHS:
        raise VmPromotionRemoteError("vm_promotion_target_dirty_path_limit")
    for item in names:
        _relative(item, field="dirty_path")
    return raw, names


def _tree_entry(root: Path, commit: str, relative: str) -> Mapping[str, str] | None:
    raw = _git(root, "ls-tree", commit, "--", relative)
    if not raw:
        return None
    lines = raw.splitlines()
    if len(lines) != 1:
        raise VmPromotionRemoteError("vm_promotion_tree_entry_ambiguous")
    match = re.fullmatch(r"(100644|100755|120000|160000) (blob|commit) ([0-9a-f]{40,64})\t(.+)", lines[0])
    if match is None or match.group(4) != relative:
        raise VmPromotionRemoteError("vm_promotion_tree_entry_invalid")
    return {
        "mode": match.group(1),
        "kind": match.group(2),
        "object": match.group(3),
        "path": match.group(4),
    }


def _path_fingerprint(path: Path) -> Mapping[str, Any]:
    try:
        observed = os.lstat(path)
    except FileNotFoundError:
        return {"kind": "absent"}
    if stat.S_ISLNK(observed.st_mode):
        target = os.readlink(path)
        return {
            "kind": "symlink",
            "target_sha256": _sha256_bytes(target.encode("utf-8")),
        }
    if stat.S_ISREG(observed.st_mode):
        return {
            "kind": "file",
            "mode": stat.S_IMODE(observed.st_mode),
            "size": observed.st_size,
            "sha256": _sha256_file(path),
        }
    if stat.S_ISDIR(observed.st_mode):
        return {"kind": "directory"}
    return {"kind": "unsupported"}


def _entrypoint(
    root: Path,
    commit: str,
    relative: str,
    *,
    allow_absent: bool = False,
) -> Mapping[str, Any]:
    entry = _tree_entry(root, commit, relative)
    path = root / relative
    if entry is None:
        if allow_absent and _path_fingerprint(path) == {"kind": "absent"}:
            return {
                "relative_path": relative,
                "path": str(path),
                "state": "absent",
            }
        raise VmPromotionRemoteError("vm_promotion_entrypoint_untracked")
    if entry["kind"] != "blob" or entry["mode"] not in {"100644", "100755"}:
        raise VmPromotionRemoteError("vm_promotion_entrypoint_untracked")
    if path.is_symlink() or not path.is_file():
        raise VmPromotionRemoteError("vm_promotion_entrypoint_missing")
    committed = _git_bytes(root, "cat-file", "blob", f"{commit}:{relative}")
    current = path.read_bytes()
    if current != committed:
        raise VmPromotionRemoteError("vm_promotion_entrypoint_dirty")
    return {
        "relative_path": relative,
        "path": str(path),
        "sha256": _sha256_bytes(current),
        "mode": entry["mode"],
        "blob": entry["object"],
    }


def _repo_facts(
    root: Path,
    entrypoint_relative: str,
    *,
    allow_absent_entrypoint: bool = False,
) -> Mapping[str, Any]:
    if root.is_symlink() or not root.is_dir():
        raise VmPromotionRemoteError("vm_promotion_repo_missing")
    top = Path(_git(root, "rev-parse", "--show-toplevel")).resolve()
    if top != root.resolve():
        raise VmPromotionRemoteError("vm_promotion_repo_root_mismatch")
    head = _commit(_git(root, "rev-parse", "--verify", "HEAD"), field="head")
    tree = _commit(_git(root, "rev-parse", f"{head}^{{tree}}"), field="tree")
    status, dirty_paths = _status(root)
    head_after = _commit(
        _git(root, "rev-parse", "--verify", "HEAD"), field="head_after"
    )
    tree_after = _commit(
        _git(root, "rev-parse", f"{head_after}^{{tree}}"), field="tree_after"
    )
    if head_after != head or tree_after != tree:
        raise VmPromotionRemoteError("vm_promotion_repo_unstable")
    branch_result = _git(root, "symbolic-ref", "-q", "HEAD", check=False)
    dirty = {
        relative: _path_fingerprint(root / relative) for relative in dirty_paths
    }
    return {
        "root": str(root),
        "head": head,
        "tree": tree,
        "head_ref": branch_result,
        "status_sha256": _sha256_bytes(status.encode("utf-8")),
        "tree_clean": status == "",
        "dirty_paths": dirty,
        "entrypoint": _entrypoint(
            root,
            head,
            entrypoint_relative,
            allow_absent=allow_absent_entrypoint,
        ),
    }


def _runtime_artifacts(root: Path, specs: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    rows = []
    for spec in specs:
        if set(spec) != {"relative_path", "sha256", "size"}:
            raise VmPromotionRemoteError("vm_promotion_runtime_artifact_shape_invalid")
        relative = _relative(spec.get("relative_path"), field="runtime_artifact")
        expected_sha = _sha256(spec.get("sha256"), field="runtime_artifact_sha256")
        expected_size = spec.get("size")
        if isinstance(expected_size, bool) or not isinstance(expected_size, int) or expected_size <= 0:
            raise VmPromotionRemoteError("vm_promotion_runtime_artifact_size_invalid")
        path = root / relative
        fingerprint = _path_fingerprint(path)
        rows.append({
            "relative_path": relative,
            "expected_sha256": expected_sha,
            "expected_size": expected_size,
            "observed": fingerprint,
        })
    return rows


def _component_specs(request: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    raw = request.get("components")
    if not isinstance(raw, list) or not raw or len(raw) > MAX_COMPONENTS:
        raise VmPromotionRemoteError("vm_promotion_components_invalid")
    rows = []
    names = set()
    targets = set()
    for item in raw:
        if not isinstance(item, Mapping) or set(item) != {
            "name",
            "candidate_root",
            "target_root",
            "desired_commit",
            "desired_tree",
            "entrypoint_relative",
            "entrypoint_sha256",
            "runtime_artifacts",
        }:
            raise VmPromotionRemoteError("vm_promotion_component_shape_invalid")
        name = str(item.get("name") or "")
        if _COMPONENT_RE.fullmatch(name) is None or name in names:
            raise VmPromotionRemoteError("vm_promotion_component_name_invalid")
        candidate = _absolute(item.get("candidate_root"), field="candidate_root")
        target = _absolute(item.get("target_root"), field="target_root")
        if candidate == target or str(target) in targets:
            raise VmPromotionRemoteError("vm_promotion_component_root_invalid")
        names.add(name)
        targets.add(str(target))
        runtime_artifacts = item.get("runtime_artifacts")
        if not isinstance(runtime_artifacts, list):
            raise VmPromotionRemoteError("vm_promotion_runtime_artifacts_invalid")
        rows.append({
            "name": name,
            "candidate_root": candidate,
            "target_root": target,
            "desired_commit": _commit(item.get("desired_commit"), field="desired_commit"),
            "desired_tree": _commit(item.get("desired_tree"), field="desired_tree"),
            "entrypoint_relative": _relative(item.get("entrypoint_relative"), field="entrypoint"),
            "entrypoint_sha256": _sha256(item.get("entrypoint_sha256"), field="entrypoint_sha256"),
            "runtime_artifacts": runtime_artifacts,
        })
    return rows


def _service_state(mode: str) -> Mapping[str, Any]:
    if mode == "none":
        if os.environ.get("PNC_RCA_VM_PROMOTION_TEST_MODE") != "1":
            raise VmPromotionRemoteError("vm_promotion_service_mode_forbidden")
        return {"mode": "none", "active": False, "main_pid": 0}
    if mode != "systemd_user":
        raise VmPromotionRemoteError("vm_promotion_service_mode_invalid")
    result = _run(
        [
            "systemctl",
            "--user",
            "show",
            SERVICE_UNIT,
            "--property=ActiveState",
            "--property=MainPID",
        ],
        check=False,
        timeout=15,
    )
    if result.returncode != 0:
        raise VmPromotionRemoteError("vm_promotion_service_observation_failed")
    values = {}
    for line in str(result.stdout or "").splitlines():
        key, separator, value = line.partition("=")
        if not separator or key in values:
            raise VmPromotionRemoteError("vm_promotion_service_observation_invalid")
        values[key] = value
    if set(values) != {"ActiveState", "MainPID"} or values["ActiveState"] not in {
        "active",
        "inactive",
        "failed",
    }:
        raise VmPromotionRemoteError("vm_promotion_service_observation_invalid")
    try:
        main_pid = int(values["MainPID"])
    except ValueError as exc:
        raise VmPromotionRemoteError("vm_promotion_service_observation_invalid") from exc
    return {
        "mode": mode,
        "active": values["ActiveState"] == "active",
        "active_state": values["ActiveState"],
        "main_pid": main_pid,
    }


def observe(request: Mapping[str, Any]) -> Mapping[str, Any]:
    if request.get("schema_version") != REQUEST_SCHEMA_VERSION:
        raise VmPromotionRemoteError("vm_promotion_request_schema_invalid")
    specs = _component_specs(request)
    components = {}
    for spec in specs:
        candidate = _repo_facts(spec["candidate_root"], spec["entrypoint_relative"])
        target = _repo_facts(
            spec["target_root"],
            spec["entrypoint_relative"],
            allow_absent_entrypoint=True,
        )
        components[spec["name"]] = {
            "candidate": candidate,
            "target": target,
            "candidate_runtime_artifacts": _runtime_artifacts(
                spec["candidate_root"], spec["runtime_artifacts"]
            ),
            "target_runtime_artifacts": _runtime_artifacts(
                spec["target_root"], spec["runtime_artifacts"]
            ),
        }
    return {
        "schema_version": OBSERVATION_SCHEMA_VERSION,
        "release_id": str(request.get("release_id") or ""),
        "components": components,
        "service": _service_state(str(request.get("service_mode") or "")),
    }


def _active_child_pids(specs: Sequence[Mapping[str, Any]]) -> list[int]:
    needles = {
        str(spec["target_root"] / spec["entrypoint_relative"]) for spec in specs
    }
    found = []
    proc = Path("/proc")
    if not proc.is_dir():
        return []
    for item in proc.iterdir():
        if not item.name.isdigit() or int(item.name) == os.getpid():
            continue
        try:
            command = (item / "cmdline").read_bytes().replace(b"\x00", b" ").decode(
                "utf-8", errors="replace"
            )
        except (FileNotFoundError, PermissionError, OSError):
            continue
        if any(needle in command for needle in needles):
            found.append(int(item.name))
    return sorted(found)


def _stop_service(mode: str, before: Mapping[str, Any]) -> None:
    if mode == "none":
        return
    _run(["systemctl", "--user", "stop", SERVICE_UNIT], timeout=30)
    after = _service_state(mode)
    if after.get("active") is not False or int(after.get("main_pid") or 0) != 0:
        raise VmPromotionRemoteError("vm_promotion_service_stop_failed")
    del before


def _restore_service(mode: str, before: Mapping[str, Any]) -> Mapping[str, Any]:
    if mode == "none":
        return _service_state(mode)
    if before.get("active") is True:
        _run(["systemctl", "--user", "start", SERVICE_UNIT], timeout=30)
        after = _service_state(mode)
        if after.get("active") is not True or int(after.get("main_pid") or 0) <= 0:
            raise VmPromotionRemoteError("vm_promotion_service_restore_failed")
        return after
    return _service_state(mode)


def _atomic_write(path: Path, payload: bytes, mode: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.rca-promote-{os.getpid()}")
    if temporary.exists() or temporary.is_symlink():
        temporary.unlink()
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, mode)
        os.replace(temporary, path)
    finally:
        if temporary.exists() or temporary.is_symlink():
            temporary.unlink()


def _atomic_symlink(path: Path, target: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.rca-promote-{os.getpid()}")
    if temporary.exists() or temporary.is_symlink():
        temporary.unlink()
    temporary.symlink_to(target)
    os.replace(temporary, path)


def _capture_path(path: Path, blob_root: Path, index: int) -> Mapping[str, Any]:
    fingerprint = _path_fingerprint(path)
    row = dict(fingerprint)
    if fingerprint["kind"] == "file":
        blob = blob_root / f"{index:05d}.bin"
        payload = path.read_bytes()
        _atomic_write(blob, payload, 0o600)
        row["blob"] = blob.name
    elif fingerprint["kind"] == "symlink":
        row["target"] = os.readlink(path)
    elif fingerprint["kind"] not in {"absent"}:
        raise VmPromotionRemoteError("vm_promotion_snapshot_path_unsupported")
    return row


def _restore_path(path: Path, row: Mapping[str, Any], blob_root: Path) -> None:
    kind = row.get("kind")
    if path.is_dir() and not path.is_symlink():
        raise VmPromotionRemoteError("vm_promotion_rollback_directory_conflict")
    if kind == "absent":
        if path.exists() or path.is_symlink():
            path.unlink()
        return
    if kind == "file":
        blob = blob_root / str(row.get("blob") or "")
        payload = blob.read_bytes()
        if _sha256_bytes(payload) != row.get("sha256"):
            raise VmPromotionRemoteError("vm_promotion_snapshot_blob_drift")
        _atomic_write(path, payload, int(row.get("mode") or 0))
        return
    if kind == "symlink":
        _atomic_symlink(path, str(row.get("target") or ""))
        return
    raise VmPromotionRemoteError("vm_promotion_snapshot_kind_invalid")


def _changed_paths(target: Path, old: str, desired: str) -> list[str]:
    raw = _git_bytes(target, "diff", "--name-only", "-z", old, desired)
    paths = sorted(
        {
            item.decode("utf-8", errors="strict")
            for item in raw.split(b"\x00")
            if item
        }
    )
    if len(paths) > MAX_AFFECTED_PATHS:
        raise VmPromotionRemoteError("vm_promotion_affected_path_limit")
    for item in paths:
        _relative(item, field="affected_path")
    return paths


def _materialize_tree_path(target: Path, commit: str, relative: str) -> None:
    path = target / relative
    entry = _tree_entry(target, commit, relative)
    if entry is None:
        if path.is_dir() and not path.is_symlink():
            raise VmPromotionRemoteError("vm_promotion_delete_directory_forbidden")
        if path.exists() or path.is_symlink():
            path.unlink()
        return
    if entry["kind"] == "commit" or entry["mode"] == "160000":
        raise VmPromotionRemoteError("vm_promotion_gitlink_change_forbidden")
    payload = _git_bytes(target, "cat-file", "blob", entry["object"])
    if entry["mode"] == "120000":
        _atomic_symlink(path, payload.decode("utf-8", errors="strict"))
        return
    _atomic_write(path, payload, 0o755 if entry["mode"] == "100755" else 0o644)


def _promote_component(
    spec: Mapping[str, Any], snapshot_root: Path
) -> Mapping[str, Any]:
    candidate = spec["candidate_root"]
    target = spec["target_root"]
    desired = spec["desired_commit"]
    before = _repo_facts(
        target,
        spec["entrypoint_relative"],
        allow_absent_entrypoint=True,
    )
    candidate_facts = _repo_facts(candidate, spec["entrypoint_relative"])
    if (
        candidate_facts["head"] != desired
        or candidate_facts["tree"] != spec["desired_tree"]
        or candidate_facts["tree_clean"] is not True
        or candidate_facts["entrypoint"]["sha256"] != spec["entrypoint_sha256"]
    ):
        raise VmPromotionRemoteError("vm_promotion_candidate_drift")
    _git(target, "fetch", "--no-tags", str(candidate), desired)
    if _git(target, "cat-file", "-t", desired) != "commit":
        raise VmPromotionRemoteError("vm_promotion_candidate_commit_unavailable")
    changed = set(_changed_paths(target, before["head"], desired))
    changed.update(before["dirty_paths"])
    for artifact in spec["runtime_artifacts"]:
        changed.add(_relative(artifact.get("relative_path"), field="runtime_artifact"))
    affected = sorted(changed)
    if len(affected) > MAX_AFFECTED_PATHS:
        raise VmPromotionRemoteError("vm_promotion_affected_path_limit")
    component_snapshot = snapshot_root / spec["name"]
    blob_root = component_snapshot / "blobs"
    blob_root.mkdir(parents=True, mode=0o700)
    rows = []
    snapshot_bytes = 0
    for index, relative in enumerate(affected):
        captured = _capture_path(target / relative, blob_root, index)
        captured = {"relative_path": relative, **captured}
        snapshot_bytes += int(captured.get("size") or 0)
        if snapshot_bytes > MAX_SNAPSHOT_BYTES:
            raise VmPromotionRemoteError("vm_promotion_snapshot_size_limit")
        rows.append(captured)
    snapshot = {
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "name": spec["name"],
        "target_root": str(target),
        "head": before["head"],
        "head_ref": before["head_ref"],
        "tree": before["tree"],
        "status_sha256": before["status_sha256"],
        "paths": rows,
    }
    _atomic_write(component_snapshot / "snapshot.json", _canonical_json(snapshot), 0o600)
    try:
        for relative in affected:
            runtime_spec = next(
                (
                    item
                    for item in spec["runtime_artifacts"]
                    if item.get("relative_path") == relative
                ),
                None,
            )
            if runtime_spec is not None:
                source = candidate / relative
                payload = source.read_bytes()
                if (
                    _sha256_bytes(payload) != runtime_spec.get("sha256")
                    or len(payload) != runtime_spec.get("size")
                ):
                    raise VmPromotionRemoteError("vm_promotion_runtime_artifact_drift")
                _atomic_write(target / relative, payload, 0o755)
            else:
                _materialize_tree_path(target, desired, relative)
        _git(target, "update-ref", "--no-deref", "HEAD", desired, before["head"])
        _git(target, "reset", "--mixed", desired)
        after = _repo_facts(target, spec["entrypoint_relative"])
        if (
            after["head"] != desired
            or after["tree"] != spec["desired_tree"]
            or after["tree_clean"] is not True
            or after["entrypoint"]["sha256"] != spec["entrypoint_sha256"]
        ):
            raise VmPromotionRemoteError("vm_promotion_post_verify_failed")
        runtime_after = _runtime_artifacts(target, spec["runtime_artifacts"])
        if any(
            item["observed"].get("sha256") != item["expected_sha256"]
            or item["observed"].get("size") != item["expected_size"]
            for item in runtime_after
        ):
            raise VmPromotionRemoteError("vm_promotion_runtime_artifact_verify_failed")
        return {
            "name": spec["name"],
            "before": before,
            "after": after,
            "runtime_artifacts": runtime_after,
            "snapshot_path": str(component_snapshot / "snapshot.json"),
            "snapshot_sha256": _sha256_file(component_snapshot / "snapshot.json"),
        }
    except Exception:
        _rollback_component(snapshot, component_snapshot)
        raise


def _rollback_component(snapshot: Mapping[str, Any], component_snapshot: Path) -> Mapping[str, Any]:
    target = _absolute(snapshot.get("target_root"), field="rollback_target")
    blob_root = component_snapshot / "blobs"
    rows = snapshot.get("paths")
    if not isinstance(rows, list):
        raise VmPromotionRemoteError("vm_promotion_snapshot_invalid")
    for row in reversed(rows):
        if not isinstance(row, Mapping):
            raise VmPromotionRemoteError("vm_promotion_snapshot_invalid")
        relative = _relative(row.get("relative_path"), field="rollback_path")
        _restore_path(target / relative, row, blob_root)
    old = _commit(snapshot.get("head"), field="rollback_head")
    current = _commit(_git(target, "rev-parse", "HEAD"), field="rollback_current")
    _git(target, "update-ref", "--no-deref", "HEAD", old, current)
    _git(target, "reset", "--mixed", old)
    head_ref = str(snapshot.get("head_ref") or "")
    if head_ref:
        if not head_ref.startswith("refs/heads/"):
            raise VmPromotionRemoteError("vm_promotion_snapshot_head_ref_invalid")
        _git(target, "symbolic-ref", "HEAD", head_ref)
        _git(target, "reset", "--mixed", old)
    after = (
        _repo_facts(
            target,
            str(snapshot.get("entrypoint_relative") or ""),
            allow_absent_entrypoint=True,
        )
        if snapshot.get("entrypoint_relative")
        else None
    )
    status, _paths = _status(target)
    if _sha256_bytes(status.encode("utf-8")) != snapshot.get("status_sha256"):
        raise VmPromotionRemoteError("vm_promotion_rollback_status_mismatch")
    return {"head": old, "status_sha256": snapshot.get("status_sha256"), "facts": after}


@contextmanager
def _lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        os.close(descriptor)
        raise VmPromotionRemoteError("vm_promotion_lock_busy") from exc
    try:
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def apply(request: Mapping[str, Any]) -> Mapping[str, Any]:
    if request.get("schema_version") != REQUEST_SCHEMA_VERSION:
        raise VmPromotionRemoteError("vm_promotion_request_schema_invalid")
    specs = _component_specs(request)
    expected = _sha256(request.get("expected_observation_sha256"), field="observation_sha256")
    work_root = _absolute(request.get("remote_work_root"), field="work_root")
    snapshot_root = work_root / "snapshot"
    lock_path = _absolute(request.get("lock_path"), field="lock_path")
    service_mode = str(request.get("service_mode") or "")
    promoted = []
    with _lock(lock_path):
        observed = observe(request)
        if _sha256_bytes(_canonical_json(observed)) != expected:
            raise VmPromotionRemoteError("vm_promotion_prestate_drift")
        if snapshot_root.exists() or snapshot_root.is_symlink():
            raise VmPromotionRemoteError("vm_promotion_snapshot_exists")
        service_before = observed["service"]
        snapshot_root.mkdir(parents=True, mode=0o700)
        service_after: Mapping[str, Any] | None = None
        try:
            _stop_service(service_mode, service_before)
            stopped_observation = observe(request)
            if stopped_observation.get("components") != observed.get("components"):
                raise VmPromotionRemoteError(
                    "vm_promotion_prestate_drift_after_service_stop"
                )
            active = _active_child_pids(specs)
            if active:
                raise VmPromotionRemoteError("vm_promotion_active_child_processes")
            for spec in specs:
                promoted.append(_promote_component(spec, snapshot_root))
            service_after = _restore_service(service_mode, service_before)
            receipt = {
                "schema_version": RECEIPT_SCHEMA_VERSION,
                "ok": True,
                "release_id": str(request.get("release_id") or ""),
                "expected_observation_sha256": expected,
                "components": promoted,
                "service_before": service_before,
                "service_after": service_after,
                "snapshot_root": str(snapshot_root),
                "production_effects_executed": True,
            }
            receipt_path = work_root / "remote-receipt.json"
            _atomic_write(receipt_path, _canonical_json(receipt), 0o600)
        except Exception as apply_exc:
            try:
                current_service = _service_state(service_mode)
                _stop_service(service_mode, current_service)
                if _active_child_pids(specs):
                    raise VmPromotionRemoteError(
                        "vm_promotion_active_child_processes_during_rollback"
                    )
                for item in reversed(promoted):
                    snapshot_path = Path(str(item["snapshot_path"]))
                    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
                    snapshot["entrypoint_relative"] = next(
                        spec["entrypoint_relative"]
                        for spec in specs
                        if spec["name"] == item["name"]
                    )
                    _rollback_component(snapshot, snapshot_path.parent)
                _restore_service(service_mode, service_before)
            except Exception as rollback_exc:
                raise VmPromotionRemoteError(
                    "vm_promotion_apply_rollback_failed"
                ) from rollback_exc
            raise apply_exc
    return {**receipt, "receipt_path": str(receipt_path), "receipt_sha256": _sha256_file(receipt_path)}


def rollback(request: Mapping[str, Any]) -> Mapping[str, Any]:
    if request.get("schema_version") != REQUEST_SCHEMA_VERSION:
        raise VmPromotionRemoteError("vm_promotion_request_schema_invalid")
    specs = _component_specs(request)
    work_root = _absolute(request.get("remote_work_root"), field="work_root")
    receipt_path = _absolute(
        request.get("remote_receipt_path"), field="remote_receipt_path"
    )
    if receipt_path != work_root / "remote-receipt.json":
        raise VmPromotionRemoteError("vm_promotion_remote_receipt_path_invalid")
    expected_receipt_sha = _sha256(
        request.get("remote_receipt_sha256"), field="remote_receipt_sha256"
    )
    if _sha256_file(receipt_path) != expected_receipt_sha:
        raise VmPromotionRemoteError("vm_promotion_remote_receipt_drift")
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    if (
        receipt.get("schema_version") != RECEIPT_SCHEMA_VERSION
        or receipt.get("ok") is not True
        or receipt.get("release_id") != request.get("release_id")
        or receipt.get("production_effects_executed") is not True
        or not isinstance(receipt.get("components"), list)
        or {item.get("name") for item in receipt["components"]}
        != {spec["name"] for spec in specs}
    ):
        raise VmPromotionRemoteError("vm_promotion_remote_receipt_invalid")
    lock_path = _absolute(request.get("lock_path"), field="lock_path")
    service_mode = str(request.get("service_mode") or "")
    service_before = receipt.get("service_before")
    if not isinstance(service_before, Mapping):
        raise VmPromotionRemoteError("vm_promotion_remote_receipt_invalid")
    rolled_back = []
    with _lock(lock_path):
        if _sha256_file(receipt_path) != expected_receipt_sha:
            raise VmPromotionRemoteError("vm_promotion_remote_receipt_drift")
        current_receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        if current_receipt != receipt:
            raise VmPromotionRemoteError("vm_promotion_remote_receipt_drift")
        _stop_service(service_mode, receipt.get("service_after") or {})
        active = _active_child_pids(specs)
        if active:
            _restore_service(service_mode, receipt.get("service_after") or {})
            raise VmPromotionRemoteError("vm_promotion_active_child_processes")
        try:
            for item in receipt["components"]:
                spec = next(spec for spec in specs if spec["name"] == item["name"])
                if _repo_facts(
                    spec["target_root"], spec["entrypoint_relative"]
                ) != item.get("after"):
                    raise VmPromotionRemoteError(
                        "vm_promotion_rollback_target_drift"
                    )
                if _runtime_artifacts(
                    spec["target_root"], spec["runtime_artifacts"]
                ) != item.get("runtime_artifacts"):
                    raise VmPromotionRemoteError(
                        "vm_promotion_rollback_runtime_artifact_drift"
                    )
            for item in reversed(receipt["components"]):
                spec = next(spec for spec in specs if spec["name"] == item["name"])
                snapshot_path = _absolute(
                    item.get("snapshot_path"), field="rollback_snapshot_path"
                )
                if snapshot_path.parent.parent != work_root / "snapshot":
                    raise VmPromotionRemoteError("vm_promotion_snapshot_path_invalid")
                if _sha256_file(snapshot_path) != item.get("snapshot_sha256"):
                    raise VmPromotionRemoteError("vm_promotion_snapshot_drift")
                snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
                snapshot["entrypoint_relative"] = spec["entrypoint_relative"]
                restored = _rollback_component(snapshot, snapshot_path.parent)
                rolled_back.append({"name": item["name"], "restored": restored})
            service_after = _restore_service(service_mode, service_before)
        except Exception:
            _restore_service(service_mode, receipt.get("service_after") or {})
            raise
    rollback_receipt = {
        "schema_version": ROLLBACK_RECEIPT_SCHEMA_VERSION,
        "ok": True,
        "release_id": request.get("release_id"),
        "promotion_receipt_sha256": expected_receipt_sha,
        "components": rolled_back,
        "service_restored": service_after,
        "production_effects_executed": True,
        "rollback_complete": True,
    }
    output = work_root / "remote-rollback-receipt.json"
    _atomic_write(output, _canonical_json(rollback_receipt), 0o600)
    return {
        **rollback_receipt,
        "receipt_path": str(output),
        "receipt_sha256": _sha256_file(output),
    }


def execute(request: Mapping[str, Any]) -> Mapping[str, Any]:
    mode = str(request.get("mode") or "")
    if mode == "observe":
        return observe(request)
    if mode == "apply":
        return apply(request)
    if mode == "rollback":
        return rollback(request)
    raise VmPromotionRemoteError("vm_promotion_mode_invalid")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--request", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    request = json.loads(args.request.read_text(encoding="utf-8"))
    print(json.dumps(execute(request), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
