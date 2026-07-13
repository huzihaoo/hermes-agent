"""Immutable identity checks for the fixed RCA shared-state creator bundle."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from hermes_constants import get_hermes_home


WORKSPACE_RUNTIME_MANIFEST_SCHEMA_VERSION = "pnc_rca_workspace_runtime_bundle_v1"
WORKSPACE_RUNTIME_IDENTITY_SCHEMA_VERSION = "pnc_rca_workspace_runtime_identity_v1"
WORKSPACE_RUNTIME_DIRECTORY_NAME = "rca-workspace-runtime"
WORKSPACE_RUNTIME_MANIFEST_NAME = "manifest.json"
WORKSPACE_RUNTIME_FILES = (
    "bin/create_task_v2.py",
    "bin/shared_state_v2.py",
    "bin/shared_state_fields.py",
)
WORKSPACE_RUNTIME_FILE_MODES = {
    "bin/create_task_v2.py": 0o755,
    "bin/shared_state_v2.py": 0o755,
    "bin/shared_state_fields.py": 0o644,
}
WORKSPACE_RUNTIME_IMPORT_CLOSURE = {
    "bin/create_task_v2.py": ["bin/shared_state_v2.py"],
    "bin/shared_state_v2.py": ["bin/shared_state_fields.py"],
    "bin/shared_state_fields.py": [],
}

_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_GIT_OBJECT_RE = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")
_GIT_COMMIT_RE = re.compile(r"[0-9a-f]{40}\Z")
_MAX_MANIFEST_BYTES = 256 * 1024
_MAX_RUNTIME_FILE_BYTES = 16 * 1024 * 1024


class WorkspaceRuntimeError(ValueError):
    """The canonical RCA creator bundle could not prove one exact identity."""

    def __init__(self, code: str, detail: str = ""):
        super().__init__(detail or code)
        self.code = code
        self.detail = detail or code


@dataclass(frozen=True)
class WorkspaceRuntimeIdentity:
    root: Path
    manifest_path: Path
    creator_path: Path
    manifest_sha256: str
    closure_sha256: str
    source_commit: str
    file_sha256: Mapping[str, str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": WORKSPACE_RUNTIME_IDENTITY_SCHEMA_VERSION,
            "root": str(self.root),
            "manifest_path": str(self.manifest_path),
            "creator_path": str(self.creator_path),
            "manifest_sha256": self.manifest_sha256,
            "closure_sha256": self.closure_sha256,
            "source_commit": self.source_commit,
            "file_sha256": dict(self.file_sha256),
        }

    def task_meta(self) -> dict[str, str]:
        return {
            "rca_workspace_runtime_manifest_sha256": self.manifest_sha256,
            "rca_workspace_runtime_closure_sha256": self.closure_sha256,
            "rca_workspace_runtime_source_commit": self.source_commit,
        }


@dataclass(frozen=True)
class _StableFile:
    path: Path
    raw: bytes
    stat_result: os.stat_result


def canonical_workspace_runtime_root(
    hermes_home: str | Path | None = None,
) -> Path:
    home = Path(hermes_home or get_hermes_home()).expanduser().absolute()
    return home / "runtime" / WORKSPACE_RUNTIME_DIRECTORY_NAME


def _canonical_json(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise WorkspaceRuntimeError("rca_workspace_runtime_manifest_invalid") from exc


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value)).hexdigest()


def _strict_json(raw: bytes) -> Mapping[str, Any]:
    if not raw or len(raw) > _MAX_MANIFEST_BYTES:
        raise WorkspaceRuntimeError("rca_workspace_runtime_manifest_size_invalid")

    def pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in values:
            if key in result:
                raise WorkspaceRuntimeError(
                    "rca_workspace_runtime_manifest_duplicate_key"
                )
            result[key] = value
        return result

    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=pairs,
            parse_constant=lambda _value: (_ for _ in ()).throw(
                WorkspaceRuntimeError("rca_workspace_runtime_manifest_number_invalid")
            ),
        )
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise WorkspaceRuntimeError("rca_workspace_runtime_manifest_json_invalid") from exc
    if not isinstance(value, dict):
        raise WorkspaceRuntimeError("rca_workspace_runtime_manifest_shape_invalid")
    return value


def _stat_identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_nlink,
        value.st_uid,
        value.st_gid,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _directory_identity(path: Path, *, mode: int) -> tuple[tuple[int, ...], tuple[str, ...]]:
    try:
        observed = path.lstat()
        names = tuple(sorted(os.listdir(path)))
        after = path.lstat()
    except OSError as exc:
        raise WorkspaceRuntimeError("rca_workspace_runtime_directory_unavailable") from exc
    if (
        stat.S_ISLNK(observed.st_mode)
        or not stat.S_ISDIR(observed.st_mode)
        or stat.S_IMODE(observed.st_mode) != mode
        or observed.st_uid != os.geteuid()
        or _stat_identity(observed) != _stat_identity(after)
    ):
        raise WorkspaceRuntimeError("rca_workspace_runtime_directory_identity_invalid")
    return _stat_identity(observed), names


def _read_stable_regular(path: Path, *, mode: int, limit: int) -> _StableFile:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise WorkspaceRuntimeError("rca_workspace_runtime_file_unavailable") from exc
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or stat.S_IMODE(before.st_mode) != mode
            or before.st_uid != os.geteuid()
            or before.st_nlink != 1
            or before.st_size < 0
            or before.st_size > limit
        ):
            raise WorkspaceRuntimeError("rca_workspace_runtime_file_identity_invalid")
        chunks: list[bytes] = []
        remaining = before.st_size
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                raise WorkspaceRuntimeError("rca_workspace_runtime_file_unstable")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise WorkspaceRuntimeError("rca_workspace_runtime_file_unstable")
        after = os.fstat(descriptor)
        lexical = path.lstat()
        if (
            stat.S_ISLNK(lexical.st_mode)
            or _stat_identity(before) != _stat_identity(after)
            or _stat_identity(before) != _stat_identity(lexical)
        ):
            raise WorkspaceRuntimeError("rca_workspace_runtime_file_unstable")
        return _StableFile(path=path, raw=b"".join(chunks), stat_result=before)
    except OSError as exc:
        raise WorkspaceRuntimeError("rca_workspace_runtime_file_unstable") from exc
    finally:
        os.close(descriptor)


def _descriptor(
    *,
    path: str,
    raw: bytes,
    mode: int,
    git_blob_oid: str,
) -> dict[str, Any]:
    return {
        "path": path,
        "sha256": hashlib.sha256(raw).hexdigest(),
        "size_bytes": len(raw),
        "mode": f"{mode:04o}",
        "git_blob_oid": git_blob_oid,
    }


def build_workspace_runtime_manifest(
    *,
    source_commit: str,
    files: Mapping[str, Mapping[str, Any]],
    imports: Mapping[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Build the deterministic manifest shared by the builder and runtime."""
    normalized_files = {path: dict(files[path]) for path in WORKSPACE_RUNTIME_FILES}
    normalized_imports = {
        path: list((imports or WORKSPACE_RUNTIME_IMPORT_CLOSURE)[path])
        for path in WORKSPACE_RUNTIME_FILES
    }
    closure = {
        "source_commit": source_commit,
        "files": normalized_files,
        "imports": normalized_imports,
    }
    return {
        "schema_version": WORKSPACE_RUNTIME_MANIFEST_SCHEMA_VERSION,
        "bundle_name": WORKSPACE_RUNTIME_DIRECTORY_NAME,
        "source_commit": source_commit,
        "files": normalized_files,
        "imports": normalized_imports,
        "closure_sha256": _sha256_json(closure),
    }


def workspace_runtime_descriptor(
    *,
    path: str,
    raw: bytes,
    git_blob_oid: str,
) -> dict[str, Any]:
    if path not in WORKSPACE_RUNTIME_FILE_MODES:
        raise WorkspaceRuntimeError("rca_workspace_runtime_file_set_invalid")
    return _descriptor(
        path=path,
        raw=raw,
        mode=WORKSPACE_RUNTIME_FILE_MODES[path],
        git_blob_oid=git_blob_oid,
    )


def _validated_manifest(value: Mapping[str, Any]) -> Mapping[str, Any]:
    if set(value) != {
        "schema_version",
        "bundle_name",
        "source_commit",
        "files",
        "imports",
        "closure_sha256",
    }:
        raise WorkspaceRuntimeError("rca_workspace_runtime_manifest_shape_invalid")
    source_commit = value.get("source_commit")
    files = value.get("files")
    imports = value.get("imports")
    if (
        value.get("schema_version") != WORKSPACE_RUNTIME_MANIFEST_SCHEMA_VERSION
        or value.get("bundle_name") != WORKSPACE_RUNTIME_DIRECTORY_NAME
        or not isinstance(source_commit, str)
        or _GIT_COMMIT_RE.fullmatch(source_commit) is None
        or not isinstance(files, dict)
        or set(files) != set(WORKSPACE_RUNTIME_FILES)
        or not isinstance(imports, dict)
        or imports != WORKSPACE_RUNTIME_IMPORT_CLOSURE
    ):
        raise WorkspaceRuntimeError("rca_workspace_runtime_manifest_binding_invalid")
    for path in WORKSPACE_RUNTIME_FILES:
        descriptor = files.get(path)
        if not isinstance(descriptor, dict) or set(descriptor) != {
            "path",
            "sha256",
            "size_bytes",
            "mode",
            "git_blob_oid",
        }:
            raise WorkspaceRuntimeError("rca_workspace_runtime_file_descriptor_invalid")
        if (
            descriptor.get("path") != path
            or _SHA256_RE.fullmatch(str(descriptor.get("sha256") or "")) is None
            or isinstance(descriptor.get("size_bytes"), bool)
            or not isinstance(descriptor.get("size_bytes"), int)
            or not 0 <= descriptor["size_bytes"] <= _MAX_RUNTIME_FILE_BYTES
            or descriptor.get("mode") != f"{WORKSPACE_RUNTIME_FILE_MODES[path]:04o}"
            or _GIT_OBJECT_RE.fullmatch(
                str(descriptor.get("git_blob_oid") or "")
            )
            is None
        ):
            raise WorkspaceRuntimeError("rca_workspace_runtime_file_descriptor_invalid")
    expected = build_workspace_runtime_manifest(
        source_commit=source_commit,
        files=files,
        imports=imports,
    )
    if value != expected:
        raise WorkspaceRuntimeError("rca_workspace_runtime_closure_invalid")
    return expected


def _validate_workspace_runtime_root(selected: Path) -> WorkspaceRuntimeIdentity:
    root_before, root_names = _directory_identity(selected, mode=0o700)
    if root_names != ("bin", WORKSPACE_RUNTIME_MANIFEST_NAME):
        raise WorkspaceRuntimeError("rca_workspace_runtime_extra_entry")
    bin_path = selected / "bin"
    bin_before, bin_names = _directory_identity(bin_path, mode=0o700)
    if bin_names != tuple(sorted(Path(path).name for path in WORKSPACE_RUNTIME_FILES)):
        raise WorkspaceRuntimeError("rca_workspace_runtime_extra_entry")

    manifest_path = selected / WORKSPACE_RUNTIME_MANIFEST_NAME
    manifest_file = _read_stable_regular(
        manifest_path,
        mode=0o600,
        limit=_MAX_MANIFEST_BYTES,
    )
    manifest = _validated_manifest(_strict_json(manifest_file.raw))
    stable_files: dict[str, _StableFile] = {}
    file_sha256: dict[str, str] = {}
    for relative in WORKSPACE_RUNTIME_FILES:
        observed = _read_stable_regular(
            selected / relative,
            mode=WORKSPACE_RUNTIME_FILE_MODES[relative],
            limit=_MAX_RUNTIME_FILE_BYTES,
        )
        descriptor = manifest["files"][relative]
        digest = hashlib.sha256(observed.raw).hexdigest()
        if digest != descriptor["sha256"] or len(observed.raw) != descriptor["size_bytes"]:
            raise WorkspaceRuntimeError("rca_workspace_runtime_file_hash_mismatch")
        stable_files[relative] = observed
        file_sha256[relative] = digest

    manifest_after = _read_stable_regular(
        manifest_path,
        mode=0o600,
        limit=_MAX_MANIFEST_BYTES,
    )
    if (
        manifest_after.raw != manifest_file.raw
        or _stat_identity(manifest_after.stat_result)
        != _stat_identity(manifest_file.stat_result)
    ):
        raise WorkspaceRuntimeError("rca_workspace_runtime_manifest_drift")
    for relative, observed in stable_files.items():
        try:
            after = (selected / relative).lstat()
        except OSError as exc:
            raise WorkspaceRuntimeError("rca_workspace_runtime_file_unstable") from exc
        if _stat_identity(after) != _stat_identity(observed.stat_result):
            raise WorkspaceRuntimeError("rca_workspace_runtime_file_unstable")
    root_after, root_names_after = _directory_identity(selected, mode=0o700)
    bin_after, bin_names_after = _directory_identity(bin_path, mode=0o700)
    if (
        root_after != root_before
        or bin_after != bin_before
        or root_names_after != root_names
        or bin_names_after != bin_names
    ):
        raise WorkspaceRuntimeError("rca_workspace_runtime_directory_drift")

    return WorkspaceRuntimeIdentity(
        root=selected,
        manifest_path=manifest_path,
        creator_path=selected / "bin" / "create_task_v2.py",
        manifest_sha256=hashlib.sha256(manifest_file.raw).hexdigest(),
        closure_sha256=str(manifest["closure_sha256"]),
        source_commit=str(manifest["source_commit"]),
        file_sha256=file_sha256,
    )


def validate_staged_workspace_runtime(
    root: str | Path,
) -> WorkspaceRuntimeIdentity:
    """Validate an explicit non-live staging root using the production contract."""
    return _validate_workspace_runtime_root(Path(root).expanduser().absolute())


def validate_workspace_runtime(
    root: str | Path | None = None,
    *,
    hermes_home: str | Path | None = None,
) -> WorkspaceRuntimeIdentity:
    """Stably hash the exact canonical bundle and return its immutable identity."""
    expected_root = canonical_workspace_runtime_root(hermes_home)
    selected = Path(root).expanduser().absolute() if root is not None else expected_root
    if selected != expected_root:
        raise WorkspaceRuntimeError("rca_workspace_runtime_root_not_canonical")
    return _validate_workspace_runtime_root(selected)
