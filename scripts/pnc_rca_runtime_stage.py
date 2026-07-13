#!/usr/bin/env python3
"""Plan or stage a future RCA Host runtime without touching live state."""

from __future__ import annotations

import argparse
import ast
import contextlib
import fcntl
import hashlib
import json
import os
import plistlib
import re
import shutil
import stat
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any


PLAN_SCHEMA_VERSION = "pnc_rca_runtime_stage_plan_v1"
MANIFEST_SCHEMA_VERSION = "pnc_rca_runtime_stage_manifest_v1"
RECEIPT_SCHEMA_VERSION = "pnc_rca_runtime_build_receipt_v2"
MANIFEST_FILENAME = "runtime-stage-manifest.json"
RUNTIME_IDENTITY_RELATIVE_PATH = "gateway/pnc_rca_runtime_identity.py"
CANONICAL_LIVE_ROOT = Path("/Users/songying/.hermes/runtime/hermes-live")
MIN_FREE_RESERVE_BYTES = 64 * 1024 * 1024
MAX_JSON_BYTES = 64 * 1024 * 1024
MAX_SOURCE_FILE_BYTES = 64 * 1024 * 1024
MAX_SOURCE_FILES = 10_000
MAX_SOURCE_TOTAL_BYTES = 512 * 1024 * 1024
MAX_VENV_FILE_BYTES = 256 * 1024 * 1024
MAX_VENV_FILES = 100_000

CANDIDATE_PLISTS = {
    "ai.hermes.gateway.candidate.plist": ("ai.hermes.gateway", ""),
    "local.pnc.completion-notice-relay.candidate.plist": (
        "local.pnc.completion-notice-relay",
        "pnc_completion_notice_relay.py",
    ),
    "local.pnc.rca-delivery-collector.candidate.plist": (
        "local.pnc.rca-delivery-collector",
        "pnc_rca_delivery_collector.py",
    ),
    "local.pnc.rca-delivery-dispatcher.candidate.plist": (
        "local.pnc.rca-delivery-dispatcher",
        "pnc_rca_delivery_dispatcher.py",
    ),
    "local.pnc.rca-kafka-consumer.candidate.plist": (
        "local.pnc.rca-kafka-consumer",
        "pnc_rca_kafka_consumer.py",
    ),
    "local.pnc.rca-outbox-dispatcher.candidate.plist": (
        "local.pnc.rca-outbox-dispatcher",
        "pnc_rca_outbox_dispatcher.py",
    ),
    "local.pnc.vm-task-sync.candidate.plist": (
        "local.pnc.vm-task-sync",
        "pnc_vm_task_sync.py",
    ),
}
CANDIDATE_PLIST_ARGUMENTS = {
    "ai.hermes.gateway.candidate.plist": (
        "{runtime}/.venv/bin/python",
        "-m",
        "hermes_cli.main",
        "gateway",
        "run",
        "--replace",
    ),
    "local.pnc.completion-notice-relay.candidate.plist": (
        "{runtime}/.venv/bin/python",
        "{runtime}/scripts/pnc_completion_notice_relay.py",
        "--send",
        "--watch",
        "--limit",
        "50",
        "--retry-failed-after",
        "600",
        "--max-attempts",
        "3",
        "--watch-canary-loops",
        "3",
        "--max-card-fallbacks-per-loop",
        "0",
        "--json",
    ),
    "local.pnc.rca-delivery-collector.candidate.plist": (
        "{runtime}/.venv/bin/python",
        "{runtime}/scripts/pnc_rca_delivery_collector.py",
    ),
    "local.pnc.rca-delivery-dispatcher.candidate.plist": (
        "{runtime}/.venv/bin/python",
        "{runtime}/scripts/pnc_rca_delivery_dispatcher.py",
    ),
    "local.pnc.rca-kafka-consumer.candidate.plist": (
        "{runtime}/.venv/bin/python",
        "{runtime}/scripts/pnc_rca_kafka_consumer.py",
    ),
    "local.pnc.rca-outbox-dispatcher.candidate.plist": (
        "{runtime}/.venv/bin/python",
        "{runtime}/scripts/pnc_rca_outbox_dispatcher.py",
    ),
    "local.pnc.vm-task-sync.candidate.plist": (
        "{runtime}/.venv/bin/python",
        "{runtime}/scripts/pnc_vm_task_sync.py",
        "--limit",
        "50",
        "--include-terminal",
        "--json",
    ),
}
RECEIPT_KEYS = {
    "schema_version",
    "observed_at",
    "venv_path",
    "python_version",
    "python_executable",
    "uv_version",
    "uv_lock_sha256",
    "pyproject_sha256",
    "requirements_sha256",
    "profile_extras",
    "python_no_user_site_required",
    "project_installed",
    "site_packages",
    "installed_distributions",
    "installed_distributions_sha256",
    "critical_versions",
}
FORBIDDEN_CACHE_PARTS = {
    "__pycache__",
    ".cache",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".lock",
    ".test-home",
    ".test-tmp",
    "test-home",
    "test-tmp",
}
FORBIDDEN_SECRET_NAMES = {
    ".env",
    ".env.local",
    ".env.production",
    "credentials.json",
    "secrets.json",
    "id_rsa",
    "id_ed25519",
}
ALLOWED_VENV_SYMLINKS = {
    PurePosixPath("bin/python"),
    PurePosixPath("bin/python3"),
    PurePosixPath("bin/python3.11"),
}
SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
GIT_OBJECT_RE = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")
GIT_COMMIT_RE = re.compile(r"[0-9a-f]{40}\Z")


class RuntimeStageError(ValueError):
    def __init__(self, code: str, detail: str = ""):
        super().__init__(detail or code)
        self.code = code
        self.detail = detail or code


@dataclass(frozen=True)
class RuntimeStageResult:
    phase: str
    staging_root: Path
    artifact_path: Path
    body: Mapping[str, Any]
    resumed: bool


@dataclass(frozen=True)
class _StableFile:
    path: Path
    raw: bytes
    stat_result: os.stat_result

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.raw).hexdigest()


@dataclass(frozen=True)
class _SourceSnapshot:
    root: Path
    commit: str
    tree: str
    runtime_files: Mapping[str, _StableFile]
    runtime_descriptors: Mapping[str, Mapping[str, Any]]
    plist_files: Mapping[str, _StableFile]
    plist_descriptors: Mapping[str, Mapping[str, Any]]
    rendered_plists: Mapping[str, bytes]
    rendered_plist_descriptors: Mapping[str, Mapping[str, Any]]
    build_inputs: Mapping[str, Mapping[str, Any]]


@dataclass(frozen=True)
class _VenvSnapshot:
    root: Path
    receipt: _StableFile
    receipt_body: Mapping[str, Any]
    directories: Mapping[str, int]
    files: Mapping[str, _StableFile]
    descriptors: Mapping[str, Mapping[str, Any]]
    probe: Mapping[str, Any]


VenvProbe = Callable[[Path], Mapping[str, Any]]
CopyHook = Callable[[str, Path | None], None]


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
        raise RuntimeStageError("runtime_stage_json_invalid") from exc


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).rstrip(b"\n")).hexdigest()


def _strict_json(raw: bytes, *, artifact: str) -> Mapping[str, Any]:
    if not raw or len(raw) > MAX_JSON_BYTES:
        raise RuntimeStageError(f"{artifact}_size_invalid")

    def pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in values:
            if key in result:
                raise RuntimeStageError(f"{artifact}_duplicate_key")
            result[key] = value
        return result

    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=pairs,
            parse_constant=lambda _value: (_ for _ in ()).throw(
                RuntimeStageError(f"{artifact}_number_invalid")
            ),
        )
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise RuntimeStageError(f"{artifact}_json_invalid") from exc
    if not isinstance(value, dict):
        raise RuntimeStageError(f"{artifact}_shape_invalid")
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


def _read_stable_file(
    path: Path,
    *,
    artifact: str,
    max_bytes: int,
    expected_mode: int | None = None,
    require_single_link: bool = True,
) -> _StableFile:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise RuntimeStageError(f"{artifact}_unavailable") from exc
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.geteuid()
            or (require_single_link and before.st_nlink != 1)
            or before.st_size < 0
            or before.st_size > max_bytes
            or (
                expected_mode is not None
                and stat.S_IMODE(before.st_mode) != expected_mode
            )
        ):
            raise RuntimeStageError(f"{artifact}_identity_invalid")
        chunks: list[bytes] = []
        remaining = before.st_size
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                raise RuntimeStageError(f"{artifact}_unstable")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise RuntimeStageError(f"{artifact}_unstable")
        after = os.fstat(descriptor)
        lexical = path.lstat()
        if (
            stat.S_ISLNK(lexical.st_mode)
            or _stat_identity(before) != _stat_identity(after)
            or _stat_identity(before) != _stat_identity(lexical)
        ):
            raise RuntimeStageError(f"{artifact}_unstable")
        return _StableFile(path, b"".join(chunks), before)
    except OSError as exc:
        raise RuntimeStageError(f"{artifact}_unstable") from exc
    finally:
        os.close(descriptor)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _git(repo: Path, *arguments: str, binary: bool = False) -> str | bytes:
    try:
        result = subprocess.run(
            ["git", "-C", str(repo), *arguments],
            check=False,
            capture_output=True,
            text=not binary,
            timeout=120,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise RuntimeStageError("runtime_stage_git_unavailable") from exc
    if result.returncode != 0:
        raise RuntimeStageError("runtime_stage_git_failed")
    return result.stdout if binary else str(result.stdout).strip()


def _safe_relative(value: str, *, artifact: str) -> str:
    path = PurePosixPath(str(value or ""))
    if (
        path.is_absolute()
        or not path.parts
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise RuntimeStageError(f"{artifact}_path_invalid")
    return str(path)


def _descriptor(
    *,
    relative: str,
    raw: bytes,
    mode: int,
    git_blob: str = "",
    source_kind: str = "regular",
) -> dict[str, Any]:
    result = {
        "path": relative,
        "sha256": hashlib.sha256(raw).hexdigest(),
        "size_bytes": len(raw),
        "mode": f"{mode:04o}",
        "source_kind": source_kind,
    }
    if git_blob:
        result["git_blob"] = git_blob
    return result


def _runtime_paths_from_identity(raw: bytes) -> tuple[str, ...]:
    try:
        tree = ast.parse(raw.decode("utf-8"))
    except (UnicodeError, SyntaxError) as exc:
        raise RuntimeStageError("runtime_stage_identity_source_invalid") from exc
    values: dict[str, tuple[str, ...]] = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        names = [target.id for target in node.targets if isinstance(target, ast.Name)]
        selected = next(
            (
                name
                for name in names
                if name
                in {"RCA_RUNTIME_RELATIVE_FILES", "GATEWAY_RCA_RUNTIME_RELATIVE_FILES"}
            ),
            "",
        )
        if not selected:
            continue
        try:
            literal = ast.literal_eval(node.value)
        except (TypeError, ValueError) as exc:
            raise RuntimeStageError("runtime_stage_identity_closure_invalid") from exc
        if not isinstance(literal, tuple) or not all(
            isinstance(item, str) for item in literal
        ):
            raise RuntimeStageError("runtime_stage_identity_closure_invalid")
        values[selected] = tuple(literal)
    if set(values) != {"RCA_RUNTIME_RELATIVE_FILES", "GATEWAY_RCA_RUNTIME_RELATIVE_FILES"}:
        raise RuntimeStageError("runtime_stage_identity_closure_missing")
    result = tuple(
        sorted(
            {
                _safe_relative(item, artifact="runtime_stage_runtime")
                for group in values.values()
                for item in group
            }
        )
    )
    if RUNTIME_IDENTITY_RELATIVE_PATH not in result:
        raise RuntimeStageError("runtime_stage_identity_closure_invalid")
    return result


def _git_blob_oid(raw: bytes, *, object_format: str) -> str:
    payload = f"blob {len(raw)}\0".encode("ascii") + raw
    if object_format == "sha1":
        return hashlib.sha1(payload, usedforsecurity=False).hexdigest()
    if object_format == "sha256":
        return hashlib.sha256(payload).hexdigest()
    raise RuntimeStageError("runtime_stage_git_object_format_invalid")


def _tracked_runtime_tree(
    source: Path,
    *,
    commit: str,
) -> tuple[dict[str, _StableFile], dict[str, Mapping[str, Any]]]:
    object_format = str(_git(source, "rev-parse", "--show-object-format"))
    if object_format not in {"sha1", "sha256"}:
        raise RuntimeStageError("runtime_stage_git_object_format_invalid")
    raw_tree = bytes(
        _git(
            source,
            "ls-tree",
            "-rlz",
            "--full-tree",
            commit,
            binary=True,
        )
    )
    excluded = set(CANDIDATE_PLISTS) | {"pyproject.toml", "uv.lock"}
    files: dict[str, _StableFile] = {}
    descriptors: dict[str, Mapping[str, Any]] = {}
    total_bytes = 0
    records = [record for record in raw_tree.split(b"\0") if record]
    if not records or len(records) > MAX_SOURCE_FILES + len(excluded):
        raise RuntimeStageError("runtime_stage_source_tree_capacity_invalid")
    for record in records:
        try:
            mode_raw, kind_raw, oid_raw, tail = record.split(b" ", 3)
            size_raw, relative_raw = tail.split(b"\t", 1)
            mode = mode_raw.decode("ascii")
            kind = kind_raw.decode("ascii")
            oid = oid_raw.decode("ascii")
            declared_size = int(size_raw.decode("ascii"))
            relative = relative_raw.decode("utf-8")
        except (UnicodeDecodeError, ValueError) as exc:
            raise RuntimeStageError("runtime_stage_source_tree_invalid") from exc
        relative = _safe_relative(relative, artifact="runtime_stage_source_tree")
        if relative in excluded:
            continue
        expected_mode = {"100644": 0o644, "100755": 0o755}.get(mode)
        expected_oid_length = 40 if object_format == "sha1" else 64
        if (
            kind != "blob"
            or expected_mode is None
            or len(oid) != expected_oid_length
            or any(character not in "0123456789abcdef" for character in oid)
            or declared_size < 0
            or declared_size > MAX_SOURCE_FILE_BYTES
            or relative in files
        ):
            raise RuntimeStageError("runtime_stage_source_tree_invalid", relative)
        observed = _read_stable_file(
            source / relative,
            artifact="runtime_stage_source_file",
            max_bytes=MAX_SOURCE_FILE_BYTES,
            expected_mode=expected_mode,
        )
        if (
            len(observed.raw) != declared_size
            or _git_blob_oid(observed.raw, object_format=object_format) != oid
        ):
            raise RuntimeStageError("runtime_stage_source_blob_mismatch", relative)
        total_bytes += declared_size
        if total_bytes > MAX_SOURCE_TOTAL_BYTES or len(files) >= MAX_SOURCE_FILES:
            raise RuntimeStageError("runtime_stage_source_tree_capacity_invalid")
        files[relative] = observed
        descriptors[relative] = _descriptor(
            relative=relative,
            raw=observed.raw,
            mode=expected_mode,
            git_blob=oid,
        )
    if not files:
        raise RuntimeStageError("runtime_stage_source_tree_invalid")
    return files, descriptors


def _source_file(
    root: Path,
    *,
    relative: str,
    commit: str,
) -> tuple[_StableFile, Mapping[str, Any]]:
    line = str(_git(root, "ls-tree", commit, "--", relative))
    try:
        prefix, tracked = line.split("\t", 1)
        git_mode, kind, blob = prefix.split(" ", 2)
    except ValueError as exc:
        raise RuntimeStageError("runtime_stage_source_untracked", relative) from exc
    if (
        tracked != relative
        or kind != "blob"
        or git_mode not in {"100644", "100755"}
        or GIT_OBJECT_RE.fullmatch(blob) is None
    ):
        raise RuntimeStageError("runtime_stage_source_untracked", relative)
    mode = 0o755 if git_mode == "100755" else 0o644
    observed = _read_stable_file(
        root / relative,
        artifact="runtime_stage_source_file",
        max_bytes=MAX_SOURCE_FILE_BYTES,
        expected_mode=mode,
    )
    committed = _git(root, "cat-file", "blob", f"{commit}:{relative}", binary=True)
    if committed != observed.raw:
        raise RuntimeStageError("runtime_stage_source_blob_mismatch", relative)
    return observed, _descriptor(
        relative=relative,
        raw=observed.raw,
        mode=mode,
        git_blob=blob,
    )


def _project_plist_value(value: Any, *, physical_root: Path) -> Any:
    if isinstance(value, dict):
        return {
            str(key): _project_plist_value(item, physical_root=physical_root)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_project_plist_value(item, physical_root=physical_root) for item in value]
    if not isinstance(value, str):
        return value
    canonical = str(CANONICAL_LIVE_ROOT)
    if value == canonical:
        return str(physical_root)
    if value.startswith(f"{canonical}/"):
        return f"{physical_root}{value[len(canonical):]}"
    return value.replace(f"{canonical}/.venv/bin:", f"{physical_root}/.venv/bin:")


def _expected_plist_arguments(filename: str, runtime_root: Path) -> list[str]:
    try:
        templates = CANDIDATE_PLIST_ARGUMENTS[filename]
    except KeyError as exc:
        raise RuntimeStageError("runtime_stage_plist_invalid", filename) from exc
    root = str(runtime_root)
    return [template.replace("{runtime}", root) for template in templates]


def _render_plist(
    raw: bytes,
    *,
    filename: str,
    staging_root: Path,
) -> tuple[bytes, str]:
    try:
        body = plistlib.loads(raw)
    except (ValueError, plistlib.InvalidFileException) as exc:
        raise RuntimeStageError("runtime_stage_plist_invalid", filename) from exc
    expected_label, _expected_script = CANDIDATE_PLISTS[filename]
    if not isinstance(body, dict) or body.get("Label") != expected_label:
        raise RuntimeStageError("runtime_stage_plist_invalid", filename)
    arguments = body.get("ProgramArguments")
    if not isinstance(arguments, list) or not arguments:
        raise RuntimeStageError("runtime_stage_plist_invalid", filename)
    projected = _project_plist_value(body, physical_root=staging_root)
    environment = projected.get("EnvironmentVariables")
    if not isinstance(environment, dict) or any(
        not isinstance(key, str) or not isinstance(value, str)
        for key, value in environment.items()
    ):
        raise RuntimeStageError("runtime_stage_plist_invalid", filename)
    projected["EnvironmentVariables"] = {
        **environment,
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    projected_args = projected.get("ProgramArguments")
    if (
        not isinstance(projected_args, list)
        or projected_args != _expected_plist_arguments(filename, staging_root)
        or projected.get("WorkingDirectory") != str(staging_root)
    ):
        raise RuntimeStageError("runtime_stage_plist_projection_invalid", filename)
    return (
        plistlib.dumps(projected, fmt=plistlib.FMT_XML, sort_keys=True),
        _sha256_json(body),
    )


def _source_snapshot(root: Path, staging_root: Path) -> _SourceSnapshot:
    lexical = root.expanduser().absolute()
    try:
        root_identity = lexical.lstat()
    except OSError as exc:
        raise RuntimeStageError("runtime_stage_source_root_invalid") from exc
    if stat.S_ISLNK(root_identity.st_mode) or not stat.S_ISDIR(root_identity.st_mode):
        raise RuntimeStageError("runtime_stage_source_root_invalid")
    source = lexical.resolve()
    if Path(str(_git(source, "rev-parse", "--show-toplevel"))).resolve() != source:
        raise RuntimeStageError("runtime_stage_source_root_invalid")
    status = str(_git(source, "status", "--porcelain=v1", "--untracked-files=all"))
    if status:
        raise RuntimeStageError("runtime_stage_source_dirty")
    commit = str(_git(source, "rev-parse", "--verify", "HEAD"))
    tree = str(_git(source, "rev-parse", "HEAD^{tree}"))
    if GIT_COMMIT_RE.fullmatch(commit) is None or GIT_OBJECT_RE.fullmatch(tree) is None:
        raise RuntimeStageError("runtime_stage_source_commit_invalid")
    identity, _identity_descriptor = _source_file(
        source,
        relative=RUNTIME_IDENTITY_RELATIVE_PATH,
        commit=commit,
    )
    declared_runtime_paths = _runtime_paths_from_identity(identity.raw)
    runtime_files, runtime_descriptors = _tracked_runtime_tree(
        source,
        commit=commit,
    )
    if not set(declared_runtime_paths).issubset(runtime_files):
        raise RuntimeStageError("runtime_stage_identity_closure_invalid")
    plist_files: dict[str, _StableFile] = {}
    plist_descriptors: dict[str, Mapping[str, Any]] = {}
    rendered_plists: dict[str, bytes] = {}
    rendered_descriptors: dict[str, Mapping[str, Any]] = {}
    for filename in sorted(CANDIDATE_PLISTS):
        observed, descriptor = _source_file(
            source,
            relative=filename,
            commit=commit,
        )
        rendered, canonical_body_sha256 = _render_plist(
            observed.raw,
            filename=filename,
            staging_root=staging_root,
        )
        plist_files[filename] = observed
        plist_descriptors[filename] = {
            **descriptor,
            "canonical_body_sha256": canonical_body_sha256,
        }
        rendered_plists[filename] = rendered
        rendered_descriptors[filename] = _descriptor(
            relative=filename,
            raw=rendered,
            mode=int(str(descriptor["mode"]), 8),
            source_kind="rendered_plist",
        )
    build_inputs: dict[str, Mapping[str, Any]] = {}
    for relative in ("pyproject.toml", "uv.lock"):
        observed, descriptor = _source_file(
            source,
            relative=relative,
            commit=commit,
        )
        build_inputs[relative] = descriptor
        if observed.sha256 != descriptor["sha256"]:
            raise RuntimeStageError("runtime_stage_build_input_invalid", relative)
    if (
        str(_git(source, "rev-parse", "--verify", "HEAD")) != commit
        or str(_git(source, "rev-parse", "HEAD^{tree}")) != tree
        or str(_git(source, "status", "--porcelain=v1", "--untracked-files=all"))
    ):
        raise RuntimeStageError("runtime_stage_source_changed")
    return _SourceSnapshot(
        source,
        commit,
        tree,
        runtime_files,
        runtime_descriptors,
        plist_files,
        plist_descriptors,
        rendered_plists,
        rendered_descriptors,
        build_inputs,
    )


def _default_venv_probe(venv: Path) -> Mapping[str, Any]:
    interpreter = venv / "bin" / "python"
    script = r"""
import hashlib
import json
import re
import site
import sys
from importlib import metadata

distributions = {}
for dist in metadata.distributions():
    name = re.sub(
        r"[-_.]+",
        "-",
        str(dist.metadata.get("Name") or "").strip().lower(),
    )
    if name:
        distributions[name] = str(dist.version)
raw = json.dumps(distributions, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode()
print(json.dumps({
    "python_version": ".".join(str(item) for item in sys.version_info[:3]),
    "python_executable": sys.executable,
    "prefix": sys.prefix,
    "base_prefix": sys.base_prefix,
    "user_site_enabled": site.ENABLE_USER_SITE,
    "site_packages": site.getsitepackages(),
    "installed_distributions": distributions,
    "installed_distributions_sha256": hashlib.sha256(raw).hexdigest(),
}, ensure_ascii=True, sort_keys=True, separators=(",", ":")))
"""
    environment = {
        "PATH": "/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin",
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONNOUSERSITE": "1",
        "VIRTUAL_ENV": str(venv),
    }
    try:
        result = subprocess.run(
            [str(interpreter), "-I", "-B", "-c", script],
            check=False,
            capture_output=True,
            text=True,
            timeout=60,
            env=environment,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise RuntimeStageError("runtime_stage_venv_probe_failed") from exc
    if result.returncode != 0:
        raise RuntimeStageError("runtime_stage_venv_probe_failed")
    return _strict_json(
        result.stdout.encode("utf-8"),
        artifact="runtime_stage_venv_probe",
    )


def _valid_sha256(value: Any) -> bool:
    return isinstance(value, str) and SHA256_RE.fullmatch(value) is not None


def _validate_receipt(
    body: Mapping[str, Any],
    *,
    receipt: _StableFile,
    source: _SourceSnapshot,
    probe: Mapping[str, Any],
) -> None:
    if set(body) != RECEIPT_KEYS or body.get("schema_version") != RECEIPT_SCHEMA_VERSION:
        raise RuntimeStageError("runtime_stage_venv_receipt_shape_invalid")
    venv = receipt.path.parent
    expected_python = venv / "bin" / "python"
    installed = body.get("installed_distributions")
    critical = body.get("critical_versions")
    site_packages = body.get("site_packages")
    profile_extras = body.get("profile_extras")
    try:
        observed_at = datetime.fromisoformat(
            str(body.get("observed_at") or "").replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise RuntimeStageError("runtime_stage_venv_receipt_binding_invalid") from exc
    if (
        receipt.path.name != "rca-runtime-build-receipt.json"
        or body.get("venv_path") != str(venv)
        or body.get("python_executable") != str(expected_python)
        or observed_at.tzinfo is None
        or observed_at.utcoffset() is None
        or not isinstance(body.get("python_version"), str)
        or re.fullmatch(r"\d+\.\d+\.\d+", body["python_version"]) is None
        or not isinstance(body.get("uv_version"), str)
        or not body["uv_version"].strip()
        or body.get("python_no_user_site_required") is not True
        or body.get("project_installed") is not False
        or not isinstance(profile_extras, list)
        or not all(isinstance(item, str) and item for item in profile_extras)
        or profile_extras != sorted(set(profile_extras))
        or not isinstance(installed, dict)
        or not installed
        or not all(
            isinstance(name, str)
            and name
            and isinstance(version, str)
            and version
            for name, version in installed.items()
        )
        or not isinstance(critical, dict)
        or not critical
        or any(installed.get(name) != version for name, version in critical.items())
        or not isinstance(site_packages, list)
        or not site_packages
        or not all(isinstance(item, str) and item.startswith(f"{venv}/") for item in site_packages)
        or not all(
            _valid_sha256(body.get(field))
            for field in (
                "uv_lock_sha256",
                "pyproject_sha256",
                "requirements_sha256",
                "installed_distributions_sha256",
            )
        )
    ):
        raise RuntimeStageError("runtime_stage_venv_receipt_binding_invalid")
    if (
        body["uv_lock_sha256"] != source.build_inputs["uv.lock"]["sha256"]
        or body["pyproject_sha256"]
        != source.build_inputs["pyproject.toml"]["sha256"]
        or body["installed_distributions_sha256"] != _sha256_json(installed)
        or probe.get("installed_distributions") != installed
        or probe.get("installed_distributions_sha256")
        != body["installed_distributions_sha256"]
        or probe.get("python_version") != body.get("python_version")
        or probe.get("python_executable") != str(expected_python)
        or probe.get("prefix") != str(venv)
        or probe.get("base_prefix") == str(venv)
        or probe.get("user_site_enabled") not in {False, None}
        or probe.get("site_packages") != site_packages
    ):
        raise RuntimeStageError("runtime_stage_venv_receipt_drift")


def _secret_or_cache(relative: PurePosixPath) -> tuple[bool, bool]:
    cache = any(part in FORBIDDEN_CACHE_PARTS for part in relative.parts) or (
        relative.suffix in {".pyc", ".pyo"}
    )
    secret = relative.name.lower() in FORBIDDEN_SECRET_NAMES
    return secret, cache


def _venv_snapshot(
    receipt_path: Path,
    *,
    source: _SourceSnapshot,
    probe_observer: VenvProbe,
) -> _VenvSnapshot:
    receipt = _read_stable_file(
        receipt_path.expanduser().absolute(),
        artifact="runtime_stage_venv_receipt",
        max_bytes=MAX_JSON_BYTES,
        expected_mode=0o600,
    )
    body = _strict_json(receipt.raw, artifact="runtime_stage_venv_receipt")
    venv = receipt.path.parent
    try:
        root_identity = venv.lstat()
    except OSError as exc:
        raise RuntimeStageError("runtime_stage_venv_root_invalid") from exc
    if (
        stat.S_ISLNK(root_identity.st_mode)
        or not stat.S_ISDIR(root_identity.st_mode)
        or root_identity.st_uid != os.geteuid()
        or stat.S_IMODE(root_identity.st_mode) != 0o755
        or venv.resolve() != venv
    ):
        raise RuntimeStageError("runtime_stage_venv_root_invalid")
    probe = dict(probe_observer(venv))
    _validate_receipt(body, receipt=receipt, source=source, probe=probe)
    directories: dict[str, int] = {"": 0o755}
    files: dict[str, _StableFile] = {}
    descriptors: dict[str, Mapping[str, Any]] = {}
    for current, raw_dirs, raw_files in os.walk(venv, topdown=True, followlinks=False):
        directory = Path(current)
        relative_directory = directory.relative_to(venv)
        kept_dirs = []
        for name in sorted(raw_dirs):
            relative = PurePosixPath(*(relative_directory.parts + (name,)))
            secret, cache = _secret_or_cache(relative)
            if secret:
                raise RuntimeStageError("runtime_stage_secret_forbidden", str(relative))
            if cache:
                continue
            path = directory / name
            identity = path.lstat()
            if (
                stat.S_ISLNK(identity.st_mode)
                or not stat.S_ISDIR(identity.st_mode)
                or identity.st_uid != os.geteuid()
                or stat.S_IMODE(identity.st_mode) & 0o022
            ):
                raise RuntimeStageError("runtime_stage_venv_directory_invalid")
            kept_dirs.append(name)
            directories[str(relative)] = stat.S_IMODE(identity.st_mode)
        raw_dirs[:] = kept_dirs
        for name in sorted(raw_files):
            relative_path = PurePosixPath(*(relative_directory.parts + (name,)))
            relative = str(relative_path)
            secret, cache = _secret_or_cache(relative_path)
            if secret:
                raise RuntimeStageError("runtime_stage_secret_forbidden", relative)
            if cache:
                continue
            path = directory / name
            lexical = path.lstat()
            source_kind = "regular"
            if stat.S_ISLNK(lexical.st_mode):
                if relative_path not in ALLOWED_VENV_SYMLINKS:
                    raise RuntimeStageError("runtime_stage_venv_symlink_forbidden", relative)
                try:
                    target = path.resolve(strict=True)
                except OSError as exc:
                    raise RuntimeStageError("runtime_stage_venv_symlink_invalid") from exc
                observed = _read_stable_file(
                    target,
                    artifact="runtime_stage_venv_symlink_target",
                    max_bytes=MAX_VENV_FILE_BYTES,
                    require_single_link=False,
                )
                source_kind = "symlink_dereferenced"
                mode = 0o755
            else:
                observed = _read_stable_file(
                    path,
                    artifact="runtime_stage_venv_file",
                    max_bytes=MAX_VENV_FILE_BYTES,
                )
                mode = stat.S_IMODE(observed.stat_result.st_mode)
                if mode & 0o022:
                    raise RuntimeStageError("runtime_stage_venv_file_mode_invalid")
            files[relative] = observed
            descriptors[relative] = _descriptor(
                relative=relative,
                raw=observed.raw,
                mode=mode,
                source_kind=source_kind,
            )
            if len(files) > MAX_VENV_FILES:
                raise RuntimeStageError("runtime_stage_venv_file_count_exceeded")
    receipt_after = _read_stable_file(
        receipt.path,
        artifact="runtime_stage_venv_receipt",
        max_bytes=MAX_JSON_BYTES,
        expected_mode=0o600,
    )
    if (
        receipt_after.raw != receipt.raw
        or _stat_identity(receipt_after.stat_result) != _stat_identity(receipt.stat_result)
    ):
        raise RuntimeStageError("runtime_stage_venv_receipt_drift")
    return _VenvSnapshot(
        venv,
        receipt,
        body,
        directories,
        files,
        descriptors,
        probe,
    )


def _plan_path(staging_root: Path) -> Path:
    return staging_root.parent / f".{staging_root.name}.runtime-stage-plan.json"


def _lock_path(staging_root: Path) -> Path:
    return staging_root.parent / f".{staging_root.name}.runtime-stage.lock"


def _validate_staging_path(staging_root: Path) -> Path:
    raw = staging_root.expanduser()
    if not raw.is_absolute() or ".." in raw.parts:
        raise RuntimeStageError("runtime_stage_path_invalid")
    stage = raw.absolute()
    parent = stage.parent
    try:
        parent_identity = parent.lstat()
        canonical_parent_identity = CANONICAL_LIVE_ROOT.parent.lstat()
    except OSError as exc:
        raise RuntimeStageError("runtime_stage_parent_invalid") from exc
    if (
        stat.S_ISLNK(parent_identity.st_mode)
        or not stat.S_ISDIR(parent_identity.st_mode)
        or parent.resolve() != parent
        or parent_identity.st_dev != canonical_parent_identity.st_dev
    ):
        raise RuntimeStageError("runtime_stage_parent_invalid")
    unsafe = stage == CANONICAL_LIVE_ROOT
    for child, ancestor in (
        (stage, CANONICAL_LIVE_ROOT),
        (CANONICAL_LIVE_ROOT, stage),
    ):
        try:
            child.relative_to(ancestor)
        except ValueError:
            continue
        unsafe = True
    if unsafe:
        raise RuntimeStageError("runtime_stage_live_path_forbidden")
    if stage.exists() or stage.is_symlink():
        identity = stage.lstat()
        if (
            stat.S_ISLNK(identity.st_mode)
            or not stat.S_ISDIR(identity.st_mode)
            or stat.S_IMODE(identity.st_mode) != 0o700
            or identity.st_uid != os.geteuid()
            or stage.resolve() != stage
        ):
            raise RuntimeStageError("runtime_stage_root_invalid")
    return stage


@contextlib.contextmanager
def _stage_lock(stage: Path):
    path = _lock_path(stage)
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags, 0o600)
    except OSError as exc:
        raise RuntimeStageError("runtime_stage_lock_invalid") from exc
    try:
        identity = os.fstat(descriptor)
        if (
            not stat.S_ISREG(identity.st_mode)
            or stat.S_IMODE(identity.st_mode) != 0o600
            or identity.st_uid != os.geteuid()
            or identity.st_nlink != 1
        ):
            raise RuntimeStageError("runtime_stage_lock_invalid")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeStageError("runtime_stage_in_progress") from exc
        yield
    finally:
        with contextlib.suppress(OSError):
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _plan_body(
    *,
    stage: Path,
    source: _SourceSnapshot,
    venv: _VenvSnapshot,
) -> dict[str, Any]:
    runtime_bytes = sum(item["size_bytes"] for item in source.runtime_descriptors.values())
    plist_bytes = sum(item["size_bytes"] for item in source.rendered_plist_descriptors.values())
    venv_bytes = sum(item["size_bytes"] for item in venv.descriptors.values())
    content = {
        "source": {
            "repo_root": str(source.root),
            "commit": source.commit,
            "tree": source.tree,
            "runtime_files": dict(source.runtime_descriptors),
            "build_inputs": dict(source.build_inputs),
        },
        "candidate_plists": {
            filename: {
                "source": source.plist_descriptors[filename],
                "staged": source.rendered_plist_descriptors[filename],
            }
            for filename in sorted(CANDIDATE_PLISTS)
        },
        "venv": {
            "source_root": str(venv.root),
            "receipt_path": str(venv.receipt.path),
            "receipt_sha256": venv.receipt.sha256,
            "receipt_schema_version": RECEIPT_SCHEMA_VERSION,
            "interpreter_sha256": venv.descriptors["bin/python"]["sha256"],
            "installed_distributions": venv.receipt_body["installed_distributions"],
            "installed_distributions_sha256": venv.receipt_body[
                "installed_distributions_sha256"
            ],
            "python_version": venv.receipt_body["python_version"],
            "directories": {
                path: f"{mode:04o}" for path, mode in venv.directories.items()
            },
            "files": dict(venv.descriptors),
        },
    }
    content_sha256 = _sha256_json(content)
    estimated = runtime_bytes + plist_bytes + venv_bytes
    reserve = max(MIN_FREE_RESERVE_BYTES, estimated // 10)
    projection = {
        "canonical_live_root": str(CANONICAL_LIVE_ROOT),
        "source_commit": source.commit,
        "source_tree": source.tree,
        "python_executable": str(CANONICAL_LIVE_ROOT / ".venv" / "bin" / "python"),
        "runtime_files_sha256": _sha256_json({
            path: descriptor["sha256"]
            for path, descriptor in source.runtime_descriptors.items()
        }),
        "candidate_plist_sha256": {
            filename: source.plist_descriptors[filename]["sha256"]
            for filename in sorted(CANDIDATE_PLISTS)
        },
        "content_sha256": content_sha256,
    }
    return {
        "schema_version": PLAN_SCHEMA_VERSION,
        "phase": "plan",
        "complete": True,
        "production_effects_executed": False,
        "live_install_supported": False,
        "staging_root": str(stage),
        "content": content,
        "content_sha256": content_sha256,
        "space_budget": {
            "estimated_copy_bytes": estimated,
            "required_free_bytes": estimated + reserve,
            "reserve_bytes": reserve,
        },
        "future_canonical_projection": projection,
    }


def _publish_file(path: Path, raw: bytes, *, mode: int) -> bool:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags, mode)
    except FileExistsError:
        observed = _read_stable_file(
            path,
            artifact="runtime_stage_existing_file",
            max_bytes=max(MAX_JSON_BYTES, len(raw)),
            expected_mode=mode,
        )
        if observed.raw != raw:
            raise RuntimeStageError("runtime_stage_content_conflict", str(path))
        return True
    try:
        os.fchmod(descriptor, mode)
        view = memoryview(raw)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise RuntimeStageError("runtime_stage_write_failed")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    _fsync_directory(path.parent)
    return False


def _ensure_directory(path: Path, *, mode: int) -> bool:
    try:
        os.mkdir(path, mode)
        created = True
        os.chmod(path, mode)
        _fsync_directory(path.parent)
    except FileExistsError:
        created = False
    identity = path.lstat()
    if (
        stat.S_ISLNK(identity.st_mode)
        or not stat.S_ISDIR(identity.st_mode)
        or stat.S_IMODE(identity.st_mode) != mode
        or identity.st_uid != os.geteuid()
    ):
        raise RuntimeStageError("runtime_stage_directory_conflict", str(path))
    return created


def _expected_stage_layout(plan: Mapping[str, Any]) -> tuple[dict[str, int], dict[str, Mapping[str, Any]]]:
    content = plan["content"]
    directories: dict[str, int] = {"": 0o700}
    files: dict[str, Mapping[str, Any]] = {}
    for relative, descriptor in content["source"]["runtime_files"].items():
        files[relative] = descriptor
        parent = PurePosixPath(relative).parent
        while str(parent) != ".":
            directories.setdefault(str(parent), 0o700)
            parent = parent.parent
    for filename, descriptor in content["candidate_plists"].items():
        files[filename] = descriptor["staged"]
    directories[".venv"] = 0o755
    for relative, raw_mode in content["venv"]["directories"].items():
        path = ".venv" if not relative else f".venv/{relative}"
        directories[path] = int(raw_mode, 8)
    for relative, descriptor in content["venv"]["files"].items():
        files[f".venv/{relative}"] = descriptor
    return directories, files


def _enumerate_stage(stage: Path) -> tuple[set[str], set[str]]:
    directories = {""}
    files: set[str] = set()
    for current, raw_dirs, raw_files in os.walk(stage, topdown=True, followlinks=False):
        root = Path(current)
        for name in raw_dirs:
            path = root / name
            relative = str(PurePosixPath(*path.relative_to(stage).parts))
            if path.is_symlink():
                raise RuntimeStageError("runtime_stage_symlink_forbidden", relative)
            directories.add(relative)
        for name in raw_files:
            path = root / name
            relative = str(PurePosixPath(*path.relative_to(stage).parts))
            if path.is_symlink():
                raise RuntimeStageError("runtime_stage_symlink_forbidden", relative)
            files.add(relative)
    return directories, files


def _manifest_body(plan: Mapping[str, Any]) -> dict[str, Any]:
    payload = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "complete": True,
        "production_effects_executed": False,
        "live_install_performed": False,
        "staging_root": plan["staging_root"],
        "plan_sha256": _sha256_json(plan),
        "content": plan["content"],
        "content_sha256": plan["content_sha256"],
        "future_canonical_projection": plan["future_canonical_projection"],
    }
    return payload


def _validate_probe_for_stage(
    probe: Mapping[str, Any],
    *,
    stage: Path,
    venv: Mapping[str, Any],
) -> None:
    expected_venv = stage / ".venv"
    expected_sites = [
        str(expected_venv / Path(path).relative_to(Path(venv["source_root"])))
        for path in venv.get("site_packages", [])
    ] if venv.get("site_packages") else []
    if (
        probe.get("python_executable") != str(expected_venv / "bin" / "python")
        or probe.get("prefix") != str(expected_venv)
        or probe.get("base_prefix") == str(expected_venv)
        or probe.get("installed_distributions") != venv["installed_distributions"]
        or probe.get("installed_distributions_sha256")
        != venv["installed_distributions_sha256"]
        or probe.get("python_version") != venv["python_version"]
        or probe.get("user_site_enabled") not in {False, None}
        or (expected_sites and probe.get("site_packages") != expected_sites)
    ):
        raise RuntimeStageError("runtime_stage_staged_venv_probe_invalid")


def _validate_manifest_descriptor(
    value: Any,
    *,
    relative: str,
    require_git: bool,
    extra_keys: set[str] | None = None,
) -> None:
    if not isinstance(value, Mapping):
        raise RuntimeStageError("runtime_stage_manifest_descriptor_invalid")
    expected = {"path", "sha256", "size_bytes", "mode", "source_kind"}
    if require_git:
        expected.add("git_blob")
    expected.update(extra_keys or set())
    if (
        set(value) != expected
        or value.get("path") != relative
        or not _valid_sha256(value.get("sha256"))
        or isinstance(value.get("size_bytes"), bool)
        or not isinstance(value.get("size_bytes"), int)
        or not 0 <= value["size_bytes"] <= MAX_VENV_FILE_BYTES
        or value.get("mode") not in {"0600", "0644", "0700", "0755"}
        or not isinstance(value.get("source_kind"), str)
        or not value["source_kind"]
        or (require_git and GIT_OBJECT_RE.fullmatch(str(value.get("git_blob") or "")) is None)
        or any(
            not _valid_sha256(value.get(key))
            for key in (extra_keys or set())
        )
    ):
        raise RuntimeStageError("runtime_stage_manifest_descriptor_invalid")


def _validate_manifest_content(
    content: Any,
    projection: Any,
) -> None:
    if not isinstance(content, Mapping) or set(content) != {
        "source",
        "candidate_plists",
        "venv",
    }:
        raise RuntimeStageError("runtime_stage_manifest_content_invalid")
    source = content.get("source")
    plists = content.get("candidate_plists")
    venv = content.get("venv")
    if (
        not isinstance(source, Mapping)
        or set(source) != {"repo_root", "commit", "tree", "runtime_files", "build_inputs"}
        or not isinstance(source.get("repo_root"), str)
        or not Path(source["repo_root"]).is_absolute()
        or GIT_COMMIT_RE.fullmatch(str(source.get("commit") or "")) is None
        or GIT_OBJECT_RE.fullmatch(str(source.get("tree") or "")) is None
        or not isinstance(source.get("runtime_files"), Mapping)
        or not source["runtime_files"]
        or not isinstance(source.get("build_inputs"), Mapping)
        or set(source["build_inputs"]) != {"pyproject.toml", "uv.lock"}
        or not isinstance(plists, Mapping)
        or set(plists) != set(CANDIDATE_PLISTS)
        or not isinstance(venv, Mapping)
        or set(venv)
        != {
            "source_root",
            "receipt_path",
            "receipt_sha256",
            "receipt_schema_version",
            "interpreter_sha256",
            "installed_distributions",
            "installed_distributions_sha256",
            "python_version",
            "directories",
            "files",
        }
    ):
        raise RuntimeStageError("runtime_stage_manifest_content_invalid")
    for relative, descriptor in source["runtime_files"].items():
        _safe_relative(relative, artifact="runtime_stage_manifest")
        _validate_manifest_descriptor(
            descriptor,
            relative=relative,
            require_git=True,
        )
    for relative, descriptor in source["build_inputs"].items():
        _validate_manifest_descriptor(
            descriptor,
            relative=relative,
            require_git=True,
        )
    for filename, descriptors in plists.items():
        if not isinstance(descriptors, Mapping) or set(descriptors) != {"source", "staged"}:
            raise RuntimeStageError("runtime_stage_manifest_content_invalid")
        _validate_manifest_descriptor(
            descriptors["source"],
            relative=filename,
            require_git=True,
            extra_keys={"canonical_body_sha256"},
        )
        _validate_manifest_descriptor(
            descriptors["staged"],
            relative=filename,
            require_git=False,
        )
    directories = venv.get("directories")
    files = venv.get("files")
    installed = venv.get("installed_distributions")
    if (
        not isinstance(venv.get("source_root"), str)
        or not Path(venv["source_root"]).is_absolute()
        or not isinstance(venv.get("receipt_path"), str)
        or not Path(venv["receipt_path"]).is_absolute()
        or venv.get("receipt_schema_version") != RECEIPT_SCHEMA_VERSION
        or not _valid_sha256(venv.get("receipt_sha256"))
        or not _valid_sha256(venv.get("interpreter_sha256"))
        or not _valid_sha256(venv.get("installed_distributions_sha256"))
        or not isinstance(installed, Mapping)
        or not installed
        or venv["installed_distributions_sha256"] != _sha256_json(installed)
        or not isinstance(venv.get("python_version"), str)
        or not isinstance(directories, Mapping)
        or directories.get("") != "0755"
        or not isinstance(files, Mapping)
        or "bin/python" not in files
        or "rca-runtime-build-receipt.json" not in files
    ):
        raise RuntimeStageError("runtime_stage_manifest_venv_invalid")
    for relative, mode in directories.items():
        if relative:
            _safe_relative(relative, artifact="runtime_stage_manifest_venv")
        if mode not in {"0700", "0755"}:
            raise RuntimeStageError("runtime_stage_manifest_venv_invalid")
    for relative, descriptor in files.items():
        _safe_relative(relative, artifact="runtime_stage_manifest_venv")
        _validate_manifest_descriptor(
            descriptor,
            relative=relative,
            require_git=False,
        )
    if not isinstance(projection, Mapping) or set(projection) != {
        "canonical_live_root",
        "source_commit",
        "source_tree",
        "python_executable",
        "runtime_files_sha256",
        "candidate_plist_sha256",
        "content_sha256",
    } or (
        projection.get("canonical_live_root") != str(CANONICAL_LIVE_ROOT)
        or projection.get("source_commit") != source["commit"]
        or projection.get("source_tree") != source["tree"]
        or projection.get("python_executable")
        != str(CANONICAL_LIVE_ROOT / ".venv" / "bin" / "python")
        or not all(
            _valid_sha256(projection.get(field))
            for field in ("runtime_files_sha256", "content_sha256")
        )
        or not isinstance(projection.get("candidate_plist_sha256"), Mapping)
        or set(projection["candidate_plist_sha256"]) != set(CANDIDATE_PLISTS)
    ):
        raise RuntimeStageError("runtime_stage_manifest_projection_invalid")


def validate_staged_runtime(
    staging_root: str | Path,
    *,
    expected_plan: Mapping[str, Any] | None = None,
    venv_probe: VenvProbe = _default_venv_probe,
) -> Mapping[str, Any]:
    stage = _validate_staging_path(Path(staging_root))
    manifest_file = _read_stable_file(
        stage / MANIFEST_FILENAME,
        artifact="runtime_stage_manifest",
        max_bytes=MAX_JSON_BYTES,
        expected_mode=0o600,
    )
    manifest = _strict_json(manifest_file.raw, artifact="runtime_stage_manifest")
    if set(manifest) != {
        "schema_version",
        "complete",
        "production_effects_executed",
        "live_install_performed",
        "staging_root",
        "plan_sha256",
        "content",
        "content_sha256",
        "future_canonical_projection",
    } or (
        manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION
        or manifest.get("complete") is not True
        or manifest.get("production_effects_executed") is not False
        or manifest.get("live_install_performed") is not False
        or manifest.get("staging_root") != str(stage)
        or not _valid_sha256(manifest.get("plan_sha256"))
        or manifest.get("content_sha256") != _sha256_json(manifest.get("content"))
    ):
        raise RuntimeStageError("runtime_stage_manifest_invalid")
    _validate_manifest_content(
        manifest["content"],
        manifest["future_canonical_projection"],
    )
    if manifest["future_canonical_projection"]["content_sha256"] != manifest[
        "content_sha256"
    ]:
        raise RuntimeStageError("runtime_stage_manifest_projection_invalid")
    if expected_plan is not None and manifest != _manifest_body(expected_plan):
        raise RuntimeStageError("runtime_stage_manifest_plan_mismatch")
    plan_view = {
        "content": manifest["content"],
    }
    expected_dirs, expected_files = _expected_stage_layout(plan_view)
    expected_files[MANIFEST_FILENAME] = {
        "path": MANIFEST_FILENAME,
        "sha256": manifest_file.sha256,
        "size_bytes": len(manifest_file.raw),
        "mode": "0600",
        "source_kind": "manifest",
    }
    actual_dirs, actual_files = _enumerate_stage(stage)
    if actual_dirs != set(expected_dirs) or actual_files != set(expected_files):
        raise RuntimeStageError("runtime_stage_extra_or_missing_entry")
    for relative, mode in expected_dirs.items():
        identity = (stage if not relative else stage / relative).lstat()
        if (
            not stat.S_ISDIR(identity.st_mode)
            or stat.S_IMODE(identity.st_mode) != mode
            or identity.st_uid != os.geteuid()
        ):
            raise RuntimeStageError("runtime_stage_directory_identity_invalid")
    for relative, descriptor in expected_files.items():
        observed = _read_stable_file(
            stage / relative,
            artifact="runtime_stage_staged_file",
            max_bytes=max(MAX_VENV_FILE_BYTES, descriptor["size_bytes"]),
            expected_mode=int(descriptor["mode"], 8),
        )
        if (
            observed.sha256 != descriptor["sha256"]
            or len(observed.raw) != descriptor["size_bytes"]
        ):
            raise RuntimeStageError("runtime_stage_staged_file_mismatch", relative)
    venv = dict(manifest["content"]["venv"])
    receipt_source = Path(str(venv["source_root"]))
    receipt_body = _strict_json(
        (stage / ".venv" / "rca-runtime-build-receipt.json").read_bytes(),
        artifact="runtime_stage_staged_receipt",
    )
    venv["site_packages"] = receipt_body.get("site_packages", [])
    venv["source_root"] = str(receipt_source)
    _validate_probe_for_stage(
        dict(venv_probe(stage / ".venv")),
        stage=stage,
        venv=venv,
    )
    actual_dirs_after, actual_files_after = _enumerate_stage(stage)
    if actual_dirs_after != set(expected_dirs) or actual_files_after != set(
        expected_files
    ):
        raise RuntimeStageError("runtime_stage_changed_during_probe")
    for relative, descriptor in expected_files.items():
        observed = _read_stable_file(
            stage / relative,
            artifact="runtime_stage_staged_file",
            max_bytes=max(MAX_VENV_FILE_BYTES, descriptor["size_bytes"]),
            expected_mode=int(descriptor["mode"], 8),
        )
        if (
            observed.sha256 != descriptor["sha256"]
            or len(observed.raw) != descriptor["size_bytes"]
        ):
            raise RuntimeStageError("runtime_stage_changed_during_probe", relative)
    manifest_after = _read_stable_file(
        stage / MANIFEST_FILENAME,
        artifact="runtime_stage_manifest",
        max_bytes=MAX_JSON_BYTES,
        expected_mode=0o600,
    )
    if (
        manifest_after.raw != manifest_file.raw
        or _stat_identity(manifest_after.stat_result)
        != _stat_identity(manifest_file.stat_result)
    ):
        raise RuntimeStageError("runtime_stage_manifest_drift")
    return manifest


def _copy_stage(
    *,
    stage: Path,
    plan: Mapping[str, Any],
    source: _SourceSnapshot,
    venv: _VenvSnapshot,
    copy_hook: CopyHook | None,
) -> bool:
    resumed = not _ensure_directory(stage, mode=0o700)
    expected_dirs, expected_files = _expected_stage_layout(plan)
    if stage.exists():
        actual_dirs, actual_files = _enumerate_stage(stage)
        allowed_files = set(expected_files) | {MANIFEST_FILENAME}
        if not actual_dirs <= set(expected_dirs) or not actual_files <= allowed_files:
            raise RuntimeStageError("runtime_stage_partial_conflict")
    for relative, mode in sorted(
        expected_dirs.items(), key=lambda item: (len(PurePosixPath(item[0]).parts), item[0])
    ):
        if relative:
            resumed |= not _ensure_directory(stage / relative, mode=mode)
    raw_by_path: dict[str, tuple[bytes, Path | None]] = {}
    for relative, observed in source.runtime_files.items():
        raw_by_path[relative] = (observed.raw, observed.path)
    for filename, raw in source.rendered_plists.items():
        raw_by_path[filename] = (raw, source.plist_files[filename].path)
    for relative, observed in venv.files.items():
        raw_by_path[f".venv/{relative}"] = (observed.raw, observed.path)
    for relative in sorted(expected_files):
        descriptor = expected_files[relative]
        raw, source_path = raw_by_path[relative]
        resumed |= _publish_file(
            stage / relative,
            raw,
            mode=int(descriptor["mode"], 8),
        )
        if copy_hook is not None:
            try:
                copy_hook(relative, source_path)
            except RuntimeStageError:
                raise
            except Exception as exc:
                raise RuntimeStageError("runtime_stage_copy_interrupted") from exc
    return resumed


def run_runtime_stage(
    *,
    phase: str,
    source_candidate: str | Path,
    venv_receipt: str | Path,
    staging_root: str | Path,
    disk_usage_observer: Callable[[str | os.PathLike[str]], Any] = shutil.disk_usage,
    venv_probe: VenvProbe = _default_venv_probe,
    copy_hook: CopyHook | None = None,
) -> RuntimeStageResult:
    if phase not in {"plan", "stage"}:
        raise RuntimeStageError("runtime_stage_phase_invalid")
    stage = _validate_staging_path(Path(staging_root))
    with _stage_lock(stage):
        source = _source_snapshot(Path(source_candidate), stage)
        venv = _venv_snapshot(
            Path(venv_receipt),
            source=source,
            probe_observer=venv_probe,
        )
        plan = _plan_body(stage=stage, source=source, venv=venv)
        plan_path = _plan_path(stage)
        resumed = _publish_file(plan_path, _canonical_json(plan), mode=0o600)
        if phase == "plan":
            return RuntimeStageResult(phase, stage, plan_path, plan, resumed)
        usage = disk_usage_observer(stage.parent)
        free = getattr(usage, "free", None)
        if isinstance(free, bool) or not isinstance(free, int) or free < plan[
            "space_budget"
        ]["required_free_bytes"]:
            raise RuntimeStageError("runtime_stage_insufficient_space")
        manifest_path = stage / MANIFEST_FILENAME
        if manifest_path.exists():
            manifest = validate_staged_runtime(
                stage,
                expected_plan=plan,
                venv_probe=venv_probe,
            )
            return RuntimeStageResult("stage", stage, manifest_path, manifest, True)
        resumed |= _copy_stage(
            stage=stage,
            plan=plan,
            source=source,
            venv=venv,
            copy_hook=copy_hook,
        )
        source_after = _source_snapshot(source.root, stage)
        venv_after = _venv_snapshot(
            venv.receipt.path,
            source=source_after,
            probe_observer=venv_probe,
        )
        if (
            _plan_body(stage=stage, source=source_after, venv=venv_after) != plan
            or _read_stable_file(
                plan_path,
                artifact="runtime_stage_plan",
                max_bytes=MAX_JSON_BYTES,
                expected_mode=0o600,
            ).raw
            != _canonical_json(plan)
        ):
            raise RuntimeStageError("runtime_stage_input_changed_during_copy")
        manifest = _manifest_body(plan)
        _publish_file(manifest_path, _canonical_json(manifest), mode=0o600)
        validated = validate_staged_runtime(
            stage,
            expected_plan=plan,
            venv_probe=venv_probe,
        )
        return RuntimeStageResult("stage", stage, manifest_path, validated, resumed)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=("plan", "stage"), required=True)
    parser.add_argument("--source-candidate", type=Path, required=True)
    parser.add_argument("--venv-receipt", type=Path, required=True)
    parser.add_argument("--staging-root", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = run_runtime_stage(
            phase=args.phase,
            source_candidate=args.source_candidate,
            venv_receipt=args.venv_receipt,
            staging_root=args.staging_root,
        )
    except (OSError, ValueError) as exc:
        code = getattr(exc, "code", "runtime_stage_failed")
        print(json.dumps({"ok": False, "code": code}, sort_keys=True))
        return 2
    print(
        json.dumps(
            {
                "ok": True,
                "phase": result.phase,
                "artifact": str(result.artifact_path),
                "resumed": result.resumed,
                "production_effects_executed": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
