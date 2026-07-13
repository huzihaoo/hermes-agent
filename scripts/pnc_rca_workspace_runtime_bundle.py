#!/usr/bin/env python3
"""Plan or stage the fixed RCA workspace creator bundle; never install it live."""

from __future__ import annotations

import argparse
import ast
import contextlib
import fcntl
import hashlib
import json
import os
import stat
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from gateway.pnc_rca_workspace_runtime import (
    WORKSPACE_RUNTIME_FILES,
    WORKSPACE_RUNTIME_FILE_MODES,
    WORKSPACE_RUNTIME_IMPORT_CLOSURE,
    WORKSPACE_RUNTIME_MANIFEST_NAME,
    WorkspaceRuntimeError,
    build_workspace_runtime_manifest,
    canonical_workspace_runtime_root,
    validate_staged_workspace_runtime,
    workspace_runtime_descriptor,
)


PLAN_SCHEMA_VERSION = "pnc_rca_workspace_runtime_bundle_plan_v1"
PLAN_FILENAME = "workspace-runtime-bundle-plan.json"
STAGED_BUNDLE_DIRECTORY = "bundle"
LOCK_FILENAME = ".workspace-runtime-bundle.lock"
_MAX_SOURCE_BYTES = 16 * 1024 * 1024


class WorkspaceRuntimeBundleError(ValueError):
    def __init__(self, code: str, detail: str = ""):
        super().__init__(detail or code)
        self.code = code
        self.detail = detail or code


@dataclass(frozen=True)
class BundleResult:
    phase: str
    output_dir: Path
    artifact_path: Path
    body: Mapping[str, Any]
    resumed: bool


@dataclass(frozen=True)
class _SourceObservation:
    source_root: Path
    source_commit: str
    files: Mapping[str, bytes]
    descriptors: Mapping[str, Mapping[str, Any]]
    imports: Mapping[str, list[str]]


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
        raise WorkspaceRuntimeBundleError("rca_workspace_bundle_json_invalid") from exc


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _ensure_owner_directory(path: Path, *, create: bool) -> tuple[Path, bool]:
    selected = path.expanduser().absolute()
    created = False
    if create:
        if not selected.parent.is_dir() or selected.parent.is_symlink():
            raise WorkspaceRuntimeBundleError("rca_workspace_bundle_parent_invalid")
        try:
            os.mkdir(selected, 0o700)
            created = True
            _fsync_directory(selected.parent)
        except FileExistsError:
            pass
    try:
        observed = selected.lstat()
    except OSError as exc:
        raise WorkspaceRuntimeBundleError("rca_workspace_bundle_output_unavailable") from exc
    if (
        stat.S_ISLNK(observed.st_mode)
        or not stat.S_ISDIR(observed.st_mode)
        or stat.S_IMODE(observed.st_mode) != 0o700
        or observed.st_uid != os.geteuid()
    ):
        raise WorkspaceRuntimeBundleError("rca_workspace_bundle_output_identity_invalid")
    return selected, created


@contextlib.contextmanager
def _output_lock(root: Path):
    descriptor = os.open(
        root / LOCK_FILENAME,
        os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0),
        0o600,
    )
    try:
        observed = os.fstat(descriptor)
        if (
            not stat.S_ISREG(observed.st_mode)
            or stat.S_IMODE(observed.st_mode) != 0o600
            or observed.st_uid != os.geteuid()
            or observed.st_nlink != 1
        ):
            raise WorkspaceRuntimeBundleError("rca_workspace_bundle_lock_invalid")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise WorkspaceRuntimeBundleError("rca_workspace_bundle_in_progress") from exc
        yield
    finally:
        with contextlib.suppress(OSError):
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _write_no_clobber(path: Path, raw: bytes, *, mode: int) -> bool:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags, mode)
    except FileExistsError:
        try:
            observed = path.lstat()
            existing = path.read_bytes()
        except OSError as exc:
            raise WorkspaceRuntimeBundleError("rca_workspace_bundle_artifact_conflict") from exc
        if (
            stat.S_ISLNK(observed.st_mode)
            or not stat.S_ISREG(observed.st_mode)
            or stat.S_IMODE(observed.st_mode) != mode
            or observed.st_uid != os.geteuid()
            or observed.st_nlink != 1
            or existing != raw
        ):
            raise WorkspaceRuntimeBundleError("rca_workspace_bundle_artifact_conflict")
        return True
    try:
        os.fchmod(descriptor, mode)
        view = memoryview(raw)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise WorkspaceRuntimeBundleError("rca_workspace_bundle_write_failed")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    _fsync_directory(path.parent)
    return False


def _git(repo: Path, *arguments: str, binary: bool = False) -> str | bytes:
    try:
        result = subprocess.run(
            ["git", "-C", str(repo), *arguments],
            check=False,
            capture_output=True,
            text=not binary,
            timeout=15,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise WorkspaceRuntimeBundleError("rca_workspace_bundle_git_unavailable") from exc
    if result.returncode != 0:
        raise WorkspaceRuntimeBundleError("rca_workspace_bundle_git_failed")
    return result.stdout if binary else str(result.stdout).strip()


def _read_source_file(path: Path, *, mode: int) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise WorkspaceRuntimeBundleError("rca_workspace_bundle_source_unavailable") from exc
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or stat.S_IMODE(before.st_mode) != mode
            or before.st_uid != os.geteuid()
            or before.st_nlink != 1
            or not 0 <= before.st_size <= _MAX_SOURCE_BYTES
        ):
            raise WorkspaceRuntimeBundleError("rca_workspace_bundle_source_identity_invalid")
        raw = b""
        remaining = before.st_size
        chunks: list[bytes] = []
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                raise WorkspaceRuntimeBundleError("rca_workspace_bundle_source_unstable")
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b"".join(chunks)
        after = os.fstat(descriptor)
        lexical = path.lstat()
        fields = ("st_dev", "st_ino", "st_mode", "st_nlink", "st_uid", "st_size", "st_mtime_ns", "st_ctime_ns")
        if stat.S_ISLNK(lexical.st_mode) or any(
            getattr(before, field) != getattr(after, field)
            or getattr(before, field) != getattr(lexical, field)
            for field in fields
        ):
            raise WorkspaceRuntimeBundleError("rca_workspace_bundle_source_unstable")
        return raw
    finally:
        os.close(descriptor)


def _local_import_closure(
    files: Mapping[str, bytes],
    *,
    local_module_paths: Mapping[str, str],
) -> Mapping[str, list[str]]:
    result: dict[str, list[str]] = {}
    for path in WORKSPACE_RUNTIME_FILES:
        try:
            tree = ast.parse(files[path].decode("utf-8"))
        except (UnicodeError, SyntaxError) as exc:
            raise WorkspaceRuntimeBundleError("rca_workspace_bundle_source_syntax_invalid") from exc
        dependencies: set[str] = set()
        for node in ast.walk(tree):
            modules: list[str] = []
            if isinstance(node, ast.Import):
                modules.extend(alias.name.split(".", 1)[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                modules.append(node.module.split(".", 1)[0])
            for module in modules:
                target = local_module_paths.get(module)
                if target and target != path:
                    dependencies.add(target)
        result[path] = sorted(dependencies)
    if result != WORKSPACE_RUNTIME_IMPORT_CLOSURE:
        raise WorkspaceRuntimeBundleError("rca_workspace_bundle_import_closure_invalid")
    return result


def observe_source_candidate(source_candidate: str | Path) -> _SourceObservation:
    lexical_source = Path(source_candidate).expanduser().absolute()
    try:
        lexical_identity = lexical_source.lstat()
    except OSError as exc:
        raise WorkspaceRuntimeBundleError(
            "rca_workspace_bundle_source_root_invalid"
        ) from exc
    if stat.S_ISLNK(lexical_identity.st_mode):
        raise WorkspaceRuntimeBundleError("rca_workspace_bundle_source_root_invalid")
    source = lexical_source.resolve()
    if not source.is_dir():
        raise WorkspaceRuntimeBundleError("rca_workspace_bundle_source_root_invalid")
    git_root = Path(str(_git(source, "rev-parse", "--show-toplevel"))).resolve()
    if git_root != source:
        raise WorkspaceRuntimeBundleError("rca_workspace_bundle_source_root_invalid")
    before = str(_git(source, "rev-parse", "--verify", "HEAD"))
    if not re_full_commit(before):
        raise WorkspaceRuntimeBundleError("rca_workspace_bundle_source_commit_invalid")
    if str(_git(source, "status", "--porcelain=v1", "--untracked-files=all")):
        raise WorkspaceRuntimeBundleError("rca_workspace_bundle_source_dirty")
    files: dict[str, bytes] = {}
    descriptors: dict[str, Mapping[str, Any]] = {}
    tracked_bin_files = str(_git(source, "ls-files", "--", "bin/*.py")).splitlines()
    local_module_paths: dict[str, str] = {}
    for tracked_path in tracked_bin_files:
        module = Path(tracked_path).stem
        if module in local_module_paths and local_module_paths[module] != tracked_path:
            raise WorkspaceRuntimeBundleError(
                "rca_workspace_bundle_local_module_ambiguous"
            )
        local_module_paths[module] = tracked_path
    for path in WORKSPACE_RUNTIME_FILES:
        expected_git_mode = (
            "100755" if WORKSPACE_RUNTIME_FILE_MODES[path] == 0o755 else "100644"
        )
        line = str(_git(source, "ls-tree", before, "--", path))
        try:
            prefix, tracked_path = line.split("\t", 1)
            git_mode, object_kind, blob_oid = prefix.split(" ", 2)
        except ValueError as exc:
            raise WorkspaceRuntimeBundleError("rca_workspace_bundle_source_tree_invalid") from exc
        if (
            tracked_path != path
            or git_mode != expected_git_mode
            or object_kind != "blob"
            or len(blob_oid) not in {40, 64}
        ):
            raise WorkspaceRuntimeBundleError("rca_workspace_bundle_source_tree_invalid")
        raw = _read_source_file(
            source / path,
            mode=WORKSPACE_RUNTIME_FILE_MODES[path],
        )
        committed = _git(source, "cat-file", "blob", f"{before}:{path}", binary=True)
        if raw != committed:
            raise WorkspaceRuntimeBundleError("rca_workspace_bundle_source_blob_mismatch")
        files[path] = raw
        descriptors[path] = workspace_runtime_descriptor(
            path=path,
            raw=raw,
            git_blob_oid=blob_oid,
        )
    imports = _local_import_closure(
        files,
        local_module_paths=local_module_paths,
    )
    after = str(_git(source, "rev-parse", "--verify", "HEAD"))
    if after != before or str(
        _git(source, "status", "--porcelain=v1", "--untracked-files=all")
    ):
        raise WorkspaceRuntimeBundleError("rca_workspace_bundle_source_changed")
    return _SourceObservation(source, before, files, descriptors, imports)


def re_full_commit(value: str) -> bool:
    return len(value) == 40 and all(character in "0123456789abcdef" for character in value)


def _plan_body(observation: _SourceObservation, *, output_dir: Path) -> dict[str, Any]:
    manifest = build_workspace_runtime_manifest(
        source_commit=observation.source_commit,
        files=observation.descriptors,
        imports=observation.imports,
    )
    return {
        "schema_version": PLAN_SCHEMA_VERSION,
        "phase": "plan",
        "production_effects_executed": False,
        "live_install_supported": False,
        "source_candidate": str(observation.source_root),
        "source_commit": observation.source_commit,
        "output_dir": str(output_dir),
        "staged_bundle_path": str(output_dir / STAGED_BUNDLE_DIRECTORY),
        "manifest": manifest,
        "manifest_sha256": hashlib.sha256(_canonical_json(manifest)).hexdigest(),
    }


def _load_exact_plan(path: Path, expected: Mapping[str, Any]) -> bool:
    try:
        observed = path.lstat()
        raw = path.read_bytes()
    except OSError:
        return False
    if (
        stat.S_ISLNK(observed.st_mode)
        or not stat.S_ISREG(observed.st_mode)
        or stat.S_IMODE(observed.st_mode) != 0o600
        or observed.st_uid != os.geteuid()
        or observed.st_nlink != 1
        or raw != _canonical_json(expected)
    ):
        raise WorkspaceRuntimeBundleError("rca_workspace_bundle_plan_conflict")
    return True


def _stage_bundle(
    *,
    output_dir: Path,
    observation: _SourceObservation,
    expected_manifest: Mapping[str, Any],
) -> tuple[Path, bool]:
    bundle = output_dir / STAGED_BUNDLE_DIRECTORY
    if bundle.exists() or bundle.is_symlink():
        try:
            identity = validate_staged_workspace_runtime(bundle)
        except WorkspaceRuntimeError as exc:
            raise WorkspaceRuntimeBundleError(
                "rca_workspace_bundle_stage_conflict", exc.code
            ) from exc
        expected_raw = _canonical_json(expected_manifest)
        if identity.manifest_sha256 != hashlib.sha256(expected_raw).hexdigest():
            raise WorkspaceRuntimeBundleError("rca_workspace_bundle_stage_conflict")
        return bundle, True
    try:
        os.mkdir(bundle, 0o700)
        os.mkdir(bundle / "bin", 0o700)
    except OSError as exc:
        raise WorkspaceRuntimeBundleError("rca_workspace_bundle_stage_create_failed") from exc
    _fsync_directory(output_dir)
    _fsync_directory(bundle)
    for path in WORKSPACE_RUNTIME_FILES:
        _write_no_clobber(
            bundle / path,
            observation.files[path],
            mode=WORKSPACE_RUNTIME_FILE_MODES[path],
        )
    second = observe_source_candidate(observation.source_root)
    if (
        second.source_commit != observation.source_commit
        or second.descriptors != observation.descriptors
        or second.imports != observation.imports
    ):
        raise WorkspaceRuntimeBundleError("rca_workspace_bundle_source_changed")
    _write_no_clobber(
        bundle / WORKSPACE_RUNTIME_MANIFEST_NAME,
        _canonical_json(expected_manifest),
        mode=0o600,
    )
    validate_staged_workspace_runtime(bundle)
    return bundle, False


def run_workspace_runtime_bundle(
    *,
    phase: str,
    source_candidate: str | Path,
    output_dir: str | Path,
    hermes_home: str | Path | None = None,
) -> BundleResult:
    if phase not in {"plan", "stage"}:
        raise WorkspaceRuntimeBundleError("rca_workspace_bundle_phase_invalid")
    output = Path(output_dir).expanduser().absolute()
    canonical = canonical_workspace_runtime_root(hermes_home)
    try:
        output.relative_to(canonical)
        unsafe = True
    except ValueError:
        try:
            canonical.relative_to(output)
            unsafe = True
        except ValueError:
            unsafe = False
    if unsafe:
        raise WorkspaceRuntimeBundleError("rca_workspace_bundle_live_output_forbidden")
    output, created = _ensure_owner_directory(output, create=True)
    with _output_lock(output):
        observation = observe_source_candidate(source_candidate)
        plan = _plan_body(observation, output_dir=output)
        plan_path = output / PLAN_FILENAME
        resumed = not created
        if _load_exact_plan(plan_path, plan):
            resumed = True
        else:
            resumed |= _write_no_clobber(
                plan_path,
                _canonical_json(plan),
                mode=0o600,
            )
        if phase == "plan":
            return BundleResult(phase, output, plan_path, plan, resumed)
        bundle, stage_resumed = _stage_bundle(
            output_dir=output,
            observation=observation,
            expected_manifest=plan["manifest"],
        )
        identity = validate_staged_workspace_runtime(bundle)
        return BundleResult(
            phase,
            output,
            bundle,
            identity.to_dict(),
            resumed or stage_resumed,
        )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=("plan", "stage"), required=True)
    parser.add_argument("--source-candidate", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--hermes-home", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = run_workspace_runtime_bundle(
            phase=args.phase,
            source_candidate=args.source_candidate,
            output_dir=args.output_dir,
            hermes_home=args.hermes_home,
        )
    except (OSError, ValueError, WorkspaceRuntimeError) as exc:
        code = getattr(exc, "code", "rca_workspace_bundle_failed")
        print(json.dumps({"ok": False, "code": code}, sort_keys=True))
        return 2
    print(
        json.dumps(
            {
                "ok": True,
                "phase": result.phase,
                "artifact": str(result.artifact_path),
                "resumed": result.resumed,
                "live_install_performed": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
