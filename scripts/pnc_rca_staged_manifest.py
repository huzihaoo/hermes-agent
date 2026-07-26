#!/usr/bin/env python3
"""Bind the active evaluator inventory into a staged release manifest.

This compiler never applies a release. It accepts an explicit staged manifest,
proves the exact pipeline Git source, and writes a separate canonical manifest.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
import tempfile
from typing import Any, Mapping, Sequence

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.pnc_rca_release_freshness_gate import (  # noqa: E402
    materialize_active_evaluator_inventory_binding,
)


PIPELINE_FACE = "g1q3_rca_pipeline"
RECEIPT_SCHEMA_VERSION = "pnc_rca_staged_evaluator_inventory_binding_receipt_v1"
MAX_MANIFEST_BYTES = 4 * 1024 * 1024
_HEX40_RE = re.compile(r"^[0-9a-f]{40}$")
_MISSING = object()


class StagedManifestError(RuntimeError):
    def __init__(self, code: str):
        self.code = str(code or "pnc_release_staged_manifest_invalid")[:160]
        super().__init__(self.code)


@dataclass(frozen=True)
class _FileIdentity:
    device: int
    inode: int
    mode: int
    uid: int
    gid: int
    links: int
    size: int
    mtime_ns: int
    ctime_ns: int


def _identity(value: os.stat_result) -> _FileIdentity:
    return _FileIdentity(
        device=value.st_dev,
        inode=value.st_ino,
        mode=value.st_mode,
        uid=value.st_uid,
        gid=value.st_gid,
        links=value.st_nlink,
        size=value.st_size,
        mtime_ns=value.st_mtime_ns,
        ctime_ns=value.st_ctime_ns,
    )


def _absolute(path: Path) -> Path:
    return Path(os.path.abspath(os.fspath(path.expanduser())))


def canonical_manifest_bytes(value: Mapping[str, Any]) -> bytes:
    try:
        rendered = json.dumps(
            dict(value),
            ensure_ascii=True,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise StagedManifestError("pnc_release_staged_manifest_not_canonical") from exc
    return (rendered + "\n").encode("utf-8")


def _json_without_duplicate_keys(raw: bytes) -> dict[str, Any]:
    def object_pairs(pairs: Sequence[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, item in pairs:
            if key in value:
                raise StagedManifestError("pnc_release_staged_manifest_duplicate_key")
            value[key] = item
        return value

    try:
        payload = json.loads(raw, object_pairs_hook=object_pairs)
    except StagedManifestError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise StagedManifestError("pnc_release_staged_manifest_json_invalid") from exc
    if not isinstance(payload, dict):
        raise StagedManifestError("pnc_release_staged_manifest_shape_invalid")
    return payload


def _owner_regular_identity(
    path: Path,
    *,
    code: str,
    allow_empty: bool = False,
) -> _FileIdentity:
    try:
        observed = path.lstat()
    except OSError as exc:
        raise StagedManifestError(code) from exc
    if (
        not stat.S_ISREG(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_uid != os.geteuid()
        or observed.st_nlink != 1
        or (not allow_empty and observed.st_size <= 0)
        or observed.st_size > MAX_MANIFEST_BYTES
    ):
        raise StagedManifestError(code)
    return _identity(observed)


def _read_owner_file(path: Path, *, code: str) -> tuple[bytes, _FileIdentity]:
    selected = _absolute(path)
    before = _owner_regular_identity(selected, code=code)
    descriptor = -1
    try:
        descriptor = os.open(
            selected,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        opened = os.fstat(descriptor)
        if _identity(opened) != before:
            raise StagedManifestError(f"{code}_changed")
        remaining = opened.st_size
        chunks: list[bytes] = []
        while remaining:
            chunk = os.read(descriptor, min(remaining, 1024 * 1024))
            if not chunk:
                raise StagedManifestError(f"{code}_changed")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise StagedManifestError(f"{code}_changed")
        if (
            _identity(os.fstat(descriptor)) != before
            or _identity(selected.lstat()) != before
        ):
            raise StagedManifestError(f"{code}_changed")
        return b"".join(chunks), before
    except StagedManifestError:
        raise
    except OSError as exc:
        raise StagedManifestError(code) from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _optional_output_identity(path: Path) -> _FileIdentity | None:
    try:
        path.lstat()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise StagedManifestError("pnc_release_staged_manifest_output_invalid") from exc
    return _owner_regular_identity(
        path,
        code="pnc_release_staged_manifest_output_invalid",
        allow_empty=True,
    )


def _validate_owner_directory(path: Path, *, create: bool) -> Path:
    selected = _absolute(path)
    if create:
        try:
            selected.mkdir(mode=0o700, parents=True, exist_ok=True)
        except OSError as exc:
            raise StagedManifestError(
                "pnc_release_staged_manifest_output_directory_invalid"
            ) from exc
    try:
        observed = selected.lstat()
        resolved = selected.resolve(strict=True)
    except OSError as exc:
        raise StagedManifestError(
            "pnc_release_staged_manifest_output_directory_invalid"
        ) from exc
    if (
        resolved != selected
        or not stat.S_ISDIR(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_uid != os.geteuid()
        or stat.S_IMODE(observed.st_mode) & 0o022
    ):
        raise StagedManifestError(
            "pnc_release_staged_manifest_output_directory_invalid"
        )
    return selected


def _protected_live_manifest_paths() -> set[Path]:
    hermes_homes = {Path.home() / ".hermes"}
    configured_home = str(os.environ.get("HOME") or "").strip()
    if configured_home:
        hermes_homes.add(Path(configured_home).expanduser() / ".hermes")
    configured_hermes = str(os.environ.get("HERMES_HOME") or "").strip()
    if configured_hermes:
        hermes_homes.add(Path(configured_hermes).expanduser())
    return {_absolute(home / "runtime" / "LIVE_MANIFEST.json") for home in hermes_homes}


def _validate_staged_scope(path: Path, staged_root: Path | None, *, code: str) -> None:
    selected = _absolute(path)
    if staged_root is None:
        return
    root = _validate_owner_directory(staged_root, create=False)
    try:
        selected.relative_to(root)
    except ValueError as exc:
        raise StagedManifestError(code) from exc


def _object_id(value: Any, *, code: str) -> str:
    selected = str(value or "").strip()
    if _HEX40_RE.fullmatch(selected) is None:
        raise StagedManifestError(code)
    return selected


def compile_staged_manifest(
    manifest: Mapping[str, Any],
    *,
    pipeline_source_root: Path,
    pipeline_commit: str,
    pipeline_tree: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return a copied manifest with one exact source-derived inventory binding."""

    if not isinstance(manifest, Mapping):
        raise StagedManifestError("pnc_release_staged_manifest_shape_invalid")
    faces = manifest.get("face_git_bindings")
    if not isinstance(faces, Mapping):
        raise StagedManifestError("pnc_release_staged_pipeline_face_missing")
    pipeline = faces.get(PIPELINE_FACE)
    if not isinstance(pipeline, Mapping):
        raise StagedManifestError("pnc_release_staged_pipeline_face_missing")

    expected_commit = _object_id(
        pipeline.get("commit"), code="pnc_release_staged_pipeline_commit_invalid"
    )
    expected_tree = _object_id(
        pipeline.get("tree"), code="pnc_release_staged_pipeline_tree_invalid"
    )
    commit = _object_id(
        pipeline_commit, code="pnc_release_staged_pipeline_commit_invalid"
    )
    tree = _object_id(pipeline_tree, code="pnc_release_staged_pipeline_tree_invalid")
    if expected_commit != commit:
        raise StagedManifestError("pnc_release_staged_pipeline_commit_mismatch")
    if expected_tree != tree:
        raise StagedManifestError("pnc_release_staged_pipeline_tree_mismatch")

    source_binding = pipeline.get("source_repository_binding")
    if source_binding is not None:
        if not isinstance(source_binding, Mapping):
            raise StagedManifestError(
                "pnc_release_staged_pipeline_source_binding_invalid"
            )
        if source_binding.get("commit") != commit or source_binding.get("tree") != tree:
            raise StagedManifestError(
                "pnc_release_staged_pipeline_source_binding_mismatch"
            )

    try:
        binding = materialize_active_evaluator_inventory_binding(
            pipeline_source_root=pipeline_source_root,
            pipeline_commit=commit,
            pipeline_tree=tree,
        )
    except ValueError as exc:
        raise StagedManifestError(str(exc)) from exc

    prior_binding = pipeline.get("evaluator_inventory", _MISSING)
    if prior_binding is not _MISSING and prior_binding != binding:
        raise StagedManifestError(
            "pnc_release_staged_evaluator_inventory_stale_replacement"
        )

    compiled = dict(manifest)
    compiled_faces = dict(faces)
    compiled_pipeline = dict(pipeline)
    compiled_pipeline["evaluator_inventory"] = binding
    compiled_faces[PIPELINE_FACE] = compiled_pipeline
    compiled["face_git_bindings"] = compiled_faces
    canonical_manifest_bytes(compiled)
    return compiled, binding


def _atomic_write(
    path: Path,
    payload: bytes,
    *,
    expected_identity: _FileIdentity | None,
) -> None:
    selected = _absolute(path)
    parent = _validate_owner_directory(selected.parent, create=True)
    if _optional_output_identity(selected) != expected_identity:
        raise StagedManifestError("pnc_release_staged_manifest_output_changed")

    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{selected.name}.tmp.", dir=parent
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb", closefd=True) as handle:
            descriptor = -1
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        temporary_identity = _owner_regular_identity(
            temporary,
            code="pnc_release_staged_manifest_temporary_invalid",
        )
        if stat.S_IMODE(temporary_identity.mode) != 0o600:
            raise StagedManifestError("pnc_release_staged_manifest_temporary_invalid")
        temporary_raw, confirmed_temporary_identity = _read_owner_file(
            temporary,
            code="pnc_release_staged_manifest_temporary_invalid",
        )
        if (
            temporary_raw != payload
            or confirmed_temporary_identity != temporary_identity
        ):
            raise StagedManifestError("pnc_release_staged_manifest_temporary_invalid")
        if _optional_output_identity(selected) != expected_identity:
            raise StagedManifestError("pnc_release_staged_manifest_output_changed")
        try:
            os.replace(temporary, selected)
            directory_descriptor = os.open(
                parent,
                os.O_RDONLY
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_DIRECTORY", 0),
            )
            try:
                os.fsync(directory_descriptor)
            finally:
                os.close(directory_descriptor)
        except OSError as exc:
            raise StagedManifestError(
                "pnc_release_staged_manifest_atomic_write_failed"
            ) from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        except OSError:
            pass


def bind_staged_manifest(
    input_manifest: Path,
    output_manifest: Path,
    *,
    pipeline_source_root: Path,
    pipeline_commit: str,
    pipeline_tree: str,
    staged_root: Path | None = None,
) -> dict[str, Any]:
    """Compile and atomically write a staged manifest without applying it."""

    source_path = _absolute(input_manifest)
    output_path = _absolute(output_manifest)
    try:
        resolved_pipeline_source_root = _absolute(pipeline_source_root).resolve(
            strict=True
        )
    except OSError as exc:
        raise StagedManifestError(
            "pnc_release_evaluator_inventory_pipeline_repository_invalid"
        ) from exc
    if source_path == output_path:
        raise StagedManifestError("pnc_release_staged_manifest_in_place_forbidden")
    if output_path in _protected_live_manifest_paths():
        raise StagedManifestError("pnc_release_live_manifest_write_forbidden")
    _validate_staged_scope(
        source_path,
        staged_root,
        code="pnc_release_staged_manifest_input_outside_root",
    )
    _validate_staged_scope(
        output_path,
        staged_root,
        code="pnc_release_staged_manifest_output_outside_root",
    )

    raw, input_identity = _read_owner_file(
        source_path, code="pnc_release_staged_manifest_input_invalid"
    )
    source_manifest = _json_without_duplicate_keys(raw)
    compiled, binding = compile_staged_manifest(
        source_manifest,
        pipeline_source_root=resolved_pipeline_source_root,
        pipeline_commit=pipeline_commit,
        pipeline_tree=pipeline_tree,
    )
    if (
        _owner_regular_identity(
            source_path, code="pnc_release_staged_manifest_input_invalid"
        )
        != input_identity
    ):
        raise StagedManifestError("pnc_release_staged_manifest_input_changed")

    payload = canonical_manifest_bytes(compiled)
    output_identity = _optional_output_identity(output_path)
    if output_identity is not None:
        output_raw, confirmed_identity = _read_owner_file(
            output_path, code="pnc_release_staged_manifest_output_invalid"
        )
        if confirmed_identity != output_identity:
            raise StagedManifestError("pnc_release_staged_manifest_output_changed")
        if output_raw == payload:
            written = False
        elif output_raw != raw:
            raise StagedManifestError(
                "pnc_release_staged_manifest_output_stale_replacement"
            )
        else:
            _atomic_write(output_path, payload, expected_identity=output_identity)
            written = True
    else:
        _atomic_write(output_path, payload, expected_identity=None)
        written = True

    return {
        "schema_version": RECEIPT_SCHEMA_VERSION,
        "ok": True,
        "written": written,
        "input_manifest": str(source_path),
        "input_manifest_sha256": hashlib.sha256(raw).hexdigest(),
        "output_manifest": str(output_path),
        "manifest_sha256": hashlib.sha256(payload).hexdigest(),
        "pipeline_source_root": str(resolved_pipeline_source_root),
        "pipeline_commit": binding["pipeline_commit"],
        "pipeline_tree": binding["pipeline_tree"],
        "source_path": binding["source_path"],
        "source_blob_sha256": binding["source_blob_sha256"],
        "evaluator_scope": binding["evaluator_scope"],
        "evaluator_count": len(binding["evaluator_ids"]),
        "inventory_sha256": binding["inventory_sha256"],
        "production_actions": {
            "manifest_applies": 0,
            "releases": 0,
            "restarts": 0,
            "database_writes": 0,
            "external_writes": 0,
        },
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-manifest", type=Path, required=True)
    parser.add_argument("--output-manifest", type=Path, required=True)
    parser.add_argument("--pipeline-source-root", type=Path, required=True)
    parser.add_argument("--pipeline-commit", required=True)
    parser.add_argument("--pipeline-tree", required=True)
    parser.add_argument("--staged-root", type=Path)
    args = parser.parse_args(argv)
    try:
        result = bind_staged_manifest(
            args.input_manifest,
            args.output_manifest,
            pipeline_source_root=args.pipeline_source_root,
            pipeline_commit=args.pipeline_commit,
            pipeline_tree=args.pipeline_tree,
            staged_root=args.staged_root,
        )
    except StagedManifestError as exc:
        print(json.dumps({"ok": False, "error": exc.code}, sort_keys=True))
        return 2
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
