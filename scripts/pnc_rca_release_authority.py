#!/usr/bin/env python3
"""Compile or verify an offline PNC RCA cross-plane release authority."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import stat
import sys
import tempfile
from typing import Any, Mapping, Sequence

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from gateway.pnc_rca_release_authority import (  # noqa: E402
    ReleaseAuthorityError,
    audit_release_projections,
    build_active_pointer,
    canonical_json_sha256,
    validate_release_authority,
)


MAX_JSON_BYTES = 8 * 1024 * 1024
COMPILE_RECEIPT_SCHEMA_VERSION = "pnc_rca_release_authority_compile_receipt_v1"


class AuthorityCliError(RuntimeError):
    def __init__(self, code: str, detail: str = ""):
        self.code = str(code or "rca_release_authority_cli_invalid")[:160]
        self.detail = str(detail or self.code)[:1000]
        super().__init__(self.detail)


def _strict_json(raw: bytes, *, label: str) -> dict[str, Any]:
    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise AuthorityCliError(
                    "rca_release_authority_duplicate_key", f"{label} has duplicate key"
                )
            result[key] = value
        return result

    def reject_constant(value: str) -> None:
        raise AuthorityCliError(
            "rca_release_authority_json_invalid", f"{label} contains {value}"
        )

    try:
        value = json.loads(
            raw,
            object_pairs_hook=pairs,
            parse_constant=reject_constant,
        )
    except AuthorityCliError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise AuthorityCliError(
            "rca_release_authority_json_invalid", f"{label} is invalid JSON"
        ) from exc
    if not isinstance(value, dict):
        raise AuthorityCliError(
            "rca_release_authority_shape_invalid", f"{label} must be an object"
        )
    return value


def _read_json(path: Path, *, label: str) -> dict[str, Any]:
    selected = path.expanduser().absolute()
    descriptor = -1
    try:
        before = selected.lstat()
        if (
            stat.S_ISLNK(before.st_mode)
            or not stat.S_ISREG(before.st_mode)
            or before.st_size <= 0
            or before.st_size > MAX_JSON_BYTES
        ):
            raise OSError("not a bounded regular file")
        descriptor = os.open(
            selected,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns) != (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
        ):
            raise OSError("identity changed")
        raw = os.read(descriptor, opened.st_size + 1)
        after = selected.lstat()
        if (
            len(raw) != opened.st_size
            or (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
            != (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
        ):
            raise OSError("content changed")
    except OSError as exc:
        raise AuthorityCliError(
            "rca_release_authority_file_unavailable", f"{label} is unavailable"
        ) from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    return _strict_json(raw, label=label)


def _pretty_json(value: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(
            dict(value),
            ensure_ascii=True,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def _protected_output(path: Path) -> bool:
    selected = path.expanduser().absolute()
    protected = (
        Path.home() / ".hermes" / "runtime",
        Path.home() / "Library" / "LaunchAgents",
    )
    return any(selected == root or root in selected.parents for root in protected)


def _output_directory(path: Path) -> Path:
    selected = path.expanduser().absolute()
    if _protected_output(selected):
        raise AuthorityCliError(
            "rca_release_authority_live_output_forbidden",
            "compiler only writes offline candidate directories",
        )
    try:
        selected.mkdir(mode=0o700, parents=True, exist_ok=True)
        observed = selected.lstat()
        resolved = selected.resolve(strict=True)
    except OSError as exc:
        raise AuthorityCliError(
            "rca_release_authority_output_invalid", "output directory unavailable"
        ) from exc
    if (
        resolved != selected
        or stat.S_ISLNK(observed.st_mode)
        or not stat.S_ISDIR(observed.st_mode)
        or observed.st_uid != os.geteuid()
        or stat.S_IMODE(observed.st_mode) & 0o022
    ):
        raise AuthorityCliError(
            "rca_release_authority_output_invalid", "output directory is unsafe"
        )
    return selected


def _atomic_write(path: Path, raw: bytes) -> None:
    if path.exists() and path.is_symlink():
        raise AuthorityCliError(
            "rca_release_authority_output_invalid", "output cannot be a symlink"
        )
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = -1
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    except OSError as exc:
        raise AuthorityCliError(
            "rca_release_authority_output_invalid", f"cannot write {path.name}"
        ) from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def compile_candidate(
    authority: Mapping[str, Any],
    *,
    output_dir: Path,
    generated_at: str,
) -> dict[str, Any]:
    validate_release_authority(authority)
    if authority.get("status") != "candidate_only":
        raise AuthorityCliError(
            "rca_release_authority_offline_status_required",
            "offline compiler only accepts candidate_only authority",
        )
    selected = _output_directory(output_dir)
    release_id = str(authority["release_id"])
    authority_path = selected / f"{release_id}.authority.json"
    pointer_path = selected / "ACTIVE_RCA_RELEASE.candidate.json"
    pointer = build_active_pointer(
        authority,
        authority_path=authority_path,
        state="candidate",
        activated_at=generated_at,
        previous_authority_sha256=(
            str(authority.get("supersedes_authority_sha256") or "") or None
        ),
    )
    authority_raw = _pretty_json(authority)
    pointer_raw = _pretty_json(pointer)
    _atomic_write(authority_path, authority_raw)
    _atomic_write(pointer_path, pointer_raw)
    return {
        "schema_version": COMPILE_RECEIPT_SCHEMA_VERSION,
        "ok": True,
        "mode": "offline_candidate_only",
        "release_id": release_id,
        "authority_sha256": canonical_json_sha256(authority),
        "artifacts": {
            "authority": {
                "path": str(authority_path),
                "raw_sha256": hashlib.sha256(authority_raw).hexdigest(),
            },
            "pointer": {
                "path": str(pointer_path),
                "raw_sha256": hashlib.sha256(pointer_raw).hexdigest(),
            },
        },
        "production_mutation_performed": False,
    }


def _arguments(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    compile_parser = commands.add_parser("compile")
    compile_parser.add_argument("--input", type=Path, required=True)
    compile_parser.add_argument("--output-dir", type=Path, required=True)
    compile_parser.add_argument("--generated-at")

    verify_parser = commands.add_parser("verify")
    verify_parser.add_argument("--authority", type=Path, required=True)
    verify_parser.add_argument("--pointer", type=Path)
    verify_parser.add_argument("--live-manifest", type=Path)
    verify_parser.add_argument("--active-binding", type=Path)
    verify_parser.add_argument("--control-db", type=Path)
    verify_parser.add_argument("--health", type=Path, action="append", default=[])
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _arguments(argv)
    try:
        if args.command == "compile":
            authority = _read_json(args.input, label="authority input")
            generated_at = args.generated_at or datetime.now(timezone.utc).isoformat()
            print(
                json.dumps(
                    compile_candidate(
                        authority,
                        output_dir=args.output_dir,
                        generated_at=generated_at,
                    ),
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
            return 0

        authority = _read_json(args.authority, label="authority")
        pointer = (
            _read_json(args.pointer, label="pointer") if args.pointer is not None else None
        )
        live_manifest = (
            _read_json(args.live_manifest, label="LIVE_MANIFEST")
            if args.live_manifest is not None
            else None
        )
        active_binding = (
            _read_json(args.active_binding, label="active binding")
            if args.active_binding is not None
            else None
        )
        health = [_read_json(path, label=f"health {path}") for path in args.health]
        result = audit_release_projections(
            authority,
            pointer=pointer,
            authority_path=args.authority,
            live_manifest=live_manifest,
            active_binding=active_binding,
            control_store_path=args.control_db,
            health_artifacts=health,
        )
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
        return 0 if result["ok"] else 2
    except (AuthorityCliError, ReleaseAuthorityError) as exc:
        print(
            json.dumps(
                {"ok": False, "code": exc.code, "detail": exc.detail},
                ensure_ascii=False,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
