"""Stable runtime identity and owner-only health publication for PNC auxiliaries."""

from __future__ import annotations

import hashlib
import json
import os
import plistlib
import stat
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import psutil


MAX_IDENTITY_FILE_BYTES = 64 * 1024 * 1024
MAX_HEALTH_BYTES = 1024 * 1024


class AuxiliaryHealthError(RuntimeError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def canonical_json_sha256(value: Any) -> str:
    raw = json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _stable_file(path: Path, *, max_bytes: int = MAX_IDENTITY_FILE_BYTES) -> bytes:
    selected = path.expanduser().absolute()
    descriptor = -1
    try:
        before = selected.lstat()
        if (
            stat.S_ISLNK(before.st_mode)
            or not stat.S_ISREG(before.st_mode)
            or before.st_size <= 0
            or before.st_size > max_bytes
        ):
            raise AuxiliaryHealthError("aux_runtime_identity_file_invalid")
        descriptor = os.open(
            selected,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        opened = os.fstat(descriptor)
        expected = (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        )
        if (
            not stat.S_ISREG(opened.st_mode)
            or (
                opened.st_dev,
                opened.st_ino,
                opened.st_size,
                opened.st_mtime_ns,
                opened.st_ctime_ns,
            )
            != expected
        ):
            raise AuxiliaryHealthError("aux_runtime_identity_file_changed")
        chunks: list[bytes] = []
        remaining = opened.st_size
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                raise AuxiliaryHealthError("aux_runtime_identity_file_changed")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise AuxiliaryHealthError("aux_runtime_identity_file_changed")
        after_fd = os.fstat(descriptor)
        after_path = selected.lstat()
        for observed in (after_fd, after_path):
            if (
                observed.st_dev,
                observed.st_ino,
                observed.st_size,
                observed.st_mtime_ns,
                observed.st_ctime_ns,
            ) != expected:
                raise AuxiliaryHealthError("aux_runtime_identity_file_changed")
        return b"".join(chunks)
    except AuxiliaryHealthError:
        raise
    except OSError as exc:
        raise AuxiliaryHealthError("aux_runtime_identity_file_unavailable") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def build_process_runtime_evidence(
    *,
    service_label: str,
    script_path: Path,
    plist_path: Path | None = None,
    executable: Path | None = None,
    cwd: Path | None = None,
) -> dict[str, Any]:
    if not service_label:
        raise AuxiliaryHealthError("aux_runtime_identity_label_invalid")
    script = script_path.expanduser().resolve()
    python = (executable or Path(sys.executable)).expanduser().resolve()
    working = (cwd or Path.cwd()).expanduser().resolve()
    plist = (
        plist_path
        or Path.home() / "Library" / "LaunchAgents" / f"{service_label}.plist"
    ).expanduser().absolute()
    script_raw = _stable_file(script)
    interpreter_raw = _stable_file(python)
    plist_raw = _stable_file(plist, max_bytes=1024 * 1024)
    try:
        body = plistlib.loads(plist_raw)
    except (ValueError, plistlib.InvalidFileException) as exc:
        raise AuxiliaryHealthError("aux_runtime_identity_plist_invalid") from exc
    arguments = body.get("ProgramArguments") if isinstance(body, Mapping) else None
    environment = body.get("EnvironmentVariables") if isinstance(body, Mapping) else None
    if (
        not isinstance(body, Mapping)
        or body.get("Label") != service_label
        or not isinstance(arguments, list)
        or not arguments
        or any(not isinstance(value, str) for value in arguments)
        or not isinstance(environment, Mapping)
        or any(
            not isinstance(key, str) or not isinstance(value, str)
            for key, value in environment.items()
        )
    ):
        raise AuxiliaryHealthError("aux_runtime_identity_plist_invalid")
    process = psutil.Process(os.getpid())
    create_time = float(process.create_time())
    started_at = datetime.fromtimestamp(create_time, timezone.utc).isoformat()
    return {
        "pid": os.getpid(),
        "process_create_time": create_time,
        "started_at": started_at,
        "runtime_identity": {
            "executable": str(python),
            "script": str(script),
            "cwd": str(working),
            "script_sha256": hashlib.sha256(script_raw).hexdigest(),
            "interpreter_sha256": hashlib.sha256(interpreter_raw).hexdigest(),
            "plist_path": str(plist),
            "plist_sha256": hashlib.sha256(plist_raw).hexdigest(),
            "program_arguments_sha256": canonical_json_sha256(arguments),
            "environment_sha256": canonical_json_sha256(dict(environment)),
        },
    }


def write_owner_health(path: Path, body: Mapping[str, Any]) -> None:
    selected = path.expanduser().absolute()
    try:
        selected.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
        parent = selected.parent.lstat()
    except OSError as exc:
        raise AuxiliaryHealthError("aux_health_parent_invalid") from exc
    if (
        stat.S_ISLNK(parent.st_mode)
        or not stat.S_ISDIR(parent.st_mode)
        or parent.st_uid != os.geteuid()
        or stat.S_IMODE(parent.st_mode) & 0o022
    ):
        raise AuxiliaryHealthError("aux_health_parent_invalid")
    if selected.exists() or selected.is_symlink():
        current = selected.lstat()
        if (
            stat.S_ISLNK(current.st_mode)
            or not stat.S_ISREG(current.st_mode)
            or current.st_uid != os.geteuid()
            or current.st_nlink != 1
            or stat.S_IMODE(current.st_mode) != 0o600
        ):
            raise AuxiliaryHealthError("aux_health_existing_file_invalid")
    raw = (
        json.dumps(
            dict(body),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")
    if len(raw) > MAX_HEALTH_BYTES:
        raise AuxiliaryHealthError("aux_health_payload_too_large")
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{selected.name}.", suffix=".tmp", dir=selected.parent
    )
    temporary_path = Path(temporary)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb", closefd=True) as handle:
            descriptor = -1
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, selected)
        directory_fd = os.open(selected.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        published = selected.lstat()
        if (
            not stat.S_ISREG(published.st_mode)
            or stat.S_ISLNK(published.st_mode)
            or published.st_uid != os.geteuid()
            or published.st_nlink != 1
            or stat.S_IMODE(published.st_mode) != 0o600
            or published.st_size != len(raw)
            or _stable_file(selected, max_bytes=MAX_HEALTH_BYTES) != raw
        ):
            raise AuxiliaryHealthError("aux_health_publish_invalid")
    except AuxiliaryHealthError:
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise AuxiliaryHealthError("aux_health_publish_failed") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            temporary_path.unlink()
        except FileNotFoundError:
            pass
