#!/usr/bin/env python3
"""Fail-closed launcher for PNC services bound by LIVE_MANIFEST.json."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import subprocess
import sys
from pathlib import Path
from typing import Any


MAX_MANIFEST_BYTES = 4 * 1024 * 1024
SERVICE_SCRIPTS = {
    "local.pnc.feishu-delivery-repair": "scripts/pnc_feishu_delivery_guard.py",
    "local.pnc.vm-task-sync": "scripts/pnc_vm_task_sync.py",
}


class LiveExecError(RuntimeError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def _identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_uid,
        value.st_gid,
        value.st_nlink,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _read_owner_file(path: Path, *, max_bytes: int) -> bytes:
    descriptor = -1
    try:
        before = path.lstat()
        if (
            not stat.S_ISREG(before.st_mode)
            or stat.S_ISLNK(before.st_mode)
            or before.st_uid != os.geteuid()
            or before.st_nlink != 1
            or stat.S_IMODE(before.st_mode) & 0o022
            or before.st_size <= 0
            or before.st_size > max_bytes
        ):
            raise LiveExecError("active_runtime_file_invalid")
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        opened = os.fstat(descriptor)
        if _identity(opened) != _identity(before):
            raise LiveExecError("active_runtime_file_changed")
        chunks: list[bytes] = []
        remaining = opened.st_size
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                raise LiveExecError("active_runtime_file_changed")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise LiveExecError("active_runtime_file_changed")
        if _identity(os.fstat(descriptor)) != _identity(before) or _identity(
            path.lstat()
        ) != _identity(before):
            raise LiveExecError("active_runtime_file_changed")
        return b"".join(chunks)
    except LiveExecError:
        raise
    except OSError as exc:
        raise LiveExecError("active_runtime_file_unavailable") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _load_manifest(path: Path) -> tuple[bytes, dict[str, Any]]:
    raw = _read_owner_file(path, max_bytes=MAX_MANIFEST_BYTES)
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise LiveExecError("active_runtime_manifest_invalid") from exc
    if not isinstance(payload, dict):
        raise LiveExecError("active_runtime_manifest_invalid")
    return raw, payload


def _absolute_path(value: Any, *, code: str) -> Path:
    selected = Path(str(value or ""))
    if not selected.is_absolute():
        raise LiveExecError(code)
    return selected


def _require_direct_child(path: Path, parent: Path, *, code: str) -> None:
    try:
        if path.parent.resolve(strict=True) != parent.resolve(strict=True):
            raise LiveExecError(code)
    except OSError as exc:
        raise LiveExecError(code) from exc


def _git_identity(runtime_root: Path) -> tuple[str, str]:
    try:
        result = subprocess.run(
            [
                "/usr/bin/git",
                "-C",
                str(runtime_root),
                "rev-parse",
                "HEAD",
                "HEAD^{tree}",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
            env={
                "HOME": str(Path.home()),
                "PATH": "/usr/bin:/bin",
                "GIT_CONFIG_NOSYSTEM": "1",
            },
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise LiveExecError("active_runtime_git_identity_unavailable") from exc
    lines = result.stdout.splitlines()
    if len(lines) != 2:
        raise LiveExecError("active_runtime_git_identity_invalid")
    return lines[0].strip(), lines[1].strip()


def _resolve_once(
    *, manifest_path: Path, hermes_home: Path, service_label: str
) -> tuple[bytes, dict[str, str]]:
    relative_script = SERVICE_SCRIPTS.get(service_label)
    if relative_script is None:
        raise LiveExecError("active_runtime_service_not_allowed")

    raw, manifest = _load_manifest(manifest_path)
    runtime_root = _absolute_path(
        manifest.get("runtime_root"), code="active_runtime_root_invalid"
    )
    runtime_venv = _absolute_path(
        manifest.get("runtime_venv"), code="active_runtime_venv_invalid"
    )
    runtime_python = _absolute_path(
        manifest.get("runtime_python"), code="active_runtime_python_invalid"
    )
    _require_direct_child(
        runtime_root,
        hermes_home / "runtime" / "releases",
        code="active_runtime_root_invalid",
    )
    _require_direct_child(
        runtime_venv,
        hermes_home / "runtime" / "venvs",
        code="active_runtime_venv_invalid",
    )
    if runtime_python != runtime_venv / "bin" / "python":
        raise LiveExecError("active_runtime_python_invalid")
    try:
        root_stat = runtime_root.lstat()
        python_stat = runtime_python.stat()
    except OSError as exc:
        raise LiveExecError("active_runtime_path_unavailable") from exc
    if (
        not stat.S_ISDIR(root_stat.st_mode)
        or stat.S_ISLNK(root_stat.st_mode)
        or root_stat.st_uid != os.geteuid()
        or stat.S_IMODE(root_stat.st_mode) & 0o022
    ):
        raise LiveExecError("active_runtime_root_invalid")
    if not stat.S_ISREG(python_stat.st_mode) or not os.access(runtime_python, os.X_OK):
        raise LiveExecError("active_runtime_python_invalid")

    faces = manifest.get("face_git_bindings")
    runtime_face = faces.get("runtime_engine") if isinstance(faces, dict) else None
    if not isinstance(runtime_face, dict):
        raise LiveExecError("active_runtime_binding_invalid")
    expected_commit = str(runtime_face.get("commit") or "")
    expected_tree = str(runtime_face.get("tree") or "")
    expected_repo = str(runtime_face.get("repo") or "")
    if (
        len(expected_commit) != 40
        or any(char not in "0123456789abcdef" for char in expected_commit)
        or len(expected_tree) != 40
        or any(char not in "0123456789abcdef" for char in expected_tree)
        or Path(expected_repo) != runtime_root
        or str(manifest.get("promotion_source_head") or "") != expected_commit
    ):
        raise LiveExecError("active_runtime_binding_invalid")
    actual_commit, actual_tree = _git_identity(runtime_root)
    if actual_commit != expected_commit:
        raise LiveExecError("active_runtime_commit_mismatch")
    if actual_tree != expected_tree:
        raise LiveExecError("active_runtime_tree_mismatch")

    target = runtime_root / relative_script
    try:
        target_raw = _read_owner_file(target, max_bytes=64 * 1024 * 1024)
    except LiveExecError as exc:
        raise LiveExecError("active_runtime_script_invalid") from exc
    return raw, {
        "service_label": service_label,
        "manifest_path": str(manifest_path),
        "manifest_sha256": hashlib.sha256(raw).hexdigest(),
        "runtime_root": str(runtime_root),
        "runtime_venv": str(runtime_venv),
        "runtime_python": str(runtime_python),
        "runtime_commit": actual_commit,
        "runtime_tree": actual_tree,
        "script": str(target),
        "script_sha256": hashlib.sha256(target_raw).hexdigest(),
    }


def resolve_active_runtime(
    *, manifest_path: Path, hermes_home: Path, service_label: str
) -> dict[str, str]:
    for _attempt in range(2):
        raw, resolved = _resolve_once(
            manifest_path=manifest_path,
            hermes_home=hermes_home,
            service_label=service_label,
        )
        confirmed, _payload = _load_manifest(manifest_path)
        if confirmed == raw:
            return resolved
    raise LiveExecError("active_runtime_manifest_changed")


def _exec_environment(resolved: dict[str, str], hermes_home: Path) -> dict[str, str]:
    runtime_venv = Path(resolved["runtime_venv"])
    home = Path.home()
    stable_paths = (
        runtime_venv / "bin",
        home / ".local" / "bin",
        home / "bin",
        Path("/opt/homebrew/bin"),
        Path("/usr/local/bin"),
        Path("/usr/bin"),
        Path("/bin"),
        Path("/usr/sbin"),
        Path("/sbin"),
    )
    environment = dict(os.environ)
    for key in (
        "HERMES_FRAMEWORK_ROOT",
        "HERMES_NATIVE_BIN",
        "PYTHONHOME",
        "PYTHONPATH",
    ):
        environment.pop(key, None)
    environment.update({
        "HERMES_HOME": str(hermes_home),
        "PATH": ":".join(str(path) for path in stable_paths),
        "PNC_LIVE_MANIFEST_SHA256": resolved["manifest_sha256"],
        "PNC_LIVE_RUNTIME_COMMIT": resolved["runtime_commit"],
        "PNC_LIVE_SERVICE_LABEL": resolved["service_label"],
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONNOUSERSITE": "1",
        "PYTHONUNBUFFERED": "1",
        "VIRTUAL_ENV": str(runtime_venv),
    })
    return environment


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Execute an allowlisted PNC service from the active manifest runtime"
    )
    parser.add_argument("--check", action="store_true")
    parser.add_argument("service_label")
    parser.add_argument("service_args", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    hermes_home = Path(
        os.environ.get("HERMES_HOME") or Path.home() / ".hermes"
    ).absolute()
    manifest_path = hermes_home / "runtime" / "LIVE_MANIFEST.json"
    try:
        resolved = resolve_active_runtime(
            manifest_path=manifest_path,
            hermes_home=hermes_home,
            service_label=args.service_label,
        )
        if args.check:
            print(json.dumps({"ok": True, **resolved}, sort_keys=True))
            return 0
        os.chdir(resolved["runtime_root"])
        runtime_python = resolved["runtime_python"]
        os.execve(
            runtime_python,
            [runtime_python, resolved["script"], *args.service_args],
            _exec_environment(resolved, hermes_home),
        )
    except LiveExecError as exc:
        print(
            json.dumps({"ok": False, "error": exc.code}, sort_keys=True),
            file=sys.stderr,
        )
        return 78
    except OSError:
        print(
            json.dumps(
                {"ok": False, "error": "active_runtime_exec_failed"},
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 78
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
