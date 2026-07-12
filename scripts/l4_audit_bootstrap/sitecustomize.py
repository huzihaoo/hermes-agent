"""Early Python audit boundary for the L4 zero-impact harness worker.

Loaded by Python's site initialization before the worker script. The OS
sandbox is the primary containment boundary; this hook supplies independent,
machine-readable attempt evidence and fails closed on disallowed IO. Audit
hooks do not fully observe dir-fd resolution, native syscalls, or descriptor
backed mmap; the harness therefore never treats this hook as a substitute for
the independently hashed default-deny OS sandbox profile.
"""
from __future__ import annotations

import json
import os
import stat
import sys
import threading
import time
from pathlib import Path
from typing import Any


def _under(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


_WRITE_ROOT_RAW = Path(os.environ["HERMES_L4_ALLOWED_WRITE_ROOT"])
if not _WRITE_ROOT_RAW.is_absolute():
    raise RuntimeError("L4 audit write root must be absolute")
_WRITE_ROOT = _WRITE_ROOT_RAW.resolve(strict=True)
if _WRITE_ROOT_RAW.absolute() != _WRITE_ROOT:
    raise RuntimeError("L4 audit write root must be canonical")
_WRITE_ROOT_INFO = _WRITE_ROOT.stat()
if (
    not stat.S_ISDIR(_WRITE_ROOT_INFO.st_mode)
    or _WRITE_ROOT_INFO.st_uid != os.getuid()
    or _WRITE_ROOT_INFO.st_mode & 0o022
):
    raise RuntimeError("L4 audit write root has unsafe owner/type/mode")
_LOG_RAW = Path(os.environ["HERMES_L4_AUDIT_LOG"])
if not _LOG_RAW.is_absolute():
    raise RuntimeError("L4 audit log must be absolute")
_LOG_PARENT = _LOG_RAW.parent.resolve(strict=True)
_LOG_PATH = _LOG_RAW.resolve(strict=True)
if _LOG_RAW.absolute() != _LOG_PATH or _LOG_RAW.parent.absolute() != _LOG_PARENT:
    raise RuntimeError("L4 audit log must be canonical")
if not _under(_LOG_PATH, _WRITE_ROOT) or _LOG_PATH == _WRITE_ROOT:
    raise RuntimeError("L4 audit log must stay strictly within the write root")
_LOG_PARENT_INFO = _LOG_PARENT.stat()
if (
    not stat.S_ISDIR(_LOG_PARENT_INFO.st_mode)
    or _LOG_PARENT_INFO.st_uid != os.getuid()
    or _LOG_PARENT_INFO.st_mode & 0o022
):
    raise RuntimeError("L4 audit log parent has unsafe owner/type/mode")
_PROTECTED_ROOTS = tuple(
    Path(os.path.abspath(os.path.normpath(value)))
    for value in os.environ.get("HERMES_L4_PROTECTED_ROOTS", "").split(os.pathsep)
    if value
)
_LOG_FD = os.open(
    _LOG_PATH,
    os.O_WRONLY
    | os.O_APPEND
    | getattr(os, "O_CLOEXEC", 0)
    | getattr(os, "O_NOFOLLOW", 0),
    0o600,
)
_LOG_INFO = os.fstat(_LOG_FD)
_LOG_VISIBLE = os.stat(_LOG_PATH, follow_symlinks=False)
if (
    not stat.S_ISREG(_LOG_INFO.st_mode)
    or _LOG_INFO.st_uid != os.getuid()
    or _LOG_INFO.st_nlink != 1
    or stat.S_IMODE(_LOG_INFO.st_mode) != 0o600
    or (_LOG_INFO.st_dev, _LOG_INFO.st_ino)
    != (_LOG_VISIBLE.st_dev, _LOG_VISIBLE.st_ino)
):
    os.close(_LOG_FD)
    raise RuntimeError("L4 audit log must be owner-only single-link regular file")
_LOCAL = threading.local()


def _path(value: Any) -> Path | None:
    if isinstance(value, int) or value is None:
        return None
    try:
        raw = os.fsdecode(value)
    except (TypeError, ValueError):
        return None
    if not raw:
        return None
    # Lexical normalization avoids probing a protected target before the audit
    # decision. Symlink resolution remains enforced by the OS sandbox.
    return Path(os.path.abspath(os.path.normpath(raw)))


def _write_log(event: str, decision: str, **details: Any) -> None:
    row = {
        "decision": decision,
        "event": event,
        "pid": os.getpid(),
        "time_ns": time.time_ns(),
        **details,
    }
    payload = (
        json.dumps(row, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        + "\n"
    ).encode("utf-8")
    os.write(_LOG_FD, payload)


def _deny(event: str, *, path: Path | None = None, detail: str = "") -> None:
    _write_log(
        event,
        "deny",
        path=str(path) if path is not None else None,
        detail=detail[:500],
    )
    raise PermissionError(f"L4 audit policy denied {event}: {path or detail}")


def _check_read(event: str, value: Any) -> None:
    path = _path(value)
    if path is None:
        return
    for protected in _PROTECTED_ROOTS:
        if _under(path, protected):
            _deny(event, path=path, detail="protected root read")


def _check_write(event: str, value: Any) -> None:
    path = _path(value)
    if path is None:
        return
    if path == Path("/dev/null") or _under(path, _WRITE_ROOT):
        _write_log(event, "allow", path=str(path))
        return
    _deny(event, path=path, detail="write outside isolated run root")


def _audit(event: str, args: tuple[Any, ...]) -> None:
    if getattr(_LOCAL, "active", False):
        return
    _LOCAL.active = True
    try:
        if event == "open" and args:
            mode = args[1] if len(args) > 1 else None
            flags = args[2] if len(args) > 2 else 0
            mode_writes = isinstance(mode, str) and any(char in mode for char in "wax+")
            flag_writes = isinstance(flags, int) and bool(
                flags
                & (
                    os.O_WRONLY
                    | os.O_RDWR
                    | os.O_CREAT
                    | os.O_TRUNC
                    | os.O_APPEND
                )
            )
            if mode_writes or flag_writes:
                _check_write(event, args[0])
            else:
                _check_read(event, args[0])
            return
        if event in {"os.listdir", "os.scandir"}:
            if args:
                _check_read(event, args[0])
            return
        if event in {"os.remove", "os.unlink", "os.rmdir", "os.mkdir", "os.chmod", "os.chown", "os.truncate", "os.utime", "os.removexattr", "os.setxattr"}:
            if args:
                _check_write(event, args[0])
            return
        if event in {"os.rename", "os.replace", "os.link", "os.symlink"}:
            if args:
                _check_write(event, args[0])
            if len(args) > 1:
                _check_write(event, args[1])
            return
        if event in {"os.chdir", "os.fchdir"}:
            if args:
                target = _path(args[0])
                if target is not None and not _under(target, _WRITE_ROOT):
                    _deny(event, path=target, detail="cwd outside isolated run root")
            return
        if event == "socket.__new__":
            _write_log(event, "observe", detail=repr(args)[:500])
            return
        if event.startswith("socket.") and event in {
            "socket.bind",
            "socket.connect",
            "socket.connect_ex",
            "socket.getaddrinfo",
            "socket.gethostbyaddr",
            "socket.gethostbyname",
            "socket.gethostbyname_ex",
            "socket.getnameinfo",
            "socket.sendto",
        }:
            _deny(event, detail=repr(args))
        if event == "subprocess.Popen" or event == "os.system" or event.startswith("os.exec") or event.startswith("os.spawn") or event == "pty.spawn":
            _deny(event, detail=repr(args))
    finally:
        _LOCAL.active = False


_write_log(
    "audit.bootstrap",
    "allow",
    allowed_write_root=str(_WRITE_ROOT),
    protected_roots=[str(path) for path in _PROTECTED_ROOTS],
)
sys.addaudithook(_audit)
