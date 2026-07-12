"""Lazy process integration for the reviewed record-only transport."""

from __future__ import annotations

import os
import re
import stat
import threading
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from gateway.record_only.transport import RecordOnlyOutboundTransport


_MODE_ENV = "HERMES_OUTBOUND_MODE"
_ROOT_ENV = "HERMES_OUTBOUND_RECORD_ROOT"
_KEY_FILE_ENV = "HERMES_OUTBOUND_RECORD_KEY_FILE"
_RECORD_ONLY_MODE = "record-only"
_LIVE_MODES = {"", "live"}
_HEX_KEY_RE = re.compile(rb"[0-9a-fA-F]{64,256}")
_lock = threading.Lock()
_transports: dict[tuple[str, str, str], "RecordOnlyOutboundTransport"] = {}


class RecordOnlyConfigurationError(RuntimeError):
    """Raised when a non-live outbound mode is not safely configured."""


def _mode() -> str:
    mode = os.environ.get(_MODE_ENV, "").strip().lower()
    if mode in _LIVE_MODES or mode == _RECORD_ONLY_MODE:
        return mode
    raise RecordOnlyConfigurationError(f"unsupported {_MODE_ENV}: {mode!r}")


def record_only_enabled() -> bool:
    return _mode() == _RECORD_ONLY_MODE


def _required_absolute_path(name: str) -> Path:
    raw = os.environ.get(name, "").strip()
    if not raw:
        raise RecordOnlyConfigurationError(f"{name} is required in record-only mode")
    path = Path(raw)
    if not path.is_absolute():
        raise RecordOnlyConfigurationError(f"{name} must be absolute")
    return path


def _read_key_file(path: Path) -> bytes:
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    fd = -1
    try:
        visible_before = os.stat(path, follow_symlinks=False)
        fd = os.open(path, flags)
        opened_before = os.fstat(fd)
        visible_opened = os.stat(path, follow_symlinks=False)
    except OSError as exc:
        if fd >= 0:
            os.close(fd)
        raise RecordOnlyConfigurationError(f"record key file unavailable: {exc}") from exc
    if (
        stat.S_ISLNK(visible_before.st_mode)
        or not stat.S_ISREG(opened_before.st_mode)
        or opened_before.st_uid != os.getuid()
        or opened_before.st_nlink != 1
        or opened_before.st_mode & 0o077
        or opened_before.st_size < 32
        or opened_before.st_size > 4096
        or (visible_before.st_dev, visible_before.st_ino)
        != (opened_before.st_dev, opened_before.st_ino)
        or (visible_opened.st_dev, visible_opened.st_ino)
        != (opened_before.st_dev, opened_before.st_ino)
    ):
        os.close(fd)
        raise RecordOnlyConfigurationError(
            "record key file must be user-owned mode 0600, single-link, regular, and bounded"
        )
    try:
        chunks: list[bytes] = []
        remaining = 4097
        while remaining > 0:
            chunk = os.read(fd, min(256, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        raw_bytes = b"".join(chunks)
        opened_after = os.fstat(fd)
        visible_after = os.stat(path, follow_symlinks=False)
    except OSError as exc:
        raise RecordOnlyConfigurationError(f"record key file cannot be read: {exc}") from exc
    finally:
        if fd >= 0:
            os.close(fd)
            fd = -1
    before_fingerprint = (
        opened_before.st_dev,
        opened_before.st_ino,
        opened_before.st_size,
        opened_before.st_mtime_ns,
        opened_before.st_ctime_ns,
    )
    after_fingerprint = (
        opened_after.st_dev,
        opened_after.st_ino,
        opened_after.st_size,
        opened_after.st_mtime_ns,
        opened_after.st_ctime_ns,
    )
    if (
        len(raw_bytes) > 4096
        or len(raw_bytes) != opened_before.st_size
        or before_fingerprint != after_fingerprint
        or (visible_after.st_dev, visible_after.st_ino)
        != (opened_after.st_dev, opened_after.st_ino)
        or stat.S_ISLNK(visible_after.st_mode)
    ):
        raise RecordOnlyConfigurationError("record key file changed while being read")
    raw = raw_bytes.strip()
    if _HEX_KEY_RE.fullmatch(raw):
        raw = bytes.fromhex(raw.decode("ascii"))
    if len(raw) < 32:
        raise RecordOnlyConfigurationError("record key must contain at least 32 bytes")
    return raw


def get_record_only_transport(source_component: str) -> "RecordOnlyOutboundTransport | None":
    """Return a cached reviewed sink, or ``None`` when outbound mode is live."""
    if not record_only_enabled():
        return None
    root = _required_absolute_path(_ROOT_ENV)
    key_file = _required_absolute_path(_KEY_FILE_ENV)
    cache_key = (str(root), str(key_file), source_component)
    with _lock:
        existing = _transports.get(cache_key)
        if existing is not None:
            return existing
        from gateway.record_only.transport import RecordOnlyOutboundTransport

        transport = RecordOnlyOutboundTransport(
            root,
            id_hash_key=_read_key_file(key_file),
            source_component=source_component,
        )
        _transports[cache_key] = transport
        return transport


def _reset_for_tests() -> None:
    with _lock:
        for transport in _transports.values():
            transport.close()
        _transports.clear()
