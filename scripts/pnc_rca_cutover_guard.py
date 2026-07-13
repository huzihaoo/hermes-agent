#!/usr/bin/env python3
"""Guard an RCA production cutover without mutating Gateway or live runtime."""

from __future__ import annotations

import argparse
import contextlib
import errno
import fcntl
import hashlib
import json
import math
import os
import platform
import re
import secrets
import stat
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import psutil

from gateway.pnc_rca_runtime_identity import GATEWAY_RCA_RUNTIME_RELATIVE_FILES


LEASE_SCHEMA_VERSION = "pnc_rca_production_cutover_lease_v1"
RUNTIME_FILES_IDENTITY_SCHEMA_VERSION = "pnc_rca_live_runtime_files_identity_v1"
GATEWAY_RUNNING_OBSERVATION_SCHEMA_VERSION = "pnc_rca_gateway_running_observation_v1"
GATEWAY_WRITER_STOP_OBSERVATION_SCHEMA_VERSION = (
    "pnc_rca_gateway_writer_stop_observation_v1"
)
WRITER_STOP_RECEIPT_SCHEMA_VERSION = "pnc_rca_gateway_writer_stop_receipt_v1"
DOCTOR_SCHEMA_VERSION = "pnc_rca_cutover_guard_doctor_v1"
PLAN_SCHEMA_VERSION = "pnc_rca_cutover_guard_plan_v1"

PRODUCTION_LOCK_PATH = Path(
    "/Users/songying/.hermes/runtime/locks/pnc-production-cutover.lock"
)
CANONICAL_LIVE_ROOT = Path("/Users/songying/.hermes/runtime/hermes-live")
GATEWAY_LABEL = "ai.hermes.gateway"
MAX_LEASE_SECONDS = 2 * 60 * 60
MAX_WRITER_STOP_AGE_SECONDS = 5 * 60
MAX_FUTURE_SKEW_SECONDS = 30
MAX_JSON_BYTES = 512 * 1024
MAX_RUNTIME_FILE_BYTES = 32 * 1024 * 1024

SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
RELEASE_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{7,127}\Z")
HOLD_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{7,127}\Z")


class CutoverGuardError(ValueError):
    """A kernel lease or writer-stop invariant failed closed."""

    def __init__(self, code: str, detail: str = ""):
        super().__init__(detail or code)
        self.code = code
        self.detail = detail or code


@dataclass(frozen=True)
class _StableFile:
    path: Path
    raw: bytes
    stat_result: os.stat_result

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.raw).hexdigest()


@dataclass(frozen=True)
class LeaseInputs:
    release_id: str
    release_prepare_manifest: Path
    approval_receipt: Path
    expected_live_runtime_identity: Mapping[str, Any]
    duration_seconds: int = MAX_LEASE_SECONDS


@dataclass(frozen=True)
class WriterStopInputs:
    hold_id: str
    plan_sha256: str
    receipt_path: Path
    expected_live_sidecar_identity: Mapping[str, Any]


@dataclass
class CutoverLease:
    path: Path
    descriptor: int
    body: Mapping[str, Any]
    raw: bytes
    fingerprint: str
    token: str
    holder_observer: Callable[[], Mapping[str, Any]] | None = None
    clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc)
    _closed: bool = False

    def assert_active(self) -> None:
        if self._closed:
            raise CutoverGuardError("cutover_lease_not_active")
        try:
            info = os.fstat(self.descriptor)
            os.lseek(self.descriptor, 0, os.SEEK_SET)
            raw = os.read(self.descriptor, MAX_JSON_BYTES + 1)
            lexical = os.lstat(self.path)
        except OSError as exc:
            raise CutoverGuardError("cutover_lease_not_active") from exc
        if (
            not stat.S_ISREG(info.st_mode)
            or not stat.S_ISREG(lexical.st_mode)
            or stat.S_IMODE(info.st_mode) != 0o600
            or stat.S_IMODE(lexical.st_mode) != 0o600
            or info.st_uid != os.geteuid()
            or lexical.st_uid != os.geteuid()
            or info.st_nlink != 1
            or lexical.st_nlink != 1
            or (info.st_dev, info.st_ino) != (lexical.st_dev, lexical.st_ino)
            or raw != self.raw
        ):
            raise CutoverGuardError("cutover_lease_identity_changed")
        if hashlib.sha256(self.token.encode("utf-8")).hexdigest() != self.body.get(
            "lease_token_sha256"
        ):
            raise CutoverGuardError("cutover_lease_token_changed")
        current = self.clock()
        if current.tzinfo is None or current.utcoffset() is None:
            raise CutoverGuardError("cutover_lease_clock_invalid")
        current = current.astimezone(timezone.utc)
        acquired = _parse_time(self.body.get("acquired_at"), artifact="cutover_lease")
        expires = _parse_time(self.body.get("expires_at"), artifact="cutover_lease")
        if current < acquired - timedelta(seconds=MAX_FUTURE_SKEW_SECONDS):
            raise CutoverGuardError("cutover_lease_not_yet_valid")
        if current >= expires:
            raise CutoverGuardError("cutover_lease_expired")
        if self.holder_observer is not None and dict(self.holder_observer()) != self.body.get(
            "holder"
        ):
            raise CutoverGuardError("cutover_lease_holder_changed")

    def close(self) -> None:
        if self._closed:
            return
        with contextlib.suppress(OSError):
            fcntl.flock(self.descriptor, fcntl.LOCK_UN)
        os.close(self.descriptor)
        self._closed = True

    def __enter__(self) -> CutoverLease:
        self.assert_active()
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


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
        raise CutoverGuardError("cutover_guard_json_invalid") from exc


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).rstrip(b"\n")).hexdigest()


def _strict_json(raw: bytes, *, artifact: str) -> Mapping[str, Any]:
    if not raw or len(raw) > MAX_JSON_BYTES:
        raise CutoverGuardError(f"{artifact}_size_invalid")

    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise CutoverGuardError(f"{artifact}_duplicate_key")
            result[key] = value
        return result

    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=pairs,
            parse_constant=lambda _value: (_ for _ in ()).throw(
                CutoverGuardError(f"{artifact}_number_invalid")
            ),
        )
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise CutoverGuardError(f"{artifact}_json_invalid") from exc
    if not isinstance(value, dict):
        raise CutoverGuardError(f"{artifact}_shape_invalid")
    return value


def _read_stable_file(
    path: Path,
    *,
    artifact: str,
    require_owner_only: bool,
    max_bytes: int = MAX_JSON_BYTES,
) -> _StableFile:
    selected = path.expanduser().absolute()
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(selected, flags)
    except OSError as exc:
        raise CutoverGuardError(f"{artifact}_unavailable") from exc
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or before.st_uid != os.geteuid()
            or before.st_size < 0
            or before.st_size > max_bytes
            or (require_owner_only and stat.S_IMODE(before.st_mode) != 0o600)
        ):
            raise CutoverGuardError(f"{artifact}_identity_invalid")
        chunks: list[bytes] = []
        remaining = before.st_size
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                raise CutoverGuardError(f"{artifact}_unstable")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise CutoverGuardError(f"{artifact}_unstable")
        after = os.fstat(descriptor)
        lexical = os.lstat(selected)
        fields = (
            "st_dev",
            "st_ino",
            "st_mode",
            "st_nlink",
            "st_uid",
            "st_size",
            "st_mtime_ns",
            "st_ctime_ns",
        )
        if stat.S_ISLNK(lexical.st_mode) or any(
            getattr(before, field) != getattr(after, field)
            or getattr(before, field) != getattr(lexical, field)
            for field in fields
        ):
            raise CutoverGuardError(f"{artifact}_unstable")
        return _StableFile(selected, b"".join(chunks), after)
    except OSError as exc:
        raise CutoverGuardError(f"{artifact}_unstable") from exc
    finally:
        os.close(descriptor)


def _read_owned_json(path: Path, *, artifact: str) -> tuple[_StableFile, Mapping[str, Any]]:
    owned = _read_stable_file(
        path,
        artifact=artifact,
        require_owner_only=True,
    )
    return owned, _strict_json(owned.raw, artifact=artifact)


def _parse_time(value: Any, *, artifact: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise CutoverGuardError(f"{artifact}_timestamp_invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise CutoverGuardError(f"{artifact}_timestamp_invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise CutoverGuardError(f"{artifact}_timestamp_invalid")
    return parsed.astimezone(timezone.utc)


def _sha256(value: Any, *, artifact: str) -> str:
    if not isinstance(value, str) or SHA256_RE.fullmatch(value) is None:
        raise CutoverGuardError(f"{artifact}_invalid")
    return value


def observe_machine_identity() -> Mapping[str, str]:
    for source, path in (
        ("etc_machine_id", Path("/etc/machine-id")),
        ("dbus_machine_id", Path("/var/lib/dbus/machine-id")),
    ):
        try:
            value = path.read_text(encoding="ascii").strip()
        except (OSError, UnicodeError):
            continue
        if re.fullmatch(r"[A-Za-z0-9-]{16,128}", value):
            return {
                "source": source,
                "sha256": hashlib.sha256(f"{source}\0{value}".encode()).hexdigest(),
            }
    if platform.system() == "Darwin":
        try:
            result = subprocess.run(
                ["/usr/sbin/ioreg", "-rd1", "-c", "IOPlatformExpertDevice"],
                check=False,
                capture_output=True,
                text=True,
                timeout=3,
            )
        except (OSError, subprocess.SubprocessError):
            result = None
        if result is not None and result.returncode == 0:
            match = re.search(
                r'"IOPlatformUUID"\s*=\s*"([A-Fa-f0-9-]{16,64})"',
                result.stdout,
            )
            if match:
                source = "darwin_ioplatformuuid"
                return {
                    "source": source,
                    "sha256": hashlib.sha256(
                        f"{source}\0{match.group(1).lower()}".encode()
                    ).hexdigest(),
                }
    raise CutoverGuardError("cutover_guard_machine_identity_unavailable")


def observe_boot_id() -> str:
    try:
        value = Path("/proc/sys/kernel/random/boot_id").read_text(
            encoding="ascii"
        ).strip()
    except (OSError, UnicodeError):
        value = ""
    if re.fullmatch(r"[A-Za-z0-9-]{16,128}", value):
        return value.lower()
    boot_time = float(psutil.boot_time())
    if not math.isfinite(boot_time) or boot_time <= 0:
        raise CutoverGuardError("cutover_guard_boot_identity_unavailable")
    return hashlib.sha256(f"psutil-boot-time\0{boot_time:.6f}".encode()).hexdigest()


def _regular_file_identity(path: Path, *, artifact: str) -> Mapping[str, Any]:
    owned = _read_stable_file(
        path,
        artifact=artifact,
        require_owner_only=False,
        max_bytes=MAX_RUNTIME_FILE_BYTES,
    )
    return {
        "path": str(owned.path),
        "sha256": owned.sha256,
        "size_bytes": len(owned.raw),
        "mode": stat.S_IMODE(owned.stat_result.st_mode),
        "device": owned.stat_result.st_dev,
        "inode": owned.stat_result.st_ino,
    }


def observe_live_runtime_files(
    canonical_root: Path = CANONICAL_LIVE_ROOT,
) -> Mapping[str, Any]:
    root = canonical_root.expanduser().absolute()
    try:
        root_info = root.lstat()
    except OSError as exc:
        raise CutoverGuardError("cutover_guard_live_runtime_unavailable") from exc
    if (
        stat.S_ISLNK(root_info.st_mode)
        or not stat.S_ISDIR(root_info.st_mode)
        or root_info.st_uid != os.geteuid()
    ):
        raise CutoverGuardError("cutover_guard_live_runtime_identity_invalid")
    relatives = tuple(sorted(set(GATEWAY_RCA_RUNTIME_RELATIVE_FILES)))
    files = {
        relative: _regular_file_identity(
            root / relative,
            artifact="cutover_guard_live_runtime_file",
        )
        for relative in relatives
    }
    interpreter = _regular_file_identity(
        root / ".venv" / "bin" / "python",
        artifact="cutover_guard_live_runtime_interpreter",
    )
    closure = {
        relative: descriptor["sha256"] for relative, descriptor in files.items()
    }
    return {
        "schema_version": RUNTIME_FILES_IDENTITY_SCHEMA_VERSION,
        "canonical_root": str(root),
        "root_identity": {
            "path": str(root),
            "device": root_info.st_dev,
            "inode": root_info.st_ino,
            "owner_uid": root_info.st_uid,
            "mode": stat.S_IMODE(root_info.st_mode),
        },
        "files": files,
        "runtime_files_sha256": _sha256_json(closure),
        "interpreter": interpreter,
    }


def _launchctl_print(
    *,
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> Mapping[str, Any]:
    try:
        result = runner(
            ["launchctl", "print", f"gui/{os.getuid()}/{GATEWAY_LABEL}"],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise CutoverGuardError("cutover_guard_launchctl_observation_failed") from exc
    if result.returncode != 0:
        raise CutoverGuardError("cutover_guard_gateway_label_not_loaded")
    pids = {int(value) for value in re.findall(r"(?m)^\s*pid\s*=\s*([0-9]+)\s*$", result.stdout)}
    states = re.findall(r"(?m)^\s*state\s*=\s*([^\n]+?)\s*$", result.stdout)
    if len(pids) > 1 or len(states) > 1:
        raise CutoverGuardError("cutover_guard_launchctl_output_ambiguous")
    return {
        "label": GATEWAY_LABEL,
        "loaded": True,
        "pid": next(iter(pids), None),
        "state": states[0].strip() if states else None,
    }


def _gateway_cmdline(cmdline: Sequence[str]) -> bool:
    normalized = [str(item).replace("\\", "/") for item in cmdline]
    for index, item in enumerate(normalized):
        following = normalized[index + 1 :]
        if (
            item == "-m"
            and len(following) >= 2
            and following[0] == "hermes_cli.main"
            and following[1] == "gateway"
        ):
            return True
        if item.endswith("/hermes_cli/main.py") and following[:1] == ["gateway"]:
            return True
        if Path(item).name == "hermes" and following[:1] == ["gateway"]:
            return True
        if item.endswith("/gateway/run.py") or item == "gateway/run.py":
            return True
    return False


def observe_gateway_process_census(
    canonical_root: Path = CANONICAL_LIVE_ROOT,
) -> Mapping[str, Any]:
    root = canonical_root.expanduser().absolute()
    matches: list[dict[str, Any]] = []
    try:
        processes = psutil.process_iter(["pid", "cmdline", "cwd", "exe", "create_time"])
        for process in processes:
            info = process.info
            cmdline = [str(item) for item in (info.get("cmdline") or [])]
            is_gateway = _gateway_cmdline(cmdline)
            path_values = [
                str(info.get("cwd") or ""),
                str(info.get("exe") or ""),
                *cmdline,
            ]
            if is_gateway:
                try:
                    environment = process.environ()
                    path_values.extend(
                        str(environment.get(name) or "")
                        for name in ("PYTHONPATH", "VIRTUAL_ENV", "PATH")
                    )
                    path_values.extend(
                        str(item.path or "") for item in process.open_files()
                    )
                    path_values.extend(
                        str(item.path or "") for item in process.memory_maps(grouped=False)
                    )
                except (OSError, psutil.Error) as exc:
                    raise CutoverGuardError(
                        "cutover_guard_process_census_failed"
                    ) from exc
            loads_root = any(
                value == str(root)
                or value.startswith(f"{root}/")
                or f":{root}/" in value
                or f"{root}/" in value.split(":")
                for value in path_values
            )
            if is_gateway and loads_root:
                matches.append({
                    "pid": int(info["pid"]),
                    "process_create_time": float(info.get("create_time") or 0),
                    "cmdline_sha256": _sha256_json(cmdline),
                })
    except (OSError, psutil.Error) as exc:
        raise CutoverGuardError("cutover_guard_process_census_failed") from exc
    return {
        "probe": "psutil_gateway_canonical_runtime_census_v1",
        "canonical_root": str(root),
        "matching_processes": sorted(
            matches, key=lambda item: (item["pid"], item["process_create_time"])
        ),
    }


def observe_gateway_running(
    canonical_root: Path = CANONICAL_LIVE_ROOT,
    *,
    launchctl_observer: Callable[[], Mapping[str, Any]] | None = None,
    runtime_observer: Callable[[], Mapping[str, Any]] | None = None,
) -> Mapping[str, Any]:
    root = canonical_root.expanduser().absolute()
    launchd = dict((launchctl_observer or _launchctl_print)())
    if (
        launchd
        != {
            "label": GATEWAY_LABEL,
            "loaded": True,
            "pid": launchd.get("pid"),
            "state": launchd.get("state"),
        }
        or not isinstance(launchd.get("pid"), int)
        or launchd["pid"] <= 0
    ):
        raise CutoverGuardError("cutover_guard_gateway_not_running")
    try:
        process = psutil.Process(launchd["pid"])
        create_time = float(process.create_time())
        executable = str(Path(process.exe()).expanduser().absolute())
        cwd = str(Path(process.cwd()).expanduser().absolute())
        cmdline = [str(item) for item in process.cmdline()]
    except (OSError, psutil.Error) as exc:
        raise CutoverGuardError("cutover_guard_gateway_process_unavailable") from exc
    if cwd != str(root) or not _gateway_cmdline(cmdline):
        raise CutoverGuardError("cutover_guard_gateway_process_mismatch")
    runtime = dict((runtime_observer or (lambda: observe_live_runtime_files(root)))())
    return {
        "schema_version": GATEWAY_RUNNING_OBSERVATION_SCHEMA_VERSION,
        "canonical_root": str(root),
        "launchd": launchd,
        "process": {
            "pid": launchd["pid"],
            "process_create_time": create_time,
            "executable": executable,
            "cwd": cwd,
            "cmdline_sha256": _sha256_json(cmdline),
            "loaded_runtime_closure_sha256": _sha256_json(runtime),
        },
        "live_runtime_identity": runtime,
    }


def _normalize_machine(value: Any) -> Mapping[str, str]:
    if not isinstance(value, Mapping) or set(value) != {"source", "sha256"}:
        raise CutoverGuardError("cutover_guard_machine_identity_invalid")
    source = value.get("source")
    if not isinstance(source, str) or not re.fullmatch(r"[a-z][a-z0-9_]{2,63}", source):
        raise CutoverGuardError("cutover_guard_machine_identity_invalid")
    return {"source": source, "sha256": _sha256(value.get("sha256"), artifact="machine_sha256")}


def _normalize_running_observation(
    value: Any,
    *,
    canonical_root: Path,
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != {
        "schema_version",
        "canonical_root",
        "launchd",
        "process",
        "live_runtime_identity",
    }:
        raise CutoverGuardError("cutover_guard_runtime_identity_shape_invalid")
    if (
        value.get("schema_version") != GATEWAY_RUNNING_OBSERVATION_SCHEMA_VERSION
        or value.get("canonical_root") != str(canonical_root)
    ):
        raise CutoverGuardError("cutover_guard_runtime_identity_invalid")
    launchd = value.get("launchd")
    process = value.get("process")
    runtime = value.get("live_runtime_identity")
    if not isinstance(launchd, Mapping) or set(launchd) != {
        "label",
        "loaded",
        "pid",
        "state",
    }:
        raise CutoverGuardError("cutover_guard_launchctl_identity_invalid")
    if (
        launchd.get("label") != GATEWAY_LABEL
        or launchd.get("loaded") is not True
        or not isinstance(launchd.get("pid"), int)
        or launchd["pid"] <= 0
        or not isinstance(launchd.get("state"), str)
        or not launchd["state"]
    ):
        raise CutoverGuardError("cutover_guard_gateway_not_running")
    if not isinstance(process, Mapping) or set(process) != {
        "pid",
        "process_create_time",
        "executable",
        "cwd",
        "cmdline_sha256",
        "loaded_runtime_closure_sha256",
    }:
        raise CutoverGuardError("cutover_guard_gateway_process_identity_invalid")
    create_time = process.get("process_create_time")
    if (
        process.get("pid") != launchd["pid"]
        or isinstance(create_time, bool)
        or not isinstance(create_time, (int, float))
        or not math.isfinite(float(create_time))
        or float(create_time) <= 0
        or process.get("cwd") != str(canonical_root)
        or not Path(str(process.get("executable") or "")).is_absolute()
    ):
        raise CutoverGuardError("cutover_guard_gateway_process_identity_invalid")
    _sha256(process.get("cmdline_sha256"), artifact="gateway_cmdline_sha256")
    loaded_hash = _sha256(
        process.get("loaded_runtime_closure_sha256"),
        artifact="gateway_loaded_runtime_sha256",
    )
    if not isinstance(runtime, Mapping) or _sha256_json(runtime) != loaded_hash:
        raise CutoverGuardError("cutover_guard_loaded_runtime_identity_invalid")
    return json.loads(json.dumps(value))


def _holder_identity(
    *,
    machine_observer: Callable[[], Mapping[str, str]],
    boot_id_observer: Callable[[], str],
    process_observer: Callable[[], Mapping[str, Any]],
) -> Mapping[str, Any]:
    machine = _normalize_machine(machine_observer())
    boot_id = boot_id_observer()
    if not isinstance(boot_id, str) or not re.fullmatch(r"[A-Za-z0-9-]{16,128}", boot_id):
        raise CutoverGuardError("cutover_guard_boot_identity_invalid")
    process = process_observer()
    if not isinstance(process, Mapping) or set(process) != {"pid", "create_time"}:
        raise CutoverGuardError("cutover_guard_holder_identity_invalid")
    pid = process.get("pid")
    create_time = process.get("create_time")
    if (
        pid != os.getpid()
        or isinstance(create_time, bool)
        or not isinstance(create_time, (int, float))
        or not math.isfinite(float(create_time))
        or float(create_time) <= 0
    ):
        raise CutoverGuardError("cutover_guard_holder_identity_invalid")
    return {
        "pid": pid,
        "process_create_time": float(create_time),
        "boot_id": boot_id,
        "machine_identity": machine,
    }


def _default_holder_process() -> Mapping[str, Any]:
    try:
        process = psutil.Process(os.getpid())
        return {"pid": process.pid, "create_time": float(process.create_time())}
    except psutil.Error as exc:
        raise CutoverGuardError("cutover_guard_holder_identity_unavailable") from exc


def _validate_lock_parent(path: Path) -> None:
    parent = path.parent
    try:
        info = parent.lstat()
    except OSError as exc:
        raise CutoverGuardError("cutover_guard_lock_parent_unavailable") from exc
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.geteuid()
    ):
        raise CutoverGuardError("cutover_guard_lock_parent_invalid")


def acquire_cutover_lease(
    inputs: LeaseInputs,
    *,
    lock_path: Path = PRODUCTION_LOCK_PATH,
    runtime_observer: Callable[[], Mapping[str, Any]] | None = None,
    machine_observer: Callable[[], Mapping[str, str]] = observe_machine_identity,
    boot_id_observer: Callable[[], str] = observe_boot_id,
    holder_process_observer: Callable[[], Mapping[str, Any]] = _default_holder_process,
    now: datetime | None = None,
    clock: Callable[[], datetime] | None = None,
) -> CutoverLease:
    if RELEASE_ID_RE.fullmatch(inputs.release_id) is None:
        raise CutoverGuardError("cutover_guard_release_id_invalid")
    if (
        isinstance(inputs.duration_seconds, bool)
        or not isinstance(inputs.duration_seconds, int)
        or inputs.duration_seconds < 1
        or inputs.duration_seconds > MAX_LEASE_SECONDS
    ):
        raise CutoverGuardError("cutover_guard_lease_duration_invalid")
    prepare = _read_stable_file(
        inputs.release_prepare_manifest,
        artifact="cutover_guard_release_prepare_manifest",
        require_owner_only=True,
    )
    approval = _read_stable_file(
        inputs.approval_receipt,
        artifact="cutover_guard_approval_receipt",
        require_owner_only=True,
    )
    expected = _normalize_running_observation(
        inputs.expected_live_runtime_identity,
        canonical_root=CANONICAL_LIVE_ROOT,
    )
    selected = lock_path.expanduser().absolute()
    _validate_lock_parent(selected)
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(selected, flags, 0o600)
    except OSError as exc:
        raise CutoverGuardError("cutover_guard_lock_unavailable") from exc
    try:
        info = os.fstat(descriptor)
        lexical = os.lstat(selected)
        if (
            not stat.S_ISREG(info.st_mode)
            or stat.S_IMODE(info.st_mode) != 0o600
            or info.st_uid != os.geteuid()
            or info.st_nlink != 1
            or stat.S_ISLNK(lexical.st_mode)
            or (info.st_dev, info.st_ino) != (lexical.st_dev, lexical.st_ino)
        ):
            raise CutoverGuardError("cutover_guard_lock_identity_invalid")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise CutoverGuardError("cutover_guard_lock_contended") from exc
        observer = runtime_observer or observe_gateway_running
        observed = _normalize_running_observation(
            observer(),
            canonical_root=CANONICAL_LIVE_ROOT,
        )
        if observed != expected:
            raise CutoverGuardError("cutover_guard_live_runtime_changed")
        current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        lease_token = secrets.token_urlsafe(32)
        holder = _holder_identity(
            machine_observer=machine_observer,
            boot_id_observer=boot_id_observer,
            process_observer=holder_process_observer,
        )
        body = {
            "schema_version": LEASE_SCHEMA_VERSION,
            "release_id": inputs.release_id,
            "acquired_at": current.isoformat(),
            "expires_at": (
                current + timedelta(seconds=inputs.duration_seconds)
            ).isoformat(),
            "release_prepare_manifest": {
                "path": str(prepare.path),
                "sha256": prepare.sha256,
            },
            "approval_receipt": {
                "path": str(approval.path),
                "sha256": approval.sha256,
            },
            "expected_live_runtime_identity": expected,
            "expected_live_runtime_identity_sha256": _sha256_json(expected),
            "lease_token_sha256": hashlib.sha256(
                lease_token.encode("utf-8")
            ).hexdigest(),
            "holder": holder,
        }
        raw = _canonical_json(body)
        if len(raw) > MAX_JSON_BYTES:
            raise CutoverGuardError("cutover_guard_lease_size_invalid")
        os.lseek(descriptor, 0, os.SEEK_SET)
        os.ftruncate(descriptor, 0)
        _write_all(descriptor, raw, artifact="cutover_guard_lease")
        os.fsync(descriptor)
        def reobserve_holder() -> Mapping[str, Any]:
            return _holder_identity(
                machine_observer=machine_observer,
                boot_id_observer=boot_id_observer,
                process_observer=holder_process_observer,
            )

        return CutoverLease(
            path=selected,
            descriptor=descriptor,
            body=body,
            raw=raw,
            fingerprint=hashlib.sha256(raw).hexdigest(),
            token=lease_token,
            holder_observer=reobserve_holder,
            clock=clock or (lambda: datetime.now(timezone.utc)),
        )
    except Exception:
        with contextlib.suppress(OSError):
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)
        raise


def _normalize_writer_stop_observation(
    value: Any,
    *,
    expected_runtime: Mapping[str, Any],
    expected_sidecar: Mapping[str, Any],
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != {
        "schema_version",
        "canonical_root",
        "launchd",
        "process_census",
        "live_runtime_identity",
        "live_sidecar_identity",
    }:
        raise CutoverGuardError("writer_stop_observation_shape_invalid")
    if (
        value.get("schema_version")
        != GATEWAY_WRITER_STOP_OBSERVATION_SCHEMA_VERSION
        or value.get("canonical_root") != str(CANONICAL_LIVE_ROOT)
    ):
        raise CutoverGuardError("writer_stop_observation_invalid")
    launchd = value.get("launchd")
    census = value.get("process_census")
    if (
        not isinstance(launchd, Mapping)
        or set(launchd) != {"label", "loaded", "pid", "state"}
        or launchd
        != {
            "label": GATEWAY_LABEL,
            "loaded": True,
            "pid": None,
            "state": "not_running",
        }
    ):
        raise CutoverGuardError("writer_stop_launchctl_state_invalid")
    if (
        not isinstance(census, Mapping)
        or set(census) != {"probe", "canonical_root", "matching_processes"}
        or census.get("probe") != "psutil_gateway_canonical_runtime_census_v1"
        or census.get("canonical_root") != str(CANONICAL_LIVE_ROOT)
        or census.get("matching_processes") != []
    ):
        raise CutoverGuardError("writer_stop_process_census_invalid")
    old_live_runtime = expected_runtime.get("live_runtime_identity")
    if (
        value.get("live_runtime_identity") != old_live_runtime
        or value.get("live_sidecar_identity") != expected_sidecar
    ):
        raise CutoverGuardError("writer_stop_live_identity_changed")
    return json.loads(json.dumps(value))


def validate_writer_stop_observation(
    value: Any,
    *,
    expected_live_runtime_identity: Mapping[str, Any],
    expected_live_sidecar_identity: Mapping[str, Any],
) -> Mapping[str, Any]:
    """Validate one externally observed stopped-Gateway state exactly."""
    return _normalize_writer_stop_observation(
        value,
        expected_runtime=expected_live_runtime_identity,
        expected_sidecar=expected_live_sidecar_identity,
    )


def observe_gateway_writer_stopped(
    *,
    expected_live_runtime_identity: Mapping[str, Any],
    expected_live_sidecar_identity: Mapping[str, Any],
    launchctl_observer: Callable[[], Mapping[str, Any]] | None = None,
    census_observer: Callable[[], Mapping[str, Any]] | None = None,
    runtime_observer: Callable[[], Mapping[str, Any]] | None = None,
    sidecar_observer: Callable[[], Mapping[str, Any]],
) -> Mapping[str, Any]:
    launchd = dict((launchctl_observer or _launchctl_print)())
    if launchd.get("pid") is None:
        launchd = {
            "label": launchd.get("label"),
            "loaded": launchd.get("loaded"),
            "pid": None,
            "state": "not_running",
        }
    value = {
        "schema_version": GATEWAY_WRITER_STOP_OBSERVATION_SCHEMA_VERSION,
        "canonical_root": str(CANONICAL_LIVE_ROOT),
        "launchd": launchd,
        "process_census": dict(
            (census_observer or observe_gateway_process_census)()
        ),
        "live_runtime_identity": dict(
            (runtime_observer or observe_live_runtime_files)()
        ),
        "live_sidecar_identity": dict(sidecar_observer()),
    }
    return _normalize_writer_stop_observation(
        value,
        expected_runtime=expected_live_runtime_identity,
        expected_sidecar=expected_live_sidecar_identity,
    )


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_all(descriptor: int, raw: bytes, *, artifact: str) -> None:
    written = 0
    while written < len(raw):
        count = os.write(descriptor, raw[written:])
        if count <= 0:
            raise CutoverGuardError(f"{artifact}_write_failed")
        written += count


def _publish_no_clobber(path: Path, body: Mapping[str, Any]) -> bool:
    destination = path.expanduser().absolute()
    raw = _canonical_json(body)
    if len(raw) > MAX_JSON_BYTES:
        raise CutoverGuardError("writer_stop_receipt_size_invalid")
    destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    parent_info = destination.parent.lstat()
    if (
        stat.S_ISLNK(parent_info.st_mode)
        or not stat.S_ISDIR(parent_info.st_mode)
        or parent_info.st_uid != os.geteuid()
    ):
        raise CutoverGuardError("writer_stop_receipt_parent_invalid")
    temporary = destination.parent / (
        f".{destination.name}.{os.getpid()}.{hashlib.sha256(raw).hexdigest()[:16]}.tmp"
    )
    if destination.exists() or destination.is_symlink():
        if temporary.exists() or temporary.is_symlink():
            destination_info = destination.lstat()
            temporary_info = temporary.lstat()
            if (
                stat.S_ISREG(destination_info.st_mode)
                and stat.S_ISREG(temporary_info.st_mode)
                and (destination_info.st_dev, destination_info.st_ino)
                == (temporary_info.st_dev, temporary_info.st_ino)
            ):
                if (
                    destination_info.st_nlink != 2
                    or temporary_info.st_nlink != 2
                    or destination_info.st_uid != os.geteuid()
                    or temporary_info.st_uid != os.geteuid()
                    or stat.S_IMODE(destination_info.st_mode) != 0o600
                    or stat.S_IMODE(temporary_info.st_mode) != 0o600
                    or destination_info.st_size != len(raw)
                ):
                    raise CutoverGuardError(
                        "writer_stop_receipt_temporary_conflict"
                    )
                descriptor = os.open(
                    destination,
                    os.O_RDONLY
                    | getattr(os, "O_CLOEXEC", 0)
                    | getattr(os, "O_NOFOLLOW", 0),
                )
                try:
                    linked_raw = os.read(descriptor, len(raw) + 1)
                finally:
                    os.close(descriptor)
                if linked_raw != raw:
                    raise CutoverGuardError("writer_stop_receipt_conflict")
                temporary.unlink()
                _fsync_directory(destination.parent)
        existing = _read_stable_file(
            destination,
            artifact="writer_stop_receipt",
            require_owner_only=True,
        )
        if existing.raw != raw:
            raise CutoverGuardError("writer_stop_receipt_conflict")
        if temporary.exists() or temporary.is_symlink():
            pending = _read_stable_file(
                temporary,
                artifact="writer_stop_receipt_temporary",
                require_owner_only=True,
            )
            if pending.raw != raw:
                raise CutoverGuardError("writer_stop_receipt_temporary_conflict")
            temporary.unlink()
            _fsync_directory(destination.parent)
        return True
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(temporary, flags, 0o600)
    except FileExistsError:
        existing = _read_stable_file(
            temporary,
            artifact="writer_stop_receipt_temporary",
            require_owner_only=True,
        )
        if existing.raw != raw:
            raise CutoverGuardError("writer_stop_receipt_temporary_conflict")
    else:
        try:
            _write_all(descriptor, raw, artifact="writer_stop_receipt")
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    try:
        os.link(temporary, destination, follow_symlinks=False)
    except FileExistsError:
        existing = _read_stable_file(
            destination,
            artifact="writer_stop_receipt",
            require_owner_only=True,
        )
        if existing.raw != raw:
            raise CutoverGuardError("writer_stop_receipt_conflict")
    _fsync_directory(destination.parent)
    with contextlib.suppress(OSError):
        temporary.unlink()
    _fsync_directory(destination.parent)
    published = _read_stable_file(
        destination,
        artifact="writer_stop_receipt",
        require_owner_only=True,
    )
    if published.raw != raw:
        raise CutoverGuardError("writer_stop_receipt_publication_invalid")
    parent_after = destination.parent.lstat()
    if (
        parent_info.st_dev,
        parent_info.st_ino,
        parent_info.st_mode,
        parent_info.st_uid,
    ) != (
        parent_after.st_dev,
        parent_after.st_ino,
        parent_after.st_mode,
        parent_after.st_uid,
    ):
        raise CutoverGuardError("writer_stop_receipt_parent_changed")
    return False


def _writer_stop_receipt_body(
    *,
    lease: CutoverLease,
    inputs: WriterStopInputs,
    observation: Mapping[str, Any],
    observed_at: datetime,
) -> Mapping[str, Any]:
    old_runtime = lease.body["expected_live_runtime_identity"]
    old_process = old_runtime["process"]
    return {
        "schema_version": WRITER_STOP_RECEIPT_SCHEMA_VERSION,
        "release_id": lease.body["release_id"],
        "hold_id": inputs.hold_id,
        "plan_sha256": inputs.plan_sha256,
        "observed_at": observed_at.isoformat(),
        "production_effects_executed": False,
        "lease_fingerprint": lease.fingerprint,
        "release_prepare_manifest_sha256": lease.body[
            "release_prepare_manifest"
        ]["sha256"],
        "approval_receipt_sha256": lease.body["approval_receipt"]["sha256"],
        "old_gateway_process": old_process,
        "old_gateway_runtime_identity": old_runtime,
        "old_gateway_runtime_identity_sha256": _sha256_json(old_runtime),
        "writer_stop_observation": observation,
        "writer_stop_observation_sha256": _sha256_json(observation),
        "live_sidecar_identity": inputs.expected_live_sidecar_identity,
        "live_sidecar_identity_sha256": _sha256_json(
            inputs.expected_live_sidecar_identity
        ),
    }


def observe_writer_stop(
    lease: CutoverLease,
    inputs: WriterStopInputs,
    *,
    writer_stop_observer: Callable[[], Mapping[str, Any]],
    now: datetime | None = None,
) -> Mapping[str, Any]:
    if HOLD_ID_RE.fullmatch(inputs.hold_id) is None:
        raise CutoverGuardError("writer_stop_hold_id_invalid")
    _sha256(inputs.plan_sha256, artifact="writer_stop_plan_sha256")
    lease.assert_active()
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    if current >= _parse_time(lease.body.get("expires_at"), artifact="cutover_lease"):
        raise CutoverGuardError("cutover_lease_expired")
    expected_runtime = lease.body["expected_live_runtime_identity"]
    first = _normalize_writer_stop_observation(
        writer_stop_observer(),
        expected_runtime=expected_runtime,
        expected_sidecar=inputs.expected_live_sidecar_identity,
    )
    body = _writer_stop_receipt_body(
        lease=lease,
        inputs=inputs,
        observation=first,
        observed_at=current,
    )
    before_publish = _normalize_writer_stop_observation(
        writer_stop_observer(),
        expected_runtime=expected_runtime,
        expected_sidecar=inputs.expected_live_sidecar_identity,
    )
    if before_publish != first:
        raise CutoverGuardError("writer_stop_observation_drift")
    _publish_no_clobber(inputs.receipt_path, body)
    after_publish = _normalize_writer_stop_observation(
        writer_stop_observer(),
        expected_runtime=expected_runtime,
        expected_sidecar=inputs.expected_live_sidecar_identity,
    )
    lease.assert_active()
    if after_publish != first:
        raise CutoverGuardError("writer_stop_observation_drift")
    owned, validated = read_writer_stop_receipt(
        inputs.receipt_path,
        now=current,
    )
    if validated != body or owned.sha256 != hashlib.sha256(_canonical_json(body)).hexdigest():
        raise CutoverGuardError("writer_stop_receipt_publication_invalid")
    return body


def read_writer_stop_receipt(
    path: Path,
    *,
    now: datetime | None = None,
) -> tuple[_StableFile, Mapping[str, Any]]:
    owned, body = _read_owned_json(path, artifact="writer_stop_receipt")
    expected_keys = {
        "schema_version",
        "release_id",
        "hold_id",
        "plan_sha256",
        "observed_at",
        "production_effects_executed",
        "lease_fingerprint",
        "release_prepare_manifest_sha256",
        "approval_receipt_sha256",
        "old_gateway_process",
        "old_gateway_runtime_identity",
        "old_gateway_runtime_identity_sha256",
        "writer_stop_observation",
        "writer_stop_observation_sha256",
        "live_sidecar_identity",
        "live_sidecar_identity_sha256",
    }
    if set(body) != expected_keys or body.get("schema_version") != (
        WRITER_STOP_RECEIPT_SCHEMA_VERSION
    ):
        raise CutoverGuardError("writer_stop_receipt_shape_invalid")
    if (
        RELEASE_ID_RE.fullmatch(str(body.get("release_id") or "")) is None
        or HOLD_ID_RE.fullmatch(str(body.get("hold_id") or "")) is None
        or body.get("production_effects_executed") is not False
    ):
        raise CutoverGuardError("writer_stop_receipt_identity_invalid")
    for field in (
        "plan_sha256",
        "lease_fingerprint",
        "release_prepare_manifest_sha256",
        "approval_receipt_sha256",
        "old_gateway_runtime_identity_sha256",
        "writer_stop_observation_sha256",
        "live_sidecar_identity_sha256",
    ):
        _sha256(body.get(field), artifact=f"writer_stop_receipt_{field}")
    if (
        body["old_gateway_runtime_identity_sha256"]
        != _sha256_json(body.get("old_gateway_runtime_identity"))
        or body["writer_stop_observation_sha256"]
        != _sha256_json(body.get("writer_stop_observation"))
        or body["live_sidecar_identity_sha256"]
        != _sha256_json(body.get("live_sidecar_identity"))
    ):
        raise CutoverGuardError("writer_stop_receipt_hash_mismatch")
    old = _normalize_running_observation(
        body.get("old_gateway_runtime_identity"),
        canonical_root=CANONICAL_LIVE_ROOT,
    )
    if body.get("old_gateway_process") != old["process"]:
        raise CutoverGuardError("writer_stop_receipt_old_process_mismatch")
    normalized_stop = _normalize_writer_stop_observation(
        body.get("writer_stop_observation"),
        expected_runtime=old,
        expected_sidecar=body.get("live_sidecar_identity"),
    )
    observed_at = _parse_time(body.get("observed_at"), artifact="writer_stop")
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    age = (current - observed_at).total_seconds()
    if age < -MAX_FUTURE_SKEW_SECONDS or age > MAX_WRITER_STOP_AGE_SECONDS:
        raise CutoverGuardError("writer_stop_receipt_stale")
    normalized = {
        **dict(body),
        "observed_at": observed_at.isoformat(),
        "old_gateway_runtime_identity": old,
        "writer_stop_observation": normalized_stop,
    }
    return owned, normalized


def plan_cutover_guard(
    *,
    runtime_observer: Callable[[], Mapping[str, Any]] = observe_gateway_running,
    lock_path: Path = PRODUCTION_LOCK_PATH,
) -> Mapping[str, Any]:
    observed = _normalize_running_observation(
        runtime_observer(),
        canonical_root=CANONICAL_LIVE_ROOT,
    )
    return {
        "schema_version": PLAN_SCHEMA_VERSION,
        "production_effects_executed": False,
        "lock_path": str(lock_path.expanduser().absolute()),
        "canonical_live_root": str(CANONICAL_LIVE_ROOT),
        "gateway_label": GATEWAY_LABEL,
        "max_lease_seconds": MAX_LEASE_SECONDS,
        "writer_stop_receipt_max_age_seconds": MAX_WRITER_STOP_AGE_SECONDS,
        "expected_live_runtime_identity": observed,
        "expected_live_runtime_identity_sha256": _sha256_json(observed),
        "mutation_commands_available": False,
    }


def doctor_cutover_lock(
    *,
    lock_path: Path = PRODUCTION_LOCK_PATH,
) -> Mapping[str, Any]:
    selected = lock_path.expanduser().absolute()
    if not selected.exists() and not selected.is_symlink():
        return {
            "schema_version": DOCTOR_SCHEMA_VERSION,
            "ok": True,
            "lock_path": str(selected),
            "state": "absent",
            "kernel_lock_held": False,
            "body_sha256": None,
            "production_effects_executed": False,
        }
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(selected, flags)
    except OSError as exc:
        raise CutoverGuardError("cutover_guard_lock_unavailable") from exc
    held = False
    try:
        info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(info.st_mode)
            or stat.S_IMODE(info.st_mode) != 0o600
            or info.st_uid != os.geteuid()
            or info.st_nlink != 1
        ):
            raise CutoverGuardError("cutover_guard_lock_identity_invalid")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            held = True
            raw = b""
        else:
            os.lseek(descriptor, 0, os.SEEK_SET)
            raw = os.read(descriptor, MAX_JSON_BYTES + 1)
            fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        os.close(descriptor)
    body_sha256 = None
    state = "held" if held else "free"
    if not held and raw:
        _strict_json(raw, artifact="cutover_guard_lock")
        body_sha256 = hashlib.sha256(raw).hexdigest()
    return {
        "schema_version": DOCTOR_SCHEMA_VERSION,
        "ok": True,
        "lock_path": str(selected),
        "state": state,
        "kernel_lock_held": held,
        "body_sha256": body_sha256,
        "production_effects_executed": False,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("plan", "doctor"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        body = plan_cutover_guard() if args.command == "plan" else doctor_cutover_lock()
    except (OSError, CutoverGuardError) as exc:
        code = exc.code if isinstance(exc, CutoverGuardError) else "cutover_guard_failed"
        print(json.dumps({"ok": False, "code": code}, sort_keys=True))
        return 2
    print(json.dumps({"ok": True, "detail": body}, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
