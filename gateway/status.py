"""
Gateway runtime status helpers.

Provides PID-file based detection of whether the gateway daemon is running,
used by send_message's check_fn to gate availability in the CLI.

The PID file lives at ``{HERMES_HOME}/gateway.pid``.  HERMES_HOME defaults to
``~/.hermes`` but can be overridden via the environment variable.  This means
separate HERMES_HOME directories naturally get separate PID files — a property
that will be useful when we add named profiles (multiple agents running
concurrently under distinct configurations).
"""

import hashlib
import json
import os
import signal
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from hermes_constants import get_hermes_home
from typing import Any, Optional
from utils import atomic_json_write

_GATEWAY_KIND = "hermes-gateway"
_RUNTIME_STATUS_FILE = "gateway_state.json"
_LOCKS_DIRNAME = "gateway-locks"
_IS_WINDOWS = sys.platform == "win32"
_UNSET = object()


def _get_pid_path() -> Path:
    """Return the path to the gateway PID file, respecting HERMES_HOME."""
    home = get_hermes_home()
    return home / "gateway.pid"


def _unlink_pid_file() -> None:
    """Best-effort unconditional PID-file unlink for known-stale records."""
    try:
        _get_pid_path().unlink(missing_ok=True)
    except Exception:
        pass


def _get_runtime_status_path() -> Path:
    """Return the persisted runtime health/status file path."""
    return _get_pid_path().with_name(_RUNTIME_STATUS_FILE)


def _get_takeover_marker_path() -> Path:
    """Return the marker written before planned gateway replacement."""
    return _get_pid_path().with_name(".gateway-takeover.json")


def _get_planned_stop_marker_path() -> Path:
    """Return the marker written before planned/manual gateway stop."""
    return _get_pid_path().with_name(".gateway-planned-stop.json")


_WINDOWS_LOCK_OFFSET = 0x7FFF0000


def _try_acquire_file_lock(handle) -> bool:
    """Best-effort non-blocking file lock used by platform-specific tests."""
    if _IS_WINDOWS:
        mod = globals().get("msvcrt")
        if mod is None:
            import msvcrt as mod  # type: ignore
        handle.seek(_WINDOWS_LOCK_OFFSET)
        handle.write("\n")
        handle.flush()
        handle.seek(_WINDOWS_LOCK_OFFSET)
        mod.locking(handle.fileno(), mod.LK_NBLCK, 1)
        return True
    try:
        import fcntl
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        return True
    except BlockingIOError:
        return False


def _release_file_lock(handle) -> None:
    if _IS_WINDOWS:
        mod = globals().get("msvcrt")
        if mod is None:
            import msvcrt as mod  # type: ignore
        handle.seek(_WINDOWS_LOCK_OFFSET)
        mod.locking(handle.fileno(), mod.LK_UNLCK, 1)
        return
    import fcntl
    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _write_json_file(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def write_takeover_marker(target_pid: int) -> bool:
    """Record that an existing gateway is being replaced intentionally."""
    try:
        _write_json_file(_get_takeover_marker_path(), {
            "target_pid": int(target_pid),
            "target_start_time": _get_process_start_time(int(target_pid)),
            "replacer_pid": os.getpid(),
            "written_at": _utc_now_iso(),
        })
        return True
    except Exception:
        return False


def clear_takeover_marker() -> None:
    """Best-effort removal of the one-shot takeover marker."""
    try:
        _get_takeover_marker_path().unlink(missing_ok=True)
    except OSError:
        pass


def consume_takeover_marker_for_self() -> bool:
    """Consume a planned-replacement marker if it names this process.

    The marker is one-shot: remove it after inspection even when stale,
    malformed, or meant for a different process so stale records cannot grief
    later shutdowns.
    """
    marker_path = _get_takeover_marker_path()
    payload = _read_json_file(marker_path)
    try:
        marker_path.unlink(missing_ok=True)
    except OSError:
        pass
    if not payload:
        return False

    try:
        written_at = payload.get("written_at")
        if written_at:
            written_dt = datetime.fromisoformat(str(written_at))
            if written_dt.tzinfo is None:
                written_dt = written_dt.replace(tzinfo=timezone.utc)
            if (datetime.now(timezone.utc) - written_dt).total_seconds() > 60:
                return False
    except Exception:
        return False

    try:
        target_pid = int(payload.get("target_pid"))
    except (TypeError, ValueError):
        return False
    if target_pid != os.getpid():
        return False

    target_start = payload.get("target_start_time")
    current_start = _get_process_start_time(os.getpid())
    if target_start is not None and current_start is not None and target_start != current_start:
        return False
    return True


def write_planned_stop_marker(target_pid: int) -> bool:
    """Record that an existing gateway is being stopped intentionally."""
    try:
        _write_json_file(_get_planned_stop_marker_path(), {
            "target_pid": int(target_pid),
            "target_start_time": _get_process_start_time(int(target_pid)),
            "stopper_pid": os.getpid(),
            "written_at": _utc_now_iso(),
        })
        return True
    except Exception:
        return False


def clear_planned_stop_marker() -> None:
    """Best-effort removal of the one-shot planned-stop marker."""
    try:
        _get_planned_stop_marker_path().unlink(missing_ok=True)
    except OSError:
        pass


def consume_planned_stop_marker_for_self() -> bool:
    """Consume a planned-stop marker if it names this process."""
    marker_path = _get_planned_stop_marker_path()
    payload = _read_json_file(marker_path)
    try:
        marker_path.unlink(missing_ok=True)
    except OSError:
        pass
    if not payload:
        return False
    try:
        written_at = payload.get("written_at")
        if written_at:
            written_dt = datetime.fromisoformat(str(written_at))
            if written_dt.tzinfo is None:
                written_dt = written_dt.replace(tzinfo=timezone.utc)
            if (datetime.now(timezone.utc) - written_dt).total_seconds() > 60:
                return False
    except Exception:
        return False
    try:
        target_pid = int(payload.get("target_pid"))
    except (TypeError, ValueError):
        return False
    if target_pid != os.getpid():
        return False
    target_start = payload.get("target_start_time")
    current_start = _get_process_start_time(os.getpid())
    if target_start is not None and current_start is not None and target_start != current_start:
        return False
    return True


def _get_lock_dir() -> Path:
    """Return the machine-local directory for token-scoped gateway locks."""
    override = os.getenv("HERMES_GATEWAY_LOCK_DIR")
    if override:
        return Path(override)
    state_home = Path(os.getenv("XDG_STATE_HOME", Path.home() / ".local" / "state"))
    return state_home / "hermes" / _LOCKS_DIRNAME


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def terminate_pid(pid: int, *, force: bool = False) -> None:
    """Terminate a PID with platform-appropriate force semantics.

    POSIX uses SIGTERM/SIGKILL. Windows uses taskkill /T /F for true force-kill
    because os.kill(..., SIGTERM) is not equivalent to a tree-killing hard stop.
    """
    if force and _IS_WINDOWS:
        try:
            result = subprocess.run(
                ["taskkill", "/PID", str(pid), "/T", "/F"],
                capture_output=True,
                text=True,
                timeout=10,
            )
        except FileNotFoundError:
            os.kill(pid, signal.SIGTERM)
            return

        if result.returncode != 0:
            details = (result.stderr or result.stdout or "").strip()
            raise OSError(details or f"taskkill failed for PID {pid}")
        return

    sig = signal.SIGTERM if not force else getattr(signal, "SIGKILL", signal.SIGTERM)
    os.kill(pid, sig)


def _scope_hash(identity: str) -> str:
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16]


def _get_scope_lock_path(scope: str, identity: str) -> Path:
    return _get_lock_dir() / f"{scope}-{_scope_hash(identity)}.lock"


def _get_process_start_time(pid: int) -> Optional[int]:
    """Return the kernel start time for a process when available."""
    stat_path = Path(f"/proc/{pid}/stat")
    try:
        # Field 22 in /proc/<pid>/stat is process start time (clock ticks).
        return int(stat_path.read_text().split()[21])
    except (FileNotFoundError, IndexError, PermissionError, ValueError, OSError):
        return None


def _read_process_cmdline(pid: int) -> Optional[str]:
    """Return the process command line as a space-separated string."""
    cmdline_path = Path(f"/proc/{pid}/cmdline")
    try:
        raw = cmdline_path.read_bytes()
    except (FileNotFoundError, PermissionError, OSError):
        return None

    if not raw:
        return None
    return raw.replace(b"\x00", b" ").decode("utf-8", errors="ignore").strip()


def _looks_like_gateway_process(pid: int) -> bool:
    """Return True when the live PID still looks like the Hermes gateway."""
    cmdline = _read_process_cmdline(pid)
    if not cmdline:
        return False

    patterns = (
        "hermes_cli.main gateway",
        "hermes_cli/main.py gateway",
        "hermes gateway",
        "gateway/run.py",
    )
    return any(pattern in cmdline for pattern in patterns)


def _record_looks_like_gateway(record: dict[str, Any]) -> bool:
    """Validate gateway identity from PID-file metadata when cmdline is unavailable."""
    if record.get("kind") != _GATEWAY_KIND:
        return False

    argv = record.get("argv")
    if not isinstance(argv, list) or not argv:
        return False

    cmdline = " ".join(str(part) for part in argv)
    patterns = (
        "hermes_cli.main gateway",
        "hermes_cli/main.py gateway",
        "hermes gateway",
        "gateway/run.py",
    )
    return any(pattern in cmdline for pattern in patterns)


def _build_pid_record() -> dict:
    return {
        "pid": os.getpid(),
        "kind": _GATEWAY_KIND,
        "argv": list(sys.argv),
        "start_time": _get_process_start_time(os.getpid()),
    }


def _build_runtime_status_record() -> dict[str, Any]:
    payload = _build_pid_record()
    payload.update({
        "gateway_state": "starting",
        "exit_reason": None,
        "restart_requested": False,
        "active_agents": 0,
        "platforms": {},
        "updated_at": _utc_now_iso(),
    })
    return payload


def _read_json_file(path: Path) -> Optional[dict[str, Any]]:
    if not path.exists():
        return None
    try:
        raw = path.read_text().strip()
    except OSError:
        return None
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def _write_json_file(path: Path, payload: dict[str, Any], *, exclusive: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if exclusive:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(payload, handle)
        except Exception:
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass
            raise
        return
    atomic_json_write(path, payload, indent=None, separators=(",", ":"))


def _read_pid_record() -> Optional[dict]:
    pid_path = _get_pid_path()
    if not pid_path.exists():
        return None

    raw = pid_path.read_text().strip()
    if not raw:
        return None

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        try:
            return {"pid": int(raw)}
        except ValueError:
            return None

    if isinstance(payload, int):
        return {"pid": payload}
    if isinstance(payload, dict):
        return payload
    return None


def write_pid_file() -> None:
    """Write the current process PID and metadata to the gateway PID file."""
    _write_json_file(_get_pid_path(), _build_pid_record(), exclusive=True)


def _get_runtime_lock_path() -> Path:
    return _get_pid_path().with_name("gateway.lock")


def acquire_gateway_runtime_lock(lock_path: Optional[Path] = None) -> bool:
    """Claim the current HERMES_HOME gateway runtime lock for this process."""
    path = Path(lock_path) if lock_path is not None else _get_runtime_lock_path()
    record = _build_pid_record()
    existing = _read_json_file(path)
    if existing:
        try:
            existing_pid = int(existing.get("pid"))
        except (TypeError, ValueError):
            existing_pid = None
        if existing_pid == os.getpid() and existing.get("start_time") == record.get("start_time"):
            _write_json_file(path, record)
            return True
        stale = existing_pid is None
        if not stale:
            try:
                os.kill(existing_pid, 0)
            except (ProcessLookupError, PermissionError):
                stale = True
            else:
                current_start = _get_process_start_time(existing_pid)
                if existing.get("start_time") is not None and current_start is not None and current_start != existing.get("start_time"):
                    stale = True
        if not stale:
            return False
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass
    try:
        _write_json_file(path, record, exclusive=True)
        return True
    except FileExistsError:
        return False


def is_gateway_runtime_lock_active(lock_path: Optional[Path] = None) -> bool:
    """Return whether a live process owns the gateway runtime lock."""
    path = Path(lock_path) if lock_path is not None else _get_runtime_lock_path()
    record = _read_json_file(path)
    if not record:
        return False
    try:
        pid = int(record.get("pid"))
    except (TypeError, ValueError):
        return False
    try:
        os.kill(pid, 0)
    except (ProcessLookupError, PermissionError):
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass
        return False
    current_start = _get_process_start_time(pid)
    if record.get("start_time") is not None and current_start is not None and current_start != record.get("start_time"):
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass
        return False
    return True


def release_gateway_runtime_lock(lock_path: Optional[Path] = None) -> None:
    """Release this process's gateway runtime lock."""
    path = Path(lock_path) if lock_path is not None else _get_runtime_lock_path()
    record = _read_json_file(path)
    if not record:
        return
    if record.get("pid") != os.getpid():
        return
    if record.get("start_time") != _get_process_start_time(os.getpid()):
        return
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass


def write_runtime_status(
    *,
    gateway_state: Any = _UNSET,
    exit_reason: Any = _UNSET,
    restart_requested: Any = _UNSET,
    active_agents: Any = _UNSET,
    platform: Any = _UNSET,
    platform_state: Any = _UNSET,
    error_code: Any = _UNSET,
    error_message: Any = _UNSET,
) -> None:
    """Persist gateway runtime health information for diagnostics/status."""
    path = _get_runtime_status_path()
    payload = _read_json_file(path) or _build_runtime_status_record()
    payload.setdefault("platforms", {})
    payload.setdefault("kind", _GATEWAY_KIND)
    payload["pid"] = os.getpid()
    payload["start_time"] = _get_process_start_time(os.getpid())
    payload["updated_at"] = _utc_now_iso()

    if gateway_state is not _UNSET:
        payload["gateway_state"] = gateway_state
    if exit_reason is not _UNSET:
        payload["exit_reason"] = exit_reason
    if restart_requested is not _UNSET:
        payload["restart_requested"] = bool(restart_requested)
    if active_agents is not _UNSET:
        payload["active_agents"] = max(0, int(active_agents))

    if platform is not _UNSET:
        platform_payload = payload["platforms"].get(platform, {})
        if platform_state is not _UNSET:
            platform_payload["state"] = platform_state
        if error_code is not _UNSET:
            platform_payload["error_code"] = error_code
        if error_message is not _UNSET:
            platform_payload["error_message"] = error_message
        platform_payload["updated_at"] = _utc_now_iso()
        payload["platforms"][platform] = platform_payload

    _write_json_file(path, payload)


def read_runtime_status() -> Optional[dict[str, Any]]:
    """Read the persisted gateway runtime health/status information."""
    return _read_json_file(_get_runtime_status_path())


def remove_pid_file() -> None:
    """Remove the gateway PID file, but only if it belongs to this process.

    During --replace handoffs, the old process's atexit handler can fire AFTER
    the new process has written its own PID file.  Blindly removing the file
    would delete the new process's record, leaving the gateway running with no
    PID file (invisible to ``get_running_pid()``).
    """
    try:
        path = _get_pid_path()
        record = _read_json_file(path)
        if record is not None:
            try:
                file_pid = int(record["pid"])
            except (KeyError, TypeError, ValueError):
                file_pid = None
            if file_pid is not None and file_pid != os.getpid():
                # PID file belongs to a different process — leave it alone.
                return
        path.unlink(missing_ok=True)
    except Exception:
        pass


def acquire_scoped_lock(scope: str, identity: str, metadata: Optional[dict[str, Any]] = None) -> tuple[bool, Optional[dict[str, Any]]]:
    """Acquire a machine-local lock keyed by scope + identity.

    Used to prevent multiple local gateways from using the same external identity
    at once (e.g. the same Telegram bot token across different HERMES_HOME dirs).
    """
    lock_path = _get_scope_lock_path(scope, identity)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    record = {
        **_build_pid_record(),
        "scope": scope,
        "identity_hash": _scope_hash(identity),
        "metadata": metadata or {},
        "updated_at": _utc_now_iso(),
    }

    existing = _read_json_file(lock_path)
    if existing is None and lock_path.exists():
        # Lock file exists but is empty or contains invalid JSON — treat as
        # stale.  This happens when a previous process was killed between
        # O_CREAT|O_EXCL and the subsequent json.dump() (e.g. DNS failure
        # during rapid Slack reconnect retries).
        try:
            lock_path.unlink(missing_ok=True)
        except OSError:
            pass
    if existing:
        try:
            existing_pid = int(existing["pid"])
        except (KeyError, TypeError, ValueError):
            existing_pid = None

        if existing_pid == os.getpid() and existing.get("start_time") == record.get("start_time"):
            _write_json_file(lock_path, record)
            return True, existing

        stale = existing_pid is None
        if not stale:
            try:
                os.kill(existing_pid, 0)
            except (ProcessLookupError, PermissionError):
                stale = True
            else:
                current_start = _get_process_start_time(existing_pid)
                if (
                    existing.get("start_time") is not None
                    and current_start is not None
                    and current_start != existing.get("start_time")
                ):
                    stale = True
                # Check if process is stopped (Ctrl+Z / SIGTSTP) — stopped
                # processes still respond to os.kill(pid, 0) but are not
                # actually running. Treat them as stale so --replace works.
                if not stale:
                    try:
                        _proc_status = Path(f"/proc/{existing_pid}/status")
                        if _proc_status.exists():
                            for _line in _proc_status.read_text().splitlines():
                                if _line.startswith("State:"):
                                    _state = _line.split()[1]
                                    if _state in ("T", "t"):  # stopped or tracing stop
                                        stale = True
                                    break
                    except (OSError, PermissionError):
                        pass
        if stale:
            try:
                lock_path.unlink(missing_ok=True)
            except OSError:
                pass
        else:
            return False, existing

    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        return False, _read_json_file(lock_path)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(record, handle)
    except Exception:
        try:
            lock_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise
    return True, None


def release_scoped_lock(scope: str, identity: str) -> None:
    """Release a previously-acquired scope lock when owned by this process."""
    lock_path = _get_scope_lock_path(scope, identity)
    existing = _read_json_file(lock_path)
    if not existing:
        return
    if existing.get("pid") != os.getpid():
        return
    if existing.get("start_time") != _get_process_start_time(os.getpid()):
        return
    try:
        lock_path.unlink(missing_ok=True)
    except OSError:
        pass


def release_all_scoped_locks(
    *,
    owner_pid: Optional[int] = None,
    owner_start_time: Optional[Any] = None,
) -> int:
    """Remove all scoped lock files in the lock directory.

    Called during --replace to clean up stale locks left by stopped/killed
    gateway processes that did not release their locks gracefully.  When
    ``owner_pid`` and/or ``owner_start_time`` are provided, only locks owned by
    that process identity are removed.
    Returns the number of lock files removed.
    """
    lock_dir = _get_lock_dir()
    removed = 0
    if lock_dir.exists():
        for lock_file in lock_dir.glob("*.lock"):
            try:
                if owner_pid is not None or owner_start_time is not None:
                    existing = _read_json_file(lock_file)
                    if not existing:
                        continue
                    if owner_pid is not None and existing.get("pid") != owner_pid:
                        continue
                    if owner_start_time is not None and existing.get("start_time") != owner_start_time:
                        continue
                lock_file.unlink(missing_ok=True)
                removed += 1
            except OSError:
                pass
    return removed


def get_running_pid(pid_path: Optional[Path] = None, *, cleanup_stale: bool = True) -> Optional[int]:
    """Return the PID of a running gateway instance, or ``None``.

    Checks the PID file and verifies the process is actually alive.
    Cleans up stale PID files automatically unless cleanup_stale=False.
    """
    original_get_pid_path = None
    if pid_path is not None:
        path = Path(pid_path)
        record = _read_json_file(path)
        if record is None and path.exists():
            try:
                record = {"pid": int(path.read_text().strip())}
            except Exception:
                record = None
    else:
        path = _get_pid_path()
        record = _read_pid_record()

    def _cleanup() -> None:
        if not cleanup_stale:
            return
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass
        try:
            path.with_name("gateway.lock").unlink(missing_ok=True)
        except OSError:
            pass

    if not record:
        _cleanup()
        return None

    try:
        pid = int(record["pid"])
    except (KeyError, TypeError, ValueError):
        _cleanup()
        return None

    try:
        os.kill(pid, 0)  # signal 0 = existence check, no actual signal sent
    except (ProcessLookupError, PermissionError):
        if path.with_name("gateway.lock").exists():
            lock_record = _read_json_file(path.with_name("gateway.lock"))
            try:
                lock_pid = int(lock_record.get("pid")) if lock_record else None
            except (TypeError, ValueError):
                lock_pid = None
            if lock_pid == os.getpid() and lock_record and lock_record.get("start_time") == record.get("start_time"):
                return os.getpid()
        _cleanup()
        return None

    recorded_start = record.get("start_time")
    current_start = _get_process_start_time(pid)
    if recorded_start is not None and current_start is not None and current_start != recorded_start:
        _cleanup()
        return None

    if not _looks_like_gateway_process(pid):
        lock_active = is_gateway_runtime_lock_active(path.with_name("gateway.lock"))
        if not lock_active:
            lock_record = _read_json_file(path.with_name("gateway.lock"))
            try:
                lock_pid = int(lock_record.get("pid")) if lock_record else None
            except (TypeError, ValueError):
                lock_pid = None
            lock_active = (
                lock_pid == pid
                and lock_record is not None
                and lock_record.get("start_time") == record.get("start_time")
            )
        if not _record_looks_like_gateway(record) or not lock_active:
            _cleanup()
            return None

    return pid


def is_gateway_running() -> bool:
    """Check if the gateway daemon is currently running."""
    return get_running_pid() is not None


def _is_process_alive(pid: int) -> bool:
    """Check if a process is still alive."""
    try:
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, PermissionError):
        return False


def get_all_gateway_processes() -> list[tuple[int, Optional[str]]]:
    """Scan all gateway processes, return list of (PID, HERMES_HOME).
    
    Returns:
        List of (pid, hermes_home) tuples. hermes_home may be None if not detectable.
        Excludes the current process.
    
    Changed in: v1.7.0 — startup guard to prevent process pileup
    """
    import logging
    logger = logging.getLogger(__name__)
    
    current_pid = os.getpid()
    processes = []
    
    try:
        # Try ps auxe first (shows environment variables)
        result = subprocess.run(
            ["ps", "auxe"] if not _IS_WINDOWS else ["tasklist", "/V"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        
        if result.returncode != 0:
            # Fall back to ps aux (no environment)
            result = subprocess.run(
                ["ps", "aux"] if not _IS_WINDOWS else ["tasklist"],
                capture_output=True,
                text=True,
                timeout=5,
            )
        
        for line in result.stdout.splitlines():
            # Look for gateway processes — match various invocation patterns:
            # - hermes gateway run
            # - hermes_cli.main gateway run
            # - hermes_cli/main.py gateway run
            # - gateway/run.py
            _lower = line.lower()
            is_gateway = (
                ("hermes gateway run" in _lower)
                or ("hermes_cli.main gateway" in _lower)
                or ("hermes_cli/main.py gateway" in _lower)
                or ("gateway/run.py" in _lower)
            )
            if not is_gateway:
                continue
            
            parts = line.split()
            if len(parts) < 2:
                continue
            
            try:
                pid = int(parts[1])
            except ValueError:
                continue
            
            # Skip self
            if pid == current_pid:
                continue
            
            # Extract HERMES_HOME from environment if available
            hermes_home = None
            for part in parts:
                if "HERMES_HOME=" in part:
                    hermes_home = part.split("=", 1)[1]
                    break
            
            processes.append((pid, hermes_home))
    
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError) as e:
        logger.warning("Failed to scan gateway processes: %s", e)
    
    return processes
