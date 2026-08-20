"""Bounded lark-cli transport for Aily Agent calls with user identity.

The lark-cli process remains the credential broker. This module never reads or
exports the user access token. Every chat is preceded by an ``--as user``
identity lookup whose app-scoped open_id and stable union_id must match
operator-pinned values.
"""

from __future__ import annotations

import json
import math
import os
import re
import signal
import shutil
import stat
import subprocess
import tempfile
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any


LARK_CONFIG_DIR_ENV = "LARKSUITE_CLI_CONFIG_DIR"

_USER_INFO_PATH = "/open-apis/authen/v1/user_info"
_CHAT_PATH = "/open-apis/aily/v1/agents/{agent_id}/chats"
_CHAT_RESULT_PATH = "/open-apis/aily/v1/agents/{agent_id}/chats/{agent_chat_id}"
_MAX_COMMAND_OUTPUT_BYTES = 8 * 1024 * 1024
_MAX_REQUEST_BYTES = 256 * 1024
_MAX_POLL_REQUESTS = 120
_POLL_INITIAL_INTERVAL_SECONDS = 1.0
_POLL_MAX_INTERVAL_SECONDS = 5.0
_NON_TERMINAL_STATUSES = {"queued", "pending", "running", "processing"}
_TERMINAL_STATUSES = {"completed", "failed", "failure", "error", "cancelled", "canceled"}

_APP_ID_RE = re.compile(r"cli_[A-Za-z0-9_-]+\Z")
_OPEN_ID_RE = re.compile(r"ou_[A-Za-z0-9_-]+\Z")
_UNION_ID_RE = re.compile(r"on_[A-Za-z0-9_-]+\Z")
_AGENT_ID_RE = re.compile(r"agent_[A-Za-z0-9_-]+\Z")
_CHAT_ID_RE = re.compile(r"[A-Za-z0-9_-]+\Z")

_TRANSPORT_LOCK = threading.Lock()


@dataclass(frozen=True)
class AilyAgentUserConfig:
    config_dir: Path
    profile: str
    expected_app_id: str
    expected_user_open_id: str
    expected_union_id: str
    agent_id: str


class AilyAgentUserTransportError(RuntimeError):
    """A constant-message, non-secret transport failure."""

    def __init__(self, phase: str, message: str, *, code: int | None = None):
        self.phase = phase
        self.code = code
        super().__init__(message)


@dataclass(frozen=True)
class _CommandResult:
    returncode: int
    payload: dict[str, Any]


def _failure(phase: str, message: str, *, code: int | None = None) -> AilyAgentUserTransportError:
    return AilyAgentUserTransportError(phase, message, code=code)


def _validate_identifier(value: str, pattern: re.Pattern[str], label: str, max_chars: int) -> str:
    normalized = str(value or "").strip()
    if len(normalized) > max_chars or not pattern.fullmatch(normalized):
        raise _failure("config", f"{label} is invalid")
    return normalized


def _validate_config(config: AilyAgentUserConfig) -> AilyAgentUserConfig:
    config_dir = Path(config.config_dir).expanduser()
    if not config_dir.is_absolute():
        raise _failure("config", "lark-cli config directory must be absolute")
    try:
        path_stat = config_dir.lstat()
    except OSError as exc:
        raise _failure("config", "lark-cli config directory is unavailable") from exc
    if stat.S_ISLNK(path_stat.st_mode) or not stat.S_ISDIR(path_stat.st_mode):
        raise _failure("config", "lark-cli config directory must be a real directory")
    if path_stat.st_uid != os.getuid():
        raise _failure("config", "lark-cli config directory must be owned by the current user")
    if stat.S_IMODE(path_stat.st_mode) != 0o700:
        raise _failure("config", "lark-cli config directory permissions must be 0700")

    expected_app_id = _validate_identifier(
        config.expected_app_id, _APP_ID_RE, "expected App ID", 128
    )
    profile = str(config.profile or "").strip()
    if profile != expected_app_id:
        raise _failure("config", "lark-cli profile must equal the expected App ID")
    expected_user_open_id = _validate_identifier(
        config.expected_user_open_id, _OPEN_ID_RE, "expected user open_id", 128
    )
    expected_union_id = _validate_identifier(
        config.expected_union_id, _UNION_ID_RE, "expected user union_id", 128
    )
    agent_id = _validate_identifier(config.agent_id, _AGENT_ID_RE, "Agent ID", 65)
    return AilyAgentUserConfig(
        config_dir=config_dir,
        profile=profile,
        expected_app_id=expected_app_id,
        expected_user_open_id=expected_user_open_id,
        expected_union_id=expected_union_id,
        agent_id=agent_id,
    )


def _resolve_lark_cli() -> str:
    executable = shutil.which("lark-cli")
    if not executable:
        raise _failure("config", "lark-cli executable was not found")
    try:
        resolved = Path(executable).resolve(strict=True)
        path_stat = resolved.stat()
    except OSError as exc:
        raise _failure("config", "lark-cli executable is unavailable") from exc
    if not stat.S_ISREG(path_stat.st_mode) or not os.access(resolved, os.X_OK):
        raise _failure("config", "lark-cli executable is invalid")
    if path_stat.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
        raise _failure("config", "lark-cli executable is group/world writable")
    return str(resolved)


def _broker_env(config_dir: Path) -> dict[str, str]:
    """Build a minimal environment without forwarding application secrets."""
    env = {LARK_CONFIG_DIR_ENV: str(config_dir)}
    for name in (
        "PATH",
        "HOME",
        "USER",
        "LOGNAME",
        "TMPDIR",
        "LANG",
        "LC_ALL",
        "LC_CTYPE",
        "SSL_CERT_FILE",
        "SSL_CERT_DIR",
    ):
        value = os.environ.get(name)
        if value:
            env[name] = value
    return env


def _as_bytes(value: Any) -> bytes:
    if value is None:
        return b""
    if isinstance(value, bytes):
        return value
    return str(value).encode("utf-8", errors="replace")


def _kill_process_group(process: subprocess.Popen) -> None:
    """Terminate the task-owned lark-cli process group and reap its leader."""
    if process.poll() is not None:
        return
    try:
        if os.name == "posix":
            os.killpg(process.pid, signal.SIGKILL)
        else:
            process.kill()
    except (OSError, ProcessLookupError):
        try:
            process.kill()
        except OSError:
            pass
    try:
        process.wait(timeout=5)
    except (OSError, subprocess.TimeoutExpired):
        pass


def _bounded_subprocess_run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess:
    """Run lark-cli with bounded file-backed output and process-group cleanup."""
    timeout = float(kwargs.pop("timeout"))
    input_bytes = kwargs.pop("input", None)
    stdin = kwargs.pop("stdin", None)
    kwargs.pop("check", None)
    kwargs.pop("stdout", None)
    kwargs.pop("stderr", None)
    kwargs["start_new_session"] = True
    deadline = time.monotonic() + timeout

    with (
        tempfile.TemporaryFile() as stdin_file,
        tempfile.TemporaryFile() as stdout_file,
        tempfile.TemporaryFile() as stderr_file,
    ):
        if input_bytes is not None:
            stdin_file.write(input_bytes)
            stdin_file.seek(0)
            stdin = stdin_file
        process = subprocess.Popen(
            command,
            stdin=stdin,
            stdout=stdout_file,
            stderr=stderr_file,
            **kwargs,
        )
        try:
            while process.poll() is None:
                output_size = (
                    os.fstat(stdout_file.fileno()).st_size
                    + os.fstat(stderr_file.fileno()).st_size
                )
                if output_size > _MAX_COMMAND_OUTPUT_BYTES:
                    _kill_process_group(process)
                    raise _failure("output", "lark-cli output exceeded the byte limit")
                if time.monotonic() >= deadline:
                    _kill_process_group(process)
                    raise subprocess.TimeoutExpired(command, timeout)
                time.sleep(0.02)

            returncode = process.wait()
            output_size = (
                os.fstat(stdout_file.fileno()).st_size
                + os.fstat(stderr_file.fileno()).st_size
            )
            if output_size > _MAX_COMMAND_OUTPUT_BYTES:
                raise _failure("output", "lark-cli output exceeded the byte limit")
            stdout_file.seek(0)
            stderr_file.seek(0)
            return subprocess.CompletedProcess(
                command,
                returncode,
                stdout_file.read(),
                stderr_file.read(),
            )
        except BaseException:
            _kill_process_group(process)
            raise


def _remaining(deadline: float, clock: Callable[[], float]) -> float:
    remaining = deadline - clock()
    if remaining <= 0:
        raise _failure("timeout", "Aily user request exceeded its total timeout")
    return remaining


def _run_json_command(
    command: list[str],
    *,
    config_dir: Path,
    deadline: float,
    input_bytes: bytes | None,
    runner: Callable[..., Any],
    clock: Callable[[], float],
) -> _CommandResult:
    kwargs: dict[str, Any] = {
        "stdout": subprocess.PIPE,
        "stderr": subprocess.PIPE,
        "timeout": _remaining(deadline, clock),
        "check": False,
        "env": _broker_env(config_dir),
        "cwd": "/",
        "start_new_session": True,
    }
    if input_bytes is None:
        kwargs["stdin"] = subprocess.DEVNULL
    else:
        if len(input_bytes) > _MAX_REQUEST_BYTES:
            raise _failure("input", "Aily user request body exceeded the byte limit")
        kwargs["input"] = input_bytes

    try:
        completed = runner(command, **kwargs)
    except subprocess.TimeoutExpired as exc:
        raise _failure("timeout", "lark-cli command exceeded the remaining timeout") from exc
    except FileNotFoundError as exc:
        raise _failure("config", "lark-cli executable was not found") from exc
    except OSError as exc:
        raise _failure("command", "lark-cli could not start") from exc

    stdout = _as_bytes(getattr(completed, "stdout", b""))
    stderr = _as_bytes(getattr(completed, "stderr", b""))
    if len(stdout) + len(stderr) > _MAX_COMMAND_OUTPUT_BYTES:
        raise _failure("output", "lark-cli output exceeded the byte limit")
    try:
        payload = json.loads(stdout.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise _failure("command", "lark-cli returned invalid JSON") from exc
    if not isinstance(payload, dict):
        raise _failure("command", "lark-cli returned non-object JSON")
    returncode = getattr(completed, "returncode", None)
    if isinstance(returncode, bool) or not isinstance(returncode, int):
        raise _failure("command", "lark-cli returned an invalid exit status")
    return _CommandResult(returncode, payload)


def _run(
    executable: str,
    config: AilyAgentUserConfig,
    args: list[str],
    *,
    deadline: float,
    input_bytes: bytes | None,
    runner: Callable[..., Any],
    clock: Callable[[], float],
) -> _CommandResult:
    return _run_json_command(
        [executable, "--profile", config.profile, *args],
        config_dir=config.config_dir,
        deadline=deadline,
        input_bytes=input_bytes,
        runner=runner,
        clock=clock,
    )


def _api_payload(result: _CommandResult, *, phase: str) -> dict[str, Any]:
    code = result.payload.get("code")
    valid_code = isinstance(code, int) and not isinstance(code, bool)
    if result.returncode != 0 or not valid_code or code != 0:
        safe_code = code if valid_code else None
        raise _failure(phase, "lark-cli Aily API request failed", code=safe_code)
    return result.payload


def _preflight_identity(
    executable: str,
    config: AilyAgentUserConfig,
    *,
    deadline: float,
    runner: Callable[..., Any],
    clock: Callable[[], float],
) -> None:
    # auth status/check follow the profile's defaultAs setting and can therefore
    # describe the bot token even when a valid user token exists. The explicit
    # user_info request is both a server-side token check and the authoritative
    # app-scoped/stable identity binding for this transport.
    user_info = _api_payload(
        _run(
            executable,
            config,
            ["api", "GET", _USER_INFO_PATH, "--as", "user", "--format", "json"],
            deadline=deadline,
            input_bytes=None,
            runner=runner,
            clock=clock,
        ),
        phase="identity",
    )
    data = user_info.get("data")
    if not isinstance(data, dict):
        raise _failure("identity", "authenticated user info has no data object")
    if data.get("open_id") != config.expected_user_open_id:
        raise _failure("identity", "user info open_id does not match the pinned identity")
    if data.get("union_id") != config.expected_union_id:
        raise _failure("identity", "user info union_id does not match the pinned identity")


def _run_agent_chat_locked(
    executable: str,
    config: AilyAgentUserConfig,
    *,
    content: str,
    session_id: str | None,
    agent_attachment_ids: list[str],
    deadline: float,
    runner: Callable[..., Any],
    clock: Callable[[], float],
    sleeper: Callable[[float], None],
) -> dict[str, Any]:
    _preflight_identity(
        executable,
        config,
        deadline=deadline,
        runner=runner,
        clock=clock,
    )

    message: dict[str, Any] = {"content": [{"type": "text", "text": content}]}
    if agent_attachment_ids:
        message["agent_attachment_ids"] = agent_attachment_ids
    body: dict[str, Any] = {"user_message": message, "stream": False}
    if session_id:
        body["session_id"] = session_id
    input_bytes = json.dumps(
        body, ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")

    create_path = _CHAT_PATH.format(agent_id=config.agent_id)
    created = _api_payload(
        _run(
            executable,
            config,
            [
                "api",
                "POST",
                create_path,
                "--as",
                "user",
                "--data",
                "-",
                "--format",
                "json",
            ],
            deadline=deadline,
            input_bytes=input_bytes,
            runner=runner,
            clock=clock,
        ),
        phase="create",
    )
    create_data = created.get("data")
    if not isinstance(create_data, dict):
        raise _failure("create", "Aily user create response has no data object")
    agent_chat_id = create_data.get("agent_chat_id")
    if (
        not isinstance(agent_chat_id, str)
        or len(agent_chat_id) > 64
        or not _CHAT_ID_RE.fullmatch(agent_chat_id)
    ):
        raise _failure("create", "Aily user create response has no valid agent_chat_id")
    created_session_id = create_data.get("session_id")
    if not isinstance(created_session_id, str):
        created_session_id = session_id

    poll_path = _CHAT_RESULT_PATH.format(
        agent_id=config.agent_id,
        agent_chat_id=agent_chat_id,
    )
    interval = _POLL_INITIAL_INTERVAL_SECONDS
    sleeper(min(interval, _remaining(deadline, clock)))
    interval = min(_POLL_MAX_INTERVAL_SECONDS, interval * 2)
    for _poll_count in range(_MAX_POLL_REQUESTS):
        polled = _api_payload(
            _run(
                executable,
                config,
                ["api", "GET", poll_path, "--as", "user", "--format", "json"],
                deadline=deadline,
                input_bytes=None,
                runner=runner,
                clock=clock,
            ),
            phase="poll",
        )
        data = polled.get("data")
        if not isinstance(data, dict):
            raise _failure("poll", "Aily user poll response has no data object")
        status_value = data.get("status")
        status = status_value.lower() if isinstance(status_value, str) else ""
        if status in _TERMINAL_STATUSES:
            completed = dict(data)
            completed.setdefault("agent_chat_id", agent_chat_id)
            if created_session_id:
                completed.setdefault("session_id", created_session_id)
            return {"code": 0, "data": completed}
        if status not in _NON_TERMINAL_STATUSES:
            raise _failure("result", "Aily user result returned an unknown status")
        remaining = _remaining(deadline, clock)
        sleeper(min(interval, remaining))
        interval = min(_POLL_MAX_INTERVAL_SECONDS, interval * 2)

    raise _failure("timeout", "Aily user result exceeded the poll request limit")


def run_agent_chat_user(
    config: AilyAgentUserConfig,
    *,
    content: str,
    session_id: str | None = None,
    agent_attachment_ids: list[str] | None = None,
    timeout: float = 120.0,
    runner: Callable[..., Any] | None = None,
    clock: Callable[[], float] | None = None,
    sleeper: Callable[[float], None] | None = None,
    executable_resolver: Callable[[], str] | None = None,
) -> dict[str, Any]:
    """Run one identity-pinned Aily Agent chat and return its terminal payload."""
    if (
        isinstance(timeout, bool)
        or not isinstance(timeout, (int, float))
        or not math.isfinite(timeout)
        or timeout <= 0
        or timeout > 300
    ):
        raise _failure("input", "timeout must be finite, positive, and at most 300 seconds")
    if not isinstance(content, str) or not content.strip():
        raise _failure("input", "Aily user content is required")
    attachments = agent_attachment_ids or []
    if not isinstance(attachments, list) or any(not isinstance(item, str) for item in attachments):
        raise _failure("input", "Aily user attachments are invalid")

    config = _validate_config(config)
    runner = runner or _bounded_subprocess_run
    clock = clock or time.monotonic
    sleeper = sleeper or time.sleep
    executable = (executable_resolver or _resolve_lark_cli)()
    deadline = clock() + float(timeout)
    acquired = _TRANSPORT_LOCK.acquire(timeout=_remaining(deadline, clock))
    if not acquired:
        raise _failure("timeout", "Aily user transport lock exceeded the total timeout")
    try:
        return _run_agent_chat_locked(
            executable,
            config,
            content=content.strip(),
            session_id=session_id,
            agent_attachment_ids=attachments,
            deadline=deadline,
            runner=runner,
            clock=clock,
            sleeper=sleeper,
        )
    finally:
        _TRANSPORT_LOCK.release()
