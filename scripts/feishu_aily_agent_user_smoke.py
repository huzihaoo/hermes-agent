#!/usr/bin/env python3
"""Run a bounded Aily Agent smoke test through lark-cli user identity.

The CLI is the only credential broker.  This script never imports token or
secret values from the environment file, and it only emits selected metadata
from lark-cli responses.  Answer text is hidden unless ``--show-answer`` is
supplied.
"""

from __future__ import annotations

import argparse
import io
import json
import math
import os
import re
import signal
import stat
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from dotenv.parser import parse_stream

CONFIG_DIR_ENV = "LARKSUITE_CLI_CONFIG_DIR"
USER_INFO_PATH = "/open-apis/authen/v1/user_info"
CHAT_PATH = "/open-apis/aily/v1/agents/{agent_id}/chats"
CHAT_RESULT_PATH = "/open-apis/aily/v1/agents/{agent_id}/chats/{agent_chat_id}"

MAX_COMMAND_OUTPUT_BYTES = 1024 * 1024
MAX_ENV_FILE_BYTES = 1024 * 1024
MAX_QUESTION_CHARS = 10_000
MAX_DISPLAY_ANSWER_CHARS = 300
MAX_POLL_REQUESTS = 120
DEFAULT_POLL_INTERVAL_SECONDS = 1.0
MAX_POLL_INTERVAL_SECONDS = 5.0

NON_TERMINAL_STATUSES = {"Queued", "Pending", "Running", "Processing"}
APP_ID_RE = re.compile(r"cli_[A-Za-z0-9_-]+\Z")
USER_OPEN_ID_RE = re.compile(r"ou_[A-Za-z0-9_-]+\Z")
USER_UNION_ID_RE = re.compile(r"on_[A-Za-z0-9_-]+\Z")
AGENT_ID_RE = re.compile(r"agent_[A-Za-z0-9_-]+\Z")
CHAT_ID_RE = re.compile(r"[A-Za-z0-9_-]+\Z")

ENV_FILE_KEYS = {
    "FEISHU_AILY_AUTH_MODE",
    "FEISHU_AILY_AUTH_APP_ID",
    "FEISHU_AILY_AGENT_ID",
    "FEISHU_AILY_USER_LARK_CONFIG_DIR",
    "FEISHU_AILY_USER_OPEN_ID",
    "FEISHU_AILY_USER_UNION_ID",
}


@dataclass(frozen=True)
class ProbeConfig:
    config_dir: Path
    profile: str
    expected_app_id: str
    expected_user_open_id: str
    expected_user_union_id: str
    agent_id: str
    lark_cli: str = "lark-cli"


class ProbeFailure(RuntimeError):
    def __init__(self, payload: dict[str, Any]):
        self.payload = payload
        super().__init__(str(payload.get("error", "Aily user smoke failed")))


@dataclass(frozen=True)
class _CommandResult:
    returncode: int
    payload: dict[str, Any]


def _failure(phase: str, error: str, **fields: Any) -> ProbeFailure:
    return ProbeFailure({"ok": False, "phase": phase, "error": error, **fields})


def _load_env_file(path: Path) -> dict[str, str]:
    """Read only the non-secret Aily smoke settings from a protected dotenv file."""
    if not path.is_absolute():
        raise _failure("input", "--env-file must be an absolute path")
    try:
        path_stat = path.lstat()
    except OSError as exc:
        raise _failure("input", "--env-file must name an existing file") from exc
    if stat.S_ISLNK(path_stat.st_mode) or not stat.S_ISREG(path_stat.st_mode):
        raise _failure("input", "--env-file must be a regular non-symlink file")
    getuid = getattr(os, "getuid", None)
    if getuid is None:
        raise _failure("config", "current user ownership checks are unavailable")
    if path_stat.st_uid != getuid():
        raise _failure("input", "--env-file must be owned by the current user")
    if stat.S_IMODE(path_stat.st_mode) != 0o600:
        raise _failure("input", "--env-file permissions must be 0600")
    if path_stat.st_size > MAX_ENV_FILE_BYTES:
        raise _failure("input", "--env-file exceeds the byte limit")

    flags = os.O_RDONLY
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        file_descriptor = os.open(path, flags)
    except OSError as exc:
        raise _failure("input", "--env-file could not be opened safely") from exc

    try:
        opened_stat = os.fstat(file_descriptor)
        if (
            not stat.S_ISREG(opened_stat.st_mode)
            or opened_stat.st_dev != path_stat.st_dev
            or opened_stat.st_ino != path_stat.st_ino
            or opened_stat.st_uid != getuid()
            or stat.S_IMODE(opened_stat.st_mode) != 0o600
            or opened_stat.st_size > MAX_ENV_FILE_BYTES
        ):
            raise _failure("input", "--env-file changed during validation")
        chunks: list[bytes] = []
        remaining = MAX_ENV_FILE_BYTES + 1
        while remaining > 0:
            chunk = os.read(file_descriptor, remaining)
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        raw_content = b"".join(chunks)
        if len(raw_content) > MAX_ENV_FILE_BYTES:
            raise _failure("input", "--env-file exceeds the byte limit")
        final_stat = os.fstat(file_descriptor)
        if (
            final_stat.st_dev != opened_stat.st_dev
            or final_stat.st_ino != opened_stat.st_ino
            or final_stat.st_size != opened_stat.st_size
            or final_stat.st_mtime_ns != opened_stat.st_mtime_ns
        ):
            raise _failure("input", "--env-file changed during validation")

        text_content = raw_content.decode("utf-8")
        values: dict[str, str] = {}
        for binding in parse_stream(io.StringIO(text_content)):
            if binding.error:
                raise _failure("input", "--env-file contains invalid dotenv syntax")
            if binding.key in ENV_FILE_KEYS and binding.value is not None:
                values[binding.key] = binding.value
        return values
    except UnicodeError as exc:
        raise _failure("input", "--env-file must be UTF-8 text") from exc
    finally:
        if file_descriptor >= 0:
            os.close(file_descriptor)


def _required_setting(value: Any, option: str) -> str:
    if not isinstance(value, str) or not value:
        raise _failure("input", f"{option} is required")
    return value


def _config_from_args(args: argparse.Namespace) -> ProbeConfig:
    env_values = _load_env_file(args.env_file) if args.env_file is not None else {}
    auth_mode = args.auth_mode or env_values.get("FEISHU_AILY_AUTH_MODE")
    if args.env_file is not None and auth_mode is None:
        raise _failure("input", "--auth-mode is required")
    if auth_mode not in {None, "user"}:
        raise _failure("input", "--auth-mode must be user")

    app_id = _required_setting(
        args.expected_app_id or env_values.get("FEISHU_AILY_AUTH_APP_ID"),
        "--expected-app-id",
    )
    config_dir_value = _required_setting(
        str(args.config_dir)
        if args.config_dir is not None
        else env_values.get("FEISHU_AILY_USER_LARK_CONFIG_DIR"),
        "--config-dir",
    )
    return ProbeConfig(
        config_dir=Path(config_dir_value),
        profile=args.profile or app_id,
        expected_app_id=app_id,
        expected_user_open_id=_required_setting(
            args.expected_user_open_id
            or env_values.get("FEISHU_AILY_USER_OPEN_ID"),
            "--expected-user-open-id",
        ),
        expected_user_union_id=_required_setting(
            args.expected_user_union_id
            or env_values.get("FEISHU_AILY_USER_UNION_ID"),
            "--expected-user-union-id",
        ),
        agent_id=_required_setting(
            args.agent_id or env_values.get("FEISHU_AILY_AGENT_ID"),
            "--agent-id",
        ),
        lark_cli=args.lark_cli,
    )


def _validate_config(config: ProbeConfig) -> ProbeConfig:
    config_dir = config.config_dir.expanduser()
    if not config_dir.is_absolute():
        config_dir = Path.cwd() / config_dir
    try:
        directory_stat = config_dir.lstat()
    except OSError as exc:
        raise _failure("input", "--config-dir must name an existing directory") from exc
    if stat.S_ISLNK(directory_stat.st_mode) or not stat.S_ISDIR(directory_stat.st_mode):
        raise _failure("input", "--config-dir must name an existing directory")
    getuid = getattr(os, "getuid", None)
    if getuid is None:
        raise _failure("config", "current user ownership checks are unavailable")
    if directory_stat.st_uid != getuid():
        raise _failure("input", "--config-dir must be owned by the current user")
    if stat.S_IMODE(directory_stat.st_mode) != 0o700:
        raise _failure("input", "--config-dir permissions must be 0700")
    if (
        not isinstance(config.expected_app_id, str)
        or not APP_ID_RE.fullmatch(config.expected_app_id)
        or len(config.expected_app_id) > 128
    ):
        raise _failure("input", "--expected-app-id must be a valid cli_ identifier")
    if config.profile != config.expected_app_id:
        raise _failure("input", "--profile must equal --expected-app-id")
    if (
        not isinstance(config.expected_user_open_id, str)
        or not USER_OPEN_ID_RE.fullmatch(config.expected_user_open_id)
        or len(config.expected_user_open_id) > 128
    ):
        raise _failure(
            "input", "--expected-user-open-id must be a valid ou_ identifier"
        )
    if (
        not isinstance(config.expected_user_union_id, str)
        or not USER_UNION_ID_RE.fullmatch(config.expected_user_union_id)
        or len(config.expected_user_union_id) > 128
    ):
        raise _failure(
            "input", "--expected-user-union-id must be a valid on_ identifier"
        )
    if (
        not isinstance(config.agent_id, str)
        or not AGENT_ID_RE.fullmatch(config.agent_id)
        or len(config.agent_id) > 65
    ):
        raise _failure("input", "--agent-id must be a valid agent_ identifier")
    if not isinstance(config.lark_cli, str) or not config.lark_cli.strip():
        raise _failure("input", "--lark-cli must not be empty")
    return ProbeConfig(
        config_dir=config_dir,
        profile=config.profile,
        expected_app_id=config.expected_app_id,
        expected_user_open_id=config.expected_user_open_id,
        expected_user_union_id=config.expected_user_union_id,
        agent_id=config.agent_id,
        lark_cli=config.lark_cli,
    )


def _validate_question(question: str) -> str:
    if not isinstance(question, str) or not question.strip():
        raise _failure("input", "question is required")
    question = question.strip()
    if len(question) > MAX_QUESTION_CHARS:
        raise _failure(
            "input", f"question must be at most {MAX_QUESTION_CHARS} characters"
        )
    return question


def _broker_env(config_dir: Path) -> dict[str, str]:
    """Build a minimal environment without forwarding credential variables."""
    env = {CONFIG_DIR_ENV: str(config_dir)}
    for name in (
        "PATH",
        "LANG",
        "LC_ALL",
        "LC_CTYPE",
        "HOME",
        "USER",
        "LOGNAME",
        "TMPDIR",
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


def _kill_process_group(process: subprocess.Popen[Any]) -> None:
    """Kill the task-owned process group and always reap its leader."""
    try:
        if os.name == "posix":
            os.killpg(process.pid, signal.SIGKILL)
        elif process.poll() is None:
            process.kill()
    except (OSError, ProcessLookupError):
        if process.poll() is None:
            try:
                process.kill()
            except OSError:
                pass
    try:
        process.wait(timeout=5)
    except (OSError, subprocess.TimeoutExpired):
        pass


def _bounded_subprocess_run(
    command: list[str], **kwargs: Any
) -> subprocess.CompletedProcess[bytes]:
    """Run with file-backed stdin/output and process-group cleanup."""
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
            stdin_file.write(_as_bytes(input_bytes))
            stdin_file.seek(0)
            stdin = stdin_file
        if time.monotonic() >= deadline:
            raise subprocess.TimeoutExpired(command, timeout)
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
                if output_size > MAX_COMMAND_OUTPUT_BYTES:
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
            if output_size > MAX_COMMAND_OUTPUT_BYTES:
                _kill_process_group(process)
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
        raise _failure("timeout", "Aily user smoke exceeded its total timeout")
    return remaining


def _run_json_command(
    command: list[str],
    *,
    config_dir: Path,
    timeout: float,
    input_bytes: bytes | None,
    runner: Callable[..., Any],
) -> _CommandResult:
    kwargs: dict[str, Any] = {
        "stdout": subprocess.PIPE,
        "stderr": subprocess.PIPE,
        "timeout": timeout,
        "check": False,
        "env": _broker_env(config_dir),
        "cwd": "/",
        "start_new_session": True,
    }
    if input_bytes is None:
        kwargs["stdin"] = subprocess.DEVNULL
    else:
        kwargs["input"] = input_bytes

    try:
        completed = runner(command, **kwargs)
    except subprocess.TimeoutExpired as exc:
        raise _failure("timeout", "lark-cli command exceeded the remaining timeout") from exc
    except FileNotFoundError as exc:
        raise _failure("config", "lark-cli executable was not found") from exc
    except OSError as exc:
        raise _failure("command", f"lark-cli could not start ({type(exc).__name__})") from exc

    stdout = _as_bytes(getattr(completed, "stdout", b""))
    stderr = _as_bytes(getattr(completed, "stderr", b""))
    if len(stdout) + len(stderr) > MAX_COMMAND_OUTPUT_BYTES:
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


def _api_payload(result: _CommandResult, *, phase: str) -> dict[str, Any]:
    code = result.payload.get("code")
    if (
        result.returncode != 0
        or isinstance(code, bool)
        or not isinstance(code, int)
        or code != 0
    ):
        fields: dict[str, Any] = {}
        if isinstance(code, int) and not isinstance(code, bool):
            fields["code"] = code
        raise _failure(phase, "lark-cli API request failed", **fields)
    return result.payload


def _content_text(data: dict[str, Any]) -> tuple[str, int, int]:
    content = data.get("content")
    if content is None:
        return "", 0, 0
    if not isinstance(content, list):
        raise _failure("result", "Completed result has invalid content")

    texts: list[str] = []
    previous_text: str | None = None
    artifact_count = 0
    for item in content:
        if not isinstance(item, dict):
            raise _failure("result", "Completed result has invalid content item")
        if item.get("agent_artifact_id"):
            artifact_count += 1
        text = item.get("text")
        if text is None:
            previous_text = None
            continue
        if not isinstance(text, str):
            raise _failure("result", "Completed result has invalid content text")
        if not text.strip():
            previous_text = None
            continue
        if text == previous_text:
            continue
        texts.append(text)
        previous_text = text
    return "".join(texts), len(texts), artifact_count


def _preflight_identity(
    config: ProbeConfig,
    *,
    deadline: float,
    clock: Callable[[], float],
    runner: Callable[..., Any],
) -> None:
    user_info = _api_payload(
        _run_json_command(
            [
                config.lark_cli,
                "--profile",
                config.profile,
                "api",
                "GET",
                USER_INFO_PATH,
                "--as",
                "user",
                "--format",
                "json",
            ],
            config_dir=config.config_dir,
            timeout=_remaining(deadline, clock),
            input_bytes=None,
            runner=runner,
        ),
        phase="identity",
    )
    user_data = user_info.get("data")
    if not isinstance(user_data, dict):
        raise _failure("identity", "user identity API returned no data object")
    if user_data.get("open_id") != config.expected_user_open_id:
        raise _failure("identity", "live user open_id does not match expectation")
    if user_data.get("union_id") != config.expected_user_union_id:
        raise _failure("identity", "live user union_id does not match expectation")


def run_probe(
    config: ProbeConfig,
    question: str,
    *,
    timeout: float = 120.0,
    include_answer: bool = False,
    runner: Callable[..., Any] | None = None,
    clock: Callable[[], float] | None = None,
    sleeper: Callable[[float], None] | None = None,
) -> dict[str, Any]:
    if (
        isinstance(timeout, bool)
        or not isinstance(timeout, (int, float))
        or not math.isfinite(timeout)
        or timeout <= 0
        or timeout > 300
    ):
        raise _failure("input", "timeout must be finite, >0 and <=300 seconds")
    config = _validate_config(config)
    question = _validate_question(question)
    runner = runner or _bounded_subprocess_run
    clock = clock or time.monotonic
    sleeper = sleeper or time.sleep
    deadline = clock() + float(timeout)

    _preflight_identity(config, deadline=deadline, clock=clock, runner=runner)
    request_body = json.dumps(
        {
            "user_message": {"content": [{"type": "text", "text": question}]},
            "stream": False,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    create_path = CHAT_PATH.format(agent_id=config.agent_id)
    created = _api_payload(
        _run_json_command(
            [
                config.lark_cli,
                "--profile",
                config.profile,
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
            config_dir=config.config_dir,
            timeout=_remaining(deadline, clock),
            input_bytes=request_body,
            runner=runner,
        ),
        phase="create",
    )
    create_data = created.get("data")
    chat_id = create_data.get("agent_chat_id") if isinstance(create_data, dict) else None
    if (
        not isinstance(chat_id, str)
        or not CHAT_ID_RE.fullmatch(chat_id)
        or len(chat_id) > 128
    ):
        raise _failure("create", "Aily create response has no valid agent_chat_id")

    poll_path = CHAT_RESULT_PATH.format(
        agent_id=config.agent_id,
        agent_chat_id=chat_id,
    )
    poll_count = 0
    interval = DEFAULT_POLL_INTERVAL_SECONDS
    completed_data: dict[str, Any] | None = None
    sleeper(min(interval, _remaining(deadline, clock)))
    interval = min(MAX_POLL_INTERVAL_SECONDS, interval * 2)
    while poll_count < MAX_POLL_REQUESTS:
        polled = _api_payload(
            _run_json_command(
                [
                    config.lark_cli,
                    "--profile",
                    config.profile,
                    "api",
                    "GET",
                    poll_path,
                    "--as",
                    "user",
                    "--format",
                    "json",
                ],
                config_dir=config.config_dir,
                timeout=_remaining(deadline, clock),
                input_bytes=None,
                runner=runner,
            ),
            phase="poll",
        )
        poll_count += 1
        data = polled.get("data")
        if not isinstance(data, dict):
            raise _failure("poll", "Aily poll response has no data object")
        status = data.get("status")
        if status == "Completed":
            completed_data = data
            break
        if status not in NON_TERMINAL_STATUSES:
            fields = {"status": status} if isinstance(status, str) and len(status) <= 64 else {}
            raise _failure("result", "Aily result did not reach Completed", **fields)
        remaining = _remaining(deadline, clock)
        sleeper(min(interval, remaining))
        interval = min(MAX_POLL_INTERVAL_SECONDS, interval * 2)

    if completed_data is None:
        raise _failure("timeout", "Aily result exceeded the poll request limit")

    answer, text_item_count, artifact_count = _content_text(completed_data)
    result: dict[str, Any] = {
        "ok": True,
        "phase": "completed",
        "status": "Completed",
        "app_id": config.expected_app_id,
        "user_identity_verified": True,
        "agent_id": config.agent_id,
        "poll_count": poll_count,
        "answer_available": bool(answer),
        "answer_length": len(answer),
        "text_item_count": text_item_count,
        "artifact_count": artifact_count,
    }
    if include_answer:
        result["answer"] = answer[:MAX_DISPLAY_ANSWER_CHARS]
        result["answer_truncated"] = len(answer) > MAX_DISPLAY_ANSWER_CHARS
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--auth-mode")
    parser.add_argument("--config-dir", type=Path)
    parser.add_argument("--profile")
    parser.add_argument("--expected-app-id")
    parser.add_argument("--expected-user-open-id")
    parser.add_argument("--expected-user-union-id")
    parser.add_argument("--agent-id")
    parser.add_argument("--lark-cli", default="lark-cli")
    parser.add_argument("--question-stdin", action="store_true", required=True)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument(
        "--show-answer",
        action="store_true",
        help=f"include at most {MAX_DISPLAY_ANSWER_CHARS} answer characters",
    )
    parser.add_argument("--pretty", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        question = sys.stdin.read(MAX_QUESTION_CHARS + 1)
        if len(question) > MAX_QUESTION_CHARS:
            raise _failure(
                "input", f"question must be at most {MAX_QUESTION_CHARS} characters"
            )
        config = _config_from_args(args)
        result = run_probe(
            config,
            question,
            timeout=args.timeout,
            include_answer=args.show_answer,
        )
    except ProbeFailure as exc:
        result = exc.payload
    print(
        json.dumps(
            result,
            ensure_ascii=False,
            indent=2 if args.pretty else None,
            sort_keys=True,
        )
    )
    if result.get("ok"):
        return 0
    return 2 if result.get("phase") in {"config", "input"} else 3


if __name__ == "__main__":
    raise SystemExit(main())
