"""Offline tests for the identity-pinned Aily Agent user transport."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from tools import feishu_aily_agent_user_transport as transport


def _config(tmp_path: Path, **overrides) -> transport.AilyAgentUserConfig:
    values = {
        "config_dir": tmp_path,
        "profile": "cli_expected",
        "expected_app_id": "cli_expected",
        "expected_user_open_id": "ou_expected",
        "expected_union_id": "on_expected",
        "agent_id": "agent_expected",
    }
    values.update(overrides)
    return transport.AilyAgentUserConfig(**values)


def _completed(payload: dict, *, returncode: int = 0, stderr: bytes = b""):
    return SimpleNamespace(
        returncode=returncode,
        stdout=json.dumps(payload, ensure_ascii=False).encode(),
        stderr=stderr,
    )


def _user_info(**overrides):
    data = {"open_id": "ou_expected", "union_id": "on_expected", "name": "Hu"}
    data.update(overrides)
    return {"code": 0, "data": data}


def _create():
    return {"code": 0, "data": {"agent_chat_id": "chat_1", "session_id": "s_1"}}


def _poll(status: str, content=None):
    data = {"status": status}
    if content is not None:
        data["content"] = content
    return {"code": 0, "data": data}


def _run(config, responses, *, content="OOI是什么?", **kwargs):
    calls = []
    response_iter = iter(responses)

    def runner(command, **run_kwargs):
        calls.append((command, run_kwargs))
        return next(response_iter)

    result = transport.run_agent_chat_user(
        config,
        content=content,
        runner=runner,
        sleeper=kwargs.pop("sleeper", lambda _seconds: None),
        executable_resolver=lambda: "lark-cli",
        **kwargs,
    )
    return result, calls


def test_happy_path_pins_user_identity_profile_and_stdin(tmp_path, monkeypatch):
    monkeypatch.setenv("FEISHU_AILY_AUTH_APP_SECRET", "must-not-forward")
    monkeypatch.setenv("OPENCLAW_CONFIG_PATH", "/must/not/select/workspace")
    monkeypatch.setenv("HOME", "/Users/tester")
    monkeypatch.setenv("USER", "tester")
    monkeypatch.setenv("LOGNAME", "tester")
    monkeypatch.setenv("TMPDIR", "/private/tmp/tester/")
    sleeps = []
    result, calls = _run(
        _config(tmp_path),
        [
            _completed(_user_info()),
            _completed(_create()),
            _completed(_poll("Queued")),
            _completed(
                _poll("Completed", [{"type": "text", "text": "internal answer"}])
            ),
        ],
        session_id="input_session",
        agent_attachment_ids=["attachment_1"],
        sleeper=sleeps.append,
    )

    assert result == {
        "code": 0,
        "data": {
            "status": "Completed",
            "content": [{"type": "text", "text": "internal answer"}],
            "agent_chat_id": "chat_1",
            "session_id": "s_1",
        },
    }
    assert calls[0][0] == [
        "lark-cli",
        "--profile",
        "cli_expected",
        "api",
        "GET",
        "/open-apis/authen/v1/user_info",
        "--as",
        "user",
        "--format",
        "json",
    ]
    create_command, create_kwargs = calls[1]
    assert create_command == [
        "lark-cli",
        "--profile",
        "cli_expected",
        "api",
        "POST",
        "/open-apis/aily/v1/agents/agent_expected/chats",
        "--as",
        "user",
        "--data",
        "-",
        "--format",
        "json",
    ]
    assert "OOI是什么?" not in create_command
    assert json.loads(create_kwargs["input"]) == {
        "user_message": {
            "content": [{"type": "text", "text": "OOI是什么?"}],
            "agent_attachment_ids": ["attachment_1"],
        },
        "stream": False,
        "session_id": "input_session",
    }
    for command, run_kwargs in calls:
        assert command[1:3] == ["--profile", "cli_expected"]
        assert run_kwargs["env"] == {
            key: value
            for key, value in run_kwargs["env"].items()
            if key
            in {
                transport.LARK_CONFIG_DIR_ENV,
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
            }
        }
        assert run_kwargs["env"][transport.LARK_CONFIG_DIR_ENV] == str(tmp_path)
        assert "FEISHU_AILY_AUTH_APP_SECRET" not in run_kwargs["env"]
        assert "OPENCLAW_CONFIG_PATH" not in run_kwargs["env"]
        assert run_kwargs["env"]["HOME"] == "/Users/tester"
        assert run_kwargs["env"]["USER"] == "tester"
        assert run_kwargs["env"]["LOGNAME"] == "tester"
        assert run_kwargs["env"]["TMPDIR"] == "/private/tmp/tester/"
        assert run_kwargs["cwd"] == "/"
        assert run_kwargs["start_new_session"] is True
    for command, _run_kwargs in calls:
        assert command[command.index("--as") + 1] == "user"
        assert "auth" not in command
    assert sleeps == [1.0, 2.0]


def test_user_info_api_failure_stops_before_question(tmp_path):
    calls = []

    def runner(command, **_kwargs):
        calls.append(command)
        return _completed({"code": 999, "msg": "private auth detail"}, returncode=1)

    with pytest.raises(transport.AilyAgentUserTransportError) as exc:
        transport.run_agent_chat_user(
            _config(tmp_path),
            content="private question",
            runner=runner,
            executable_resolver=lambda: "lark-cli",
        )

    assert exc.value.phase == "identity"
    assert len(calls) == 1
    assert all("private question" not in command for command in calls)
    assert "private auth detail" not in str(exc.value)


@pytest.mark.parametrize(
    "user_info",
    [
        _user_info(open_id="ou_other"),
        _user_info(union_id="on_other"),
    ],
)
def test_user_info_mismatch_stops_before_question(tmp_path, user_info):
    responses = iter([_completed(user_info)])
    calls = []

    def runner(command, **_kwargs):
        calls.append(command)
        return next(responses)

    with pytest.raises(transport.AilyAgentUserTransportError) as exc:
        transport.run_agent_chat_user(
            _config(tmp_path),
            content="private question",
            runner=runner,
            executable_resolver=lambda: "lark-cli",
        )

    assert exc.value.phase == "identity"
    assert len(calls) == 1
    assert all("private question" not in command for command in calls)


def test_failed_terminal_payload_is_returned_for_shared_agent_parser(tmp_path):
    result, _calls = _run(
        _config(tmp_path),
        [
            _completed(_user_info()),
            _completed(_create()),
            _completed(_poll("Failed")),
        ],
    )

    assert result["data"]["status"] == "Failed"
    assert result["data"]["agent_chat_id"] == "chat_1"


def test_oversized_output_is_rejected_without_echo(tmp_path):
    oversized = b"private" + b"x" * transport._MAX_COMMAND_OUTPUT_BYTES

    with pytest.raises(transport.AilyAgentUserTransportError) as exc:
        transport.run_agent_chat_user(
            _config(tmp_path),
            content="question",
            runner=lambda _command, **_kwargs: SimpleNamespace(
                returncode=0, stdout=oversized, stderr=b""
            ),
            executable_resolver=lambda: "lark-cli",
        )

    assert exc.value.phase == "output"
    assert "private" not in str(exc.value)


def test_subprocess_timeout_does_not_echo_captured_output(tmp_path):
    def runner(command, **kwargs):
        raise subprocess.TimeoutExpired(
            command,
            kwargs["timeout"],
            output=b"private answer",
            stderr=b"private error",
        )

    with pytest.raises(transport.AilyAgentUserTransportError) as exc:
        transport.run_agent_chat_user(
            _config(tmp_path),
            content="question",
            runner=runner,
            executable_resolver=lambda: "lark-cli",
        )

    assert exc.value.phase == "timeout"
    assert "private" not in str(exc.value)


@pytest.mark.skipif(os.name != "posix", reason="process-group assertion is POSIX-specific")
def test_bounded_runner_reaps_descendant_on_timeout(tmp_path):
    marker = tmp_path / "descendant-survived"
    child = (
        "import pathlib,time;"
        "time.sleep(0.4);"
        f"pathlib.Path({str(marker)!r}).write_text('alive')"
    )
    parent = (
        "import subprocess,sys,time;"
        f"subprocess.Popen([sys.executable,'-c',{child!r}]);"
        "time.sleep(10)"
    )

    harness = f"""
import subprocess, sys, time
from tools import feishu_aily_agent_user_transport as transport
try:
    transport._bounded_subprocess_run(
        [sys.executable, '-c', {parent!r}],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=0.1,
        check=False,
        env={{'PATH': {os.environ.get('PATH', '')!r}}},
        cwd='/',
        start_new_session=True,
    )
except subprocess.TimeoutExpired:
    pass
else:
    raise SystemExit(2)
time.sleep(0.5)
raise SystemExit(1 if __import__('pathlib').Path({str(marker)!r}).exists() else 0)
"""
    completed = subprocess.run(
        [sys.executable, "-c", harness],
        cwd=Path(__file__).parents[2],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=5,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr.decode(errors="replace")


@pytest.mark.skipif(os.name != "posix", reason="process-group assertion is POSIX-specific")
def test_bounded_runner_timeout_covers_child_that_does_not_read_stdin():
    harness = """
import subprocess, sys, time
from tools import feishu_aily_agent_user_transport as transport
started = time.monotonic()
try:
    transport._bounded_subprocess_run(
        [sys.executable, '-c', 'import time; time.sleep(10)'],
        input=b'x' * (200 * 1024),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=0.1,
        check=False,
        env={'PATH': __import__('os').environ.get('PATH', '')},
        cwd='/',
    )
except subprocess.TimeoutExpired:
    raise SystemExit(0 if time.monotonic() - started < 1.0 else 3)
else:
    raise SystemExit(2)
"""
    completed = subprocess.run(
        [sys.executable, "-c", harness],
        cwd=Path(__file__).parents[2],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=5,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr.decode(errors="replace")


def test_bounded_runner_kills_command_when_output_limit_is_exceeded():
    # The repository's pytest guard intentionally blocks killpg outside the
    # pytest process group. Exercise the real process-group cleanup in an
    # isolated interpreter where that global guard is not installed.
    harness = """
import subprocess, sys
from tools import feishu_aily_agent_user_transport as transport
transport._MAX_COMMAND_OUTPUT_BYTES = 1024
try:
    transport._bounded_subprocess_run(
        [sys.executable, '-c', "import os; os.write(1, b'x' * 4096)"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=2,
        check=False,
        env={'PATH': __import__('os').environ.get('PATH', '')},
        cwd='/',
        start_new_session=True,
    )
except transport.AilyAgentUserTransportError as exc:
    raise SystemExit(0 if exc.phase == 'output' else 3)
else:
    raise SystemExit(2)
"""
    completed = subprocess.run(
        [sys.executable, "-c", harness],
        cwd=Path(__file__).parents[2],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=5,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr.decode(errors="replace")


def test_profile_must_equal_expected_app_before_command(tmp_path):
    called = False

    def runner(*_args, **_kwargs):
        nonlocal called
        called = True

    with pytest.raises(transport.AilyAgentUserTransportError) as exc:
        transport.run_agent_chat_user(
            _config(tmp_path, profile="other_profile"),
            content="question",
            runner=runner,
            executable_resolver=lambda: "lark-cli",
        )

    assert exc.value.phase == "config"
    assert called is False


def test_config_directory_permissions_are_exact(tmp_path):
    tmp_path.chmod(0o750)

    with pytest.raises(transport.AilyAgentUserTransportError) as exc:
        transport.run_agent_chat_user(
            _config(tmp_path),
            content="question",
            runner=lambda *_args, **_kwargs: None,
            executable_resolver=lambda: "lark-cli",
        )

    assert "0700" in str(exc.value)


def test_lock_wait_is_part_of_total_deadline(tmp_path, monkeypatch):
    class BusyLock:
        def acquire(self, *, timeout):
            assert 0 < timeout <= 1.0
            return False

        def release(self):
            raise AssertionError("unacquired lock must not be released")

    monkeypatch.setattr(transport, "_TRANSPORT_LOCK", BusyLock())
    with pytest.raises(transport.AilyAgentUserTransportError) as exc:
        transport.run_agent_chat_user(
            _config(tmp_path),
            content="question",
            timeout=1.0,
            runner=lambda *_args, **_kwargs: None,
            executable_resolver=lambda: "lark-cli",
        )

    assert exc.value.phase == "timeout"
