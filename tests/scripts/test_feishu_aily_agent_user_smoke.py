"""Offline tests for the lark-cli user-identity Aily Agent smoke."""

from __future__ import annotations

import importlib.util
import io
import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

SCRIPT = Path(__file__).parents[2] / "scripts" / "feishu_aily_agent_user_smoke.py"
DOC = Path(__file__).parents[2] / "docs" / "feishu-aily-agent.md"
SPEC = importlib.util.spec_from_file_location("feishu_aily_agent_user_smoke", SCRIPT)
assert SPEC and SPEC.loader
smoke = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = smoke
SPEC.loader.exec_module(smoke)


def test_documented_smoke_keeps_question_out_of_shell_arguments():
    documentation = DOC.read_text(encoding="utf-8")

    assert "--question-stdin" in documentation
    assert "--env-file /Users/songying/.hermes/.env" in documentation
    assert "printf '%s'" not in documentation
    assert "source /Users/songying/.hermes/.env" not in documentation


def _env_file(tmp_path: Path, content: str) -> Path:
    path = tmp_path / ".env"
    path.write_text(content, encoding="utf-8")
    path.chmod(0o600)
    return path


def _config(tmp_path: Path, **overrides):
    values = {
        "config_dir": tmp_path,
        "profile": "cli_expected",
        "expected_app_id": "cli_expected",
        "expected_user_open_id": "ou_expected",
        "expected_user_union_id": "on_expected",
        "agent_id": "agent_expected",
    }
    values.update(overrides)
    return smoke.ProbeConfig(**values)


def _completed(payload: dict, returncode: int = 0):
    return SimpleNamespace(
        returncode=returncode,
        stdout=json.dumps(payload, ensure_ascii=False).encode(),
        stderr=b"",
    )


def _user_info(**overrides):
    data = {
        "name": "Display Name",
        "open_id": "ou_expected",
        "union_id": "on_expected",
    }
    data.update(overrides)
    return {"code": 0, "data": data}


def _create():
    return {"code": 0, "data": {"agent_chat_id": "chat_1", "session_id": "s_1"}}


def _poll(status: str, content=None):
    data = {"status": status}
    if content is not None:
        data["content"] = content
    return {"code": 0, "data": data}


def test_happy_path_uses_user_info_then_stdin_create_and_poll(tmp_path, monkeypatch):
    responses = iter(
        [
            _completed(_user_info()),
            _completed(_create()),
            _completed(_poll("Queued")),
            _completed(
                _poll(
                    "Completed",
                    [
                        {"type": "text", "text": "A"},
                        {"type": "text", "text": "A"},
                        {"type": "text", "text": "   \n"},
                        {"type": "text", "text": "B"},
                        {"type": "text", "text": "A"},
                        {"type": "artifact", "agent_artifact_id": "artifact_1"},
                    ],
                )
            ),
        ]
    )
    calls = []

    def fake_run(command, **kwargs):
        calls.append((command, kwargs))
        return next(responses)

    sleeps = []
    monkeypatch.setattr(smoke, "_bounded_subprocess_run", fake_run)
    monkeypatch.setattr(smoke.time, "sleep", sleeps.append)
    monkeypatch.setenv("FEISHU_AILY_USER_ACCESS_TOKEN", "must-not-forward")
    monkeypatch.setenv("FEISHU_AILY_AUTH_APP_SECRET", "must-not-forward-either")
    monkeypatch.setenv("HERMES_HOME", "/must-not-forward-hermes")
    monkeypatch.setenv("OPENCLAW_HOME", "/must-not-forward-openclaw")
    monkeypatch.setenv("HOME", "/safe-home")
    monkeypatch.setenv("USER", "safe-user")
    monkeypatch.setenv("LOGNAME", "safe-logname")
    monkeypatch.setenv("TMPDIR", "/safe-tmp")

    result = smoke.run_probe(_config(tmp_path), "OOI是什么?")

    assert result == {
        "ok": True,
        "phase": "completed",
        "status": "Completed",
        "app_id": "cli_expected",
        "user_identity_verified": True,
        "agent_id": "agent_expected",
        "poll_count": 2,
        "answer_available": True,
        "answer_length": len("ABA"),
        "text_item_count": 3,
        "artifact_count": 1,
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
    assert json.loads(create_kwargs["input"])["user_message"]["content"] == [
        {"type": "text", "text": "OOI是什么?"}
    ]

    allowed_env = {
        smoke.CONFIG_DIR_ENV,
        "PATH",
        "LANG",
        "LC_ALL",
        "LC_CTYPE",
        "HOME",
        "USER",
        "LOGNAME",
        "TMPDIR",
    }
    for command, kwargs in calls:
        assert kwargs["env"][smoke.CONFIG_DIR_ENV] == str(tmp_path)
        assert set(kwargs["env"]) <= allowed_env
        assert "FEISHU_AILY_USER_ACCESS_TOKEN" not in kwargs["env"]
        assert "FEISHU_AILY_AUTH_APP_SECRET" not in kwargs["env"]
        assert "HERMES_HOME" not in kwargs["env"]
        assert "OPENCLAW_HOME" not in kwargs["env"]
        assert kwargs["env"]["HOME"] == "/safe-home"
        assert kwargs["env"]["USER"] == "safe-user"
        assert kwargs["env"]["LOGNAME"] == "safe-logname"
        assert kwargs["env"]["TMPDIR"] == "/safe-tmp"
        assert kwargs["cwd"] == "/"
        assert kwargs["start_new_session"] is True
        assert command[1:3] == ["--profile", "cli_expected"]
        if command[3:5] == ["api", "GET"]:
            assert command[6:8] == ["--as", "user"]
            assert kwargs["stdin"] is subprocess.DEVNULL
    assert sleeps == [1.0, 2.0]


def test_show_answer_is_explicit_and_bounded(tmp_path):
    long_text = "x" * (smoke.MAX_DISPLAY_ANSWER_CHARS + 25)
    responses = iter(
        [
            _completed(_user_info()),
            _completed(_create()),
            _completed(_poll("Completed", [{"type": "text", "text": long_text}])),
        ]
    )

    result = smoke.run_probe(
        _config(tmp_path),
        "question",
        include_answer=True,
        runner=lambda _command, **_kwargs: next(responses),
    )

    assert result["answer_length"] == len(long_text)
    assert result["answer"] == long_text[: smoke.MAX_DISPLAY_ANSWER_CHARS]
    assert result["answer_truncated"] is True


def test_profile_must_equal_expected_app_id_before_runner(tmp_path):
    called = False

    def runner(*_args, **_kwargs):
        nonlocal called
        called = True

    with pytest.raises(smoke.ProbeFailure) as exc:
        smoke.run_probe(
            _config(tmp_path, profile="cli_other"), "question", runner=runner
        )

    assert exc.value.payload["error"] == "--profile must equal --expected-app-id"
    assert called is False


@pytest.mark.parametrize(
    "user_info_override",
    [
        {"open_id": "ou_other"},
        {"union_id": "on_other"},
    ],
)
def test_user_info_hard_pins_open_and_union_ids_before_create(
    tmp_path, user_info_override
):
    calls = []

    def runner(command, **_kwargs):
        calls.append(command)
        return _completed(_user_info(**user_info_override))

    with pytest.raises(smoke.ProbeFailure) as exc:
        smoke.run_probe(_config(tmp_path), "question", runner=runner)

    assert exc.value.payload["phase"] == "identity"
    assert len(calls) == 1
    assert not any(command[3:5] == ["api", "POST"] for command in calls)


def test_invalid_user_info_json_fails_before_create_without_echo(tmp_path):
    calls = []

    def runner(command, **_kwargs):
        calls.append(command)
        return SimpleNamespace(
            returncode=0, stdout=b"private invalid JSON", stderr=b""
        )

    with pytest.raises(smoke.ProbeFailure) as exc:
        smoke.run_probe(_config(tmp_path), "question", runner=runner)

    assert exc.value.payload["phase"] == "command"
    assert "private" not in json.dumps(exc.value.payload)
    assert len(calls) == 1


def test_missing_user_token_fails_before_create(tmp_path):
    calls = []

    def runner(command, **_kwargs):
        calls.append(command)
        return _completed({"ok": False, "error": {"type": "auth"}}, returncode=3)

    with pytest.raises(smoke.ProbeFailure) as exc:
        smoke.run_probe(_config(tmp_path), "question", runner=runner)

    assert exc.value.payload["phase"] == "identity"
    assert len(calls) == 1


@pytest.mark.parametrize("terminal_status", ["Failed", "Cancelled", "Finished", "completed"])
def test_only_exact_completed_status_succeeds(tmp_path, terminal_status):
    responses = iter(
        [
            _completed(_user_info()),
            _completed(_create()),
            _completed(_poll(terminal_status, [{"type": "text", "text": "partial"}])),
        ]
    )

    with pytest.raises(smoke.ProbeFailure) as exc:
        smoke.run_probe(
            _config(tmp_path),
            "question",
            runner=lambda _command, **_kwargs: next(responses),
        )

    assert exc.value.payload["phase"] == "result"
    assert exc.value.payload["status"] == terminal_status


def test_total_timeout_covers_poll_sleep(tmp_path):
    now = [0.0]
    responses = iter(
        [
            _completed(_user_info()),
            _completed(_create()),
            _completed(_poll("Queued")),
        ]
    )

    def sleep(seconds):
        now[0] += seconds

    with pytest.raises(smoke.ProbeFailure) as exc:
        smoke.run_probe(
            _config(tmp_path),
            "question",
            timeout=0.5,
            runner=lambda _command, **_kwargs: next(responses),
            clock=lambda: now[0],
            sleeper=sleep,
        )

    assert exc.value.payload["phase"] == "timeout"


def test_injected_runner_timeout_does_not_echo_captured_output(tmp_path):
    def runner(command, **kwargs):
        raise subprocess.TimeoutExpired(
            command, kwargs["timeout"], output=b"private-answer", stderr=b"private-error"
        )

    with pytest.raises(smoke.ProbeFailure) as exc:
        smoke.run_probe(_config(tmp_path), "question", runner=runner)

    serialized = json.dumps(exc.value.payload)
    assert exc.value.payload["phase"] == "timeout"
    assert "private" not in serialized


def test_injected_runner_oversized_output_is_rejected_without_echo(tmp_path):
    oversized = b"sensitive" + b"x" * smoke.MAX_COMMAND_OUTPUT_BYTES

    with pytest.raises(smoke.ProbeFailure) as exc:
        smoke.run_probe(
            _config(tmp_path),
            "question",
            runner=lambda _command, **_kwargs: SimpleNamespace(
                returncode=0, stdout=oversized, stderr=b""
            ),
        )

    assert exc.value.payload == {
        "ok": False,
        "phase": "output",
        "error": "lark-cli output exceeded the byte limit",
    }
    assert "sensitive" not in json.dumps(exc.value.payload)


def test_invalid_input_fails_before_starting_lark_cli(tmp_path):
    called = False

    def runner(*_args, **_kwargs):
        nonlocal called
        called = True

    with pytest.raises(smoke.ProbeFailure) as exc:
        smoke.run_probe(_config(tmp_path), " ", runner=runner)

    assert exc.value.payload["phase"] == "input"
    assert called is False


def test_config_dir_rejects_group_or_world_permissions(tmp_path):
    tmp_path.chmod(0o750)

    with pytest.raises(smoke.ProbeFailure) as exc:
        smoke.run_probe(_config(tmp_path), "question", runner=lambda *_a, **_k: None)

    assert exc.value.payload["error"] == "--config-dir permissions must be 0700"


def test_config_dir_rejects_symlink(tmp_path):
    target = tmp_path / "target"
    target.mkdir(mode=0o700)
    link = tmp_path / "link"
    link.symlink_to(target, target_is_directory=True)

    with pytest.raises(smoke.ProbeFailure) as exc:
        smoke.run_probe(
            _config(link),
            "question",
            runner=lambda *_args, **_kwargs: pytest.fail("runner must not start"),
        )

    assert exc.value.payload["error"] == "--config-dir must name an existing directory"


def test_config_dir_rejects_different_owner(tmp_path, monkeypatch):
    monkeypatch.setattr(smoke.os, "getuid", lambda: tmp_path.stat().st_uid + 1)

    with pytest.raises(smoke.ProbeFailure) as exc:
        smoke.run_probe(_config(tmp_path), "question", runner=lambda *_a, **_k: None)

    assert exc.value.payload["error"] == "--config-dir must be owned by the current user"


def test_main_reads_question_only_from_stdin_and_hides_identity_ids(
    tmp_path, monkeypatch, capsys
):
    responses = iter(
        [
            _completed(_user_info()),
            _completed(_create()),
            _completed(_poll("Completed", [{"type": "text", "text": "internal"}])),
        ]
    )
    monkeypatch.setattr(
        smoke, "_bounded_subprocess_run", lambda _command, **_kwargs: next(responses)
    )
    monkeypatch.setattr(smoke.sys, "stdin", io.StringIO("OOI是什么?"))

    exit_code = smoke.main(
        [
            "--config-dir",
            str(tmp_path),
            "--profile",
            "cli_expected",
            "--expected-app-id",
            "cli_expected",
            "--expected-user-open-id",
            "ou_expected",
            "--expected-user-union-id",
            "on_expected",
            "--agent-id",
            "agent_expected",
            "--question-stdin",
        ]
    )

    output = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert output["answer_available"] is True
    assert output["answer_length"] == len("internal")
    assert "answer" not in output
    serialized = json.dumps(output)
    assert "ou_expected" not in serialized
    assert "on_expected" not in serialized
    assert "user_name" not in output
    assert output["user_identity_verified"] is True


def test_env_file_is_parsed_as_data_and_only_returns_allowlisted_values(tmp_path):
    marker = tmp_path / "must-not-exist"
    env_file = _env_file(
        tmp_path,
        "\n".join(
            [
                "FEISHU_AILY_AUTH_MODE=user",
                "FEISHU_AILY_AUTH_APP_ID=cli_expected",
                "FEISHU_AILY_AGENT_ID=agent_expected",
                f"FEISHU_AILY_USER_LARK_CONFIG_DIR={tmp_path}",
                "FEISHU_AILY_USER_OPEN_ID=ou_expected",
                "FEISHU_AILY_USER_UNION_ID=on_expected",
                "FEISHU_AILY_AUTH_APP_SECRET=must-not-be-returned",
                "FEISHU_AILY_USER_ACCESS_TOKEN=must-not-be-returned",
                f"EVIL=$(touch {marker})",
            ]
        ),
    )

    values = smoke._load_env_file(env_file)

    assert set(values) == smoke.ENV_FILE_KEYS
    assert "must-not-be-returned" not in repr(values)
    assert not marker.exists()


def test_main_loads_env_file_and_defaults_profile_to_app_id(
    tmp_path, monkeypatch, capsys
):
    env_file = _env_file(
        tmp_path,
        "\n".join(
            [
                "FEISHU_AILY_AUTH_MODE=user",
                "FEISHU_AILY_AUTH_APP_ID=cli_expected",
                "FEISHU_AILY_AGENT_ID=agent_expected",
                f"FEISHU_AILY_USER_LARK_CONFIG_DIR={tmp_path}",
                "FEISHU_AILY_USER_OPEN_ID=ou_expected",
                "FEISHU_AILY_USER_UNION_ID=on_expected",
            ]
        ),
    )
    captured = {}

    def fake_probe(config, question, **_kwargs):
        captured["config"] = config
        captured["question"] = question
        return {"ok": True, "phase": "completed"}

    monkeypatch.setattr(smoke, "run_probe", fake_probe)
    monkeypatch.setattr(smoke.sys, "stdin", io.StringIO("OOI是什么?"))

    exit_code = smoke.main(
        ["--env-file", str(env_file), "--question-stdin"]
    )

    assert exit_code == 0
    assert json.loads(capsys.readouterr().out)["ok"] is True
    assert captured["question"] == "OOI是什么?"
    assert captured["config"] == _config(tmp_path)


def test_explicit_flags_override_env_file_values(tmp_path):
    env_file = _env_file(
        tmp_path,
        "\n".join(
            [
                "FEISHU_AILY_AUTH_MODE=user",
                "FEISHU_AILY_AUTH_APP_ID=cli_from_env",
                "FEISHU_AILY_AGENT_ID=agent_from_env",
                f"FEISHU_AILY_USER_LARK_CONFIG_DIR={tmp_path}",
                "FEISHU_AILY_USER_OPEN_ID=ou_from_env",
                "FEISHU_AILY_USER_UNION_ID=on_from_env",
            ]
        ),
    )
    args = smoke._parser().parse_args(
        [
            "--env-file",
            str(env_file),
            "--expected-app-id",
            "cli_explicit",
            "--agent-id",
            "agent_explicit",
            "--expected-user-open-id",
            "ou_explicit",
            "--expected-user-union-id",
            "on_explicit",
            "--question-stdin",
        ]
    )

    config = smoke._config_from_args(args)

    assert config.expected_app_id == "cli_explicit"
    assert config.profile == "cli_explicit"
    assert config.agent_id == "agent_explicit"
    assert config.expected_user_open_id == "ou_explicit"
    assert config.expected_user_union_id == "on_explicit"


@pytest.mark.parametrize(
    ("kind", "expected_error"),
    [
        ("missing", "--env-file must name an existing file"),
        ("permissions", "--env-file permissions must be 0600"),
        ("symlink", "--env-file must be a regular non-symlink file"),
    ],
)
def test_env_file_path_safety_checks(tmp_path, kind, expected_error):
    env_file = tmp_path / ".env"
    if kind == "permissions":
        env_file.write_text("FEISHU_AILY_AUTH_MODE=user\n", encoding="utf-8")
        env_file.chmod(0o640)
    elif kind == "symlink":
        target = _env_file(tmp_path, "FEISHU_AILY_AUTH_MODE=user\n")
        env_file = tmp_path / "linked.env"
        env_file.symlink_to(target)

    with pytest.raises(smoke.ProbeFailure) as exc:
        smoke._load_env_file(env_file)

    assert exc.value.payload["error"] == expected_error


def test_env_file_rejects_different_owner(tmp_path, monkeypatch):
    env_file = _env_file(tmp_path, "FEISHU_AILY_AUTH_MODE=user\n")
    monkeypatch.setattr(smoke.os, "getuid", lambda: env_file.stat().st_uid + 1)

    with pytest.raises(smoke.ProbeFailure) as exc:
        smoke._load_env_file(env_file)

    assert exc.value.payload["error"] == "--env-file must be owned by the current user"


def test_env_file_growth_after_open_is_bounded_and_rejected(tmp_path, monkeypatch):
    env_file = _env_file(tmp_path, "FEISHU_AILY_AUTH_MODE=user\n")
    real_read = smoke.os.read
    grew = False

    def growing_read(file_descriptor, size):
        nonlocal grew
        if not grew:
            grew = True
            with env_file.open("ab") as stream:
                stream.write(b"x" * (smoke.MAX_ENV_FILE_BYTES + 1))
        return real_read(file_descriptor, size)

    monkeypatch.setattr(smoke.os, "read", growing_read)

    with pytest.raises(smoke.ProbeFailure) as exc:
        smoke._load_env_file(env_file)

    assert exc.value.payload["error"] == "--env-file exceeds the byte limit"


def test_env_file_missing_required_setting_fails_before_probe(
    tmp_path, monkeypatch, capsys
):
    env_file = _env_file(tmp_path, "FEISHU_AILY_AUTH_MODE=user\n")
    called = False

    def fake_probe(*_args, **_kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(smoke, "run_probe", fake_probe)
    monkeypatch.setattr(smoke.sys, "stdin", io.StringIO("question"))

    exit_code = smoke.main(
        ["--env-file", str(env_file), "--question-stdin"]
    )

    output = json.loads(capsys.readouterr().out)
    assert exit_code == 2
    assert output["phase"] == "input"
    assert output["error"] == "--expected-app-id is required"
    assert called is False


def test_main_bounds_stdin_before_config_or_network(monkeypatch, capsys):
    class TrackingStdin:
        def __init__(self):
            self.requested_size = None

        def read(self, size=-1):
            self.requested_size = size
            return "x" * size

    stdin = TrackingStdin()
    called = False

    def fake_probe(*_args, **_kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(smoke.sys, "stdin", stdin)
    monkeypatch.setattr(smoke, "run_probe", fake_probe)

    exit_code = smoke.main(["--question-stdin"])

    output = json.loads(capsys.readouterr().out)
    assert exit_code == 2
    assert stdin.requested_size == smoke.MAX_QUESTION_CHARS + 1
    assert output["phase"] == "input"
    assert called is False


def test_cli_rejects_question_argument(tmp_path):
    with pytest.raises(SystemExit) as exc:
        smoke.main(
            [
                "--config-dir",
                str(tmp_path),
                "--profile",
                "cli_expected",
                "--expected-app-id",
                "cli_expected",
                "--expected-user-open-id",
                "ou_expected",
                "--expected-user-union-id",
                "on_expected",
                "--agent-id",
                "agent_expected",
                "--question",
                "must not enter argv",
            ]
        )

    assert exc.value.code == 2


@pytest.mark.skipif(os.name != "posix", reason="requires POSIX process groups")
def test_bounded_runner_timeout_kills_child_group_and_reaps_leader(tmp_path):
    marker = tmp_path / "child-survived"
    child_code = (
        "import pathlib,time;"
        "time.sleep(0.4);"
        f"pathlib.Path({str(marker)!r}).write_text('alive')"
    )
    parent_code = (
        "import subprocess,sys,time;"
        f"subprocess.Popen([sys.executable,'-c',{child_code!r}]);"
        "time.sleep(10)"
    )
    harness = f"""
import importlib.util, pathlib, subprocess, sys, time
spec = importlib.util.spec_from_file_location('user_smoke_harness', {str(SCRIPT)!r})
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)
try:
    module._bounded_subprocess_run(
        [sys.executable, '-c', {parent_code!r}],
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
raise SystemExit(1 if pathlib.Path({str(marker)!r}).exists() else 0)
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


@pytest.mark.skipif(os.name != "posix", reason="requires POSIX process groups")
def test_bounded_runner_timeout_when_child_does_not_read_stdin():
    harness = f"""
import importlib.util, subprocess, sys, time
spec = importlib.util.spec_from_file_location('user_smoke_stdin_harness', {str(SCRIPT)!r})
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)
started = time.monotonic()
try:
    module._bounded_subprocess_run(
        [sys.executable, '-c', 'import time; time.sleep(1)'],
        input=b'x' * (2 * 1024 * 1024),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=0.1,
        check=False,
        env={{'PATH': {os.environ.get('PATH', '')!r}}},
        cwd='/',
        start_new_session=True,
    )
except subprocess.TimeoutExpired:
    elapsed = time.monotonic() - started
    raise SystemExit(0 if elapsed < 0.8 else 3)
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


@pytest.mark.skipif(os.name != "posix", reason="requires POSIX process groups")
def test_bounded_runner_kills_process_on_oversized_output():
    harness = f"""
import importlib.util, subprocess, sys
spec = importlib.util.spec_from_file_location('user_smoke_output_harness', {str(SCRIPT)!r})
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)
module.MAX_COMMAND_OUTPUT_BYTES = 1024
try:
    module._bounded_subprocess_run(
        [sys.executable, '-c', "import os,time; os.write(1, b'x' * 4096); time.sleep(10)"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=2,
        check=False,
        env={{'PATH': {os.environ.get('PATH', '')!r}}},
        cwd='/',
        start_new_session=True,
    )
except module.ProbeFailure as exc:
    raise SystemExit(0 if exc.payload.get('phase') == 'output' else 3)
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
