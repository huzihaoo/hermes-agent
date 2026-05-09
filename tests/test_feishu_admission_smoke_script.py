from __future__ import annotations

import importlib.util
from pathlib import Path


repo_root = Path(__file__).parent.parent
script_path = repo_root / "scripts" / "feishu_admission_smoke.py"
spec = importlib.util.spec_from_file_location("feishu_admission_smoke", str(script_path))
smoke = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(smoke)


def test_smoke_assertions_accept_expected_non_public_dispatch_result():
    result = {
        "dispatch": {
            "items": [
                {
                    "lane": "fast",
                    "chat_id": "oc_smoke_group",
                    "chat_type": "group",
                    "thread_id": "topic:om_smoke_thread",
                    "platform": "feishu",
                },
                {
                    "lane": "standard",
                    "chat_id": "oc_smoke_group",
                    "chat_type": "group",
                    "thread_id": "topic:om_smoke_thread",
                    "platform": "feishu",
                },
                {
                    "lane": "heavy",
                    "chat_id": "oc_smoke_group",
                    "chat_type": "group",
                    "thread_id": "topic:om_smoke_thread",
                    "platform": "feishu",
                },
            ],
            "public_feedback_sent": [
                {
                    "chat_id": "oc_smoke_group",
                    "content": "heavy notice",
                    "metadata": {"thread_id": "topic:om_smoke_thread"},
                }
            ],
            "reconstructed": [
                {"chat_type": "group", "thread_id": "topic:om_smoke_thread"},
                {"chat_type": "group", "thread_id": "topic:om_smoke_thread"},
                {"chat_type": "group", "thread_id": "topic:om_smoke_thread"},
            ],
        },
        "health": {
            "status": "ok",
            "gateway_state": "running",
            "platforms": {
                "feishu": {"state": "connected"},
                "api_server": {"state": "connected"},
            },
        },
        "metrics": {
            "queue_depth_zero_count": 9,
            "admission_total_failed_zero": True,
        },
    }

    assert smoke._assert_smoke(result) == []


def test_smoke_assertions_fail_when_fast_feedback_is_public():
    result = {
        "dispatch": {
            "items": [
                {
                    "lane": "fast",
                    "chat_id": "oc_smoke_group",
                    "chat_type": "group",
                    "thread_id": "topic:om_smoke_thread",
                    "platform": "feishu",
                },
                {
                    "lane": "standard",
                    "chat_id": "oc_smoke_group",
                    "chat_type": "group",
                    "thread_id": "topic:om_smoke_thread",
                    "platform": "feishu",
                },
                {
                    "lane": "heavy",
                    "chat_id": "oc_smoke_group",
                    "chat_type": "group",
                    "thread_id": "topic:om_smoke_thread",
                    "platform": "feishu",
                },
            ],
            "public_feedback_sent": [
                {"metadata": {"thread_id": "topic:om_smoke_thread"}},
                {"metadata": {"thread_id": "topic:om_smoke_thread"}},
            ],
            "reconstructed": [
                {"chat_type": "group", "thread_id": "topic:om_smoke_thread"},
                {"chat_type": "group", "thread_id": "topic:om_smoke_thread"},
                {"chat_type": "group", "thread_id": "topic:om_smoke_thread"},
            ],
        }
    }

    errors = smoke._assert_smoke(result)

    assert any("exactly one heavy public feedback" in error for error in errors)

release_script_path = repo_root / "scripts" / "hermes_release_smoke.py"
release_spec = importlib.util.spec_from_file_location("hermes_release_smoke", str(release_script_path))
release_smoke = importlib.util.module_from_spec(release_spec)
assert release_spec.loader is not None
release_spec.loader.exec_module(release_smoke)


def test_release_smoke_fetch_json_reports_http_error_without_traceback(monkeypatch):
    class FakeHTTPError(release_smoke.urllib.error.HTTPError):
        def read(self):
            return b'{"status":"starting"}'

    def fake_urlopen(url, timeout):
        raise FakeHTTPError(url, 503, "Service Unavailable", hdrs=None, fp=None)

    monkeypatch.setattr(release_smoke.urllib.request, "urlopen", fake_urlopen)

    result = release_smoke._fetch_json("http://127.0.0.1:18789/health/detailed", 1)

    assert result["status"] == "unavailable"
    assert result["error"] == "http_error"
    assert result["http_status"] == 503
    assert result["body_head"] == "[omitted]"


def test_release_smoke_fetch_json_reports_invalid_json_without_traceback(monkeypatch):
    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def read(self):
            return b"not-json"

    monkeypatch.setattr(release_smoke.urllib.request, "urlopen", lambda url, timeout: FakeResponse())

    result = release_smoke._fetch_json("http://127.0.0.1:18789/health/detailed", 1)

    assert result["status"] == "unavailable"
    assert result["error"] == "invalid_json"
    assert "url" in result


def test_release_smoke_fetch_json_redacts_url_query_on_failures(monkeypatch):
    def fake_urlopen(url, timeout):
        raise TimeoutError("timed out")

    monkeypatch.setattr(release_smoke.urllib.request, "urlopen", fake_urlopen)

    result = release_smoke._fetch_json("http://127.0.0.1:18789/health/detailed?token=secret#frag", 1)

    assert result["status"] == "unavailable"
    assert result["error"] == "timeout"
    assert result["url"] == "http://127.0.0.1:18789/health/detailed?[REDACTED]#[REDACTED]"
    assert "secret" not in result["url"]


def test_release_smoke_ignores_process_command_echo_high_signal(tmp_path):
    log = tmp_path / "gateway.error.log"
    log.write_text(
        "  songying         93079   0.0  0.0 34256784   1172   ??  Ss    "
        "5:56PM   0:00.01 /bin/bash -c tail -500 agent.log | rg -n "
        '"Traceback|NameError|ValueError" -C 8\n'
        "Traceback (most recent call last):\n"
        "ERROR root: API call failed after 3 retries. HTTP 502: Upstream request failed\n",
        encoding="utf-8",
    )

    high_signal = release_smoke._high_signal_log_lines(log, 120)

    assert not any("/bin/bash -c" in line for line in high_signal)
    assert high_signal == [
        "Traceback (most recent call last):",
        "ERROR root: API call failed after 3 retries. HTTP 502: Upstream request failed",
    ]


def test_release_smoke_log_start_offset_limits_high_signal_scan(tmp_path):
    log = tmp_path / "gateway.error.log"
    old = b"Traceback (old failure)\n"
    log.write_bytes(old + "INFO clean restart\n".encode("utf-8"))

    high_signal = release_smoke._high_signal_log_lines(log, 120, start_offset=len(old))

    assert high_signal == []


def test_release_smoke_resolves_log_start_offset_boundaries(tmp_path):
    log = tmp_path / "gateway.error.log"
    log.write_bytes(b"12345")

    assert release_smoke._resolve_log_start_offset(log, None) == 0
    assert release_smoke._resolve_log_start_offset(log, -10) == 0
    assert release_smoke._resolve_log_start_offset(log, 999) == 5
    assert release_smoke._resolve_log_start_offset(log, 2) == 2
    assert release_smoke._resolve_log_start_offset(log, 0, from_end=True) == 5


def test_release_smoke_log_start_at_end_sets_boundary(tmp_path):
    log = tmp_path / "gateway.error.log"
    log.write_text("Traceback (old failure)\n", encoding="utf-8")

    args = release_smoke.argparse.Namespace(
        hermes_home=str(tmp_path),
        config=str(tmp_path / "missing-config.yaml"),
        env=str(tmp_path / "missing.env"),
        launchd_plist=str(tmp_path / "missing.plist"),
        gateway_error_log=str(log),
        sync_script=str(tmp_path / "missing-sync"),
        cli=str(tmp_path / "missing-cli"),
        no_runtime=True,
        health_url="http://127.0.0.1:18789/health/detailed",
        timeout=1.0,
        no_cli=True,
        with_vm=False,
        ssh_mini_agent=str(tmp_path / "missing-ssh-mini-agent"),
        vm_timeout=1.0,
        recommended_turn_budget=release_smoke.DEFAULT_RECOMMENDED_TURN_BUDGET,
        log_tail_lines=120,
        log_start_offset=0,
        log_start_at_end=True,
    )

    result = release_smoke.collect_smoke(args)

    assert result["logs"]["log_start_offset"] == log.stat().st_size
    assert result["logs"]["log_start_at_end"] is True
    assert result["logs"]["high_signal_tail"] == []


def test_release_smoke_receipt_summary_is_compact():
    result = {
        "ok": True,
        "errors": [],
        "health": {
            "status": "ok",
            "gateway_state": "running",
            "pid": 123,
            "platforms": {
                "feishu": {"state": "connected"},
                "api_server": {"state": "connected"},
            },
        },
        "logs": {
            "log_start_offset": 42,
            "log_start_at_end": True,
            "current_size": 42,
            "high_signal_tail": [],
        },
        "config": {"terminal_cwd": "/tmp/should-not-be-in-receipt"},
    }

    receipt = release_smoke._smoke_receipt_summary(result)

    assert receipt == {
        "ok": True,
        "errors": [],
        "gateway_state": "running",
        "health_status": "ok",
        "pid": 123,
        "feishu_state": "connected",
        "api_server_state": "connected",
        "log_start_offset": 42,
        "log_start_at_end": True,
        "current_size": 42,
        "high_signal_count": 0,
    }


def test_release_smoke_help_includes_release_gate_examples(capsys):
    parser = release_smoke._build_parser()

    parser.print_help()
    help_text = capsys.readouterr().out

    assert "Release gate examples" in help_text
    assert "--log-start-at-end --receipt" in help_text
    assert "--log-start-offset 12345 --receipt" in help_text


def test_release_smoke_assertions_accept_healthy_host_result():
    result = {
        "config": {"agent_max_turns": 3000, "terminal_cwd": "/work"},
        "env": {"deprecated_keys_present": []},
        "health": {
            "status": "ok",
            "gateway_state": "running",
            "platforms": {
                "feishu": {"state": "connected"},
                "api_server": {"state": "connected"},
            },
        },
        "cli": {"ok": True},
        "launchd": {
            "exists": True,
            "program_arguments": ["python", "-m", "hermes_cli.main", "gateway", "run"],
            "working_directory": "/repo",
            "virtual_env": "/repo/.venv",
        },
        "logs": {"high_signal_tail": []},
        "sync_script": {"writes_deprecated_messaging_cwd": False},
        "vm": {"skipped": True},
        "thresholds": {"recommended_turn_budget": 2500},
    }

    assert release_smoke.assert_smoke(result, require_runtime=True, require_vm=False) == []


def test_release_smoke_assertions_fail_on_rebase_regression_signals():
    result = {
        "config": {"agent_max_turns": 900, "terminal_cwd": ""},
        "env": {"deprecated_keys_present": ["MESSAGING_CWD"]},
        "health": {
            "status": "ok",
            "gateway_state": "running",
            "platforms": {
                "feishu": {"state": "disconnected"},
                "api_server": {"state": "connected"},
            },
        },
        "cli": {"ok": False},
        "launchd": {
            "exists": True,
            "program_arguments": ["python", "old.py"],
            "working_directory": "/repo",
            "virtual_env": "/other/.venv",
        },
        "logs": {"high_signal_tail": ["NameError: name 'DEVELOPER_ROLE_MODELS' is not defined"]},
        "sync_script": {"writes_deprecated_messaging_cwd": True},
        "vm": {"ok": False},
        "thresholds": {"recommended_turn_budget": 2500},
    }

    errors = release_smoke.assert_smoke(result, require_runtime=True, require_vm=True)

    assert any("max_turns below" in error for error in errors)
    assert any("deprecated env keys" in error for error in errors)
    assert any("sync script still writes" in error for error in errors)
    assert any("feishu not connected" in error for error in errors)
    assert any("gateway.error.log contains high-signal" in error for error in errors)
    assert any("ssh-mini-agent doctor failed" in error for error in errors)
