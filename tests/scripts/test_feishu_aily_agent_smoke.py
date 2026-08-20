"""Offline tests for the redacted Aily Agent smoke probe."""

import builtins
import importlib.util
import json
import math
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).parents[2] / "scripts" / "feishu_aily_agent_smoke.py"
SPEC = importlib.util.spec_from_file_location("feishu_aily_agent_smoke", SCRIPT)
assert SPEC and SPEC.loader
smoke = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = smoke
SPEC.loader.exec_module(smoke)


@pytest.fixture(autouse=True)
def _no_poll_sleep(monkeypatch):
    monkeypatch.setattr(smoke.time, "sleep", lambda _seconds: None)


class _Response:
    def __init__(self, body, *, status=200, headers=None):
        self._body = body
        self.status = status
        self.headers = headers or {}

    def read(self, amount=-1):
        if amount < 0:
            return self._body
        return self._body[:amount]

    def close(self):
        return None


def _config(**overrides):
    values = {
        "auth_app_id": "cli_auth",
        "auth_app_secret": "secret-never-print",
        "agent_id": "agent_test",
        "domain": "feishu",
    }
    values.update(overrides)
    return smoke.ProbeConfig(**values)


def test_config_requires_agent_id_and_caps_length(monkeypatch):
    monkeypatch.setenv("FEISHU_AILY_AUTH_APP_ID", "cli_auth")
    monkeypatch.setenv("FEISHU_AILY_AUTH_APP_SECRET", "secret")
    monkeypatch.setenv("FEISHU_AILY_AGENT_ID", "a" * 66)
    with pytest.raises(smoke.ProbeFailure) as exc:
        smoke.load_probe_config()
    assert exc.value.payload["phase"] == "config"
    assert exc.value.payload["network_request_sent"] is False


def test_config_rejects_non_agent_id_before_network(monkeypatch):
    monkeypatch.setenv("FEISHU_AILY_AUTH_APP_ID", "cli_auth")
    monkeypatch.setenv("FEISHU_AILY_AUTH_APP_SECRET", "secret")
    monkeypatch.setenv("FEISHU_AILY_AGENT_ID", "cli_wrong")

    with pytest.raises(smoke.ProbeFailure) as exc:
        smoke.load_probe_config()

    assert exc.value.payload["phase"] == "config"
    assert exc.value.payload["network_request_sent"] is False


def test_default_main_only_checks_config(monkeypatch, capsys):
    monkeypatch.delenv("FEISHU_AILY_AUTH_APP_ID", raising=False)
    monkeypatch.delenv("FEISHU_AILY_AUTH_APP_SECRET", raising=False)
    monkeypatch.delenv("FEISHU_AILY_AGENT_ID", raising=False)
    assert smoke.main(["--pretty"]) == 2
    output = json.loads(capsys.readouterr().out)
    assert output["phase"] == "config"
    assert output["network_request_sent"] is False


def test_run_probe_posts_then_polls_without_leaking_secret():
    responses = iter(
        [
            _Response(
                json.dumps({"code": 0, "tenant_access_token": "t-secret"}).encode(),
                headers={"content-type": "application/json"},
            ),
            _Response(
                json.dumps(
                    {
                        "code": 0,
                        "data": {"agent_chat_id": "chat_1", "session_id": "conversation_1"},
                    }
                ).encode(),
                headers={"content-type": "application/json"},
            ),
            _Response(
                json.dumps(
                    {
                        "code": 0,
                        "data": {
                            "status": "Queued",
                            "finish_reason": "failed",
                            "content": [{"type": "text", "text": "partial"}],
                        },
                    }
                ).encode(),
                headers={"content-type": "application/json"},
            ),
            _Response(
                json.dumps(
                    {
                        "code": 0,
                        "data": {
                            "status": "Completed",
                            "finish_reason": "error",
                            "content": [{"type": "text", "text": "OOI answer"}],
                        },
                    }
                ).encode(),
                headers={"content-type": "application/json", "x-tt-logid": "log_1"},
            ),
        ]
    )
    requests = []

    def opener(request, timeout):
        requests.append(request)
        return next(responses)

    result = smoke.run_probe(
        _config(), "OOI是什么?", include_answer=True, opener=opener
    )

    assert result["ok"] is True
    assert result["answer"] == "OOI answer"
    assert result["answer_length"] == len("OOI answer")
    assert result["status"] == "Completed"
    assert result["poll_count"] == 2
    assert result["artifact_count"] == 0
    assert len(requests) == 4
    chat_body = json.loads(requests[1].data)
    assert chat_body["stream"] is False
    assert chat_body["user_message"]["content"][0]["text"] == "OOI是什么?"
    assert "/agents/agent_test/chats" in requests[1].full_url
    assert "/agents/agent_test/chats/chat_1" in requests[2].full_url
    assert "/agents/agent_test/chats/chat_1" in requests[3].full_url
    assert "t-secret" not in json.dumps(result)
    assert "secret-never-print" not in json.dumps(result)


def test_run_probe_redacts_known_secrets_from_success_receipt():
    responses = iter(
        [
            _Response(
                json.dumps(
                    {"code": 0, "tenant_access_token": "t-sensitive-token"}
                ).encode()
            ),
            _Response(
                json.dumps({"code": 0, "data": {"agent_chat_id": "chat_1"}}).encode()
            ),
            _Response(
                json.dumps(
                    {
                        "code": 0,
                        "data": {
                            "status": "Completed",
                            "finish_reason": "secret-never-print t-sensitive-token",
                            "content": [
                                {
                                    "type": "text",
                                    "text": "answer secret-never-print t-sensitive-token",
                                }
                            ],
                        },
                    }
                ).encode(),
                headers={
                    "content-type": "application/json; t-sensitive-token",
                    "x-tt-logid": "secret-never-print",
                },
            ),
        ]
    )

    result = smoke.run_probe(
        _config(),
        "OOI是什么?",
        include_answer=True,
        opener=lambda _request, timeout: next(responses),
    )

    receipt = json.dumps(result)
    assert result["status"] == "Completed"
    assert "secret-never-print" not in receipt
    assert "t-sensitive-token" not in receipt


def test_run_probe_hides_answer_by_default():
    responses = iter(
        [
            _Response(json.dumps({"code": 0, "tenant_access_token": "token"}).encode()),
            _Response(
                json.dumps({"code": 0, "data": {"agent_chat_id": "chat_1"}}).encode()
            ),
            _Response(
                json.dumps(
                    {
                        "code": 0,
                        "data": {
                            "status": "Completed",
                            "content": [{"type": "text", "text": "internal answer"}],
                        },
                    }
                ).encode()
            ),
        ]
    )

    result = smoke.run_probe(
        _config(), "OOI是什么?", opener=lambda _request, timeout: next(responses)
    )

    assert result["answer_available"] is True
    assert result["answer_length"] == len("internal answer")
    assert "answer" not in result


def test_agent_text_deduplicates_identical_completed_items():
    assert smoke._agent_text(
        {
            "content": [
                {"type": "text", "text": "OOI internal answer"},
                {"type": "text", "text": "OOI internal answer"},
            ]
        }
    ) == "OOI internal answer"


def test_agent_text_preserves_nonadjacent_repeated_items():
    assert smoke._agent_text(
        {
            "content": [
                {"type": "text", "text": "A"},
                {"type": "text", "text": "B"},
                {"type": "text", "text": "A"},
            ]
        }
    ) == "ABA"


def test_agent_text_preserves_repeats_separated_by_non_text_items():
    assert smoke._agent_text(
        {
            "content": [
                {"type": "text", "text": "A"},
                {"type": "artifact", "agent_artifact_id": "artifact_1"},
                {"type": "text", "text": "A"},
                {"type": "text", "text": "   "},
                {"type": "text", "text": "A"},
            ]
        }
    ) == "AAA"


def test_run_probe_rejects_cancelled_stop_with_partial_text():
    responses = iter(
        [
            _Response(json.dumps({"code": 0, "tenant_access_token": "token"}).encode()),
            _Response(
                json.dumps({"code": 0, "data": {"agent_chat_id": "chat_1"}}).encode()
            ),
            _Response(
                json.dumps(
                    {
                        "code": 0,
                        "data": {
                            "status": "Cancelled",
                            "finish_reason": "stop",
                            "content": [{"type": "text", "text": "partial"}],
                        },
                    }
                ).encode()
            ),
        ]
    )

    with pytest.raises(smoke.ProbeFailure) as exc:
        smoke.run_probe(
            _config(), "OOI是什么?", opener=lambda _request, timeout: next(responses)
        )

    assert "Cancelled" in exc.value.payload["error"]


def test_run_probe_rejects_bad_question_before_network():
    called = False

    def opener(*_args, **_kwargs):
        nonlocal called
        called = True
        raise AssertionError("network should not be called")

    with pytest.raises(smoke.ProbeFailure) as exc:
        smoke.run_probe(_config(), " ", opener=opener)
    assert exc.value.payload["phase"] == "input"
    assert called is False


def test_run_probe_preserves_unknown_identity_business_code_and_message():
    responses = iter(
        [
            _Response(json.dumps({"code": 0, "tenant_access_token": "token"}).encode()),
            _Response(
                json.dumps({"code": 10010, "msg": "permission denied"}).encode(),
                status=403,
            ),
        ]
    )

    def opener(_request, timeout):
        assert 0 < timeout <= 120.0
        return next(responses)

    with pytest.raises(smoke.ProbeFailure) as exc:
        smoke.run_probe(_config(), "OOI是什么?", opener=opener)

    assert exc.value.payload["code"] == 10010
    assert "permission denied" in exc.value.payload["error"]


def test_redact_removes_explicit_secrets():
    output = smoke._redact(
        "secret-never-print and t-secret",
        secrets=("secret-never-print", "t-secret"),
    )

    assert "secret-never-print" not in output
    assert "t-secret" not in output


def test_redact_fails_closed_when_redactor_is_unavailable(monkeypatch):
    real_import = builtins.__import__

    def broken_import(name, *args, **kwargs):
        if name == "agent.redact":
            raise RuntimeError("redactor unavailable")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", broken_import)

    assert smoke._redact("sensitive") == "[redaction unavailable]"


def test_run_probe_redacts_server_echoes_of_known_secrets():
    responses = iter(
        [
            _Response(
                json.dumps({"code": 0, "tenant_access_token": "t-secret"}).encode()
            ),
            _Response(
                json.dumps({"code": 10007, "msg": "secret-never-print t-secret"}).encode(),
                status=403,
                headers={"x-tt-logid": "t-secret"},
            ),
        ]
    )

    with pytest.raises(smoke.ProbeFailure) as exc:
        smoke.run_probe(
            _config(), "OOI是什么?", opener=lambda _request, timeout: next(responses)
        )

    receipt = json.dumps(exc.value.payload)
    assert "secret-never-print" not in receipt
    assert "t-secret" not in receipt


@pytest.mark.parametrize(
    ("token_payload", "expected"),
    [
        ([], "non-object JSON"),
        ({"code": []}, "invalid business code"),
    ],
)
def test_run_probe_rejects_malformed_token_json(token_payload, expected):
    with pytest.raises(smoke.ProbeFailure) as exc:
        smoke.run_probe(
            _config(),
            "OOI是什么?",
            opener=lambda _request, timeout: _Response(
                json.dumps(token_payload).encode()
            ),
        )

    assert expected in exc.value.payload["error"]


def test_run_probe_rejects_non_list_completed_content():
    responses = iter(
        [
            _Response(json.dumps({"code": 0, "tenant_access_token": "token"}).encode()),
            _Response(
                json.dumps({"code": 0, "data": {"agent_chat_id": "chat_1"}}).encode()
            ),
            _Response(
                json.dumps(
                    {"code": 0, "data": {"status": "Completed", "content": 1}}
                ).encode()
            ),
        ]
    )

    with pytest.raises(smoke.ProbeFailure) as exc:
        smoke.run_probe(
            _config(), "OOI是什么?", opener=lambda _request, timeout: next(responses)
        )

    assert "invalid content" in exc.value.payload["error"]


def test_run_probe_rejects_nan_timeout_before_network():
    called = False

    def opener(*_args, **_kwargs):
        nonlocal called
        called = True

    with pytest.raises(smoke.ProbeFailure) as exc:
        smoke.run_probe(_config(), "OOI是什么?", timeout=math.nan, opener=opener)

    assert exc.value.payload["phase"] == "input"
    assert called is False


def test_run_probe_passes_decreasing_total_budget(monkeypatch):
    responses = iter(
        [
            _Response(json.dumps({"code": 0, "tenant_access_token": "token"}).encode()),
            _Response(
                json.dumps({"code": 0, "data": {"agent_chat_id": "chat_1"}}).encode()
            ),
            _Response(
                json.dumps(
                    {"code": 0, "data": {"status": "Completed", "content": []}}
                ).encode()
            ),
        ]
    )
    now = [0.0]
    request_timeouts = []
    monkeypatch.setattr(smoke.time, "monotonic", lambda: now[0])

    def opener(_request, timeout):
        request_timeouts.append(timeout)
        now[0] += 10.0
        return next(responses)

    result = smoke.run_probe(_config(), "OOI是什么?", timeout=120.0, opener=opener)

    assert result["ok"] is True
    assert request_timeouts == [120.0, 110.0, 100.0]
