"""Offline tests for the redacted Aily Agent smoke probe."""

import importlib.util
import json
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).parents[2] / "scripts" / "feishu_aily_agent_smoke.py"
SPEC = importlib.util.spec_from_file_location("feishu_aily_agent_smoke", SCRIPT)
assert SPEC and SPEC.loader
smoke = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = smoke
SPEC.loader.exec_module(smoke)


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


def test_default_main_only_checks_config(monkeypatch, capsys):
    monkeypatch.delenv("FEISHU_AILY_AUTH_APP_ID", raising=False)
    monkeypatch.delenv("FEISHU_AILY_AUTH_APP_SECRET", raising=False)
    monkeypatch.delenv("FEISHU_AILY_AGENT_ID", raising=False)
    assert smoke.main(["--pretty"]) == 2
    output = json.loads(capsys.readouterr().out)
    assert output["phase"] == "config"
    assert output["network_request_sent"] is False


def test_run_probe_posts_token_then_stream_chat_without_leaking_secret():
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
                            "status": "Cancelled",
                            "finish_reason": "stop",
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

    result = smoke.run_probe(_config(), "OOI是什么?", opener=opener)

    assert result["ok"] is True
    assert result["answer"] == "OOI answer"
    assert result["artifact_count"] == 0
    assert len(requests) == 3
    chat_body = json.loads(requests[1].data)
    assert chat_body["stream"] is False
    assert chat_body["user_message"]["content"][0]["text"] == "OOI是什么?"
    assert "/agents/agent_test/chats" in requests[1].full_url
    assert "/agents/agent_test/chats/chat_1" in requests[2].full_url
    assert "t-secret" not in json.dumps(result)
    assert "secret-never-print" not in json.dumps(result)


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
