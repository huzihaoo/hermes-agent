"""Offline tests for the redacted Feishu Aily smoke probe."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest


SCRIPT = Path(__file__).parents[2] / "scripts" / "feishu_aily_knowledge_smoke.py"
SPEC = importlib.util.spec_from_file_location("feishu_aily_knowledge_smoke", SCRIPT)
assert SPEC and SPEC.loader
smoke = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = smoke
SPEC.loader.exec_module(smoke)


class _Response:
    def __init__(
        self,
        body=b"",
        *,
        status=200,
        content_type="application/json",
        lines=None,
        headers=None,
    ):
        self.status = status
        self.headers = {
            "Content-Type": content_type,
            **(headers or {}),
        }
        self._body = body
        self._lines = list(lines or [])

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self, *_args):
        return self._body

    def __iter__(self):
        return iter(self._lines)


def _config():
    return smoke.ProbeConfig(
        auth_app_id="caller-app",
        auth_app_secret="caller-secret",
        target_app_id="spring_target__c",
        domain="feishu",
    )


def _sse(*payloads):
    lines = []
    for payload in payloads:
        lines.extend([
            f"data: {json.dumps(payload, ensure_ascii=False)}\n".encode("utf-8"),
            b"\n",
        ])
    return lines


def test_config_check_reads_allowlisted_env_without_network_or_file_mutation(
    tmp_path, monkeypatch
):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "FEISHU_AILY_AUTH_APP_ID=caller-app\n"
        "FEISHU_AILY_AUTH_APP_SECRET=caller-secret\n"
        "FEISHU_AILY_TARGET_APP_ID=spring_target__c\n",
        encoding="utf-8",
    )
    env_file.chmod(0o600)
    before = env_file.read_bytes()
    monkeypatch.delenv("FEISHU_AILY_AUTH_APP_ID", raising=False)
    monkeypatch.delenv("FEISHU_AILY_AUTH_APP_SECRET", raising=False)
    monkeypatch.delenv("FEISHU_AILY_TARGET_APP_ID", raising=False)

    config = smoke.load_probe_config(env_file)
    summary = smoke.config_summary(config)

    assert summary == {
        "auth_app_configured": True,
        "target_app_configured": True,
        "target_looks_like_aily_app": True,
        "domain": "feishu",
    }
    assert env_file.read_bytes() == before
    assert "caller-secret" not in json.dumps(summary)


def test_config_rejects_caller_app_as_aily_target(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "FEISHU_AILY_AUTH_APP_ID=same-id\n"
        "FEISHU_AILY_AUTH_APP_SECRET=secret\n"
        "FEISHU_AILY_TARGET_APP_ID=same-id\n",
        encoding="utf-8",
    )
    env_file.chmod(0o600)

    with pytest.raises(smoke.ProbeFailure, match="caller Open Platform app ID"):
        smoke.load_probe_config(env_file)


def test_config_rejects_missing_explicit_env_file_without_using_process_env(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("FEISHU_AILY_AUTH_APP_ID", "process-app")
    monkeypatch.setenv("FEISHU_AILY_AUTH_APP_SECRET", "process-secret")
    monkeypatch.setenv("FEISHU_AILY_TARGET_APP_ID", "spring_process")

    with pytest.raises(smoke.ProbeFailure, match="explicit env file") as caught:
        smoke.load_probe_config(tmp_path / "typo.env")

    assert caught.value.payload["network_request_sent"] is False


def test_config_rejects_oversized_target_before_network(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "FEISHU_AILY_AUTH_APP_ID=caller-app\n"
        "FEISHU_AILY_AUTH_APP_SECRET=secret\n"
        f"FEISHU_AILY_TARGET_APP_ID={'s' * 256}\n",
        encoding="utf-8",
    )
    env_file.chmod(0o600)

    with pytest.raises(smoke.ProbeFailure, match="at most 255") as caught:
        smoke.load_probe_config(env_file)

    assert caught.value.payload["network_request_sent"] is False


def test_config_rejects_world_readable_env_file(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "FEISHU_AILY_AUTH_APP_ID=caller-app\n"
        "FEISHU_AILY_AUTH_APP_SECRET=secret\n"
        "FEISHU_AILY_TARGET_APP_ID=spring_target__c\n",
        encoding="utf-8",
    )
    env_file.chmod(0o644)

    with pytest.raises(smoke.ProbeFailure, match="group/world-readable"):
        smoke.load_probe_config(env_file)


def test_run_probe_uses_tenant_token_and_parses_finished_sse():
    responses = iter([
        _Response(
            json.dumps({
                "code": 0,
                "tenant_access_token": "tenant-secret",
                "expire": 7200,
            }).encode()
        ),
        _Response(
            content_type="text/event-stream; charset=utf-8",
            headers={"X-Tt-Logid": "log-123"},
            lines=_sse(
                {"status": "processing", "message": {"content": "partial"}},
                {
                    "status": "finished",
                    "finish_type": "qa",
                    "has_answer": True,
                    "message": {"content": "answer"},
                    "process_data": {"chunks": ["a"], "sql_data": []},
                },
            ),
        ),
    ])
    requests = []

    def opener(request, timeout):
        requests.append((request, timeout))
        return next(responses)

    result = smoke.run_probe(
        _config(),
        "What is OOI?",
        data_asset_ids=["asset-1"],
        data_asset_tag_ids=["tag-1"],
        opener=opener,
    )

    assert result["ok"] is True
    assert result["status"] == "finished"
    assert result["has_answer"] is True
    assert result["grounded"] is True
    assert result["answer_available"] is True
    assert result["content"] == "answer"
    assert result["evidence_counts"] == {"chunks": 1, "sql_data": 0}
    assert "tenant-secret" not in json.dumps(result)
    token_request, _ = requests[0]
    ask_request, _ = requests[1]
    assert token_request.full_url.endswith(smoke.TOKEN_PATH)
    assert ask_request.full_url.endswith(
        "/open-apis/aily/v1/apps/spring_target__c/knowledges/ask"
    )
    assert ask_request.headers["Accept"] == "text/event-stream"
    assert ask_request.headers["Authorization"] == "Bearer tenant-secret"
    assert json.loads(ask_request.data.decode("utf-8"))["data_asset_ids"] == ["asset-1"]


def test_run_probe_reports_finished_no_answer_as_valid_unanswered_result():
    responses = iter([
        _Response(json.dumps({"code": 0, "tenant_access_token": "token"}).encode()),
        _Response(
            content_type="text/event-stream",
            lines=_sse(
                {"status": "processing", "message": {"content": "partial"}},
                {
                    "status": "finished",
                    "finish_type": "qa",
                    "has_answer": False,
                    "message": {"content": ""},
                },
            ),
        ),
    ])

    result = smoke.run_probe(
        _config(), "question", opener=lambda *_args, **_kwargs: next(responses)
    )

    assert result["ok"] is True
    assert result["has_answer"] is False
    assert result["grounded"] is False
    assert result["answer_available"] is False
    assert result["content"] == ""
    assert "partial" not in json.dumps(result)


@pytest.mark.parametrize(
    "finished",
    [
        {
            "status": "finished",
            "finish_type": "qa",
            "has_answer": "false",
            "message": {"content": "must not be trusted"},
        },
        {
            "status": "finished",
            "finish_type": "unknown",
            "has_answer": True,
            "message": {"content": "must not be trusted"},
        },
    ],
)
def test_run_probe_rejects_malformed_finished_contract(finished):
    responses = iter([
        _Response(json.dumps({"code": 0, "tenant_access_token": "token"}).encode()),
        _Response(content_type="text/event-stream", lines=_sse(finished)),
    ])

    with pytest.raises(smoke.ProbeFailure, match="finished event") as caught:
        smoke.run_probe(
            _config(), "question", opener=lambda *_args, **_kwargs: next(responses)
        )

    assert caught.value.payload["phase"] == "ask"
    assert caught.value.payload["ok"] is False


def test_run_probe_surfaces_embedded_business_error_without_waiting_for_finished():
    responses = iter([
        _Response(json.dumps({"code": 0, "tenant_access_token": "token"}).encode()),
        _Response(
            content_type="text/event-stream",
            lines=_sse({"code": 2700034, "msg": "forbidden"}),
        ),
    ])

    with pytest.raises(smoke.ProbeFailure) as caught:
        smoke.run_probe(
            _config(), "question", opener=lambda *_args, **_kwargs: next(responses)
        )

    assert caught.value.payload["code"] == 2700034
    assert caught.value.payload["error"] == "forbidden"


def test_http_failure_receipt_keeps_rate_limit_metadata_and_redacts_secret():
    headers = {"X-Tt-Logid": "log-429", "X-Ogw-Ratelimit-Reset": "9"}

    class _FailingOpener:
        def __init__(self):
            self.calls = 0

        def __call__(self, request, timeout):
            self.calls += 1
            if self.calls == 1:
                return _Response(
                    json.dumps({"code": 0, "tenant_access_token": "token"}).encode()
                )
            raise smoke.urllib.error.HTTPError(
                request.full_url,
                429,
                "rate limited",
                headers,
                __import__("io").BytesIO(b'{"code":99991400,"msg":"slow down"}'),
            )

    with pytest.raises(smoke.ProbeFailure) as caught:
        smoke.run_probe(_config(), "question", opener=_FailingOpener())

    payload = caught.value.payload
    assert payload["http_status"] == 429
    assert payload["code"] == 99991400
    assert payload["log_id"] == "log-429"
    assert payload["rate_limit_reset"] == "9"
    assert "caller-secret" not in json.dumps(payload)


def test_main_returns_input_exit_code_without_sending_network(tmp_path, capsys):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "FEISHU_AILY_AUTH_APP_ID=caller-app\n"
        "FEISHU_AILY_AUTH_APP_SECRET=secret\n"
        "FEISHU_AILY_TARGET_APP_ID=spring_target__c\n",
        encoding="utf-8",
    )
    env_file.chmod(0o600)

    exit_code = smoke.main([
        "--env-file",
        str(env_file),
        "--execute",
        "--question",
        "question",
        "--timeout",
        "0",
    ])

    receipt = json.loads(capsys.readouterr().out)
    assert exit_code == 2
    assert receipt["phase"] == "input"
    assert receipt["network_request_sent"] is False
