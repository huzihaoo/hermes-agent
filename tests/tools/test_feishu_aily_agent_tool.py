"""Contract tests for the Aily Agent Chat tool.

All responses are synthetic.  These tests never use the credentials supplied
in chat and never call the Feishu network.
"""

import json
import sys
from types import ModuleType, SimpleNamespace

import pytest

from hermes_cli.tools_config import (
    CONFIGURABLE_TOOLSETS,
    _DEFAULT_OFF_TOOLSETS,
    _get_platform_tools,
    _toolset_allowed_for_platform,
)
from toolsets import resolve_toolset
from tools import feishu_aily_agent_tool as agent


def _sse(*payloads):
    return "\n\n".join(
        f"data: {json.dumps(payload, ensure_ascii=False)}" for payload in payloads
    ).encode("utf-8")


def _response(content, *, code=0, msg="", status_code=200, headers=None):
    return SimpleNamespace(
        code=code,
        msg=msg,
        raw=SimpleNamespace(
            content=content,
            status_code=status_code,
            headers=headers or {},
        ),
    )


class _FakeClient:
    def __init__(self, response):
        self.response = response
        self.requests = []

    def request(self, request):
        self.requests.append(request)
        if isinstance(self.response, list):
            return self.response.pop(0)
        return self.response


@pytest.fixture(autouse=True)
def _clean_state(monkeypatch):
    for name in (
        "FEISHU_AILY_AUTH_APP_ID",
        "FEISHU_AILY_AUTH_APP_SECRET",
        "FEISHU_AILY_AGENT_ID",
        "FEISHU_AILY_DOMAIN",
    ):
        monkeypatch.delenv(name, raising=False)
    token = agent.set_client(None)
    agent._reset_configured_client_cache()
    yield
    agent._reset_configured_client_cache()
    agent.reset_client(token)


@pytest.fixture
def fake_lark_sdk(monkeypatch):
    class RequestBuilder:
        def __init__(self):
            self.values = {}

        def _set(self, name, value):
            self.values[name] = value
            return self

        def http_method(self, value):
            return self._set("http_method", value)

        def uri(self, value):
            return self._set("uri", value)

        def token_types(self, value):
            return self._set("token_types", value)

        def paths(self, value):
            return self._set("paths", value)

        def headers(self, value):
            return self._set("headers", value)

        def body(self, value):
            return self._set("body", value)

        def build(self):
            return SimpleNamespace(**self.values)

    class BaseRequest:
        @staticmethod
        def builder():
            return RequestBuilder()

    lark = ModuleType("lark_oapi")
    lark.AccessTokenType = SimpleNamespace(TENANT="tenant")
    enum = ModuleType("lark_oapi.core.enum")
    enum.HttpMethod = SimpleNamespace(POST="POST", GET="GET")
    base_request = ModuleType("lark_oapi.core.model.base_request")
    base_request.BaseRequest = BaseRequest
    for name, module in {
        "lark_oapi": lark,
        "lark_oapi.core.enum": enum,
        "lark_oapi.core.model.base_request": base_request,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)


def test_parser_requires_completed_event_and_preserves_artifacts():
    result = json.loads(
        agent._parse_agent_response(
            _sse(
                {
                    "data": {
                        "status": "running",
                        "content": [{"type": "text", "text": "OOI"}],
                    }
                },
                {
                    "data": {
                        "status": "completed",
                        "finish_reason": "stop",
                        "content": [
                            {"type": "text", "text": "OOI 是答案"},
                            {
                                "type": "artifact",
                                "agent_artifact_id": "artifact_1",
                                "artifact_type": "file",
                            },
                        ],
                    }
                },
            )
        )
    )

    assert result["success"] is True
    assert result["content"] == "OOI 是答案"
    assert result["finish_reason"] == "stop"
    assert result["artifacts"] == [
        {"agent_artifact_id": "artifact_1", "artifact_type": "file"}
    ]


def test_parser_rejects_processing_only_stream():
    result = json.loads(
        agent._parse_agent_response(
            _sse({"data": {"status": "running", "content": [{"text": "partial"}]}})
        )
    )
    assert "before a completed event" in result["error"]


def test_parser_does_not_report_cancelled_agent_as_success():
    result = json.loads(
        agent._parse_agent_response(
            _sse({"data": {"status": "Cancelled", "finish_reason": "cancelled"}})
        )
    )
    assert "did not complete" in result["error"]


def test_poll_failure_status_returns_tool_error(monkeypatch, fake_lark_sdk):
    client = _FakeClient(
        [
            _response(
                json.dumps(
                    {"code": 0, "data": {"agent_chat_id": "chat_1"}}
                ).encode()
            ),
            _response(
                json.dumps(
                    {"code": 0, "data": {"status": "Failed"}}
                ).encode()
            ),
        ]
    )
    agent.set_client(client)
    monkeypatch.setenv("FEISHU_AILY_AGENT_ID", "agent_test")
    result = json.loads(agent._handle_feishu_aily_agent_chat({"content": "q"}))
    assert "did not complete" in result["error"]


def test_parser_surfaces_embedded_business_error():
    result = json.loads(
        agent._parse_agent_response(
            _sse({"code": 2700001, "msg": "bad request"})
        )
    )
    assert result["code"] == 2700001
    assert "bad request" in result["error"]


def test_parser_accepts_non_stream_json_result():
    result = json.loads(
        agent._parse_agent_response(
            json.dumps(
                {
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "status": "completed",
                        "session_id": "conversation_1",
                        "content": [{"type": "text", "text": "answer"}],
                    },
                }
            ).encode()
        )
    )
    assert result["content"] == "answer"
    assert result["session_id"] == "conversation_1"


def test_handler_builds_agent_chat_request(fake_lark_sdk):
    client = _FakeClient(
        [
            _response(
                json.dumps(
                    {
                        "code": 0,
                        "data": {
                            "agent_chat_id": "chat_1",
                            "session_id": "conversation_1",
                        },
                    }
                ).encode()
            ),
            _response(
                json.dumps(
                    {
                        "code": 0,
                        "data": {
                            "status": "Cancelled",
                            "finish_reason": "stop",
                            "content": [{"type": "text", "text": "OOI answer"}],
                        },
                    }
                ).encode()
            ),
        ]
    )
    agent.set_client(client)
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setenv("FEISHU_AILY_AGENT_ID", "agent_test")
    try:
        result = json.loads(
            agent._handle_feishu_aily_agent_chat(
                {
                    "content": "OOI是什么?",
                    "session_id": "conversation_1",
                    "agent_attachment_ids": ["attachment_1"],
                }
            )
        )
    finally:
        monkeypatch.undo()

    assert result["content"] == "OOI answer"
    create_request = client.requests[0]
    assert create_request.uri == "/open-apis/aily/v1/agents/:agent_id/chats"
    assert create_request.paths == {"agent_id": "agent_test"}
    assert create_request.token_types == {"tenant"}
    assert create_request.headers["Accept"] == "application/json"
    assert create_request.body == {
        "user_message": {
            "content": [{"type": "text", "text": "OOI是什么?"}],
            "agent_attachment_ids": ["attachment_1"],
        },
        "stream": False,
        "session_id": "conversation_1",
    }
    get_request = client.requests[1]
    assert get_request.uri == "/open-apis/aily/v1/agents/:agent_id/chats/:agent_chat_id"
    assert get_request.paths == {"agent_id": "agent_test", "agent_chat_id": "chat_1"}


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("content", "x" * 10_001),
        ("session_id", "x" * 256),
        ("agent_attachment_ids", ["x"] * 9),
    ],
)
def test_handler_rejects_agent_limits(field, value, monkeypatch):
    monkeypatch.setenv("FEISHU_AILY_AGENT_ID", "agent_test")
    result = json.loads(agent._handle_feishu_aily_agent_chat({"content": "ok", field: value}))
    assert "error" in result


def test_handler_requires_agent_id_without_network(monkeypatch):
    called = False

    def fail():
        nonlocal called
        called = True
        raise AssertionError("client must not be resolved without agent id")

    monkeypatch.setattr(agent, "_resolve_client", fail)
    result = json.loads(agent._handle_feishu_aily_agent_chat({"content": "OOI是什么?"}))
    assert "FEISHU_AILY_AGENT_ID" in result["error"]
    assert called is False


def test_agent_toolset_is_explicitly_scoped_and_default_off():
    configurable = {name for name, _label, _description in CONFIGURABLE_TOOLSETS}
    assert "feishu_aily_agent" in configurable
    assert "feishu_aily_agent" in _DEFAULT_OFF_TOOLSETS
    assert _toolset_allowed_for_platform("feishu_aily_agent", "cli")
    assert _toolset_allowed_for_platform("feishu_aily_agent", "feishu")
    assert not _toolset_allowed_for_platform("feishu_aily_agent", "telegram")
    assert "feishu_aily_agent_chat" in resolve_toolset("feishu_aily_agent")
    assert "feishu_aily_agent" not in _get_platform_tools(
        {}, "cli", include_default_mcp_servers=False
    )
