"""Behavior tests for the Feishu Aily knowledge-QA tool."""

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
from tools import feishu_aily_knowledge_tool as aily


_AILY_ENV_VARS = (
    "FEISHU_AILY_AUTH_APP_ID",
    "FEISHU_AILY_AUTH_APP_SECRET",
    "FEISHU_AILY_TARGET_APP_ID",
    "FEISHU_AILY_DOMAIN",
)


class _FakeClient:
    def __init__(self, response):
        self.response = response
        self.requests = []

    def request(self, request):
        self.requests.append(request)
        return self.response


def _response(content, *, code=0, msg="", status_code=None, headers=None):
    return SimpleNamespace(
        code=code,
        msg=msg,
        raw=SimpleNamespace(
            content=content,
            status_code=status_code,
            headers=headers or {},
        ),
    )


def _sse(*payloads):
    return "\n\n".join(
        f"data: {json.dumps(payload, ensure_ascii=False)}" for payload in payloads
    ).encode("utf-8")


@pytest.fixture(autouse=True)
def _clean_aily_state(monkeypatch):
    for name in _AILY_ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    client_token = aily.set_client(None)
    aily._reset_configured_client_cache()
    yield
    aily._reset_configured_client_cache()
    aily.reset_client(client_token)


@pytest.fixture
def fake_lark_sdk(monkeypatch):
    """Provide the SDK surface used by the tool without requiring the extra."""
    state = {}

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

    class ClientBuilder:
        def __init__(self):
            self.values = {}

        def _set(self, name, value):
            self.values[name] = value
            return self

        def app_id(self, value):
            return self._set("app_id", value)

        def app_secret(self, value):
            return self._set("app_secret", value)

        def domain(self, value):
            return self._set("domain", value)

        def log_level(self, value):
            return self._set("log_level", value)

        def timeout(self, value):
            return self._set("timeout", value)

        def build(self):
            client = SimpleNamespace(_config=SimpleNamespace(**self.values))
            state["client"] = client
            return client

    lark = ModuleType("lark_oapi")
    lark.AccessTokenType = SimpleNamespace(TENANT="tenant")
    lark.Client = SimpleNamespace(builder=ClientBuilder)
    lark.LogLevel = SimpleNamespace(WARNING="warning")
    const = ModuleType("lark_oapi.core.const")
    const.FEISHU_DOMAIN = "https://open.feishu.cn"
    const.LARK_DOMAIN = "https://open.larksuite.com"
    enum = ModuleType("lark_oapi.core.enum")
    enum.HttpMethod = SimpleNamespace(POST="POST")
    base_request = ModuleType("lark_oapi.core.model.base_request")
    base_request.BaseRequest = BaseRequest

    for name, module in {
        "lark_oapi": lark,
        "lark_oapi.core.const": const,
        "lark_oapi.core.enum": enum,
        "lark_oapi.core.model.base_request": base_request,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)
    return state


def test_sse_parser_returns_only_finished_answer():
    raw = _sse(
        {"status": "processing", "message": {"content": "partial"}},
        {
            "status": "finished",
            "finish_type": "qa",
            "has_answer": True,
            "message": {"content": "OOI is the configured answer."},
            "process_data": {"chunks": ["large internal evidence"]},
        },
    )

    result = json.loads(aily._parse_sse_response(raw))

    assert result == {
        "success": True,
        "content": "OOI is the configured answer.",
        "has_answer": True,
        "grounded": True,
        "answer_available": True,
        "finish_type": "qa",
    }


def test_sse_parser_fails_closed_on_processing_only_stream():
    result = json.loads(
        aily._parse_sse_response(
            _sse({"status": "processing", "message": {"content": "partial"}})
        )
    )

    assert "stream ended before a finished event" in result["error"]


def test_sse_parser_surfaces_embedded_error_even_if_stream_finishes():
    result = json.loads(
        aily._parse_sse_response(
            _sse(
                {"code": 2700034, "msg": "forbidden"},
                {
                    "status": "finished",
                    "message": {"content": "must not be returned"},
                },
            )
        )
    )

    assert result["code"] == 2700034
    assert "ask is forbidden" in result["error"]


def test_sse_parser_uses_faq_answer_when_message_is_empty():
    result = json.loads(
        aily._parse_sse_response(
            _sse(
                {
                    "status": "finished",
                    "finish_type": "faq",
                    "has_answer": True,
                    "message": {"content": ""},
                    "faq_result": {"question": "What is OOI?", "answer": "FAQ answer"},
                }
            )
        )
    )

    assert result["content"] == "FAQ answer"
    assert result["matched_question"] == "What is OOI?"


def test_sse_parser_handles_comments_crlf_and_multiline_data_event():
    raw = (
        b": keep-alive\r\n"
        b"event: message\r\n"
        b'data: {"status":"finished",\r\n'
        b'data: "finish_type":"qa",\r\n'
        b'data: "has_answer":true,\r\n'
        b'data: "message":{"content":"multiline"}}\r\n\r\n'
    )

    result = json.loads(aily._parse_sse_response(raw))

    assert result["success"] is True
    assert result["content"] == "multiline"


def test_sse_parser_does_not_promote_processing_snapshot_to_final_answer():
    result = json.loads(
        aily._parse_sse_response(
            _sse(
                {"status": "processing", "message": {"content": "first"}},
                {"status": "processing", "message": {"content": "complete"}},
                {
                    "status": "finished",
                    "finish_type": "qa",
                    "has_answer": True,
                    "message": {"content": ""},
                },
            )
        )
    )

    assert result["content"] == ""
    assert result["message"] == "No answer content returned"


def test_sse_parser_does_not_promote_processing_when_has_answer_is_false():
    result = json.loads(
        aily._parse_sse_response(
            _sse(
                {"status": "processing", "message": {"content": "partial"}},
                {
                    "status": "finished",
                    "finish_type": "qa",
                    "has_answer": False,
                    "message": {"content": ""},
                },
            )
        )
    )

    assert result["success"] is True
    assert result["has_answer"] is False
    assert result["content"] == ""


def test_sse_parser_preserves_nonempty_content_when_has_answer_is_false():
    result = json.loads(
        aily._parse_sse_response(
            _sse(
                {
                    "status": "finished",
                    "finish_type": "qa",
                    "has_answer": False,
                    "message": {"content": "ungrounded fallback"},
                }
            )
        )
    )

    assert result["success"] is True
    assert result["has_answer"] is False
    assert result["grounded"] is False
    assert result["answer_available"] is True
    assert result["content"] == "ungrounded fallback"


@pytest.mark.parametrize("has_answer", ["false", 0, None])
def test_sse_parser_rejects_non_boolean_has_answer(has_answer):
    result = json.loads(
        aily._parse_sse_response(
            _sse(
                {
                    "status": "finished",
                    "finish_type": "qa",
                    "has_answer": has_answer,
                    "message": {"content": "must not be trusted"},
                }
            )
        )
    )

    assert "error" in result
    assert "invalid has_answer" in result["error"]


@pytest.mark.parametrize("finish_type", [None, "", "other"])
def test_sse_parser_rejects_missing_or_unknown_finish_type(finish_type):
    result = json.loads(
        aily._parse_sse_response(
            _sse(
                {
                    "status": "finished",
                    "finish_type": finish_type,
                    "has_answer": True,
                    "message": {"content": "must not be trusted"},
                }
            )
        )
    )

    assert "error" in result
    assert "invalid finish_type" in result["error"]


def test_sse_parser_fails_closed_when_stream_limits_are_exceeded():
    with pytest.raises(aily.SSEProtocolError, match="event exceeded"):
        list(
            aily.iter_sse_payloads(
                [b"data: " + (b"x" * 32) + b"\n\n"],
                max_event_bytes=8,
                max_total_bytes=128,
            )
        )

    with pytest.raises(aily.SSEProtocolError, match="event count exceeded"):
        list(
            aily.iter_sse_payloads(
                [b"data: {}\n", b"\n", b"data: {}\n", b"\n"],
                max_events=1,
            )
        )


def test_sse_parser_prefers_finished_faq_over_processing_snapshot():
    result = json.loads(
        aily._parse_sse_response(
            _sse(
                {"status": "processing", "message": {"content": "partial"}},
                {
                    "status": "finished",
                    "finish_type": "faq",
                    "has_answer": True,
                    "message": {"content": ""},
                    "faq_result": {
                        "question": "What is OOI?",
                        "answer": "authoritative FAQ",
                    },
                },
            )
        )
    )

    assert result["content"] == "authoritative FAQ"


def test_handler_builds_json_request_from_operator_target(monkeypatch, fake_lark_sdk):
    finished = _sse(
        {
            "status": "finished",
            "finish_type": "qa",
            "has_answer": True,
            "message": {"content": "answer"},
        }
    )
    client = _FakeClient(_response(finished))
    monkeypatch.setenv("FEISHU_AILY_TARGET_APP_ID", "spring_target")
    aily.set_client(client)

    result = json.loads(
        aily._handle_feishu_aily_knowledge_ask(
            {
                "content": " OOI是什么？ ",
                "data_asset_ids": [" asset_1 ", ""],
                "data_asset_tag_ids": ["tag_1"],
            }
        )
    )

    assert result["content"] == "answer"
    request = client.requests[0]
    assert request.paths == {"app_id": "spring_target"}
    assert request.headers == {
        "Accept": "text/event-stream",
        "Content-Type": "application/json; charset=utf-8",
    }
    assert request.body == {
        "message": {"content": "OOI是什么？"},
        "data_asset_ids": ["asset_1"],
        "data_asset_tag_ids": ["tag_1"],
    }


def test_handler_surfaces_regular_json_api_error(monkeypatch, fake_lark_sdk):
    client = _FakeClient(
        _response(
            json.dumps({"code": 2320008, "msg": "Aily app is not published"}).encode(),
            code=0,
        )
    )
    monkeypatch.setenv("FEISHU_AILY_TARGET_APP_ID", "spring_target")
    aily.set_client(client)

    result = json.loads(aily._handle_feishu_aily_knowledge_ask({"content": "OOI是什么？"}))

    assert result["code"] == 2320008
    assert "Aily app is not published" in result["error"]


def test_handler_surfaces_safe_http_rate_limit_diagnostics(monkeypatch, fake_lark_sdk):
    client = _FakeClient(
        _response(
            json.dumps({"code": 2700033, "msg": "limited"}).encode(),
            status_code=429,
            headers={
                "X-Tt-Logid": "log-safe-123",
                "X-Ogw-Ratelimit-Reset": "17",
            },
        )
    )
    monkeypatch.setenv("FEISHU_AILY_TARGET_APP_ID", "spring_target")
    aily.set_client(client)

    result = json.loads(aily._handle_feishu_aily_knowledge_ask({"content": "question"}))

    assert result["code"] == 2700033
    assert "http_status=429" in result["error"]
    assert "log_id=log-safe-123" in result["error"]
    assert "rate_limit_reset=17" in result["error"]


def test_handler_fails_closed_on_non_2xx_without_a_business_code(monkeypatch, fake_lark_sdk):
    client = _FakeClient(
        _response(
            b"upstream unavailable",
            status_code=400,
            headers={"X-Tt-Logid": "log-400"},
        )
    )
    monkeypatch.setenv("FEISHU_AILY_TARGET_APP_ID", "spring_target")
    aily.set_client(client)

    result = json.loads(aily._handle_feishu_aily_knowledge_ask({"content": "question"}))

    assert result["code"] == 400
    assert "http_status=400" in result["error"]
    assert "log_id=log-400" in result["error"]


def test_handler_redacts_configured_secret_from_transport_error(monkeypatch, fake_lark_sdk):
    secret = "opaque-test-secret-value"
    client = _FakeClient(_response(b""))
    monkeypatch.setenv("FEISHU_AILY_TARGET_APP_ID", "spring_target")
    monkeypatch.setenv("FEISHU_AILY_AUTH_APP_SECRET", secret)
    monkeypatch.setattr(aily, "_resolve_client", lambda: client)

    def fail(_request):
        raise RuntimeError(f"transport echoed {secret}")

    client.request = fail
    result = json.loads(
        aily._handle_feishu_aily_knowledge_ask({"content": "question"})
    )

    assert secret not in result["error"]
    assert "redacted-secret" in result["error"]


def test_dedicated_client_takes_precedence_over_injected_client(monkeypatch, fake_lark_sdk):
    finished = _sse(
        {
            "status": "finished",
            "finish_type": "qa",
            "has_answer": True,
            "message": {"content": "dedicated"},
        }
    )
    dedicated = _FakeClient(_response(finished))
    injected = _FakeClient(_response(finished))
    monkeypatch.setenv("FEISHU_AILY_TARGET_APP_ID", "spring_target")
    monkeypatch.setattr(aily, "_build_configured_client", lambda: dedicated)
    aily.set_client(injected)

    result = json.loads(aily._handle_feishu_aily_knowledge_ask({"content": "question"}))

    assert result["content"] == "dedicated"
    assert len(dedicated.requests) == 1
    assert injected.requests == []


def test_partial_dedicated_credentials_fail_instead_of_falling_back(monkeypatch):
    monkeypatch.setenv("FEISHU_AILY_TARGET_APP_ID", "spring_target")
    monkeypatch.setenv("FEISHU_AILY_AUTH_APP_ID", "cli_auth")
    aily.set_client(_FakeClient(_response(b"")))

    result = json.loads(aily._handle_feishu_aily_knowledge_ask({"content": "question"}))

    assert "FEISHU_AILY_AUTH_APP_SECRET is required" in result["error"]


def test_dedicated_env_builds_and_reuses_sdk_client(monkeypatch, fake_lark_sdk):
    monkeypatch.setenv("FEISHU_AILY_AUTH_APP_ID", "cli_auth")
    monkeypatch.setenv("FEISHU_AILY_AUTH_APP_SECRET", "test_secret")
    monkeypatch.setenv("FEISHU_AILY_DOMAIN", "feishu")

    first = aily._build_configured_client()
    second = aily._build_configured_client()

    assert first is second
    assert first._config.app_id == "cli_auth"
    assert first._config.app_secret == "test_secret"
    assert first._config.domain == "https://open.feishu.cn"
    assert first._config.timeout == 120


def test_env_reads_from_active_profile_secret_scope(monkeypatch):
    from agent import secret_scope

    monkeypatch.setenv("FEISHU_AILY_TARGET_APP_ID", "spring_wrong_global")
    previous_mode = secret_scope.is_multiplex_active()
    secret_scope.set_multiplex_active(True)
    token = secret_scope.set_secret_scope(
        {"FEISHU_AILY_TARGET_APP_ID": "spring_profile_target"}
    )
    try:
        assert aily._env("FEISHU_AILY_TARGET_APP_ID") == "spring_profile_target"
    finally:
        secret_scope.reset_secret_scope(token)
        secret_scope.set_multiplex_active(previous_mode)


def test_unscoped_multiplex_call_fails_closed(monkeypatch):
    from agent import secret_scope

    monkeypatch.setenv("FEISHU_AILY_TARGET_APP_ID", "spring_wrong_global")
    previous_mode = secret_scope.is_multiplex_active()
    secret_scope.set_multiplex_active(True)
    try:
        result = json.loads(
            aily._handle_feishu_aily_knowledge_ask({"content": "question"})
        )
    finally:
        secret_scope.set_multiplex_active(previous_mode)

    assert "no profile secret scope" in result["error"]


def test_target_app_id_is_not_a_model_parameter():
    parameters = aily.FEISHU_AILY_KNOWLEDGE_ASK_SCHEMA["parameters"]

    assert parameters["required"] == ["content"]
    assert "app_id" not in parameters["properties"]


def test_aily_is_independent_of_default_platform_bundles():
    tool_name = "feishu_aily_knowledge_ask"
    assert tool_name in resolve_toolset("feishu_aily")
    assert tool_name not in resolve_toolset("hermes-feishu")
    assert tool_name not in resolve_toolset("hermes-cli")


def test_aily_toolset_is_manageable_scoped_and_default_off():
    configurable = {name for name, _label, _description in CONFIGURABLE_TOOLSETS}
    assert "feishu_aily" in configurable
    assert "feishu_aily" in _DEFAULT_OFF_TOOLSETS
    assert _toolset_allowed_for_platform("feishu_aily", "cli")
    assert _toolset_allowed_for_platform("feishu_aily", "feishu")
    assert not _toolset_allowed_for_platform("feishu_aily", "telegram")

    assert "feishu_aily" not in _get_platform_tools(
        {}, "cli", include_default_mcp_servers=False
    )
    assert "feishu_aily" not in _get_platform_tools(
        {}, "feishu", include_default_mcp_servers=False
    )


@pytest.mark.parametrize("platform", ["cli", "feishu"])
def test_explicit_platform_config_enables_aily_toolset(platform):
    config = {"platform_toolsets": {platform: ["feishu_aily"]}}

    assert "feishu_aily" in _get_platform_tools(
        config, platform, include_default_mcp_servers=False
    )


@pytest.mark.parametrize(
    "toolsets", [["hermes-feishu"], ["hermes-feishu", "web"]]
)
def test_feishu_composite_does_not_implicitly_enable_aily(toolsets):
    config = {"platform_toolsets": {"feishu": toolsets}}

    assert "feishu_aily" not in _get_platform_tools(
        config, "feishu", include_default_mcp_servers=False
    )


def test_injected_client_context_propagates_and_resets():
    import concurrent.futures

    from tools.thread_context import propagate_context_to_thread

    original = object()
    injected = object()
    outer_token = aily.set_client(original)
    token = aily.set_client(injected)
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            observed = executor.submit(
                propagate_context_to_thread(aily.get_client)
            ).result()
        assert observed is injected
    finally:
        aily.reset_client(token)
    try:
        assert aily.get_client() is original
    finally:
        aily.reset_client(outer_token)


def test_aily_secret_is_registered_as_protected_env():
    from hermes_cli.config import OPTIONAL_ENV_VARS
    from tools.environments.local import _HERMES_PROVIDER_ENV_BLOCKLIST

    secret_name = "FEISHU_AILY_AUTH_APP_SECRET"
    assert OPTIONAL_ENV_VARS[secret_name]["password"] is True
    assert secret_name in _HERMES_PROVIDER_ENV_BLOCKLIST
