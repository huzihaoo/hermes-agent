"""Chat with a Feishu Aily Agent and its configured tools/MCP services.

This is deliberately separate from ``feishu_aily_knowledge_tool``.  An Aily
Agent is addressed by an ``agent_...`` id and uses the Agent Chat contract;
the legacy data-knowledge API is addressed by a ``spring_...`` app id and has
a different request and response schema.
"""

from __future__ import annotations

import importlib.util
import json
import logging
import time
from typing import Any

from tools.registry import registry, tool_error, tool_result

# Reuse the existing, scoped credential/client boundary and optional SSE
# framing code. Keeping this in one place is important: comment-context client
# injection and secret redaction must behave identically for both API families.
from tools.feishu_aily_knowledge_tool import (  # noqa: E402
    SSEProtocolError,
    _build_configured_client,
    _env,
    _parse_json_response,
    _reset_configured_client_cache,
    _raw_content,
    _raw_header,
    _raw_status_code,
    _safe_error_text,
    get_client,
    iter_sse_payloads,
    reset_client,
    set_client,
)

logger = logging.getLogger(__name__)

_AGENT_ID_ENV = "FEISHU_AILY_AGENT_ID"
_AGENT_CHAT_URI = "/open-apis/aily/v1/agents/:agent_id/chats"
_AGENT_CHAT_RESULT_URI = "/open-apis/aily/v1/agents/:agent_id/chats/:agent_chat_id"
_MAX_AGENT_ID_CHARS = 65
_MAX_MESSAGE_CHARS = 10_000
_MAX_SESSION_ID_CHARS = 255
_MAX_CHAT_ID_CHARS = 64
_MAX_ATTACHMENTS = 8
_MAX_ATTACHMENT_ID_CHARS = 255
_MAX_SSE_BYTES = 8 * 1024 * 1024
_MAX_ARTIFACTS = 32
_MAX_ARTIFACT_ID_CHARS = 255
_MAX_ARTIFACT_TYPE_CHARS = 64
_POLL_INTERVAL_SECONDS = 0.5
_POLL_TIMEOUT_SECONDS = 120.0
_MAX_POLL_REQUESTS = 120


FEISHU_AILY_AGENT_CHAT_SCHEMA = {
    "name": "feishu_aily_agent_chat",
    "description": (
        "与指定的飞书 Aily 智能体对话。智能体在 Aily 后台配置的知识库、"
        "MCP 服务和其他工具会由 Aily 执行；返回完成后的文本答案。"
        "Agent ID 来自 aily.feishu.cn 智能体详情地址栏（agent_...），"
        "不是调用方 cli_... 应用 ID，也不是旧知识应用的 spring_... ID。"
        "当前实现使用 aily:agent_chat:write 创建并用 aily:agent_chat:read 轮询完成结果。"
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "content": {
                "type": "string",
                "description": "发给智能体的文本问题，例如 'OOI是什么？'。",
            },
            "session_id": {
                "type": "string",
                "description": "可选。复用 Aily Agent 会话 ID 以保持多轮上下文。",
            },
            "agent_attachment_ids": {
                "type": "array",
                "items": {"type": "string"},
                "description": "可选。已通过 Aily 附件 API 上传的附件 ID，最多 8 个。",
            },
        },
        "required": ["content"],
    },
}


def _check_feishu() -> bool:
    try:
        return importlib.util.find_spec("lark_oapi") is not None
    except (ImportError, ValueError):
        return False


def _resolve_client() -> Any | None:
    configured = _build_configured_client()
    return configured if configured is not None else get_client()


def _validate_attachments(value: Any) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError("agent_attachment_ids must be an array of strings")
    if len(value) > _MAX_ATTACHMENTS:
        raise ValueError(f"agent_attachment_ids must contain at most {_MAX_ATTACHMENTS} items")
    result: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise ValueError("agent_attachment_ids must contain non-empty strings")
        item = item.strip()
        if len(item) > _MAX_ATTACHMENT_ID_CHARS:
            raise ValueError(
                f"agent_attachment_ids entries must be at most {_MAX_ATTACHMENT_ID_CHARS} characters"
            )
        result.append(item)
    return result


def _extract_text(value: Any) -> str:
    """Extract text content from Agent response message/content shapes."""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        parts = [_extract_text(item) for item in value]
        return "".join(part for part in parts if part)
    if not isinstance(value, dict):
        return ""
    direct = value.get("text")
    if isinstance(direct, str):
        return direct
    for key in ("content", "message", "answer", "data"):
        if key in value:
            text = _extract_text(value[key])
            if text:
                return text
    return ""


def _response_containers(payload: dict[str, Any]) -> list[dict[str, Any]]:
    containers = [payload]
    data = payload.get("data")
    if isinstance(data, dict):
        containers.insert(0, data)
    return containers


def _extract_payload_text(payload: dict[str, Any]) -> str:
    for container in _response_containers(payload):
        for key in ("content", "message", "answer"):
            if key in container:
                text = _extract_text(container[key])
                if text:
                    return text
    return ""


def _extract_payload_status(payload: dict[str, Any]) -> tuple[str | None, str | None]:
    for container in _response_containers(payload):
        status = container.get("status")
        finish_reason = container.get("finish_reason")
        if isinstance(status, str) or isinstance(finish_reason, str):
            return (
                status if isinstance(status, str) else None,
                finish_reason if isinstance(finish_reason, str) else None,
            )
    return None, None


def _is_terminal_payload(payload: dict[str, Any], *, event_name: str | None = None) -> bool:
    status, finish_reason = _extract_payload_status(payload)
    if isinstance(finish_reason, str) and finish_reason.strip():
        return True
    if isinstance(status, str) and status.lower() in {
        "finished",
        "complete",
        "completed",
        "done",
        "success",
        "succeeded",
    }:
        return True
    return isinstance(event_name, str) and event_name.lower() in {
        "done",
        "finish",
        "finished",
        "complete",
    }


def _is_failed_payload(payload: dict[str, Any]) -> bool:
    status, reason = _extract_payload_status(payload)
    values = [value.lower() for value in (status, reason) if isinstance(value, str)]
    # Aily's documented response example uses status="Cancelled" together
    # with finish_reason="stop" for a successful answer.  Treat cancellation
    # as failure only when it is itself the terminal reason.
    if any(value in {"failed", "failure", "error"} for value in values):
        return True
    return any(value in {"cancelled", "canceled"} for value in values) and not (
        isinstance(reason, str) and reason.lower() not in {"cancelled", "canceled"}
    )


def _payload_code_msg(payload: dict[str, Any]) -> tuple[Any, Any]:
    for container in _response_containers(payload):
        if "code" in container:
            return container.get("code"), container.get("msg", "")
    return 0, ""


def _bounded_artifacts(payload: dict[str, Any]) -> list[dict[str, str]]:
    """Keep only non-sensitive artifact identifiers/types from Agent content."""
    artifacts: list[dict[str, str]] = []
    for container in _response_containers(payload):
        content = container.get("content")
        if not isinstance(content, list):
            continue
        for item in content:
            if not isinstance(item, dict):
                continue
            artifact_id = item.get("agent_artifact_id")
            artifact_type = item.get("artifact_type")
            if not isinstance(artifact_id, str) or not artifact_id:
                continue
            entry = {"agent_artifact_id": artifact_id[:_MAX_ARTIFACT_ID_CHARS]}
            if isinstance(artifact_type, str) and artifact_type:
                entry["artifact_type"] = artifact_type[:_MAX_ARTIFACT_TYPE_CHARS]
            if entry not in artifacts:
                artifacts.append(entry)
            if len(artifacts) >= _MAX_ARTIFACTS:
                return artifacts
    return artifacts


def _merge_stream_text(current: str, incoming: str) -> str:
    """Accept either cumulative snapshots or token-like SSE fragments."""
    if not incoming:
        return current
    if not current:
        return incoming
    if incoming.startswith(current):
        return incoming
    if current.startswith(incoming) or current.endswith(incoming):
        return current
    return current + incoming


def _agent_result(
    content: str,
    *,
    session_id: str | None = None,
    agent_chat_id: str | None = None,
    artifacts: list[dict[str, str]] | None = None,
    finish_reason: str | None = None,
) -> str:
    result: dict[str, Any] = {
        "success": True,
        "content": content,
        "answer_available": bool(content),
    }
    if session_id:
        result["session_id"] = session_id
    if agent_chat_id:
        result["agent_chat_id"] = agent_chat_id
    if finish_reason:
        result["finish_reason"] = finish_reason
    if artifacts:
        result["artifacts"] = artifacts
    if not content:
        result["message"] = "Agent completed without text content"
    return tool_result(result)


def _parse_agent_response(raw_content: bytes | str) -> str:
    """Parse a JSON or SSE Agent Chat response, requiring a terminal event."""
    if isinstance(raw_content, bytes):
        if len(raw_content) > _MAX_SSE_BYTES:
            return tool_error("Aily Agent response exceeded the configured byte limit")
        text = raw_content.decode("utf-8", errors="replace")
    else:
        text = str(raw_content)
        if len(text.encode("utf-8", errors="replace")) > _MAX_SSE_BYTES:
            return tool_error("Aily Agent response exceeded the configured byte limit")

    parsed = _parse_json_response(raw_content)
    if parsed is not None and "code" in parsed:
        code = parsed.get("code", 0)
        msg = parsed.get("msg", "")
        if code not in (None, 0):
            return tool_error(
                f"Aily Agent chat failed: [{code}] {_safe_error_text(msg or 'unknown error')}",
                code=code,
            )
        if _is_failed_payload(parsed):
            status, reason = _extract_payload_status(parsed)
            return tool_error(
                "Aily Agent chat did not complete: "
                + _safe_error_text(reason or status or "failed")
            )
        data = parsed.get("data") if isinstance(parsed.get("data"), dict) else {}
        content = _extract_payload_text(parsed)
        if content or _is_terminal_payload(parsed):
            return _agent_result(
                content,
                session_id=data.get("session_id") if isinstance(data.get("session_id"), str) else None,
                agent_chat_id=data.get("agent_chat_id") if isinstance(data.get("agent_chat_id"), str) else None,
                artifacts=_bounded_artifacts(parsed),
                finish_reason=data.get("finish_reason") if isinstance(data.get("finish_reason"), str) else None,
            )
        return tool_error("Aily Agent chat returned no completed content")

    saw_event = False
    finished = False
    latest_text = ""
    latest_artifacts: list[dict[str, str]] = []
    finish_reason: str | None = None
    session_id: str | None = None
    agent_chat_id: str | None = None
    try:
        for payload in iter_sse_payloads(text.splitlines(keepends=True)):
            saw_event = True
            code, msg = _payload_code_msg(payload)
            if code not in (None, 0):
                return tool_error(
                    f"Aily Agent chat failed: [{code}] {_safe_error_text(msg or 'unknown error')}",
                    code=code,
                )
            if _is_failed_payload(payload):
                status, reason = _extract_payload_status(payload)
                return tool_error(
                    "Aily Agent chat did not complete: "
                    + _safe_error_text(reason or status or "failed")
                )
            data = payload.get("data")
            if isinstance(data, dict):
                if isinstance(data.get("session_id"), str):
                    session_id = data["session_id"]
                if isinstance(data.get("agent_chat_id"), str):
                    agent_chat_id = data["agent_chat_id"]
            incoming = _extract_payload_text(payload)
            latest_text = _merge_stream_text(latest_text, incoming)
            for artifact in _bounded_artifacts(payload):
                if artifact not in latest_artifacts:
                    latest_artifacts.append(artifact)
            _status, reason = _extract_payload_status(payload)
            if reason:
                finish_reason = reason
            if _is_terminal_payload(payload):
                finished = True
    except SSEProtocolError as exc:
        return tool_error(f"Aily Agent chat protocol error: {exc}")

    if not finished:
        detail = "stream ended before a completed event" if saw_event else "no valid SSE events"
        return tool_error(f"Aily Agent chat: {detail}")
    return _agent_result(
        latest_text,
        session_id=session_id,
        agent_chat_id=agent_chat_id,
        artifacts=latest_artifacts,
        finish_reason=finish_reason,
    )


def _poll_agent_chat(
    client: Any,
    *,
    agent_id: str,
    agent_chat_id: str,
    session_id: str | None,
    access_token_type: Any,
    http_method: Any,
    base_request: Any,
) -> str:
    """Poll the documented JSON result endpoint until the Agent finishes."""
    deadline = time.monotonic() + _POLL_TIMEOUT_SECONDS
    poll_count = 0
    while True:
        poll_count += 1
        if poll_count > _MAX_POLL_REQUESTS:
            return tool_error("Aily Agent chat exceeded the poll request limit")
        request = (
            base_request.builder()
            .http_method(http_method.GET)
            .uri(_AGENT_CHAT_RESULT_URI)
            .token_types({access_token_type.TENANT})
            .paths({"agent_id": agent_id, "agent_chat_id": agent_chat_id})
            .headers({"Accept": "application/json"})
            .build()
        )
        try:
            response = client.request(request)
        except Exception as exc:
            safe_error = _safe_error_text(exc)
            logger.warning("feishu_aily_agent_chat poll failed: %s", safe_error)
            return tool_error(f"Aily Agent chat poll failed: {safe_error}")
        raw = _raw_content(response)
        status_code = _raw_status_code(response)
        payload = _parse_json_response(raw)
        code = payload.get("code") if payload else getattr(response, "code", None)
        msg = payload.get("msg") if payload else getattr(response, "msg", None)
        if status_code is not None and not 200 <= status_code < 300 and code in (None, 0):
            code = status_code
            msg = msg or f"HTTP {status_code}"
        if code not in (None, 0):
            details = [f"code={code}", f"msg={_safe_error_text(msg or 'unknown error')}"]
            if status_code is not None:
                details.insert(0, f"http_status={status_code}")
            log_id = _raw_header(response, "x-tt-logid")
            if log_id:
                details.append(f"log_id={_safe_error_text(log_id)}")
            return tool_error("Aily Agent chat poll failed: " + " ".join(details), code=code)
        if payload is None:
            return tool_error("Aily Agent chat poll returned non-JSON content")
        data = payload.get("data")
        if not isinstance(data, dict):
            return tool_error("Aily Agent chat poll returned no data")
        wrapped = {"data": data}
        if _is_failed_payload(wrapped):
            _status, reason = _extract_payload_status(wrapped)
            return tool_error(
                "Aily Agent chat did not complete: "
                + _safe_error_text(reason or _status or "failed")
            )
        if _is_terminal_payload(wrapped):
            status, reason = _extract_payload_status(wrapped)
            content = _extract_payload_text(wrapped)
            return _agent_result(
                content,
                session_id=session_id,
                agent_chat_id=agent_chat_id,
                artifacts=_bounded_artifacts(wrapped),
                finish_reason=reason,
            )
        if time.monotonic() >= deadline:
            return tool_error("Aily Agent chat timed out before a completed result")
        time.sleep(_POLL_INTERVAL_SECONDS)


def _handle_feishu_aily_agent_chat(args: dict, **kwargs) -> str:
    raw_content = args.get("content")
    if not isinstance(raw_content, str):
        return tool_error("content must be a string")
    content = raw_content.strip()
    if not content:
        return tool_error("content is required")
    if len(content) > _MAX_MESSAGE_CHARS:
        return tool_error(f"content must be at most {_MAX_MESSAGE_CHARS} characters")

    try:
        agent_id = _env(_AGENT_ID_ENV)
    except RuntimeError as exc:
        return tool_error(f"Aily Agent configuration error: {_safe_error_text(exc)}")
    if not agent_id:
        return tool_error(
            f"Aily Agent is not configured; set {_AGENT_ID_ENV} to the agent_... ID "
            "from the Aily agent detail URL"
        )
    if len(agent_id) > _MAX_AGENT_ID_CHARS:
        return tool_error(f"{_AGENT_ID_ENV} must be at most {_MAX_AGENT_ID_CHARS} characters")

    session_id = args.get("session_id")
    if session_id is not None:
        if not isinstance(session_id, str) or not session_id.strip():
            return tool_error("session_id must be a non-empty string when provided")
        session_id = session_id.strip()
        if len(session_id) > _MAX_SESSION_ID_CHARS:
            return tool_error(
                f"session_id must be at most {_MAX_SESSION_ID_CHARS} characters"
            )
    try:
        attachments = _validate_attachments(args.get("agent_attachment_ids"))
        client = _resolve_client()
    except (RuntimeError, ValueError) as exc:
        return tool_error(f"Aily Agent configuration error: {_safe_error_text(exc)}")
    if client is None:
        return tool_error(
            "Aily client is not configured; set both "
            "FEISHU_AILY_AUTH_APP_ID and FEISHU_AILY_AUTH_APP_SECRET, or call from a Feishu context"
        )

    try:
        from lark_oapi import AccessTokenType
        from lark_oapi.core.enum import HttpMethod
        from lark_oapi.core.model.base_request import BaseRequest
    except ImportError:
        return tool_error("lark_oapi not installed")

    user_message: dict[str, Any] = {
        "content": [{"type": "text", "text": content}]
    }
    if attachments:
        user_message["agent_attachment_ids"] = attachments
    # The public Agent docs define the JSON create + GET result contract, but
    # do not define SSE event fields.  Use the documented non-stream path and
    # poll so MCP/tool execution is not coupled to an undocumented event shape.
    body: dict[str, Any] = {"user_message": user_message, "stream": False}
    if session_id:
        body["session_id"] = session_id
    request = (
        BaseRequest.builder()
        .http_method(HttpMethod.POST)
        .uri(_AGENT_CHAT_URI)
        .token_types({AccessTokenType.TENANT})
        .paths({"agent_id": agent_id})
        .headers(
            {
                "Accept": "application/json",
                "Content-Type": "application/json; charset=utf-8",
            }
        )
        .body(body)
        .build()
    )
    try:
        response = client.request(request)
    except Exception as exc:
        safe_error = _safe_error_text(exc)
        logger.warning("feishu_aily_agent_chat request failed: %s", safe_error)
        return tool_error(f"Aily Agent chat request failed: {safe_error}")

    raw = _raw_content(response)
    http_status = _raw_status_code(response)
    parsed_json = _parse_json_response(raw)
    code = getattr(response, "code", None)
    msg = getattr(response, "msg", None)
    if parsed_json:
        code = parsed_json.get("code", code)
        msg = parsed_json.get("msg", msg)
    if http_status is not None and not 200 <= http_status < 300 and code in (None, 0):
        code = http_status
        msg = msg or f"HTTP {http_status}"
    if code not in (None, 0):
        details = [f"code={code}", f"msg={_safe_error_text(msg or 'unknown error')}"]
        if http_status is not None:
            details.insert(0, f"http_status={http_status}")
        log_id = _raw_header(response, "x-tt-logid")
        if log_id:
            details.append(f"log_id={_safe_error_text(log_id)}")
        return tool_error("Aily Agent chat failed: " + " ".join(details), code=code)
    if raw is None:
        return tool_error("Aily Agent chat: no raw response content")
    if parsed_json is None:
        return tool_error("Aily Agent chat create returned non-JSON content")
    data = parsed_json.get("data")
    if not isinstance(data, dict):
        return tool_error("Aily Agent chat create returned no data")
    agent_chat_id = data.get("agent_chat_id")
    if not isinstance(agent_chat_id, str) or not agent_chat_id:
        return tool_error("Aily Agent chat create returned no agent_chat_id")
    if len(agent_chat_id) > _MAX_CHAT_ID_CHARS:
        return tool_error("Aily Agent chat returned an invalid agent_chat_id")
    created_session_id = data.get("session_id")
    if not isinstance(created_session_id, str):
        created_session_id = session_id
    return _poll_agent_chat(
        client,
        agent_id=agent_id,
        agent_chat_id=agent_chat_id,
        session_id=created_session_id,
        access_token_type=AccessTokenType,
        http_method=HttpMethod,
        base_request=BaseRequest,
    )


registry.register(
    name="feishu_aily_agent_chat",
    toolset="feishu_aily_agent",
    schema=FEISHU_AILY_AGENT_CHAT_SCHEMA,
    handler=_handle_feishu_aily_agent_chat,
    check_fn=_check_feishu,
    requires_env=[],
    is_async=False,
    description="与配置的飞书 Aily 智能体对话（包括其已配置的 MCP 服务）",
    emoji="Aily",
    max_result_size_chars=50_000,
)
