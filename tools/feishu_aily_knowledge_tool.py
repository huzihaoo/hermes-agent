"""Query a configured Feishu Aily knowledge app over its SSE API.

The target Aily app and optional dedicated auth app are operator config, not
model inputs. Feishu comment agents may inject their existing Lark client as a
fallback; CLI and normal gateway turns use the dedicated env-backed client.
"""

from __future__ import annotations

import importlib.util
import json
import logging
import threading
from contextvars import ContextVar, Token
from typing import Any

from tools.registry import registry, tool_error, tool_result

logger = logging.getLogger(__name__)

_AUTH_APP_ID_ENV = "FEISHU_AILY_AUTH_APP_ID"
_AUTH_APP_SECRET_ENV = "FEISHU_AILY_AUTH_APP_SECRET"
_TARGET_APP_ID_ENV = "FEISHU_AILY_TARGET_APP_ID"
_DOMAIN_ENV = "FEISHU_AILY_DOMAIN"
_KNOWLEDGE_ASK_URI = "/open-apis/aily/v1/apps/:app_id/knowledges/ask"
_AILY_API_TIMEOUT_SECONDS = 120

_injected_client: ContextVar[Any | None] = ContextVar(
    "feishu_aily_injected_client", default=None
)
_configured_client_lock = threading.Lock()
_configured_client: Any | None = None
_configured_client_identity: tuple[str, str, str] | None = None


def set_client(client: Any | None) -> Token:
    """Install a context-local Feishu fallback client and return its token."""
    return _injected_client.set(client)


def reset_client(token: Token) -> None:
    """Restore the fallback client that preceded ``set_client``."""
    _injected_client.reset(token)


def get_client() -> Any | None:
    """Return the Feishu-context fallback client for this execution context."""
    return _injected_client.get()


def _reset_configured_client_cache() -> None:
    """Clear the dedicated client cache (used after credential rotation/tests)."""
    global _configured_client, _configured_client_identity
    with _configured_client_lock:
        _configured_client = None
        _configured_client_identity = None


def _env(name: str) -> str:
    from agent.secret_scope import get_secret

    return (get_secret(name) or "").strip()


def _safe_error_text(value: Any) -> str:
    """Redact known credential material before errors reach logs or the model."""
    from agent.redact import redact_sensitive_text

    text = str(value)
    try:
        app_secret = _env(_AUTH_APP_SECRET_ENV)
    except RuntimeError:
        app_secret = ""
    if app_secret:
        text = text.replace(app_secret, "«redacted-secret»")
    return redact_sensitive_text(text, force=True)


def _build_configured_client() -> Any | None:
    """Build or reuse a client backed by the dedicated Aily auth app."""
    global _configured_client, _configured_client_identity

    app_id = _env(_AUTH_APP_ID_ENV)
    app_secret = _env(_AUTH_APP_SECRET_ENV)
    if not app_id and not app_secret:
        return None
    if not app_id or not app_secret:
        missing = _AUTH_APP_ID_ENV if not app_id else _AUTH_APP_SECRET_ENV
        raise ValueError(f"{missing} is required when dedicated Aily credentials are configured")

    domain_name = (_env(_DOMAIN_ENV) or _env("FEISHU_DOMAIN") or "feishu").lower()
    if domain_name not in {"feishu", "lark"}:
        raise ValueError(f"{_DOMAIN_ENV} must be 'feishu' or 'lark'")

    identity = (app_id, app_secret, domain_name)
    with _configured_client_lock:
        if _configured_client is not None and _configured_client_identity == identity:
            return _configured_client

        try:
            import lark_oapi as lark
            from lark_oapi.core.const import FEISHU_DOMAIN, LARK_DOMAIN
        except ImportError as exc:
            raise RuntimeError("lark_oapi not installed") from exc

        sdk_domain = LARK_DOMAIN if domain_name == "lark" else FEISHU_DOMAIN
        _configured_client = (
            lark.Client.builder()
            .app_id(app_id)
            .app_secret(app_secret)
            .domain(sdk_domain)
            .log_level(lark.LogLevel.WARNING)
            .timeout(_AILY_API_TIMEOUT_SECONDS)
            .build()
        )
        _configured_client_identity = identity
        return _configured_client


def _resolve_client() -> Any | None:
    configured = _build_configured_client()
    return configured if configured is not None else get_client()


FEISHU_AILY_KNOWLEDGE_ASK_SCHEMA = {
    "name": "feishu_aily_knowledge_ask",
    "description": (
        "向当前 Hermes 已配置的飞书 Aily 智能伙伴发起数据知识问答。"
        "可通过 data_asset_ids 或 data_asset_tag_ids 限定知识范围。"
        "使用 tenant_access_token 时无法查询以直连模式引入的飞书云文档。"
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "content": {
                "type": "string",
                "description": "要提问的纯文本问题，例如 'OOI是什么？'。",
            },
            "data_asset_ids": {
                "type": "array",
                "items": {"type": "string"},
                "description": "可选。限定数据知识 ID；省略时使用应用配置的全部数据知识。",
            },
            "data_asset_tag_ids": {
                "type": "array",
                "items": {"type": "string"},
                "description": "可选。限定数据知识分类 ID。",
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


def _validated_string_list(args: dict, name: str) -> list[str] | None:
    value = args.get(name)
    if value is None:
        return None
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise ValueError(f"{name} must be an array of strings")
    if len(value) > 65_535:
        raise ValueError(f"{name} must contain at most 65535 items")
    return [item.strip() for item in value if item.strip()]


def _raw_content(response: Any) -> bytes | str | None:
    raw = getattr(response, "raw", None)
    return getattr(raw, "content", None) if raw is not None else None


def _parse_json_response(raw_content: bytes | str | None) -> dict | None:
    if raw_content is None:
        return None
    try:
        text = (
            raw_content.decode("utf-8", errors="replace")
            if isinstance(raw_content, bytes)
            else str(raw_content)
        )
        value = json.loads(text)
    except (TypeError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def _handle_feishu_aily_knowledge_ask(args: dict, **kwargs) -> str:
    raw_content = args.get("content")
    if not isinstance(raw_content, str):
        return tool_error("content must be a string")
    content = raw_content.strip()
    if not content:
        return tool_error("content is required")
    if len(content) > 65_535:
        return tool_error("content must be at most 65535 characters")

    try:
        target_app_id = _env(_TARGET_APP_ID_ENV)
    except RuntimeError as exc:
        return tool_error(f"Aily client configuration error: {_safe_error_text(exc)}")
    if not target_app_id:
        return tool_error(
            f"Aily target app is not configured; set {_TARGET_APP_ID_ENV} "
            "to the App ID shown in aily.feishu.cn (typically spring_...)"
        )
    if len(target_app_id) > 255:
        return tool_error(f"{_TARGET_APP_ID_ENV} must be at most 255 characters")

    try:
        data_asset_ids = _validated_string_list(args, "data_asset_ids")
        data_asset_tag_ids = _validated_string_list(args, "data_asset_tag_ids")
        client = _resolve_client()
    except (RuntimeError, ValueError) as exc:
        return tool_error(f"Aily client configuration error: {_safe_error_text(exc)}")

    if client is None:
        return tool_error(
            "Aily client is not configured; set both "
            f"{_AUTH_APP_ID_ENV} and {_AUTH_APP_SECRET_ENV}, or call from a Feishu context"
        )

    try:
        from lark_oapi import AccessTokenType
        from lark_oapi.core.enum import HttpMethod
        from lark_oapi.core.model.base_request import BaseRequest
    except ImportError:
        return tool_error("lark_oapi not installed")

    body: dict[str, Any] = {"message": {"content": content}}
    if data_asset_ids:
        body["data_asset_ids"] = data_asset_ids
    if data_asset_tag_ids:
        body["data_asset_tag_ids"] = data_asset_tag_ids

    request = (
        BaseRequest.builder()
        .http_method(HttpMethod.POST)
        .uri(_KNOWLEDGE_ASK_URI)
        .token_types({AccessTokenType.TENANT})
        .paths({"app_id": target_app_id})
        .headers({"Content-Type": "application/json; charset=utf-8"})
        .body(body)
        .build()
    )

    try:
        response = client.request(request)
    except Exception as exc:
        safe_error = _safe_error_text(exc)
        logger.warning("feishu_aily_knowledge_ask request failed: %s", safe_error)
        return tool_error(f"Aily knowledge ask request failed: {safe_error}")

    raw_content = _raw_content(response)
    code = getattr(response, "code", None)
    msg = getattr(response, "msg", None)
    json_response = _parse_json_response(raw_content)
    if json_response:
        code = json_response.get("code", code)
        msg = json_response.get("msg", msg)
    if code != 0:
        safe_msg = _safe_error_text(msg or "unknown error")
        return tool_error(
            f"Aily knowledge ask failed: code={code} msg={safe_msg}",
            code=code,
        )
    if raw_content is None:
        return tool_error("Aily knowledge ask: no raw response content")

    return _parse_sse_response(raw_content)


_AILY_ERROR_MESSAGES = {
    2700001: "param is invalid",
    2700033: "failed to ask knowledges",
    2700034: "ask is forbidden, please contact developer",
    2700035: "run time too long",
}


def _parse_sse_response(raw_content: bytes | str) -> str:
    """Return only a completed Aily answer; never treat a partial stream as success."""
    try:
        text = (
            raw_content.decode("utf-8", errors="replace")
            if isinstance(raw_content, bytes)
            else str(raw_content)
        )
    except Exception:
        return tool_error("Aily knowledge ask: failed to decode response")

    saw_event = False
    finished_payload: dict | None = None
    error_info: dict | None = None

    for line in text.splitlines():
        line = line.strip()
        if not line.startswith("data:"):
            continue
        payload_text = line[5:].strip()
        if not payload_text:
            continue
        try:
            payload = json.loads(payload_text)
        except json.JSONDecodeError:
            logger.debug("feishu_aily_knowledge_ask: invalid SSE data line: %r", line[:200])
            continue
        if not isinstance(payload, dict):
            continue

        saw_event = True
        sse_code = payload.get("code")
        if sse_code not in (None, 0):
            sse_msg = payload.get("msg", "")
            error_info = {
                "code": sse_code,
                "msg": _AILY_ERROR_MESSAGES.get(
                    sse_code, _safe_error_text(sse_msg or "unknown error")
                ),
            }
        if payload.get("status") == "finished":
            finished_payload = payload

    if error_info:
        return tool_error(
            f"Aily knowledge ask failed: [{error_info['code']}] {error_info['msg']}",
            code=error_info["code"],
        )
    if finished_payload is None:
        detail = "stream ended before a finished event" if saw_event else "no valid SSE events"
        return tool_error(f"Aily knowledge ask: {detail}")

    message = finished_payload.get("message") or {}
    content = message.get("content", "") if isinstance(message, dict) else ""
    faq_result = finished_payload.get("faq_result") or {}
    if not content and isinstance(faq_result, dict):
        content = faq_result.get("answer", "")
    result = {
        "success": True,
        "content": content,
        "has_answer": bool(finished_payload.get("has_answer", False)),
        "finish_type": finished_payload.get("finish_type", ""),
    }
    if isinstance(faq_result, dict) and faq_result.get("question"):
        result["matched_question"] = faq_result["question"]
    if not content:
        result["message"] = "No answer content returned"
    return tool_result(result)


registry.register(
    name="feishu_aily_knowledge_ask",
    toolset="feishu_aily",
    schema=FEISHU_AILY_KNOWLEDGE_ASK_SCHEMA,
    handler=_handle_feishu_aily_knowledge_ask,
    check_fn=_check_feishu,
    requires_env=[],
    is_async=False,
    description="向当前配置的飞书 Aily 知识应用发起问答",
    emoji="Aily",
    max_result_size_chars=50_000,
)
