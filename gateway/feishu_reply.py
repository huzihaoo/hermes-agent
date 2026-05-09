"""Feishu-specific user-visible reply formatting.

Keep noisy gateway/LLM lifecycle diagnostics in logs while Feishu users see a
short human-facing status.  This module intentionally handles only Feishu
surface text; canonical retry/fallback details stay in run_agent.py logs and
session artifacts.
"""

from __future__ import annotations

import re
from typing import Any

from gateway.config import Platform

_FEISHU_INTERNAL_ERROR_TEXT = (
    "这次处理没跑完，我这边会按网关日志继续排。\n"
    "你可以稍后重试一次；如果还是这样，我再把具体卡点收出来。"
)

_FEISHU_TRANSIENT_LIFECYCLE_PATTERNS = (
    re.compile(r"^\s*⏳\s*Retrying in\b", re.I),
    re.compile(r"^\s*⏱️?\s*Rate limited\.\s*Waiting\b", re.I),
    re.compile(r"^\s*⏱️?\s*Rate limit reached\.\s*Waiting\b", re.I),
    re.compile(r"^\s*⛔\s*Provider lane open\b", re.I),
    re.compile(r"^\s*⏳\s*Provider lane cooling down\b", re.I),
    re.compile(r"^\s*🔄\s*Primary model failed\b", re.I),
    re.compile(r"^\s*⚠️?\s*Max retries \(\d+\) exhausted\s+—\s+trying fallback", re.I),
    re.compile(r"^\s*⚠️?\s*Max retries \(\d+\) for invalid responses\s+—\s+trying fallback", re.I),
    re.compile(r"^\s*⚠️?\s*Empty/malformed response\s+—\s+switching to fallback", re.I),
    re.compile(r"^\s*⚠️?\s*Non-retryable error \(HTTP\s*\d{3}\)\s+—\s+trying fallback", re.I),
    re.compile(r"^\s*⚠️?\s*No response from provider\b", re.I),
    re.compile(r"^\s*⚠️?\s*Connection (?:to provider )?dropped\b", re.I),
    re.compile(r"^\s*🔄\s*Reconnected\s+—\s+resuming", re.I),
    re.compile(r"^\s*🔌\s*Detected stale connections from a previous provider issue\b", re.I),
    re.compile(r"^\s*⚠️?\s*Empty response from model\s+—\s+retrying", re.I),
    re.compile(r"^\s*⚠️?\s*Model returning empty responses\s+—\s+switching to fallback provider", re.I),
    re.compile(r"^\s*↻\s*Switched to fallback:", re.I),
    re.compile(r"^\s*↻\s*Provider returned an empty post-tool message\b", re.I),
    re.compile(r"^\s*↻\s*Thinking-only response\s+—\s+prefilling to continue", re.I),
)

_FEISHU_FINAL_MODEL_FAILURE_PATTERNS = (
    re.compile(r"^\s*❌?\s*API failed after \d+ retries\b", re.I),
    re.compile(r"^\s*❌?\s*Rate limited after \d+ retries\b", re.I),
    re.compile(r"^\s*❌?\s*Connection to provider failed after \d+ attempts\b", re.I),
    re.compile(r"^\s*❌?\s*Non-retryable error \(HTTP\s*\d{3}\):", re.I),
    re.compile(r"^\s*❌?\s*Model returned no content after all retries\b", re.I),
    re.compile(r"^\s*API call failed after \d+ retries\b", re.I),
    re.compile(r"^\s*Error:\s*API call failed after \d+ retries\b", re.I),
)

_FEISHU_INTERNAL_ERROR_PATTERNS = (
    re.compile(r"^\s*Sorry, I encountered an error\b", re.I),
    re.compile(r"^\s*The request failed:\s*", re.I),
    re.compile(r"\bTry again or use /reset to start a fresh session\.?\s*$", re.I),
)

_FEISHU_SENSITIVE_DIAGNOSTIC_PATTERNS = (
    re.compile(r"\b(?:NameError|TypeError|ValueError|KeyError|AttributeError|RuntimeError|Traceback)\b"),
    re.compile(r"\b(?:HTTP\s*\d{3}|Upstream request failed|API key|OAuth|provider|fallback|retries)\b", re.I),
    re.compile(r"\b(?:custom:[\w.-]+|sub2api|gpt[-\w.]+|claude[-\w.]*)\b", re.I),
)

_FEISHU_MODEL_FAILURE_DIAGNOSTIC_PATTERNS = (
    re.compile(r"\b(?:HTTP\s*\d{3}|Upstream request failed|provider|fallback|retries|BadRequestError|RateLimitError|APIConnectionError|ReadError|Proxy error)\b", re.I),
    re.compile(r"\b(?:custom:[\w.-]+|sub2api|gpt[-\w.]+|claude[-\w.]*)\b", re.I),
)

_FEISHU_MODEL_FAILURE_TEXT = (
    "这次模型服务没有正常返回，我这边已经自动重试过了。\n"
    "可以稍后再试一次；如果连续出现，我再继续排网关和上游链路。"
)

_FEISHU_CONTEXT_FAILURE_TEXT = (
    "这轮对话太长了，模型已经放不下完整上下文。\n"
    "可以先发 /compact 压缩一下，或者 /reset 开一个新会话。"
)

_FEISHU_CONTEXT_FAILURE_PATTERNS = (
    re.compile(r"\b(?:context|token|too large|too long|exceed|payload)\b", re.I),
    re.compile(r"Session too large for the model's context window", re.I),
)


def _is_feishu(platform: Platform | str | None) -> bool:
    if platform == Platform.FEISHU:
        return True
    return str(platform or "").lower() == Platform.FEISHU.value


def _is_human_mode(progress_mode: str | None) -> bool:
    return (progress_mode or "human").strip().lower() == "human"


def format_feishu_lifecycle_status(
    platform: Platform | str | None,
    progress_mode: str | None,
    event_type: str | None,
    message: Any,
) -> str | None:
    """Return Feishu-visible lifecycle text, or None to suppress noise.

    The retry/fallback messages in run_agent.py are useful diagnostics but too
    chatty for Feishu topics.  In Feishu human mode, suppress transient retry
    chatter and show only a compact final failure when the whole turn fails.
    Other platforms/modes keep the original text.
    """
    text = str(message or "").strip()
    if not text:
        return None
    if not (_is_feishu(platform) and _is_human_mode(progress_mode)):
        return text
    if (event_type or "") != "lifecycle":
        return text

    if any(pattern.search(text) for pattern in _FEISHU_TRANSIENT_LIFECYCLE_PATTERNS):
        return None
    if any(pattern.search(text) for pattern in _FEISHU_FINAL_MODEL_FAILURE_PATTERNS):
        return _FEISHU_MODEL_FAILURE_TEXT
    return text


def sanitize_feishu_visible_text(
    platform: Platform | str | None,
    progress_mode: str | None,
    response: Any,
    *,
    failed: bool = False,
) -> str:
    """Sanitize any Feishu-visible assistant text before sending.

    This is a broader boundary filter for streamed/interim assistant messages.
    It keeps normal assistant prose intact, but collapses raw diagnostic blobs if
    a model accidentally emits provider/tool/error internals as user text.
    """
    text = str(response or "").strip()
    if not text:
        return text
    if not (_is_feishu(platform) and _is_human_mode(progress_mode)):
        return text

    if any(pattern.search(text) for pattern in _FEISHU_CONTEXT_FAILURE_PATTERNS):
        return _FEISHU_CONTEXT_FAILURE_TEXT
    if any(pattern.search(text) for pattern in _FEISHU_FINAL_MODEL_FAILURE_PATTERNS):
        return _FEISHU_MODEL_FAILURE_TEXT
    if any(pattern.search(text) for pattern in _FEISHU_INTERNAL_ERROR_PATTERNS):
        return _FEISHU_INTERNAL_ERROR_TEXT
    if failed and any(pattern.search(text) for pattern in _FEISHU_MODEL_FAILURE_DIAGNOSTIC_PATTERNS):
        return _FEISHU_MODEL_FAILURE_TEXT
    if failed and any(pattern.search(text) for pattern in _FEISHU_SENSITIVE_DIAGNOSTIC_PATTERNS):
        return _FEISHU_INTERNAL_ERROR_TEXT
    if _looks_like_raw_diagnostic_blob(text):
        if any(pattern.search(text) for pattern in _FEISHU_MODEL_FAILURE_DIAGNOSTIC_PATTERNS):
            return _FEISHU_MODEL_FAILURE_TEXT
        if any(pattern.search(text) for pattern in _FEISHU_SENSITIVE_DIAGNOSTIC_PATTERNS):
            return _FEISHU_INTERNAL_ERROR_TEXT
    return text


def _looks_like_raw_diagnostic_blob(text: str) -> bool:
    lowered = text.lower()
    if "to=functions." in lowered or "<tool_call" in lowered or "tool_call" in lowered:
        return True
    if re.search(r"^\s*(?:assistant\s+)?to=functions\.[\w.:-]+\b", text, re.I | re.M):
        return True
    if re.search(r"^\s*<\|channel\|>commentary\s+to=functions\.[\w.:-]+\b", text, re.I | re.M):
        return True
    if re.search(r"^\s*⚠️?\s*Proxy error\s*\(HTTP?\s*\d{3}\):", text, re.I | re.M):
        return True
    if re.search(r"^\s*(?:mcp_[\w.:-]+|skill_view|skill_manage|skills_list|browser_[\w.:-]+|terminal)\s*[:({]", text, re.I | re.M):
        return True
    diagnostic_hits = sum(
        1
        for pattern in (*_FEISHU_MODEL_FAILURE_DIAGNOSTIC_PATTERNS, *_FEISHU_SENSITIVE_DIAGNOSTIC_PATTERNS)
        if pattern.search(text)
    )
    return diagnostic_hits >= 2


def sanitize_feishu_final_response(
    platform: Platform | str | None,
    progress_mode: str | None,
    response: Any,
    *,
    failed: bool = False,
) -> str:
    """Sanitize final Feishu replies for model/provider failures.

    Final responses bypass lifecycle status_callback, so apply the same human
    model-failure wording before adapter.send().
    """
    text = str(response or "").strip()
    if not text:
        return text
    if not (_is_feishu(platform) and _is_human_mode(progress_mode)):
        return text

    if any(pattern.search(text) for pattern in _FEISHU_FINAL_MODEL_FAILURE_PATTERNS):
        return _FEISHU_MODEL_FAILURE_TEXT
    if any(pattern.search(text) for pattern in _FEISHU_CONTEXT_FAILURE_PATTERNS):
        return _FEISHU_CONTEXT_FAILURE_TEXT
    if any(pattern.search(text) for pattern in _FEISHU_INTERNAL_ERROR_PATTERNS):
        return _FEISHU_INTERNAL_ERROR_TEXT
    if failed and any(pattern.search(text) for pattern in _FEISHU_MODEL_FAILURE_DIAGNOSTIC_PATTERNS):
        return _FEISHU_MODEL_FAILURE_TEXT
    if failed and any(pattern.search(text) for pattern in _FEISHU_SENSITIVE_DIAGNOSTIC_PATTERNS):
        return _FEISHU_INTERNAL_ERROR_TEXT
    return text


def sanitize_feishu_internal_error(
    platform: Platform | str | None,
    progress_mode: str | None,
    error_type: Any,
    error_detail: Any,
    *,
    status_hint: Any = "",
) -> str:
    """Return a Feishu-safe message for uncaught gateway exceptions.

    Logs still carry exception type/details. Feishu users should not see Python
    exception classes, provider names, credentials hints, or raw stack details.
    """
    raw = "\n".join(
        part for part in (str(error_type or ""), str(error_detail or ""), str(status_hint or "")) if part
    )
    if not (_is_feishu(platform) and _is_human_mode(progress_mode)):
        return (
            f"Sorry, I encountered an error ({error_type}).\n"
            f"{error_detail}\n"
            f"{status_hint}"
            "Try again or use /reset to start a fresh session."
        )
    if any(pattern.search(raw) for pattern in _FEISHU_CONTEXT_FAILURE_PATTERNS):
        return _FEISHU_CONTEXT_FAILURE_TEXT
    if any(pattern.search(raw) for pattern in _FEISHU_FINAL_MODEL_FAILURE_PATTERNS):
        return _FEISHU_MODEL_FAILURE_TEXT
    return _FEISHU_INTERNAL_ERROR_TEXT
