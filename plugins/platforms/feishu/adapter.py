"""
Feishu/Lark platform adapter.

Supports:
- WebSocket long connection and Webhook transport
- Direct-message and group @mention-gated text receive/send
- Inbound image/file/audio/media caching
- Gateway allowlist integration via FEISHU_ALLOWED_USERS
- Persistent dedup state across restarts
- Per-chat serial message processing (matches openclaw createChatQueue)
- Processing status reactions: Typing while working, removed on success,
  swapped for CrossMark on failure
- Reaction events routed as synthetic text events (matches openclaw)
- Interactive card button-click events routed as synthetic COMMAND events
- Webhook anomaly tracking (matches openclaw createWebhookAnomalyTracker)
- Verification token validation as second auth layer (matches openclaw)

Feishu identity model
---------------------
Feishu uses three user-ID tiers (official docs:
https://open.feishu.cn/document/home/user-identity-introduction/introduction):

  open_id  (ou_xxx)  — **App-scoped**.  The same person gets a different
                        open_id under each Feishu app.  Always available in
                        event payloads without extra permissions.
  user_id  (u_xxx)   — **Tenant-scoped**.  Stable within a company but
                        requires the ``contact:user.employee_id:readonly``
                        scope.  May not be present.
  union_id (on_xxx)  — **Developer-scoped**.  Same across all apps owned by
                        one developer/ISV.  Best cross-app stable ID.

For bots specifically:

  app_id              — The application's canonical credential identifier.
  bot open_id         — Returned by ``/bot/v3/info``.  This is the bot's own
                        open_id *within its app context* and is what Feishu
                        puts in ``mentions[].id.open_id`` when someone
                        @-mentions the bot.  Used for mention gating only.

In single-bot mode (what Hermes currently supports), open_id works as a
de-facto unique user identifier since there is only one app context.

Session-key participant isolation prefers ``union_id`` (via user_id_alt)
over ``open_id`` (via user_id) so that sessions stay stable if the same
user is seen through different apps in the future.
"""

from __future__ import annotations

import asyncio
import collections
import concurrent.futures
import hashlib
import hmac
import itertools
import json
import logging
import mimetypes
import os
import re
import threading
import time
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Literal, Optional, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

# aiohttp/websockets are independent optional deps — import outside lark_oapi
# so they remain available for tests and webhook mode even if lark_oapi is missing.
try:
    import aiohttp
    from aiohttp import web
except ImportError:
    aiohttp = None  # type: ignore[assignment]
    web = None  # type: ignore[assignment]

try:
    import websockets
except ImportError:
    websockets = None  # type: ignore[assignment]

try:
    import lark_oapi as lark
    from lark_oapi.api.application.v6 import GetApplicationRequest
    from lark_oapi.api.im.v1 import (
        CreateFileRequest,
        CreateFileRequestBody,
        CreateImageRequest,
        CreateImageRequestBody,
        CreateMessageRequest,
        CreateMessageRequestBody,
        GetChatRequest,
        GetMessageRequest,
        GetMessageResourceRequest,
        P2ImMessageMessageReadV1,
        ReplyMessageRequest,
        ReplyMessageRequestBody,
        UpdateMessageRequest,
        UpdateMessageRequestBody,
    )
    from lark_oapi.core import AccessTokenType, HttpMethod
    from lark_oapi.core.const import FEISHU_DOMAIN, LARK_DOMAIN
    from lark_oapi.core.model import BaseRequest
    from lark_oapi.event.callback.model.p2_card_action_trigger import (
        CallBackCard,
        P2CardActionTriggerResponse,
    )
    from lark_oapi.event.dispatcher_handler import EventDispatcherHandler
    from lark_oapi.ws import Client as FeishuWSClient

    FEISHU_AVAILABLE = True
except ImportError:
    FEISHU_AVAILABLE = False
    lark = None  # type: ignore[assignment]
    CallBackCard = None  # type: ignore[assignment]
    P2CardActionTriggerResponse = None  # type: ignore[assignment]
    EventDispatcherHandler = None  # type: ignore[assignment]
    FeishuWSClient = None  # type: ignore[assignment]
    FEISHU_DOMAIN = None  # type: ignore[assignment]
    LARK_DOMAIN = None  # type: ignore[assignment]

FEISHU_WEBSOCKET_AVAILABLE = websockets is not None
FEISHU_WEBHOOK_AVAILABLE = aiohttp is not None

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    ProcessingOutcome,
    SendResult,
    SUPPORTED_DOCUMENT_TYPES,
    cache_document_from_bytes,
    cache_image_from_url,
    cache_audio_from_bytes,
    cache_image_from_bytes,
    _reply_anchor_for_event,
    _thread_metadata_for_source,
)
from gateway.feishu_reply import FeishuBotMessageRegistry, record_feishu_bot_message_fingerprint
from gateway.feishu_interaction_policy import (
    FeishuInteractionContext,
    build_intake_ack,
    build_integration_tools_runbook_fast_reply,
    classify_integration_tools_intent,
)
from gateway.status import acquire_scoped_lock, release_scoped_lock
from gateway.record_only.runtime import get_record_only_transport
from hermes_constants import get_hermes_home
from hermes_cli.config import cfg_get, load_config
from utils import atomic_json_write, env_float, env_int

logger = logging.getLogger(__name__)


PNC_ALL_BUSINESS_TEST_GROUP_ID = "oc_16614f4ba25b8c88b69c0b8e9ebc2fb5"
G1Q3_RCA_GROUP_ID = "oc_6cfc782212009ff4cd815349909dd423"
_FEISHU_QUEUE_EVENT_CONTEXT_SCHEMA = "feishu_queue_message_event_v1"
_G1Q3_RCA_MANUAL_QUEUE_ROUTE = "g1q3_rca_manual_v1"
_MAX_FEISHU_QUEUE_REPLY_TEXT_CHARS = 32 * 1024
_MAX_FEISHU_QUEUE_LINK_COUNT = 32
_MAX_FEISHU_QUEUE_LINK_CHARS = 4096
_MAX_API_POLL_PENDING_PER_CHAT = 1000
_MAX_API_POLL_ITEM_BYTES = 128 * 1024
_MAX_API_POLL_TOTAL_BYTES = 16 * 1024 * 1024
_MAX_API_POLL_PER_CHAT_BYTES = 4 * 1024 * 1024
_MAX_API_POLL_SCAN_ITEMS = 10_000
_MAX_API_POLL_TERMINAL_HOLES = 1000
_MAX_FEISHU_INBOX_STATE_BYTES = 16 * 1024 * 1024
_API_POLL_CAPACITY_RESERVE_BYTES = 16 * 1024
_API_POLL_SIDECAR_SCHEMA = "feishu_api_poll_state_v1"
_API_POLL_INVALID_TOKEN_RE = re.compile(
    r"(?is)(?:page[_\s-]*token.{0,80}(?:invalid|expired|illegal|not\s+valid))"
    r"|(?:(?:invalid|expired|illegal|not\s+valid).{0,80}page[_\s-]*token)"
)


class _FeishuReplyContextUnavailable(RuntimeError):
    """A referenced parent message could not be read reliably."""


class _FeishuApiPollInvalidContinuation(RuntimeError):
    """A persisted API page token is explicitly invalid and may be reset."""

    def __init__(self, message: str, page_token: str):
        super().__init__(message)
        self.page_token = page_token


@dataclass(frozen=True)
class _FeishuApiPollScanContinuation:
    state: Dict[str, Any]


def _integration_tools_intake_chat_ids() -> set[str]:
    """Return configured integration_tools intake Feishu chats."""
    try:
        cfg = load_config() or {}
        block = cfg_get(cfg, "business_lines", "integration_tools", default={}) or {}
        if not isinstance(block, dict) or not bool(block.get("enabled", False)):
            return set()
        raw_ids: list[Any] = []
        for key in ("intake_chat_ids", "intake_chat_id", "intake_group_ids", "intake_group_id"):
            value = block.get(key)
            if isinstance(value, (list, tuple, set)):
                raw_ids.extend(value)
            elif value:
                raw_ids.append(value)
        return {str(v).strip() for v in raw_ids if str(v or "").strip()}
    except Exception:
        return set()


def _looks_like_g1q3_rca_request_for_admission(text: str) -> bool:
    body = str(text or "")
    lower = body.lower()
    if "g1q3" in lower or "rca" in lower:
        return True
    if re.search(r"(?:飞书问题|问题|issue|work[_ -]?item)\s*[:：#]?\s*\d{6,}", body, re.IGNORECASE):
        return True
    if re.search(
        r"project\.feishu\.cn/[^\s)]+/issue/detail/\d+",
        body,
        re.IGNORECASE,
    ):
        return True
    if re.search(r"\bcase\s+g1q3[-_ ]?\d+", lower):
        return True
    return False


def _admission_text_with_issue_links(event: MessageEvent) -> str:
    """Persist only allowlisted Feishu issue links needed after queue dequeue."""
    text = str(getattr(event, "text", "") or "")
    metadata = getattr(event, "metadata", None)
    feishu = metadata.get("feishu") if isinstance(metadata, dict) else {}
    raw_urls = feishu.get("link_urls") if isinstance(feishu, dict) else []
    if not isinstance(raw_urls, list):
        raw_urls = []
    issue_urls: list[str] = []
    for raw_url in raw_urls:
        url = str(raw_url or "").strip()
        if not url.startswith(("http://", "https://")):
            continue
        if not re.search(
            r"^https?://project\.feishu\.cn/[^\s/?#]+/issue/detail/\d+",
            url,
            re.IGNORECASE,
        ):
            continue
        if url not in issue_urls and url not in text:
            issue_urls.append(url)
    if not issue_urls:
        return text
    return "\n".join([text, *issue_urls]).strip()


def _requires_durable_g1q3_gateway_decision(event: MessageEvent) -> bool:
    """Route trusted fixed-group mentions through Gateway before inbox ACK."""
    source = getattr(event, "source", None)
    if source is None:
        return False
    chat_id = str(getattr(source, "chat_id", "") or "").strip()
    if (
        chat_id not in {G1Q3_RCA_GROUP_ID, PNC_ALL_BUSINESS_TEST_GROUP_ID}
        or str(getattr(source, "chat_type", "") or "").strip().lower() != "group"
        or bool(getattr(source, "is_bot", False))
    ):
        return False
    metadata = getattr(event, "metadata", None)
    feishu = metadata.get("feishu") if isinstance(metadata, dict) else None
    if not isinstance(feishu, dict):
        return False
    if (
        feishu.get("self_mentioned") is not True
        or feishu.get("self_mention_command_directed") is not True
        or feishu.get("is_bot_sender") is True
        or feishu.get("sender_type") not in {None, "user"}
    ):
        return False
    return True


def _build_feishu_queue_event_context(
    event: MessageEvent,
    *,
    durable_rca_manual: bool,
) -> Dict[str, Any]:
    """Serialize only the transport fields the queue worker may trust."""
    source = event.source
    metadata = event.metadata if isinstance(event.metadata, dict) else {}
    raw_feishu = metadata.get("feishu")
    raw_feishu = raw_feishu if isinstance(raw_feishu, dict) else {}
    raw_links = raw_feishu.get("link_urls")
    raw_link_urls = (
        [str(url).strip() for url in raw_links if str(url or "").strip()]
        if isinstance(raw_links, list)
        else []
    )
    reply_to_text = str(event.reply_to_text or "")
    if durable_rca_manual:
        if len(reply_to_text) > _MAX_FEISHU_QUEUE_REPLY_TEXT_CHARS:
            raise ValueError("durable Feishu reply context is too large")
        if len(raw_link_urls) > _MAX_FEISHU_QUEUE_LINK_COUNT:
            raise ValueError("durable Feishu link context has too many URLs")
        if any(len(url) > _MAX_FEISHU_QUEUE_LINK_CHARS for url in raw_link_urls):
            raise ValueError("durable Feishu link URL is too large")
    else:
        reply_to_text = reply_to_text[:_MAX_FEISHU_QUEUE_REPLY_TEXT_CHARS]
        raw_link_urls = raw_link_urls[:_MAX_FEISHU_QUEUE_LINK_COUNT]
    link_urls: list[str] = []
    for url in raw_link_urls:
        bounded_url = url[:_MAX_FEISHU_QUEUE_LINK_CHARS]
        if bounded_url not in link_urls:
            link_urls.append(bounded_url)
    platform = getattr(source.platform, "value", source.platform)
    receive_time_ms = raw_feishu.get("receive_time_ms")
    if isinstance(receive_time_ms, bool) or not isinstance(
        receive_time_ms, (str, int, float, type(None))
    ):
        receive_time_ms = None
    return {
        "schema_version": _FEISHU_QUEUE_EVENT_CONTEXT_SCHEMA,
        "route_contract": (
            _G1Q3_RCA_MANUAL_QUEUE_ROUTE if durable_rca_manual else None
        ),
        "source": {
            "platform": str(platform or "feishu"),
            "user_id": str(source.user_id or ""),
            "user_id_alt": str(source.user_id_alt or "") or None,
            "user_name": str(source.user_name or "") or None,
            "chat_id": str(source.chat_id or ""),
            "chat_name": str(source.chat_name or "") or None,
            "chat_type": str(source.chat_type or "dm"),
            "thread_id": str(source.thread_id or "") or None,
            "is_bot": source.is_bot is True,
        },
        "event": {
            "message_id": str(event.message_id or ""),
            "reply_to_message_id": str(event.reply_to_message_id or "") or None,
            "reply_to_text": reply_to_text or None,
        },
        "feishu": {
            "message_id": str(event.message_id or ""),
            "root_id": str(raw_feishu.get("root_id") or "") or None,
            "parent_id": str(raw_feishu.get("parent_id") or "") or None,
            "thread_id": str(source.thread_id or "") or None,
            "sender_id": str(source.user_id or ""),
            "sender_type": "bot" if source.is_bot else "user",
            "is_bot_sender": source.is_bot is True,
            "is_topic": bool(source.thread_id),
            "raw_container_id": str(source.chat_id or ""),
            "receive_time_ms": receive_time_ms,
            "ingress_source": str(raw_feishu.get("ingress_source") or "") or None,
            "link_urls": link_urls or None,
            "self_mentioned": raw_feishu.get("self_mentioned") is True,
            "self_mention_command_directed": (
                raw_feishu.get("self_mention_command_directed") is True
            ),
            "mention_required": raw_feishu.get("mention_required") is True,
        },
    }


def _validated_feishu_queue_event_context(item: Any) -> Optional[Dict[str, Any]]:
    """Return a normalized context only when all transport identity mirrors agree."""
    context = getattr(item, "event_context", None)
    if not isinstance(context, dict) or context.get("schema_version") != _FEISHU_QUEUE_EVENT_CONTEXT_SCHEMA:
        return None
    source = context.get("source")
    event = context.get("event")
    feishu = context.get("feishu")
    if not all(isinstance(value, dict) for value in (source, event, feishu)):
        return None

    def _norm(value: Any) -> str:
        return str(value or "").strip()

    expected_user_id = _norm(getattr(item, "user_id", ""))
    expected_chat_id = _norm(getattr(item, "chat_id", ""))
    expected_chat_type = _norm(getattr(item, "chat_type", "")) or "dm"
    expected_thread_id = _norm(getattr(item, "thread_id", ""))
    expected_message_id = _norm(getattr(item, "request_message_id", ""))
    source_is_bot = source.get("is_bot") is True
    if (
        _norm(getattr(item, "platform", "")) != "feishu"
        or _norm(source.get("platform")) != "feishu"
        or _norm(source.get("user_id")) != expected_user_id
        or _norm(source.get("chat_id")) != expected_chat_id
        or _norm(source.get("chat_type")) != expected_chat_type
        or _norm(source.get("thread_id")) != expected_thread_id
        or _norm(event.get("message_id")) != expected_message_id
        or _norm(feishu.get("message_id")) != expected_message_id
        or _norm(feishu.get("sender_id")) != expected_user_id
        or _norm(feishu.get("raw_container_id")) != expected_chat_id
        or _norm(feishu.get("thread_id")) != expected_thread_id
        or (feishu.get("is_bot_sender") is True) != source_is_bot
        or _norm(feishu.get("sender_type")) != ("bot" if source_is_bot else "user")
    ):
        return None

    raw_links = feishu.get("link_urls")
    link_urls = []
    if isinstance(raw_links, list):
        if len(raw_links) > _MAX_FEISHU_QUEUE_LINK_COUNT:
            return None
        for raw_url in raw_links:
            url = _norm(raw_url)
            if len(url) > _MAX_FEISHU_QUEUE_LINK_CHARS:
                return None
            if url.startswith(("http://", "https://")) and url not in link_urls:
                link_urls.append(url)
    receive_time_ms = feishu.get("receive_time_ms")
    if isinstance(receive_time_ms, bool) or not isinstance(
        receive_time_ms, (str, int, float, type(None))
    ):
        receive_time_ms = None
    ingress_source = _norm(feishu.get("ingress_source"))
    if ingress_source not in {"event_callback", "api_poll"}:
        ingress_source = "event_callback"

    reply_to_text = str(event.get("reply_to_text") or "")
    if len(reply_to_text) > _MAX_FEISHU_QUEUE_REPLY_TEXT_CHARS:
        return None

    return {
        "route_contract": context.get("route_contract"),
        "source": {
            "user_id_alt": _norm(source.get("user_id_alt")) or None,
            "user_name": _norm(source.get("user_name")) or None,
            "chat_name": _norm(source.get("chat_name")) or None,
            "is_bot": source_is_bot,
        },
        "event": {
            "reply_to_message_id": _norm(event.get("reply_to_message_id")) or None,
            "reply_to_text": reply_to_text or None,
        },
        "feishu": {
            "message_id": expected_message_id,
            "root_id": _norm(feishu.get("root_id")) or None,
            "parent_id": _norm(feishu.get("parent_id")) or None,
            "thread_id": expected_thread_id or None,
            "sender_id": expected_user_id,
            "sender_type": "bot" if source_is_bot else "user",
            "is_bot_sender": source_is_bot,
            "is_topic": feishu.get("is_topic") is True,
            "raw_container_id": expected_chat_id,
            "receive_time_ms": receive_time_ms,
            "ingress_source": ingress_source,
            "link_urls": link_urls or None,
            "self_mentioned": feishu.get("self_mentioned") is True,
            "self_mention_command_directed": (
                feishu.get("self_mention_command_directed") is True
            ),
            "mention_required": feishu.get("mention_required") is True,
        },
    }


def _is_integration_tools_intake_chat(chat_id: str) -> bool:
    """Return True for dedicated/configured integration_tools intake chats.

    Kept for compatibility with tests and callers that only know the chat.  The
    all-business test group requires message-intent gating; use
    _is_integration_tools_message_context for live routing decisions.
    """
    return str(chat_id or "") in _integration_tools_intake_chat_ids()


def _is_integration_tools_message_context(chat_id: str, text: str) -> bool:
    """Route Feishu admission to integration-tools without hijacking test traffic."""
    cid = str(chat_id or "").strip()
    if not cid:
        return False
    ids = _integration_tools_intake_chat_ids()
    if cid not in ids:
        return False
    # Feishu issue URLs belong to the G1Q3 RCA policy boundary, including its
    # fixed-group manual control plane. Integration-tools must never claim
    # them; metadata-only card URLs were appended above before this check.
    if re.search(
        r"https?://project\.feishu\.cn/[^\s/?#)]+/issue/detail/\d+",
        str(text or ""),
        re.IGNORECASE,
    ):
        return False
    if cid == PNC_ALL_BUSINESS_TEST_GROUP_ID:
        if _looks_like_g1q3_rca_request_for_admission(text):
            return False
        return classify_integration_tools_intent(text) != "general"
    return True


# ---------------------------------------------------------------------------
# Regex patterns
# ---------------------------------------------------------------------------

_MARKDOWN_HINT_RE = re.compile(
    r"(^#{1,6}\s)|(^\s*[-*]\s)|(^\s*\d+\.\s)|(^\s*---+\s*$)|(```)|(`[^`\n]+`)|(\*\*[^*\n].+?\*\*)|(~~[^~\n].+?~~)|(<u>.+?</u>)|(\*[^*\n]+\*)|(\[[^\]]+\]\([^)]+\))|(^>\s)",
    re.MULTILINE,
)
# Detect markdown tables: a line starting with | followed by a separator line.
# Feishu post-type 'md' elements do not render tables, so we force text mode.
_MARKDOWN_TABLE_RE = re.compile(r"^\|.*\|\n\|[-|: ]+\|", re.MULTILINE)
_MARKDOWN_LINK_RE = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")
_MARKDOWN_FENCE_OPEN_RE = re.compile(r"^```([^\n`]*)\s*$")
_MARKDOWN_FENCE_CLOSE_RE = re.compile(r"^```\s*$")
_MENTION_RE = re.compile(r"@_user_\d+")
_MULTISPACE_RE = re.compile(r"[ \t]{2,}")
_POST_CONTENT_INVALID_RE = re.compile(r"content format of the post type is incorrect", re.IGNORECASE)
_FEISHU_TOPIC_THREAD_PREFIX = "topic:"
# ---------------------------------------------------------------------------
# Media type sets and upload constants
# ---------------------------------------------------------------------------

_IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"}
_AUDIO_EXTENSIONS = {".ogg", ".mp3", ".wav", ".m4a", ".aac", ".flac", ".opus", ".webm"}
_VIDEO_EXTENSIONS = {".mp4", ".mov", ".avi", ".mkv", ".webm", ".m4v", ".3gp"}
_DOCUMENT_MIME_TO_EXT = {mime: ext for ext, mime in SUPPORTED_DOCUMENT_TYPES.items()}
_FEISHU_IMAGE_UPLOAD_TYPE = "message"
_FEISHU_FILE_UPLOAD_TYPE = "stream"
_FEISHU_OPUS_UPLOAD_EXTENSIONS = {".ogg", ".opus"}
_FEISHU_MEDIA_UPLOAD_EXTENSIONS = {".mp4", ".mov", ".avi", ".m4v"}
_FEISHU_DOC_UPLOAD_TYPES = {
    ".pdf": "pdf",
    ".doc": "doc",
    ".docx": "doc",
    ".xls": "xls",
    ".xlsx": "xls",
    ".ppt": "ppt",
    ".pptx": "ppt",
}
# ---------------------------------------------------------------------------
# Connection, retry and batching tuning
# ---------------------------------------------------------------------------

_MAX_TEXT_INJECT_BYTES = 100 * 1024
_FEISHU_RESOURCE_READ_CHUNK_BYTES = 1024 * 1024
_DEFAULT_FEISHU_MAX_FILE_BYTES = 32 * 1024 * 1024
_FEISHU_KNOWN_RESOURCE_WARNINGS = {
    234037: "文件超过飞书机器人消息下载限制，请提供 VM/NAS 路径，或拆分/压缩为较小文件。VM 临时目录建议使用 /mnt/tmp/<task_id>/，对外路径为 //hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/<task_id>/。",
    234001: "飞书文件夹不能通过消息附件接口直接下载；请发 zip 或提供 VM/NAS 路径。若要支持文件夹读取，需要开通 Drive scopes 并走 Drive API。",
    234003: "飞书文件夹不能通过消息附件接口直接下载；请发 zip 或提供 VM/NAS 路径。若要支持文件夹读取，需要开通 Drive scopes 并走 Drive API。",
    99991672: "当前飞书应用缺少 Drive 读取权限，无法读取文件夹；请发 zip 或提供 VM/NAS 路径。",
}
_FEISHU_CONNECT_ATTEMPTS = 3
_FEISHU_SEND_ATTEMPTS = 3
_FEISHU_APP_LOCK_SCOPE = "feishu-app-id"
_DEFAULT_TEXT_BATCH_DELAY_SECONDS = 0.6
_DEFAULT_TEXT_BATCH_MAX_MESSAGES = 8
_DEFAULT_TEXT_BATCH_MAX_CHARS = 4000
_DEFAULT_MEDIA_BATCH_DELAY_SECONDS = 0.8
_DEFAULT_DEDUP_CACHE_SIZE = 2048
_DEFAULT_API_POLL_PAGE_SIZE = 10
_MAX_API_POLL_PAGE_SIZE = 50
_DEFAULT_API_POLL_STARTUP_LOOKBACK_SECONDS = 0
_MAX_API_POLL_STARTUP_LOOKBACK_SECONDS = 10 * 60
_MAX_API_POLL_STARTUP_PAGES = 20
_API_POLL_REQUEST_TIMEOUT_SECONDS = 20
_API_POLL_CANCEL_WAIT_SECONDS = _API_POLL_REQUEST_TIMEOUT_SECONDS + 5
_DEFAULT_WEBHOOK_HOST = "127.0.0.1"
_DEFAULT_WEBHOOK_PORT = 8765
_DEFAULT_WEBHOOK_PATH = "/feishu/webhook"
# ---------------------------------------------------------------------------
# TTL, rate-limit and webhook security constants
# ---------------------------------------------------------------------------

_FEISHU_DEDUP_TTL_SECONDS = 24 * 60 * 60          # 24 hours — matches openclaw
_FEISHU_SENDER_NAME_TTL_SECONDS = 10 * 60          # 10 minutes sender-name cache
_FEISHU_WEBHOOK_MAX_BODY_BYTES = 1 * 1024 * 1024   # 1 MB body limit
_FEISHU_WEBHOOK_RATE_WINDOW_SECONDS = 60            # sliding window for rate limiter
_FEISHU_WEBHOOK_RATE_LIMIT_MAX = 120               # max requests per window per IP — matches openclaw
_FEISHU_WEBHOOK_RATE_MAX_KEYS = 4096               # max tracked keys (prevents unbounded growth)
_FEISHU_WEBHOOK_BODY_TIMEOUT_SECONDS = 30          # max seconds to read request body
_FEISHU_API_TIMEOUT_SECONDS = 12.0
_FEISHU_WEBHOOK_ANOMALY_THRESHOLD = 25             # consecutive error responses before WARNING log
_FEISHU_WEBHOOK_ANOMALY_TTL_SECONDS = 6 * 60 * 60  # anomaly tracker TTL (6 hours) — matches openclaw
_FEISHU_CARD_ACTION_DEDUP_TTL_SECONDS = 15 * 60    # card action token dedup window (15 min)
_FEISHU_APPROVAL_STATE_TTL_SECONDS = 10 * 60        # stale approval button state retention window

_APPROVAL_CHOICE_MAP: Dict[str, str] = {
    "approve_once": "once",
    "approve_session": "session",
    "approve_always": "always",
    "deny": "deny",
    "grant_senior": "grant_senior",
    "grant_permission": "grant_permission",
    "select_requested_role": "select_requested_role",
}
_APPROVAL_LABEL_MAP: Dict[str, str] = {
    "once": "Approved once",
    "session": "Approved for session",
    "always": "Approved always",
    "deny": "Denied",
    "grant_senior": "Granted senior access",
    "grant_permission": "Granted permission",
    "select_requested_role": "Requested role updated",
}
async def _read_limited_feishu_webhook_body(request: Any, max_bytes: int) -> bytes:
    """Read at most ``max_bytes`` from an aiohttp request body."""
    try:
        body = await request.content.readexactly(max_bytes + 1)
    except asyncio.IncompleteReadError as exc:
        body = exc.partial
    if len(body) > max_bytes:
        raise ValueError("payload too large")
    return body


_PERMISSION_GRANT_ROLE_OPTIONS: tuple[tuple[str, str], ...] = (
    ("owner", "Owner"),
    ("admin", "Admin"),
    ("senior", "Senior"),
    ("member", "Member"),
)
_PERMISSION_GRANT_ALLOWED_ROLES = {role for role, _label in _PERMISSION_GRANT_ROLE_OPTIONS}
_PERMISSION_REQUEST_TEXT_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"(?:开通|申请|给我|帮我).{0,8}(?:权限|访问|角色)", re.IGNORECASE),
    re.compile(r"(?:我是|我叫|叫我).{1,20}(?:开通|申请).{0,8}(?:权限|访问|角色)", re.IGNORECASE),
    re.compile(r"permission\s*(?:grant|access|role)", re.IGNORECASE),
)
_PERMISSION_REQUEST_ROLE_HINTS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"(?:高级用户|高级权限|高级角色|senior|高级)", re.IGNORECASE), "senior"),
    (re.compile(r"(?:管理员|管理权限|admin)", re.IGNORECASE), "admin"),
    (re.compile(r"(?:普通用户|普通权限|member)", re.IGNORECASE), "member"),
)
_FEISHU_PERMISSION_REQUEST_DEDUP_TTL_SECONDS = 5 * 60
_PERMISSION_REQUEST_ACK_TEXT = "已收到你的权限申请，正在等待管理员审批。审批通过后我会继续为你开通。"
_DIRECT_PERMISSION_GRANT_TEXT_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"(?:给|帮)\s*[^\s，,。]{1,20}\s*(?:开通|开|授权).{0,12}(?:权限|访问|角色)", re.IGNORECASE),
    re.compile(r"(?:grant|authorize)\s+[^\s，,。]{1,40}\s+(?:permission|access|role)", re.IGNORECASE),
)

_FEISHU_ACK_EMOJI = "OK"
_FEISHU_BOT_MSG_TRACK_SIZE = 512                   # LRU size for tracking sent message IDs
_FEISHU_REPLY_FALLBACK_CODES = frozenset({2200, 230011, 231003})  # reply target/thread rejected → create fallback

# Feishu reactions render as prominent badges, unlike Discord/Telegram's
# small footer emoji — a success badge on every message would add noise, so
# we only mark start (Typing) and failure (CrossMark); the reply itself is
# the success signal.
_FEISHU_REACTION_IN_PROGRESS = "Typing"
_FEISHU_REACTION_FAILURE = "CrossMark"
# Bound on the (message_id → reaction_id) handle cache. Happy-path entries
# drain on completion; the cap is a safeguard against unbounded growth from
# delete-failures, not a capacity plan.
_FEISHU_PROCESSING_REACTION_CACHE_SIZE = 1024
_FEISHU_MESSAGE_TEXT_CACHE_SIZE = 512       # LRU cap for reply-context message text lookups

# QR onboarding constants
_ONBOARD_ACCOUNTS_URLS = {
    "feishu": "https://accounts.feishu.cn",
    "lark": "https://accounts.larksuite.com",
}
_ONBOARD_OPEN_URLS = {
    "feishu": "https://open.feishu.cn",
    "lark": "https://open.larksuite.com",
}
_REGISTRATION_PATH = "/oauth/v1/app/registration"
_ONBOARD_REQUEST_TIMEOUT_S = 10

# ---------------------------------------------------------------------------
# Fallback display strings
# ---------------------------------------------------------------------------

FALLBACK_POST_TEXT = "[Rich text message]"
FALLBACK_FORWARD_TEXT = "[Merged forward message]"
FALLBACK_SHARE_CHAT_TEXT = "[Shared chat]"
FALLBACK_INTERACTIVE_TEXT = "[Interactive message]"
FALLBACK_IMAGE_TEXT = "[Image]"
FALLBACK_ATTACHMENT_TEXT = "[Attachment]"
# ---------------------------------------------------------------------------
# Post/card parsing helpers
# ---------------------------------------------------------------------------

_PREFERRED_LOCALES = ("zh_cn", "en_us")
_MARKDOWN_SPECIAL_CHARS_RE = re.compile(r"([\\`*_{}\[\]()#+\-!|>~])")
_MENTION_PLACEHOLDER_RE = re.compile(r"@_user_\d+")
_MENTION_BOUNDARY_CHARS = frozenset(" \t\n\r.,;:!?、，。；：！？()[]{}<>\"'`")
_TRAILING_TERMINAL_PUNCT = frozenset(" \t\n\r.!?。！？")
_WHITESPACE_RE = re.compile(r"\s+")
_SUPPORTED_CARD_TEXT_KEYS = (
    "title",
    "text",
    "content",
    "label",
    "value",
    "name",
    "summary",
    "subtitle",
    "description",
    "placeholder",
    "hint",
)
_SKIP_TEXT_KEYS = {
    "tag",
    "type",
    "msg_type",
    "message_type",
    "chat_id",
    "open_chat_id",
    "share_chat_id",
    "file_key",
    "image_key",
    "user_id",
    "open_id",
    "union_id",
    "url",
    "href",
    "link",
    "token",
    "template",
    "locale",
}


@dataclass(frozen=True)
class FeishuPostMediaRef:
    file_key: str
    file_name: str = ""
    resource_type: str = "file"


@dataclass(frozen=True)
class FeishuMentionRef:
    name: str = ""
    open_id: str = ""
    is_all: bool = False
    is_self: bool = False


@dataclass(frozen=True)
class _FeishuBotIdentity:
    open_id: str = ""
    user_id: str = ""
    name: str = ""

    def matches(self, *, open_id: str, user_id: str, name: str) -> bool:
        mention_open_id = str(open_id or "").strip()
        mention_user_id = str(user_id or "").strip()
        mention_name = str(name or "").strip()
        bot_open_id = str(self.open_id or "").strip()
        bot_user_id = str(self.user_id or "").strip()
        bot_name = str(self.name or "").strip()

        # IDs are authoritative. An open_id comparison is decisive when both
        # sides provide one; otherwise a comparable user_id is decisive. Name
        # fallback is safe only when neither side has any usable ID.
        if mention_open_id and bot_open_id:
            return mention_open_id == bot_open_id
        if mention_user_id and bot_user_id:
            return mention_user_id == bot_user_id
        if mention_open_id or mention_user_id or bot_open_id or bot_user_id:
            return False
        return bool(bot_name) and mention_name == bot_name


@dataclass(frozen=True)
class FeishuPostParseResult:
    text_content: str
    image_keys: List[str] = field(default_factory=list)
    media_refs: List[FeishuPostMediaRef] = field(default_factory=list)


@dataclass(frozen=True)
class FeishuNormalizedMessage:
    raw_type: str
    text_content: str
    preferred_message_type: str = "text"
    image_keys: List[str] = field(default_factory=list)
    media_refs: List[FeishuPostMediaRef] = field(default_factory=list)
    mentions: List[FeishuMentionRef] = field(default_factory=list)
    relation_kind: str = "plain"
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FeishuResourceDownloadResult:
    path: str = ""
    media_type: str = ""
    warning: str = ""


@dataclass(frozen=True)
class FeishuAdapterSettings:
    app_id: str  # Canonical bot/app identifier (credential, not from event payloads)
    app_secret: str
    domain_name: str
    connection_mode: str
    encrypt_key: str
    verification_token: str
    group_policy: str
    allowed_group_users: frozenset[str]
    # Bot's own open_id (app-scoped) — returned by /bot/v3/info.  Used only for
    # @mention matching: Feishu puts this value in mentions[].id.open_id when
    # a user @-mentions the bot in a group chat.
    bot_open_id: str
    # Bot's user_id (tenant-scoped) — optional, used as fallback mention match.
    bot_user_id: str
    bot_name: str
    dedup_cache_size: int
    text_batch_delay_seconds: float
    text_batch_split_delay_seconds: float
    text_batch_max_messages: int
    text_batch_max_chars: int
    media_batch_delay_seconds: float
    webhook_host: str
    webhook_port: int
    webhook_path: str
    ws_reconnect_nonce: int = 30
    ws_reconnect_interval: int = 120
    ws_ping_interval: Optional[int] = None
    ws_ping_timeout: Optional[int] = None
    admins: frozenset[str] = frozenset()
    default_group_policy: str = ""
    group_rules: Dict[str, FeishuGroupRule] = field(default_factory=dict)
    allow_bots: str = "none"  # "none" | "mentions" | "all"
    require_mention: bool = True


@dataclass
class FeishuGroupRule:
    """Per-group policy rule for controlling which users may interact with the bot."""

    policy: str  # "open" | "allowlist" | "blacklist" | "admin_only" | "disabled"
    allowlist: set[str] = field(default_factory=set)
    blacklist: set[str] = field(default_factory=set)
    require_mention: Optional[bool] = None  # None = inherit global


@dataclass
class FeishuBatchState:
    events: Dict[str, MessageEvent] = field(default_factory=dict)
    tasks: Dict[str, asyncio.Task] = field(default_factory=dict)
    counts: Dict[str, int] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Admission: policy types
# ---------------------------------------------------------------------------


RejectReason = Literal[
    "self_echo",
    "self_ids_unknown",
    "bots_disabled",
    "bot_not_mentioned",
    "group_policy_rejected",
]


def _is_bot_sender(sender: Any) -> bool:
    # receive_v1 docs say {user, bot}; accept "app" defensively.
    return getattr(sender, "sender_type", "") in {"bot", "app"}


def _sender_identity(sender: Any) -> frozenset:
    # Take any non-empty id variant — tenant sender_id_type decides which are populated.
    sid = getattr(sender, "sender_id", None)
    if sid is None:
        return frozenset()
    return frozenset(
        v for v in (
            getattr(sid, "open_id", None),
            getattr(sid, "user_id", None),
            getattr(sid, "union_id", None),
        )
        if v
    )


# ---------------------------------------------------------------------------
# Markdown rendering helpers
# ---------------------------------------------------------------------------


def _escape_markdown_text(text: str) -> str:
    return _MARKDOWN_SPECIAL_CHARS_RE.sub(r"\\\1", text)


def _to_boolean(value: Any) -> bool:
    return value is True or value == 1 or value == "true"


def _is_style_enabled(style: Dict[str, Any] | None, key: str) -> bool:
    if not style:
        return False
    return _to_boolean(style.get(key))


def _wrap_inline_code(text: str) -> str:
    max_run = max([0, *[len(run) for run in re.findall(r"`+", text)]])
    fence = "`" * (max_run + 1)
    body = f" {text} " if text.startswith("`") or text.endswith("`") else text
    return f"{fence}{body}{fence}"


def _sanitize_fence_language(language: str) -> str:
    return language.strip().replace("\n", " ").replace("\r", " ")


def _render_text_element(element: Dict[str, Any]) -> str:
    text = str(element.get("text", "") or "")
    style = element.get("style")
    style_dict = style if isinstance(style, dict) else None

    if _is_style_enabled(style_dict, "code"):
        return _wrap_inline_code(text)

    rendered = _escape_markdown_text(text)
    if not rendered:
        return ""
    if _is_style_enabled(style_dict, "bold"):
        rendered = f"**{rendered}**"
    if _is_style_enabled(style_dict, "italic"):
        rendered = f"*{rendered}*"
    if _is_style_enabled(style_dict, "underline"):
        rendered = f"<u>{rendered}</u>"
    if _is_style_enabled(style_dict, "strikethrough"):
        rendered = f"~~{rendered}~~"
    return rendered


def _render_code_block_element(element: Dict[str, Any]) -> str:
    language = _sanitize_fence_language(
        str(element.get("language", "") or "") or str(element.get("lang", "") or "")
    )
    code = (
        str(element.get("text", "") or "") or str(element.get("content", "") or "")
    ).replace("\r\n", "\n")
    trailing_newline = "" if code.endswith("\n") else "\n"
    return f"```{language}\n{code}{trailing_newline}```"


def _strip_markdown_to_plain_text(text: str) -> str:
    """Strip markdown formatting to plain text for Feishu text fallbacks.

    Delegates common markdown stripping to the shared helper and adds
    Feishu-specific patterns (blockquotes, strikethrough, underline tags,
    horizontal rules, \\r\\n normalisation).
    """
    from gateway.platforms.helpers import strip_markdown
    plain = text.replace("\r\n", "\n")
    plain = _MARKDOWN_LINK_RE.sub(lambda m: f"{m.group(1)} ({m.group(2).strip()})", plain)
    plain = re.sub(r"^>\s?", "", plain, flags=re.MULTILINE)
    plain = re.sub(r"^\s*---+\s*$", "---", plain, flags=re.MULTILINE)
    plain = re.sub(r"~~([^~\n]+)~~", r"\1", plain)
    plain = re.sub(r"<u>([\s\S]*?)</u>", r"\1", plain)
    plain = strip_markdown(plain)
    return plain


def _coerce_int(value: Any, default: Optional[int] = None, min_value: int = 0) -> Optional[int]:
    """Coerce value to int with optional default and minimum constraint."""
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed >= min_value else default


def _coerce_required_int(value: Any, default: int, min_value: int = 0) -> int:
    parsed = _coerce_int(value, default=default, min_value=min_value)
    return default if parsed is None else parsed


# ---------------------------------------------------------------------------
# Post payload builders and parsers
# ---------------------------------------------------------------------------


def _build_markdown_post_payload(content: str) -> str:
    rows = _build_markdown_post_rows(content)
    return json.dumps(
        {
            "zh_cn": {
                "content": rows,
            }
        },
        ensure_ascii=False,
    )


def _build_markdown_post_rows(content: str) -> List[List[Dict[str, str]]]:
    """Build Feishu post rows while isolating fenced code blocks.

    Feishu's `md` renderer can swallow trailing content when a fenced code block
    appears inside one large markdown element. Split the reply at real fence
    lines so prose before/after the code block remains visible while code stays
    in a dedicated row.
    """
    if not content:
        return [[{"tag": "md", "text": ""}]]
    if "```" not in content:
        return [[{"tag": "md", "text": content}]]

    rows: List[List[Dict[str, str]]] = []
    current: List[str] = []
    in_code_block = False

    def _flush_current() -> None:
        nonlocal current
        if not current:
            return
        segment = "\n".join(current)
        if segment.strip():
            rows.append([{"tag": "md", "text": segment}])
        current = []

    for raw_line in content.splitlines():
        stripped_line = raw_line.strip()
        is_fence = bool(
            _MARKDOWN_FENCE_CLOSE_RE.match(stripped_line)
            if in_code_block
            else _MARKDOWN_FENCE_OPEN_RE.match(stripped_line)
        )

        if is_fence:
            if not in_code_block:
                _flush_current()
            current.append(raw_line)
            in_code_block = not in_code_block
            if not in_code_block:
                _flush_current()
            continue

        current.append(raw_line)

    _flush_current()
    return rows or [[{"tag": "md", "text": content}]]


def parse_feishu_post_payload(
    payload: Any,
    *,
    mentions_map: Optional[Dict[str, FeishuMentionRef]] = None,
) -> FeishuPostParseResult:
    resolved = _resolve_post_payload(payload)
    if not resolved:
        return FeishuPostParseResult(text_content=FALLBACK_POST_TEXT)

    image_keys: List[str] = []
    media_refs: List[FeishuPostMediaRef] = []
    parts: List[str] = []

    title = _normalize_feishu_text(str(resolved.get("title", "")).strip())
    if title:
        parts.append(title)

    for row in resolved.get("content", []) or []:
        if not isinstance(row, list):
            continue
        row_text = _normalize_feishu_text(
            "".join(
                _render_post_element(item, image_keys, media_refs, mentions_map)
                for item in row
            )
        )
        if row_text:
            parts.append(row_text)

    return FeishuPostParseResult(
        text_content="\n".join(parts).strip() or FALLBACK_POST_TEXT,
        image_keys=image_keys,
        media_refs=media_refs,
    )


def _resolve_post_payload(payload: Any) -> Dict[str, Any]:
    direct = _to_post_payload(payload)
    if direct:
        return direct
    if not isinstance(payload, dict):
        return {}

    wrapped = payload.get("post")
    wrapped_direct = _resolve_locale_payload(wrapped)
    if wrapped_direct:
        return wrapped_direct
    return _resolve_locale_payload(payload)


def _resolve_locale_payload(payload: Any) -> Dict[str, Any]:
    direct = _to_post_payload(payload)
    if direct:
        return direct
    if not isinstance(payload, dict):
        return {}

    for key in _PREFERRED_LOCALES:
        candidate = _to_post_payload(payload.get(key))
        if candidate:
            return candidate
    for value in payload.values():
        candidate = _to_post_payload(value)
        if candidate:
            return candidate
    return {}


def _to_post_payload(candidate: Any) -> Dict[str, Any]:
    if not isinstance(candidate, dict):
        return {}
    content = candidate.get("content")
    if not isinstance(content, list):
        return {}
    return {
        "title": str(candidate.get("title", "") or ""),
        "content": content,
    }


def _render_post_element(
    element: Any,
    image_keys: List[str],
    media_refs: List[FeishuPostMediaRef],
    mentions_map: Optional[Dict[str, FeishuMentionRef]] = None,
) -> str:
    if isinstance(element, str):
        return element
    if not isinstance(element, dict):
        return ""

    tag = str(element.get("tag", "")).strip().lower()
    if tag == "text":
        return _render_text_element(element)
    if tag == "a":
        href = str(element.get("href", "")).strip()
        label = str(element.get("text", href) or "").strip()
        if not label:
            return ""
        escaped_label = _escape_markdown_text(label)
        return f"[{escaped_label}]({href})" if href else escaped_label
    if tag == "at":
        # Post <at>.user_id is a placeholder ("@_user_N" or "@_all"); look up
        # the real ref in mentions_map for the display name.
        placeholder = str(element.get("user_id", "")).strip()
        if placeholder == "@_all":
            # Feishu SDK sometimes omits @_all from the top-level mentions
            # payload; record it here so the caller's mention list stays complete.
            if mentions_map is not None and "@_all" not in mentions_map:
                mentions_map["@_all"] = FeishuMentionRef(is_all=True)
            return "@all"
        ref = (mentions_map or {}).get(placeholder)
        if ref is not None:
            display_name = ref.name or ref.open_id or "user"
        else:
            open_id = str(element.get("open_id", "")).strip()
            display_name = str(element.get("user_name", "")).strip() or "user"
            if mentions_map is not None and (open_id or display_name):
                mentions_map[placeholder or open_id or display_name] = FeishuMentionRef(
                    name=display_name,
                    open_id=open_id,
                )
        return f"@{_escape_markdown_text(display_name)}"
    if tag in {"img", "image"}:
        image_key = str(element.get("image_key", "")).strip()
        if image_key and image_key not in image_keys:
            image_keys.append(image_key)
        alt = str(element.get("text", "")).strip() or str(element.get("alt", "")).strip()
        return f"[Image: {alt}]" if alt else "[Image]"
    if tag in {"media", "file", "audio", "video"}:
        file_key = str(element.get("file_key", "")).strip()
        file_name = (
            str(element.get("file_name", "")).strip()
            or str(element.get("title", "")).strip()
            or str(element.get("text", "")).strip()
        )
        if file_key:
            media_refs.append(
                FeishuPostMediaRef(
                    file_key=file_key,
                    file_name=file_name,
                    resource_type=tag if tag in {"audio", "video"} else "file",
                )
            )
        return f"[Attachment: {file_name}]" if file_name else "[Attachment]"
    if tag in {"emotion", "emoji"}:
        label = str(element.get("text", "")).strip() or str(element.get("emoji_type", "")).strip()
        return f":{_escape_markdown_text(label)}:" if label else "[Emoji]"
    if tag == "br":
        return "\n"
    if tag in {"hr", "divider"}:
        return "\n\n---\n\n"
    if tag == "code":
        code = str(element.get("text", "") or "") or str(element.get("content", "") or "")
        return _wrap_inline_code(code) if code else ""
    if tag in {"code_block", "pre"}:
        return _render_code_block_element(element)

    nested_parts: List[str] = []
    for key in ("text", "title", "content", "children", "elements"):
        extracted = _render_nested_post(element.get(key), image_keys, media_refs, mentions_map)
        if extracted:
            nested_parts.append(extracted)
    return " ".join(part for part in nested_parts if part)


def _render_nested_post(
    value: Any,
    image_keys: List[str],
    media_refs: List[FeishuPostMediaRef],
    mentions_map: Optional[Dict[str, FeishuMentionRef]] = None,
) -> str:
    if isinstance(value, str):
        return _escape_markdown_text(value)
    if isinstance(value, list):
        return " ".join(
            part
            for item in value
            for part in [_render_nested_post(item, image_keys, media_refs, mentions_map)]
            if part
        )
    if isinstance(value, dict):
        direct = _render_post_element(value, image_keys, media_refs, mentions_map)
        if direct:
            return direct
        return " ".join(
            part
            for item in value.values()
            for part in [_render_nested_post(item, image_keys, media_refs, mentions_map)]
            if part
        )
    return ""


# ---------------------------------------------------------------------------
# Message normalization
# ---------------------------------------------------------------------------


def normalize_feishu_message(
    *,
    message_type: str,
    raw_content: str,
    mentions: Optional[Sequence[Any]] = None,
    bot: _FeishuBotIdentity = _FeishuBotIdentity(),
) -> FeishuNormalizedMessage:
    normalized_type = str(message_type or "").strip().lower()
    payload = _load_feishu_payload(raw_content)
    mentions_map = _build_mentions_map(mentions, bot)

    if normalized_type == "text":
        text = str(payload.get("text", "") or "")
        # Feishu SDK sometimes omits @_all from the mentions payload even when
        # the text literal contains it (confirmed via im.v1.message.get).
        if "@_all" in text and "@_all" not in mentions_map:
            mentions_map["@_all"] = FeishuMentionRef(is_all=True)
        return FeishuNormalizedMessage(
            raw_type=normalized_type,
            text_content=_normalize_feishu_text(text, mentions_map),
            metadata={"link_urls": _collect_feishu_link_urls(payload)},
            mentions=list(mentions_map.values()),
        )
    if normalized_type == "post":
        # The walker writes back to mentions_map if it encounters
        # <at user_id="@_all">, so reading .values() after parsing is enough.
        parsed_post = parse_feishu_post_payload(payload, mentions_map=mentions_map)
        return FeishuNormalizedMessage(
            raw_type=normalized_type,
            text_content=parsed_post.text_content,
            image_keys=list(parsed_post.image_keys),
            media_refs=list(parsed_post.media_refs),
            mentions=list(mentions_map.values()),
            relation_kind="post",
            metadata={"link_urls": _collect_feishu_link_urls(payload)},
        )
    mention_refs = list(mentions_map.values())
    if normalized_type == "image":
        image_key = str(payload.get("image_key", "") or "").strip()
        alt_text = _normalize_feishu_text(
            str(payload.get("text", "") or "")
            or str(payload.get("alt", "") or "")
            or FALLBACK_IMAGE_TEXT,
            mentions_map,
        )
        return FeishuNormalizedMessage(
            raw_type=normalized_type,
            text_content=alt_text if alt_text != FALLBACK_IMAGE_TEXT else "",
            preferred_message_type="photo",
            image_keys=[image_key] if image_key else [],
            relation_kind="image",
            mentions=mention_refs,
        )
    if normalized_type in {"file", "audio", "media"}:
        media_ref = _build_media_ref_from_payload(payload, resource_type=normalized_type)
        placeholder = _attachment_placeholder(media_ref.file_name)
        return FeishuNormalizedMessage(
            raw_type=normalized_type,
            text_content="",
            preferred_message_type="audio" if normalized_type == "audio" else "document",
            media_refs=[media_ref] if media_ref.file_key else [],
            relation_kind=normalized_type,
            metadata={"placeholder_text": placeholder},
            mentions=mention_refs,
        )
    if normalized_type == "folder":
        folder_name = _first_non_empty_text(
            payload.get("file_name"),
            payload.get("folder_name"),
            payload.get("title"),
            payload.get("text"),
        )
        warning = _feishu_folder_unsupported_warning(folder_name)
        return FeishuNormalizedMessage(
            raw_type=normalized_type,
            text_content=warning,
            preferred_message_type="text",
            relation_kind="folder",
            metadata={
                "file_key": str(payload.get("file_key", "") or "").strip(),
                "file_name": folder_name,
                "warning": warning,
                "unsupported_feishu_folder": True,
            },
        )
    if normalized_type == "merge_forward":
        return _normalize_merge_forward_message(payload)
    if normalized_type == "share_chat":
        return _normalize_share_chat_message(payload)
    if normalized_type in {"interactive", "card"}:
        return _normalize_interactive_message(normalized_type, payload)

    return FeishuNormalizedMessage(raw_type=normalized_type, text_content="")


def _load_feishu_payload(raw_content: str) -> Dict[str, Any]:
    try:
        parsed = json.loads(raw_content) if raw_content else {}
    except json.JSONDecodeError:
        return {"text": raw_content}
    return parsed if isinstance(parsed, dict) else {"content": parsed}


def _collect_feishu_link_urls(value: Any) -> List[str]:
    """Collect Feishu/Lark link URLs carried in rich-message payload blocks.

    Some Feishu Project cards render as clickable cards in the client while the
    plain text delivered to Hermes only contains the title/field labels.  Keep a
    small URL side channel so business routers can recover issue ids without
    storing the full raw payload in downstream receipts.
    """
    urls: List[str] = []

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            for key, item in node.items():
                if str(key).lower() in {"href", "url", "link"} and isinstance(item, str):
                    text = item.strip()
                    if text.startswith(("http://", "https://")):
                        urls.append(text)
                walk(item)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(value)
    seen: set[str] = set()
    out: List[str] = []
    for url in urls:
        if url not in seen:
            seen.add(url)
            out.append(url)
    return out[:20]


def _normalize_merge_forward_message(payload: Dict[str, Any]) -> FeishuNormalizedMessage:
    title = _first_non_empty_text(
        payload.get("title"),
        payload.get("summary"),
        payload.get("preview"),
        _find_first_text(payload, keys=("title", "summary", "preview", "description")),
    )
    entries = _collect_forward_entries(payload)
    lines: List[str] = []
    if title:
        lines.append(title)
    lines.extend(entries[:8])
    text_content = "\n".join(lines).strip() or FALLBACK_FORWARD_TEXT
    return FeishuNormalizedMessage(
        raw_type="merge_forward",
        text_content=text_content,
        relation_kind="merge_forward",
        metadata={"entry_count": len(entries), "title": title},
    )


def _normalize_share_chat_message(payload: Dict[str, Any]) -> FeishuNormalizedMessage:
    chat_name = _first_non_empty_text(
        payload.get("chat_name"),
        payload.get("name"),
        payload.get("title"),
        _find_first_text(payload, keys=("chat_name", "name", "title")),
    )
    share_id = _first_non_empty_text(
        payload.get("chat_id"),
        payload.get("open_chat_id"),
        payload.get("share_chat_id"),
    )
    lines = []
    if chat_name:
        lines.append(f"Shared chat: {chat_name}")
    else:
        lines.append(FALLBACK_SHARE_CHAT_TEXT)
    if share_id:
        lines.append(f"Chat ID: {share_id}")
    text_content = "\n".join(lines)
    return FeishuNormalizedMessage(
        raw_type="share_chat",
        text_content=text_content,
        relation_kind="share_chat",
        metadata={"chat_id": share_id, "chat_name": chat_name},
    )


def _normalize_interactive_message(message_type: str, payload: Dict[str, Any]) -> FeishuNormalizedMessage:
    card_payload = payload.get("card") if isinstance(payload.get("card"), dict) else payload
    title = _first_non_empty_text(
        _find_header_title(card_payload),
        payload.get("title"),
        _find_first_text(card_payload, keys=("title", "summary", "subtitle")),
    )
    body_lines = _collect_card_lines(card_payload)
    actions = _collect_action_labels(card_payload)

    lines: List[str] = []
    if title:
        lines.append(title)
    for line in body_lines:
        if line != title:
            lines.append(line)
    if actions:
        lines.append(f"Actions: {', '.join(actions)}")

    text_content = "\n".join(lines[:12]).strip() or FALLBACK_INTERACTIVE_TEXT
    return FeishuNormalizedMessage(
        raw_type=message_type,
        text_content=text_content,
        relation_kind="interactive",
        metadata={"title": title, "actions": actions},
    )


# ---------------------------------------------------------------------------
# Content extraction utilities (card / forward / text walking)
# ---------------------------------------------------------------------------


def _collect_forward_entries(payload: Dict[str, Any]) -> List[str]:
    candidates: List[Any] = []
    for key in ("messages", "items", "message_list", "records", "content"):
        value = payload.get(key)
        if isinstance(value, list):
            candidates.extend(value)
    entries: List[str] = []
    for item in candidates:
        if not isinstance(item, dict):
            text = _normalize_feishu_text(str(item or ""))
            if text:
                entries.append(f"- {text}")
            continue
        sender = _first_non_empty_text(
            item.get("sender_name"),
            item.get("user_name"),
            item.get("sender"),
            item.get("name"),
        )
        nested_type = str(item.get("message_type", "") or item.get("msg_type", "")).strip().lower()
        if nested_type == "post":
            body = parse_feishu_post_payload(item.get("content") or item).text_content
        else:
            body = _first_non_empty_text(
                item.get("text"),
                item.get("summary"),
                item.get("preview"),
                item.get("content"),
                _find_first_text(item, keys=("text", "content", "summary", "preview", "title")),
            )
        body = _normalize_feishu_text(body)
        if sender and body:
            entries.append(f"- {sender}: {body}")
        elif body:
            entries.append(f"- {body}")
    return _unique_lines(entries)


def _collect_card_lines(payload: Any) -> List[str]:
    lines = _collect_text_segments(payload, in_rich_block=False)
    normalized = [_normalize_feishu_text(line) for line in lines]
    return _unique_lines([line for line in normalized if line])


def _collect_action_labels(payload: Any) -> List[str]:
    labels: List[str] = []
    for item in _walk_nodes(payload):
        if not isinstance(item, dict):
            continue
        tag = str(item.get("tag", "") or item.get("type", "")).strip().lower()
        if tag not in {"button", "select_static", "overflow", "date_picker", "picker"}:
            continue
        label = _first_non_empty_text(
            item.get("text"),
            item.get("name"),
            item.get("value"),
            _find_first_text(item, keys=("text", "content", "name", "value")),
        )
        if label:
            labels.append(label)
    return _unique_lines(labels)


def _collect_text_segments(value: Any, *, in_rich_block: bool) -> List[str]:
    if isinstance(value, str):
        return [_normalize_feishu_text(value)] if in_rich_block else []
    if isinstance(value, list):
        segments: List[str] = []
        for item in value:
            segments.extend(_collect_text_segments(item, in_rich_block=in_rich_block))
        return segments
    if not isinstance(value, dict):
        return []

    tag = str(value.get("tag", "") or value.get("type", "")).strip().lower()
    next_in_rich_block = in_rich_block or tag in {
        "plain_text",
        "lark_md",
        "markdown",
        "note",
        "div",
        "column_set",
        "column",
        "action",
        "button",
        "select_static",
        "date_picker",
    }

    segments: List[str] = []
    for key in _SUPPORTED_CARD_TEXT_KEYS:
        item = value.get(key)
        if isinstance(item, str) and next_in_rich_block:
            normalized = _normalize_feishu_text(item)
            if normalized:
                segments.append(normalized)

    for key, item in value.items():
        if key in _SKIP_TEXT_KEYS:
            continue
        segments.extend(_collect_text_segments(item, in_rich_block=next_in_rich_block))
    return segments


def _build_media_ref_from_payload(payload: Dict[str, Any], *, resource_type: str) -> FeishuPostMediaRef:
    file_key = str(payload.get("file_key", "") or "").strip()
    file_name = _first_non_empty_text(
        payload.get("file_name"),
        payload.get("title"),
        payload.get("text"),
    )
    effective_type = resource_type if resource_type in {"audio", "video"} else "file"
    return FeishuPostMediaRef(file_key=file_key, file_name=file_name, resource_type=effective_type)


def _attachment_placeholder(file_name: str) -> str:
    normalized_name = _normalize_feishu_text(file_name)
    return f"[Attachment: {normalized_name}]" if normalized_name else FALLBACK_ATTACHMENT_TEXT


def _feishu_folder_unsupported_warning(folder_name: str = "") -> str:
    normalized_name = _normalize_feishu_text(folder_name)
    prefix = f"[Folder: {normalized_name}]\n" if normalized_name else "[Folder]\n"
    return (
        prefix
        + "飞书文件夹不能通过消息附件接口直接下载；请发 zip 或提供 VM/NAS 路径。"
        + "中大文件请放到 /mnt/tmp/<task_id>/，对外路径为 //hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/<task_id>/。"
        + "若要支持文件夹读取，需要开通 Drive scopes 并走 Drive API。"
    )


def _feishu_resource_warning(code: Any, file_name: str = "") -> str:
    try:
        numeric_code = int(code)
    except (TypeError, ValueError):
        return ""
    warning = _FEISHU_KNOWN_RESOURCE_WARNINGS.get(numeric_code, "")
    if not warning:
        return ""
    normalized_name = _normalize_feishu_text(file_name)
    return f"{normalized_name}: {warning}" if normalized_name else warning


def _configured_feishu_max_file_bytes() -> int:
    raw_value = os.environ.get("HERMES_FEISHU_MAX_FILE_BYTES", "")
    if not raw_value:
        return _DEFAULT_FEISHU_MAX_FILE_BYTES
    try:
        parsed = int(raw_value)
    except ValueError:
        logger.warning("[Feishu] Ignoring invalid HERMES_FEISHU_MAX_FILE_BYTES=%r", raw_value)
        return _DEFAULT_FEISHU_MAX_FILE_BYTES
    if parsed == 0:
        return 0
    if parsed < 0:
        logger.warning("[Feishu] Ignoring negative HERMES_FEISHU_MAX_FILE_BYTES=%r", raw_value)
        return _DEFAULT_FEISHU_MAX_FILE_BYTES
    return parsed


def _bounded_int_setting(
    config_value: Any,
    *,
    env_name: str,
    default: int,
    minimum: int,
    maximum: int,
) -> int:
    raw_value = os.environ.get(env_name, config_value)
    if raw_value in (None, ""):
        return default
    try:
        parsed = int(raw_value)
    except (TypeError, ValueError):
        logger.warning("[Feishu] Ignoring invalid %s=%r", env_name, raw_value)
        return default
    if parsed < minimum or parsed > maximum:
        logger.warning(
            "[Feishu] Ignoring out-of-range %s=%r (expected %d..%d)",
            env_name,
            raw_value,
            minimum,
            maximum,
        )
        return default
    return parsed


def _find_header_title(payload: Any) -> str:
    if not isinstance(payload, dict):
        return ""
    header = payload.get("header")
    if not isinstance(header, dict):
        return ""
    title = header.get("title")
    if isinstance(title, dict):
        return _first_non_empty_text(title.get("content"), title.get("text"), title.get("name"))
    return _normalize_feishu_text(str(title or ""))


def _find_first_text(payload: Any, *, keys: tuple[str, ...]) -> str:
    for node in _walk_nodes(payload):
        if not isinstance(node, dict):
            continue
        for key in keys:
            value = node.get(key)
            if isinstance(value, str):
                normalized = _normalize_feishu_text(value)
                if normalized:
                    return normalized
    return ""


def _walk_nodes(value: Any):
    if isinstance(value, dict):
        yield value
        for item in value.values():
            yield from _walk_nodes(item)
    elif isinstance(value, list):
        for item in value:
            yield from _walk_nodes(item)


def _first_non_empty_text(*values: Any) -> str:
    for value in values:
        if isinstance(value, str):
            normalized = _normalize_feishu_text(value)
            if normalized:
                return normalized
        elif value is not None and not isinstance(value, (dict, list)):
            normalized = _normalize_feishu_text(str(value))
            if normalized:
                return normalized
    return ""


# ---------------------------------------------------------------------------
# General text utilities
# ---------------------------------------------------------------------------


def _normalize_feishu_text(
    text: str,
    mentions_map: Optional[Dict[str, FeishuMentionRef]] = None,
) -> str:
    def _sub(match: "re.Match[str]") -> str:
        key = match.group(0)
        ref = (mentions_map or {}).get(key)
        if ref is None:
            return " "
        name = ref.name or ref.open_id or "user"
        return f"@{name}"

    cleaned = _MENTION_PLACEHOLDER_RE.sub(_sub, text or "")
    cleaned = cleaned.replace("@_all", "@all")
    cleaned = cleaned.replace("\r\n", "\n").replace("\r", "\n")
    cleaned = "\n".join(_WHITESPACE_RE.sub(" ", line).strip() for line in cleaned.split("\n"))
    cleaned = "\n".join(line for line in cleaned.split("\n") if line)
    cleaned = _MULTISPACE_RE.sub(" ", cleaned)
    return cleaned.strip()


def _unique_lines(lines: List[str]) -> List[str]:
    seen: set[str] = set()
    unique: List[str] = []
    for line in lines:
        if not line or line in seen:
            continue
        seen.add(line)
        unique.append(line)
    return unique


# ---------------------------------------------------------------------------
# Mention helpers
# ---------------------------------------------------------------------------


def _extract_mention_ids(mention: Any) -> tuple[str, str]:
    # Returns (open_id, user_id). im.v1.message.get hands back id as a string
    # plus id_type discriminator; event payloads hand back a nested UserId
    # object carrying both fields.
    mention_id = getattr(mention, "id", None)
    if isinstance(mention_id, str):
        id_type = str(getattr(mention, "id_type", "") or "").lower()
        if id_type == "open_id":
            return mention_id, ""
        if id_type == "user_id":
            return "", mention_id
        return "", ""
    if mention_id is None:
        return "", ""
    return (
        str(getattr(mention_id, "open_id", "") or ""),
        str(getattr(mention_id, "user_id", "") or ""),
    )


def _build_mentions_map(
    mentions: Optional[Sequence[Any]],
    bot: _FeishuBotIdentity,
) -> Dict[str, FeishuMentionRef]:
    result: Dict[str, FeishuMentionRef] = {}
    for mention in mentions or []:
        key = str(getattr(mention, "key", "") or "")
        if not key:
            continue
        if key == "@_all":
            result[key] = FeishuMentionRef(is_all=True)
            continue
        open_id, user_id = _extract_mention_ids(mention)
        name = str(getattr(mention, "name", "") or "").strip()
        result[key] = FeishuMentionRef(
            name=name,
            open_id=open_id,
            is_self=bot.matches(open_id=open_id, user_id=user_id, name=name),
        )
    return result


def _build_mention_hint(mentions: Sequence[FeishuMentionRef]) -> str:
    parts: List[str] = []
    seen: set = set()
    for ref in mentions:
        if ref.is_self:
            continue
        signature = (ref.is_all, ref.open_id, ref.name)
        if signature in seen:
            continue
        seen.add(signature)
        if ref.is_all:
            parts.append("@all")
        elif ref.open_id:
            parts.append(f"{ref.name or 'unknown'} (open_id={ref.open_id})")
        else:
            parts.append(ref.name or "unknown")
    return f"[Mentioned: {', '.join(parts)}]" if parts else ""


def _strip_edge_self_mentions(
    text: str,
    mentions: Sequence[FeishuMentionRef],
) -> str:
    # Leading: strip consecutive self-mentions unconditionally.
    # Trailing: strip only when followed by whitespace/terminal punct, so
    # mid-sentence references ("don't @Bot again") stay intact.
    # Leading word-boundary prevents @Al from eating @Alice.
    if not text:
        return text
    self_names = [
        f"@{ref.name or ref.open_id or 'user'}"
        for ref in mentions
        if ref.is_self
    ]
    if not self_names:
        return text

    remaining = text.lstrip()
    while True:
        for nm in self_names:
            if not remaining.startswith(nm):
                continue
            after = remaining[len(nm):]
            if after and after[0] not in _MENTION_BOUNDARY_CHARS:
                continue
            remaining = after.lstrip()
            break
        else:
            break

    while True:
        i = len(remaining)
        while i > 0 and remaining[i - 1] in _TRAILING_TERMINAL_PUNCT:
            i -= 1
        body = remaining[:i]
        tail = remaining[i:]
        for nm in self_names:
            if body.endswith(nm):
                remaining = body[: -len(nm)].rstrip() + tail
                break
        else:
            return remaining


def _self_mention_is_command_directed(
    text: str,
    mentions: Sequence[FeishuMentionRef],
) -> bool:
    """Return whether this bot is the first addressee in the message."""
    remaining = str(text or "").lstrip()
    if not remaining:
        return False
    for ref in mentions:
        if not getattr(ref, "is_self", False):
            continue
        mention_text = (
            f"@{getattr(ref, 'name', '') or getattr(ref, 'open_id', '') or 'user'}"
        )
        if not remaining.startswith(mention_text):
            continue
        after = remaining[len(mention_text) :]
        if not after or after[0] in _MENTION_BOUNDARY_CHARS:
            return True
    return False


def _run_official_feishu_ws_client(ws_client: Any, adapter: Any) -> None:
    """Run the official Lark WS client in its own thread-local event loop."""
    import lark_oapi.ws.client as ws_client_module

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    ws_client_module.loop = loop
    adapter._ws_thread_loop = loop

    original_connect = ws_client_module.websockets.connect
    original_configure = getattr(ws_client, "_configure", None)

    def _apply_runtime_ws_overrides() -> None:
        try:
            setattr(ws_client, "_reconnect_nonce", adapter._ws_reconnect_nonce)
            setattr(ws_client, "_reconnect_interval", adapter._ws_reconnect_interval)
            if adapter._ws_ping_interval is not None:
                setattr(ws_client, "_ping_interval", adapter._ws_ping_interval)
        except Exception:
            logger.debug("[Feishu] Failed to apply websocket runtime overrides", exc_info=True)

    def _connect_with_overrides(*args: Any, **kwargs: Any) -> Any:
        if adapter._ws_ping_interval is not None and "ping_interval" not in kwargs:
            kwargs["ping_interval"] = adapter._ws_ping_interval
        if adapter._ws_ping_timeout is not None and "ping_timeout" not in kwargs:
            kwargs["ping_timeout"] = adapter._ws_ping_timeout
        return original_connect(*args, **kwargs)

    def _configure_with_overrides(conf: Any) -> Any:
        if original_configure is None:
            raise RuntimeError("Feishu _configure_with_overrides called but original_configure is None")
        result = original_configure(conf)
        _apply_runtime_ws_overrides()
        return result

    ws_client_module.websockets.connect = _connect_with_overrides
    if original_configure is not None:
        setattr(ws_client, "_configure", _configure_with_overrides)
    _apply_runtime_ws_overrides()
    try:
        ws_client.start()
    except Exception:
        pass
    finally:
        ws_client_module.websockets.connect = original_connect
        if original_configure is not None:
            setattr(ws_client, "_configure", original_configure)
        pending = [t for t in asyncio.all_tasks(loop) if not t.done()]
        for task in pending:
            task.cancel()
        if pending:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        try:
            loop.stop()
        except Exception:
            pass
        try:
            loop.close()
        except Exception:
            pass
        adapter._ws_thread_loop = None


def check_feishu_requirements() -> bool:
    """Check if Feishu/Lark dependencies are available.

    Lazy-installs lark-oapi via ``tools.lazy_deps.ensure("platform.feishu")``
    on first call if not present. Rebinds all module-level globals on success.
    """
    if FEISHU_AVAILABLE:
        return True

    def _import():
        import lark_oapi as lark
        from lark_oapi.api.application.v6 import GetApplicationRequest
        from lark_oapi.api.im.v1 import (
            CreateFileRequest, CreateFileRequestBody,
            CreateImageRequest, CreateImageRequestBody,
            CreateMessageRequest, CreateMessageRequestBody,
            GetChatRequest, GetMessageRequest, GetMessageResourceRequest,
            P2ImMessageMessageReadV1,
            ReplyMessageRequest, ReplyMessageRequestBody,
            UpdateMessageRequest, UpdateMessageRequestBody,
        )
        from lark_oapi.core import AccessTokenType, HttpMethod
        from lark_oapi.core.const import FEISHU_DOMAIN, LARK_DOMAIN
        from lark_oapi.core.model import BaseRequest
        from lark_oapi.event.callback.model.p2_card_action_trigger import (
            CallBackCard, P2CardActionTriggerResponse,
        )
        from lark_oapi.event.dispatcher_handler import EventDispatcherHandler
        from lark_oapi.ws import Client as FeishuWSClient
        return {
            "lark": lark,
            "GetApplicationRequest": GetApplicationRequest,
            "CreateFileRequest": CreateFileRequest,
            "CreateFileRequestBody": CreateFileRequestBody,
            "CreateImageRequest": CreateImageRequest,
            "CreateImageRequestBody": CreateImageRequestBody,
            "CreateMessageRequest": CreateMessageRequest,
            "CreateMessageRequestBody": CreateMessageRequestBody,
            "GetChatRequest": GetChatRequest,
            "GetMessageRequest": GetMessageRequest,
            "GetMessageResourceRequest": GetMessageResourceRequest,
            "P2ImMessageMessageReadV1": P2ImMessageMessageReadV1,
            "ReplyMessageRequest": ReplyMessageRequest,
            "ReplyMessageRequestBody": ReplyMessageRequestBody,
            "UpdateMessageRequest": UpdateMessageRequest,
            "UpdateMessageRequestBody": UpdateMessageRequestBody,
            "AccessTokenType": AccessTokenType,
            "HttpMethod": HttpMethod,
            "FEISHU_DOMAIN": FEISHU_DOMAIN,
            "LARK_DOMAIN": LARK_DOMAIN,
            "BaseRequest": BaseRequest,
            "CallBackCard": CallBackCard,
            "P2CardActionTriggerResponse": P2CardActionTriggerResponse,
            "EventDispatcherHandler": EventDispatcherHandler,
            "FeishuWSClient": FeishuWSClient,
            "FEISHU_AVAILABLE": True,
        }

    from tools.lazy_deps import ensure_and_bind
    return ensure_and_bind("platform.feishu", _import, globals(), prompt=False)


class FeishuAdapter(BasePlatformAdapter):
    """Feishu/Lark bot adapter."""

    supports_code_blocks = True  # Feishu renders fenced code blocks
    splits_long_messages = True  # send() chunks via truncate_message(MAX_MESSAGE_LENGTH)

    MAX_MESSAGE_LENGTH = 8000
    # Max distinct chat IDs retained in _chat_locks before LRU eviction kicks in.
    CHAT_LOCK_MAX_SIZE: int = 1000
    # Threshold for detecting Feishu client-side message splits.
    # When a chunk is near the ~4096-char practical limit, a continuation
    # is almost certain.
    _SPLIT_THRESHOLD = 4000

    # =========================================================================
    # Lifecycle — init / settings / connect / disconnect
    # =========================================================================

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform.FEISHU)

        self._settings = self._load_settings(config.extra or {})
        self._apply_settings(self._settings)
        self._client: Optional[Any] = None
        # Adapter-owned thread pool for blocking Feishu SDK calls. Routing SDK
        # work through this pool (instead of asyncio's shared default executor)
        # means a torn-down default executor can no longer wedge sends with
        # "Executor shutdown has been called" — the pool is recreated on demand
        # if it has been shut down. See issue #10849.
        self._sdk_executor_lock = threading.Lock()
        self._sdk_executor: Optional[concurrent.futures.ThreadPoolExecutor] = None
        # Set on disconnect/shutdown so a real teardown can't be resurrected
        # by the recreate-on-shutdown path; cleared on connect for reconnects.
        self._sdk_executor_closing = False
        self._ws_client: Optional[Any] = None
        self._ws_future: Optional[asyncio.Future] = None
        self._ws_thread_loop: Optional[asyncio.AbstractEventLoop] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._webhook_runner: Optional[Any] = None
        self._webhook_site: Optional[Any] = None
        self._event_handler: Optional[Any] = None
        # Completed IDs are durable for the dedup TTL. Processing IDs are
        # persisted separately, but intentionally become retryable on restart.
        self._seen_message_ids: Dict[str, float] = {}
        self._seen_message_order: List[str] = []
        self._pending_dedup_message_ids: set[str] = set()
        self._processing_message_ids: Dict[str, float] = {}
        self._dedup_state_path = get_hermes_home() / "feishu_seen_message_ids.json"
        self._api_poll_state_path = get_hermes_home() / "feishu_api_poll_state_v1.json"
        self._dedup_lock = threading.Lock()
        self._sender_name_cache: Dict[str, tuple[str, float]] = {}  # sender_id → (name, expire_at)
        self._webhook_rate_counts: Dict[str, tuple[int, float]] = {}  # rate_key → (count, window_start)
        self._webhook_anomaly_counts: Dict[str, tuple[int, str, float]] = {}  # ip → (count, last_status, first_seen)
        self._card_action_tokens: Dict[str, float] = {}  # token → first_seen_time
        # Inbound events that arrived before the adapter loop was ready
        # (e.g. during startup/restart or network-flap reconnect). A single
        # drainer thread replays them as soon as the loop becomes available.
        self._pending_inbound_events: List[Any] = []
        self._pending_inbound_lock = threading.Lock()
        self._pending_drain_scheduled = False
        self._pending_inbound_max_depth = 1000  # cap queue; drop oldest beyond
        self._chat_locks: "collections.OrderedDict[str, asyncio.Lock]" = collections.OrderedDict()  # chat_id → lock (per-chat serial processing, LRU-bounded)
        self._sent_message_ids_to_chat: Dict[str, str] = {}  # message_id → chat_id (for reaction routing)
        self._sent_message_id_order: List[str] = []  # LRU order for _sent_message_ids_to_chat
        self._chat_info_cache: Dict[str, Dict[str, Any]] = {}
        self._message_text_cache: "OrderedDict[str, Optional[str]]" = OrderedDict()
        self._app_lock_identity: Optional[str] = None
        self._api_poll_chat_ids = self._parse_api_poll_chat_ids(config.extra.get("api_poll_chat_ids"))
        self._api_poll_interval_seconds = max(3.0, float(config.extra.get("api_poll_interval_seconds", 10.0) or 10.0))
        self._api_poll_page_size = _bounded_int_setting(
            config.extra.get("api_poll_page_size"),
            env_name="HERMES_FEISHU_API_POLL_PAGE_SIZE",
            default=_DEFAULT_API_POLL_PAGE_SIZE,
            minimum=1,
            maximum=_MAX_API_POLL_PAGE_SIZE,
        )
        self._api_poll_startup_lookback_seconds = _bounded_int_setting(
            config.extra.get("api_poll_startup_lookback_seconds"),
            env_name="HERMES_FEISHU_API_POLL_STARTUP_LOOKBACK_SECONDS",
            default=_DEFAULT_API_POLL_STARTUP_LOOKBACK_SECONDS,
            minimum=0,
            maximum=_MAX_API_POLL_STARTUP_LOOKBACK_SECONDS,
        )
        self._api_poll_task: Optional[asyncio.Task] = None
        self._api_poll_seen_message_ids: set[str] = set()
        self._api_poll_seen_message_order: List[str] = []
        self._api_poll_baselined_chat_ids: set[str] = set()
        self._api_poll_last_seen_create_time_ms: Dict[str, int] = {}
        self._api_poll_cursor_message_ids: Dict[str, set[str]] = {}
        self._api_poll_discovery_floor_ms: Dict[str, int] = {}
        self._api_poll_pending_items: Dict[str, List[Dict[str, Any]]] = {}
        self._api_poll_scan_state: Dict[str, Dict[str, Any]] = {}
        self._api_poll_terminal_holes: List[Dict[str, Any]] = []
        self._api_poll_state_error: Optional[str] = None
        self._api_poll_raw_state: Any = None
        self._api_poll_block_persistence = False
        self._api_poll_revision = 0
        self._api_poll_sidecar_initialized = False
        self._api_poll_app_scope = hashlib.sha256(
            f"{self._domain_name}\0{self._app_id}".encode("utf-8")
        ).hexdigest()[:32]
        self._api_poll_started_at_ms = int(time.time() * 1000)
        self._api_poll_start_cursor_ms = (
            self._api_poll_started_at_ms - (self._api_poll_startup_lookback_seconds * 1000)
        )
        self._text_batch_state = FeishuBatchState()
        self._pending_text_batches = self._text_batch_state.events
        self._pending_text_batch_tasks = self._text_batch_state.tasks
        self._pending_text_batch_counts = self._text_batch_state.counts
        self._media_batch_state = FeishuBatchState()
        self._pending_media_batches = self._media_batch_state.events
        self._pending_media_batch_tasks = self._media_batch_state.tasks
        # Exec approval button state (approval_id → {session_key, message_id, chat_id})
        self._approval_state: Dict[int, Dict[str, str]] = {}
        self._approval_counter = itertools.count(1)
        self._permission_request_seen: Dict[str, float] = {}
        self._feishu_bot_message_registry = FeishuBotMessageRegistry(max_entries=512)
        # Approval timeout callbacks (session_key → callback)
        self._approval_callbacks: Dict[str, Any] = {}
        # Update prompt button state (prompt_id → {session_key, message_id, chat_id})
        self._update_prompt_state: Dict[int, Dict[str, str]] = {}
        self._update_prompt_counter = itertools.count(1)
        # Feishu reaction deletion requires the opaque reaction_id returned
        # by create, so we cache it per message_id.
        self._pending_processing_reactions: "OrderedDict[str, str]" = OrderedDict()
        self._load_seen_message_ids()

        # Admission control (optional, enabled via config)
        self._admission_enabled = config.extra.get("admission_control_enabled", False)
        self._admission_controller: Optional[Any] = None
        self._queue_worker: Optional[Any] = None
        self._metrics_server: Optional[Any] = None
        self._metrics_exporter: Optional[Any] = None
        if self._admission_enabled:
            from gateway.admission import AdmissionController
            from gateway.admission.worker import QueueWorker
            self._admission_controller = AdmissionController()

            # Validate configuration
            is_valid, errors = self._admission_controller.validate_config()
            if not is_valid:
                logger.error("[admission] Configuration validation failed:")
                for error in errors:
                    logger.error("[admission]   - %s", error)
                logger.warning("[admission] Continuing with admission control disabled")
                self._admission_enabled = False
                self._admission_controller = None
            else:
                self._queue_worker = QueueWorker(
                    self._admission_controller,
                    self._process_queue_item
                )
                logger.info("[admission] Configuration validated successfully")

                # Auto-load policy template if specified
                template_name = config.extra.get("admission_template")
                if template_name:
                    try:
                        from gateway.admission.templates import TemplateStore
                        store = TemplateStore()
                        tpl = store.get(template_name)
                        if tpl:
                            self._admission_controller.apply_template(tpl)
                            logger.info("[admission] Applied template: %s", template_name)
                        else:
                            logger.warning("[admission] Template not found: %s", template_name)
                    except Exception as e:
                        logger.warning("[admission] Failed to load template %s: %s", template_name, e)

                # Auto-start metrics server if port specified
                metrics_port = config.extra.get("admission_metrics_port")
                if metrics_port:
                    try:
                        from gateway.admission.metrics_export import MetricsExporter
                        from gateway.admission.metrics_server import MetricsServer
                        self._metrics_exporter = MetricsExporter(self._admission_controller)
                        self._metrics_server = MetricsServer(self._metrics_exporter, port=int(metrics_port))
                        logger.info("[admission] Metrics server configured on port %s", metrics_port)
                    except Exception as e:
                        logger.warning("[admission] Failed to configure metrics server: %s", e)

    @staticmethod
    def _load_settings(extra: Dict[str, Any]) -> FeishuAdapterSettings:
        # Parse per-group rules from config
        raw_group_rules = extra.get("group_rules", {})
        group_rules: Dict[str, FeishuGroupRule] = {}
        if isinstance(raw_group_rules, dict):
            for chat_id, rule_cfg in raw_group_rules.items():
                if not isinstance(rule_cfg, dict):
                    continue
                # Only override when the key is explicitly set — missing vs false
                # must not collapse.
                per_chat_require_mention: Optional[bool] = None
                if "require_mention" in rule_cfg:
                    per_chat_require_mention = _to_boolean(rule_cfg.get("require_mention"))
                group_rules[str(chat_id)] = FeishuGroupRule(
                    policy=str(rule_cfg.get("policy", "open")).strip().lower(),
                    allowlist={str(u).strip() for u in rule_cfg.get("allowlist", []) if str(u).strip()},
                    blacklist={str(u).strip() for u in rule_cfg.get("blacklist", []) if str(u).strip()},
                    require_mention=per_chat_require_mention,
                )

        # Delivery/business group ingress is configured in two layers:
        #   1) adapter group policy (may process this group at all)
        #   2) gateway user authorization (who may use the business group)
        # ``group_allowed_chats`` was originally only read by layer 2, which
        # meant an opened business group could still be dropped before gateway
        # auth when default_group_policy=disabled and group_rules drifted. Treat
        # group_allowed_chats as an adapter-level open policy as well; explicit
        # group_rules above still win and can set require_mention/blacklist.
        raw_group_allowed_chats = extra.get("group_allowed_chats", [])
        if isinstance(raw_group_allowed_chats, str):
            group_allowed_chats = [item.strip() for item in raw_group_allowed_chats.split(",") if item.strip()]
        elif isinstance(raw_group_allowed_chats, (list, tuple, set)):
            group_allowed_chats = [str(item).strip() for item in raw_group_allowed_chats if str(item).strip()]
        else:
            group_allowed_chats = []
        for chat_id in group_allowed_chats:
            group_rules.setdefault(chat_id, FeishuGroupRule(policy="open"))

        # Bot-level admins
        raw_admins = extra.get("admins", [])
        admins = frozenset(str(u).strip() for u in raw_admins if str(u).strip())

        # Default group policy (for groups not in group_rules)
        default_group_policy = str(extra.get("default_group_policy", "")).strip().lower()

        # Env-only so adapter and gateway auth bypass share one source; yaml
        # feishu.allow_bots is bridged to this env var at config load.
        allow_bots = os.getenv("FEISHU_ALLOW_BOTS", "none").strip().lower()
        if allow_bots not in {"none", "mentions", "all"}:
            logger.warning(
                "[Feishu] Unknown allow_bots=%r, falling back to 'none'. Valid: none, mentions, all.",
                allow_bots,
            )
            allow_bots = "none"

        return FeishuAdapterSettings(
            app_id=str(extra.get("app_id") or os.getenv("FEISHU_APP_ID", "")).strip(),
            app_secret=str(extra.get("app_secret") or os.getenv("FEISHU_APP_SECRET", "")).strip(),
            domain_name=str(extra.get("domain") or os.getenv("FEISHU_DOMAIN", "feishu")).strip().lower(),
            connection_mode=str(
                extra.get("connection_mode") or os.getenv("FEISHU_CONNECTION_MODE", "websocket")
            ).strip().lower(),
            encrypt_key=str(extra.get("encrypt_key") or os.getenv("FEISHU_ENCRYPT_KEY", "")).strip(),
            verification_token=str(
                extra.get("verification_token") or os.getenv("FEISHU_VERIFICATION_TOKEN", "")
            ).strip(),
            group_policy=os.getenv("FEISHU_GROUP_POLICY", "allowlist").strip().lower(),
            allowed_group_users=frozenset(
                item.strip()
                for item in os.getenv("FEISHU_ALLOWED_USERS", "").split(",")
                if item.strip()
            ),
            bot_open_id=os.getenv("FEISHU_BOT_OPEN_ID", "").strip(),
            bot_user_id=os.getenv("FEISHU_BOT_USER_ID", "").strip(),
            bot_name=os.getenv("FEISHU_BOT_NAME", "").strip(),
            dedup_cache_size=max(
                32,
                env_int("HERMES_FEISHU_DEDUP_CACHE_SIZE", _DEFAULT_DEDUP_CACHE_SIZE),
            ),
            text_batch_delay_seconds=env_float(
                "HERMES_FEISHU_TEXT_BATCH_DELAY_SECONDS", _DEFAULT_TEXT_BATCH_DELAY_SECONDS
            ),
            text_batch_split_delay_seconds=env_float(
                "HERMES_FEISHU_TEXT_BATCH_SPLIT_DELAY_SECONDS", 2.0
            ),
            text_batch_max_messages=max(
                1,
                env_int("HERMES_FEISHU_TEXT_BATCH_MAX_MESSAGES", _DEFAULT_TEXT_BATCH_MAX_MESSAGES),
            ),
            text_batch_max_chars=max(
                1,
                env_int("HERMES_FEISHU_TEXT_BATCH_MAX_CHARS", _DEFAULT_TEXT_BATCH_MAX_CHARS),
            ),
            media_batch_delay_seconds=env_float(
                "HERMES_FEISHU_MEDIA_BATCH_DELAY_SECONDS", _DEFAULT_MEDIA_BATCH_DELAY_SECONDS
            ),
            webhook_host=str(
                extra.get("webhook_host") or os.getenv("FEISHU_WEBHOOK_HOST", _DEFAULT_WEBHOOK_HOST)
            ).strip(),
            webhook_port=int(
                extra.get("webhook_port") or os.getenv("FEISHU_WEBHOOK_PORT", str(_DEFAULT_WEBHOOK_PORT))
            ),
            webhook_path=(
                str(extra.get("webhook_path") or os.getenv("FEISHU_WEBHOOK_PATH", _DEFAULT_WEBHOOK_PATH)).strip()
                or _DEFAULT_WEBHOOK_PATH
            ),
            ws_reconnect_nonce=_coerce_required_int(extra.get("ws_reconnect_nonce"), default=30, min_value=0),
            ws_reconnect_interval=_coerce_required_int(extra.get("ws_reconnect_interval"), default=120, min_value=1),
            ws_ping_interval=_coerce_int(extra.get("ws_ping_interval"), default=None, min_value=1),
            ws_ping_timeout=_coerce_int(extra.get("ws_ping_timeout"), default=None, min_value=1),
            admins=admins,
            default_group_policy=default_group_policy,
            group_rules=group_rules,
            allow_bots=allow_bots,
            require_mention=_to_boolean(
                extra.get("require_mention", os.getenv("FEISHU_REQUIRE_MENTION", "true"))
            ),
        )

    def _apply_settings(self, settings: FeishuAdapterSettings) -> None:
        self._app_id = settings.app_id
        self._app_secret = settings.app_secret
        self._domain_name = settings.domain_name
        self._connection_mode = settings.connection_mode
        self._encrypt_key = settings.encrypt_key
        self._verification_token = settings.verification_token
        self._group_policy = settings.group_policy
        self._allowed_group_users = set(settings.allowed_group_users)
        self._admins = set(settings.admins)
        self._default_group_policy = settings.default_group_policy or settings.group_policy
        self._group_rules = settings.group_rules
        self._bot_open_id = settings.bot_open_id
        self._bot_user_id = settings.bot_user_id
        self._bot_name = settings.bot_name
        self._dedup_cache_size = settings.dedup_cache_size
        self._text_batch_delay_seconds = settings.text_batch_delay_seconds
        self._text_batch_split_delay_seconds = settings.text_batch_split_delay_seconds
        self._text_batch_max_messages = settings.text_batch_max_messages
        self._text_batch_max_chars = settings.text_batch_max_chars
        self._media_batch_delay_seconds = settings.media_batch_delay_seconds
        self._webhook_host = settings.webhook_host
        self._webhook_port = settings.webhook_port
        self._webhook_path = settings.webhook_path
        self._ws_reconnect_nonce = settings.ws_reconnect_nonce
        self._ws_reconnect_interval = settings.ws_reconnect_interval
        self._ws_ping_interval = settings.ws_ping_interval
        self._ws_ping_timeout = settings.ws_ping_timeout
        self._allow_bots = settings.allow_bots
        self._require_mention = settings.require_mention

    def _build_event_handler(self) -> Any:
        if EventDispatcherHandler is None:
            return None
        builder = EventDispatcherHandler.builder(
            self._encrypt_key,
            self._verification_token,
        )
        registrations = (
            ("register_p2_im_message_message_read_v1", self._on_message_read_event),
            ("register_p2_im_message_receive_v1", self._on_message_event),
            (
                "register_p2_im_message_reaction_created_v1",
                lambda data: self._on_reaction_event("im.message.reaction.created_v1", data),
            ),
            (
                "register_p2_im_message_reaction_deleted_v1",
                lambda data: self._on_reaction_event("im.message.reaction.deleted_v1", data),
            ),
            ("register_p2_card_action_trigger", self._on_card_action_trigger),
            ("register_p2_im_chat_member_bot_added_v1", self._on_bot_added_to_chat),
            ("register_p2_im_chat_member_bot_deleted_v1", self._on_bot_removed_from_chat),
            ("register_p2_im_chat_access_event_bot_p2p_chat_entered_v1", self._on_p2p_chat_entered),
            ("register_p2_im_message_recalled_v1", self._on_message_recalled),
        )
        for method_name, handler in registrations:
            register = getattr(builder, method_name, None)
            if register is not None:
                builder = register(handler)
        for event_key, handler in (
            ("drive.notice.comment_add_v1", self._on_drive_comment_event),
            ("vc.bot.meeting_invited_v1", self._on_meeting_invited_event),
        ):
            register_custom = getattr(builder, "register_p2_customized_event", None)
            if register_custom is not None:
                builder = register_custom(event_key, handler)
        return builder.build()

    def _get_sdk_executor(self) -> concurrent.futures.ThreadPoolExecutor:
        """Return the adapter-owned executor for blocking Feishu SDK calls.

        Recreates the pool if it was never built or was shut down by an
        *external* teardown of the loop's default executor, so that can no
        longer permanently wedge sends (#10849). Refuses to resurrect once
        the adapter itself is closing — a real disconnect/shutdown stays shut.
        """
        lock = getattr(self, "_sdk_executor_lock", None)
        if lock is None:
            lock = threading.Lock()
            self._sdk_executor_lock = lock
        with lock:
            if getattr(self, "_sdk_executor_closing", False):
                raise RuntimeError("Feishu adapter is shutting down; SDK executor unavailable")
            executor = getattr(self, "_sdk_executor", None)
            if executor is None or getattr(executor, "_shutdown", False):
                executor = concurrent.futures.ThreadPoolExecutor(
                    max_workers=10,
                    thread_name_prefix="hermes-feishu-sdk",
                )
                self._sdk_executor = executor
            return executor

    async def _run_blocking(self, func, *args):
        """Run a blocking Feishu SDK call on the adapter-owned thread pool."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._get_sdk_executor(), func, *args)

    def _shutdown_sdk_executor(self) -> None:
        """Stop the adapter-owned SDK executor without touching the loop default."""
        lock = getattr(self, "_sdk_executor_lock", None)
        if lock is None:
            return
        with lock:
            self._sdk_executor_closing = True
            executor = getattr(self, "_sdk_executor", None)
            self._sdk_executor = None
        if executor is None:
            return
        try:
            executor.shutdown(wait=False, cancel_futures=True)
        except TypeError:
            executor.shutdown(wait=False)

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        """Connect to Feishu/Lark."""
        # A fresh connect (or reconnect) re-arms the SDK executor after a prior
        # disconnect set the closing flag.
        self._sdk_executor_closing = False
        if not FEISHU_AVAILABLE:
            logger.error("[Feishu] lark-oapi not installed")
            return False
        if not self._app_id or not self._app_secret:
            logger.error("[Feishu] FEISHU_APP_ID or FEISHU_APP_SECRET not set")
            return False
        if self._connection_mode not in {"websocket", "webhook"}:
            logger.error(
                "[Feishu] Unsupported FEISHU_CONNECTION_MODE=%s. Supported modes: websocket, webhook.",
                self._connection_mode,
            )
            return False
        if self._connection_mode == "webhook" and not (self._verification_token or self._encrypt_key):
            logger.error(
                "[Feishu] Webhook mode requires FEISHU_VERIFICATION_TOKEN or FEISHU_ENCRYPT_KEY."
            )
            return False

        try:
            self._app_lock_identity = self._app_id
            acquired, existing = acquire_scoped_lock(
                _FEISHU_APP_LOCK_SCOPE,
                self._app_lock_identity,
                metadata={"platform": self.platform.value},
            )
            if not acquired:
                owner_pid = existing.get("pid") if isinstance(existing, dict) else None
                message = (
                    "Another local Hermes gateway is already using this Feishu app_id"
                    + (f" (PID {owner_pid})." if owner_pid else ".")
                    + " Stop the other gateway before starting a second Feishu websocket client."
                )
                logger.error("[Feishu] %s", message)
                self._set_fatal_error("feishu_app_lock", message, retryable=False)
                return False

            self._loop = asyncio.get_running_loop()
            await self._connect_with_retry()
            await self._establish_api_poll_startup_baselines()
            self._mark_connected()

            # Start admission queue worker if enabled
            if self._admission_enabled and self._queue_worker:
                await self._queue_worker.start()
                logger.info("[admission] Queue worker started")

            # Start metrics server if configured
            if self._metrics_server:
                self._metrics_server.start()
                logger.info("[admission] Metrics server started on port %s", self._metrics_server.port)

            self._start_api_polling_if_configured()

            logger.info("[Feishu] Connected in %s mode (%s)", self._connection_mode, self._domain_name)
            return True
        except Exception as exc:
            await self._release_app_lock()
            message = f"Feishu startup failed: {exc}"
            self._set_fatal_error("feishu_connect_error", message, retryable=True)
            logger.error("[Feishu] Failed to connect: %s", exc, exc_info=True)
            return False

    async def disconnect(self) -> None:
        """Disconnect from Feishu/Lark."""
        self._running = False

        # Stop metrics server if running
        if self._metrics_server:
            self._metrics_server.stop()
            logger.info("[admission] Metrics server stopped")

        # Stop admission queue worker if enabled
        if self._admission_enabled and self._queue_worker:
            await self._queue_worker.stop()
            logger.info("[admission] Queue worker stopped")

        await self._stop_api_polling()
        await self._cancel_pending_tasks(self._pending_text_batch_tasks)
        await self._cancel_pending_tasks(self._pending_media_batch_tasks)
        self._reset_batch_buffers()

        # Send a WebSocket CLOSE frame to Feishu BEFORE tearing down the
        # thread loop. Without this, Feishu's server never learns the
        # connection is dead and continues routing messages to the stale
        # endpoint — the channel goes silent until the server-side
        # CLOSE-WAIT expires (minutes to hours). See issue #10202.
        #
        # ``_disable_websocket_auto_reconnect()`` nils ``self._ws_client``,
        # so capture the client reference first.
        ws_client = self._ws_client
        ws_thread_loop = self._ws_thread_loop
        self._disable_websocket_auto_reconnect()
        await self._stop_webhook_server()

        if (
            ws_client is not None
            and ws_thread_loop is not None
            and not ws_thread_loop.is_closed()
            and hasattr(ws_client, "_disconnect")
        ):
            try:
                future = asyncio.run_coroutine_threadsafe(
                    ws_client._disconnect(), ws_thread_loop
                )
                # 5s is generous — the CLOSE frame is a single WebSocket
                # control frame. If it takes longer than that the
                # connection is already wedged and we gain nothing by
                # waiting further.
                await asyncio.wait_for(asyncio.wrap_future(future), timeout=5.0)
                logger.debug("[Feishu] Sent WebSocket CLOSE frame to Feishu")
            except asyncio.TimeoutError:
                logger.warning(
                    "[Feishu] CLOSE frame not acknowledged within 5s — "
                    "Feishu may briefly route messages to the stale "
                    "connection until server-side timeout"
                )
            except Exception as exc:
                logger.debug(
                    "[Feishu] Could not send WebSocket CLOSE frame: %s",
                    exc,
                    exc_info=True,
                )

        if ws_thread_loop is not None and not ws_thread_loop.is_closed():
            logger.debug("[Feishu] Cancelling websocket thread tasks and stopping loop")

            def cancel_all_tasks() -> None:
                tasks = [t for t in asyncio.all_tasks(ws_thread_loop) if not t.done()]
                logger.debug("[Feishu] Found %d pending tasks in websocket thread", len(tasks))
                for task in tasks:
                    task.cancel()
                ws_thread_loop.call_later(0.1, ws_thread_loop.stop)

            ws_thread_loop.call_soon_threadsafe(cancel_all_tasks)

        ws_future = self._ws_future
        if ws_future is not None:
            try:
                logger.debug("[Feishu] Waiting for websocket thread to exit (timeout=10s)")
                await asyncio.wait_for(asyncio.shield(ws_future), timeout=10.0)
                logger.debug("[Feishu] Websocket thread exited cleanly")
            except asyncio.TimeoutError:
                logger.warning("[Feishu] Websocket thread did not exit within 10s - may be stuck")
            except asyncio.CancelledError:
                logger.debug("[Feishu] Websocket thread cancelled during disconnect")
            except Exception as exc:
                logger.debug("[Feishu] Websocket thread exited with error: %s", exc, exc_info=True)

        self._ws_future = None
        self._ws_thread_loop = None
        self._loop = None
        self._event_handler = None
        self._shutdown_sdk_executor()
        self._persist_seen_message_ids()
        await self._release_app_lock()

        self._mark_disconnected()
        logger.info("[Feishu] Disconnected")


    @staticmethod
    def _parse_api_poll_chat_ids(value: Any) -> List[str]:
        if value is None:
            return []
        if isinstance(value, str):
            items = value.split(",")
        elif isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
            items = value
        else:
            return []
        seen: set[str] = set()
        result: List[str] = []
        for item in items:
            chat_id = str(item or "").strip()
            if chat_id and chat_id not in seen:
                seen.add(chat_id)
                result.append(chat_id)
        return result

    def _start_api_polling_if_configured(self) -> None:
        if not self._api_poll_chat_ids:
            return
        if self._api_poll_task and not self._api_poll_task.done():
            return
        loop = self._loop
        if not self._loop_accepts_callbacks(loop):
            logger.warning("[Feishu] API polling configured but adapter loop is not ready")
            return
        self._api_poll_task = loop.create_task(self._poll_api_chats_loop())
        logger.info(
            "[Feishu] API polling fallback enabled for %d chat(s), interval=%.1fs, page_size=%d, startup_lookback=%ds",
            len(self._api_poll_chat_ids),
            self._api_poll_interval_seconds,
            self._api_poll_page_size,
            self._api_poll_startup_lookback_seconds,
        )

    async def _establish_api_poll_startup_baselines(self) -> None:
        if not self._api_poll_chat_ids or self._api_poll_startup_lookback_seconds <= 0:
            return
        for chat_id in self._api_poll_chat_ids:
            await self._poll_api_chat_once(chat_id)
        missing = set(self._api_poll_chat_ids) - self._api_poll_baselined_chat_ids
        if missing:
            raise RuntimeError(
                "API polling startup lookback did not establish every configured chat baseline"
            )
        logger.info(
            "[Feishu] API polling startup lookback complete for %d chat(s)",
            len(self._api_poll_chat_ids),
        )

    async def _stop_api_polling(self) -> None:
        task = self._api_poll_task
        self._api_poll_task = None
        if task and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    def api_poll_persistence_status(self) -> Dict[str, Any]:
        """Return bounded operational state without exposing message payloads."""
        with self._dedup_lock:
            pending_count = sum(
                len(items) for items in self._api_poll_pending_items.values()
            )
            continuation_count = len(self._api_poll_scan_state)
            return {
                "healthy": self._api_poll_state_error is None,
                "error": self._api_poll_state_error,
                "pending_count": pending_count,
                "pending_by_chat": {
                    chat_id: len(items)
                    for chat_id, items in self._api_poll_pending_items.items()
                },
                "scan_continuation_count": continuation_count,
                "terminal_hole_count": len(self._api_poll_terminal_holes),
                "rollback_ready": pending_count == 0 and continuation_count == 0,
                "rollback_blocking_pending_count": pending_count,
                "rollback_blocking_scan_continuation_count": continuation_count,
            }

    async def _poll_api_chats_loop(self) -> None:
        while True:
            for chat_id in list(self._api_poll_chat_ids):
                try:
                    await self._poll_api_chat_once(chat_id)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning("[Feishu] API polling failed for chat %s: %s", chat_id, exc, exc_info=True)
            await asyncio.sleep(self._api_poll_interval_seconds)

    @staticmethod
    def _api_poll_item_create_time_ms(item: Dict[str, Any]) -> Optional[int]:
        raw = str(item.get("create_time") or "").strip()
        if not raw:
            return None
        try:
            value = int(raw)
        except ValueError:
            return None
        if value < 10_000_000_000:
            return value * 1000
        return value

    @staticmethod
    def _bounded_api_poll_field(value: Any, max_chars: int) -> str:
        text = str(value or "")
        if len(text) <= max_chars:
            return text
        digest = hashlib.sha256(text.encode("utf-8", errors="replace")).hexdigest()
        suffix = f"#sha256:{digest}"
        return f"{text[:max(0, max_chars - len(suffix))]}{suffix}"

    @staticmethod
    def _api_poll_poison_stub(
        item: Any,
        *,
        expected_chat_id: Optional[str],
        code: str,
    ) -> Dict[str, Any]:
        try:
            raw_payload = json.dumps(
                item,
                ensure_ascii=False,
                separators=(",", ":"),
                default=str,
            ).encode("utf-8")
        except Exception:
            raw_payload = repr(item).encode("utf-8", errors="replace")
        raw_item = item if isinstance(item, dict) else {}
        existing_poison = raw_item.get("_hermes_api_poll_poison")
        existing_poison = existing_poison if isinstance(existing_poison, dict) else {}
        payload_sha256 = str(existing_poison.get("payload_sha256") or "").strip()
        if len(payload_sha256) != 64:
            payload_sha256 = hashlib.sha256(raw_payload).hexdigest()
        raw_message_id = str(raw_item.get("message_id") or "").strip()
        existing_original_id = str(
            existing_poison.get("original_message_id") or ""
        ).strip()
        original_message_id = existing_original_id or raw_message_id
        raw_chat_id = expected_chat_id or str(raw_item.get("chat_id") or "").strip()
        poison_identity = hashlib.sha256(
            f"{raw_chat_id}\0{payload_sha256}".encode("utf-8", errors="replace")
        ).hexdigest()
        message_id = f"poison-{poison_identity}"
        chat_id = FeishuAdapter._bounded_api_poll_field(raw_chat_id, 512)
        sender = raw_item.get("sender")
        sender = sender if isinstance(sender, dict) else {}
        raw_payload_bytes = existing_poison.get("payload_bytes")
        try:
            payload_bytes = int(raw_payload_bytes or len(raw_payload))
        except (TypeError, ValueError):
            payload_bytes = len(raw_payload)
        payload_bytes = max(0, min(payload_bytes, 2**63 - 1))
        stub = {
            "message_id": message_id,
            "msg_type": FeishuAdapter._bounded_api_poll_field(
                raw_item.get("msg_type") or "unknown", 64
            ),
            "chat_id": chat_id,
            "create_time": FeishuAdapter._bounded_api_poll_field(
                raw_item.get("create_time"), 64
            ),
            "update_time": FeishuAdapter._bounded_api_poll_field(
                raw_item.get("update_time"), 64
            ),
            "root_id": FeishuAdapter._bounded_api_poll_field(
                raw_item.get("root_id"), 512
            ),
            "parent_id": FeishuAdapter._bounded_api_poll_field(
                raw_item.get("parent_id"), 512
            ),
            "body": {"content": ""},
            "sender": {
                "id": FeishuAdapter._bounded_api_poll_field(sender.get("id"), 512),
                "id_type": FeishuAdapter._bounded_api_poll_field(
                    sender.get("id_type"), 64
                ),
                "sender_type": FeishuAdapter._bounded_api_poll_field(
                    sender.get("sender_type") or "user", 64
                ),
            },
            "mentions": [],
            "_hermes_api_poll_poison": {
                "code": FeishuAdapter._bounded_api_poll_field(
                    existing_poison.get("code") or code, 64
                ),
                "payload_sha256": payload_sha256,
                "payload_bytes": payload_bytes,
                "original_message_id": FeishuAdapter._bounded_api_poll_field(
                    original_message_id, 512
                ),
            },
        }
        encoded_stub = json.dumps(
            stub,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        if len(encoded_stub) > _MAX_API_POLL_ITEM_BYTES:
            raise ValueError("bounded API poll poison stub exceeds 128 KiB")
        return stub

    @staticmethod
    def _validated_api_poll_pending_item(
        item: Any,
        *,
        expected_chat_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        if not isinstance(item, dict):
            return FeishuAdapter._api_poll_poison_stub(
                item,
                expected_chat_id=expected_chat_id,
                code="item_not_object",
            )
        existing_poison = item.get("_hermes_api_poll_poison")
        if isinstance(existing_poison, dict):
            return FeishuAdapter._api_poll_poison_stub(
                item,
                expected_chat_id=expected_chat_id,
                code=str(existing_poison.get("code") or "poison"),
            )
        body = item.get("body")
        body = body if isinstance(body, dict) else {}
        sender = item.get("sender")
        sender = sender if isinstance(sender, dict) else {}
        raw_mentions = item.get("mentions") or []
        if not isinstance(raw_mentions, list) or len(raw_mentions) > 64:
            return FeishuAdapter._api_poll_poison_stub(
                item,
                expected_chat_id=expected_chat_id,
                code="invalid_mentions",
            )
        mentions = []
        for raw_mention in raw_mentions:
            if not isinstance(raw_mention, dict):
                return FeishuAdapter._api_poll_poison_stub(
                    item,
                    expected_chat_id=expected_chat_id,
                    code="invalid_mention",
                )
            mentions.append(
                {
                    "id": str(raw_mention.get("id") or ""),
                    "id_type": str(raw_mention.get("id_type") or ""),
                    "key": str(raw_mention.get("key") or ""),
                    "name": str(raw_mention.get("name") or ""),
                }
            )
        # Persist the smallest allowlisted envelope needed to reconstruct the
        # callback. Unknown API response fields cannot inflate durable state.
        envelope = {
            "message_id": str(item.get("message_id") or ""),
            "msg_type": str(item.get("msg_type") or ""),
            "chat_id": str(item.get("chat_id") or ""),
            "create_time": str(item.get("create_time") or ""),
            "update_time": str(item.get("update_time") or ""),
            "root_id": str(item.get("root_id") or ""),
            "parent_id": str(item.get("parent_id") or ""),
            "body": {"content": str(body.get("content") or "")},
            "sender": {
                "id": str(sender.get("id") or ""),
                "id_type": str(sender.get("id_type") or ""),
                "sender_type": str(sender.get("sender_type") or "user"),
            },
            "mentions": mentions,
        }
        try:
            encoded = json.dumps(
                envelope,
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise ValueError("API poll pending item must be JSON serializable") from exc
        if len(encoded) > _MAX_API_POLL_ITEM_BYTES:
            return FeishuAdapter._api_poll_poison_stub(
                item,
                expected_chat_id=expected_chat_id,
                code="item_exceeds_128_kib",
            )
        normalized = json.loads(encoded.decode("utf-8"))
        message_id = str(normalized.get("message_id") or "").strip()
        chat_id = str(normalized.get("chat_id") or "").strip()
        if not message_id or not chat_id:
            return FeishuAdapter._api_poll_poison_stub(
                item,
                expected_chat_id=expected_chat_id,
                code="missing_transport_identity",
            )
        if expected_chat_id and chat_id != expected_chat_id:
            return FeishuAdapter._api_poll_poison_stub(
                item,
                expected_chat_id=expected_chat_id,
                code="chat_identity_mismatch",
            )
        return normalized

    def _remember_api_poll_seen_locked(self, message_ids: Sequence[str]) -> None:
        order = self._api_poll_seen_message_order
        seen = self._api_poll_seen_message_ids
        for raw_message_id in message_ids:
            message_id = str(raw_message_id or "").strip()
            if not message_id:
                continue
            if message_id in seen:
                try:
                    order.remove(message_id)
                except ValueError:
                    pass
            seen.add(message_id)
            order.append(message_id)
        limit = max(1, int(getattr(self, "_dedup_cache_size", 1000) or 1000))
        while len(order) > limit:
            stale = order.pop(0)
            seen.discard(stale)

    def _api_poll_state_snapshot(self) -> Dict[str, Any]:
        return {
            "pending": self._api_poll_pending_items,
            "baselined_chat_ids": sorted(self._api_poll_baselined_chat_ids),
            "last_seen_create_time_ms": self._api_poll_last_seen_create_time_ms,
            "cursor_message_ids": {
                chat_id: sorted(message_ids)
                for chat_id, message_ids in self._api_poll_cursor_message_ids.items()
            },
            "discovery_floor_ms": self._api_poll_discovery_floor_ms,
            "scan_state": self._api_poll_scan_state,
            "terminal_holes": self._api_poll_terminal_holes,
            "seen_message_ids": list(self._api_poll_seen_message_order),
        }

    def _api_poll_sidecar_payload(
        self,
        state: Dict[str, Any],
        *,
        revision: Optional[int] = None,
    ) -> Dict[str, Any]:
        pending = state.get("pending", {})
        scan_state = state.get("scan_state", {})
        pending_count = sum(
            len(items) for items in pending.values() if isinstance(items, list)
        )
        continuation_count = len(scan_state) if isinstance(scan_state, dict) else 0
        return {
            "schema_version": _API_POLL_SIDECAR_SCHEMA,
            "app_scope": self._api_poll_app_scope,
            "revision": self._api_poll_revision + 1 if revision is None else revision,
            "updated_at": time.time(),
            "rollback_readiness": {
                "ready": pending_count == 0 and continuation_count == 0,
                "pending_count": pending_count,
                "scan_continuation_count": continuation_count,
            },
            "state": state,
        }

    @staticmethod
    def _api_poll_encoded_size(value: Any) -> int:
        return len(
            json.dumps(
                value,
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
        )

    @staticmethod
    def _api_poll_chat_state_view(
        state: Dict[str, Any],
        chat_id: str,
    ) -> Dict[str, Any]:
        pending = state.get("pending", {})
        scan_state = state.get("scan_state", {})
        cursor_ids = state.get("cursor_message_ids", {})
        cursors = state.get("last_seen_create_time_ms", {})
        floors = state.get("discovery_floor_ms", {})
        holes = state.get("terminal_holes", [])
        return {
            "pending": pending.get(chat_id, []) if isinstance(pending, dict) else [],
            "scan_state": (
                scan_state.get(chat_id) if isinstance(scan_state, dict) else None
            ),
            "terminal_holes": [
                hole
                for hole in holes
                if isinstance(hole, dict) and hole.get("chat_id") == chat_id
            ]
            if isinstance(holes, list)
            else [],
            "last_seen_create_time_ms": (
                cursors.get(chat_id) if isinstance(cursors, dict) else None
            ),
            "cursor_message_ids": (
                cursor_ids.get(chat_id, []) if isinstance(cursor_ids, dict) else []
            ),
            "discovery_floor_ms": (
                floors.get(chat_id) if isinstance(floors, dict) else None
            ),
        }

    def _validate_api_poll_capacity(
        self,
        state: Dict[str, Any],
        payload: Dict[str, Any],
    ) -> None:
        pending = state.get("pending", {})
        scan_state = state.get("scan_state", {})
        holes = state.get("terminal_holes", [])
        chat_ids = set(pending) | set(scan_state)
        chat_ids.update(
            str(hole.get("chat_id") or "")
            for hole in holes
            if isinstance(hole, dict) and hole.get("chat_id")
        )
        for chat_id in chat_ids:
            pending_items = pending.get(chat_id, [])
            scan = scan_state.get(chat_id)
            scan_candidates = scan.get("candidates", []) if isinstance(scan, dict) else []
            ownership_count = len(pending_items) + len(scan_candidates)
            if ownership_count > _MAX_API_POLL_PENDING_PER_CHAT:
                raise ValueError(
                    f"API poll ownership count exceeds per-chat capacity for {chat_id}"
                )
            chat_bytes = self._api_poll_encoded_size(
                self._api_poll_chat_state_view(state, chat_id)
            )
            if chat_bytes > _MAX_API_POLL_PER_CHAT_BYTES:
                raise ValueError(
                    f"API poll ownership bytes exceed per-chat capacity for {chat_id}"
                )
        payload_bytes = self._api_poll_encoded_size(payload)
        if payload_bytes > _MAX_API_POLL_TOTAL_BYTES:
            raise ValueError("API poll sidecar exceeds 16 MiB byte capacity")

    def _persist_api_poll_state(self, *, require_success: bool = False) -> bool:
        if self._api_poll_block_persistence:
            logger.error("[Feishu] API poll sidecar writes are blocked after an unsafe load")
            if require_success:
                raise RuntimeError("Feishu API poll sidecar is unhealthy")
            return False
        try:
            state = self._api_poll_state_snapshot()
            next_revision = self._api_poll_revision + 1
            payload = self._api_poll_sidecar_payload(state, revision=next_revision)
            self._validate_api_poll_capacity(state, payload)
            self._api_poll_state_path.parent.mkdir(parents=True, exist_ok=True)
            atomic_json_write(self._api_poll_state_path, payload, indent=None)
            self._api_poll_revision = next_revision
            self._api_poll_sidecar_initialized = True
            return True
        except (OSError, TypeError, ValueError) as exc:
            logger.warning(
                "[Feishu] Failed to persist API poll sidecar to %s",
                self._api_poll_state_path,
                exc_info=True,
            )
            if require_success:
                raise RuntimeError("Feishu API poll sidecar write failed") from exc
            return False

    def _api_poll_candidate_capacity(self, chat_id: str) -> tuple[int, int]:
        """Return conservative (count, bytes) available to this chat's next chunk."""
        with self._dedup_lock:
            state = dict(self._api_poll_state_snapshot())
            state["scan_state"] = dict(state["scan_state"])
            state["scan_state"].pop(chat_id, None)
            pending = state.get("pending", {}).get(chat_id, [])
            count_budget = max(
                0,
                _MAX_API_POLL_PENDING_PER_CHAT - len(pending),
            )
            payload = self._api_poll_sidecar_payload(state)
            total_budget = max(
                0,
                _MAX_API_POLL_TOTAL_BYTES
                - self._api_poll_encoded_size(payload)
                - _API_POLL_CAPACITY_RESERVE_BYTES,
            )
            chat_budget = max(
                0,
                _MAX_API_POLL_PER_CHAT_BYTES
                - self._api_poll_encoded_size(
                    self._api_poll_chat_state_view(state, chat_id)
                )
                - _API_POLL_CAPACITY_RESERVE_BYTES,
            )
            return count_budget, min(total_budget, chat_budget)

    def _validated_api_poll_scan_state(
        self,
        chat_id: str,
        state: Any,
    ) -> Dict[str, Any]:
        if not isinstance(state, dict):
            raise ValueError("API poll scan state must be an object")
        page_token = str(state.get("page_token") or "").strip()
        watermark_ms = state.get("watermark_ms")
        raw_cursor_ids = state.get("cursor_message_ids", [])
        raw_candidates = state.get("candidates", [])
        if not page_token or len(page_token) > 2048:
            raise ValueError("API poll scan page token is invalid")
        if (
            isinstance(watermark_ms, bool)
            or not isinstance(watermark_ms, int)
            or watermark_ms < 0
        ):
            raise ValueError("API poll scan watermark is invalid")
        if not isinstance(raw_cursor_ids, list) or not isinstance(raw_candidates, list):
            raise ValueError("API poll scan collection state is invalid")
        if len(raw_candidates) > _MAX_API_POLL_PENDING_PER_CHAT:
            raise ValueError("API poll scan candidates exceed capacity")
        candidates = [
            self._validated_api_poll_pending_item(
                item,
                expected_chat_id=chat_id,
            )
            for item in raw_candidates
        ]
        return {
            "page_token": page_token,
            "watermark_ms": watermark_ms,
            "cursor_message_ids": sorted(
                {
                    str(value).strip()
                    for value in raw_cursor_ids
                    if str(value or "").strip()
                }
            ),
            "candidates": candidates,
            "baselined": state.get("baselined") is True,
        }

    def _commit_api_poll_scan_state(
        self,
        chat_id: str,
        state: Dict[str, Any],
    ) -> None:
        normalized = self._validated_api_poll_scan_state(chat_id, state)
        with self._dedup_lock:
            previous_state = self._api_poll_scan_state.get(chat_id)
            previous_baselined = chat_id in self._api_poll_baselined_chat_ids
            previous_floor = self._api_poll_discovery_floor_ms.get(chat_id)
            self._api_poll_scan_state[chat_id] = normalized
            self._api_poll_baselined_chat_ids.add(chat_id)
            self._api_poll_discovery_floor_ms.setdefault(
                chat_id,
                self._api_poll_start_cursor_ms,
            )
            try:
                self._persist_api_poll_state(require_success=True)
            except BaseException:
                if previous_state is None:
                    self._api_poll_scan_state.pop(chat_id, None)
                else:
                    self._api_poll_scan_state[chat_id] = previous_state
                if previous_baselined:
                    self._api_poll_baselined_chat_ids.add(chat_id)
                else:
                    self._api_poll_baselined_chat_ids.discard(chat_id)
                if previous_floor is None:
                    self._api_poll_discovery_floor_ms.pop(chat_id, None)
                else:
                    self._api_poll_discovery_floor_ms[chat_id] = previous_floor
                raise

    def _reset_api_poll_scan_state(
        self,
        chat_id: str,
        *,
        expected_page_token: str,
    ) -> bool:
        """CAS-clear only an invalid continuation, preserving its watermark."""
        with self._dedup_lock:
            current = self._api_poll_scan_state.get(chat_id)
            if not isinstance(current, dict):
                return False
            current_token = str(current.get("page_token") or "").strip()
            if not current_token or current_token != expected_page_token:
                return False
            previous = current
            self._api_poll_scan_state.pop(chat_id, None)
            try:
                self._persist_api_poll_state(require_success=True)
            except BaseException:
                self._api_poll_scan_state[chat_id] = previous
                raise
            return True

    def _commit_api_poll_discovery(
        self,
        chat_id: str,
        *,
        pending_items: Sequence[Dict[str, Any]],
        seen_message_ids: Sequence[str] = (),
        cursor_ms: Optional[int] = None,
        cursor_message_ids: Sequence[str] = (),
        mark_baselined: bool = False,
        clear_scan_state: bool = False,
    ) -> None:
        """Persist a fetched page before any callback can own its messages."""
        normalized_items = [
            self._validated_api_poll_pending_item(
                item,
                expected_chat_id=chat_id,
            )
            for item in pending_items
        ]
        normalized_seen = {
            str(message_id).strip()
            for message_id in seen_message_ids
            if str(message_id or "").strip()
        }
        with self._dedup_lock:
            if self._api_poll_state_error:
                raise RuntimeError(
                    f"API poll persistent state is unhealthy: {self._api_poll_state_error}"
                )
            previous_pending = list(self._api_poll_pending_items.get(chat_id, []))
            previous_seen = set(self._api_poll_seen_message_ids)
            previous_seen_order = list(self._api_poll_seen_message_order)
            previous_baselined = chat_id in self._api_poll_baselined_chat_ids
            previous_cursor = self._api_poll_last_seen_create_time_ms.get(chat_id)
            previous_cursor_ids = set(
                self._api_poll_cursor_message_ids.get(chat_id, set())
            )
            previous_floor = self._api_poll_discovery_floor_ms.get(chat_id)
            previous_scan_state = self._api_poll_scan_state.get(chat_id)

            pending = list(previous_pending)
            known_ids = {
                str(item.get("message_id") or "").strip()
                for item in pending
            }
            for item in normalized_items:
                message_id = str(item.get("message_id") or "").strip()
                if message_id in known_ids or message_id in self._api_poll_seen_message_ids:
                    continue
                pending.append(item)
                known_ids.add(message_id)
            if len(pending) > _MAX_API_POLL_PENDING_PER_CHAT:
                logger.error(
                    "[Feishu] API poll pending count capacity exceeded for chat %s",
                    chat_id,
                )
                raise RuntimeError(
                    f"API poll pending capacity exceeded for chat {chat_id}"
                )
            pending.sort(
                key=lambda value: (
                    self._api_poll_item_create_time_ms(value) or 0,
                    str(value.get("message_id") or ""),
                )
            )
            if pending:
                self._api_poll_pending_items[chat_id] = pending
            else:
                self._api_poll_pending_items.pop(chat_id, None)
            self._remember_api_poll_seen_locked(sorted(normalized_seen))
            if mark_baselined:
                self._api_poll_baselined_chat_ids.add(chat_id)
                self._api_poll_discovery_floor_ms.setdefault(
                    chat_id,
                    self._api_poll_start_cursor_ms,
                )
            if clear_scan_state:
                self._api_poll_scan_state.pop(chat_id, None)
            if cursor_ms is not None:
                normalized_cursor_ids = {
                    str(message_id).strip()
                    for message_id in cursor_message_ids
                    if str(message_id or "").strip()
                }
                if previous_cursor is None or cursor_ms > previous_cursor:
                    self._api_poll_last_seen_create_time_ms[chat_id] = cursor_ms
                    self._api_poll_cursor_message_ids[chat_id] = normalized_cursor_ids
                elif cursor_ms == previous_cursor:
                    self._api_poll_cursor_message_ids[chat_id] = (
                        previous_cursor_ids | normalized_cursor_ids
                    )
            try:
                self._persist_api_poll_state(require_success=True)
            except BaseException:
                if previous_pending:
                    self._api_poll_pending_items[chat_id] = previous_pending
                else:
                    self._api_poll_pending_items.pop(chat_id, None)
                self._api_poll_seen_message_ids = previous_seen
                self._api_poll_seen_message_order = previous_seen_order
                if previous_baselined:
                    self._api_poll_baselined_chat_ids.add(chat_id)
                else:
                    self._api_poll_baselined_chat_ids.discard(chat_id)
                if previous_cursor is None:
                    self._api_poll_last_seen_create_time_ms.pop(chat_id, None)
                else:
                    self._api_poll_last_seen_create_time_ms[chat_id] = previous_cursor
                if previous_cursor_ids:
                    self._api_poll_cursor_message_ids[chat_id] = previous_cursor_ids
                else:
                    self._api_poll_cursor_message_ids.pop(chat_id, None)
                if previous_floor is None:
                    self._api_poll_discovery_floor_ms.pop(chat_id, None)
                else:
                    self._api_poll_discovery_floor_ms[chat_id] = previous_floor
                if previous_scan_state is None:
                    self._api_poll_scan_state.pop(chat_id, None)
                else:
                    self._api_poll_scan_state[chat_id] = previous_scan_state
                raise

    def _finalize_api_poll_pending_item(
        self,
        chat_id: str,
        item: Dict[str, Any],
        *,
        terminal_hole: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Atomically retire pending ownership and advance its durable cursor."""
        message_id = str(item.get("message_id") or "").strip()
        create_time = self._api_poll_item_create_time_ms(item)
        with self._dedup_lock:
            previous_pending = list(self._api_poll_pending_items.get(chat_id, []))
            previous_seen = set(self._api_poll_seen_message_ids)
            previous_seen_order = list(self._api_poll_seen_message_order)
            previous_cursor = self._api_poll_last_seen_create_time_ms.get(chat_id)
            previous_cursor_ids = set(
                self._api_poll_cursor_message_ids.get(chat_id, set())
            )
            previous_holes = list(self._api_poll_terminal_holes)
            remaining = [
                pending
                for pending in previous_pending
                if str(pending.get("message_id") or "").strip() != message_id
            ]
            if remaining:
                self._api_poll_pending_items[chat_id] = remaining
            else:
                self._api_poll_pending_items.pop(chat_id, None)
            self._remember_api_poll_seen_locked([message_id])
            if terminal_hole is not None:
                hole = {
                    "schema_version": "feishu_api_poll_terminal_hole_v1",
                    "kind": str(terminal_hole.get("kind") or "unknown")[:64],
                    "status": str(terminal_hole.get("status") or "terminal")[:64],
                    "message_id": message_id,
                    "chat_id": self._bounded_api_poll_field(chat_id, 512),
                    "create_time": str(item.get("create_time") or "")[:64],
                    "sender_id": str(
                        (item.get("sender") or {}).get("id")
                        if isinstance(item.get("sender"), dict)
                        else ""
                    )[:512],
                    "payload_sha256": str(
                        terminal_hole.get("payload_sha256") or ""
                    )[:64],
                    "original_message_id": self._bounded_api_poll_field(
                        terminal_hole.get("original_message_id"), 512
                    ),
                    "error": str(terminal_hole.get("error") or "")[:512],
                    "admission_item_id": str(
                        terminal_hole.get("admission_item_id") or ""
                    )[:128],
                    "recorded_at": time.time(),
                }
                self._api_poll_terminal_holes.append(hole)
                self._api_poll_terminal_holes = self._api_poll_terminal_holes[
                    -_MAX_API_POLL_TERMINAL_HOLES:
                ]
            if create_time is not None:
                if previous_cursor is None or create_time > previous_cursor:
                    self._api_poll_last_seen_create_time_ms[chat_id] = create_time
                    self._api_poll_cursor_message_ids[chat_id] = {message_id}
                elif create_time == previous_cursor:
                    self._api_poll_cursor_message_ids[chat_id] = (
                        previous_cursor_ids | {message_id}
                    )
            try:
                self._persist_api_poll_state(require_success=True)
            except BaseException:
                if previous_pending:
                    self._api_poll_pending_items[chat_id] = previous_pending
                else:
                    self._api_poll_pending_items.pop(chat_id, None)
                self._api_poll_seen_message_ids = previous_seen
                self._api_poll_seen_message_order = previous_seen_order
                if previous_cursor is None:
                    self._api_poll_last_seen_create_time_ms.pop(chat_id, None)
                else:
                    self._api_poll_last_seen_create_time_ms[chat_id] = previous_cursor
                if previous_cursor_ids:
                    self._api_poll_cursor_message_ids[chat_id] = previous_cursor_ids
                else:
                    self._api_poll_cursor_message_ids.pop(chat_id, None)
                self._api_poll_terminal_holes = previous_holes
                raise

    def _api_poll_admission_owner(self, message_id: str) -> Any:
        controller = getattr(self, "_admission_controller", None)
        getter = getattr(controller, "get_transport_item", None)
        if not callable(getter):
            return None
        return getter("feishu", message_id)

    async def _drain_api_poll_pending(self, chat_id: str) -> bool:
        """Process staged events in order; return False while durable work owns one."""
        while True:
            with self._dedup_lock:
                pending = self._api_poll_pending_items.get(chat_id, [])
                item = pending[0] if pending else None
            if item is None:
                return True

            message_id = str(item.get("message_id") or "").strip()
            poison = item.get("_hermes_api_poll_poison")
            if isinstance(poison, dict):
                code = str(poison.get("code") or "poison")
                logger.error(
                    "[Feishu] API poll retiring poison message %s: %s",
                    message_id,
                    code,
                )
                self._finalize_api_poll_pending_item(
                    chat_id,
                    item,
                    terminal_hole={
                        "kind": "poison",
                        "status": "dead",
                        "payload_sha256": poison.get("payload_sha256"),
                        "original_message_id": poison.get("original_message_id"),
                        "error": code,
                    },
                )
                continue

            owner = self._api_poll_admission_owner(message_id)
            owner_status = str(getattr(owner, "status", "") or "")
            if owner_status == "completed":
                result = getattr(owner, "result", None)
                if (
                    isinstance(result, dict)
                    and result.get("durable_feishu_completion") is True
                ):
                    self._complete_message_processing(message_id)
                    self._finalize_api_poll_pending_item(chat_id, item)
                    continue
            if owner_status in {"queued", "processing"}:
                return False
            if owner_status in {"dead", "cancelled"}:
                logger.error(
                    "[Feishu] API poll pending message %s reached terminal admission status=%s",
                    message_id,
                    owner_status,
                )
                self._finalize_api_poll_pending_item(
                    chat_id,
                    item,
                    terminal_hole={
                        "kind": "admission_terminal",
                        "status": owner_status,
                        "error": f"admission_{owner_status}",
                        "admission_item_id": getattr(owner, "id", ""),
                    },
                )
                continue
            if owner is not None:
                logger.error(
                    "[Feishu] API poll pending message %s is held by non-durable admission status=%s",
                    message_id,
                    owner_status or "<empty>",
                )
                return False

            # Admission ownership is authoritative. Only consult the transport
            # inbox after an owner is terminal or absent; the durable worker can
            # complete the inbox just before QueueWorker commits queue completion.
            if self._message_processing_completed(message_id):
                self._finalize_api_poll_pending_item(chat_id, item)
                continue

            with self._dedup_lock:
                if message_id in self._processing_message_ids:
                    return False

            logger.info(
                "[Feishu] API polling replaying staged message_id=%s chat_id=%s msg_type=%s",
                message_id,
                chat_id,
                item.get("msg_type"),
            )
            completed = await self._handle_message_event_data(
                self._api_message_item_to_event_data(item)
            )
            if not completed:
                return False
            self._finalize_api_poll_pending_item(chat_id, item)

    async def _poll_api_chat_once(self, chat_id: str) -> None:
        # Keep the derived cursor coherent for tests and callers that replace
        # the start clock before the first poll. Persisted per-chat floors still
        # remain authoritative after a baseline has been committed.
        self._api_poll_start_cursor_ms = self._api_poll_started_at_ms - (
            self._api_poll_startup_lookback_seconds * 1000
        )
        if self._api_poll_state_error:
            raise RuntimeError(
                f"API poll persistent state is unhealthy: {self._api_poll_state_error}"
            )

        pending_error: Optional[Exception] = None
        pending_drained = True
        try:
            pending_drained = await self._drain_api_poll_pending(chat_id)
        except Exception as exc:
            pending_error = exc

        try:
            fetch_result = await self._fetch_api_poll_messages(chat_id)
        except _FeishuApiPollInvalidContinuation as exc:
            reset = self._reset_api_poll_scan_state(
                chat_id,
                expected_page_token=exc.page_token,
            )
            if not reset:
                raise
            logger.warning(
                "[Feishu] API poll continuation reset for chat %s after explicit token rejection",
                chat_id,
            )
            fetch_result = await self._fetch_api_poll_messages(chat_id)
        if isinstance(fetch_result, _FeishuApiPollScanContinuation):
            self._commit_api_poll_scan_state(chat_id, fetch_result.state)
            if pending_error is not None:
                raise pending_error
            return
        items = fetch_result
        items = [
            self._validated_api_poll_pending_item(
                item,
                expected_chat_id=chat_id,
            )
            for item in items
        ]
        cursor_ms = self._api_poll_last_seen_create_time_ms.get(chat_id)
        discovered_seen_ids: List[str] = []
        discovered_seen_times: List[tuple[str, int]] = []
        if chat_id not in self._api_poll_baselined_chat_ids:
            new_items = []
            for item in items:
                message_id = str(item.get("message_id") or "").strip()
                if not message_id:
                    continue
                create_time = self._api_poll_item_create_time_ms(item)
                if create_time is not None and create_time >= self._api_poll_start_cursor_ms:
                    new_items.append(item)
                else:
                    discovered_seen_ids.append(message_id)
                    if create_time is not None:
                        discovered_seen_times.append((message_id, create_time))
                        cursor_ms = max(cursor_ms or create_time, create_time)
            logger.info(
                "[Feishu] API polling baseline prepared for chat %s with %d recent message(s), replaying %d since startup cursor",
                chat_id,
                len(items),
                len(new_items),
            )
        else:
            new_items = []
            for item in items:
                message_id = str(item.get("message_id") or "").strip()
                if not message_id or message_id in self._api_poll_seen_message_ids:
                    continue
                create_time = self._api_poll_item_create_time_ms(item)
                cursor_ids = self._api_poll_cursor_message_ids.get(chat_id, set())
                if (
                    cursor_ms is not None
                    and create_time is not None
                    and (
                        create_time < cursor_ms
                        or (create_time == cursor_ms and message_id in cursor_ids)
                    )
                ):
                    discovered_seen_ids.append(message_id)
                    continue
                new_items.append(item)

        new_items.sort(key=lambda item: self._api_poll_item_create_time_ms(item) or 0)
        discovered_cursor_ids = [
            message_id
            for message_id, create_time in discovered_seen_times
            if cursor_ms is not None and create_time == cursor_ms
        ]
        self._commit_api_poll_discovery(
            chat_id,
            pending_items=new_items,
            seen_message_ids=discovered_seen_ids,
            cursor_ms=cursor_ms,
            cursor_message_ids=discovered_cursor_ids,
            mark_baselined=True,
            clear_scan_state=True,
        )
        if pending_error is not None:
            raise pending_error
        if pending_drained:
            await self._drain_api_poll_pending(chat_id)

    async def _fetch_api_poll_messages(
        self,
        chat_id: str,
    ) -> List[Dict[str, Any]] | _FeishuApiPollScanContinuation:
        cancel_event = threading.Event()
        fetch_task = asyncio.create_task(
            asyncio.to_thread(
                self._fetch_recent_chat_messages_via_api,
                chat_id,
                cancel_event=cancel_event,
                durable_scan=True,
            )
        )
        try:
            return await asyncio.shield(fetch_task)
        except asyncio.CancelledError:
            cancel_event.set()
            try:
                await asyncio.wait_for(
                    asyncio.shield(fetch_task),
                    timeout=_API_POLL_CANCEL_WAIT_SECONDS,
                )
            except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
                pass
            raise

    def _fetch_recent_chat_messages_legacy(
        self,
        chat_id: str,
        *,
        startup_cursor_ms: Optional[int],
        cancel_event: Optional[threading.Event],
    ) -> List[Dict[str, Any]]:
        """Preserve the v0.18.2 direct-fetch contract outside durable polling."""
        token = self._fetch_tenant_access_token_via_api()
        base_url = (
            "https://open.larksuite.com"
            if self._domain_name == "lark"
            else "https://open.feishu.cn"
        )
        page_size = (
            _MAX_API_POLL_PAGE_SIZE
            if startup_cursor_ms is not None
            else self._api_poll_page_size
        )
        max_pages = (
            _MAX_API_POLL_STARTUP_PAGES if startup_cursor_ms is not None else 1
        )
        page_token = ""
        seen_page_tokens: set[str] = set()
        items: List[Dict[str, Any]] = []

        for page_number in range(1, max_pages + 1):
            if cancel_event is not None and cancel_event.is_set():
                raise RuntimeError("message list fetch cancelled")
            query_params = {
                "container_id_type": "chat",
                "container_id": chat_id,
                "sort_type": "ByCreateTimeDesc",
                "page_size": str(page_size),
            }
            if page_token:
                query_params["page_token"] = page_token
            request = Request(
                f"{base_url}/open-apis/im/v1/messages?{urlencode(query_params)}",
                headers={"Authorization": f"Bearer {token}"},
            )
            with urlopen(
                request,
                timeout=_API_POLL_REQUEST_TIMEOUT_SECONDS,
            ) as response:
                payload = json.loads(response.read().decode("utf-8"))
            if cancel_event is not None and cancel_event.is_set():
                raise RuntimeError("message list fetch cancelled")
            if payload.get("code") != 0:
                raise RuntimeError(
                    f"message list failed: {payload.get('code')} {payload.get('msg')}"
                )

            data = payload.get("data") or {}
            page_items = list(data.get("items") or [])
            items.extend(page_items)
            if startup_cursor_ms is not None and not isinstance(
                data.get("has_more"), bool
            ):
                raise RuntimeError(
                    "message list startup response missing boolean has_more"
                )
            has_more = bool(data.get("has_more"))
            if startup_cursor_ms is None or not has_more:
                return items
            if any(
                create_time is not None and create_time < startup_cursor_ms
                for create_time in (
                    self._api_poll_item_create_time_ms(item) for item in page_items
                )
            ):
                logger.info(
                    "[Feishu] API polling startup lookback fetched %d message(s) across %d page(s) for chat %s",
                    len(items),
                    page_number,
                    chat_id,
                )
                return items

            next_page_token = str(data.get("page_token") or "").strip()
            if not next_page_token or next_page_token in seen_page_tokens:
                raise RuntimeError(
                    "message list pagination returned a missing or repeated page_token"
                )
            seen_page_tokens.add(next_page_token)
            page_token = next_page_token

        raise RuntimeError(
            "message list startup lookback exceeded "
            f"{_MAX_API_POLL_STARTUP_PAGES} pages before reaching the startup cursor"
        )

    def _fetch_recent_chat_messages_via_api(
        self,
        chat_id: str,
        *,
        startup_cursor_ms: Optional[int] = None,
        cancel_event: Optional[threading.Event] = None,
        durable_scan: bool = True,
    ) -> List[Dict[str, Any]] | _FeishuApiPollScanContinuation:
        if not durable_scan or startup_cursor_ms is not None:
            return self._fetch_recent_chat_messages_legacy(
                chat_id,
                startup_cursor_ms=startup_cursor_ms,
                cancel_event=cancel_event,
            )
        token = self._fetch_tenant_access_token_via_api()
        if cancel_event is not None and cancel_event.is_set():
            raise RuntimeError("message list fetch cancelled")
        base_url = "https://open.larksuite.com" if self._domain_name == "lark" else "https://open.feishu.cn"
        watermark_ms = self._api_poll_last_seen_create_time_ms.get(
            chat_id,
            self._api_poll_discovery_floor_ms.get(
                chat_id,
                self._api_poll_start_cursor_ms,
            ),
        )
        scan_state = self._api_poll_scan_state.get(chat_id)
        scan_baselined = chat_id in self._api_poll_baselined_chat_ids
        scan_cursor_ids = set(self._api_poll_cursor_message_ids.get(chat_id, set()))
        page_token = ""
        persisted_page_token = ""
        # The API sorts newest-first. Retain the oldest bounded chunk above the
        # watermark; once that chunk completes, the cursor advances and the next
        # poll retrieves the next chunk. This handles gaps larger than the
        # persistent pending capacity without skipping older work.
        candidates: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
        candidate_sizes: Dict[str, int] = {}
        candidate_bytes = 0
        scanned_candidates = 0
        if isinstance(scan_state, dict):
            normalized_scan = self._validated_api_poll_scan_state(chat_id, scan_state)
            if normalized_scan["watermark_ms"] == watermark_ms:
                page_token = normalized_scan["page_token"]
                persisted_page_token = page_token
                scan_baselined = normalized_scan["baselined"]
                scan_cursor_ids = set(normalized_scan["cursor_message_ids"])
                for candidate in normalized_scan["candidates"]:
                    message_id = str(candidate.get("message_id") or "").strip()
                    encoded_size = len(
                        json.dumps(
                            candidate,
                            ensure_ascii=False,
                            separators=(",", ":"),
                        ).encode("utf-8")
                    )
                    candidates[message_id] = candidate
                    candidate_sizes[message_id] = encoded_size
                    candidate_bytes += encoded_size
        candidate_count_budget, candidate_byte_budget = (
            self._api_poll_candidate_capacity(chat_id)
        )
        if len(candidates) > candidate_count_budget or candidate_bytes > candidate_byte_budget:
            while candidates and (
                len(candidates) > candidate_count_budget
                or candidate_bytes > candidate_byte_budget
            ):
                stale_message_id, _stale_item = candidates.popitem(last=False)
                candidate_bytes -= candidate_sizes.pop(stale_message_id, 0)
        max_pages = max(
            1,
            (_MAX_API_POLL_SCAN_ITEMS + self._api_poll_page_size - 1)
            // self._api_poll_page_size,
        ) + 1

        for page_index in range(max_pages):
            if cancel_event is not None and cancel_event.is_set():
                raise RuntimeError("message list fetch cancelled")
            query_params = {
                "container_id_type": "chat",
                "container_id": chat_id,
                "sort_type": "ByCreateTimeDesc",
                "page_size": str(self._api_poll_page_size),
            }
            if page_token:
                query_params["page_token"] = page_token
            query = urlencode(query_params)
            request = Request(
                f"{base_url}/open-apis/im/v1/messages?{query}",
                headers={"Authorization": f"Bearer {token}"},
            )
            try:
                with urlopen(
                    request,
                    timeout=_API_POLL_REQUEST_TIMEOUT_SECONDS,
                ) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                if cancel_event is not None and cancel_event.is_set():
                    raise RuntimeError("message list fetch cancelled")
            except HTTPError as exc:
                if page_token:
                    try:
                        error_payload = json.loads(exc.read().decode("utf-8"))
                    except Exception:
                        error_payload = {}
                    detail = f"{error_payload.get('code')} {error_payload.get('msg')}"
                    if _API_POLL_INVALID_TOKEN_RE.search(detail):
                        raise _FeishuApiPollInvalidContinuation(
                            "message list rejected persisted page token",
                            persisted_page_token or page_token,
                        ) from exc
                raise
            if payload.get("code") != 0:
                detail = f"{payload.get('code')} {payload.get('msg')}"
                if page_token and _API_POLL_INVALID_TOKEN_RE.search(detail):
                    raise _FeishuApiPollInvalidContinuation(
                        "message list rejected persisted page token",
                        persisted_page_token or page_token,
                    )
                raise RuntimeError(
                    f"message list failed: {payload.get('code')} {payload.get('msg')}"
                )
            data = payload.get("data") or {}
            page_items = list(data.get("items") or [])
            for item in page_items:
                persisted_candidate = self._validated_api_poll_pending_item(
                    item,
                    expected_chat_id=chat_id,
                )
                message_id = str(
                    persisted_candidate.get("message_id") or ""
                ).strip()
                create_time = self._api_poll_item_create_time_ms(
                    persisted_candidate
                )
                if scan_baselined:
                    is_candidate = bool(
                        create_time is None
                        or create_time > watermark_ms
                        or (
                            create_time == watermark_ms
                            and message_id not in scan_cursor_ids
                        )
                    )
                else:
                    is_candidate = bool(
                        create_time is None or create_time >= watermark_ms
                    )
                if not is_candidate:
                    continue
                scanned_candidates += 1
                encoded_size = len(
                    json.dumps(
                        persisted_candidate,
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ).encode("utf-8")
                )
                if encoded_size > candidate_byte_budget:
                    raise RuntimeError(
                        f"API poll item cannot fit persistent budget for chat {chat_id}"
                    )
                candidates.pop(message_id, None)
                candidate_bytes -= candidate_sizes.pop(message_id, 0)
                candidates[message_id] = persisted_candidate
                candidate_sizes[message_id] = encoded_size
                candidate_bytes += encoded_size
                while (
                    len(candidates) > candidate_count_budget
                    or candidate_bytes > candidate_byte_budget
                ):
                    stale_message_id, _stale_item = candidates.popitem(last=False)
                    candidate_bytes -= candidate_sizes.pop(stale_message_id, 0)

            if watermark_ms is not None and any(
                create_time is not None and create_time < watermark_ms
                for create_time in (
                    self._api_poll_item_create_time_ms(item)
                    for item in page_items
                )
            ):
                return list(candidates.values())
            if data.get("has_more") is not True:
                return list(candidates.values())
            next_page_token = str(data.get("page_token") or "").strip()
            if not next_page_token or next_page_token == page_token:
                if page_token:
                    raise _FeishuApiPollInvalidContinuation(
                        "message list pagination returned a repeated page token",
                        persisted_page_token or page_token,
                    )
                raise RuntimeError("message list pagination returned an invalid page token")
            page_token = next_page_token

            if (
                scanned_candidates >= _MAX_API_POLL_SCAN_ITEMS
                or page_index + 1 >= max_pages
            ):
                logger.warning(
                    "[Feishu] API polling scan checkpointed for chat %s before watermark=%s",
                    chat_id,
                    watermark_ms,
                )
                return _FeishuApiPollScanContinuation(
                    state={
                        "page_token": page_token,
                        "watermark_ms": watermark_ms,
                        "cursor_message_ids": sorted(scan_cursor_ids),
                        "candidates": list(candidates.values()),
                        "baselined": scan_baselined,
                    }
                )

        raise RuntimeError(f"API polling pagination did not terminate for chat {chat_id}")

    def _fetch_tenant_access_token_via_api(self) -> str:
        base_url = "https://open.larksuite.com" if self._domain_name == "lark" else "https://open.feishu.cn"
        body = json.dumps({"app_id": self._app_id, "app_secret": self._app_secret}).encode("utf-8")
        request = Request(
            f"{base_url}/open-apis/auth/v3/tenant_access_token/internal",
            data=body,
            headers={"Content-Type": "application/json"},
        )
        with urlopen(request, timeout=15) as response:
            payload = json.loads(response.read().decode("utf-8"))
        token = str(payload.get("tenant_access_token") or "").strip()
        if payload.get("code") != 0 or not token:
            raise RuntimeError(f"tenant token failed: {payload.get('code')} {payload.get('msg')}")
        return token

    @staticmethod
    def _api_user_id_object(raw_id: str, raw_id_type: str) -> Any:
        id_type = str(raw_id_type or "").strip()
        value = str(raw_id or "").strip()
        return SimpleNamespace(
            open_id=value if id_type == "open_id" else None,
            user_id=value if id_type == "user_id" else None,
            union_id=value if id_type == "union_id" else None,
        )

    def _api_message_item_to_event_data(self, item: Dict[str, Any]) -> Any:
        sender = item.get("sender") or {}
        body = item.get("body") or {}
        mentions = []
        for raw in item.get("mentions") or []:
            mentions.append(
                SimpleNamespace(
                    key=str(raw.get("key") or ""),
                    name=str(raw.get("name") or ""),
                    id=self._api_user_id_object(str(raw.get("id") or ""), str(raw.get("id_type") or "")),
                )
            )
        message = SimpleNamespace(
            message_id=str(item.get("message_id") or ""),
            message_type=str(item.get("msg_type") or ""),
            content=str(body.get("content") or ""),
            chat_id=str(item.get("chat_id") or ""),
            chat_type="group",
            create_time=str(item.get("create_time") or ""),
            update_time=str(item.get("update_time") or ""),
            root_id=str(item.get("root_id") or "") or None,
            parent_id=str(item.get("parent_id") or "") or None,
            upper_message_id=str(item.get("parent_id") or item.get("root_id") or "") or None,
            mentions=mentions,
        )
        sender_obj = SimpleNamespace(
            sender_type=str(sender.get("sender_type") or "user"),
            sender_id=self._api_user_id_object(str(sender.get("id") or ""), str(sender.get("id_type") or "")),
        )
        data = SimpleNamespace(event=SimpleNamespace(message=message, sender=sender_obj))
        setattr(data, "_hermes_ingress_source", "api_poll")
        return data

    async def _cancel_pending_tasks(self, tasks: Dict[str, asyncio.Task]) -> None:
        pending = [task for task in tasks.values() if task and not task.done()]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        tasks.clear()

    def _reset_batch_buffers(self) -> None:
        self._pending_text_batches.clear()
        self._pending_text_batch_counts.clear()
        self._pending_media_batches.clear()

    def _disable_websocket_auto_reconnect(self) -> None:
        if self._ws_client is None:
            return
        try:
            setattr(self._ws_client, "_auto_reconnect", False)
        except Exception:
            pass
        finally:
            self._ws_client = None

    async def _stop_webhook_server(self) -> None:
        if self._webhook_runner is None:
            return
        try:
            await self._webhook_runner.cleanup()
        finally:
            self._webhook_runner = None
            self._webhook_site = None

    # =========================================================================
    # Outbound — send / edit / send_image / send_voice / …
    # =========================================================================

    def _record_only_outbound_result(
        self,
        *,
        operation: str,
        chat_id: str,
        payload_type: str,
        payload: Any,
        metadata: Optional[Dict[str, Any]] = None,
        thread_id: Optional[str] = None,
        message_id: Optional[str] = None,
        reply_mode: str = "none",
        update_mode: str = "none",
    ) -> Optional[SendResult]:
        try:
            recorder = get_record_only_transport("gateway.feishu.adapter")
        except Exception as exc:
            return SendResult(success=False, error=f"record-only configuration refused outbound: {exc}")
        if recorder is None:
            return None
        payload_value = payload
        if payload_type == "interactive_card" and isinstance(payload, str):
            try:
                payload_value = json.loads(payload)
            except json.JSONDecodeError as exc:
                return SendResult(success=False, error=f"record-only refused invalid card JSON: {exc}")
        meta = dict(metadata or {})
        task_id = meta.get("task_id")
        terminal_state = meta.get("terminal_state")
        dedupe_key = meta.get("dedupe_key")
        try:
            result = recorder.record(
                operation=operation,
                platform="feishu",
                destination_kind="message" if update_mode != "none" else ("thread" if thread_id else "chat"),
                destination_id=chat_id,
                thread_id=thread_id,
                message_id=message_id,
                payload_type=payload_type,
                payload=payload_value,
                task_id=task_id if isinstance(task_id, str) and task_id else None,
                terminal_state=terminal_state if isinstance(terminal_state, str) and terminal_state else None,
                reply_mode=reply_mode,
                update_mode=update_mode,
                caller_dedupe_key=dedupe_key if isinstance(dedupe_key, str) and dedupe_key else None,
                metadata=meta,
            )
        except Exception as exc:
            return SendResult(success=False, error=f"record-only refused outbound: {exc}")
        return SendResult(success=True, message_id=result.message_id, raw_response=result)

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send a Feishu message."""
        formatted = self.format_message(content)
        thread_id = str((metadata or {}).get("thread_id") or "") or None
        recorded = self._record_only_outbound_result(
            operation="text_reply" if (reply_to or thread_id) else "text_send",
            chat_id=chat_id,
            payload_type="text",
            payload=formatted,
            metadata=metadata,
            thread_id=thread_id,
            message_id=reply_to,
            reply_mode="message" if reply_to else ("thread" if thread_id else "none"),
        )
        if recorded is not None:
            return recorded
        if not self._client:
            return SendResult(success=False, error="Not connected")

        chunks = self.truncate_message(formatted, self.MAX_MESSAGE_LENGTH)
        last_response = None

        try:
            for chunk in chunks:
                msg_type, payload = self._build_outbound_payload(chunk)
                try:
                    response = await self._feishu_send_with_retry(
                        chat_id=chat_id,
                        msg_type=msg_type,
                        payload=payload,
                        reply_to=reply_to,
                        metadata=metadata,
                    )
                except Exception as exc:
                    if msg_type != "post" or not _POST_CONTENT_INVALID_RE.search(str(exc)):
                        raise
                    logger.warning("[Feishu] Invalid post payload rejected by API; falling back to plain text")
                    response = await self._feishu_send_with_retry(
                        chat_id=chat_id,
                        msg_type="text",
                        payload=json.dumps({"text": _strip_markdown_to_plain_text(chunk)}, ensure_ascii=False),
                        reply_to=reply_to,
                        metadata=metadata,
                    )
                if (
                    msg_type == "post"
                    and not self._response_succeeded(response)
                    and _POST_CONTENT_INVALID_RE.search(str(getattr(response, "msg", "") or ""))
                ):
                    logger.warning("[Feishu] Post payload rejected by API response; falling back to plain text")
                    response = await self._feishu_send_with_retry(
                        chat_id=chat_id,
                        msg_type="text",
                        payload=json.dumps({"text": _strip_markdown_to_plain_text(chunk)}, ensure_ascii=False),
                        reply_to=reply_to,
                        metadata=metadata,
                    )
                last_response = response

            return self._finalize_send_result(last_response, "send failed")
        except Exception as exc:
            logger.error("[Feishu] Send error: %s", exc, exc_info=True)
            return SendResult(success=False, error=str(exc))

    def _record_outbound_bot_fingerprint(
        self,
        *,
        chat_id: str,
        content: str,
        message_id: str | None,
        metadata: Optional[Dict[str, Any]],
        category: str,
    ) -> None:
        scope = metadata or {}
        record_feishu_bot_message_fingerprint(
            self._feishu_bot_message_registry,
            platform=self.platform,
            chat_id=chat_id,
            thread_id=str(scope.get("thread_id") or "").strip() or None,
            message_id=message_id,
            content=content,
            category=category,
        )

    async def edit_message(
        self,
        chat_id: str,
        message_id: str,
        content: str,
        *,
        finalize: bool = False,
    ) -> SendResult:
        """Edit a previously sent Feishu text/post message."""
        content = self.format_message(content)
        recorded = self._record_only_outbound_result(
            operation="text_update",
            chat_id=chat_id,
            payload_type="text",
            payload=content,
            message_id=message_id,
            update_mode="patch",
        )
        if recorded is not None:
            return recorded
        if not self._client:
            return SendResult(success=False, error="Not connected")

        try:
            msg_type, payload = self._build_outbound_payload(content)
            body = self._build_update_message_body(msg_type=msg_type, content=payload)
            request = self._build_update_message_request(message_id=message_id, request_body=body)
            response = await self._run_blocking(self._client.im.v1.message.update, request)
            result = self._finalize_send_result(response, "update failed")
            if not result.success and msg_type == "post" and _POST_CONTENT_INVALID_RE.search(result.error or ""):
                logger.warning("[Feishu] Invalid post update payload rejected by API; falling back to plain text")
                fallback_body = self._build_update_message_body(
                    msg_type="text",
                    content=json.dumps({"text": _strip_markdown_to_plain_text(content)}, ensure_ascii=False),
                )
                fallback_request = self._build_update_message_request(message_id=message_id, request_body=fallback_body)
                fallback_response = await self._run_blocking(self._client.im.v1.message.update, fallback_request)
                result = self._finalize_send_result(fallback_response, "update failed")
            if result.success:
                result.message_id = message_id
            return result
        except Exception as exc:
            logger.error("[Feishu] Failed to edit message %s: %s", message_id, exc, exc_info=True)
            return SendResult(success=False, error=str(exc))

    async def send_exec_approval(
        self, chat_id: str, command: str, session_key: str,
        description: str = "dangerous command",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send an interactive card with approval buttons.

        The buttons carry ``hermes_action`` in their value dict so that
        ``_handle_card_action_event`` can intercept them and call
        ``resolve_gateway_approval()`` to unblock the waiting agent thread.
        """
        try:
            recorder = get_record_only_transport("gateway.feishu.adapter")
        except Exception as exc:
            return SendResult(success=False, error=f"record-only configuration refused outbound: {exc}")
        if not self._client and recorder is None:
            return SendResult(success=False, error="Not connected")

        try:
            self._gc_stale_approval_state()
            approval_id = next(self._approval_counter)
            cmd_preview = command[:3000] + "..." if len(command) > 3000 else command
            approval_kind = str((metadata or {}).get("approval_kind") or "exec").strip().lower()
            requested_role = str((metadata or {}).get("requested_role") or "").strip().lower()
            target_user_id = str((metadata or {}).get("target_user_id") or "").strip()
            target_user_name = str((metadata or {}).get("target_user_name") or "").strip()
            request_chat_id = str((metadata or {}).get("request_chat_id") or "").strip()
            request_thread_id = str((metadata or {}).get("request_thread_id") or "").strip()
            request_chat_name = str((metadata or {}).get("request_chat_name") or "").strip()
            request_message_id = str((metadata or {}).get("request_message_id") or "").strip()
            request_text = str((metadata or {}).get("request_text") or "").strip()

            def _btn(label: str, action_name: str, btn_type: str = "default") -> dict:
                return {
                    "tag": "button",
                    "text": {"tag": "plain_text", "content": label},
                    "type": btn_type,
                    "value": {"hermes_action": action_name, "approval_id": approval_id},
                }

            if approval_kind == "permission_grant":
                title = "🛂 Permission Grant Approval Required"
                header_template = "orange"
                role_value = requested_role if requested_role in _PERMISSION_GRANT_ALLOWED_ROLES else "member"
                primary_label = (
                    f"✅ Approve {role_value.title()}"
                    if role_value != "member"
                    else "✅ Approve"
                )
                body = (
                    f"**Applicant:** {target_user_name or '(unknown)'}\n"
                    f"**User ID:** `{target_user_id or '(missing)'}`\n"
                    f"**Requested role:** `{role_value}`\n"
                    f"**Source chat:** {request_chat_name or '(unknown)'}\n"
                    f"**Request message ID:** `{request_message_id or '(missing)'}`\n"
                    f"**Requested action:** {description}\n"
                    f"**Original request:** {request_text or description}"
                )
                actions = [
                    {
                        "tag": "button",
                        "text": {"tag": "plain_text", "content": primary_label},
                        "type": "primary",
                        "value": {
                            "hermes_action": "grant_permission",
                            "approval_id": approval_id,
                            "requested_role": role_value,
                        },
                    },
                    {
                        "tag": "button",
                        "text": {"tag": "plain_text", "content": "✅ Approve Member"},
                        "type": "default",
                        "value": {
                            "hermes_action": "grant_permission",
                            "approval_id": approval_id,
                            "requested_role": "member",
                        },
                    },
                    _btn("❌ Deny", "deny", "danger"),
                ]
            else:
                title = "⚠️ Command Approval Required"
                header_template = "orange"
                body = f"```\n{cmd_preview}\n```\n**Reason:** {description}"
                actions = [
                    _btn("✅ Allow Once", "approve_once", "primary"),
                    _btn("✅ Session", "approve_session"),
                    _btn("✅ Always", "approve_always"),
                    _btn("❌ Deny", "deny", "danger"),
                ]

            card = {
                "config": {"wide_screen_mode": True},
                "header": {
                    "title": {"content": title, "tag": "plain_text"},
                    "template": header_template,
                },
                "elements": [
                    {
                        "tag": "markdown",
                        "content": body,
                    },
                    {
                        "tag": "action",
                        "actions": actions,
                    },
                ],
            }

            payload = json.dumps(card, ensure_ascii=False)
            response = await self._feishu_send_with_retry(
                chat_id=chat_id,
                msg_type="interactive",
                payload=payload,
                reply_to=None,
                metadata=metadata,
            )

            result = self._finalize_send_result(response, "send_exec_approval failed")
            if result.success:
                self._approval_state[approval_id] = {
                    "session_key": session_key,
                    "message_id": result.message_id or "",
                    "chat_id": chat_id,
                    "created_at": time.monotonic(),
                    "approval_kind": approval_kind,
                    "target_user_id": target_user_id,
                    "target_user_name": target_user_name,
                    "requested_role": requested_role,
                    "description": description,
                    "request_chat_id": request_chat_id,
                    "request_thread_id": request_thread_id,
                    "request_chat_name": request_chat_name,
                    "request_message_id": request_message_id,
                    "request_text": request_text,
                }
            return result
        except Exception as exc:
            logger.warning("[Feishu] send_exec_approval failed: %s", exc)
            return SendResult(success=False, error=str(exc))

    def register_approval_timeout_callback(self, session_key: str, callback: Any) -> None:
        """Register a callback to be invoked when an approval request times out.

        The callback will be called with a single argument: the choice string "timeout".
        """
        self._approval_callbacks[session_key] = callback

    @staticmethod
    def _build_update_prompt_card(*, prompt: str, default: str, prompt_id: int) -> Dict[str, Any]:
        default_hint = f"\n\nDefault: `{default}`" if default else ""

        def _btn(label: str, answer: str, btn_type: str) -> dict:
            return {
                "tag": "button",
                "text": {"tag": "plain_text", "content": label},
                "type": btn_type,
                "value": {
                    "hermes_update_prompt_action": answer,
                    "update_prompt_id": prompt_id,
                },
            }

        return {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"content": "⚕ Update Needs Your Input", "tag": "plain_text"},
                "template": "orange",
            },
            "elements": [
                {"tag": "markdown", "content": f"{prompt}{default_hint}"},
                {
                    "tag": "action",
                    "actions": [
                        _btn("✓ Yes", "y", "primary"),
                        _btn("✗ No", "n", "danger"),
                    ],
                },
            ],
        }

    async def send_update_prompt(
        self, chat_id: str, prompt: str, default: str = "",
        session_key: str = "",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send an interactive update prompt with Yes/No buttons."""
        try:
            recorder = get_record_only_transport("gateway.feishu.adapter")
        except Exception as exc:
            return SendResult(success=False, error=f"record-only configuration refused outbound: {exc}")
        if not self._client and recorder is None:
            return SendResult(success=False, error="Not connected")

        try:
            prompt_id = next(self._update_prompt_counter)
            payload = json.dumps(
                self._build_update_prompt_card(prompt=prompt, default=default, prompt_id=prompt_id),
                ensure_ascii=False,
            )
            response = await self._feishu_send_with_retry(
                chat_id=chat_id,
                msg_type="interactive",
                payload=payload,
                reply_to=None,
                metadata=metadata,
            )

            result = self._finalize_send_result(response, "send_update_prompt failed")
            if result.success:
                self._update_prompt_state[prompt_id] = {
                    "session_key": session_key,
                    "message_id": result.message_id or "",
                    "chat_id": chat_id,
                }
            return result
        except Exception as exc:
            logger.warning("[Feishu] send_update_prompt failed: %s", exc)
            return SendResult(success=False, error=str(exc))

    @staticmethod
    def _build_resolved_approval_card(
        *,
        choice: str,
        user_name: str,
        approval_kind: str = "exec",
        target_user_name: str = "",
        requested_role: str = "",
    ) -> Dict[str, Any]:
        """Build raw card JSON for a resolved approval action."""
        icon = "❌" if choice == "deny" else "✅"
        label = _APPROVAL_LABEL_MAP.get(choice, "Resolved")
        if approval_kind == "permission_grant":
            resolved_role = requested_role or "member"
            outcome_line = (
                f"{icon} **{target_user_name or '该用户'}** 权限申请已通过\n"
                f"**角色：** `{resolved_role}`\n"
                f"**审批人：** {user_name}"
                if choice != "deny"
                else f"{icon} **{target_user_name or '该用户'}** 权限申请未通过\n**审批人：** {user_name}"
            )
            return {
                "config": {"wide_screen_mode": True},
                "header": {
                    "title": {"content": f"{icon} {label}", "tag": "plain_text"},
                    "template": "red" if choice == "deny" else "green",
                },
                "elements": [
                    {
                        "tag": "markdown",
                        "content": outcome_line,
                    },
                ],
            }
        return {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"content": f"{icon} {label}", "tag": "plain_text"},
                "template": "red" if choice == "deny" else "green",
            },
            "elements": [
                {
                    "tag": "markdown",
                    "content": f"{icon} **{label}** by {user_name}",
                },
            ],
        }

    @staticmethod
    def _build_permission_role_updated_card(*, user_name: str, requested_role: str) -> Dict[str, Any]:
        return {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"content": "📝 Requested role updated", "tag": "plain_text"},
                "template": "blue",
            },
            "elements": [
                {
                    "tag": "markdown",
                    "content": (
                        f"📝 **Requested role:** `{requested_role}`\n"
                        f"Updated by {user_name}."
                    ),
                },
            ],
        }

    @staticmethod
    def _build_resolved_update_prompt_card(*, answer: str, user_name: str) -> Dict[str, Any]:
        yes = answer == "y"
        label = "Yes" if yes else "No"
        return {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"content": f"{'✅' if yes else '❌'} Update prompt answered: {label}", "tag": "plain_text"},
                "template": "green" if yes else "red",
            },
            "elements": [
                {"tag": "markdown", "content": f"Answered by **{user_name}**"},
            ],
        }

    @staticmethod
    def _build_permission_denied_approval_card(*, user_name: str) -> Dict[str, Any]:
        """Build raw card JSON for a rejected approval click without sufficient privileges."""
        return {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"content": "⛔ Approval Not Allowed", "tag": "plain_text"},
                "template": "red",
            },
            "elements": [
                {
                    "tag": "markdown",
                    "content": (
                        f"⛔ **{user_name}** does not have permission to approve this command. "
                        "Ask the requester, an admin, or the owner to approve it."
                    ),
                },
            ],
        }

    @staticmethod
    def _build_expired_approval_card() -> Dict[str, Any]:
        """Build raw card JSON for an expired approval request."""
        return {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"content": "⏱️ Approval Expired", "tag": "plain_text"},
                "template": "grey",
            },
            "elements": [
                {
                    "tag": "markdown",
                    "content": "⏱️ This approval request has expired and is no longer valid.",
                },
            ],
        }

    def _gc_stale_approval_state(self, max_age_seconds: float = _FEISHU_APPROVAL_STATE_TTL_SECONDS) -> None:
        """Drop stale approval button state entries that are unlikely to be resolved."""
        now = time.monotonic()
        stale_ids = [
            approval_id
            for approval_id, state in self._approval_state.items()
            if now - float(state.get("created_at", 0) or 0) > max_age_seconds
        ]
        for approval_id in stale_ids:
            state = self._approval_state.pop(approval_id, None)
            if state:
                # Fire timeout callback if registered
                session_key = state.get("session_key", "")
                if session_key and session_key in self._approval_callbacks:
                    callback = self._approval_callbacks.pop(session_key, None)
                    if callback:
                        try:
                            callback("timeout")
                        except Exception as exc:
                            logger.warning("[Feishu] approval timeout callback failed: %s", exc)

                # Update Feishu card to expired state
                message_id = state.get("message_id", "")
                chat_id = state.get("chat_id", "")
                if message_id and chat_id and self._loop and not self._loop.is_closed():
                    try:
                        asyncio.run_coroutine_threadsafe(
                            self._update_approval_card_to_expired(message_id, chat_id),
                            self._loop
                        )
                    except Exception as exc:
                        logger.warning("[Feishu] failed to schedule expired card update: %s", exc)

    async def _update_approval_card_to_expired(self, message_id: str, chat_id: str) -> None:
        """Update a Feishu approval card to show expired state via message PATCH API."""
        card = self._build_expired_approval_card()
        recorded = self._record_only_outbound_result(
            operation="card_update",
            chat_id=chat_id,
            payload_type="interactive_card",
            payload=card,
            message_id=message_id,
            update_mode="patch",
        )
        if recorded is not None:
            if not recorded.success:
                logger.warning("[Feishu] %s", recorded.error)
            return
        if not self._client:
            return
        try:
            payload = json.dumps(card, ensure_ascii=False)
            body = self._build_update_message_body(msg_type="interactive", content=payload)
            request = self._build_update_message_request(message_id=message_id, request_body=body)
            response = await asyncio.to_thread(self._client.im.v1.message.update, request)
            result = self._finalize_send_result(response, "expired card update failed")
            if result.success:
                logger.info("[Feishu] Updated approval card %s to expired state", message_id)
            else:
                logger.warning("[Feishu] Failed to update approval card %s to expired: %s", message_id, result.error)
        except Exception as exc:
            logger.warning("[Feishu] Exception updating approval card to expired: %s", exc)

    @staticmethod
    def _write_update_prompt_response(answer: str) -> None:
        response_path = get_hermes_home() / ".update_response"
        tmp_path = response_path.with_suffix(".tmp")
        tmp_path.write_text(answer)
        tmp_path.replace(response_path)

    async def send_voice(
        self,
        chat_id: str,
        audio_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> SendResult:
        """Send audio to Feishu as a file attachment plus optional caption."""
        return await self._send_uploaded_file_message(
            chat_id=chat_id,
            file_path=audio_path,
            reply_to=reply_to,
            metadata=metadata,
            caption=caption,
            outbound_message_type="audio",
        )

    async def send_document(
        self,
        chat_id: str,
        file_path: str,
        caption: Optional[str] = None,
        file_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> SendResult:
        """Send a document/file attachment to Feishu."""
        return await self._send_uploaded_file_message(
            chat_id=chat_id,
            file_path=file_path,
            reply_to=reply_to,
            metadata=metadata,
            caption=caption,
            file_name=file_name,
        )

    async def send_video(
        self,
        chat_id: str,
        video_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> SendResult:
        """Send a video file to Feishu."""
        return await self._send_uploaded_file_message(
            chat_id=chat_id,
            file_path=video_path,
            reply_to=reply_to,
            metadata=metadata,
            caption=caption,
            outbound_message_type="media",
        )

    async def send_image_file(
        self,
        chat_id: str,
        image_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> SendResult:
        """Send a local image file to Feishu."""
        thread_id = str((metadata or {}).get("thread_id") or "") or None
        recorded = self._record_only_outbound_result(
            operation="file_reply" if (reply_to or thread_id) else "file_send",
            chat_id=chat_id,
            payload_type="image",
            payload={"path": image_path, "caption": caption},
            metadata=metadata,
            thread_id=thread_id,
            message_id=reply_to,
            reply_mode="message" if reply_to else ("thread" if thread_id else "none"),
        )
        if recorded is not None:
            return recorded
        if not self._client:
            return SendResult(success=False, error="Not connected")
        if not os.path.exists(image_path):
            return SendResult(success=False, error=f"Image file not found: {image_path}")

        try:
            import io as _io
            with open(image_path, "rb") as f:
                image_bytes = f.read()
            # Wrap in BytesIO so lark SDK's MultipartEncoder can read .name and .tell()
            image_file = _io.BytesIO(image_bytes)
            image_file.name = os.path.basename(image_path)
            body = self._build_image_upload_body(
                image_type=_FEISHU_IMAGE_UPLOAD_TYPE,
                image=image_file,
            )
            request = self._build_image_upload_request(body)
            upload_response = await self._run_blocking(self._client.im.v1.image.create, request)
            image_key = self._extract_response_field(upload_response, "image_key")
            if not image_key:
                return self._response_error_result(
                    upload_response,
                    default_message="image upload failed",
                    override_error="Feishu image upload missing image_key",
                )

            if caption:
                post_payload = self._build_media_post_payload(
                    caption=caption,
                    media_tag={"tag": "img", "image_key": image_key},
                )
                message_response = await self._feishu_send_with_retry(
                    chat_id=chat_id,
                    msg_type="post",
                    payload=post_payload,
                    reply_to=reply_to,
                    metadata=metadata,
                )
            else:
                message_response = await self._feishu_send_with_retry(
                    chat_id=chat_id,
                    msg_type="image",
                    payload=json.dumps({"image_key": image_key}, ensure_ascii=False),
                    reply_to=reply_to,
                    metadata=metadata,
                )
            return self._finalize_send_result(message_response, "image send failed")
        except Exception as exc:
            logger.error("[Feishu] Failed to send image %s: %s", image_path, exc, exc_info=True)
            return SendResult(success=False, error=str(exc))

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        """Feishu bot API does not expose a typing indicator."""
        return None

    async def send_image(
        self,
        chat_id: str,
        image_url: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Download a remote image then send it through the native Feishu image flow."""
        thread_id = str((metadata or {}).get("thread_id") or "") or None
        recorded = self._record_only_outbound_result(
            operation="file_reply" if (reply_to or thread_id) else "file_send",
            chat_id=chat_id,
            payload_type="image",
            payload={"url": image_url, "caption": caption},
            metadata=metadata,
            thread_id=thread_id,
            message_id=reply_to,
            reply_mode="message" if reply_to else ("thread" if thread_id else "none"),
        )
        if recorded is not None:
            return recorded
        try:
            image_path = await self._download_remote_image(image_url)
        except Exception as exc:
            logger.error("[Feishu] Failed to download image %s: %s", image_url, exc, exc_info=True)
            return await super().send_image(
                chat_id=chat_id,
                image_url=image_url,
                caption=caption,
                reply_to=reply_to,
                metadata=metadata,
            )
        return await self.send_image_file(
            chat_id=chat_id,
            image_path=image_path,
            caption=caption,
            reply_to=reply_to,
            metadata=metadata,
        )

    async def send_animation(
        self,
        chat_id: str,
        animation_url: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Feishu has no native GIF bubble; degrade to a downloadable file."""
        thread_id = str((metadata or {}).get("thread_id") or "") or None
        recorded = self._record_only_outbound_result(
            operation="file_reply" if (reply_to or thread_id) else "file_send",
            chat_id=chat_id,
            payload_type="animation",
            payload={"url": animation_url, "caption": caption},
            metadata=metadata,
            thread_id=thread_id,
            message_id=reply_to,
            reply_mode="message" if reply_to else ("thread" if thread_id else "none"),
        )
        if recorded is not None:
            return recorded
        try:
            file_path, file_name = await self._download_remote_document(
                animation_url,
                default_ext=".gif",
                preferred_name="animation.gif",
            )
        except Exception as exc:
            logger.error("[Feishu] Failed to download animation %s: %s", animation_url, exc, exc_info=True)
            return await super().send_animation(
                chat_id=chat_id,
                animation_url=animation_url,
                caption=caption,
                reply_to=reply_to,
                metadata=metadata,
            )
        degraded_caption = f"[GIF downgraded to file]\n{caption}" if caption else "[GIF downgraded to file]"
        return await self.send_document(
            chat_id=chat_id,
            file_path=file_path,
            file_name=file_name,
            caption=degraded_caption,
            reply_to=reply_to,
            metadata=metadata,
        )

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        """Return real chat metadata from Feishu when available."""
        fallback = {
            "chat_id": chat_id,
            "name": chat_id,
            "type": "dm",
        }
        if not self._client:
            return fallback

        cached = self._chat_info_cache.get(chat_id)
        if cached is not None:
            return dict(cached)

        try:
            request = self._build_get_chat_request(chat_id)
            response = await self._run_blocking(self._client.im.v1.chat.get, request)
            if not response or getattr(response, "success", lambda: False)() is False:
                code = getattr(response, "code", "unknown")
                msg = getattr(response, "msg", "chat lookup failed")
                logger.warning("[Feishu] Failed to get chat info for %s: [%s] %s", chat_id, code, msg)
                return fallback

            data = getattr(response, "data", None)
            raw_chat_type = str(getattr(data, "chat_type", "") or "").strip().lower()
            info = {
                "chat_id": chat_id,
                "name": str(getattr(data, "name", None) or chat_id),
                "type": self._map_chat_type(raw_chat_type),
                "raw_type": raw_chat_type or None,
            }
            self._chat_info_cache[chat_id] = info
            return dict(info)
        except Exception:
            logger.warning("[Feishu] Failed to get chat info for %s", chat_id, exc_info=True)
            return fallback

    def format_message(self, content: str) -> str:
        """Feishu text messages are plain text by default."""
        return content.strip()

    # =========================================================================
    # Inbound event handlers
    # =========================================================================

    def _on_message_event(self, data: Any) -> None:
        """Normalize Feishu inbound events into MessageEvent.

        Called by the lark_oapi SDK's event dispatcher on a background thread.
        If the adapter loop is not currently accepting callbacks (brief window
        during startup/restart or network-flap reconnect), the event is queued
        for replay instead of dropped.
        """
        loop = self._loop
        if not self._loop_accepts_callbacks(loop):
            start_drainer = self._enqueue_pending_inbound_event(data)
            if start_drainer:
                threading.Thread(
                    target=self._drain_pending_inbound_events,
                    name="feishu-pending-inbound-drainer",
                    daemon=True,
                ).start()
            return
        self._submit_on_loop(loop, self._handle_message_event_data(data))

    def _enqueue_pending_inbound_event(self, data: Any) -> bool:
        """Append an event to the pending-inbound queue.

        Returns True if the caller should spawn a drainer thread (no drainer
        currently scheduled), False if a drainer is already running and will
        pick up the new event on its next pass.
        """
        with self._pending_inbound_lock:
            if len(self._pending_inbound_events) >= self._pending_inbound_max_depth:
                # Queue full — drop the oldest to make room. This happens only
                # if the loop stays unavailable for an extended period AND the
                # WS keeps firing callbacks. Still better than silent drops.
                dropped = self._pending_inbound_events.pop(0)
                try:
                    event = getattr(dropped, "event", None)
                    message = getattr(event, "message", None)
                    message_id = str(getattr(message, "message_id", "") or "unknown")
                except Exception:
                    message_id = "unknown"
                logger.error(
                    "[Feishu] Pending-inbound queue full (%d); dropped oldest event %s",
                    self._pending_inbound_max_depth,
                    message_id,
                )
            self._pending_inbound_events.append(data)
            depth = len(self._pending_inbound_events)
            should_start = not self._pending_drain_scheduled
            if should_start:
                self._pending_drain_scheduled = True
        logger.warning(
            "[Feishu] Queued inbound event for replay (loop not ready, queue depth=%d)",
            depth,
        )
        return should_start

    def _drain_pending_inbound_events(self) -> None:
        """Replay queued inbound events once the adapter loop is ready.

        Runs in a dedicated daemon thread. Polls ``_running`` and
        ``_loop_accepts_callbacks`` until events can be dispatched or the
        adapter shuts down. A single drainer handles the entire queue;
        concurrent ``_on_message_event`` calls just append.
        """
        poll_interval = 0.25
        max_wait_seconds = 120.0  # safety cap: drop queue after 2 minutes
        waited = 0.0
        try:
            while True:
                if not getattr(self, "_running", True):
                    # Adapter shutting down — drop queued events rather than
                    # holding them against a closed loop.
                    with self._pending_inbound_lock:
                        dropped = len(self._pending_inbound_events)
                        self._pending_inbound_events.clear()
                    if dropped:
                        logger.warning(
                            "[Feishu] Dropped %d queued inbound event(s) during shutdown",
                            dropped,
                        )
                    return
                loop = self._loop
                if self._loop_accepts_callbacks(loop):
                    with self._pending_inbound_lock:
                        batch = self._pending_inbound_events[:]
                        self._pending_inbound_events.clear()
                    if not batch:
                        # Queue emptied between check and grab; done.
                        with self._pending_inbound_lock:
                            if not self._pending_inbound_events:
                                return
                        continue
                    dispatched = 0
                    requeue: List[Any] = []
                    for event in batch:
                        if self._submit_on_loop(
                            loop, self._handle_message_event_data(event)
                        ):
                            dispatched += 1
                        else:
                            # Loop closed/unavailable — requeue and poll again.
                            requeue.append(event)
                    if requeue:
                        with self._pending_inbound_lock:
                            self._pending_inbound_events[:0] = requeue
                    if dispatched:
                        logger.info(
                            "[Feishu] Replayed %d queued inbound event(s)",
                            dispatched,
                        )
                    if not requeue:
                        # Successfully drained; check if more arrived while
                        # we were dispatching and exit if not.
                        with self._pending_inbound_lock:
                            if not self._pending_inbound_events:
                                return
                    # More events queued or requeue pending — loop again.
                    continue
                if waited >= max_wait_seconds:
                    with self._pending_inbound_lock:
                        dropped = len(self._pending_inbound_events)
                        self._pending_inbound_events.clear()
                    logger.error(
                        "[Feishu] Adapter loop unavailable for %.0fs; "
                        "dropped %d queued inbound event(s)",
                        max_wait_seconds,
                        dropped,
                    )
                    return
                time.sleep(poll_interval)
                waited += poll_interval
        finally:
            with self._pending_inbound_lock:
                self._pending_drain_scheduled = False

    async def _handle_message_event_data(self, data: Any) -> bool:
        """Shared inbound message handling for websocket and webhook transports."""
        event = getattr(data, "event", None)
        message = getattr(event, "message", None)
        sender = getattr(event, "sender", None)
        if not getattr(data, "_hermes_ingress_source", None):
            setattr(data, "_hermes_ingress_source", "event_callback")
        if not message or not sender or not getattr(sender, "sender_id", None):
            logger.debug("[Feishu] Dropping malformed inbound event: missing message/sender")
            return True

        message_id = getattr(message, "message_id", None)
        if not message_id or not self._begin_message_processing(message_id):
            logger.debug("[Feishu] Dropping duplicate/missing message_id: %s", message_id)
            return bool(message_id and self._message_processing_completed(message_id))

        try:
            reason = self._admit(sender, message)
            if reason is not None:
                logger.info(
                    "[Feishu] Dropping inbound event before processing: reason=%s message_id=%s chat_id=%s chat_type=%s",
                    reason,
                    message_id,
                    getattr(message, "chat_id", "") or "",
                    getattr(message, "chat_type", "") or "",
                )
                self._complete_message_processing(message_id)
                return True

            chat_type = getattr(message, "chat_type", "p2p")
            durable_completion = await self._process_inbound_message(
                data=data,
                message=message,
                sender_id=getattr(sender, "sender_id", None),
                chat_type=chat_type,
                message_id=message_id,
                is_bot=_is_bot_sender(sender),
            )
            if durable_completion is False:
                # The RCA queue worker owns this inbox entry until Gateway has
                # committed the source-neutral control-store admission.
                return self._message_processing_completed(message_id)
        except BaseException:
            self._abandon_message_processing(message_id)
            raise
        else:
            self._complete_message_processing(message_id)
            return True

    def _on_message_read_event(self, data: P2ImMessageMessageReadV1) -> None:
        """Ignore read-receipt events that Hermes does not act on."""
        event = getattr(data, "event", None)
        message = getattr(event, "message", None)
        message_id = getattr(message, "message_id", None) or ""
        logger.debug("[Feishu] Ignoring message_read event: %s", message_id)

    def _on_bot_added_to_chat(self, data: Any) -> None:
        """Handle bot being added to a group chat."""
        event = getattr(data, "event", None)
        chat_id = str(getattr(event, "chat_id", "") or "")
        logger.info("[Feishu] Bot added to chat: %s", chat_id)
        self._chat_info_cache.pop(chat_id, None)

    def _on_bot_removed_from_chat(self, data: Any) -> None:
        """Handle bot being removed from a group chat."""
        event = getattr(data, "event", None)
        chat_id = str(getattr(event, "chat_id", "") or "")
        logger.info("[Feishu] Bot removed from chat: %s", chat_id)
        self._chat_info_cache.pop(chat_id, None)

    def _on_p2p_chat_entered(self, data: Any) -> None:
        logger.debug("[Feishu] User entered P2P chat with bot")

    def _on_message_recalled(self, data: Any) -> None:
        logger.debug("[Feishu] Message recalled by user")

    def _on_drive_comment_event(self, data: Any) -> None:
        """Handle drive document comment notification (drive.notice.comment_add_v1).

        Delegates to :mod:`gateway.platforms.feishu_comment` for parsing,
        logging, and reaction.  Scheduling follows the same
        ``run_coroutine_threadsafe`` pattern used by ``_on_message_event``.
        """
        from plugins.platforms.feishu.feishu_comment import handle_drive_comment_event

        loop = self._loop
        if not self._loop_accepts_callbacks(loop):
            logger.warning("[Feishu] Dropping drive comment event before adapter loop is ready")
            return
        self._submit_on_loop(
            loop,
            handle_drive_comment_event(self._client, data, self_open_id=self._bot_open_id),
        )

    def _on_meeting_invited_event(self, data: Any) -> None:
        """Handle VC bot meeting invitation notification (vc.bot.meeting_invited_v1)."""
        from plugins.platforms.feishu.feishu_meeting_invite import handle_meeting_invited_event

        loop = self._loop
        if not self._loop_accepts_callbacks(loop):
            logger.warning("[Feishu] Dropping meeting invite event before adapter loop is ready")
            return
        self._submit_on_loop(loop, handle_meeting_invited_event(self, data))

    def _on_reaction_event(self, event_type: str, data: Any) -> None:
        """Route user reactions on bot messages as synthetic text events."""
        event = getattr(data, "event", None)
        message_id = str(getattr(event, "message_id", "") or "")
        operator_type = str(getattr(event, "operator_type", "") or "")
        reaction_type_obj = getattr(event, "reaction_type", None)
        emoji_type = str(getattr(reaction_type_obj, "emoji_type", "") or "")
        action = "added" if "created" in event_type else "removed"
        logger.debug(
            "[Feishu] Reaction %s on message %s (operator_type=%s, emoji=%s)",
            action,
            message_id,
            operator_type,
            emoji_type,
        )
        if emoji_type in {_FEISHU_ACK_EMOJI, _FEISHU_REACTION_IN_PROGRESS, _FEISHU_REACTION_FAILURE}:
            return
        # Drop bot/app-origin reactions to break feedback loops. Managed
        # lifecycle emojis are suppressed above for every operator.
        loop = self._loop
        if (
            operator_type in {"bot", "app"}
            or not message_id
            or loop is None
            or bool(getattr(loop, "is_closed", lambda: False)())
        ):
            return
        self._submit_on_loop(loop, self._handle_reaction_event(event_type, data))

    def _on_card_action_trigger(self, data: Any) -> Any:
        """Handle card-action callback from the Feishu SDK (synchronous).

        For approval actions: parses the event once, returns the resolved card
        inline (the only reliable way to sync all clients), and schedules a
        lightweight async method to actually unblock the agent.

        For other card actions: delegates to ``_handle_card_action_event``.
        """
        loop = self._loop
        if not self._loop_accepts_callbacks(loop):
            logger.warning("[Feishu] Dropping card action before adapter loop is ready")
            return P2CardActionTriggerResponse() if P2CardActionTriggerResponse else None

        event = getattr(data, "event", None)
        action = getattr(event, "action", None)
        action_value = getattr(action, "value", {}) or {}
        if isinstance(action_value, str):
            try:
                action_value = json.loads(action_value)
            except (TypeError, ValueError, json.JSONDecodeError):
                logger.debug("[Feishu] Card action value is not valid JSON: %s", action_value)
                action_value = {}
        hermes_action = action_value.get("hermes_action") if isinstance(action_value, dict) else None
        update_prompt_action = (
            action_value.get("hermes_update_prompt_action")
            if isinstance(action_value, dict) else None
        )

        if hermes_action and str(hermes_action).startswith("repo_acl_"):
            return self._handle_repo_acl_card_action(event=event, action_value=action_value)

        if hermes_action == "task_confirm":
            return self._handle_task_confirm_card_action(event=event, action_value=action_value)

        if hermes_action == "intake_clarify":
            return self._handle_intake_clarify_card_action(event=event, action_value=action_value)

        if hermes_action == "rca_clarify":
            return self._handle_rca_clarify_card_action(event=event, action_value=action_value)

        if hermes_action == "clarify":
            return self._handle_clarify_card_action(event=event, action_value=action_value)

        if hermes_action:
            return self._handle_approval_card_action(event=event, action_value=action_value, loop=loop)
        if update_prompt_action:
            return self._handle_update_prompt_card_action(
                event=event,
                action_value=action_value,
                loop=loop,
            )

        self._submit_on_loop(loop, self._handle_card_action_event(data))
        if P2CardActionTriggerResponse is None:
            return None
        return P2CardActionTriggerResponse()

    @staticmethod
    def _loop_accepts_callbacks(loop: Any) -> bool:
        """Return True when the adapter loop can accept thread-safe submissions."""
        return loop is not None and not bool(getattr(loop, "is_closed", lambda: False)())

    def _submit_on_loop(self, loop: Any, coro: Any) -> bool:
        """Schedule background work on the adapter loop with shared failure logging."""
        from agent.async_utils import safe_schedule_threadsafe
        future = safe_schedule_threadsafe(
            coro, loop,
            logger=logger,
            log_message="[Feishu] Failed to schedule background callback work",
            log_level=logging.WARNING,
        )
        if future is None:
            return False
        future.add_done_callback(self._log_background_failure)
        return True

    def _is_interactive_operator_authorized(self, open_id: str) -> bool:
        """Return whether this card-action operator may answer gated prompts."""
        normalized = str(open_id or "").strip()
        if not normalized:
            return False
        allowed_ids = set(self._admins) | set(self._allowed_group_users)
        if not allowed_ids:
            return True
        return "*" in allowed_ids or normalized in allowed_ids

    def _resolve_approval_operator_display_name(self, operator: Any) -> str:
        """Return a user-visible approver name without exposing Feishu IDs."""
        candidates = [
            getattr(operator, "name", None),
            getattr(operator, "user_name", None),
            getattr(operator, "display_name", None),
            getattr(operator, "nickname", None),
            getattr(operator, "en_name", None),
        ]
        ids_to_check = []
        for attr in ("open_id", "user_id", "union_id"):
            value = str(getattr(operator, attr, "") or "").strip()
            if value and value not in ids_to_check:
                ids_to_check.append(value)
        user_id_obj = getattr(operator, "user_id", None)
        for attr in ("open_id", "user_id", "union_id"):
            value = str(getattr(user_id_obj, attr, "") or "").strip()
            if value and value not in ids_to_check:
                ids_to_check.append(value)
        for sender_id in ids_to_check:
            candidates.append(self._get_cached_sender_name(sender_id))
        try:
            from tools.permission_policy import _load_config

            mapping = (_load_config().get("user_id_mapping") or {})
            for sender_id in ids_to_check:
                mapped_name = str(mapping.get(sender_id) or "").strip()
                if mapped_name:
                    candidates.append(mapped_name)
        except Exception:
            logger.debug("[Feishu] approval operator local-name lookup failed", exc_info=True)
        for candidate in candidates:
            name = str(candidate or "").strip()
            if name and not name.startswith(("ou_", "on_", "ou-", "on-", "u_", "u-")):
                return name
        return "审批人"

    def _handle_repo_acl_card_action(self, *, event: Any, action_value: Dict[str, Any]) -> Any:
        """Record repo ACL approval-card clicks without granting repo access."""
        request_id = str(action_value.get("request_id") or "").strip()
        action = str(action_value.get("action") or "").strip()
        if not request_id or not action:
            logger.debug("[Feishu] Repo ACL card action missing request_id/action, ignoring")
            return P2CardActionTriggerResponse() if P2CardActionTriggerResponse else None
        operator = getattr(event, "operator", None)
        user_name = self._resolve_approval_operator_display_name(operator)
        try:
            from tools.repo_acl_approval import build_repo_acl_resolved_card, resolve_repo_acl_card_action

            request = resolve_repo_acl_card_action(request_id, action, user_name)
            response_card = build_repo_acl_resolved_card(request)
        except Exception as exc:
            logger.warning("[Feishu] Failed to resolve repo ACL card action %s/%s: %s", request_id, action, exc)
            response_card = {
                "config": {"wide_screen_mode": True},
                "header": {
                    "template": "red",
                    "title": {"tag": "plain_text", "content": "Repo ACL 审批处理失败"},
                },
                "elements": [
                    {
                        "tag": "markdown",
                        "content": "审批点击未能记录，请联系管理员检查本地 outbox / request store。",
                    }
                ],
            }
        if P2CardActionTriggerResponse is None:
            return None
        response = P2CardActionTriggerResponse()
        if CallBackCard is not None:
            card = CallBackCard()
            card.type = "raw"
            card.data = response_card
            response.card = card
        return response


    def _build_task_confirm_callback_card(self, *, changed: bool, duplicate: bool, choice: str = "") -> Dict[str, Any]:
        if changed:
            text = f"已记录选择：{choice or '已确认'}。我会继续推进。"
            template = "green"
        elif duplicate:
            text = "这个确认已经记录过了；我已刷新卡片状态。"
            template = "blue"
        else:
            text = "确认点击已收到，但没有找到可更新的待确认项。"
            template = "orange"
        return {
            "config": {"wide_screen_mode": True},
            "header": {"title": {"content": "任务确认", "tag": "plain_text"}, "template": template},
            "elements": [{"tag": "markdown", "content": text}],
        }

    def _task_confirm_response(self, result: Dict[str, Any]) -> Any:
        if P2CardActionTriggerResponse is None:
            return None
        response = P2CardActionTriggerResponse()
        if CallBackCard is not None:
            card = CallBackCard()
            card.type = "raw"
            card.data = self._build_task_confirm_callback_card(
                changed=bool(result.get("changed")),
                duplicate=bool(result.get("duplicate")),
                choice=str(result.get("choice") or ""),
            )
            response.card = card
        return response

    def _handle_task_confirm_card_action(self, *, event: Any, action_value: Dict[str, Any]) -> Any:
        try:
            from gateway.feishu_task_confirm import resolve_task_confirm

            operator = getattr(event, "operator", None)
            actor_id = str(getattr(operator, "open_id", "") or getattr(operator, "user_id", "") or "")
            actor_name = self._resolve_approval_operator_display_name(operator)
            token = str(getattr(event, "token", "") or "")
            result = resolve_task_confirm(
                task_id=str(action_value.get("task_id") or ""),
                confirm_id=str(action_value.get("confirm_id") or ""),
                choice=str(action_value.get("choice") or ""),
                actor_id=actor_id,
                actor_name=actor_name,
                source="button",
                event_id=token,
            )
        except Exception as exc:
            logger.warning("[Feishu] task_confirm card action failed: %s", exc, exc_info=True)
            result = {"ok": False, "changed": False, "error": str(exc)}
        return self._task_confirm_response(result)

    def _handle_intake_clarify_card_action(self, *, event: Any, action_value: Dict[str, Any]) -> Any:
        """Resolve an intake clarification dimension from a button click.

        Gated by INTAKE_CLARIFY_ENABLED: when off we still ACK the click (so the
        user isn't left hanging) but do not touch any sidecar. Mirrors the
        task_confirm handler's atomic/idempotent resolution.
        """
        if os.environ.get("INTAKE_CLARIFY_ENABLED", "").strip().lower() not in {"1", "true", "yes", "on"}:
            return self._task_confirm_response({"ok": False, "changed": False, "error": "intake_clarify disabled"})
        try:
            from gateway.intake_clarification import resolve_intake_clarify

            operator = getattr(event, "operator", None)
            actor_id = str(getattr(operator, "open_id", "") or getattr(operator, "user_id", "") or "")
            actor_name = self._resolve_approval_operator_display_name(operator)
            token = str(getattr(event, "token", "") or "")
            result = resolve_intake_clarify(
                request_id=str(action_value.get("request_id") or ""),
                dimension_id=str(action_value.get("dimension_id") or ""),
                choice=str(action_value.get("choice") or ""),
                actor_id=actor_id,
                actor_name=actor_name,
                source="button",
                event_id=token,
            )
        except Exception as exc:
            logger.warning("[Feishu] intake_clarify card action failed: %s", exc, exc_info=True)
            result = {"ok": False, "changed": False, "error": str(exc)}
        # When the last dimension is resolved, schedule the continuation (confirm
        # the clarified intent back to the user). Idempotent via mark_continued.
        if result.get("ok") and result.get("all_resolved"):
            loop = self._loop
            if self._loop_accepts_callbacks(loop):
                self._submit_on_loop(loop, self._continue_after_intake_clarify(str(action_value.get("request_id") or "")))
        return self._task_confirm_response(result)

    _RCA_CLARIFY_GUIDANCE = {
        "rca_case_status_check": "**查 case 状态** — 发 G1Q3 case 编号（如 `G1Q3-1234`），或直接问「这个 case 跑到哪一步了」。",
        "rca_issue_intake": "**飞书问题** — 普通问题卡或链接只查状态；手工运行仅在固定 RCA 群内真实 @ 机器人，并发送明确动作和完整问题链接。",
        "rca_case_evidence_summary": "**证据 / 缺项查询** — 发 G1Q3 case 编号，并说明要看的证据或缺什么（如「G1Q3-1234 缺什么」）。",
    }

    def _handle_rca_clarify_card_action(self, *, event: Any, action_value: Dict[str, Any]) -> Any:
        """#17: user picked an RCA intent on the pnc_group_binding clarify card.

        Stateless: update the card in place with concrete "what to provide next"
        guidance. No task is created here — the user supplies a G1Q3 case on their
        next message, which then routes normally through pnc_group_binding.
        """
        choice = str(action_value.get("choice") or "")
        guidance = self._RCA_CLARIFY_GUIDANCE.get(choice, "请补充 G1Q3 case 编号后再发一次。")
        return self._rca_clarify_response(guidance)

    def _rca_clarify_response(self, guidance: str) -> Any:
        if P2CardActionTriggerResponse is None:
            return None
        response = P2CardActionTriggerResponse()
        if CallBackCard is not None:
            card = CallBackCard()
            card.type = "raw"
            card.data = {
                "config": {"wide_screen_mode": True},
                "header": {"title": {"content": "G1Q3 RCA · 已选择", "tag": "plain_text"}, "template": "green"},
                "elements": [{"tag": "markdown", "content": guidance}],
            }
            response.card = card
        return response

    async def send_clarify(
        self,
        chat_id: str,
        question: str,
        choices: Optional[list],
        clarify_id: str,
        session_key: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> "SendResult":
        """Render an agent clarify prompt as a native Feishu interactive card.

        Mirrors the Telegram override: one button per choice + an "其他(直接打字)"
        button that flips into text-capture. Button clicks resolve via the shared
        tools.clarify_gateway backend (same as Telegram/Discord), so timeout,
        text-intercept and session handling are all reused — no parallel system.

        Gated by FEISHU_CLARIFY_BUTTONS_ENABLED: when off, defer to the base
        text-numbered-list fallback (current behaviour) for instant rollback.
        """
        if os.environ.get("FEISHU_CLARIFY_BUTTONS_ENABLED", "").strip().lower() not in {"1", "true", "yes", "on"}:
            return await super().send_clarify(chat_id, question, choices, clarify_id, session_key, metadata)
        if not choices:
            # open-ended: register() already set awaiting_text; just send the question
            return await self.send(chat_id=chat_id, content=f"❓ {question}", metadata=metadata)
        try:
            elements: list[dict[str, Any]] = [{"tag": "markdown", "content": f"❓ {question}"}]
            opt_lines = "\n".join(f"{i+1}. {c}" for i, c in enumerate(choices))
            elements.append({"tag": "markdown", "content": opt_lines})
            buttons = []
            for c in choices:
                label = str(c).strip()
                if not label:
                    continue
                buttons.append({
                    "tag": "button",
                    "text": {"tag": "plain_text", "content": label[:60]},
                    "type": "default",
                    "value": {"hermes_action": "clarify", "clarify_id": clarify_id, "choice": label},
                })
            buttons.append({
                "tag": "button",
                "text": {"tag": "plain_text", "content": "✏️ 其他(直接打字)"},
                "type": "default",
                "value": {"hermes_action": "clarify", "clarify_id": clarify_id, "other": True},
            })
            for i in range(0, len(buttons), 4):
                elements.append({"tag": "action", "actions": buttons[i:i+4]})
            card = {
                "config": {"wide_screen_mode": True},
                "header": {"title": {"tag": "plain_text", "content": "需要你确认一下"}, "template": "turquoise"},
                "elements": elements,
            }
            payload = json.dumps(card, ensure_ascii=False)
            response = await self._feishu_send_with_retry(
                chat_id=chat_id, msg_type="interactive", payload=payload,
                reply_to=None, metadata=metadata,
            )
            result = self._finalize_send_result(response, "clarify card send failed")
            if result.success:
                state = getattr(self, "_clarify_state", None)
                if state is None:
                    state = {}; self._clarify_state = state
                state[clarify_id] = session_key
            return result
        except Exception as e:
            logger.warning("[Feishu] send_clarify failed: %s; falling back to text", e, exc_info=True)
            return await super().send_clarify(chat_id, question, choices, clarify_id, session_key, metadata)

    def _handle_clarify_card_action(self, *, event: Any, action_value: Dict[str, Any]) -> Any:
        """Resolve an agent clarify (send_clarify) button click via clarify_gateway."""
        clarify_id = str(action_value.get("clarify_id") or "").strip()
        if not clarify_id:
            return self._task_confirm_response({"ok": False, "changed": False, "error": "missing clarify_id"})
        try:
            if action_value.get("other"):
                from tools.clarify_gateway import mark_awaiting_text
                mark_awaiting_text(clarify_id)
                return self._task_confirm_response({"ok": True, "changed": True, "choice": "✏️ 直接打字回复即可"})
            from tools.clarify_gateway import resolve_gateway_clarify
            choice = str(action_value.get("choice") or "").strip()
            ok = resolve_gateway_clarify(clarify_id, choice)
            state = getattr(self, "_clarify_state", None)
            if ok and isinstance(state, dict):
                state.pop(clarify_id, None)
            return self._task_confirm_response({"ok": bool(ok), "changed": bool(ok), "duplicate": not ok, "choice": choice})
        except Exception as exc:
            logger.warning("[Feishu] clarify card action failed: %s", exc, exc_info=True)
            return self._task_confirm_response({"ok": False, "changed": False, "error": str(exc)})

    async def _continue_after_intake_clarify(self, request_id: str) -> None:
        """Create the deferred task once all dimensions are answered, then confirm.

        The intake path deferred task creation (it sent a clarification card instead),
        so here we call the same idempotent handoff with the ORIGINAL message_id —
        idempotent-by-message_id means no double-create — enriching the request text
        with the clarified choices. Idempotent overall via mark_continued.
        """
        try:
            from gateway.intake_clarification import summarize_clarified_choices, mark_continued

            summary = summarize_clarified_choices(request_id)
            if not summary:
                return
            if not mark_continued(request_id):
                return  # another all-resolved click already continued
            choices = summary.get("choices") or {}
            parts = "、".join(f"{k}={v}" for k, v in choices.items() if v)
            chat_id = str(summary.get("chat_id") or "")
            thread_id = str(summary.get("thread_id") or "")

            # knowledge-question intent: do not create an execution task
            if summary.get("is_qa"):
                text = f"收到，按你的选择当作知识问答处理（{parts}），不创建执行任务。"
            else:
                try:
                    from gateway.run import _submit_integration_tools_intake_handoff
                    from hermes_cli.config import get_hermes_home

                    receipt_dir = get_hermes_home() / "pnc_agent" / "receipts" / "integration_tools"
                    enriched = f"{summary.get('raw_text') or ''}\n[澄清] {parts}"
                    handoff = _submit_integration_tools_intake_handoff(
                        requester=str(summary.get("originator_open_id") or ""),
                        source_group_id=chat_id,
                        message_id=request_id,
                        source_thread_id=thread_id,
                        request_text=enriched,
                        receipt_dir=receipt_dir,
                    )
                    ok = bool(isinstance(handoff, dict) and handoff.get("success"))
                    text = (f"收到，按你的选择接手（{parts}），已建任务推进。"
                            if ok else
                            f"收到你的选择（{parts}），但建任务这步没成功，我已保留这次澄清，可重试。")
                except Exception:
                    logger.warning("[Feishu] intake_clarify handoff failed", exc_info=True)
                    text = f"收到你的选择（{parts}），建任务时出了点问题，我已保留澄清结果。"

            if chat_id:
                await self.send(chat_id, text, reply_to=thread_id or None)
        except Exception:
            logger.warning("[Feishu] intake_clarify continuation failed", exc_info=True)

    async def _maybe_resolve_task_confirm_text(self, event: MessageEvent) -> bool:
        try:
            from gateway.feishu_task_confirm import resolve_task_confirm_by_text

            source = event.source
            if not source or source.platform.value != "feishu":
                return False
            text = str(event.text or "").strip()
            if not text:
                return False
            result = resolve_task_confirm_by_text(
                chat_id=str(source.chat_id or ""),
                thread_id=str(source.thread_id or ""),
                text=text,
                actor_id=str(source.user_id or source.user_id_alt or ""),
                actor_name=str(source.user_name or ""),
                event_id=str(event.message_id or ""),
            )
            if result.get("ok"):
                logger.info(
                    "[Feishu] task_confirm text resolved task=%s confirm=%s changed=%s duplicate=%s",
                    result.get("task_id"),
                    result.get("confirm_id"),
                    result.get("changed"),
                    result.get("duplicate"),
                )
                return True
        except Exception:
            logger.warning("[Feishu] task_confirm text fallback failed", exc_info=True)
        return False

    def _handle_approval_card_action(self, *, event: Any, action_value: Dict[str, Any], loop: Any) -> Any:
        """Schedule approval resolution and build the synchronous callback response."""
        approval_id = action_value.get("approval_id")
        if approval_id is None:
            logger.debug("[Feishu] Card action missing approval_id, ignoring")
            return P2CardActionTriggerResponse() if P2CardActionTriggerResponse else None
        state = self._approval_state.get(approval_id)
        if not state:
            logger.debug("[Feishu] Approval %s already resolved or unknown", approval_id)
            return P2CardActionTriggerResponse() if P2CardActionTriggerResponse else None
        choice = _APPROVAL_CHOICE_MAP.get(action_value.get("hermes_action"), "deny")
        requested_role_override = str(action_value.get("requested_role") or "").strip().lower()

        operator = getattr(event, "operator", None)
        open_id = str(getattr(operator, "open_id", "") or "").strip()
        operator_user_id = str(getattr(operator, "user_id", "") or "").strip()
        sender_id = SimpleNamespace(open_id=open_id, user_id=operator_user_id)
        if not self._allow_group_message(sender_id, state.get("chat_id", ""), is_bot=False):
            logger.warning("[Feishu] Unauthorized approval click by %s", open_id or "<unknown>")
            return P2CardActionTriggerResponse() if P2CardActionTriggerResponse else None

        callback_chat_id = str(getattr(getattr(event, "context", None), "open_chat_id", "") or "")
        expected_chat_id = str(state.get("chat_id", "") or "")
        if callback_chat_id and expected_chat_id and callback_chat_id != expected_chat_id:
            logger.warning(
                "[Feishu] Approval callback chat mismatch for %s (expected=%s, got=%s)",
                approval_id,
                expected_chat_id,
                callback_chat_id,
            )
            return P2CardActionTriggerResponse() if P2CardActionTriggerResponse else None

        approver_lookup_ids = [value for value in (open_id, operator_user_id) if value]
        user_name = self._resolve_approval_operator_display_name(operator)

        try:
            from tools.permission_policy import get_user_role_by_id

            role = "member"
            for lookup_id in approver_lookup_ids:
                role = get_user_role_by_id(lookup_id)
                if role != "member":
                    break
        except Exception:
            logger.debug("[Feishu] Failed to resolve approval-click user role", exc_info=True)
            role = "member"

        if choice != "deny" and role == "member":
            logger.info(
                "[Feishu] Blocking approval click from unauthorized member %s for approval %s",
                open_id,
                approval_id,
            )
            if P2CardActionTriggerResponse is None:
                return None
            response = P2CardActionTriggerResponse()
            if CallBackCard is not None:
                card = CallBackCard()
                card.type = "raw"
                card.data = self._build_permission_denied_approval_card(user_name=user_name)
                response.card = card
            return response

        if choice == "select_requested_role":
            requested_role = requested_role_override
            if state and requested_role in _PERMISSION_GRANT_ALLOWED_ROLES:
                state["requested_role"] = requested_role
            if P2CardActionTriggerResponse is None:
                return None
            response = P2CardActionTriggerResponse()
            if CallBackCard is not None:
                card = CallBackCard()
                card.type = "raw"
                card.data = self._build_permission_role_updated_card(
                    user_name=user_name,
                    requested_role=state.get("requested_role", requested_role) if state else requested_role,
                )
                response.card = card
            return response

        if choice == "grant_permission" and requested_role_override in _PERMISSION_GRANT_ALLOWED_ROLES and state:
            state["requested_role"] = requested_role_override
        if not self._submit_on_loop(
            loop,
            self._resolve_approval(
                approval_id,
                choice,
                user_name,
                open_id=open_id,
                chat_id=callback_chat_id,
            ),
        ):
            return P2CardActionTriggerResponse() if P2CardActionTriggerResponse else None

        if P2CardActionTriggerResponse is None:
            return None
        response = P2CardActionTriggerResponse()
        if CallBackCard is not None:
            card = CallBackCard()
            card.type = "raw"
            state = state or {}
            card.data = self._build_resolved_approval_card(
                choice=choice,
                user_name=user_name,
                approval_kind=str(state.get("approval_kind") or "exec"),
                target_user_name=str(state.get("target_user_name") or ""),
                requested_role=str(state.get("requested_role") or ""),
            )
            response.card = card
        return response

    @staticmethod
    def _is_usable_permission_display_name(value: Any) -> bool:
        text = str(value or "").strip()
        if not text:
            return False
        if text in {"(unknown)", "该用户", "审批人", "unknown"}:
            return False
        if re.match(r"^(?:ou|on|u|oc|om)[_-]", text):
            return False
        return True

    @staticmethod
    def _permission_name_from_local_mapping(user_id: str) -> Optional[str]:
        lookup_id = str(user_id or "").strip()
        if not lookup_id:
            return None
        try:
            from tools.permission_policy import _load_config

            mapped_name = (_load_config().get("user_id_mapping") or {}).get(lookup_id)
        except Exception:
            logger.debug("[Feishu] Failed to resolve permission target from local mapping", exc_info=True)
            return None
        mapped_name = str(mapped_name or "").strip()
        return mapped_name or None

    async def _normalize_permission_request_identity(
        self,
        *,
        user_id: str,
        user_name: str,
    ) -> tuple[str, str]:
        target_user_id = str(user_id or "").strip()
        target_user_name = str(user_name or "").strip()
        if not target_user_id:
            return "", ""
        if self._is_usable_permission_display_name(target_user_name):
            return target_user_id, target_user_name

        mapped_name = self._permission_name_from_local_mapping(target_user_id)
        if self._is_usable_permission_display_name(mapped_name):
            return target_user_id, str(mapped_name).strip()

        api_name = await self._resolve_sender_name_from_api(target_user_id)
        if self._is_usable_permission_display_name(api_name):
            return target_user_id, str(api_name).strip()

        logger.warning(
            "[Feishu] Permission request target name unavailable for %s; falling back to user id",
            target_user_id,
        )
        return target_user_id, target_user_id

    def _recover_permission_target_identity(self, state: Dict[str, Any]) -> tuple[str, str]:
        target_user_id = str(state.get("target_user_id") or "").strip()
        target_user_name = str(state.get("target_user_name") or "").strip()

        if not target_user_id:
            session_key_parts = str(state.get("session_key") or "").split(":")
            if len(session_key_parts) >= 3 and session_key_parts[0] == "permission_grant":
                target_user_id = session_key_parts[1].strip()
                if target_user_id:
                    state["target_user_id"] = target_user_id

        if not self._is_usable_permission_display_name(target_user_name):
            mapped_name = self._permission_name_from_local_mapping(target_user_id)
            if self._is_usable_permission_display_name(mapped_name):
                target_user_name = str(mapped_name).strip()
                state["target_user_name"] = target_user_name

        if not self._is_usable_permission_display_name(target_user_name):
            request_hint = str(state.get("request_text") or state.get("description") or "")
            name_match = re.search(r"(?:我是|我叫|叫我)\s*([^，,。\s]{1,20})", request_hint)
            if name_match:
                recovered_name = name_match.group(1).strip()
                if self._is_usable_permission_display_name(recovered_name):
                    target_user_name = recovered_name
                    state["target_user_name"] = target_user_name

        if not self._is_usable_permission_display_name(target_user_name) and target_user_id:
            logger.warning(
                "[Feishu] Permission grant target name unavailable for %s; falling back to user id",
                target_user_id,
            )
            target_user_name = target_user_id
            state["target_user_name"] = target_user_name

        return target_user_id, target_user_name

    def _emit_permission_audit_event(
        self,
        *,
        outcome: str,
        state: Dict[str, Any],
        approver_name: str,
        applicant_notify_success: Optional[bool] = None,
        group_broadcast_success: Optional[bool] = None,
        reuse_hit: Optional[bool] = None,
        dedup_hit: Optional[bool] = None,
    ) -> None:
        try:
            from hermes_cli.config import get_hermes_home
            from hermes_events import EventEmitter

            trace_file = get_hermes_home() / "analytics" / "permission_approvals.jsonl"
            EventEmitter(trace_file=trace_file).emit(
                "permission_grant:resolved",
                {
                    "outcome": outcome,
                    "approval_kind": str(state.get("approval_kind") or "permission_grant"),
                    "target_user_id": str(state.get("target_user_id") or ""),
                    "target_user_name": str(state.get("target_user_name") or ""),
                    "requested_role": str(state.get("requested_role") or ""),
                    "request_chat_id": str(state.get("request_chat_id") or ""),
                    "request_chat_name": str(state.get("request_chat_name") or ""),
                    "request_message_id": str(state.get("request_message_id") or ""),
                    "request_text": str(state.get("request_text") or ""),
                    "reused_approval_id": state.get("reused_approval_id"),
                    "reused_approval_message_id": str(state.get("reused_approval_message_id") or ""),
                    "approver_name": approver_name,
                    "applicant_notify_success": applicant_notify_success,
                    "group_broadcast_success": group_broadcast_success,
                    "reuse_hit": reuse_hit,
                    "dedup_hit": dedup_hit,
                },
            )
        except Exception:
            logger.debug("[Feishu] Failed to emit permission approval audit event", exc_info=True)

    async def _broadcast_permission_request_result(self, state: Dict[str, Any], *, approved: bool) -> bool:
        if self.config.extra.get("permission_result_broadcast_enabled") is False:
            return False
        request_chat_id = str(state.get("request_chat_id") or "").strip()
        request_chat_name = str(state.get("request_chat_name") or "").strip()
        if not request_chat_id:
            return False
        request_message_id = str(state.get("request_message_id") or "").strip() or None
        request_thread_id = str(state.get("request_thread_id") or "").strip() or None
        _target_user_id, recovered_target_name = self._recover_permission_target_identity(state)
        target_user_name = recovered_target_name if self._is_usable_permission_display_name(recovered_target_name) else "该用户"
        requested_role = str(state.get("requested_role") or "member").strip().lower() or "member"
        text = (
            f"{request_chat_name or '当前群组'}：{target_user_name} 的权限申请已通过，角色：{requested_role}。"
            if approved
            else f"{request_chat_name or '当前群组'}：{target_user_name} 的权限申请未通过。"
        )
        try:
            result = await self.send(
                chat_id=request_chat_id,
                content=text,
                reply_to=request_message_id,
                metadata={"thread_id": request_thread_id} if request_thread_id else None,
            )
            if not getattr(result, "success", False):
                logger.warning(
                    "[Feishu] Failed to broadcast permission request result for %s: %s",
                    target_user_name,
                    getattr(result, "error", None),
                )
                return False
            return True
        except Exception as exc:
            logger.warning(
                "[Feishu] Exception while broadcasting permission request result for %s: %s",
                target_user_name,
                exc,
            )
            return False

    async def _notify_permission_request_result(self, state: Dict[str, Any], *, approved: bool) -> bool:
        request_chat_id = str(state.get("request_chat_id") or "").strip()
        if not request_chat_id:
            return False
        request_message_id = str(state.get("request_message_id") or "").strip() or None
        request_thread_id = str(state.get("request_thread_id") or "").strip() or None
        _target_user_id, recovered_target_name = self._recover_permission_target_identity(state)
        target_user_name = recovered_target_name if self._is_usable_permission_display_name(recovered_target_name) else "该用户"
        requested_role = str(state.get("requested_role") or "member").strip().lower() or "member"
        text = (
            f"你的权限申请已通过，已开通为 {requested_role}。"
            if approved
            else f"你的权限申请未通过，请联系管理员了解详情。"
        )
        try:
            result = await self.send(
                chat_id=request_chat_id,
                content=text,
                reply_to=request_message_id,
                metadata={"thread_id": request_thread_id} if request_thread_id else None,
            )
            if not getattr(result, "success", False):
                logger.warning(
                    "[Feishu] Failed to notify permission request result for %s: %s",
                    target_user_name,
                    getattr(result, "error", None),
                )
                return False
            return True
        except Exception as exc:
            logger.warning(
                "[Feishu] Exception while notifying permission request result for %s: %s",
                target_user_name,
                exc,
            )
            return False

    def _handle_update_prompt_card_action(self, *, event: Any, action_value: Dict[str, Any], loop: Any) -> Any:
        """Schedule update prompt resolution and build the synchronous callback response."""
        prompt_id = action_value.get("update_prompt_id")
        if prompt_id is None:
            logger.debug("[Feishu] Card action missing update_prompt_id, ignoring")
            return P2CardActionTriggerResponse() if P2CardActionTriggerResponse else None
        state = self._update_prompt_state.get(prompt_id)
        if not state:
            logger.debug("[Feishu] Update prompt %s already resolved or unknown", prompt_id)
            return P2CardActionTriggerResponse() if P2CardActionTriggerResponse else None

        answer = str(action_value.get("hermes_update_prompt_action", "") or "").strip().lower()
        if answer not in {"y", "n"}:
            logger.debug("[Feishu] Card action has invalid update prompt answer=%r", answer)
            return P2CardActionTriggerResponse() if P2CardActionTriggerResponse else None

        operator = getattr(event, "operator", None)
        open_id = str(getattr(operator, "open_id", "") or "")
        sender_id = SimpleNamespace(open_id=open_id, user_id=str(getattr(operator, "user_id", "") or ""))
        if not self._allow_group_message(sender_id, state.get("chat_id", ""), is_bot=False):
            logger.warning("[Feishu] Unauthorized update prompt click by %s", open_id or "<unknown>")
            return P2CardActionTriggerResponse() if P2CardActionTriggerResponse else None

        callback_chat_id = str(getattr(getattr(event, "context", None), "open_chat_id", "") or "")
        expected_chat_id = str(state.get("chat_id", "") or "")
        if callback_chat_id and expected_chat_id and callback_chat_id != expected_chat_id:
            logger.warning(
                "[Feishu] Update prompt callback chat mismatch for %s (expected=%s, got=%s)",
                prompt_id,
                expected_chat_id,
                callback_chat_id,
            )
            return P2CardActionTriggerResponse() if P2CardActionTriggerResponse else None

        user_name = self._get_cached_sender_name(open_id) or open_id
        if not self._submit_on_loop(
            loop,
            self._resolve_update_prompt(
                prompt_id,
                answer,
                user_name,
                open_id=open_id,
                chat_id=callback_chat_id,
            ),
        ):
            return P2CardActionTriggerResponse() if P2CardActionTriggerResponse else None

        if P2CardActionTriggerResponse is None:
            return None
        response = P2CardActionTriggerResponse()
        if CallBackCard is not None:
            card = CallBackCard()
            card.type = "raw"
            card.data = self._build_resolved_update_prompt_card(answer=answer, user_name=user_name)
            response.card = card
        return response

    async def _resolve_approval(
        self,
        approval_id: Any,
        choice: str,
        user_name: str,
        *,
        open_id: str = "",
        chat_id: str = "",
    ) -> None:
        """Pop approval state and unblock the waiting agent thread."""
        state = self._approval_state.get(approval_id)
        if not state:
            logger.debug("[Feishu] Approval %s already resolved or unknown", approval_id)
            return
        if not self._is_interactive_operator_authorized(open_id):
            logger.warning("[Feishu] Unauthorized approval click by %s for approval %s", open_id or "<unknown>", approval_id)
            return
        expected_chat_id = str(state.get("chat_id", "") or "")
        if expected_chat_id and chat_id and expected_chat_id != chat_id:
            logger.warning(
                "[Feishu] Approval %s chat mismatch (expected=%s, got=%s)",
                approval_id, expected_chat_id, chat_id,
            )
            return
        state = self._approval_state.pop(approval_id, None)
        if not state:
            logger.debug("[Feishu] Approval %s already resolved while validating callback", approval_id)
            return

        # Clean up timeout callback since approval was resolved normally
        session_key = state.get("session_key", "")
        if session_key:
            self._approval_callbacks.pop(session_key, None)

        approval_kind = str(state.get("approval_kind") or "exec").strip().lower()
        if choice == "deny":
            if approval_kind == "permission_grant":
                applicant_notify_success = await self._notify_permission_request_result(state, approved=False)
                group_broadcast_success = await self._broadcast_permission_request_result(state, approved=False)
                self._emit_permission_audit_event(
                    outcome="denied",
                    state=state,
                    approver_name=user_name,
                    applicant_notify_success=applicant_notify_success,
                    group_broadcast_success=group_broadcast_success,
                    reuse_hit=bool(state.get("reuse_hit")),
                    dedup_hit=bool(state.get("dedup_hit")),
                )
                return
            try:
                from tools.approval import resolve_gateway_approval
                count = resolve_gateway_approval(state["session_key"], choice)
                logger.info(
                    "Feishu button denied %d approval(s) for session %s (user=%s)",
                    count, state["session_key"], user_name,
                )
            except Exception as exc:
                logger.error("Failed to deny gateway approval from Feishu button: %s", exc)
            return

        if choice in {"grant_senior", "grant_permission"} and approval_kind == "permission_grant":
            try:
                from gateway.pairing import PairingStore
                from tools.permission_policy import map_user_id, set_user_role

                target_user_id, target_user_name = self._recover_permission_target_identity(state)
                requested_role = str(state.get("requested_role") or "member").strip().lower() or "member"

                if not target_user_id or not target_user_name:
                    raise ValueError("missing target user identity for permission grant")

                role_value = requested_role if requested_role in _PERMISSION_GRANT_ALLOWED_ROLES else "member"
                set_user_role(target_user_name, role_value)
                map_user_id(target_user_name, target_user_id)
                PairingStore().approve_user("feishu", target_user_id, target_user_name)
                logger.info(
                    "[Feishu] Granted %s role to %s (%s) via approval card by %s",
                    role_value,
                    target_user_name,
                    target_user_id,
                    user_name,
                )
                applicant_notify_success = await self._notify_permission_request_result(state, approved=True)
                group_broadcast_success = await self._broadcast_permission_request_result(state, approved=True)
                self._emit_permission_audit_event(
                    outcome="approved",
                    state=state,
                    approver_name=user_name,
                    applicant_notify_success=applicant_notify_success,
                    group_broadcast_success=group_broadcast_success,
                    reuse_hit=bool(state.get("reuse_hit")),
                    dedup_hit=bool(state.get("dedup_hit")),
                )
            except Exception as exc:
                logger.error("[Feishu] Failed to apply permission grant approval: %s", exc, exc_info=True)
            return

        try:
            from tools.approval import resolve_gateway_approval
            count = resolve_gateway_approval(state["session_key"], choice)
            logger.info(
                "Feishu button resolved %d approval(s) for session %s (choice=%s, user=%s)",
                count, state["session_key"], choice, user_name,
            )
        except Exception as exc:
            logger.error("Failed to resolve gateway approval from Feishu button: %s", exc)

    async def _resolve_update_prompt(
        self,
        prompt_id: Any,
        answer: str,
        user_name: str,
        *,
        open_id: str = "",
        chat_id: str = "",
    ) -> None:
        """Persist an update prompt answer for the detached update process."""
        state = self._update_prompt_state.get(prompt_id)
        if not state:
            logger.debug("[Feishu] Update prompt %s already resolved or unknown", prompt_id)
            return
        if open_id:
            sender_id = SimpleNamespace(open_id=open_id, user_id="")
            if not self._allow_group_message(sender_id, state.get("chat_id", ""), is_bot=False):
                logger.warning("[Feishu] Unauthorized update prompt click by %s for prompt %s", open_id, prompt_id)
                return
        expected_chat_id = str(state.get("chat_id", "") or "")
        if expected_chat_id and chat_id and expected_chat_id != chat_id:
            logger.warning(
                "[Feishu] Update prompt %s chat mismatch (expected=%s, got=%s)",
                prompt_id,
                expected_chat_id,
                chat_id,
            )
            return
        state = self._update_prompt_state.pop(prompt_id, None)
        if not state:
            logger.debug("[Feishu] Update prompt %s already resolved while validating callback", prompt_id)
            return
        try:
            self._write_update_prompt_response(answer)
            logger.info(
                "Feishu update prompt resolved for session %s (answer=%s, user=%s)",
                state["session_key"], answer, user_name,
            )
        except Exception as exc:
            logger.error("Failed to resolve Feishu update prompt: %s", exc)

    async def _handle_reaction_event(self, event_type: str, data: Any) -> None:
        """Fetch the reacted-to message; if it was sent by this bot, emit a synthetic text event."""
        if not self._client:
            return
        event = getattr(data, "event", None)
        message_id = str(getattr(event, "message_id", "") or "")
        if not message_id:
            return

        # Fetch the target message to verify it was sent by us and to obtain chat context.
        try:
            request = self._build_get_message_request(message_id)
            response = await self._run_blocking(self._client.im.v1.message.get, request)
            if not response or not getattr(response, "success", lambda: False)():
                return
            items = getattr(getattr(response, "data", None), "items", None) or []
            msg = items[0] if items else None
            if not msg:
                return
            # GET im/v1/messages returns sender.id=app_id for bot messages —
            # peer bots and us share sender_type="app" but differ on app_id.
            sender = getattr(msg, "sender", None)
            if str(getattr(sender, "id", "") or "") != self._app_id:
                return  # only route reactions on this bot's own messages
            chat_id = str(getattr(msg, "chat_id", "") or "")
            chat_type_raw = str(getattr(msg, "chat_type", "p2p") or "p2p")
            if not chat_id:
                return
        except Exception:
            logger.debug("[Feishu] Failed to fetch message for reaction routing", exc_info=True)
            return

        user_id_obj = getattr(event, "user_id", None)
        reaction_type_obj = getattr(event, "reaction_type", None)
        emoji_type = str(getattr(reaction_type_obj, "emoji_type", "") or "UNKNOWN")
        action = "added" if "created" in event_type else "removed"
        synthetic_text = f"reaction:{action}:{emoji_type}"

        sender_profile = await self._resolve_sender_profile(user_id_obj)
        chat_info = await self.get_chat_info(chat_id)
        source = self.build_source(
            chat_id=chat_id,
            chat_name=chat_info.get("name") or chat_id or "Feishu Chat",
            chat_type=self._resolve_source_chat_type(chat_info=chat_info, event_chat_type=chat_type_raw),
            user_id=sender_profile["user_id"],
            user_name=sender_profile["user_name"],
            thread_id=None,
            user_id_alt=sender_profile["user_id_alt"],
        )
        synthetic_event = MessageEvent(
            text=synthetic_text,
            message_type=MessageType.TEXT,
            source=source,
            raw_message=data,
            message_id=message_id,
            channel_prompt=self._resolve_channel_prompt(chat_id),
            timestamp=datetime.now(),
        )
        logger.info("[Feishu] Routing reaction %s:%s on bot message %s as synthetic event", action, emoji_type, message_id)
        await self._handle_message_with_guards(synthetic_event)

    def _is_card_action_duplicate(self, token: str) -> bool:
        """Return True if this card action token was already processed within the dedup window."""
        now = time.time()
        # Prune expired tokens lazily each call.
        expired = [t for t, ts in self._card_action_tokens.items() if now - ts > _FEISHU_CARD_ACTION_DEDUP_TTL_SECONDS]
        for t in expired:
            del self._card_action_tokens[t]
        if token in self._card_action_tokens:
            return True
        self._card_action_tokens[token] = now
        return False

    async def _handle_card_action_event(self, data: Any) -> None:
        """Route Feishu interactive card button clicks as synthetic COMMAND events."""
        event = getattr(data, "event", None)
        token = str(getattr(event, "token", "") or "")
        if token and self._is_card_action_duplicate(token):
            logger.debug("[Feishu] Dropping duplicate card action token: %s", token)
            return

        context = getattr(event, "context", None)
        chat_id = str(getattr(context, "open_chat_id", "") or "")
        operator = getattr(event, "operator", None)
        open_id = str(getattr(operator, "open_id", "") or "")
        if not chat_id or not open_id:
            logger.debug("[Feishu] Card action missing chat_id or operator open_id, dropping")
            return

        action = getattr(event, "action", None)
        action_tag = str(getattr(action, "tag", "") or "button")
        action_value = getattr(action, "value", {}) or {}

        synthetic_text = f"/card {action_tag}"
        if action_value:
            try:
                synthetic_text += f" {json.dumps(action_value, ensure_ascii=False)}"
            except Exception:
                pass

        sender_id = SimpleNamespace(open_id=open_id, user_id=None, union_id=None)
        sender_profile = await self._resolve_sender_profile(sender_id)
        chat_info = await self.get_chat_info(chat_id)
        source = self.build_source(
            chat_id=chat_id,
            chat_name=chat_info.get("name") or chat_id or "Feishu Chat",
            chat_type=self._resolve_source_chat_type(chat_info=chat_info, event_chat_type="group"),
            user_id=sender_profile["user_id"],
            user_name=sender_profile["user_name"],
            thread_id=None,
            user_id_alt=sender_profile["user_id_alt"],
        )
        synthetic_event = MessageEvent(
            text=synthetic_text,
            message_type=MessageType.COMMAND,
            source=source,
            raw_message=data,
            message_id=token or str(uuid.uuid4()),
            channel_prompt=self._resolve_channel_prompt(chat_id),
            timestamp=datetime.now(),
        )
        logger.info("[Feishu] Routing card action %r from %s in %s as synthetic command", action_tag, open_id, chat_id)
        await self._handle_message_with_guards(synthetic_event)

    # =========================================================================
    # Per-chat serialization and typing indicator
    # =========================================================================

    def _get_chat_lock(self, chat_id: str) -> asyncio.Lock:
        """Return (creating if needed) the per-chat asyncio.Lock for serial message processing.

        Bounded with LRU eviction so a long-running gateway that sees many
        distinct chats does not grow ``_chat_locks`` without limit. Locks that
        are currently held are never evicted; if every entry is locked we fall
        back to dropping the least-recently-used one.
        """
        lock = self._chat_locks.get(chat_id)
        if lock is not None:
            self._chat_locks.move_to_end(chat_id)
            return lock
        if len(self._chat_locks) >= self.CHAT_LOCK_MAX_SIZE:
            evicted = False
            for key in list(self._chat_locks):
                if not self._chat_locks[key].locked():
                    self._chat_locks.pop(key)
                    evicted = True
                    break
            if not evicted:
                self._chat_locks.pop(next(iter(self._chat_locks)))
        lock = asyncio.Lock()
        self._chat_locks[chat_id] = lock
        return lock

    async def _handle_message_with_guards(self, event: MessageEvent) -> None:
        """Dispatch a single event through the agent pipeline with per-chat serialization
        before handing the event off to the agent.

        Per-chat lock ensures messages in the same chat are processed one at a
        time (matches openclaw's createChatQueue serial queue behaviour).
        """
        chat_id = getattr(event.source, "chat_id", "") or "" if event.source else ""
        chat_lock = self._get_chat_lock(chat_id)
        async with chat_lock:
            message_id = getattr(event, "message_id", None)
            if message_id and self._reactions_enabled():
                await self._add_ack_reaction(message_id)
            if await self._maybe_resolve_task_confirm_text(event):
                return
            await self.handle_message(event)

    # =========================================================================
    # Processing status reactions
    # =========================================================================

    def _reactions_enabled(self) -> bool:
        return os.getenv("FEISHU_REACTIONS", "true").strip().lower() not in {"false", "0", "no"}

    async def _add_reaction(self, message_id: str, emoji_type: str) -> Optional[str]:
        """Return the reaction_id on success, else None. The id is needed later for deletion."""
        if not message_id or not emoji_type:
            return None
        recorder = get_record_only_transport("gateway.feishu.adapter")
        if recorder is not None:
            result = recorder.record(
                operation="reaction_add",
                platform="feishu",
                destination_kind="message",
                destination_id=message_id,
                message_id=message_id,
                payload_type="reaction",
                payload={"emoji_type": emoji_type},
                update_mode="create",
            )
            return result.message_id if result.success else None
        if not self._client:
            return None
        try:
            from lark_oapi.api.im.v1 import (
                CreateMessageReactionRequest,
                CreateMessageReactionRequestBody,
            )
            body = (
                CreateMessageReactionRequestBody.builder()
                .reaction_type({"emoji_type": emoji_type})
                .build()
            )
            request = (
                CreateMessageReactionRequest.builder()
                .message_id(message_id)
                .request_body(body)
                .build()
            )
            response = await self._run_blocking(self._client.im.v1.message_reaction.create, request)
            if response and getattr(response, "success", lambda: False)():
                data = getattr(response, "data", None)
                return getattr(data, "reaction_id", None)
            logger.debug(
                "[Feishu] Add reaction %s on %s rejected: code=%s msg=%s",
                emoji_type,
                message_id,
                getattr(response, "code", None),
                getattr(response, "msg", None),
            )
        except Exception:
            logger.warning(
                "[Feishu] Add reaction %s on %s raised",
                emoji_type,
                message_id,
                exc_info=True,
            )
        return None

    async def _add_ack_reaction(self, message_id: str) -> Optional[str]:
        reaction_id = await self._add_reaction(message_id, _FEISHU_ACK_EMOJI)
        if reaction_id is None:
            logger.warning("[Feishu] Failed to add ack reaction to %s", message_id)
        return reaction_id

    async def _remove_reaction(self, message_id: str, reaction_id: str) -> bool:
        if not message_id or not reaction_id:
            return False
        recorder = get_record_only_transport("gateway.feishu.adapter")
        if recorder is not None:
            result = recorder.record(
                operation="reaction_remove",
                platform="feishu",
                destination_kind="message",
                destination_id=message_id,
                message_id=message_id,
                payload_type="reaction",
                payload={"reaction_id": reaction_id},
                update_mode="delete",
            )
            return result.success
        if not self._client:
            return False
        try:
            from lark_oapi.api.im.v1 import DeleteMessageReactionRequest
            request = (
                DeleteMessageReactionRequest.builder()
                .message_id(message_id)
                .reaction_id(reaction_id)
                .build()
            )
            response = await self._run_blocking(self._client.im.v1.message_reaction.delete, request)
            if response and getattr(response, "success", lambda: False)():
                return True
            logger.debug(
                "[Feishu] Remove reaction %s on %s rejected: code=%s msg=%s",
                reaction_id,
                message_id,
                getattr(response, "code", None),
                getattr(response, "msg", None),
            )
        except Exception:
            logger.warning(
                "[Feishu] Remove reaction %s on %s raised",
                reaction_id,
                message_id,
                exc_info=True,
            )
        return False

    def _remember_processing_reaction(self, message_id: str, reaction_id: str) -> None:
        cache = self._pending_processing_reactions
        cache[message_id] = reaction_id
        cache.move_to_end(message_id)
        while len(cache) > _FEISHU_PROCESSING_REACTION_CACHE_SIZE:
            cache.popitem(last=False)

    def _pop_processing_reaction(self, message_id: str) -> Optional[str]:
        return self._pending_processing_reactions.pop(message_id, None)

    async def on_processing_start(self, event: MessageEvent) -> None:
        if not self._reactions_enabled():
            return
        message_id = event.message_id
        if not message_id or message_id in self._pending_processing_reactions:
            return
        reaction_id = await self._add_reaction(message_id, _FEISHU_REACTION_IN_PROGRESS)
        if reaction_id:
            self._remember_processing_reaction(message_id, reaction_id)

    async def on_processing_complete(
        self, event: MessageEvent, outcome: ProcessingOutcome
    ) -> None:
        if not self._reactions_enabled():
            return
        message_id = event.message_id
        if not message_id:
            return

        start_reaction_id = self._pending_processing_reactions.get(message_id)
        if start_reaction_id:
            if not await self._remove_reaction(message_id, start_reaction_id):
                # Don't stack a second badge on top of a Typing we couldn't
                # remove — UI would read as both "working" and "done/failed"
                # simultaneously. Keep the handle so LRU eventually evicts it.
                return
            self._pop_processing_reaction(message_id)

        if outcome is ProcessingOutcome.FAILURE:
            await self._add_reaction(message_id, _FEISHU_REACTION_FAILURE)

    # =========================================================================
    # Webhook server and security
    # =========================================================================

    def _record_webhook_anomaly(self, remote_ip: str, status: str) -> None:
        """Increment the anomaly counter for remote_ip and emit a WARNING every threshold hits.

        Mirrors openclaw's createWebhookAnomalyTracker: TTL 6 hours, log every 25 consecutive
        error responses from the same IP.
        """
        now = time.time()
        entry = self._webhook_anomaly_counts.get(remote_ip)
        if entry is not None:
            count, _last_status, first_seen = entry
            if now - first_seen < _FEISHU_WEBHOOK_ANOMALY_TTL_SECONDS:
                count += 1
                if count % _FEISHU_WEBHOOK_ANOMALY_THRESHOLD == 0:
                    logger.warning(
                        "[Feishu] Webhook anomaly: %d consecutive error responses (%s) from %s "
                        "over the last %.0fs",
                        count,
                        status,
                        remote_ip,
                        now - first_seen,
                    )
                self._webhook_anomaly_counts[remote_ip] = (count, status, first_seen)
                return
        # Either first occurrence or TTL expired — start fresh.
        self._webhook_anomaly_counts[remote_ip] = (1, status, now)

    def _clear_webhook_anomaly(self, remote_ip: str) -> None:
        """Reset the anomaly counter for remote_ip after a successful request."""
        self._webhook_anomaly_counts.pop(remote_ip, None)

    # =========================================================================
    # Inbound processing pipeline
    # =========================================================================

    def _resolve_channel_prompt(self, chat_id: str, parent_id: str | None = None) -> str | None:
        """Resolve a Feishu per-channel system prompt.

        Mirrors the Discord/Slack behaviour so ``channel_prompts: {<chat_id>:
        "<prompt>"}`` in ``PlatformConfig.extra`` is honoured for Feishu chats
        instead of being silently ignored.
        """
        from gateway.platforms.base import resolve_channel_prompt
        _config = getattr(self, "config", None)
        _extra = getattr(_config, "extra", None) or {}
        return resolve_channel_prompt(_extra, chat_id, parent_id)

    async def _process_inbound_message(
        self,
        *,
        data: Any,
        message: Any,
        sender_id: Any,
        chat_type: str,
        message_id: str,
        is_bot: bool = False,
    ) -> Optional[bool]:
        try:
            extracted = await self._extract_message_content(message, include_mentions=True)
        except TypeError as exc:
            if "include_mentions" not in str(exc):
                raise
            extracted = await self._extract_message_content(message)
        if len(extracted) == 4:
            text, inbound_type, media_urls, media_types = extracted
            mentions = getattr(message, "mentions", None) or []
        else:
            text, inbound_type, media_urls, media_types, mentions = extracted

        self_mention_command_directed = bool(
            inbound_type == MessageType.TEXT
            and _self_mention_is_command_directed(text, mentions)
        )
        if inbound_type == MessageType.TEXT:
            text = _strip_edge_self_mentions(text, mentions)
            if text.startswith("/"):
                inbound_type = MessageType.COMMAND

        # Guard runs post-strip so a pure "@Bot" message (stripped to "") is dropped.
        if inbound_type == MessageType.TEXT and not text and not media_urls:
            logger.debug("[Feishu] Ignoring empty text message id=%s", message_id)
            return

        if inbound_type != MessageType.COMMAND:
            hint = _build_mention_hint(mentions)
            if hint:
                text = f"{hint}\n\n{text}" if text else hint

        thread_id = getattr(message, "thread_id", None) or getattr(message, "root_id", None) or None
        reply_to_message_id = (
            getattr(message, "parent_id", None)
            or getattr(message, "upper_message_id", None)
            or getattr(message, "root_id", None)
            or None
        )
        raw_chat_id = str(getattr(message, "chat_id", "") or "").strip()
        requires_complete_reply_context = bool(
            reply_to_message_id
            and raw_chat_id in {G1Q3_RCA_GROUP_ID, PNC_ALL_BUSINESS_TEST_GROUP_ID}
            and str(chat_type or "").strip().lower() not in {"p2p", "dm"}
            and not is_bot
            and self_mention_command_directed
            and self._mentions_self(message)
        )
        try:
            reply_to_text = (
                await self._fetch_message_text(reply_to_message_id)
                if reply_to_message_id
                else None
            )
        except Exception:
            if requires_complete_reply_context:
                raise
            logger.warning(
                "[Feishu] Continuing ordinary message without parent context: %s",
                reply_to_message_id,
                exc_info=True,
            )
            reply_to_text = None

        sender_primary = (
            getattr(sender_id, "open_id", None)
            or getattr(sender_id, "user_id", None)
            or getattr(sender_id, "union_id", None)
            or "<unknown>"
        )
        ingress_source = str(getattr(data, "_hermes_ingress_source", "") or "event_callback")
        logger.info(
            "[Feishu] Inbound %s message received: id=%s type=%s chat_id=%s sender=%s:%s ingress=%s text=%r media=%d",
            "dm" if chat_type == "p2p" else "group",
            message_id,
            inbound_type.value,
            getattr(message, "chat_id", "") or "",
            "bot" if is_bot else "user",
            sender_primary,
            ingress_source,
            text[:120],
            len(media_urls),
        )

        chat_id = raw_chat_id
        chat_info = await self.get_chat_info(chat_id)
        try:
            sender_profile = await self._resolve_sender_profile(sender_id, is_bot=is_bot)
        except TypeError as exc:
            if "is_bot" not in str(exc):
                raise
            sender_profile = await self._resolve_sender_profile(sender_id)
        source_chat_type = self._resolve_source_chat_type(chat_info=chat_info, event_chat_type=chat_type)
        source = self.build_source(
            chat_id=chat_id,
            chat_name=chat_info.get("name") or chat_id or "Feishu Chat",
            chat_type=source_chat_type,
            user_id=sender_profile["user_id"],
            user_name=sender_profile["user_name"],
            thread_id=self._resolve_source_thread_id(
                message=message,
                message_id=message_id,
                source_chat_type=source_chat_type,
            ),
            user_id_alt=sender_profile["user_id_alt"],
            is_bot=is_bot,
        )
        is_group_message = source_chat_type != "dm"
        self_mentioned = bool(self._mentions_self(message))
        mention_required = bool(
            is_group_message and self._require_mention_for(chat_id)
        )
        link_urls: List[str] = []
        try:
            raw_content_for_links = getattr(message, "content", "") or ""
            raw_type_for_links = getattr(message, "message_type", "") or ""
            link_metadata = normalize_feishu_message(
                message_type=raw_type_for_links,
                raw_content=raw_content_for_links,
                mentions=getattr(message, "mentions", None),
                bot=self._bot_identity(),
            ).metadata
            if isinstance(link_metadata, dict):
                raw_link_urls = link_metadata.get("link_urls") or []
                if isinstance(raw_link_urls, list):
                    link_urls = [str(url) for url in raw_link_urls if str(url or "").strip()]
        except Exception:
            logger.debug("[Feishu] Failed to collect link metadata for message %s", message_id, exc_info=True)

        normalized = MessageEvent(
            text=text,
            message_type=inbound_type,
            source=source,
            raw_message=data,
            message_id=message_id,
            media_urls=media_urls,
            media_types=media_types,
            reply_to_message_id=reply_to_message_id,
            reply_to_text=reply_to_text,
            channel_prompt=self._resolve_channel_prompt(chat_id, thread_id or None),
            metadata={
                "feishu": {
                    "message_id": message_id,
                    "root_id": str(getattr(message, "root_id", "") or "").strip() or None,
                    "parent_id": str(getattr(message, "parent_id", "") or "").strip() or None,
                    "thread_id": source.thread_id or None,
                    "sender_id": sender_profile["user_id"],
                    "sender_type": "bot" if is_bot else "user",
                    "is_bot_sender": bool(is_bot),
                    "is_topic": bool(source.thread_id),
                    "raw_container_id": str(getattr(message, "chat_id", "") or chat_id or "").strip() or None,
                    "receive_time_ms": getattr(message, "create_time", None),
                    "ingress_source": ingress_source,
                    "link_urls": link_urls or None,
                    "self_mentioned": self_mentioned,
                    "self_mention_command_directed": self_mention_command_directed,
                    "mention_required": mention_required,
                }
            },
            timestamp=datetime.now(),
        )

        if chat_type != "p2p":
            direct_grant = await self._maybe_handle_direct_permission_grant(
                event=normalized,
                chat_id=chat_id,
                actor_user_id=str(sender_profile.get("user_id") or ""),
                actor_user_name=str(sender_profile.get("user_name") or ""),
            )
            if direct_grant is not None:
                return

            maybe_role = await self._maybe_handle_permission_request(
                event=normalized,
                chat_id=chat_id,
                user_id=sender_profile["user_id"],
                user_name=sender_profile["user_name"],
            )
            if maybe_role is not None:
                return

        return await self._dispatch_inbound_event(normalized)

    # =========================================================================
    # Admission queue processing
    # =========================================================================

    async def _process_durable_g1q3_queue_event(self, event: MessageEvent) -> Dict[str, Any]:
        """Run a fixed-group directed mention inline until Gateway decides it."""
        if not self._message_handler:
            raise RuntimeError("gateway message handler is unavailable")

        chat_id = str(event.source.chat_id or "")
        chat_lock = self._get_chat_lock(chat_id)
        async with chat_lock:
            if event.message_id and self._reactions_enabled():
                await self._add_ack_reaction(event.message_id)
            await self._run_processing_hook("on_processing_start", event)
            try:
                response = await self._message_handler(event)
                metadata = event.metadata if isinstance(event.metadata, dict) else {}
                policy_error = metadata.get("pnc_group_binding_error")
                if (
                    isinstance(policy_error, dict)
                    and policy_error.get("schema_version")
                    == "pnc_group_binding_error_v1"
                    and policy_error.get("retryable") is True
                ):
                    code = str(policy_error.get("code") or "unknown")
                    raise RuntimeError(f"retryable PNC group binding error: {code}")
                manual_admission = metadata.get("pnc_manual_rca_admission")
                binding = metadata.get("pnc_group_binding")
                authorization = metadata.get("pnc_manual_authorization")
                binding = binding if isinstance(binding, dict) else {}
                authorization = authorization if isinstance(authorization, dict) else {}
                manual_route = (
                    binding.get("decision") == "accepted"
                    and binding.get("route_surface") == "rca_manual_intake"
                )
                terminal_rejection = bool(
                    manual_route and authorization.get("authorized") is False
                )
                if manual_route and not isinstance(manual_admission, dict) and not terminal_rejection:
                    raise RuntimeError("RCA manual admission did not reach a durable decision")

                response_text, ephemeral_ttl = self._unwrap_ephemeral(response)
                if response_text:
                    try:
                        send_result = await self._send_with_retry(
                            chat_id=chat_id,
                            content=str(response_text),
                            reply_to=_reply_anchor_for_event(event),
                            metadata=_thread_metadata_for_source(
                                event.source,
                                _reply_anchor_for_event(event),
                            ),
                        )
                        if (
                            ephemeral_ttl > 0
                            and send_result.success
                            and send_result.message_id
                        ):
                            self._schedule_ephemeral_delete(
                                chat_id=chat_id,
                                message_id=send_result.message_id,
                                ttl_seconds=ephemeral_ttl,
                            )
                    except Exception:
                        # The control-store decision is already durable. Response
                        # delivery failure must not create another RCA generation.
                        logger.warning(
                            "[admission] Failed to send durable RCA decision response",
                            exc_info=True,
                        )
            except BaseException:
                await self._run_processing_hook(
                    "on_processing_complete", event, ProcessingOutcome.FAILURE
                )
                raise
            await self._run_processing_hook(
                "on_processing_complete", event, ProcessingOutcome.SUCCESS
            )
            return {
                "durable_admission": isinstance(manual_admission, dict),
                "terminal_rejection": terminal_rejection,
            }

    async def _process_queue_item(self, item) -> dict:
        """Process a queue item by reconstructing the event and dispatching it.

        Called by QueueWorker when an item is dequeued.
        """
        raw_context = getattr(item, "event_context", None)
        requires_durable_decision = bool(
            isinstance(raw_context, dict)
            and raw_context.get("route_contract") == _G1Q3_RCA_MANUAL_QUEUE_ROUTE
        )
        try:
            from gateway.session import SessionSource

            context = _validated_feishu_queue_event_context(item)
            if requires_durable_decision and context is None:
                raise RuntimeError("trusted Feishu queue context is missing or invalid")
            source_context = context.get("source", {}) if context else {}
            event_context = context.get("event", {}) if context else {}
            source = SessionSource(
                platform=Platform.FEISHU,
                user_id=item.user_id,
                user_id_alt=source_context.get("user_id_alt"),
                user_name=source_context.get("user_name"),
                chat_id=item.chat_id or "",
                chat_name=source_context.get("chat_name"),
                chat_type=item.chat_type or "dm",
                thread_id=item.thread_id or None,
                is_bot=source_context.get("is_bot") is True,
                message_id=item.request_message_id or item.id,
            )
            event = MessageEvent(
                source=source,
                text=item.message,
                message_type=MessageType.TEXT,
                message_id=item.request_message_id or item.id,
                reply_to_message_id=event_context.get("reply_to_message_id"),
                reply_to_text=event_context.get("reply_to_text"),
                metadata={"feishu": context["feishu"]} if context else {},
            )
            durable_decision = _requires_durable_g1q3_gateway_decision(event)
            if requires_durable_decision and not durable_decision:
                raise RuntimeError("trusted Feishu RCA route contract did not validate")
            if durable_decision:
                result = await self._process_durable_g1q3_queue_event(event)
                self._complete_message_processing(event.message_id)
                return {
                    "status": "completed",
                    "durable_feishu_completion": True,
                    **result,
                }
            await self._handle_message_with_guards(event)
            return {"status": "completed"}
        except BaseException as exc:
            if requires_durable_decision and item.request_message_id:
                self._abandon_message_processing(item.request_message_id)
            logger.error("[admission] Failed to process queue item %s: %s", item.id, exc, exc_info=True)
            raise

    # =========================================================================
    # Message dispatch
    # =========================================================================

    async def _dispatch_inbound_event(self, event: MessageEvent) -> bool:
        """Apply Feishu-specific burst protection before entering the base adapter."""
        # --- Admission gate (optional) ---
        if self._admission_enabled and self._admission_controller:
            user_id = event.source.user_id or ""
            message_text = _admission_text_with_issue_links(event)
            chat_id = event.source.chat_id or ""
            thread_id = event.source.thread_id or ""
            chat_type = getattr(event.source, "chat_type", "dm") or "dm"
            # Map SessionSource chat_type ("dm"/"group") to admission chat_type
            admission_chat_type = "group" if chat_type == "group" else None
            requires_durable_decision = _requires_durable_g1q3_gateway_decision(event)
            try:
                business_line = "integration_tools" if _is_integration_tools_message_context(chat_id, message_text) else "generic"
                intent = (
                    classify_integration_tools_intent(message_text)
                    if business_line == "integration_tools"
                    else "general"
                )

                # Deterministic integration-tools runbook Q&A must terminate before
                # admission enqueue.  If we enqueue first, QueueWorker can still
                # consume the item and create a long-running intake/card for a
                # question that has already been answered.
                pre_admission_fast_reply = (
                    build_integration_tools_runbook_fast_reply(message_text)
                    if business_line == "integration_tools"
                    else None
                )
                if pre_admission_fast_reply:
                    policy_ctx = FeishuInteractionContext(
                        business_line=business_line,
                        intent=intent,
                        chat_id=chat_id,
                        thread_id=thread_id,
                        request_message_id=str(event.message_id or ""),
                        lane="fast",
                    )
                    ack_text = build_intake_ack(policy_ctx)
                    try:
                        if ack_text:
                            await self.send(
                                chat_id,
                                ack_text,
                                metadata={"thread_id": thread_id} if thread_id else None,
                            )
                        await self.send(
                            chat_id,
                            pre_admission_fast_reply,
                            metadata={"thread_id": thread_id} if thread_id else None,
                        )
                    except Exception:
                        logger.warning("[admission] Failed to send pre-admission integration-tools fast reply", exc_info=True)
                    return True

                admitted, feedback, queue_item = await self._admission_controller.admit(
                    user_id=user_id,
                    message=message_text,
                    chat_id=chat_id,
                    chat_type=admission_chat_type,
                    thread_id=thread_id,
                    request_message_id=event.message_id,
                    platform="feishu",
                    event_context=_build_feishu_queue_event_context(
                        event,
                        durable_rca_manual=requires_durable_decision,
                    ),
                    require_durable_persistence=requires_durable_decision,
                )
                if not admitted:
                    logger.info("[admission] Rejected user=%s: %s", user_id, feedback)
                    return True
                if requires_durable_decision and queue_item is None:
                    raise RuntimeError("durable RCA admission returned no queue item")
                logger.debug(
                    "[admission] Queued user=%s lane=%s id=%s",
                    user_id,
                    queue_item.lane if queue_item else "?",
                    queue_item.id if queue_item else "?",
                )

                policy_ctx = FeishuInteractionContext(
                    business_line=business_line,
                    intent=intent,
                    chat_id=chat_id,
                    thread_id=thread_id,
                    request_message_id=str(event.message_id or ""),
                    lane=(queue_item.lane if queue_item else "standard"),
                )

                # One public Feishu entry style: business flows acknowledge intake in
                # the original topic immediately.  G1Q3 completion/card behavior is
                # untouched; this fills the earlier admission/direct-QA gap.
                ack_text = build_intake_ack(policy_ctx)
                if ack_text:
                    try:
                        await self.send(
                            chat_id,
                            ack_text,
                            metadata={"thread_id": thread_id} if thread_id else None,
                        )
                    except Exception:
                        logger.warning("[admission] Failed to send intake ack", exc_info=True)

                # Public queue notices are still useful for generic heavy/VM work;
                # business flows use the shared intake ack above and later cards.
                if feedback and queue_item and queue_item.lane == "heavy" and business_line == "generic":
                    feedback_text = f"{feedback}。这是 heavy/VM 类任务，可能需要几分钟；我会尽量保持在这个话题回传。"
                    try:
                        await self.send(
                            chat_id,
                            feedback_text,
                            metadata={"thread_id": thread_id} if thread_id else None,
                        )
                    except Exception:
                        logger.warning("[admission] Failed to send queue feedback", exc_info=True)
                if requires_durable_decision:
                    result = getattr(queue_item, "result", None)
                    return bool(
                        getattr(queue_item, "status", None) == "completed"
                        and isinstance(result, dict)
                        and result.get("durable_feishu_completion") is True
                    )
                return True  # Worker will process via _process_queue_item
            except Exception:
                logger.warning("[admission] Gate error, falling through", exc_info=True)
                if requires_durable_decision:
                    raise

        if event.message_type == MessageType.TEXT and not event.is_command():
            await self._enqueue_text_event(event)
            return True
        if self._should_batch_media_event(event):
            await self._enqueue_media_event(event)
            return True
        await self._handle_message_with_guards(event)
        return True

    def _is_permission_request_message(self, text: str) -> bool:
        normalized = str(text or "").strip()
        if not normalized:
            return False
        return any(pattern.search(normalized) for pattern in _PERMISSION_REQUEST_TEXT_PATTERNS)

    def _infer_permission_requested_role(self, text: str, *, default: str = "member") -> str:
        normalized = str(text or "")
        for pattern, role in _PERMISSION_REQUEST_ROLE_HINTS:
            if pattern.search(normalized):
                return role
        return default if default in _PERMISSION_GRANT_ALLOWED_ROLES else "member"

    def _is_direct_permission_grant_message(self, text: str) -> bool:
        normalized = str(text or "").strip()
        if not normalized:
            return False
        return any(pattern.search(normalized) for pattern in _DIRECT_PERMISSION_GRANT_TEXT_PATTERNS)

    @staticmethod
    def _strip_permission_request_actor_prefix(text: str) -> str:
        normalized = str(text or "").strip()
        if not normalized:
            return ""
        return re.sub(
            r"^(?:\[Mentioned:[^\]]+\]\s*)?(?:@[\w\-\u4e00-\u9fff]+\s*)+",
            "",
            normalized,
        ).strip()

    def _extract_direct_permission_target_from_text(self, text: str) -> tuple[str, str]:
        normalized = self._strip_permission_request_actor_prefix(text)
        if not normalized:
            return "", ""

        mention_matches = list(
            re.finditer(
                r"(?P<name>[\u4e00-\u9fffA-Za-z0-9_.-]{1,40})\s*\(open_id=(?P<open_id>ou_[^)\s]+)\)",
                normalized,
            )
        )
        for match in mention_matches:
            tail = normalized[match.end(): match.end() + 80]
            head = normalized[max(0, match.start() - 12): match.start()]
            if re.search(r"(?:开通|开|授权|权限|访问|角色)", tail) or re.search(r"(?:给|帮)$", head):
                return match.group("open_id").strip(), match.group("name").strip()
        if len(mention_matches) == 1 and re.search(r"(?:开通|开|授权|权限|访问|角色)", normalized):
            match = mention_matches[0]
            return match.group("open_id").strip(), match.group("name").strip()

        patterns = (
            r"(?:给|帮)\s*(?P<name>[^\s，,。:：]{1,20})\s*(?:开通|开|授权)",
            r"(?:给|帮)\s*(?P<name>[^\s，,。:：]{1,20})\s*对应权限",
            r"(?:grant|authorize)\s+(?P<name>[^\s，,。:：]{1,40})",
        )
        for pattern in patterns:
            match = re.search(pattern, normalized, re.IGNORECASE)
            if match:
                name = match.group("name").strip(" @，,。:：;；")
                if self._is_usable_permission_display_name(name):
                    mapped_user_id = self._permission_user_id_from_local_mapping(name)
                    if mapped_user_id:
                        return mapped_user_id, name
                    return "", name
        return "", ""

    @staticmethod
    def _permission_user_id_from_local_mapping(display_name: str) -> Optional[str]:
        normalized_name = str(display_name or "").strip()
        if not normalized_name:
            return None
        try:
            from tools.permission_policy import find_user_id_by_name

            return find_user_id_by_name(normalized_name)
        except Exception:
            logger.debug("[Feishu] Failed to resolve permission target id from local mapping", exc_info=True)
            return None

    async def _maybe_handle_direct_permission_grant(
        self,
        *,
        event: MessageEvent,
        chat_id: str,
        actor_user_id: str,
        actor_user_name: str,
    ) -> Optional[str]:
        text = str(getattr(event, "text", "") or "").strip()
        if not self._is_direct_permission_grant_message(text):
            return None

        try:
            from gateway.pairing import PairingStore
            from tools.permission_policy import get_user_role_by_id, map_user_id, set_user_role

            actor_role = get_user_role_by_id(actor_user_id) if actor_user_id else "member"
            if actor_role not in {"owner", "admin"}:
                return None

            target_user_id, target_user_name = self._extract_direct_permission_target_from_text(text)
            if not target_user_id and target_user_name:
                target_user_id = self._permission_user_id_from_local_mapping(target_user_name) or ""
            if target_user_id and not self._is_usable_permission_display_name(target_user_name):
                mapped_name = self._permission_name_from_local_mapping(target_user_id)
                target_user_name = str(mapped_name or target_user_name or "").strip()
            if not target_user_id or not self._is_usable_permission_display_name(target_user_name):
                return None

            requested_role = self._infer_permission_requested_role(text, default="senior")
            if requested_role == "member":
                requested_role = "senior"
            set_user_role(target_user_name, requested_role)
            map_user_id(target_user_name, target_user_id)
            PairingStore().approve_user("feishu", target_user_id, target_user_name)
        except Exception as exc:
            logger.error("[Feishu] Failed to apply direct permission grant: %s", exc, exc_info=True)
            await self.send(
                chat_id=chat_id,
                content=f"权限开通失败：{exc}",
                reply_to=event.message_id,
                metadata={"thread_id": getattr(event.source, "thread_id", None)} if getattr(event.source, "thread_id", None) else None,
            )
            return "direct_permission_grant_failed"

        logger.info(
            "[Feishu] Directly granted %s role to %s (%s) by %s (%s)",
            requested_role,
            target_user_name,
            target_user_id,
            actor_user_name,
            actor_user_id,
        )
        await self.send(
            chat_id=chat_id,
            content=(
                f"已为 {target_user_name} 开通 {requested_role} 权限，并完成 Feishu 配对。\n"
                "现在可重新发起 VM worker / PNC 任务；我会按当前身份和 repo ACL 重新判定。"
            ),
            reply_to=event.message_id,
            metadata={"thread_id": getattr(event.source, "thread_id", None)} if getattr(event.source, "thread_id", None) else None,
        )
        return requested_role

    def _select_permission_admin_chat_id(self, event: MessageEvent) -> Optional[str]:
        explicit_chat_id = str(self.config.extra.get("permission_approval_chat_id") or "").strip()
        if explicit_chat_id:
            return explicit_chat_id
        source = getattr(event, "source", None)
        chat_id = str(getattr(source, "chat_id", "") or "")
        if chat_id and chat_id in self._admins:
            return chat_id
        home_channel = getattr(self.config, "home_channel", None)
        home_chat_id = str(getattr(home_channel, "chat_id", "") or "")
        if home_chat_id:
            return home_chat_id
        admins = sorted(str(item).strip() for item in self._admins if str(item).strip())
        return admins[0] if admins else None

    def _prune_permission_request_dedup(self) -> None:
        now = time.monotonic()
        ttl_seconds = int(self.config.extra.get("permission_request_dedup_ttl_seconds", _FEISHU_PERMISSION_REQUEST_DEDUP_TTL_SECONDS) or _FEISHU_PERMISSION_REQUEST_DEDUP_TTL_SECONDS)
        stale_keys = [
            key for key, seen_at in self._permission_request_seen.items()
            if now - seen_at > ttl_seconds
        ]
        for key in stale_keys:
            self._permission_request_seen.pop(key, None)

    def _find_pending_permission_approval(self, *, user_id: str, chat_id: str) -> Optional[Dict[str, Any]]:
        self._gc_stale_approval_state()
        for state in self._approval_state.values():
            if (
                str(state.get("approval_kind") or "").strip().lower() == "permission_grant"
                and str(state.get("target_user_id") or "").strip() == user_id
                and str(state.get("request_chat_id") or "").strip() == chat_id
            ):
                return state
        return None

    def _mark_permission_request_seen(self, *, user_id: str, chat_id: str) -> bool:
        self._prune_permission_request_dedup()
        dedup_key = f"{chat_id}:{user_id}"
        if dedup_key in self._permission_request_seen:
            return True
        self._permission_request_seen[dedup_key] = time.monotonic()
        return False

    async def _maybe_handle_permission_request(
        self,
        *,
        event: MessageEvent,
        chat_id: str,
        user_id: str,
        user_name: str,
    ) -> Optional[str]:
        text = str(getattr(event, "text", "") or "").strip()
        if not self._is_permission_request_message(text):
            return None

        try:
            from tools.permission_policy import get_user_role_by_id

            current_role = get_user_role_by_id(user_id) if user_id else "member"
        except Exception:
            logger.debug("[Feishu] Failed to resolve current role for permission request", exc_info=True)
            current_role = "member"

        if current_role != "member":
            return None

        pending_state = self._find_pending_permission_approval(user_id=user_id, chat_id=chat_id)
        if pending_state is not None:
            logger.info("[Feishu] Reusing pending permission approval for %s in %s", user_id, chat_id)
            pending_state["reuse_hit"] = True
            pending_state["reused_approval_id"] = pending_state.get("approval_id")
            pending_state["reused_approval_message_id"] = pending_state.get("message_id")
            self._emit_permission_audit_event(
                outcome="reused_pending",
                state=pending_state,
                approver_name="system",
                reuse_hit=True,
                dedup_hit=False,
            )
            await self.send(
                chat_id=chat_id,
                content="你的权限申请正在处理，请等待管理员审批结果。",
                reply_to=event.message_id,
                metadata={"thread_id": getattr(event.source, "thread_id", None)} if getattr(event.source, "thread_id", None) else None,
            )
            return "pending_permission_request"

        if self._mark_permission_request_seen(user_id=user_id, chat_id=chat_id):
            logger.info("[Feishu] Deduplicated repeated permission request from %s in %s", user_id, chat_id)
            self._emit_permission_audit_event(
                outcome="deduplicated",
                state={
                    "approval_kind": "permission_grant",
                    "target_user_id": user_id,
                    "target_user_name": user_name,
                    "request_chat_id": chat_id,
                    "request_chat_name": str(getattr(event.source, "chat_name", "") or "").strip(),
                    "request_message_id": event.message_id,
                    "request_text": text,
                },
                approver_name="system",
                reuse_hit=False,
                dedup_hit=True,
            )
            return "duplicate_permission_request"

        admin_chat_id = self._select_permission_admin_chat_id(event)
        if not admin_chat_id:
            logger.warning("[Feishu] No admin chat available for permission request from %s", user_id)
            fallback_result = await self.send(
                chat_id=chat_id,
                content="已收到你的申请，但管理员审批通道未配置，请联系管理员处理。",
                reply_to=event.message_id,
                metadata={"thread_id": getattr(event.source, "thread_id", None)} if getattr(event.source, "thread_id", None) else None,
            )
            if not getattr(fallback_result, "success", False):
                logger.warning(
                    "[Feishu] Failed to send missing-approval-channel notice to %s: %s",
                    chat_id,
                    getattr(fallback_result, "error", None),
                )
            return "missing_approval_channel"

        ack_result = await self.send(
            chat_id=chat_id,
            content=_PERMISSION_REQUEST_ACK_TEXT,
            reply_to=event.message_id,
            metadata={"thread_id": getattr(event.source, "thread_id", None)} if getattr(event.source, "thread_id", None) else None,
        )
        if not getattr(ack_result, "success", False):
            logger.warning("[Feishu] Failed to send permission request ack to %s: %s", chat_id, getattr(ack_result, "error", None))

        requested_role = self._infer_permission_requested_role(text, default="member")
        target_user_id, target_user_name = await self._normalize_permission_request_identity(
            user_id=user_id,
            user_name=user_name,
        )
        request_thread_id = str(getattr(event.source, "thread_id", "") or "").strip()
        session_key = f"permission_grant:{target_user_id or user_id}:{event.message_id or uuid.uuid4()}"
        approval_metadata = {
            "approval_kind": "permission_grant",
            "target_user_id": target_user_id,
            "target_user_name": target_user_name,
            "requested_role": requested_role,
            "request_chat_id": chat_id,
            "request_thread_id": request_thread_id,
            "request_chat_name": str(getattr(event.source, "chat_name", "") or "").strip(),
            "request_message_id": event.message_id,
            "request_text": text,
            "reuse_hit": False,
            "dedup_hit": False,
        }
        description = text[:500]
        result = await self.send_exec_approval(
            chat_id=admin_chat_id,
            command=text,
            session_key=session_key,
            description=description,
            metadata=approval_metadata,
        )
        if not getattr(result, "success", False):
            logger.warning(
                "[Feishu] Failed to send permission approval card for %s (%s): %s",
                user_name,
                user_id,
                getattr(result, "error", None),
            )
        return requested_role

    # =========================================================================
    # Media batching
    # =========================================================================

    def _should_batch_media_event(self, event: MessageEvent) -> bool:
        return bool(
            event.media_urls
            and event.message_type in {MessageType.PHOTO, MessageType.VIDEO, MessageType.DOCUMENT, MessageType.AUDIO}
        )

    def _media_batch_key(self, event: MessageEvent) -> str:
        from gateway.session import build_session_key

        session_key = build_session_key(
            event.source,
            group_sessions_per_user=self.config.extra.get("group_sessions_per_user", True),
            thread_sessions_per_user=self.config.extra.get("thread_sessions_per_user", False),
        )
        return f"{session_key}:media:{event.message_type.value}"

    @staticmethod
    def _media_batch_is_compatible(existing: MessageEvent, incoming: MessageEvent) -> bool:
        return (
            existing.message_type == incoming.message_type
            and existing.reply_to_message_id == incoming.reply_to_message_id
            and existing.reply_to_text == incoming.reply_to_text
            and existing.source.thread_id == incoming.source.thread_id
        )

    async def _enqueue_media_event(self, event: MessageEvent) -> None:
        key = self._media_batch_key(event)
        existing = self._pending_media_batches.get(key)
        if existing is None:
            self._pending_media_batches[key] = event
            self._schedule_media_batch_flush(key)
            return
        if not self._media_batch_is_compatible(existing, event):
            await self._flush_media_batch_now(key)
            self._pending_media_batches[key] = event
            self._schedule_media_batch_flush(key)
            return
        existing.media_urls.extend(event.media_urls)
        existing.media_types.extend(event.media_types)
        if event.text:
            existing.text = self._merge_caption(existing.text, event.text)
        existing.timestamp = event.timestamp
        if event.message_id:
            existing.message_id = event.message_id
        self._schedule_media_batch_flush(key)

    def _schedule_media_batch_flush(self, key: str) -> None:
        self._reschedule_batch_task(
            self._pending_media_batch_tasks,
            key,
            self._flush_media_batch,
        )

    async def _flush_media_batch(self, key: str) -> None:
        current_task = asyncio.current_task()
        try:
            await asyncio.sleep(self._media_batch_delay_seconds)
            await self._flush_media_batch_now(key)
        finally:
            if self._pending_media_batch_tasks.get(key) is current_task:
                self._pending_media_batch_tasks.pop(key, None)

    async def _flush_media_batch_now(self, key: str) -> None:
        event = self._pending_media_batches.pop(key, None)
        if not event:
            return
        logger.info(
            "[Feishu] Flushing media batch %s with %d attachment(s)",
            key,
            len(event.media_urls),
        )
        await self._handle_message_with_guards(event)

    async def _download_remote_image(self, image_url: str) -> str:
        ext = self._guess_remote_extension(image_url, default=".jpg")
        return await cache_image_from_url(image_url, ext=ext)

    async def _download_remote_document(
        self,
        file_url: str,
        *,
        default_ext: str,
        preferred_name: str,
    ) -> tuple[str, str]:
        from tools.url_safety import is_safe_url
        if not is_safe_url(file_url):
            raise ValueError(f"Blocked unsafe URL (SSRF protection): {file_url[:80]}")

        import httpx

        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            response = await client.get(
                file_url,
                headers={
                    "User-Agent": "Mozilla/5.0 (compatible; HermesAgent/1.0)",
                    "Accept": "*/*",
                },
            )
            response.raise_for_status()
            # Snapshot Content-Type and body while the client context is
            # still active so pooled connections fully release on exit.
            # See #18451.
            content_type_hdr = str(response.headers.get("Content-Type", ""))
            body = response.content
        filename = self._derive_remote_filename(
            file_url,
            content_type=content_type_hdr,
            default_name=preferred_name,
            default_ext=default_ext,
        )
        cached_path = cache_document_from_bytes(body, filename)
        return cached_path, filename

    @staticmethod
    def _guess_remote_extension(url: str, *, default: str) -> str:
        ext = Path((url or "").split("?", 1)[0]).suffix.lower()
        return ext if ext in (_IMAGE_EXTENSIONS | _AUDIO_EXTENSIONS | _VIDEO_EXTENSIONS | set(SUPPORTED_DOCUMENT_TYPES)) else default

    @staticmethod
    def _derive_remote_filename(file_url: str, *, content_type: str, default_name: str, default_ext: str) -> str:
        candidate = Path((file_url or "").split("?", 1)[0]).name or default_name
        ext = Path(candidate).suffix.lower()
        if not ext:
            guessed = mimetypes.guess_extension((content_type or "").split(";", 1)[0].strip().lower() or "") or default_ext
            candidate = f"{candidate}{guessed}"
        return candidate

    @staticmethod
    def _namespace_from_mapping(value: Any) -> Any:
        if isinstance(value, dict):
            return SimpleNamespace(**{key: FeishuAdapter._namespace_from_mapping(item) for key, item in value.items()})
        if isinstance(value, list):
            return [FeishuAdapter._namespace_from_mapping(item) for item in value]
        return value

    async def _handle_webhook_request(self, request: Any) -> Any:
        remote_ip = (getattr(request, "remote", None) or "unknown")

        # Rate limiting — composite key: app_id:path:remote_ip (matches openclaw key structure).
        rate_key = f"{self._app_id}:{self._webhook_path}:{remote_ip}"
        if not self._check_webhook_rate_limit(rate_key):
            logger.warning("[Feishu] Webhook rate limit exceeded for %s", remote_ip)
            self._record_webhook_anomaly(remote_ip, "429")
            return web.Response(status=429, text="Too Many Requests")

        # Content-Type guard — Feishu always sends application/json.
        headers = getattr(request, "headers", {}) or {}
        content_type = str(headers.get("Content-Type", "") or "").split(";")[0].strip().lower()
        if content_type and content_type != "application/json":
            logger.warning("[Feishu] Webhook rejected: unexpected Content-Type %r from %s", content_type, remote_ip)
            self._record_webhook_anomaly(remote_ip, "415")
            return web.Response(status=415, text="Unsupported Media Type")

        # Body size guard — reject early via Content-Length when present.
        content_length = getattr(request, "content_length", None)
        if content_length is not None and content_length > _FEISHU_WEBHOOK_MAX_BODY_BYTES:
            logger.warning("[Feishu] Webhook body too large (%d bytes) from %s", content_length, remote_ip)
            self._record_webhook_anomaly(remote_ip, "413")
            return web.Response(status=413, text="Request body too large")

        try:
            body_bytes: bytes = await asyncio.wait_for(
                _read_limited_feishu_webhook_body(
                    request,
                    _FEISHU_WEBHOOK_MAX_BODY_BYTES,
                ),
                timeout=_FEISHU_WEBHOOK_BODY_TIMEOUT_SECONDS,
            )
        except ValueError:
            logger.warning("[Feishu] Webhook body exceeds limit from %s", remote_ip)
            self._record_webhook_anomaly(remote_ip, "413")
            return web.Response(status=413, text="Request body too large")
        except asyncio.TimeoutError:
            logger.warning("[Feishu] Webhook body read timed out after %ds from %s", _FEISHU_WEBHOOK_BODY_TIMEOUT_SECONDS, remote_ip)
            self._record_webhook_anomaly(remote_ip, "408")
            return web.Response(status=408, text="Request Timeout")
        except Exception:
            self._record_webhook_anomaly(remote_ip, "400")
            return web.json_response({"code": 400, "msg": "failed to read body"}, status=400)

        try:
            payload = json.loads(body_bytes.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            self._record_webhook_anomaly(remote_ip, "400")
            return web.json_response({"code": 400, "msg": "invalid json"}, status=400)

        # Verification token check — second layer of defence beyond signature (matches openclaw).
        if self._verification_token:
            header = payload.get("header") or {}
            incoming_token = str(header.get("token") or payload.get("token") or "")
            if not incoming_token or not hmac.compare_digest(incoming_token, self._verification_token):
                logger.warning("[Feishu] Webhook rejected: invalid verification token from %s", remote_ip)
                self._record_webhook_anomaly(remote_ip, "401-token")
                return web.Response(status=401, text="Invalid verification token")

        # URL verification challenge — Feishu includes the verification token in
        # challenge requests. Validate the token (above) before reflecting the
        # challenge so an unauthenticated remote request cannot prove endpoint
        # control by getting attacker-supplied challenge data echoed back.
        if payload.get("type") == "url_verification":
            return web.json_response({"challenge": payload.get("challenge", "")})

        # Timing-safe signature verification (only enforced when encrypt_key is set).
        if self._encrypt_key and not self._is_webhook_signature_valid(request.headers, body_bytes):
            logger.warning("[Feishu] Webhook rejected: invalid signature from %s", remote_ip)
            self._record_webhook_anomaly(remote_ip, "401-sig")
            return web.Response(status=401, text="Invalid signature")

        if payload.get("encrypt"):
            logger.error("[Feishu] Encrypted webhook payloads are not supported by Hermes webhook mode")
            self._record_webhook_anomaly(remote_ip, "400-encrypted")
            return web.json_response({"code": 400, "msg": "encrypted webhook payloads are not supported"}, status=400)

        self._clear_webhook_anomaly(remote_ip)

        event_type = str((payload.get("header") or {}).get("event_type") or "")
        data = self._namespace_from_mapping(payload)
        if event_type == "im.message.receive_v1":
            self._on_message_event(data)
        elif event_type == "im.message.message_read_v1":
            self._on_message_read_event(data)
        elif event_type == "im.chat.member.bot.added_v1":
            self._on_bot_added_to_chat(data)
        elif event_type == "im.chat.member.bot.deleted_v1":
            self._on_bot_removed_from_chat(data)
        elif event_type in {"im.message.reaction.created_v1", "im.message.reaction.deleted_v1"}:
            self._on_reaction_event(event_type, data)
        elif event_type == "card.action.trigger":
            self._on_card_action_trigger(data)
        elif event_type == "drive.notice.comment_add_v1":
            self._on_drive_comment_event(data)
        elif event_type == "vc.bot.meeting_invited_v1":
            self._on_meeting_invited_event(data)
        else:
            logger.debug("[Feishu] Ignoring webhook event type: %s", event_type or "unknown")
        return web.json_response({"code": 0, "msg": "ok"})

    def _is_webhook_signature_valid(self, headers: Any, body_bytes: bytes) -> bool:
        """Verify Feishu webhook signature using timing-safe comparison.

        Feishu signature algorithm:
            SHA256(timestamp + nonce + encrypt_key + body_string)
        Headers checked: x-lark-request-timestamp, x-lark-request-nonce, x-lark-signature.
        """
        timestamp = str(headers.get("x-lark-request-timestamp", "") or "")
        nonce = str(headers.get("x-lark-request-nonce", "") or "")
        signature = str(headers.get("x-lark-signature", "") or "")
        if not timestamp or not nonce or not signature:
            return False
        try:
            body_str = body_bytes.decode("utf-8", errors="replace")
            content = f"{timestamp}{nonce}{self._encrypt_key}{body_str}"
            computed = hashlib.sha256(content.encode("utf-8")).hexdigest()
            return hmac.compare_digest(computed, signature)
        except Exception:
            logger.debug("[Feishu] Signature verification raised an exception", exc_info=True)
            return False

    def _check_webhook_rate_limit(self, rate_key: str) -> bool:
        """Return False when the composite rate_key has exceeded _FEISHU_WEBHOOK_RATE_LIMIT_MAX.

        The rate_key is composed as "{app_id}:{path}:{remote_ip}" — matching openclaw's key
        structure so the limit is scoped to a specific (account, endpoint, IP) triple rather
        than a bare IP, which causes fewer false-positive denials in multi-tenant setups.

        The tracking dict is capped at _FEISHU_WEBHOOK_RATE_MAX_KEYS entries to prevent unbounded
        memory growth. Stale (expired) entries are pruned when the cap is reached.
        """
        now = time.time()
        # Fast path: existing entry within the current window.
        entry = self._webhook_rate_counts.get(rate_key)
        if entry is not None:
            count, window_start = entry
            if now - window_start < _FEISHU_WEBHOOK_RATE_WINDOW_SECONDS:
                if count >= _FEISHU_WEBHOOK_RATE_LIMIT_MAX:
                    return False
                self._webhook_rate_counts[rate_key] = (count + 1, window_start)
                return True
        # New window for an existing key, or a brand-new key — prune stale entries first.
        if len(self._webhook_rate_counts) >= _FEISHU_WEBHOOK_RATE_MAX_KEYS:
            stale_keys = [
                k for k, (_, ws) in self._webhook_rate_counts.items()
                if now - ws >= _FEISHU_WEBHOOK_RATE_WINDOW_SECONDS
            ]
            for k in stale_keys:
                del self._webhook_rate_counts[k]
            # If still at capacity after pruning, deny untracked keys (fail closed).
            # The table only fills with this many distinct (account, endpoint, IP)
            # triples under abuse; allowing untracked requests through at capacity
            # would let an attacker who flooded the table bypass the limiter entirely.
            if rate_key not in self._webhook_rate_counts and len(self._webhook_rate_counts) >= _FEISHU_WEBHOOK_RATE_MAX_KEYS:
                logger.warning(
                    "[Feishu] Webhook rate-limit table at capacity (%d keys) — denying untracked key",
                    _FEISHU_WEBHOOK_RATE_MAX_KEYS,
                )
                return False
        self._webhook_rate_counts[rate_key] = (1, now)
        return True

    # =========================================================================
    # Text batching
    # =========================================================================

    def _text_batch_key(self, event: MessageEvent) -> str:
        """Return the session-scoped key used for Feishu text aggregation."""
        from gateway.session import build_session_key

        return build_session_key(
            event.source,
            group_sessions_per_user=self.config.extra.get("group_sessions_per_user", True),
            thread_sessions_per_user=self.config.extra.get("thread_sessions_per_user", False),
        )

    @staticmethod
    def _text_batch_is_compatible(existing: MessageEvent, incoming: MessageEvent) -> bool:
        """Only merge text events when reply/thread context is identical."""
        return (
            existing.reply_to_message_id == incoming.reply_to_message_id
            and existing.reply_to_text == incoming.reply_to_text
            and existing.source.thread_id == incoming.source.thread_id
        )

    async def _enqueue_text_event(self, event: MessageEvent) -> None:
        """Debounce rapid Feishu text bursts into a single MessageEvent."""
        key = self._text_batch_key(event)
        chunk_len = len(event.text or "")
        existing = self._pending_text_batches.get(key)
        if existing is None:
            event._last_chunk_len = chunk_len  # type: ignore[attr-defined]
            self._pending_text_batches[key] = event
            self._pending_text_batch_counts[key] = 1
            self._schedule_text_batch_flush(key)
            return

        if not self._text_batch_is_compatible(existing, event):
            await self._flush_text_batch_now(key)
            self._pending_text_batches[key] = event
            self._pending_text_batch_counts[key] = 1
            self._schedule_text_batch_flush(key)
            return

        existing_count = self._pending_text_batch_counts.get(key, 1)
        next_count = existing_count + 1
        appended_text = event.text or ""
        next_text = f"{existing.text}\n{appended_text}" if existing.text and appended_text else (existing.text or appended_text)
        if next_count > self._text_batch_max_messages or len(next_text) > self._text_batch_max_chars:
            await self._flush_text_batch_now(key)
            self._pending_text_batches[key] = event
            self._pending_text_batch_counts[key] = 1
            self._schedule_text_batch_flush(key)
            return

        existing.text = next_text
        existing._last_chunk_len = chunk_len  # type: ignore[attr-defined]
        existing.timestamp = event.timestamp
        if event.message_id:
            existing.message_id = event.message_id
        self._pending_text_batch_counts[key] = next_count
        self._schedule_text_batch_flush(key)

    def _schedule_text_batch_flush(self, key: str) -> None:
        """Reset the debounce timer for a pending Feishu text batch."""
        self._reschedule_batch_task(
            self._pending_text_batch_tasks,
            key,
            self._flush_text_batch,
        )

    @staticmethod
    def _reschedule_batch_task(
        task_map: Dict[str, asyncio.Task],
        key: str,
        flush_fn: Any,
    ) -> None:
        prior_task = task_map.get(key)
        if prior_task and not prior_task.done():
            prior_task.cancel()
        task_map[key] = asyncio.create_task(flush_fn(key))

    async def _flush_text_batch(self, key: str) -> None:
        """Flush a pending text batch after the quiet period.

        Uses a longer delay when the latest chunk is near Feishu's ~4096-char
        split point, since a continuation chunk is almost certain.
        """
        current_task = asyncio.current_task()
        try:
            # Adaptive delay: if the latest chunk is near the split threshold,
            # a continuation is almost certain — wait longer.
            pending = self._pending_text_batches.get(key)
            last_len = getattr(pending, "_last_chunk_len", 0) if pending else 0
            if last_len >= self._SPLIT_THRESHOLD:
                delay = self._text_batch_split_delay_seconds
            else:
                delay = self._text_batch_delay_seconds
            await asyncio.sleep(delay)
            await self._flush_text_batch_now(key)
        finally:
            if self._pending_text_batch_tasks.get(key) is current_task:
                self._pending_text_batch_tasks.pop(key, None)

    async def _flush_text_batch_now(self, key: str) -> None:
        """Dispatch the current text batch immediately."""
        event = self._pending_text_batches.pop(key, None)
        self._pending_text_batch_counts.pop(key, None)
        if not event:
            return
        logger.info(
            "[Feishu] Flushing text batch %s (%d chars)",
            key,
            len(event.text or ""),
        )
        await self._handle_message_with_guards(event)

    # =========================================================================
    # Message content extraction and resource download
    # =========================================================================

    async def _extract_message_content(
        self, message: Any, *, include_mentions: bool = False
    ) -> tuple[str, MessageType, List[str], List[str]] | tuple[str, MessageType, List[str], List[str], List[FeishuMentionRef]]:
        raw_content = getattr(message, "content", "") or ""
        raw_type = getattr(message, "message_type", "") or ""
        message_id = str(getattr(message, "message_id", "") or "")
        logger.info("[Feishu] Received raw message type=%s message_id=%s", raw_type, message_id)

        normalized = normalize_feishu_message(
            message_type=raw_type,
            raw_content=raw_content,
            mentions=getattr(message, "mentions", None),
            bot=self._bot_identity(),
        )
        media_urls, media_types, download_warnings = await self._download_feishu_message_resources_with_warnings(
            message_id=message_id,
            normalized=normalized,
        )
        inbound_type = self._resolve_normalized_message_type(normalized, media_types)
        text = normalized.text_content
        warning_parts = [warning for warning in download_warnings if warning]
        metadata_warning = str(normalized.metadata.get("warning", "") or "")
        if metadata_warning and metadata_warning not in warning_parts and metadata_warning not in text:
            warning_parts.insert(0, metadata_warning)
        if warning_parts:
            text = "\n".join(part for part in [text, *warning_parts] if part).strip()
            if not media_urls:
                inbound_type = MessageType.TEXT

        if (
            inbound_type in {MessageType.DOCUMENT, MessageType.AUDIO, MessageType.VIDEO, MessageType.PHOTO}
            and len(media_urls) == 1
            and normalized.preferred_message_type in {"document", "audio"}
        ):
            injected = await self._maybe_extract_text_document(media_urls[0], media_types[0])
            if injected:
                text = injected

        if include_mentions:
            return text, inbound_type, media_urls, media_types, list(normalized.mentions)
        return text, inbound_type, media_urls, media_types

    async def _download_feishu_message_resources(
        self,
        *,
        message_id: str,
        normalized: FeishuNormalizedMessage,
    ) -> tuple[List[str], List[str]]:
        media_urls, media_types, _warnings = await self._download_feishu_message_resources_with_warnings(
            message_id=message_id,
            normalized=normalized,
        )
        return media_urls, media_types

    async def _download_feishu_message_resources_with_warnings(
        self,
        *,
        message_id: str,
        normalized: FeishuNormalizedMessage,
    ) -> tuple[List[str], List[str], List[str]]:
        media_urls: List[str] = []
        media_types: List[str] = []
        warnings: List[str] = []

        for image_key in normalized.image_keys:
            cached_path, media_type = await self._download_feishu_image(
                message_id=message_id,
                image_key=image_key,
            )
            if cached_path:
                media_urls.append(cached_path)
                media_types.append(media_type)

        for media_ref in normalized.media_refs:
            legacy_downloader = self.__dict__.get("_download_feishu_message_resource")
            if legacy_downloader is None:
                class_downloader = getattr(type(self), "_download_feishu_message_resource", None)
                if class_downloader is not FeishuAdapter._download_feishu_message_resource:
                    legacy_downloader = self._download_feishu_message_resource
            if legacy_downloader is not None:
                cached_path, media_type = await legacy_downloader(
                    message_id=message_id,
                    file_key=media_ref.file_key,
                    resource_type=media_ref.resource_type,
                    fallback_filename=media_ref.file_name,
                )
                result = FeishuResourceDownloadResult(cached_path, media_type)
            else:
                result = await self._download_feishu_message_resource_result(
                    message_id=message_id,
                    file_key=media_ref.file_key,
                    resource_type=media_ref.resource_type,
                    fallback_filename=media_ref.file_name,
                )
            if result.path:
                media_urls.append(result.path)
                media_types.append(result.media_type)
            if result.warning:
                warnings.append(result.warning)

        return media_urls, media_types, warnings

    @staticmethod
    def _resolve_media_message_type(media_type: str, *, default: MessageType) -> MessageType:
        normalized = (media_type or "").lower()
        if normalized.startswith("image/"):
            return MessageType.PHOTO
        if normalized.startswith("audio/"):
            return MessageType.AUDIO
        if normalized.startswith("video/"):
            return MessageType.VIDEO
        return default

    def _resolve_normalized_message_type(
        self,
        normalized: FeishuNormalizedMessage,
        media_types: List[str],
    ) -> MessageType:
        preferred = normalized.preferred_message_type
        if preferred == "photo":
            return self._resolve_media_message_type(media_types[0] if media_types else "", default=MessageType.PHOTO)
        if preferred == "audio":
            return self._resolve_media_message_type(media_types[0] if media_types else "", default=MessageType.AUDIO)
        if preferred == "document":
            return self._resolve_media_message_type(media_types[0] if media_types else "", default=MessageType.DOCUMENT)
        return MessageType.TEXT

    async def _maybe_extract_text_document(self, cached_path: str, media_type: str) -> str:
        if not cached_path or not media_type.startswith("text/"):
            return ""
        try:
            if os.path.getsize(cached_path) > _MAX_TEXT_INJECT_BYTES:
                return ""
            ext = Path(cached_path).suffix.lower()
            if ext not in {".txt", ".md"} and media_type not in {"text/plain", "text/markdown"}:
                return ""
            content = Path(cached_path).read_text(encoding="utf-8")
            display_name = self._display_name_from_cached_path(cached_path)
            return f"[Content of {display_name}]:\n{content}"
        except (OSError, UnicodeDecodeError):
            logger.warning("[Feishu] Failed to inject text document content from %s", cached_path, exc_info=True)
            return ""

    async def _download_feishu_image(self, *, message_id: str, image_key: str) -> tuple[str, str]:
        if not self._client or not message_id:
            return "", ""
        try:
            request = self._build_message_resource_request(
                message_id=message_id,
                file_key=image_key,
                resource_type="image",
            )
            response = await self._run_blocking(self._client.im.v1.message_resource.get, request)
            if not response or not response.success():
                logger.warning(
                    "[Feishu] Failed to download image %s: %s %s",
                    image_key,
                    getattr(response, "code", "unknown"),
                    getattr(response, "msg", "request failed"),
                )
                return "", ""
            raw_bytes, _oversized = self._read_binary_response(response)
            if not raw_bytes:
                return "", ""
            content_type = self._get_response_header(response, "Content-Type")
            filename = getattr(response, "file_name", None) or f"{image_key}.jpg"
            ext = self._guess_extension(filename, content_type, ".jpg", allowed=_IMAGE_EXTENSIONS)
            cached_path = cache_image_from_bytes(raw_bytes, ext=ext)
            media_type = self._normalize_media_type(content_type, default=self._default_image_media_type(ext))
            return cached_path, media_type
        except Exception:
            logger.warning("[Feishu] Failed to cache image resource %s", image_key, exc_info=True)
            return "", ""

    async def _download_feishu_message_resource(
        self,
        *,
        message_id: str,
        file_key: str,
        resource_type: str,
        fallback_filename: str,
    ) -> tuple[str, str]:
        result = await self._download_feishu_message_resource_result(
            message_id=message_id,
            file_key=file_key,
            resource_type=resource_type,
            fallback_filename=fallback_filename,
        )
        return result.path, result.media_type

    async def _download_feishu_message_resource_result(
        self,
        *,
        message_id: str,
        file_key: str,
        resource_type: str,
        fallback_filename: str,
    ) -> FeishuResourceDownloadResult:
        if not self._client or not message_id:
            return FeishuResourceDownloadResult()

        max_file_bytes = _configured_feishu_max_file_bytes()
        if max_file_bytes == 0:
            warning = (
                f"{_normalize_feishu_text(fallback_filename or file_key)}: "
                "当前网关已通过 HERMES_FEISHU_MAX_FILE_BYTES=0（0 bytes）禁用飞书附件下载；"
                "请提供 VM/NAS 路径，或将中大文件放到 /mnt/tmp/<task_id>/，对外路径为 "
                "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/<task_id>/。"
            )
            logger.warning("[Feishu] Message resource download disabled for %s/%s: %s", message_id, file_key, warning)
            return FeishuResourceDownloadResult(warning=warning)

        request_types = [resource_type]
        if resource_type in {"audio", "media"}:
            request_types.append("file")

        first_warning = ""
        for request_type in request_types:
            try:
                request = self._build_message_resource_request(
                    message_id=message_id,
                    file_key=file_key,
                    resource_type=request_type,
                )
                response = await self._run_blocking(self._client.im.v1.message_resource.get, request)
                if not response or not response.success():
                    code = getattr(response, "code", "unknown")
                    msg = getattr(response, "msg", "request failed")
                    logger.debug(
                        "[Feishu] Resource download failed for %s/%s via type=%s: %s %s",
                        message_id,
                        file_key,
                        request_type,
                        code,
                        msg,
                    )
                    warning = _feishu_resource_warning(code, fallback_filename)
                    if warning and not first_warning:
                        first_warning = warning
                    continue

                content_type = self._get_response_header(response, "Content-Type")
                response_filename = getattr(response, "file_name", None) or ""
                filename = response_filename or fallback_filename or f"{request_type}_{file_key}"
                raw_bytes, oversized = self._read_binary_response(response, max_bytes=max_file_bytes)
                if oversized:
                    limit_text = f"{max_file_bytes} bytes" if max_file_bytes is not None else "当前网关配置"
                    warning = (
                        f"{_normalize_feishu_text(filename)}: 文件超过当前网关下载上限 "
                        f"{limit_text}；请提供 VM/NAS 路径，或拆分/压缩为较小文件。"
                        "中大文件请放到 /mnt/tmp/<task_id>/，对外路径为 "
                        "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/<task_id>/。"
                    )
                    logger.warning("[Feishu] Refusing oversized message resource %s/%s: %s", message_id, file_key, warning)
                    return FeishuResourceDownloadResult(warning=warning)
                if not raw_bytes:
                    continue
                media_type = self._normalize_media_type(
                    content_type,
                    default=self._guess_media_type_from_filename(filename),
                )

                if media_type.startswith("image/"):
                    ext = self._guess_extension(filename, content_type, ".jpg", allowed=_IMAGE_EXTENSIONS)
                    cached_path = cache_image_from_bytes(raw_bytes, ext=ext)
                    logger.info("[Feishu] Cached message image resource at %s", cached_path)
                    return FeishuResourceDownloadResult(cached_path, media_type or self._default_image_media_type(ext))

                if request_type == "audio" or media_type.startswith("audio/"):
                    ext = self._guess_extension(filename, content_type, ".ogg", allowed=_AUDIO_EXTENSIONS)
                    cached_path = cache_audio_from_bytes(raw_bytes, ext=ext)
                    logger.info("[Feishu] Cached message audio resource at %s", cached_path)
                    return FeishuResourceDownloadResult(cached_path, media_type or f"audio/{ext.lstrip('.') or 'ogg'}")

                if media_type.startswith("video/"):
                    if not Path(filename).suffix:
                        filename = f"{filename}.mp4"
                    cached_path = cache_document_from_bytes(raw_bytes, filename)
                    logger.info("[Feishu] Cached message video resource at %s", cached_path)
                    return FeishuResourceDownloadResult(cached_path, media_type)

                if not Path(filename).suffix and media_type in _DOCUMENT_MIME_TO_EXT:
                    filename = f"{filename}{_DOCUMENT_MIME_TO_EXT[media_type]}"
                cached_path = cache_document_from_bytes(raw_bytes, filename)
                logger.info("[Feishu] Cached message document resource at %s", cached_path)
                return FeishuResourceDownloadResult(cached_path, media_type or self._guess_document_media_type(filename))
            except Exception:
                logger.warning(
                    "[Feishu] Failed to cache message resource %s/%s",
                    message_id,
                    file_key,
                    exc_info=True,
                )
        return FeishuResourceDownloadResult(warning=first_warning)

    @staticmethod
    def _read_binary_response(response: Any, *, max_bytes: Optional[int] = None) -> tuple[bytes, bool]:
        file_obj = getattr(response, "file", None)
        if file_obj is None:
            return b"", False

        # max_bytes=None means no host-side size gate. max_bytes=0 is an
        # operator kill-switch: deny without touching the response stream.
        if max_bytes is not None and max_bytes <= 0:
            return b"", True

        limit = max_bytes

        # Prefer bounded chunked read() whenever available, even if getvalue()
        # exists. BytesIO.getvalue() materializes the whole payload, which is
        # exactly what the host-side gate must avoid for large Feishu files.
        if hasattr(file_obj, "read"):
            chunks: List[bytes] = []
            total = 0
            stream_position = None
            try:
                if hasattr(file_obj, "tell"):
                    stream_position = file_obj.tell()
                if hasattr(file_obj, "seek"):
                    file_obj.seek(0)
                while True:
                    read_size = _FEISHU_RESOURCE_READ_CHUNK_BYTES
                    if limit is not None:
                        read_size = max(1, min(read_size, limit + 1 - total))
                    try:
                        chunk = file_obj.read(read_size)
                    except TypeError:
                        if limit is not None:
                            # A stream that cannot honor bounded reads cannot
                            # be safely size-gated without an unbounded read.
                            return b"\0", True
                        chunk = file_obj.read()
                    if not chunk:
                        break
                    chunk_bytes = bytes(chunk)
                    chunks.append(chunk_bytes)
                    total += len(chunk_bytes)
                    if limit is not None and total > limit:
                        return b"".join(chunks)[: limit + 1], True
            finally:
                if stream_position is not None and hasattr(file_obj, "seek"):
                    try:
                        file_obj.seek(stream_position)
                    except Exception:
                        pass
            return b"".join(chunks), False

        # Fallback: getvalue() only when no read() exists. This is uncommon for
        # SDK responses; if it exceeds the limit, report oversized.
        if hasattr(file_obj, "getvalue"):
            data = bytes(file_obj.getvalue())
            if limit is not None and len(data) > limit:
                return data[: limit + 1], True
            return data, False

        return b"", False

    @staticmethod
    def _get_response_header(response: Any, name: str) -> str:
        headers = getattr(getattr(response, "raw", None), "headers", None)
        if not headers:
            return ""
        try:
            return str(headers.get(name) or headers.get(name.lower()) or "")
        except Exception:
            return ""

    @staticmethod
    def _guess_extension(filename: str, content_type: str, default: str, *, allowed: set[str]) -> str:
        ext = Path(filename or "").suffix.lower()
        if ext in allowed:
            return ext
        guessed = mimetypes.guess_extension((content_type or "").split(";", 1)[0].strip().lower() or "")
        if guessed in allowed:
            return guessed
        return default

    @staticmethod
    def _normalize_media_type(content_type: str, *, default: str) -> str:
        normalized = (content_type or "").split(";", 1)[0].strip().lower()
        return normalized or default

    @staticmethod
    def _guess_document_media_type(filename: str) -> str:
        ext = Path(filename or "").suffix.lower()
        return SUPPORTED_DOCUMENT_TYPES.get(ext, mimetypes.guess_type(filename or "")[0] or "application/octet-stream")

    @staticmethod
    def _display_name_from_cached_path(path: str) -> str:
        basename = os.path.basename(path)
        parts = basename.split("_", 2)
        display_name = parts[2] if len(parts) >= 3 else basename
        return re.sub(r"[^\w.\- ]", "_", display_name)

    @staticmethod
    def _guess_media_type_from_filename(filename: str) -> str:
        guessed = (mimetypes.guess_type(filename or "")[0] or "").lower()
        if guessed:
            return guessed
        ext = Path(filename or "").suffix.lower()
        if ext in _VIDEO_EXTENSIONS:
            return f"video/{ext.lstrip('.')}"
        if ext in _AUDIO_EXTENSIONS:
            return f"audio/{ext.lstrip('.')}"
        if ext in _IMAGE_EXTENSIONS:
            return FeishuAdapter._default_image_media_type(ext)
        return ""

    @staticmethod
    def _map_chat_type(raw_chat_type: str) -> str:
        normalized = (raw_chat_type or "").strip().lower()
        if normalized == "p2p":
            return "dm"
        if "topic" in normalized or "thread" in normalized or "forum" in normalized:
            return "forum"
        if normalized == "group":
            return "group"
        return "dm"

    @staticmethod
    def _resolve_source_chat_type(*, chat_info: Dict[str, Any], event_chat_type: str) -> str:
        resolved = str(chat_info.get("type") or "").strip().lower()
        if resolved in {"group", "forum"}:
            return resolved
        if event_chat_type == "p2p":
            return "dm"
        return "group"

    @staticmethod
    def _build_topic_thread_id(anchor: Optional[str]) -> Optional[str]:
        normalized = str(anchor or "").strip()
        if not normalized:
            return None
        return f"{_FEISHU_TOPIC_THREAD_PREFIX}{normalized}"

    @staticmethod
    def _topic_anchor_from_thread_id(thread_id: Optional[str]) -> Optional[str]:
        normalized = str(thread_id or "").strip()
        if not normalized.startswith(_FEISHU_TOPIC_THREAD_PREFIX):
            return None
        anchor = normalized[len(_FEISHU_TOPIC_THREAD_PREFIX):].strip()
        return anchor or None

    def _resolve_source_thread_id(
        self,
        *,
        message: Any,
        message_id: str,
        source_chat_type: str,
    ) -> Optional[str]:
        if source_chat_type == "dm":
            return getattr(message, "thread_id", None) or None
        anchor = (
            getattr(message, "root_id", None)
            or getattr(message, "upper_message_id", None)
            or getattr(message, "parent_id", None)
            or getattr(message, "message_id", None)
            or message_id
            or None
        )
        return self._build_topic_thread_id(anchor)

    def _resolve_reply_target(
        self,
        *,
        reply_to: Optional[str],
        metadata: Optional[Dict[str, Any]],
    ) -> tuple[Optional[str], bool]:
        thread_id = str((metadata or {}).get("thread_id") or "").strip() or None
        topic_anchor = self._topic_anchor_from_thread_id(thread_id)
        metadata_reply_to = str((metadata or {}).get("reply_to_message_id") or "").strip() or None
        effective_reply_to = reply_to or metadata_reply_to or topic_anchor
        reply_in_thread = bool(thread_id)
        return effective_reply_to, reply_in_thread

    @staticmethod
    def _message_idempotency_uuid(metadata: Optional[Dict[str, Any]]) -> str:
        value = str((metadata or {}).get("idempotency_uuid") or "").strip()
        if not value:
            return str(uuid.uuid4())
        if len(value) > 50 or not re.fullmatch(r"[A-Za-z0-9._:-]+", value):
            raise ValueError("Feishu idempotency_uuid must be 1-50 safe ASCII characters")
        return value

    async def _resolve_sender_profile(
        self,
        sender_id: Any,
        *,
        is_bot: bool = False,
    ) -> Dict[str, Optional[str]]:
        """Map Feishu's three-tier user IDs onto Hermes' SessionSource fields.

        For normal users, ``open_id`` remains primary to preserve the local
        session/authz contract; ``user_id`` is the fallback. Bot profiles use
        tenant-scoped ``user_id`` first when Feishu provides it.

        ``user_id_alt`` carries the union_id (developer-scoped, stable across
        all apps by the same developer).  Session-key generation prefers
        user_id_alt when present, so participant isolation stays stable even
        if the primary ID is the app-scoped open_id.
        """
        open_id = getattr(sender_id, "open_id", None) or None
        user_id = getattr(sender_id, "user_id", None) or None
        union_id = getattr(sender_id, "union_id", None) or None
        # For normal inbound events, preserve the local invariant: the
        # app-scoped open_id is the primary Hermes identity.  For bot profile
        # resolution, keep tenant user_id primary when Feishu provides it while
        # still using open_id for the bot-name API below.
        primary_id = (user_id or open_id) if is_bot else (open_id or user_id)
        # bot/v3/bots/basic_batch only accepts open_id.
        name_lookup_id = open_id if is_bot else (primary_id or union_id)
        display_name = await self._resolve_sender_name_from_api(
            name_lookup_id, is_bot=is_bot,
        )
        return {
            "user_id": primary_id,
            "user_name": display_name,
            "user_id_alt": union_id,
        }

    def _get_cached_sender_name(self, sender_id: Optional[str]) -> Optional[str]:
        """Return a cached sender name only while its TTL is still valid."""
        if not sender_id:
            return None
        cached = self._sender_name_cache.get(sender_id)
        if cached is None:
            return None
        name, expire_at = cached
        if time.time() < expire_at:
            return name
        self._sender_name_cache.pop(sender_id, None)
        return None

    async def _resolve_sender_name_from_api(
        self,
        sender_id: Optional[str],
        *,
        is_bot: bool = False,
    ) -> Optional[str]:
        """Bots divert to bot/basic_batch — contact API doesn't return bot names.
        Failures are silent so the pipeline never blocks on name resolution.
        """
        if not sender_id or not self._client:
            return None
        trimmed = sender_id.strip()
        if not trimmed:
            return None
        now = time.time()
        cached_name = self._get_cached_sender_name(trimmed)
        if cached_name is not None:
            return cached_name or None  # "" cached means "known nameless"
        if is_bot:
            names = await self._fetch_bot_names([trimmed])
            if names is None:
                return None
            expire_at = now + _FEISHU_SENDER_NAME_TTL_SECONDS
            for oid, name in names.items():
                self._sender_name_cache[oid] = (name, expire_at)
            hit = self._sender_name_cache.get(trimmed)
            return (hit[0] or None) if hit else None
        try:
            from lark_oapi.api.contact.v3 import GetUserRequest  # lazy import
            if trimmed.startswith("ou_"):
                id_type = "open_id"
            elif trimmed.startswith("on_"):
                id_type = "union_id"
            else:
                id_type = "user_id"
            request = GetUserRequest.builder().user_id(trimmed).user_id_type(id_type).build()
            response = await self._run_blocking(self._client.contact.v3.user.get, request)
            if not response or not response.success():
                return None
            user = getattr(getattr(response, "data", None), "user", None)
            name = (
                getattr(user, "name", None)
                or getattr(user, "display_name", None)
                or getattr(user, "nickname", None)
                or getattr(user, "en_name", None)
            )
            if name and isinstance(name, str):
                name = name.strip()
                if name:
                    self._sender_name_cache[trimmed] = (name, now + _FEISHU_SENDER_NAME_TTL_SECONDS)
                    return name
        except Exception:
            logger.debug("[Feishu] Failed to resolve sender name for %s", sender_id, exc_info=True)
        return None

    async def _fetch_bot_names(self, bot_ids: List[str]) -> Optional[Dict[str, str]]:
        if not self._client or not bot_ids:
            return None
        try:
            req = (
                BaseRequest.builder()
                .http_method(HttpMethod.GET)
                .uri("/open-apis/bot/v3/bots/basic_batch")
                .queries([("bot_ids", oid) for oid in bot_ids])
                .token_types({AccessTokenType.TENANT})
                .build()
            )
            resp = await self._run_blocking(self._client.request, req)
            content = getattr(getattr(resp, "raw", None), "content", None)
            if not content:
                return None
            payload = json.loads(content)
            if payload.get("code") != 0:
                return None
            bots = (payload.get("data") or {}).get("bots") or {}
            return {
                oid: str(info.get("name") or "").strip()
                for oid, info in bots.items()
                if oid
            }
        except Exception:
            logger.debug("[Feishu] Failed to fetch bot names for %s", bot_ids, exc_info=True)
            return None

    async def _fetch_message_text(self, message_id: str) -> Optional[str]:
        if not message_id:
            return None
        if not getattr(self, "_client", None):
            raise _FeishuReplyContextUnavailable(
                f"Feishu client unavailable for parent message {message_id}"
            )
        if message_id in self._message_text_cache:
            self._message_text_cache.move_to_end(message_id)
            return self._message_text_cache[message_id]
        try:
            request = self._build_get_message_request(message_id)
            response = await self._run_blocking(self._client.im.v1.message.get, request)
            if not response or getattr(response, "success", lambda: False)() is False:
                code = getattr(response, "code", "unknown")
                msg = getattr(response, "msg", "message lookup failed")
                logger.warning("[Feishu] Failed to fetch parent message %s: [%s] %s", message_id, code, msg)
                raise _FeishuReplyContextUnavailable(
                    f"Feishu parent lookup failed for {message_id}: {code}"
                )
            items = getattr(getattr(response, "data", None), "items", None) or []
            parent = items[0] if items else None
            if parent is None:
                raise _FeishuReplyContextUnavailable(
                    f"Feishu parent lookup returned no item for {message_id}"
                )
            body = getattr(parent, "body", None)
            msg_type = getattr(parent, "msg_type", "") or ""
            raw_content = getattr(body, "content", "") or ""
            parent_mentions = getattr(parent, "mentions", None) if parent else None
            normalized = normalize_feishu_message(
                message_type=msg_type,
                raw_content=raw_content,
                mentions=parent_mentions,
                bot=self._bot_identity(),
            )
            text = normalized.text_content
            if not text:
                text = str(normalized.metadata.get("placeholder_text") or "").strip() or None
            issue_links: list[str] = []
            raw_links = normalized.metadata.get("link_urls")
            if isinstance(raw_links, list):
                for raw_link in raw_links:
                    match = re.match(
                        r"^https?://project\.feishu\.cn/[^\s/?#)]+/issue/detail/\d+",
                        str(raw_link or "").strip(),
                        re.IGNORECASE,
                    )
                    if match:
                        canonical_link = match.group(0).rstrip("/")
                        if canonical_link not in issue_links:
                            issue_links.append(canonical_link)
            if issue_links:
                text = "\n".join([str(text or "").strip(), *issue_links]).strip()
            self._message_text_cache[message_id] = text
            while len(self._message_text_cache) > _FEISHU_MESSAGE_TEXT_CACHE_SIZE:
                self._message_text_cache.popitem(last=False)
            return text
        except _FeishuReplyContextUnavailable:
            raise
        except Exception as exc:
            logger.warning("[Feishu] Failed to fetch parent message %s", message_id, exc_info=True)
            raise _FeishuReplyContextUnavailable(
                f"Feishu parent lookup raised for {message_id}"
            ) from exc

    def _extract_text_from_raw_content(
        self,
        *,
        msg_type: str,
        raw_content: str,
        mentions: Optional[Sequence[Any]] = None,
    ) -> Optional[str]:
        normalized = normalize_feishu_message(
            message_type=msg_type,
            raw_content=raw_content,
            mentions=mentions,
            bot=self._bot_identity(),
        )
        if normalized.text_content:
            return normalized.text_content
        placeholder = normalized.metadata.get("placeholder_text") if isinstance(normalized.metadata, dict) else None
        return str(placeholder).strip() or None

    @staticmethod
    def _default_image_media_type(ext: str) -> str:
        normalized_ext = (ext or "").lower()
        if normalized_ext in {".jpg", ".jpeg"}:
            return "image/jpeg"
        return f"image/{normalized_ext.lstrip('.') or 'jpeg'}"

    @staticmethod
    def _log_background_failure(future: Any) -> None:
        try:
            future.result()
        except Exception:
            logger.exception("[Feishu] Background inbound processing failed")

    # =========================================================================
    # Inbound admission
    # =========================================================================

    def _admit(self, sender: Any, message: Any) -> Optional[RejectReason]:
        sender_ids = _sender_identity(sender)
        self_ids = frozenset(v for v in (self._bot_open_id, self._bot_user_id) if v)
        is_bot = _is_bot_sender(sender)
        is_group = getattr(message, "chat_type", "p2p") != "p2p"
        chat_id = getattr(message, "chat_id", "") or ""
        require_mention = is_group and self._require_mention_for(chat_id)

        # Defensive only — Feishu doesn't echo our outbound back as inbound,
        # and open_id is always populated on both sides.
        if self_ids and sender_ids & self_ids:
            return "self_echo"

        if is_bot:
            mode = self._allow_bots
            if mode != "mentions" and mode != "all":
                return "bots_disabled"
            # Defensive: pre-hydration or malformed payloads.
            if not self_ids or not sender_ids:
                return "self_ids_unknown"
            # Step 4 covers mention enforcement for groups when require_mention
            # is on; check here only on paths step 4 won't reach.
            if mode == "mentions" and not require_mention and not self._mentions_self(message):
                return "bot_not_mentioned"

        if not is_group:
            if os.getenv("FEISHU_ALLOW_ALL_USERS", "").strip().lower() in {"true", "1", "yes"}:
                return None
            if os.getenv("GATEWAY_ALLOW_ALL_USERS", "").strip().lower() in {"true", "1", "yes"}:
                return None
            # Empty FEISHU_ALLOWED_USERS is the pairing-mode default from setup:
            # forward DMs to gateway intake so the pairing handshake can run.
            # Gateway auth fail-closes agent access until approval.
            if not self._allowed_group_users:
                return None
            if not (sender_ids and (sender_ids & self._allowed_group_users)):
                return "dm_policy_rejected"
            return None

        if not self._allow_group_message(
            getattr(sender, "sender_id", None), chat_id, is_bot=is_bot,
        ):
            return "group_policy_rejected"
        # Groups still need a real @mention when mention gating is enabled, even
        # for normal user senders. Previously only bot senders reached the
        # bot_not_mentioned path, so human messages without a parsed self-mention
        # were folded into the generic group_policy_rejected bucket. That made
        # live RC debugging look like the message never reached the gateway,
        # while the actual cause was mention-gate failure. Surface the precise
        # reason so operators can distinguish ingress failure from mention mismatch.
        if require_mention and not self._mentions_self(message):
            return "bot_not_mentioned"
        return None

    def _require_mention_for(self, chat_id: str) -> bool:
        group_rules = getattr(self, "_group_rules", {})
        rule = group_rules.get(chat_id) if chat_id else None
        if rule and rule.require_mention is not None:
            return rule.require_mention
        return bool(getattr(self, "_require_mention", False))

    # --- Group policy ---------------------------------------------------------

    def _allow_group_message(
        self,
        sender_id: Any,
        chat_id: str = "",
        *,
        is_bot: bool = False,
    ) -> bool:
        """Per-group policy gate for non-DM traffic."""
        sender_open_id = getattr(sender_id, "open_id", None)
        sender_user_id = getattr(sender_id, "user_id", None)
        sender_ids = {sender_open_id, sender_user_id} - {None}

        if sender_ids and self._admins and (sender_ids & self._admins):
            return True

        rule = self._group_rules.get(chat_id) if chat_id else None
        if rule:
            policy = rule.policy
            allowlist = rule.allowlist
            blacklist = rule.blacklist
        else:
            policy = self._default_group_policy or self._group_policy
            allowlist = self._allowed_group_users
            blacklist = set()

        # Channel locks apply to everyone; allowlist/blacklist only gate humans
        # (bots were already cleared upstream by FEISHU_ALLOW_BOTS).
        if policy == "disabled":
            return False
        if policy == "open":
            return True
        if policy == "admin_only":
            return False
        if is_bot:
            return True

        if policy == "allowlist":
            return bool(sender_ids and (sender_ids & allowlist))
        if policy == "blacklist":
            return bool(sender_ids and not (sender_ids & blacklist))

        return bool(sender_ids and (sender_ids & self._allowed_group_users))

    # --- Mention detection ----------------------------------------------------

    def _should_accept_group_message(self, message: Any, sender_id: Any, chat_id: str = "") -> bool:
        """Backward-compatible group gate used by tests and older call sites."""
        if not self._allow_group_message(sender_id, chat_id):
            return False
        return self._mentions_self(message)

    def _mentions_self(self, message: Any) -> bool:
        raw_content = getattr(message, "content", "") or ""
        mentions = getattr(message, "mentions", None) or []
        if mentions and self._message_mentions_bot(mentions):
            return True
        normalized = normalize_feishu_message(
            message_type=getattr(message, "message_type", "") or "",
            raw_content=raw_content,
            mentions=getattr(message, "mentions", None),
            bot=self._bot_identity(),
        )
        return self._post_mentions_bot(normalized.mentions)

    def _message_mentions_bot(self, mentions: List[Any]) -> bool:
        identity = self._bot_identity()
        for mention in mentions:
            mention_open_id, mention_user_id = _extract_mention_ids(mention)
            mention_name = (getattr(mention, "name", None) or "").strip()
            if identity.matches(
                open_id=mention_open_id,
                user_id=mention_user_id,
                name=mention_name,
            ):
                return True

        return False

    def _post_mentions_bot(self, mentions: Sequence[Any]) -> bool:
        identity = self._bot_identity()
        for mention in mentions:
            if isinstance(mention, FeishuMentionRef):
                if mention.is_self:
                    return True
                if identity.matches(
                    open_id=mention.open_id,
                    user_id="",
                    name=mention.name,
                ):
                    return True
                continue
            if isinstance(mention, str):
                mention_id = mention.strip()
                if mention_id and mention_id in {
                    value
                    for value in (self._bot_open_id, self._bot_user_id)
                    if value
                }:
                    return True
        return False

    def _bot_identity(self) -> _FeishuBotIdentity:
        return _FeishuBotIdentity(
            open_id=self._bot_open_id,
            user_id=self._bot_user_id,
            name=self._bot_name,
        )

    async def _hydrate_bot_identity(self) -> None:
        """Best-effort discovery of bot identity for precise group mention gating
        and self-sent bot event filtering.

        Populates ``_bot_open_id`` and ``_bot_name`` from /open-apis/bot/v3/info
        (no extra scopes required beyond the tenant access token). The probe
        always runs when a client is available so stale env vars from app/bot
        migrations do not break group @mention gating. Falls back to the
        application info endpoint for ``_bot_name`` only when the first probe
        doesn't return it. If the probe fails, env-provided values are preserved.
        """
        if not self._client:
            return

        # Primary probe: /open-apis/bot/v3/info — returns bot_name + open_id, no
        # extra scopes required. This is the same endpoint the onboarding wizard
        # uses via probe_bot().
        try:
            req = (
                BaseRequest.builder()
                .http_method(HttpMethod.GET)
                .uri("/open-apis/bot/v3/info")
                .token_types({AccessTokenType.TENANT})
                .build()
            )
            resp = await self._run_blocking(self._client.request, req)
            content = getattr(getattr(resp, "raw", None), "content", None)
            if content:
                payload = json.loads(content)
                parsed = _parse_bot_response(payload) or {}
                open_id = (parsed.get("bot_open_id") or "").strip()
                bot_name = (parsed.get("bot_name") or "").strip()
                if open_id:
                    if self._bot_open_id and self._bot_open_id != open_id:
                        logger.warning(
                            "[Feishu] FEISHU_BOT_OPEN_ID is stale; using /bot/v3/info open_id for group @mention gating."
                        )
                    self._bot_open_id = open_id
                if bot_name:
                    if self._bot_name and self._bot_name != bot_name:
                        logger.info(
                            "[Feishu] FEISHU_BOT_NAME differs from /bot/v3/info; using hydrated bot name for group @mention gating."
                        )
                    self._bot_name = bot_name
        except Exception:
            logger.debug(
                "[Feishu] /bot/v3/info probe failed during hydration",
                exc_info=True,
            )

        # Fallback probe for _bot_name only: application info endpoint. Needs
        # admin:app.info:readonly or application:application:self_manage scope,
        # so it's best-effort.
        if self._bot_name:
            return
        try:
            request = self._build_get_application_request(app_id=self._app_id, lang="en_us")
            response = await self._run_blocking(self._client.application.v6.application.get, request)
            if not response or not response.success():
                code = getattr(response, "code", None)
                if code == 99991672:
                    logger.warning(
                        "[Feishu] Unable to hydrate bot name from application info. "
                        "Grant admin:app.info:readonly or application:application:self_manage "
                        "so group @mention gating can resolve the bot name precisely."
                    )
                return
            app = getattr(getattr(response, "data", None), "app", None)
            app_name = (getattr(app, "app_name", None) or "").strip()
            if app_name and not self._bot_name:
                self._bot_name = app_name
        except Exception:
            logger.debug("[Feishu] Failed to hydrate bot name from application info", exc_info=True)

    # =========================================================================
    # Deduplication — seen message ID cache (persistent)
    # =========================================================================

    def _load_api_poll_state(self, raw_state: Any) -> bool:
        if not isinstance(raw_state, dict):
            self._api_poll_state_error = "api_poll state must be an object"
            self._api_poll_raw_state = raw_state
            logger.error("[Feishu] Persisted API poll state is invalid")
            return False
        try:
            raw_pending = raw_state.get("pending", {})
            if not isinstance(raw_pending, dict):
                raise ValueError("api_poll.pending must be an object")
            pending: Dict[str, List[Dict[str, Any]]] = {}
            for raw_chat_id, raw_items in raw_pending.items():
                chat_id = str(raw_chat_id or "").strip()
                if not chat_id or not isinstance(raw_items, list):
                    raise ValueError("api_poll pending chat entry is invalid")
                if len(raw_items) > _MAX_API_POLL_PENDING_PER_CHAT:
                    raise ValueError("api_poll pending chat exceeds capacity")
                normalized = [
                    self._validated_api_poll_pending_item(
                        item,
                        expected_chat_id=chat_id,
                    )
                    for item in raw_items
                ]
                message_ids = [
                    str(item.get("message_id") or "").strip()
                    for item in normalized
                ]
                if len(message_ids) != len(set(message_ids)):
                    raise ValueError("api_poll pending message IDs are duplicated")
                normalized.sort(
                    key=lambda value: (
                        self._api_poll_item_create_time_ms(value) or 0,
                        str(value.get("message_id") or ""),
                    )
                )
                if normalized:
                    pending[chat_id] = normalized

            pending_bytes = len(
                json.dumps(
                    pending,
                    ensure_ascii=False,
                    separators=(",", ":"),
                ).encode("utf-8")
            )
            if pending_bytes > _MAX_API_POLL_TOTAL_BYTES:
                raise ValueError("api_poll pending state exceeds byte capacity")

            raw_baselined = raw_state.get("baselined_chat_ids", [])
            raw_seen = raw_state.get("seen_message_ids", [])
            raw_cursors = raw_state.get("last_seen_create_time_ms", {})
            raw_cursor_ids = raw_state.get("cursor_message_ids", {})
            raw_floors = raw_state.get("discovery_floor_ms", {})
            raw_scan_state = raw_state.get("scan_state", {})
            raw_holes = raw_state.get("terminal_holes", [])
            if not isinstance(raw_baselined, list) or not isinstance(raw_seen, list):
                raise ValueError("api_poll list state is invalid")
            if (
                not isinstance(raw_cursors, dict)
                or not isinstance(raw_cursor_ids, dict)
                or not isinstance(raw_floors, dict)
                or not isinstance(raw_scan_state, dict)
                or not isinstance(raw_holes, list)
            ):
                raise ValueError("api_poll cursor state is invalid")
            baselined = {
                str(value).strip()
                for value in raw_baselined
                if str(value or "").strip()
            }
            seen_order = [
                str(value).strip()
                for value in raw_seen
                if str(value or "").strip()
            ]
            seen_order = list(dict.fromkeys(seen_order))[
                -max(1, int(getattr(self, "_dedup_cache_size", 1000) or 1000)):
            ]
            seen = set(seen_order)
            cursors: Dict[str, int] = {}
            for raw_chat_id, raw_cursor in raw_cursors.items():
                chat_id = str(raw_chat_id or "").strip()
                if (
                    not chat_id
                    or isinstance(raw_cursor, bool)
                    or not isinstance(raw_cursor, int)
                    or raw_cursor < 0
                ):
                    raise ValueError("api_poll cursor entry is invalid")
                cursors[chat_id] = raw_cursor
            cursor_ids: Dict[str, set[str]] = {}
            for raw_chat_id, raw_ids in raw_cursor_ids.items():
                chat_id = str(raw_chat_id or "").strip()
                if not chat_id or not isinstance(raw_ids, list):
                    raise ValueError("api_poll cursor message IDs are invalid")
                ids = {
                    str(value).strip()
                    for value in raw_ids
                    if str(value or "").strip()
                }
                if ids:
                    cursor_ids[chat_id] = ids
            floors: Dict[str, int] = {}
            for raw_chat_id, raw_floor in raw_floors.items():
                chat_id = str(raw_chat_id or "").strip()
                if (
                    not chat_id
                    or isinstance(raw_floor, bool)
                    or not isinstance(raw_floor, int)
                    or raw_floor < 0
                ):
                    raise ValueError("api_poll discovery floor entry is invalid")
                floors[chat_id] = raw_floor
            scan_state: Dict[str, Dict[str, Any]] = {}
            for raw_chat_id, raw_scan in raw_scan_state.items():
                chat_id = str(raw_chat_id or "").strip()
                if not chat_id:
                    raise ValueError("api_poll scan chat identity is invalid")
                scan_state[chat_id] = self._validated_api_poll_scan_state(
                    chat_id,
                    raw_scan,
                )
            holes: List[Dict[str, Any]] = []
            for raw_hole in raw_holes[-_MAX_API_POLL_TERMINAL_HOLES:]:
                if not isinstance(raw_hole, dict):
                    raise ValueError("api_poll terminal hole is invalid")
                holes.append(
                    {
                        "schema_version": "feishu_api_poll_terminal_hole_v1",
                        "kind": str(raw_hole.get("kind") or "unknown")[:64],
                        "status": str(raw_hole.get("status") or "terminal")[:64],
                        "message_id": str(raw_hole.get("message_id") or "")[:512],
                        "chat_id": str(raw_hole.get("chat_id") or "")[:512],
                        "create_time": str(raw_hole.get("create_time") or "")[:64],
                        "sender_id": str(raw_hole.get("sender_id") or "")[:512],
                        "payload_sha256": str(
                            raw_hole.get("payload_sha256") or ""
                        )[:64],
                        "original_message_id": self._bounded_api_poll_field(
                            raw_hole.get("original_message_id"), 512
                        ),
                        "error": str(raw_hole.get("error") or "")[:512],
                        "admission_item_id": str(
                            raw_hole.get("admission_item_id") or ""
                        )[:128],
                        "recorded_at": float(raw_hole.get("recorded_at") or 0.0),
                    }
                )
            baselined.update(pending)
            baselined.update(scan_state)
        except (TypeError, ValueError) as exc:
            self._api_poll_state_error = f"{type(exc).__name__}: {exc}"
            self._api_poll_raw_state = raw_state
            logger.error(
                "[Feishu] Persisted API poll state is invalid: %s",
                exc,
            )
            return False

        self._api_poll_pending_items = pending
        self._api_poll_baselined_chat_ids = baselined
        self._api_poll_seen_message_ids = seen
        self._api_poll_seen_message_order = seen_order
        self._api_poll_last_seen_create_time_ms = cursors
        self._api_poll_cursor_message_ids = cursor_ids
        self._api_poll_discovery_floor_ms = floors
        self._api_poll_scan_state = scan_state
        self._api_poll_terminal_holes = holes
        normalized_state = self._api_poll_state_snapshot()
        normalized_payload = self._api_poll_sidecar_payload(
            normalized_state,
            revision=max(0, self._api_poll_revision),
        )
        try:
            self._validate_api_poll_capacity(normalized_state, normalized_payload)
        except ValueError as exc:
            self._api_poll_state_error = f"ValueError: {exc}"
            self._api_poll_raw_state = raw_state
            logger.error("[Feishu] Persisted API poll state exceeds capacity: %s", exc)
            return False
        return True

    def _load_api_poll_sidecar(self) -> bool:
        """Load the independent ownership sidecar; return whether it existed."""
        try:
            if self._api_poll_state_path.stat().st_size > _MAX_API_POLL_TOTAL_BYTES:
                raise ValueError("API poll sidecar exceeds 16 MiB")
            payload = json.loads(self._api_poll_state_path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return False
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            self._api_poll_state_error = (
                f"API poll sidecar unreadable: {type(exc).__name__}"
            )
            self._api_poll_block_persistence = True
            logger.error(
                "[Feishu] Failed to load API poll sidecar from %s",
                self._api_poll_state_path,
                exc_info=True,
            )
            return True
        try:
            if not isinstance(payload, dict):
                raise ValueError("API poll sidecar top level must be an object")
            if payload.get("schema_version") != _API_POLL_SIDECAR_SCHEMA:
                raise ValueError("API poll sidecar schema is unsupported")
            if payload.get("app_scope") != self._api_poll_app_scope:
                raise ValueError("API poll sidecar app scope does not match")
            revision = payload.get("revision")
            if (
                isinstance(revision, bool)
                or not isinstance(revision, int)
                or revision < 0
            ):
                raise ValueError("API poll sidecar revision is invalid")
            self._api_poll_revision = revision
            if not self._load_api_poll_state(payload.get("state")):
                raise ValueError(self._api_poll_state_error or "API poll state is invalid")
        except (TypeError, ValueError) as exc:
            self._api_poll_state_error = f"{type(exc).__name__}: {exc}"
            self._api_poll_raw_state = payload
            self._api_poll_block_persistence = True
            logger.error("[Feishu] Persisted API poll sidecar is invalid: %s", exc)
            return True
        self._api_poll_sidecar_initialized = True
        return True

    def _load_seen_message_ids(self) -> None:
        payload: Any = None
        try:
            if self._dedup_state_path.stat().st_size > _MAX_FEISHU_INBOX_STATE_BYTES:
                self._api_poll_state_error = "persistent inbox exceeds 16 MiB"
                self._api_poll_block_persistence = True
                logger.error(
                    "[Feishu] Refusing oversized persisted inbox at %s",
                    self._dedup_state_path,
                )
            else:
                payload = json.loads(
                    self._dedup_state_path.read_text(encoding="utf-8")
                )
        except FileNotFoundError:
            payload = None
        except (OSError, json.JSONDecodeError) as exc:
            self._api_poll_state_error = f"persistent inbox unreadable: {type(exc).__name__}"
            self._api_poll_block_persistence = True
            logger.warning("[Feishu] Failed to load persisted dedup state from %s", self._dedup_state_path, exc_info=True)
            payload = None
        if payload is not None and not isinstance(payload, dict):
            self._api_poll_state_error = "persistent inbox top level must be an object"
            self._api_poll_block_persistence = True
            logger.error(
                "[Feishu] Persisted inbox top level is invalid at %s",
                self._dedup_state_path,
            )
            payload = None

        sidecar_exists = self._load_api_poll_sidecar()
        if (
            not sidecar_exists
            and isinstance(payload, dict)
            and "api_poll" in payload
            and self._api_poll_state_error is None
        ):
            if self._load_api_poll_state(payload.get("api_poll")):
                try:
                    self._persist_api_poll_state(require_success=True)
                    logger.info(
                        "[Feishu] Migrated embedded API poll ownership to %s",
                        self._api_poll_state_path,
                    )
                except RuntimeError as exc:
                    self._api_poll_state_error = (
                        f"API poll sidecar migration failed: {type(exc).__name__}"
                    )
                    self._api_poll_block_persistence = True
                    logger.error(
                        "[Feishu] Failed to migrate embedded API poll ownership",
                        exc_info=True,
                    )

        if not isinstance(payload, dict):
            return
        seen_data: Any = {}
        if isinstance(payload.get("messages"), dict):
            # v2 is a minimal persistent inbox. Only completed entries are
            # deduplicated after restart; processing entries represent a crash
            # window and are deliberately recoverable by redelivery.
            seen_data = {
                key: value.get("updated_at")
                for key, value in payload["messages"].items()
                if isinstance(value, dict) and value.get("status") == "completed"
            }
        else:
            seen_data = payload.get("message_ids", {})
        now = time.time()
        ttl = _FEISHU_DEDUP_TTL_SECONDS
        # Backward-compat: old format stored a plain list of IDs (no timestamps).
        if isinstance(seen_data, list):
            entries: Dict[str, float] = {str(item).strip(): 0.0 for item in seen_data if str(item).strip()}
        elif isinstance(seen_data, dict):
            entries = {}
            for key, value in seen_data.items():
                if not isinstance(key, str) or not key.strip():
                    continue
                try:
                    entries[key] = float(value)
                except (TypeError, ValueError):
                    continue
        else:
            return
        # Filter out TTL-expired entries (entries saved with ts=0.0 are treated as immortal
        # for one migration cycle to avoid nuking old data on first upgrade).
        valid: Dict[str, float] = {
            msg_id: ts for msg_id, ts in entries.items()
            if ts == 0.0 or ttl <= 0 or now - ts < ttl
        }
        # Apply size cap; keep the most recently seen IDs.
        sorted_ids = sorted(valid, key=lambda k: valid[k], reverse=True)[:self._dedup_cache_size]
        self._seen_message_order = list(reversed(sorted_ids))
        self._seen_message_ids = {k: valid[k] for k in sorted_ids}

    def _persist_seen_message_ids(self, *, require_success: bool = False) -> bool:
        if getattr(self, "_api_poll_block_persistence", False):
            logger.error(
                "[Feishu] Persistent inbox writes are blocked after an unsafe load"
            )
            if require_success:
                raise RuntimeError("Feishu persistent inbox is unhealthy")
            return False
        try:
            self._dedup_state_path.parent.mkdir(parents=True, exist_ok=True)
            recent = self._seen_message_order[-self._dedup_cache_size:]
            completed = {
                key: self._seen_message_ids[key]
                for key in recent
                if key in self._seen_message_ids
            }
            messages = {
                key: {"status": "completed", "updated_at": timestamp}
                for key, timestamp in completed.items()
            }
            messages.update(
                {
                    key: {"status": "processing", "updated_at": timestamp}
                    for key, timestamp in self._processing_message_ids.items()
                }
            )
            payload = {
                "schema_version": "feishu_message_inbox_v2",
                "messages": messages,
                # Rollback compatibility for older binaries.
                "message_ids": completed,
            }
            encoded_payload = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            if len(encoded_payload) > _MAX_FEISHU_INBOX_STATE_BYTES:
                raise ValueError(
                    "Feishu persistent inbox exceeds 16 MiB byte capacity"
                )
            atomic_json_write(self._dedup_state_path, payload, indent=None)
            return True
        except (OSError, TypeError, ValueError) as exc:
            logger.warning("[Feishu] Failed to persist dedup state to %s", self._dedup_state_path, exc_info=True)
            if require_success:
                raise RuntimeError("Feishu persistent inbox write failed") from exc
            return False

    def _begin_message_processing(self, message_id: str) -> bool:
        """Persist processing before callback work without marking completion."""
        now = time.time()
        ttl = _FEISHU_DEDUP_TTL_SECONDS
        with self._dedup_lock:
            seen_at = self._seen_message_ids.get(message_id)
            if seen_at is not None and (ttl <= 0 or now - seen_at < ttl):
                return False
            if seen_at is not None:
                self._seen_message_ids.pop(message_id, None)
                self._seen_message_order = [
                    item for item in self._seen_message_order if item != message_id
                ]
            if message_id in self._processing_message_ids:
                return False
            self._processing_message_ids[message_id] = now
            self._persist_seen_message_ids()
            return True

    def _complete_message_processing(self, message_id: str) -> None:
        now = time.time()
        with self._dedup_lock:
            self._processing_message_ids.pop(message_id, None)
            self._seen_message_ids[message_id] = now
            if message_id in self._seen_message_order:
                self._seen_message_order.remove(message_id)
            self._seen_message_order.append(message_id)
            while len(self._seen_message_order) > self._dedup_cache_size:
                stale = self._seen_message_order.pop(0)
                self._seen_message_ids.pop(stale, None)
            self._persist_seen_message_ids()

    def _message_processing_completed(self, message_id: str) -> bool:
        now = time.time()
        ttl = _FEISHU_DEDUP_TTL_SECONDS
        with self._dedup_lock:
            seen_at = self._seen_message_ids.get(message_id)
            return bool(
                seen_at is not None and (ttl <= 0 or now - seen_at < ttl)
            )

    def _abandon_message_processing(self, message_id: str) -> None:
        with self._dedup_lock:
            if self._processing_message_ids.pop(message_id, None) is not None:
                self._persist_seen_message_ids()

    def _is_duplicate(self, message_id: str) -> bool:
        """Legacy atomic check-and-complete helper for older call sites/tests."""
        if not self._begin_message_processing(message_id):
            return True
        self._complete_message_processing(message_id)
        return False

    def _claim_message_id(self, message_id: str, *, durable: bool) -> bool:
        """Compatibility helper for pre-v0.18.2 callers and focused tests."""
        now = time.time()
        ttl = _FEISHU_DEDUP_TTL_SECONDS
        with self._dedup_lock:
            seen_at = self._seen_message_ids.get(message_id)
            if seen_at is not None and (ttl <= 0 or now - seen_at < ttl):
                return True
            if message_id in self._pending_dedup_message_ids:
                return True
            if seen_at is not None:
                self._seen_message_ids.pop(message_id, None)
                self._seen_message_order = [
                    item for item in self._seen_message_order if item != message_id
                ]
            if not durable:
                self._pending_dedup_message_ids.add(message_id)
                return False
            self._record_seen_message_id_locked(message_id, now)
            return False

    def _record_seen_message_id_locked(self, message_id: str, seen_at: float) -> None:
        # Caller holds _dedup_lock.
        self._seen_message_ids[message_id] = seen_at
        self._seen_message_order.append(message_id)
        while len(self._seen_message_order) > self._dedup_cache_size:
            stale = self._seen_message_order.pop(0)
            self._seen_message_ids.pop(stale, None)
        self._persist_seen_message_ids()

    def _commit_pending_message_id(self, message_id: str) -> None:
        now = time.time()
        ttl = _FEISHU_DEDUP_TTL_SECONDS
        with self._dedup_lock:
            pending_ids = getattr(self, "_pending_dedup_message_ids", None)
            if pending_ids is not None:
                pending_ids.discard(message_id)
            seen_at = self._seen_message_ids.get(message_id)
            if seen_at is not None and (ttl <= 0 or now - seen_at < ttl):
                return
            if seen_at is not None:
                self._seen_message_ids.pop(message_id, None)
                self._seen_message_order = [item for item in self._seen_message_order if item != message_id]
            # Record with current wall-clock timestamp so TTL works across restarts.
            self._record_seen_message_id_locked(message_id, now)

    def _release_pending_message_id(self, message_id: str) -> None:
        with self._dedup_lock:
            pending_ids = getattr(self, "_pending_dedup_message_ids", None)
            if pending_ids is not None:
                pending_ids.discard(message_id)

    # =========================================================================
    # Outbound payload construction and send pipeline
    # =========================================================================

    def _build_outbound_payload(self, content: str) -> tuple[str, str]:
        # Feishu post-type 'md' elements do not render markdown tables; sending
        # table content as post causes the message to appear blank on the client.
        # Force plain text for anything that looks like a markdown table.
        if _MARKDOWN_TABLE_RE.search(content):
            text_payload = {"text": content}
            return "text", json.dumps(text_payload, ensure_ascii=False)
        if _MARKDOWN_HINT_RE.search(content):
            return "post", _build_markdown_post_payload(content)
        text_payload = {"text": content}
        return "text", json.dumps(text_payload, ensure_ascii=False)

    async def _send_uploaded_file_message(
        self,
        *,
        chat_id: str,
        file_path: str,
        reply_to: Optional[str],
        metadata: Optional[Dict[str, Any]],
        caption: Optional[str] = None,
        file_name: Optional[str] = None,
        outbound_message_type: str = "file",
    ) -> SendResult:
        thread_id = str((metadata or {}).get("thread_id") or "") or None
        payload_type = {
            "audio": "audio",
            "media": "video",
        }.get(outbound_message_type, "file")
        recorded = self._record_only_outbound_result(
            operation="file_reply" if (reply_to or thread_id) else "file_send",
            chat_id=chat_id,
            payload_type=payload_type,
            payload={
                "path": file_path,
                "file_name": file_name,
                "caption": caption,
                "outbound_message_type": outbound_message_type,
            },
            metadata=metadata,
            thread_id=thread_id,
            message_id=reply_to,
            reply_mode="message" if reply_to else ("thread" if thread_id else "none"),
        )
        if recorded is not None:
            return recorded
        if not self._client:
            return SendResult(success=False, error="Not connected")
        if not os.path.exists(file_path):
            return SendResult(success=False, error=f"File not found: {file_path}")

        display_name = file_name or os.path.basename(file_path)
        upload_file_type, resolved_message_type = self._resolve_outbound_file_routing(
            file_path=display_name,
            requested_message_type=outbound_message_type,
        )
        try:
            with open(file_path, "rb") as file_obj:
                body = self._build_file_upload_body(
                    file_type=upload_file_type,
                    file_name=display_name,
                    file=file_obj,
                )
                request = self._build_file_upload_request(body)
                upload_response = await self._run_blocking(self._client.im.v1.file.create, request)
            file_key = self._extract_response_field(upload_response, "file_key")
            if not file_key:
                return self._response_error_result(
                    upload_response,
                    default_message="file upload failed",
                    override_error="Feishu file upload missing file_key",
                )

            if caption:
                media_tag = {
                    "tag": "media",
                    "file_key": file_key,
                    "file_name": display_name,
                }
                message_response = await self._feishu_send_with_retry(
                    chat_id=chat_id,
                    msg_type="post",
                    payload=self._build_media_post_payload(caption=caption, media_tag=media_tag),
                    reply_to=reply_to,
                    metadata=metadata,
                )
            else:
                message_response = await self._feishu_send_with_retry(
                    chat_id=chat_id,
                    msg_type=resolved_message_type,
                    payload=json.dumps({"file_key": file_key}, ensure_ascii=False),
                    reply_to=reply_to,
                    metadata=metadata,
                )
            return self._finalize_send_result(message_response, "file send failed")
        except Exception as exc:
            logger.error("[Feishu] Failed to send file %s: %s", file_path, exc, exc_info=True)
            return SendResult(success=False, error=str(exc))

    async def _send_raw_message(
        self,
        *,
        chat_id: str,
        msg_type: str,
        payload: str,
        reply_to: Optional[str],
        metadata: Optional[Dict[str, Any]],
    ) -> Any:
        idempotency_uuid = self._message_idempotency_uuid(metadata)
        effective_reply_to, reply_in_thread = self._resolve_reply_target(
            reply_to=reply_to,
            metadata=metadata,
        )
        thread_id = str((metadata or {}).get("thread_id") or "") or None
        is_card = msg_type == "interactive"
        recorded = self._record_only_outbound_result(
            operation=(
                "card_reply" if is_card and (effective_reply_to or thread_id)
                else "card_send" if is_card
                else "text_reply" if (effective_reply_to or thread_id)
                else "text_send"
            ),
            chat_id=chat_id,
            payload_type="interactive_card" if is_card else "text",
            payload=payload,
            metadata=metadata,
            thread_id=thread_id,
            message_id=effective_reply_to if effective_reply_to and effective_reply_to != thread_id else None,
            reply_mode=("thread" if reply_in_thread or thread_id else "message") if (effective_reply_to or thread_id) else "none",
            update_mode="create" if is_card else "none",
        )
        if recorded is not None:
            return recorded
        if effective_reply_to:
            body = self._build_reply_message_body(
                content=payload,
                msg_type=msg_type,
                reply_in_thread=reply_in_thread,
                uuid_value=idempotency_uuid,
            )
            request = self._build_reply_message_request(effective_reply_to, body)
            return await self._run_blocking(self._client.im.v1.message.reply, request)

        # For topic/thread messages that fell back from reply→create, use
        # thread_id as receive_id so the message lands in the topic instead of
        # the main chat.
        _thread_id = (metadata or {}).get("thread_id")
        if _thread_id:
            body = self._build_create_message_body(
                receive_id=_thread_id,
                msg_type=msg_type,
                content=payload,
                uuid_value=idempotency_uuid,
            )
            request = self._build_create_message_request("thread_id", body)
        else:
            receive_id = chat_id
            receive_id_type = "chat_id"
            if chat_id.startswith("feishu_user_id:"):
                receive_id = chat_id.split(":", 1)[1]
                receive_id_type = "user_id"
            elif chat_id.startswith("ou_"):
                receive_id_type = "open_id"

            body = self._build_create_message_body(
                receive_id=receive_id,
                msg_type=msg_type,
                content=payload,
                uuid_value=idempotency_uuid,
            )
            request = self._build_create_message_request(receive_id_type, body)
        return await self._run_blocking(self._client.im.v1.message.create, request)

    @staticmethod
    def _response_succeeded(response: Any) -> bool:
        success = getattr(response, "success", None)
        if callable(success):
            return bool(response and success())
        if isinstance(success, bool):
            return bool(response and success)
        return False

    @staticmethod
    def _extract_response_field(response: Any, field_name: str) -> Any:
        if not FeishuAdapter._response_succeeded(response):
            return None
        data = getattr(response, "data", None)
        return getattr(data, field_name, None) if data else None

    def _response_error_result(
        self,
        response: Any,
        *,
        default_message: str,
        override_error: Optional[str] = None,
    ) -> SendResult:
        if override_error:
            return SendResult(success=False, error=override_error, raw_response=response)
        code = getattr(response, "code", "unknown")
        msg = getattr(response, "msg", default_message)
        return SendResult(success=False, error=f"[{code}] {msg}", raw_response=response)

    def _finalize_send_result(self, response: Any, default_message: str) -> SendResult:
        if isinstance(response, SendResult):
            return response
        if not self._response_succeeded(response):
            return self._response_error_result(response, default_message=default_message)
        return SendResult(
            success=True,
            message_id=self._extract_response_field(response, "message_id"),
            raw_response=response,
        )

    # =========================================================================
    # Connection internals — websocket / webhook setup
    # =========================================================================

    async def _connect_with_retry(self) -> None:
        for attempt in range(_FEISHU_CONNECT_ATTEMPTS):
            try:
                if self._connection_mode == "websocket":
                    await self._connect_websocket()
                else:
                    await self._connect_webhook()
                return
            except Exception as exc:
                self._running = False
                self._disable_websocket_auto_reconnect()
                self._ws_future = None
                await self._stop_webhook_server()
                if attempt >= _FEISHU_CONNECT_ATTEMPTS - 1:
                    raise
                wait_seconds = 2 ** attempt
                logger.warning(
                    "[Feishu] Connect attempt %d/%d failed; retrying in %ds: %s",
                    attempt + 1,
                    _FEISHU_CONNECT_ATTEMPTS,
                    wait_seconds,
                    exc,
                )
                await asyncio.sleep(wait_seconds)

    async def _connect_websocket(self) -> None:
        if not FEISHU_WEBSOCKET_AVAILABLE:
            raise RuntimeError("websockets not installed; websocket mode unavailable")
        domain = FEISHU_DOMAIN if self._domain_name != "lark" else LARK_DOMAIN
        self._client = self._build_lark_client(domain)
        self._event_handler = self._build_event_handler()
        if self._event_handler is None:
            raise RuntimeError("failed to build Feishu event handler")
        loop = self._loop
        if loop is None or loop.is_closed():
            raise RuntimeError("adapter loop is not ready")
        await self._hydrate_bot_identity()
        self._ws_client = FeishuWSClient(
            app_id=self._app_id,
            app_secret=self._app_secret,
            log_level=lark.LogLevel.INFO,
            event_handler=self._event_handler,
            domain=domain,
        )
        self._ws_future = loop.run_in_executor(
            None,
            _run_official_feishu_ws_client,
            self._ws_client,
            self,
        )

    async def _connect_webhook(self) -> None:
        if not FEISHU_WEBHOOK_AVAILABLE:
            raise RuntimeError("aiohttp not installed; webhook mode unavailable")
        domain = FEISHU_DOMAIN if self._domain_name != "lark" else LARK_DOMAIN
        self._client = self._build_lark_client(domain)
        self._event_handler = self._build_event_handler()
        if self._event_handler is None:
            raise RuntimeError("failed to build Feishu event handler")
        await self._hydrate_bot_identity()
        # client_max_size backstops the bounded reader in
        # _handle_webhook_request; aiohttp then enforces the same cap on
        # every read path (#58536/#58902/#59180 pattern).
        app = web.Application(client_max_size=_FEISHU_WEBHOOK_MAX_BODY_BYTES)
        app.router.add_post(self._webhook_path, self._handle_webhook_request)
        self._webhook_runner = web.AppRunner(app)
        await self._webhook_runner.setup()
        self._webhook_site = web.TCPSite(self._webhook_runner, self._webhook_host, self._webhook_port)
        await self._webhook_site.start()

    def _build_lark_client(self, domain: Any) -> Any:
        return (
            lark.Client.builder()
            .app_id(self._app_id)
            .app_secret(self._app_secret)
            .domain(domain)
            .log_level(lark.LogLevel.WARNING)
            .timeout(_FEISHU_API_TIMEOUT_SECONDS)
            .build()
        )

    @staticmethod
    def _reply_fallback_metadata(metadata: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not metadata:
            return None
        cleaned = dict(metadata)
        cleaned.pop("thread_id", None)
        return cleaned or None

    @staticmethod
    def _metadata_has_topic_thread(metadata: Optional[Dict[str, Any]]) -> bool:
        thread_id = str((metadata or {}).get("thread_id") or "").strip()
        return thread_id.startswith(_FEISHU_TOPIC_THREAD_PREFIX)

    def _topic_reply_rejected_result(self, response: Any) -> SendResult:
        code = getattr(response, "code", "unknown")
        msg = getattr(response, "msg", "reply failed")
        return SendResult(
            success=False,
            error=f"[{code}] reply target/thread rejected; refusing chat fallback for Feishu topic route: {msg}",
            raw_response=response,
        )

    @staticmethod
    def _should_fallback_reply_response(response: Any) -> bool:
        code = getattr(response, "code", None)
        if code in {230011, 231003}:
            return True
        if code == 2200:
            msg = str(getattr(response, "msg", "") or "").lower()
            return "internal error" in msg
        return False

    async def _feishu_send_with_retry(
        self,
        *,
        chat_id: str,
        msg_type: str,
        payload: str,
        reply_to: Optional[str],
        metadata: Optional[Dict[str, Any]],
    ) -> Any:
        last_error: Optional[Exception] = None
        send_metadata = dict(metadata or {})
        send_metadata.setdefault("idempotency_uuid", str(uuid.uuid4()))
        metadata_thread_id = str(send_metadata.get("thread_id") or "").strip()
        metadata_reply_to = str(send_metadata.get("reply_to_message_id") or "").strip() or None
        active_reply_to = reply_to or metadata_reply_to or self._topic_anchor_from_thread_id(metadata_thread_id)
        for attempt in range(_FEISHU_SEND_ATTEMPTS):
            try:
                response = await self._send_raw_message(
                    chat_id=chat_id,
                    msg_type=msg_type,
                    payload=payload,
                    reply_to=active_reply_to,
                    metadata=send_metadata,
                )
                # If replying to a message failed because it was withdrawn or not found,
                # fall back to posting a new message directly to the chat only for plain
                # direct replies. A topic route must fail closed; otherwise Feishu can
                # return API success for a message that lands in the group instead of the
                # task topic.
                if active_reply_to and not self._response_succeeded(response):
                    code = getattr(response, "code", None)
                    if self._should_fallback_reply_response(response):
                        if self._metadata_has_topic_thread(send_metadata):
                            logger.warning(
                                "[Feishu] Reply to topic %s failed (code %s — reply target/thread rejected); "
                                "refusing chat fallback for topic route in chat %s",
                                active_reply_to,
                                code,
                                chat_id,
                            )
                            return self._topic_reply_rejected_result(response)
                        logger.warning(
                            "[Feishu] Reply to %s failed (code %s — reply target/thread rejected); "
                            "falling back to new message in chat %s",
                            active_reply_to,
                            code,
                            chat_id,
                        )
                        active_reply_to = None
                        response = await self._send_raw_message(
                            chat_id=chat_id,
                            msg_type=msg_type,
                            payload=payload,
                            reply_to=None,
                            metadata=self._reply_fallback_metadata(send_metadata),
                        )
                return response
            except Exception as exc:
                last_error = exc
                if msg_type == "post" and _POST_CONTENT_INVALID_RE.search(str(exc)):
                    raise
                if attempt >= _FEISHU_SEND_ATTEMPTS - 1:
                    raise
                wait_seconds = 2 ** attempt
                logger.warning(
                    "[Feishu] Send attempt %d/%d failed for chat %s; retrying in %ds: %s",
                    attempt + 1,
                    _FEISHU_SEND_ATTEMPTS,
                    chat_id,
                    wait_seconds,
                    exc,
                )
                await asyncio.sleep(wait_seconds)
        raise last_error or RuntimeError("Feishu send failed")

    async def _release_app_lock(self) -> None:
        if not self._app_lock_identity:
            return
        try:
            release_scoped_lock(_FEISHU_APP_LOCK_SCOPE, self._app_lock_identity)
        except Exception as exc:
            logger.warning("[Feishu] Failed to release app lock: %s", exc, exc_info=True)
        finally:
            self._app_lock_identity = None

    # =========================================================================
    # Lark API request builders
    # =========================================================================

    @staticmethod
    def _build_get_chat_request(chat_id: str) -> Any:
        if "GetChatRequest" in globals():
            return GetChatRequest.builder().chat_id(chat_id).build()
        return SimpleNamespace(chat_id=chat_id)

    @staticmethod
    def _build_get_message_request(message_id: str) -> Any:
        if "GetMessageRequest" in globals():
            return GetMessageRequest.builder().message_id(message_id).build()
        return SimpleNamespace(message_id=message_id)

    @staticmethod
    def _build_message_resource_request(*, message_id: str, file_key: str, resource_type: str) -> Any:
        if "GetMessageResourceRequest" in globals():
            return (
                GetMessageResourceRequest.builder()
                .message_id(message_id)
                .file_key(file_key)
                .type(resource_type)
                .build()
            )
        return SimpleNamespace(message_id=message_id, file_key=file_key, type=resource_type)

    @staticmethod
    def _build_get_application_request(*, app_id: str, lang: str) -> Any:
        if "GetApplicationRequest" in globals():
            return (
                GetApplicationRequest.builder()
                .app_id(app_id)
                .lang(lang)
                .build()
            )
        return SimpleNamespace(app_id=app_id, lang=lang)

    @staticmethod
    def _build_reply_message_body(*, content: str, msg_type: str, reply_in_thread: bool, uuid_value: str) -> Any:
        if "ReplyMessageRequestBody" in globals():
            return (
                ReplyMessageRequestBody.builder()
                .content(content)
                .msg_type(msg_type)
                .reply_in_thread(reply_in_thread)
                .uuid(uuid_value)
                .build()
            )
        return SimpleNamespace(
            content=content,
            msg_type=msg_type,
            reply_in_thread=reply_in_thread,
            uuid=uuid_value,
        )

    @staticmethod
    def _build_reply_message_request(message_id: str, request_body: Any) -> Any:
        if "ReplyMessageRequest" in globals():
            return (
                ReplyMessageRequest.builder()
                .message_id(message_id)
                .request_body(request_body)
                .build()
            )
        return SimpleNamespace(message_id=message_id, request_body=request_body)

    @staticmethod
    def _build_update_message_body(*, msg_type: str, content: str) -> Any:
        if "UpdateMessageRequestBody" in globals():
            return (
                UpdateMessageRequestBody.builder()
                .msg_type(msg_type)
                .content(content)
                .build()
            )
        return SimpleNamespace(msg_type=msg_type, content=content)

    @staticmethod
    def _build_update_message_request(message_id: str, request_body: Any) -> Any:
        if "UpdateMessageRequest" in globals():
            return (
                UpdateMessageRequest.builder()
                .message_id(message_id)
                .request_body(request_body)
                .build()
            )
        return SimpleNamespace(message_id=message_id, request_body=request_body)

    @staticmethod
    def _build_create_message_body(*, receive_id: str, msg_type: str, content: str, uuid_value: str) -> Any:
        if "CreateMessageRequestBody" in globals():
            return (
                CreateMessageRequestBody.builder()
                .receive_id(receive_id)
                .msg_type(msg_type)
                .content(content)
                .uuid(uuid_value)
                .build()
            )
        return SimpleNamespace(
            receive_id=receive_id,
            msg_type=msg_type,
            content=content,
            uuid=uuid_value,
        )

    @staticmethod
    def _build_create_message_request(receive_id_type: str, request_body: Any) -> Any:
        if "CreateMessageRequest" in globals():
            return (
                CreateMessageRequest.builder()
                .receive_id_type(receive_id_type)
                .request_body(request_body)
                .build()
            )
        return SimpleNamespace(receive_id_type=receive_id_type, request_body=request_body)

    @staticmethod
    def _build_image_upload_body(*, image_type: str, image: Any) -> Any:
        if "CreateImageRequestBody" in globals():
            return (
                CreateImageRequestBody.builder()
                .image_type(image_type)
                .image(image)
                .build()
            )
        return SimpleNamespace(image_type=image_type, image=image)

    @staticmethod
    def _build_image_upload_request(request_body: Any) -> Any:
        if "CreateImageRequest" in globals():
            return CreateImageRequest.builder().request_body(request_body).build()
        return SimpleNamespace(request_body=request_body)

    @staticmethod
    def _build_file_upload_body(*, file_type: str, file_name: str, file: Any) -> Any:
        if "CreateFileRequestBody" in globals():
            return (
                CreateFileRequestBody.builder()
                .file_type(file_type)
                .file_name(file_name)
                .file(file)
                .build()
            )
        return SimpleNamespace(file_type=file_type, file_name=file_name, file=file)

    @staticmethod
    def _build_file_upload_request(request_body: Any) -> Any:
        if "CreateFileRequest" in globals():
            return CreateFileRequest.builder().request_body(request_body).build()
        return SimpleNamespace(request_body=request_body)

    def _build_post_payload(self, content: str) -> str:
        return _build_markdown_post_payload(content)

    def _build_media_post_payload(self, *, caption: str, media_tag: Dict[str, str]) -> str:
        payload = json.loads(self._build_post_payload(caption))
        content = payload.setdefault("zh_cn", {}).setdefault("content", [])
        content.append([media_tag])
        return json.dumps(payload, ensure_ascii=False)

    @staticmethod
    def _resolve_outbound_file_routing(
        *,
        file_path: str,
        requested_message_type: str,
    ) -> tuple[str, str]:
        ext = Path(file_path).suffix.lower()

        if ext in _FEISHU_OPUS_UPLOAD_EXTENSIONS:
            return "opus", "audio"

        if ext in _FEISHU_MEDIA_UPLOAD_EXTENSIONS:
            return "mp4", "media"

        if ext in _FEISHU_DOC_UPLOAD_TYPES:
            return _FEISHU_DOC_UPLOAD_TYPES[ext], "file"

        if requested_message_type == "file":
            return _FEISHU_FILE_UPLOAD_TYPE, "file"

        return _FEISHU_FILE_UPLOAD_TYPE, "file"


# =============================================================================
# QR scan-to-create onboarding
#
# Device-code flow: user scans a QR code with Feishu/Lark mobile app and the
# platform creates a fully configured bot application automatically.
# Called by `hermes gateway setup` via _setup_feishu() in hermes_cli/gateway.py.
# =============================================================================


def _accounts_base_url(domain: str) -> str:
    return _ONBOARD_ACCOUNTS_URLS.get(domain, _ONBOARD_ACCOUNTS_URLS["feishu"])


def _onboard_open_base_url(domain: str) -> str:
    return _ONBOARD_OPEN_URLS.get(domain, _ONBOARD_OPEN_URLS["feishu"])


def _post_registration(base_url: str, body: Dict[str, str]) -> dict:
    """POST form-encoded data to the registration endpoint, return parsed JSON.

    The registration endpoint returns JSON even on 4xx (e.g. poll returns
    authorization_pending as a 400). We always parse the body regardless of
    HTTP status.
    """
    url = f"{base_url}{_REGISTRATION_PATH}"
    data = urlencode(body).encode("utf-8")
    req = Request(url, data=data, headers={"Content-Type": "application/x-www-form-urlencoded"})
    try:
        with urlopen(req, timeout=_ONBOARD_REQUEST_TIMEOUT_S) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except HTTPError as exc:
        body_bytes = exc.read()
        if body_bytes:
            try:
                return json.loads(body_bytes.decode("utf-8"))
            except (ValueError, json.JSONDecodeError):
                raise exc from None
        raise


def _init_registration(domain: str = "feishu") -> None:
    """Verify the environment supports client_secret auth.

    Raises RuntimeError if not supported.
    """
    base_url = _accounts_base_url(domain)
    res = _post_registration(base_url, {"action": "init"})
    methods = res.get("supported_auth_methods") or []
    if "client_secret" not in methods:
        raise RuntimeError(
            f"Feishu / Lark registration environment does not support client_secret auth. "
            f"Supported: {methods}"
        )


def _begin_registration(domain: str = "feishu") -> dict:
    """Start the device-code flow. Returns device_code, qr_url, user_code, interval, expire_in."""
    base_url = _accounts_base_url(domain)
    res = _post_registration(base_url, {
        "action": "begin",
        "archetype": "PersonalAgent",
        "auth_method": "client_secret",
        "request_user_info": "open_id",
    })
    device_code = res.get("device_code")
    if not device_code:
        raise RuntimeError("Feishu / Lark registration did not return a device_code")
    qr_url = res.get("verification_uri_complete", "")
    if "?" in qr_url:
        qr_url += "&from=hermes&tp=hermes"
    else:
        qr_url += "?from=hermes&tp=hermes"
    return {
        "device_code": device_code,
        "qr_url": qr_url,
        "user_code": res.get("user_code", ""),
        "interval": res.get("interval") or 5,
        "expire_in": res.get("expire_in") or 600,
    }


def _poll_registration(
    *,
    device_code: str,
    interval: int,
    expire_in: int,
    domain: str = "feishu",
) -> Optional[dict]:
    """Poll until the user scans the QR code, or timeout/denial.

    Returns dict with app_id, app_secret, domain, open_id on success.
    Returns None on failure.
    """
    deadline = time.monotonic() + expire_in
    current_domain = domain
    domain_switched = False
    poll_count = 0

    while time.monotonic() < deadline:
        base_url = _accounts_base_url(current_domain)
        try:
            res = _post_registration(base_url, {
                "action": "poll",
                "device_code": device_code,
                "tp": "ob_app",
            })
        except (URLError, OSError, json.JSONDecodeError):
            time.sleep(interval)
            continue

        poll_count += 1
        if poll_count == 1:
            print("  Fetching configuration results...", end="", flush=True)
        elif poll_count % 6 == 0:
            print(".", end="", flush=True)

        # Domain auto-detection
        user_info = res.get("user_info") or {}
        tenant_brand = user_info.get("tenant_brand")
        if tenant_brand == "lark" and not domain_switched:
            current_domain = "lark"
            domain_switched = True
            # Fall through — server may return credentials in this same response.

        # Success
        if res.get("client_id") and res.get("client_secret"):
            if poll_count > 0:
                print()  # newline after "Fetching configuration results..." dots
            return {
                "app_id": res["client_id"],
                "app_secret": res["client_secret"],
                "domain": current_domain,
                "open_id": user_info.get("open_id"),
            }

        # Terminal errors
        error = res.get("error", "")
        if error in {"access_denied", "expired_token"}:
            if poll_count > 0:
                print()
            logger.warning("[Feishu onboard] Registration %s", error)
            return None

        # authorization_pending or unknown — keep polling
        time.sleep(interval)

    if poll_count > 0:
        print()
    logger.warning("[Feishu onboard] Poll timed out after %ds", expire_in)
    return None


try:
    import qrcode as _qrcode_mod
except (ImportError, TypeError):
    _qrcode_mod = None  # type: ignore[assignment]


def _render_qr(url: str) -> bool:
    """Try to render a QR code in the terminal. Returns True if successful."""
    if _qrcode_mod is None:
        return False
    try:
        qr = _qrcode_mod.QRCode()
        qr.add_data(url)
        qr.make(fit=True)
        qr.print_ascii(invert=True)
        return True
    except Exception:
        return False


def probe_bot(app_id: str, app_secret: str, domain: str) -> Optional[dict]:
    """Verify bot connectivity via /open-apis/bot/v3/info.

    Uses lark_oapi SDK when available, falls back to raw HTTP otherwise.
    Returns {"bot_name": ..., "bot_open_id": ...} on success, None on failure.

    Note: ``bot_open_id`` here is the bot's app-scoped open_id — the same ID
    that Feishu puts in @mention payloads.  It is NOT the app_id.
    """
    if FEISHU_AVAILABLE:
        return _probe_bot_sdk(app_id, app_secret, domain)
    return _probe_bot_http(app_id, app_secret, domain)


def _build_onboard_client(app_id: str, app_secret: str, domain: str) -> Any:
    """Build a lark Client for the given credentials and domain."""
    sdk_domain = LARK_DOMAIN if domain == "lark" else FEISHU_DOMAIN
    return (
        lark.Client.builder()
        .app_id(app_id)
        .app_secret(app_secret)
        .domain(sdk_domain)
        .log_level(lark.LogLevel.WARNING)
        .build()
    )


def _parse_bot_response(data: dict) -> Optional[dict]:
    # /bot/v3/info returns bot.app_name; legacy paths used bot_name — accept both.
    if data.get("code") != 0:
        return None
    bot = data.get("bot") or data.get("data", {}).get("bot") or {}
    return {
        "bot_name": bot.get("app_name") or bot.get("bot_name"),
        "bot_open_id": bot.get("open_id"),
    }


def _probe_bot_sdk(app_id: str, app_secret: str, domain: str) -> Optional[dict]:
    """Probe bot info using lark_oapi SDK."""
    try:
        client = _build_onboard_client(app_id, app_secret, domain)
        req = (
            BaseRequest.builder()
            .http_method(HttpMethod.GET)
            .uri("/open-apis/bot/v3/info")
            .token_types({AccessTokenType.TENANT})
            .build()
        )
        resp = client.request(req)
        content = getattr(getattr(resp, "raw", None), "content", None)
        if content is None:
            return None
        return _parse_bot_response(json.loads(content))
    except Exception as exc:
        logger.debug("[Feishu onboard] SDK probe failed: %s", exc)
        return None


def _probe_bot_http(app_id: str, app_secret: str, domain: str) -> Optional[dict]:
    """Fallback probe using raw HTTP (when lark_oapi is not installed)."""
    base_url = _onboard_open_base_url(domain)
    try:
        token_data = json.dumps({"app_id": app_id, "app_secret": app_secret}).encode("utf-8")
        token_req = Request(
            f"{base_url}/open-apis/auth/v3/tenant_access_token/internal",
            data=token_data,
            headers={"Content-Type": "application/json"},
        )
        with urlopen(token_req, timeout=_ONBOARD_REQUEST_TIMEOUT_S) as resp:
            token_res = json.loads(resp.read().decode("utf-8"))

        access_token = token_res.get("tenant_access_token")
        if not access_token:
            return None

        bot_req = Request(
            f"{base_url}/open-apis/bot/v3/info",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            },
        )
        with urlopen(bot_req, timeout=_ONBOARD_REQUEST_TIMEOUT_S) as resp:
            bot_res = json.loads(resp.read().decode("utf-8"))

        return _parse_bot_response(bot_res)
    except (URLError, OSError, KeyError, json.JSONDecodeError) as exc:
        logger.debug("[Feishu onboard] HTTP probe failed: %s", exc)
        return None


def qr_register(
    *,
    initial_domain: str = "feishu",
    timeout_seconds: int = 600,
) -> Optional[dict]:
    """Run the Feishu / Lark scan-to-create QR registration flow.

    Returns on success::

        {
            "app_id": str,
            "app_secret": str,
            "domain": "feishu" | "lark",
            "open_id": str | None,
            "bot_name": str | None,
            "bot_open_id": str | None,
        }

    Returns None on expected failures (network, auth denied, timeout).
    Unexpected errors (bugs, protocol regressions) propagate to the caller.
    """
    try:
        return _qr_register_inner(initial_domain=initial_domain, timeout_seconds=timeout_seconds)
    except (RuntimeError, URLError, OSError, json.JSONDecodeError) as exc:
        logger.warning("[Feishu onboard] Registration failed: %s", exc)
        return None


def _qr_register_inner(
    *,
    initial_domain: str,
    timeout_seconds: int,
) -> Optional[dict]:
    """Run init → begin → poll → probe. Raises on network/protocol errors."""
    print("  Connecting to Feishu / Lark...", end="", flush=True)
    _init_registration(initial_domain)
    begin = _begin_registration(initial_domain)
    print(" done.")

    print()
    qr_url = begin["qr_url"]
    if _render_qr(qr_url):
        print(f"\n  Scan the QR code above, or open this URL directly:\n  {qr_url}")
    else:
        print(f"  Open this URL in Feishu / Lark on your phone:\n\n  {qr_url}\n")
        print("  Tip: pip install qrcode  to display a scannable QR code here next time")
    print()

    result = _poll_registration(
        device_code=begin["device_code"],
        interval=begin["interval"],
        expire_in=min(begin["expire_in"], timeout_seconds),
        domain=initial_domain,
    )
    if not result:
        return None

    # Probe bot — best-effort, don't fail the registration
    bot_info = probe_bot(result["app_id"], result["app_secret"], result["domain"])
    if bot_info:
        result["bot_name"] = bot_info.get("bot_name")
        result["bot_open_id"] = bot_info.get("bot_open_id")
    else:
        result["bot_name"] = None
        result["bot_open_id"] = None

    return result


# ──────────────────────────────────────────────────────────────────────────
# Plugin migration glue (#41112 / #3823)
#
# Added when the Feishu adapter (+ its feishu_comment / feishu_comment_rules /
# feishu_meeting_invite satellites) moved from gateway/platforms/ into this
# bundled plugin. Mirrors the Discord (#24356) / Slack migrations: a
# register(ctx) entry point plus hook implementations that replace the
# per-platform core touchpoints (the Platform.FEISHU elif in gateway/run.py,
# the feishu_cfg YAML→env block + _PLATFORM_CONNECTED_CHECKERS entry in
# gateway/config.py, the _setup_feishu wizard + _PLATFORMS["feishu"] static
# dict in hermes_cli/gateway.py, and the _send_feishu dispatch in
# tools/send_message_tool.py).
# ──────────────────────────────────────────────────────────────────────────

_MIGRATION_IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".gif"}
_MIGRATION_VIDEO_EXTS = {".mp4", ".mov", ".avi", ".mkv", ".3gp"}
_MIGRATION_AUDIO_EXTS = {".ogg", ".opus", ".mp3", ".wav", ".m4a", ".flac"}
_MIGRATION_VOICE_EXTS = {".ogg", ".opus"}


async def _standalone_send(
    pconfig,
    chat_id,
    message,
    *,
    thread_id=None,
    media_files=None,
    force_document=False,
):
    """Out-of-process Feishu/Lark delivery via the adapter's send pipeline.

    Implements the standalone_sender_fn contract so deliver=feishu cron jobs
    succeed when cron runs separately from the gateway. Builds a transient
    FeishuAdapter, hydrates its lark client, and sends text + native media
    (images, video, voice, documents). Replaces the legacy _send_feishu helper.
    """
    if not FEISHU_AVAILABLE:
        return {"error": "Feishu dependencies not installed. Run: pip install 'hermes-agent[feishu]'"}

    media_files = media_files or []
    try:
        adapter = FeishuAdapter(pconfig)
        domain_name = getattr(adapter, "_domain_name", "feishu")
        domain = FEISHU_DOMAIN if domain_name != "lark" else LARK_DOMAIN
        adapter._client = adapter._build_lark_client(domain)
        metadata = {"thread_id": thread_id} if thread_id else None

        last_result = None
        if message.strip():
            last_result = await adapter.send(chat_id, message, metadata=metadata)
            if not last_result.success:
                return {"error": f"Feishu send failed: {last_result.error}"}

        for media_path, is_voice in media_files:
            if not os.path.exists(media_path):
                return {"error": f"Media file not found: {media_path}"}
            ext = os.path.splitext(media_path)[1].lower()
            if ext in _MIGRATION_IMAGE_EXTS:
                last_result = await adapter.send_image_file(chat_id, media_path, metadata=metadata)
            elif ext in _MIGRATION_VIDEO_EXTS:
                last_result = await adapter.send_video(chat_id, media_path, metadata=metadata)
            elif ext in _MIGRATION_VOICE_EXTS and is_voice:
                last_result = await adapter.send_voice(chat_id, media_path, metadata=metadata)
            elif ext in _MIGRATION_AUDIO_EXTS:
                last_result = await adapter.send_voice(chat_id, media_path, metadata=metadata)
            else:
                last_result = await adapter.send_document(chat_id, media_path, metadata=metadata)
            if not last_result.success:
                return {"error": f"Feishu media send failed: {last_result.error}"}

        if last_result is None:
            return {"error": "No deliverable text or media remained after processing MEDIA tags"}
        return {
            "success": True,
            "platform": "feishu",
            "chat_id": chat_id,
            "message_id": last_result.message_id,
        }
    except Exception as e:
        return {"error": f"Feishu send failed: {e}"}


def interactive_setup() -> None:
    """Interactive setup for Feishu / Lark — scan-to-create or manual creds.

    Replaces the central _setup_feishu in hermes_cli/gateway.py and the static
    _PLATFORMS["feishu"] dict. CLI helpers are lazy-imported.
    """
    from hermes_cli.config import get_env_value, save_env_value
    from hermes_cli.setup import prompt_choice
    from hermes_cli.cli_output import (
        prompt,
        prompt_yes_no,
        print_header,
        print_info,
        print_success,
        print_warning,
        print_error,
    )

    print_header("Feishu / Lark")
    existing_app_id = get_env_value("FEISHU_APP_ID")
    existing_secret = get_env_value("FEISHU_APP_SECRET")
    if existing_app_id and existing_secret:
        print_success("Feishu / Lark is already configured.")
        if not prompt_yes_no("Reconfigure Feishu / Lark?", False):
            return

    method_idx = prompt_choice(
        "How would you like to set up Feishu / Lark?",
        [
            "Scan QR code to create a new bot automatically (recommended)",
            "Enter existing App ID and App Secret manually",
        ],
        0,
    )

    credentials = None
    used_qr = False

    if method_idx == 0:
        try:
            credentials = qr_register()
        except KeyboardInterrupt:
            print_warning("Feishu / Lark setup cancelled.")
            return
        except Exception as exc:
            print_warning(f"QR registration failed: {exc}")
        if credentials:
            used_qr = True
        else:
            print_info("QR setup did not complete. Continuing with manual input.")

    if not credentials:
        print_info("Go to https://open.feishu.cn/ (or https://open.larksuite.com/ for Lark)")
        print_info("Create an app, enable the Bot capability, and copy the credentials.")
        app_id = prompt("App ID", password=False)
        if not app_id:
            print_warning("Skipped — Feishu / Lark won't work without an App ID.")
            return
        app_secret = prompt("App Secret", password=True)
        if not app_secret:
            print_warning("Skipped — Feishu / Lark won't work without an App Secret.")
            return
        domain_idx = prompt_choice("Domain", ["feishu (China)", "lark (International)"], 0)
        domain = "lark" if domain_idx == 1 else "feishu"

        bot_name = None
        try:
            bot_info = probe_bot(app_id, app_secret, domain)
            if bot_info:
                bot_name = bot_info.get("bot_name")
                print_success(f"Credentials verified — bot: {bot_name or 'unnamed'}")
            else:
                print_warning("Could not verify bot connection. Credentials saved anyway.")
        except Exception as exc:
            print_warning(f"Credential verification skipped: {exc}")

        credentials = {
            "app_id": app_id,
            "app_secret": app_secret,
            "domain": domain,
            "open_id": None,
            "bot_name": bot_name,
        }

    app_id = credentials["app_id"]
    app_secret = credentials["app_secret"]
    domain = credentials.get("domain", "feishu")
    open_id = credentials.get("open_id")
    bot_name = credentials.get("bot_name")

    save_env_value("FEISHU_APP_ID", app_id)
    save_env_value("FEISHU_APP_SECRET", app_secret)
    save_env_value("FEISHU_DOMAIN", domain)

    if used_qr:
        connection_mode = "websocket"
    else:
        mode_idx = prompt_choice(
            "Connection mode",
            [
                "WebSocket (recommended — no public URL needed)",
                "Webhook (requires a reachable HTTP endpoint)",
            ],
            0,
        )
        connection_mode = "webhook" if mode_idx == 1 else "websocket"
        if connection_mode == "webhook":
            print_info("Webhook defaults: 127.0.0.1:8765/feishu/webhook")
            print_info("Override with FEISHU_WEBHOOK_HOST / FEISHU_WEBHOOK_PORT / FEISHU_WEBHOOK_PATH")
            print_info("For signature verification, set FEISHU_ENCRYPT_KEY and FEISHU_VERIFICATION_TOKEN")
    save_env_value("FEISHU_CONNECTION_MODE", connection_mode)

    if bot_name:
        print_success(f"Bot created: {bot_name}")

    access_idx = prompt_choice(
        "How should direct messages be authorized?",
        [
            "Use DM pairing approval (recommended)",
            "Allow all direct messages",
            "Only allow listed user IDs",
        ],
        0,
    )
    if access_idx == 0:
        save_env_value("FEISHU_ALLOW_ALL_USERS", "false")
        save_env_value("FEISHU_ALLOWED_USERS", "")
        print_success("DM pairing enabled.")
        print_info("Unknown users can request access; approve with `hermes pairing approve`.")
    elif access_idx == 1:
        save_env_value("FEISHU_ALLOW_ALL_USERS", "true")
        save_env_value("FEISHU_ALLOWED_USERS", "")
        print_warning("Open DM access enabled for Feishu / Lark.")
    else:
        save_env_value("FEISHU_ALLOW_ALL_USERS", "false")
        default_allow = open_id or ""
        allowlist = prompt(
            "Allowed user IDs (comma-separated)", default_allow, password=False
        ).replace(" ", "")
        save_env_value("FEISHU_ALLOWED_USERS", allowlist)
        print_success("Allowlist saved.")

    group_idx = prompt_choice(
        "How should group chats be handled?",
        [
            "Respond only when @mentioned in groups (recommended)",
            "Disable group chats",
        ],
        0,
    )
    if group_idx == 0:
        save_env_value("FEISHU_GROUP_POLICY", "open")
        print_info("Group chats enabled (bot must be @mentioned).")
    else:
        save_env_value("FEISHU_GROUP_POLICY", "disabled")
        print_info("Group chats disabled.")

    home_channel = prompt("Home chat ID (optional, for cron/notifications)", password=False)
    if home_channel:
        save_env_value("FEISHU_HOME_CHANNEL", home_channel)
        print_success(f"Home channel set to {home_channel}")

    print_success("🪽 Feishu / Lark configured!")
    print_info(f"App ID: {app_id}")
    print_info(f"Domain: {domain}")
    if bot_name:
        print_info(f"Bot: {bot_name}")


def _apply_yaml_config(yaml_cfg: dict, feishu_cfg: dict) -> dict | None:
    """Translate config.yaml feishu: keys into FEISHU_* env vars.

    Implements the apply_yaml_config_fn contract (#24849). Mirrors the legacy
    feishu_cfg block from gateway/config.py::load_gateway_config() (allow_bots).
    Env vars take precedence over YAML. Returns None — flows through env.
    """
    if "allow_bots" in feishu_cfg and not os.getenv("FEISHU_ALLOW_BOTS"):
        os.environ["FEISHU_ALLOW_BOTS"] = str(feishu_cfg["allow_bots"]).lower()
    return None


def _is_connected(config) -> bool:
    """Feishu is connected when app_id is configured. Mirrors the legacy
    _PLATFORM_CONNECTED_CHECKERS[Platform.FEISHU] = lambda cfg: bool(app_id)."""
    extra = getattr(config, "extra", {}) or {}
    return bool(extra.get("app_id"))


def _build_adapter(config):
    """Factory wrapper that constructs FeishuAdapter from a PlatformConfig."""
    return FeishuAdapter(config)


def register(ctx) -> None:
    """Plugin entry point — called by the Hermes plugin system."""
    ctx.register_platform(
        name="feishu",
        label="Feishu / Lark",
        adapter_factory=_build_adapter,
        check_fn=check_feishu_requirements,
        is_connected=_is_connected,
        validate_config=_is_connected,
        required_env=["FEISHU_APP_ID", "FEISHU_APP_SECRET"],
        install_hint="pip install 'hermes-agent[feishu]'",
        setup_fn=interactive_setup,
        apply_yaml_config_fn=_apply_yaml_config,
        allowed_users_env="FEISHU_ALLOWED_USERS",
        allow_all_env="FEISHU_ALLOW_ALL_USERS",
        cron_deliver_env_var="FEISHU_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        max_message_length=8000,
        emoji="🪽",
        allow_update_command=True,
    )
