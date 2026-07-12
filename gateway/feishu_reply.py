"""Feishu-specific user-visible reply formatting.

Keep noisy gateway/LLM lifecycle diagnostics in logs while Feishu users see a
short human-facing status.  This module intentionally handles only Feishu
surface text; canonical retry/fallback details stay in run_agent.py logs and
session artifacts.
"""

from __future__ import annotations

import hashlib
import re
import time
from dataclasses import dataclass, field
from typing import Any, Iterable

from gateway.config import Platform
from gateway.context_policy import GatewayContextPolicy

_FEISHU_HUMAN_WORDING_STYLE = (
    "直说发生了什么；一条状态只给一个结论和一个可执行下一步；"
    "不暴露 provider、fallback、HTTP、traceback、tool_call 等内部细节；"
    "避开客服腔和 AI/公文腔。"
)

_FEISHU_INTERNAL_ERROR_TEXT = (
    "这次处理没跑完，我会继续看网关日志。\n"
    "你可以稍后重试一次；如果还是失败，我再把卡住的位置整理出来。"
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
    "这次模型服务没正常返回，我这边已经重试过。\n"
    "可以稍后再试一次；如果还不行，我再看网关日志和上游连接。"
)

_FEISHU_CONTEXT_FAILURE_TEXT = (
    "这条我没处理完（上下文超限）。已记录这次请求，请 @我重发；"
    "如果是某个 case 的结论/报告，我按该话题的任务继续跟。"
)

FEISHU_CONTEXT_AUTO_COMPACT_TEXT = _FEISHU_CONTEXT_FAILURE_TEXT

_FEISHU_ROUTINE_INTERNAL_TOOL_NAMES = frozenset({
    "terminal",
    "read_file",
    "search_files",
    "session_search",
    "skill_view",
    "skills_list",
    "skill_manage",
    "browser_snapshot",
    "browser_console",
    "browser_navigate",
    "browser_click",
    "browser_type",
    "browser_scroll",
    "browser_back",
    "browser_press",
    "browser_get_images",
    "browser_vision",
})

_FEISHU_CONTEXT_FAILURE_PATTERNS = (
    re.compile(r"\b(?:context|token|too large|too long|exceed|payload)\b", re.I),
    re.compile(r"Session too large for the model's context window", re.I),
    re.compile(r"这轮上下文接近上限", re.I),
    re.compile(r"放不下完整上下文", re.I),
)

_DEFAULT_FEISHU_BOT_DISPLAY_NAMES = ("胡子豪的小助手",)

_FEISHU_INBOUND_STRUCTURAL_HISTORY_PATTERNS = (
    re.compile(r"^\s*\d+\s*条话题回复\s*$", re.M),
)

_FEISHU_INBOUND_ROLE_HISTORY_PATTERNS = (
    re.compile(r"^\s*机器人\s*$", re.M),
    re.compile(r"^\s*网关\s*$", re.M),
)

_FEISHU_INBOUND_CONTENT_HISTORY_PATTERNS = (
    re.compile(r"^\s*⚙️\s*正在处理请求\.\.\.\s*(?:\(×\d+\))?\s*$", re.M),
    re.compile(r"^\s*Cronjob Response:\b", re.M),
    re.compile(r"^\s*【VM 任务监控 heartbeat】\s*$", re.M),
    re.compile(r"\bwatchdog\b", re.I),
    re.compile(r"\bheartbeat\b", re.I),
    re.compile(r"\bheartbeat_at\b", re.I),
    re.compile(r"\bupdated_at\b", re.I),
    re.compile(r"^\s*任务\s*ID[：:]", re.I),
)

def _feishu_inbound_history_patterns(
    bot_display_names: Iterable[str] | None = None,
) -> tuple[re.Pattern[str], ...]:
    names = tuple(
        str(name).strip()
        for name in (bot_display_names if bot_display_names is not None else _DEFAULT_FEISHU_BOT_DISPLAY_NAMES)
        if str(name).strip()
    )
    return (
        *_FEISHU_INBOUND_STRUCTURAL_HISTORY_PATTERNS,
        *_feishu_inbound_self_history_patterns(names),
    )


def _feishu_inbound_self_history_patterns(
    bot_display_names: Iterable[str] | None = None,
) -> tuple[re.Pattern[str], ...]:
    names = tuple(str(name).strip() for name in (bot_display_names or ()) if str(name).strip())
    return (
        *tuple(re.compile(rf"^\s*{re.escape(name)}\s*$", re.M) for name in names),
        *_FEISHU_INBOUND_ROLE_HISTORY_PATTERNS,
        *_FEISHU_INBOUND_CONTENT_HISTORY_PATTERNS,
    )


_FEISHU_INBOUND_BOT_HISTORY_PATTERNS = _feishu_inbound_history_patterns()

_FEISHU_TOPIC_REPLY_MARKER_RE = re.compile(r"^\s*\d+\s*条话题回复\s*$", re.M)
_FEISHU_TOPIC_NEW_MESSAGE_RE = re.compile(
    r"(?:^|\n)[ \t]*新消息[ \t]*\n(?:[^\n]{1,80}\n)?[ \t]*\d{1,2}:\d{2}[ \t]*\n(?P<message>.+)[ \t]*\Z",
    re.S,
)
_FEISHU_TOPIC_REPLY_SUMMARY = "[Feishu topic history omitted.]"

_FEISHU_ISSUE_CARD_KEY_ALIASES = {
    "work_item_id": ("work_item_id", "workitemid", "工作项id", "工作项", "缺陷id", "问题id", "飞书问题", "meegle id"),
    "title": ("title", "标题", "问题标题", "缺陷标题", "名称"),
    "status": ("status", "状态", "流转状态"),
    "priority": ("priority", "优先级"),
    "severity": ("severity", "严重程度", "严重级别"),
    "assignee": ("assignee", "负责人", "处理人", "经办人"),
    "project": ("project", "项目", "空间"),
}
_FEISHU_ISSUE_CARD_DESCRIPTION_KEYS = ("缺陷描述", "问题描述", "需求描述", "详细描述", "description", "复现步骤", "实际结果", "期望结果")
_FEISHU_ISSUE_CARD_TRIGGER_RE = re.compile(r"(?:转发.*(?:问题|缺陷|工作项|卡片)|(?:飞书|meegle).*(?:问题|缺陷|工作项)|缺陷描述|问题描述)", re.I)
_FEISHU_ISSUE_CARD_FIELD_RE = re.compile(r"^\s*(?P<key>[\w\u4e00-\u9fff /_-]{1,24})\s*[：:]\s*(?P<value>.+?)\s*$")
_FEISHU_ISSUE_CARD_INLINE_ID_RE = re.compile(r"(?:work[_ -]?item[_ -]?id|工作项(?:id)?|缺陷(?:id)?|问题(?:id)?|飞书问题)\s*[：:=# ]+\s*(?P<id>\d{5,})", re.I)


@dataclass(frozen=True)
class FeishuInboundMetadata:
    """Structured Feishu inbound metadata available before text sanitization."""

    message_id: str | None = None
    root_id: str | None = None
    parent_id: str | None = None
    thread_id: str | None = None
    sender_id: str | None = None
    sender_type: str | None = None
    is_bot_sender: bool | None = None
    is_topic: bool = False
    raw_container_id: str | None = None
    receive_time_ms: int | None = None

    @classmethod
    def from_mapping(cls, value: Any) -> "FeishuInboundMetadata | None":
        if value is None:
            return None
        if isinstance(value, cls):
            return value
        if not isinstance(value, dict):
            return None
        return cls(
            message_id=_metadata_str(value.get("message_id")),
            root_id=_metadata_str(value.get("root_id")),
            parent_id=_metadata_str(value.get("parent_id")),
            thread_id=_metadata_str(value.get("thread_id")),
            sender_id=_metadata_str(value.get("sender_id")),
            sender_type=_metadata_str(value.get("sender_type")),
            is_bot_sender=value.get("is_bot_sender") if isinstance(value.get("is_bot_sender"), bool) else None,
            is_topic=bool(value.get("is_topic")),
            raw_container_id=_metadata_str(value.get("raw_container_id")),
            receive_time_ms=_metadata_int(value.get("receive_time_ms")),
        )


@dataclass(frozen=True)
class FeishuBotMessageFingerprint:
    platform: str
    chat_id: str
    thread_id: str | None
    message_id: str | None
    content_hash: str
    normalized_preview_hash: str
    category: str
    created_at_monotonic: float | None = None


@dataclass
class FeishuBotMessageRegistry:
    max_entries: int = 256
    ttl_seconds: float = 900.0
    by_scope: dict[tuple[str, str, str | None], list[FeishuBotMessageFingerprint]] = field(default_factory=dict)

    def record(self, fingerprint: FeishuBotMessageFingerprint) -> None:
        scope = (fingerprint.platform, fingerprint.chat_id, fingerprint.thread_id)
        bucket = self.by_scope.setdefault(scope, [])
        created = fingerprint.created_at_monotonic if fingerprint.created_at_monotonic is not None else time.monotonic()
        bucket.append(
            FeishuBotMessageFingerprint(
                platform=fingerprint.platform,
                chat_id=fingerprint.chat_id,
                thread_id=fingerprint.thread_id,
                message_id=fingerprint.message_id,
                content_hash=fingerprint.content_hash,
                normalized_preview_hash=fingerprint.normalized_preview_hash,
                category=fingerprint.category,
                created_at_monotonic=created,
            )
        )
        self._prune_bucket(scope, now_monotonic=created)

    def prune(self, *, now_monotonic: float | None = None) -> None:
        now = time.monotonic() if now_monotonic is None else float(now_monotonic)
        for scope in list(self.by_scope.keys()):
            self._prune_bucket(scope, now_monotonic=now)

    def contains_content(self, *, platform: Platform | str | None, chat_id: str, thread_id: str | None, content: str) -> bool:
        scope = (_normalize_platform_name(platform), str(chat_id or ""), str(thread_id or "") or None)
        self._prune_bucket(scope)
        bucket = self.by_scope.get(scope, [])
        if not bucket:
            return False
        content_hash = _hash_text(content)
        preview_hash = _hash_text(_normalized_preview_text(content))
        return any(
            item.content_hash == content_hash or item.normalized_preview_hash == preview_hash
            for item in bucket
        )

    def _prune_bucket(self, scope: tuple[str, str, str | None], *, now_monotonic: float | None = None) -> None:
        bucket = self.by_scope.get(scope)
        if not bucket:
            return
        now = time.monotonic() if now_monotonic is None else float(now_monotonic)
        ttl = max(0.0, float(self.ttl_seconds))
        bucket[:] = [
            item for item in bucket
            if item.created_at_monotonic is None or now - item.created_at_monotonic <= ttl
        ]
        if not bucket:
            self.by_scope.pop(scope, None)
            return
        while len(bucket) > self.max_entries:
            removable_index = next((i for i, item in enumerate(bucket) if item.category == "progress"), 0)
            del bucket[removable_index]
        if not bucket:
            self.by_scope.pop(scope, None)


def record_feishu_bot_message_fingerprint(
    registry: FeishuBotMessageRegistry | None,
    *,
    platform: Platform | str | None,
    chat_id: str,
    thread_id: str | None,
    message_id: str | None,
    content: str,
    category: str,
) -> None:
    if registry is None:
        return
    registry.record(
        FeishuBotMessageFingerprint(
            platform=_normalize_platform_name(platform),
            chat_id=str(chat_id or ""),
            thread_id=str(thread_id or "") or None,
            message_id=str(message_id or "") or None,
            content_hash=_hash_text(content),
            normalized_preview_hash=_hash_text(_normalized_preview_text(content)),
            category=str(category or "generic"),
            created_at_monotonic=time.monotonic(),
        )
    )


def _normalize_platform_name(platform: Platform | str | None) -> str:
    if platform == Platform.FEISHU:
        return Platform.FEISHU.value
    return str(platform or "").strip().lower()


def _normalized_preview_text(content: str) -> str:
    normalized = str(content or "").strip().lower()
    normalized = re.sub(r"\d{1,2}:\d{2}", "", normalized)
    normalized = re.sub(r"\(×\d+\)", "", normalized)
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip()


def _hash_text(content: str) -> str:
    return hashlib.sha256(str(content or "").encode("utf-8")).hexdigest()[:16]


def extract_feishu_latest_user_message(text: str) -> str:
    """Return the latest human message slot from sanitized Feishu text."""
    if not text:
        return ""
    summary_index = text.find(_FEISHU_TOPIC_REPLY_SUMMARY)
    if summary_index >= 0:
        return text[:summary_index].strip()
    return text.strip()


def _metadata_str(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _metadata_int(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _is_bot_metadata(metadata: FeishuInboundMetadata | dict[str, Any] | None) -> bool:
    meta = FeishuInboundMetadata.from_mapping(metadata)
    if meta is None:
        return False
    if meta.is_bot_sender is True:
        return True
    return str(meta.sender_type or "").strip().lower() in {"bot", "app"}


def _normalize_issue_card_key(key: str) -> str | None:
    compact = re.sub(r"\s+", "", str(key or "").strip().lower())
    compact = compact.rstrip("：:")
    for canonical, aliases in _FEISHU_ISSUE_CARD_KEY_ALIASES.items():
        if any(compact == re.sub(r"\s+", "", alias.lower()) for alias in aliases):
            return canonical
    return None


def _fold_feishu_issue_card_text(raw: str) -> str | None:
    text = str(raw or "")
    if len(text) < 700 and not any(key in text for key in _FEISHU_ISSUE_CARD_DESCRIPTION_KEYS):
        return None
    if not _FEISHU_ISSUE_CARD_TRIGGER_RE.search(text):
        return None

    fields: dict[str, str] = {}
    description_seen = False
    for line in text.splitlines():
        stripped = line.strip().strip("｜|")
        if not stripped:
            continue
        match = _FEISHU_ISSUE_CARD_FIELD_RE.match(stripped)
        if not match:
            if not fields.get("work_item_id"):
                inline = _FEISHU_ISSUE_CARD_INLINE_ID_RE.search(stripped)
                if inline:
                    fields["work_item_id"] = inline.group("id")
            continue
        key = match.group("key").strip()
        value = match.group("value").strip()
        if not value:
            continue
        if any(key.strip().lower() == item.lower() for item in _FEISHU_ISSUE_CARD_DESCRIPTION_KEYS):
            description_seen = True
            continue
        canonical = _normalize_issue_card_key(key)
        if canonical and canonical not in fields:
            fields[canonical] = value[:240]

    if not fields.get("work_item_id"):
        inline = _FEISHU_ISSUE_CARD_INLINE_ID_RE.search(text)
        if inline:
            fields["work_item_id"] = inline.group("id")
    if not fields.get("title"):
        title_match = re.search(r"(?:标题|问题标题|缺陷标题|title)\s*[：:]\s*(?P<title>[^\n]{4,180})", text, re.I)
        if title_match:
            fields["title"] = title_match.group("title").strip()

    if not (fields.get("work_item_id") or fields.get("title")):
        return None
    if not description_seen and len(text) < 1200:
        return None

    issue_id = fields.get("work_item_id") or "unknown"
    lines = [f"飞书问题 {issue_id}"] if fields.get("work_item_id") else []
    ordered_keys = ("title", "status", "priority", "severity", "assignee", "project")
    labels = {
        "title": "title",
        "status": "status",
        "priority": "priority",
        "severity": "severity",
        "assignee": "assignee",
        "project": "project",
    }
    lines.extend(f"{labels[key]}：{fields[key]}" for key in ordered_keys if fields.get(key))
    lines.append(f"[问题卡片正文已折叠，work_item_id={issue_id}]")
    return "\n".join(lines)


def sanitize_feishu_inbound_text(
    platform: Platform | str | None,
    text: Any,
    *,
    policy: GatewayContextPolicy | None = None,
    max_chars_after_marker: int | None = None,
    bot_display_names: Iterable[str] | None = None,
    record_metrics: bool = False,
    metadata: FeishuInboundMetadata | dict[str, Any] | None = None,
    message_registry: FeishuBotMessageRegistry | None = None,
    registry_scope: dict[str, Any] | None = None,
) -> str:
    """Trim Feishu synthetic topic-history blobs before they enter the agent.

    Feishu gateway events can arrive with the latest user text followed by a
    platform-rendered topic transcript (for example ``28 条话题回复`` plus the
    bot's own busy/status/cron messages).  That transcript is not the user's new
    request, and keeping it verbatim causes self-history context bloat.
    """
    raw = str(text or "")
    if not raw or not _is_feishu(platform):
        return raw
    if _is_bot_metadata(metadata):
        if record_metrics:
            from gateway.context_metrics import record_feishu_topic_sanitized

            record_feishu_topic_sanitized(
                removed_chars=len(raw),
                new_message_extracted=False,
                fingerprint_filtered=False,
            )
        return ""

    folded_issue_card = _fold_feishu_issue_card_text(raw)
    if folded_issue_card is not None:
        if record_metrics:
            from gateway.context_metrics import record_feishu_topic_sanitized

            record_feishu_topic_sanitized(
                removed_chars=max(0, len(raw) - len(folded_issue_card)),
                new_message_extracted=False,
                fingerprint_filtered=False,
            )
        return folded_issue_card

    marker = _FEISHU_TOPIC_REPLY_MARKER_RE.search(raw)
    if not marker:
        return raw

    tail = raw[marker.start():]
    history_patterns = _feishu_inbound_history_patterns(bot_display_names)
    self_history_patterns = _feishu_inbound_self_history_patterns(
        bot_display_names if bot_display_names is not None else _DEFAULT_FEISHU_BOT_DISPLAY_NAMES
    )
    registry_self_history_lines = _filter_registry_self_history_lines(
        lines=tail.splitlines()[1:],
        platform=platform,
        registry=message_registry,
        registry_scope=registry_scope,
    )
    new_message = _latest_feishu_topic_tail_message(tail, history_patterns=history_patterns)
    if not new_message and not any(pattern.search(tail) for pattern in self_history_patterns) and not registry_self_history_lines:
        return raw

    latest = new_message or raw[:marker.start()].strip()
    context_policy = policy or GatewayContextPolicy()
    residual_budget = (
        max_chars_after_marker
        if max_chars_after_marker is not None
        else context_policy.recent_human_max_chars
    )
    residual_budget = max(0, int(residual_budget))
    # Preserve a very small non-bot-looking tail only as a hint. This avoids
    # losing genuinely user-supplied quoted context while still cutting the
    # repeated bot/cron/status transcript that caused the bloat incident.
    kept_lines: list[str] = []
    if not new_message:
        for line in tail.splitlines()[1:]:
            stripped = line.strip()
            if not stripped:
                continue
            if any(pattern.search(stripped) for pattern in history_patterns):
                continue
            if stripped in registry_self_history_lines:
                continue
            if _FEISHU_CONTEXT_FAILURE_PATTERNS[1].search(stripped):
                continue
            if "/compact" in stripped or "/reset" in stripped:
                continue
            if re.fullmatch(r"\d{1,2}:\d{2}", stripped):
                continue
            kept_lines.append(stripped)
            if sum(len(item) + 1 for item in kept_lines) >= residual_budget:
                break

    parts = [latest] if latest else []
    parts.append(_FEISHU_TOPIC_REPLY_SUMMARY)
    if kept_lines:
        parts.append("[Small residual topic context]\n" + "\n".join(kept_lines))
    result = "\n\n".join(part for part in parts if part).strip()
    if record_metrics:
        from gateway.context_metrics import record_feishu_topic_sanitized

        removed_chars = len(raw[:marker.start()]) + len(tail)
        preserved_chars = len(latest) + sum(len(item) for item in kept_lines)
        record_feishu_topic_sanitized(
            removed_chars=max(0, removed_chars - preserved_chars),
            new_message_extracted=bool(new_message),
            fingerprint_filtered=bool(registry_self_history_lines),
        )
    return result


def _filter_registry_self_history_lines(
    *,
    lines: list[str],
    platform: Platform | str | None,
    registry: FeishuBotMessageRegistry | None,
    registry_scope: dict[str, Any] | None,
) -> set[str]:
    if registry is None or not registry_scope:
        return set()
    chat_id = str((registry_scope or {}).get("chat_id") or "").strip()
    if not chat_id:
        return set()
    thread_id = str((registry_scope or {}).get("thread_id") or "").strip() or None
    matched: set[str] = set()
    for raw_line in lines:
        stripped = raw_line.strip()
        if not stripped or re.fullmatch(r"\d{1,2}:\d{2}", stripped):
            continue
        if registry.contains_content(
            platform=platform,
            chat_id=chat_id,
            thread_id=thread_id,
            content=stripped,
        ):
            matched.add(stripped)
    return matched


def _latest_feishu_topic_tail_message(
    tail: str,
    *,
    history_patterns: tuple[re.Pattern[str], ...] = _FEISHU_INBOUND_BOT_HISTORY_PATTERNS,
) -> str | None:
    """Return the newest human message from a Feishu synthetic topic tail.

    Feishu often serializes a topic as: old root message, ``N 条话题回复``, bot
    status replies, then ``新消息`` + human name + timestamp + the actual latest
    user text.  For agent input, the newest human text is the actionable turn;
    the older root request should be recovered from task/session state instead
    of being re-injected as raw transcript.
    """
    match = _FEISHU_TOPIC_NEW_MESSAGE_RE.search(tail or "")
    if not match:
        return None
    candidate = match.group("message").strip()
    if not candidate:
        return None
    if any(pattern.search(candidate) for pattern in history_patterns):
        return None
    return candidate


def _is_feishu(platform: Platform | str | None) -> bool:
    if platform == Platform.FEISHU:
        return True
    return str(platform or "").lower() == Platform.FEISHU.value


def _is_human_mode(progress_mode: str | None) -> bool:
    return (progress_mode or "human").strip().lower() == "human"


def format_feishu_tool_progress(
    platform: Platform | str | None,
    progress_mode: str | None,
    tool_name: Any,
    preview: Any = None,
    args: dict | None = None,
) -> str | None:
    """Return user-facing tool progress text for Feishu human mode.

    Routine internal tools are intentionally invisible in Feishu topics; they
    belong in logs/dashboard traces, not as chat-visible work chatter.  Domain
    tools may still emit one compact human status when useful.
    """
    name = str(tool_name or "").strip()
    if not name:
        return None
    if not (_is_feishu(platform) and _is_human_mode(progress_mode)):
        if preview:
            return f"⚙️ {name}: \"{preview}\""
        return f"⚙️ {name}..."

    if name in _FEISHU_ROUTINE_INTERNAL_TOOL_NAMES:
        return None
    if name.startswith("mcp_feishu_project_"):
        return "🔎 正在查询飞书项目数据..."
    if name.startswith("mcp_feishu_doc_") or name.startswith("feishu_"):
        return "🔎 正在处理飞书数据..."
    # Unknown internal tool names should not leak by default in Feishu human
    # mode.  Let the assistant's own concise decision updates carry meaning.
    return None


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
        # Routine context pressure is an internal operations event. Suppress it
        # only when it is actually a failure/diagnostic blob; keep normal prose
        # that merely mentions context/token intact.
        if (
            failed
            or _FEISHU_CONTEXT_FAILURE_PATTERNS[1].search(text)
            or "这轮上下文接近上限" in text
            or "放不下完整上下文" in text
            or _looks_like_raw_diagnostic_blob(text)
            or any(pattern.search(text) for pattern in _FEISHU_INTERNAL_ERROR_PATTERNS)
        ):
            return _FEISHU_CONTEXT_FAILURE_TEXT if failed else ""
    if re.fullmatch(r"⚙️\s*正在处理请求\.\.\.\s*(?:\(×\d+\))?", text):
        return ""
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
        if failed or _looks_like_raw_diagnostic_blob(text) or any(pattern.search(text) for pattern in _FEISHU_INTERNAL_ERROR_PATTERNS):
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
        # Internal context pressure should trigger auto-compaction, not repeated
        # Feishu topic spam.  Keep final exception text compact when delivery is
        # unavoidable, but avoid the old scary "manual /compact" style.
        return _FEISHU_CONTEXT_FAILURE_TEXT
    if any(pattern.search(raw) for pattern in _FEISHU_FINAL_MODEL_FAILURE_PATTERNS):
        return _FEISHU_MODEL_FAILURE_TEXT
    return _FEISHU_INTERNAL_ERROR_TEXT
