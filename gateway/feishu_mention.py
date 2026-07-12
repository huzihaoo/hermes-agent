"""Originator @mention helpers for Feishu task-card notifications.

Pure helpers (one filesystem read for the name map, no network, no state
mutation) used to build a *separate* fresh text reply that @-mentions the task
originator when a task needs the human to act (need_input / awaiting_user /
abandoned).  This exists because Feishu interactive-card *patches* do not fire a
push notification — only a freshly-sent message does.  The one-card policy keeps
patching the status card silently; this module produces the one extra text ping
that actually reaches the originator.

The mention uses Feishu's native text @ format ``<at user_id="ou_xxx">名字</at>``
which renders as a real, notifying mention when the message is sent as msg_type
"text".  Keep the surrounding text free of markdown so the outbound payload
builder routes it as text (not post), otherwise the raw <at> tag is mangled.
"""
from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Any


# Markdown-ish fragments that would make the Feishu outbound builder route the
# message as a "post" (rich) payload, where a raw <at> tag is NOT rendered as a
# notifying mention.  We strip these so the ping stays msg_type "text".
_MD_STRIP_RES = (
    (re.compile(r"`+"), ""),              # backticks (inline code / paths)
    (re.compile(r"\*\*([^*]+)\*\*"), r"\1"),  # bold
    (re.compile(r"(?<!\w)\*([^*\n]+)\*(?!\w)"), r"\1"),  # italics
    (re.compile(r"^\s*#{1,6}\s+", re.M), ""),  # headings
    (re.compile(r"^\s*>\s+", re.M), ""),        # blockquote
    (re.compile(r"\[([^\]]+)\]\([^)]+\)"), r"\1"),  # links -> label
)


def _plain(text: str) -> str:
    """Reduce a reason string to plain text safe for a Feishu text-mode @ ping.

    Strips inline markdown that would otherwise reroute the message to post-type
    (which silently breaks the <at> notification).  Also flattens list-leading
    "- "/"* "/"N. " markers and newlines into inline separators.
    """
    out = str(text or "").strip()
    if not out:
        return ""
    for pattern, repl in _MD_STRIP_RES:
        out = pattern.sub(repl, out)
    # Flatten any line that begins like a markdown list item, and join lines.
    lines = [re.sub(r"^\s*(?:[-*]|\d+\.)\s+", "", ln).strip() for ln in out.splitlines()]
    return "；".join(ln for ln in lines if ln)


def _roles_path() -> Path:
    """Canonical roles/identity map (open_id -> display name under user_id_mapping).

    Resolved via get_hermes_home() so it honors HERMES_HOME overrides (tests,
    alternate runtimes); falls back to ~/.hermes when the config import is
    unavailable.  Same file tools/permission_policy.py reads.
    """
    try:
        from hermes_cli.config import get_hermes_home

        home = Path(get_hermes_home())
    except Exception:
        home = Path.home() / ".hermes"
    return home / "config" / "user-roles.json"


def resolve_originator_open_id(meta: dict[str, Any] | None) -> str:
    """Extract the task originator's Feishu open_id from a shared-state meta.

    Field name differs by business line: integration_tools writes ``requester``
    while g1q3-rca writes ``user_id``; some older paths nest it under
    ``source.user_id``.  Returns "" if none is present.
    """
    if not isinstance(meta, dict):
        return ""
    for value in (
        meta.get("requester"),
        meta.get("user_id"),
        (meta.get("source") or {}).get("user_id") if isinstance(meta.get("source"), dict) else None,
    ):
        text = str(value or "").strip()
        # Only Feishu user open_ids are valid mention targets; skip session
        # keys like "agent:main:main" that also live in requester-ish fields.
        if text.startswith("ou_"):
            return text
    return ""


@lru_cache(maxsize=8)
def _load_user_id_mapping_cached(path_str: str, mtime_ns: int) -> dict[str, str]:
    try:
        cfg = json.loads(Path(path_str).read_text(encoding="utf-8"))
    except Exception:
        return {}
    mapping = cfg.get("user_id_mapping") if isinstance(cfg, dict) else None
    return {str(k): str(v) for k, v in mapping.items()} if isinstance(mapping, dict) else {}


def _load_user_id_mapping() -> dict[str, str]:
    path = _roles_path()
    try:
        mtime_ns = path.stat().st_mtime_ns
    except OSError:
        return {}
    return _load_user_id_mapping_cached(str(path), mtime_ns)


def resolve_display_name(open_id: str) -> str:
    """Resolve a Feishu open_id to a display name via user_id_mapping; "" if unknown."""
    open_id = str(open_id or "").strip()
    if not open_id:
        return ""
    return _load_user_id_mapping().get(open_id, "")


def build_at_mention(open_id: str, name: str = "") -> str:
    """Build a Feishu native text @mention tag, or "" when no open_id is known.

    When the display name is unknown we still emit the tag with empty inner text;
    Feishu auto-resolves the mentioned user's name from its directory, so the
    notification still fires and renders a real name.
    """
    open_id = str(open_id or "").strip()
    if not open_id:
        return ""
    label = str(name or "").strip()
    return f'<at user_id="{open_id}">{label}</at>' if label else f'<at user_id="{open_id}"></at>'


def compute_notify_key(*, user_state: str, transition_marker: str, pending_confirm_ids: list[str] | None = None, extra: str = "") -> str:
    """A stable key identifying one human-action transition.

    Re-pings only when this key changes, so the same waiting state is announced
    exactly once.

    IMPORTANT: ``transition_marker`` must be a *semantic* transition signal — the
    shared-state ``state`` plus its ``latest_summary`` — NOT ``meta.updated_at``.
    update_task_state bumps updated_at on every upsert (log appends, passive
    re-syncs), so keying on it would re-@ the originator on writes that carry no
    new ask.  state+latest_summary only changes on a real state-write event.
    """
    confirms = ",".join(sorted(str(c) for c in (pending_confirm_ids or [])))
    return "|".join([str(user_state or ""), str(transition_marker or ""), confirms, str(extra or "")])


def build_need_input_notify_text(
    *,
    mention: str,
    reason: str,
    task_id: str,
    kind: str = "need_input",
) -> str:
    """Compose the plain-text @originator ping.

    Must stay markdown-free (no -, *, #, `, [](), >) so the outbound payload
    builder sends it as msg_type "text" and the <at> tag renders/notifies.
    """
    lead = mention or "（未识别到发起人）"
    reason_line = _plain(reason)
    if kind == "abandoned":
        body = "这条任务等待补充已超时，先暂时关闭了。如果还需要处理，回复本话题补充信息后我再继续。"
    elif kind == "confirm":
        body = f"这条任务需要你确认下一步：{reason_line}" if reason_line else "这条任务需要你确认下一步，回复本话题或点卡片按钮即可。"
    else:
        body = f"这条任务需要你补充：{reason_line}" if reason_line else "这条任务需要你补充输入才能继续。"
    tail = "回复本话题即可继续。" if kind != "abandoned" else ""
    track = f"追踪号 {task_id}" if task_id else ""
    parts = [f"{lead} {body}".strip(), tail, track]
    return "\n".join(p for p in parts if p)
