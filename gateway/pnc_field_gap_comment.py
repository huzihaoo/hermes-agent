"""Field-gap auto-comment executor for G1Q3 RCA intake (S8).

When host-side intake reads the Feishu issue successfully but a required
business field is missing/invalid (问题数据地址_PDCL, 问题发生frameid), this
module prepares — and, only when explicitly enabled, posts — a Feishu Project
comment asking the issue owner to fill the field, closing the field-quality
loop that gates rollout (only ~2/10 issues carry a valid PDCL today).

Safety contract:
- The ONLY Feishu write this module can perform is ``meegle comment add``.
- Default is plan-only: unless ``HERMES_G1Q3_FIELD_GAP_COMMENT=1`` the plan
  is returned/receipted for operator review and nothing is written.
- Idempotent twice over: a per-issue ledger entry (30-day window) plus a
  scan of existing comments for this template's signature sentence.
- Daily cap via ``HERMES_G1Q3_FIELD_GAP_COMMENT_DAILY_CAP`` (default 10).
- Comment text is plain markdown; no hidden markers (matches the writeback
  plan convention). Owner is named in text; real @mention needs lark_user_id
  lookup and stays out of v1.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from gateway.feishu_mention import build_at_mention, resolve_display_name

MeegleRunner = Callable[[list[str]], tuple[int, str, str]]

ENABLE_ENV = "HERMES_G1Q3_FIELD_GAP_COMMENT"
DAILY_CAP_ENV = "HERMES_G1Q3_FIELD_GAP_COMMENT_DAILY_CAP"
DEFAULT_DAILY_CAP = 10
REPEAT_WINDOW_DAYS = 30

# Signature sentences double as content-based dedup keys: if any existing
# comment contains the signature, we never post again.
_TEMPLATES = {
    "issue_field_missing_remote_data_reference": {
        "signature": "缺少可远程读取的数据引用",
        "body": (
            "【G1Q3 RCA 机器人提醒】本问题卡片缺少可远程读取的 event/clip 数据引用，自动根因分析无法启动。\n\n"
            "请{owner}在「问题数据地址_PDCL」字段补充可解析的 event/clip 地址，地址中必须包含明确的 event UUID 或 clip UUID。\n\n"
            "该字段只用于提取远程读取引用，RCA 不会执行 MDI 下载。新建问题单由 Kafka 自动受理；"
            "已建单补齐字段后，可在固定群（HERMES_RCA_MANUAL_CHAT_IDS 当前启用子集）真实 @小助手并发送“分析/重跑 + 完整问题单 URL”。"
            "普通 URL、未 @ 或私聊仍只读；人工触发结果回到原任务话题。"
        ),
    },
    "issue_field_invalid_remote_data_reference": {
        "signature": "问题数据地址_PDCL 无法解析为远程数据引用",
        "body": (
            "【G1Q3 RCA 机器人提醒】本问题卡片的「问题数据地址_PDCL」无法解析为 RemoteEventReader/RemoteClipReader 引用。\n\n"
            "请{owner}补充明确的 event UUID 或 clip UUID；仅 ticket、NAS 路径、回放命令和 raw/group/eventset 地址当前不能进入自动 RCA。\n\n"
            "新建问题单由 Kafka 自动受理；已建单补齐字段后，可在固定群（HERMES_RCA_MANUAL_CHAT_IDS 当前启用子集）"
            "真实 @小助手并发送“分析/重跑 + 完整问题单 URL”。普通 URL、未 @ 或私聊仍只读；人工触发结果回到原任务话题。"
        ),
    },
    "missing_frame_id": {
        "signature": "缺少 问题发生frameid",
        "body": (
            "【G1Q3 RCA 机器人提醒】本问题卡片缺少 问题发生frameid，数据已就绪但无法定位触发帧，自动根因分析停在对齐前。\n\n"
            "请{owner}在「问题发生frameid」字段填写大于 0 的触发帧号，或测试打点时间（格式：YYYY-MM-DD HH:MM:SS / YYYYMMDD, HH:MM:SS）。\n\n新建问题单由 Kafka 自动受理；"
            "已建单补齐字段后，可在固定群（HERMES_RCA_MANUAL_CHAT_IDS 当前启用子集）真实 @小助手并发送“分析/重跑 + 完整问题单 URL”。"
            "普通 URL、未 @ 或私聊仍只读；人工触发结果回到原任务话题。"
        ),
    },
    "missing_frame_id:replay_cmd": {
        "signature": "问题发生frameid 填成了回放命令",
        "body": "【G1Q3 RCA 机器人提醒】本问题卡片的「问题发生frameid」字段像是填入了 cyber_recorder/数据命令，该字段只接受触发帧号或测试打点时间。\n\n请{owner}把数据/回放相关内容移到「问题数据地址_PDCL」或描述字段，并在「问题发生frameid」填写大于 0 的数字帧号，或 YYYY-MM-DD HH:MM:SS / YYYYMMDD, HH:MM:SS 格式的时间。新建问题单由 Kafka 自动受理；已建单补齐字段后，可在固定群（HERMES_RCA_MANUAL_CHAT_IDS 当前启用子集）真实 @小助手并发送“分析/重跑 + 完整问题单 URL”。普通 URL、未 @ 或私聊仍只读；人工触发结果回到原任务话题。",
    },
}

# Keep historical blocker identifiers readable while projecting the current
# remote-read-only contract. They are compatibility keys, not download paths.
_LEGACY_TEMPLATE_ALIASES = {
    "issue_field_missing_pdcl_download_cmd": "issue_field_missing_remote_data_reference",
    "issue_field_invalid_pdcl_download_cmd": "issue_field_invalid_remote_data_reference",
}


def _enabled() -> bool:
    return os.getenv(ENABLE_ENV, "").strip().lower() in {"1", "true", "yes", "on"}


def _daily_cap() -> int:
    raw = os.getenv(DAILY_CAP_ENV, "").strip()
    try:
        return max(0, int(raw)) if raw else DEFAULT_DAILY_CAP
    except ValueError:
        return DEFAULT_DAILY_CAP


def _owner_text(owners: list[str] | None, owner_open_ids: list[str] | None) -> str:
    owner_names = [str(o).strip() for o in (owners or []) if str(o).strip()]
    mentions: list[str] = []
    for idx, open_id in enumerate(owner_open_ids or []):
        open_id = str(open_id or "").strip()
        if not open_id:
            continue
        label = owner_names[idx] if idx < len(owner_names) else (resolve_display_name(open_id) or "")
        mention = build_at_mention(open_id, label)
        if mention:
            mentions.append(mention)
    if mentions:
        return " " + "、".join(mentions[:3]) + " "
    if owner_names:
        return " " + "、".join(owner_names[:3]) + " "
    return ""


def build_field_gap_comment(
    blocker_kind: str,
    owners: list[str] | None = None,
    *,
    sub_kind: str = "",
    owner_open_ids: list[str] | None = None,
) -> dict[str, str] | None:
    """Render the comment plan for a supported field-gap blocker, else None."""
    kind = str(blocker_kind or "").strip()
    sub = str(sub_kind or "").strip()
    if kind == "issue_field_invalid_pdcl_download_cmd" and sub == "empty":
        kind = "issue_field_missing_remote_data_reference"
    else:
        kind = _LEGACY_TEMPLATE_ALIASES.get(kind, kind)
    template = _TEMPLATES.get(f"{kind}:{sub}") if sub else None
    if template is None:
        template = _TEMPLATES.get(kind)
    if template is None:
        return None
    return {
        "signature": template["signature"],
        "content": template["body"].format(owner=_owner_text(owners, owner_open_ids)),
    }


def _ledger_path(ledger_dir: Path) -> Path:
    return ledger_dir / "g1q3_field_gap_comments.json"


def _load_ledger(ledger_dir: Path) -> dict[str, Any]:
    try:
        payload = json.loads(_ledger_path(ledger_dir).read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _now(now: datetime | None) -> datetime:
    return now or datetime.now(timezone.utc)


def _recently_commented(ledger: dict[str, Any], work_item_id: str, kind: str, now: datetime) -> bool:
    entry = (ledger.get("issues") or {}).get(f"{work_item_id}:{kind}")
    if not isinstance(entry, dict):
        return False
    try:
        posted_at = datetime.fromisoformat(str(entry.get("posted_at")))
    except (TypeError, ValueError):
        return False
    return (now - posted_at).days < REPEAT_WINDOW_DAYS


def _today_count(ledger: dict[str, Any], now: datetime) -> int:
    day = now.date().isoformat()
    daily = ledger.get("daily") or {}
    return int(daily.get(day) or 0)


def _record_post(ledger_dir: Path, ledger: dict[str, Any], work_item_id: str, kind: str, now: datetime) -> None:
    issues = ledger.setdefault("issues", {})
    issues[f"{work_item_id}:{kind}"] = {"posted_at": now.isoformat()}
    daily = ledger.setdefault("daily", {})
    day = now.date().isoformat()
    daily[day] = int(daily.get(day) or 0) + 1
    for stale in [d for d in daily if d < (now.date().isoformat()[:8] + "01")][:-31]:
        daily.pop(stale, None)
    ledger_dir.mkdir(parents=True, exist_ok=True)
    _ledger_path(ledger_dir).write_text(json.dumps(ledger, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _existing_comment_has_signature(
    *, project_key: str, work_item_id: str, signature: str, runner: MeegleRunner,
) -> bool | None:
    """True/False when comments were listed; None when the read failed."""
    try:
        rc, out, _err = runner(["comment", "list", "--project-key", project_key, "--work-item-id", work_item_id, "--format", "json"])
    except Exception:
        return None
    if rc != 0:
        return None
    return signature in (out or "")


def maybe_comment_field_gap(
    *,
    project_key: str,
    work_item_id: str,
    blocker_kind: str,
    owners: list[str] | None = None,
    sub_kind: str = "",
    owner_open_ids: list[str] | None = None,
    ledger_dir: str | Path,
    meegle_runner: MeegleRunner | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Plan (and optionally post) a field-gap comment. Never raises.

    Returns ``{"action": planned|posted|skipped_recent|skipped_existing|
    skipped_cap|comment_failed|unsupported_kind, ...}``.
    """
    try:
        plan = build_field_gap_comment(blocker_kind, owners, sub_kind=sub_kind, owner_open_ids=owner_open_ids)
        if plan is None:
            return {"action": "unsupported_kind", "blocker_kind": blocker_kind}
        result: dict[str, Any] = {
            "blocker_kind": blocker_kind,
            "sub_kind": str(sub_kind or ""),
            "work_item_id": str(work_item_id),
            "comment_signature": plan["signature"],
            "comment_content": plan["content"],
        }
        if not _enabled():
            result["action"] = "planned"
            result["reason"] = f"{ENABLE_ENV} not enabled; plan only"
            return result

        if meegle_runner is None:
            from gateway.pnc_issue_context import default_meegle_runner
            meegle_runner = default_meegle_runner

        current = _now(now)
        ledger_dir = Path(ledger_dir)
        ledger = _load_ledger(ledger_dir)
        if _recently_commented(ledger, str(work_item_id), blocker_kind, current):
            result["action"] = "skipped_recent"
            return result
        if _today_count(ledger, current) >= _daily_cap():
            result["action"] = "skipped_cap"
            return result
        has_existing = _existing_comment_has_signature(
            project_key=str(project_key), work_item_id=str(work_item_id),
            signature=plan["signature"], runner=meegle_runner,
        )
        if has_existing is None:
            # Fail closed: cannot verify dedup, do not write.
            result["action"] = "comment_failed"
            result["reason"] = "comment list unreadable; refusing to post without dedup check"
            return result
        if has_existing:
            result["action"] = "skipped_existing"
            _record_post(ledger_dir, ledger, str(work_item_id), blocker_kind, current)
            return result

        rc, out, err = meegle_runner([
            "comment", "add",
            "--project-key", str(project_key),
            "--work-item-id", str(work_item_id),
            "--content", plan["content"],
        ])
        if rc != 0:
            result["action"] = "comment_failed"
            result["reason"] = str(err or out or "meegle comment add failed")[:200]
            return result
        _record_post(ledger_dir, ledger, str(work_item_id), blocker_kind, current)
        result["action"] = "posted"
        return result
    except Exception as exc:
        return {"action": "comment_failed", "blocker_kind": blocker_kind, "reason": f"{type(exc).__name__}: {exc}"[:200]}
