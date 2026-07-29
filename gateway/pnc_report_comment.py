"""Report-ready auto-comment executor for G1Q3 RCA completion.

Safety contract:
- The only Feishu Project write is ``meegle comment add``.
- Disabled unless ``HERMES_G1Q3_REPORT_COMMENT=1``.
- Idempotent by per-issue ledger plus existing-comment signature scan.
- Daily cap via ``HERMES_G1Q3_REPORT_COMMENT_DAILY_CAP`` (default 50).
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

MeegleRunner = Callable[[list[str]], tuple[int, str, str]]

ENABLE_ENV = "HERMES_G1Q3_REPORT_COMMENT"
DAILY_CAP_ENV = "HERMES_G1Q3_REPORT_COMMENT_DAILY_CAP"
NOT_BEFORE_ENV = "HERMES_G1Q3_REPORT_COMMENT_NOT_BEFORE"
DEFAULT_DAILY_CAP = 50
REPEAT_WINDOW_DAYS = 30
SIGNATURE_PREFIX = "【G1Q3 RCA 机器人报告】"
SIGNATURE_SENTENCE = "G1Q3 RCA 报告已生成，需人工复核后结案。"


def enabled() -> bool:
    return os.getenv(ENABLE_ENV, "").strip().lower() in {"1", "true", "yes", "on"}


def daily_cap() -> int:
    raw = os.getenv(DAILY_CAP_ENV, "").strip()
    try:
        return max(0, int(raw)) if raw else DEFAULT_DAILY_CAP
    except ValueError:
        return DEFAULT_DAILY_CAP


def parse_not_before() -> datetime | None:
    raw = os.getenv(NOT_BEFORE_ENV, "").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def is_after_not_before(value: str | None) -> bool:
    gate = parse_not_before()
    if gate is None:
        return True
    if not value:
        return False
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return False
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc) >= gate


def build_report_comment(*, work_item_id: str, title: str = "", rca_status: dict[str, Any] | None = None, boundaries: list[str] | None = None) -> dict[str, str]:
    status = rca_status if isinstance(rca_status, dict) else {}
    lines = [
        SIGNATURE_PREFIX + SIGNATURE_SENTENCE,
        "",
    ]
    if work_item_id:
        lines.append(f"问题：{work_item_id}" + (f"（{title}）" if title else ""))
    report_status = str(status.get("report_status") or "").strip()
    attribution = str(status.get("attribution_status") or "").strip()
    cause = str(status.get("candidate_cause") or "").strip()
    responsibility = str(status.get("candidate_responsibility") or "").strip()
    causal_chain = str(status.get("causal_chain") or cause).strip()
    evidence = str(status.get("evidence") or "").strip()
    if cause:
        lines.append(f"归因结论：{cause}")
    if responsibility:
        lines.append(f"责任模块：{responsibility}")
    if causal_chain:
        lines.append(f"因果关系：{causal_chain}")
    clean_boundaries = [str(item).strip() for item in (boundaries or []) if str(item).strip()]
    evidence = evidence or ("；".join(clean_boundaries[:3]) if clean_boundaries else "未提供可核验的关键证据。")
    lines.append(f"关键证据：{evidence}")
    return {"signature": SIGNATURE_SENTENCE, "content": "\n".join(lines)}


def _ledger_path(ledger_dir: Path) -> Path:
    return ledger_dir / "g1q3_report_comments.json"


def _load_ledger(ledger_dir: Path) -> dict[str, Any]:
    try:
        payload = json.loads(_ledger_path(ledger_dir).read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _now(now: datetime | None) -> datetime:
    return now or datetime.now(timezone.utc)


def _recently_commented(ledger: dict[str, Any], work_item_id: str, now: datetime) -> bool:
    entry = (ledger.get("issues") or {}).get(work_item_id)
    if not isinstance(entry, dict):
        return False
    try:
        posted_at = datetime.fromisoformat(str(entry.get("posted_at")))
    except (TypeError, ValueError):
        return False
    return (now - posted_at).days < REPEAT_WINDOW_DAYS


def _today_count(ledger: dict[str, Any], now: datetime) -> int:
    daily = ledger.get("daily") or {}
    return int(daily.get(now.date().isoformat()) or 0)


def _record_post(ledger_dir: Path, ledger: dict[str, Any], work_item_id: str, task_id: str, now: datetime) -> None:
    ledger.setdefault("issues", {})[work_item_id] = {"posted_at": now.isoformat(), "task_id": task_id}
    daily = ledger.setdefault("daily", {})
    day = now.date().isoformat()
    daily[day] = int(daily.get(day) or 0) + 1
    ledger_dir.mkdir(parents=True, exist_ok=True)
    _ledger_path(ledger_dir).write_text(json.dumps(ledger, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _existing_comment_has_signature(*, project_key: str, work_item_id: str, signature: str, runner: MeegleRunner) -> bool | None:
    try:
        rc, out, _err = runner(["comment", "list", "--project-key", project_key, "--work-item-id", work_item_id, "--format", "json"])
    except Exception:
        return None
    if rc != 0:
        return None
    return signature in (out or "")


def _record_only_comment_intents(
    *,
    project_key: str,
    work_item_id: str,
    task_id: str,
    plan: dict[str, str],
) -> dict[str, Any] | None:
    from gateway.record_only.runtime import get_record_only_transport

    recorder = get_record_only_transport("gateway.pnc_report_comment")
    if recorder is None:
        return None
    common = {
        "platform": "feishu_project",
        "destination_kind": "work_item",
        "destination_id": str(work_item_id),
        "task_id": str(task_id),
        "reply_mode": "none",
        "update_mode": "none",
    }
    list_intent = recorder.record(
        operation="project_comment_list",
        payload_type="query",
        payload={"signature": plan["signature"]},
        caller_dedupe_key=f"g1q3-report-comment:list:{project_key}:{work_item_id}:{task_id}",
        metadata={"project_key": str(project_key), "intent": "dedupe_check"},
        **common,
    )
    add_intent = recorder.record(
        operation="project_comment_add",
        payload_type="text",
        payload=plan["content"],
        caller_dedupe_key=f"g1q3-report-comment:add:{project_key}:{work_item_id}:{task_id}",
        metadata={
            "project_key": str(project_key),
            "conditional_on": "comment_signature_absent",
            "intent": "conditional_add",
        },
        **common,
    )
    return {
        "action": "recorded_intents",
        "posted": False,
        "list_record_id": list_intent.record_id,
        "add_record_id": add_intent.record_id,
        "duplicate": list_intent.duplicate and add_intent.duplicate,
        **recorder.safety_status(),
    }


def maybe_comment_report_ready(
    *,
    project_key: str,
    work_item_id: str,
    task_id: str,
    title: str = "",
    rca_status: dict[str, Any] | None = None,
    boundaries: list[str] | None = None,
    ledger_dir: str | Path,
    meegle_runner: MeegleRunner | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Plan/post report-ready comment. Never raises."""
    try:
        plan = build_report_comment(work_item_id=work_item_id, title=title, rca_status=rca_status, boundaries=boundaries)
        result: dict[str, Any] = {
            "work_item_id": str(work_item_id),
            "task_id": str(task_id),
            "comment_signature": plan["signature"],
            "comment_content": plan["content"],
        }
        if not enabled():
            result["action"] = "planned"
            result["reason"] = f"{ENABLE_ENV} not enabled; plan only"
            return result
        if not project_key or not work_item_id:
            result["action"] = "comment_failed"
            result["reason"] = "missing project_key or work_item_id"
            return result
        recorded = _record_only_comment_intents(
            project_key=str(project_key),
            work_item_id=str(work_item_id),
            task_id=str(task_id),
            plan=plan,
        )
        if recorded is not None:
            result.update(recorded)
            return result
        result["action"] = "skipped_superseded"
        result["reason"] = (
            "canonical RCA delivery dispatcher exclusively owns Feishu comments"
        )
        return result
    except Exception as exc:
        return {"action": "comment_failed", "work_item_id": str(work_item_id), "task_id": str(task_id), "reason": f"{type(exc).__name__}: {exc}"[:200]}
