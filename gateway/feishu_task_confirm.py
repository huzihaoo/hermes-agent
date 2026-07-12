"""Shared task-card confirmation resolution for Feishu buttons and text fallback.

All write paths use the same atomic sidecar update so button clicks and plain
text replies remain equivalent and idempotent.
"""
from __future__ import annotations

import fcntl
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

from hermes_cli.config import get_hermes_home
from scripts.vm_task_state_bridge import _atomic_write_json, _load_existing, sidecar_path, safe_task_id

_CONFIRM_TEXT_ALIASES = {
    "确认": {"确认", "同意", "可以", "好的", "好", "ok", "OK", "继续", "接受", "接受边界"},
    "调整": {"调整", "修改", "改一下", "换方案"},
    "取消": {"取消", "不用了", "终止", "中止", "停止"},
    "继续": {"继续", "确认", "可以", "好的", "好", "ok", "OK"},
    "中止": {"中止", "停止", "取消"},
    "接受边界": {"接受边界", "接受", "确认", "可以"},
    "要求补全": {"要求补全", "补全", "补充", "不接受"},
    "方案A": {"方案A", "选A", "A", "a"},
    "方案B": {"方案B", "选B", "B", "b"},
}


def _now_iso() -> str:
    return datetime.now(timezone(timedelta(hours=8))).isoformat()


@contextmanager
def _confirm_lock(task_id: str):
    lock_dir = get_hermes_home() / "locks" / "task-confirms"
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_path = lock_dir / f"{safe_task_id(task_id)}.lock"
    with lock_path.open("w", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _open_confirms(task_card: dict[str, Any]) -> list[dict[str, Any]]:
    items = task_card.get("pending_confirms") if isinstance(task_card.get("pending_confirms"), list) else []
    return [item for item in items if isinstance(item, dict) and item.get("resolved") is None]


_CONFIRM_OPTION_PRESETS = {
    "default": ["确认", "调整", "取消"],
    "plan_ab": ["方案A", "方案B"],
    "continue_stop": ["继续", "中止"],
    "boundary": ["接受边界", "要求补全"],
}


def _options(item: dict[str, Any]) -> list[str]:
    raw = item.get("options")
    if isinstance(raw, list) and raw:
        return [str(value).strip() for value in raw if str(value).strip()]
    preset = str(item.get("preset") or "default").strip()
    return list(_CONFIRM_OPTION_PRESETS.get(preset, _CONFIRM_OPTION_PRESETS["default"]))


def _canonical_choice(raw_choice: str, options: list[str]) -> str:
    normalized = str(raw_choice or "").strip()
    if not normalized:
        return ""
    for option in options:
        if normalized == option:
            return option
    lower = normalized.lower()
    for option in options:
        aliases = _CONFIRM_TEXT_ALIASES.get(option, {option}) | {option}
        if normalized in aliases or lower in {str(alias).lower() for alias in aliases}:
            return option
    return ""


def _find_sidecar_by_context(*, chat_id: str, thread_id: str | None = None) -> tuple[str, Path, dict[str, Any], dict[str, Any]] | None:
    root = get_hermes_home() / "task-state"
    if not root.exists():
        return None
    normalized_chat = str(chat_id or "").strip()
    normalized_thread = str(thread_id or "").strip()
    candidates: list[tuple[float, str, Path, dict[str, Any], dict[str, Any]]] = []
    for path in root.glob("*.json"):
        body = _load_existing(path)
        task_card = body.get("task_card") if isinstance(body.get("task_card"), dict) else None
        if not task_card or not _open_confirms(task_card):
            continue
        if str(task_card.get("chat_id") or "").strip() != normalized_chat:
            continue
        card_thread = str(task_card.get("thread_id") or "").strip()
        if normalized_thread and card_thread and card_thread != normalized_thread:
            continue
        candidates.append((path.stat().st_mtime, path.stem, path, body, task_card))
    if not candidates:
        return None
    _mtime, task_id, path, body, task_card = sorted(candidates, reverse=True)[0]
    return task_id, path, body, task_card


def _clear_card_send_hash(task_card: dict[str, Any]) -> None:
    # Force relay/watch to patch the card after pending_confirms changes.
    task_card.pop("last_sent_hash", None)
    task_card.pop("last_render_hash", None)
    task_card.pop("last_error", None)


def add_pending_confirm(
    *,
    task_id: str,
    confirm_id: str,
    question: str,
    preset: str = "boundary",
    options: list[str] | None = None,
) -> dict[str, Any]:
    """Attach a single-select confirm to an existing task card (boundary node).

    Used by the execution flow when a task hits a boundary-uncertain decision
    (e.g. 受治理 vs 快速原型, 接受边界 vs 要求补全). Reuses the SAME pending_confirms
    structure that buttons + text fallback already resolve, so no new resolution
    path is needed. Idempotent by confirm_id. Clears the card hash so the relay
    re-renders with the new buttons.
    """
    task_id = str(task_id or "").strip()
    confirm_id = str(confirm_id or "").strip()
    if not task_id or not confirm_id:
        return {"ok": False, "added": False, "error": "missing task_id or confirm_id"}
    with _confirm_lock(task_id):
        path = sidecar_path(task_id)
        body = _load_existing(path)
        task_card = body.get("task_card") if isinstance(body.get("task_card"), dict) else {}
        pending = task_card.get("pending_confirms") if isinstance(task_card.get("pending_confirms"), list) else []
        if any(isinstance(it, dict) and str(it.get("id") or "").strip() == confirm_id for it in pending):
            return {"ok": True, "added": False, "duplicate": True, "task_id": task_id, "confirm_id": confirm_id}
        item: dict[str, Any] = {
            "id": confirm_id,
            "question": str(question or "请确认下一步"),
            "preset": str(preset or "boundary"),
            "resolved": None,
        }
        if options:
            item["options"] = [str(o).strip() for o in options if str(o).strip()]
        pending.append(item)
        task_card["pending_confirms"] = pending
        _clear_card_send_hash(task_card)
        body["task_card"] = task_card
        body["updated_at"] = _now_iso()
        _atomic_write_json(path, body)
        return {"ok": True, "added": True, "duplicate": False, "task_id": task_id, "confirm_id": confirm_id}


def resolve_task_confirm(
    *,
    task_id: str,
    confirm_id: str,
    choice: str,
    actor_id: str = "",
    actor_name: str = "",
    source: str = "button",
    event_id: str = "",
) -> dict[str, Any]:
    """Resolve one pending_confirm in a task-state sidecar.

    Idempotence: if the confirm is already resolved, no mutation is made and
    duplicate=True is returned. This is the only write path used by both card
    buttons and text fallback.
    """
    task_id = str(task_id or "").strip()
    confirm_id = str(confirm_id or "").strip()
    if not task_id or not confirm_id:
        return {"ok": False, "changed": False, "error": "missing task_id or confirm_id"}
    with _confirm_lock(task_id):
        path = sidecar_path(task_id)
        body = _load_existing(path)
        task_card = body.get("task_card") if isinstance(body.get("task_card"), dict) else {}
        pending = task_card.get("pending_confirms") if isinstance(task_card.get("pending_confirms"), list) else []
        for item in pending:
            if not isinstance(item, dict) or str(item.get("id") or "").strip() != confirm_id:
                continue
            if item.get("resolved") is not None:
                _clear_card_send_hash(task_card)
                refreshed_at = _now_iso()
                body["task_card"] = task_card
                body["updated_at"] = refreshed_at
                _atomic_write_json(path, body)
                return {"ok": True, "changed": False, "duplicate": True, "task_id": task_id, "confirm_id": confirm_id, "task_card": task_card}
            options = _options(item)
            canonical = _canonical_choice(choice, options)
            if not canonical:
                return {"ok": False, "changed": False, "error": "choice not allowed", "options": options}
            resolved = {
                "choice": canonical,
                "raw_choice": str(choice or "").strip(),
                "resolved_at": _now_iso(),
                "source": source,
                "actor_id": str(actor_id or "").strip(),
                "actor_name": str(actor_name or "").strip(),
            }
            if event_id:
                resolved["event_id"] = str(event_id)
            item["resolved"] = resolved
            _clear_card_send_hash(task_card)
            task_card["pending_confirms"] = pending
            body["task_card"] = task_card
            body["updated_at"] = resolved["resolved_at"]
            events = body.get("recent_events") if isinstance(body.get("recent_events"), list) else []
            events.append({"ts": resolved["resolved_at"], "phase": body.get("current_phase"), "summary": f"确认项 {confirm_id} -> {canonical}"})
            body["recent_events"] = events[-50:]
            _atomic_write_json(path, body)
            return {"ok": True, "changed": True, "duplicate": False, "task_id": task_id, "confirm_id": confirm_id, "choice": canonical, "task_card": task_card}
    return {"ok": False, "changed": False, "error": "confirm not found", "task_id": task_id, "confirm_id": confirm_id}


def resolve_task_confirm_by_text(
    *,
    chat_id: str,
    thread_id: str | None,
    text: str,
    actor_id: str = "",
    actor_name: str = "",
    event_id: str = "",
) -> dict[str, Any]:
    found = _find_sidecar_by_context(chat_id=chat_id, thread_id=thread_id)
    if not found:
        return {"ok": False, "changed": False, "error": "no open confirm for context"}
    task_id, _path, _body, task_card = found
    matches: list[tuple[str, str]] = []
    for item in _open_confirms(task_card):
        options = _options(item)
        canonical = _canonical_choice(text, options)
        if canonical:
            matches.append((str(item.get("id") or "confirm"), canonical))
    if not matches:
        return {"ok": False, "changed": False, "error": "text does not match confirm options", "task_id": task_id}
    if len(matches) > 1:
        return {"ok": False, "changed": False, "error": "ambiguous confirm text", "task_id": task_id}
    confirm_id, canonical = matches[0]
    return resolve_task_confirm(
        task_id=task_id,
        confirm_id=confirm_id,
        choice=canonical,
        actor_id=actor_id,
        actor_name=actor_name,
        source="text",
        event_id=event_id,
    )
