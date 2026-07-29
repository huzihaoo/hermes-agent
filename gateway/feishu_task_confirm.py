"""Shared task-card confirmation resolution for Feishu buttons and text fallback.

All write paths use the same atomic sidecar update so button clicks and plain
text replies remain equivalent and idempotent.
"""
from __future__ import annotations

import fcntl
import hashlib
import json
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
    "追认": {"追认", "确认结论", "认可结论", "认可", "通过"},
    "更正": {"更正", "撤回", "撤回结论", "作废", "否定"},
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
    # This preset is deliberately named by semantic intent.  Its resolver is
    # backed by the immutable RCA adjudication ledger, not by card text.
    "rca_candidate_conclusion_review": ["追认", "更正"],
}

RCA_CANDIDATE_REVIEW_PRESET = "rca_candidate_conclusion_review"
RCA_CANDIDATE_REVIEW_SCHEMA_VERSION = "g1q3_rca_candidate_review_confirm_v1"
RCA_CANDIDATE_REVIEW_CARD_RESPONSE_MODE = "durable_patch_only"


def _semantic_card_response_fields(semantic: Any) -> dict[str, str]:
    if (
        isinstance(semantic, dict)
        and semantic.get("kind") == RCA_CANDIDATE_REVIEW_PRESET
    ):
        return {
            "card_response_mode": RCA_CANDIDATE_REVIEW_CARD_RESPONSE_MODE,
        }
    return {}


def rca_candidate_confirm_id(
    *,
    business_key: str,
    generation: int,
    work_item_id: str,
    original_effect_key: str,
) -> str:
    """Return the stable identity for one DB-published candidate review.

    Only immutable delivery identity participates in the digest.  In
    particular, the human-facing question and card text are not identity
    inputs, so a relay retry cannot create a second confirmation slot.
    """
    material = {
        "business_key": str(business_key or "").strip(),
        "generation": int(generation),
        "work_item_id": str(work_item_id or "").strip(),
        "original_effect_key": str(original_effect_key or "").strip(),
    }
    if not all(material.values()) or material["generation"] < 1:
        raise ValueError("rca candidate confirmation identity is incomplete")
    encoded = json.dumps(material, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return "rca-candidate-review-v1-" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _candidate_value(candidate: Any, key: str, default: Any = "") -> Any:
    if isinstance(candidate, dict):
        return candidate.get(key, default)
    return getattr(candidate, key, default)


def add_rca_candidate_conclusion_confirm(
    *,
    task_id: str,
    candidate: Any,
) -> dict[str, Any]:
    """Add a review confirm from a *DB-proven* queue item.

    The caller (the completion relay) must obtain ``candidate`` from
    ``RcaDeliveryStore.list_conclusion_review_queue``.  This helper accepts
    only the queue item's immutable identity and stores that proof alongside
    the pending confirm; no card conclusion is used to qualify it.
    """
    business_key = str(_candidate_value(candidate, "business_key") or "").strip()
    generation_raw = _candidate_value(candidate, "generation")
    work_item_id = str(_candidate_value(candidate, "work_item_id") or "").strip()
    original_effect_key = str(_candidate_value(candidate, "original_effect_key") or "").strip()
    try:
        generation = int(generation_raw)
    except (TypeError, ValueError) as exc:
        raise ValueError("rca candidate generation is invalid") from exc
    confirm_id = rca_candidate_confirm_id(
        business_key=business_key,
        generation=generation,
        work_item_id=work_item_id,
        original_effect_key=original_effect_key,
    )
    conclusion = str(_candidate_value(candidate, "conclusion") or "").strip()
    responsibility = str(_candidate_value(candidate, "responsibility_domain") or "").strip()
    if not conclusion or not responsibility:
        raise ValueError("rca candidate review material is incomplete")
    semantic = {
        "schema_version": RCA_CANDIDATE_REVIEW_SCHEMA_VERSION,
        "kind": RCA_CANDIDATE_REVIEW_PRESET,
        "business_key": business_key,
        "generation": generation,
        "work_item_id": work_item_id,
        "original_effect_key": original_effect_key,
        "completed_at": str(_candidate_value(candidate, "completed_at") or "").strip(),
        # These are copied for display/audit only.  The adjudication store
        # revalidates the candidate before it creates an immutable effect.
        "candidate_conclusion": conclusion,
        "responsibility_domain": responsibility,
    }
    question = f"候选结论（{work_item_id}）：{conclusion}；责任域：{responsibility}。请选择是否追认。"
    return add_pending_confirm(
        task_id=task_id,
        confirm_id=confirm_id,
        question=question,
        preset=RCA_CANDIDATE_REVIEW_PRESET,
        semantic=semantic,
    )


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
    semantic: dict[str, Any] | None = None,
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
        existing = next(
            (
                it
                for it in pending
                if isinstance(it, dict)
                and str(it.get("id") or "").strip() == confirm_id
            ),
            None,
        )
        if existing is not None:
            if semantic is not None and existing.get("semantic") != semantic:
                return {
                    "ok": False,
                    "added": False,
                    "duplicate": False,
                    "error": "confirm semantic identity conflict",
                    "task_id": task_id,
                    "confirm_id": confirm_id,
                }
            return {"ok": True, "added": False, "duplicate": True, "task_id": task_id, "confirm_id": confirm_id}
        item: dict[str, Any] = {
            "id": confirm_id,
            "question": str(question or "请确认下一步"),
            "preset": str(preset or "boundary"),
            "resolved": None,
        }
        if options:
            item["options"] = [str(o).strip() for o in options if str(o).strip()]
        if semantic is not None:
            item["semantic"] = dict(semantic)
        pending.append(item)
        task_card["pending_confirms"] = pending
        _clear_card_send_hash(task_card)
        body["task_card"] = task_card
        body["updated_at"] = _now_iso()
        _atomic_write_json(path, body)
        return {"ok": True, "added": True, "duplicate": False, "task_id": task_id, "confirm_id": confirm_id}


def _semantic_rca_resolution(
    *,
    confirm_id: str,
    task_card: dict[str, Any],
    semantic: dict[str, Any],
    choice: str,
    actor_id: str,
    actor_name: str,
    event_id: str,
) -> dict[str, Any]:
    if semantic.get("schema_version") != RCA_CANDIDATE_REVIEW_SCHEMA_VERSION:
        raise ValueError("rca candidate confirmation schema is invalid")
    if semantic.get("kind") != RCA_CANDIDATE_REVIEW_PRESET:
        raise ValueError("rca candidate confirmation kind is invalid")
    expected_confirm_id = rca_candidate_confirm_id(
        business_key=str(semantic.get("business_key") or "").strip(),
        generation=int(semantic.get("generation") or 0),
        work_item_id=str(semantic.get("work_item_id") or "").strip(),
        original_effect_key=str(semantic.get("original_effect_key") or "").strip(),
    )
    if confirm_id != expected_confirm_id:
        raise ValueError("rca candidate confirmation identity mismatch")
    action = {"追认": "recognize", "更正": "retract"}.get(choice)
    if action is None:
        raise ValueError("rca candidate confirmation choice is invalid")

    # Keep this import lazy: owner review already imports the delivery store,
    # while ordinary task confirms must remain sidecar-only and lightweight.
    from types import SimpleNamespace

    from gateway.pnc_rca_owner_review import resolve_candidate_conclusion_review

    source = SimpleNamespace(
        platform="feishu",
        chat_id=str(task_card.get("chat_id") or "").strip(),
        thread_id=str(task_card.get("thread_id") or "").strip(),
        user_id=str(actor_id or "").strip(),
        user_name=str(actor_name or "").strip(),
    )
    event = SimpleNamespace(source=source, message_id=str(event_id or "").strip())
    reason = (
        "owner_confirmed_medium_confidence_candidate"
        if action == "recognize"
        else "owner_retracted_medium_confidence_candidate"
    )
    result = resolve_candidate_conclusion_review(
        event=event,
        hermes_home=get_hermes_home(),
        issue_ids=(str(semantic.get("work_item_id") or "").strip(),),
        action=action,
        reason=reason,
        owner_id=str(actor_id or "").strip(),
        owner_name=str(actor_name or "").strip(),
        candidate_bindings={
            str(semantic.get("work_item_id") or "").strip(): {
                "business_key": str(semantic.get("business_key") or "").strip(),
                "generation": int(semantic.get("generation") or 0),
                "original_effect_key": str(
                    semantic.get("original_effect_key") or ""
                ).strip(),
            }
        },
    )
    rows = result.get("results") if isinstance(result.get("results"), list) else []
    if len(rows) != 1 or not isinstance(rows[0], dict):
        raise RuntimeError("rca candidate adjudication did not return one result")
    row = rows[0]
    expected = {
        "business_key": str(semantic.get("business_key") or "").strip(),
        "generation": int(semantic.get("generation") or 0),
        "work_item_id": str(semantic.get("work_item_id") or "").strip(),
        "original_effect_key": str(semantic.get("original_effect_key") or "").strip(),
    }
    if any(row.get(key) != value for key, value in expected.items()):
        raise RuntimeError("rca candidate adjudication identity mismatch")
    return {
        "schema_version": RCA_CANDIDATE_REVIEW_SCHEMA_VERSION,
        "kind": RCA_CANDIDATE_REVIEW_PRESET,
        "action": action,
        "conclusion_state": str(row.get("conclusion_state") or ""),
        "adjudication_id": str(row.get("adjudication_id") or ""),
        "business_key": expected["business_key"],
        "generation": expected["generation"],
        "work_item_id": expected["work_item_id"],
        "original_effect_key": expected["original_effect_key"],
        "correction_effect_key": str(row.get("correction_effect_key") or ""),
        "created": bool(row.get("created")),
        "artifact_repair_pending": bool(result.get("artifact_failures")),
    }


def _apply_semantic_resolution_to_card(
    task_card: dict[str, Any], semantic_result: dict[str, Any]
) -> None:
    task_card["rca_conclusion_review"] = dict(semantic_result)
    delivery = (
        task_card.get("delivery")
        if isinstance(task_card.get("delivery"), dict)
        else {}
    )
    state = str(semantic_result.get("conclusion_state") or "").strip()
    delivery["conclusion_state"] = state
    delivery["conclusion_adjudication_id"] = str(
        semantic_result.get("adjudication_id") or ""
    )
    if state == "invalidated":
        previous = str(delivery.get("conclusion") or "").strip()
        if previous and not delivery.get("invalidated_conclusion"):
            delivery["invalidated_conclusion"] = previous
        delivery["conclusion"] = "原候选结论已作废；更正已进入既有单一更正槽。"
    task_card["delivery"] = delivery


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
            semantic = item.get("semantic") if isinstance(item.get("semantic"), dict) else None
            response_fields = _semantic_card_response_fields(semantic)
            if item.get("resolved") is not None:
                _clear_card_send_hash(task_card)
                refreshed_at = _now_iso()
                body["task_card"] = task_card
                body["updated_at"] = refreshed_at
                _atomic_write_json(path, body)
                return {
                    "ok": True,
                    "changed": False,
                    "duplicate": True,
                    "task_id": task_id,
                    "confirm_id": confirm_id,
                    "task_card": task_card,
                    **response_fields,
                }
            options = _options(item)
            canonical = _canonical_choice(choice, options)
            if not canonical:
                return {
                    "ok": False,
                    "changed": False,
                    "error": "choice not allowed",
                    "options": options,
                    **response_fields,
                }
            semantic_result = None
            if semantic is not None:
                if semantic.get("kind") != RCA_CANDIDATE_REVIEW_PRESET:
                    return {
                        "ok": False,
                        "changed": False,
                        "error": "unknown semantic confirm kind",
                    }
                try:
                    semantic_result = _semantic_rca_resolution(
                        confirm_id=confirm_id,
                        task_card=task_card,
                        semantic=semantic,
                        choice=canonical,
                        actor_id=actor_id,
                        actor_name=actor_name,
                        event_id=event_id,
                    )
                except Exception as exc:
                    return {
                        "ok": False,
                        "changed": False,
                        "error": f"semantic confirm failed: {exc}",
                        "task_id": task_id,
                        "confirm_id": confirm_id,
                        **response_fields,
                    }
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
            if semantic_result is not None:
                resolved["semantic_result"] = semantic_result
                _apply_semantic_resolution_to_card(task_card, semantic_result)
            item["resolved"] = resolved
            _clear_card_send_hash(task_card)
            task_card["pending_confirms"] = pending
            body["task_card"] = task_card
            body["updated_at"] = resolved["resolved_at"]
            events = body.get("recent_events") if isinstance(body.get("recent_events"), list) else []
            events.append({"ts": resolved["resolved_at"], "phase": body.get("current_phase"), "summary": f"确认项 {confirm_id} -> {canonical}"})
            body["recent_events"] = events[-50:]
            _atomic_write_json(path, body)
            result = {
                "ok": True,
                "changed": True,
                "duplicate": False,
                "task_id": task_id,
                "confirm_id": confirm_id,
                "choice": canonical,
                "task_card": task_card,
                **response_fields,
            }
            if semantic_result is not None:
                result["semantic_result"] = semantic_result
            return result
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
