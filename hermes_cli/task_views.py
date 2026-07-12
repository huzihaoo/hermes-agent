"""Read-only task observability views for the PNC delivery dashboard."""

from __future__ import annotations

import json
import re
import sqlite3
import time
from urllib.parse import quote, urlsplit, urlunsplit
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, cast

from gateway.tasks.store import TaskStore
from gateway.tasks.types import Task, TaskStatus
from hermes_cli.config import get_hermes_home

_STALE_AFTER_SECONDS = 24 * 60 * 60

# Collaboration contract (schema v1)
#
# This is a read-only, explicit-first coordination layer for task detail. The
# preferred write path is to persist these keys directly in shared-state
# ``tasks/<task_id>/meta.json`` or in the per-task sidecar under
# ``~/.hermes/task-state/<task_id>.json``. The task view then renders those
# explicit values first and falls back conservatively when they are absent.
#
# Preferred explicit fields:
# - requester: human/task submitter identity
# - owner: current operator / worker owner
# - acceptance_criteria: list[str]
# - next_action: human next step
# - needs_user_input: bool
# - last_operator_note: latest human-readable operator note
#
# Optional contract metadata:
# - schema_version: integer collaboration payload version (currently 1)
# - field_sources: populated by the view layer to distinguish explicit values
#   from derived / fallback values in UI and export surfaces

_STAGE_LABELS = {
    "intake": "已接收",
    "admission": "分类中",
    "planning": "规划中",
    "vm_dispatch": "VM 派发",
    "vm_running": "VM 执行中",
    "tool_execution": "工具执行中",
    "tool.completed": "工具执行中",
    "verifying": "验证中",
    "blocked": "阻塞",
    "delivering": "交付中",
    "completed": "已完成",
    "failed": "失败",
    "stale": "已过期",
    "pending": "待处理",
    "running": "运行中",
    "cancelled": "已取消",
}


def build_task_detail_url(base_url: Optional[str], task_id: str) -> Optional[str]:
    """Build a safe human Dashboard task-detail URL.

    The base URL must be explicit and must not contain query/fragment data so
    tokens cannot leak into external status surfaces.
    """
    if not base_url:
        return None
    raw = str(base_url).strip()
    if not raw:
        return None
    parsed = urlsplit(raw)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    if parsed.username or parsed.password:
        return None
    if parsed.query or parsed.fragment:
        return None
    normalized = urlunsplit((parsed.scheme, parsed.netloc, parsed.path.rstrip("/"), "", ""))
    encoded_task_id = quote(str(task_id), safe="")
    return f"{normalized}/tasks/detail?id={encoded_task_id}"


def _shared_state_root() -> Path:
    return get_hermes_home() / "runtime" / "shared-state"


def _intake_task_root(task_id: str) -> Path:
    return _shared_state_root() / "tasks" / task_id


def _write_json_atomic(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def create_intake_task(
    *,
    requester: str,
    request_summary: str,
    acceptance_criteria: List[str],
    next_action: Optional[str] = None,
    owner: Optional[str] = None,
    needs_user_input: bool = False,
    last_operator_note: Optional[str] = None,
    source: str = "dashboard",
) -> Dict[str, Any]:
    from gateway.tasks.types import TaskType

    requester_text = str(requester or "").strip()
    summary_text = str(request_summary or "").strip()
    if not requester_text:
        raise ValueError("requester is required")
    if not summary_text:
        raise ValueError("request_summary is required")

    normalized_acceptance = [str(item).strip() for item in acceptance_criteria if str(item).strip()]
    if not normalized_acceptance:
        raise ValueError("acceptance_criteria must contain at least one non-empty item")

    task_id = f"intake-{int(time.time() * 1000)}"
    started_at = time.time()
    task = Task(
        task_id=task_id,
        status=TaskStatus.PENDING,
        task_type=TaskType.INTAKE,
        user_id=requester_text,
        platform="dashboard",
        request_summary=summary_text,
        started_at=started_at,
        agent_route="pending_review",
    )
    store = TaskStore(_task_store_path())
    store.upsert(task)

    meta = {
        "task_id": task_id,
        "title": summary_text,
        "state": "pending",
        "source": source,
        "requester": requester_text,
        "owner": owner,
        "acceptance_criteria": normalized_acceptance,
        "next_action": next_action or "等待人工 review / admission",
        "needs_user_input": bool(needs_user_input),
        "last_operator_note": last_operator_note or "pending_review created from dashboard intake",
        "latest_summary": summary_text,
        "created_at": datetime.fromtimestamp(started_at, timezone.utc).isoformat(),
        "updated_at": datetime.fromtimestamp(started_at, timezone.utc).isoformat(),
        "schema_version": 1,
    }
    _write_json_atomic(_intake_task_root(task_id) / "meta.json", meta)
    _write_json_atomic(_sidecar_path(task_id), {
        "owner": owner,
        "requester": requester_text,
        "acceptance_criteria": normalized_acceptance,
        "next_action": next_action or "等待人工 review / admission",
        "needs_user_input": bool(needs_user_input),
        "last_operator_note": last_operator_note or "intake captured",
        "updated_at": meta["updated_at"],
    })
    return get_task_view(task_id) or {}


def _load_shared_state_meta(task_id: str) -> Dict[str, Any]:
    path = _shared_state_root() / "tasks" / task_id / "meta.json"
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return {}
    return raw if isinstance(raw, dict) else {}


def _iter_shared_state_meta() -> List[Dict[str, Any]]:
    tasks_root = _shared_state_root() / "tasks"
    try:
        task_dirs = [path for path in tasks_root.iterdir() if path.is_dir()]
    except OSError:
        return []
    metas: List[Dict[str, Any]] = []
    for task_dir in task_dirs:
        try:
            raw = json.loads((task_dir / "meta.json").read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, UnicodeDecodeError):
            continue
        if not isinstance(raw, dict):
            continue
        task_id = str(raw.get("task_id") or task_dir.name)
        if not task_id:
            continue
        raw["task_id"] = task_id
        metas.append(raw)
    return metas


def _status_from_phase(current_phase: Optional[str], meta_state: Optional[str]) -> TaskStatus:
    phase = str(current_phase or meta_state or "").lower()
    if phase == "completed":
        return TaskStatus.COMPLETED
    if phase == "failed":
        return TaskStatus.FAILED
    if phase == "cancelled":
        return TaskStatus.CANCELLED
    if phase == "pending":
        return TaskStatus.PENDING
    return TaskStatus.RUNNING


def _task_store_path() -> Path:
    return get_hermes_home() / "analytics" / "tasks.db"


def _trace_file_path() -> Path:
    return get_hermes_home() / "analytics" / "events.jsonl"


def _sidecar_path(task_id: str) -> Path:
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", task_id or "default")[:120]
    return get_hermes_home() / "task-state" / f"{safe}.json"


def _sidecar_task_id(path: Path) -> str:
    return path.stem


def _load_sidecar(task_id: str) -> Dict[str, Any]:
    path = _sidecar_path(task_id)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return {}
    return raw if isinstance(raw, dict) else {}


def _iter_vm_bridge_sidecars() -> List[Dict[str, Any]]:
    root = get_hermes_home() / "task-state"
    try:
        paths = list(root.glob("*.json"))
    except OSError:
        return []
    rows: List[Dict[str, Any]] = []
    for path in paths:
        task_id = _sidecar_task_id(path)
        sidecar = _load_sidecar(task_id)
        if not isinstance(sidecar.get("vm_bridge"), dict):
            continue
        rows.append({"task_id": task_id, "sidecar": sidecar})
    return rows


def _parse_timestamp(value: Any) -> Optional[float]:
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def _last_update_ts(task: Task, sidecar: Dict[str, Any]) -> float:
    for value in (sidecar.get("updated_at"), task.completed_at, task.started_at):
        parsed = _parse_timestamp(value)
        if parsed is not None:
            return parsed
    return time.time()


def _list_count(value: Any) -> int:
    return len(value) if isinstance(value, list) else 0


def _first_text(items: Any) -> Optional[str]:
    if not isinstance(items, list) or not items:
        return None
    first = items[0]
    if isinstance(first, str):
        return first
    if isinstance(first, dict):
        for key in ("summary", "reason", "message", "text", "status", "event"):
            value = first.get(key)
            if value:
                return str(value)
        try:
            return json.dumps(first, ensure_ascii=False)
        except TypeError:
            return str(first)
    return str(first)


def _first_nonempty(*values: Any) -> Optional[str]:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _collaboration_list(value: Any) -> List[str]:
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return []


def _collaboration_explicit_bool(*values: Any) -> Optional[bool]:
    for value in values:
        if isinstance(value, bool):
            return value
    return None


def _collaboration_field_source(explicit_sidecar: bool = False, explicit_meta: bool = False, fallback: Optional[str] = None) -> str:
    if explicit_sidecar:
        return "explicit_sidecar"
    if explicit_meta:
        return "explicit_meta"
    return fallback or "missing"


def _collaboration_pick_text(
    sidecar: Dict[str, Any],
    meta: Dict[str, Any],
    sidecar_keys: List[str],
    meta_keys: List[str],
    *,
    fallback: Optional[str] = None,
    fallback_source: str = "missing",
) -> tuple[Optional[str], str]:
    sidecar_value = _first_nonempty(*(sidecar.get(key) for key in sidecar_keys))
    meta_value = _first_nonempty(*(meta.get(key) for key in meta_keys))
    value = _first_nonempty(sidecar_value, meta_value, fallback)
    source = _collaboration_field_source(
        explicit_sidecar=sidecar_value is not None,
        explicit_meta=meta_value is not None,
        fallback=fallback_source if fallback else "missing",
    )
    return value, source


def _collaboration_pick_list(
    sidecar: Dict[str, Any],
    meta: Dict[str, Any],
    key: str,
) -> tuple[List[str], str]:
    sidecar_value = _collaboration_list(sidecar.get(key))
    meta_value = _collaboration_list(meta.get(key))
    value = sidecar_value or meta_value
    source = _collaboration_field_source(
        explicit_sidecar=bool(sidecar_value),
        explicit_meta=bool(meta_value),
        fallback="missing",
    )
    return value, source


def _collaboration_pick_bool(
    sidecar: Dict[str, Any],
    meta: Dict[str, Any],
    key: str,
    *,
    fallback: bool,
) -> tuple[bool, str]:
    explicit = _collaboration_explicit_bool(sidecar.get(key), meta.get(key))
    if explicit is not None:
        return explicit, _collaboration_field_source(
            explicit_sidecar=isinstance(sidecar.get(key), bool),
            explicit_meta=isinstance(meta.get(key), bool),
        )
    return fallback, "derived"


def _needs_user_input(status: TaskStatus, sidecar: Dict[str, Any], blockers: List[Any]) -> bool:
    explicit = sidecar.get("needs_user_input")
    if isinstance(explicit, bool):
        return explicit
    if str(sidecar.get("current_phase") or "").lower() in {"blocked", "needs_user_input", "waiting_user"}:
        return True
    joined = " ".join(str(item).lower() for item in blockers)
    return bool(joined and any(token in joined for token in ("user", "用户", "确认", "approval", "审批", "input")))


def _effective_status(task_status: TaskStatus, sidecar: Dict[str, Any]) -> TaskStatus:
    """Merge TaskStore status with explicit VM/shared-state sidecar phase.

    Feishu PNC tasks are first recorded in TaskStore, while the long-running
    work finishes through remote shared-state and writes a sidecar.  Treat a
    terminal sidecar phase as execution truth for the delivery view so users do
    not see a completed VM task as forever "running".
    """
    phase = str(sidecar.get("current_phase") or "").strip().lower() if isinstance(sidecar, dict) else ""
    if phase in {"completed", "failed", "cancelled"}:
        return TaskStatus(phase)
    vm_bridge = sidecar.get("vm_bridge") if isinstance(sidecar, dict) else None
    vm_state = str((vm_bridge or {}).get("state") or "").strip().lower() if isinstance(vm_bridge, dict) else ""
    if vm_state in {"completed", "failed", "cancelled"}:
        return TaskStatus(vm_state)
    return task_status


def _derive_next_action(status: TaskStatus, stage_label: str, sidecar: Dict[str, Any], blockers: List[Any], needs_user_input: bool) -> str:
    explicit = _first_nonempty(sidecar.get("next_action"), sidecar.get("next_step"))
    if explicit:
        return explicit
    if needs_user_input:
        return "等待用户补充或确认"
    if blockers:
        return "处理当前阻塞后继续"
    if status == TaskStatus.PENDING:
        return "等待 admission / dispatch"
    if status == TaskStatus.RUNNING:
        return f"继续执行：{stage_label}"
    if status == TaskStatus.COMPLETED:
        return "检查验证和产物，等待验收确认"
    if status == TaskStatus.FAILED:
        return "查看失败原因并决定是否重跑"
    if status == TaskStatus.CANCELLED:
        return "任务已取消；如需继续请重新提交"
    return "—"


def _collaboration_card(
    *,
    task_id: str,
    status: TaskStatus,
    platform: Optional[str],
    request_summary: Optional[str],
    user_id: Optional[str],
    agent_route: Optional[str],
    current_phase: Optional[str],
    stage_label: str,
    sidecar: Dict[str, Any],
    meta: Optional[Dict[str, Any]] = None,
    thread_id: Optional[str] = None,
    message_id: Optional[str] = None,
    error_message: Optional[str] = None,
) -> Dict[str, Any]:
    meta = meta if isinstance(meta, dict) else {}
    artifacts = sidecar.get("artifacts") if isinstance(sidecar.get("artifacts"), list) else []
    verification = sidecar.get("verification") if isinstance(sidecar.get("verification"), list) else []
    raw_blockers = sidecar.get("blockers")
    blockers: List[Any] = list(raw_blockers) if isinstance(raw_blockers, list) else []
    recent_events = sidecar.get("recent_events") if isinstance(sidecar.get("recent_events"), list) else []

    owner, owner_source = _collaboration_pick_text(
        sidecar, meta, ["owner", "current_owner", "operator"], ["owner", "operator"],
        fallback=agent_route, fallback_source="derived_agent_route",
    )
    requester, requester_source = _collaboration_pick_text(
        sidecar, meta, ["requester"], ["requester"],
        fallback=user_id, fallback_source="fallback_user_id",
    )
    acceptance_criteria, acceptance_source = _collaboration_pick_list(sidecar, meta, "acceptance_criteria")

    inferred_needs_input = _needs_user_input(status, sidecar, blockers)
    needs_input, needs_input_source = _collaboration_pick_bool(
        sidecar, meta, "needs_user_input", fallback=inferred_needs_input,
    )

    explicit_next_action, next_action_source = _collaboration_pick_text(
        sidecar, meta, ["next_action", "next_step"], ["next_action", "next_step"],
        fallback=None, fallback_source="derived",
    )
    next_action = explicit_next_action or _derive_next_action(status, stage_label, sidecar, blockers, needs_input)
    if explicit_next_action is None:
        next_action_source = "derived"

    current_blocker = _first_text(blockers) or _first_nonempty(error_message, meta.get("last_error"))

    recent_event_note = _first_text(recent_events)
    last_operator_note, last_operator_note_source = _collaboration_pick_text(
        sidecar, meta, ["last_operator_note"], ["last_operator_note"],
        fallback=recent_event_note, fallback_source="fallback_recent_event",
    )

    linked_session = sidecar.get("linked_session") or meta.get("linked_session") or thread_id or message_id
    related_logs = sidecar.get("linked_logs") or sidecar.get("logs") or []
    if isinstance(related_logs, str):
        related_logs = [related_logs]
    elif not isinstance(related_logs, list):
        related_logs = []
    return {
        "schema_version": 1,
        "requester": requester,
        "owner": owner,
        "source": _first_nonempty(platform, meta.get("source"), agent_route),
        "current_phase": current_phase or status.value,
        "phase_label": stage_label,
        "next_action": next_action,
        "needs_user_input": needs_input,
        "acceptance_criteria": acceptance_criteria,
        "current_blocker": current_blocker,
        "last_update": _first_nonempty(sidecar.get("updated_at"), meta.get("updated_at")),
        "last_user_message": _first_nonempty(sidecar.get("last_user_message"), meta.get("last_user_message"), request_summary),
        "last_operator_note": last_operator_note,
        "linked_session": linked_session,
        "linked_logs": [str(item) for item in related_logs],
        "linked_artifacts": artifacts,
        "linked_verification": verification,
        "field_sources": {
            "requester": requester_source,
            "owner": owner_source,
            "next_action": next_action_source,
            "needs_user_input": needs_input_source,
            "acceptance_criteria": acceptance_source,
            "last_operator_note": last_operator_note_source,
        },
        "task_id": task_id,
    }


def _completion_notice_summary(sidecar: Dict[str, Any]) -> Dict[str, Any]:
    notice = sidecar.get("completion_notice") if isinstance(sidecar.get("completion_notice"), dict) else None
    if not notice:
        return {"completion_notice_status": None, "completion_notice_sent_at": None}
    return {
        "completion_notice_status": str(notice.get("send_status") or "pending"),
        "completion_notice_sent_at": notice.get("sent_at") or notice.get("last_attempt_at"),
    }


def _observability_summary(sidecar: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "artifact_count": _list_count(sidecar.get("artifacts")),
        "verification_count": _list_count(sidecar.get("verification")),
        "blocker_count": _list_count(sidecar.get("blockers")),
        "has_vm_bridge": isinstance(sidecar.get("vm_bridge"), dict),
        **_completion_notice_summary(sidecar),
    }


_TRACE_DATA_ALLOWLIST = {
    "task_id",
    "platform",
    "user_id",
    "user_name",
    "chat_type",
    "thread_id",
    "message_id",
    "request_summary",
    "task_type",
    "model",
    "provider",
    "input_tokens",
    "output_tokens",
    "total_tokens",
    "api_calls",
    "tool_calls",
    "tool_name",
    "args_preview",
    "error_class",
    "error_message",
    "delivery_verified",
}


def _safe_trace_data(data: Any) -> Dict[str, Any]:
    if not isinstance(data, dict):
        return {}
    safe: Dict[str, Any] = {}
    for key in _TRACE_DATA_ALLOWLIST:
        value = data.get(key)
        if value is None:
            continue
        if isinstance(value, str):
            safe[key] = value[:1000]
        elif isinstance(value, (int, float, bool)):
            safe[key] = value
        else:
            try:
                safe[key] = json.loads(json.dumps(value, ensure_ascii=False))  # keep JSON scalar/list/dict only
            except (TypeError, ValueError):
                safe[key] = str(value)[:1000]
    return safe


def _task_trace_summary(task_id: str, *, limit: int = 50) -> Dict[str, Any]:
    """Return a task-scoped, sanitized trace view for delivery troubleshooting.

    This intentionally exposes only events for the requested task id and only a
    small allowlist of fields.  Full raw logs, config, secrets, and unrelated
    CLI/model traffic remain available only on the native operator dashboard.
    """
    trace_file = _trace_file_path()
    events: List[Dict[str, Any]] = []
    if trace_file.exists():
        try:
            with trace_file.open("r", encoding="utf-8") as fh:
                for line in fh:
                    text = line.strip()
                    if not text:
                        continue
                    try:
                        raw = json.loads(text)
                    except json.JSONDecodeError:
                        continue
                    data = raw.get("data")
                    if not isinstance(data, dict) or str(data.get("task_id") or "") != task_id:
                        continue
                    events.append({
                        "timestamp": raw.get("timestamp"),
                        "event": str(raw.get("event") or "unknown"),
                        "data": _safe_trace_data(data),
                    })
        except OSError:
            events = []
    recent = events[-limit:]
    total_tokens = 0
    api_calls = 0
    tool_calls = 0
    models: List[str] = []
    providers: List[str] = []
    for event in events:
        data = event.get("data") if isinstance(event, dict) else {}
        if not isinstance(data, dict):
            continue
        if event.get("event") == "api:call":
            api_calls += 1
        if event.get("event") == "tool:call":
            tool_calls += 1
        for key in ("input_tokens", "output_tokens", "total_tokens"):
            value = data.get(key)
            if isinstance(value, (int, float)):
                total_tokens += int(value)
        model = str(data.get("model") or "").strip()
        provider = str(data.get("provider") or "").strip()
        if model and model not in models:
            models.append(model)
        if provider and provider not in providers:
            providers.append(provider)
    return {
        "events": recent,
        "event_count": len(events),
        "truncated": len(events) > len(recent),
        "summary": {
            "api_calls": api_calls,
            "tool_calls": tool_calls,
            "approx_tokens": total_tokens,
            "models": models,
            "providers": providers,
        },
    }


def _public_context_summary(*, task: Task | None = None, meta: Dict[str, Any] | None = None, sidecar: Dict[str, Any] | None = None) -> Dict[str, Any]:
    meta = meta if isinstance(meta, dict) else {}
    sidecar = sidecar if isinstance(sidecar, dict) else {}
    issue_context = sidecar.get("issue_context") or meta.get("issue_context")
    if isinstance(issue_context, str):
        issue_context = issue_context[:4000]
    elif isinstance(issue_context, dict):
        try:
            issue_context = json.loads(json.dumps(issue_context, ensure_ascii=False))  # JSON-safe copy
        except (TypeError, ValueError):
            issue_context = str(issue_context)[:4000]
    elif issue_context is not None:
        issue_context = str(issue_context)[:4000]
    return {
        "request_summary": (task.request_summary if task is not None else None) or meta.get("latest_summary") or meta.get("title"),
        "last_user_message": sidecar.get("last_user_message") or meta.get("last_user_message"),
        "issue_context": issue_context,
        "vm_bridge": sidecar.get("vm_bridge") if isinstance(sidecar.get("vm_bridge"), dict) else None,
        "completion_notice": sidecar.get("completion_notice") if isinstance(sidecar.get("completion_notice"), dict) else None,
    }


def _evidence_paths(task_id: str, source_labels: List[str]) -> Dict[str, str]:
    paths: Dict[str, str] = {}
    if "task-store" in source_labels:
        paths["task-store"] = str(_task_store_path())
    if "shared-state-meta" in source_labels:
        paths["shared-state-meta"] = str(_shared_state_root() / "tasks" / task_id / "meta.json")
    if "vm-sidecar" in source_labels:
        paths["vm-sidecar"] = str(_sidecar_path(task_id))
    return paths


def _observability_provenance(source_labels: List[str], *, task_id: str = "") -> Dict[str, Any]:
    labels: List[str] = []
    for label in source_labels:
        text = str(label or "").strip()
        if text and text not in labels:
            labels.append(text)
    label_text = {
        "task-store": "TaskStore primary record",
        "shared-state-meta": "shared-state meta fallback",
        "vm-sidecar": "VM bridge sidecar",
    }
    missing: List[str] = []
    if "task-store" not in labels:
        missing.append("task-store")
    if "shared-state-meta" in labels and "vm-sidecar" not in labels:
        missing.append("vm-sidecar")
    if "vm-sidecar" in labels and "shared-state-meta" not in labels and "task-store" not in labels:
        missing.append("shared-state-meta")
    summary = " + ".join(label_text.get(label, label) for label in labels) if labels else "unknown source"
    return {
        "observability_sources": labels,
        "observability_source_summary": summary,
        "observability_missing": missing,
        "observability_evidence_paths": _evidence_paths(task_id, labels) if task_id else {},
    }


def _source_labels(*, task_store: bool = False, shared_state_meta: bool = False, sidecar: Dict[str, Any] | None = None) -> List[str]:
    labels: List[str] = []
    if task_store:
        labels.append("task-store")
    if shared_state_meta:
        labels.append("shared-state-meta")
    if isinstance(sidecar, dict) and sidecar:
        labels.append("vm-sidecar")
    return labels


def _summary_fallback(task_id: str, platform: Optional[str]) -> str:
    source = (platform or "task").strip() or "task"
    return f"{source.upper()} 任务 {task_id}"


def _stage_label(status: TaskStatus, current_phase: Optional[str], is_stale: bool) -> str:
    if status in {TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED}:
        label = _STAGE_LABELS.get(status.value)
        return label if label is not None else status.value
    if is_stale:
        if status == TaskStatus.PENDING:
            return "待处理超时"
        return _STAGE_LABELS["stale"]
    if current_phase:
        return _STAGE_LABELS.get(str(current_phase).lower(), str(current_phase))
    label = _STAGE_LABELS.get(status.value)
    return label if label is not None else status.value


def _shared_state_view_from_meta(
    meta: Dict[str, Any],
    *,
    detail: bool,
    now: Optional[float] = None,
    shared_state_meta: bool = True,
) -> Dict[str, Any]:
    task_id = str(meta.get("task_id") or "")
    sidecar = _load_sidecar(task_id)
    current_phase = sidecar.get("current_phase") if isinstance(sidecar, dict) else None
    status = _status_from_phase(current_phase, meta.get("state") if isinstance(meta, dict) else None)
    updated_ts = _parse_timestamp(meta.get("updated_at") if isinstance(meta, dict) else None) or time.time()
    created_ts = _parse_timestamp(meta.get("created_at") if isinstance(meta, dict) else None) or updated_ts
    now_ts = time.time() if now is None else now
    completed_at = updated_ts if status in {TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED} else None
    is_stale = bool(meta.get("stale")) if isinstance(meta, dict) else False
    if status in {TaskStatus.PENDING, TaskStatus.RUNNING} and now_ts - max(updated_ts, created_ts) > _STALE_AFTER_SECONDS:
        is_stale = True
    view: Dict[str, Any] = {
        "task_id": task_id,
        "status": status.value,
        "task_type": "coding",
        "user_id": None,
        "platform": "shared-state",
        "request_summary": meta.get("latest_summary") or meta.get("title") or task_id,
        "started_at": created_ts,
        "completed_at": completed_at,
        "duration_seconds": ((completed_at or now_ts) - created_ts) if (completed_at or now_ts) >= created_ts else 0,
        "is_stale": is_stale,
        "stage_label": _stage_label(status, current_phase, is_stale),
        "current_phase": current_phase,
        "agent_route": "shared-state",
        "chat_id": None,
        "chat_type": None,
        "thread_id": None,
        "message_id": None,
        "error_class": None,
        "error_message": meta.get("last_error") if isinstance(meta, dict) else None,
        "delivery_verified": None,
        **_observability_summary(sidecar),
        **_observability_provenance(_source_labels(shared_state_meta=shared_state_meta, sidecar=sidecar), task_id=task_id),
    }
    if detail:
        view.update({
            "recent_events": sidecar.get("recent_events") if isinstance(sidecar.get("recent_events"), list) else [],
            "artifacts": sidecar.get("artifacts") if isinstance(sidecar.get("artifacts"), list) else [],
            "verification": sidecar.get("verification") if isinstance(sidecar.get("verification"), list) else [],
            "blockers": sidecar.get("blockers") if isinstance(sidecar.get("blockers"), list) else [],
            "vm_bridge": sidecar.get("vm_bridge") if isinstance(sidecar.get("vm_bridge"), dict) else None,
            "trace": _task_trace_summary(task_id),
            "public_context": _public_context_summary(meta=meta, sidecar=sidecar),
            "collaboration": _collaboration_card(
                task_id=task_id,
                status=status,
                platform="shared-state",
                request_summary=str(view.get("request_summary") or ""),
                user_id=None,
                agent_route="shared-state",
                current_phase=current_phase,
                stage_label=str(view.get("stage_label") or status.value),
                sidecar=sidecar,
                meta=meta,
                error_message=str(view.get("error_message") or "") or None,
            ),
        })
    return view


def _sidecar_only_view(task_id: str, sidecar: Dict[str, Any], *, now: Optional[float] = None) -> Dict[str, Any]:
    meta = {
        "task_id": task_id,
        "latest_summary": sidecar.get("summary") or task_id,
        "state": sidecar.get("current_phase") or (sidecar.get("vm_bridge") or {}).get("state"),
        "created_at": sidecar.get("created_at") or sidecar.get("updated_at"),
        "updated_at": sidecar.get("updated_at"),
    }
    return _shared_state_view_from_meta(meta, detail=False, now=now, shared_state_meta=False)


def task_to_view(task: Task, *, now: Optional[float] = None, detail: bool = True) -> Dict[str, Any]:
    sidecar = _load_sidecar(task.task_id)
    current_phase = sidecar.get("current_phase")
    status = _effective_status(task.status, sidecar)
    now_ts = time.time() if now is None else now
    is_stale = (
        status == TaskStatus.RUNNING
        and now_ts - _last_update_ts(task, sidecar) > _STALE_AFTER_SECONDS
    )
    completed_at = task.completed_at
    if completed_at is None and status in {TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED}:
        completed_at = _parse_timestamp(sidecar.get("updated_at")) or now_ts
    view: Dict[str, Any] = {
        "task_id": task.task_id,
        "status": status.value,
        "task_type": task.task_type.value,
        "user_id": task.user_id,
        "platform": task.platform,
        "request_summary": task.request_summary or _summary_fallback(task.task_id, task.platform),
        "started_at": task.started_at,
        "completed_at": completed_at,
        "duration_seconds": (completed_at or now_ts) - task.started_at,
        "is_stale": is_stale,
        "stage_label": _stage_label(status, current_phase, is_stale),
        "current_phase": current_phase,
        "agent_route": task.agent_route,
        "chat_id": task.chat_id,
        "chat_type": task.chat_type,
        "thread_id": task.thread_id,
        "message_id": task.message_id,
        "error_class": task.error_class,
        "error_message": task.error_message,
        "delivery_verified": task.delivery_verified,
        **_observability_summary(sidecar),
        **_observability_provenance(_source_labels(task_store=True, sidecar=sidecar), task_id=task.task_id),
    }
    if detail:
        vm_bridge = sidecar.get("vm_bridge")
        view.update({
            "recent_events": sidecar.get("recent_events") if isinstance(sidecar.get("recent_events"), list) else [],
            "artifacts": sidecar.get("artifacts") if isinstance(sidecar.get("artifacts"), list) else [],
            "verification": sidecar.get("verification") if isinstance(sidecar.get("verification"), list) else [],
            "blockers": sidecar.get("blockers") if isinstance(sidecar.get("blockers"), list) else [],
            "vm_bridge": vm_bridge if isinstance(vm_bridge, dict) else None,
            "trace": _task_trace_summary(task.task_id),
            "public_context": _public_context_summary(task=task, sidecar=sidecar),
            "collaboration": _collaboration_card(
                task_id=task.task_id,
                status=status,
                platform=task.platform,
                request_summary=view.get("request_summary"),
                user_id=task.user_id,
                agent_route=task.agent_route,
                current_phase=current_phase,
                stage_label=str(view.get("stage_label") or task.status.value),
                sidecar=sidecar,
                thread_id=task.thread_id,
                message_id=task.message_id,
                error_message=task.error_message,
            ),
        })
    return view




def _iter_store_tasks_for_effective_counts(*, platform: Optional[str] = None, chat_id: Optional[str] = None) -> List[Task]:
    store = TaskStore(_task_store_path())
    where_clauses: List[str] = []
    params: List[Any] = []
    if platform:
        where_clauses.append("platform = ?")
        params.append(platform)
    if chat_id:
        where_clauses.append("chat_id = ?")
        params.append(chat_id)
    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    with sqlite3.connect(store.db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(f"SELECT * FROM tasks {where_sql} ORDER BY started_at DESC", params).fetchall()
        return [store._task_from_row(row) for row in rows]


def _effective_status_counts_for_views(views: List[Dict[str, Any]]) -> Dict[str, int]:
    counts = {task_status.value: 0 for task_status in TaskStatus}
    for view in views:
        key = str(view.get("status") or TaskStatus.RUNNING.value)
        if key in counts:
            counts[key] += 1
    return counts

def list_task_views(
    *,
    status: Optional[TaskStatus] = None,
    platform: Optional[str] = None,
    chat_id: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
) -> Dict[str, Any]:
    store = TaskStore(_task_store_path())
    # Status in TaskStore can lag behind VM sidecars (e.g. Feishu task remains
    # RUNNING while the VM bridge has terminal state). Keep the list read capped
    # to the requested window, but compute counts separately from all scoped
    # tasks so public status chips match delivery-effective row status.
    window_limit = max(limit + offset + limit, limit)
    raw_store_tasks = [] if platform == "shared-state" else store.list_recent(
        limit=window_limit,
        offset=0,
        status=None,
        platform=platform,
        chat_id=chat_id,
    )
    store_views_all = [task_to_view(task, detail=False) for task in raw_store_tasks]
    count_store_tasks = [] if platform == "shared-state" else _iter_store_tasks_for_effective_counts(
        platform=platform,
        chat_id=chat_id,
    )
    count_store_views = [task_to_view(task, detail=False) for task in count_store_tasks]
    store_views = [
        view for view in store_views_all
        if status is None or view.get("status") == status.value
    ]

    shared_views_all: List[Dict[str, Any]] = []
    include_shared_state = platform in {None, "shared-state"}
    if include_shared_state:
        store_ids = {str(view.get("task_id")) for view in store_views_all}
        for meta in _iter_shared_state_meta():
            view = _shared_state_view_from_meta(meta, detail=False, now=time.time())
            if view["task_id"] in store_ids:
                continue
            shared_views_all.append(view)
        known_ids = store_ids | {str(view.get("task_id")) for view in shared_views_all}
        for row in _iter_vm_bridge_sidecars():
            task_id = str(row.get("task_id") or "")
            if not task_id or task_id in known_ids:
                continue
            raw_sidecar = row.get("sidecar")
            sidecar: Dict[str, Any] = raw_sidecar if isinstance(raw_sidecar, dict) else {}
            view = _sidecar_only_view(task_id, sidecar, now=time.time())
            shared_views_all.append(view)
            known_ids.add(task_id)
    shared_views = [
        view for view in shared_views_all
        if status is None or view.get("status") == status.value
    ]

    all_count_views = count_store_views + shared_views_all
    status_counts = _effective_status_counts_for_views(all_count_views)

    all_views = sorted(
        store_views + shared_views,
        key=lambda view: float(view.get("completed_at") or view.get("started_at") or 0),
        reverse=True,
    )
    sliced = all_views[offset : offset + limit]
    return {
        "tasks": sliced,
        "total": len(all_views),
        "status_counts": status_counts,
        "limit": limit,
        "offset": offset,
    }


def get_task_view(task_id: str) -> Optional[Dict[str, Any]]:
    store = TaskStore(_task_store_path())
    task = store.get(task_id)
    if task is not None:
        return task_to_view(task, detail=True)

    meta = _load_shared_state_meta(task_id)
    if not meta:
        sidecar = _load_sidecar(task_id)
        if not sidecar:
            return None
        meta = {"task_id": task_id, "latest_summary": task_id}
        return _shared_state_view_from_meta(meta, detail=True, shared_state_meta=False)
    meta = dict(meta)
    meta["task_id"] = str(meta.get("task_id") or task_id)
    return _shared_state_view_from_meta(meta, detail=True)
