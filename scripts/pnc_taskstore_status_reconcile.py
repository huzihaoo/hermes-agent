#!/usr/bin/env python3
"""Reconcile PNC Feishu TaskStore status with delivery-effective task views.

Default is dry-run. With --apply, only PNC/G1Q3 Feishu tasks whose raw
TaskStore status is non-terminal and whose delivery-effective status is
terminal are updated. Existing terminal TaskStore records are never overwritten.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from gateway.tasks.store import TaskStore  # noqa: E402
from gateway.tasks.types import Task, TaskStatus  # noqa: E402
from hermes_cli.config import get_hermes_home  # noqa: E402
from hermes_cli.task_views import task_to_view  # noqa: E402
from scripts.pnc_feishu_delivery_guard import BUSINESS_GROUPS  # noqa: E402

TERMINAL = {TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED}


def _task_store_path() -> Path:
    return get_hermes_home() / "analytics" / "tasks.db"


def _parse_completed_at(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str) and value.strip():
        try:
            return datetime.fromisoformat(value.strip().replace("Z", "+00:00")).timestamp()
        except ValueError:
            return None
    return None


def _effective_terminal_status(task: Task) -> tuple[TaskStatus | None, dict[str, Any]]:
    view = task_to_view(task, detail=False)
    raw = task.status
    try:
        effective = TaskStatus(str(view.get("status") or raw.value))
    except ValueError:
        return None, view
    if raw in TERMINAL:
        return None, view
    if effective not in TERMINAL:
        return None, view
    return effective, view


def reconcile_statuses(*, chat_ids: Iterable[str] | None = None, apply: bool = False, limit: int = 10_000) -> dict[str, Any]:
    store = TaskStore(_task_store_path())
    selected_chat_ids = list(chat_ids or [group.chat_id for group in BUSINESS_GROUPS])
    rows: list[dict[str, Any]] = []
    errors: list[str] = []
    for chat_id in selected_chat_ids:
        for task in store.list_recent(limit=limit, platform="feishu", chat_id=chat_id):
            effective_status, view = _effective_terminal_status(task)
            if effective_status is None:
                continue
            row: dict[str, Any] = {
                "task_id": task.task_id,
                "chat_id": chat_id,
                "raw_status": task.status.value,
                "effective_status": effective_status.value,
                "request_summary": task.request_summary,
                "vm_task_id": task.vm_task_id,
                "applied": False,
            }
            if apply:
                try:
                    task.status = effective_status
                    task.completed_at = task.completed_at or _parse_completed_at(view.get("completed_at")) or datetime.now(timezone.utc).timestamp()
                    if effective_status == TaskStatus.FAILED and not task.error_message:
                        task.error_message = str(view.get("error_message") or "Delivery-effective task status reached failed")
                    store.upsert(task)
                    row["applied"] = True
                except Exception as exc:  # pragma: no cover - defensive path surfaced in JSON
                    message = f"{task.task_id}: {type(exc).__name__}: {exc}"
                    row["error"] = message
                    errors.append(message)
            rows.append(row)
    return {
        "ok": not errors,
        "dry_run": not apply,
        "candidate_count": len(rows),
        "applied_count": sum(1 for row in rows if row.get("applied")),
        "rows": rows,
        "errors": errors,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--chat-id", action="append", default=[])
    parser.add_argument("--limit", type=int, default=10_000)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    result = reconcile_statuses(chat_ids=args.chat_id or None, apply=args.apply, limit=max(1, args.limit))
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        mode = "APPLY" if args.apply else "DRY-RUN"
        print(f"[{mode}] PNC TaskStore status reconcile: {result['applied_count']}/{result['candidate_count']} applied")
        for row in result["rows"][:20]:
            print(f"- {row['task_id']}: {row['raw_status']} -> {row['effective_status']} applied={row['applied']}")
        if len(result["rows"]) > 20:
            print(f"... truncated {len(result['rows']) - 20} rows")
        for error in result["errors"]:
            print(f"error: {error}")
    return 0 if result["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
