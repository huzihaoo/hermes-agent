"""Structured task event emission for Hermes task tracing.

Phase 1 local JSONL event stream used for per-task trace inspection.
This module is intentionally small and side-effect safe: failures to emit
must never break the agent or gateway flow.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional


class EventEmitter:
    """Append-only JSONL emitter for task events."""

    def __init__(self, trace_file: Optional[Path] = None, task_store=None):
        self.trace_file = Path(trace_file) if trace_file else None
        self.task_store = task_store  # Optional TaskStore for SQLite persistence

    def emit(self, event: str, data: Dict[str, Any]) -> None:
        if self.trace_file is None:
            return
        try:
            self.trace_file.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "timestamp": time.time(),
                "event": event,
                "data": data,
            }
            with self.trace_file.open("a", encoding="utf-8") as f:
                f.write(json.dumps(payload, ensure_ascii=False) + "\n")

            # Sync to TaskStore if available
            if self.task_store:
                self._sync_to_store(event, data, payload["timestamp"])
        except Exception:
            return

    def _sync_to_store(self, event: str, data: Dict[str, Any], timestamp: float) -> None:
        """Sync task lifecycle events to SQLite store."""
        try:
            from gateway.tasks.types import Task, TaskStatus, TaskType, _infer_task_type

            task_id = data.get("task_id")
            if not task_id:
                return

            if event in {"task:start", "request:start"}:
                explicit_task_type = data.get("task_type")
                task_type = TaskType(explicit_task_type) if explicit_task_type else _infer_task_type(data.get("request_summary"))
                task = Task(
                    task_id=task_id,
                    status=TaskStatus.RUNNING,
                    task_type=task_type,
                    user_id=data.get("user_id"),
                    platform=data.get("platform"),
                    request_summary=data.get("request_summary"),
                    started_at=timestamp,
                    chat_id=data.get("chat_id"),
                    chat_type=data.get("chat_type"),
                    thread_id=data.get("thread_id"),
                    message_id=data.get("message_id"),
                    receipt_path=data.get("receipt_path"),
                    delivery_verified=data.get("delivery_verified"),
                )
                self.task_store.upsert(task)
            elif event == "task:complete":
                existing = self.task_store.get(task_id)
                if existing:
                    existing.status = TaskStatus.COMPLETED
                    existing.completed_at = timestamp
                    if "message_id" in data:
                        existing.message_id = data.get("message_id")
                    if "thread_id" in data:
                        existing.thread_id = data.get("thread_id")
                    if "receipt_path" in data:
                        existing.receipt_path = data.get("receipt_path")
                    if "delivery_verified" in data:
                        existing.delivery_verified = data.get("delivery_verified")
                    elif existing.platform == "feishu" and existing.message_id:
                        existing.delivery_verified = True
                    self.task_store.upsert(existing)
            elif event == "task:failed":
                existing = self.task_store.get(task_id)
                if existing:
                    existing.status = TaskStatus.FAILED
                    existing.completed_at = timestamp
                    existing.error_class = data.get("error_class")
                    existing.error_message = data.get("error_message")
                    if "receipt_path" in data:
                        existing.receipt_path = data.get("receipt_path")
                    if "delivery_verified" in data:
                        existing.delivery_verified = data.get("delivery_verified")
                    self.task_store.upsert(existing)
        except Exception:
            # Never break event emission due to store sync failure
            pass

    def mark_pending(self, task_id: str) -> None:
        if self.trace_file is None:
            return
        try:
            self.trace_file.parent.mkdir(parents=True, exist_ok=True)
            pending_file = self.trace_file.parent / f".pending-{task_id}"
            pending_file.write_text(json.dumps({"task_id": task_id, "timestamp": time.time()}), encoding="utf-8")
        except Exception:
            return

    def finalize_pending(self, task_id: str) -> None:
        if self.trace_file is None:
            return
        try:
            pending_file = self.trace_file.parent / f".pending-{task_id}"
            pending_file.unlink(missing_ok=True)
        except Exception:
            return


class TaskEvent:
    """Factories for standardized task-trace events."""

    @staticmethod
    def task_start(*, task_id: str, platform: str, user_id: Optional[str] = None, task_type: Optional[str] = None) -> Dict[str, Any]:
        return {
            "event": "task:start",
            "data": {
                "task_id": task_id,
                "platform": platform,
                "user_id": user_id,
                **({"task_type": task_type} if task_type is not None else {}),
            },
        }

    @staticmethod
    def api_call(
        *,
        task_id: str,
        model: str,
        provider: Optional[str],
        input_tokens: int,
        output_tokens: int,
    ) -> Dict[str, Any]:
        return {
            "event": "api:call",
            "data": {
                "task_id": task_id,
                "model": model,
                "provider": provider,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
            },
        }

    @staticmethod
    def tool_call(*, task_id: str, tool_name: str, args_preview: Optional[str] = None) -> Dict[str, Any]:
        return {
            "event": "tool:call",
            "data": {
                "task_id": task_id,
                "tool_name": tool_name,
                "args_preview": args_preview,
            },
        }

    @staticmethod
    def task_complete(
        *,
        task_id: str,
        total_tokens: int,
        api_calls: int,
        tool_calls: int,
        message_id: Optional[str] = None,
        thread_id: Optional[str] = None,
        delivery_verified: Optional[bool] = None,
    ) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "task_id": task_id,
            "total_tokens": total_tokens,
            "api_calls": api_calls,
            "tool_calls": tool_calls,
        }
        if message_id is not None:
            data["message_id"] = message_id
        if thread_id is not None:
            data["thread_id"] = thread_id
        if delivery_verified is not None:
            data["delivery_verified"] = delivery_verified
        return {
            "event": "task:complete",
            "data": data,
        }

    @staticmethod
    def task_failed(*, task_id: str, error_class: str, error_message: str) -> Dict[str, Any]:
        return {
            "event": "task:failed",
            "data": {
                "task_id": task_id,
                "error_class": error_class,
                "error_message": error_message,
            },
        }

    @staticmethod
    def task_timeout(*, task_id: str, reason: str) -> Dict[str, Any]:
        return {
            "event": "task:timeout",
            "data": {
                "task_id": task_id,
                "reason": reason,
            },
        }


def cleanup_stale_pending(*, trace_file: Path, timeout_minutes: int = 30) -> None:
    trace_path = Path(trace_file)
    emitter = EventEmitter(trace_path)
    try:
        for pending_file in trace_path.parent.glob(".pending-*"):
            age_s = time.time() - pending_file.stat().st_mtime
            if age_s <= timeout_minutes * 60:
                continue
            try:
                data = json.loads(pending_file.read_text(encoding="utf-8"))
                task_id = data.get("task_id") or pending_file.name.replace(".pending-", "", 1)
            except Exception:
                task_id = pending_file.name.replace(".pending-", "", 1)
            timeout_event = TaskEvent.task_timeout(
                task_id=task_id,
                reason=f"stale pending > {timeout_minutes} minutes",
            )
            emitter.emit(timeout_event["event"], timeout_event["data"])
            pending_file.unlink(missing_ok=True)
    except Exception:
        return


def trace_task_events(*, trace_file: Path, task_id: str) -> List[Dict[str, Any]]:
    trace_path = Path(trace_file)
    if not trace_path.exists():
        return []
    events: List[Dict[str, Any]] = []
    with trace_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except Exception:
                continue
            if (payload.get("data") or {}).get("task_id") == task_id:
                events.append(payload)
    events.sort(key=lambda item: item.get("timestamp", 0))
    return events
