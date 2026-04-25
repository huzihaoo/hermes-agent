"""Task trace CLI — view structured event logs for tasks."""

import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
from gateway.tasks.types import Task, TaskReceipt, TaskStatus, TaskType, _infer_task_type


_hermes_home = Path.home() / ".hermes"


def generate_receipt(*, trace_file: Path, task_id: str) -> TaskReceipt:
    """Generate a TaskReceipt from events.jsonl.
    
    Receipt = derived view of a task's lifecycle, not a separate store.
    """
    summary = get_task_summary(trace_file=trace_file, task_id=task_id)
    if summary["status"] == "not_found":
        return TaskReceipt(
            task_id=task_id,
            status=TaskStatus.PENDING,
            task_type=TaskType.UNKNOWN,
            user_id=None,
            platform=None,
            request_summary=None,
            started_at=0,
            completed_at=None,
        )
    
    # Collect tool calls
    tool_calls: List[Dict[str, Any]] = []
    with trace_file.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                event = json.loads(line)
                if event.get("event") == "tool:call" and event.get("data", {}).get("task_id") == task_id:
                    tool_calls.append(event["data"])
            except (json.JSONDecodeError, KeyError):
                continue
    
    _status_map = {
        "completed": TaskStatus.COMPLETED,
        "failed": TaskStatus.FAILED,
        "pending": TaskStatus.PENDING,
    }
    
    return TaskReceipt(
        task_id=task_id,
        status=_status_map.get(summary["status"], TaskStatus.PENDING),
        task_type=_infer_task_type(summary.get("request_summary")),
        user_id=summary.get("user_id"),
        platform=summary.get("platform"),
        request_summary=summary.get("request_summary"),
        started_at=summary.get("started_at") or 0,
        completed_at=summary.get("completed_at"),
        total_tokens=summary.get("total_tokens", 0),
        tool_calls=len(tool_calls),
        tool_call_details=tool_calls,
        error_class=summary.get("error_class"),
        error_message=summary.get("error_message"),
    )


def cmd_task_trace(
    tail: int = 20,
    task_id: Optional[str] = None,
    user_id: Optional[str] = None,
    event_type: Optional[str] = None,
):
    """View task trace events from the structured event log.

    Args:
        tail: Number of recent events to show (default: 20)
        task_id: Filter by task ID
        user_id: Filter by user ID
        event_type: Filter by event type (task:start, api:call, tool:call, task:complete, task:failed)
    """
    trace_file = _hermes_home / "analytics" / "events.jsonl"
    if not trace_file.exists():
        print(f"No trace file found at {trace_file}")
        return

    events = []
    with open(trace_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if task_id and event.get("data", {}).get("task_id") != task_id:
                continue
            if user_id and event.get("data", {}).get("user_id") != user_id:
                continue
            if event_type and event.get("event") != event_type:
                continue
            events.append(event)

    if not events:
        print("No matching events found.")
        return

    for event in events[-tail:]:
        ts = event.get("timestamp", 0)
        ts_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))
        event_name = event.get("event", "unknown")
        data = event.get("data", {})
        tid = data.get("task_id", "")
        print(f"[{ts_str}] {event_name:<20} task_id={tid:<20} {json.dumps(data, ensure_ascii=False)}")


def list_tasks(
    *,
    trace_file: Path,
    limit: int = 10,
    user_id: Optional[str] = None,
) -> List[Task]:
    """List recent tasks from event log, return Task objects."""
    if not trace_file.exists():
        return []

    tasks: Dict[str, Dict[str, Any]] = {}
    with open(trace_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            data = event.get("data") or {}
            task_id = data.get("task_id")
            if not task_id:
                continue
            task = tasks.setdefault(task_id, {
                "task_id": task_id,
                "user_id": data.get("user_id"),
                "platform": data.get("platform"),
                "request_summary": None,
                "started_at": None,
                "status": TaskStatus.PENDING,
                "task_type": TaskType.UNKNOWN,
                "agent_route": None,
                "completed_at": None,
            })
            if event.get("event") in {"task:start", "request:start"}:
                if user_id and data.get("user_id") != user_id:
                    continue
                task["started_at"] = event.get("timestamp", 0)
                task["user_id"] = data.get("user_id") or task.get("user_id")
                task["platform"] = data.get("platform") or task.get("platform")
                task["request_summary"] = data.get("request_summary") or task.get("request_summary")
                task["status"] = TaskStatus.RUNNING
                task["task_type"] = _infer_task_type(task["request_summary"])
            elif event.get("event") == "task:complete":
                task["status"] = TaskStatus.COMPLETED
                task["completed_at"] = event.get("timestamp", 0)
            elif event.get("event") == "task:failed":
                task["status"] = TaskStatus.FAILED
                task["completed_at"] = event.get("timestamp", 0)

    sorted_tasks = sorted(tasks.values(), key=lambda t: t.get("started_at") or 0, reverse=True)
    return [
        Task(
            task_id=t["task_id"],
            status=t["status"],
            task_type=t["task_type"],
            user_id=t["user_id"],
            platform=t["platform"],
            request_summary=t["request_summary"],
            started_at=t["started_at"] or 0,
            completed_at=t.get("completed_at"),
            agent_route=t.get("agent_route"),
        )
        for t in sorted_tasks[:limit]
    ]


def get_task_summary(
    *,
    trace_file: Path,
    task_id: str,
) -> Dict[str, Any]:
    if not trace_file.exists():
        return {"task_id": task_id, "status": "not_found"}

    task = {
        "task_id": task_id,
        "user_id": None,
        "platform": None,
        "request_summary": None,
        "started_at": None,
        "status": "pending",
        "total_tokens": 0,
        "error_class": None,
        "error_message": None,
        "events": [],
    }

    with open(trace_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            data = event.get("data") or {}
            if data.get("task_id") != task_id:
                continue
            task["events"].append(event)
            if event.get("event") in {"task:start", "request:start"}:
                task["started_at"] = event.get("timestamp", 0)
                task["user_id"] = data.get("user_id") or task.get("user_id")
                task["platform"] = data.get("platform") or task.get("platform")
                task["request_summary"] = data.get("request_summary") or task.get("request_summary")
            elif event.get("event") == "task:complete":
                task["status"] = "completed"
                task["total_tokens"] = data.get("total_tokens", task.get("total_tokens", 0))
            elif event.get("event") == "task:failed":
                task["status"] = "failed"
                task["error_class"] = data.get("error_class")
                task["error_message"] = data.get("error_message")

    return task


if __name__ == "__main__":
    import fire
    fire.Fire(cmd_task_trace)
