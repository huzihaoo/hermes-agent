"""Event-based Insights Engine for Hermes task trace events.

Analyzes the Phase 1 JSONL event stream to produce per-user/admin usage insights
without relying on the SQLite session database.
"""

from __future__ import annotations

import json
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional


class EventInsightsEngine:
    def __init__(self, trace_file: Path):
        self.trace_file = Path(trace_file)

    def generate(
        self,
        days: int = 30,
        user_id: Optional[str] = None,
        admin: bool = False,
    ) -> Dict[str, Any]:
        cutoff = time.time() - (days * 86400)
        events = self._load_events(cutoff=None)
        if not events:
            return {
                "days": days,
                "user_id": user_id,
                "admin": admin,
                "empty": True,
                "overview": {},
                "models": [],
                "tools": [],
                "users": [],
            }

        tasks = self._build_task_records(events)
        tasks = [t for t in tasks if (t.get("started_at") or 0) >= cutoff]
        if user_id:
            tasks = [t for t in tasks if t.get("user_id") == user_id]

        if not tasks:
            return {
                "days": days,
                "user_id": user_id,
                "admin": admin,
                "empty": True,
                "overview": {},
                "models": [],
                "tools": [],
                "users": [],
            }

        return {
            "days": days,
            "user_id": user_id,
            "admin": admin,
            "empty": False,
            "generated_at": time.time(),
            "overview": self._compute_overview(tasks),
            "models": self._compute_model_breakdown(tasks),
            "tools": self._compute_tool_breakdown(tasks),
            "users": self._compute_user_breakdown(tasks) if admin else [],
        }

    def format_terminal(self, report: Dict[str, Any]) -> str:
        if report.get("empty"):
            return "Hermes Event Insights\n\nNo events found for the selected filters."

        overview = report["overview"]
        lines = ["Hermes Event Insights", "=" * 60, ""]
        lines.append("Overview")
        lines.append("-" * 60)
        lines.append(f"Tasks:         {overview['total_tasks']}")
        lines.append(f"Input tokens:  {overview['total_input_tokens']:,}")
        lines.append(f"Output tokens: {overview['total_output_tokens']:,}")
        lines.append(f"Total tokens:  {overview['total_tokens']:,}")
        lines.append("")

        if report.get("models"):
            lines.append("Models")
            lines.append("-" * 60)
            for m in report["models"]:
                lines.append(f"{m['model']:<24} tasks={m['tasks']:<3} tokens={m['total_tokens']:,}")
            lines.append("")

        if report.get("tools"):
            lines.append("Top Tools")
            lines.append("-" * 60)
            for t in report["tools"]:
                lines.append(f"{t['tool']:<24} count={t['count']}")
            lines.append("")

        if report.get("admin") and report.get("users"):
            lines.append("Users")
            lines.append("-" * 60)
            for u in report["users"]:
                lines.append(f"{u['user_id']:<24} tasks={u['tasks']:<3} tokens={u['total_tokens']:,}")

        return "\n".join(lines).rstrip()

    def format_gateway(self, report: Dict[str, Any]) -> str:
        if report.get("empty"):
            return "No events found for the selected filters."
        overview = report["overview"]
        lines = [
            "📊 **Hermes Event Insights**",
            f"Tasks: {overview['total_tasks']}",
            f"Tokens: {overview['total_tokens']:,} ({overview['total_input_tokens']:,} in / {overview['total_output_tokens']:,} out)",
        ]
        top_tools = report.get("tools", [])[:3]
        if top_tools:
            tool_line = ", ".join(f"{t['tool']}×{t['count']}" for t in top_tools)
            lines.append(f"Top tools: {tool_line}")
        if report.get("admin") and report.get("users"):
            top_users = report["users"][:3]
            user_line = ", ".join(f"{u['user_id']}×{u['tasks']}" for u in top_users)
            lines.append(f"Top users: {user_line}")
        return "\n".join(lines)

    def _load_events(self, cutoff: float | None) -> List[Dict[str, Any]]:
        if not self.trace_file.exists():
            return []
        events: List[Dict[str, Any]] = []
        with self.trace_file.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if cutoff is None or event.get("timestamp", 0) >= cutoff:
                    events.append(event)
        return events

    def _build_task_records(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        tasks: Dict[str, Dict[str, Any]] = {}
        for event in sorted(events, key=lambda e: e.get("timestamp", 0)):
            data = event.get("data") or {}
            task_id = data.get("task_id")
            if not task_id:
                continue
            task = tasks.setdefault(task_id, {
                "task_id": task_id,
                "user_id": None,
                "platform": None,
                "model": None,
                "input_tokens": 0,
                "output_tokens": 0,
                "total_tokens": 0,
                "tools": [],
                "started_at": None,
                "completed": False,
                "api_calls": 0,
                "tool_call_count": 0,
            })
            if event.get("event") in {"task:start", "request:start"}:
                task["user_id"] = data.get("user_id") or task.get("user_id")
                task["platform"] = data.get("platform") or task.get("platform")
                if event.get("event") == "task:start" and task.get("started_at") is None:
                    task["started_at"] = event.get("timestamp", 0)
            elif event.get("event") == "api:call":
                task["model"] = data.get("model") or task.get("model")
                task["input_tokens"] += int(data.get("input_tokens") or 0)
                task["output_tokens"] += int(data.get("output_tokens") or 0)
                task["api_calls"] += 1
            elif event.get("event") == "tool:call":
                tool_name = data.get("tool_name")
                if tool_name:
                    task["tools"].append(tool_name)
                    task["tool_call_count"] += 1
            elif event.get("event") == "task:complete":
                task["total_tokens"] = int(data.get("total_tokens") or task["input_tokens"] + task["output_tokens"])
                task["completed"] = True
        for task in tasks.values():
            if not task["total_tokens"]:
                task["total_tokens"] = task["input_tokens"] + task["output_tokens"]
        return list(tasks.values())

    def _compute_overview(self, tasks: List[Dict[str, Any]]) -> Dict[str, Any]:
        total_input = sum(t["input_tokens"] for t in tasks)
        total_output = sum(t["output_tokens"] for t in tasks)
        completed_tasks = sum(1 for t in tasks if t.get("completed"))
        total_tasks = len(tasks)
        total_tokens = sum(t["total_tokens"] for t in tasks)
        total_tool_calls = sum(t.get("tool_call_count", 0) for t in tasks)
        return {
            "total_tasks": total_tasks,
            "completed_tasks": completed_tasks,
            "success_rate": (completed_tasks / total_tasks * 100.0) if total_tasks else 0.0,
            "total_input_tokens": total_input,
            "total_output_tokens": total_output,
            "total_tokens": total_tokens,
            "avg_tokens_per_task": (total_tokens / total_tasks) if total_tasks else 0.0,
            "avg_tool_calls_per_task": (total_tool_calls / total_tasks) if total_tasks else 0.0,
        }

    def _compute_model_breakdown(self, tasks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        data: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"tasks": 0, "total_tokens": 0})
        for t in tasks:
            model = t.get("model") or "unknown"
            data[model]["tasks"] += 1
            data[model]["total_tokens"] += t["total_tokens"]
        return [
            {"model": model, "tasks": vals["tasks"], "total_tokens": vals["total_tokens"]}
            for model, vals in sorted(data.items(), key=lambda x: x[1]["tasks"], reverse=True)
        ]

    def _compute_tool_breakdown(self, tasks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        counts = Counter()
        for t in tasks:
            counts.update(t.get("tools") or [])
        return [
            {"tool": tool, "count": count}
            for tool, count in counts.most_common()
        ]

    def _compute_user_breakdown(self, tasks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        data: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"tasks": 0, "total_tokens": 0})
        for t in tasks:
            uid = t.get("user_id") or "unknown"
            data[uid]["tasks"] += 1
            data[uid]["total_tokens"] += t["total_tokens"]
        return [
            {"user_id": uid, "tasks": vals["tasks"], "total_tokens": vals["total_tokens"]}
            for uid, vals in sorted(data.items(), key=lambda x: x[1]["tasks"], reverse=True)
        ]
