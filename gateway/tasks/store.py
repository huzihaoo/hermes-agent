"""Task store — SQLite persistence for Task objects.

Schema:
  tasks(
    task_id TEXT PRIMARY KEY,
    status TEXT,
    task_type TEXT,
    user_id TEXT,
    platform TEXT,
    request_summary TEXT,
    started_at REAL,
    completed_at REAL,
    agent_route TEXT
  )

This replaces JSONL full-scan for task listing and detail queries.
Event log remains the source of truth for tool calls and token counts.
"""

import sqlite3
from pathlib import Path
from typing import List, Optional

from gateway.tasks.types import Task, TaskStatus, TaskType


class TaskStore:
    """SQLite-backed task storage."""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._init_schema()

    def _init_schema(self) -> None:
        """Create tasks table if not exists."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    task_type TEXT NOT NULL,
                    user_id TEXT,
                    platform TEXT,
                    request_summary TEXT,
                    started_at REAL NOT NULL,
                    completed_at REAL,
                    agent_route TEXT
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_tasks_user_started
                ON tasks(user_id, started_at DESC)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_tasks_status
                ON tasks(status)
            """)
            conn.commit()

    def upsert(self, task: Task) -> None:
        """Insert or update a task."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO tasks (
                    task_id, status, task_type, user_id, platform,
                    request_summary, started_at, completed_at, agent_route
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(task_id) DO UPDATE SET
                    status = excluded.status,
                    task_type = excluded.task_type,
                    completed_at = excluded.completed_at,
                    agent_route = excluded.agent_route
            """, (
                task.task_id,
                task.status.value,
                task.task_type.value,
                task.user_id,
                task.platform,
                task.request_summary,
                task.started_at,
                task.completed_at,
                task.agent_route,
            ))
            conn.commit()

    def get(self, task_id: str) -> Optional[Task]:
        """Get a single task by ID."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
            ).fetchone()
            if not row:
                return None
            return Task(
                task_id=row["task_id"],
                status=TaskStatus(row["status"]),
                task_type=TaskType(row["task_type"]),
                user_id=row["user_id"],
                platform=row["platform"],
                request_summary=row["request_summary"],
                started_at=row["started_at"],
                completed_at=row["completed_at"],
                agent_route=row["agent_route"],
            )

    def list_recent(
        self, *, limit: int = 10, user_id: Optional[str] = None
    ) -> List[Task]:
        """List recent tasks, optionally filtered by user_id."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            if user_id:
                rows = conn.execute(
                    """
                    SELECT * FROM tasks
                    WHERE user_id = ?
                    ORDER BY started_at DESC
                    LIMIT ?
                    """,
                    (user_id, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT * FROM tasks
                    ORDER BY started_at DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
            return [
                Task(
                    task_id=row["task_id"],
                    status=TaskStatus(row["status"]),
                    task_type=TaskType(row["task_type"]),
                    user_id=row["user_id"],
                    platform=row["platform"],
                    request_summary=row["request_summary"],
                    started_at=row["started_at"],
                    completed_at=row["completed_at"],
                    agent_route=row["agent_route"],
                )
                for row in rows
            ]
