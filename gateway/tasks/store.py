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
    agent_route TEXT,
    chat_id TEXT,
    chat_type TEXT,
    thread_id TEXT,
    message_id TEXT,
    error_class TEXT,
	    error_message TEXT,
	    receipt_path TEXT,
	    delivery_verified INTEGER,
	    memory_write_count INTEGER,
	    skill_write_count INTEGER,
	    permission_decision TEXT,
	    vm_task_id TEXT,
	    delivery_receipt TEXT
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

    @staticmethod
    def _ensure_columns(conn: sqlite3.Connection, columns: dict[str, str]) -> None:
        """Add missing columns for older tasks.db files."""
        allowed_types = {"TEXT", "INTEGER", "REAL"}
        existing = {row[1] for row in conn.execute("PRAGMA table_info(tasks)")}
        for name, column_type in columns.items():
            if not name.isidentifier() or column_type not in allowed_types:
                raise ValueError(f"invalid migration column: {name} {column_type}")
            if name not in existing:
                conn.execute(f"ALTER TABLE tasks ADD COLUMN {name} {column_type}")

    @staticmethod
    def _bool_to_db(value: Optional[bool]) -> Optional[int]:
        if value is None:
            return None
        return 1 if value else 0

    @staticmethod
    def _bool_from_db(value) -> Optional[bool]:
        if value is None:
            return None
        return bool(value)

    @staticmethod
    def _task_from_row(row: sqlite3.Row) -> Task:
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
            chat_id=row["chat_id"],
            chat_type=row["chat_type"],
            thread_id=row["thread_id"],
            message_id=row["message_id"],
            error_class=row["error_class"],
	            error_message=row["error_message"],
	            receipt_path=row["receipt_path"],
	            delivery_verified=TaskStore._bool_from_db(row["delivery_verified"]),
	            memory_write_count=row["memory_write_count"],
	            skill_write_count=row["skill_write_count"],
	            permission_decision=row["permission_decision"],
	            vm_task_id=row["vm_task_id"],
	            delivery_receipt=row["delivery_receipt"],
	        )

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
                    agent_route TEXT,
                    chat_id TEXT,
                    chat_type TEXT,
                    thread_id TEXT,
                    message_id TEXT,
	                    error_class TEXT,
	                    error_message TEXT,
	                    receipt_path TEXT,
	                    delivery_verified INTEGER,
	                    memory_write_count INTEGER,
	                    skill_write_count INTEGER,
	                    permission_decision TEXT,
	                    vm_task_id TEXT,
	                    delivery_receipt TEXT
	                )
	            """)
            self._ensure_columns(conn, {
                "chat_id": "TEXT",
                "chat_type": "TEXT",
                "thread_id": "TEXT",
                "message_id": "TEXT",
                "error_class": "TEXT",
	                "error_message": "TEXT",
	                "receipt_path": "TEXT",
	                "delivery_verified": "INTEGER",
	                "memory_write_count": "INTEGER",
	                "skill_write_count": "INTEGER",
	                "permission_decision": "TEXT",
	                "vm_task_id": "TEXT",
	                "delivery_receipt": "TEXT",
	            })
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
	                    request_summary, started_at, completed_at, agent_route,
	                    chat_id, chat_type, thread_id, message_id,
	                    error_class, error_message, receipt_path, delivery_verified,
	                    memory_write_count, skill_write_count, permission_decision,
	                    vm_task_id, delivery_receipt
	                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	                ON CONFLICT(task_id) DO UPDATE SET
	                    status = excluded.status,
	                    task_type = excluded.task_type,
                    completed_at = excluded.completed_at,
                    agent_route = excluded.agent_route,
                    chat_id = COALESCE(excluded.chat_id, tasks.chat_id),
                    chat_type = COALESCE(excluded.chat_type, tasks.chat_type),
                    thread_id = COALESCE(excluded.thread_id, tasks.thread_id),
                    message_id = COALESCE(excluded.message_id, tasks.message_id),
	                    error_class = COALESCE(excluded.error_class, tasks.error_class),
	                    error_message = COALESCE(excluded.error_message, tasks.error_message),
	                    receipt_path = COALESCE(excluded.receipt_path, tasks.receipt_path),
	                    delivery_verified = COALESCE(excluded.delivery_verified, tasks.delivery_verified),
	                    memory_write_count = COALESCE(excluded.memory_write_count, tasks.memory_write_count),
	                    skill_write_count = COALESCE(excluded.skill_write_count, tasks.skill_write_count),
	                    permission_decision = COALESCE(excluded.permission_decision, tasks.permission_decision),
	                    vm_task_id = COALESCE(excluded.vm_task_id, tasks.vm_task_id),
	                    delivery_receipt = COALESCE(excluded.delivery_receipt, tasks.delivery_receipt)
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
                task.chat_id,
                task.chat_type,
                task.thread_id,
                task.message_id,
                task.error_class,
	                task.error_message,
	                task.receipt_path,
	                self._bool_to_db(task.delivery_verified),
	                task.memory_write_count,
	                task.skill_write_count,
	                task.permission_decision,
	                task.vm_task_id,
	                task.delivery_receipt,
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
            return self._task_from_row(row)

    def find_recent_for_topic(
        self,
        *,
        chat_id: str,
        thread_id: Optional[str] = None,
        message_id: Optional[str] = None,
        limit: int = 5,
    ) -> List[Task]:
        """Return recent tasks bound to the same chat topic/root message."""
        if not chat_id:
            return []

        predicates = ["chat_id = ?"]
        params = [chat_id]
        topic_clauses = []
        if thread_id:
            topic_clauses.append("thread_id = ?")
            params.append(thread_id)
        if message_id:
            topic_clauses.append("message_id = ?")
            params.append(message_id)
        if topic_clauses:
            predicates.append("(" + " OR ".join(topic_clauses) + ")")

        where_sql = " AND ".join(predicates)
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                f"""
                SELECT * FROM tasks
                WHERE {where_sql}
                ORDER BY started_at DESC
                LIMIT ?
                """,
                [*params, max(1, int(limit or 1))],
            ).fetchall()
            return [self._task_from_row(row) for row in rows]

    def list_recent(
        self,
        *,
        limit: int = 10,
        offset: int = 0,
        user_id: Optional[str] = None,
        status: Optional[TaskStatus] = None,
        platform: Optional[str] = None,
        chat_id: Optional[str] = None,
    ) -> List[Task]:
        """List recent tasks with pagination and filters.

        Args:
            limit: Max tasks to return (default: 10)
            offset: Skip first N tasks (default: 0)
            user_id: Filter by user_id
            status: Filter by status
            platform: Filter by platform
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row

            # Build query
            where_clauses = []
            params = []

            if user_id:
                where_clauses.append("user_id = ?")
                params.append(user_id)

            if status:
                where_clauses.append("status = ?")
                params.append(status.value)

            if platform:
                where_clauses.append("platform = ?")
                params.append(platform)

            if chat_id:
                where_clauses.append("chat_id = ?")
                params.append(chat_id)

            where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

            query = f"""
                SELECT * FROM tasks
                {where_sql}
                ORDER BY started_at DESC
                LIMIT ? OFFSET ?
            """
            params.extend([limit, offset])

            rows = conn.execute(query, params).fetchall()
            return [self._task_from_row(row) for row in rows]

    def count_tasks(
        self,
        *,
        user_id: Optional[str] = None,
        status: Optional[TaskStatus] = None,
        platform: Optional[str] = None,
        chat_id: Optional[str] = None,
    ) -> int:
        """Count total tasks matching filters."""
        with sqlite3.connect(self.db_path) as conn:
            where_clauses = []
            params = []

            if user_id:
                where_clauses.append("user_id = ?")
                params.append(user_id)

            if status:
                where_clauses.append("status = ?")
                params.append(status.value)

            if platform:
                where_clauses.append("platform = ?")
                params.append(platform)

            if chat_id:
                where_clauses.append("chat_id = ?")
                params.append(chat_id)

            where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

            query = f"SELECT COUNT(*) FROM tasks {where_sql}"
            result = conn.execute(query, params).fetchone()
            return result[0] if result else 0


    def count_tasks_by_status(
        self,
        *,
        user_id: Optional[str] = None,
        platform: Optional[str] = None,
        chat_id: Optional[str] = None,
    ) -> dict[str, int]:
        """Count tasks grouped by status, scoped by non-status filters."""
        counts = {status.value: 0 for status in TaskStatus}
        with sqlite3.connect(self.db_path) as conn:
            where_clauses = []
            params = []

            if user_id:
                where_clauses.append("user_id = ?")
                params.append(user_id)

            if platform:
                where_clauses.append("platform = ?")
                params.append(platform)

            if chat_id:
                where_clauses.append("chat_id = ?")
                params.append(chat_id)

            where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
            rows = conn.execute(
                f"SELECT status, COUNT(*) FROM tasks {where_sql} GROUP BY status",
                params,
            ).fetchall()

        for status, count in rows:
            if status in counts:
                counts[status] = int(count)
        return counts

    def cancel_task(self, task_id: str) -> bool:
        """Cancel a running or pending task.

        Returns True if the task was cancelled, False if not found or
        already in a terminal state (completed/failed/cancelled).
        """
        import time as _time
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute(
                """UPDATE tasks SET status = ?, completed_at = ?
                   WHERE task_id = ? AND status IN (?, ?)""",
                (
                    TaskStatus.CANCELLED.value,
                    _time.time(),
                    task_id,
                    TaskStatus.PENDING.value,
                    TaskStatus.RUNNING.value,
                ),
            )
            conn.commit()
            return cur.rowcount > 0

    def retry_task(self, task_id: str) -> Optional[Task]:
        """Reset a failed or cancelled task back to pending.

        Returns the updated Task if successful, None if not found or
        the task is not in a retryable state.
        """
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute(
                """UPDATE tasks SET status = ?, completed_at = NULL
                   WHERE task_id = ? AND status IN (?, ?)""",
                (
                    TaskStatus.PENDING.value,
                    task_id,
                    TaskStatus.FAILED.value,
                    TaskStatus.CANCELLED.value,
                ),
            )
            conn.commit()
            if cur.rowcount == 0:
                return None
            return self.get(task_id)

    def cleanup_old_tasks(self, retention_days: int = 90) -> int:
        """Delete tasks older than retention_days.

        Args:
            retention_days: Keep tasks for this many days (default: 90)

        Returns:
            Number of deleted tasks
        """
        import time
        with sqlite3.connect(self.db_path) as conn:
            cutoff = time.time() - (retention_days * 86400)
            deleted = conn.execute(
                "DELETE FROM tasks WHERE started_at < ?",
                (cutoff,)
            ).rowcount
            conn.commit()
            return deleted
