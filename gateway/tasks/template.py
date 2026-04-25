"""Task templates — Phase 1.5-C minimal template system.

Templates are created from successful tasks and store:
- template_id (UUID)
- source_task_id
- name
- task_type
- request_summary
- created_at

Later phases add params/default route/skill/trigger.
"""

import sqlite3
import uuid
from pathlib import Path
from typing import List, Optional


class TemplateStore:
    """SQLite-backed template storage."""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._init_schema()

    def _init_schema(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS templates (
                    template_id TEXT PRIMARY KEY,
                    source_task_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    task_type TEXT NOT NULL,
                    request_summary TEXT,
                    created_at REAL NOT NULL
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_templates_created
                ON templates(created_at DESC)
            """)
            conn.commit()

    def create_from_task(
        self, *, source_task_id: str, name: str, task_type: str,
        request_summary: Optional[str], created_at: float
    ) -> str:
        template_id = str(uuid.uuid4())
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO templates (
                    template_id, source_task_id, name, task_type,
                    request_summary, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (template_id, source_task_id, name, task_type, request_summary, created_at),
            )
            conn.commit()
        return template_id

    def get(self, template_id: str) -> Optional[dict]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM templates WHERE template_id = ?", (template_id,)
            ).fetchone()
            return dict(row) if row else None

    def list_recent(self, *, limit: int = 20) -> List[dict]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM templates ORDER BY created_at DESC LIMIT ?", (limit,)
            ).fetchall()
            return [dict(row) for row in rows]
