"""Task templates — Phase 2 parameterized template system.

Templates are created from successful tasks and store:
- template_id (UUID)
- source_task_id
- name
- task_type
- request_summary (with {{variable}} placeholders)
- params (JSON dict of variable definitions)
- created_at

Phase 2 adds:
- Parameter extraction from request_summary
- Parameter validation and substitution
- render() method for instantiation
"""

import json
import re
import sqlite3
import uuid
from pathlib import Path
from typing import Dict, List, Optional


class TemplateStore:
    """SQLite-backed template storage with parameterization."""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._init_schema()

    def _init_schema(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.db_path) as conn:
            # Check if params column exists (migration)
            cursor = conn.execute("PRAGMA table_info(templates)")
            columns = {row[1] for row in cursor.fetchall()}

            if "templates" in [row[0] for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()]:
                # Migration: add params column if missing
                if "params" not in columns:
                    conn.execute("ALTER TABLE templates ADD COLUMN params TEXT")
                # Migration: add usage_count column if missing
                if "usage_count" not in columns:
                    conn.execute("ALTER TABLE templates ADD COLUMN usage_count INTEGER DEFAULT 0")
                # Migration: add last_used_at column if missing
                if "last_used_at" not in columns:
                    conn.execute("ALTER TABLE templates ADD COLUMN last_used_at REAL")
                # Migration: add skills column if missing
                if "skills" not in columns:
                    conn.execute("ALTER TABLE templates ADD COLUMN skills TEXT")
                conn.commit()

            conn.execute("""
                CREATE TABLE IF NOT EXISTS templates (
                    template_id TEXT PRIMARY KEY,
                    source_task_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    task_type TEXT NOT NULL,
                    request_summary TEXT,
                    params TEXT,
                    created_at REAL NOT NULL,
                    usage_count INTEGER DEFAULT 0,
                    last_used_at REAL,
                    skills TEXT
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_templates_created
                ON templates(created_at DESC)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_templates_usage
                ON templates(usage_count DESC)
            """)
            conn.commit()

    @staticmethod
    def _extract_params(text: str) -> Dict[str, dict]:
        """Extract {{variable}} placeholders from text.

        Returns dict: {var_name: {"type": "string", "required": True}}
        """
        if not text:
            return {}
        pattern = r'\{\{(\w+)\}\}'
        matches = re.findall(pattern, text)
        return {var: {"type": "string", "required": True} for var in set(matches)}

    def create_from_task(
        self, *, source_task_id: str, name: str, task_type: str,
        request_summary: Optional[str], created_at: float,
        params: Optional[Dict[str, dict]] = None,
        skills: Optional[List[str]] = None
    ) -> str:
        """Create template from task, auto-extracting params if not provided."""
        template_id = str(uuid.uuid4())

        # Auto-extract params from request_summary if not provided
        if params is None and request_summary:
            params = self._extract_params(request_summary)

        params_json = json.dumps(params or {})
        skills_json = json.dumps(skills or [])

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO templates (
                    template_id, source_task_id, name, task_type,
                    request_summary, params, created_at, skills
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (template_id, source_task_id, name, task_type,
                 request_summary, params_json, created_at, skills_json),
            )
            conn.commit()
        return template_id

    def get(self, template_id: str) -> Optional[dict]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM templates WHERE template_id = ?", (template_id,)
            ).fetchone()
            if not row:
                return None
            result = dict(row)
            # Parse params JSON
            if result.get("params"):
                try:
                    result["params"] = json.loads(result["params"])
                except json.JSONDecodeError:
                    result["params"] = {}
            else:
                result["params"] = {}
            # Parse skills JSON
            if result.get("skills"):
                try:
                    result["skills"] = json.loads(result["skills"])
                except json.JSONDecodeError:
                    result["skills"] = []
            else:
                result["skills"] = []
            return result

    def list_recent(self, *, limit: int = 20, sort_by: str = "created") -> List[dict]:
        """List templates sorted by creation time or usage.

        Args:
            limit: Maximum number of templates to return
            sort_by: Sort order - "created" (default) or "usage"
        """
        order_clause = "created_at DESC" if sort_by == "created" else "usage_count DESC, last_used_at DESC"

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                f"SELECT * FROM templates ORDER BY {order_clause} LIMIT ?", (limit,)
            ).fetchall()
            results = []
            for row in rows:
                result = dict(row)
                # Parse params JSON
                if result.get("params"):
                    try:
                        result["params"] = json.loads(result["params"])
                    except json.JSONDecodeError:
                        result["params"] = {}
                else:
                    result["params"] = {}
                # Parse skills JSON
                if result.get("skills"):
                    try:
                        result["skills"] = json.loads(result["skills"])
                    except json.JSONDecodeError:
                        result["skills"] = []
                else:
                    result["skills"] = []
                results.append(result)
            return results

    def render(self, template_id: str, values: Dict[str, str]) -> Optional[str]:
        """Render template with provided values.

        Returns rendered request_summary or None if template not found.
        Raises ValueError if required params are missing.
        Applies defaults for optional params not provided.
        """
        template = self.get(template_id)
        if not template:
            return None

        request_summary = template.get("request_summary", "")
        params = template.get("params", {})

        # Build final values: user-provided + defaults for missing optional params
        final_values = dict(values)
        for param_name, param_def in params.items():
            if param_name not in final_values:
                if param_def.get("required", True):
                    # Still missing after defaults
                    pass  # Will be caught below
                elif "default" in param_def:
                    final_values[param_name] = param_def["default"]

        # Validate required params
        missing = [k for k, v in params.items()
                   if v.get("required", True) and k not in final_values]
        if missing:
            raise ValueError(f"Missing required parameters: {', '.join(missing)}")

        # Substitute {{variable}} with final_values
        result = request_summary
        for var, value in final_values.items():
            result = result.replace(f"{{{{{var}}}}}", value)

        # Record usage
        self.record_usage(template_id)

        return result

    def delete(self, template_id: str) -> bool:
        """Delete a template by ID.

        Returns True if deleted, False if not found.
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM templates WHERE template_id = ?", (template_id,))
            conn.commit()
            return cursor.rowcount > 0

    def update(self, template_id: str, name: str = None, request_summary: str = None, skills: Optional[List[str]] = None) -> bool:
        """Update template name, content, and/or skills. Returns True if updated, False if not found.

        If request_summary is provided, params are re-extracted from it.
        """
        template = self.get(template_id)
        if not template:
            return False

        # Prepare updates
        updates = []
        values = []

        if name is not None:
            updates.append("name = ?")
            values.append(name)

        if request_summary is not None:
            updates.append("request_summary = ?")
            values.append(request_summary)

            # Re-extract params
            params = self._extract_params(request_summary)
            updates.append("params = ?")
            values.append(json.dumps(params) if params else None)

        if skills is not None:
            updates.append("skills = ?")
            values.append(json.dumps(skills))

        if not updates:
            return True  # Nothing to update

        # Execute update
        values.append(template_id)
        query = f"UPDATE templates SET {', '.join(updates)} WHERE template_id = ?"

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(query, values)
            conn.commit()
            return cursor.rowcount > 0

    def export_template(self, template_id: str) -> Optional[dict]:
        """Export template as a portable dict (without IDs and timestamps)."""
        template = self.get(template_id)
        if not template:
            return None

        return {
            "name": template["name"],
            "task_type": template["task_type"],
            "request_summary": template["request_summary"],
            "params": template.get("params", {}),
            "skills": template.get("skills", []),
        }

    def import_template(self, data: dict) -> str:
        """Import template from exported dict. Returns new template_id."""
        import time

        # Validate required fields
        required = ["name", "task_type", "request_summary"]
        for field in required:
            if field not in data:
                raise ValueError(f"Missing required field: {field}")

        # Create template
        template_id = str(uuid.uuid4())
        params = data.get("params", {})
        skills = data.get("skills", [])

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO templates (template_id, source_task_id, name, task_type, request_summary, params, created_at, skills)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    template_id,
                    "imported",
                    data["name"],
                    data["task_type"],
                    data["request_summary"],
                    json.dumps(params),
                    time.time(),
                    json.dumps(skills),
                ),
            )
            conn.commit()

        return template_id

    def record_usage(self, template_id: str) -> bool:
        """Record template usage. Increments usage_count and updates last_used_at.

        Returns True if successful, False if template not found.
        """
        import time

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE templates
                SET usage_count = COALESCE(usage_count, 0) + 1,
                    last_used_at = ?
                WHERE template_id = ?
                """,
                (time.time(), template_id),
            )
            conn.commit()
            return cursor.rowcount > 0
