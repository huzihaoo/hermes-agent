"""SQLite storage for traces and spans."""

import json
import sqlite3
from pathlib import Path
from typing import Optional, List, Dict, Any

from .trace import Trace, Span


class TraceStore:
    """Persistent storage for observability data."""
    
    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
    
    def _init_db(self):
        """Create tables if they don't exist."""
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS traces (
                trace_id TEXT PRIMARY KEY,
                user_id TEXT,
                platform TEXT,
                request_summary TEXT,
                start_time REAL NOT NULL,
                end_time REAL,
                status TEXT DEFAULT 'running',
                error TEXT DEFAULT '',
                total_tokens INTEGER DEFAULT 0,
                total_cost_usd REAL DEFAULT 0.0,
                task_type TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS spans (
                span_id TEXT PRIMARY KEY,
                trace_id TEXT NOT NULL,
                parent_span_id TEXT,
                name TEXT NOT NULL,
                kind TEXT DEFAULT 'internal',
                start_time REAL NOT NULL,
                end_time REAL,
                duration_ms REAL DEFAULT 0.0,
                status TEXT DEFAULT 'ok',
                metadata TEXT,
                input_tokens INTEGER DEFAULT 0,
                output_tokens INTEGER DEFAULT 0,
                model TEXT,
                cost_usd REAL DEFAULT 0.0,
                tool_name TEXT,
                tool_args_preview TEXT,
                FOREIGN KEY (trace_id) REFERENCES traces(trace_id)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_spans_trace ON spans(trace_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_traces_user ON traces(user_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_traces_time ON traces(start_time DESC)")
        conn.commit()
        conn.close()
    
    def save(self, trace: Trace):
        """Save a trace and all its spans."""
        conn = sqlite3.connect(self.db_path)
        
        # Save trace
        conn.execute("""
            INSERT OR REPLACE INTO traces 
            (trace_id, user_id, platform, request_summary, start_time, end_time, 
             status, error, total_tokens, total_cost_usd, task_type)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            trace.trace_id, trace.user_id, trace.platform, trace.request_summary,
            trace.start_time, trace.end_time, trace.status, trace.error,
            trace.total_tokens, trace.total_cost_usd, trace.task_type
        ))
        
        # Save spans
        for span in trace.spans:
            conn.execute("""
                INSERT OR REPLACE INTO spans
                (span_id, trace_id, parent_span_id, name, kind, start_time, end_time,
                 duration_ms, status, metadata, input_tokens, output_tokens, model,
                 cost_usd, tool_name, tool_args_preview)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                span.span_id, span.trace_id, span.parent_span_id, span.name, span.kind,
                span.start_time, span.end_time, span.duration_ms, span.status,
                json.dumps(span.metadata), span.input_tokens, span.output_tokens,
                span.model, span.cost_usd, span.tool_name, span.tool_args_preview
            ))
        
        conn.commit()
        conn.close()
    
    def get(self, trace_id: str) -> Optional[Dict[str, Any]]:
        """Get a trace by ID."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM traces WHERE trace_id = ?", (trace_id,)).fetchone()
        conn.close()
        return dict(row) if row else None
    
    def get_spans(self, trace_id: str) -> List[Dict[str, Any]]:
        """Get all spans for a trace."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM spans WHERE trace_id = ? ORDER BY start_time",
            (trace_id,)
        ).fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    def list_recent(self, limit: int = 20, user_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """List recent traces."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        
        if user_id:
            rows = conn.execute(
                "SELECT * FROM traces WHERE user_id = ? ORDER BY start_time DESC LIMIT ?",
                (user_id, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM traces ORDER BY start_time DESC LIMIT ?",
                (limit,)
            ).fetchall()
        
        conn.close()
        return [dict(row) for row in rows]
    
    def stats_daily(self, days: int = 7) -> Dict[str, Any]:
        """Get aggregate stats for the last N days."""
        conn = sqlite3.connect(self.db_path)
        cutoff = time.time() - (days * 86400)
        
        row = conn.execute("""
            SELECT 
                COUNT(*) as total_traces,
                SUM(total_tokens) as total_tokens,
                SUM(total_cost_usd) as total_cost_usd
            FROM traces
            WHERE start_time >= ?
        """, (cutoff,)).fetchone()
        
        conn.close()
        return {
            "total_traces": row[0] or 0,
            "total_tokens": row[1] or 0,
            "total_cost_usd": row[2] or 0.0
        }
    
    def stats_by_user(self, days: int = 30) -> List[Dict[str, Any]]:
        """Get per-user stats for the last N days."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cutoff = time.time() - (days * 86400)
        
        rows = conn.execute("""
            SELECT 
                user_id,
                COUNT(*) as trace_count,
                SUM(total_tokens) as total_tokens,
                SUM(total_cost_usd) as total_cost_usd
            FROM traces
            WHERE start_time >= ?
            GROUP BY user_id
            ORDER BY total_cost_usd DESC
        """, (cutoff,)).fetchall()
        
        conn.close()
        return [dict(row) for row in rows]


import time  # for stats_daily cutoff
