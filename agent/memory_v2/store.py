"""Memory v2 - SQLite FTS5 based memory with evolution."""

import json
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Optional, List, Dict, Any


class MemoryStore:
    """SQLite-backed memory store with full-text search and evolution."""
    
    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
    
    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS memories (
                memory_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                content TEXT NOT NULL,
                category TEXT DEFAULT 'general',
                source TEXT DEFAULT 'agent',
                importance REAL DEFAULT 1.0,
                access_count INTEGER DEFAULT 0,
                last_accessed_at REAL,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                is_deleted INTEGER DEFAULT 0
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_mem_user ON memories(user_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_mem_importance ON memories(importance DESC)")
        
        # FTS5 for full-text search
        conn.execute("""
            CREATE VIRTUAL TABLE IF NOT EXISTS memories_fts USING fts5(
                memory_id UNINDEXED,
                content,
                category,
                content=memories,
                content_rowid=rowid
            )
        """)
        
        # Triggers to keep FTS in sync
        conn.executescript("""
            CREATE TRIGGER IF NOT EXISTS mem_ai AFTER INSERT ON memories BEGIN
                INSERT INTO memories_fts(rowid, memory_id, content, category)
                VALUES (new.rowid, new.memory_id, new.content, new.category);
            END;
            
            CREATE TRIGGER IF NOT EXISTS mem_ad AFTER DELETE ON memories BEGIN
                INSERT INTO memories_fts(memories_fts, rowid, memory_id, content, category)
                VALUES ('delete', old.rowid, old.memory_id, old.content, old.category);
            END;
            
            CREATE TRIGGER IF NOT EXISTS mem_au AFTER UPDATE ON memories BEGIN
                INSERT INTO memories_fts(memories_fts, rowid, memory_id, content, category)
                VALUES ('delete', old.rowid, old.memory_id, old.content, old.category);
                INSERT INTO memories_fts(rowid, memory_id, content, category)
                VALUES (new.rowid, new.memory_id, new.content, new.category);
            END;
        """)
        conn.commit()
        conn.close()
    
    def add(self, user_id: str, content: str, category: str = "general", source: str = "agent", importance: float = 1.0) -> str:
        """Add a memory entry."""
        memory_id = uuid.uuid4().hex[:16]
        now = time.time()
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            INSERT INTO memories (memory_id, user_id, content, category, source, importance, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (memory_id, user_id, content, category, source, importance, now, now))
        conn.commit()
        conn.close()
        return memory_id
    
    def search(self, query: str, user_id: Optional[str] = None, limit: int = 10) -> List[Dict[str, Any]]:
        """Full-text search with BM25 ranking."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        
        if user_id:
            rows = conn.execute("""
                SELECT m.*, rank
                FROM memories_fts fts
                JOIN memories m ON fts.memory_id = m.memory_id
                WHERE memories_fts MATCH ? AND m.user_id = ? AND m.is_deleted = 0
                ORDER BY rank
                LIMIT ?
            """, (query, user_id, limit)).fetchall()
        else:
            rows = conn.execute("""
                SELECT m.*, rank
                FROM memories_fts fts
                JOIN memories m ON fts.memory_id = m.memory_id
                WHERE memories_fts MATCH ? AND m.is_deleted = 0
                ORDER BY rank
                LIMIT ?
            """, (query, limit)).fetchall()
        
        conn.close()
        
        results = []
        for row in rows:
            d = dict(row)
            # Record access
            self._record_access(d["memory_id"])
            results.append(d)
        
        return results
    
    def get(self, memory_id: str) -> Optional[Dict[str, Any]]:
        """Get a memory by ID."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM memories WHERE memory_id = ? AND is_deleted = 0", (memory_id,)).fetchone()
        conn.close()
        return dict(row) if row else None
    
    def list_recent(self, user_id: Optional[str] = None, limit: int = 20) -> List[Dict[str, Any]]:
        """List recent memories."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        if user_id:
            rows = conn.execute(
                "SELECT * FROM memories WHERE user_id = ? AND is_deleted = 0 ORDER BY updated_at DESC LIMIT ?",
                (user_id, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM memories WHERE is_deleted = 0 ORDER BY updated_at DESC LIMIT ?",
                (limit,)
            ).fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    def delete(self, memory_id: str) -> bool:
        """Soft-delete a memory."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.execute("UPDATE memories SET is_deleted = 1 WHERE memory_id = ?", (memory_id,))
        conn.commit()
        conn.close()
        return cursor.rowcount > 0
    
    def _record_access(self, memory_id: str):
        """Record memory access for evolution."""
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            UPDATE memories SET access_count = access_count + 1, last_accessed_at = ? WHERE memory_id = ?
        """, (time.time(), memory_id))
        conn.commit()
        conn.close()
    
    # --- Evolution ---
    
    def decay_unused(self, days: int = 30, decay_factor: float = 0.9) -> int:
        """Reduce importance of memories not accessed in N days."""
        cutoff = time.time() - (days * 86400)
        conn = sqlite3.connect(self.db_path)
        cursor = conn.execute("""
            UPDATE memories 
            SET importance = importance * ?, updated_at = ?
            WHERE (last_accessed_at IS NULL OR last_accessed_at < ?) AND is_deleted = 0 AND importance > 0.1
        """, (decay_factor, time.time(), cutoff))
        conn.commit()
        count = cursor.rowcount
        conn.close()
        return count
    
    def promote_frequent(self, min_access: int = 5, boost: float = 1.1) -> int:
        """Boost importance of frequently accessed memories."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.execute("""
            UPDATE memories 
            SET importance = MIN(importance * ?, 10.0), updated_at = ?
            WHERE access_count >= ? AND is_deleted = 0
        """, (boost, time.time(), min_access))
        conn.commit()
        count = cursor.rowcount
        conn.close()
        return count
    
    def stats(self, user_id: Optional[str] = None) -> Dict[str, Any]:
        """Get memory statistics."""
        conn = sqlite3.connect(self.db_path)
        
        where = "WHERE is_deleted = 0"
        params = ()
        if user_id:
            where += " AND user_id = ?"
            params = (user_id,)
        
        row = conn.execute(f"""
            SELECT COUNT(*) as total, AVG(importance) as avg_importance, 
                   SUM(access_count) as total_accesses
            FROM memories {where}
        """, params).fetchone()
        
        conn.close()
        return {
            "total_memories": row[0] or 0,
            "avg_importance": row[1] or 0.0,
            "total_accesses": row[2] or 0
        }
