"""Concurrency control and token quota management."""

import asyncio
import sqlite3
import time
from pathlib import Path
from typing import Optional, Dict, Any


class UserConcurrencyLimiter:
    """Per-user concurrency limiter."""
    
    def __init__(self, max_concurrent: int = 3):
        self.max_concurrent = max_concurrent
        self._active: Dict[str, int] = {}
        self._lock = asyncio.Lock()
    
    async def acquire(self, user_id: str) -> bool:
        """Try to acquire a slot. Returns False if at limit."""
        async with self._lock:
            current = self._active.get(user_id, 0)
            if current >= self.max_concurrent:
                return False
            self._active[user_id] = current + 1
            return True
    
    async def release(self, user_id: str):
        """Release a slot."""
        async with self._lock:
            self._active[user_id] = max(0, self._active.get(user_id, 0) - 1)
    
    def get_active(self, user_id: str) -> int:
        """Get current active count for a user."""
        return self._active.get(user_id, 0)
    
    def get_all_active(self) -> Dict[str, int]:
        """Get all active counts."""
        return dict(self._active)


class TokenQuotaManager:
    """Per-user monthly token quota management."""
    
    def __init__(self, db_path: Path, default_monthly_limit: int = 500_000):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.default_monthly_limit = default_monthly_limit
        self._init_db()
    
    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS token_usage (
                user_id TEXT NOT NULL,
                month TEXT NOT NULL,
                tokens_used INTEGER DEFAULT 0,
                cost_usd REAL DEFAULT 0.0,
                PRIMARY KEY (user_id, month)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS user_quotas (
                user_id TEXT PRIMARY KEY,
                monthly_token_limit INTEGER,
                updated_at REAL
            )
        """)
        conn.commit()
        conn.close()
    
    def _current_month(self) -> str:
        return time.strftime("%Y-%m")
    
    def get_limit(self, user_id: str) -> int:
        """Get monthly token limit for a user."""
        conn = sqlite3.connect(self.db_path)
        row = conn.execute("SELECT monthly_token_limit FROM user_quotas WHERE user_id = ?", (user_id,)).fetchone()
        conn.close()
        return row[0] if row and row[0] else self.default_monthly_limit
    
    def set_limit(self, user_id: str, limit: int):
        """Set monthly token limit for a user."""
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            INSERT OR REPLACE INTO user_quotas (user_id, monthly_token_limit, updated_at)
            VALUES (?, ?, ?)
        """, (user_id, limit, time.time()))
        conn.commit()
        conn.close()
    
    def get_usage(self, user_id: str) -> Dict[str, Any]:
        """Get current month's usage for a user."""
        month = self._current_month()
        limit = self.get_limit(user_id)
        
        conn = sqlite3.connect(self.db_path)
        row = conn.execute(
            "SELECT tokens_used, cost_usd FROM token_usage WHERE user_id = ? AND month = ?",
            (user_id, month)
        ).fetchone()
        conn.close()
        
        used = row[0] if row else 0
        cost = row[1] if row else 0.0
        
        return {
            "user_id": user_id,
            "month": month,
            "tokens_used": used,
            "tokens_limit": limit,
            "tokens_remaining": max(0, limit - used),
            "usage_percent": (used / limit * 100) if limit > 0 else 0,
            "cost_usd": cost,
            "over_limit": used >= limit
        }
    
    def consume(self, user_id: str, tokens: int, cost_usd: float = 0.0) -> bool:
        """Record token consumption. Returns True if within quota."""
        month = self._current_month()
        limit = self.get_limit(user_id)
        
        conn = sqlite3.connect(self.db_path)
        
        # Get current usage
        row = conn.execute(
            "SELECT tokens_used FROM token_usage WHERE user_id = ? AND month = ?",
            (user_id, month)
        ).fetchone()
        current = row[0] if row else 0
        
        # Check quota
        within_quota = (current + tokens) <= limit
        
        # Record usage regardless (for tracking)
        conn.execute("""
            INSERT INTO token_usage (user_id, month, tokens_used, cost_usd)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id, month) DO UPDATE SET
                tokens_used = tokens_used + ?,
                cost_usd = cost_usd + ?
        """, (user_id, month, tokens, cost_usd, tokens, cost_usd))
        conn.commit()
        conn.close()
        
        return within_quota
    
    def check_quota(self, user_id: str) -> bool:
        """Check if user is within quota."""
        usage = self.get_usage(user_id)
        return not usage["over_limit"]
