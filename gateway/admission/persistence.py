"""SQLite persistence for admission queue items."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import List

from .types import QueueItem


def init_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS queue_items (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            user_role TEXT NOT NULL,
            message TEXT NOT NULL,
            lane TEXT NOT NULL,
            priority INTEGER NOT NULL,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            started_at TEXT,
            completed_at TEXT,
            result TEXT,
            chat_id TEXT,
            thread_id TEXT,
            platform TEXT
        )
        """
    )
    conn.commit()
    conn.close()


def save_items(db_path: Path, items: List[QueueItem]) -> None:
    init_db(db_path)
    conn = sqlite3.connect(db_path)
    conn.execute("DELETE FROM queue_items WHERE status = 'queued'")

    for item in items:
        conn.execute(
            """
            INSERT OR REPLACE INTO queue_items (
                id, user_id, user_role, message, lane, priority, status,
                created_at, started_at, completed_at, result,
                chat_id, thread_id, platform
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                item.id,
                item.user_id,
                item.user_role,
                item.message,
                item.lane,
                item.priority,
                item.status,
                item.created_at.isoformat(),
                item.started_at.isoformat() if item.started_at else None,
                item.completed_at.isoformat() if item.completed_at else None,
                json.dumps(item.result) if item.result else None,
                item.chat_id,
                item.thread_id,
                item.platform,
            ),
        )

    conn.commit()
    conn.close()


def load_items(db_path: Path) -> List[QueueItem]:
    if not db_path.exists():
        return []

    init_db(db_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.execute(
        "SELECT id, user_id, user_role, message, lane, priority, status, created_at, started_at, completed_at, result, chat_id, thread_id, platform FROM queue_items WHERE status = 'queued'"
    )

    items: list[QueueItem] = []
    for row in cursor.fetchall():
        items.append(
            QueueItem(
                id=row[0],
                user_id=row[1],
                user_role=row[2],
                message=row[3],
                lane=row[4],
                priority=row[5],
                status=row[6],
                created_at=datetime.fromisoformat(row[7]),
                started_at=datetime.fromisoformat(row[8]) if row[8] else None,
                completed_at=datetime.fromisoformat(row[9]) if row[9] else None,
                result=json.loads(row[10]) if row[10] else None,
                chat_id=row[11],
                thread_id=row[12],
                platform=row[13],
            )
        )

    conn.close()
    return items
