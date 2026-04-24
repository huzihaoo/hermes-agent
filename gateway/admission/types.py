"""Admission queue data types."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

Lane = Literal["fast", "standard", "heavy"]
QueueStatus = Literal["queued", "processing", "completed", "failed", "cancelled"]


@dataclass
class QueueItem:
    """A single item in the admission queue."""

    id: str
    user_id: str
    user_role: str  # owner / admin / senior / member
    message: str
    lane: Lane
    priority: int  # owner=100, admin=50, senior=30, member=10
    status: QueueStatus = "queued"
    created_at: datetime = field(default_factory=datetime.now)
    started_at: datetime | None = None
    completed_at: datetime | None = None
    result: dict | None = None
    # Optional context for reconstructing the event after dequeue
    chat_id: str | None = None
    thread_id: str | None = None
    platform: str | None = None
