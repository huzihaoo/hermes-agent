"""Admission queue data types."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal

Lane = Literal["fast", "standard", "heavy"]
Domain = Literal["user", "group", "vm"]
QueueStatus = Literal["queued", "processing", "completed", "failed", "cancelled", "dead"]

# All valid lanes and domains as tuples for iteration
ALL_LANES: tuple[Lane, ...] = ("fast", "standard", "heavy")
ALL_DOMAINS: tuple[Domain, ...] = ("user", "group", "vm")

# Retry defaults
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_BASE_DELAY = 2.0  # seconds, exponential backoff base


@dataclass
class QueueItem:
    """A single item in the admission queue."""

    id: str
    user_id: str
    user_role: str  # owner / admin / senior / member
    message: str
    lane: Lane
    priority: int  # owner=100, admin=50, senior=30, member=10
    domain: Domain = "user"
    domain_id: str = ""  # user_id for user domain, chat_id for group, vm_id for vm
    status: QueueStatus = "queued"
    created_at: datetime = field(default_factory=datetime.now)
    started_at: datetime | None = None
    completed_at: datetime | None = None
    result: dict | None = None
    # Optional context for reconstructing the event after dequeue
    chat_id: str | None = None
    chat_type: str | None = None
    thread_id: str | None = None
    request_message_id: str | None = None
    platform: str | None = None
    # Allowlisted, adapter-built context needed to reconstruct trusted events.
    # User message text must never be interpreted as this structure.
    event_context: dict[str, Any] | None = None
    # Retry tracking
    retry_count: int = 0
    max_retries: int = DEFAULT_MAX_RETRIES
    last_error: str | None = None
    next_retry_at: datetime | None = None
