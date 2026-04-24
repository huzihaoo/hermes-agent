"""Audit logging for admission decisions."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal

AuditResult = Literal["allowed", "denied", "confirmed", "approved", "queued", "completed", "failed"]


@dataclass
class AuditEvent:
    user_id: str
    action: str
    resource: str
    result: AuditResult
    metadata: dict | None = None
    timestamp: datetime | None = None

    def __post_init__(self) -> None:
        if self.timestamp is None:
            self.timestamp = datetime.now()


def log_audit(event: AuditEvent, audit_dir: Path | None = None) -> None:
    """Append one audit event to the daily JSONL file."""
    if audit_dir is None:
        audit_dir = Path.home() / ".hermes" / "audit"

    audit_dir.mkdir(parents=True, exist_ok=True)
    log_file = audit_dir / f"{event.timestamp.strftime('%Y-%m-%d')}.jsonl"

    payload = asdict(event)
    payload["timestamp"] = event.timestamp.isoformat()

    with log_file.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")
