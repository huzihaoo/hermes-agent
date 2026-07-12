"""Worktree session audit — tracks user → worktree → branch → operations.

Standalone sidecar module. Does NOT modify gateway or admission source code.
Called from system_prompt-guided agent behavior and worktree management scripts.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Optional


AUDIT_DIR = Path.home() / ".hermes" / "audit" / "worktree"


def log_worktree_event(
    user: str,
    repo: str,
    action: str,
    *,
    branch: Optional[str] = None,
    worktree_path: Optional[str] = None,
    session_id: Optional[str] = None,
    detail: Optional[str] = None,
) -> None:
    """Append a worktree audit event to the daily JSONL file."""
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    now = datetime.now()
    log_file = AUDIT_DIR / f"{now.strftime('%Y-%m-%d')}.jsonl"

    event = {
        "timestamp": now.isoformat(),
        "user": user,
        "repo": repo,
        "action": action,  # create_worktree, checkout, edit, merge, push, delete
        "branch": branch,
        "worktree_path": worktree_path,
        "session_id": session_id,
        "detail": detail,
    }
    # Drop None values for compact output
    event = {k: v for k, v in event.items() if v is not None}

    with log_file.open("a", encoding="utf-8") as f:
        f.write(json.dumps(event, ensure_ascii=False) + "\n")


def query_user_activity(
    user: str,
    days: int = 7,
    repo: Optional[str] = None,
) -> list[dict]:
    """Query recent worktree activity for a user."""
    results = []
    today = datetime.now()
    for i in range(days):
        d = today.replace(hour=0, minute=0, second=0) - __import__("datetime").timedelta(days=i)
        log_file = AUDIT_DIR / f"{d.strftime('%Y-%m-%d')}.jsonl"
        if not log_file.exists():
            continue
        for line in log_file.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            event = json.loads(line)
            if event.get("user") != user:
                continue
            if repo and event.get("repo") != repo:
                continue
            results.append(event)
    return results
