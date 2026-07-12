#!/usr/bin/env python3
"""Write Hermes Dashboard task-state sidecars for VM bridge progress.

This script is intentionally host-local and side-effect bounded: it only writes
``~/.hermes/task-state/<safe_task_id>.json`` so the existing Dashboard task
views can display VM progress without a new web service.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hermes_cli.config import get_hermes_home

_MAX_EVENTS = 50
_L4_EVENT_EPOCH_MAX = 4_102_444_800.0  # 2100-01-01T00:00:00Z


def safe_task_id(task_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", task_id or "default")[:120]


def sidecar_path(task_id: str) -> Path:
    return get_hermes_home() / "task-state" / f"{safe_task_id(task_id)}.json"


def _load_existing(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _append_unique(items: list[Any], value: Any | None) -> list[Any]:
    if value is None or value == "":
        return items
    if value not in items:
        items.append(value)
    return items


def _now_iso() -> str:
    return datetime.fromtimestamp(_now_epoch(), timezone.utc).isoformat()


def _now_epoch() -> float:
    """Return the sealed L4 event clock, otherwise the live UTC epoch."""

    if (
        os.getenv("HERMES_OUTBOUND_MODE", "").strip().lower() == "record-only"
        and os.getenv("HERMES_L4_SANDBOX_ACTIVE", "").strip() == "1"
    ):
        raw = os.getenv("HERMES_L4_EVENT_EPOCH", "").strip()
        if not raw:
            raise RuntimeError("record-only L4 sandbox requires HERMES_L4_EVENT_EPOCH")
        try:
            value = float(raw)
        except ValueError as exc:
            raise RuntimeError("HERMES_L4_EVENT_EPOCH must be a finite UTC epoch") from exc
        if not math.isfinite(value) or not 0.0 <= value <= _L4_EVENT_EPOCH_MAX:
            raise RuntimeError("HERMES_L4_EVENT_EPOCH is outside the accepted UTC epoch range")
        return value
    return time.time()


def _atomic_write_json(path: Path, body: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(body, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, path)
    finally:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass


def write_task_state(
    task_id: str,
    *,
    phase: str | None = None,
    event: str | None = None,
    artifact: str | None = None,
    verification: str | None = None,
    blocker: str | None = None,
    vm_summary: str | None = None,
    vm_state: str | None = None,
    work_tmp_dir: str | None = None,
    user_visible_path: str | None = None,
    progress_json: str | None = None,
) -> Path:
    path = sidecar_path(task_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    body = _load_existing(path)

    updated_at = _now_iso()
    body["updated_at"] = updated_at
    previous_phase = body.get("current_phase")
    if phase:
        body["current_phase"] = phase

    events = _list(body.get("recent_events"))
    if event:
        event_phase = phase or body.get("current_phase")
        same_state_event_exists = any(
            isinstance(item, dict)
            and str(item.get("phase") or "") == str(event_phase or "")
            and str(item.get("summary") or "") == str(event)
            for item in events
        )
        # Pipeline-progress rows may be interleaved with the collected state.
        # Suppress a poll repeat whenever the business phase itself did not
        # transition; the same event may recur after a real phase transition.
        if not (
            str(previous_phase or "") == str(event_phase or "")
            and same_state_event_exists
        ):
            events.append(
                {
                    "ts": updated_at,
                    "time": updated_at,
                    "phase": event_phase,
                    "summary": event,
                }
            )
    body["recent_events"] = events[-_MAX_EVENTS:]

    body["artifacts"] = _append_unique(_list(body.get("artifacts")), artifact)
    body["verification"] = _append_unique(_list(body.get("verification")), verification)
    body["blockers"] = _append_unique(_list(body.get("blockers")), blocker)

    existing_bridge = body.get("vm_bridge")
    vm_bridge: dict[str, Any] = dict(existing_bridge) if isinstance(existing_bridge, dict) else {}
    if vm_summary:
        vm_bridge["summary"] = vm_summary
    if vm_state:
        vm_bridge["state"] = vm_state
    if task_id:
        vm_bridge["vm_task_id"] = task_id
    if work_tmp_dir:
        vm_bridge["work_tmp_dir"] = work_tmp_dir
    if user_visible_path:
        vm_bridge["user_visible_path"] = user_visible_path
    if progress_json:
        try:
            progress = json.loads(progress_json)
        except json.JSONDecodeError:
            progress = {"raw": progress_json}
        if isinstance(progress, dict):
            vm_bridge["progress"] = progress
    if vm_bridge:
        body["vm_bridge"] = vm_bridge

    _atomic_write_json(path, body)
    return path


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--phase")
    parser.add_argument("--event")
    parser.add_argument("--artifact", action="append", default=[])
    parser.add_argument("--verification", action="append", default=[])
    parser.add_argument("--blocker", action="append", default=[])
    parser.add_argument("--vm-summary")
    parser.add_argument("--vm-state")
    parser.add_argument("--work-tmp-dir")
    parser.add_argument("--user-visible-path")
    parser.add_argument("--progress-json")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    path: Path | None = None
    max_len = max(len(args.artifact), len(args.verification), len(args.blocker), 1)
    for idx in range(max_len):
        path = write_task_state(
            args.task_id,
            phase=args.phase,
            event=args.event if idx == 0 else None,
            artifact=args.artifact[idx] if idx < len(args.artifact) else None,
            verification=args.verification[idx] if idx < len(args.verification) else None,
            blocker=args.blocker[idx] if idx < len(args.blocker) else None,
            vm_summary=args.vm_summary,
            vm_state=args.vm_state,
            work_tmp_dir=args.work_tmp_dir,
            user_visible_path=args.user_visible_path,
            progress_json=args.progress_json,
        )
    print(json.dumps({"ok": True, "path": str(path)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
