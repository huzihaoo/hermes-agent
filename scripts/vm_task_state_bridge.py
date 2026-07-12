#!/usr/bin/env python3
"""Write Hermes Dashboard task-state sidecars for VM bridge progress.

This script is intentionally host-local and side-effect bounded: it only writes
``~/.hermes/task-state/<safe_task_id>.json`` so the existing Dashboard task
views can display VM progress without a new web service.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hermes_cli.config import get_hermes_home

_MAX_EVENTS = 50


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
    return datetime.now(timezone.utc).isoformat()


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
    if phase:
        body["current_phase"] = phase

    events = _list(body.get("recent_events"))
    if event:
        events.append({"ts": updated_at, "time": updated_at, "phase": phase or body.get("current_phase"), "summary": event})
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
