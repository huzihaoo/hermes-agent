#!/usr/bin/env python3
"""Poll one shared-state VM task until terminal and print its user-facing result.

This wrapper is intended to be launched as a Hermes managed background process
with notify_on_complete=True.  The gateway process watcher then injects the
final stdout back into the originating Feishu topic, giving VM/shared-state
submissions the same completion-notify behavior as ordinary background tools.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any


def _load_notify_module():
    workspace_root = Path(os.environ.get("HERMES_WORKSPACE_ROOT") or Path.home() / ".hermes" / "workspace-work")
    script = workspace_root / "bin" / "notify_delegated_task_completions.py"
    sys.path.insert(0, str(script.parent))
    import importlib.util

    spec = importlib.util.spec_from_file_location("notify_delegated_task_completions", script)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load notify script: {script}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _read_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8", errors="replace"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _state_for_task(root: Path, task_id: str) -> str:
    for queue in ("completed", "failed", "abandoned", "claimed", "pending"):
        dispatch = root / "dispatch" / queue / f"{task_id}.json"
        if dispatch.is_file():
            payload = _read_json(dispatch)
            return str(payload.get("state") or queue).strip().lower() or queue
    meta = _read_json(root / "tasks" / task_id / "meta.json")
    return str(meta.get("state") or "").strip().lower()


def main() -> int:
    parser = argparse.ArgumentParser(description="Poll one VM shared-state task and print final user-facing result.")
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--root", default=str(Path.home() / "Mounts" / "mini_root" / ".hermes" / "shared-state"))
    parser.add_argument("--interval", type=float, default=10.0)
    parser.add_argument("--timeout", type=float, default=21600.0)
    args = parser.parse_args()

    task_id = str(args.task_id).strip()
    root = Path(args.root).expanduser().resolve()
    task_dir = root / "tasks" / task_id
    deadline = time.monotonic() + max(1.0, args.timeout)
    terminal_states = {"completed", "failed", "abandoned", "blocked"}
    state = ""
    while time.monotonic() < deadline:
        state = _state_for_task(root, task_id)
        if state in terminal_states:
            break
        time.sleep(max(1.0, args.interval))
    else:
        print(f"VM 任务完成通知探针超时：{task_id}\n当前状态：{state or 'unknown'}")
        return 124

    notify = _load_notify_module()
    class Row(dict):
        def __getitem__(self, key):
            return self.get(key, "")

    meta = _read_json(task_dir / "meta.json")
    item = dict(meta)
    item["state"] = state
    item.setdefault("updated_at", meta.get("updated_at", ""))
    row = Row(
        task_id=task_id,
        title=meta.get("title") or task_id,
        state=state,
        updated_at=meta.get("updated_at", ""),
        sync_version=meta.get("sync_version", ""),
        latest_summary=meta.get("latest_summary") or meta.get("summary") or "",
    )
    entries = notify.stage_entries(row, item, task_dir)
    stage_key = entries[0][0] if entries else ("terminal:" + state if state in {"completed", "failed", "abandoned"} else "state:blocked")
    print(notify.build_notice(row, item, task_dir, stage_key))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
