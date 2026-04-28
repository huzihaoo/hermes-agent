"""Shared-state v2 task submission helper for VM worker execution."""

from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Dict

from hermes_constants import get_hermes_home
from tools.registry import registry

_DEFAULT_BRIDGE_ROOT = Path.home() / "Mounts" / "mini_root" / "tmp" / "openclaw-shared-state"

VM_TASK_SUBMIT_SCHEMA = {
    "name": "vm_task_submit",
    "description": (
        "Submit a long-running VM/business task to shared-state v2 so the VM worker executes it. "
        "Use this instead of direct ssh-mini-run / ssh-mini-agent write execution for Feishu VM tasks."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "title": {
                "type": "string",
                "description": "Short human-readable task title.",
            },
            "goal": {
                "type": "string",
                "description": "Full self-contained VM-visible task brief. Include repo, branch, user/worktree, paths, expected verification, and output requirements.",
            },
            "owner": {
                "type": "string",
                "description": "Optional owner/requester label.",
            },
        },
        "required": ["title", "goal"],
    },
}


def _create_task_script() -> Path:
    return get_hermes_home() / "workspace-work" / "bin" / "create_task_v2.py"


def _python_executable() -> str:
    return shutil.which("python3.11") or shutil.which("python3") or "python3"


def vm_task_submit(title: str, goal: str, owner: str = "") -> Dict[str, Any]:
    """Create and bridge-deliver a shared-state v2 task for VM worker pickup."""
    title = str(title or "").strip()
    goal = str(goal or "").strip()
    owner = str(owner or "").strip()
    if not title:
        return {"success": False, "error": "title is required"}
    if not goal:
        return {"success": False, "error": "goal is required"}

    create_task = _create_task_script()
    if not create_task.exists():
        return {"success": False, "error": f"create_task_v2.py not found: {create_task}"}

    with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".md", delete=False) as f:
        f.write(goal)
        goal_file = f.name

    cmd = [
        _python_executable(),
        str(create_task),
        "--title",
        title,
        "--goal-file",
        goal_file,
        "--bridge-root",
        str(_DEFAULT_BRIDGE_ROOT),
        "--deliver-bridge",
        "--json",
    ]
    if owner:
        cmd.extend(["--owner", owner])

    try:
        proc = subprocess.run(cmd, text=True, capture_output=True, timeout=120)
    except FileNotFoundError as exc:
        return {"success": False, "error": f"failed to launch task creator: {exc}", "returncode": None}
    except subprocess.TimeoutExpired as exc:
        return {
            "success": False,
            "error": "task creation timed out",
            "returncode": None,
            "stdout": (exc.stdout or "").strip() if isinstance(exc.stdout, str) else "",
            "stderr": (exc.stderr or "").strip() if isinstance(exc.stderr, str) else "",
        }
    except Exception as exc:
        return {"success": False, "error": f"task creation failed: {type(exc).__name__}: {exc}", "returncode": None}
    finally:
        try:
            Path(goal_file).unlink(missing_ok=True)
        except Exception:
            pass

    raw = (proc.stdout or "").strip()
    try:
        parsed = json.loads(raw) if raw else {}
    except Exception:
        parsed = {"raw_stdout": raw}

    return {
        "success": proc.returncode == 0,
        "returncode": proc.returncode,
        "task": parsed,
        "stderr": (proc.stderr or "").strip(),
        "routing": {
            "host_state": "host-created" if proc.returncode == 0 else "failed",
            "delivery_attempted": True,
            "bridge_root": str(_DEFAULT_BRIDGE_ROOT),
            "next_truth_checks": [
                "confirm task appears in VM canonical queue before saying delivered-to-VM",
                "confirm VM worker claim before saying picked-up",
                "use canonical status reader/result import for completion truth",
            ],
        },
    }


def vm_task_submit_json(title: str, goal: str, owner: str = "") -> str:
    return json.dumps(vm_task_submit(title=title, goal=goal, owner=owner), ensure_ascii=False)


registry.register(
    name="vm_task_submit",
    toolset="vm_tasks",
    schema=VM_TASK_SUBMIT_SCHEMA,
    handler=lambda args, **kw: vm_task_submit_json(
        title=args.get("title", ""),
        goal=args.get("goal", ""),
        owner=args.get("owner", ""),
    ),
    emoji="🛰️",
)
