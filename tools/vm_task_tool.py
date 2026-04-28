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


def _session_value(name: str) -> str:
    try:
        from gateway.session_context import get_session_env

        return (get_session_env(name, "") or "").strip()
    except Exception:
        return ""


def _resolve_submitter(user_id: str = "") -> tuple[str, str]:
    resolved_user_id = str(user_id or _session_value("HERMES_SESSION_USER_ID")).strip()
    user_name = _session_value("HERMES_SESSION_USER_NAME")
    if resolved_user_id:
        try:
            from tools.permission_policy import _load_config

            mapped = _load_config().get("user_id_mapping", {}).get(resolved_user_id)
            if mapped:
                user_name = str(mapped).strip()
        except Exception:
            pass
    return user_name, resolved_user_id


def _check_vm_task_permission(user_name: str, user_id: str = "") -> str | None:
    try:
        from tools.permission_policy import get_user_role, get_user_role_by_id

        role = get_user_role_by_id(user_id) if user_id else get_user_role(user_name)
    except Exception as exc:
        return f"permission policy unavailable for vm_task_submit; refusing VM task submission: {exc}"
    if role not in {"owner", "admin", "senior"}:
        return f"permission denied for vm_task_submit: role {role!r} is not allowed to submit VM worker tasks"
    return None


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
                "description": "Optional requester label. Ignored in gateway sessions; trusted session identity is used instead.",
            },
        },
        "required": ["title", "goal"],
    },
}


def _create_task_script() -> Path:
    return get_hermes_home() / "workspace-work" / "bin" / "create_task_v2.py"


def _python_executable() -> str:
    return shutil.which("python3.11") or shutil.which("python3") or "python3"


def vm_task_submit(title: str, goal: str, owner: str = "", user_id: str = "") -> Dict[str, Any]:
    """Create and bridge-deliver a shared-state v2 task for VM worker pickup."""
    title = str(title or "").strip()
    goal = str(goal or "").strip()
    trusted_user_name, trusted_user_id = _resolve_submitter(user_id)
    if trusted_user_name or trusted_user_id:
        permission_error = _check_vm_task_permission(trusted_user_name, trusted_user_id)
        if permission_error:
            return {"success": False, "error": permission_error, "returncode": None}
        owner = trusted_user_name or trusted_user_id
    else:
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


def vm_task_submit_json(title: str, goal: str, owner: str = "", user_id: str = "") -> str:
    return json.dumps(vm_task_submit(title=title, goal=goal, owner=owner, user_id=user_id), ensure_ascii=False)


registry.register(
    name="vm_task_submit",
    toolset="vm_tasks",
    schema=VM_TASK_SUBMIT_SCHEMA,
    handler=lambda args, **kw: vm_task_submit_json(
        title=args.get("title", ""),
        goal=args.get("goal", ""),
        owner=args.get("owner", ""),
        user_id=kw.get("user_id", ""),
    ),
    emoji="🛰️",
)
