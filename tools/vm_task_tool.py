"""Shared-state v2 task submission helper for VM worker execution."""

from __future__ import annotations

import json
import re
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
_DEFAULT_VM_CANONICAL_ROOT = Path.home() / "Mounts" / "mini_root" / ".hermes" / "shared-state"
_TASK_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")

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

VM_TASK_STATUS_SCHEMA = {
    "name": "vm_task_status",
    "description": (
        "Read shared-state v2 canonical status/result for a VM task by task_id. "
        "Use after vm_task_submit before telling the user a VM task was picked up or completed."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "task_id": {
                "type": "string",
                "description": "Shared-state v2 task id returned by vm_task_submit.",
            },
            "include_markdown": {
                "type": "boolean",
                "description": "Include status.md and result.md snippets when present. Defaults to true.",
            },
        },
        "required": ["task_id"],
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

    # Capture session routing context for Feishu delivery
    routing_meta = {}
    routing_env_map = {
        "platform": "HERMES_SESSION_PLATFORM",
        "chat_id": "HERMES_SESSION_CHAT_ID",
        "chat_name": "HERMES_SESSION_CHAT_NAME",
        "thread_id": "HERMES_SESSION_THREAD_ID",
        "user_id": "HERMES_SESSION_USER_ID",
        "user_name": "HERMES_SESSION_USER_NAME",
        "session_key": "HERMES_SESSION_KEY",
    }
    for meta_key, env_key in routing_env_map.items():
        value = _session_value(env_key)
        if value:
            routing_meta[meta_key] = value

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
    if routing_meta:
        cmd.extend(["--meta", json.dumps(routing_meta, ensure_ascii=False)])

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


def _read_text_if_present(path: Path, *, limit_chars: int = 12000) -> str:
    if not path.is_file():
        return ""
    text = path.read_text(encoding="utf-8", errors="replace")
    if len(text) <= limit_chars:
        return text
    return text[:limit_chars] + "\n...[truncated]"


def _read_json_if_present(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def vm_task_status(task_id: str, include_markdown: bool = True) -> Dict[str, Any]:
    task_id = str(task_id or "").strip()
    if not task_id or not _TASK_ID_RE.match(task_id):
        return {"success": False, "error": f"invalid task_id: {task_id!r}"}

    root = _DEFAULT_VM_CANONICAL_ROOT
    task_dir = root / "tasks" / task_id
    dispatch_queue = ""
    dispatch_payload: dict[str, Any] = {}
    for queue in ("pending", "claimed", "completed", "failed"):
        candidate = root / "dispatch" / queue / f"{task_id}.json"
        if candidate.is_file():
            dispatch_queue = queue
            dispatch_payload = _read_json_if_present(candidate)
            break

    if not task_dir.exists() and not dispatch_queue:
        return {
            "success": False,
            "task_id": task_id,
            "state": "missing",
            "error": f"task not found in shared-state: {task_id}",
            "paths": {"root": str(root)},
        }

    status_path = task_dir / "status.md"
    result_path = task_dir / "result.md"
    meta_path = task_dir / "meta.json"
    meta = _read_json_if_present(meta_path)
    state = str(dispatch_payload.get("state") or meta.get("state") or dispatch_queue or "unknown")
    payload: Dict[str, Any] = {
        "success": True,
        "task_id": task_id,
        "state": state,
        "dispatch_queue": dispatch_queue,
        "summary": dispatch_payload.get("summary") or dispatch_payload.get("latest_summary") or meta.get("summary") or "",
        "owner": dispatch_payload.get("owner") or meta.get("owner") or "",
        "updated_at": dispatch_payload.get("updated_at") or meta.get("updated_at") or "",
        "run_id": dispatch_payload.get("run_id") or meta.get("run_id") or "",
        "agent_host": dispatch_payload.get("agent_host") or meta.get("agent_host") or "",
        "paths": {
            "root": str(root),
            "task_dir": str(task_dir),
            "status_md": str(status_path),
            "result_md": str(result_path),
        },
    }
    if include_markdown:
        payload["status_md"] = _read_text_if_present(status_path)
        payload["result_md"] = _read_text_if_present(result_path)
    return payload


def vm_task_status_json(task_id: str, include_markdown: bool = True) -> str:
    return json.dumps(vm_task_status(task_id=task_id, include_markdown=include_markdown), ensure_ascii=False)


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

registry.register(
    name="vm_task_status",
    toolset="vm_tasks",
    schema=VM_TASK_STATUS_SCHEMA,
    handler=lambda args, **kw: vm_task_status_json(
        task_id=args.get("task_id", ""),
        include_markdown=args.get("include_markdown", True),
    ),
    emoji="📡",
)
