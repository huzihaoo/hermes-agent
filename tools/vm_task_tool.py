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


def _resolve_submitter(user_id: str = "", owner: str = "") -> tuple[str, str]:
    resolved_user_id = str(user_id or _session_value("HERMES_SESSION_USER_ID")).strip()
    explicit_owner = str(owner or "").strip()
    user_name = _session_value("HERMES_SESSION_USER_NAME") or explicit_owner
    if resolved_user_id:
        try:
            from tools.permission_policy import _load_config

            mapped = _load_config().get("user_id_mapping", {}).get(resolved_user_id)
            if mapped:
                user_name = str(mapped).strip()
            elif explicit_owner and resolved_user_id == explicit_owner:
                # Local tool fallbacks sometimes pass a display name through the
                # user_id parameter because no platform open_id is available.
                # Treat an exact owner/display-name match as a name, not as an
                # unknown platform id that would fail closed to member.
                resolved_user_id = ""
                user_name = explicit_owner
        except Exception:
            if explicit_owner and resolved_user_id == explicit_owner:
                resolved_user_id = ""
                user_name = explicit_owner
    return user_name, resolved_user_id


def _vm_task_permission_denied_payload(user_name: str = "", role: str = "") -> dict[str, Any]:
    requester = str(user_name or "当前账号").strip() or "当前账号"
    return {
        "success": False,
        "error_code": "vm_task_permission_denied",
        "error": (
            f"{requester}当前账号没有 VM 编译/执行任务权限，本次未执行。"
            "请管理员授权 VM worker task 权限，或由 owner/有权限的人在同一线程重新发起。"
            "授权后会按审计规则在对应 worktree 执行，并回传 commit SHA、CI 链接和产物路径或失败摘要。"
        ),
        "retryable": False,
        "returncode": None,
    }


def _vm_task_permission_policy_unavailable_payload() -> dict[str, Any]:
    return {
        "success": False,
        "error_code": "vm_task_permission_policy_unavailable",
        "error": "暂时无法确认 VM 编译/执行任务权限，本次未执行。请稍后重试，或让管理员在同一线程重新发起。",
        "retryable": True,
        "returncode": None,
    }


def _check_vm_task_permission(user_name: str, user_id: str = "") -> dict[str, Any] | None:
    try:
        from tools.permission_policy import get_user_role, get_user_role_by_id

        role = get_user_role_by_id(user_id) if user_id else get_user_role(user_name)
    except Exception:
        return _vm_task_permission_policy_unavailable_payload()
    if role not in {"owner", "admin", "senior"}:
        return _vm_task_permission_denied_payload(user_name, role)
    return None


_DEFAULT_BRIDGE_ROOT = Path.home() / "Mounts" / "mini_root" / "tmp" / "openclaw-shared-state"
_DEFAULT_VM_CANONICAL_ROOT = Path.home() / "Mounts" / "mini_root" / ".hermes" / "shared-state"
_TASK_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_ALLOWED_LANES = {"fast", "standard", "heavy"}
_ALLOWED_RESOURCE_CLASSES = {"cpu", "io", "repo", "pnc_data", "network", "mixed"}
_ALLOWED_WORKSPACE_SCOPES = {"owner_main_repo", "user_worktree", "shared_nested_repo", "none", "unknown"}
_ALLOWED_RISK_CLASSES = {"low", "normal", "high"}
_SAFE_PATH_SEGMENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_REPO_SCOPE_RE = _SAFE_PATH_SEGMENT_RE
_VM_ARTIFACT_PREFIX = "/mnt/tmp/"
_CIFS_ARTIFACT_PREFIX = "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/"
_VM_PATH_CONTRACT = """

---
VM path contract:
- Code/repos live under `/home/mini/<repo>` for owner/admin main repositories, or `/home/mini/worktrees/<repo>/<user>` for member/senior isolated worktrees.
- Shared-state execution truth lives under `/home/mini/.hermes/shared-state`; legacy/OpenClaw bridge state may appear under `/home/mini/tmp/openclaw-shared-state`.
- Task data, downloads, conversion intermediates, caches, raw packages, and generated artifacts must default to `/mnt/tmp/<task_id>/` and `/mnt/tmp/<task_id>/downloads`.
- User-visible CIFS path for `/mnt/tmp/<task_id>/` is `//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/<task_id>/`.
- Mounted data/tool roots include `/mnt/evaluation_data` and `/mnt/pnc_tools`.
- Do not default task artifacts to `~/Downloads`, `/tmp`, repo source dirs, `~/.cache`, or old `/home/mini/nas/miniPan/tmp/...` unless explicitly requested.
- If the user asks where a download/output/path is, or says a path cannot be found, answer with both the VM-internal path and the user-visible CIFS path. If only one side is known, translate `/mnt/tmp/...` <-> `//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/...`.
""".strip()


def _goal_with_vm_path_contract(goal: str) -> str:
    if "VM path contract:" in goal:
        return goal
    return f"{goal.rstrip()}\n\n{_VM_PATH_CONTRACT}\n"

VM_TASK_SUBMIT_SCHEMA = {
    "name": "vm_task_submit",
    "description": (
        "Submit a long-running VM/business task to shared-state v2 so the VM worker executes it. "
        "Use this instead of direct ssh-mini-run / ssh-mini-agent write execution for Feishu VM tasks. "
        "The submitted goal is automatically appended with the VM path contract: code under /home/mini, "
        "artifacts under /mnt/tmp/<task_id>/, and user-visible CIFS paths under "
        "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/<task_id>/."
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
                "description": (
                    "Full self-contained VM-visible task brief. Include repo, branch, user/worktree, "
                    "expected verification, output requirements, and any known user-facing artifact/download path. "
                    "The tool appends the canonical VM path contract automatically."
                ),
            },
            "owner": {
                "type": "string",
                "description": "Optional requester label. Ignored in gateway sessions; trusted session identity is used instead.",
            },
            "lane": {
                "type": "string",
                "enum": ["fast", "standard", "heavy"],
                "description": "Optional VM scheduler lane metadata. Use heavy for PNC data/conversion/evaluation and long-running VM tasks.",
            },
            "resource_class": {
                "type": "string",
                "enum": ["cpu", "io", "repo", "pnc_data", "network", "mixed"],
                "description": "Optional VM scheduler resource-class metadata for slot/resource isolation.",
            },
            "repo_scope": {
                "type": "string",
                "description": "Optional VM scheduler repository scope, e.g. pnc_specs, minieye_dnp_nop, none, or unknown.",
            },
            "workspace_scope": {
                "type": "string",
                "enum": ["owner_main_repo", "user_worktree", "shared_nested_repo", "none", "unknown"],
                "description": "Optional VM scheduler workspace isolation metadata.",
            },
            "risk_class": {
                "type": "string",
                "enum": ["low", "normal", "high"],
                "description": "Optional VM scheduler risk metadata.",
            },
            "artifact_root": {
                "type": "string",
                "description": "Optional expected VM artifact root, preferably /mnt/tmp/<task_id>/.",
            },
            "artifact_cifs_root": {
                "type": "string",
                "description": "Optional user-visible CIFS artifact root corresponding to artifact_root.",
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


def _validate_optional_enum(name: str, value: str, allowed: set[str]) -> str | None:
    if not value:
        return None
    if value not in allowed:
        return f"invalid {name}: {value!r}; expected one of {sorted(allowed)}"
    return None


def _validate_artifact_root(name: str, value: str, prefix: str) -> str | None:
    if not value:
        return None
    if not value.startswith(prefix):
        return f"invalid {name}: must be under {prefix}<task_id>/"
    relative = value[len(prefix):]
    if not relative:
        return f"invalid {name}: must include task id under {prefix}"
    parts = [part for part in relative.split("/") if part]
    if not parts:
        return f"invalid {name}: must include task id under {prefix}"
    if any(part in {".", ".."} or not _SAFE_PATH_SEGMENT_RE.match(part) for part in parts):
        return f"invalid {name}: path segments must be safe labels without traversal"
    return None


def _build_scheduler_meta(
    *,
    lane: str = "",
    resource_class: str = "",
    repo_scope: str = "",
    workspace_scope: str = "",
    risk_class: str = "",
    artifact_root: str = "",
    artifact_cifs_root: str = "",
) -> tuple[dict[str, str], str | None]:
    values = {
        "lane": str(lane or "").strip(),
        "resource_class": str(resource_class or "").strip(),
        "repo_scope": str(repo_scope or "").strip(),
        "workspace_scope": str(workspace_scope or "").strip(),
        "risk_class": str(risk_class or "").strip(),
        "artifact_root": str(artifact_root or "").strip(),
        "artifact_cifs_root": str(artifact_cifs_root or "").strip(),
    }
    checks = (
        ("lane", values["lane"], _ALLOWED_LANES),
        ("resource_class", values["resource_class"], _ALLOWED_RESOURCE_CLASSES),
        ("workspace_scope", values["workspace_scope"], _ALLOWED_WORKSPACE_SCOPES),
        ("risk_class", values["risk_class"], _ALLOWED_RISK_CLASSES),
    )
    for name, value, allowed in checks:
        error = _validate_optional_enum(name, value, allowed)
        if error:
            return {}, error
    if values["repo_scope"] and not _REPO_SCOPE_RE.match(values["repo_scope"]):
        return {}, "invalid repo_scope: must be a safe repository label, not a filesystem path"
    artifact_root_error = _validate_artifact_root("artifact_root", values["artifact_root"], _VM_ARTIFACT_PREFIX)
    if artifact_root_error:
        return {}, artifact_root_error
    cifs_root_error = _validate_artifact_root("artifact_cifs_root", values["artifact_cifs_root"], _CIFS_ARTIFACT_PREFIX)
    if cifs_root_error:
        return {}, cifs_root_error
    return {k: v for k, v in values.items() if v}, None


def vm_task_submit(
    title: str,
    goal: str,
    owner: str = "",
    user_id: str = "",
    lane: str = "",
    resource_class: str = "",
    repo_scope: str = "",
    workspace_scope: str = "",
    risk_class: str = "",
    artifact_root: str = "",
    artifact_cifs_root: str = "",
) -> Dict[str, Any]:
    """Create and bridge-deliver a shared-state v2 task for VM worker pickup."""
    title = str(title or "").strip()
    goal = str(goal or "").strip()
    trusted_user_name, trusted_user_id = _resolve_submitter(user_id, owner)
    if trusted_user_name or trusted_user_id:
        permission_error = _check_vm_task_permission(trusted_user_name, trusted_user_id)
        if permission_error:
            return permission_error
        owner = trusted_user_name or trusted_user_id
    else:
        owner = str(owner or "").strip()
    if not title:
        return {"success": False, "error": "title is required"}
    if not goal:
        return {"success": False, "error": "goal is required"}
    scheduler_meta, scheduler_meta_error = _build_scheduler_meta(
        lane=lane,
        resource_class=resource_class,
        repo_scope=repo_scope,
        workspace_scope=workspace_scope,
        risk_class=risk_class,
        artifact_root=artifact_root,
        artifact_cifs_root=artifact_cifs_root,
    )
    if scheduler_meta_error:
        return {"success": False, "error": scheduler_meta_error}
    goal = _goal_with_vm_path_contract(goal)

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
    routing_meta.update(scheduler_meta)

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


def vm_task_submit_json(
    title: str,
    goal: str,
    owner: str = "",
    user_id: str = "",
    lane: str = "",
    resource_class: str = "",
    repo_scope: str = "",
    workspace_scope: str = "",
    risk_class: str = "",
    artifact_root: str = "",
    artifact_cifs_root: str = "",
) -> str:
    return json.dumps(
        vm_task_submit(
            title=title,
            goal=goal,
            owner=owner,
            user_id=user_id,
            lane=lane,
            resource_class=resource_class,
            repo_scope=repo_scope,
            workspace_scope=workspace_scope,
            risk_class=risk_class,
            artifact_root=artifact_root,
            artifact_cifs_root=artifact_cifs_root,
        ),
        ensure_ascii=False,
    )


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
        lane=args.get("lane", ""),
        resource_class=args.get("resource_class", ""),
        repo_scope=args.get("repo_scope", ""),
        workspace_scope=args.get("workspace_scope", ""),
        risk_class=args.get("risk_class", ""),
        artifact_root=args.get("artifact_root", ""),
        artifact_cifs_root=args.get("artifact_cifs_root", ""),
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
