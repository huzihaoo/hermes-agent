"""Shared-state v2 task submission helper for VM worker execution."""

from __future__ import annotations

import json
import re
import shlex
import shutil
import sqlite3
import subprocess
import sys
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


def _integration_tools_session_vm_permission_open() -> bool:
    """Return True when current session is the integration_tools intake group.

    Business policy: members of that group have VM system/tool business
    execution permission by default. This bypasses only the coarse role gate;
    risk_class=high and governed MCAP/destructive guards remain outside this
    helper and must still fail closed.
    """
    chat_id = _session_value("HERMES_SESSION_CHAT_ID")
    if not chat_id:
        return False
    try:
        from hermes_cli.config import cfg_get, load_config

        cfg = load_config() or {}
        block = cfg_get(cfg, "business_lines", "integration_tools", default={}) or {}
        if not isinstance(block, dict) or not bool(block.get("enabled", False)):
            return False
        vm_policy = block.get("vm_tool_business_permission") or {}
        if isinstance(vm_policy, dict) and vm_policy.get("group_default_allowed") is False:
            return False
        raw_ids: list[Any] = []
        for key in ("intake_chat_ids", "intake_chat_id", "intake_group_ids", "intake_group_id"):
            value = block.get(key)
            if isinstance(value, (list, tuple, set)):
                raw_ids.extend(value)
            elif value:
                raw_ids.append(value)
        return chat_id in {str(v).strip() for v in raw_ids if str(v or "").strip()}
    except Exception:
        return False


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


def _check_vm_task_permission(user_name: str, user_id: str = "", risk_class: str = "") -> dict[str, Any] | None:
    # integration_tools business group defaults VM system/tool execution open.
    # Keep explicit high-risk tasks blocked for manual_review/escalation.
    if _integration_tools_session_vm_permission_open() and str(risk_class or "").strip().lower() != "high":
        return None
    try:
        from tools.permission_policy import get_user_role, get_user_role_by_id

        role = get_user_role_by_id(user_id) if user_id else get_user_role(user_name)
    except Exception:
        return _vm_task_permission_policy_unavailable_payload()
    if role not in {"owner", "admin", "senior"}:
        return _vm_task_permission_denied_payload(user_name, role)
    return None


_DEFAULT_BRIDGE_ROOT = Path.home() / "Mounts" / "mini_root" / "tmp" / "openclaw-shared-state"
_DEFAULT_HOST_CANONICAL_ROOT = get_hermes_home() / "runtime" / "shared-state"
_DEFAULT_VM_CANONICAL_ROOT = Path.home() / "Mounts" / "mini_root" / ".hermes" / "shared-state"
_TASK_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_ALLOWED_LANES = {"fast", "standard", "heavy"}
_ALLOWED_RESOURCE_CLASSES = {"cpu", "io", "repo", "pnc_data", "network", "mixed"}
_ALLOWED_WORKSPACE_SCOPES = {"owner_main_repo", "user_worktree", "shared_nested_repo", "none", "unknown"}
_ALLOWED_RISK_CLASSES = {"low", "normal", "high"}
_ALLOWED_EXECUTOR_TYPES = {"coding_agent", "direct_cli", "governed_tool"}
_ALLOWED_AGENT_BACKENDS = {"codex", "openclaw"}
_SAFE_PATH_SEGMENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_REPO_SCOPE_RE = _SAFE_PATH_SEGMENT_RE
_VM_ARTIFACT_PREFIX = "/mnt/tmp/"
_CIFS_ARTIFACT_PREFIX = "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/"
_VM_PATH_CONTRACT = """

---
VM path contract:
- Code/repos live under `/home/mini/<repo>` for owner/admin main repositories, or `/home/mini/worktrees/<repo>/<user>` for member/senior isolated worktrees.
- Shared-state execution truth lives under `/home/mini/.hermes/shared-state`; legacy/OpenClaw bridge state may appear under `/home/mini/tmp/openclaw-shared-state`.
- Default task data, downloads, conversion intermediates, caches, raw packages, and generated artifacts must go under `/mnt/tmp/<task_id>/` and `/mnt/tmp/<task_id>/downloads`.
- For integration_tools mdrive4 work, generated artifacts also go under `/mnt/tmp/<task_id>/` (owner decision 2026-06-12); `/mnt/minieye/mdrive4` source data stays read-only by default.
- User-visible CIFS path for `/mnt/tmp/<task_id>/` is `//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/<task_id>/`.
- Mounted data/tool roots include `/mnt/evaluation_data`, `/mnt/pnc_tools`, and mdrive4 data at `/mnt/minieye/mdrive4`.
- Do not default task artifacts to `~/Downloads`, `/tmp`, repo source dirs, `~/.cache`, or old `/home/mini/nas/miniPan/tmp/...` unless explicitly requested.
- If the user asks where a download/output/path is, or says a path cannot be found, answer with both the VM-internal path and the user-visible/shareable path.
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
            "task_id": {
                "type": "string",
                "description": "Optional explicit shared-state task id. Leave empty unless a coordinator must pre-reserve a completion-probe-visible id.",
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
            "executor_type": {
                "type": "string",
                "enum": ["coding_agent", "direct_cli", "governed_tool"],
                "description": (
                    "VM execution plane. Use coding_agent for repository/code understanding, edits, refactors, "
                    "debugging, and test-fix tasks; use direct_cli/governed_tool only for deterministic bounded "
                    "tool or wrapper tasks. Defaults to coding_agent."
                ),
            },
            "agent_backend": {
                "type": "string",
                "enum": ["codex", "openclaw"],
                "description": "Coding-agent backend when executor_type=coding_agent. Defaults to codex.",
            },
            "codex_backend_enabled": {
                "type": "boolean",
                "description": "Enable VM-side Codex backend for coding_agent tasks. Defaults to true when agent_backend=codex.",
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


def _host_completion_probe_script() -> Path:
    return Path(__file__).resolve().with_name("vm_task_completion_probe.py")


def _python_executable() -> str:
    return shutil.which("python3.11") or shutil.which("python3") or "python3"


def _validate_optional_enum(name: str, value: str, allowed: set[str]) -> str | None:
    if not value:
        return None
    if value not in allowed:
        return f"invalid {name}: {value!r}; expected one of {sorted(allowed)}"
    return None


def _validate_artifact_root(name: str, value: str, prefixes: str | tuple[str, ...]) -> str | None:
    if not value:
        return None
    if isinstance(prefixes, str):
        allowed_prefixes = (prefixes,)
    else:
        allowed_prefixes = tuple(prefixes)
    matched_prefix = next((prefix for prefix in allowed_prefixes if value.startswith(prefix)), "")
    if not matched_prefix:
        allowed = " or ".join(f"{prefix}<task_id>/" for prefix in allowed_prefixes)
        return f"invalid {name}: must be under {allowed}"
    relative = value[len(matched_prefix):]
    if not relative:
        return f"invalid {name}: must include task id under {matched_prefix}"
    parts = [part for part in relative.split("/") if part]
    if not parts:
        return f"invalid {name}: must include task id under {matched_prefix}"
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
    executor_type: str = "",
    agent_backend: str = "",
    codex_backend_enabled: bool | None = None,
) -> tuple[dict[str, Any], str | None]:
    values = {
        "lane": str(lane or "").strip(),
        "resource_class": str(resource_class or "").strip(),
        "repo_scope": str(repo_scope or "").strip(),
        "workspace_scope": str(workspace_scope or "").strip(),
        "risk_class": str(risk_class or "").strip(),
        "artifact_root": str(artifact_root or "").strip(),
        "artifact_cifs_root": str(artifact_cifs_root or "").strip(),
        "executor_type": str(executor_type or "coding_agent").strip(),
        "agent_backend": str(agent_backend or "codex").strip(),
    }
    checks = (
        ("lane", values["lane"], _ALLOWED_LANES),
        ("resource_class", values["resource_class"], _ALLOWED_RESOURCE_CLASSES),
        ("workspace_scope", values["workspace_scope"], _ALLOWED_WORKSPACE_SCOPES),
        ("risk_class", values["risk_class"], _ALLOWED_RISK_CLASSES),
        ("executor_type", values["executor_type"], _ALLOWED_EXECUTOR_TYPES),
        ("agent_backend", values["agent_backend"], _ALLOWED_AGENT_BACKENDS),
    )
    for name, value, allowed in checks:
        error = _validate_optional_enum(name, value, allowed)
        if error:
            return {}, error
    if values["repo_scope"] and not _REPO_SCOPE_RE.match(values["repo_scope"]):
        return {}, "invalid repo_scope: must be a safe repository label, not a filesystem path"
    artifact_root_error = _validate_artifact_root("artifact_root", values["artifact_root"], (_VM_ARTIFACT_PREFIX,))
    if artifact_root_error:
        return {}, artifact_root_error
    cifs_root_error = _validate_artifact_root("artifact_cifs_root", values["artifact_cifs_root"], _CIFS_ARTIFACT_PREFIX)
    if cifs_root_error:
        return {}, cifs_root_error
    meta: dict[str, Any] = {k: v for k, v in values.items() if v}
    if values["executor_type"] == "coding_agent" and values["agent_backend"] == "codex":
        meta["codex_backend_enabled"] = True if codex_backend_enabled is None else bool(codex_backend_enabled)
    elif codex_backend_enabled is not None:
        meta["codex_backend_enabled"] = bool(codex_backend_enabled)
    return meta, None


def vm_task_submit(
    title: str,
    goal: str,
    task_id: str = "",
    owner: str = "",
    user_id: str = "",
    lane: str = "",
    resource_class: str = "",
    repo_scope: str = "",
    workspace_scope: str = "",
    risk_class: str = "",
    artifact_root: str = "",
    artifact_cifs_root: str = "",
    executor_type: str = "",
    agent_backend: str = "",
    codex_backend_enabled: bool | None = None,
) -> Dict[str, Any]:
    """Create and bridge-deliver a shared-state v2 task for VM worker pickup."""
    title = str(title or "").strip()
    goal = str(goal or "").strip()
    trusted_user_name, trusted_user_id = _resolve_submitter(user_id, owner)
    if trusted_user_name or trusted_user_id:
        permission_error = _check_vm_task_permission(trusted_user_name, trusted_user_id, risk_class)
        if permission_error:
            return permission_error
        owner = trusted_user_name or trusted_user_id
    else:
        owner = str(owner or "").strip()
    if not title:
        return {"success": False, "error": "title is required"}
    if not goal:
        return {"success": False, "error": "goal is required"}
    task_id = str(task_id or "").strip()
    if task_id and not _TASK_ID_RE.match(task_id):
        return {"success": False, "error": "invalid task_id: must be a safe shared-state task id"}
    scheduler_meta, scheduler_meta_error = _build_scheduler_meta(
        lane=lane,
        resource_class=resource_class,
        repo_scope=repo_scope,
        workspace_scope=workspace_scope,
        risk_class=risk_class,
        artifact_root=artifact_root,
        artifact_cifs_root=artifact_cifs_root,
        executor_type=executor_type,
        agent_backend=agent_backend,
        codex_backend_enabled=codex_backend_enabled,
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
        # Feishu topic completion/progress relays need the original message id as
        # a reply anchor.  thread_id alone can identify the topic, but carrying
        # message_id through shared-state metadata makes the route auditable and
        # lets future relays fail closed instead of falling back to the parent
        # group when a topic anchor is lost.
        "message_id": "HERMES_SESSION_MESSAGE_ID",
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
    if task_id:
        cmd.extend(["--task-id", task_id])
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

    notify_process: dict[str, Any] = {}
    if proc.returncode == 0:
        task_id = str(parsed.get("task_id") or parsed.get("id") or "").strip()
        notify_process = _spawn_completion_probe_background(task_id)

    return {
        "success": proc.returncode == 0,
        "returncode": proc.returncode,
        "task": parsed,
        "stderr": (proc.stderr or "").strip(),
        "notify_process": notify_process,
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


def _is_safe_completion_probe_task_id(task_id: str) -> bool:
    """Fail closed for production notify probes; test/mock ids must not create real Feishu callbacks."""
    if not task_id or not _TASK_ID_RE.match(task_id):
        return False
    # Real shared-state tasks are timestamp-prefixed (YYYYMMDD-HHMMSS-...).
    # Short fixture ids such as "t1" previously leaked from tests into the
    # live process watcher and caused repeated Feishu topic notifications.
    return bool(re.match(r"^\d{8}-\d{6}-[A-Za-z0-9][A-Za-z0-9_.-]*$", task_id))


def _spawn_completion_probe_background(task_id: str) -> dict[str, Any]:
    """Start a host-side completion probe so VM task terminal state returns to the origin topic.

    VM/shared-state tasks are not Hermes-managed background processes, so the
    gateway process watcher cannot see them by itself.  Start the existing host
    relay script as a managed background process with notify_on_complete=True;
    when it exits, the gateway injects a synthetic internal event into the
    original Feishu topic and the agent can summarize the result there.
    """
    task_id = str(task_id or "").strip()
    if not _is_safe_completion_probe_task_id(task_id):
        return {"started": False, "reason": "missing_or_invalid_or_test_task_id"}
    script = _host_completion_probe_script()
    if not script.exists():
        return {"started": False, "reason": f"completion probe script not found: {script}"}
    try:
        from tools.terminal_tool import terminal_tool

        command = " ".join(
            [
                shlex.quote(sys.executable or _python_executable()),
                shlex.quote(str(script)),
                "--task-id",
                shlex.quote(task_id),
            ]
        )
        try:
            has_gateway_route = bool(_session_value("HERMES_SESSION_PLATFORM") and _session_value("HERMES_SESSION_CHAT_ID"))
        except Exception:
            has_gateway_route = False
        raw = terminal_tool(command=command, background=True, notify_on_complete=has_gateway_route, timeout=600)
        try:
            payload = json.loads(raw) if raw else {}
        except Exception:
            payload = {"raw": raw}
        return {
            "started": bool(payload.get("session_id")),
            "session_id": payload.get("session_id", ""),
            "notify_on_complete": bool(payload.get("notify_on_complete")),
            "error": payload.get("error"),
        }
    except Exception as exc:
        return {"started": False, "reason": f"failed_to_start_completion_probe: {type(exc).__name__}: {exc}"}


def vm_task_submit_json(
    title: str,
    goal: str,
    task_id: str = "",
    owner: str = "",
    user_id: str = "",
    lane: str = "",
    resource_class: str = "",
    repo_scope: str = "",
    workspace_scope: str = "",
    risk_class: str = "",
    artifact_root: str = "",
    artifact_cifs_root: str = "",
    executor_type: str = "",
    agent_backend: str = "",
    codex_backend_enabled: bool | None = None,
) -> str:
    return json.dumps(
        vm_task_submit(
            title=title,
            goal=goal,
            task_id=task_id,
            owner=owner,
            user_id=user_id,
            lane=lane,
            resource_class=resource_class,
            repo_scope=repo_scope,
            workspace_scope=workspace_scope,
            risk_class=risk_class,
            artifact_root=artifact_root,
            artifact_cifs_root=artifact_cifs_root,
            executor_type=executor_type,
            agent_backend=agent_backend,
            codex_backend_enabled=codex_backend_enabled,
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


def _read_task_db_row(root: Path, task_id: str) -> dict[str, Any]:
    db_path = root / "state.db"
    if not db_path.is_file():
        return {}
    conn = None
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=1)
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id,)).fetchone()
    except sqlite3.Error:
        return {}
    finally:
        if conn is not None:
            conn.close()
    return dict(row) if row is not None else {}


def _first_non_empty(*values: Any) -> Any:
    for value in values:
        if value not in (None, ""):
            return value
    return ""


def vm_task_status(task_id: str, include_markdown: bool = True) -> Dict[str, Any]:
    task_id = str(task_id or "").strip()
    if not task_id or not _TASK_ID_RE.match(task_id):
        return {"success": False, "error": f"invalid task_id: {task_id!r}"}

    roots = []
    for candidate_root in (_DEFAULT_HOST_CANONICAL_ROOT, _DEFAULT_VM_CANONICAL_ROOT):
        root_path = Path(candidate_root)
        if root_path not in roots:
            roots.append(root_path)

    checked_roots: list[str] = []
    for root in roots:
        checked_roots.append(str(root))
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
            continue

        status_path = task_dir / "status.md"
        result_path = task_dir / "result.md"
        meta_path = task_dir / "meta.json"
        meta = _read_json_if_present(meta_path)
        db_row = _read_task_db_row(root, task_id)
        state = str(_first_non_empty(dispatch_payload.get("state"), db_row.get("state"), meta.get("state"), dispatch_queue, "unknown"))
        payload: Dict[str, Any] = {
            "success": True,
            "task_id": task_id,
            "state": state,
            "dispatch_queue": dispatch_queue,
            "summary": _first_non_empty(
                dispatch_payload.get("summary"),
                dispatch_payload.get("latest_summary"),
                db_row.get("latest_summary"),
                db_row.get("summary"),
                meta.get("summary"),
            ),
            "owner": _first_non_empty(dispatch_payload.get("owner"), db_row.get("owner"), meta.get("owner")),
            "updated_at": _first_non_empty(dispatch_payload.get("updated_at"), db_row.get("updated_at"), meta.get("updated_at")),
            "run_id": _first_non_empty(dispatch_payload.get("run_id"), db_row.get("run_id"), meta.get("run_id")),
            "agent_host": _first_non_empty(dispatch_payload.get("agent_host"), db_row.get("agent_host"), meta.get("agent_host")),
            "paths": {
                "root": str(root),
                "checked_roots": checked_roots,
                "task_dir": str(task_dir),
                "status_md": str(status_path),
                "result_md": str(result_path),
            },
        }
        if include_markdown:
            payload["status_md"] = _read_text_if_present(status_path)
            payload["result_md"] = _read_text_if_present(result_path)
        return payload

    return {
        "success": False,
        "task_id": task_id,
        "state": "missing",
        "error": f"task not found in shared-state: {task_id}",
        "paths": {"root": str(roots[0]), "checked_roots": checked_roots},
    }


def vm_task_status_json(task_id: str, include_markdown: bool = True) -> str:
    return json.dumps(vm_task_status(task_id=task_id, include_markdown=include_markdown), ensure_ascii=False)


registry.register(
    name="vm_task_submit",
    toolset="vm_tasks",
    schema=VM_TASK_SUBMIT_SCHEMA,
    handler=lambda args, **kw: vm_task_submit_json(
        title=args.get("title", ""),
        goal=args.get("goal", ""),
        task_id=args.get("task_id", ""),
        owner=args.get("owner", ""),
        user_id=kw.get("user_id", ""),
        lane=args.get("lane", ""),
        resource_class=args.get("resource_class", ""),
        repo_scope=args.get("repo_scope", ""),
        workspace_scope=args.get("workspace_scope", ""),
        risk_class=args.get("risk_class", ""),
        artifact_root=args.get("artifact_root", ""),
        artifact_cifs_root=args.get("artifact_cifs_root", ""),
        executor_type=args.get("executor_type", ""),
        agent_backend=args.get("agent_backend", ""),
        codex_backend_enabled=args.get("codex_backend_enabled") if "codex_backend_enabled" in args else None,
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
