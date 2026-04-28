"""PNC/MCU domain agent wrappers.

These tools expose the validated CLI agents from pnc_specs to Hermes/Feishu
users. The actual agents run on the mini VM through ssh-mini-agent so gateway
sessions can invoke them without needing direct VM shell access.
"""

from __future__ import annotations

import json
import os
import shlex
import subprocess
from pathlib import PurePosixPath
from typing import Any

from tools.registry import registry, tool_error, tool_result


REMOTE_AGENT_SUBDIR = os.getenv(
    "PNC_TOOLS_AGENT_SUBDIR",
    "pnc_tools_ai_native/32_AI_Native_repo_骨架包_真实首批版_v1",
)
REMOTE_AGENT_ROOT = os.getenv(
    "PNC_TOOLS_AGENT_ROOT",
    f"/home/mini/worktrees/pnc_specs/宋伟军/{REMOTE_AGENT_SUBDIR}",
)
REMOTE_WORKTREE_MANAGER = os.getenv(
    "PNC_TOOLS_WORKTREE_MANAGER",
    "/home/mini/.hermes/hermes-agent/gateway/admission/worktree_manager.py",
)
USER_ROLES_CONFIG = os.getenv(
    "PNC_TOOLS_USER_ROLES_CONFIG",
    "~/.hermes/config/user-roles.json",
)
DEFAULT_REPO = "pnc_specs"
LOCAL_WRAPPER = os.getenv("SSH_MINI_AGENT_BIN", "ssh-mini-agent")
DEFAULT_TIMEOUT_SECONDS = 300
MAX_TIMEOUT_SECONDS = 1800
MAX_TAIL_CHARS = 12000


def _is_absolute_posix_path(value: str) -> bool:
    return bool(value) and PurePosixPath(value).is_absolute()


def _coerce_timeout(value: Any) -> int:
    try:
        timeout = int(value) if value is not None else DEFAULT_TIMEOUT_SECONDS
    except (TypeError, ValueError):
        timeout = DEFAULT_TIMEOUT_SECONDS
    return max(1, min(timeout, MAX_TIMEOUT_SECONDS))


def _tail(text: str, limit: int = MAX_TAIL_CHARS) -> str:
    return text[-limit:] if len(text) > limit else text


def _session_value(name: str) -> str:
    try:
        from gateway.session_context import get_session_env

        return (get_session_env(name, "") or "").strip()
    except Exception:
        return (os.getenv(name, "") or "").strip()


def _current_session_user_name() -> str:
    return _session_value("HERMES_SESSION_USER_NAME")


def _current_session_user_id() -> str:
    return _session_value("HERMES_SESSION_USER_ID")

def _resolve_user_name_from_id(user_id: str | None) -> str:
    normalized_user_id = str(user_id or "").strip()
    if not normalized_user_id:
        return ""
    config_path = os.path.expanduser(USER_ROLES_CONFIG)
    try:
        with open(config_path, encoding="utf-8") as fh:
            data = json.load(fh)
    except Exception:
        return ""
    mapping = data.get("user_id_mapping") if isinstance(data, dict) else None
    if not isinstance(mapping, dict):
        return ""
    resolved = mapping.get(normalized_user_id)
    return str(resolved).strip() if resolved else ""


def _resolve_user_from_session(user_id_override: str = "") -> str:
    """Resolve the Feishu sender to the canonical VM user name when possible."""
    user_name = _current_session_user_name()
    if user_name:
        return user_name

    user_id = str(user_id_override or _current_session_user_id()).strip()
    return _resolve_user_name_from_id(user_id)


def _allow_debug_user_override() -> bool:
    return os.getenv("PNC_TOOLS_ALLOW_USER_OVERRIDE", "").strip().lower() in {"1", "true", "yes", "on"}


def _resolve_execution_user(args: dict[str, Any], user_id: str = "") -> str:
    """Resolve canonical worktree user without trusting model-supplied args by default."""
    if _allow_debug_user_override():
        explicit_user = str(args.get("user") or "").strip()
        if explicit_user:
            return explicit_user
    return _resolve_user_from_session(user_id)


def _check_pnc_permission(agent_name: str, user: str, user_id: str = "") -> str | None:
    """Fail closed for shared Feishu users that are not allowed to run PNC VM tools."""
    # These tools run a fixed, domain-specific VM CLI through ssh-mini-agent.
    # They are safer than arbitrary ssh-mini-agent run_bash_json, but still need
    # identity-based isolation and role gating before the subprocess path.
    try:
        from tools.permission_policy import get_user_role, get_user_role_by_id

        role = get_user_role_by_id(user_id) if user_id else get_user_role(user)
    except Exception as exc:
        return f"permission policy unavailable for {agent_name}; refusing VM execution: {exc}"

    if role not in {"owner", "admin", "senior"}:
        return f"permission denied for {agent_name}: role {role!r} is not allowed to run PNC VM tools"
    return None


def _build_remote_script(agent_name: str, args: dict[str, Any], user_id: str = "") -> str:
    user = _resolve_execution_user(args, user_id=user_id)
    if not user:
        raise ValueError("Unable to resolve Feishu user for PNC VM worktree; refusing to use a shared fallback")
    repo = DEFAULT_REPO
    branch = ""
    if _allow_debug_user_override():
        repo = str(args.get("repo") or DEFAULT_REPO).strip() or DEFAULT_REPO
        branch = str(args.get("branch") or "").strip()
    agent_q = shlex.quote(f"./{agent_name}")
    cmd = [agent_q]

    for key in ("project", "platform", "profile", "input", "output", "regression"):
        value = args.get(key)
        if value:
            cmd.extend([f"--{key}", shlex.quote(str(value))])

    lines = ["set -euo pipefail"]

    if user:
        manager_q = shlex.quote(REMOTE_WORKTREE_MANAGER)
        user_q = shlex.quote(user)
        repo_q = shlex.quote(repo)
        ensure_cmd = f"python3 {manager_q} ensure {user_q} {repo_q}"
        if branch:
            ensure_cmd += f" --branch {shlex.quote(branch)}"
        lines.extend(
            [
                f"ENSURE_JSON=$({ensure_cmd})",
                "WORKTREE_PATH=$(python3 -c 'import json,sys; data=json.loads(sys.argv[1]); print(data.get(\"path\", \"\"))' \"$ENSURE_JSON\")",
                "if [[ -z \"$WORKTREE_PATH\" ]]; then",
                "  echo \"failed to resolve worktree: $ENSURE_JSON\" >&2",
                "  exit 2",
                "fi",
                f"AGENT_ROOT=\"$WORKTREE_PATH/{REMOTE_AGENT_SUBDIR}\"",
            ]
        )

    lines.extend(
        [
            "cd \"$AGENT_ROOT\"",
            "if [[ ! -x " + agent_q + " ]]; then",
            f"  echo 'agent executable not found: {agent_name}' >&2",
            "  exit 127",
            "fi",
            " ".join(cmd),
        ]
    )
    return "\n".join(lines)


def _run_remote_agent(agent_name: str, args: dict[str, Any], user_id: str = "") -> str:
    for path_key in ("input", "output", "regression"):
        value = args.get(path_key)
        if value and not _is_absolute_posix_path(str(value)):
            return tool_error(
                f"{path_key} must be an absolute VM path, got: {value}",
                agent=agent_name,
            )

    timeout = _coerce_timeout(args.get("timeout"))
    user = _resolve_execution_user(args, user_id=user_id)
    if not user:
        return tool_error(
            "Unable to resolve Feishu user for PNC VM worktree; refusing to use a shared fallback",
            agent=agent_name,
        )
    permission_error = _check_pnc_permission(agent_name, user, user_id=user_id)
    if permission_error:
        return tool_error(permission_error, agent=agent_name)

    try:
        script = _build_remote_script(agent_name, args, user_id=user_id)
    except ValueError as exc:
        return tool_error(str(exc), agent=agent_name)

    try:
        completed = subprocess.run(
            [LOCAL_WRAPPER, "run_bash_json"],
            input=script,
            text=True,
            capture_output=True,
            timeout=timeout + 5,
            check=False,
        )
    except FileNotFoundError:
        return tool_error(
            f"local wrapper not found: {LOCAL_WRAPPER}",
            agent=agent_name,
        )
    except subprocess.TimeoutExpired as exc:
        return tool_error(
            f"{agent_name} timed out after {timeout} seconds",
            agent=agent_name,
            stdout=_tail(exc.stdout or ""),
            stderr=_tail(exc.stderr or ""),
        )

    if completed.returncode != 0:
        return tool_error(
            f"{agent_name} invocation failed",
            agent=agent_name,
            exit_code=completed.returncode,
            stdout=_tail(completed.stdout),
            stderr=_tail(completed.stderr),
        )

    try:
        payload = json.loads(completed.stdout.strip() or "{}")
    except json.JSONDecodeError:
        return tool_error(
            "ssh-mini-agent returned non-JSON output",
            agent=agent_name,
            stdout=_tail(completed.stdout),
            stderr=_tail(completed.stderr),
        )

    if not isinstance(payload, dict):
        payload = {"result": payload}

    return tool_result({"ok": True, "agent": agent_name, **payload})


def generate_dbc_tool(args: dict[str, Any], user_id: str = "", **_: Any) -> str:
    """Run the generate-dbc CLI agent on the mini VM."""
    return _run_remote_agent("generate-dbc", args or {}, user_id=user_id)


def parse_bus_data_tool(args: dict[str, Any], user_id: str = "", **_: Any) -> str:
    """Run the parse-bus-data CLI agent on the mini VM."""
    return _run_remote_agent("parse-bus-data", args or {}, user_id=user_id)


def check_requirements() -> bool:
    return bool(shutil_which(LOCAL_WRAPPER))


def shutil_which(command: str) -> str | None:
    # Small wrapper to keep tests monkeypatchable without importing shutil at
    # module import sites elsewhere.
    import shutil

    return shutil.which(command)


_COMMON_PROPERTIES = {
    "project": {"type": "string", "description": "Project ID to pass to the agent."},
    "platform": {"type": "string", "description": "Platform ID to pass to the agent."},
    "profile": {"type": "string", "description": "Profile ID to pass to the agent."},
    "input": {"type": "string", "description": "Absolute path on the mini VM to the input file or directory."},
    "output": {"type": "string", "description": "Absolute path on the mini VM to the output directory."},
    "regression": {"type": "string", "description": "Absolute path on the mini VM to the regression directory."},
    "timeout": {"type": "integer", "description": "Maximum runtime in seconds, default 300, capped at 1800."},
}

registry.register(
    name="generate_dbc",
    toolset="pnc_agents",
    schema={
        "name": "generate_dbc",
        "description": (
            "Run the generate-dbc MCU/PNC agent on the mini VM. "
            "Use it to generate DBC-related outputs from an input DBC file. "
            "Paths must be absolute paths on the VM. The gateway sender is "
            "automatically mapped to that user's pnc_specs worktree."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                **_COMMON_PROPERTIES,
                "input": {
                    "type": "string",
                    "description": "Absolute path on the mini VM to the DBC input file.",
                },
            },
        },
    },
    handler=generate_dbc_tool,
    check_fn=check_requirements,
    description="Run the generate-dbc MCU/PNC CLI agent on the mini VM",
    emoji="🚗",
    max_result_size_chars=20000,
)

registry.register(
    name="parse_bus_data",
    toolset="pnc_agents",
    schema={
        "name": "parse_bus_data",
        "description": (
            "Run the parse-bus-data MCU/PNC agent on the mini VM. "
            "Use it to parse bus data from a dataset root or recording directory. "
            "Paths must be absolute paths on the VM. The gateway sender is "
            "automatically mapped to that user's pnc_specs worktree."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                **_COMMON_PROPERTIES,
                "input": {
                    "type": "string",
                    "description": "Absolute path on the mini VM to the dataset root or recording directory.",
                },
            },
        },
    },
    handler=parse_bus_data_tool,
    check_fn=check_requirements,
    description="Run the parse-bus-data MCU/PNC CLI agent on the mini VM",
    emoji="🚌",
    max_result_size_chars=20000,
)
