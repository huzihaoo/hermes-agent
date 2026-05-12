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

from tools import vm_task_tool
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
DEFAULT_PNC_PROJECT = "D2L3"
DEFAULT_PNC_PLATFORM = "mcu"
DEFAULT_PNC_PROFILE = "default"
D1Q9_CONTROL_UDP_PARSE_DEFAULTS = {
    "project": "D1Q9",
    "platform": "mcu",
    "profile": "control-udp-bin-to-asc",
}
OPEN_FOXGLOVE_DEFAULTS = {
    "platform": "soc",
    "profile": "one-click-convert",
}
OPEN_FOXGLOVE_PROJECT_ALIASES = {
    "d4q": "d4q",
    "d2l3": "d2l3",
    "g1q3": "g1q3",
}
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
    """Resolve the sender to a canonical VM user name, preferring stable user_id."""
    user_id = str(user_id_override or _current_session_user_id()).strip()
    mapped_user = _resolve_user_name_from_id(user_id)
    if mapped_user:
        return mapped_user

    return _current_session_user_name()


def _allow_debug_user_override() -> bool:
    return os.getenv("PNC_TOOLS_ALLOW_USER_OVERRIDE", "").strip().lower() in {"1", "true", "yes", "on"}


def _resolve_execution_user(args: dict[str, Any], user_id: str = "") -> str:
    """Resolve canonical worktree user without trusting model-supplied args by default."""
    if _allow_debug_user_override():
        explicit_user = str(args.get("user") or "").strip()
        if explicit_user:
            return explicit_user
    return _resolve_user_from_session(user_id)


def _check_pnc_permission(agent_name: str, user: str, user_id: str = "", repo: str = DEFAULT_REPO) -> str | None:
    """Fail closed for shared Feishu users that are not allowed to run PNC VM tools."""
    # These tools run a fixed, domain-specific VM CLI through ssh-mini-agent.
    # They are safer than arbitrary ssh-mini-agent run_bash_json, but still need
    # identity-based isolation and role + repo ACL gating before subprocess or
    # VM task submission.
    try:
        from tools.permission_policy import get_user_role, get_user_role_by_id, repo_acl_allows

        role = get_user_role_by_id(user_id) if user_id else get_user_role(user)
        if role not in {"owner", "admin", "senior"}:
            return f"permission denied for {agent_name}: role {role!r} is not allowed to run PNC VM tools"
        if role not in {"owner", "admin"} and not repo_acl_allows(user, repo, "read"):
            return f"permission denied for {agent_name}: missing repo ACL read grant for {repo}"
    except Exception as exc:
        return f"permission policy unavailable for {agent_name}; refusing VM execution: {exc}"

    return None


def _build_remote_path_checks(args: dict[str, Any]) -> list[str]:
    lines = [
        "WORKTREE_REAL=$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' \"$WORKTREE_PATH\")",
        "case \"$WORKTREE_REAL\" in /home/mini/worktrees/pnc_specs/*) ;; *) echo \"unsafe worktree path for PNC tools: $WORKTREE_REAL\" >&2; exit 3 ;; esac",
    ]
    for key in ("input", "output", "regression"):
        value = args.get(key)
        if not value:
            continue
        value_q = shlex.quote(str(value))
        key_q = shlex.quote(key)
        lines.extend(
            [
                f"RAW_PATH={value_q}",
                "REAL_PATH=$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' \"$RAW_PATH\")",
                "case \"$REAL_PATH\" in \"$WORKTREE_REAL\"|\"$WORKTREE_REAL\"/*) ;; *) echo \"path outside resolved worktree for " + key_q + ": $RAW_PATH -> $REAL_PATH\" >&2; exit 3 ;; esac",
            ]
        )
    return lines


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
        lines.extend(_build_remote_path_checks(args))

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


def _parse_json_payload(text: str) -> dict[str, Any]:
    """Parse ssh-mini-agent JSON output, tolerating transport warnings before it."""
    raw = (text or "").strip()
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        payload = None
        for line in raw.splitlines():
            candidate = line.strip()
            if not candidate.startswith(("{", "[")):
                continue
            try:
                payload = json.loads(candidate)
                break
            except json.JSONDecodeError:
                continue
        if payload is None:
            raise
    if not isinstance(payload, dict):
        return {"result": payload}
    return payload


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
        payload = _parse_json_payload(completed.stdout)
    except json.JSONDecodeError:
        return tool_error(
            "ssh-mini-agent returned non-JSON output",
            agent=agent_name,
            stdout=_tail(completed.stdout),
            stderr=_tail(completed.stderr),
        )

    return tool_result({"ok": True, "agent": agent_name, **payload})


def _looks_like_d1q9_control_udp_input(value: Any) -> bool:
    text = str(value or "").lower()
    return "d1q9" in text and ("control_udp" in text or "control-udp" in text or "control udp" in text or "control" in text)


def _normalise_open_foxglove_project(value: Any) -> str:
    project = str(value or "").strip().lower()
    return OPEN_FOXGLOVE_PROJECT_ALIASES.get(project, project)


def _effective_pnc_args(agent_name: str, args: dict[str, Any]) -> dict[str, Any]:
    effective_args = dict(args)
    if agent_name == "generate-dbc":
        # The VM-side generate-dbc CLI requires these even for smoke/failure-path
        # validation. Keep caller-provided values authoritative, but default to
        # the currently locked PNC skeleton values so generated VM goals are
        # executable instead of failing before input validation.
        effective_args.setdefault("project", DEFAULT_PNC_PROJECT)
        effective_args.setdefault("platform", DEFAULT_PNC_PLATFORM)
        effective_args.setdefault("profile", DEFAULT_PNC_PROFILE)
    elif agent_name == "parse-bus-data" and _looks_like_d1q9_control_udp_input(effective_args.get("input")):
        # D1Q9 Control UDP ASC conversion is a standard 宋伟军-maintained profile.
        # Pin the CLI context before task submission so Feishu requests do not fall
        # back to a generic VM worker or an arbitrary manifest default.
        for key, value in D1Q9_CONTROL_UDP_PARSE_DEFAULTS.items():
            effective_args.setdefault(key, value)
    elif agent_name == "validate-data-validity":
        # Current validate-data-validity implementation exposes the SOC-simple
        # profile in its manifest. Accept caller overrides for future MCU packs,
        # but default to the validated profile so Feishu data checks run today.
        effective_args.setdefault("project", "d4q")
        effective_args.setdefault("platform", "soc")
        effective_args.setdefault("profile", "soc-simple")
    elif agent_name == "open-foxglove":
        # open-foxglove is the pnc_specs Foxglove MCAP conversion entrypoint.
        # It accepts D4Q/D2L3/G1Q3 project packs and always uses AI Native
        # platform=soc with the one-click-convert profile unless explicitly
        # overridden by the caller.
        if effective_args.get("project"):
            effective_args["project"] = _normalise_open_foxglove_project(effective_args.get("project"))
        effective_args.setdefault("platform", OPEN_FOXGLOVE_DEFAULTS["platform"])
        effective_args.setdefault("profile", OPEN_FOXGLOVE_DEFAULTS["profile"])
    return effective_args


def _pnc_command_args(agent_name: str, args: dict[str, Any]) -> list[str]:
    effective_args = _effective_pnc_args(agent_name, args)
    cmd: list[str] = []
    for key in ("project", "platform", "profile", "input", "output", "regression"):
        value = effective_args.get(key)
        if value:
            cmd.extend([f"--{key}", str(value)])
    return cmd


def _pnc_invocation(agent_name: str, args: dict[str, Any]) -> list[str]:
    if agent_name == "validate-data-validity":
        return ["python3", "src/tools/validate-data-validity/cli.py", *_pnc_command_args(agent_name, args)]
    if agent_name == "open-foxglove":
        return ["./open-foxglove", *_pnc_command_args(agent_name, args)]
    return [f"./{agent_name}", *_pnc_command_args(agent_name, args)]


def _build_pnc_task_goal(agent_name: str, args: dict[str, Any], user: str, user_id: str = "") -> str:
    repo = DEFAULT_REPO
    branch = ""
    if _allow_debug_user_override():
        repo = str(args.get("repo") or DEFAULT_REPO).strip() or DEFAULT_REPO
        branch = str(args.get("branch") or "").strip()

    title_slug = agent_name.replace("_", "-")
    work_tmp_dir = f"/mnt/tmp/pnc-{title_slug}"
    download_dir = f"{work_tmp_dir}/downloads"
    user_visible_path = (
        "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving"
        f"/tmp/pnc-{title_slug}/"
    )
    cli_args = " ".join(shlex.quote(part) for part in _pnc_invocation(agent_name, args))
    if agent_name == "validate-data-validity":
        manifest_relpath = "src/tools/validate-data-validity/manifest.yaml"
    elif agent_name == "open-foxglove":
        manifest_relpath = "src/tools/open-foxglove/manifest.yaml"
    else:
        manifest_relpath = f"src/tools/{agent_name}/manifest.yaml"
    branch_suffix = f" --branch {shlex.quote(branch)}" if branch else ""

    return "\n".join(
        [
            f"# PNC VM task: {agent_name}",
            "",
            "Execution contract:",
            "- Host/main is control plane only; VM worker owns execution truth.",
            "- executor: fixed-cli under VM worker",
            "- Do not report completion until canonical result/log/proof is written and imported.",
            "",
            "Requester:",
            f"- requester_user: {user}",
            f"- requester_user_id: {user_id or '(unknown)'}",
            "",
            "Repository/worktree:",
            f"- repo: {repo}",
            f"- ensure_command: python3 {REMOTE_WORKTREE_MANAGER} ensure {shlex.quote(user)} {repo}{branch_suffix}",
            "- runtime_worktree: clean-latest",
            f"- agent_subdir: {REMOTE_AGENT_SUBDIR}",
            "",
            "Repository freshness preflight:",
            "- Before running the domain CLI, execute `git fetch origin --prune` in WORKTREE_PATH.",
            "- Refuse execution unless a resolved_snapshot is captured with: repo, worktree_path, branch, commit, upstream_ref, upstream_commit, dirty=false, behind=false, manifest_path, manifest_sha256.",
            "- Refuse execution if the worktree is detached, dirty, behind upstream, missing the manifest, or missing the requested project/platform/profile in the manifest.",
            "- Write the resolved_snapshot into the canonical result/log so status answers can prove the exact pnc_specs representative used.",
            f"- manifest_relpath: {manifest_relpath}",
            "",
            "VM data landing rules:",
            f"- download_dir={download_dir}",
            f"- work_tmp_dir={work_tmp_dir}",
            f"- user_visible_path={user_visible_path}",
            "- Use /mnt/tmp task directories for intermediates/cache/extracted files.",
            "- When reporting artifacts to users, include the CIFS source path from user_visible_path; do not return only /mnt/tmp/... paths.",
            "- Do not default new task data to ~/Downloads, /tmp, repo source dirs, ~/.cache, or /home/mini/nas/miniPan/tmp/... unless explicitly requested.",
            "",
            "Command to run after resolving WORKTREE_PATH from ensure_command:",
            f"- cd \"$WORKTREE_PATH/{REMOTE_AGENT_SUBDIR}\"",
            f"- {cli_args}",
            "",
            "Safety constraints:",
            "- Validate input/output/regression paths before use.",
            "- For generate-dbc, parse-bus-data, validate-data-validity, and open-foxglove tasks, call the pnc_specs standard CLI and generate fresh outputs; do not satisfy the task by copying/reusing existing artifacts from the input directory unless the requester explicitly asks for reuse.",
            "- Keep user worktree isolation; do not operate in another user's worktree.",
            "- Before any git operation, call /home/mini/worktrees/audit-logger.sh with user, repo, and command summary.",
            "- Never use git push --force unless owner explicitly requested it.",
            "",
            "Required result:",
            "- Write a concise result summary with exit code, stdout/stderr summary, artifacts, verification evidence, and remaining risk.",
            "- Include final artifact paths and any failure diagnostics.",
        ]
    )


def _submit_pnc_task(agent_name: str, args: dict[str, Any], user_id: str = "") -> str:
    for path_key in ("input", "output", "regression"):
        value = args.get(path_key)
        if value and not _is_absolute_posix_path(str(value)):
            return tool_error(
                f"{path_key} must be an absolute VM path, got: {value}",
                agent=agent_name,
            )

    effective_user_id = str(user_id or _current_session_user_id()).strip()
    user = _resolve_execution_user(args, user_id=effective_user_id)
    if not user:
        return tool_error(
            "Unable to resolve Feishu user for PNC VM worktree; refusing to use a shared fallback",
            agent=agent_name,
        )
    permission_error = _check_pnc_permission(agent_name, user, user_id=effective_user_id)
    if permission_error:
        return tool_error(permission_error, agent=agent_name)

    goal = _build_pnc_task_goal(agent_name, args, user=user, user_id=effective_user_id)
    title = f"PNC {agent_name} task for {user}"
    raw = vm_task_tool.vm_task_submit_json(title=title, goal=goal, owner=user, user_id=effective_user_id)
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return tool_error("vm_task_submit returned non-JSON output", agent=agent_name, stdout=_tail(raw))
    if not payload.get("success"):
        return tool_error(
            payload.get("error") or "vm_task_submit failed",
            agent=agent_name,
            vm_task=payload,
        )
    task_payload = payload.get("task") if isinstance(payload.get("task"), dict) else {}
    task_id = task_payload.get("task_id")
    routing = payload.get("routing", {})
    return tool_result(
        {
            "ok": True,
            "mode": "submitted",
            "agent": agent_name,
            "task_id": task_id,
            "task": task_payload,
            "routing": routing,
            "user_message": (
                f"已提交 {agent_name} VM 任务，task_id={task_id or '(unknown)'}。"
                "这只是提交成功，不是完成；请用 vm_task_status 查询 picked-up/terminal result。"
            ),
            "next_action": {
                "tool": "vm_task_status",
                "task_id": task_id,
                "reason": "确认 VM canonical queue / worker pickup / terminal result before reporting completion",
            },
        }
    )


def _build_smoke_script(user: str, repo: str, branch: str = "") -> str:
    manager_q = shlex.quote(REMOTE_WORKTREE_MANAGER)
    user_q = shlex.quote(user)
    repo_q = shlex.quote(repo)
    ensure_cmd = f"python3 {manager_q} ensure {user_q} {repo_q}"
    if branch:
        ensure_cmd += f" --branch {shlex.quote(branch)}"
    return "\n".join(
        [
            "set -euo pipefail",
            f"ENSURE_JSON=$({ensure_cmd})",
            "WORKTREE_PATH=$(python3 -c 'import json,sys; data=json.loads(sys.argv[1]); print(data.get(\"path\", \"\"))' \"$ENSURE_JSON\")",
            "if [[ -z \"$WORKTREE_PATH\" ]]; then",
            "  echo \"failed to resolve worktree: $ENSURE_JSON\" >&2",
            "  exit 2",
            "fi",
            f"AGENT_ROOT=\"$WORKTREE_PATH/{REMOTE_AGENT_SUBDIR}\"",
            "AGENT_ROOT_EXISTS=false",
            "GENERATE_DBC_EXECUTABLE=false",
            "PARSE_BUS_DATA_EXECUTABLE=false",
            "VALIDATE_DATA_VALIDITY_CLI=false",
            "[[ -d \"$AGENT_ROOT\" ]] && AGENT_ROOT_EXISTS=true",
            "[[ -x \"$AGENT_ROOT/generate-dbc\" ]] && GENERATE_DBC_EXECUTABLE=true",
            "[[ -x \"$AGENT_ROOT/parse-bus-data\" ]] && PARSE_BUS_DATA_EXECUTABLE=true",
            "[[ -f \"$AGENT_ROOT/src/tools/validate-data-validity/cli.py\" ]] && VALIDATE_DATA_VALIDITY_CLI=true",
            "OPEN_FOXGLOVE_EXECUTABLE=false",
            "[[ -x \"$AGENT_ROOT/open-foxglove\" ]] && OPEN_FOXGLOVE_EXECUTABLE=true",
            "python3 - <<'PY' \"$ENSURE_JSON\" \"$WORKTREE_PATH\" \"$AGENT_ROOT\" \"$AGENT_ROOT_EXISTS\" \"$GENERATE_DBC_EXECUTABLE\" \"$PARSE_BUS_DATA_EXECUTABLE\" \"$VALIDATE_DATA_VALIDITY_CLI\" \"$OPEN_FOXGLOVE_EXECUTABLE\"",
            "import json, sys",
            "ensure_json = json.loads(sys.argv[1])",
            "payload = {",
            "    'ok': True,",
            "    'ensure_json': ensure_json,",
            "    'worktree_path': sys.argv[2],",
            "    'agent_root': sys.argv[3],",
            "    'agent_root_exists': sys.argv[4] == 'true',",
            "    'generate_dbc_executable': sys.argv[5] == 'true',",
            "    'parse_bus_data_executable': sys.argv[6] == 'true',",
            "    'validate_data_validity_cli': sys.argv[7] == 'true',",
            "    'open_foxglove_executable': sys.argv[8] == 'true',",
            "}",
            "print(json.dumps(payload, ensure_ascii=False))",
            "PY",
        ]
    )


def pnc_agents_smoke_tool(args: dict[str, Any], user_id: str = "", **_: Any) -> str:
    """Safely verify PNC user resolution and VM worktree/tool-root availability."""
    args = args or {}
    timeout = _coerce_timeout(args.get("timeout") or 60)
    user = _resolve_execution_user(args, user_id=user_id)
    if not user:
        return tool_error(
            "Unable to resolve Feishu user for PNC VM worktree; refusing to use a shared fallback",
            agent="pnc_agents_smoke",
        )
    permission_error = _check_pnc_permission("pnc_agents_smoke", user, user_id=user_id)
    if permission_error:
        return tool_error(permission_error, agent="pnc_agents_smoke")
    repo = DEFAULT_REPO
    branch = ""
    if _allow_debug_user_override():
        repo = str(args.get("repo") or DEFAULT_REPO).strip() or DEFAULT_REPO
        branch = str(args.get("branch") or "").strip()
    script = _build_smoke_script(user, repo, branch=branch)
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
        return tool_error(f"local wrapper not found: {LOCAL_WRAPPER}", agent="pnc_agents_smoke")
    except subprocess.TimeoutExpired as exc:
        return tool_error(
            f"pnc_agents_smoke timed out after {timeout} seconds",
            agent="pnc_agents_smoke",
            stdout=_tail(exc.stdout or ""),
            stderr=_tail(exc.stderr or ""),
        )
    if completed.returncode != 0:
        return tool_error(
            "pnc_agents_smoke invocation failed",
            agent="pnc_agents_smoke",
            exit_code=completed.returncode,
            stdout=_tail(completed.stdout),
            stderr=_tail(completed.stderr),
        )
    try:
        payload = _parse_json_payload(completed.stdout)
    except json.JSONDecodeError:
        return tool_error(
            "ssh-mini-agent returned non-JSON output",
            agent="pnc_agents_smoke",
            stdout=_tail(completed.stdout),
            stderr=_tail(completed.stderr),
        )
    return tool_result({"ok": True, "agent": "pnc_agents_smoke", "user": user, "repo": repo, **payload})


def generate_dbc_tool(args: dict[str, Any], user_id: str = "", **_: Any) -> str:
    """Submit the generate-dbc CLI agent task for VM worker execution."""
    return _submit_pnc_task("generate-dbc", args or {}, user_id=user_id)


def parse_bus_data_tool(args: dict[str, Any], user_id: str = "", **_: Any) -> str:
    """Submit the parse-bus-data CLI agent task for VM worker execution."""
    return _submit_pnc_task("parse-bus-data", args or {}, user_id=user_id)


def validate_data_validity_tool(args: dict[str, Any], user_id: str = "", **_: Any) -> str:
    """Submit the validate-data-validity CLI agent task for VM worker execution."""
    return _submit_pnc_task("validate-data-validity", args or {}, user_id=user_id)


def open_foxglove_tool(args: dict[str, Any], user_id: str = "", **_: Any) -> str:
    """Submit the open-foxglove MCAP conversion task for VM worker execution."""
    return _submit_pnc_task("open-foxglove", args or {}, user_id=user_id)


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
    name="pnc_agents_smoke",
    toolset="pnc_agents",
    schema={
        "name": "pnc_agents_smoke",
        "description": (
            "Safely verify PNC VM tool routing without running domain agents: "
            "map the gateway sender to the user's pnc_specs worktree, run ensure, "
            "and check the tool root/executables."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "timeout": {"type": "integer", "description": "Maximum runtime in seconds, default 60, capped at 1800."},
            },
        },
    },
    handler=pnc_agents_smoke_tool,
    check_fn=check_requirements,
    description="Smoke-check PNC VM user/worktree/tool-root routing without running domain agents",
    emoji="🧪",
    max_result_size_chars=20000,
)


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

registry.register(
    name="validate_data_validity",
    toolset="pnc_agents",
    schema={
        "name": "validate_data_validity",
        "description": (
            "Run the validate-data-validity MCU/PNC agent on the mini VM. "
            "Use it when a Feishu user provides MCU data or a dataset path and asks whether "
            "the data is valid. Paths must be absolute paths on the VM. Defaults currently "
            "match the validated manifest: project=d4q, platform=soc, profile=soc-simple; "
            "callers may override these when MCU-specific packs are available. The gateway "
            "sender is automatically mapped to that user's pnc_specs worktree."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                **_COMMON_PROPERTIES,
                "input": {
                    "type": "string",
                    "description": "Absolute path on the mini VM to the data file or dataset directory to validate.",
                },
                "output": {
                    "type": "string",
                    "description": "Absolute path on the mini VM to write validation_report.json and summaries.",
                },
            },
            "required": ["input", "output"],
        },
    },
    handler=validate_data_validity_tool,
    check_fn=check_requirements,
    description="Run the validate-data-validity MCU/PNC CLI agent on the mini VM",
    emoji="✅",
    max_result_size_chars=20000,
)

registry.register(
    name="open_foxglove",
    toolset="pnc_agents",
    schema={
        "name": "open_foxglove",
        "description": (
            "Run the open-foxglove PNC agent on the mini VM. Use it when a Feishu user "
            "provides a raw MCAP and asks to convert it into a Foxglove-loadable MCAP. "
            "Supports project=d4q/d2l3/g1q3, defaults to platform=soc and "
            "profile=one-click-convert. Paths must be absolute VM paths. The gateway "
            "sender is automatically mapped to that user's pnc_specs worktree."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                **_COMMON_PROPERTIES,
                "project": {
                    "type": "string",
                    "description": "Project pack ID: d4q, d2l3, or g1q3. Case-insensitive.",
                },
                "input": {
                    "type": "string",
                    "description": "Absolute path on the mini VM to the raw MCAP input file.",
                },
                "output": {
                    "type": "string",
                    "description": "Absolute path on the mini VM to the output directory for <stem>.converted.mcap, summary.json, artifacts.json, and foxglove_open_plan.json.",
                },
            },
            "required": ["project", "input", "output"],
        },
    },
    handler=open_foxglove_tool,
    check_fn=check_requirements,
    description="Convert raw MCAP to Foxglove-loadable MCAP via pnc_specs open-foxglove on the mini VM",
    emoji="🦊",
    max_result_size_chars=20000,
)
