"""PNC/MCU domain agent wrappers.

These tools expose the validated CLI agents from pnc_specs to Hermes/Feishu
users. The actual agents run on the mini VM through ssh-mini-agent so gateway
sessions can invoke them without needing direct VM shell access.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import subprocess
from pathlib import Path, PurePosixPath
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
    "d2j": "d2j",
    "g3y": "g3y",
}
LOCAL_WRAPPER = os.getenv("SSH_MINI_AGENT_BIN", "ssh-mini-agent")
DEFAULT_TIMEOUT_SECONDS = 300
MAX_TIMEOUT_SECONDS = 1800
MAX_TAIL_CHARS = 12000
PNC_EFFECTIVE_BRANCH = os.getenv("PNC_TOOLS_EFFECTIVE_BRANCH", "g1q3-rca")
INTEGRATION_TOOLS_EFFECTIVE_BRANCH = os.getenv("PNC_INTEGRATION_TOOLS_EFFECTIVE_BRANCH", "main")
PATH_CANDIDATE_MOUNTS = ("/mnt/ad-data", "/mnt/minieye/mdrive4", "/mnt/evaluation_data")

# P2-A bootstrap registry.  H3 live VM check (2026-06-19) confirmed
# src/tools/*/manifest.yaml exists, including src/tools/mdrive4-cli/manifest.yaml.
# Keep this host-side registry minimal and deterministic for dispatch safety; a
# later VM manifest sync can generate the same shape automatically.
PNC_TOOL_REGISTRY: dict[str, dict[str, Any]] = {
    "generate-dbc": {
        "canonical": "generate-dbc",
        "aliases": ("generate_dbc",),
        "repo_relpath": "",
        "entrypoint": ("./generate-dbc",),
        "manifest": "src/tools/generate-dbc/manifest.yaml",
    },
    "parse-bus-data": {
        "canonical": "parse-bus-data",
        "aliases": ("parse_bus_data",),
        "repo_relpath": "",
        "entrypoint": ("./parse-bus-data",),
        "manifest": "src/tools/parse-bus-data/manifest.yaml",
    },
    "validate-data-validity": {
        "canonical": "validate-data-validity",
        "aliases": ("validate_data_validity", "validate-validity", "validate_validity"),
        "repo_relpath": "",
        "entrypoint": ("python3", "src/tools/validate-data-validity/cli.py"),
        "manifest": "src/tools/validate-data-validity/manifest.yaml",
    },
    "open-foxglove": {
        "canonical": "open-foxglove",
        "aliases": ("open_foxglove", "foxglove"),
        "repo_relpath": "",
        "entrypoint": ("./open-foxglove",),
        "manifest": "src/tools/open-foxglove/manifest.yaml",
    },
    "mdrive4-cli": {
        "canonical": "mdrive4-cli",
        "aliases": ("mdrive_cli", "mdrive4_cli", "mdrive-cli", "mdrive4cli"),
        "repo_relpath": "",
        "entrypoint": ("python3", "src/tools/mdrive4-cli/cli.py"),
        "manifest": "src/tools/mdrive4-cli/manifest.yaml",
    },
}


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


def _json_dumps_compact(value: dict[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _progress_bridge_line(task_id_expr: str, phase: str, message: str, state: str = "running") -> str:
    progress = _json_dumps_compact({"phase": phase, "message": message})
    return (
        "python3 /home/mini/.hermes/hermes-agent/scripts/vm_task_state_bridge.py "
        f"--task-id {task_id_expr} --phase {shlex.quote(phase)} --event {shlex.quote(message)} "
        f"--vm-state {shlex.quote(state)} --progress-json {shlex.quote(progress)} >/dev/null 2>&1 || true"
    )


def _path_validation_function_shell() -> str:
    mounts = " ".join(shlex.quote(item) for item in PATH_CANDIDATE_MOUNTS)
    return f"""validate_vm_path() {{
  local key=\"$1\" raw=\"$2\" mode=\"${{3:-input}}\"
  [[ -z \"$raw\" ]] && return 0
  if [[ -e \"$raw\" ]]; then
    printf '{{\"event\":\"path_validated\",\"key\":\"%s\",\"path\":\"%s\",\"exists\":true}}\\n' \"$key\" \"$raw\" >&2
    return 0
  fi
  if [[ \"$mode\" != \"input\" ]]; then
    printf '{{\"error\":\"path_not_found\",\"key\":\"%s\",\"path\":\"%s\",\"needs_user_confirmation\":false}}\\n' \"$key\" \"$raw\" >&2
    return 20
  fi
  local base; base=$(basename -- \"$raw\")
  local tmp; tmp=$(mktemp)
  for mount in {mounts}; do
    [[ -d \"$mount\" ]] || continue
    timeout 20 find \"$mount\" -xdev -name \"$base\" -printf '%p\\t%s\\t%TY-%Tm-%Td %TH:%TM:%TS\\n' 2>/dev/null | head -n 20 >> \"$tmp\" || true
  done
  local count; count=$(wc -l < \"$tmp\" | tr -d ' ')
  if [[ \"$count\" == \"1\" ]]; then
    local line path size mtime record mount_root
    line=$(cat \"$tmp\")
    path=${{line%%$'\\t'*}}
    size=$(printf '%s' \"$line\" | cut -f2)
    mtime=$(printf '%s' \"$line\" | cut -f3-)
    record=$(printf '%s' \"$path\" | grep -oE 'record\\.[0-9]+\\.[0-9]+|record_[0-9_]+|record[^/]*' | tail -1 || true)
    mount_root=\"\"
    for mount in {mounts}; do case \"$path\" in \"$mount\"/*|\"$mount\") mount_root=\"$mount\"; break;; esac; done
    printf '{{\"error\":\"path_not_found_candidate_requires_confirmation\",\"key\":\"%s\",\"original_path\":\"%s\",\"candidate_path\":\"%s\",\"size_bytes\":%s,\"mtime\":\"%s\",\"mount\":\"%s\",\"record_segment\":\"%s\",\"needs_user_confirmation\":true}}\\n' \"$key\" \"$raw\" \"$path\" \"${{size:-0}}\" \"$mtime\" \"$mount_root\" \"$record\" >&2
    rm -f \"$tmp\"
    return 21
  fi
  if [[ \"$count\" != \"0\" ]]; then
    python3 - <<'PYJSON' \"$key\" \"$raw\" \"$tmp\"
import json, sys
key, raw, tmp = sys.argv[1:4]
items=[]
for line in open(tmp, encoding='utf-8', errors='replace'):
    parts=line.rstrip('\\n').split('\\t')
    if len(parts) >= 3:
        items.append({{'path': parts[0], 'size_bytes': int(parts[1] or 0), 'mtime': parts[2]}})
print(json.dumps({{'error':'path_not_found_multiple_candidates','key':key,'original_path':raw,'candidates':items[:20],'needs_user_confirmation':True}}, ensure_ascii=False), file=sys.stderr)
PYJSON
    rm -f \"$tmp\"
    return 22
  fi
  rm -f \"$tmp\"
  printf '{{\"error\":\"path_not_found\",\"key\":\"%s\",\"path\":\"%s\",\"needs_user_confirmation\":false}}\\n' \"$key\" \"$raw\" >&2
  return 20
}}
"""


def _git_preflight_shell(*, expected_sha: str = "", branch: str = PNC_EFFECTIVE_BRANCH, manifest_relpath: str = "") -> str:
    expected_line = f"EXPECTED_SHA={shlex.quote(expected_sha)}" if expected_sha else "EXPECTED_SHA=\"\""
    manifest_q = shlex.quote(manifest_relpath)
    return "\n".join([
        "export GIT_TERMINAL_PROMPT=0",
        "export GIT_ASKPASS=/bin/false",
        "export GIT_SSH_COMMAND=\"${GIT_SSH_COMMAND:-ssh} -o IdentityAgent=none -o BatchMode=yes\"",
        f"PINNED_BRANCH={shlex.quote(branch)}",
        expected_line,
        "# Production checkout is never fetched/checked out/reset here; worktree_manager created this per-user worktree from mirror pin source.",
        "ACTUAL_SHA=$(git rev-parse HEAD)",
        "if [[ -n \"$EXPECTED_SHA\" && \"$ACTUAL_SHA\" != \"$EXPECTED_SHA\" ]]; then printf '{\"error\":\"pinned_sha_mismatch\",\"expected\":\"%s\",\"actual\":\"%s\"}\n' \"$EXPECTED_SHA\" \"$ACTUAL_SHA\" >&2; exit 35; fi",
        f"MANIFEST_PATH={manifest_q}",
        "MANIFEST_SHA=",
        "if [[ -n \"$MANIFEST_PATH\" && -f \"$MANIFEST_PATH\" ]]; then MANIFEST_SHA=$(sha256sum \"$MANIFEST_PATH\" | awk '{print $1}'); fi",
        "printf '{\"event\":\"repo_snapshot\",\"branch\":\"%s\",\"commit\":\"%s\",\"manifest_sha256\":\"%s\"}\n' \"$PINNED_BRANCH\" \"$ACTUAL_SHA\" \"$MANIFEST_SHA\" >&2",
    ])




def _production_repo(repo: str) -> str:
    if repo == DEFAULT_REPO:
        return "/home/mini/pnc_specs"
    if repo == INTEGRATION_TOOLS_REPO:
        return "/home/mini/minieye_dnp_nop"
    return f"/home/mini/{repo}"


def _mirror_repo(repo: str) -> str:
    return f"/home/mini/.hermes/git-mirrors/{repo.replace('/', '__')}.git"


def _resolve_remote_head_sha(repo: str, branch: str = "", *, timeout: int = 30) -> dict[str, Any]:
    """Resolve the task pin from the mirror at origin/<production-current-branch>.

    Production checkout is read-only and used only to detect its current branch.
    Mirror owns fetch/pin; no task path fetches/checks out the production copy.
    """
    if os.getenv("PNC_TOOLS_ENABLE_HOST_GIT_PROBE", "").strip().lower() not in {"1", "true", "yes", "on"}:
        detected = branch or PNC_EFFECTIVE_BRANCH
        return {"ok": True, "skipped": True, "repo": repo, "branch": detected, "sha": "", "production_repo": _production_repo(repo), "mirror_repo": _mirror_repo(repo)}
    production = _production_repo(repo)
    mirror = _mirror_repo(repo)
    script = "\n".join([
        "set -euo pipefail",
        f"PRODUCTION={shlex.quote(production)}",
        f"MIRROR={shlex.quote(mirror)}",
        "BRANCH=$(git -C \"$PRODUCTION\" symbolic-ref --short HEAD 2>/dev/null || git -C \"$PRODUCTION\" rev-parse --abbrev-ref HEAD)",
        "[[ -n \"$BRANCH\" && \"$BRANCH\" != HEAD ]] || { echo '{\"error\":\"production_branch_unresolved\"}' >&2; exit 41; }",
        "[[ -d \"$MIRROR\" ]] || { echo '{\"error\":\"mirror_missing\"}' >&2; exit 42; }",
        "SHA=$(git -C \"$MIRROR\" rev-parse \"refs/remotes/origin/${BRANCH}\" 2>/dev/null || git -C \"$MIRROR\" rev-parse \"${BRANCH}\")",
        "printf '%s %s\n' \"$SHA\" \"$BRANCH\"",
    ])
    try:
        proc = subprocess.run([LOCAL_WRAPPER, "run_bash_json"], input=script, text=True, capture_output=True, timeout=timeout + 5, check=False)
    except Exception as exc:
        return {"ok": False, "error": f"mirror pin probe failed: {type(exc).__name__}: {exc}", "repo": repo}
    if proc.returncode != 0:
        return {"ok": False, "error": "mirror pin probe failed", "repo": repo, "stdout": _tail(proc.stdout), "stderr": _tail(proc.stderr)}
    text = (proc.stdout or "") + "\n" + (proc.stderr or "")
    m = re.search(r"\b([0-9a-f]{40})\s+([^\s]+)", text)
    if not m:
        return {"ok": False, "error": "mirror pin probe returned no sha", "repo": repo, "stdout": _tail(proc.stdout), "stderr": _tail(proc.stderr)}
    return {"ok": True, "repo": repo, "branch": m.group(2), "sha": m.group(1), "sha7": m.group(1)[:7], "production_repo": production, "mirror_repo": mirror}


def _repo_pin_lines(repo: str, branch: str, pin: dict[str, Any]) -> list[str]:
    sha = str((pin or {}).get("sha") or "")
    sha7 = sha[:7] if sha else "unresolved-host-probe-disabled"
    warning = "" if sha else " ⚠️ 仓库版本可能滞后：host-side mirror pin 未启用/未解析"
    return [
        f"- effective_repo: {repo}",
        f"- effective_branch: {branch}",
        f"- pinned_head_sha: {sha or '(unresolved)'}",
        f"- task_card_repo_version: {repo} @ {branch} @ {sha7}{warning}",
        "- Host dispatch must resolve the pin from /home/mini/.hermes/git-mirrors/<repo>.git after read-only production-branch detection; mirror pin probe failure is fail-closed.",
    ]

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


def _agent_requires_repo_write(agent_name: str) -> bool:
    """Return True only for tools that can write back to repo/remote refs."""
    normalized = str(agent_name or "").strip().lower().replace("_", "-")
    return normalized in {
        "git-push",
        "repo-push",
        "push",
        "writeback",
        "repo-writeback",
        "git-writeback",
        "commit-and-push",
    }


def _session_is_bound_pnc_group() -> bool:
    """Business-bound Feishu groups grant read-only/diagnostic VM tool access."""
    platform = _session_value("HERMES_SESSION_PLATFORM").strip().lower()
    chat_id = _session_value("HERMES_SESSION_CHAT_ID").strip()
    if platform != "feishu" or not chat_id:
        return False
    try:
        from gateway.pnc_group_binding import is_g1q3_rca_bound_chat

        if is_g1q3_rca_bound_chat(chat_id):
            return True
    except Exception:
        pass
    try:
        from tools.permission_policy import _integration_tools_session_vm_permission_open

        if _integration_tools_session_vm_permission_open():
            return True
    except Exception:
        pass
    return False


def _check_pnc_permission(agent_name: str, user: str, user_id: str = "", repo: str = DEFAULT_REPO) -> str | None:
    """Fail closed for writeback, but allow bound-group read-only diagnostics."""
    # Fixed PNC VM tools are split into two permission tiers:
    # - read-only/diagnostic tools: bound Feishu business-group members may run
    #   them without per-repo read ACL, because outputs land under /mnt/tmp and
    #   production repos are isolated by mirror/per-user worktrees.
    # - repo/remote writeback (push/commit+push): requires explicit write/push
    #   ACL and never inherits the read-only group grant.
    try:
        from tools.permission_policy import get_user_role, get_user_role_by_id, repo_acl_allows

        role = get_user_role_by_id(user_id) if user_id else get_user_role(user)
        if role in {"owner", "admin"}:
            return None

        requires_write = _agent_requires_repo_write(agent_name)
        if requires_write:
            if repo_acl_allows(user, repo, "push") or (role in {"senior"} and repo_acl_allows(user, repo, "write")):
                return None
            return (
                f"permission denied for {agent_name}: repo write/push permission required for {repo}; "
                "需 write/push 权限，请找 owner 授权"
            )

        if _session_is_bound_pnc_group():
            if role in {"member", "senior"}:
                return None
            return f"permission denied for {agent_name}: unknown role {role!r} is not allowed in bound PNC group"

        if role not in {"senior"}:
            return f"permission denied for {agent_name}: role {role!r} is not allowed to run PNC VM tools outside a bound PNC group"
        if not repo_acl_allows(user, repo, "read"):
            return f"permission denied for {agent_name}: missing repo ACL read grant for {repo}"
    except Exception as exc:
        return f"permission policy unavailable for {agent_name}; refusing VM execution: {exc}"

    return None


def _build_remote_path_checks(args: dict[str, Any]) -> list[str]:
    lines = [
        "WORKTREE_REAL=$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' \"$WORKTREE_PATH\")",
        "case \"$WORKTREE_REAL\" in /home/mini/worktrees/pnc_specs/.runtime/*) ;; *) echo \"unsafe worktree path for PNC tools: $WORKTREE_REAL\" >&2; exit 3 ;; esac",
        _path_validation_function_shell(),
    ]
    for key in ("input", "output", "regression"):
        value = args.get(key)
        if not value:
            continue
        mode = "input" if key == "input" else "output"
        lines.append(f"validate_vm_path {shlex.quote(key)} {shlex.quote(str(value))} {mode}")
    return lines


def _build_remote_script(agent_name: str, args: dict[str, Any], user_id: str = "") -> str:
    tool = _resolve_pnc_tool(agent_name)
    if not tool.get("ok"):
        raise ValueError(str(tool.get("message") or "PNC tool resolution failed"))
    canonical_agent = str(tool.get("canonical") or agent_name)
    user = _resolve_execution_user(args, user_id=user_id)
    if not user:
        raise ValueError("Unable to resolve Feishu user for PNC VM worktree; refusing to use a shared fallback")
    repo = DEFAULT_REPO
    if _allow_debug_user_override():
        repo = str(args.get("repo") or DEFAULT_REPO).strip() or DEFAULT_REPO
    pin = args.get("_repo_pin") if isinstance(args.get("_repo_pin"), dict) else {}
    effective_branch = str(pin.get("branch") or PNC_EFFECTIVE_BRANCH)
    invocation = _pnc_invocation(canonical_agent, args)
    cmd = [shlex.quote(part) for part in invocation]
    entrypoint = [str(part) for part in (tool.get("entrypoint") or ())]

    lines = ["set -euo pipefail"]

    if user:
        manager_q = shlex.quote(REMOTE_WORKTREE_MANAGER)
        user_q = shlex.quote(user)
        repo_q = shlex.quote(repo)
        ensure_cmd = f"python3 {manager_q} ensure {user_q} {repo_q}"
        # Branch selection is production-derived inside worktree_manager; Feishu args cannot switch production branch.
        lines.extend(
            [
                f"ENSURE_JSON=$({ensure_cmd})",
                "WORKTREE_PATH=$(python3 -c 'import json,sys; data=json.loads(sys.argv[1]); print(data.get(\"path\", \"\"))' \"$ENSURE_JSON\")",
                "if [[ -z \"$WORKTREE_PATH\" ]]; then",
                "  echo \"failed to resolve worktree: $ENSURE_JSON\" >&2",
                "  exit 2",
                "fi",
                f"AGENT_ROOT=\"$WORKTREE_PATH/{REMOTE_AGENT_SUBDIR}\"",
                _progress_bridge_line('"${PNC_TASK_ID:-pnc-direct}"', "accepted", "已接单"),
                _progress_bridge_line('"${PNC_TASK_ID:-pnc-direct}"', "sync_repo", "同步仓库"),
                "cd \"$WORKTREE_PATH\"",
                _git_preflight_shell(expected_sha=str(pin.get("sha") or ""), branch=effective_branch),
                _progress_bridge_line('"${PNC_TASK_ID:-pnc-direct}"', "read_input", "读取输入路径"),
            ]
        )
        lines.extend(_build_remote_path_checks(args))

    lines.extend(
        [
            "cd \"$AGENT_ROOT\"",
            _pnc_entrypoint_check_shell(canonical_agent, entrypoint),
            _progress_bridge_line('"${PNC_TASK_ID:-pnc-direct}"', "run_tool", "运行工具"),
            " ".join(cmd),
            _progress_bridge_line('"${PNC_TASK_ID:-pnc-direct}"', "completed", "完成", state="completed"),
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
        # It accepts D4Q/D2L3/G1Q3/D2J/G3Y project packs and always uses AI Native
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


def _tool_name_key(value: Any) -> str:
    return str(value or "").strip().lower().replace("_", "-")


def _tool_name_compact(value: Any, *, drop_digits: bool = False) -> str:
    text = re.sub(r"[^a-z0-9]+", "", _tool_name_key(value))
    if drop_digits:
        text = re.sub(r"\d+", "", text)
    return text


def _registry_entries() -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for key, item in PNC_TOOL_REGISTRY.items():
        if not isinstance(item, dict):
            continue
        canonical = str(item.get("canonical") or key).strip()
        if not canonical:
            continue
        entries.append({**item, "canonical": canonical})
    return entries


def _pnc_tool_resolution_error(requested: str, matches: list[dict[str, Any]]) -> dict[str, Any]:
    candidates = sorted({str(item.get("canonical") or "").strip() for item in matches if str(item.get("canonical") or "").strip()})
    if candidates:
        return {
            "ok": False,
            "requested": requested,
            "reason": "ambiguous",
            "candidates": candidates,
            "message": f"工具名有歧义：{requested}，请确认使用哪一个：{', '.join(candidates)}",
        }
    available = sorted({str(item.get("canonical") or "").strip() for item in _registry_entries()})
    return {
        "ok": False,
        "requested": requested,
        "reason": "not_found",
        "candidates": available,
        "message": f"未识别工具名：{requested}。请确认工具名；不会静默回退到 ./{requested}。",
    }


def _resolve_pnc_tool(agent_name: str) -> dict[str, Any]:
    requested = str(agent_name or "").strip()
    if not requested:
        return _pnc_tool_resolution_error(requested, [])
    key = _tool_name_key(requested)
    entries = _registry_entries()

    exact = [item for item in entries if _tool_name_key(item.get("canonical")) == key]
    if len(exact) == 1:
        return {**exact[0], "ok": True, "requested": requested, "resolution": "canonical"}
    if len(exact) > 1:
        return _pnc_tool_resolution_error(requested, exact)

    alias_matches = [
        item
        for item in entries
        if key in {_tool_name_key(alias) for alias in (item.get("aliases") or [])}
    ]
    if len(alias_matches) == 1:
        return {**alias_matches[0], "ok": True, "requested": requested, "resolution": "alias"}
    if len(alias_matches) > 1:
        return _pnc_tool_resolution_error(requested, alias_matches)

    compact = _tool_name_compact(requested)
    compact_no_digits = _tool_name_compact(requested, drop_digits=True)
    fuzzy_matches = []
    for item in entries:
        names = [item.get("canonical"), *(item.get("aliases") or [])]
        keys = {_tool_name_compact(name) for name in names}
        digitless = {_tool_name_compact(name, drop_digits=True) for name in names}
        requested_has_digits = bool(re.search(r"\d", requested))
        if compact in keys or (not requested_has_digits and compact_no_digits and compact_no_digits in digitless):
            fuzzy_matches.append(item)
    if len(fuzzy_matches) == 1:
        return {**fuzzy_matches[0], "ok": True, "requested": requested, "resolution": "fuzzy"}
    return _pnc_tool_resolution_error(requested, fuzzy_matches)


def _pnc_invocation(agent_name: str, args: dict[str, Any]) -> list[str]:
    tool = _resolve_pnc_tool(agent_name)
    if not tool.get("ok"):
        raise ValueError(str(tool.get("message") or "PNC tool resolution failed"))
    canonical = str(tool.get("canonical") or agent_name)
    entrypoint = [str(part) for part in (tool.get("entrypoint") or ())]
    if not entrypoint:
        raise ValueError(f"PNC tool {canonical} has no registered entrypoint")
    return [*entrypoint, *_pnc_command_args(canonical, args)]


def _pnc_entrypoint_check_shell(agent_name: str, entrypoint: list[str]) -> str:
    if not entrypoint:
        return f"echo 'agent entrypoint not registered: {agent_name}' >&2; exit 127"
    first = entrypoint[0]
    if first in {"python", "python3"} and len(entrypoint) >= 2:
        relpath = shlex.quote(entrypoint[1])
        return "\n".join([
            f"if [[ ! -f {relpath} ]]; then",
            f"  echo 'agent cli not found: {agent_name} ({entrypoint[1]})' >&2",
            "  exit 127",
            "fi",
        ])
    executable = shlex.quote(first)
    return "\n".join([
        f"if [[ ! -x {executable} ]]; then",
        f"  echo 'agent executable not found: {agent_name} ({first})' >&2",
        "  exit 127",
        "fi",
    ])


def _build_pnc_task_goal(agent_name: str, args: dict[str, Any], user: str, user_id: str = "") -> str:
    tool = _resolve_pnc_tool(agent_name)
    if not tool.get("ok"):
        raise ValueError(str(tool.get("message") or "PNC tool resolution failed"))
    requested_agent = str(tool.get("requested") or agent_name)
    canonical_agent = str(tool.get("canonical") or agent_name)
    repo = DEFAULT_REPO
    if _allow_debug_user_override():
        repo = str(args.get("repo") or DEFAULT_REPO).strip() or DEFAULT_REPO
    pin = args.get("_repo_pin") if isinstance(args.get("_repo_pin"), dict) else {}
    effective_branch = str(pin.get("branch") or PNC_EFFECTIVE_BRANCH)
    pin = args.get("_repo_pin") if isinstance(args.get("_repo_pin"), dict) else {}

    title_slug = canonical_agent.replace("_", "-")
    work_tmp_dir = f"/mnt/tmp/pnc-{title_slug}"
    download_dir = f"{work_tmp_dir}/downloads"
    user_visible_path = (
        "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving"
        f"/tmp/pnc-{title_slug}/"
    )
    cli_args = " ".join(shlex.quote(part) for part in _pnc_invocation(canonical_agent, args))
    manifest_relpath = str(tool.get("manifest") or f"src/tools/{canonical_agent}/manifest.yaml")
    resolution_line = (
        f"- 已解析: {requested_agent} → {canonical_agent}"
        if requested_agent != canonical_agent or str(tool.get("resolution") or "") != "canonical"
        else f"- 已解析: {canonical_agent}"
    )
    branch_suffix = ""
    repo_pin_lines = _repo_pin_lines(repo, effective_branch, pin)

    return "\n".join(
        [
            f"# PNC VM task: {canonical_agent}",
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
            *repo_pin_lines,
            f"- agent_subdir: {REMOTE_AGENT_SUBDIR}",
            resolution_line,
            "",
            "Repository freshness preflight:",
            "- Do not fetch/checkout/reset the production repo. worktree_manager creates this per-user worktree from /home/mini/.hermes/git-mirrors/<repo>.git; only validate HEAD against pinned_head_sha here.",
            "- Worker must compare `git rev-parse HEAD` to pinned_head_sha when pinned_head_sha is resolved; mismatch => stop and report pinned_sha_mismatch.",
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
            "- Validate input/output/regression paths before use: exact `test -e` first; if input is missing, search only /mnt/ad-data, /mnt/minieye/mdrive4, /mnt/evaluation_data by basename and return confirmation metadata (size/mtime/mount/record segment). Never silently substitute a same-name candidate.",
            "- Write VM progress via vm_task_state_bridge at phases: 已接单 -> 同步仓库 -> 读取mcap/输入 -> 运行工具 -> 作图/生成产物 -> 落地输出 -> 完成.",
            "- For generate-dbc, parse-bus-data, validate-data-validity, open-foxglove, and mdrive4-cli tasks, call the pnc_specs standard CLI and generate fresh outputs; do not satisfy the task by copying/reusing existing artifacts from the input directory unless the requester explicitly asks for reuse.",
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
    tool = _resolve_pnc_tool(agent_name)
    if not tool.get("ok"):
        return tool_error(
            str(tool.get("message") or "PNC tool resolution failed"),
            agent=agent_name,
            tool_resolution=tool,
            needs_user_confirmation=True,
        )
    canonical_agent = str(tool.get("canonical") or agent_name)
    for path_key in ("input", "output", "regression"):
        value = args.get(path_key)
        if value and not _is_absolute_posix_path(str(value)):
            return tool_error(
                f"{path_key} must be an absolute VM path, got: {value}",
                agent=canonical_agent,
                requested_agent=agent_name,
            )

    effective_user_id = str(user_id or _current_session_user_id()).strip()
    user = _resolve_execution_user(args, user_id=effective_user_id)
    if not user:
        return tool_error(
            "Unable to resolve Feishu user for PNC VM worktree; refusing to use a shared fallback",
            agent=canonical_agent,
            requested_agent=agent_name,
        )
    permission_error = _check_pnc_permission(canonical_agent, user, user_id=effective_user_id)
    if permission_error:
        return tool_error(permission_error, agent=canonical_agent, requested_agent=agent_name)

    pin = _resolve_remote_head_sha(DEFAULT_REPO, "")
    if not pin.get("ok"):
        return tool_error(pin.get("error") or "mirror pin probe failed", agent=canonical_agent, requested_agent=agent_name, repo_pin=pin)
    goal = _build_pnc_task_goal(agent_name, {**args, "_repo_pin": pin}, user=user, user_id=effective_user_id)
    title = f"PNC {canonical_agent} task for {user}"
    raw = vm_task_tool.vm_task_submit_json(title=title, goal=goal, owner=user, user_id=effective_user_id)
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return tool_error("vm_task_submit returned non-JSON output", agent=canonical_agent, requested_agent=agent_name, stdout=_tail(raw))
    if not payload.get("success"):
        return tool_error(
            payload.get("error") or "vm_task_submit failed",
            agent=canonical_agent,
            requested_agent=agent_name,
            vm_task=payload,
        )
    task_payload = payload.get("task") if isinstance(payload.get("task"), dict) else {}
    task_id = task_payload.get("task_id")
    routing = payload.get("routing", {})
    return tool_result(
        {
            "ok": True,
            "mode": "submitted",
            "agent": canonical_agent,
            "requested_agent": agent_name,
            "tool_resolution": {
                "requested": tool.get("requested"),
                "canonical": canonical_agent,
                "resolution": tool.get("resolution"),
                "manifest": tool.get("manifest"),
            },
            "task_id": task_id,
            "task": task_payload,
            "routing": routing,
            "user_message": (
                f"已提交 {canonical_agent} VM 任务，task_id={task_id or '(unknown)'}。"
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





INTEGRATION_TOOLS_REPO = "minieye_dnp_nop"
INTEGRATION_TOOLS_ALLOWED = {
    "logsim-replay": {
        "resource_class": "vm_heavy",
        "required": ("input",),
        "optional": ("protocol", "mode", "topic_config", "extra_args", "clean_only", "timeout", "memory_mb"),
        "description": "Run governed DNP logsim replay after optional mcap clean.",
    },
    "mcap-clean": {
        "resource_class": "vm_heavy",
        "required": ("input",),
        "optional": ("protocol", "topic_config", "clean_only", "merge", "merge_only", "timeout", "memory_mb"),
        "description": "Run governed MCAP clean into /mnt/tmp/<task_id>/.",
    },
    "mcap-translate": {
        "resource_class": "vm_heavy",
        "required": ("input",),
        "optional": ("format", "topics", "timeout", "memory_mb", "cpus"),
        "description": "Run governed mcap_data_translate into /mnt/tmp/<task_id>/.",
    },
    "build-repro": {
        "resource_class": "vm_build",
        "required": ("ref",),
        "optional": (
            "profile", "system", "subsystem", "project", "buildtype",
            "module", "sim", "nproc", "memory_mb", "timeout"
        ),
        "description": "Reproduce a DNP build from a specific ref in a clean worktree and summarize failures.",
    },
}


def _sanitize_task_slug(value: str) -> str:
    import re
    slug = re.sub(r"[^A-Za-z0-9_.:-]+", "-", value.strip())[:80].strip("-._")
    return slug or "integration-tools"


def _integration_task_dir(args: dict[str, Any], cli_name: str) -> str:
    explicit = str(args.get("task_id") or "").strip()
    suffix = _sanitize_task_slug(explicit or cli_name)
    return f"/mnt/tmp/{suffix}"


def _integration_fixed_cli_args(cli_name: str, args: dict[str, Any]) -> list[str]:
    spec = INTEGRATION_TOOLS_ALLOWED[cli_name]
    for key in spec["required"]:
        if not str(args.get(key) or "").strip():
            raise ValueError(f"{cli_name} requires {key}")
    cmd = ["integration-tools-fixed-cli", cli_name]
    value_keys = (
        "input", "output", "protocol", "mode", "topic_config", "format", "topics",
        "extra_args", "ref", "profile", "system", "subsystem", "project",
        "buildtype", "module", "sim", "nproc", "memory_mb", "cpus", "timeout",
    )
    for key in value_keys:
        value = args.get(key)
        if value not in (None, "", [], {}):
            cmd.extend([f"--{key.replace('_', '-')}", str(value)])
    for flag in ("clean_only", "merge", "merge_only"):
        if bool(args.get(flag)):
            cmd.append(f"--{flag.replace('_', '-')}")
    return cmd


def _build_integration_tools_task_goal(cli_name: str, args: dict[str, Any], user: str, user_id: str = "") -> str:
    if cli_name not in INTEGRATION_TOOLS_ALLOWED:
        raise ValueError(f"unsupported integration_tools fixed-cli: {cli_name}")
    for key in ("input", "output", "topic_config"):
        value = args.get(key)
        if value and not _is_absolute_posix_path(str(value)):
            raise ValueError(f"{key} must be an absolute VM path, got: {value}")
    if cli_name == "build-repro":
        ref = str(args.get("ref") or "").strip()
        if not ref:
            raise ValueError("build-repro requires ref")
        if ref.startswith("-") or ".." in ref or any(ch.isspace() for ch in ref):
            raise ValueError(f"unsafe build-repro ref: {ref}")
    task_dir = _integration_task_dir(args, cli_name)
    output_dir = str(args.get("output") or f"{task_dir}/output")
    effective_args = dict(args)
    effective_args["output"] = output_dir
    cmd = " ".join(shlex.quote(part) for part in _integration_fixed_cli_args(cli_name, effective_args))
    task_id_hint = _sanitize_task_slug(str(args.get("task_id") or f"it-{cli_name}"))
    pin = args.get("_repo_pin") if isinstance(args.get("_repo_pin"), dict) else {}
    repo_pin_lines = _repo_pin_lines(INTEGRATION_TOOLS_REPO, INTEGRATION_TOOLS_EFFECTIVE_BRANCH, pin)
    cifs = f"//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/{Path(task_dir).name}/"
    return "\n".join([
        f"# Integration tools VM task: {cli_name}",
        "",
        "Execution contract:",
        "- Host/main is control plane only; VM worker owns execution truth.",
        "- executor: fixed-cli under VM worker",
        "- integration_tools_fixed_cli: true",
        "- runtime_worktree: clean-latest",
        f"- resource_class: {INTEGRATION_TOOLS_ALLOWED[cli_name]['resource_class']}",
        "- Do not report completion until canonical result/log/proof is written and imported.",
        "",
        "Requester:",
        f"- requester_user: {user}",
        f"- requester_user_id: {user_id or '(unknown)'}",
        "",
        "Repository/worktree:",
        f"- repo: {INTEGRATION_TOOLS_REPO}",
        "- runtime_worktree: clean-latest",
        "- source_repo: /home/mini/minieye_dnp_nop",
        f"- runtime_worktree_path: /home/mini/worktrees/minieye_dnp_nop/.runtime/{task_id_hint}/latest-main",
        *repo_pin_lines,
        "",
        "VM data landing rules:",
        f"- work_tmp_dir={task_dir}",
        f"- output_dir={output_dir}",
        f"- user_visible_path={cifs}",
        "- All generated artifacts/cache/intermediates must stay under work_tmp_dir/output_dir.",
        "- Never write generated task outputs under /home/mini/minieye_dnp_nop or /mnt/minieye/mdrive4.",
        "",
        "Runner governance:",
        "- MCAP/open-foxglove/mcap_data_translate work must use governed limits.",
        "- Worker must not fetch/checkout/reset the production repo; use mirror-derived runtime worktree and validate commit pin before execution; fail closed on pinned_sha_mismatch.",
        "- Worker must execute mcap-clean/mcap-translate/logsim-replay only through concrete M2-1b bounded runners.",
        "- Validate input path with `test -e` first; if missing, search only /mnt/ad-data, /mnt/minieye/mdrive4, /mnt/evaluation_data by basename and return confirmation metadata. Never silently substitute a candidate.",
        "- Write VM progress via vm_task_state_bridge at phases: 已接单 -> 同步仓库 -> 读取mcap -> 运行工具 -> 作图/生成产物 -> 落地输出 -> 完成.",
        "- For wrappers whose upstream scripts write beside input, worker must stage input under /mnt/tmp/<task_id>/input before execution.",
        "- mcap-clean wheel has no --output parameter; runner must validate/copy its input-adjacent cleaned output into output_dir.",
        "- logsim-replay must pass -co only for clean_only and require logs/logsim_log.txt for replay/logsim success.",
        "- build-repro must execute only inside this clean runtime worktree for the requested --ref.",
        "- build-repro must run scripts/cmakebuild/build.sh through a whitelist: profile/system/subsystem/project/buildtype/module/sim/nproc.",
        "- Every long command must have explicit timeout, nproc and memory bounds or fail closed.",
        "",
        "Command to run after clean-latest worktree preflight:",
        f"- cd \"$WORKTREE_PATH\"",
        f"- {cmd}",
        "",
        "Required result:",
        "- Write result.json and concise result.md with exit code, artifacts, stdout/stderr tail, VM path, CIFS path, and remaining risk.",
    ])


def _submit_integration_tools_task(cli_name: str, args: dict[str, Any], user_id: str = "") -> str:
    args = args or {}
    try:
        timeout = _coerce_timeout(args.get("timeout"))
        effective_user_id = str(user_id or _current_session_user_id()).strip()
        user = _resolve_execution_user(args, user_id=effective_user_id)
        if not user:
            return tool_error("Unable to resolve Feishu user; refusing integration_tools VM task", agent=cli_name)
        permission_error = _check_pnc_permission(cli_name, user, user_id=effective_user_id, repo=INTEGRATION_TOOLS_REPO)
        if permission_error:
            return tool_error(permission_error, agent=cli_name)
        pin = _resolve_remote_head_sha(INTEGRATION_TOOLS_REPO, "")
        if not pin.get("ok"):
            return tool_error(pin.get("error") or "mirror pin probe failed", agent=cli_name, repo_pin=pin)
        goal = _build_integration_tools_task_goal(cli_name, {**args, "timeout": timeout, "_repo_pin": pin}, user=user, user_id=effective_user_id)
    except ValueError as exc:
        return tool_error(str(exc), agent=cli_name)
    title = f"Integration tools {cli_name} task for {user}"
    raw = vm_task_tool.vm_task_submit_json(title=title, goal=goal, owner=user, user_id=effective_user_id)
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return tool_error("vm_task_submit returned non-JSON output", agent=cli_name, stdout=_tail(raw))
    if not payload.get("success"):
        return tool_error(payload.get("error") or "vm_task_submit failed", agent=cli_name, vm_task=payload)
    task_payload = payload.get("task") if isinstance(payload.get("task"), dict) else {}
    task_id = task_payload.get("task_id")
    return tool_result({
        "ok": True,
        "mode": "submitted",
        "agent": cli_name,
        "task_id": task_id,
        "task": task_payload,
        "user_message": f"已提交 {cli_name} VM fixed-CLI 任务，task_id={task_id or '(unknown)'}。提交成功不是完成；等待 VM worker terminal receipt。",
        "next_action": {"tool": "vm_task_status", "task_id": task_id},
    })


def logsim_replay_tool(args: dict[str, Any], user_id: str = "", **_: Any) -> str:
    return _submit_integration_tools_task("logsim-replay", args or {}, user_id=user_id)


def mcap_clean_tool(args: dict[str, Any], user_id: str = "", **_: Any) -> str:
    return _submit_integration_tools_task("mcap-clean", args or {}, user_id=user_id)


def mcap_translate_tool(args: dict[str, Any], user_id: str = "", **_: Any) -> str:
    return _submit_integration_tools_task("mcap-translate", args or {}, user_id=user_id)


def build_repro_tool(args: dict[str, Any], user_id: str = "", **_: Any) -> str:
    return _submit_integration_tools_task("build-repro", args or {}, user_id=user_id)



def open_foxglove_tool(args: dict[str, Any], user_id: str = "", **_: Any) -> str:
    """Submit the open-foxglove MCAP conversion task for VM worker execution."""
    return _submit_pnc_task("open-foxglove", args or {}, user_id=user_id)


def mdrive4_cli_tool(args: dict[str, Any], user_id: str = "", **_: Any) -> str:
    """Submit the mdrive4-cli MCAP inspection task for VM worker execution."""
    return _submit_pnc_task("mdrive4-cli", args or {}, user_id=user_id)


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
            "Supports project=d4q/d2l3/g1q3/d2j/g3y, defaults to platform=soc and "
            "profile=one-click-convert. Paths must be absolute VM paths. The gateway "
            "sender is automatically mapped to that user's pnc_specs worktree."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                **_COMMON_PROPERTIES,
                "project": {
                    "type": "string",
                    "description": "Project pack ID: d4q, d2l3, g1q3, d2j, or g3y. Case-insensitive.",
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


registry.register(
    name="mdrive4_cli",
    toolset="pnc_agents",
    schema={
        "name": "mdrive4_cli",
        "description": (
            "Run the pnc_specs mdrive4-cli MCAP inspection agent on the mini VM. "
            "Aliases accepted by the host resolver include mdrive_cli and mdrive4-cli; "
            "the task card/goal echoes the canonical resolution."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                **_COMMON_PROPERTIES,
                "project": {"type": "string", "description": "Project pack ID, defaults to manifest/worker context; mdrive4 is typical."},
                "platform": {"type": "string", "description": "Platform ID, typically soc."},
                "profile": {"type": "string", "description": "Profile ID, typically inspect."},
                "input": {"type": "string", "description": "Absolute VM path to the mdrive4 MCAP file."},
                "output": {"type": "string", "description": "Optional absolute VM output directory."},
            },
            "required": ["input"],
        },
    },
    handler=mdrive4_cli_tool,
    check_fn=check_requirements,
    description="Inspect mdrive4 MCAP topics/fields/camera streams via pnc_specs mdrive4-cli",
    emoji="🧭",
    max_result_size_chars=20000,
)



_INTEGRATION_PROPERTIES = {
    "task_id": {"type": "string", "description": "Optional task id slug; defaults to tool name. Determines /mnt/tmp/<task_id>/ landing."},
    "input": {"type": "string", "description": "Absolute VM path to input mcap/file/directory."},
    "output": {"type": "string", "description": "Optional absolute VM output dir; defaults to /mnt/tmp/<task_id>/output."},
    "protocol": {"type": "string", "description": "Protocol such as d01 or d01p."},
    "topic_config": {"type": "string", "description": "Optional absolute VM path to topic config JSON."},
    "timeout": {"type": "integer", "description": "Maximum runtime seconds; capped at 1800 for submit metadata."},
    "memory_mb": {"type": "integer", "description": "Worker-enforced per-process virtual memory cap in MB."},
    "cpus": {"type": "integer", "description": "Worker-enforced CPU/thread cap where supported."},
}

_BUILD_REPRO_PROPERTIES = {
    "task_id": {"type": "string", "description": "Optional task id slug; defaults to build-repro. Determines /mnt/tmp/<task_id>/ landing."},
    "ref": {"type": "string", "description": "Required git branch/tag/commit to build in the clean minieye_dnp_nop runtime worktree."},
    "output": {"type": "string", "description": "Optional absolute VM output dir; defaults to /mnt/tmp/<task_id>/output."},
    "profile": {"type": "string", "description": "build.sh -profile value; default resolved by worker."},
    "system": {"type": "string", "description": "build.sh system flag name, e.g. adas; default adas."},
    "subsystem": {"type": "string", "description": "build.sh system flag value, e.g. dnp; default dnp."},
    "project": {"type": "string", "description": "build.sh -pj value; default d01p."},
    "buildtype": {"type": "string", "description": "build.sh -buildtype value; default release."},
    "module": {"type": "string", "description": "Optional whitelisted module target."},
    "sim": {"type": "string", "description": "Optional simulation selector such as -sim; must be from worker allowlist."},
    "nproc": {"type": "integer", "description": "Build parallelism cap; worker enforces max."},
    "memory_mb": {"type": "integer", "description": "Per-process virtual memory cap in MB; worker enforces max."},
    "timeout": {"type": "integer", "description": "Maximum runtime seconds; capped at submit metadata and worker runner."},
}

registry.register(
    name="logsim_replay",
    toolset="integration_tools",
    schema={"name": "logsim_replay", "description": "Submit governed logsim replay fixed-CLI task to VM worker. M2-1b runner executes only in clean runtime worktree with /mnt/tmp landing and resource bounds.", "parameters": {"type": "object", "properties": {**_INTEGRATION_PROPERTIES, "mode": {"type": "string"}, "clean_only": {"type": "boolean"}, "extra_args": {"type": "string"}}, "required": ["input"]}},
    handler=logsim_replay_tool,
    check_fn=check_requirements,
    description="Submit governed logsim replay fixed-CLI task",
    emoji="🔁",
    max_result_size_chars=20000,
)

registry.register(
    name="mcap_clean",
    toolset="integration_tools",
    schema={"name": "mcap_clean", "description": "Submit governed mcap-clean fixed-CLI task to VM worker. M2-1b runner installs the worktree wheel into /mnt/tmp venv and validates config.json output.", "parameters": {"type": "object", "properties": {**_INTEGRATION_PROPERTIES, "clean_only": {"type": "boolean"}, "merge": {"type": "boolean"}, "merge_only": {"type": "boolean"}}, "required": ["input"]}},
    handler=mcap_clean_tool,
    check_fn=check_requirements,
    description="Submit governed mcap-clean fixed-CLI task",
    emoji="🧹",
    max_result_size_chars=20000,
)

registry.register(
    name="mcap_translate",
    toolset="integration_tools",
    schema={"name": "mcap_translate", "description": "Submit governed mcap_data_translate fixed-CLI task to VM worker. M2-1b runner calls tools/mcap_data_translate/scripts/one_click_translate.sh with explicit /mnt/tmp output and resource bounds.", "parameters": {"type": "object", "properties": {**_INTEGRATION_PROPERTIES, "format": {"type": "string"}, "topics": {"type": "string"}}, "required": ["input"]}},
    handler=mcap_translate_tool,
    check_fn=check_requirements,
    description="Submit governed mcap_data_translate fixed-CLI task",
    emoji="🔄",
    max_result_size_chars=20000,
)

registry.register(
    name="build_repro",
    toolset="integration_tools",
    schema={"name": "build_repro", "description": "Submit governed build-repro fixed-CLI task to VM worker. Reproduces build.sh from a specific ref in clean minieye_dnp_nop runtime worktree and summarizes failures.", "parameters": {"type": "object", "properties": _BUILD_REPRO_PROPERTIES, "required": ["ref"]}},
    handler=build_repro_tool,
    check_fn=check_requirements,
    description="Submit governed build-repro fixed-CLI task",
    emoji="🏗️",
    max_result_size_chars=20000,
)
