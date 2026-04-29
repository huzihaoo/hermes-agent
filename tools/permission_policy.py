"""Permission policy — identity-based command classification and decision."""

from __future__ import annotations

import json
import os
import re
import shlex
from pathlib import Path
from typing import Literal, Optional

Decision = Literal["ALLOW", "CONFIRM", "APPROVE", "DENY"]
Role = Literal["owner", "admin", "senior", "member"]
OpType = Literal[
    "read",
    "write",
    "delete_small",
    "dangerous",
    "vm_direct_exec",
    "vm_git_routine",
    "vm_git_push",
    "vm_git_dangerous",
]

_CONFIG_PATH = Path.home() / ".hermes" / "config" / "user-roles.json"
_config: dict | None = None


def _normalize_user_name(display_name: str) -> str:
    return str(display_name or "").strip()


def _save_config(cfg: dict) -> None:
    global _config
    _CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    _CONFIG_PATH.write_text(json.dumps(cfg, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    _config = cfg


def _load_config() -> dict:
    global _config
    if _config is None:
        _config = json.loads(_CONFIG_PATH.read_text())
    return _config


def get_user_role(display_name: str) -> Role:
    cfg = _load_config()
    normalized_name = _normalize_user_name(display_name)
    return cfg["users"].get(normalized_name, cfg["users"].get("default", "member"))


def set_user_role(display_name: str, role: Role) -> Role:
    cfg = _load_config()
    normalized_name = _normalize_user_name(display_name)
    if not normalized_name:
        raise ValueError("display_name is required")
    if role not in cfg.get("permission_matrix", {}):
        raise ValueError(f"unknown role: {role}")
    users = cfg.setdefault("users", {})
    users[normalized_name] = role
    _save_config(cfg)
    return role


def map_user_id(display_name: str, user_id: str) -> None:
    cfg = _load_config()
    normalized_name = _normalize_user_name(display_name)
    normalized_user_id = str(user_id or "").strip()
    if not normalized_name:
        raise ValueError("display_name is required")
    if not normalized_user_id:
        raise ValueError("user_id is required")
    mapping = cfg.setdefault("user_id_mapping", {})
    mapping[normalized_user_id] = normalized_name
    _save_config(cfg)


def find_user_id_by_name(display_name: str) -> Optional[str]:
    cfg = _load_config()
    normalized_name = _normalize_user_name(display_name)
    if not normalized_name:
        return None
    for mapped_user_id, mapped_name in cfg.get("user_id_mapping", {}).items():
        if _normalize_user_name(mapped_name) == normalized_name:
            return str(mapped_user_id).strip() or None
    return None


def get_user_role_by_id(user_id: str) -> Role:
    """Get user role by user_id (e.g., feishu open_id)."""
    cfg = _load_config()
    mapping = cfg.get("user_id_mapping", {})
    display_name = mapping.get(user_id)
    if display_name:
        return get_user_role(display_name)
    return cfg["users"].get("default", "member")


_ROUTINE_GIT_OPS = (
    "fetch",
    "pull",
    "checkout",
    "switch",
    "status",
    "diff",
    "log",
    "branch",
    "commit",
    "add",
    "restore",
    "merge",
    "rebase",
    "submodule",
)


def _extract_ssh_mini_run_remote(cmd: str) -> str | None:
    if re.search(r"[\r\n]", cmd):
        return None
    try:
        argv = shlex.split(cmd)
    except ValueError:
        return None
    if len(argv) != 2:
        return None
    if argv[0] not in {"ssh-mini-run", "~/.local/bin/ssh-mini-run", "/Users/songying/.local/bin/ssh-mini-run"}:
        return None
    return argv[1].strip()


def _has_shell_control_chars(segment: str) -> bool:
    # Keep the routine-git bypass narrow: no command chaining beyond explicit
    # && segments, no pipes, redirects, command substitution, backgrounding, or
    # slash is only allowed for the exact audit-logger path validated below.
    return bool(re.search(r"[;|`$<>\r\n#'\"\\(){}*?\[\]]|(?<!&)&(?!&)", segment))


def _expected_session_user_name() -> str:
    return (os.getenv("HERMES_SESSION_USER_NAME") or os.getenv("HERMES_USER_NAME") or "").strip()


def _worktree_user_from_cd(segment: str) -> str | None:
    match = re.match(r"^cd\s+/home/mini/worktrees/([A-Za-z0-9._-]+)/([A-Za-z0-9._\-\u4e00-\u9fff]+)/?$", segment.strip())
    if not match:
        return None
    repo, user = match.groups()
    if repo in (".", "..") or user in (".", ".."):
        return None
    return user


def _is_cd_to_user_worktree(segment: str) -> bool:
    return _worktree_user_from_cd(segment) is not None


def _is_audit_logger(segment: str) -> bool:
    return bool(re.match(r"^/home/mini/worktrees/audit-logger\.sh\s+[A-Za-z0-9._\-\u4e00-\u9fff]+\s+[A-Za-z0-9._-]+\s+[A-Za-z0-9._-]+$", segment.strip()))


def _git_op(segment: str) -> str | None:
    match = re.fullmatch(r"git\s+([a-zA-Z0-9_-]+)(?:\s+[^\r\n;|`$<>]*)?", segment.strip())
    return match.group(1) if match else None


def _is_git_force_push(segment: str) -> bool:
    stripped = segment.strip()
    return bool(
        re.match(r"^git\s+push\b", stripped)
        and (
            re.search(r"\s(--force(?:-with-lease)?\b|-f\b)", stripped)
            or re.search(r"\s\+[^\s]+", stripped)
        )
    )


def _is_git_clean(segment: str) -> bool:
    return bool(re.match(r"^git\s+clean\b", segment.strip()))


def _is_git_dangerous(segment: str) -> bool:
    stripped = segment.strip()
    if re.match(r"^git\s+reset\b", stripped) and re.search(r"\s--hard\b", stripped):
        return True
    if _is_git_clean(stripped):
        return True
    if _is_git_force_push(stripped):
        return True
    if re.match(r"^git\s+branch\b", stripped):
        tokens = stripped.split()[2:]
        short_flags = "".join(token[1:] for token in tokens if token.startswith("-") and not token.startswith("--"))
        long_flags = {token for token in tokens if token.startswith("--")}
        has_delete = "D" in short_flags or "d" in short_flags or "--delete" in long_flags
        has_force = "D" in short_flags or "f" in short_flags or "--force" in long_flags
        if has_delete and has_force:
            return True
    return False


def _dangerous_git_branch_pattern() -> str:
    return r"\bgit\s+branch\b[^\r\n;|`$<>]*(\s-D\S*|\s-[A-Za-z]*D[A-Za-z]*\b|(?=[^\r\n;|`$<>]*\s-[A-Za-z]*d[A-Za-z]*\b)(?=[^\r\n;|`$<>]*\s-[A-Za-z]*f[A-Za-z]*\b)|(?=[^\r\n;|`$<>]*\s--delete\b)(?=[^\r\n;|`$<>]*\s--force\b))"


def _contains_dangerous_git_text(cmd: str) -> bool:
    """Detect destructive git forms anywhere in a shell-ish command string."""
    normalized_cmd = re.sub(r"[\\'\"]", "", cmd)
    checks = (
        r"\bgit\s+reset\b[^\r\n;|`$<>]*\s--hard\b",
        r"\bgit\s+clean\b",
        r"\bgit\s+push\b[^\r\n;|`$<>]*(\s--force(?:-with-lease)?\b|\s-[A-Za-z]*f[A-Za-z]*\b|\s\+[^\s]+)",
        _dangerous_git_branch_pattern(),
    )
    return any(re.search(pattern, normalized_cmd) for pattern in checks)


def _git_segment_has_unsafe_path_args(segment: str) -> bool:
    if re.search(r"\s--no-index\b", segment):
        return True
    for token in segment.split()[2:]:
        if token.startswith(("/", "~")) or "../" in token or token == "..":
            return True
    return False


def _classify_safe_git_sequence(cmd: str) -> OpType | None:
    """Classify a narrowly-validated git-only command sequence.

    This intentionally does not allow arbitrary ssh-mini-run payloads that merely
    contain `git status` somewhere.  The whole command must be a simple direct
    git command or a quoted ssh-mini-run payload made only of `cd <user
    worktree>`, optional audit logger calls, and git operations joined by &&.
    """
    remote = _extract_ssh_mini_run_remote(cmd)
    if remote is None:
        return None
    candidate = remote
    segments = [part.strip() for part in candidate.split("&&")]
    if not segments or any(not part for part in segments):
        return None

    expected_user = _expected_session_user_name()
    if not expected_user:
        return None
    saw_git = False
    saw_push = False
    saw_worktree_cd = False
    cd_user: str | None = None
    for segment in segments:
        if _is_audit_logger(segment):
            continue
        segment_cd_user = _worktree_user_from_cd(segment)
        if segment_cd_user is not None:
            if saw_git:
                return None
            if cd_user is not None and segment_cd_user != cd_user:
                return None
            if expected_user and segment_cd_user != expected_user:
                return None
            cd_user = segment_cd_user
            saw_worktree_cd = True
            continue
        if _has_shell_control_chars(segment):
            return None
        op = _git_op(segment)
        if not op:
            return None
        if _git_segment_has_unsafe_path_args(segment):
            return None
        if remote is not None and not saw_worktree_cd:
            return None
        saw_git = True
        if _is_git_dangerous(segment):
            return "vm_git_dangerous"
        if op == "push":
            saw_push = True
            continue
        if op == "submodule" and re.search(r"\sforeach\b", segment):
            return None
        if op == "rebase" and re.search(r"\s(--exec|-x\S*)\b", segment):
            return None
        if op not in _ROUTINE_GIT_OPS:
            return None

    if not saw_git or not saw_worktree_cd:
        return None
    return "vm_git_push" if saw_push else "vm_git_routine"


def classify_command(command: str) -> OpType:
    if _contains_dangerous_git_text(command):
        return "vm_git_dangerous"
    if re.search(r"[\r\n]", command):
        return "vm_direct_exec" if "ssh-mini-run" in command else "write"
    cmd = command.strip()

    # Git operations inside a user's VM worktree are normal collaboration, not
    # generic VM direct execution.  Keep this bypass deliberately narrow: the
    # whole command must validate as a git-only sequence, otherwise ssh-mini-run
    # remains vm_direct_exec and member users cannot smuggle arbitrary commands
    # next to a harmless `git status`.
    git_classification = _classify_safe_git_sequence(cmd)
    if git_classification:
        return git_classification

    # VM direct execution: this is distinct from ordinary writes.  In shared
    # Feishu usage it bypasses the shared-state v2 -> VM worker execution plane,
    # so non-owner users should not be able to normalize it as a routine write.
    # Keep this config-independent so gateway approval/yolo bypass checks cannot
    # fail open if ~/.hermes/config/user-roles.json is missing or malformed.
    if re.search(r"\bssh-mini-agent\s+(run_bash_json|run_py_json|edit_file)\b", cmd):
        return "vm_direct_exec"
    if re.search(r"\bssh-mini-run\b", cmd):
        return "vm_direct_exec"
    if re.search(r"\bssh\b[^\n;]*\bmini@", cmd):
        return "vm_direct_exec"

    cfg = _load_config()

    # Check critical paths first — always dangerous
    for pattern in cfg.get("critical_paths", []):
        expanded = str(Path(pattern.replace("~", str(Path.home()))).parent)
        if expanded in cmd:
            return "dangerous"

    # Match against pattern lists
    for op_type in ("dangerous", "vm_direct_exec", "delete_small", "write", "read"):
        for pattern in cfg["command_patterns"].get(op_type, []):
            if pattern in cmd:
                return op_type  # type: ignore[return-value]

    # Large rm -rf heuristic
    if re.search(r"rm\s+-rf?\s+", cmd):
        if re.search(r"[\*\?]", cmd) or len(cmd.split()[-1]) < 3:
            return "dangerous"
        return "delete_small"

    return "write"  # default unknown to write (safer than read)


def _decision_for(role: str, op_type: str, cfg: dict) -> Decision:
    if op_type == "vm_git_dangerous":
        return "DENY"
    if op_type == "vm_git_push":
        return "APPROVE"
    role_matrix = cfg.get("permission_matrix", {}).get(role) or cfg.get("permission_matrix", {}).get("member", {})
    decision = role_matrix.get(op_type)
    if decision:
        return decision  # type: ignore[return-value]
    if op_type == "vm_direct_exec":
        # Direct VM execution bypasses shared-state v2 -> VM worker.  Fail
        # closed by default for Feishu/shared usage; owner can only bypass with
        # an explicit emergency marker handled in the approval layer.
        return "DENY"
    if op_type == "vm_git_routine":
        return "ALLOW"
    return "DENY"


def get_decision(display_name: str, command: str) -> Decision:
    role = get_user_role(display_name)
    op_type = classify_command(command)
    cfg = _load_config()
    return _decision_for(role, op_type, cfg)


def get_decision_by_id(user_id: str, command: str) -> Decision:
    """Get permission decision by user_id."""
    role = get_user_role_by_id(user_id)
    op_type = classify_command(command)
    cfg = _load_config()
    decision = _decision_for(role, op_type, cfg)
    _log_decision(user_id, role, command, op_type, decision)
    return decision


def _log_decision(user_id: str, role: str, command: str, op_type: str, decision: str) -> None:
    """Append approval decision to audit log."""
    import json as _json
    from datetime import datetime as _dt
    from pathlib import Path as _Path

    audit_dir = _Path.home() / ".hermes" / "audit" / "approvals"
    audit_dir.mkdir(parents=True, exist_ok=True)
    log_file = audit_dir / f"{_dt.now().strftime('%Y-%m-%d')}.jsonl"

    # Resolve display name
    cfg = _load_config()
    display_name = cfg.get("user_id_mapping", {}).get(user_id, user_id)

    event = {
        "timestamp": _dt.now().isoformat(),
        "user_id": user_id,
        "user_name": display_name,
        "role": role,
        "command": command[:200],
        "op_type": op_type,
        "decision": decision,
    }
    try:
        with log_file.open("a", encoding="utf-8") as f:
            f.write(_json.dumps(event, ensure_ascii=False) + "\n")
    except Exception:
        pass  # Don't let audit logging break the permission check
