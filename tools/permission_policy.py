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
RepoAction = Literal["read", "write", "push"]
OpType = Literal[
    "read",
    "write",
    "delete_small",
    "dangerous",
    "vm_direct_exec",
    "vm_git_routine",
    "vm_git_push",
    "vm_git_dangerous",
    "vm_repo_unauthorized",
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


def _repo_acl_for_user(cfg: dict, user_name: str) -> dict:
    """Return repo ACL grants for a display name.

    ACL config is intentionally explicit and fail-closed.  VM-local clones are
    execution resources, not authorization.  If a repo grant is absent, non-owner
    users do not get source/git access just because a worktree path exists.
    """
    acl = cfg.get("repo_acl", {}) or {}
    grants = acl.get(user_name)
    if grants is None:
        grants = acl.get("default", {})
    return grants if isinstance(grants, dict) else {}


def _repo_grant_allows(grant: object, action: RepoAction) -> bool:
    if grant == "admin":
        return True
    if isinstance(grant, str):
        if grant == "read":
            return action == "read"
        if grant == "write":
            return action in {"read", "write"}
        if grant == "push":
            return action in {"read", "write", "push"}
        return False
    if isinstance(grant, dict):
        if grant.get("admin") is True:
            return True
        actions = grant.get("actions", [])
        if isinstance(actions, str):
            actions = [actions]
        if action in actions:
            return True
        if action == "read" and any(a in actions for a in ("write", "push")):
            return True
        if action == "write" and "push" in actions:
            return True
    return False


def _validate_repo_acl_grant(grant: str) -> str:
    normalized_grant = str(grant or "").strip().lower()
    if normalized_grant not in {"read", "write", "push", "admin"}:
        raise ValueError(f"invalid repo ACL grant: {grant}")
    return normalized_grant


def _validate_repo_name(repo: str) -> str:
    normalized_repo = str(repo or "").strip().strip("/")
    if not normalized_repo:
        raise ValueError("repo is required")
    if normalized_repo == "*":
        return normalized_repo
    parts = normalized_repo.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise ValueError(f"invalid repo name: {repo}")
    if parts[-1] == "*":
        # Group wildcard grants are allowed for GitLab project groups, but only
        # as a terminal segment such as planning_algo/*, never as a global '*'
        # approval-card shortcut or as a mid-path broadening pattern.
        if len(parts) < 2:
            raise ValueError(f"invalid repo name: {repo}")
        parts_to_validate = parts[:-1]
    elif any(part == "*" for part in parts):
        raise ValueError(f"invalid repo name: {repo}")
    else:
        parts_to_validate = parts
    if not all(re.fullmatch(r"[A-Za-z0-9._-]+", part) for part in parts_to_validate):
        raise ValueError(f"invalid repo name: {repo}")
    return normalized_repo


def _validate_repo_lookup_name(repo: str) -> str:
    normalized_repo = _validate_repo_name(repo)
    if "*" in normalized_repo:
        raise ValueError(f"invalid repo name: {repo}")
    return normalized_repo


def grant_repo_acl(display_name: str, repo: str, grant: str) -> str:
    cfg = _load_config()
    normalized_name = _normalize_user_name(display_name)
    if not normalized_name:
        raise ValueError("display_name is required")
    normalized_repo = _validate_repo_name(repo)
    normalized_grant = _validate_repo_acl_grant(grant)
    repo_acl = cfg.setdefault("repo_acl", {})
    user_acl = repo_acl.setdefault(normalized_name, {})
    if not isinstance(user_acl, dict):
        user_acl = {}
        repo_acl[normalized_name] = user_acl
    user_acl[normalized_repo] = normalized_grant
    _save_config(cfg)
    return normalized_grant


def revoke_repo_acl(display_name: str, repo: str) -> bool:
    cfg = _load_config()
    normalized_name = _normalize_user_name(display_name)
    if not normalized_name:
        raise ValueError("display_name is required")
    normalized_repo = _validate_repo_name(repo)
    repo_acl = cfg.setdefault("repo_acl", {})
    user_acl = repo_acl.get(normalized_name)
    if not isinstance(user_acl, dict) or normalized_repo not in user_acl:
        return False
    del user_acl[normalized_repo]
    if not user_acl:
        repo_acl.pop(normalized_name, None)
    _save_config(cfg)
    return True


def list_repo_acl(display_name: str | None = None) -> dict:
    cfg = _load_config()
    repo_acl = cfg.get("repo_acl", {}) or {}
    if display_name is None:
        return repo_acl if isinstance(repo_acl, dict) else {}
    normalized_name = _normalize_user_name(display_name)
    if not normalized_name:
        raise ValueError("display_name is required")
    grants = repo_acl.get(normalized_name, {}) if isinstance(repo_acl, dict) else {}
    return grants if isinstance(grants, dict) else {}


def _lookup_repo_grant(grants: dict, repo: str) -> object:
    grant = grants.get(repo)
    if grant is not None:
        return grant
    best_prefix_len = -1
    best_grant = None
    for scope, scoped_grant in grants.items():
        if not isinstance(scope, str) or not scope.endswith("/*"):
            continue
        prefix = scope[:-2]
        if repo.startswith(prefix + "/"):
            if len(prefix) > best_prefix_len:
                best_prefix_len = len(prefix)
                best_grant = scoped_grant
    if best_grant is not None:
        return best_grant
    return grants.get("*")


def repo_acl_allows(user_name: str, repo: str, action: RepoAction) -> bool:
    cfg = _load_config()
    role = get_user_role(user_name)
    if role in {"owner", "admin"}:
        return True
    try:
        normalized_repo = _validate_repo_lookup_name(repo)
    except ValueError:
        return False
    grants = _repo_acl_for_user(cfg, user_name)
    grant = _lookup_repo_grant(grants, normalized_repo)
    return _repo_grant_allows(grant, action)


def repo_acl_allows_by_id(user_id: str, repo: str, action: RepoAction) -> bool:
    cfg = _load_config()
    display_name = cfg.get("user_id_mapping", {}).get(user_id, "")
    if not display_name:
        return False
    return repo_acl_allows(display_name, repo, action)



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
    try:
        from gateway.session_context import get_session_env

        session_user = (get_session_env("HERMES_SESSION_USER_NAME") or "").strip()
        if session_user:
            return session_user
    except Exception:
        pass
    return (os.getenv("HERMES_SESSION_USER_NAME") or os.getenv("HERMES_USER_NAME") or "").strip()


def _known_repo_keys() -> list[str]:
    cfg = _load_config()
    candidates: set[str] = set()
    repos = ((cfg.get("repo_config") or {}).get("repos") or {})
    if isinstance(repos, dict):
        candidates.update(str(repo).strip().strip("/") for repo in repos)
    repo_acl = cfg.get("repo_acl", {}) or {}
    if isinstance(repo_acl, dict):
        for grants in repo_acl.values():
            if not isinstance(grants, dict):
                continue
            for scope in grants:
                scope = str(scope or "").strip().strip("/")
                if scope and "*" not in scope:
                    candidates.add(scope)
    valid = []
    for repo in candidates:
        try:
            valid.append(_validate_repo_name(repo))
        except ValueError:
            continue
    return sorted(set(valid), key=lambda repo: len(repo.split("/")), reverse=True)


def _vm_path_has_dot_segments(path: str) -> bool:
    return any(part in {".", ".."} for part in str(path or "").split("/"))


def _parse_worktree_repo_user(rest: str) -> tuple[str, str] | None:
    parts = rest.strip("/").split("/") if rest.strip("/") else []
    if len(parts) < 2 or any(part in {"", ".", ".."} for part in parts):
        return None
    if any(part in {".", ".."} for part in rest.split("/")):
        return None
    for repo in _known_repo_keys():
        repo_parts = repo.split("/")
        if parts[:len(repo_parts)] != repo_parts or len(parts) <= len(repo_parts):
            continue
        user = parts[len(repo_parts)]
        if re.fullmatch(r"[A-Za-z0-9._\-\u4e00-\u9fff]+", user or ""):
            return repo, user
    repo, user = parts[0], parts[1]
    try:
        _validate_repo_name(repo)
    except ValueError:
        return None
    if not re.fullmatch(r"[A-Za-z0-9._\-\u4e00-\u9fff]+", user or ""):
        return None
    return repo, user


def _repo_name_from_vm_path(path: str) -> tuple[str, str | None, bool] | None:
    """Return (repo, worktree_user, is_main_repo_path) for known VM repo paths."""
    worktree_prefix = "/home/mini/worktrees/"
    if path == "/home/mini/worktrees" or path.startswith(worktree_prefix):
        parsed = _parse_worktree_repo_user(path[len(worktree_prefix):])
        if not parsed:
            return None
        repo, user = parsed
        return repo, user, False
    main = re.match(r"^/home/mini/([A-Za-z0-9._-]+)(?:/.*)?$", path)
    if main:
        repo = main.group(1)
        if repo in (".", "..", "worktrees"):
            return None
        return repo, None, True
    return None


def _classify_ssh_mini_agent_repo_read(cmd: str) -> OpType | None:
    try:
        argv = shlex.split(cmd)
    except ValueError:
        return None
    if not argv:
        return None
    agent_names = {"ssh-mini-agent", "~/.local/bin/ssh-mini-agent", "/Users/songying/.local/bin/ssh-mini-agent"}
    if argv[0] not in agent_names or len(argv) < 2:
        return None
    if argv[1] not in {"list_files", "read_file", "grep", "head", "tail"}:
        return None
    vm_paths = [token for token in argv[2:] if token.startswith("/home/mini/")]
    if not vm_paths:
        return None
    expected_user = _expected_session_user_name()
    for path in vm_paths:
        if _vm_path_has_dot_segments(path):
            return "vm_repo_unauthorized"
        parsed = _repo_name_from_vm_path(path)
        if not parsed:
            continue
        repo, path_user, is_main_repo_path = parsed
        if is_main_repo_path:
            # Main repo source is owner/admin only.  Advanced users should go via
            # their authorized worktree so repo ACL and user isolation both apply.
            if not expected_user or get_user_role(expected_user) not in {"owner", "admin"}:
                return "vm_repo_unauthorized"
            continue
        if not expected_user or path_user != expected_user:
            return "vm_repo_unauthorized"
        if not repo_acl_allows(path_user, repo, "read"):
            return "vm_repo_unauthorized"
    return "read"


def _worktree_repo_user_from_cd(segment: str) -> tuple[str, str] | None:
    match = re.match(r"^cd\s+/home/mini/worktrees/([^\r\n;|`$<>]+)/?$", segment.strip())
    if not match:
        return None
    return _parse_worktree_repo_user(match.group(1))


def _worktree_user_from_cd(segment: str) -> str | None:
    repo_user = _worktree_repo_user_from_cd(segment)
    return repo_user[1] if repo_user else None


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
    cd_repo: str | None = None
    for segment in segments:
        if _is_audit_logger(segment):
            continue
        segment_repo_user = _worktree_repo_user_from_cd(segment)
        if segment_repo_user is not None:
            segment_repo, segment_cd_user = segment_repo_user
            if saw_git:
                return None
            if cd_user is not None and segment_cd_user != cd_user:
                return None
            if cd_repo is not None and segment_repo != cd_repo:
                return None
            if expected_user and segment_cd_user != expected_user:
                return None
            cd_user = segment_cd_user
            cd_repo = segment_repo
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
            if not cd_repo or not cd_user or not repo_acl_allows(cd_user, cd_repo, "push"):
                return "vm_repo_unauthorized"
            saw_push = True
            continue
        if op == "rebase" and re.search(r"\s(--exec|-x\S*)\b", segment):
            return None
        if op == "submodule" and re.search(r"\sforeach\b", segment):
            return None
        if op in {"add", "commit", "restore", "merge", "rebase", "checkout", "switch"}:
            if not cd_repo or not cd_user or not repo_acl_allows(cd_user, cd_repo, "write"):
                return "vm_repo_unauthorized"
        if op not in _ROUTINE_GIT_OPS:
            return None

    if not saw_git or not saw_worktree_cd:
        return None
    if not cd_repo or not cd_user or not repo_acl_allows(cd_user, cd_repo, "read"):
        return "vm_repo_unauthorized"
    return "vm_git_push" if saw_push else "vm_git_routine"


def _remote_cd_path_from_ssh_mini_run(cmd: str) -> str | None:
    remote = _extract_ssh_mini_run_remote(cmd)
    if remote is None:
        return None
    first_segment = remote.split("&&", 1)[0].strip()
    match = re.match(r"^cd\s+([^\r\n;|`$<>]+)$", first_segment)
    return match.group(1).rstrip("/") if match else None


def _contains_source_read_intent(remote: str) -> bool:
    """Detect raw repo-rooted VM commands that are likely to disclose source.

    Repo ACL should gate source/code reads, while authorized worktree users who
    try arbitrary execution should still hit the normal vm_direct_exec denial
    path rather than being converted into a repo ACL request.
    """
    return bool(re.search(
        r"\b(?:cat|less|more|head|tail|sed|awk|grep|rg|find|ls|tree|python(?:3(?:\.\d+)?)?)\b",
        remote,
    ) and not re.search(r"\bpython(?:3(?:\.\d+)?)?\s+[^&;|]*\.(?:py|sh)\b", remote))


def _classify_ssh_mini_run_source_scope(cmd: str) -> OpType | None:
    """Return source-scope denial only for raw VM commands rooted in repos.

    repo_acl is a source-leakage guard, not a general execution gate.  A raw
    ssh-mini-run outside known repo roots should continue through ordinary
    dangerous-command approval instead of being denied as a missing repo grant.
    """
    cd_path = _remote_cd_path_from_ssh_mini_run(cmd)
    if not cd_path:
        return None
    if _vm_path_has_dot_segments(cd_path):
        return "vm_repo_unauthorized"
    parsed = _repo_name_from_vm_path(cd_path)
    if not parsed:
        return None
    repo, path_user, is_main_repo_path = parsed
    expected_user = _expected_session_user_name()
    if is_main_repo_path:
        if not expected_user or get_user_role(expected_user) not in {"owner", "admin"}:
            return "vm_repo_unauthorized"
        return None
    if not expected_user or path_user != expected_user:
        return "vm_repo_unauthorized"
    if not repo_acl_allows(path_user or "", repo, "read"):
        return "vm_repo_unauthorized"
    return None


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

    # Source-bearing raw VM commands rooted inside repo/worktree paths require
    # repo_acl.  Non-source ssh-mini-run payloads outside repo roots remain
    # ordinary writes so repo_acl does not become a general execution blocker.
    source_scope_classification = _classify_ssh_mini_run_source_scope(cmd)
    if source_scope_classification:
        return source_scope_classification

    # VM direct execution helpers that can edit files or run arbitrary snippets
    # are still approval-worthy, but only repo-rooted payloads above are denied
    # as source ACL violations.
    if re.search(r"\bssh-mini-agent\s+(run_bash_json|run_py_json|edit_file)\b", cmd):
        return "write"
    if re.search(r"\bssh-mini-run\b", cmd):
        remote = _extract_ssh_mini_run_remote(cmd)
        if remote is None:
            return "vm_direct_exec"
        if "/home/mini/" in remote:
            return "vm_direct_exec"
        return "write"
    if re.search(r"\bssh\b[^\n;]*\bmini@", cmd):
        return "write"

    # VM repo reads through ssh-mini-agent still need repository ACL.  A read
    # helper is not safe just because it is non-mutating.
    repo_read_classification = _classify_ssh_mini_agent_repo_read(cmd)
    if repo_read_classification:
        return repo_read_classification

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


def _integration_tools_session_vm_permission_open() -> bool:
    """Business policy: integration_tools intake group defaults VM tool ops open."""
    try:
        from gateway.session_context import get_session_env
        from hermes_cli.config import cfg_get, load_config

        chat_id = (get_session_env("HERMES_SESSION_CHAT_ID") or "").strip()
        if not chat_id:
            return False
        cfg_all = load_config() or {}
        block = cfg_get(cfg_all, "business_lines", "integration_tools", default={}) or {}
        if not isinstance(block, dict) or not bool(block.get("enabled", False)):
            return False
        vm_policy = block.get("vm_tool_business_permission") or {}
        if isinstance(vm_policy, dict) and vm_policy.get("group_default_allowed") is False:
            return False
        raw_ids: list[object] = []
        for key in ("intake_chat_ids", "intake_chat_id", "intake_group_ids", "intake_group_id"):
            value = block.get(key)
            if isinstance(value, (list, tuple, set)):
                raw_ids.extend(value)
            elif value:
                raw_ids.append(value)
        return chat_id in {str(v).strip() for v in raw_ids if str(v or "").strip()}
    except Exception:
        return False


def _decision_for(role: str, op_type: str, cfg: dict) -> Decision:
    if op_type == "vm_git_dangerous":
        return "DENY"
    if op_type == "vm_repo_unauthorized":
        return "DENY"
    if _integration_tools_session_vm_permission_open() and op_type in {"read", "write", "vm_git_routine", "vm_direct_exec"}:
        return "ALLOW"
    if op_type == "vm_git_push":
        # Senior/admin/owner users with explicit repo push ACL should not be
        # blocked by an extra approval round for normal collaborative pushes.
        # Destructive git operations (force-push/reset/clean/etc.) are already
        # classified as vm_git_dangerous above and remain denied.
        return "DENY" if role == "member" else "ALLOW"
    if op_type == "vm_direct_exec" and role not in {"owner", "admin"}:
        return "DENY"
    if role == "member" and op_type in {"read", "vm_git_routine"}:
        return "DENY"
    role_matrix = cfg.get("permission_matrix", {}).get(role) or cfg.get("permission_matrix", {}).get("member", {})
    decision = role_matrix.get(op_type)
    if decision:
        return decision  # type: ignore[return-value]
    if op_type == "vm_direct_exec":
        # Unknown or smuggled direct VM execution still fails closed. Well-formed
        # non-source ssh-mini-run payloads are classified as ordinary write.
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
        "effective_policy": "platform_role_intersect_repo_acl_workspace_task_policy",
    }
    try:
        with log_file.open("a", encoding="utf-8") as f:
            f.write(_json.dumps(event, ensure_ascii=False) + "\n")
    except Exception:
        pass  # Don't let audit logging break the permission check
