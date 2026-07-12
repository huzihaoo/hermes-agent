#!/usr/bin/env python3
"""Worktree manager — auto-create, list, gc for multi-user VM repo isolation.

Usage:
    python3 worktree_manager.py ensure <user> <repo> [--branch <branch>]
    python3 worktree_manager.py list [--user <user>]
    python3 worktree_manager.py gc [--older-than 30]
    python3 worktree_manager.py status <user> <repo>

Designed to be called via ssh-mini-agent from the gateway host.
"""

from __future__ import annotations

import datetime
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

CONFIG_PATH = Path.home() / ".hermes" / "config" / "user-roles.json"
USER_OVERRIDES_DIR = Path.home() / ".hermes" / "config" / "user_overrides"
AUDIT_LOG_PATH = Path("/home/mini/worktrees/.audit.log")
MIRROR_BASE = Path(os.getenv("HERMES_GIT_MIRROR_BASE", "/home/mini/.hermes/git-mirrors"))
LOCK_BASE = Path(os.getenv("HERMES_WORKTREE_LOCK_BASE", "/home/mini/worktrees/.locks"))


def _git_noninteractive_env() -> dict[str, str]:
    env = os.environ.copy()
    env.update({
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_ASKPASS": "/bin/false",
    })
    ssh_cmd = env.get("GIT_SSH_COMMAND", "ssh")
    if "IdentityAgent" not in ssh_cmd:
        ssh_cmd = f"{ssh_cmd} -o IdentityAgent=none"
    if "BatchMode" not in ssh_cmd:
        ssh_cmd = f"{ssh_cmd} -o BatchMode=yes"
    env["GIT_SSH_COMMAND"] = ssh_cmd
    return env


def _run_git_noninteractive(args: list[str], *, cwd: str | None = None, timeout: int = 30) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=_git_noninteractive_env(),
    )


def _production_current_branch(source: str) -> str:
    result = _run_git_noninteractive(["-C", source, "symbolic-ref", "--short", "HEAD"], timeout=10)
    if result.returncode == 0 and result.stdout.strip():
        return result.stdout.strip()
    result = _run_git_noninteractive(["-C", source, "rev-parse", "--abbrev-ref", "HEAD"], timeout=10)
    return result.stdout.strip() if result.returncode == 0 and result.stdout.strip() else "HEAD"


def _mirror_repo_path(repo: str) -> Path:
    safe = _safe_repo_key(repo).replace("/", "__")
    return MIRROR_BASE / f"{safe}.git"


def _remote_url(source: str) -> str:
    result = _run_git_noninteractive(["-C", source, "remote", "get-url", "origin"], timeout=10)
    if result.returncode == 0 and result.stdout.strip():
        return result.stdout.strip()
    return source


def _with_flock(lock_path: Path, command: str, *, timeout: int = 120) -> subprocess.CompletedProcess[str]:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    if not (shutil.which("flock") and os.access(shutil.which("flock") or "", os.X_OK)):
        # macOS test hosts may not have Linux flock(1).  Keep production command
        # semantics on Linux, but use a real advisory lock locally so tests still
        # exercise non-mock concurrency paths.
        import fcntl

        with open(lock_path, "a", encoding="utf-8") as lock_fh:
            fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX)
            try:
                return subprocess.run(
                    ["bash", "-lc", command],
                    capture_output=True,
                    text=True,
                    timeout=timeout,
                    env=_git_noninteractive_env(),
                )
            finally:
                fcntl.flock(lock_fh.fileno(), fcntl.LOCK_UN)
    return subprocess.run(
        ["flock", str(lock_path), "bash", "-lc", command],
        capture_output=True,
        text=True,
        timeout=timeout,
        env=_git_noninteractive_env(),
    )


def _ensure_mirror(repo: str, source: str, *, timeout: int = 120) -> tuple[Path | None, dict | None]:
    mirror = _mirror_repo_path(repo)
    lock = LOCK_BASE / "mirrors" / f"{repo.replace('/', '__')}.lock"
    remote = _remote_url(source)
    if mirror.exists():
        return mirror, None
    cmd = (
        f"if [ -d {shlex_quote(str(mirror))} ]; then exit 0; fi; "
        f"mkdir -p {shlex_quote(str(mirror.parent))} && "
        f"git clone --mirror {shlex_quote(remote)} {shlex_quote(str(mirror))}"
    )
    proc = _with_flock(lock, cmd, timeout=timeout)
    if proc.returncode != 0 and not mirror.exists():
        return None, {"error": f"git clone --mirror failed: {(proc.stderr or proc.stdout or '').strip()}"}
    return mirror, None


def shlex_quote(value: str) -> str:
    return "'" + str(value).replace("'", "'\''") + "'"


def _fetch_origin_prune(mirror: Path, repo: str, *, timeout: int = 60) -> dict | None:
    """Refresh the independent mirror.

    This function is intentionally not called from ensure_worktree().  User task
    admission must not perform network fetches against either production
    checkouts or mirrors; a separate owner-approved refresh path/timer owns
    mirror freshness.
    """
    lock = LOCK_BASE / "mirrors" / f"{repo.replace('/', '__')}.lock"
    proc = _with_flock(lock, f"git -C {shlex_quote(str(mirror))} fetch origin --prune", timeout=timeout)
    if proc.returncode != 0:
        return {"error": f"git mirror fetch origin --prune failed: {(proc.stderr or proc.stdout or '').strip()}"}
    return None


def refresh_mirrors(*, create_missing: bool = True, only_repos: list[str] | None = None) -> dict:
    """Refresh all configured repo mirrors at the L1 main-repo source layer.

    This path intentionally uses only git clone --mirror and git fetch
    origin --prune.  It does not recurse into submodules, run pc_init.sh, or
    download CI artifacts; those L2/L3 artifact refresh semantics are separate.
    """
    config = load_config()
    repos = (get_repo_config(config).get("repos", {}) or {})
    if only_repos:
        wanted = {_safe_repo_key(repo) for repo in only_repos}
        repos = {repo: info for repo, info in repos.items() if _safe_repo_key(repo) in wanted}
    summary: dict[str, list[dict]] = {"refreshed": [], "skipped": [], "error": []}
    for repo in sorted(repos):
        try:
            safe_repo = _safe_repo_key(repo)
            source = str(repos[repo].get("source") or "")
            mirror = _mirror_repo_path(safe_repo)
            if not mirror.exists():
                if not create_missing:
                    summary["skipped"].append({"repo": safe_repo, "source": source, "skipped": "mirror_missing"})
                    continue
                mirror, mirror_error = _ensure_mirror(safe_repo, source)
                if mirror_error:
                    summary["error"].append({"repo": safe_repo, "source": source, **mirror_error})
                    continue
            assert mirror is not None
            error = _fetch_origin_prune(mirror, safe_repo)
            if error:
                summary["error"].append({"repo": safe_repo, "source": source, "mirror": str(mirror), **error})
                continue
            summary["refreshed"].append({"repo": safe_repo, "source": source, "mirror": str(mirror)})
        except Exception as exc:
            summary["error"].append({"repo": str(repo), "error": f"{type(exc).__name__}: {exc}"})
    return summary

def log_audit(user: str, repo: str, action: str) -> None:
    """Log worktree operation to audit log."""
    try:
        AUDIT_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(AUDIT_LOG_PATH, "a", encoding="utf-8") as f:
            timestamp = datetime.datetime.now().isoformat()
            f.write(f"{timestamp}|{user}|{repo}|{action}\n")
    except Exception as e:
        # Don't fail the operation if audit logging fails
        print(f"Warning: audit log failed: {e}", file=sys.stderr)


def load_config() -> dict:
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def get_repo_config(config: dict) -> dict:
    return config.get("repo_config", {})


def get_user_name(config: dict, user_id: str) -> str | None:
    return config.get("user_id_mapping", {}).get(user_id)


def get_user_role(config: dict, user_name: str) -> str:
    return config.get("users", {}).get(user_name, config.get("users", {}).get("default", "member"))


def _parse_minimal_yaml_mapping(text: str) -> dict:
    """Parse the tiny YAML subset used by user_overrides without PyYAML.

    Supported subset:
    - indentation-based mappings only
    - scalar string values, optionally single/double quoted
    - blank lines and whole-line/trailing comments

    Unsupported YAML forms are treated as plain scalar text where possible; malformed
    files fall back to repo defaults via load_user_override().
    """
    result: dict = {}
    stack: list[tuple[int, dict]] = [(-1, result)]
    for raw_line in text.splitlines():
        line = raw_line.split("#", 1)[0].rstrip()
        if not line.strip() or ":" not in line:
            continue
        indent = len(line) - len(line.lstrip(" "))
        if len(stack) == 1 and indent != 0:
            raise ValueError("top-level keys in user override must not be indented")
        key, value = line.strip().split(":", 1)
        key = key.strip()
        value = value.strip()
        while stack and indent <= stack[-1][0]:
            stack.pop()
        if not stack:
            raise ValueError("invalid indentation in user override")
        parent = stack[-1][1]
        if not value:
            child: dict = {}
            parent[key] = child
            stack.append((indent, child))
        else:
            if value == "{}":
                parent[key] = {}
                continue
            if (value.startswith('\"') and value.endswith('\"')) or (value.startswith("'") and value.endswith("'")):
                value = value[1:-1]
            parent[key] = value
    return result


def _warn_user_override(message: str) -> None:
    print(f"Warning: user override ignored: {message}", file=sys.stderr)


def load_user_override(user: str) -> dict:
    try:
        safe_user = _safe_component(user, "user")
    except ValueError as exc:
        _warn_user_override(str(exc))
        return {}
    path = USER_OVERRIDES_DIR / f"{safe_user}.yaml"
    if not path.is_file():
        return {}
    try:
        return _parse_minimal_yaml_mapping(path.read_text(encoding="utf-8"))
    except Exception as exc:
        _warn_user_override(f"{path}: {exc}")
        return {}


def get_user_default_branch(config: dict, user: str, repo: str, fallback: str) -> str:
    override = load_user_override(user)
    branches = override.get("default_branches", {}) if isinstance(override, dict) else {}
    if isinstance(branches, dict):
        value = branches.get(repo)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return fallback


def _safe_component(value: str, label: str) -> str:
    value = str(value or "").strip()
    if not value:
        raise ValueError(f"{label} is required")
    if "/" in value or "\\" in value or value in {".", ".."} or "\x00" in value:
        raise ValueError(f"invalid {label}: {value!r}")
    return value


def _safe_repo_key(value: str) -> str:
    value = str(value or "").strip().strip("/")
    if not value:
        raise ValueError("repo is required")
    if "\\" in value or "\x00" in value or "*" in value:
        raise ValueError(f"invalid repo: {value!r}")
    parts = value.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise ValueError(f"invalid repo: {value!r}")
    if not all(re.fullmatch(r"[A-Za-z0-9._-]+", part) for part in parts):
        raise ValueError(f"invalid repo: {value!r}")
    return value


def _safe_branch(value: str) -> str:
    value = str(value or "").strip()
    if not value:
        raise ValueError("branch is required")
    if value.startswith("-") or "\x00" in value or re.search(r"[\s~^:?*\[\\]", value) or ".." in value or "@{" in value:
        raise ValueError(f"invalid branch: {value!r}")
    return value


def _repo_grant_allows(grant: object, action: str) -> bool:
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


def repo_acl_allows(config: dict, user_name: str, repo: str, action: str) -> bool:
    role = get_user_role(config, user_name)
    if role in {"owner", "admin"}:
        return True
    try:
        repo = _safe_repo_key(repo)
    except ValueError:
        return False
    acl = config.get("repo_acl", {}) or {}
    grants = acl.get(user_name)
    if grants is None:
        grants = acl.get("default", {})
    if not isinstance(grants, dict):
        return False
    grant = _lookup_repo_grant(grants, repo)
    return _repo_grant_allows(grant, action)


def worktree_path(rc: dict, repo: str, user: str) -> str:
    base = Path(rc.get("worktree_base", "/home/mini/worktrees"))
    return str(base / _safe_repo_key(repo) / ".runtime" / _safe_component(user, "user"))


def _repo_user_worktree_entries(rc: dict) -> list[tuple[str, str, Path]]:
    """Return configured repo/user worktree leaf directories.

    Repo keys may contain '/' (for example planning_algo/nop/planning), so
    scanning worktree_base as <repo>/<user> is ambiguous. Iterate configured
    repo keys instead and treat only direct children of each repo path as users.
    """
    base = Path(rc.get("worktree_base", "/home/mini/worktrees"))
    repos = rc.get("repos", {}) or {}
    entries: list[tuple[str, str, Path]] = []
    for repo in sorted(repos):
        try:
            repo_path = base / _safe_repo_key(repo)
        except ValueError:
            continue
        if not repo_path.exists():
            continue
        scan_root = repo_path / ".runtime" if (repo_path / ".runtime").is_dir() else repo_path
        for user_dir in sorted(scan_root.iterdir()):
            if not user_dir.is_dir():
                continue
            try:
                user_name = _safe_component(user_dir.name, "user")
            except ValueError:
                continue
            entries.append((repo, user_name, user_dir))
    return entries


def ensure_worktree(user: str, repo: str, branch: str | None = None) -> dict:
    """Ensure a per-user runtime worktree exists for user+repo. Create if missing.

    The production checkout is read-only and is used only to detect the current
    production branch. Fetch/pin/worktree source is the independent mirror.

    Returns: {"path": str, "branch": str, "created": bool}
    """
    config = load_config()
    rc = get_repo_config(config)
    repos = rc.get("repos", {})

    try:
        repo = _safe_repo_key(repo)
        user = _safe_component(user, "user")
    except ValueError as exc:
        return {"error": str(exc)}

    if repo not in repos:
        return {"error": f"Unknown repo: {repo}. Known: {list(repos.keys())}"}

    repo_info = repos[repo]
    source = repo_info["source"]
    production_branch = _production_current_branch(source)
    try:
        target_branch = _safe_branch(production_branch)
    except ValueError as exc:
        return {"error": str(exc)}

    # All user tasks, including owner-triggered tasks, use isolated runtime worktrees.
    # The production checkout is a read-only branch probe only.
    user_role = get_user_role(config, user)
    if user_role not in {"owner", "admin", "senior"}:
        return {"error": f"repo access denied for {user}: role {user_role!r} has no VM repo read permission"}
    if not repo_acl_allows(config, user, repo, "read"):
        return {"error": f"repo access denied for {user}: missing read ACL for {repo}"}

    # Use an isolated execution worktree sourced from a mirror, never the production checkout.
    wt_path = str(Path(rc.get("worktree_base", "/home/mini/worktrees")) / _safe_repo_key(repo) / ".runtime" / _safe_component(user, "user"))
    mirror, mirror_error = _ensure_mirror(repo, source)
    if mirror_error:
        return mirror_error
    assert mirror is not None

    # Check if worktree already exists
    if os.path.isdir(wt_path):
        return {"path": wt_path, "branch": _get_current_branch(wt_path), "production_branch": production_branch, "mirror": str(mirror), "created": False}

    # Create worktree
    os.makedirs(os.path.dirname(wt_path), exist_ok=True)

    user_lock = LOCK_BASE / "users" / f"{repo.replace('/', '__')}__{user}.lock"
    user_lock.parent.mkdir(parents=True, exist_ok=True)
    worktree_lock = LOCK_BASE / "mirrors" / f"{repo.replace('/', '__')}.lock"
    lock_prefix = f"flock {shlex_quote(str(user_lock))} " if (shutil.which("flock") and os.access(shutil.which("flock") or "", os.X_OK)) else ""
    # Create a detached worktree at the mirror's production branch ref so many
    # users can run concurrently without fighting over a shared checked-out
    # branch name.  Any fallback checkout happens only inside wt_path.
    result = _with_flock(worktree_lock, f"{lock_prefix}git -C {shlex_quote(str(mirror))} worktree add --detach {shlex_quote(wt_path)} {shlex_quote(target_branch)}", timeout=60)
    if result.returncode != 0:
        result = _with_flock(worktree_lock, f"{lock_prefix}git -C {shlex_quote(str(mirror))} worktree add --detach {shlex_quote(wt_path)} {shlex_quote('refs/heads/' + target_branch)}", timeout=60)
        if result.returncode != 0:
            return {"error": f"Failed to create worktree: {result.stderr.strip()}"}

    actual_branch = _get_current_branch(wt_path)

    # Log audit
    log_audit(user, repo, f"auto-create worktree: {wt_path} @ {actual_branch}")

    return {"path": wt_path, "branch": actual_branch, "production_branch": production_branch, "mirror": str(mirror), "created": True}


def list_worktrees(user: str | None = None) -> list[dict]:
    """List all worktrees, optionally filtered by user."""
    config = load_config()
    rc = get_repo_config(config)
    base = Path(rc.get("worktree_base", "/home/mini/worktrees"))
    results = []

    if not base.exists():
        return results

    for repo_name, user_name, user_dir in _repo_user_worktree_entries(rc):
        if user and user_name != user:
            continue
        branch = _get_current_branch(str(user_dir))
        results.append({
            "repo": repo_name,
            "user": user_name,
            "path": str(user_dir),
            "branch": branch,
        })
    return results


def worktree_status(user: str, repo: str) -> dict:
    """Get status of a user's worktree."""
    config = load_config()
    rc = get_repo_config(config)
    wt = worktree_path(rc, repo, user)

    if not os.path.isdir(wt):
        return {"exists": False, "path": wt}

    branch = _get_current_branch(wt)

    # Check for uncommitted changes
    result = subprocess.run(
        ["git", "-C", wt, "status", "--porcelain"],
        capture_output=True, text=True,
    )
    dirty = bool(result.stdout.strip())

    return {
        "exists": True,
        "path": wt,
        "branch": branch,
        "dirty": dirty,
        "uncommitted_files": len(result.stdout.strip().splitlines()) if dirty else 0,
    }


def gc_worktrees(older_than_days: int = 30) -> list[dict]:
    """List stale worktrees (not accessed in N days). Does NOT delete — returns candidates."""
    import time
    config = load_config()
    rc = get_repo_config(config)
    base = Path(rc.get("worktree_base", "/home/mini/worktrees"))
    cutoff = time.time() - (older_than_days * 86400)
    stale = []

    if not base.exists():
        return stale

    for repo_name, user_name, user_dir in _repo_user_worktree_entries(rc):
        # Use mtime of .git file as proxy for last access
        git_file = user_dir / ".git"
        if git_file.exists():
            mtime = git_file.stat().st_mtime
            if mtime < cutoff:
                stale.append({
                    "repo": repo_name,
                    "user": user_name,
                    "path": str(user_dir),
                    "last_access": time.strftime("%Y-%m-%d", time.localtime(mtime)),
                })
    return stale


def _get_current_branch(wt_path: str) -> str:
    result = subprocess.run(
        ["git", "-C", wt_path, "rev-parse", "--abbrev-ref", "HEAD"],
        capture_output=True, text=True,
    )
    return result.stdout.strip() if result.returncode == 0 else "unknown"


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "ensure":
        if len(sys.argv) < 4:
            print("Usage: ensure <user> <repo> [--branch <branch>]")
            sys.exit(1)
        user, repo = sys.argv[2], sys.argv[3]
        branch = None
        if "--branch" in sys.argv:
            idx = sys.argv.index("--branch")
            branch = sys.argv[idx + 1] if idx + 1 < len(sys.argv) else None
        print(json.dumps(ensure_worktree(user, repo, branch), ensure_ascii=False))

    elif cmd == "list":
        user = None
        if "--user" in sys.argv:
            idx = sys.argv.index("--user")
            user = sys.argv[idx + 1] if idx + 1 < len(sys.argv) else None
        print(json.dumps(list_worktrees(user), ensure_ascii=False, indent=2))

    elif cmd == "gc":
        days = 30
        if "--older-than" in sys.argv:
            idx = sys.argv.index("--older-than")
            days = int(sys.argv[idx + 1]) if idx + 1 < len(sys.argv) else 30
        stale = gc_worktrees(days)
        if stale:
            print(json.dumps(stale, ensure_ascii=False, indent=2))
        else:
            print("No stale worktrees found.")

    elif cmd == "refresh-mirrors":
        create_missing = "--no-create" not in sys.argv
        only_repos: list[str] = []
        args = sys.argv[2:]
        for idx, arg in enumerate(args):
            if arg == "--repo" and idx + 1 < len(args):
                only_repos.append(args[idx + 1])
            elif arg.startswith("--repo="):
                only_repos.append(arg.split("=", 1)[1])
        print(json.dumps(refresh_mirrors(create_missing=create_missing, only_repos=only_repos or None), ensure_ascii=False, indent=2))

    elif cmd == "status":
        if len(sys.argv) < 4:
            print("Usage: status <user> <repo>")
            sys.exit(1)
        print(json.dumps(worktree_status(sys.argv[2], sys.argv[3]), ensure_ascii=False, indent=2))

    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)


if __name__ == "__main__":
    main()
