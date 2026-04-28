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
import subprocess
import sys
from pathlib import Path

CONFIG_PATH = Path.home() / ".hermes" / "config" / "user-roles.json"
AUDIT_LOG_PATH = Path("/home/mini/worktrees/.audit.log")


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


def _safe_component(value: str, label: str) -> str:
    value = str(value or "").strip()
    if not value:
        raise ValueError(f"{label} is required")
    if "/" in value or "\\" in value or value in {".", ".."} or "\x00" in value:
        raise ValueError(f"invalid {label}: {value!r}")
    return value


def _safe_branch(value: str) -> str:
    value = str(value or "").strip()
    if not value:
        raise ValueError("branch is required")
    if value.startswith("-") or "\x00" in value or re.search(r"[\s~^:?*\[\\]", value) or ".." in value or "@{" in value:
        raise ValueError(f"invalid branch: {value!r}")
    return value


def worktree_path(rc: dict, repo: str, user: str) -> str:
    base = Path(rc.get("worktree_base", "/home/mini/worktrees"))
    return str(base / _safe_component(repo, "repo") / _safe_component(user, "user"))


def ensure_worktree(user: str, repo: str, branch: str | None = None) -> dict:
    """Ensure a worktree exists for user+repo. Create if missing.
    
    Owner users get the source repo directly.
    Non-owner users get isolated worktrees.
    
    Returns: {"path": str, "branch": str, "created": bool}
    """
    config = load_config()
    rc = get_repo_config(config)
    repos = rc.get("repos", {})

    try:
        repo = _safe_component(repo, "repo")
        user = _safe_component(user, "user")
    except ValueError as exc:
        return {"error": str(exc)}

    if repo not in repos:
        return {"error": f"Unknown repo: {repo}. Known: {list(repos.keys())}"}

    repo_info = repos[repo]
    source = repo_info["source"]
    default_branch = repo_info.get("default_branch", "main")
    try:
        target_branch = _safe_branch(branch or default_branch)
    except ValueError as exc:
        return {"error": str(exc)}
    
    # Check if user is owner
    user_role = get_user_role(config, user)
    if user_role == "owner":
        # Owner uses source repo directly
        return {"path": source, "branch": _get_current_branch(source), "created": False}
    
    # Non-owner: use worktree
    wt_path = worktree_path(rc, repo, user)

    # Check if worktree already exists
    if os.path.isdir(wt_path):
        return {"path": wt_path, "branch": _get_current_branch(wt_path), "created": False}

    # Create worktree
    os.makedirs(os.path.dirname(wt_path), exist_ok=True)
    
    # First try to checkout the target branch
    result = subprocess.run(
        ["git", "-C", source, "worktree", "add", wt_path, target_branch],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        # Branch might not exist locally, try detached HEAD then checkout
        result = subprocess.run(
            ["git", "-C", source, "worktree", "add", "--detach", wt_path],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            return {"error": f"Failed to create worktree: {result.stderr.strip()}"}
        
        # Try to checkout the branch in the new worktree
        subprocess.run(
            ["git", "-C", wt_path, "checkout", target_branch],
            capture_output=True, text=True,
        )

    actual_branch = _get_current_branch(wt_path)
    
    # Log audit
    log_audit(user, repo, f"auto-create worktree: {wt_path} @ {actual_branch}")
    
    return {"path": wt_path, "branch": actual_branch, "created": True}


def list_worktrees(user: str | None = None) -> list[dict]:
    """List all worktrees, optionally filtered by user."""
    config = load_config()
    rc = get_repo_config(config)
    base = Path(rc.get("worktree_base", "/home/mini/worktrees"))
    results = []

    if not base.exists():
        return results

    for repo_dir in sorted(base.iterdir()):
        if not repo_dir.is_dir():
            continue
        for user_dir in sorted(repo_dir.iterdir()):
            if not user_dir.is_dir():
                continue
            if user and user_dir.name != user:
                continue
            branch = _get_current_branch(str(user_dir))
            results.append({
                "repo": repo_dir.name,
                "user": user_dir.name,
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

    for repo_dir in base.iterdir():
        if not repo_dir.is_dir():
            continue
        for user_dir in repo_dir.iterdir():
            if not user_dir.is_dir():
                continue
            # Use mtime of .git file as proxy for last access
            git_file = user_dir / ".git"
            if git_file.exists():
                mtime = git_file.stat().st_mtime
                if mtime < cutoff:
                    stale.append({
                        "repo": repo_dir.name,
                        "user": user_dir.name,
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
