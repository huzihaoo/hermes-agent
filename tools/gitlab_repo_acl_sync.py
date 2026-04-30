"""Read-only GitLab member snapshot builder for Hermes repo ACL planning.

This module deliberately writes candidate/snapshot artifacts only. It does not
mutate ~/.hermes/config/user-roles.json. Live ACL application should happen via
explicit pairing CLI commands or an approved apply path.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Any

import requests


DEFAULT_GITLAB_BASE_URL = "https://git.minieye.tech"
DEFAULT_INVENTORY_PATH = Path.home() / ".hermes" / "workspace-work" / "knowledge" / "outputs" / "ci-config-git-repos-2026-04-30.json"
DEFAULT_OUTPUT_PATH = Path.home() / ".hermes" / "workspace-work" / "knowledge" / "outputs" / "gitlab-repo-acl-snapshot.json"
ACCESS_LEVEL_NAMES = {
    10: "Guest",
    20: "Reporter",
    30: "Developer",
    40: "Maintainer",
    50: "Owner",
}


def normalize_gitlab_project_key(value: str) -> str:
    """Normalize SSH/HTTPS GitLab URLs and project paths to group/project."""
    raw = str(value or "").strip()
    if not raw:
        raise ValueError("project key is required")

    ssh_prefix = "git@git.minieye.tech:"
    if raw.startswith(ssh_prefix):
        raw = raw[len(ssh_prefix):]
    else:
        for prefix in ("https://git.minieye.tech/", "http://git.minieye.tech/"):
            if raw.startswith(prefix):
                raw = raw[len(prefix):]
                break

    if raw.endswith(".git"):
        raw = raw[:-4]
    raw = raw.strip("/")
    if not raw or "/" not in raw or any(part in {"", ".", ".."} for part in raw.split("/")):
        raise ValueError(f"invalid GitLab project key: {value}")
    return raw


def access_level_to_grant(access_level: int | str | None) -> str | None:
    """Map GitLab numeric access level to conservative Hermes repo grant."""
    try:
        level = int(access_level)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if level >= 50:
        return "admin"
    if level >= 40:
        return "push"
    if level >= 30:
        return "write"
    if level >= 20:
        return "read"
    return None


def load_repo_inventory(path: str | Path) -> list[str]:
    """Load repo URLs/project paths from a ci_config extraction artifact."""
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    raw_repos = data.get("repos") or data.get("projects") or []
    if not isinstance(raw_repos, list):
        raise ValueError("inventory repos/projects must be a list")
    normalized = {normalize_gitlab_project_key(item) for item in raw_repos if str(item or "").strip()}
    return sorted(normalized)


def _request_gitlab_json(url: str, token: str) -> Any:
    headers = {"PRIVATE-TOKEN": token}
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()


def _access_level_name(access_level: int | str | None) -> str | None:
    try:
        return ACCESS_LEVEL_NAMES.get(int(access_level))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _max_grant(current: str | None, new: str) -> str:
    order = {"read": 1, "write": 2, "push": 3, "admin": 4}
    if current is None or order[new] > order[current]:
        return new
    return current


def build_snapshot(
    repos: Iterable[str],
    base_url: str,
    token: str,
    get_json: Callable[[str, str], Any] = _request_gitlab_json,
) -> dict:
    """Fetch projects + members/all and build a read-only ACL candidate snapshot."""
    base = str(base_url or DEFAULT_GITLAB_BASE_URL).rstrip("/")
    snapshot: dict[str, Any] = {
        "source": "gitlab",
        "gitlab_base_url": base,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "repo_count": 0,
        "repos": {},
        "repo_acl_by_gitlab_user": {},
        "ignored_members": [],
        "errors": [],
    }

    for repo in repos:
        project_key = normalize_gitlab_project_key(repo)
        encoded = urllib.parse.quote(project_key, safe="")
        project_url = f"{base}/api/v4/projects/{encoded}"
        try:
            project = get_json(project_url, token)
            project_id = project["id"]
            members_url = f"{base}/api/v4/projects/{project_id}/members/all?per_page=100"
            members = get_json(members_url, token)
        except Exception as exc:  # keep batch best-effort and explicit
            snapshot["errors"].append({"repo": project_key, "error": str(exc)})
            continue

        repo_members = []
        for member in members:
            username = str(member.get("username") or "").strip()
            access_level = member.get("access_level")
            grant = access_level_to_grant(access_level)
            normalized_member = {
                "id": member.get("id"),
                "username": username,
                "name": member.get("name"),
                "state": member.get("state"),
                "access_level": access_level,
                "access_name": _access_level_name(access_level),
                "mapped_grant": grant,
                "web_url": member.get("web_url"),
            }
            repo_members.append(normalized_member)
            if not username or grant is None:
                snapshot["ignored_members"].append({"repo": project_key, **normalized_member})
                continue
            user_acl = snapshot["repo_acl_by_gitlab_user"].setdefault(username, {})
            user_acl[project_key] = _max_grant(user_acl.get(project_key), grant)

        snapshot["repos"][project_key] = {
            "project_id": project_id,
            "path_with_namespace": project.get("path_with_namespace", project_key),
            "web_url": project.get("web_url"),
            "members": repo_members,
        }

    snapshot["repo_count"] = len(snapshot["repos"])
    return snapshot


def load_identity_map(path: str | Path | None) -> dict[str, str]:
    """Load explicit GitLab username -> Hermes/Feishu display_name mapping."""
    if not path:
        return {}
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    mapping = data.get("gitlab_to_feishu", data) if isinstance(data, dict) else {}
    if not isinstance(mapping, dict):
        raise ValueError("identity map must be an object or contain gitlab_to_feishu object")
    normalized: dict[str, str] = {}
    for gitlab_username, display_name in mapping.items():
        username = str(gitlab_username or "").strip()
        name = str(display_name or "").strip()
        if username and name:
            normalized[username] = name
    return normalized


def build_hermes_acl_candidate(snapshot: dict, identity_map: dict[str, str]) -> dict:
    """Map GitLab ACL by username into Hermes display-name ACL, fail-closed on unknowns."""
    repo_acl: dict[str, dict[str, str]] = {}
    unknown_identity_mappings: list[dict[str, Any]] = []
    for username in sorted(snapshot.get("repo_acl_by_gitlab_user", {})):
        repos = snapshot["repo_acl_by_gitlab_user"][username]
        display_name = identity_map.get(username)
        if not display_name:
            unknown_identity_mappings.append({"gitlab_username": username, "repos": repos})
            continue
        user_acl = repo_acl.setdefault(display_name, {})
        for repo, grant in sorted(repos.items()):
            user_acl[repo] = _max_grant(user_acl.get(repo), grant)

    return {
        "source": "gitlab_identity_mapped_candidate",
        "gitlab_base_url": snapshot.get("gitlab_base_url"),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "repo_acl": repo_acl,
        "unknown_identity_mappings": unknown_identity_mappings,
        "identity_mapping_required": bool(unknown_identity_mappings),
        "note": "Candidate only. Do not apply to live user-roles.json without explicit identity review and approval.",
    }


def write_snapshot(snapshot: dict, output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build a read-only GitLab members/all snapshot for Hermes repo_acl planning")
    parser.add_argument("--inventory", default=str(DEFAULT_INVENTORY_PATH), help="ci_config repo inventory JSON")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_PATH), help="raw GitLab member snapshot output JSON path")
    parser.add_argument("--candidate-output", default="", help="optional Hermes repo_acl candidate JSON output path")
    parser.add_argument("--identity-map", default="", help="optional explicit GitLab username -> Hermes/Feishu display_name JSON map")
    parser.add_argument("--base-url", default=os.getenv("GITLAB_BASE_URL", DEFAULT_GITLAB_BASE_URL), help="GitLab base URL")
    parser.add_argument("--token-env", default="GITLAB_TOKEN", help="environment variable containing GitLab token")
    args = parser.parse_args(argv)

    token = os.getenv(args.token_env) or os.getenv("GITLAB_PRIVATE_TOKEN")
    if not token:
        print(f"Missing GitLab token. Set {args.token_env} or GITLAB_PRIVATE_TOKEN.", file=sys.stderr)
        return 2

    repos = load_repo_inventory(args.inventory)
    snapshot = build_snapshot(repos=repos, base_url=args.base_url, token=token)
    output = write_snapshot(snapshot, args.output)
    result = {"ok": True, "output": str(output), "repo_count": snapshot["repo_count"], "errors": len(snapshot["errors"])}
    if args.candidate_output:
        identity_map = load_identity_map(args.identity_map)
        candidate = build_hermes_acl_candidate(snapshot, identity_map)
        candidate_output = write_snapshot(candidate, args.candidate_output)
        result.update({
            "candidate_output": str(candidate_output),
            "mapped_users": len(candidate["repo_acl"]),
            "unknown_identity_mappings": len(candidate["unknown_identity_mappings"]),
        })
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
