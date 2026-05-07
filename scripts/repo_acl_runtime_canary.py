#!/usr/bin/env python3
"""Post-reload repo_acl runtime canary for Hermes/VM source admission.

This script is intentionally side-effect-light by default. It imports the VM-side
worktree admission module and verifies the live config/hash/ACL decisions that
protect source access. Set REPO_ACL_CANARY_RUN_ENSURE=1 to also exercise real
ensure_worktree calls, which may create/use worktrees.
"""
from __future__ import annotations

import hashlib
import importlib.util
import json
import os
from pathlib import Path

DEFAULT_EXPECTED_ROLES_SHA = "d36e10a2a343e0deea2a56145448672722ac4de9e62603df4d7af456155be719"
DEFAULT_EXPECTED_WTM_SHA = "c60684c9f4f77abdcf0fff44f56f95824b4600abfcc2cf4bcee4803339f82534"
DEFAULT_ROLES_PATH = Path(os.environ.get("REPO_ACL_CANARY_ROLES_PATH", "/home/mini/.hermes/config/user-roles.json"))
DEFAULT_WTM_PATH = Path(os.environ.get("REPO_ACL_CANARY_WTM_PATH", "/home/mini/.hermes/hermes-agent/gateway/admission/worktree_manager.py"))
DEFAULT_SENIORS = ["陈玉", "刘旭", "郭艳彬", "宋伟军", "王平", "王中坤"]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_wtm(wtm_path: Path):
    spec = importlib.util.spec_from_file_location("repo_acl_canary_worktree_manager", wtm_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load spec for {wtm_path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def check(name: str, value, errors: list[str]):
    if not value:
        errors.append(name)
    return bool(value)


def run_canary(*, roles_path: Path = DEFAULT_ROLES_PATH, wtm_path: Path = DEFAULT_WTM_PATH, expected_roles_sha: str = DEFAULT_EXPECTED_ROLES_SHA, expected_wtm_sha: str = DEFAULT_EXPECTED_WTM_SHA, seniors: list[str] | None = None, run_ensure: bool | None = None) -> dict:
    errors: list[str] = []
    seniors = seniors or list(DEFAULT_SENIORS)
    if run_ensure is None:
        run_ensure = os.environ.get("REPO_ACL_CANARY_RUN_ENSURE") == "1"
    roles_sha = sha256(roles_path)
    wtm_sha = sha256(wtm_path)
    config = json.loads(roles_path.read_text(encoding="utf-8"))
    wtm = load_wtm(wtm_path)

    checks = {
        "roles_sha256": roles_sha,
        "worktree_manager_sha256": wtm_sha,
        "repo_count": len((config.get("repo_config") or {}).get("repos") or {}),
        "repo_acl_default_empty": (config.get("repo_acl") or {}).get("default") == {},
        "senior_roles": {u: (config.get("users") or {}).get(u) == "senior" for u in seniors},
        "grant_counts": {
            u: sum(1 for v in ((config.get("repo_acl") or {}).get(u) or {}).values() if v)
            for u in seniors
        },
        "minieye_read": {u: wtm.repo_acl_allows(config, u, "minieye_dnp_nop", "read") for u in seniors},
        "wang_missing_d2l3_denied": {
            u: not wtm.repo_acl_allows(config, u, "D2L3_Release", "read")
            for u in ["王平", "王中坤"]
        },
    }

    check("roles_sha_match", roles_sha == expected_roles_sha, errors)
    check("worktree_manager_sha_match", wtm_sha == expected_wtm_sha, errors)
    check("repo_count_53", checks["repo_count"] == 53, errors)
    check("repo_acl_default_empty", checks["repo_acl_default_empty"], errors)
    check("all_senior_roles", all(checks["senior_roles"].values()), errors)
    check("all_minieye_read", all(checks["minieye_read"].values()), errors)
    check("wang_missing_d2l3_denied", all(checks["wang_missing_d2l3_denied"].values()), errors)

    try:
        rc = (config.get("repo_config") or {})
        wtm.worktree_path(rc, "../minieye_dnp_nop", "王平")
    except Exception as exc:
        checks["invalid_repo_key_error"] = str(exc)
        check("invalid_repo_key_denied", "invalid repo" in str(exc), errors)
    else:
        checks["invalid_repo_key_error"] = None
        errors.append("invalid_repo_key_denied")

    if run_ensure:
        ensure = {}
        try:
            r = wtm.ensure_worktree("胡子豪", "minieye_dnp_nop", branch="dev-nop")
            ensure["owner"] = r
            check("ensure_owner_path", r.get("path") == "/home/mini/minieye_dnp_nop", errors)
            check("ensure_owner_no_error", not r.get("error"), errors)
        except Exception as exc:
            ensure["owner_error"] = str(exc)
            errors.append("ensure_owner")
        try:
            r = wtm.ensure_worktree("王平", "minieye_dnp_nop")
            ensure["senior_granted"] = r
            check("ensure_senior_granted_path", r.get("path") == "/home/mini/worktrees/minieye_dnp_nop/王平", errors)
            check("ensure_senior_granted_no_error", not r.get("error"), errors)
        except Exception as exc:
            ensure["senior_granted_error"] = str(exc)
            errors.append("ensure_senior_granted")
        try:
            r = wtm.ensure_worktree("王平", "D2L3_Release")
            ensure["senior_missing_acl"] = r
            check("ensure_senior_missing_acl_denied", "missing read ACL" in str(r.get("error")), errors)
        except Exception as exc:
            ensure["senior_missing_acl_error"] = str(exc)
            errors.append("ensure_senior_missing_acl_denied")
        checks["ensure"] = ensure

    return {"ok": not errors, "run_ensure": run_ensure, "checks": checks, "errors": errors}


def main() -> int:
    result = run_canary()
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
