from __future__ import annotations

import importlib.util
import json
from pathlib import Path


repo_root = Path(__file__).parent.parent
script_path = repo_root / "scripts" / "repo_acl_runtime_canary.py"
spec = importlib.util.spec_from_file_location("repo_acl_runtime_canary", str(script_path))
canary = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(canary)


class FakeWtm:
    @staticmethod
    def repo_acl_allows(config, user, repo, action):
        grant = ((config.get("repo_acl") or {}).get(user) or {}).get(repo, "")
        order = {"": 0, "read": 1, "write": 2, "push": 3, "admin": 4}
        return order.get(grant, 0) >= order[action]

    @staticmethod
    def worktree_path(rc, repo, user):
        if ".." in repo:
            raise ValueError(f"invalid repo: {repo!r}")
        return f"/home/mini/worktrees/{repo}/{user}"

    @staticmethod
    def ensure_worktree(user, repo, branch=None):
        if user == "胡子豪" and repo == "minieye_dnp_nop":
            return {"path": "/home/mini/minieye_dnp_nop", "branch": branch or "dev-nop", "created": False}
        if user == "王平" and repo == "minieye_dnp_nop":
            return {"path": "/home/mini/worktrees/minieye_dnp_nop/王平", "branch": "HEAD", "created": False}
        if user == "王平" and repo == "D2L3_Release":
            return {"error": "repo access denied for 王平: missing read ACL for D2L3_Release"}
        return {"error": "unexpected fake call"}


def _write_fixture(tmp_path: Path, monkeypatch, *, missing_d2l3_denied: bool = True):
    roles = tmp_path / "user-roles.json"
    wtm = tmp_path / "worktree_manager.py"
    seniors = ["陈玉", "刘旭", "郭艳彬", "宋伟军", "王平", "王中坤"]
    repo_acl = {
        user: {
            "minieye_dnp_nop": "read",
            "D2L3_Release": "" if missing_d2l3_denied else "read",
        }
        for user in seniors
    }
    data = {
        "users": {user: "senior" for user in seniors},
        "repo_config": {"repos": {f"repo_{i}": {} for i in range(51)} | {"minieye_dnp_nop": {}, "D2L3_Release": {}}},
        "repo_acl": {"default": {}, **repo_acl},
    }
    roles.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    wtm.write_text("# fake module placeholder\n", encoding="utf-8")
    monkeypatch.setattr(canary, "load_wtm", lambda path: FakeWtm)
    return roles, wtm, canary.sha256(roles), canary.sha256(wtm), seniors


def test_runtime_canary_accepts_expected_acl_fixture(tmp_path, monkeypatch):
    roles, wtm, roles_sha, wtm_sha, seniors = _write_fixture(tmp_path, monkeypatch)

    result = canary.run_canary(
        roles_path=roles,
        wtm_path=wtm,
        expected_roles_sha=roles_sha,
        expected_wtm_sha=wtm_sha,
        seniors=seniors,
        run_ensure=True,
    )

    assert result["ok"] is True
    assert result["run_ensure"] is True
    assert result["errors"] == []
    assert result["checks"]["repo_count"] == 53
    assert result["checks"]["ensure"]["senior_missing_acl"]["error"].endswith("missing read ACL for D2L3_Release")


def test_runtime_canary_fails_if_wang_d2l3_is_accidentally_granted(tmp_path, monkeypatch):
    roles, wtm, roles_sha, wtm_sha, seniors = _write_fixture(tmp_path, monkeypatch, missing_d2l3_denied=False)

    result = canary.run_canary(
        roles_path=roles,
        wtm_path=wtm,
        expected_roles_sha=roles_sha,
        expected_wtm_sha=wtm_sha,
        seniors=seniors,
        run_ensure=False,
    )

    assert result["ok"] is False
    assert "wang_missing_d2l3_denied" in result["errors"]
