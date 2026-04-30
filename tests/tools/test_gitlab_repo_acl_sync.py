import json
from pathlib import Path

from tools.gitlab_repo_acl_sync import (
    access_level_to_grant,
    build_hermes_acl_candidate,
    build_snapshot,
    load_identity_map,
    load_repo_inventory,
    normalize_gitlab_project_key,
)


def test_normalize_gitlab_project_key_accepts_ssh_url_https_url_and_project_path():
    assert normalize_gitlab_project_key("git@git.minieye.tech:planning_algo/nop/planning.git") == "planning_algo/nop/planning"
    assert normalize_gitlab_project_key("https://git.minieye.tech/planning_algo/nop/planning.git") == "planning_algo/nop/planning"
    assert normalize_gitlab_project_key("planning_algo/nop/planning.git") == "planning_algo/nop/planning"
    assert normalize_gitlab_project_key(" planning_algo/nop/planning ") == "planning_algo/nop/planning"


def test_access_level_to_grant_maps_gitlab_levels_conservatively():
    assert access_level_to_grant(10) is None
    assert access_level_to_grant(20) == "read"
    assert access_level_to_grant(30) == "write"
    assert access_level_to_grant(40) == "push"
    assert access_level_to_grant(50) == "admin"


def test_load_repo_inventory_deduplicates_repos_from_ci_config_artifact(tmp_path):
    inventory = {
        "repos": [
            "git@git.minieye.tech:planning_algo/nop/planning.git",
            "planning_algo/nop/planning.git",
            "https://git.minieye.tech/vehicle_dev/object_perception.git",
        ]
    }
    path = tmp_path / "ci-config-git-repos.json"
    path.write_text(json.dumps(inventory), encoding="utf-8")

    assert load_repo_inventory(path) == [
        "planning_algo/nop/planning",
        "vehicle_dev/object_perception",
    ]


def test_build_snapshot_fetches_members_all_and_builds_candidate_acl(tmp_path):
    calls = []

    def fake_get_json(url, token):
        calls.append(url)
        if url.endswith("/projects/planning_algo%2Fnop%2Fplanning"):
            return {"id": 101, "path_with_namespace": "planning_algo/nop/planning"}
        if url.endswith("/projects/vehicle_dev%2Fobject_perception"):
            return {"id": 202, "path_with_namespace": "vehicle_dev/object_perception"}
        if url.endswith("/projects/101/members/all?per_page=100"):
            return [
                {"username": "alice", "name": "Alice", "access_level": 30},
                {"username": "guest", "name": "Guest", "access_level": 10},
            ]
        if url.endswith("/projects/202/members/all?per_page=100"):
            return [{"username": "bob", "name": "Bob", "access_level": 20}]
        raise AssertionError(f"unexpected url: {url}")

    snapshot = build_snapshot(
        repos=["planning_algo/nop/planning", "vehicle_dev/object_perception"],
        base_url="https://git.minieye.tech",
        token="token",
        get_json=fake_get_json,
    )

    assert calls == [
        "https://git.minieye.tech/api/v4/projects/planning_algo%2Fnop%2Fplanning",
        "https://git.minieye.tech/api/v4/projects/101/members/all?per_page=100",
        "https://git.minieye.tech/api/v4/projects/vehicle_dev%2Fobject_perception",
        "https://git.minieye.tech/api/v4/projects/202/members/all?per_page=100",
    ]
    assert snapshot["repo_count"] == 2
    assert snapshot["repo_acl_by_gitlab_user"] == {
        "alice": {"planning_algo/nop/planning": "write"},
        "bob": {"vehicle_dev/object_perception": "read"},
    }
    assert snapshot["repos"]["planning_algo/nop/planning"]["members"][0]["access_name"] == "Developer"
    assert snapshot["ignored_members"][0]["username"] == "guest"


def test_load_identity_map_accepts_plain_or_wrapped_map(tmp_path):
    plain = tmp_path / "plain.json"
    plain.write_text(json.dumps({"alice": "陈玉"}), encoding="utf-8")
    wrapped = tmp_path / "wrapped.json"
    wrapped.write_text(json.dumps({"gitlab_to_feishu": {"bob": "王平"}}), encoding="utf-8")

    assert load_identity_map(plain) == {"alice": "陈玉"}
    assert load_identity_map(wrapped) == {"bob": "王平"}


def test_build_hermes_acl_candidate_is_fail_closed_for_unknown_gitlab_users():
    snapshot = {
        "gitlab_base_url": "https://git.minieye.tech",
        "repo_acl_by_gitlab_user": {
            "alice": {"planning_algo/nop/planning": "write"},
            "bob": {"vehicle_dev/object_perception": "read"},
        },
    }

    candidate = build_hermes_acl_candidate(snapshot, {"alice": "陈玉"})

    assert candidate["repo_acl"] == {"陈玉": {"planning_algo/nop/planning": "write"}}
    assert candidate["unknown_identity_mappings"] == [
        {
            "gitlab_username": "bob",
            "repos": {"vehicle_dev/object_perception": "read"},
        }
    ]
    assert candidate["identity_mapping_required"] is True
