"""Tests for identity-based permission policy command classification."""

import json
import sys
from pathlib import Path

_repo = str(Path(__file__).resolve().parents[2])
if _repo not in sys.path:
    sys.path.insert(0, _repo)


def _write_policy_config(path: Path) -> None:
    config = {
        "users": {
            "胡子豪": "owner",
            "陈玉": "senior",
            "王平": "member",
            "default": "member",
        },
        "repo_acl": {
            "陈玉": {
                "minieye_dnp_nop": "push",
                "pnc_specs": "read",
            },
            "default": {},
        },
        "repo_config": {
            "repos": {
                "minieye_dnp_nop": {"source": "/home/mini/minieye_dnp_nop", "default_branch": "main"},
                "pnc_specs": {"source": "/home/mini/pnc_specs", "default_branch": "main"},
            }
        },
        "user_id_mapping": {
            "ou_owner": "胡子豪",
            "ou_member": "王平",
        },
        "command_patterns": {
            "read": ["ssh-mini-agent list_files", "ssh-mini-agent read_file"],
            "vm_direct_exec": ["ssh-mini-run", "ssh mini@"],
            "write": ["ssh-mini-agent edit_file"],
            "delete_small": ["rm -f"],
            "dangerous": ["git reset --hard", "git push --force"],
        },
        "critical_paths": [],
        "permission_matrix": {
            "owner": {
                "read": "ALLOW",
                "write": "ALLOW",
                "delete_small": "ALLOW",
                "dangerous": "ALLOW",
                "vm_direct_exec": "ALLOW",
                "vm_git_routine": "ALLOW",
                "vm_git_push": "ALLOW",
            },
            "member": {
                "read": "ALLOW",
                "write": "APPROVE",
                "delete_small": "APPROVE",
                "dangerous": "DENY",
                "vm_direct_exec": "DENY",
                "vm_git_routine": "ALLOW",
                "vm_git_push": "APPROVE",
            },
        },
    }
    path.write_text(json.dumps(config, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")



def test_repo_acl_supports_gitlab_project_exact_and_group_wildcard(monkeypatch, tmp_path):
    import tools.permission_policy as permission_policy

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    cfg = json.loads(config_path.read_text(encoding="utf-8"))
    cfg["users"]["刘旭"] = "senior"
    cfg["repo_acl"]["刘旭"] = {
        "planning_algo/*": "read",
        "vehicle_dev/object_perception": "write",
    }
    config_path.write_text(json.dumps(cfg, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)

    assert permission_policy.repo_acl_allows("刘旭", "planning_algo/nop/planning", "read") is True
    assert permission_policy.repo_acl_allows("刘旭", "planning_algo", "read") is False
    assert permission_policy.repo_acl_allows("刘旭", "planning_algo/nop/planning", "write") is False
    assert permission_policy.repo_acl_allows("刘旭", "vehicle_dev/object_perception", "write") is True
    assert permission_policy.repo_acl_allows("刘旭", "vehicle_dev/other", "read") is False


def test_grant_repo_acl_accepts_gitlab_project_paths_and_group_wildcards(monkeypatch, tmp_path):
    import tools.permission_policy as permission_policy

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)

    permission_policy.grant_repo_acl("陈玉", "planning_algo/nop/planning", "read")
    permission_policy.grant_repo_acl("陈玉", "planning_algo/*", "write")

    saved = json.loads(config_path.read_text(encoding="utf-8"))
    assert saved["repo_acl"]["陈玉"]["planning_algo/nop/planning"] == "read"
    assert saved["repo_acl"]["陈玉"]["planning_algo/*"] == "write"
    assert permission_policy.repo_acl_allows("陈玉", "planning_algo/nop/other", "write") is True
    assert permission_policy.repo_acl_allows("陈玉", "planning_algo", "write") is False
    assert permission_policy.repo_acl_allows("陈玉", "planning_algo/*", "read") is False


def test_slash_repo_worktree_paths_use_configured_repo_boundary(monkeypatch, tmp_path):
    import tools.permission_policy as permission_policy

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    config = json.loads(config_path.read_text(encoding="utf-8"))
    config["users"]["刘旭"] = "senior"
    config["repo_acl"]["刘旭"] = {"planning_algo/*": "write"}
    config["repo_config"]["repos"]["planning_algo"] = {
        "source": "/home/mini/planning_algo",
        "default_branch": "main",
    }
    config["repo_config"]["repos"]["planning_algo/nop/planning"] = {
        "source": "/home/mini/planning_algo_nop_planning",
        "default_branch": "main",
    }
    config_path.write_text(json.dumps(config, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    monkeypatch.setenv("HERMES_SESSION_USER_NAME", "刘旭")
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)

    read_cmd = "ssh-mini-agent read_file /home/mini/worktrees/planning_algo/nop/planning/刘旭/README.md --start 1 --lines 5"
    git_cmd = "ssh-mini-run 'cd /home/mini/worktrees/planning_algo/nop/planning/刘旭 && git status'"

    assert permission_policy.classify_command(read_cmd) == "read"
    assert permission_policy.classify_command(git_cmd) == "vm_git_routine"
    assert permission_policy.get_decision("刘旭", git_cmd) == "ALLOW"


def test_slash_repo_worktree_wrong_user_is_denied(monkeypatch, tmp_path):
    import tools.permission_policy as permission_policy

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    config = json.loads(config_path.read_text(encoding="utf-8"))
    config["users"]["刘旭"] = "senior"
    config["repo_acl"]["刘旭"] = {"planning_algo/*": "read"}
    config["repo_config"]["repos"]["planning_algo/nop/planning"] = {
        "source": "/home/mini/planning_algo_nop_planning",
        "default_branch": "main",
    }
    config_path.write_text(json.dumps(config, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    monkeypatch.setenv("HERMES_SESSION_USER_NAME", "刘旭")
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)

    cmd = "ssh-mini-agent read_file /home/mini/worktrees/planning_algo/nop/planning/王平/README.md --start 1 --lines 5"

    assert permission_policy.classify_command(cmd) == "vm_repo_unauthorized"
    assert permission_policy.get_decision("刘旭", cmd) == "DENY"


def test_ssh_mini_agent_worktree_dotdot_path_is_denied(monkeypatch, tmp_path):
    import tools.permission_policy as permission_policy

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    config = json.loads(config_path.read_text(encoding="utf-8"))
    config["users"]["刘旭"] = "senior"
    config["repo_acl"]["刘旭"] = {"minieye_dnp_nop": "read"}
    config_path.write_text(json.dumps(config, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    monkeypatch.setenv("HERMES_SESSION_USER_NAME", "刘旭")
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)

    cmd = "ssh-mini-agent read_file /home/mini/worktrees/minieye_dnp_nop/刘旭/../王平/README.md --start 1 --lines 5"

    assert permission_policy.classify_command(cmd) == "vm_repo_unauthorized"
    assert permission_policy.get_decision("刘旭", cmd) == "DENY"


def test_check_dangerous_command_does_not_turn_generic_denies_into_repo_acl_request(monkeypatch, tmp_path):
    import tools.approval as approval
    import tools.permission_policy as permission_policy
    from gateway.session_context import clear_session_vars, set_session_vars

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)
    monkeypatch.setenv("HERMES_GATEWAY_SESSION", "1")
    monkeypatch.delenv("HERMES_YOLO_MODE", raising=False)
    monkeypatch.delenv("HERMES_INTERACTIVE", raising=False)
    monkeypatch.setenv("HERMES_REPO_ACL_APPROVAL_DIR", str(tmp_path / "repo-acl"))
    tokens = set_session_vars(
        platform="feishu",
        chat_id="oc_test",
        user_id="ou_member",
        user_name="王平",
        session_key="agent:main:feishu:group:generic-danger-deny",
    )
    try:
        with approval._lock:
            approval._gateway_notify_cbs.clear()
            approval._gateway_timeout_cbs.clear()
            approval._gateway_queues.clear()
        approval.clear_session("agent:main:feishu:group:generic-danger-deny")
        result = approval.check_dangerous_command("git push --force origin main", "local")
    finally:
        clear_session_vars(tokens)
        with approval._lock:
            approval._gateway_notify_cbs.clear()
            approval._gateway_timeout_cbs.clear()
            approval._gateway_queues.clear()
        approval.clear_session("agent:main:feishu:group:generic-danger-deny")

    assert result["approved"] is False
    assert result.get("description") != "VM repository access denied by repo ACL policy"
    assert "repo_acl_request" not in result
    assert "repo_acl_approval_card" not in result
    assert result.get("status") != "repo_acl_approval_pending"


def test_check_dangerous_command_yolo_does_not_bypass_repo_acl_source_denial(monkeypatch, tmp_path):
    import tools.approval as approval
    import tools.permission_policy as permission_policy
    from gateway.session_context import clear_session_vars, set_session_vars

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)
    monkeypatch.setenv("HERMES_GATEWAY_SESSION", "1")
    monkeypatch.setenv("HERMES_YOLO_MODE", "1")
    monkeypatch.delenv("HERMES_INTERACTIVE", raising=False)
    monkeypatch.setenv("HERMES_REPO_ACL_APPROVAL_DIR", str(tmp_path / "repo-acl"))
    tokens = set_session_vars(
        platform="feishu",
        chat_id="oc_test",
        user_id="ou_member",
        user_name="王平",
        session_key="agent:main:feishu:group:yolo-source-deny",
    )
    try:
        result = approval.check_dangerous_command(
            "ssh-mini-run 'cd /home/mini/worktrees/minieye_dnp_nop/王平 && python3 run_eval.py'",
            "local",
        )
    finally:
        clear_session_vars(tokens)
        monkeypatch.delenv("HERMES_YOLO_MODE", raising=False)

    assert result["approved"] is False
    assert result.get("description") == "VM repository access denied by repo ACL policy"
    assert result.get("status") in {None, "repo_acl_approval_pending"}


def test_authorized_senior_raw_repo_command_gets_direct_exec_denial_not_repo_acl_request(monkeypatch, tmp_path):
    import tools.approval as approval
    import tools.permission_policy as permission_policy
    from gateway.session_context import clear_session_vars, set_session_vars

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)
    monkeypatch.setenv("HERMES_GATEWAY_SESSION", "1")
    monkeypatch.delenv("HERMES_YOLO_MODE", raising=False)
    monkeypatch.delenv("HERMES_INTERACTIVE", raising=False)
    monkeypatch.setenv("HERMES_REPO_ACL_APPROVAL_DIR", str(tmp_path / "repo-acl"))
    tokens = set_session_vars(
        platform="feishu",
        chat_id="oc_test",
        user_id="ou_chenyu",
        user_name="陈玉",
        session_key="agent:main:feishu:group:authorized-raw-repo-deny",
    )
    command = "ssh-mini-run 'cd /home/mini/worktrees/minieye_dnp_nop/陈玉 && python3 run_eval.py'"
    try:
        with approval._lock:
            approval._gateway_notify_cbs.clear()
            approval._gateway_timeout_cbs.clear()
            approval._gateway_queues.clear()
        approval.clear_session("agent:main:feishu:group:authorized-raw-repo-deny")

        assert permission_policy.classify_command(command) == "vm_direct_exec"
        result = approval.check_all_command_guards(command, "local")
    finally:
        clear_session_vars(tokens)
        with approval._lock:
            approval._gateway_notify_cbs.clear()
            approval._gateway_timeout_cbs.clear()
            approval._gateway_queues.clear()
        approval.clear_session("agent:main:feishu:group:authorized-raw-repo-deny")

    assert result["approved"] is False
    assert result.get("description") == "Permission policy denied this command"
    assert "repo_acl_request" not in result
    assert "repo_acl_approval_card" not in result
    assert result.get("status") != "repo_acl_approval_pending"

def test_member_cannot_run_routine_vm_git_commands(monkeypatch, tmp_path):
    import tools.permission_policy as permission_policy

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    monkeypatch.setenv("HERMES_SESSION_USER_NAME", "王平")
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)

    commands = [
        "ssh-mini-run 'cd /home/mini/worktrees/minieye_dnp_nop/王平 && git fetch origin'",
        "ssh-mini-run 'cd /home/mini/worktrees/minieye_dnp_nop/王平 && git pull --ff-only origin dev-nop'",
    ]

    for command in commands:
        assert permission_policy.classify_command(command) == "vm_repo_unauthorized"
        assert permission_policy.get_decision("王平", command) == "DENY"
def test_member_read_vm_repo_denied(monkeypatch, tmp_path):
    import tools.permission_policy as permission_policy

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    monkeypatch.setenv("HERMES_SESSION_USER_NAME", "王平")
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)

    command = "ssh-mini-agent read_file /home/mini/worktrees/minieye_dnp_nop/王平/src/main.py --start 1 --lines 20"

    assert permission_policy.classify_command(command) == "vm_repo_unauthorized"
    assert permission_policy.get_decision("王平", command) == "DENY"


def test_senior_read_vm_repo_requires_repo_acl(monkeypatch, tmp_path):
    import tools.permission_policy as permission_policy

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    monkeypatch.setenv("HERMES_SESSION_USER_NAME", "陈玉")
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)

    allowed = "ssh-mini-agent read_file /home/mini/worktrees/pnc_specs/陈玉/README.md --start 1 --lines 20"
    denied = "ssh-mini-agent read_file /home/mini/worktrees/dnp_develop_enviroment/陈玉/README.md --start 1 --lines 20"

    assert permission_policy.classify_command(allowed) == "read"
    assert permission_policy.get_decision("陈玉", allowed) == "ALLOW"
    assert permission_policy.classify_command(denied) == "vm_repo_unauthorized"
    assert permission_policy.get_decision("陈玉", denied) == "DENY"


def test_senior_cannot_read_main_repo_even_with_repo_acl(monkeypatch, tmp_path):
    import tools.permission_policy as permission_policy

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    monkeypatch.setenv("HERMES_SESSION_USER_NAME", "陈玉")
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)

    command = "ssh-mini-agent read_file /home/mini/minieye_dnp_nop/README.md --start 1 --lines 20"

    assert permission_policy.classify_command(command) == "vm_repo_unauthorized"
    assert permission_policy.get_decision("陈玉", command) == "DENY"


def test_member_cannot_write_vm_git_even_with_repo_acl(monkeypatch, tmp_path):
    import tools.permission_policy as permission_policy

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    config = json.loads(config_path.read_text(encoding="utf-8"))
    config["repo_acl"]["王平"] = {"minieye_dnp_nop": "write"}
    config_path.write_text(json.dumps(config, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    monkeypatch.setenv("HERMES_SESSION_USER_NAME", "王平")
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)

    command = "ssh-mini-run 'cd /home/mini/worktrees/minieye_dnp_nop/王平 && git checkout dev-nop-wp'"

    assert permission_policy.classify_command(command) == "vm_git_routine"
    assert permission_policy.get_decision("王平", command) == "DENY"


def test_member_direct_vm_git_push_is_denied(monkeypatch, tmp_path):
    import tools.permission_policy as permission_policy

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    monkeypatch.setenv("HERMES_SESSION_USER_NAME", "王平")
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)

    command = "ssh-mini-run 'cd /home/mini/worktrees/minieye_dnp_nop/王平 && git push origin wp/fix'"

    assert permission_policy.classify_command(command) == "vm_repo_unauthorized"
    assert permission_policy.get_decision("王平", command) == "DENY"

def test_senior_with_repo_acl_can_run_routine_vm_git_commands(monkeypatch, tmp_path):
    import tools.permission_policy as permission_policy

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    monkeypatch.setenv("HERMES_SESSION_USER_NAME", "陈玉")
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)

    commands = [
        "ssh-mini-run 'cd /home/mini/worktrees/minieye_dnp_nop/陈玉 && git fetch origin'",
        "ssh-mini-run 'cd /home/mini/worktrees/minieye_dnp_nop/陈玉 && git pull --ff-only origin dev-nop'",
    ]

    for command in commands:
        assert permission_policy.classify_command(command) == "vm_git_routine"
        assert permission_policy.get_decision("陈玉", command) == "ALLOW"


def test_senior_without_repo_acl_cannot_run_vm_git(monkeypatch, tmp_path):
    import tools.permission_policy as permission_policy

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    monkeypatch.setenv("HERMES_SESSION_USER_NAME", "陈玉")
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)

    command = "ssh-mini-run 'cd /home/mini/worktrees/dnp_develop_enviroment/陈玉 && git status'"

    assert permission_policy.classify_command(command) == "vm_repo_unauthorized"
    assert permission_policy.get_decision("陈玉", command) == "DENY"


def test_senior_read_only_repo_acl_cannot_write_git(monkeypatch, tmp_path):
    import tools.permission_policy as permission_policy

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    monkeypatch.setenv("HERMES_SESSION_USER_NAME", "陈玉")
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)

    status = "ssh-mini-run 'cd /home/mini/worktrees/pnc_specs/陈玉 && git status'"
    checkout = "ssh-mini-run 'cd /home/mini/worktrees/pnc_specs/陈玉 && git checkout main'"

    assert permission_policy.classify_command(status) == "vm_git_routine"
    assert permission_policy.get_decision("陈玉", status) == "ALLOW"
    assert permission_policy.classify_command(checkout) == "vm_repo_unauthorized"
    assert permission_policy.get_decision("陈玉", checkout) == "DENY"


def test_senior_repo_acl_push_is_allowed_without_approval(monkeypatch, tmp_path):
    import tools.permission_policy as permission_policy

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    monkeypatch.setenv("HERMES_SESSION_USER_NAME", "陈玉")
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)

    command = "ssh-mini-run 'cd /home/mini/worktrees/minieye_dnp_nop/陈玉 && git push origin cy/fix'"

    assert permission_policy.classify_command(command) == "vm_git_push"
    assert permission_policy.get_decision("陈玉", command) == "ALLOW"


def test_senior_routine_master_sync_push_is_allowed_without_approval(monkeypatch, tmp_path):
    import tools.permission_policy as permission_policy

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    cfg = json.loads(config_path.read_text(encoding="utf-8"))
    cfg["users"]["刘旭"] = "senior"
    cfg["repo_acl"]["刘旭"] = {"minieye_ci_eval": "push"}
    cfg["repo_config"]["repos"]["minieye_ci_eval"] = {
        "source": "/home/mini/minieye_ci_eval",
        "default_branch": "master",
    }
    config_path.write_text(json.dumps(cfg, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    monkeypatch.setenv("HERMES_SESSION_USER_NAME", "刘旭")
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)

    command = "ssh-mini-run 'cd /home/mini/worktrees/minieye_ci_eval/刘旭 && git fetch origin && git merge origin/dev-nop && git push origin master'"

    assert permission_policy.classify_command(command) == "vm_git_push"
    assert permission_policy.get_decision("刘旭", command) == "ALLOW"


def test_member_force_push_remains_denied(monkeypatch, tmp_path):
    import tools.permission_policy as permission_policy

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    monkeypatch.setenv("HERMES_SESSION_USER_NAME", "王平")
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)

    command = "ssh-mini-run 'cd /home/mini/worktrees/minieye_dnp_nop/王平 && git push --force origin dev-nop'"

    assert permission_policy.classify_command(command) == "vm_git_dangerous"
    assert permission_policy.get_decision("王平", command) == "DENY"


def test_non_git_ssh_mini_run_outside_repo_does_not_require_repo_acl(monkeypatch, tmp_path):
    import tools.permission_policy as permission_policy

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    monkeypatch.setenv("HERMES_SESSION_USER_NAME", "王平")
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)

    command = "ssh-mini-run 'cd /mnt/tmp/eval_job && python3 run_eval.py --input data.json'"

    assert permission_policy.classify_command(command) == "write"
    assert permission_policy.get_decision("王平", command) == "APPROVE"


def test_non_git_ssh_mini_run_inside_unauthorized_repo_is_source_guarded(monkeypatch, tmp_path):
    import tools.permission_policy as permission_policy

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    monkeypatch.setenv("HERMES_SESSION_USER_NAME", "王平")
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)

    command = "ssh-mini-run 'cd /home/mini/worktrees/minieye_dnp_nop/王平 && python3 run_eval.py'"

    assert permission_policy.classify_command(command) == "vm_repo_unauthorized"
    assert permission_policy.get_decision("王平", command) == "DENY"


def test_mixed_arbitrary_vm_command_with_git_status_stays_direct_exec(monkeypatch, tmp_path):
    import tools.permission_policy as permission_policy

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    monkeypatch.setenv("HERMES_SESSION_USER_NAME", "王平")
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)

    command = "ssh-mini-run 'cd /home/mini/worktrees/minieye_dnp_nop/王平 && python3 run_eval.py && git status'"

    assert permission_policy.classify_command(command) == "vm_repo_unauthorized"
    assert permission_policy.get_decision("王平", command) == "DENY"


def test_routine_git_requires_user_worktree_scope(monkeypatch, tmp_path):
    import tools.permission_policy as permission_policy

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    monkeypatch.setenv("HERMES_SESSION_USER_NAME", "王平")
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)

    command = "ssh-mini-run 'cd /home/mini/minieye_dnp_nop && git status'"

    assert permission_policy.classify_command(command) == "vm_repo_unauthorized"
    assert permission_policy.get_decision("王平", command) == "DENY"


def test_member_audit_logger_then_git_stays_direct_exec(monkeypatch, tmp_path):
    import tools.permission_policy as permission_policy

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    monkeypatch.setenv("HERMES_SESSION_USER_NAME", "王平")
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)

    command = (
        "ssh-mini-run 'cd /home/mini/worktrees/minieye_dnp_nop/王平 && "
        "/home/mini/worktrees/audit-logger.sh 王平 minieye_dnp_nop git_fetch_origin && "
        "git fetch origin'"
    )

    assert permission_policy.classify_command(command) == "vm_repo_unauthorized"
    assert permission_policy.get_decision("王平", command) == "DENY"


def test_newline_smuggling_stays_direct_exec(monkeypatch, tmp_path):
    import tools.permission_policy as permission_policy

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    monkeypatch.setenv("HERMES_SESSION_USER_NAME", "王平")
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)

    command = "ssh-mini-run 'cd /home/mini/worktrees/minieye_dnp_nop/王平 && git status\npython3 run_eval.py'"

    assert permission_policy.classify_command(command) == "vm_direct_exec"
    assert permission_policy.get_decision("王平", command) == "DENY"


def test_git_before_worktree_cd_stays_direct_exec(monkeypatch, tmp_path):
    import tools.permission_policy as permission_policy

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    monkeypatch.setenv("HERMES_SESSION_USER_NAME", "王平")
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)

    command = "ssh-mini-run 'git status && cd /home/mini/worktrees/minieye_dnp_nop/王平'"

    assert permission_policy.classify_command(command) == "vm_direct_exec"
    assert permission_policy.get_decision("王平", command) == "DENY"


def test_member_cannot_target_another_users_worktree(monkeypatch, tmp_path):
    import tools.permission_policy as permission_policy

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    monkeypatch.setenv("HERMES_SESSION_USER_NAME", "王平")
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)

    command = "ssh-mini-run 'cd /home/mini/worktrees/minieye_dnp_nop/陈玉 && git status'"

    assert permission_policy.classify_command(command) == "vm_repo_unauthorized"
    assert permission_policy.get_decision("王平", command) == "DENY"


def test_force_with_lease_remains_denied(monkeypatch, tmp_path):
    import tools.permission_policy as permission_policy

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    monkeypatch.setenv("HERMES_SESSION_USER_NAME", "王平")
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)

    command = "ssh-mini-run 'cd /home/mini/worktrees/minieye_dnp_nop/王平 && git push --force-with-lease origin dev-nop'"

    assert permission_policy.classify_command(command) == "vm_git_dangerous"
    assert permission_policy.get_decision("王平", command) == "DENY"


def test_direct_local_git_command_is_not_member_routine(monkeypatch, tmp_path):
    import tools.permission_policy as permission_policy

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    monkeypatch.setenv("HERMES_SESSION_USER_NAME", "王平")
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)

    command = "git status"

    assert permission_policy.classify_command(command) == "write"
    assert permission_policy.get_decision("王平", command) == "APPROVE"


def test_plus_refspec_force_push_remains_denied(monkeypatch, tmp_path):
    import tools.permission_policy as permission_policy

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    monkeypatch.setenv("HERMES_SESSION_USER_NAME", "王平")
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)

    command = "ssh-mini-run 'cd /home/mini/worktrees/minieye_dnp_nop/王平 && git push origin +main'"

    assert permission_policy.classify_command(command) == "vm_git_dangerous"
    assert permission_policy.get_decision("王平", command) == "DENY"


def test_git_clean_split_force_flag_remains_denied(monkeypatch, tmp_path):
    import tools.permission_policy as permission_policy

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    monkeypatch.setenv("HERMES_SESSION_USER_NAME", "王平")
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)

    commands = [
        "ssh-mini-run 'cd /home/mini/worktrees/minieye_dnp_nop/王平 && git clean -d -f'",
        "ssh-mini-run 'cd /home/mini/worktrees/minieye_dnp_nop/王平 && git clean -n'",
    ]

    for command in commands:
        assert permission_policy.classify_command(command) == "vm_git_dangerous"
        assert permission_policy.get_decision("王平", command) == "DENY"


def test_untrusted_path_prefixed_ssh_mini_run_is_not_routine(monkeypatch, tmp_path):
    import tools.permission_policy as permission_policy

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    monkeypatch.setenv("HERMES_SESSION_USER_NAME", "王平")
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)

    command = "/tmp/evil/ssh-mini-run 'cd /home/mini/worktrees/minieye_dnp_nop/王平 && git status'"

    assert permission_policy.classify_command(command) == "vm_direct_exec"
    assert permission_policy.get_decision("王平", command) == "DENY"


def test_git_submodule_foreach_is_not_routine(monkeypatch, tmp_path):
    import tools.permission_policy as permission_policy

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    monkeypatch.setenv("HERMES_SESSION_USER_NAME", "王平")
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)

    command = "ssh-mini-run 'cd /home/mini/worktrees/minieye_dnp_nop/王平 && git submodule foreach python3 run_eval.py'"

    assert permission_policy.classify_command(command) == "vm_repo_unauthorized"
    assert permission_policy.get_decision("王平", command) == "DENY"


def test_git_rebase_exec_is_not_routine(monkeypatch, tmp_path):
    import tools.permission_policy as permission_policy

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    monkeypatch.setenv("HERMES_SESSION_USER_NAME", "王平")
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)

    command = "ssh-mini-run 'cd /home/mini/worktrees/minieye_dnp_nop/王平 && git rebase --exec python3 origin/main'"

    assert permission_policy.classify_command(command) == "vm_repo_unauthorized"
    assert permission_policy.get_decision("王平", command) == "DENY"


def test_worktree_path_traversal_is_not_routine(monkeypatch, tmp_path):
    import tools.permission_policy as permission_policy

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    monkeypatch.setenv("HERMES_SESSION_USER_NAME", "王平")
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)

    command = "ssh-mini-run 'cd /home/mini/worktrees/minieye_dnp_nop/王平/../../陈玉 && git status'"

    assert permission_policy.classify_command(command) == "vm_repo_unauthorized"
    assert permission_policy.get_decision("王平", command) == "DENY"


def test_worktree_repo_dotdot_is_not_routine(monkeypatch, tmp_path):
    import tools.permission_policy as permission_policy

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    monkeypatch.setenv("HERMES_SESSION_USER_NAME", "王平")
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)

    command = "ssh-mini-run 'cd /home/mini/worktrees/../王平 && git status'"

    assert permission_policy.classify_command(command) == "vm_repo_unauthorized"
    assert permission_policy.get_decision("王平", command) == "DENY"


def test_quoted_payload_with_local_suffix_is_not_routine(monkeypatch, tmp_path):
    import tools.permission_policy as permission_policy

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    monkeypatch.setenv("HERMES_SESSION_USER_NAME", "王平")
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)

    command = "ssh-mini-run 'cd /home/mini/worktrees/minieye_dnp_nop/王平 && git status' && python3 run_eval.py #'"

    assert permission_policy.classify_command(command) == "vm_direct_exec"
    assert permission_policy.get_decision("王平", command) == "DENY"


def test_outer_command_newline_is_not_routine(monkeypatch, tmp_path):
    import tools.permission_policy as permission_policy

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    monkeypatch.setenv("HERMES_SESSION_USER_NAME", "王平")
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)

    command = "ssh-mini-run\n'cd /home/mini/worktrees/minieye_dnp_nop/王平 && git status'"

    assert permission_policy.classify_command(command) == "vm_direct_exec"
    assert permission_policy.get_decision("王平", command) == "DENY"


def test_git_glob_metacharacters_are_not_routine(monkeypatch, tmp_path):
    import tools.permission_policy as permission_policy

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    monkeypatch.setenv("HERMES_SESSION_USER_NAME", "王平")
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)

    command = "ssh-mini-run 'cd /home/mini/worktrees/minieye_dnp_nop/王平 && git add *'"

    assert permission_policy.classify_command(command) == "vm_repo_unauthorized"
    assert permission_policy.get_decision("王平", command) == "DENY"


def test_git_rebase_attached_exec_is_not_routine(monkeypatch, tmp_path):
    import tools.permission_policy as permission_policy

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    monkeypatch.setenv("HERMES_SESSION_USER_NAME", "王平")
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)

    command = "ssh-mini-run 'cd /home/mini/worktrees/minieye_dnp_nop/王平 && git rebase -xpython3 origin/main'"

    assert permission_policy.classify_command(command) == "vm_repo_unauthorized"
    assert permission_policy.get_decision("王平", command) == "DENY"


def test_branch_combined_force_delete_remains_denied(monkeypatch, tmp_path):
    import tools.permission_policy as permission_policy

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    monkeypatch.setenv("HERMES_SESSION_USER_NAME", "王平")
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)

    commands = [
        "ssh-mini-run 'cd /home/mini/worktrees/minieye_dnp_nop/王平 && git branch -Dr origin/foo'",
        "ssh-mini-run 'cd /home/mini/worktrees/minieye_dnp_nop/王平 && git branch --delete --force origin/foo'",
        "ssh-mini-run 'cd /home/mini/worktrees/minieye_dnp_nop/王平 && git branch -d -f origin/foo'",
        "ssh-mini-run 'cd /home/mini/worktrees/minieye_dnp_nop/王平 && git branch -df origin/foo'",
        "ssh-mini-run 'cd /home/mini/worktrees/minieye_dnp_nop/王平 && git branch --delete -f origin/foo'",
        "ssh-mini-run 'cd /home/mini/worktrees/minieye_dnp_nop/王平 && git branch -d --force origin/foo'",
        "git branch -df origin/foo",
    ]

    for command in commands:
        assert permission_policy.classify_command(command) == "vm_git_dangerous"
        assert permission_policy.get_decision("王平", command) == "DENY"


def test_vm_git_dangerous_denies_even_if_matrix_allows(monkeypatch, tmp_path):
    import tools.permission_policy as permission_policy

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    config = json.loads(config_path.read_text(encoding="utf-8"))
    config["permission_matrix"]["member"]["vm_git_dangerous"] = "ALLOW"
    config_path.write_text(json.dumps(config, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)

    command = "ssh-mini-run 'cd /home/mini/worktrees/minieye_dnp_nop/王平 && git push --force origin dev-nop'"

    assert permission_policy.classify_command(command) == "vm_git_dangerous"
    assert permission_policy.get_decision("王平", command) == "DENY"


def test_vm_git_push_denied_for_member_even_if_matrix_allows(monkeypatch, tmp_path):
    import tools.permission_policy as permission_policy

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    config = json.loads(config_path.read_text(encoding="utf-8"))
    config["permission_matrix"]["member"]["vm_git_push"] = "ALLOW"
    config["repo_acl"]["王平"] = {"minieye_dnp_nop": "push"}
    config_path.write_text(json.dumps(config, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    monkeypatch.setenv("HERMES_SESSION_USER_NAME", "王平")
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)

    command = "ssh-mini-run 'cd /home/mini/worktrees/minieye_dnp_nop/王平 && git push origin wp/fix'"

    assert permission_policy.classify_command(command) == "vm_git_push"
    assert permission_policy.get_decision("王平", command) == "DENY"


def test_reset_hard_with_leading_options_remains_denied(monkeypatch, tmp_path):
    import tools.permission_policy as permission_policy

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    monkeypatch.setenv("HERMES_SESSION_USER_NAME", "王平")
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)

    commands = [
        "ssh-mini-run 'cd /home/mini/worktrees/minieye_dnp_nop/王平 && git reset -q --hard HEAD'",
        "ssh-mini-run 'cd /home/mini/worktrees/minieye_dnp_nop/王平 && git reset --recurse-submodules --hard HEAD'",
    ]

    for command in commands:
        assert permission_policy.classify_command(command) == "vm_git_dangerous"
        assert permission_policy.get_decision("王平", command) == "DENY"


def test_newline_dangerous_git_still_classifies_dangerous(monkeypatch, tmp_path):
    import tools.permission_policy as permission_policy

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)

    command = "ssh-mini-run\n'cd /home/mini/worktrees/minieye_dnp_nop/王平 && git reset --hard HEAD'"

    assert permission_policy.classify_command(command) == "vm_git_dangerous"
    assert permission_policy.get_decision("王平", command) == "DENY"


def test_split_branch_flags_outside_safe_sequence_are_dangerous(monkeypatch, tmp_path):
    import tools.permission_policy as permission_policy

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)

    commands = [
        "git branch -d foo -f",
        "git branch -f foo -d",
        "git push -fu origin main",
        "git push -uf origin main",
    ]

    for command in commands:
        assert permission_policy.classify_command(command) == "vm_git_dangerous"
        assert permission_policy.get_decision("王平", command) == "DENY"


def test_quoted_or_backslash_dangerous_git_forms_are_dangerous(monkeypatch, tmp_path):
    import tools.permission_policy as permission_policy

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)

    commands = [
        "git 'clean' -fd",
        "git c\\lean -fd",
        "git 'reset' --hard",
        "git push -f\\u origin main",
    ]

    for command in commands:
        assert permission_policy.classify_command(command) == "vm_git_dangerous"
        assert permission_policy.get_decision("王平", command) == "DENY"


def test_routine_git_rejects_absolute_or_tilde_paths(monkeypatch, tmp_path):
    import tools.permission_policy as permission_policy

    config_path = tmp_path / "user-roles.json"
    _write_policy_config(config_path)
    monkeypatch.setenv("HERMES_SESSION_USER_NAME", "王平")
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", config_path)
    monkeypatch.setattr(permission_policy, "_config", None)

    commands = [
        "ssh-mini-run 'cd /home/mini/worktrees/minieye_dnp_nop/王平 && git diff --no-index /etc/passwd /dev/null'",
        "ssh-mini-run 'cd /home/mini/worktrees/minieye_dnp_nop/王平 && git diff --no-index ~/.ssh/id_rsa /dev/null'",
        "ssh-mini-run 'cd /home/mini/worktrees/minieye_dnp_nop/王平 && git add ../other/file.py'",
    ]

    for command in commands:
        assert permission_policy.classify_command(command) == "vm_repo_unauthorized"
        assert permission_policy.get_decision("王平", command) == "DENY"
