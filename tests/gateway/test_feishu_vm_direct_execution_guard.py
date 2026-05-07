"""Tests for Feishu VM direct-execution guardrails."""

import json
import sys
from pathlib import Path

import pytest

_repo = str(Path(__file__).resolve().parents[2])
if _repo not in sys.path:
    sys.path.insert(0, _repo)

from gateway.session_context import clear_session_vars, set_session_vars
from tools.approval import check_all_command_guards, detect_dangerous_command


@pytest.fixture(autouse=True)
def _stable_permission_policy(monkeypatch):
    """Keep guard tests independent of the operator's live user-role config.

    The production permission policy intentionally reads ~/.hermes config, but
    these tests exercise guard behavior for fixed identities.  Pin the policy
    helpers so a local ACL rollout (for example granting 王平 repo read) does not
    rewrite the expected safety semantics.
    """
    import tools.permission_policy as permission_policy

    command_classifications = {
        "~/.local/bin/ssh-mini-agent read_file /home/mini/worktrees/minieye_dnp_nop/王平/README.md --start 1 --lines 5": "vm_repo_unauthorized",
        "ssh-mini-run 'cd /mnt/tmp/eval_job && python3 run_eval.py --input data.json'": "write",
        "ssh-mini-run 'cd /home/mini/worktrees/minieye_dnp_nop/王平 && python3 run_eval.py'": "vm_repo_unauthorized",
        "ssh-mini-run 'cd /home/mini/minieye_dnp_nop && python3 run_eval.py'": "vm_direct_exec",
        "ssh-mini-run 'python3 /tmp/eval.py'": "vm_direct_exec",
        "~/.local/bin/ssh-mini-agent run_bash_json < /tmp/eval.sh": "vm_direct_exec",
        "ssh mini@host 'cd /home/mini/minieye_dnp_nop && python3 run_eval.py'": "vm_direct_exec",
        "git push --force origin main": "vm_git_dangerous",
    }
    user_roles = {
        "ou_d1d3cfeba1be0a22faa36aaf4fb3907d": "owner",
        "ou_e3e6da5b6814a5c5d7e8ccf21cfbea0a": "member",
    }

    def classify_command(command):
        return command_classifications.get(command, "write")

    def get_decision_by_id(user_id, command):
        role = user_roles.get(user_id, "member")
        op_type = classify_command(command)
        if op_type in {"vm_repo_unauthorized", "vm_git_dangerous"}:
            return "DENY"
        if op_type == "vm_direct_exec" and role != "owner":
            return "DENY"
        return "ALLOW"

    monkeypatch.setattr(permission_policy, "classify_command", classify_command)
    monkeypatch.setattr(permission_policy, "get_decision_by_id", get_decision_by_id)
    monkeypatch.setattr(permission_policy, "get_user_role", lambda name: "owner" if name == "胡子豪" else "member")


def test_ssh_mini_agent_repo_read_is_policy_guarded_for_gateway_member(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_GATEWAY_SESSION", "1")
    monkeypatch.setenv("HERMES_REPO_ACL_APPROVAL_DIR", str(tmp_path))
    tokens = set_session_vars(
        platform="feishu",
        chat_id="oc_test",
        thread_id="omt_repo_acl",
        user_id="ou_e3e6da5b6814a5c5d7e8ccf21cfbea0a",
        user_name="王平",
        session_key="agent:main:feishu:group:oc_test_member_repo_read",
    )
    try:
        result = check_all_command_guards(
            "~/.local/bin/ssh-mini-agent read_file /home/mini/worktrees/minieye_dnp_nop/王平/README.md --start 1 --lines 5",
            "local",
        )
    finally:
        clear_session_vars(tokens)

    assert result["approved"] is False
    assert "权限不足" in result["message"]
    assert result["status"] == "repo_acl_approval_pending"
    request = result["repo_acl_request"]
    assert request["repo"] == "minieye_dnp_nop"
    assert request["requested_grant"] == "read"
    assert request["requester"]["display_name"] == "王平"
    assert request["delivery"]["chat_id"] == "oc_test"
    assert request["delivery"]["thread_id"] == "omt_repo_acl"
    assert request["apply"]["auto_apply"] is False
    assert request["request_context"]["command"].startswith("~/.local/bin/ssh-mini-agent read_file")
    card = result["repo_acl_approval_card"]
    assert card["header"]["title"]["content"] == "Repo 权限审批"
    assert request["request_id"] in str(card)
    outbox = json.loads((tmp_path / "repo-acl-approval-outbox.json").read_text(encoding="utf-8"))
    envelope = outbox[request["request_id"]]
    assert envelope["send_mode"] == "dry_run"
    assert envelope["sent"] is False
    assert envelope["delivery"] == {"platform": "feishu", "chat_id": "oc_test", "thread_id": "omt_repo_acl"}
    assert envelope["card"] == card
    assert envelope["safety"]["live_send"] is False
    assert envelope["safety"]["auto_apply"] is False


def test_repo_acl_denial_without_parseable_repo_does_not_create_request(monkeypatch, tmp_path):
    import tools.permission_policy as permission_policy

    monkeypatch.setenv("HERMES_GATEWAY_SESSION", "1")
    monkeypatch.setenv("HERMES_REPO_ACL_APPROVAL_DIR", str(tmp_path))
    monkeypatch.setattr(permission_policy, "classify_command", lambda _command: "vm_repo_unauthorized")
    monkeypatch.setattr(permission_policy, "get_decision_by_id", lambda _user_id, _command: "DENY")
    tokens = set_session_vars(
        platform="feishu",
        chat_id="oc_test",
        user_id="ou_e3e6da5b6814a5c5d7e8ccf21cfbea0a",
        user_name="王平",
        session_key="agent:main:feishu:group:oc_test_member_repo_read_unparseable",
    )
    try:
        result = check_all_command_guards(
            "~/.local/bin/ssh-mini-agent read_file /mnt/tmp/report.md --start 1 --lines 5",
            "local",
        )
    finally:
        clear_session_vars(tokens)

    assert result["approved"] is False
    assert "权限不足" in result["message"]
    assert "repo_acl_request" not in result
    assert "repo_acl_approval_card" not in result


def test_ssh_mini_agent_write_is_guarded_for_gateway_vm_tasks():
    command = "~/.local/bin/ssh-mini-agent run_bash_json < /tmp/eval.sh"

    is_dangerous, _key, description = detect_dangerous_command(command)

    assert is_dangerous is True
    assert "VM direct execution" in description


def test_ssh_mini_agent_read_remains_unblocked():
    command = "~/.local/bin/ssh-mini-agent list_files /home/mini/minieye_dnp_nop --max 20"

    is_dangerous, _key, _description = detect_dangerous_command(command)

    assert is_dangerous is False


def test_ssh_mini_raw_remote_write_is_guarded():
    command = "ssh-mini-run 'cd /home/mini/minieye_dnp_nop && python3 run_eval.py'"

    is_dangerous, _key, description = detect_dangerous_command(command)

    assert is_dangerous is True
    assert "VM direct execution" in description


def test_member_non_source_vm_task_command_is_not_blocked_by_repo_acl(monkeypatch):
    monkeypatch.setenv("HERMES_GATEWAY_SESSION", "1")
    monkeypatch.setenv("HERMES_EXEC_ASK", "1")
    monkeypatch.delenv("HERMES_YOLO_MODE", raising=False)
    tokens = set_session_vars(
        platform="feishu",
        chat_id="oc_test",
        user_id="ou_e3e6da5b6814a5c5d7e8ccf21cfbea0a",  # 王平 / member
        user_name="王平",
        session_key="agent:main:feishu:group:oc_test_non_source_vm_task",
    )
    try:
        result = check_all_command_guards(
            "ssh-mini-run 'cd /mnt/tmp/eval_job && python3 run_eval.py --input data.json'",
            "local",
        )
    finally:
        clear_session_vars(tokens)

    assert result["approved"] is True
    assert "repo_acl_request" not in result


def test_member_direct_vm_command_in_repo_is_source_guarded(monkeypatch):
    monkeypatch.setenv("HERMES_GATEWAY_SESSION", "1")
    monkeypatch.setenv("HERMES_EXEC_ASK", "1")
    monkeypatch.delenv("HERMES_YOLO_MODE", raising=False)
    tokens = set_session_vars(
        platform="feishu",
        chat_id="oc_test",
        user_id="ou_e3e6da5b6814a5c5d7e8ccf21cfbea0a",  # 王平 / member
        user_name="王平",
        session_key="agent:main:feishu:group:oc_test",
    )
    try:
        result = check_all_command_guards(
            "ssh-mini-run 'cd /home/mini/worktrees/minieye_dnp_nop/王平 && python3 run_eval.py'",
            "local",
        )
    finally:
        clear_session_vars(tokens)

    assert result["approved"] is False
    assert "权限不足" in result["message"]


def test_owner_business_vm_write_in_main_repo_is_source_guarded(monkeypatch):
    monkeypatch.setenv("HERMES_GATEWAY_SESSION", "1")
    monkeypatch.delenv("HERMES_YOLO_MODE", raising=False)
    monkeypatch.setenv("HERMES_VM_DIRECT_EXEC_EMERGENCY", "1")
    tokens = set_session_vars(
        platform="feishu",
        chat_id="oc_test",
        user_id="ou_d1d3cfeba1be0a22faa36aaf4fb3907d",  # 胡子豪 / owner
        user_name="胡子豪",
        session_key="agent:main:feishu:group:oc_test_owner",
    )
    try:
        result = check_all_command_guards(
            "ssh-mini-run 'cd /home/mini/minieye_dnp_nop && python3 run_eval.py'",
            "local",
        )
    finally:
        clear_session_vars(tokens)

    assert result["approved"] is True


def test_vm_direct_exec_yolo_requires_emergency_override(monkeypatch):
    monkeypatch.setenv("HERMES_GATEWAY_SESSION", "1")
    monkeypatch.setenv("HERMES_YOLO_MODE", "1")
    monkeypatch.delenv("HERMES_VM_DIRECT_EXEC_EMERGENCY", raising=False)
    tokens = set_session_vars(
        platform="feishu",
        chat_id="oc_test",
        user_id="ou_d1d3cfeba1be0a22faa36aaf4fb3907d",
        user_name="胡子豪",
        session_key="agent:main:feishu:group:oc_test_owner_yolo",
    )
    try:
        result = check_all_command_guards(
            "ssh-mini-run 'cd /home/mini/minieye_dnp_nop && python3 run_eval.py'",
            "local",
        )
    finally:
        clear_session_vars(tokens)

    assert result["approved"] is False
    assert "emergency override" in result["message"]


def test_vm_direct_exec_yolo_blocks_all_direct_vm_variants_without_emergency(monkeypatch):
    monkeypatch.setenv("HERMES_GATEWAY_SESSION", "1")
    monkeypatch.setenv("HERMES_YOLO_MODE", "1")
    monkeypatch.delenv("HERMES_VM_DIRECT_EXEC_EMERGENCY", raising=False)
    tokens = set_session_vars(
        platform="feishu",
        chat_id="oc_test",
        user_id="ou_d1d3cfeba1be0a22faa36aaf4fb3907d",
        user_name="胡子豪",
        session_key="agent:main:feishu:group:oc_test_owner_yolo_variants",
    )
    commands = [
        "~/.local/bin/ssh-mini-agent run_bash_json < /tmp/eval.sh",
        "ssh mini@host 'cd /home/mini/minieye_dnp_nop && python3 run_eval.py'",
        "ssh-mini-run 'python3 /tmp/eval.py'",
    ]
    try:
        results = [check_all_command_guards(command, "local") for command in commands]
    finally:
        clear_session_vars(tokens)

    assert all(result["approved"] is False for result in results)
    assert all("emergency override" in result["message"] for result in results)


def test_vm_direct_exec_session_yolo_blocks_all_direct_vm_variants_without_emergency(monkeypatch):
    from tools.approval import clear_session, enable_session_yolo

    monkeypatch.setenv("HERMES_GATEWAY_SESSION", "1")
    monkeypatch.delenv("HERMES_YOLO_MODE", raising=False)
    monkeypatch.delenv("HERMES_VM_DIRECT_EXEC_EMERGENCY", raising=False)
    session_key = "agent:main:feishu:group:oc_test_owner_session_yolo_variants"
    tokens = set_session_vars(
        platform="feishu",
        chat_id="oc_test",
        user_id="ou_d1d3cfeba1be0a22faa36aaf4fb3907d",
        user_name="胡子豪",
        session_key=session_key,
    )
    commands = [
        "~/.local/bin/ssh-mini-agent run_bash_json < /tmp/eval.sh",
        "ssh mini@host 'cd /home/mini/minieye_dnp_nop && python3 run_eval.py'",
        "ssh-mini-run 'python3 /tmp/eval.py'",
    ]
    try:
        enable_session_yolo(session_key)
        results = [check_all_command_guards(command, "local") for command in commands]
    finally:
        clear_session(session_key)
        clear_session_vars(tokens)

    assert all(result["approved"] is False for result in results)
    assert all("emergency override" in result["message"] for result in results)


def test_vm_direct_exec_yolo_allows_with_emergency_override(monkeypatch):
    monkeypatch.setenv("HERMES_GATEWAY_SESSION", "1")
    monkeypatch.setenv("HERMES_YOLO_MODE", "1")
    monkeypatch.setenv("HERMES_VM_DIRECT_EXEC_EMERGENCY", "1")
    tokens = set_session_vars(
        platform="feishu",
        chat_id="oc_test",
        user_id="ou_d1d3cfeba1be0a22faa36aaf4fb3907d",
        user_name="胡子豪",
        session_key="agent:main:feishu:group:oc_test_owner_yolo_emergency",
    )
    try:
        result = check_all_command_guards(
            "ssh-mini-run 'cd /home/mini/minieye_dnp_nop && python3 run_eval.py'",
            "local",
        )
    finally:
        clear_session_vars(tokens)

    assert result["approved"] is True


def test_vm_direct_exec_yolo_fails_closed_when_permission_config_unavailable(monkeypatch):
    """VM direct execution must not fail open if user-role config cannot load."""
    import tools.permission_policy as permission_policy

    monkeypatch.setenv("HERMES_GATEWAY_SESSION", "1")
    monkeypatch.setenv("HERMES_YOLO_MODE", "1")
    monkeypatch.delenv("HERMES_VM_DIRECT_EXEC_EMERGENCY", raising=False)
    monkeypatch.setattr(permission_policy, "_config", None)
    monkeypatch.setattr(permission_policy, "_CONFIG_PATH", Path("/missing/user-roles.json"))
    tokens = set_session_vars(
        platform="feishu",
        chat_id="oc_test",
        user_id="ou_d1d3cfeba1be0a22faa36aaf4fb3907d",
        user_name="胡子豪",
        session_key="agent:main:feishu:group:oc_test_owner_missing_config",
    )
    try:
        result = check_all_command_guards(
            "ssh-mini-run 'cd /home/mini/minieye_dnp_nop && python3 run_eval.py'",
            "local",
        )
    finally:
        clear_session_vars(tokens)

    assert result["approved"] is False
    assert "emergency override" in result["message"]


def test_gateway_generic_dangerous_deny_does_not_create_repo_acl_request(monkeypatch, tmp_path):
    import tools.approval as approval
    import tools.permission_policy as permission_policy

    monkeypatch.setenv("HERMES_GATEWAY_SESSION", "1")
    monkeypatch.setenv("HERMES_EXEC_ASK", "1")
    monkeypatch.setenv("HERMES_REPO_ACL_APPROVAL_DIR", str(tmp_path))
    monkeypatch.delenv("HERMES_YOLO_MODE", raising=False)
    monkeypatch.setattr(permission_policy, "classify_command", lambda _command: "vm_git_dangerous")
    monkeypatch.setattr(permission_policy, "get_decision_by_id", lambda _user_id, _command: "DENY")
    with approval._lock:
        approval._pending.clear()
        approval._session_approved.clear()
        approval._session_yolo.clear()
        approval._gateway_notify_cbs.clear()
        approval._gateway_timeout_cbs.clear()
        approval._gateway_queues.clear()
    tokens = set_session_vars(
        platform="feishu",
        chat_id="oc_test",
        user_id="ou_e3e6da5b6814a5c5d7e8ccf21cfbea0a",
        user_name="王平",
        session_key="agent:main:feishu:group:generic-danger-check-all",
    )
    try:
        result = check_all_command_guards("git push --force origin main", "local")
    finally:
        clear_session_vars(tokens)
        with approval._lock:
            approval._pending.clear()
            approval._session_approved.clear()
            approval._session_yolo.clear()
            approval._gateway_notify_cbs.clear()
            approval._gateway_timeout_cbs.clear()
            approval._gateway_queues.clear()

    assert result["approved"] is False
    assert result.get("description") != "VM repository access denied by repo ACL policy"
    assert result.get("description") == "Permission policy denied this command"
    assert "repo_acl_request" not in result
    assert "repo_acl_approval_card" not in result
    assert result.get("status") != "repo_acl_approval_pending"


def test_member_with_emergency_override_and_yolo_still_cannot_direct_vm_write(monkeypatch):
    monkeypatch.setenv("HERMES_GATEWAY_SESSION", "1")
    monkeypatch.setenv("HERMES_YOLO_MODE", "1")
    monkeypatch.setenv("HERMES_VM_DIRECT_EXEC_EMERGENCY", "1")
    tokens = set_session_vars(
        platform="feishu",
        chat_id="oc_test",
        user_id="ou_e3e6da5b6814a5c5d7e8ccf21cfbea0a",  # 王平 / member
        user_name="王平",
        session_key="agent:main:feishu:group:oc_test_member_emergency_yolo",
    )
    try:
        result = check_all_command_guards(
            "ssh-mini-run 'cd /home/mini/minieye_dnp_nop && python3 run_eval.py'",
            "local",
        )
    finally:
        clear_session_vars(tokens)

    assert result["approved"] is False
    assert "权限不足" in result["message"]


def test_owner_can_emergency_override_vm_write(monkeypatch):
    monkeypatch.setenv("HERMES_GATEWAY_SESSION", "1")
    monkeypatch.setenv("HERMES_VM_DIRECT_EXEC_EMERGENCY", "1")
    monkeypatch.delenv("HERMES_YOLO_MODE", raising=False)
    tokens = set_session_vars(
        platform="feishu",
        chat_id="oc_test",
        user_id="ou_d1d3cfeba1be0a22faa36aaf4fb3907d",  # 胡子豪 / owner
        user_name="胡子豪",
        session_key="agent:main:feishu:group:oc_test_owner",
    )
    try:
        result = check_all_command_guards(
            "ssh-mini-run 'cd /home/mini/minieye_dnp_nop && python3 run_eval.py'",
            "local",
        )
    finally:
        clear_session_vars(tokens)

    assert result["approved"] is True
