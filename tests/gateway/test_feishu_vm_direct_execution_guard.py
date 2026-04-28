"""Tests for Feishu VM direct-execution guardrails."""

import sys
from pathlib import Path

_repo = str(Path(__file__).resolve().parents[2])
if _repo not in sys.path:
    sys.path.insert(0, _repo)

from gateway.session_context import clear_session_vars, set_session_vars
from tools.approval import check_all_command_guards, detect_dangerous_command


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


def test_member_cannot_directly_execute_vm_write(monkeypatch):
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
            "ssh-mini-run 'cd /home/mini/minieye_dnp_nop && python3 run_eval.py'",
            "local",
        )
    finally:
        clear_session_vars(tokens)

    assert result["approved"] is False
    assert "权限不足" in result["message"]


def test_owner_can_direct_vm_write_without_member_gate(monkeypatch):
    monkeypatch.setenv("HERMES_GATEWAY_SESSION", "1")
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
