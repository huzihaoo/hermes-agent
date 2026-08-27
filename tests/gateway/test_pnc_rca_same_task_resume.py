import hashlib
import json
from types import SimpleNamespace

from gateway import pnc_rca_same_task_resume as resume


def _claim(**updates):
    values = {
        "task_id": resume.AUTHORIZED_TASK_ID,
        "submission_key": resume.AUTHORIZED_TASK_ID,
        "business_key": resume.AUTHORIZED_BUSINESS_KEY,
        "generation": resume.AUTHORIZED_GENERATION,
        "work_item_id": resume.AUTHORIZED_ISSUE_ID,
    }
    values.update(updates)
    return SimpleNamespace(**values)


def _preflight():
    goal = "Run exact generation 7 task.\n"
    return {
        "schema_version": resume.VM_PREFLIGHT_SCHEMA_VERSION,
        "ok": True,
        "task_id": resume.AUTHORIZED_TASK_ID,
        "submission_key": resume.AUTHORIZED_TASK_ID,
        "business_key": resume.AUTHORIZED_BUSINESS_KEY,
        "generation": resume.AUTHORIZED_GENERATION,
        "issue_id": resume.AUTHORIZED_ISSUE_ID,
        "goal_text": goal,
        "goal_sha256": hashlib.sha256(goal.encode()).hexdigest(),
        "stable_bindings": {
            "contract_sha256": "11" * 32,
            "reservation_id": "reservation-1",
            "reservation_fence": "1",
            "reservation_contract_sha256": "22" * 32,
        },
        "preflight_fingerprint": "33" * 32,
        "target_runtime_root": resume.RCA_PROD_VM_RELEASE_ROOT,
        "target_cli_sha256": "44" * 32,
    }


def _admission_meta():
    return {
        "resource_class": "rca_prod",
        "lane": "heavy",
        "queue_if_blocked": False,
        "resource_gate_bypass": False,
        "rca_prod_capacity_mode": "steady",
        "rca_prod_attempt_id": "attempt-fresh",
        "reservation_id": "reservation-1",
        "reservation_fence": "1",
        "reservation_contract_sha256": "22" * 32,
        "rca_prod_goal_sha256": _preflight()["goal_sha256"],
        "rca_prod_command_sha256": "55" * 32,
        "rca_prod_contract_sha256": "11" * 32,
        "rca_prod_admission_receipt": {"receipt_fingerprint": "66" * 32},
        "rca_prod_admission_key_fingerprint": "77" * 32,
    }


def _applied():
    return {
        "schema_version": resume.VM_RECEIPT_SCHEMA_VERSION,
        "success": True,
        "status": "applied",
        "task_id": resume.AUTHORIZED_TASK_ID,
        "submission_key": resume.AUTHORIZED_TASK_ID,
        "business_key": resume.AUTHORIZED_BUSINESS_KEY,
        "generation": resume.AUTHORIZED_GENERATION,
        "issue_id": resume.AUTHORIZED_ISSUE_ID,
        "blocker_kind": resume.SUPPORTED_BLOCKER,
        "operation": resume.SUPPORTED_OPERATION,
        "resumed_same_task": True,
        "business_external_writes": False,
        "created_task_ids": [],
        "target_runtime_root": resume.RCA_PROD_VM_RELEASE_ROOT,
    }


def test_authorized_resume_uses_one_preflight_and_one_apply():
    calls = []

    def remote(action, payload, timeout_seconds):
        calls.append((action, dict(payload), timeout_seconds))
        return _preflight() if action == "preflight" else _applied()

    issuer_calls = []

    def issuer(**kwargs):
        issuer_calls.append(kwargs)
        return SimpleNamespace(meta=_admission_meta())

    result = resume.resume_same_task(
        _claim(),
        {"kind": resume.SUPPORTED_BLOCKER},
        {"op": resume.SUPPORTED_OPERATION, "resume_from_stage": "s2_remote_read"},
        90,
        remote=remote,
        issuer=issuer,
    )

    assert result["success"] is True
    assert result["resumed_same_task"] is True
    assert result["external_writes"] is False
    assert [item[0] for item in calls] == ["preflight", "apply"]
    request = calls[1][1]
    assert request["task_id"] == resume.AUTHORIZED_TASK_ID
    assert request["generation"] == 7
    assert request["target_runtime_root"].endswith("r15c13b")
    assert len(issuer_calls) == 1
    assert issuer_calls[0]["submission_key"] == resume.AUTHORIZED_TASK_ID


def test_other_issue_is_held_without_remote_or_admission_calls():
    def unexpected(*_args, **_kwargs):
        raise AssertionError("out-of-scope claim must not execute")

    result = resume.resume_same_task(
        _claim(work_item_id="other-issue"),
        {"kind": resume.SUPPORTED_BLOCKER},
        {"op": resume.SUPPORTED_OPERATION},
        90,
        remote=unexpected,
        issuer=unexpected,
    )

    assert result["success"] is False
    assert result["status"] == "held"
    assert result["error_code"] == "infra_remediation_scope_not_authorized"


def test_apply_cannot_report_a_second_created_task():
    def remote(action, _payload, _timeout_seconds):
        if action == "preflight":
            return _preflight()
        value = _applied()
        value["created_task_ids"] = ["generation-8-task"]
        return value

    result = resume.resume_same_task(
        _claim(),
        {"kind": resume.SUPPORTED_BLOCKER},
        {"op": resume.SUPPORTED_OPERATION},
        90,
        remote=remote,
        issuer=lambda **_kwargs: SimpleNamespace(meta=_admission_meta()),
    )

    assert result["success"] is False
    assert result["error_code"] == "remote_resume_apply_invalid"


def test_remote_call_does_not_forward_host_admission_secret(monkeypatch):
    observed = {}

    def run_func(command, **kwargs):
        observed["command"] = command
        observed["env"] = kwargs["env"]
        observed["input"] = kwargs["input"]
        return SimpleNamespace(returncode=0, stdout=json.dumps({"ok": True}), stderr="")

    monkeypatch.setenv("HERMES_RCA_PROD_ADMISSION_HMAC_KEY", "hex:" + "aa" * 32)
    result = resume.remote_call(
        "preflight",
        {"task_id": resume.AUTHORIZED_TASK_ID},
        10,
        run_func=run_func,
    )

    assert result == {"ok": True}
    assert observed["command"][-1] == "run_py_json"
    assert "HERMES_RCA_PROD_ADMISSION_HMAC_KEY" not in observed["env"]
    assert str(resume.VM_TOOL_PATH) in observed["input"]
