"""Regression tests for the G1Q3-RCA governance-path download coordinator.

Decision logic (flag parsing, command/goal builders, follow-up decision) is
tested on the REAL functions (no control-flow stubbing). Only the ssh-mini /
vm_task_submit IO boundaries are substituted in the end-to-end wiring test.
"""
import json
import subprocess

from hermes_constants import reset_hermes_home_override, set_hermes_home_override
from scripts import pnc_g1q3_governance_rca as gov


def _record_only_remote_read_runner(calls):
    def run(cmd, *, timeout):
        calls.append({"cmd": cmd, "timeout": timeout})
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    return run


def test_flag_default_off_and_truthy_variants():
    assert gov.governance_download_enabled({}) is False
    assert gov.governance_download_enabled({"G1Q3_GOVERNANCE_DOWNLOAD_ENABLED": "false"}) is False
    for v in ("1", "true", "yes", "on", "ON", "Yes"):
        assert gov.governance_download_enabled({"G1Q3_GOVERNANCE_DOWNLOAD_ENABLED": v}) is True


def test_datapipe_task_id_is_sanitized_and_stable():
    assert gov.datapipe_task_id("g1q3_rca_issue_intake_7023754183_254d6d") == "g1q3-datapipe-g1q3_rca_issue_intake_7023754183_254d6d"
    assert gov.datapipe_task_id("a b/c;d") == "g1q3-datapipe-abcd"
    assert gov.datapipe_task_id("") == "g1q3-datapipe-unknown"


def test_build_datapipe_bash_runs_pipeline_via_plain_shell_from_s2():
    bash = gov.build_datapipe_bash(
        artifact_root="/mnt/tmp/case_x/",
        execution_request_path="/mnt/tmp/case_x/rca_execution_request.json",
        max_download_gb=20,
    )
    assert "run_rca_auto_pipeline.py" in bash
    assert "--from-stage s2_download" in bash
    assert "--max-download-gb 20" in bash
    assert "/mnt/tmp/case_x/rca_execution_request.json" in bash
    assert "mkdir -p /mnt/tmp/case_x/downloads" in bash
    assert "DATAPIPE_REQUEST_MISSING" not in bash
    # plain shell, NOT codex: no codex invocation in the governance datapipe
    assert "codex" not in bash.lower()


def test_build_datapipe_bash_embeds_request_json_on_vm_local_path():
    bash = gov.build_datapipe_bash(
        artifact_root="/mnt/tmp/case_x/",
        execution_request_path="/mnt/tmp/case_x/rca_execution_request.json",
        request_json='{"schema_version":"g1q3_rca_execution_request_v1"}',
    )
    assert "REQ_B64=" in bash
    assert "DATAPIPE_REQUEST_MISSING" in bash
    assert "path.write_bytes(base64.b64decode" in bash


def test_decide_followup_treats_blocked_pipeline_state_as_not_green():
    out = gov.decide_followup(
        datapipe_exit=0,
        products_present=True,
        pipeline_status="blocked",
        blocker={"kind": "invalid_schema_version"},
    )
    assert out["datapipe"] == "blocked"
    assert out["blocker"]["kind"] == "invalid_schema_version"
    assert "不得假绿" in out["note"]


def test_decide_followup_never_false_greens_on_failure():
    # success + products -> report_ready expected downstream
    ok = gov.decide_followup(datapipe_exit=0, products_present=True)
    assert ok["action"] == "codex_readonly" and ok["datapipe"] == "succeeded"
    # exit 0 but no products -> honest, not completed
    nop = gov.decide_followup(datapipe_exit=0, products_present=False)
    assert nop["datapipe"] == "succeeded_no_products"
    # non-zero exit -> failed, honest need_download (NOT completed/false-green)
    fail = gov.decide_followup(datapipe_exit=1, products_present=False)
    assert fail["datapipe"] == "failed" and "need_download" in fail["note"]
    # missing exit (timeout/none) -> failed
    none = gov.decide_followup(datapipe_exit=None, products_present=False)
    assert none["datapipe"] == "failed"


def test_followup_goal_is_readonly_and_forbids_sandbox_download():
    goal = gov.build_followup_goal(
        template_id="rca_issue_intake", full_case_id="", work_item_id="7023754183",
        source_group_id="oc_x", message_id="om_x", artifact_root="/mnt/tmp/case_x/",
        artifact_cifs_root="//hfs1/x/", execution_request_path="/mnt/tmp/case_x/req.json",
        request_json='{"schema_version":"g1q3_rca_execution_request_v1"}',
        followup=gov.decide_followup(datapipe_exit=1, products_present=False),
    )
    assert "run_rca_execution_request.py" in goal  # read-only runner
    assert "不得在 sandbox 内下载" in goal
    assert "7023754183" in goal
    assert "need_download" in goal  # honest failure surfaced in the goal


def test_remote_read_helpers_use_injected_ssh_wrapper():
    calls = []

    def runner(cmd, *, timeout):
        calls.append({"cmd": cmd, "timeout": timeout})
        command = cmd[1]
        if command.endswith("/exit.code 2>/dev/null || echo __none__"):
            stdout = "0\n"
        elif command.endswith("/pipeline_state.json 2>/dev/null || true"):
            stdout = '{"status":"completed"}\n'
        else:
            stdout = "/mnt/tmp/case_x/index.html\n__present__\n"
        return subprocess.CompletedProcess(cmd, 0, stdout=stdout, stderr="")

    wrapper = "/record-only/ssh-mini-run"
    poll = gov.poll_datapipe_exit(
        task_slug="case_x",
        artifact_root="/mnt/tmp/case_x/",
        timeout_seconds=0,
        ssh_mini_run=wrapper,
        runner=runner,
    )
    state = gov.pipeline_state_verdict(
        artifact_root="/mnt/tmp/case_x/",
        ssh_mini_run=wrapper,
        runner=runner,
    )
    present = gov.products_present(
        artifact_root="/mnt/tmp/case_x/",
        ssh_mini_run=wrapper,
        runner=runner,
    )

    assert poll == {"done": True, "exit": 0}
    assert state == {"status": "completed", "blocker": None}
    assert present is True
    assert len(calls) == 3
    assert all(call["cmd"][0] == wrapper for call in calls)
    assert all(call["timeout"] == 40 for call in calls)


def test_coordinate_failure_path_creates_honest_readonly_task(monkeypatch):
    # Substitute only the IO boundaries; the decision/wiring runs for real.
    monkeypatch.setattr(gov, "dispatch_datapipe", lambda **k: {"ok": True, "task_id": "t"})
    monkeypatch.setattr(gov, "poll_datapipe_exit", lambda **k: {"done": True, "exit": 1})
    monkeypatch.setattr(gov, "products_present", lambda **k: False)
    captured = {}
    remote_calls = []

    def fake_submit(**kwargs):
        captured.update(kwargs)
        return {"success": True, "task": {"task_id": "vm-1"}}

    import tools.vm_task_tool as vtt
    monkeypatch.setattr(vtt, "vm_task_submit", fake_submit)

    result = gov.coordinate({
        "task_slug": "case_x", "artifact_root": "/mnt/tmp/case_x/",
        "execution_request_path": "/mnt/tmp/case_x/req.json",
        "work_item_id": "7023754183", "request_json": "{}",
        "artifact_cifs_root": "//hfs1/x/",
    }, sleep=lambda s: None, ssh_mini_run="/record-only/ssh-mini-run",
        remote_read_runner=_record_only_remote_read_runner(remote_calls))

    assert result["ok"] is True
    assert result["followup"]["datapipe"] == "failed"
    # follow-up is a standard read-only codex task (executor governed_tool), honest goal
    assert captured["executor_type"] == "governed_tool"
    assert captured["agent_backend"] == "codex"
    assert "need_download" in captured["goal"]
    assert "run_rca_execution_request.py" in captured["goal"]
    assert remote_calls == [{
        "cmd": ["/record-only/ssh-mini-run", "cat /mnt/tmp/case_x/pipeline_state.json 2>/dev/null || true"],
        "timeout": 40,
    }]


def test_coordinate_success_path_creates_readonly_task_pointing_at_materialized(monkeypatch):
    monkeypatch.setattr(gov, "dispatch_datapipe", lambda **k: {"ok": True, "task_id": "t"})
    monkeypatch.setattr(gov, "poll_datapipe_exit", lambda **k: {"done": True, "exit": 0})
    monkeypatch.setattr(gov, "products_present", lambda **k: True)
    monkeypatch.setattr(gov, "pipeline_state_verdict", lambda **k: {"status": "completed"})
    captured = {}
    import tools.vm_task_tool as vtt
    monkeypatch.setattr(vtt, "vm_task_submit", lambda **kw: captured.update(kw) or {"success": True})

    result = gov.coordinate({
        "task_slug": "case_x", "artifact_root": "/mnt/tmp/case_x/",
        "execution_request_path": "/mnt/tmp/case_x/req.json",
        "work_item_id": "7023754183", "request_json": "{}",
    }, sleep=lambda s: None)

    assert result["followup"]["datapipe"] == "succeeded"
    assert "report_ready" in captured["goal"]


def test_seed_governance_early_card_writes_pending_progress_sidecar(tmp_path):
    token = set_hermes_home_override(tmp_path)
    try:
        result = gov.seed_governance_early_card(
            task_id="20260709-170000-g1q3-rca-issue-intake-7041712812-x",
            chat_id=gov.G1Q3_RCA_CHAT_ID,
            message_id="om_seed",
            artifact_root="/mnt/tmp/g1q3_rca_issue_intake_7041712812_x/",
            artifact_cifs_root="//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/g1q3_rca_issue_intake_7041712812_x/",
            submit_result={"success": True, "notify_process": {"started": True, "session_id": "probe-1"}},
        )
        body = json.loads((tmp_path / "task-state" / "20260709-170000-g1q3-rca-issue-intake-7041712812-x.json").read_text(encoding="utf-8"))
    finally:
        reset_hermes_home_override(token)

    assert result["ok"] is True
    assert body["task_card"]["user_state"] == "pending"
    assert body["task_card"]["one_card_policy"] is True
    assert body["vm_bridge"]["progress"]["message"] == "已受理，数据管线启动中"
    assert body["vm_bridge"]["progress"]["phase"] == "dispatched"
    assert body["governance_early_submit"]["notify_process"]["session_id"] == "probe-1"


def test_coordinate_existing_early_card_suppresses_second_submit_probe_and_bridge(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    try:
        task_id = "20260709-170000-g1q3-rca-issue-intake-7041712812-x"
        gov.seed_governance_early_card(
            task_id=task_id,
            chat_id=gov.G1Q3_RCA_CHAT_ID,
            message_id="om_seed",
            artifact_root="/mnt/tmp/case_x/",
            artifact_cifs_root="//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/case_x/",
            submit_result={
                "success": True,
                "notify_process": {"started": True, "session_id": "probe-early"},
                "task": {"bridge_delivery": {"path": "inbox/state/early-delivery.json"}},
            },
        )
        sidecar = tmp_path / "task-state" / f"{task_id}.json"
        body = json.loads(sidecar.read_text(encoding="utf-8"))
        body["task_card"]["card_message_id"] = "om_card"
        sidecar.write_text(json.dumps(body, ensure_ascii=False), encoding="utf-8")

        monkeypatch.setattr(gov, "dispatch_datapipe", lambda **k: {"ok": True, "task_id": "datapipe"})
        monkeypatch.setattr(gov, "poll_datapipe_exit", lambda **k: {"done": True, "exit": 0})
        monkeypatch.setattr(gov, "products_present", lambda **k: True)
        monkeypatch.setattr(gov, "pipeline_state_verdict", lambda **k: {"status": "completed"})
        monkeypatch.setattr(gov, "_delivery_from_followup_contract", lambda params, followup: {
            "conclusion": "7041712812 RCA 报告已生成",
            "report_status": "html_delivery_ready",
            "artifact_path": "http://example/report.html",
            "source": "test_contract",
        })
        submit_calls = []
        import tools.vm_task_tool as vtt
        monkeypatch.setattr(vtt, "vm_task_submit", lambda **kw: submit_calls.append(kw) or {"success": True})

        result = gov.coordinate({
            "task_slug": "case_x",
            "followup_task_id": task_id,
            "artifact_root": "/mnt/tmp/case_x/",
            "artifact_cifs_root": "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/case_x/",
            "execution_request_path": "/mnt/tmp/case_x/req.json",
            "work_item_id": "7041712812",
            "request_json": "{}",
        }, sleep=lambda s: None)
        updated = json.loads(sidecar.read_text(encoding="utf-8"))
    finally:
        reset_hermes_home_override(token)

    assert submit_calls == []
    assert result["submit"]["idempotent_update"] is True
    assert result["submit"]["suppressed_deliver_bridge"] is True
    assert result["submit"]["suppressed_completion_probe"] is True
    assert updated["task_card"]["card_message_id"] == "om_card"
    assert updated["governance_early_submit"]["notify_process"]["session_id"] == "probe-early"
    proposal = updated["vm_delivery_proposal"]
    assert proposal["idempotent_update"] is True
    assert proposal["suppressed_deliver_bridge"] is True
    assert proposal["suppressed_completion_probe"] is True
    assert proposal["user_state"] == "done"


def test_coordinate_missing_early_card_falls_back_to_original_submit(monkeypatch):
    monkeypatch.setattr(gov, "dispatch_datapipe", lambda **k: {"ok": True, "task_id": "datapipe"})
    monkeypatch.setattr(gov, "poll_datapipe_exit", lambda **k: {"done": True, "exit": 1})
    monkeypatch.setattr(gov, "products_present", lambda **k: False)
    captured = {}
    remote_calls = []
    import tools.vm_task_tool as vtt
    monkeypatch.setattr(vtt, "vm_task_submit", lambda **kw: captured.update(kw) or {"success": True, "notify_process": {"started": True}})

    result = gov.coordinate({
        "task_slug": "case_missing",
        "followup_task_id": "20260709-170000-g1q3-rca-issue-intake-missing",
        "artifact_root": "/mnt/tmp/case_missing/",
        "execution_request_path": "/mnt/tmp/case_missing/req.json",
        "work_item_id": "7041712812",
        "request_json": "{}",
    }, sleep=lambda s: None, ssh_mini_run="/record-only/ssh-mini-run",
        remote_read_runner=_record_only_remote_read_runner(remote_calls))

    assert result["ok"] is True
    assert captured["task_id"] == "20260709-170000-g1q3-rca-issue-intake-missing"
    assert remote_calls == [{
        "cmd": ["/record-only/ssh-mini-run", "cat /mnt/tmp/case_missing/pipeline_state.json 2>/dev/null || true"],
        "timeout": 40,
    }]


def test_gateway_early_acceptance_helper_fail_safe_on_seed_error():
    from gateway import run as gateway_run

    def fake_submit(**kwargs):
        return {"success": True, "notify_process": {"started": True, "session_id": "probe-1"}}

    def bad_seed(**kwargs):
        raise RuntimeError("seed boom")

    result = gateway_run._create_governance_early_acceptance_card(
        vm_task_submit_func=fake_submit,
        task_id="20260709-170000-g1q3-rca-issue-intake-7041712812-x",
        task_title="title",
        goal="goal",
        requester="ou_x",
        chat_id=gov.G1Q3_RCA_CHAT_ID,
        message_id="om_seed",
        artifact_root="/mnt/tmp/case_x/",
        artifact_cifs_root="//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/case_x/",
        seed_func=bad_seed,
    )

    assert result["early_submit"]["success"] is True
    assert result["notify_process"]["started"] is False
    assert "seed boom" in result["early_error"]


def test_gateway_early_acceptance_helper_does_not_seed_when_submit_fails():
    from gateway import run as gateway_run
    seed_calls = []

    result = gateway_run._create_governance_early_acceptance_card(
        vm_task_submit_func=lambda **kwargs: {"success": False, "error": "permission denied"},
        task_id="20260709-170000-g1q3-rca-issue-intake-7041712812-x",
        task_title="title",
        goal="goal",
        requester="ou_x",
        chat_id=gov.G1Q3_RCA_CHAT_ID,
        message_id="om_seed",
        artifact_root="/mnt/tmp/case_x/",
        artifact_cifs_root="//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/case_x/",
        seed_func=lambda **kwargs: seed_calls.append(kwargs) or {"ok": True},
    )

    assert seed_calls == []
    assert result["early_submit"]["success"] is False
    assert result["early_card"]["reason"] == "early_submit_failed"

def test_datapipe_candidate_baseline_sets_contract_env():
    bash = gov.build_datapipe_bash(
        artifact_root="/mnt/tmp/case_x/",
        execution_request_path="/mnt/tmp/case_x/rca_execution_request.json",
        translate_contract_path="api/g1q3_rca/translate_contract.candidate.json",
    )
    assert 'RCA_TRANSLATE_CONTRACT_PATH="api/g1q3_rca/translate_contract.candidate.json"' in bash
    assert "run_rca_auto_pipeline.py" in bash


def test_followup_goal_includes_translate_baseline():
    goal = gov.build_followup_goal(
        template_id="rca_issue_intake", full_case_id="", work_item_id="7023754183",
        source_group_id="oc_x", message_id="om_x", artifact_root="/mnt/tmp/case_x/",
        artifact_cifs_root="//hfs1/x/", execution_request_path="/mnt/tmp/case_x/req.json",
        request_json='{"schema_version":"g1q3_rca_execution_request_v1"}',
        followup=gov.decide_followup(datapipe_exit=1, products_present=False),
        translate_baseline="candidate",
        translate_contract_path="api/g1q3_rca/translate_contract.candidate.json",
    )
    assert "translate_baseline: candidate" in goal
    assert "translate_contract_path: api/g1q3_rca/translate_contract.candidate.json" in goal
