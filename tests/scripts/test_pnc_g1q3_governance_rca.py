"""Regression tests for the retired G1Q3-RCA download coordinator."""

import json

import pytest

from hermes_constants import reset_hermes_home_override, set_hermes_home_override
from scripts import pnc_g1q3_governance_rca as gov


def test_download_flag_is_permanently_disabled():
    assert gov.governance_download_enabled({}) is False
    assert gov.governance_download_enabled({gov.FLAG_ENV: "false"}) is False
    assert gov.governance_download_enabled({gov.FLAG_ENV: "1"}) is False
    assert gov.governance_download_enabled({gov.FLAG_ENV: "true"}) is False


def test_command_builder_is_retired():
    with pytest.raises(RuntimeError, match="legacy G1Q3 RCA download coordinator is retired"):
        gov.build_datapipe_bash(
            artifact_root="/mnt/tmp/case_x/",
            execution_request_path="/mnt/tmp/case_x/rca_execution_request.json",
        )


def test_dispatch_boundary_has_no_side_effects(monkeypatch):
    monkeypatch.setattr(
        gov.subprocess,
        "run",
        lambda *_args, **_kwargs: pytest.fail("retired dispatcher touched subprocess"),
    )

    result = gov.dispatch_datapipe(
        task_slug="case_x",
        artifact_root="/mnt/tmp/case_x/",
        execution_request_path="/mnt/tmp/case_x/rca_execution_request.json",
    )

    assert result == {
        "ok": False,
        "stage": "retired",
        "error_code": gov.RETIRED_ERROR_CODE,
        "error": gov.RETIRED_ERROR,
        "side_effects_suppressed": True,
    }


def test_coordinate_boundary_does_not_dispatch(monkeypatch):
    monkeypatch.setattr(
        gov,
        "dispatch_datapipe",
        lambda **_kwargs: pytest.fail("retired coordinator attempted dispatch"),
    )

    result = gov.coordinate({"task_slug": "case_x"})

    assert result["ok"] is False
    assert result["stage"] == "retired"
    assert result["error_code"] == gov.RETIRED_ERROR_CODE
    assert result["side_effects_suppressed"] is True


def test_cli_exits_retired_without_vm_or_task_io(tmp_path, monkeypatch, capsys):
    params_file = tmp_path / "params.json"
    params_file.write_text('{"task_slug":"case_x"}', encoding="utf-8")
    monkeypatch.setattr(
        gov.subprocess,
        "run",
        lambda *_args, **_kwargs: pytest.fail("retired CLI touched subprocess"),
    )

    assert gov.main(["--params-file", str(params_file)]) == 3
    payload = json.loads(capsys.readouterr().out)
    assert payload["error_code"] == gov.RETIRED_ERROR_CODE
    assert payload["side_effects_suppressed"] is True
    assert "mdi download" not in json.dumps(payload).lower()


def test_retired_error_points_to_unified_kafka_and_manual_admission():
    assert "Kafka/manual admission" in gov.RETIRED_ERROR
    assert "pdcl_pyclip remote-read" in gov.RETIRED_ERROR


def test_gateway_compat_dispatcher_is_retired_without_popen(monkeypatch):
    from gateway import run as gateway_run

    monkeypatch.setattr(
        gateway_run.subprocess,
        "Popen",
        lambda *_args, **_kwargs: pytest.fail("retired gateway boundary spawned a process"),
    )

    result = gateway_run._dispatch_governance_datapipe_coordinator(
        task_slug="case_x",
        artifact_root="/mnt/tmp/case_x/",
        artifact_cifs_root="//hfs1/x/",
        execution_request_path="/mnt/tmp/case_x/rca_execution_request.json",
        request_json="{}",
        template_id="rca_issue_intake",
        full_case_id="G1Q3-case-x",
        work_item_id="7041712812",
        source_group_id="oc_x",
        message_id="om_x",
        requester="ou_x",
    )

    assert result["dispatched"] is False
    assert result["error_code"] == gov.RETIRED_ERROR_CODE
    assert result["side_effects_suppressed"] is True


def test_seed_governance_early_card_writes_remote_read_progress(tmp_path):
    token = set_hermes_home_override(tmp_path)
    try:
        result = gov.seed_governance_early_card(
            task_id="20260709-170000-g1q3-rca-issue-intake-7041712812-x",
            chat_id=gov.G1Q3_RCA_CHAT_ID,
            message_id="om_seed",
            artifact_root="/mnt/tmp/g1q3_rca_issue_intake_7041712812_x/",
            artifact_cifs_root=(
                "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/"
                "tmp/g1q3_rca_issue_intake_7041712812_x/"
            ),
            submit_result={
                "success": True,
                "notify_process": {"started": True, "session_id": "probe-1"},
            },
        )
        body = json.loads(
            (
                tmp_path
                / "task-state"
                / "20260709-170000-g1q3-rca-issue-intake-7041712812-x.json"
            ).read_text(encoding="utf-8")
        )
    finally:
        reset_hermes_home_override(token)

    assert result["ok"] is True
    assert body["task_card"]["user_state"] == "pending"
    assert body["task_card"]["one_card_policy"] is True
    assert body["vm_bridge"]["progress"]["message"] == "已受理，远程读取管线启动中"
    assert body["vm_bridge"]["progress"]["phase"] == "dispatched"
    assert body["governance_early_submit"]["notify_process"]["session_id"] == "probe-1"


def test_gateway_early_acceptance_helper_is_permanently_retired():
    from gateway import run as gateway_run

    def fake_submit(**_kwargs):
        pytest.fail("retired helper must not submit")

    def bad_seed(**_kwargs):
        pytest.fail("retired helper must not seed")

    result = gateway_run._create_governance_early_acceptance_card(
        vm_task_submit_func=fake_submit,
        task_id="20260709-170000-g1q3-rca-issue-intake-7041712812-x",
        task_title="title",
        goal="goal",
        requester="ou_x",
        chat_id=gov.G1Q3_RCA_CHAT_ID,
        message_id="om_seed",
        artifact_root="/mnt/tmp/case_x/",
        artifact_cifs_root="//hfs1/x/",
        seed_func=bad_seed,
    )

    assert result["early_submit"]["success"] is False
    assert result["early_submit"]["error_code"] == "g1q3_rca_chat_handoff_retired"
    assert result["early_submit"]["side_effects_suppressed"] is True
    assert result["notify_process"]["started"] is False
    assert result["early_card"]["reason"] == "legacy_rca_handoff_retired"


def test_gateway_early_acceptance_helper_never_calls_injected_functions():
    from gateway import run as gateway_run

    seed_calls = []
    result = gateway_run._create_governance_early_acceptance_card(
        vm_task_submit_func=lambda **_kwargs: pytest.fail("retired helper must not submit"),
        task_id="20260709-170000-g1q3-rca-issue-intake-7041712812-x",
        task_title="title",
        goal="goal",
        requester="ou_x",
        chat_id=gov.G1Q3_RCA_CHAT_ID,
        message_id="om_seed",
        artifact_root="/mnt/tmp/case_x/",
        artifact_cifs_root="//hfs1/x/",
        seed_func=lambda **kwargs: seed_calls.append(kwargs) or pytest.fail("must not seed"),
    )

    assert seed_calls == []
    assert result["early_submit"]["success"] is False
    assert result["early_submit"]["side_effects_suppressed"] is True
    assert result["early_card"]["reason"] == "legacy_rca_handoff_retired"
