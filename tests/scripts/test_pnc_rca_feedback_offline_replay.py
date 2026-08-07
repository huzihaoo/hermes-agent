from __future__ import annotations

import json
from pathlib import Path

import pytest

from gateway.pnc_rca_issue_focus import (
    ANALYSIS_CAPABILITY_UNSUPPORTED,
    ISSUE_FOCUS_EVIDENCE_SCHEMA_VERSION,
    issue_title_sha256,
    resolve_issue_intent,
)
from scripts.pnc_rca_feedback_offline_replay import (
    SCHEMA_VERSION,
    OfflineReplayGateError,
    build_offline_replay_report,
    sha256_file,
    validate_result_field_two_lines,
    write_offline_replay_report,
)


TITLES = (
    ("7058500122", "HMI-S弯"),
    ("7058503076", "ACC-前车切入，跟停前前车，自车加速后刹停"),
    ("7058246921", "ACC-同车道环卫工人推着垃圾桶，自车不刹车，驾驶员接管"),
    ("7057685645", "AEB-AEB触发仪表无双闪"),
    ("7057689072", "ACC-前方仪表无目标，制动"),
    ("7057490076", "HMI-左侧车模出现先闪一下在稳定"),
    ("7057374059", "ACC-进入匝道减速过大"),
    ("7057608102", "ACC-ACC设定70，跟随前车，刹车晚，二次起步"),
    (
        "7056819193",
        "ACC-大货车向右切出后停在车道线中间，先识别到大货车模型，自车开始减速，后模型释放，自车未减速跟停",
    ),
)


def _zero_readback_side_effects() -> dict:
    return {
        "feishu_writes": 0,
        "comment_writes": 0,
        "field_writes": 0,
        "workflow_writes": 0,
        "workhour_writes": 0,
        "control_db_writes": 0,
    }


def _zero_census_side_effects() -> dict:
    return {
        "feishu_writes": 0,
        "comment_writes": 0,
        "field_writes": 0,
        "workflow_writes": 0,
        "workhour_writes": 0,
        "production_db_writes": 0,
        "network_writes": 0,
        "candidate_code_writes": 0,
    }


def _item(case_id: str, title: str, *, terminal: bool = False) -> dict:
    marker = (
        "RCA_TERMINAL:g1q3-terminal[terminal_failed]1"
        if terminal
        else "RCA_DELIVERY:g1q3-effect:abcd"
    )
    return {
        "work_item_id": case_id,
        "name": title,
        # Historical field_9193cb values frequently carried causal/evidence
        # paragraphs; the new public projection must reject those extra lines.
        "fields": {
            "field_9193cb": "归因结论：旧结论\n责任模块：旧模块\n关键证据：旧证据"
        },
        "comments": {"items": [{"marker": marker}]},
        "control_readonly": {
            "execution_watch": {
                "generation": 1,
                "state": "delivery_created",
                "last_error_code": "vm_terminal_failed_unclassified" if terminal else "",
            },
            "delivery_job": None
            if terminal
            else {
                "generation": 1,
                "status": "delivered",
                "outcome": "success",
            },
        },
    }


def _readback(*, terminal_id: str = "7056819193") -> dict:
    return {
        "schema_version": "g1q3_feedback_issue_readonly_readback_v1",
        "observed_at": "2026-08-07T03:08:54+00:00",
        "read_only": True,
        "side_effects": _zero_readback_side_effects(),
        "items": [
            _item(case_id, title, terminal=case_id == terminal_id)
            for case_id, title in TITLES
        ],
    }


def _census() -> dict:
    return {
        "schema_version": "g1q3_feedback_issue_intent_census_v2",
        "observed_side_effects": _zero_census_side_effects(),
        "cases": [
            {
                "work_item_id": case_id,
                "intent_status": (
                    "capability_gate"
                    if case_id == "7057685645"
                    else "evidence_required"
                ),
                "report_data": {
                    "availability": "http_hash_verified"
                    if index < 3
                    else "declared_hash_only_path_absent",
                    "sha256": f"{'a' * 63}{index:x}" if index < 8 else None,
                    "size": 100 + index if index < 8 else None,
                    "issue_focus_present": False,
                },
            }
            for index, (case_id, _title) in enumerate(TITLES)
        ],
    }


def test_result_field_gate_requires_exact_public_two_line_projection():
    assert validate_result_field_two_lines("归因结论：候选\n责任模块：感知")["valid"]
    assert validate_result_field_two_lines("归因结论：候选\n责任模块：感知\n证据：x")["code"] == (
        "result_field_line_count_invalid"
    )
    assert validate_result_field_two_lines("责任模块：感知\n归因结论：候选")["code"] == (
        "result_field_label_invalid"
    )


def test_nine_case_gate_is_fail_closed_and_has_zero_external_side_effects():
    report = build_offline_replay_report(
        _readback(), census=_census(), generated_at="2026-08-07T06:30:00+00:00"
    )

    assert report["schema_version"] == SCHEMA_VERSION
    assert report["coverage"]["items"] == 9
    assert report["coverage"]["statement_insufficient"] == 1
    assert report["coverage"]["capability_gate"] == 1
    assert report["coverage"]["generation2_rerun_required"] == 1
    assert report["coverage"]["focus_evidence_missing"] == 6
    assert report["coverage"]["result_field_two_line_failures"] == 9
    assert report["full_replay_ready"] is False
    assert report["replay_gate"]["status"] == "blocked"
    assert "historical_report_input_incomplete" in report["replay_gate"]["blockers"]
    assert report["replay_gate"]["external_side_effects"] == 0
    assert report["side_effect_gate"]["passed"] is True
    assert all(value == 0 for value in report["side_effect_gate"]["counts"].values())

    by_id = {case["work_item_id"]: case for case in report["cases"]}
    assert by_id["7058500122"]["focus_status"] == "insufficient_statement"
    assert by_id["7057685645"]["focus_status"] == "capability_unsupported"
    assert by_id["7056819193"]["replay_status"] == "generation2_required"
    assert "detection_pipeline" in by_id["7057689072"]["required_focus"]["capabilities"]


def test_capability_stop_requires_a_bound_census_audit_status():
    title = "AEB-AEB触发仪表无双闪"
    readback = {
        "schema_version": "g1q3_feedback_issue_readonly_readback_v1",
        "read_only": True,
        "side_effects": _zero_readback_side_effects(),
        "items": [_item("hazard", title)],
    }

    report = build_offline_replay_report(readback)

    assert report["cases"][0]["focus_status"] == "focus_evidence_missing"
    assert report["coverage"]["capability_gate"] == 0


def test_side_effect_nonzero_is_a_blocking_observation():
    readback = _readback()
    readback["side_effects"]["field_writes"] = 1

    report = build_offline_replay_report(readback, census=_census())

    assert report["side_effect_gate"]["passed"] is False
    assert report["replay_gate"]["external_side_effects"] == 1
    assert "external_side_effect_observed" in report["replay_gate"]["blockers"]


@pytest.mark.parametrize("invalid", [None, 0.5, float("nan"), "0.5"])
def test_side_effect_fractional_or_null_counts_fail_closed(invalid):
    readback = _readback()
    readback["side_effects"]["field_writes"] = invalid

    with pytest.raises(OfflineReplayGateError) as raised:
        build_offline_replay_report(readback, census=_census())

    assert raised.value.code == "offline_replay_side_effect_count_invalid"


def test_missing_side_effect_count_is_rejected_instead_of_assumed_zero():
    readback = _readback()
    del readback["side_effects"]["control_db_writes"]

    with pytest.raises(OfflineReplayGateError) as raised:
        build_offline_replay_report(readback, census=_census())

    assert raised.value.code == "offline_replay_side_effect_contract_incomplete"


def test_focus_payload_is_revalidated_when_census_declares_it_present():
    title = "AEB-AEB触发仪表无双闪"
    intent = resolve_issue_intent(title)
    focus_payload = {
        "schema_version": ISSUE_FOCUS_EVIDENCE_SCHEMA_VERSION,
        "issue_intent": intent.to_dict(),
        "title_sha256": issue_title_sha256(title),
        "analysis_status": ANALYSIS_CAPABILITY_UNSUPPORTED,
        "capabilities": [],
        "segments": [],
        "entities": [],
        "measurements": [],
        "checks": [],
        "calculations": [],
        "missing_requirements": ["capability:vehicle_signal_chain"],
        "unsupported_capabilities": ["vehicle_signal_chain"],
        "stop_reason": "当前候选版本没有双闪请求到仪表反馈的闭环能力。",
    }
    readback = {
        "schema_version": "g1q3_feedback_issue_readonly_readback_v1",
        "read_only": True,
        "side_effects": _zero_readback_side_effects(),
        "items": [_item("hazard", title)],
    }
    census = {
        "schema_version": "g1q3_feedback_issue_intent_census_v2",
        "observed_side_effects": _zero_census_side_effects(),
        "cases": [
            {
                "work_item_id": "hazard",
                "intent_status": "capability_gate",
                "report_data": {
                    "availability": "http_hash_verified",
                    "sha256": "a" * 64,
                    "size": 1,
                    "issue_focus_present": True,
                    "issue_focus": focus_payload,
                },
            }
        ]
    }

    report = build_offline_replay_report(readback, census=census)

    case = report["cases"][0]
    assert case["report_data"]["focus_contract"]["status"] == "validated"
    assert case["focus_status"] == "capability_unsupported"
    assert case["attribution_allowed"] is False


def test_non_read_only_input_is_rejected():
    readback = _readback()
    readback["read_only"] = False

    with pytest.raises(OfflineReplayGateError) as raised:
        build_offline_replay_report(readback, census=_census())

    assert raised.value.code == "offline_replay_read_only_binding_missing"


def test_unknown_readback_schema_is_rejected():
    readback = _readback()
    readback["schema_version"] = "unknown"

    with pytest.raises(OfflineReplayGateError) as raised:
        build_offline_replay_report(readback, census=_census())

    assert raised.value.code == "offline_replay_readback_schema_unsupported"


def test_write_receipt_contains_path_hash_and_replay_state(tmp_path: Path):
    report = build_offline_replay_report(_readback(), census=_census())
    output = tmp_path / "offline-replay.json"

    receipt = write_offline_replay_report(report, output)

    assert receipt["path"] == str(output)
    assert receipt["sha256"] == sha256_file(output)
    assert receipt["bytes"] == output.stat().st_size
    assert receipt["full_replay_ready"] is False
    assert json.loads(output.read_text(encoding="utf-8"))["schema_version"] == SCHEMA_VERSION
