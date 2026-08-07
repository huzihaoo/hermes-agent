from __future__ import annotations

import copy

import pytest

from gateway.pnc_rca_issue_focus import (
    ANALYSIS_CAPABILITY_UNSUPPORTED,
    ANALYSIS_COMPLETE,
    ANALYSIS_INSUFFICIENT_STATEMENT,
    ISSUE_FOCUS_EVIDENCE_SCHEMA_VERSION,
    IssueFocusContractError,
    issue_title_sha256,
    resolve_issue_intent,
    validate_issue_focus_evidence,
)


FEEDBACK_TITLES = (
    "ACC-前车切入，跟停前前车，自车加速后刹停",
    "ACC-同车道环卫工人推着垃圾桶，自车不刹车，驾驶员接管",
    "AEB-AEB触发仪表无双闪",
    "ACC-前方仪表无目标，制动",
    "HMI-左侧车模出现先闪一下在稳定",
    "ACC-进入匝道减速过大",
    "ACC-ACC设定70，跟随前车，刹车晚，二次起步",
    "ACC-大货车向右切出后停在车道线中间，先识别到大货车模型，自车开始减速，后模型释放，自车未减速跟停",
)


def _row_refs():
    return ["report_data.json#/issue_focus"]


def _complete_payload(title: str):
    intent = resolve_issue_intent(title)
    return {
        "schema_version": ISSUE_FOCUS_EVIDENCE_SCHEMA_VERSION,
        "issue_intent": intent.to_dict(),
        "title_sha256": issue_title_sha256(title),
        "analysis_status": ANALYSIS_COMPLETE,
        "capabilities": [
            {
                "key": key,
                "status": "available",
                "provider": "g1q3_rca_worker",
                "version": "test-v1",
                "evidence_refs": _row_refs(),
            }
            for key in intent.required_capabilities
        ],
        "segments": [
            {"role": key, "start_ts": 1.0, "end_ts": 2.0, "evidence_refs": _row_refs()}
            for key in intent.required_segments
        ],
        "entities": [
            {
                "role": key,
                "target_id": str(index + 10),
                "object_class": "vru" if "vru" in key else "vehicle",
                "speed_summary": "1.0s 到 2.0s 为 8.0 -> 2.0 m/s。",
                "distance_summary": "1.0s 到 2.0s 为 18.0 -> 5.0 m。",
                "evidence_refs": _row_refs(),
            }
            for index, key in enumerate(intent.required_entities)
        ],
        "measurements": [
            {"key": key, "unit": "m/s", "summary": f"{key} 已复算。", "evidence_refs": _row_refs()}
            for key in intent.required_measurements
        ],
        "checks": [
            {"key": key, "status": "supported", "summary": f"{key} 已闭环。", "evidence_refs": _row_refs()}
            for key in intent.required_checks
        ],
        "calculations": [
            {
                "key": key,
                "formula": "a_lat = v^2 * kappa",
                "unit": "m/s^2",
                "summary": "峰值向心加速度为 2.1 m/s^2。",
                "evidence_refs": _row_refs(),
            }
            for key in intent.required_calculations
        ],
        "missing_requirements": [],
        "unsupported_capabilities": [],
        "stop_reason": "",
    }


def test_feedback_samples_are_composed_from_shared_intent_dimensions():
    intents = [resolve_issue_intent(title) for title in FEEDBACK_TITLES]

    assert all(intent.statement_sufficient for intent in intents)
    assert "unexpected_longitudinal_response" in intents[0].phenomena
    assert {"cut_in_target", "lead_lead_target"}.issubset(intents[0].entity_roles)
    assert "vru_target" in intents[1].entity_roles
    assert "vehicle_signal_chain" in intents[2].required_capabilities
    assert "hmi_projection" in intents[3].required_capabilities
    assert {
        "detection_pipeline",
        "object_identity",
        "output_continuity",
    }.issubset(intents[3].required_capabilities)
    assert "lead_target" in intents[3].required_entities
    assert "output_continuity" in intents[4].required_capabilities
    assert "centripetal_acceleration" in intents[5].required_calculations
    assert {"late_response", "start_behavior_anomaly"}.issubset(intents[6].phenomena)
    assert "cut_out_target" in intents[7].entity_roles


@pytest.mark.parametrize("title", FEEDBACK_TITLES)
def test_complete_feedback_sample_covers_every_composed_requirement(title):
    result = validate_issue_focus_evidence(issue_title=title, value=_complete_payload(title))

    assert result.attribution_allowed is True
    assert result.missing_requirements == ()


@pytest.mark.parametrize(
    "title",
    (
        "HMI-S弯",
        "HMI-岔路口",
        "HMI-111",
        "ACC-加减",
        "ACC-二轮车",
        "LCC-汇入场景",
        "LDP-LDP触发",
        "LCC-直道方向盘",
        "LCC-跟停方向盘",
    ),
)
def test_vague_titles_require_honest_stop(title):
    intent = resolve_issue_intent(title)
    payload = {
        "schema_version": ISSUE_FOCUS_EVIDENCE_SCHEMA_VERSION,
        "issue_intent": intent.to_dict(),
        "title_sha256": issue_title_sha256(title),
        "analysis_status": ANALYSIS_INSUFFICIENT_STATEMENT,
        "capabilities": [],
        "segments": [],
        "entities": [],
        "measurements": [],
        "checks": [],
        "calculations": [],
        "missing_requirements": ["statement:problem_statement"],
        "unsupported_capabilities": [],
        "stop_reason": "问题标题未给出可核验的实际异常和预期行为。",
    }

    result = validate_issue_focus_evidence(issue_title=title, value=payload)

    assert result.attribution_allowed is False


@pytest.mark.parametrize(
    ("title", "expected_phenomena"),
    (
        ("ACC-车速60限速60跟停减速太晚", {"late_response"}),
        ("LCC-弯道行驶向左偏", {"lateral_path_anomaly"}),
        (
            "ELKA-自车偏向路沿，ELKA触发，车道线变红，有预警，无纠偏",
            {"lateral_correction_missing"},
        ),
        (
            "LCC-打转向灯，转向灯拨杆不回退，抑制TJA进入",
            {"function_activation_inhibited"},
        ),
        ("LCC-跟车起步方向盘轻微左打", {"lateral_path_anomaly"}),
        ("LCC-错位路口方向盘向右打", {"lateral_path_anomaly"}),
        ("ACC-大巴车切出，确认目标释放时机", {"target_release_timing"}),
        ("ACC-前车切入，减速后释放过慢", {"target_release_timing"}),
    ),
)
def test_narrow_lexical_recovery_binds_expected_phenomena(title, expected_phenomena):
    intent = resolve_issue_intent(title)

    assert intent.statement_sufficient is True
    assert expected_phenomena.issubset(set(intent.phenomena))


def test_any_required_capability_can_stop_without_unrelated_analysis():
    title = "AEB-AEB触发仪表无双闪"
    intent = resolve_issue_intent(title)
    unsupported = "vehicle_signal_chain"
    payload = {
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
        "missing_requirements": [f"capability:{unsupported}"],
        "unsupported_capabilities": [unsupported],
        "stop_reason": "当前版本未接入双闪请求到仪表反馈的闭环归因能力。",
    }

    result = validate_issue_focus_evidence(issue_title=title, value=payload)

    assert result.attribution_allowed is False
    assert result.unsupported_capabilities == (unsupported,)


def test_capability_stop_rejects_generic_evaluator_evidence_expansion():
    title = "AEB-AEB触发仪表无双闪"
    payload = _complete_payload(title)
    payload.update(
        analysis_status=ANALYSIS_CAPABILITY_UNSUPPORTED,
        unsupported_capabilities=["vehicle_signal_chain"],
        missing_requirements=["capability:vehicle_signal_chain"],
        stop_reason="能力未接入。",
    )

    with pytest.raises(IssueFocusContractError) as raised:
        validate_issue_focus_evidence(issue_title=title, value=payload)

    assert raised.value.code == "issue_focus_unsupported_analysis_must_stop"


def test_target_case_rejects_missing_target_id():
    title = "ACC-同车道环卫工人推着垃圾桶，自车不刹车，驾驶员接管"
    payload = _complete_payload(title)
    payload["entities"][0]["target_id"] = ""

    with pytest.raises(IssueFocusContractError) as raised:
        validate_issue_focus_evidence(issue_title=title, value=payload)

    assert raised.value.code == "issue_focus_target_id_invalid"


def test_composite_issue_rejects_one_uncovered_question():
    title = "ACC-ACC设定70，跟随前车，刹车晚，二次起步"
    payload = _complete_payload(title)
    payload["segments"] = [
        row for row in payload["segments"] if row["role"] != "start_behavior_window"
    ]

    with pytest.raises(IssueFocusContractError) as raised:
        validate_issue_focus_evidence(issue_title=title, value=payload)

    assert raised.value.code == "issue_focus_complete_requirements_missing"


def test_complete_issue_rejects_unrelated_generic_evidence():
    title = "AEB-AEB触发仪表无双闪"
    payload = _complete_payload(title)
    payload["checks"].append(
        {
            "key": "generic_ooi_quality",
            "status": "supported",
            "summary": "与双闪问题无直接关系。",
            "evidence_refs": _row_refs(),
        }
    )

    with pytest.raises(IssueFocusContractError) as raised:
        validate_issue_focus_evidence(issue_title=title, value=payload)

    assert raised.value.code == "issue_focus_unexpected_requirements"


def test_insufficient_statement_rejects_generic_analysis_rows():
    title = "HMI-S弯"
    intent = resolve_issue_intent(title)
    payload = {
        "schema_version": ISSUE_FOCUS_EVIDENCE_SCHEMA_VERSION,
        "issue_intent": intent.to_dict(),
        "title_sha256": issue_title_sha256(title),
        "analysis_status": ANALYSIS_INSUFFICIENT_STATEMENT,
        "capabilities": [],
        "segments": [
            {
                "role": "generic_window",
                "start_ts": 1.0,
                "end_ts": 2.0,
                "evidence_refs": _row_refs(),
            }
        ],
        "entities": [],
        "measurements": [],
        "checks": [],
        "calculations": [],
        "missing_requirements": ["statement:problem_statement"],
        "unsupported_capabilities": [],
        "stop_reason": "问题陈述不足。",
    }

    with pytest.raises(IssueFocusContractError) as raised:
        validate_issue_focus_evidence(issue_title=title, value=payload)

    assert raised.value.code == "issue_focus_insufficient_analysis_must_stop"


def test_title_binding_prevents_cross_issue_evidence_reuse():
    source_title = "ACC-前方仪表无目标，制动"
    other_title = "HMI-左侧车模出现先闪一下在稳定"
    payload = copy.deepcopy(_complete_payload(source_title))
    payload["issue_intent"] = resolve_issue_intent(other_title).to_dict()

    with pytest.raises(IssueFocusContractError) as raised:
        validate_issue_focus_evidence(issue_title=other_title, value=payload)

    assert raised.value.code == "issue_focus_title_binding_mismatch"
