from gateway.pnc_delivery_policy import (
    G1Q3RcaTaskIntent,
    evaluate_pnc_delivery,
    validate_g1q3_rca_task_intent,
)


G1Q3_GROUP_ID = "oc_6cfc782212009ff4cd815349909dd423"


def test_progress_update_allowed_to_group_summary_profile():
    decision = evaluate_pnc_delivery(
        delivery_profile_ref="dp_g1q3_rca_group_summary",
        requested_output_level="L0",
        content_preview="收到 G1Q3-042，当前处于 dry-run 路由验证。",
    )

    assert decision.decision == "allow"
    assert decision.max_output_level == "L1"


def test_completion_summary_allowed_to_group_summary_profile():
    decision = evaluate_pnc_delivery(
        delivery_profile_ref="dp_g1q3_rca_group_summary",
        requested_output_level="L1",
        content_preview="G1Q3-042 当前缺少车辆轨迹证据，未启动业务归因。",
    )

    assert decision.decision == "allow"
    assert decision.reason is None


def test_detailed_evidence_deferred_from_default_group():
    decision = evaluate_pnc_delivery(
        delivery_profile_ref="dp_g1q3_rca_group_summary",
        requested_output_level="L2",
        content_preview="需要展示更多证据片段。",
    )

    assert decision.decision == "defer"
    assert decision.reason == "output_level_above_group_cap"


def test_raw_logs_blocked_even_if_level_claims_l1():
    decision = evaluate_pnc_delivery(
        delivery_profile_ref="dp_g1q3_rca_group_summary",
        requested_output_level="L1",
        content_preview="完整原始日志如下: ...",
    )

    assert decision.decision == "block"
    assert decision.reason == "raw_log"


def test_source_diff_blocked_from_group():
    decision = evaluate_pnc_delivery(
        delivery_profile_ref="dp_g1q3_rca_group_summary",
        requested_output_level="L1",
        content_preview="源码 diff 显示 /home/mini/data3/yj-evaluation-server 有修改。",
    )

    assert decision.decision == "block"
    assert decision.reason == "source_or_diff"


def test_valid_status_handoff_intent_is_minimal_and_validates():
    intent = G1Q3RcaTaskIntent(
        template_id="rca_case_status_check",
        case_id="G1Q3-042",
        requester="ou_test_user",
        source_group_id=G1Q3_GROUP_ID,
    )

    decision = validate_g1q3_rca_task_intent(intent)

    assert decision.decision == "valid"
    assert decision.intent == intent


def test_missing_case_rejected_before_handoff():
    decision = validate_g1q3_rca_task_intent(
        G1Q3RcaTaskIntent(
            template_id="rca_case_status_check",
            case_id="",
            requester="ou_test_user",
            source_group_id=G1Q3_GROUP_ID,
        )
    )

    assert decision.decision == "reject"
    # Both case_id and work_item_id are valid issue identifiers.  Keep the
    # rejection reason aligned with that expanded handoff contract.
    assert decision.reason == "missing_issue_identifier"


def test_cross_line_intent_rejected():
    decision = validate_g1q3_rca_task_intent(
        G1Q3RcaTaskIntent(
            template_id="rca_case_status_check",
            case_id="G1Q3-042",
            requester="ou_test_user",
            source_group_id=G1Q3_GROUP_ID,
            business_line_ref="evaluation_gate",
        )
    )

    assert decision.decision == "reject"
    assert decision.reason == "cross_business_line"


def test_worker_requested_l3_is_rejected_by_group_cap():
    decision = validate_g1q3_rca_task_intent(
        G1Q3RcaTaskIntent(
            template_id="rca_case_status_check",
            case_id="G1Q3-042",
            requester="ou_test_user",
            source_group_id=G1Q3_GROUP_ID,
            output_cap="L3",
        )
    )

    assert decision.decision == "reject"
    assert decision.reason == "output_cap_above_group_profile"


def test_source_path_leakage_in_intent_rejected():
    decision = validate_g1q3_rca_task_intent(
        G1Q3RcaTaskIntent(
            template_id="rca_case_status_check",
            case_id="G1Q3-042",
            requester="ou_test_user",
            source_group_id="/home/mini/data3/yj-evaluation-server",
        )
    )

    assert decision.decision == "reject"
    assert decision.reason == "forbidden_operator_or_path_detail"
