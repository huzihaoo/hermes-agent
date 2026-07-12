from gateway.pnc_group_binding import evaluate_pnc_group_request


G1Q3_GROUP_ID = "oc_6cfc782212009ff4cd815349909dd423"


def decide(text: str, *, chat_id: str = G1Q3_GROUP_ID):
    return evaluate_pnc_group_request(platform="feishu", chat_id=chat_id, text=text)


def test_g1q3_group_routes_case_status_request_to_rca_binding():
    decision = decide("帮我看一下 case G1Q3-042 现在归因做到哪一步了")

    assert decision.decision == "accepted"
    assert decision.group_binding_id == "gb_g1q3_rca_feishu_group"
    assert decision.business_line_ref == "rca"
    assert decision.project_space_ref == "g1q3_rca"
    assert decision.template_id == "rca_case_status_check"
    assert decision.output_cap == "L1"
    assert decision.reason is None
    assert decision.handoff_contract["case_id"] == "042"


def test_g1q3_group_routes_evidence_summary_request():
    decision = decide("汇总一下 G1Q3-105 当前已有证据，还缺什么")

    assert decision.decision == "accepted"
    assert decision.template_id == "rca_case_evidence_summary"
    assert decision.reason is None
    assert decision.handoff_contract["case_id"] == "105"


def test_g1q3_group_routes_plain_feishu_issue_missing_fields_followup():
    decision = decide("@胡子豪的小助手 飞书问题 7013527412 缺少什么")

    assert decision.decision == "accepted"
    assert decision.template_id == "rca_case_evidence_summary"
    assert decision.handoff_contract["work_item_id"] == "7013527412"


def test_g1q3_group_routes_report_generate_request():
    decision = decide("给 G1Q3-088 生成一版 RCA 摘要报告")

    assert decision.decision == "dry_run"
    assert decision.template_id == "rca_report_generate"
    assert decision.reason == "template_not_enabled_for_real_handoff"


def test_gray_delivery_routes_bare_issue_id_status_followups():
    for text in ("再看下 7013527412", "7013527412 结论是什么", "7013527412 报告在哪里", "7013527412 跑过没"):
        decision = decide(text)
        assert decision.decision == "accepted"
        assert decision.template_id == "rca_case_status_check"
        assert decision.handoff_contract["work_item_id"] == "7013527412"


def test_gray_delivery_does_not_reject_pdcl_data_address_question_as_cross_business_line():
    decision = decide("飞书问题 7013527412 的 PDCL 数据地址缺什么")

    assert decision.decision == "accepted"
    assert decision.template_id == "rca_case_evidence_summary"
    assert decision.handoff_contract["work_item_id"] == "7013527412"


def test_non_g1q3_group_is_not_bound_by_rca_policy():
    decision = decide("帮我看一下 case G1Q3-042 现在归因做到哪一步了", chat_id="oc_other")

    assert decision.decision == "allow"
    assert decision.group_binding_id is None
    assert decision.reason is None


def test_quality_gate_request_is_rejected_as_cross_business_line():
    decision = decide("帮我看下这次评测门禁为什么没过")

    assert decision.decision == "reject"
    assert decision.reason == "cross_business_line"


def test_g1q4_case_is_rejected_as_cross_project_space():
    decision = decide("这个不是 G1Q3，是 G1Q4 的 case，也一起处理一下")

    assert decision.decision == "reject"
    assert decision.reason == "cross_project_space"


def test_arbitrary_repo_read_is_rejected():
    decision = decide("帮我直接读取仓库，把最近修改和归因一起分析掉")

    assert decision.decision == "reject"
    assert decision.reason == "arbitrary_repo_read"


def test_gray_delivery_rejection_carries_risk_gate_for_audit():
    decision = decide("帮我读取仓库源码")

    assert decision.decision == "reject"
    assert decision.risk_gate == "arbitrary_repo_read"
    assert decision.gray_delivery_phase == "g1q3_rca_business_delivery_gray"


def test_raw_log_group_delivery_is_rejected_as_output_too_high():
    decision = decide("把完整原始日志直接发到群里")

    assert decision.decision == "reject"
    assert decision.reason == "output_level_too_high"


def test_operator_topic_delivery_is_rejected_while_topic_disabled():
    decision = decide("把详细证据发到 operator topic")

    assert decision.decision == "reject"
    assert decision.reason == "delivery_surface_not_enabled"


def test_report_without_case_id_is_rejected_as_missing_required_input():
    decision = decide("帮我生成一版 RCA 报告")

    assert decision.decision == "reject"
    assert decision.reason == "missing_required_input"

TEST_GROUP_ID = "oc_16614f4ba25b8c88b69c0b8e9ebc2fb5"


def test_all_business_test_group_routes_g1q3_request_to_rca_binding():
    decision = decide("帮我看一下 case G1Q3-042 现在归因做到哪一步了", chat_id=TEST_GROUP_ID)

    assert decision.decision == "accepted"
    assert decision.group_binding_id == "gb_g1q3_rca_feishu_group"
    assert decision.business_line_ref == "rca"
    assert decision.project_space_ref == "g1q3_rca"
    assert decision.template_id == "rca_case_status_check"


def test_all_business_test_group_does_not_hijack_integration_tools_prompt():
    decision = decide("我想用 logsim 回放一包 mcap，怎么安全发起？", chat_id=TEST_GROUP_ID)

    assert decision.decision == "allow"
    assert decision.group_binding_id is None
