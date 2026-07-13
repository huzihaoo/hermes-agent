import pytest

from gateway.pnc_group_binding import evaluate_pnc_group_request


G1Q3_GROUP_ID = "oc_6cfc782212009ff4cd815349909dd423"


def decide(text: str, *, chat_id: str = G1Q3_GROUP_ID, **kwargs):
    kwargs.setdefault("manual_mention_directed", True)
    return evaluate_pnc_group_request(
        platform="feishu",
        chat_id=chat_id,
        text=text,
        **kwargs,
    )


def test_g1q3_group_routes_case_status_request_to_rca_binding():
    decision = decide("帮我看一下 case G1Q3-042 现在归因做到哪一步了")

    assert decision.decision == "accepted"
    assert decision.group_binding_id == "gb_g1q3_rca_feishu_group"
    assert decision.business_line_ref == "rca"
    assert decision.project_space_ref == "g1q3_rca"
    assert decision.template_id == "rca_case_status_check"
    assert decision.output_cap == "L1"
    assert decision.reason == "kafka_only_read_only_status"
    assert decision.route_surface == "rca_kafka_read_only_status"
    assert decision.risk_gate == "kafka_only_read_only"
    assert decision.handoff_contract["read_only"] is True
    assert decision.handoff_contract["case_id"] == "042"


def test_g1q3_group_routes_evidence_summary_request():
    decision = decide("汇总一下 G1Q3-105 当前已有证据，还缺什么")

    assert decision.decision == "accepted"
    assert decision.template_id == "rca_case_evidence_summary"
    assert decision.reason == "kafka_only_read_only_status"
    assert decision.route_surface == "rca_kafka_read_only_status"
    assert decision.handoff_contract["read_only"] is True
    assert decision.handoff_contract["case_id"] == "105"


def test_g1q3_group_routes_plain_feishu_issue_missing_fields_followup():
    decision = decide("@胡子豪的小助手 飞书问题 7013527412 缺少什么")

    assert decision.decision == "accepted"
    assert decision.template_id == "rca_case_evidence_summary"
    assert decision.handoff_contract["work_item_id"] == "7013527412"


def test_complete_issue_url_with_explicit_action_routes_manual_intake():
    url = "https://project.feishu.cn/g1q3/issue/detail/7013527412"
    decision = decide(f"@PNC-Agent 分析这个问题 {url}")

    assert decision.decision == "accepted"
    assert decision.template_id == "rca_issue_intake"
    assert decision.route_surface == "rca_manual_intake"
    assert decision.risk_gate == "manual_intake_control_store"
    assert decision.handoff_contract["issue_url"] == url
    assert decision.handoff_contract["mode"] == "run_or_join"


def test_explicit_rerun_debug_and_emergency_actions_have_bounded_modes():
    url = "https://project.feishu.cn/g1q3/issue/detail/7013527412"
    cases = {
        f"重跑这个问题 {url}": "rerun",
        f"debug 这个问题 {url}": "debug",
        f"紧急分析这个问题 {url}": "run_or_join",
    }
    for text, mode in cases.items():
        decision = decide(text)
        assert decision.route_surface == "rca_manual_intake"
        assert decision.handoff_contract["mode"] == mode


@pytest.mark.parametrize(
    "action",
    [
        "@小助手 帮我分析一下这个问题",
        "帮忙分析一下这个问题",
        "麻烦分析一下这个问题",
        "紧急问题，请分析",
        "给这个问题做 RCA",
        "请尽快分析一下这个问题，辛苦了",
        "麻烦尽快帮忙分析一下这个问题",
    ],
)
def test_common_explicit_analysis_phrases_route_manual_intake(action):
    url = "https://project.feishu.cn/g1q3/issue/detail/7013527412"

    decision = decide(f"{action} {url}")

    assert decision.route_surface == "rca_manual_intake"
    assert decision.handoff_contract["mode"] == "run_or_join"


@pytest.mark.parametrize(
    "action",
    [
        "不要分析这个问题",
        "请不要分析这个问题",
        "先别重跑这个问题",
        "无需分析这个问题",
        "不用调试这个问题",
        "暂不做 RCA",
        "分析这个问题？不要执行，只查状态",
        "分析这个问题，但不要创建任务",
        "请分析这个问题，仅查询状态",
    ],
)
def test_negated_manual_actions_remain_read_only(action):
    url = "https://project.feishu.cn/g1q3/issue/detail/7013527412"

    decision = decide(f"{action} {url}")

    assert decision.route_surface == "rca_kafka_issue_status"
    assert decision.handoff_contract["read_only"] is True


@pytest.mark.parametrize(
    "action",
    (
        "这个问题很紧急，麻烦分析",
        "这个问题很紧急，麻烦帮忙分析一下",
        "这个问题很紧急，请帮我分析下",
    ),
)
def test_urgent_natural_language_with_explicit_analysis_routes_manual_intake(action):
    url = "https://project.feishu.cn/g1q3/issue/detail/7013527412"

    decision = decide(f"{action} {url}")

    assert decision.route_surface == "rca_manual_intake"
    assert decision.handoff_contract["mode"] == "run_or_join"


def test_manual_action_with_gateway_mention_hint_append_still_routes_intake():
    url = "https://project.feishu.cn/g1q3/issue/detail/7013527412"

    decision = decide(
        f"这个问题很紧急，麻烦帮忙分析一下 {url}\n"
        "[Mentioned: PNC-Agent, 值班同学]"
    )

    assert decision.route_surface == "rca_manual_intake"
    assert decision.handoff_contract["issue_url"] == url


@pytest.mark.parametrize(
    "query",
    (
        "分析这个问题的报告在哪里",
        "分析这个问题的任务进展如何",
        "分析这个问题是谁触发的",
        "分析任务状态怎么样",
    ),
)
def test_analysis_status_questions_never_enter_manual_intake(query):
    url = "https://project.feishu.cn/g1q3/issue/detail/7013527412"

    decision = decide(f"{query} {url}")

    assert decision.route_surface == "rca_kafka_issue_status"
    assert decision.handoff_contract["read_only"] is True


@pytest.mark.parametrize(
    "command",
    (
        "分析这个问题\n但不要执行，只查状态",
        "分析这个问题\n报告在哪里",
        "分析这个问题\n能否先只看一下状态？",
    ),
)
def test_multiline_read_only_intent_overrides_action(command):
    url = "https://project.feishu.cn/g1q3/issue/detail/7013527412"

    decision = decide(f"{command}\n{url}")

    assert decision.route_surface == "rca_kafka_issue_status"
    assert decision.handoff_contract["read_only"] is True


@pytest.mark.parametrize("suffix", ("但不要执行，只查状态", "报告在哪里"))
def test_same_line_text_after_url_remains_in_user_command_scope(suffix):
    url = "https://project.feishu.cn/g1q3/issue/detail/7013527412"

    decision = decide(f"分析这个问题 {url} {suffix}")

    assert decision.route_surface == "rca_kafka_issue_status"
    assert decision.handoff_contract["read_only"] is True


def test_multiple_distinct_issue_identities_fail_closed_with_clarification():
    first = "https://project.feishu.cn/g1q3/issue/detail/7013527412"
    second = "https://project.feishu.cn/g1q3/issue/detail/7013527999"

    decision = decide(f"分析这个问题 {first}\n{second}")

    assert decision.decision == "clarify"
    assert decision.reason == "ambiguous_issue_identity"
    assert decision.route_surface == "rca_issue_identity_clarify"


def test_repeated_same_canonical_issue_identity_is_not_ambiguous():
    url = "https://project.feishu.cn/g1q3/issue/detail/7013527412"

    decision = decide(f"分析这个问题 {url}\n{url}?openScene=4")

    assert decision.route_surface == "rca_manual_intake"
    assert decision.handoff_contract["work_item_id"] == "7013527412"


def test_reply_identity_is_used_only_when_it_is_exactly_one():
    url = "https://project.feishu.cn/g1q3/issue/detail/7013527412"

    accepted = decide(
        "分析这个问题",
        reply_to_text=f"问题卡片\n{url}",
    )
    ambiguous = decide(
        "分析这个问题",
        reply_to_text=(
            f"{url}\nhttps://project.feishu.cn/g1q3/issue/detail/7013527999"
        ),
    )

    assert accepted.route_surface == "rca_manual_intake"
    assert accepted.handoff_contract["issue_identity_source"] == "reply"
    assert ambiguous.reason == "ambiguous_issue_identity"


def test_current_and_reply_identities_must_resolve_to_the_same_issue():
    current = "https://project.feishu.cn/g1q3/issue/detail/7013527412"
    replied = "https://project.feishu.cn/g1q3/issue/detail/7013527999"

    conflicting = decide(
        f"分析这个问题 {current}",
        reply_to_text=f"被回复的问题卡片\n{replied}",
    )
    repeated = decide(
        f"分析这个问题 {current}",
        reply_to_text=f"被回复的问题卡片\n{current}?openScene=4",
    )

    assert conflicting.decision == "clarify"
    assert conflicting.reason == "ambiguous_issue_identity"
    assert repeated.route_surface == "rca_manual_intake"
    assert repeated.handoff_contract["work_item_id"] == "7013527412"


def test_reply_card_text_cannot_supply_execution_mode():
    url = "https://project.feishu.cn/g1q3/issue/detail/7013527412"

    decision = decide(
        "查一下状态",
        reply_to_text=f"debug 这个问题\n{url}",
    )

    assert decision.route_surface == "rca_kafka_issue_status"
    assert decision.handoff_contract["read_only"] is True


def test_metadata_issue_identity_does_not_import_card_body_into_command_scope():
    url = "https://project.feishu.cn/g1q3/issue/detail/7013527412"
    decision = decide(
        "@小助手\n"
        "分析这个问题\n"
        "[Mentioned: 值班同学]",
        issue_link_urls=[url],
    )

    assert decision.route_surface == "rca_manual_intake"
    assert decision.handoff_contract["mode"] == "run_or_join"


def test_newline_text_after_explicit_url_remains_in_user_command_scope():
    url = "https://project.feishu.cn/g1q3/issue/detail/7013527412"

    decision = decide(f"分析这个问题 {url}\n但不要执行，只查状态")

    assert decision.route_surface == "rca_kafka_issue_status"
    assert decision.handoff_contract["read_only"] is True


def test_missing_or_non_directed_mention_is_fail_closed_for_pure_policy_callers():
    url = "https://project.feishu.cn/g1q3/issue/detail/7013527412"

    missing = evaluate_pnc_group_request(
        platform="feishu",
        chat_id=G1Q3_GROUP_ID,
        text=f"分析这个问题 {url}",
    )
    other_bot = evaluate_pnc_group_request(
        platform="feishu",
        chat_id=G1Q3_GROUP_ID,
        text=f"@OtherBot 分析这个问题 {url}",
        manual_mention_directed=False,
    )

    assert missing.route_surface == "rca_kafka_issue_status"
    assert other_bot.route_surface == "rca_kafka_issue_status"


def test_manual_action_after_issue_url_is_in_current_user_command_scope():
    url = "https://project.feishu.cn/g1q3/issue/detail/7013527412"

    decision = decide(f"{url}\n分析这个问题")

    assert decision.route_surface == "rca_manual_intake"
    assert decision.handoff_contract["mode"] == "run_or_join"


def test_natural_urgent_issue_request_routes_manual_intake():
    url = "https://project.feishu.cn/g1q3/issue/detail/7013527412"

    decision = decide(f"有个紧急问题，麻烦分析一下 {url}")

    assert decision.route_surface == "rca_manual_intake"
    assert decision.handoff_contract["mode"] == "run_or_join"


def test_issue_card_content_cannot_be_misread_as_manual_action():
    url = "https://project.feishu.cn/g1q3/issue/detail/7013527412"

    for text in (
        f"{url} debug 日志已上传",
        f"问题标题：重跑后仍然失败 {url}",
        f"状态查询\n调试信息：无 {url}",
    ):
        decision = decide(text)
        assert decision.route_surface == "rca_kafka_issue_status"
        assert decision.handoff_contract["read_only"] is True


def test_bare_issue_url_and_status_question_remain_read_only():
    url = "https://project.feishu.cn/g1q3/issue/detail/7013527412"
    for text in (url, f"这个问题进展怎么样 {url}"):
        decision = decide(text)
        assert decision.route_surface == "rca_kafka_issue_status"
        assert decision.handoff_contract["read_only"] is True


def test_manual_action_outside_fixed_groups_remains_read_only():
    url = "https://project.feishu.cn/g1q3/issue/detail/7013527412"
    decision = decide(f"分析这个问题 {url}", chat_id="oc_other")
    assert decision.route_surface == "rca_kafka_issue_status"
    assert decision.handoff_contract["read_only"] is True


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
