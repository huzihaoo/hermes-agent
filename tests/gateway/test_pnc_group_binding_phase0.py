from gateway.pnc_group_binding import evaluate_pnc_group_request

G1Q3_GROUP_ID = "oc_6cfc782212009ff4cd815349909dd423"


def decide(text: str, **kwargs):
    return evaluate_pnc_group_request(platform="feishu", chat_id=G1Q3_GROUP_ID, text=text, **kwargs)


def assert_not_rejected_as(text: str, reason: str):
    decision = decide(text)
    assert decision.reason != reason
    assert not (decision.decision == "reject" and decision.reason == reason)


def test_phase0_followups_are_not_misclassified_as_privileged_or_missing_input():
    assert_not_rejected_as("为什么任务提交失败", "arbitrary_git")
    assert_not_rejected_as("最终的结论和报告呢？", "missing_required_input")
    assert_not_rejected_as("卡在哪？获取飞书项目信息么？", "unsupported_task_template")
    assert_not_rejected_as("这个不就是标准的任务么？哪里不满足？", "unsupported_task_template")


def test_phase0_true_privileged_and_cross_scope_requests_still_reject():
    cases = [
        ("帮我 git clone xxx 仓库", {"arbitrary_repo_read", "arbitrary_git"}),
        ("执行一下这条命令", {"arbitrary_shell"}),
        ("评测门禁结果", {"cross_business_line"}),
        ("g1q4 的问题", {"cross_project_space"}),
    ]
    for text, reasons in cases:
        decision = decide(text)
        assert decision.decision == "reject"
        assert decision.reason in reasons


def test_phase0_thread_followup_uses_active_thread_case():
    decision = decide(
        "结论呢",
        active_thread_case={"work_item_id": "7017699515", "task_id": "20260616-103757-g1q3-rca-issue-intake-7017699515"},
    )

    assert decision.decision == "accepted"
    assert decision.template_id == "rca_case_status_check"
    assert decision.handoff_contract["work_item_id"] == "7017699515"
    assert decision.handoff_contract["source_task_id"] == "20260616-103757-g1q3-rca-issue-intake-7017699515"
