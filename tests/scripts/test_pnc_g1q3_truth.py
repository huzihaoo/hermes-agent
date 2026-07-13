"""Unit tests for the shared G1Q3-RCA delivery truth model.

These pin the authoritative business_result behaviour added 2026-06-23 so the
card pipeline can never again regex-infer a wrong gate/attribution from log
text when the worker wrote an explicit verdict.
"""
from scripts import pnc_g1q3_truth


def test_business_result_skipped_gate_maps_legacy_download_state_to_remote_read():
    verdict = pnc_g1q3_truth.reconcile_report_truth(
        gate_result={},
        report_status="",
        attribution_status="hypothesis_ready",  # stale upstream value
        log_text="ready_to_download token present; need_source_or_evidence",
        business_result={
            "gate_decision": "skipped",
            "gate_skip_reason": "missing_or_invalid_pdcl_download_cmd",
            "status": "need_evidence",
            "terminal_state": "need_download",
        },
    )
    assert verdict["truth_source"] == "business_result"
    assert verdict["gate_decision"] == "skipped"
    assert verdict["gate_green"] is False
    assert verdict["honest_report_status"] == "need_evidence"
    # Blocked intake read zero evidence -> NO attribution, never hypothesis_ready.
    assert verdict["honest_attribution_status"] == ""
    assert verdict["gate_skip_reason"] == "missing_or_invalid_pdcl_download_cmd"
    assert "gate=skipped" in verdict["honest_conclusion"]
    assert "待远程读取/解析或补充证据" in verdict["honest_conclusion"]
    assert "不执行 MDI 下载" in verdict["honest_conclusion"]


def test_business_result_read_failure_terminal_is_non_deliverable():
    verdict = pnc_g1q3_truth.reconcile_report_truth(
        gate_result={},
        log_text="",
        business_result={
            "gate_decision": "skipped",
            "gate_skip_reason": "feishu_issue_read_failed",
            "status": "need_evidence",
            "terminal_state": "need_download",
        },
    )
    assert verdict["truth_source"] == "business_result"
    assert verdict["honest_report_status"] == "need_evidence"
    assert verdict["honest_attribution_status"] == ""
    assert verdict["gate_skip_reason"] == "feishu_issue_read_failed"


def test_green_deliverable_business_result_falls_through_to_inferred_path():
    # A genuinely green/delivered verdict must NOT be hijacked by the
    # business_result short-circuit; it flows through the inferred path so
    # attribution can come from report_data.
    verdict = pnc_g1q3_truth.reconcile_report_truth(
        gate_result={"decision": "green"},
        report_status="html_delivery_ready",
        attribution_status="hypothesis_ready",
        log_text="",
        report_data={"parsed_l2_assets_present": True},
        business_result={
            "gate_decision": "green",
            "status": "completed",
            "terminal_state": "report_ready",
        },
    )
    assert verdict.get("truth_source") != "business_result"
    assert verdict["gate_green"] is True


def test_no_business_result_keeps_inferred_behaviour():
    verdict = pnc_g1q3_truth.reconcile_report_truth(
        gate_result={},
        log_text="gate=ready_to_download",
    )
    assert verdict.get("truth_source") != "business_result"
    assert verdict["gate_decision"] == "ready_to_download"
    assert verdict["honest_report_status"] == "need_evidence"


def test_skipped_is_recognised_as_non_green():
    assert "skipped" in pnc_g1q3_truth.NON_GREEN_GATE_DECISIONS
    assert pnc_g1q3_truth.gate_is_green({"decision": "skipped"}) is False


def test_downgrade_strips_dead_html_url_under_non_green_gate():
    # Regression (issue 7025381565): the L0/L1 notice must not carry a dead
    # html_url to a summary that was never written, under a non-green gate.
    text = (
        "intake 与准入校验完成\n"
        "cause: 以报告为准\n"
        "html_url：//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/x/L0_L1_issue_intake_summary.md\n"
        "verification: 人工复核"
    )
    verdict = {
        "gate_green": False,
        "gate_decision": "skipped",
        "honest_conclusion": "intake 与准入校验完成；待远程读取/解析或补充证据后再出 RCA 结论（gate=skipped；不执行 MDI 下载）",
    }
    out = pnc_g1q3_truth.downgrade_g1q3_notice_text(text, verdict)
    assert "html_url" not in out
    assert "//hfs1" not in out
    assert "L0_L1_issue_intake_summary.md" not in out
    assert "gate=skipped" in out
    assert "不执行 MDI 下载" in out


def test_downgrade_keeps_text_untouched_when_gate_green():
    text = "RCA 报告已生成\nhtml_url：https://example/report.html"
    out = pnc_g1q3_truth.downgrade_g1q3_notice_text(text, {"gate_green": True})
    assert out == text
