from __future__ import annotations

import pytest

from gateway.pnc_rca_quality_oracle import (
    CANDIDATE_HYPOTHESIS,
    CONSUMER_DELIVERY_FAILURE,
    HONEST_NON_ATTRIBUTION,
    MEDIUM_TIER_DISCLAIMER,
    SUPPORTED_ATTRIBUTION,
    TECHNICAL_FAILURE,
    TierOracleConflict,
    evaluate_structural_tier,
    require_publishable,
)


def _contract(
    *,
    evaluator_status: str = "supported",
    refs: bool = True,
    conclusion: str = "车道线横向跳变经控制链传导，导致车道保持不稳。",
    candidate: bool = False,
) -> dict:
    if candidate:
        conclusion += " 当前为候选方向，需人工复核。"
    return {
        "consumer_capability": {
            "actual_evaluators": [
                {
                    "evaluator_id": "lane_geometry_quality",
                    "status": evaluator_status,
                    "decode_status": "decoded",
                    "evidence_role": "decoded_evaluator",
                }
            ],
            "unused_capabilities": [
                {
                    "evaluator_id": "inventory_only_alias",
                    "status": "not_invoked",
                    "reason": "not applicable",
                }
            ],
            "evidence": {
                "issue_frame_id": 160304,
                "focus_window": {"start_ts": 0.0, "end_ts": 1.0},
                "field_lineage": {"fidelity_ok": True},
                "viz_lineage": {"ok": True, "status": "pass"},
            },
        },
        "report": {
            "candidate_owner_domain": "PERCEPTION_LANE",
            "is_candidate": candidate,
        },
        "public_result": {
            "summary": {"short_conclusion": conclusion},
            "candidate": "PERCEPTION_LANE",
            "responsibility": {"status": "candidate" if candidate else "supported"},
            "evidence_summary": {
                "refs": ([{"evidence_ref": "frame:160304/lane"}] if refs else [])
            },
            "causal_chain": {
                "narrative": [
                    {"role": "现象", "text": "车道保持不稳。"},
                    {"role": "证据", "text": "车道线横向跳变。"},
                    {"role": "因果判断", "text": conclusion},
                ]
            },
            "user_action": {},
        },
    }


def test_supported_attribution_requires_emitted_supported_key_and_full_structure():
    result = evaluate_structural_tier(
        _contract(),
        publication_text=(
            "归因结论：车道线异常经控制链传导。\n"
            "责任模块：PERCEPTION_LANE\n"
            "因果关系：车道线异常导致车道保持不稳。\n"
            "关键证据：frame:160304/lane"
        ),
    )

    assert result.terminal_class == SUPPORTED_ATTRIBUTION
    assert result.confidence_tier == "high"
    assert result.publication_allowed is True
    assert result.facts.supported_evaluator_keys == ("lane_geometry_quality",)
    assert result.facts.evidence_complete is True
    assert result.facts.causal_chain_closed is True


@pytest.mark.parametrize("status", ["refuted", "likely", "unknown"])
def test_only_actual_supported_status_counts_as_an_evaluator_hit(status):
    result = evaluate_structural_tier(_contract(evaluator_status=status))

    assert result.terminal_class == HONEST_NON_ATTRIBUTION
    assert result.facts.supported_evaluator_count == 0
    assert "inventory_only_alias" not in result.facts.supported_evaluator_keys


def test_candidate_requires_exact_medium_disclaimer_at_publication():
    contract = _contract(refs=False, candidate=True)
    missing = evaluate_structural_tier(contract, publication_text="候选结论。")
    compliant = evaluate_structural_tier(
        contract,
        publication_text=f"置信说明：{MEDIUM_TIER_DISCLAIMER}\n候选结论。",
    )

    assert missing.terminal_class == CANDIDATE_HYPOTHESIS
    assert missing.publication_allowed is False
    assert "candidate_disclaimer_missing" in missing.violations
    assert compliant.publication_allowed is True


def test_explicit_candidate_flag_prevents_high_promotion():
    contract = _contract()
    contract["report"]["is_candidate"] = True

    result = evaluate_structural_tier(contract)

    assert result.terminal_class == CANDIDATE_HYPOTHESIS
    assert result.confidence_tier == "medium"


def test_live_candidate_status_prevents_high_promotion():
    contract = _contract()
    contract["public_result"]["responsibility"]["status"] = "candidate_from_live_rca"

    result = evaluate_structural_tier(contract)

    assert result.terminal_class == CANDIDATE_HYPOTHESIS


def test_string_evidence_refs_do_not_count_as_complete_evidence():
    contract = _contract()
    contract["public_result"]["evidence_summary"]["refs"] = "frame:160304/lane"

    result = evaluate_structural_tier(contract)

    assert result.terminal_class == CANDIDATE_HYPOTHESIS
    assert result.facts.evidence_ref_count == 0
    assert result.facts.evidence_complete is False


def test_partial_focus_window_does_not_count_as_complete_evidence():
    contract = _contract()
    contract["consumer_capability"]["evidence"]["focus_window"] = {"start_ts": 0.0}

    result = evaluate_structural_tier(contract)

    assert result.terminal_class == CANDIDATE_HYPOTHESIS
    assert result.facts.focus_window_present is False


def test_non_attribution_wording_can_only_be_honest_non_attribution():
    contract = _contract(
        conclusion="自动RCA未归因：当前证据不能确认归因。",
    )
    result = evaluate_structural_tier(
        contract,
        claimed_terminal_class=SUPPORTED_ATTRIBUTION,
        publication_text=(
            "归因结论：系统已完成现有证据分析，但未形成可确认的归因结论。\n"
            "责任模块：暂无法判断。\n因果关系：现有证据不足以闭合责任因果链。\n"
            "关键证据：证据仅支持记录分析边界。"
        ),
    )

    assert result.terminal_class == HONEST_NON_ATTRIBUTION
    assert result.publication_allowed is False
    assert (
        "terminal_class_mismatch:supported_attribution:honest_non_attribution"
        in result.violations
    )
    assert "supported_attribution_non_attribution_wording" in result.violations


def test_non_attribution_boundary_prevents_supported_promotion():
    contract = _contract()
    contract["public_result"]["evidence_boundary"] = ["自动RCA未归因：边界证据不足。"]

    result = evaluate_structural_tier(contract)

    assert result.terminal_class == HONEST_NON_ATTRIBUTION
    assert result.facts.explicit_non_attribution is True


def test_supported_claim_with_zero_evaluator_fails_closed():
    contract = _contract(evaluator_status="refuted", refs=False)
    result = evaluate_structural_tier(
        contract,
        claimed_terminal_class=SUPPORTED_ATTRIBUTION,
    )

    assert result.facts.supported_evaluator_count == 0
    assert "supported_attribution_evaluator_count_zero" in result.violations
    with pytest.raises(TierOracleConflict):
        require_publishable(result)


def test_empty_human_decision_cannot_claim_approval_ready():
    result = evaluate_structural_tier(
        _contract(),
        human_decision="",
        approval_ready=True,
    )

    assert "approval_ready_without_human_decision" in result.violations
    assert result.publication_allowed is False


@pytest.mark.parametrize(
    "publication_text", ("quality-approved", "approval_ready=true")
)
def test_empty_human_decision_rejects_approval_ready_text(publication_text):
    result = evaluate_structural_tier(
        _contract(),
        human_decision="",
        publication_text=publication_text,
    )

    assert "approval_ready_without_human_decision" in result.violations
    assert result.publication_allowed is False


def test_low_tier_rejects_user_action_and_blame_wording():
    contract = _contract(evaluator_status="refuted", refs=False)
    result = evaluate_structural_tier(
        contract,
        publication_text=(
            "归因结论：问题单缺少问题数据地址。\n"
            "责任模块：暂无法判断。\n请补齐后重新发起。"
        ),
    )

    assert result.terminal_class == HONEST_NON_ATTRIBUTION
    assert "honest_non_attribution_user_action" in result.violations
    assert "honest_non_attribution_blame_wording" in result.violations


def test_banned_phrase_is_rejected_even_when_renderer_would_hide_it():
    contract = _contract(
        evaluator_status="refuted",
        refs=False,
        conclusion="自动RCA未归因：请核对问题数据地址。",
    )
    result = evaluate_structural_tier(
        contract,
        publication_text=(
            "归因结论：系统已完成现有证据分析，但未形成可确认的归因结论。\n"
            "责任模块：暂无法判断。"
        ),
    )

    assert "banned_public_phrase:请核对问题数据地址" in result.violations
    assert result.publication_allowed is False


def test_execution_and_consumer_failures_are_distinct_nonpublishable_terminals():
    technical = evaluate_structural_tier(
        _contract(),
        execution_outcome="terminal_failed",
    )
    consumer = evaluate_structural_tier(
        _contract(),
        consumer_delivery_status="readback_failed",
    )

    assert technical.terminal_class == TECHNICAL_FAILURE
    assert technical.publication_allowed is False
    assert consumer.terminal_class == CONSUMER_DELIVERY_FAILURE
    assert consumer.publication_allowed is False


def test_unknown_terminal_claim_fails_closed():
    result = evaluate_structural_tier(
        _contract(),
        claimed_terminal_class="model_confident",
    )

    assert "terminal_class_invalid:model_confident" in result.violations
    assert result.publication_allowed is False


def test_route_boundary_is_honest_non_attribution_not_technical_failure():
    result = evaluate_structural_tier(
        _contract(evaluator_status="refuted", refs=False),
        terminal_error_code="business_route_unresolved",
    )

    assert result.terminal_class == HONEST_NON_ATTRIBUTION
