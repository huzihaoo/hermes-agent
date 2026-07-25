from __future__ import annotations

import json

import pytest

from gateway import pnc_rca_quality_oracle as oracle_module
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


def _release_registry(evaluator_id: str = "lane_geometry_quality") -> dict:
    return {
        "present": True,
        "valid": True,
        "low_tier_golden_ready": True,
        "evaluators": {
            evaluator_id: {
                "evaluator_id": evaluator_id,
                "status": "passed",
                "evaluator_source_sha256": "c" * 64,
                "positive_golden_sha256": "a" * 64,
                "negative_golden_sha256": "b" * 64,
                "test_receipt_sha256": "d" * 64,
            }
        },
    }


def test_release_registry_accepts_current_git_object_ids_and_tracks_red_suite(
    tmp_path,
):
    path = tmp_path / "registry.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": "pnc_rca_release_golden_registry_v1",
                "pipeline_commit": "a" * 40,
                "pipeline_tree": "b" * 40,
                "low_tier_suite": {
                    "status": "failing",
                    "positive_case_count": 1,
                    "negative_case_count": 1,
                    "receipt_sha256": "c" * 64,
                    "vm_path": "/mnt/tmp/w1/receipt.json",
                    "user_visible_path": (
                        "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/"
                        "tmp/w1/"
                    ),
                },
                "evaluators": [],
            }
        ),
        encoding="utf-8",
    )

    status = oracle_module.release_golden_registry_status(path)

    assert status["valid"] is True
    assert status["low_tier_golden_ready"] is False


def test_release_registry_rejects_malformed_git_object_id(tmp_path):
    path = tmp_path / "registry.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": "pnc_rca_release_golden_registry_v1",
                "pipeline_commit": "a" * 39,
                "pipeline_tree": "b" * 40,
                "low_tier_suite": {
                    "status": "passed",
                    "positive_case_count": 1,
                    "negative_case_count": 1,
                    "receipt_sha256": "c" * 64,
                    "vm_path": "/mnt/tmp/w1/receipt.json",
                    "user_visible_path": "//hfs1.minieye.tech/share/w1/",
                },
                "evaluators": [],
            }
        ),
        encoding="utf-8",
    )

    assert oracle_module.release_golden_registry_status(path)["valid"] is False


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
                "field_lineage": {
                    "schema_version": "g1q3_field_lineage_v2",
                    "fidelity_ok": True,
                    "status": "pass",
                },
                "viz_lineage": {
                    "schema_version": "g1q3_viz_lineage_v1",
                    "ok": True,
                    "status": "pass",
                },
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


def test_supported_attribution_requires_emitted_supported_key_and_full_structure(
    monkeypatch,
):
    monkeypatch.setattr(
        oracle_module,
        "release_golden_registry_status",
        lambda: _release_registry(),
    )
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


def test_summary_only_evidence_item_does_not_count_as_evidence_ref():
    contract = _contract()
    contract["public_result"]["evidence_summary"]["refs"] = [
        {"summary": "a narrative is not an evidence reference"}
    ]

    result = evaluate_structural_tier(contract)

    assert result.terminal_class == CANDIDATE_HYPOTHESIS
    assert result.facts.evidence_ref_count == 0


def test_missing_release_controlled_evaluator_goldens_prevents_high_tier():
    contract = _contract()

    result = evaluate_structural_tier(contract)

    assert result.terminal_class == CANDIDATE_HYPOTHESIS
    assert result.facts.golden_coverage_complete is False


def test_producer_self_attested_golden_hashes_cannot_unlock_high_tier():
    contract = _contract()
    contract["consumer_capability"]["golden_coverage"] = {
        "schema_version": "pnc_rca_evaluator_golden_coverage_v1",
        "evaluators": [
            {
                "evaluator_id": "lane_geometry_quality",
                "status": "passed",
                "positive_golden_sha256": "a" * 64,
                "negative_golden_sha256": "b" * 64,
            }
        ],
    }

    result = evaluate_structural_tier(contract)

    assert result.terminal_class == CANDIDATE_HYPOTHESIS
    assert result.facts.golden_covered_evaluator_keys == ()


def test_invalid_evidence_types_and_contradictory_viz_fail_closed():
    contract = _contract()
    evidence = contract["consumer_capability"]["evidence"]
    evidence["issue_frame_id"] = False
    evidence["focus_window"] = {"start_ts": False, "end_ts": False}
    evidence["viz_lineage"] = {
        "ok": False,
        "status": "completed",
        "errors": ["render_failed"],
    }

    result = evaluate_structural_tier(contract)

    assert result.terminal_class == CANDIDATE_HYPOTHESIS
    assert result.facts.issue_frame_present is False
    assert result.facts.focus_window_present is False
    assert result.facts.viz_lineage_complete is False


def test_contradictory_pass_boole_and_failure_statuses_are_not_complete():
    contract = _contract()
    evidence = contract["consumer_capability"]["evidence"]
    evidence["field_lineage"]["status"] = "failed"
    evidence["viz_lineage"].update(ok=True, status="failed", errors=[])

    result = evaluate_structural_tier(contract)

    assert result.terminal_class == CANDIDATE_HYPOTHESIS
    assert result.facts.field_lineage_complete is False
    assert result.facts.viz_lineage_complete is False


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(10**10000, id="huge-int"),
        pytest.param(float("nan"), id="nan"),
        pytest.param(float("inf"), id="positive-inf"),
        pytest.param(-float("inf"), id="negative-inf"),
    ],
)
def test_unbounded_or_nonfinite_focus_values_are_invalid_not_exceptions(value):
    contract = _contract()
    contract["consumer_capability"]["evidence"]["focus_window"] = {
        "start_ts": value,
        "end_ts": value,
    }

    result = evaluate_structural_tier(contract)

    assert result.terminal_class == CANDIDATE_HYPOTHESIS
    assert result.facts.focus_window_present is False


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


def test_candidate_publication_cannot_mix_in_non_attribution_wording():
    contract = _contract(refs=False, candidate=True)

    result = evaluate_structural_tier(
        contract,
        publication_text=(
            f"置信说明：{MEDIUM_TIER_DISCLAIMER}\n自动RCA未归因。"
        ),
    )

    assert result.terminal_class == HONEST_NON_ATTRIBUTION
    assert result.publication_allowed is False
    assert "honest_non_attribution_candidate_wording" in result.violations


def test_non_attribution_boundary_prevents_supported_promotion():
    contract = _contract()
    contract["public_result"]["evidence_boundary"] = ["自动RCA未归因：边界证据不足。"]

    result = evaluate_structural_tier(contract)

    assert result.terminal_class == HONEST_NON_ATTRIBUTION
    assert result.facts.explicit_non_attribution is True


@pytest.mark.parametrize(
    "error_code",
    [
        "business_route_unresolved",
        "business_profile_unsupported",
        "business_profile_adapter_not_ready",
    ],
)
def test_route_boundary_stays_honest_low_when_execution_is_terminal_failed(
    error_code,
):
    result = evaluate_structural_tier(
        {},
        execution_outcome="terminal_failed",
        terminal_error_code=error_code,
    )

    assert result.terminal_class == HONEST_NON_ATTRIBUTION
    assert result.confidence_tier == "low"


def test_serialized_approval_ready_without_human_decision_is_blocked():
    result = evaluate_structural_tier(
        _contract(refs=False, candidate=True),
        publication_text=(
            f"置信说明：{MEDIUM_TIER_DISCLAIMER}\n"
            '{"approval_ready": true}'
        ),
    )

    assert result.publication_allowed is False
    assert "approval_ready_without_human_decision" in result.violations


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
