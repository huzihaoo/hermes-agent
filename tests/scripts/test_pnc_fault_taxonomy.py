"""Real-function regression tests for the fail-closed RCA fault taxonomy."""

import pytest

from scripts import pnc_fault_taxonomy as tax


@pytest.mark.parametrize(
    ("kind", "lane", "route"),
    [
        (
            "translate_workdir_permission",
            tax.INFRA_SELF_HEALABLE,
            tax.INFRA_REMEDIATION_HOLD,
        ),
        (
            "frame_id_unresolved_after_auto_discovery",
            tax.NEEDS_HUMAN_INPUT,
            tax.INTERNAL_BACKLOG,
        ),
        (
            "html_capability_payload_mismatch",
            tax.HARD_DEFECT,
            tax.INTERNAL_ALERT,
        ),
        ("service_provenance_unavailable", tax.HARD_DEFECT, tax.INTERNAL_ALERT),
        ("delivery_lineage_unavailable", tax.HARD_DEFECT, tax.INTERNAL_ALERT),
        ("viz_mcap_build_failed", tax.HARD_DEFECT, tax.INTERNAL_ALERT),
    ],
)
def test_named_production_blockers_have_exactly_one_lane(kind, lane, route):
    decision = tax.decide_failure({"kind": kind})

    assert decision.known is True
    assert decision.raw_code == kind
    assert decision.terminal_error_code == kind
    assert decision.lane == lane
    assert decision.internal_route == route


def test_remote_event_not_found_requires_self_proving_parse_audit():
    reference_sha256 = "a" * 64
    blocker = {
        "kind": "remote_event_not_found",
        "retryable": False,
        "audit": {
            "parse_attempts": [
                {
                    "attempt_id": "parse-attempt-1",
                    "parser": "remote_event_reader",
                    "status": "parsed",
                    "reference_sha256": reference_sha256,
                }
            ],
            "data_sources": [
                {
                    "source_id": "data-source-1",
                    "source_kind": "pdcl_event",
                    "status": "not_found",
                    "reference_sha256": reference_sha256,
                }
            ],
            "results": [
                {
                    "attempt_id": "parse-attempt-1",
                    "source_id": "data-source-1",
                    "status": "not_found",
                    "returned_count": 0,
                    "reference_sha256": reference_sha256,
                }
            ],
        },
    }

    decision = tax.decide_failure(blocker, require_complete=True)

    assert decision.known is True
    assert decision.terminal_error_code == "remote_event_not_found"
    assert decision.lane == tax.NEEDS_HUMAN_INPUT
    assert set(decision.audit) == {"parse_attempts", "data_sources", "results"}


def test_remote_event_without_audit_fails_closed_to_internal_alert():
    decision = tax.decide_failure({"kind": "remote_event_not_found"})

    assert decision.terminal_error_code == "remote_event_not_found"
    assert decision.lane == tax.HARD_DEFECT
    assert decision.internal_route == tax.INTERNAL_ALERT
    assert decision.known is False
    with pytest.raises(tax.TaxonomyContractError):
        tax.decide_failure({"kind": "remote_event_not_found"}, require_complete=True)


@pytest.mark.parametrize(
    "audit",
    [
        {
            "parse_attempts": [{}],
            "data_sources": [{}],
            "results": [{}],
        },
        {
            "parse_attempts": [
                {
                    "attempt_id": "parse-attempt-1",
                    "parser": "remote_event_reader",
                    "status": "parsed",
                    "reference_sha256": "a" * 64,
                }
            ],
            "data_sources": [
                {
                    "source_id": "data-source-1",
                    "source_kind": "pdcl_event",
                    "status": "not_found",
                    "reference_sha256": "a" * 64,
                }
            ],
            "results": [
                {
                    "attempt_id": "other-attempt",
                    "source_id": "data-source-1",
                    "status": "not_found",
                    "returned_count": 0,
                    "reference_sha256": "a" * 64,
                }
            ],
        },
        {
            "parse_attempts": [
                {
                    "attempt_id": "opaque-event-id",
                    "parser": "remote_event_reader",
                    "status": "parsed",
                    "reference_sha256": "a" * 64,
                }
            ],
            "data_sources": [
                {
                    "source_id": "data-source-1",
                    "source_kind": "pdcl_event",
                    "status": "available",
                    "reference_sha256": "b" * 64,
                }
            ],
            "results": [
                {
                    "attempt_id": "opaque-event-id",
                    "source_id": "data-source-1",
                    "status": "empty",
                    "returned_count": 1,
                    "reference_sha256": "c" * 64,
                }
            ],
        },
    ],
)
def test_remote_event_malformed_or_uncorrelated_audit_is_rejected(audit):
    decision = tax.decide_failure({
        "kind": "remote_event_not_found",
        "retryable": False,
        "audit": audit,
    })

    assert decision.known is False
    assert decision.lane == tax.HARD_DEFECT
    assert decision.internal_route == tax.INTERNAL_ALERT
    with pytest.raises(tax.TaxonomyContractError):
        tax.decide_failure(
            {"kind": "remote_event_not_found", "retryable": False, "audit": audit},
            require_complete=True,
        )


def _valid_remote_event_audit():
    reference_sha256 = "a" * 64
    return {
        "parse_attempts": [
            {
                "attempt_id": "parse-attempt-1",
                "parser": "remote_event_reader",
                "status": "parsed",
                "reference_sha256": reference_sha256,
            }
        ],
        "data_sources": [
            {
                "source_id": "data-source-1",
                "source_kind": "pdcl_event",
                "status": "not_found",
                "reference_sha256": reference_sha256,
            }
        ],
        "results": [
            {
                "attempt_id": "parse-attempt-1",
                "source_id": "data-source-1",
                "status": "not_found",
                "returned_count": 0,
                "reference_sha256": reference_sha256,
            }
        ],
    }


@pytest.mark.parametrize(
    ("mutate", "expected_error"),
    [
        (
            lambda audit: audit["parse_attempts"][0].update(extra="producer-trusted"),
            "remote_event_not_found_parse_attempts_invalid",
        ),
        (
            lambda audit: audit["parse_attempts"][0].update(parser="event_url_v2"),
            "remote_event_not_found_parse_attempts_invalid",
        ),
        (
            lambda audit: audit["data_sources"][0].update(source_kind="pdcl_prod"),
            "remote_event_not_found_data_sources_invalid",
        ),
        (
            lambda audit: audit["data_sources"][0].update(status="empty"),
            "remote_event_not_found_data_sources_invalid",
        ),
        (
            lambda audit: audit["results"][0].update(reference_sha256="b" * 64),
            "remote_event_not_found_reference_sha256_mismatch",
        ),
        (
            lambda audit: audit["results"][0].update(status="empty"),
            "remote_event_not_found_results_invalid",
        ),
        (
            lambda audit: audit["results"][0].update(returned_count=1),
            "remote_event_not_found_results_invalid",
        ),
    ],
)
def test_remote_event_audit_contract_is_independently_validated(mutate, expected_error):
    audit = _valid_remote_event_audit()
    mutate(audit)

    decision = tax.decide_failure({
        "kind": "remote_event_not_found",
        "retryable": False,
        "audit": audit,
    })

    assert decision.known is False
    assert decision.lane == tax.HARD_DEFECT
    assert expected_error in decision.contract_errors


def test_unknown_injection_becomes_taxonomy_gap_and_gate_is_nonzero():
    decision = tax.decide_failure({"kind": "Future VM / Wild Error", "retryable": True})

    assert decision.terminal_error_code == "taxonomy_gap:future_vm_wild_error"
    assert decision.lane == tax.HARD_DEFECT
    assert decision.internal_route == tax.INTERNAL_ALERT
    assert decision.known is False
    with pytest.raises(tax.TaxonomyContractError) as raised:
        tax.decide_failure(
            {"kind": "Future VM / Wild Error", "retryable": True},
            require_complete=True,
        )
    assert raised.value.decision.terminal_error_code == decision.terminal_error_code


def test_missing_blocker_never_collapses_to_unclassified():
    decision = tax.decide_failure(None)

    assert decision.terminal_error_code == "taxonomy_gap:missing_blocker"
    assert "unclassified" not in decision.terminal_error_code
    assert decision.lane == tax.HARD_DEFECT


def test_explicit_classification_conflict_is_a_taxonomy_gap():
    decision = tax.decide_failure({
        "kind": "translate_workdir_permission",
        "fault_class": tax.NEEDS_HUMAN_INPUT,
    })

    assert decision.terminal_error_code == "taxonomy_gap:translate_workdir_permission"
    assert decision.contract_errors == ("explicit_fault_class_conflict",)
    assert decision.lane == tax.HARD_DEFECT


def test_retryable_is_validation_not_an_unknown_kind_tiebreaker():
    unknown = tax.decide_failure({"kind": "unknown_retryable", "retryable": True})
    conflict = tax.decide_failure({
        "kind": "html_capability_payload_mismatch",
        "retryable": True,
    })

    assert unknown.lane == tax.HARD_DEFECT
    assert unknown.terminal_error_code == "taxonomy_gap:unknown_retryable"
    assert conflict.lane == tax.HARD_DEFECT
    assert "retryable_contract_conflict" in conflict.contract_errors


def test_data_gate_without_blocker_is_a_known_internal_backlog():
    decision = tax.decide_failure(None, gate_decision="need_download")

    assert decision.terminal_error_code == "need_source_or_evidence"
    assert decision.lane == tax.NEEDS_HUMAN_INPUT
    assert decision.internal_route == tax.INTERNAL_BACKLOG


@pytest.mark.parametrize("gate", ["need_user_data", "need_input", "need_keyframe"])
def test_stable_user_input_gate_names_are_not_treated_as_unknown_blockers(gate):
    decision = tax.decide_failure(None, gate_decision=gate)

    assert decision.known is True
    assert decision.lane == tax.NEEDS_HUMAN_INPUT


def test_remediation_is_same_generation_stage_specific():
    remediation = tax.remediation_for({"kind": "translate_workdir_permission"})

    assert remediation == {
        "op": "normalize_workdir_ownership",
        "detail": "normalize the pipeline-owned translate workdir, then retry",
        "resume_from_stage": "s3b_translate",
    }
    assert tax.TERMINAL_FALLBACK_SECONDS == 1800
