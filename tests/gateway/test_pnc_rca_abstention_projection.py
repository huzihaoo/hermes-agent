import builtins

import pytest

from gateway.pnc_rca_abstention_projection import (
    RcaEvidenceProjectionError,
    UNMATERIALIZED_FAILURE_MESSAGES,
    project_materialized_evaluator_evidence,
    project_unmaterialized_case_anchor,
)


def _forbid_open(*_args, **_kwargs):
    raise AssertionError("projection must not perform I/O")


def test_materialized_projection_preserves_refutation_without_defect_label(monkeypatch):
    monkeypatch.setattr(builtins, "open", _forbid_open)
    report = {
        "rca_evaluators": [
            {
                "key": "lcc_path",
                "domain": "lcc",
                "pattern": "lateral_offset",
                "status": "refuted",
                "window": {"start_us": 10, "end_us": 20},
                "checks": [
                    {
                        "thresholds": {"max_offset_m": 0.2},
                        "evidence": {"fields": ["lateral_offset_m"]},
                    }
                ],
                "evidence_refs": [{"evidence": "offset stayed below 0.2m"}],
                "defect": "must not cross projection boundary",
            }
        ]
    }

    result = project_materialized_evaluator_evidence(report)

    entry = result["evaluators"][0]
    assert entry["status"] == "refuted"
    assert entry["evidence"] == ["offset stayed below 0.2m"]
    assert entry["window"] == {"start_us": 10, "end_us": 20}
    assert "defect" not in entry
    assert report["rca_evaluators"][0]["defect"] == "must not cross projection boundary"


def test_materialized_projection_keeps_exact_missing_fields_without_synthesis():
    result = project_materialized_evaluator_evidence({
        "rca_evaluators": [
            {
                "key": "aeb",
                "status": "need_fields",
                "missing_fields": ["object_speed_mps", "aeb_state"],
                "checks": [],
                "evidence_refs": [],
            }
        ]
    })

    entry = result["evaluators"][0]
    assert entry["missing_fields"] == ["object_speed_mps", "aeb_state"]
    assert "checks" not in entry
    assert "evidence" not in entry
    assert entry["source_field_absent"] == ["domain", "pattern"]


@pytest.mark.parametrize("failure_class", sorted(UNMATERIALIZED_FAILURE_MESSAGES))
def test_unmaterialized_projection_keeps_each_failure_class_distinct(failure_class):
    result = project_unmaterialized_case_anchor({
        "input_materialized": False,
        "failure_class": failure_class,
        "frame_lookup": {"management_timestamp": 1_783_841_476_000_000},
        "marker_time": "2026-07-30T10:00:00+08:00",
        "event_uuid": "event-123",
    })

    assert result["failure_class"] == failure_class
    assert result["message"] == UNMATERIALIZED_FAILURE_MESSAGES[failure_class]
    assert result["anchors"] == {
        "management_timestamp": 1_783_841_476_000_000,
        "marker_time": "2026-07-30T10:00:00+08:00",
        "event_uuid": "event-123",
    }
    assert set(result) == {
        "schema_version",
        "input_materialized",
        "failure_class",
        "message",
        "anchors",
    }


@pytest.mark.parametrize(
    "extra",
    [
        {"foxglove_url": "https://foxglove.dev/view"},
        {"report_url": "https://reports.example.test/rca"},
        {"confidence": "high"},
        {"conclusion": "planner fault"},
        {"rca_evaluators": [{"status": "supported"}]},
    ],
)
def test_unmaterialized_projection_rejects_disclosures_and_materialized_evidence(extra):
    case = {
        "input_materialized": False,
        "failure_class": "remote_event_not_found",
    }
    case.update(extra)

    with pytest.raises(RcaEvidenceProjectionError):
        project_unmaterialized_case_anchor(case)


def test_unmaterialized_projection_requires_explicit_unmaterialized_input():
    with pytest.raises(
        RcaEvidenceProjectionError, match="unmaterialized_input_required"
    ):
        project_unmaterialized_case_anchor({"failure_class": "remote_event_not_found"})
