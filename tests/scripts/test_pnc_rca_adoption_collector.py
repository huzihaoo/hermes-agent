from __future__ import annotations

import pytest

from scripts import pnc_business_metrics
from scripts import pnc_quality_metrics
from scripts import pnc_rca_adoption_collector as collector


OBSERVED_AT_MS = 1785250005000
OBSERVED_AT = "2026-07-29T13:26:45Z"


def _manifest() -> dict:
    return {
        "schema_version": collector.MANIFEST_SCHEMA_VERSION,
        "observed_at_ms": OBSERVED_AT_MS,
        "records": [
            {
                "business_key": "g1q3-generation-1",
                "project_key": "t03o4q",
                "work_item_id": "7048004715",
                "generation": 1,
                "conclusion_time_ms": 1785250000000,
            }
        ],
    }


class _Adapter:
    def __init__(self, *, fail: bool = False):
        self.fail = fail
        self.calls = []

    def read_generation_adoption(self, project_key, work_item_id, **kwargs):
        self.calls.append((project_key, work_item_id, kwargs))
        if self.fail:
            return {
                "success": False,
                "permanent": True,
                "error_code": "meegle_read_failed",
            }
        return {
            "success": True,
            "source": "official_meegle_api",
            "scope": {
                "project_key": project_key,
                "work_item_id": work_item_id,
            },
            "field_key": "field_b23cb8",
            "generation": kwargs["generation"],
            "start_ms": kwargs["conclusion_time_ms"],
            "end_ms": OBSERVED_AT_MS,
            "window_semantics": "closed_conclusion_to_observed_at",
            "status": "adopted",
            "explicit": True,
            "operation": {
                "field_key": "field_b23cb8",
                "operation_time": 1785250001000,
                "operator": "ou_reviewer",
                "operator_type": "user",
                "new": "rya79_oos",
            },
        }


def _observation(entry: str) -> dict:
    return {
        "record_id": f"record-{entry}",
        "pair_id": "delivery-1",
        "release_id": "release-1",
        "business_line": "g1q3_rca",
        "source_kind": (
            "kafka_workflow_event" if entry == "kafka" else "feishu_group_manual"
        ),
        "confidence_tier": "medium",
        "denominator_kind": "business",
        "e2e": {"status": "success"},
        "technical": {
            "delivery_status": "succeeded",
            "readback_status": "verified",
        },
        "attribution": {
            "outcome": "candidate",
            "owner_decision": "accepted",
        },
        "golden": {
            "evaluated": True,
            "false_high_confidence": False,
            "regression": False,
        },
        "signals": {
            "triage": {"kind": "lane", "expected_kind": "lane"},
            "gate": {"decision": "allow", "review_decision": "allow"},
        },
        "delivery_provenance": {
            "business_key": "g1q3-generation-1",
            "generation": 1,
            "project_key": "t03o4q",
            "work_item_id": "7048004715",
        },
    }


def test_collector_to_metrics_pipeline_preserves_generation_and_deduplicates_entry():
    adapter = _Adapter()
    batch = collector.collect_adoption_signals(_manifest(), adapter=adapter)
    rows = pnc_business_metrics.normalize_records([
        _observation("kafka"),
        _observation("feishu"),
    ])
    merged = pnc_quality_metrics.merge_adoption_batch(rows, batch)

    report = pnc_quality_metrics.build_daily_report(
        merged,
        observed_at=OBSERVED_AT,
        adoption_semantics_confirmed=True,
    )

    assert batch["ok"] is True
    assert batch["read_only"] is True
    assert batch["write_commands_performed"] == 0
    assert len(adapter.calls) == 1
    assert report["adoption_signal"]["states"]["adopted"] == 1
    assert report["adoption_signal"]["denominator"] == 1
    assert report["adoption_signal"]["rate_pct"] == 100.0


def test_collector_read_failure_is_structured_and_cannot_emit_a_rate():
    batch = collector.collect_adoption_signals(
        _manifest(),
        adapter=_Adapter(fail=True),
    )
    rows = pnc_business_metrics.normalize_records([
        _observation("kafka"),
        _observation("feishu"),
    ])
    merged = pnc_quality_metrics.merge_adoption_batch(rows, batch)

    report = pnc_quality_metrics.build_daily_report(
        merged,
        observed_at=OBSERVED_AT,
        strict=False,
        adoption_semantics_confirmed=True,
    )

    assert batch["ok"] is False
    assert batch["error_count"] == 1
    assert report["adoption_signal"]["status"] == "read_error"
    assert report["adoption_signal"]["rate_pct"] is None


def test_manifest_rejects_duplicate_generation_identity():
    manifest = _manifest()
    manifest["records"].append(dict(manifest["records"][0]))

    try:
        collector.normalize_manifest(manifest)
    except collector.AdoptionCollectorError as exc:
        assert exc.code == "adoption_manifest_identity_duplicate"
    else:
        raise AssertionError("duplicate generation must fail closed")


def test_metrics_merge_rejects_non_object_delivery_provenance():
    batch = collector.collect_adoption_signals(_manifest(), adapter=_Adapter())
    row = _observation("kafka")
    row["delivery_provenance"] = "not-an-object"

    with pytest.raises(pnc_business_metrics.MetricsValidationError) as error:
        pnc_quality_metrics.merge_adoption_batch([row], batch)

    assert error.value.code == "metrics_adoption_observation_identity_invalid"
