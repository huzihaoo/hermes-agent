from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys

import pytest

from scripts import pnc_business_metrics as business_metrics
from scripts import pnc_quality_metrics as quality_metrics


OBSERVED_AT = "2026-07-26T00:00:00Z"


def _record(
    *,
    pair_id: str,
    entry: str,
    scope: str,
    tier: str,
    e2e: str = "success",
    delivery: str = "succeeded",
    readback: str = "verified",
    attribution: str = "owner_accepted",
    owner_decision: str = "accepted",
    false_high: bool = False,
    regression: bool = False,
    triage_kind: str = "lane",
    triage_expected_kind: str = "lane",
    gate_decision: str = "allow",
    gate_review_decision: str = "allow",
    coverage_count: int = 100,
    report_count: int = 200,
    field_write_count: int = 300,
) -> dict:
    source_kind = "kafka_workflow_event" if entry == "kafka" else "feishu_group_manual"
    return {
        "record_id": f"{pair_id}-{entry}",
        "pair_id": pair_id,
        "release_id": "release-20260726",
        "business_line": "g1q3_rca",
        "source_kind": source_kind,
        "confidence_tier": tier,
        "denominator_kind": scope,
        "e2e": {"status": e2e},
        "technical": {
            "delivery_status": delivery,
            "readback_status": readback,
        },
        "attribution": {
            "outcome": attribution,
            "owner_decision": owner_decision,
        },
        "golden": {
            "evaluated": True,
            "false_high_confidence": false_high,
            "regression": regression,
        },
        "signals": {
            "triage": {
                "kind": triage_kind,
                "expected_kind": triage_expected_kind,
            },
            "gate": {
                "decision": gate_decision,
                "review_decision": gate_review_decision,
            },
        },
        "coverage_count": coverage_count,
        "report_count": report_count,
        "field_write_count": field_write_count,
    }


def _clean_records() -> list[dict]:
    return [
        _record(
            pair_id="business-pair",
            entry="kafka",
            scope="business",
            tier="medium",
            attribution="candidate",
        ),
        _record(
            pair_id="business-pair",
            entry="feishu",
            scope="business",
            tier="medium",
            owner_decision="rejected",
            attribution="owner_rejected",
            triage_expected_kind="aeb",
        ),
        _record(
            pair_id="system-pair",
            entry="kafka",
            scope="system",
            tier="high",
            attribution="unsupported",
        ),
        _record(
            pair_id="system-pair",
            entry="feishu",
            scope="system",
            tier="high",
            e2e="failed",
            readback="failed",
            attribution="event_not_found",
            false_high=True,
            gate_review_decision="block",
        ),
    ]


def _group(report: dict, *, entry: str, tier: str) -> dict:
    return next(
        group
        for group in report["groups"]
        if group["dimensions"]["entry"] == entry
        and group["dimensions"]["confidence_tier"] == tier
    )


def test_daily_report_groups_four_axes_and_never_mixes_denominators() -> None:
    report = quality_metrics.build_daily_report(
        _clean_records(), observed_at=OBSERVED_AT
    )

    assert report["ok"] is True
    assert report["grouping"] == [
        "release",
        "business",
        "entry",
        "confidence_tier",
    ]
    assert len(report["groups"]) == 4

    business = _group(report, entry="kafka", tier="medium")
    assert business["dimensions"]["business"] == "g1q3-rca"
    assert business["denominators"] == {"business": 1, "system": 0}
    assert business["metrics"]["dual_entry_e2e_success"]["by_denominator"][
        "business"
    ] == {
        "numerator": 1,
        "denominator": 1,
        "rate_pct": 100.0,
    }
    assert (
        business["metrics"]["useful_attribution"]["by_denominator"]["business"][
            "numerator"
        ]
        == 1
    )
    assert (
        business["metrics"]["useful_attribution"]["by_denominator"]["system"][
            "denominator"
        ]
        == 0
    )

    system = _group(report, entry="kafka", tier="high")
    assert system["denominators"] == {"business": 0, "system": 1}
    # Each entry has its own denominator; the Feishu failure cannot pollute
    # Kafka's success rate.
    assert system["metrics"]["dual_entry_e2e_success"]["by_denominator"]["system"] == {
        "numerator": 1,
        "denominator": 1,
        "rate_pct": 100.0,
    }
    assert (
        system["metrics"]["technical_delivery_readback"]["by_denominator"]["system"][
            "numerator"
        ]
        == 1
    )
    assert (
        system["metrics"]["useful_attribution"]["by_denominator"]["business"][
            "denominator"
        ]
        == 0
    )

    for group in report["groups"]:
        for metric in group["metrics"].values():
            assert set(metric["by_denominator"]) == {"business", "system"}
            assert "denominator" not in metric, (
                "a mixed top-level denominator is forbidden"
            )


def test_unsupported_and_event_not_found_are_auxiliary_not_attribution_success() -> (
    None
):
    records = [
        _record(
            pair_id="excluded",
            entry="kafka",
            scope="business",
            tier="low",
            attribution="unsupported",
            owner_decision="pending",
        ),
        _record(
            pair_id="excluded",
            entry="feishu",
            scope="business",
            tier="low",
            attribution="event-not-found",
            owner_decision="pending",
        ),
    ]

    report = quality_metrics.build_daily_report(records, observed_at=OBSERVED_AT)
    kafka = _group(report, entry="kafka", tier="low")
    feishu = _group(report, entry="feishu", tier="low")

    assert (
        kafka["metrics"]["useful_attribution"]["by_denominator"]["business"][
            "denominator"
        ]
        == 0
    )
    assert (
        feishu["metrics"]["useful_attribution"]["by_denominator"]["business"][
            "denominator"
        ]
        == 0
    )
    assert report["auxiliary"]["attribution_exclusions"] == {
        "event_not_found": 1,
        "unsupported": 1,
    }


def test_auxiliary_counts_cannot_inflate_any_metric_denominator() -> None:
    records = [
        _record(
            pair_id="auxiliary",
            entry=entry,
            scope="business",
            tier="medium",
            coverage_count=10_000,
            report_count=20_000,
            field_write_count=30_000,
        )
        for entry in ("kafka", "feishu")
    ]

    report = quality_metrics.build_daily_report(records, observed_at=OBSERVED_AT)

    assert report["auxiliary"]["coverage_count"] == 20_000
    assert report["auxiliary"]["report_count"] == 40_000
    assert report["auxiliary"]["field_write_count"] == 60_000
    for group in report["groups"]:
        for metric in group["metrics"].values():
            assert (
                sum(
                    bucket["denominator"]
                    for bucket in metric["by_denominator"].values()
                )
                <= 1
            )


def test_three_former_todo_signals_have_clean_fields_and_rates() -> None:
    report = quality_metrics.build_daily_report(
        _clean_records(), observed_at=OBSERVED_AT
    )

    inventory = {item["name"]: item for item in report["signal_inventory"]}
    assert set(inventory) == {
        "triage_accuracy_kind_distribution",
        "rca_adoption_rate",
        "gate_consistency_rate",
    }
    assert all(item["status"] == "have" for item in inventory.values())
    assert all(item["clean_fields"] for item in inventory.values())

    business = _group(report, entry="kafka", tier="medium")
    assert (
        business["signals"]["triage_accuracy_kind_distribution"]["by_denominator"][
            "business"
        ]["rate_pct"]
        == 100.0
    )
    assert (
        business["signals"]["rca_adoption_rate"]["by_denominator"]["business"][
            "rate_pct"
        ]
        == 100.0
    )

    system = _group(report, entry="feishu", tier="high")
    assert (
        system["signals"]["gate_consistency_rate"]["by_denominator"]["system"][
            "rate_pct"
        ]
        == 0.0
    )
    assert system["metrics"]["false_high_confidence_no_regression"]["failure_counts"][
        "system"
    ] == {"false_high_confidence": 1, "regression": 0}


def test_markdown_keeps_auxiliary_in_a_separate_section() -> None:
    report = quality_metrics.build_daily_report(
        _clean_records(), observed_at=OBSERVED_AT
    )

    rendered = quality_metrics.render_markdown(report)

    assert "release × business × entry × confidence_tier" in rendered
    assert "## Auxiliary (not metric denominators)" in rendered
    assert "coverage_count:" in rendered


def test_normalizer_rejects_an_implicit_denominator_scope() -> None:
    row = _record(
        pair_id="implicit-scope",
        entry="kafka",
        scope="business",
        tier="medium",
    )
    row.pop("denominator_kind")

    with pytest.raises(business_metrics.MetricsValidationError) as error:
        business_metrics.normalize_record(row)

    assert error.value.code == "metrics_dimension_required"


def test_negative_mixed_scope_pair_injection_exits_nonzero(tmp_path: Path) -> None:
    injected = [
        _record(
            pair_id="mixed-scope",
            entry="kafka",
            scope="business",
            tier="medium",
        ),
        _record(
            pair_id="mixed-scope",
            entry="feishu",
            scope="system",
            tier="medium",
        ),
    ]
    path = tmp_path / "mixed-scope-injection.json"
    path.write_text(json.dumps(injected), encoding="utf-8")

    completed = subprocess.run(
        [
            sys.executable,
            str(Path(quality_metrics.__file__).resolve()),
            "--input",
            str(path),
            "--observed-at",
            OBSERVED_AT,
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode != 0
    failure = json.loads(completed.stderr)
    assert failure["ok"] is False
    assert failure["code"] == "metrics_report_not_clean"
    assert "metrics_pair_denominator_mixed" in failure["detail"]
