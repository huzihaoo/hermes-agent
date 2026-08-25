from __future__ import annotations

from scripts import pnc_rca_capacity_probe as probe


def _report(*, authorization_ready: bool = True) -> dict:
    return {
        "resource_class": "rca_prod",
        "ok_for_rca_prod_submit": True,
        "rca_prod_reasons": [],
        "warnings": [],
        "rca_prod_snapshot_sha256": "a" * 64,
        "rca_prod_snapshot": {"observed_at": "2026-08-25T00:00:00+00:00"},
        "rca_capacity_authorization": {
            "authorization_ready": authorization_ready,
            "status": "valid" if authorization_ready else "missing",
            "policy_version": "capacity-policy-v1",
            "max_concurrency": 4,
            "reason_codes": [] if authorization_ready else ["receipt_missing"],
        },
    }


def _scheduler() -> dict:
    return {
        "ready": True,
        "capacity_profile": probe.CAPACITY_PROFILE,
        "policy_version": "capacity-policy-v1",
        "max_concurrency": 4,
    }


def _measurement(concurrency: int, throughput: float) -> dict:
    count = 8
    return {
        "concurrency": concurrency,
        "sample_count": count,
        "successful_count": count,
        "duration_seconds": 100.0,
        "throughput_per_second": throughput,
        "p50_seconds": 10.0,
        "p95_seconds": 15.0,
        "kafka_lag_before": 8,
        "kafka_lag_after": 0,
        "offset_commits": count,
        "outbox_oldest_age_before_seconds": 20.0,
        "outbox_oldest_age_after_seconds": 0.0,
        "vm_started": count,
        "vm_completed": count,
        "collector_completed": count,
        "delivery_completed": count,
        "duplicate_count": 0,
        "lost_count": 0,
        "failure_codes": [],
        "max_cpu_ratio": 0.7,
        "max_rss_ratio": 0.7,
        "max_load_ratio": 0.7,
        "min_swap_free_ratio": 0.3,
        "max_storage_ratio": 0.7,
        "same_business_key_serial": True,
        "different_business_keys_only": True,
    }


def test_static_resource_green_does_not_replace_steady_authorization():
    receipt = probe.build_capacity_receipt(_report(authorization_ready=False))

    assert receipt["status"] == "no_go"
    assert receipt["C_safe"] is None
    assert {stage["status"] for stage in receipt["sequence"]} == {"not_run"}
    assert receipt["sequence"][0]["reason"] == "steady_capacity_authorization_not_ready"
    assert receipt["external_side_effects"] == {"vm_submissions": 0, "kafka_commits": 0}


def test_capacity_measurements_must_pass_in_c1_c2_c4_order():
    measurements = [
        _measurement(1, 1.0),
        _measurement(2, 1.7),
        _measurement(4, 3.3),
    ]

    receipt = probe.build_capacity_receipt(
        _report(), scheduler_evidence=_scheduler(), measurements=measurements
    )

    assert receipt["status"] == "measured"
    assert receipt["T1"] == 1.0
    assert receipt["C_safe"] == 4
    assert [stage["status"] for stage in receipt["sequence"]] == [
        "passed",
        "passed",
        "passed",
    ]


def test_failed_c2_prevents_c4_measurement():
    measurements = [
        _measurement(1, 1.0),
        _measurement(2, 1.5),
        _measurement(4, 4.0),
    ]

    receipt = probe.build_capacity_receipt(
        _report(), scheduler_evidence=_scheduler(), measurements=measurements
    )

    assert receipt["status"] == "measured_no_go_for_next_level"
    assert receipt["C_safe"] == 1
    assert receipt["sequence"][1]["gate_failures"] == [
        "throughput_scaling_below_80pct"
    ]
    assert receipt["sequence"][2] == {
        "concurrency": 4,
        "status": "not_run",
        "reason": "measurement_not_run",
    }


def test_authorization_and_scheduler_max_concurrency_must_match():
    report = _report()
    report["rca_capacity_authorization"]["max_concurrency"] = 1

    receipt = probe.build_capacity_receipt(
        report, scheduler_evidence=_scheduler(), measurements=[]
    )

    assert receipt["status"] == "no_go"
    assert receipt["sequence"][0]["reason"] == "scheduler_evidence_not_ready"
