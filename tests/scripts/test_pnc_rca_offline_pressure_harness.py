from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path
import sqlite3
import threading

import pytest

from scripts import pnc_rca_offline_pressure_harness as harness


def _config(**overrides) -> harness.HarnessConfig:
    base = harness.HarnessConfig(
        profile="retry-crash-25",
        total_cases=16,
        kafka_ratio=0.5,
        workers=4,
        failure_rate=0.125,
        timeout_rate=0.125,
        duplicate_rate=0.5,
        seed=20260713,
        high_watermark=5,
        resume_watermark=2,
        batch_size=4,
        poll_interval_ms=1,
        arrival_span_seconds=0.0,
        run_timeout_seconds=30.0,
    )
    return replace(base, **overrides)


def _harness_threads() -> list[str]:
    return [
        thread.name
        for thread in threading.enumerate()
        if thread.name.startswith("rca-offline-")
    ]


def test_profiles_cover_fixed_production_scenarios() -> None:
    plan = harness.build_plan(harness.HarnessConfig())

    assert harness.PROFILES["expected-50-day"].total_cases == 50
    assert harness.PROFILES["expected-50-day"].arrival_span_seconds == 86_400.0
    assert harness.PROFILES["burst-1000"].total_cases == 1_000
    assert set(plan["acceptance_scenarios"]["dual_source_fairness"]) == {
        "manual-flood",
        "kafka-flood",
    }
    assert harness.PROFILES["retry-crash-25"].failure_rate + harness.PROFILES[
        "retry-crash-25"
    ].timeout_rate == pytest.approx(0.25)
    assert plan["acceptance_scenarios"]["source_and_delivery_exact_once"] == [
        "duplicate-source",
        "delivery-exact-once",
    ]
    assert "RcaControlStore.claim_outbox/retry_outbox/complete_outbox" in plan[
        "store_contract"
    ]["public_apis"]
    assert "RcaDeliveryStore.claim_due_effect/mark_effect_write_started" in plan[
        "store_contract"
    ]["public_apis"]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("total_cases", 0),
        ("kafka_ratio", 1.1),
        ("workers", 0),
        ("workers", harness.MAX_WORKERS + 1),
        ("failure_rate", 0.8),
        ("high_watermark", 1),
        ("resume_watermark", 100),
        ("batch_size", 0),
        ("poll_interval_ms", 0),
        ("run_timeout_seconds", 0.5),
    ],
)
def test_config_rejects_unbounded_or_invalid_parameters(
    field: str, value: object
) -> None:
    changes = {field: value}
    if field == "failure_rate":
        changes["timeout_rate"] = 0.3
    with pytest.raises(ValueError):
        replace(_config(), **changes).validated()


def test_cases_are_deterministic_and_have_exact_source_count() -> None:
    config = _config(total_cases=31, kafka_ratio=0.35, duplicate_rate=0.4)

    first = harness.build_cases(config)
    second = harness.build_cases(config)

    assert first == second
    assert sum(case.source == "kafka" for case in first) == round(31 * 0.35)
    assert {case.issue_id for case in first} == {
        9_100_000_000 + index for index in range(31)
    }


def test_paths_are_confined_to_non_live_os_temporary_roots(tmp_path: Path) -> None:
    accepted = harness.validate_temporary_path(
        tmp_path / "control.sqlite3", kind="control"
    )
    assert accepted.is_absolute()

    with pytest.raises(ValueError, match="absolute"):
        harness.validate_temporary_path("relative.sqlite3", kind="control")
    with pytest.raises(ValueError, match="temporary root"):
        harness.validate_temporary_path(
            Path.home() / "rca-pressure.sqlite3", kind="control"
        )
    with pytest.raises(ValueError, match="live RCA runtime"):
        harness.validate_temporary_path(
            Path("/tmp/.hermes/runtime/pnc_agent/control.sqlite3"),
            kind="control",
        )


def test_real_stores_fault_recovery_exact_once_and_bounded_report(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "control.sqlite3"

    report = harness.run_harness(_config(), control_path=db_path)

    assert report["result"] == "pass"
    assert report["slo"]["passed"] is True
    assert all(report["slo"]["checks"].values())
    assert report["workload"]["unique_cases"] == 16
    assert report["workload"]["source_counts"] == {"kafka": 8, "manual": 8}
    assert report["reliability"]["idempotency"]["duplicate_source_replays"] > 0
    assert report["reliability"]["idempotency"][
        "duplicate_source_replays"
    ] == report["reliability"]["idempotency"]["idempotent_source_replays"]
    assert report["reliability"]["idempotency"][
        "duplicate_remote_effect_writes"
    ] == 0
    assert report["reliability"]["delivery"]["lease_timeouts_injected"] > 0
    assert report["reliability"]["delivery"]["lease_recoveries"] == report[
        "reliability"
    ]["delivery"]["lease_timeouts_injected"]
    assert report["backpressure"]["high_watermark_reached"] is True
    assert report["backpressure"]["max_outbox_backlog"] == 5
    assert report["circuit"]["dispatch_rounds_blocked_while_open"] == 1
    assert report["fairness"]["source_completed"] == {"kafka": 8, "manual": 8}
    assert report["queue_latency_ms"]["wall_admission_to_outbox_completion"][
        "p99"
    ] >= 0
    assert report["queue_latency_ms"]["virtual_offer_to_outbox_completion"]["p99"] >= 0
    assert report["resources"]["db_growth_bytes"] > 0
    assert report["resources"]["process_peak_rss_delta_bytes"] >= 0
    assert len(harness._canonical_json(report).encode()) <= harness.MAX_REPORT_BYTES
    assert _harness_threads() == []

    with sqlite3.connect(db_path) as connection:
        outbox_count = connection.execute(
            "SELECT COUNT(*) FROM rca_outbox WHERE status = 'completed'"
        ).fetchone()[0]
        effect_count = connection.execute(
            "SELECT COUNT(*) FROM rca_delivery_effects WHERE status = 'succeeded'"
        ).fetchone()[0]
    assert outbox_count == 16
    assert effect_count == report["reliability"]["delivery"]["effects"]


def test_smooth_expected_volume_does_not_require_burst_backpressure(
    tmp_path: Path,
) -> None:
    report = harness.run_harness(
        _config(
            profile="expected-50-day",
            total_cases=3,
            kafka_ratio=2 / 3,
            workers=1,
            failure_rate=0.0,
            timeout_rate=0.0,
            duplicate_rate=0.0,
            high_watermark=2,
            resume_watermark=0,
            batch_size=1,
            arrival_span_seconds=86_400.0,
        ),
        control_path=tmp_path / "smooth.sqlite3",
    )

    assert report["result"] == "pass"
    assert report["backpressure"]["high_watermark_reached"] is False
    assert report["slo"]["checks"]["backpressure_high_resume_observed"] is True


def test_deadline_fails_closed_and_executor_threads_are_joined(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ticks = iter((0.0, 0.0, 2.0))
    monkeypatch.setattr(
        harness.time,
        "perf_counter",
        lambda: next(ticks, 2.0),
    )

    with pytest.raises(RuntimeError, match="exceeded 1s during"):
        harness.run_harness(
            _config(total_cases=2, run_timeout_seconds=1.0),
            control_path=tmp_path / "deadline.sqlite3",
        )
    assert _harness_threads() == []


def test_output_is_private_bounded_and_no_clobber(tmp_path: Path) -> None:
    output = tmp_path / "report.json"
    payload = harness.build_plan(_config(total_cases=4))

    harness._write_output(str(output), payload)

    assert output.stat().st_mode & 0o777 == 0o600
    assert json.loads(output.read_text(encoding="utf-8")) == payload
    with pytest.raises(ValueError, match="already exists"):
        harness._write_output(str(output), payload)


def test_cli_plan_is_bounded_json(capsys: pytest.CaptureFixture[str]) -> None:
    result = harness.main(
        [
            "plan",
            "--profile",
            "burst-1000",
            "--workers",
            "3",
            "--run-timeout-seconds",
            "30",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert result == 0
    assert payload["mode"] == "plan"
    assert payload["planned_workload"]["unique_cases"] == 1_000
    assert payload["parameters"]["workers"] == 3
    assert len(harness._canonical_json(payload).encode()) <= harness.MAX_REPORT_BYTES
