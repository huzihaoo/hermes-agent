from __future__ import annotations

from dataclasses import replace
import json
import hashlib
from datetime import timedelta
from pathlib import PurePosixPath
import sqlite3
from types import SimpleNamespace

import pytest

from gateway.pnc_rca_delivery_contract import (
    TERMINAL_FALLBACK_CONTRACT_SCHEMA_VERSION,
    TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION,
)
from gateway.pnc_rca_delivery_store import RcaDeliveryStore
from scripts import pnc_rca_delivery_collector as collector
from scripts.pnc_foxglove_delivery import canonical_viz_mcap_path
from tests.gateway.test_pnc_rca_delivery_store import NOW, _control, _delivery
from tests.gateway.test_pnc_rca_w3_snapshot import _runtime_authority


def _config_env(tmp_path) -> dict[str, str]:
    return {
        "HERMES_RCA_DELIVERY_COLLECTOR_ENABLED": "true",
        "HERMES_RCA_DELIVERY_COLLECTOR_CONTROL_DB_PATH": str(
            tmp_path / "control.sqlite3"
        ),
        "HERMES_RCA_DELIVERY_COLLECTOR_HEALTH_PATH": str(tmp_path / "health.json"),
        "HERMES_RCA_DELIVERY_COLLECTOR_SSH_MINI_AGENT": "/safe/ssh-mini-agent",
        "HERMES_RCA_DELIVERY_COLLECTOR_ARTIFACT_READ_TIMEOUT_SECONDS": "30",
        "HERMES_RCA_DELIVERY_COLLECTOR_LEASE_SECONDS": "60",
        "HERMES_RCA_DELIVERY_COLLECTOR_CAPACITY_SAMPLE_ENABLED": "true",
        "HERMES_RCA_DELIVERY_COLLECTOR_ACTIVATION_REQUIRED": "true",
    }


def test_remote_bundle_reader_uses_formal_viz_publication_root():
    submission_key = "g1q3-rca-s1-" + "a" * 64
    formal_root = str(PurePosixPath(canonical_viz_mcap_path(submission_key)).parent)
    script = collector._remote_bundle_script(submission_key)

    assert f"FORMAL_VIZ_ROOT = {formal_root!r}" in script
    assert "FORMAL_VIZ_ROOT = posixpath.normpath(ROOT)" not in script


def test_remote_bundle_reader_scans_sealed_public_artifacts_for_banned_phrases():
    script = collector._remote_bundle_script("g1q3-rca-s1-" + "c" * 64)

    assert repr(tuple(collector.BANNED_PUBLIC_PHRASES)) in script
    assert "raise RuntimeError('public_artifact_banned_phrase')" in script
    assert "json.dumps(report_data, ensure_ascii=False, sort_keys=True)" in script
    assert "reject_banned_public_phrase(text)" in script
    assert "except RuntimeError:\n            report_data = {}" not in script
    assert "report_data_missing" in collector._EVENTUAL_ARTIFACT_CODES


def test_viz_surface_errors_retry_internally_instead_of_becoming_user_results():
    assert (
        "viz_publication_missing" in collector._RETRYABLE_INFRASTRUCTURE_ARTIFACT_CODES
    )
    assert (
        "viz_publication_path_invalid"
        in collector._RETRYABLE_INFRASTRUCTURE_ARTIFACT_CODES
    )
    script = collector._remote_bundle_script("g1q3-rca-s1-" + "b" * 64)
    assert "if viz_publication:" in script


def test_config_exposes_capacity_sampling_without_restoring_activation_gate(tmp_path):
    config = collector.CollectorConfig.from_env(
        _config_env(tmp_path), hermes_home=tmp_path
    )

    public = config.public_dict()
    assert "activation_required" not in public
    assert public["capacity_sample_enabled"] is True
    assert public["capacity_sample_batch_size"] == 20


def test_collect_batch_collects_delivery_then_capacity_samples():
    instance = collector.DeliveryCollector.__new__(collector.DeliveryCollector)
    instance.config = SimpleNamespace(batch_size=3)
    instance.stats = collector.CollectorStats()
    instance.backfill = lambda: 0
    outcomes = iter([
        collector.CollectOutcome(status="running"),
        collector.CollectOutcome(status="idle"),
    ])
    instance.collect_one = lambda: next(outcomes)
    capacity_calls = []
    instance.collect_capacity_samples = lambda: capacity_calls.append(True)

    result = instance.collect_batch()

    assert [item.status for item in result] == ["running", "idle"]
    assert capacity_calls == [True]


def test_collector_stats_expose_capacity_counters_without_activation_counter():
    public = collector.asdict(collector.CollectorStats())
    assert "activation_blocked" not in public
    assert public["capacity_scanned"] == 0
    assert public["capacity_eligible"] == 0
    assert public["capacity_appended"] == 0
    assert public["capacity_rejected"] == 0
    assert public["capacity_frozen"] == 0
    assert public["capacity_last_error"] == ""
    assert public["stale_lease"] == 0


def test_capacity_observation_error_does_not_mark_delivery_unhealthy(tmp_path):
    config = collector.CollectorConfig.from_env(
        _config_env(tmp_path), hermes_home=tmp_path
    )
    reporter = object.__new__(collector.HealthReporter)
    reporter.config = config
    reporter.store = SimpleNamespace(health=lambda **_kwargs: {"ok": True})
    reporter.started_at = collector._utc_iso()
    reporter.runtime_identity = SimpleNamespace(to_dict=lambda: {})
    reporter._remote_css_parser_receipt = {"status": "ok"}
    reporter._remote_css_parser_error = ""
    reporter._remote_css_parser_observed_at = collector._utc_now()

    stats = collector.CollectorStats(
        capacity_last_error="rca_capacity_vm_measurement_time_invalid"
    )
    reporter.write(state="idle", stats=stats, refresh_dependencies=False)

    payload = json.loads(config.health_path.read_text(encoding="utf-8"))
    assert payload["healthy"] is True
    assert payload["capacity_samples"]["observation_healthy"] is False
    assert payload["capacity_samples"]["blocks_delivery_health"] is False


def _remote_event_blocker():
    reference_sha256 = "a" * 64
    return {
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


def _real_terminal_collector(
    tmp_path,
    *,
    clock,
    blocker=None,
    status_reader=None,
    failure_receipt_reader=None,
    infra_remediation_runner=None,
):
    _control(tmp_path)
    env = _config_env(tmp_path)
    env["HERMES_RCA_DELIVERY_COLLECTOR_CAPACITY_SAMPLE_ENABLED"] = "false"
    config = collector.CollectorConfig.from_env(env, hermes_home=tmp_path)
    instance = collector.DeliveryCollector(
        store=RcaDeliveryStore(tmp_path / "control.sqlite3"),
        config=config,
        status_reader=status_reader
        or (
            lambda task_id: {
                "success": True,
                "task_id": task_id,
                "state": "failed",
                "summary": "private VM failure",
            }
        ),
        failure_receipt_reader=failure_receipt_reader
        or (
            lambda claim: {
                "schema_version": collector.FAILURE_RECEIPT_SCHEMA_VERSION,
                "task_id": claim.task_id,
                "status": "pipeline_not_successful",
                "pipeline_status": "needs_fix",
                "pipeline_stage": "s6_report",
                "blocker": blocker,
            }
        ),
        infra_remediation_runner=infra_remediation_runner,
        now=lambda: clock[0],
        lease_owner="taxonomy-real-path",
    )
    assert instance.backfill() == 1
    return instance


def _age_work_start(instance, *, seconds):
    started_at = (NOW - timedelta(seconds=seconds)).isoformat()
    with sqlite3.connect(instance.store.db_path) as conn:
        conn.execute(
            "UPDATE business_triggers SET created_at = ?",
            (started_at,),
        )
        conn.execute(
            "UPDATE rca_outbox SET created_at = ?, retry_window_started_at = ?",
            (started_at, started_at),
        )


def test_snapshot_required_collector_quarantines_missing_snapshot_without_effect(
    tmp_path,
):
    status_calls = []
    instance = _real_terminal_collector(
        tmp_path,
        clock=[NOW],
        status_reader=lambda task_id: status_calls.append(task_id),
    )
    instance.config = replace(
        instance.config,
        w3_snapshot_read_mode="snapshot_required",
        w3_snapshot_authority=_runtime_authority(),
    )

    outcome = instance.collect_one()

    assert outcome.status == "quarantined"
    assert outcome.error_code == "w3_execution_snapshot_missing"
    assert status_calls == []
    assert instance.store.list_rows("rca_delivery_effects") == []
    assert instance.store.list_rows("rca_delivery_jobs") == []
    assert instance.store.list_rows("rca_execution_watch")[0]["state"] == (
        "quarantined"
    )


@pytest.mark.parametrize(
    ("blocker", "lane", "route_kind", "owner", "error_code"),
    [
        (
            {"kind": "translate_workdir_permission", "retryable": True},
            "infra_self_healable",
            "infra_remediation_hold",
            "rca-infra",
            "translate_workdir_permission",
        ),
        (
            _remote_event_blocker(),
            "needs_human_input",
            "internal_backlog",
            "rca-triage",
            "remote_event_not_found",
        ),
        (
            {"kind": "html_capability_payload_mismatch", "retryable": False},
            "hard_defect",
            "internal_alert",
            "rca-engineering",
            "html_capability_payload_mismatch",
        ),
    ],
)
def test_all_failure_lanes_are_silent_until_admission_deadline_then_oracle_low(
    tmp_path, blocker, lane, route_kind, owner, error_code
):
    clock = [NOW]
    instance = _real_terminal_collector(
        tmp_path,
        blocker=blocker,
        clock=clock,
    )

    held = instance.collect_one()

    assert held.status == "failure_hold"
    assert held.error_code == error_code
    assert instance.store.list_rows("rca_delivery_jobs") == []
    assert instance.store.list_rows("rca_delivery_effects") == []
    [route] = instance.store.list_rows("rca_failure_routes")
    assert route["lane"] == lane
    assert route["route_kind"] == route_kind
    assert route["owner"] == owner
    assert route["retry_count"] == 1
    assert route["observation_count"] == 1
    assert route["next_retry_at"]
    watch = instance.store.list_rows("rca_execution_watch")[0]
    assert watch["generation"] == 1
    assert watch["state"] == "pending"

    clock[0] = NOW + timedelta(seconds=1800)
    fallback = instance.collect_one()

    assert fallback.status == "terminal_failed"
    [job] = instance.store.list_rows("rca_delivery_jobs")
    assert job["generation"] == 1
    assert job["terminal_error_code"] == error_code
    [route] = instance.store.list_rows("rca_failure_routes")
    assert route["status"] == "terminal_fallback"
    assert route["completed_at"]
    [effect] = instance.store.list_rows("rca_delivery_effects")
    payload = json.loads(effect["payload_json"])
    contract = json.loads(job["contract_json"])
    assert payload["schema_version"] == TERMINAL_FALLBACK_DELIVERY_EFFECT_SCHEMA_VERSION
    assert payload["terminal_class"] == "honest_non_attribution"
    assert payload["confidence_tier"] == "low"
    assert payload["terminal_fallback"]["work_started_at"] == NOW.isoformat()
    assert (
        payload["terminal_fallback"]["deadline_at"]
        == (NOW + timedelta(seconds=1800)).isoformat()
    )
    assert payload["quality_oracle"]["schema_version"] == (
        "pnc_rca_structural_tier_oracle_v2"
    )
    oracle_sha = hashlib.sha256(
        json.dumps(
            payload["quality_oracle"],
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    assert payload["quality_oracle_sha256"] == oracle_sha
    assert contract["schema_version"] == TERMINAL_FALLBACK_CONTRACT_SCHEMA_VERSION
    assert "diagnostic_code" not in contract
    assert error_code not in payload["comment_content"]
    assert "请补齐" not in payload["comment_content"]
    assert "请修正" not in payload["comment_content"]
    watch = instance.store.list_rows("rca_execution_watch")[0]
    taxonomy = json.loads(watch["last_status_json"])["failure_taxonomy"]
    assert taxonomy["terminal_fallback"]["confidence_tier"] == "low"
    assert taxonomy["terminal_fallback"]["elapsed_seconds"] == 1800


def test_infra_remediation_runner_executes_once_for_same_task(tmp_path):
    clock = [NOW]
    marker = tmp_path / "remediation-ran"
    calls = []

    def remediate(claim, blocker, remediation, timeout_seconds):
        marker.write_text(claim.task_id, encoding="utf-8")
        calls.append((claim.submission_key, blocker["kind"], remediation["op"]))
        return {
            "schema_version": collector.INFRA_REMEDIATION_SCHEMA_VERSION,
            "success": True,
            "status": "succeeded",
            "submission_key": claim.submission_key,
            "business_key": claim.business_key,
            "generation": claim.generation,
            "task_id": claim.task_id,
            "operation": remediation["op"],
            "blocker_kind": blocker["kind"],
            "resumed_same_task": True,
            "external_writes": False,
            "timeout_seconds": timeout_seconds,
            "error_code": "",
        }

    instance = _real_terminal_collector(
        tmp_path,
        blocker={"kind": "translate_workdir_permission", "retryable": True},
        clock=clock,
        infra_remediation_runner=remediate,
    )

    first = instance.collect_one()
    clock[0] = NOW + timedelta(seconds=60)
    second = instance.collect_one()

    assert first.status == second.status == "failure_hold"
    assert marker.read_text(encoding="utf-8").startswith("g1q3-rca-s1-")
    assert len(calls) == 1
    [route] = instance.store.list_rows("rca_failure_routes")
    assert route["remediation_attempt_count"] == 1
    assert route["status"] == "remediation_succeeded"
    result = json.loads(route["remediation_result_json"])
    assert result["resumed_same_task"] is True
    assert result["generation"] == 1


def test_infra_remediation_crossing_deadline_falls_back_without_extra_hold(tmp_path):
    clock = [NOW]

    def crossing_remediation(claim, blocker, remediation, timeout_seconds):
        clock[0] = NOW + timedelta(seconds=5)
        return collector.default_infra_remediation_runner(
            claim,
            blocker,
            remediation,
            timeout_seconds,
        )

    instance = _real_terminal_collector(
        tmp_path,
        blocker={"kind": "translate_workdir_permission", "retryable": True},
        clock=clock,
        infra_remediation_runner=crossing_remediation,
    )
    _age_work_start(instance, seconds=1795)

    fallback = instance.collect_one()

    assert fallback.status == "terminal_failed"
    assert fallback.error_code == "translate_workdir_permission"
    [route] = instance.store.list_rows("rca_failure_routes")
    assert route["remediation_attempt_count"] == 1
    assert route["status"] == "terminal_fallback"
    assert route["next_retry_at"] is None
    assert instance.stats.failure_holds == 0


def test_unknown_code_is_held_fail_closed_then_persisted_as_taxonomy_gap(tmp_path):
    clock = [NOW]
    instance = _real_terminal_collector(
        tmp_path,
        blocker={"kind": "new_vm_failure", "retryable": True},
        clock=clock,
    )

    first = instance.collect_one()

    assert first.status == "failure_hold"
    assert first.error_code == "taxonomy_gap:new_vm_failure"
    assert instance.store.list_rows("rca_delivery_jobs") == []
    clock[0] = NOW + timedelta(seconds=1800)
    fallback = instance.collect_one()
    assert fallback.status == "terminal_failed"
    assert (
        instance.store.list_rows("rca_delivery_jobs")[0]["terminal_error_code"]
        == "taxonomy_gap:new_vm_failure"
    )


def test_forever_running_falls_back_at_admission_deadline(tmp_path):
    clock = [NOW]
    instance = _real_terminal_collector(
        tmp_path,
        clock=clock,
        status_reader=lambda task_id: {
            "success": True,
            "task_id": task_id,
            "state": "running",
        },
    )

    assert instance.collect_one().status == "running"
    assert instance.store.list_rows("rca_failure_routes") == []
    assert instance.store.list_rows("rca_delivery_effects") == []
    clock[0] = NOW + timedelta(seconds=1800)
    fallback = instance.collect_one()

    assert fallback.status == "terminal_failed"
    assert fallback.error_code == "rca_work_deadline_exceeded"
    assert instance.store.list_rows("rca_failure_routes")[0]["lane"] == "hard_defect"


@pytest.mark.parametrize(
    ("status_reader", "expected_code"),
    [
        (
            lambda _task_id: (_ for _ in ()).throw(OSError("offline")),
            "vm_status_reader_unavailable",
        ),
        (lambda _task_id: {"success": False, "state": "missing"}, "vm_status_missing"),
    ],
)
def test_status_missing_and_reader_error_use_same_admission_deadline(
    tmp_path, status_reader, expected_code
):
    clock = [NOW]
    instance = _real_terminal_collector(
        tmp_path,
        clock=clock,
        status_reader=status_reader,
    )

    held = instance.collect_one()
    assert held.status == "failure_hold"
    assert held.error_code == expected_code
    assert instance.store.list_rows("rca_delivery_effects") == []
    clock[0] = NOW + timedelta(seconds=1800)
    fallback = instance.collect_one()

    assert fallback.status == "terminal_failed"
    assert fallback.error_code == expected_code
    assert instance.store.list_rows("rca_failure_routes")[0]["status"] == (
        "terminal_fallback"
    )


def test_invalid_submission_admission_is_silent_until_work_deadline(tmp_path):
    clock = [NOW]
    instance = _real_terminal_collector(
        tmp_path,
        clock=clock,
        blocker={"kind": "service_pipeline_runner_failed", "retryable": False},
    )
    with sqlite3.connect(instance.store.db_path) as conn:
        conn.execute("UPDATE rca_outbox SET payload_json = '{}' ")

    held = instance.collect_one()
    assert held.status == "failure_hold"
    assert held.error_code == "submission_outbox_contract_invalid"
    assert instance.store.list_rows("rca_delivery_effects") == []
    clock[0] = NOW + timedelta(seconds=1800)
    fallback = instance.collect_one()

    assert fallback.status == "terminal_failed"
    assert fallback.error_code == "submission_outbox_contract_invalid"
    assert instance.store.list_rows("rca_failure_routes")[0]["route_kind"] == (
        "internal_alert"
    )


def test_permanent_artifact_error_is_silent_until_work_deadline(tmp_path):
    clock = [NOW]

    def invalid_bundle(_claim):
        raise collector.ArtifactBundleReadError(
            "artifact_hash_mismatch",
            "sealed artifact hash changed",
            permanent=True,
        )

    instance = _real_terminal_collector(
        tmp_path,
        clock=clock,
        status_reader=lambda task_id: {
            "success": True,
            "task_id": task_id,
            "state": "completed",
        },
    )
    instance.artifact_bundle_reader = invalid_bundle

    held = instance.collect_one()
    assert held.status == "failure_hold"
    assert held.error_code == "artifact_hash_mismatch"
    assert instance.store.list_rows("rca_delivery_effects") == []
    clock[0] = NOW + timedelta(seconds=1800)
    fallback = instance.collect_one()

    assert fallback.status == "terminal_failed"
    assert fallback.error_code == "artifact_hash_mismatch"


def test_admission_parsing_crossing_deadline_skips_status_read(tmp_path, monkeypatch):
    clock = [NOW]
    status_calls = []
    instance = _real_terminal_collector(
        tmp_path,
        clock=clock,
        status_reader=lambda task_id: status_calls.append(task_id),
    )
    _age_work_start(instance, seconds=1799)
    original = collector._submission_admission

    def crossing_admission(claim):
        admission = original(claim)
        clock[0] = NOW + timedelta(seconds=1)
        return admission

    monkeypatch.setattr(collector, "_submission_admission", crossing_admission)

    fallback = instance.collect_one()

    assert fallback.status == "terminal_failed"
    assert fallback.error_code == "rca_work_deadline_exceeded"
    assert status_calls == []
    assert instance.store.list_rows("rca_delivery_jobs")[0]["outcome"] == (
        "terminal_failed"
    )


def test_status_read_crossing_deadline_skips_artifact_read(tmp_path):
    clock = [NOW]
    artifact_calls = []

    def late_completed_status(task_id):
        clock[0] = NOW + timedelta(seconds=1)
        return {"success": True, "task_id": task_id, "state": "completed"}

    instance = _real_terminal_collector(
        tmp_path,
        clock=clock,
        status_reader=late_completed_status,
    )
    _age_work_start(instance, seconds=1799)
    instance.artifact_bundle_reader = lambda claim: artifact_calls.append(claim)

    fallback = instance.collect_one()

    assert fallback.status == "terminal_failed"
    assert fallback.error_code == "rca_work_deadline_exceeded"
    assert artifact_calls == []
    assert instance.store.list_rows("rca_delivery_effects")[0]["outcome"] == (
        "terminal_failed"
    )


def test_late_valid_completed_bundle_becomes_low_fallback_not_delivery(
    tmp_path, monkeypatch
):
    clock = [NOW]
    observed_claim = []
    instance = _real_terminal_collector(
        tmp_path,
        clock=clock,
        status_reader=lambda task_id: {
            "success": True,
            "task_id": task_id,
            "state": "completed",
        },
    )
    _age_work_start(instance, seconds=1799)

    def bundle_reader(claim):
        observed_claim.append(claim)
        return {}

    def late_valid_delivery(**_kwargs):
        clock[0] = NOW + timedelta(seconds=1)
        return _delivery(observed_claim[0])

    instance.artifact_bundle_reader = bundle_reader
    monkeypatch.setattr(collector, "verify_delivery_bundle", late_valid_delivery)

    fallback = instance.collect_one()

    assert fallback.status == "terminal_failed"
    assert fallback.error_code == "rca_work_deadline_exceeded"
    [job] = instance.store.list_rows("rca_delivery_jobs")
    assert job["outcome"] == "terminal_failed"
    [effect] = instance.store.list_rows("rca_delivery_effects")
    payload = json.loads(effect["payload_json"])
    assert payload["terminal_class"] == "honest_non_attribution"
    assert payload["confidence_tier"] == "low"


def test_failure_receipt_reader_script_is_exact_and_read_only():
    claim = SimpleNamespace(
        submission_key="g1q3-rca-s1-" + "a" * 64,
        task_id="g1q3-rca-s1-" + "a" * 64,
    )
    script = collector._remote_failure_receipt_script(claim)

    assert "/mnt/tmp/g1q3-rca-s1-" + "a" * 64 in script
    assert "rca_service_result.json" in script
    assert "os.O_RDONLY" in script
    assert "O_NOFOLLOW" in script
    assert "os.O_WRONLY" not in script
    assert "os.O_RDWR" not in script
    assert "write" not in script.lower()
