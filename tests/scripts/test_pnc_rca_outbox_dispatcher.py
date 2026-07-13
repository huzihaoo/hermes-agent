from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import threading
import time

import pytest

from gateway.pnc_rca_control_store import (
    ActivationEpochError,
    MANUAL_TRIGGER_SCHEMA_VERSION,
    KafkaRecord,
    ManualRcaTriggerRequest,
    RcaControlStore,
)
from gateway.pnc_rca_delivery_store import (
    DELIVERY_BACKPRESSURE_SNAPSHOT_SCHEMA_VERSION,
    DELIVERY_OUTCOME_CONSECUTIVE_FAILURE_THRESHOLD,
    DELIVERY_OUTCOME_CONSECUTIVE_WINDOW_SECONDS,
    DELIVERY_OUTCOME_SLO_SCHEMA_VERSION,
    DELIVERY_OUTCOME_SLO_WINDOWS,
    DeliveryBackpressureSnapshot,
    DeliveryDispatcherCircuit,
    RcaDeliveryStore,
)
from gateway.pnc_rca_derived_capacity_reservation import (
    CAPACITY_SCOPE,
    DERIVED_PRECREATE_ABORT_SCHEMA_VERSION,
    DERIVED_RESERVATION_RECEIPT_SCHEMA_VERSION,
    DERIVED_RESERVATION_REQUEST_SCHEMA_VERSION,
    DerivedCapacityReservationDecision,
    HFS_PATH,
    TMP_PATH,
    validate_derived_capacity_precreate_abort_receipt,
    validate_derived_capacity_reservation_receipt,
)
from gateway.pnc_rca_kafka_contract import WorkflowEventPolicy, WorkflowTransition
from gateway.pnc_rca_runtime_identity import RCA_RUNTIME_RELATIVE_FILES
from gateway.pnc_rca_schema import RcaIssueContext
from gateway.pnc_rca_workspace_runtime import (
    WorkspaceRuntimeError,
    WorkspaceRuntimeIdentity,
)
from scripts import pnc_rca_outbox_dispatcher as dispatcher_module
from scripts.pnc_rca_outbox_dispatcher import (
    DispatchCircuitError,
    DispatcherConfig,
    EnrichmentNotReady,
    HealthReporter,
    OutboxDispatcher,
    StorageAdmissionRequest,
    default_storage_admission,
    run_dispatch_loop,
)


def test_outbox_env_loader_preserves_literal_expansion_syntax(tmp_path, monkeypatch):
    env_file = tmp_path / "outbox.env"
    env_file.write_text(
        "HERMES_RCA_OUTBOX_SERVICE_ID=${AMBIENT_OUTBOX_SERVICE}\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("HERMES_RCA_OUTBOX_SERVICE_ID", raising=False)
    monkeypatch.setenv("AMBIENT_OUTBOX_SERVICE", "must-not-expand")

    try:
        dispatcher_module.load_dispatcher_environment(env_file)
        assert os.environ["HERMES_RCA_OUTBOX_SERVICE_ID"] == (
            "${AMBIENT_OUTBOX_SERVICE}"
        )
    finally:
        os.environ.pop("HERMES_RCA_OUTBOX_SERVICE_ID", None)


TOPIC = "feishu-project-workflow-event"
PDCL_COMMAND = "mdi download event -u 0190abcd-1111-2222-3333-444455556666 -s ./"
WORKSPACE_RUNTIME = WorkspaceRuntimeIdentity(
    root=Path("/fixed/rca-workspace-runtime"),
    manifest_path=Path("/fixed/rca-workspace-runtime/manifest.json"),
    creator_path=Path("/fixed/rca-workspace-runtime/bin/create_task_v2.py"),
    manifest_sha256="b" * 64,
    closure_sha256="c" * 64,
    source_commit="d" * 40,
    file_sha256={
        "bin/create_task_v2.py": "1" * 64,
        "bin/shared_state_v2.py": "2" * 64,
        "bin/shared_state_fields.py": "3" * 64,
    },
)


@pytest.fixture(autouse=True)
def _fixed_workspace_runtime(monkeypatch):
    monkeypatch.setattr(
        dispatcher_module,
        "validate_workspace_runtime",
        lambda: WORKSPACE_RUNTIME,
    )


class Clock:
    def __init__(self, current: datetime):
        self.current = current

    def __call__(self) -> datetime:
        return self.current


def _runtime_decision(
    *,
    state: str = "STEADY_ACTIVE",
    mode: str = "steady",
    ready: bool = True,
    generation: int = 2,
    irreversible: bool = True,
) -> dict:
    blocked = not ready
    return {
        "schema_version": "pnc_rca_capacity_runtime_decision_v1",
        "configured": True,
        "legacy_compatibility": False,
        "initial_policy": "bootstrap",
        "effective_state": state,
        "effective_mode": mode,
        "generation": generation,
        "irreversible": irreversible,
        "ready": ready,
        "reason_code": (
            "rca_capacity_steady_commit_valid"
            if ready
            else "rca_capacity_steady_evidence_missing"
        ),
        "current_release_id": "rca-prod-20260713-001",
        "current_bootstrap_epoch_id": "rca-bootstrap-release-20260713",
        "ratchet_origin_release_id": "rca-prod-origin-20260713",
        "ratchet_origin_bootstrap_epoch_id": "rca-bootstrap-origin-20260713",
        "active_release_binding_sha256": "c" * 64,
        "ledger": {
            "sample_count": 20,
            "sha256": "1" * 64,
            "window_seconds": 604800.0,
            "max_gap_seconds": 36000.0,
            "first_observed_at": "2026-07-13T00:00:00Z",
            "last_observed_at": "2026-07-20T00:00:00Z",
            "steady_qualified": True,
        },
        "artifacts": (
            None
            if blocked or mode == "bootstrap"
            else {
                "transition_authorization_sha256": "2" * 64,
                "transition_authorization_fingerprint": "3" * 64,
                "transition_receipt_sha256": "4" * 64,
                "transition_receipt_fingerprint": "5" * 64,
                "commit_marker_sha256": "6" * 64,
                "commit_marker_fingerprint": "7" * 64,
                "evidence_bundle_sha256": "8" * 64,
                "evidence_bundle_fingerprint": "9" * 64,
            }
        ),
        "lock": {"held": True, "latency_ms": 0.5, "error_code": ""},
    }


class FakeCapacityRuntime:
    def __init__(self, decisions):
        self.decisions = list(decisions)
        self.calls = 0
        self.inside = False

    def _next(self):
        index = min(self.calls, len(self.decisions) - 1)
        self.calls += 1
        return dict(self.decisions[index])

    @contextmanager
    def shared_decision(self):
        self.inside = True
        try:
            yield self._next()
        finally:
            self.inside = False

    def observe(self):
        return self._next()


class DeliverySnapshotSource:
    def __init__(self, *values):
        self.values = list(values)
        self.calls = 0

    def backpressure_snapshot(self, *, now=None):
        del now
        self.calls += 1
        value = self.values.pop(0)
        if isinstance(value, Exception):
            raise value
        return value


def _policy() -> WorkflowEventPolicy:
    return WorkflowEventPolicy(
        topic=TOPIC,
        policy_version="issue-created-v1",
        project_keys=frozenset({"t03o4q"}),
        project_simple_names=frozenset({"g1q3"}),
        work_item_type_keys=frozenset({"issue"}),
        status_change_types=frozenset({"Reached"}),
        transitions=(
            WorkflowTransition(
                state_key="new-problem-state", pre_status=1, cur_status=2
            ),
        ),
    )


def _record(
    offset: int = 10,
    *,
    issue_id: int = 7041712812,
) -> KafkaRecord:
    return KafkaRecord(
        topic=TOPIC,
        partition=2,
        offset=offset,
        value=json.dumps(
            {
                "id": issue_id,
                "name": "ACC braking issue",
                "nodes": [
                    {
                        "state_key": "new-problem-state",
                        "node_name": "New problem",
                        "pre_status": 1,
                        "cur_status": 2,
                    }
                ],
                "project_key": "t03o4q",
                "project_simple_name": "g1q3",
                "status_change_type": "Reached",
                "updated_at": 1783650000000,
                "work_item_type_key": "issue",
            },
            sort_keys=True,
        ).encode(),
    )


def _store(tmp_path, *, shadow: bool = False) -> RcaControlStore:
    db_path = tmp_path / "control.sqlite3"
    store = RcaControlStore(db_path)
    store.ingest_record(_record(), policy=_policy(), submit_enabled=not shadow)
    RcaDeliveryStore(db_path)
    return store


def _activation_manual_identity(message_id, *, mode="run_or_join", issue_id=7041712813):
    return {
        "chat_id": "oc_activation_dispatcher",
        "requester_id": "ou_activation_dispatcher",
        "message_id": message_id,
        "thread_id": f"topic:{message_id}",
        "issue_url": f"https://project.feishu.cn/g1q3/issue/detail/{issue_id}",
        "mode": mode,
    }


def _prepare_activation_epoch(store, *, kafka_offset=10, bounded=False):
    epoch_id = "rca-dispatcher-activation-20260712"
    created = store.create_activation_epoch(
        epoch_id=epoch_id,
        preauthorization_fingerprint="1" * 64,
        preauthorization_gate_receipt_sha256="3" * 64,
        preauthorization_capsule_sha256="4" * 64,
        config_sha256="2" * 64,
        db_logical_identity={
            "device": 7,
            "inode": 11,
            "logical_store_id": "dispatcher-test-control",
        },
        partition_start_fence={TOPIC: {"2": kafka_offset}},
        operator="dispatcher-test",
        reason="exercise outbox activation wiring",
    )
    store.preauthorize_activation_epoch(
        epoch_id=epoch_id,
        preproduction_fingerprint="5" * 64,
        preproduction_gate_receipt_sha256="6" * 64,
        preproduction_capsule_sha256="7" * 64,
        expected_preauthorization_fingerprint="1" * 64,
        expected_preauthorization_gate_receipt_sha256="3" * 64,
        expected_preauthorization_capsule_sha256="4" * 64,
        expected_config_sha256=created["config_sha256"],
        expected_db_logical_identity_sha256=created[
            "db_logical_identity_sha256"
        ],
        expected_partition_start_fence_sha256=created[
            "partition_start_fence_sha256"
        ],
        operator="dispatcher-test",
        reason="bind exact preproduction capsule for outbox activation wiring",
    )
    identities = {
        "kafka_success": (
            "kafka",
            {"event_uid": f"{TOPIC}:2:{kafka_offset}"},
        ),
        "manual_success": (
            "manual",
            _activation_manual_identity("om_dispatcher_manual_success"),
        ),
        "manual_terminal_failure": (
            "manual",
            _activation_manual_identity(
                "om_dispatcher_manual_terminal",
                mode="debug",
                issue_id=7041712814,
            ),
        ),
    }
    for slot_kind, (source_kind, source_identity) in identities.items():
        store.authorize_activation_slot(
            epoch_id=epoch_id,
            slot_kind=slot_kind,
            source_kind=source_kind,
            source_identity=source_identity,
            operator="dispatcher-test",
            reason=f"authorize exact {slot_kind} source",
        )
    if bounded:
        store.transition_activation_epoch(
            epoch_id=epoch_id,
            expected_state="preauthorized",
            target_state="bounded_active",
            operator="dispatcher-test",
            reason="open exact bounded dispatch",
        )
    return epoch_id, identities


def _prepare_activation_epoch_with_historical_outbox(
    store, *, kafka_offset=10, bounded=False
):
    with sqlite3.connect(store.db_path) as conn:
        conn.execute(
            "UPDATE rca_outbox SET status = 'completed' WHERE status = 'pending'"
        )
    prepared = _prepare_activation_epoch(
        store,
        kafka_offset=kafka_offset,
        bounded=bounded,
    )
    with sqlite3.connect(store.db_path) as conn:
        conn.execute(
            "UPDATE rca_outbox SET status = 'pending' "
            "WHERE activation_epoch_id IS NULL"
        )
    return prepared


def _materialize_activation_slots(store, identities, *, kafka_issue_id=7041712815):
    kafka_identity = identities["kafka_success"][1]
    kafka_offset = int(str(kafka_identity["event_uid"]).rsplit(":", 1)[1])
    kafka = store.ingest_record(
        _record(offset=kafka_offset, issue_id=kafka_issue_id),
        policy=_policy(),
        submit_enabled=True,
        activation_required=True,
        activation_slot_kind="kafka_success",
    )
    assert kafka.decision == "accepted"
    manual_results = []
    for slot_kind in ("manual_success", "manual_terminal_failure"):
        identity = identities[slot_kind][1]
        request = ManualRcaTriggerRequest(
            schema_version=MANUAL_TRIGGER_SCHEMA_VERSION,
            issue_url=str(identity["issue_url"]),
            mode=str(identity["mode"]),
            reason="dispatcher_activation_test",
            platform="feishu",
            chat_id=str(identity["chat_id"]),
            thread_id=str(identity["thread_id"]),
            message_id=str(identity["message_id"]),
            requester_id=str(identity["requester_id"]),
        )
        result = store.admit_manual_trigger(
            request,
            allowed_chat_ids={request.chat_id},
            submit_enabled=True,
            operator_authorized=True,
            active_policy=_policy(),
            activation_required=True,
            activation_slot_kind=slot_kind,
        )
        assert result.outcome == "created"
        manual_results.append(result)
    return kafka, tuple(manual_results)


def _confirm_activation_epoch(store, epoch_id, *, kafka_offset=10):
    end_fence = {TOPIC: {"2": kafka_offset + 1}}
    epoch = store.activation_epoch()
    assert epoch is not None
    store.transition_activation_epoch(
        epoch_id=epoch_id,
        expected_state="bounded_active",
        target_state="confirmed",
        partition_end_fence=end_fence,
        production_fingerprint="3" * 64,
        production_gate_receipt_sha256="4" * 64,
        expected_config_sha256=epoch["config_sha256"],
        expected_db_logical_identity_sha256=epoch[
            "db_logical_identity_sha256"
        ],
        expected_partition_start_fence_sha256=epoch[
            "partition_start_fence_sha256"
        ],
        expected_release_binding_sha256=(
            store.activation_release_binding_sha256(
                epoch_id=epoch_id,
                partition_end_fence=end_fence,
            )
        ),
        operator="dispatcher-test",
        reason="bind passing production receipt",
    )


def _config(
    tmp_path,
    *,
    enabled: bool = True,
    activation_required: bool | str = False,
    max_age_seconds: int = 86_400,
    input_wait_max_age_seconds: int | None = None,
):
    env = {
            "HERMES_RCA_OUTBOX_DISPATCH_ENABLED": str(enabled).lower(),
            "HERMES_RCA_OUTBOX_ACTIVATION_REQUIRED": str(
                activation_required
            ).lower(),
            "HERMES_RCA_OUTBOX_CONTROL_DB_PATH": str(tmp_path / "control.sqlite3"),
            "HERMES_RCA_OUTBOX_HEALTH_PATH": str(tmp_path / "health.json"),
            "HERMES_RCA_OUTBOX_LEASE_SECONDS": "180",
            "HERMES_RCA_OUTBOX_MAX_AGE_SECONDS": str(max_age_seconds),
            "HERMES_RCA_OUTBOX_BATCH_SIZE": "5",
            "HERMES_RCA_OUTBOX_STORAGE_ADMISSION_ENABLED": str(enabled).lower(),
            "HERMES_RCA_OUTBOX_STORAGE_RESERVATION_ENABLED": "false",
            "HERMES_RCA_OUTBOX_DERIVED_CAPACITY_RESERVATION_ENABLED": str(
                enabled
            ).lower(),
            "HERMES_RCA_OUTBOX_DELIVERY_BACKPRESSURE_ENABLED": str(enabled).lower(),
        }
    if input_wait_max_age_seconds is not None:
        env["HERMES_RCA_OUTBOX_INPUT_WAIT_MAX_AGE_SECONDS"] = str(
            input_wait_max_age_seconds
        )
    return DispatcherConfig.from_env(
        env,
        hermes_home=tmp_path,
    )


def _context(event) -> RcaIssueContext:
    return RcaIssueContext(
        project_key=str(event["project_key"]),
        work_item_type=str(event["work_item_type_key"]),
        work_item_id=str(event["work_item_id"]),
        url=str(event["issue_url"]),
        title=str(event["title"]),
        pdcl_download_cmd=PDCL_COMMAND,
        is_pdcl_format=True,
        description_markdown=(
            "- title: ACC braking issue\n"
            f"- 数据地址: {PDCL_COMMAND}\n"
            "- 根因分析字段: pending"
        ),
        source_quality="full",
    )


def _success(admission, _request):
    return {
        "success": True,
        "created": True,
        "task": {"task_id": admission.submission_key, "state": "submitted"},
    }


def _storage_payload(
    request: StorageAdmissionRequest,
    *,
    status: str = "pass",
    extra: dict | None = None,
):
    blocked = status == "blocked"
    expected_bytes = request.expected_artifact_cache_bytes
    logical_cache = expected_bytes
    logical_artifacts = (expected_bytes * 9 + 3) // 4
    bytes_per_case = logical_cache + logical_artifacts
    required = bytes_per_case * request.requested_cases
    block_size = 4096
    total_bytes = 30 * 1024**4
    if blocked:
        free_bytes = 6 * 1024**4
        available_bytes = 4 * 1024**4
    else:
        free_bytes = 29 * 1024**4
        available_bytes = 28 * 1024**4
    reserve_bytes = (
        total_bytes * request.reserve_percent + 99
    ) // 100
    admittable_bytes = max(0, available_bytes - reserve_bytes)
    max_additional_cases = admittable_bytes // bytes_per_case
    horizon = (
        (
            admittable_bytes
            * 1_000
            // (bytes_per_case * request.assumed_cases_per_day)
        )
        / 1_000
        if admittable_bytes
        else 0.0
    )
    blocker = "task_output_below_reserve_watermark" if blocked else None
    target = {
        "name": "task_output",
        "path": "/mnt/tmp",
        "capacity_scope": "derived_artifact_and_cache",
        "observed_at": "2026-07-10T09:00:00+00:00",
        "multiplier": 3.25,
        "bytes_per_case": bytes_per_case,
        "required_bytes": required,
        "ok": not blocked,
        "blocker": blocker,
        "filesystem_block_size_bytes": block_size,
        "total_bytes": total_bytes,
        "free_bytes": free_bytes,
        "available_bytes": available_bytes,
        "reserve_bytes": reserve_bytes,
        "admittable_bytes": admittable_bytes,
        "projected_available_after_request_bytes": available_bytes - required,
        "headroom_after_request_bytes": admittable_bytes - required,
        "max_additional_cases": max_additional_cases,
        "days_horizon_at_assumed_cases_per_day": horizon,
    }
    body = {
        "schema_version": "g1q3_rca_storage_admission_v2",
        "observed_at": "2026-07-10T09:00:00+00:00",
        "ok": not blocked,
        "status": status,
        "capacity_scope": "derived_artifact_and_cache",
        "blockers": [blocker] if blocked else [],
        "side_effects": "none_read_only_statvfs",
        "policy": {
            "requested_cases": request.requested_cases,
            "concurrency_reserve_cases": request.requested_cases,
            "requested_cases_scope": "this_admission_capacity_reservation_only",
            "assumed_cases_per_day": request.assumed_cases_per_day,
            "assumed_cases_per_day_scope": "days_horizon_calculation_only",
            "expected_derived_artifact_bytes_per_case": expected_bytes,
            "input_materialization_bytes_per_case": 0,
            "input_materialization": "forbidden",
            "input_unit": "bytes",
            "gb_definition_bytes": 1_000_000_000,
            "reserve_ratio": request.reserve_percent / 100.0,
            "reserve_percent": float(request.reserve_percent),
            "task_output_multiplier": 3.25,
            "logical_budget_multipliers": {
                "derived_cache": 1.0,
                "derived_artifacts_and_publisher": 2.25,
                "total": 3.25,
            },
            "logical_budget_bytes_per_case": {
                "derived_cache": logical_cache,
                "derived_artifacts_and_publisher": logical_artifacts,
                "total": bytes_per_case,
            },
        },
        "required_bytes_total": required,
        "max_additional_cases": max_additional_cases,
        "days_horizon_at_assumed_cases_per_day": horizon,
        "target": target,
    }
    body.update(extra or {})
    return body


def _storage_pass(request: StorageAdmissionRequest):
    return _storage_payload(request)


def test_storage_v2_accepts_read_only_observation_failure_without_fake_capacity():
    request = StorageAdmissionRequest(4, 200, 1_000_000_000, 30, 9)
    body = _storage_payload(request, status="blocked")
    target = body["target"]
    body["blockers"] = ["task_output_invalid_statvfs"]
    body["max_additional_cases"] = 0
    body["days_horizon_at_assumed_cases_per_day"] = 0.0
    body["target"] = {
        key: target[key]
        for key in (
            "name",
            "path",
            "capacity_scope",
            "observed_at",
            "multiplier",
            "bytes_per_case",
            "required_bytes",
            "ok",
            "blocker",
        )
    }
    body["target"].update(
        blocker="task_output_invalid_statvfs",
        observation_error={"type": "ValueError", "message": "invalid fields"},
        total_bytes=None,
        free_bytes=None,
        available_bytes=None,
        reserve_bytes=None,
        admittable_bytes=0,
        max_additional_cases=0,
        days_horizon_at_assumed_cases_per_day=0.0,
    )

    summary = dispatcher_module.validate_storage_admission(body, request)

    assert summary["status"] == "blocked"
    assert summary["target"]["blocker"] == "task_output_invalid_statvfs"
    assert "/mnt/tmp" not in json.dumps(summary, sort_keys=True)


def _sha256_json(value) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _byte_totals(tmp: int, hfs: int):
    return {"tmp": tmp, "hfs": hfs, "total": tmp + hfs}


def _derived_reservation_receipt(request, *, status: str = "reserved"):
    requested = request.requested_bytes
    waiting = status == "waiting_capacity"
    released = status == "released"
    total = _byte_totals(40_000_000_000, 0)
    if waiting:
        available = _byte_totals(12_000_000_000, 0)
        reserve = _byte_totals(12_000_000_000, 0)
        effective = _byte_totals(0, 0)
        blockers = ["task_output_publisher_insufficient_derived_capacity"]
    else:
        available = _byte_totals(40_000_000_000, 0)
        reserve = _byte_totals(12_000_000_000, 0)
        effective = _byte_totals(28_000_000_000, 0)
        blockers = []
    contract = request.contract()
    contract_sha256 = _sha256_json(contract)
    admitted = status in {"reserved", "active"}
    if waiting:
        blocker = {
            "kind": "derived_capacity_waiting",
            "retryable": True,
            "capacity_blockers": blockers,
        }
    elif released:
        blocker = {
            "kind": "derived_capacity_reservation_released_reconcile_only",
            "retryable": False,
            "reconcile_only": True,
            "create_allowed": False,
        }
    else:
        blocker = None
    return {
        "schema_version": DERIVED_RESERVATION_RECEIPT_SCHEMA_VERSION,
        "request_schema_version": DERIVED_RESERVATION_REQUEST_SCHEMA_VERSION,
        "ok": admitted,
        "status": status,
        "reservation_id": "2d13a73f-a91c-4738-a3ae-98df25d23d2f",
        "submission_key": request.submission_key,
        "contract_sha256": contract_sha256,
        "fence": 1,
        "operation": "reserve",
        "idempotent": released,
        "observed_at": "2026-07-11T04:00:00+00:00",
        "contract": contract,
        "reservation": {
            "reservation_id": "2d13a73f-a91c-4738-a3ae-98df25d23d2f",
            "submission_key": request.submission_key,
            "contract_sha256": contract_sha256,
            "state": status,
            "fence": 1,
            "run_id": request.task_id if status in {"active", "released"} else "",
            "requested_bytes": requested,
            "held_bytes": requested if admitted else _byte_totals(0, 0),
            "created_at": "2026-07-11T04:00:00+00:00",
            "updated_at": "2026-07-11T04:00:00+00:00",
            "lease_expires_at": (None if released else "2026-07-11T04:30:00+00:00"),
            "activated_at": (
                "2026-07-11T04:00:00+00:00"
                if status in {"active", "released"}
                else None
            ),
            "released_at": ("2026-07-11T04:00:00+00:00" if released else None),
        },
        "capacity": {
            "scope": CAPACITY_SCOPE,
            "atomic_reservation": True,
            "observed_at": "2026-07-11T04:00:00+00:00",
            "paths": {"tmp": TMP_PATH, "hfs": HFS_PATH},
            "reserve_ratio": "0.30",
            "required_bytes": requested,
            "total_bytes": total,
            "available_bytes": available,
            "reserve_bytes": reserve,
            "outstanding_held_bytes": _byte_totals(0, 0),
            "effective_admittable_bytes": effective,
            "admitted": not blockers,
            "blockers": blockers,
        },
        "blocker": blocker,
    }


def _reservation_pass(request):
    return validate_derived_capacity_reservation_receipt(
        _derived_reservation_receipt(request), request
    )


def _precreate_abort_pass(request, reservation_receipt):
    receipt = {
        "schema_version": DERIVED_PRECREATE_ABORT_SCHEMA_VERSION,
        "operation": "abort_precreate",
        "released": True,
        "idempotent": False,
        "observed_at": "2026-07-11T04:00:00+00:00",
        "reservation_id": reservation_receipt["reservation_id"],
        "submission_key": request.submission_key,
        "task_id": request.task_id,
        "contract_sha256": reservation_receipt["contract_sha256"],
        "fence": reservation_receipt["fence"],
        "prior_state": "reserved",
        "state": "expired",
        "held_bytes": _byte_totals(0, 0),
    }
    return validate_derived_capacity_precreate_abort_receipt(
        receipt, request, reservation_receipt
    )


def _clock_for(store: RcaControlStore) -> Clock:
    created = datetime.fromisoformat(store.list_rows("rca_outbox")[0]["created_at"])
    return Clock(created + timedelta(seconds=1))


def _delivery_snapshot(
    config: DispatcherConfig,
    *,
    pending: int = 0,
    claimed: int = 0,
    retry_wait: int = 0,
    uncertain: int = 0,
    untracked_completed: int = 0,
    pending_watches: int = 0,
    running_watches: int = 0,
    circuit_state: str = "closed",
    outcome_slo_healthy: bool = True,
) -> DeliveryBackpressureSnapshot:
    counts = (pending, claimed, retry_wait, uncertain)
    pipeline_counts = (
        untracked_completed,
        pending_watches,
        running_watches,
    )
    observed_at = "2026-07-10T09:00:00+00:00"
    consecutive_failures = (
        0
        if outcome_slo_healthy
        else DELIVERY_OUTCOME_CONSECUTIVE_FAILURE_THRESHOLD
    )
    outcome_slo = {
        "schema_version": DELIVERY_OUTCOME_SLO_SCHEMA_VERSION,
        "observed_at": observed_at,
        "success_delivery_statuses": ["delivered", "partial"],
        "failure_delivery_statuses": ["quarantined"],
        "windows": {
            name: {
                "window_seconds": window_seconds,
                "min_samples": min_samples,
                "max_failure_rate": max_failure_rate,
                "sample_count": 0,
                "failure_count": 0,
                "failure_rate": 0.0,
                "breached": False,
            }
            for name, window_seconds, min_samples, max_failure_rate in (
                DELIVERY_OUTCOME_SLO_WINDOWS
            )
        },
        "consecutive_failure_window_seconds": (
            DELIVERY_OUTCOME_CONSECUTIVE_WINDOW_SECONDS
        ),
        "consecutive_failure_threshold": (
            DELIVERY_OUTCOME_CONSECUTIVE_FAILURE_THRESHOLD
        ),
        "consecutive_failure_count": consecutive_failures,
        "consecutive_failure_breached": not outcome_slo_healthy,
        "contract_valid": True,
        "healthy": outcome_slo_healthy,
    }
    return DeliveryBackpressureSnapshot(
        schema_version=DELIVERY_BACKPRESSURE_SNAPSHOT_SCHEMA_VERSION,
        observed_at=observed_at,
        db_path=str(config.delivery_db_path),
        pending=pending,
        claimed=claimed,
        retry_wait=retry_wait,
        uncertain=uncertain,
        unresolved_effects=sum(counts),
        untracked_completed_submissions=untracked_completed,
        pending_watches=pending_watches,
        running_watches=running_watches,
        unresolved_work=sum(counts) + sum(pipeline_counts),
        outcome_slo=outcome_slo,
        circuit=DeliveryDispatcherCircuit(
            state=circuit_state,
            reason_code=("feishu_auth_failed" if circuit_state == "open" else ""),
        ),
        circuits={
            effect_kind: DeliveryDispatcherCircuit(
                state=circuit_state,
                reason_code=(
                    "feishu_auth_failed" if circuit_state == "open" else ""
                ),
            )
            for effect_kind in ("feishu_issue_comment", "feishu_thread_reply")
        },
    )


def test_activation_required_defaults_false_and_is_runtime_public(tmp_path):
    config = _config(tmp_path)

    assert config.activation_required is False
    assert config.public_dict()["activation_required"] is False
    assert config.runtime_public_dict()["activation_required"] is False


@pytest.mark.parametrize("value", ["1", "0", "yes", "on", "enabled", ""])
def test_activation_required_accepts_only_literal_boolean(tmp_path, value):
    with pytest.raises(ValueError, match="exactly true or false"):
        _config(tmp_path, activation_required=value)


def test_activation_required_true_is_runtime_public(tmp_path):
    config = _config(tmp_path, activation_required=True)

    assert config.activation_required is True
    assert config.public_dict()["activation_required"] is True
    assert config.runtime_public_dict()["activation_required"] is True


def test_lease_renewal_forwards_activation_requirement(tmp_path):
    captured = {}

    class Store:
        @staticmethod
        def extend_outbox_lease(**kwargs):
            captured.update(kwargs)
            return "2026-07-12T08:03:00+00:00"

    dispatcher = object.__new__(OutboxDispatcher)
    dispatcher.store = Store()
    dispatcher.config = _config(tmp_path, activation_required=True)
    dispatcher.now = lambda: datetime(2026, 7, 12, 8, 0, tzinfo=timezone.utc)
    claim = type(
        "Claim",
        (),
        {
            "outbox_id": 7,
            "lease_token": "lease-token",
            "lease_owner": "activation-dispatcher",
        },
    )()

    dispatcher._renew(claim)

    assert captured["activation_required"] is True
    assert captured["outbox_id"] == 7


def test_disabled_dispatcher_never_claims_or_calls_boundaries(tmp_path):
    store = _store(tmp_path)
    called = []
    dispatcher = OutboxDispatcher(
        store=store,
        config=_config(tmp_path, enabled=False),
        enrich=lambda event: called.append(event) or pytest.fail("must not enrich"),
        storage_admission=lambda request: pytest.fail("must not check storage"),
        submit=lambda admission, request: pytest.fail("must not submit"),
    )

    assert dispatcher.dispatch_one().status == "disabled"
    assert called == []
    assert store.list_rows("rca_outbox")[0]["status"] == "pending"


def _activation_dispatcher(store, config):
    return OutboxDispatcher(
        store=store,
        config=config,
        enrich=_context,
        storage_admission=_storage_pass,
        derived_capacity_reservation=_reservation_pass,
        submit=_success,
        now=_clock_for(store),
        lease_owner="activation-dispatcher",
    )


def test_activation_preauthorized_outbox_is_held_before_claim(tmp_path):
    store = _store(tmp_path)
    _prepare_activation_epoch_with_historical_outbox(store)
    dispatcher = _activation_dispatcher(
        store,
        _config(tmp_path, activation_required=True),
    )

    outcome = dispatcher.dispatch_one()

    assert outcome.status == "idle"
    [outbox] = store.list_rows("rca_outbox")
    assert outbox["status"] == "pending"
    assert outbox["attempt"] == 0


def test_activation_confirmation_rejects_historical_outbox_before_claim(tmp_path):
    store = _store(tmp_path)
    epoch_id, identities = _prepare_activation_epoch_with_historical_outbox(
        store,
        kafka_offset=20,
        bounded=True,
    )
    _materialize_activation_slots(
        store,
        identities,
        kafka_issue_id=7041712815,
    )
    dispatcher = _activation_dispatcher(
        store,
        _config(tmp_path, activation_required=True),
    )

    bounded = [dispatcher.dispatch_one() for _ in range(3)]
    assert all(item.status == "completed" for item in bounded)
    with pytest.raises(
        ActivationEpochError,
        match="activation_historical_backlog_not_drained",
    ):
        _confirm_activation_epoch(store, epoch_id, kafka_offset=20)

    outcome = dispatcher.dispatch_one()

    assert outcome.status == "idle"
    assert store.activation_epoch()["state"] == "bounded_active"
    outboxes = store.list_rows("rca_outbox")
    assert len(outboxes) == 4
    assert sum(row["status"] == "completed" for row in outboxes) == 3
    [historical] = [
        row for row in outboxes if row["activation_epoch_id"] is None
    ]
    assert historical["status"] == "pending"
    assert historical["attempt"] == 0


def test_current_epoch_blocks_historical_outbox_even_when_config_is_false(tmp_path):
    store = _store(tmp_path)
    _prepare_activation_epoch_with_historical_outbox(store, bounded=True)
    dispatcher = _activation_dispatcher(
        store,
        _config(tmp_path, activation_required=False),
    )

    outcome = dispatcher.dispatch_one()

    assert outcome.status == "idle"
    [outbox] = store.list_rows("rca_outbox")
    assert outbox["status"] == "pending"
    assert outbox["activation_epoch_id"] is None


def test_activation_bounded_exact_ledger_outbox_is_claimed(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    _prepare_activation_epoch(store, bounded=True)
    accepted = store.ingest_record(
        _record(offset=10),
        policy=_policy(),
        submit_enabled=True,
        activation_required=True,
        activation_slot_kind="kafka_success",
    )
    RcaDeliveryStore(tmp_path / "control.sqlite3")
    dispatcher = _activation_dispatcher(
        store,
        _config(tmp_path, activation_required=True),
    )

    outcome = dispatcher.dispatch_one()

    assert outcome.status == "completed"
    assert outcome.submission_key == accepted.submission_key
    [outbox] = store.list_rows("rca_outbox")
    assert outbox["status"] == "completed"
    assert outbox["activation_epoch_id"] == "rca-dispatcher-activation-20260712"
    assert outbox["activation_ledger_id"] is not None


def test_activation_steady_ledger_outbox_is_claimed(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    epoch_id, identities = _prepare_activation_epoch(store, bounded=True)
    _materialize_activation_slots(store, identities)
    RcaDeliveryStore(tmp_path / "control.sqlite3")
    dispatcher = _activation_dispatcher(
        store,
        _config(tmp_path, activation_required=True),
    )
    bounded = [dispatcher.dispatch_one() for _ in range(3)]
    assert all(item.status == "completed" for item in bounded)
    _confirm_activation_epoch(store, epoch_id)
    store.transition_activation_epoch(
        epoch_id=epoch_id,
        expected_state="confirmed",
        target_state="steady_active",
        operator="dispatcher-test",
        reason="enter steady dispatch",
    )
    accepted = store.ingest_record(
        _record(offset=30, issue_id=7041712816),
        policy=_policy(),
        submit_enabled=True,
        activation_required=True,
        activation_slot_kind="kafka_success",
    )

    outcome = dispatcher.dispatch_one()

    assert outcome.status == "completed"
    assert outcome.submission_key == accepted.submission_key
    assert store.activation_epoch()["state"] == "steady_active"
    outboxes = store.list_rows("rca_outbox")
    completed = next(
        row for row in outboxes if row["submission_key"] == accepted.submission_key
    )
    assert completed["status"] == "completed"
    assert all(row["status"] == "completed" for row in outboxes)


def test_delivery_backpressure_uses_high_low_hysteresis_before_claim(tmp_path):
    store = _store(tmp_path)
    config = replace(
        _config(tmp_path),
        delivery_high_watermark=3,
        delivery_resume_watermark=1,
    )
    snapshots = DeliverySnapshotSource(
        _delivery_snapshot(config, pending=1, claimed=1, uncertain=1),
        _delivery_snapshot(config, pending=1, retry_wait=1),
        _delivery_snapshot(config, pending=1),
    )
    submit_calls = []
    dispatcher = OutboxDispatcher(
        store=store,
        config=config,
        delivery_store=snapshots,
        enrich=_context,
        storage_admission=_storage_pass,
        derived_capacity_reservation=_reservation_pass,
        submit=lambda admission, request: (
            submit_calls.append(request) or _success(admission, request)
        ),
        now=_clock_for(store),
        lease_owner="worker-1",
    )

    high = dispatcher.dispatch_one()
    assert high.status == "downstream_backpressure"
    assert high.error_code == "delivery_pending_high_watermark"
    assert high.downstream_unresolved_effects == 3
    assert store.list_rows("rca_outbox")[0]["attempt"] == 0
    assert store.dispatcher_circuit().is_open is False

    hysteresis = dispatcher.dispatch_one()
    assert hysteresis.status == "downstream_backpressure"
    assert hysteresis.error_code == "delivery_pending_above_resume_watermark"
    assert hysteresis.downstream_unresolved_effects == 2
    assert store.list_rows("rca_outbox")[0]["attempt"] == 0

    assert dispatcher.dispatch_one().status == "completed"
    assert len(submit_calls) == 1
    assert dispatcher.stats.claimed == 1
    assert dispatcher.stats.delivery_backpressure_blocked == 2
    assert dispatcher.stats.delivery_backpressure_resumed == 1


def test_delivery_pipeline_work_counts_before_effect_creation(tmp_path):
    store = _store(tmp_path)
    config = replace(
        _config(tmp_path),
        delivery_high_watermark=3,
        delivery_resume_watermark=1,
    )
    dispatcher = OutboxDispatcher(
        store=store,
        config=config,
        delivery_store=DeliverySnapshotSource(
            _delivery_snapshot(config, pending_watches=3)
        ),
        enrich=lambda event: pytest.fail("must not enrich"),
        storage_admission=lambda request: pytest.fail("must not check storage"),
        derived_capacity_reservation=lambda request: pytest.fail(
            "must not reserve storage"
        ),
        submit=lambda admission, request: pytest.fail("must not submit"),
    )

    outcome = dispatcher.dispatch_one()

    assert outcome.status == "downstream_backpressure"
    assert outcome.downstream_unresolved_effects == 0
    assert outcome.downstream_unresolved_work == 3
    assert store.list_rows("rca_outbox")[0]["attempt"] == 0


def test_delivery_outcome_slo_blocks_new_outbox_without_opening_delivery_circuit(
    tmp_path,
):
    store = _store(tmp_path)
    config = _config(tmp_path)
    snapshots = DeliverySnapshotSource(
        _delivery_snapshot(config, outcome_slo_healthy=False),
        _delivery_snapshot(config, outcome_slo_healthy=True),
    )
    submit_calls = []
    dispatcher = OutboxDispatcher(
        store=store,
        config=config,
        delivery_store=snapshots,
        enrich=_context,
        storage_admission=_storage_pass,
        derived_capacity_reservation=_reservation_pass,
        submit=lambda admission, request: (
            submit_calls.append(request) or _success(admission, request)
        ),
        now=_clock_for(store),
        lease_owner="worker-1",
    )

    blocked = dispatcher.dispatch_one()

    assert blocked.status == "downstream_backpressure"
    assert blocked.error_code == "delivery_outcome_slo_failed"
    assert blocked.downstream_circuit_state == "closed"
    assert store.list_rows("rca_outbox")[0]["attempt"] == 0
    assert dispatcher.stats.claimed == 0
    assert dispatcher.stats.delivery_outcome_slo_blocked == 1
    health = dispatcher.delivery_backpressure_health()
    assert health["active"] is True
    assert health["last_snapshot"]["delivery_outcome_slo"]["healthy"] is False

    resumed = dispatcher.dispatch_one()

    assert resumed.status == "completed"
    assert len(submit_calls) == 1
    assert dispatcher.stats.delivery_backpressure_resumed == 1


def test_delivery_backpressure_probe_never_initializes_missing_schema(tmp_path):
    db_path = tmp_path / "control.sqlite3"
    store = RcaControlStore(db_path)
    store.ingest_record(_record(), policy=_policy(), submit_enabled=True)
    config = _config(tmp_path)
    dispatcher = OutboxDispatcher(
        store=store,
        config=config,
        enrich=lambda event: pytest.fail("must not enrich"),
        storage_admission=lambda request: pytest.fail("must not check storage"),
        derived_capacity_reservation=lambda request: pytest.fail(
            "must not reserve storage"
        ),
        submit=lambda admission, request: pytest.fail("must not submit"),
    )

    outcome = dispatcher.dispatch_one()

    assert outcome.status == "downstream_error"
    assert outcome.error_code == "delivery_backpressure_unavailable"
    with sqlite3.connect(db_path) as conn:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
    assert "rca_delivery_meta" not in tables


def test_delivery_circuit_blocks_before_claim_and_never_submits(tmp_path):
    store = _store(tmp_path)
    config = _config(tmp_path)
    dispatcher = OutboxDispatcher(
        store=store,
        config=config,
        delivery_store=DeliverySnapshotSource(
            _delivery_snapshot(config, circuit_state="open")
        ),
        enrich=lambda event: pytest.fail("must not enrich"),
        storage_admission=lambda request: pytest.fail("must not check storage"),
        derived_capacity_reservation=lambda request: pytest.fail(
            "must not reserve storage"
        ),
        submit=lambda admission, request: pytest.fail("must not submit"),
    )

    outcome = dispatcher.dispatch_one()
    assert outcome.status == "downstream_backpressure"
    assert outcome.error_code == "delivery_dispatcher_circuit_open"
    assert outcome.downstream_circuit_state == "open"
    assert store.list_rows("rca_outbox")[0]["attempt"] == 0
    assert store.dispatcher_circuit().is_open is False
    assert dispatcher.stats.claimed == 0
    assert dispatcher.stats.delivery_circuit_blocked == 1


def test_persisted_permanent_failure_circuit_blocks_submit_until_manual_reset(
    tmp_path,
):
    control = RcaControlStore(tmp_path / "control.sqlite3")
    completed_at = None
    for index in range(2):
        result = control.ingest_record(
            _record(offset=10 + index, issue_id=7041712812 + index),
            policy=_policy(),
            submit_enabled=True,
        )
        pending = next(
            row for row in control.list_rows("rca_outbox") if row["status"] == "pending"
        )
        completed_at = datetime.fromisoformat(pending["created_at"]) + timedelta(
            seconds=1
        )
        claim = control.claim_outbox(
            lease_owner="submission-worker",
            now=completed_at,
        )
        assert claim is not None
        control.complete_outbox(
            outbox_id=claim.outbox_id,
            lease_token=claim.lease_token,
            result={
                "success": True,
                "submission_key": result.submission_key,
                "task_id": result.submission_key,
                "task_state": "submitted",
                "deduped": False,
            },
            now=completed_at,
        )

    assert completed_at is not None
    delivery = RcaDeliveryStore(tmp_path / "control.sqlite3")
    assert delivery.backfill_completed_submissions(now=completed_at) == 2
    for index in range(2):
        watch = delivery.claim_due_watch(
            lease_owner="collector",
            now=completed_at + timedelta(seconds=index),
        )
        assert watch is not None
        delivery.terminal_failure(
            submission_key=watch.submission_key,
            lease_token=watch.lease_token,
            status={"success": True, "state": "failed"},
            error_code="vm_terminal_failed",
            error_detail="permanent VM task failure",
            now=completed_at + timedelta(seconds=index),
        )
    assert delivery.delivery_dispatcher_circuit().is_open is True

    pending_result = control.ingest_record(
        _record(offset=12, issue_id=7041712814),
        policy=_policy(),
        submit_enabled=True,
    )
    pending_row = next(
        row
        for row in control.list_rows("rca_outbox")
        if row["submission_key"] == pending_result.submission_key
    )
    clock = Clock(
        max(
            completed_at + timedelta(seconds=3),
            datetime.fromisoformat(pending_row["created_at"]) + timedelta(seconds=1),
        )
    )
    submit_calls = []
    dispatcher = OutboxDispatcher(
        store=control,
        config=_config(tmp_path),
        delivery_store=delivery,
        enrich=_context,
        storage_admission=_storage_pass,
        derived_capacity_reservation=_reservation_pass,
        submit=lambda admission, request: (
            submit_calls.append(request) or _success(admission, request)
        ),
        now=clock,
        lease_owner="worker-1",
    )

    blocked = dispatcher.dispatch_one()

    assert blocked.status == "downstream_backpressure"
    assert blocked.error_code == "delivery_dispatcher_circuit_open"
    assert submit_calls == []
    row = next(
        item
        for item in control.list_rows("rca_outbox")
        if item["submission_key"] == pending_result.submission_key
    )
    assert row["status"] == "pending"
    assert row["attempt"] == 0

    delivery.close_delivery_dispatcher_circuit(now=clock.current)
    resumed = dispatcher.dispatch_one()

    assert resumed.status == "completed"
    assert len(submit_calls) == 1
    assert delivery.permanent_failure_circuit_state()["consecutive_failures"] == 0


def test_delivery_database_failure_is_fail_closed_before_claim(tmp_path):
    store = _store(tmp_path)
    config = _config(tmp_path)
    dispatcher = OutboxDispatcher(
        store=store,
        config=config,
        delivery_store=DeliverySnapshotSource(
            RuntimeError("delivery_backpressure_store_unavailable:sqlite")
        ),
        enrich=lambda event: pytest.fail("must not enrich"),
        storage_admission=lambda request: pytest.fail("must not check storage"),
        derived_capacity_reservation=lambda request: pytest.fail(
            "must not reserve storage"
        ),
        submit=lambda admission, request: pytest.fail("must not submit"),
    )

    outcome = dispatcher.dispatch_one()
    assert outcome.status == "downstream_error"
    assert outcome.error_code == "delivery_backpressure_unavailable"
    assert store.list_rows("rca_outbox")[0]["attempt"] == 0
    assert dispatcher.stats.claimed == 0
    assert dispatcher.stats.delivery_backpressure_errors == 1
    health = HealthReporter(
        config,
        store,
        delivery_backpressure_status=dispatcher.delivery_backpressure_health,
    )
    health.write(
        state=outcome.status,
        stats=dispatcher.stats,
        last_outcome=outcome,
    )
    payload = json.loads(config.health_path.read_text(encoding="utf-8"))
    assert payload["ok"] is False
    assert payload["delivery_backpressure"]["last_error"]["code"] == (
        "delivery_backpressure_unavailable"
    )


def test_future_delivery_schema_is_fail_closed_before_claim(tmp_path):
    store = _store(tmp_path)
    config = _config(tmp_path)
    delivery_store = RcaDeliveryStore(config.delivery_db_path)
    conn = delivery_store._connect()
    try:
        conn.execute(
            "UPDATE rca_delivery_meta SET value = 'pnc_rca_delivery_store_v999' "
            "WHERE key = 'schema_version'"
        )
    finally:
        conn.close()
    dispatcher = OutboxDispatcher(
        store=store,
        config=config,
        delivery_store=delivery_store,
        enrich=lambda event: pytest.fail("must not enrich"),
        storage_admission=lambda request: pytest.fail("must not check storage"),
        derived_capacity_reservation=lambda request: pytest.fail(
            "must not reserve storage"
        ),
        submit=lambda admission, request: pytest.fail("must not submit"),
    )

    outcome = dispatcher.dispatch_one()
    assert outcome.status == "downstream_error"
    assert outcome.error_code == "delivery_backpressure_contract_invalid"
    assert store.list_rows("rca_outbox")[0]["attempt"] == 0


def test_dispatcher_rejects_lease_shorter_than_external_boundaries(tmp_path):
    with pytest.raises(ValueError, match="at least 180"):
        DispatcherConfig.from_env(
            {
                "HERMES_RCA_OUTBOX_DISPATCH_ENABLED": "false",
                "HERMES_RCA_OUTBOX_LEASE_SECONDS": "179",
            },
            hermes_home=tmp_path,
        )


def test_shadow_row_is_never_claimed_by_enabled_dispatcher(tmp_path):
    store = _store(tmp_path, shadow=True)
    dispatcher = OutboxDispatcher(
        store=store,
        config=_config(tmp_path),
        enrich=lambda event: pytest.fail("shadow must not enrich"),
        storage_admission=lambda request: pytest.fail("shadow must not check storage"),
        derived_capacity_reservation=lambda request: pytest.fail(
            "shadow must not reserve"
        ),
        submit=lambda admission, request: pytest.fail("shadow must not submit"),
    )

    assert dispatcher.dispatch_one().status == "idle"
    assert store.list_rows("rca_outbox")[0]["status"] == "shadow"


def test_dispatch_builds_canonical_paths_source_refs_and_completes(tmp_path):
    store = _store(tmp_path)
    captured = {}

    def submit(admission, request):
        captured["admission"] = admission
        captured["request"] = request
        return {
            "success": True,
            "deduped": True,
            "created": False,
            "task": {"task_id": admission.submission_key, "state": "running"},
        }

    dispatcher = OutboxDispatcher(
        store=store,
        config=_config(tmp_path),
        enrich=_context,
        storage_admission=_storage_pass,
        derived_capacity_reservation=_reservation_pass,
        submit=submit,
        now=_clock_for(store),
        lease_owner="worker-1",
    )
    outcome = dispatcher.dispatch_one()

    admission = captured["admission"]
    request = captured["request"]
    expected_vm = f"/mnt/tmp/{admission.submission_key}/"
    expected_cifs = (
        "//hfs1.minieye.tech/department-pnc_team-planning_algo-driving/tmp/"
        f"{admission.submission_key}/"
    )
    assert outcome.status == "completed"
    assert outcome.deduped is True
    assert request.data["artifact_root"] == expected_vm
    assert request.data["artifact_cifs_root"] == expected_cifs
    assert request.execution_policy["artifact_root"] == expected_vm
    assert request.data["data_access"]["mode"] == "remote_read"
    assert request.data["data_access"]["references"] == [
        {
            "kind": "event",
            "event_uuid": "0190abcd-1111-2222-3333-444455556666",
            "reader_class": "RemoteEventReader",
        }
    ]
    assert "pdcl_download_cmd" not in request.data
    assert request.execution_policy["mode"] == "remote_read"
    assert request.execution_policy["data_access_mode"] == "remote_read"
    assert request.execution_policy["allow_download"] is False
    assert request.execution_policy["input_materialization"] == "forbidden"
    assert request.execution_policy["derived_artifacts_allowed"] is True
    assert "storage_reservation" not in request.toolchain
    derived_reservation = request.toolchain["derived_capacity_reservation"]
    assert derived_reservation["status"] == "reserved"
    assert derived_reservation["capacity"]["atomic_reservation"] is True
    assert derived_reservation["contract"]["execution_identity"][
        "data_access_sha256"
    ] == dispatcher_module.canonical_data_access_sha256(request.data["data_access"])
    assert request.source_refs == {
        "task_id": admission.submission_key,
        "source_kind": "kafka_workflow_event",
        "source_event_id": f"{TOPIC}:2:10",
        "topic": TOPIC,
        "partition": 2,
        "offset": 10,
        "rule_version": "issue-created-v1",
        "generation": 1,
        "business_key": admission.business_key,
        "submission_key": admission.submission_key,
        "origin_source_id": store.list_rows("rca_outbox")[0]["origin_source_id"],
    }
    row = store.list_rows("rca_outbox")[0]
    assert row["status"] == "completed"
    assert row["lease_token"] is None
    result_receipt = json.loads(row["result_json"])
    assert result_receipt["deduped"] is True
    assert result_receipt["capacity_admission"]["schema_version"] == (
        "pnc_rca_derived_capacity_admission_v2"
    )
    assert result_receipt["capacity_admission"]["capacity_scope"] == (
        "derived_artifact_and_cache"
    )
    assert result_receipt["capacity_admission"]["atomic_reservation"] is False
    assert result_receipt["capacity_admission"]["status"] == "pass"
    assert len(result_receipt["capacity_admission"]["summary_sha256"]) == 64
    assert result_receipt["derived_capacity_reservation"]["atomic_reservation"] is True
    assert result_receipt["derived_capacity_reservation"]["status"] == "reserved"
    assert len(result_receipt["derived_capacity_reservation"]["receipt_sha256"]) == 64
    assert PDCL_COMMAND not in json.dumps(result_receipt, sort_keys=True)
    serialized_request = json.dumps(asdict(request), sort_keys=True)
    assert PDCL_COMMAND not in serialized_request
    assert "mdi download" not in serialized_request.lower()
    assert "[remote data reference redacted]" in serialized_request


def test_manual_outbox_dispatch_uses_source_neutral_request_lineage(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    store.persist_raw(_record(), policy=_policy(), submit_enabled=False)
    admission_result = store.admit_manual_trigger(
        ManualRcaTriggerRequest(
            schema_version=MANUAL_TRIGGER_SCHEMA_VERSION,
            issue_url="https://project.feishu.cn/g1q3/issue/detail/7041712812",
            mode="run_or_join",
            reason="manual production intake",
            platform="feishu",
            chat_id="oc_rca_fixed",
            thread_id="topic:om_root",
            message_id="om_manual_1",
            requester_id="ou_operator",
        ),
        allowed_chat_ids={"oc_rca_fixed"},
        submit_enabled=True,
    )
    RcaDeliveryStore(tmp_path / "control.sqlite3")
    captured = {}

    def submit(admission, request):
        captured["admission"] = admission
        captured["request"] = request
        return _success(admission, request)

    dispatcher = OutboxDispatcher(
        store=store,
        config=_config(tmp_path),
        enrich=_context,
        storage_admission=_storage_pass,
        derived_capacity_reservation=_reservation_pass,
        submit=submit,
        now=_clock_for(store),
        lease_owner="manual-worker",
    )

    outcome = dispatcher.dispatch_one()

    assert outcome.status == "completed"
    assert captured["admission"].submission_key == admission_result.submission_key
    assert captured["request"].source_refs == {
        "task_id": admission_result.submission_key,
        "source_kind": "feishu_group_manual",
        "rule_version": "issue-created-v1",
        "generation": 1,
        "business_key": admission_result.business_key,
        "submission_key": admission_result.submission_key,
        "origin_source_id": admission_result.source_id,
    }
    assert all(
        key not in captured["request"].source_refs
        for key in ("source_event_id", "topic", "partition", "offset")
    )


@pytest.mark.parametrize(
    "tamper",
    ["allow_download", "mdi_field", "mdi_text", "legacy_reservation"],
)
def test_execution_boundary_rejects_mdi_carry_or_authorization(
    tmp_path, monkeypatch, tamper
):
    store = _store(tmp_path)
    original_build = dispatcher_module.build_execution_request

    def tampered_build(**kwargs):
        request = original_build(**kwargs)
        if tamper == "allow_download":
            return replace(
                request,
                execution_policy={**request.execution_policy, "allow_download": True},
            )
        if tamper == "mdi_field":
            return replace(
                request,
                data={**request.data, "pdcl_download_cmd": PDCL_COMMAND},
            )
        if tamper == "mdi_text":
            return replace(
                request,
                evidence={
                    **request.evidence,
                    "description_markdown": PDCL_COMMAND,
                },
            )
        return replace(
            request,
            toolchain={
                **request.toolchain,
                "storage_reservation": {"status": "reserved"},
            },
        )

    monkeypatch.setattr(dispatcher_module, "build_execution_request", tampered_build)
    submit_calls = []
    dispatcher = OutboxDispatcher(
        store=store,
        config=_config(tmp_path),
        enrich=_context,
        storage_admission=_storage_pass,
        derived_capacity_reservation=_reservation_pass,
        submit=lambda admission, request: (
            submit_calls.append(request) or _success(admission, request)
        ),
        now=_clock_for(store),
        lease_owner="worker-1",
    )

    outcome = dispatcher.dispatch_one()

    assert outcome.status == "quarantined"
    assert outcome.error_code == "dispatcher_remote_data_access_invalid"
    assert submit_calls == []
    assert store.dispatcher_circuit().is_open is False


def test_default_submit_reconciles_only_a_released_derived_reservation(
    monkeypatch, tmp_path
):
    store = _store(tmp_path)
    clock = _clock_for(store)
    claim = store.claim_outbox(
        lease_owner="submit-test",
        lease_seconds=180,
        max_age_seconds=86_400,
        now=clock(),
    )
    assert claim is not None
    admission, event = dispatcher_module._validated_claim_contract(claim)
    artifact_root, artifact_cifs_root = dispatcher_module.canonical_artifact_paths(
        admission.submission_key
    )
    request = dispatcher_module.build_execution_request(
        request_kind="issue_intake",
        task_id=admission.submission_key,
        issue_context=_context(event),
        artifact_root=artifact_root,
        artifact_cifs_root=artifact_cifs_root,
        toolchain={"storage_admission": {"status": "pass"}},
    )
    captured = {}

    def fake_submit_service(**kwargs):
        captured.update(kwargs)
        return {"success": False, "error_code": "expected"}

    monkeypatch.setattr(
        "tools.vm_task_tool.vm_task_submit_service", fake_submit_service
    )

    dispatcher_module.default_submit(admission, request)

    assert captured["reconcile_only"] is False
    assert captured["capacity_mode"] == "steady"
    released = replace(
        request,
        toolchain={
            **request.toolchain,
            "derived_capacity_reservation": {"status": "released"},
        },
    )
    dispatcher_module.default_submit(admission, released)
    assert captured["reconcile_only"] is True


def test_default_submit_reloads_release_bound_bootstrap_authorization_per_call(
    monkeypatch, tmp_path
):
    store = _store(tmp_path)
    claim = store.claim_outbox(
        lease_owner="bootstrap-submit-test",
        lease_seconds=180,
        max_age_seconds=86_400,
        now=_clock_for(store)(),
    )
    assert claim is not None
    admission, event = dispatcher_module._validated_claim_contract(claim)
    artifact_root, artifact_cifs_root = dispatcher_module.canonical_artifact_paths(
        admission.submission_key
    )
    request = dispatcher_module.build_execution_request(
        request_kind="issue_intake",
        task_id=admission.submission_key,
        issue_context=_context(event),
        artifact_root=artifact_root,
        artifact_cifs_root=artifact_cifs_root,
        toolchain={"storage_admission": {"status": "pass"}},
    )
    config = replace(
        _config(tmp_path),
        capacity_mode="bootstrap",
        release_id="rca-prod-20260713-001",
        bootstrap_epoch_id="rca-bootstrap-release-20260713",
    )
    loads = []
    submits = []

    def fake_load(selected_config):
        loads.append(selected_config)
        return {
            "bootstrap_epoch_id": "rca-bootstrap-release-20260713",
            "release_bom_sha256": "ab" * 32,
            "started_at": "2026-07-13T00:00:00+00:00",
            "deadline": "2026-07-20T00:00:00+00:00",
            "receipt_fingerprint": "ef" * 32,
            "active_release_binding_sha256": "12" * 32,
            "candidate_env_sha256": "34" * 32,
        }

    monkeypatch.setattr(
        dispatcher_module, "_load_bound_bootstrap_authorization", fake_load
    )
    monkeypatch.setattr(
        "tools.vm_task_tool.vm_task_submit_service",
        lambda **kwargs: submits.append(kwargs) or {"success": False},
    )

    dispatcher_module.default_submit(admission, request, config=config)
    dispatcher_module.default_submit(admission, request, config=config)

    assert len(loads) == 2
    assert loads == [config, config]
    assert submits[0]["capacity_mode"] == "bootstrap"
    assert submits[0]["bootstrap_epoch_id"] == "rca-bootstrap-release-20260713"
    assert submits[0]["release_bom_sha256"] == "ab" * 32
    assert submits[0]["bootstrap_authorization_fingerprint"] == "ef" * 32
    assert submits[0]["active_release_binding_sha256"] == "12" * 32


def test_default_submit_reloads_dynamic_mode_and_holds_shared_lock(
    monkeypatch, tmp_path
):
    config = replace(
        _config(tmp_path),
        capacity_mode="bootstrap",
        release_id="rca-prod-20260713-001",
        bootstrap_epoch_id="rca-bootstrap-release-20260713",
    )
    capacity_runtime = FakeCapacityRuntime(
        [
            _runtime_decision(
                state="STEADY_READY",
                mode="bootstrap",
                generation=1,
                irreversible=False,
            ),
            _runtime_decision(),
        ]
    )
    monkeypatch.setattr(
        dispatcher_module,
        "_load_bound_bootstrap_authorization",
        lambda _config: {
            "bootstrap_epoch_id": config.bootstrap_epoch_id,
            "release_bom_sha256": "a" * 64,
            "started_at": "2026-07-13T00:00:00Z",
            "deadline": "2026-07-21T00:00:00Z",
            "receipt_fingerprint": "b" * 64,
            "active_release_binding_sha256": "c" * 64,
        },
    )
    submits = []

    def submit_service(**kwargs):
        assert capacity_runtime.inside is True
        submits.append(kwargs)
        return {"success": False}

    monkeypatch.setattr(
        "tools.vm_task_tool.vm_task_submit_service", submit_service
    )
    request = type("Request", (), {"toolchain": {}})()
    dispatcher_module.default_submit(
        object(), request, config=config, capacity_runtime=capacity_runtime
    )
    dispatcher_module.default_submit(
        object(), request, config=config, capacity_runtime=capacity_runtime
    )

    assert capacity_runtime.calls == 2
    assert [item["capacity_mode"] for item in submits] == ["bootstrap", "steady"]
    assert "bootstrap_epoch_id" in submits[0]
    assert submits[0]["active_release_binding_sha256"] == "c" * 64
    assert "bootstrap_epoch_id" not in submits[1]
    assert "active_release_binding_sha256" not in submits[1]


def test_default_submit_dynamic_block_never_reaches_vm_boundary(monkeypatch, tmp_path):
    capacity_runtime = FakeCapacityRuntime(
        [_runtime_decision(state="STEADY_BLOCKED", mode="blocked", ready=False)]
    )
    monkeypatch.setattr(
        "tools.vm_task_tool.vm_task_submit_service",
        lambda **_kwargs: pytest.fail("blocked capacity must suppress VM submit"),
    )
    with pytest.raises(DispatchCircuitError, match="steady_evidence_missing") as error:
        dispatcher_module.default_submit(
            object(),
            type("Request", (), {"toolchain": {}})(),
            config=_config(tmp_path),
            capacity_runtime=capacity_runtime,
        )
    assert error.value.code == "dispatcher_capacity_runtime_blocked"


def test_default_submit_dynamic_bootstrap_binding_drift_never_reaches_vm_boundary(
    monkeypatch, tmp_path
):
    config = replace(
        _config(tmp_path),
        capacity_mode="bootstrap",
        release_id="rca-prod-20260713-001",
        bootstrap_epoch_id="rca-bootstrap-release-20260713",
    )
    capacity_runtime = FakeCapacityRuntime(
        [
            _runtime_decision(
                state="STEADY_READY",
                mode="bootstrap",
                generation=1,
                irreversible=False,
            )
        ]
    )
    monkeypatch.setattr(
        dispatcher_module,
        "_load_bound_bootstrap_authorization",
        lambda _config: {
            "bootstrap_epoch_id": config.bootstrap_epoch_id,
            "release_bom_sha256": "a" * 64,
            "started_at": "2026-07-13T00:00:00Z",
            "deadline": "2026-07-21T00:00:00Z",
            "receipt_fingerprint": "b" * 64,
            "active_release_binding_sha256": "d" * 64,
        },
    )
    monkeypatch.setattr(
        "tools.vm_task_tool.vm_task_submit_service",
        lambda **_kwargs: pytest.fail("binding drift must suppress VM submit"),
    )

    with pytest.raises(
        DispatchCircuitError, match="active_release_binding_mismatch"
    ):
        dispatcher_module.default_submit(
            object(),
            type("Request", (), {"toolchain": {}})(),
            config=config,
            capacity_runtime=capacity_runtime,
        )


def test_default_submit_bootstrap_authorization_failure_opens_global_boundary(
    monkeypatch, tmp_path
):
    config = replace(
        _config(tmp_path),
        capacity_mode="bootstrap",
        release_id="rca-prod-20260713-001",
        bootstrap_epoch_id="rca-bootstrap-release-20260713",
    )
    monkeypatch.setattr(
        dispatcher_module,
        "_load_bound_bootstrap_authorization",
        lambda _config: (_ for _ in ()).throw(
            dispatcher_module.RcaBootstrapAuthorizationError(
                "rca_bootstrap_authorization_file_unavailable"
            )
        ),
    )

    with pytest.raises(
        dispatcher_module.DispatchCircuitError,
        match="rca_bootstrap_authorization_file_unavailable",
    ) as error:
        dispatcher_module.default_submit(
            object(), type("Request", (), {"toolchain": {}})(), config=config
        )

    assert error.value.code == "dispatcher_bootstrap_authorization_invalid"


def test_bound_bootstrap_loader_rejects_authority_replacement(
    monkeypatch, tmp_path
):
    config = replace(
        _config(tmp_path),
        capacity_mode="bootstrap",
        release_id="rca-prod-20260713-001",
        bootstrap_epoch_id="rca-bootstrap-release-20260713",
    )
    binding = {
        "binding_receipt_sha256": "11" * 32,
        "release_id": config.release_id,
        "bootstrap_epoch_id": config.bootstrap_epoch_id,
        "release_bom_sha256": "22" * 32,
        "approval_evidence_sha256": "33" * 32,
        "authorization_receipt_sha256": "44" * 32,
        "authorization_fingerprint": "55" * 32,
        "candidate_env_sha256": "66" * 32,
    }
    monkeypatch.setattr(
        dispatcher_module,
        "load_active_release_binding",
        lambda **_kwargs: dict(binding),
    )
    authorization = {
        "authorization_receipt_sha256": "44" * 32,
        "receipt_fingerprint": "55" * 32,
    }
    captured = {}

    def load_authorization(**kwargs):
        captured.update(kwargs)
        return dict(authorization)

    monkeypatch.setattr(
        dispatcher_module, "load_bootstrap_authorization", load_authorization
    )

    result = dispatcher_module._load_bound_bootstrap_authorization(config)
    assert result["active_release_binding_sha256"] == "11" * 32
    assert captured == {
        "expected_epoch_id": config.bootstrap_epoch_id,
        "expected_release_bom_sha256": "22" * 32,
        "expected_release_approval_id": config.release_id,
        "expected_approval_evidence_sha256": "33" * 32,
    }

    authorization["receipt_fingerprint"] = "77" * 32
    with pytest.raises(
        dispatcher_module.RcaBootstrapAuthorizationError,
        match="authorization_identity_mismatch",
    ):
        dispatcher_module._load_bound_bootstrap_authorization(config)


def test_storage_admission_runs_between_enrichment_and_submit_and_is_redacted(
    tmp_path,
):
    store = _store(tmp_path)
    calls = []
    captured = {}

    def enrich(event):
        calls.append("enrich")
        return _context(event)

    def storage(request):
        calls.append("storage")
        return _storage_payload(request)

    def submit(admission, request):
        calls.append("submit")
        captured["request"] = request
        return _success(admission, request)

    def reserve(request):
        calls.append("reservation")
        return _reservation_pass(request)

    dispatcher = OutboxDispatcher(
        store=store,
        config=_config(tmp_path),
        enrich=enrich,
        storage_admission=storage,
        derived_capacity_reservation=reserve,
        submit=submit,
        now=_clock_for(store),
        lease_owner="worker-1",
    )

    assert dispatcher.dispatch_one().status == "completed"
    assert calls == ["enrich", "storage", "reservation", "submit"]
    summary = captured["request"].toolchain["storage_admission"]
    serialized = json.dumps(summary, sort_keys=True)
    assert summary["schema_version"] == "pnc_rca_derived_capacity_admission_v2"
    assert summary["capacity_scope"] == "derived_artifact_and_cache"
    assert summary["atomic_reservation"] is False
    assert summary["input_materialization_bytes_per_case"] == 0
    assert summary["input_materialization"] == "forbidden"
    assert summary["policy"]["requested_cases"] == 4
    assert summary["policy"]["requested_cases_scope"] == (
        "this_capacity_admission_only"
    )
    assert summary["policy"]["assumed_cases_per_day"] == 200
    assert summary["policy"]["expected_derived_artifact_bytes_per_case"] == (
        1_000_000_000
    )
    for forbidden in (
        "/mnt/tmp",
        "available_bytes",
        "free_bytes",
        "secret",
    ):
        assert forbidden not in serialized


def test_storage_blocked_retries_without_submit_and_is_reevaluated(tmp_path):
    store = _store(tmp_path)
    clock = _clock_for(store)
    storage_calls = 0
    submit_calls = 0

    def storage(request):
        nonlocal storage_calls
        storage_calls += 1
        return _storage_payload(
            request, status="blocked" if storage_calls == 1 else "pass"
        )

    def submit(admission, request):
        nonlocal submit_calls
        submit_calls += 1
        return _success(admission, request)

    dispatcher = OutboxDispatcher(
        store=store,
        config=_config(tmp_path),
        enrich=_context,
        storage_admission=storage,
        derived_capacity_reservation=_reservation_pass,
        submit=submit,
        now=clock,
        lease_owner="worker-1",
    )

    blocked = dispatcher.dispatch_one()
    assert blocked.status == "pending"
    assert blocked.error_code == "storage_admission_blocked"
    assert storage_calls == 1
    assert submit_calls == 0
    clock.current = datetime.fromisoformat(blocked.next_attempt_at)

    assert dispatcher.dispatch_one().status == "completed"
    assert storage_calls == 2
    assert submit_calls == 1
    assert dispatcher.stats.storage_admission_blocked == 1
    assert dispatcher.stats.storage_admission_passed == 1


def test_derived_atomic_reservation_runs_without_mdi_or_duplicate_legacy_boundary(
    tmp_path,
):
    store = _store(tmp_path)
    reservation_requests = []
    submit_calls = 0

    def reserve(request):
        reservation_requests.append(request)
        return _reservation_pass(request)

    def submit(admission, request):
        nonlocal submit_calls
        submit_calls += 1
        return _success(admission, request)

    dispatcher = OutboxDispatcher(
        store=store,
        config=_config(tmp_path),
        enrich=_context,
        storage_admission=_storage_pass,
        derived_capacity_reservation=reserve,
        submit=submit,
        now=_clock_for(store),
        lease_owner="worker-1",
    )

    assert dispatcher.dispatch_one().status == "completed"
    assert len(reservation_requests) == 1
    payload = json.dumps(reservation_requests[0].payload(), sort_keys=True)
    assert "data_access_sha256" in payload
    assert "pdcl_download_cmd" not in payload
    assert "mdi download" not in payload.lower()
    assert not hasattr(dispatcher, "storage_reservation")
    assert submit_calls == 1


def test_derived_reservation_blocked_retries_and_is_reevaluated(tmp_path):
    store = _store(tmp_path)
    clock = _clock_for(store)
    reservation_calls = 0
    submit_calls = 0

    def reserve(request):
        nonlocal reservation_calls
        reservation_calls += 1
        status = "waiting_capacity" if reservation_calls == 1 else "reserved"
        return validate_derived_capacity_reservation_receipt(
            _derived_reservation_receipt(request, status=status), request
        )

    def submit(admission, request):
        nonlocal submit_calls
        submit_calls += 1
        return _success(admission, request)

    dispatcher = OutboxDispatcher(
        store=store,
        config=_config(tmp_path),
        enrich=_context,
        storage_admission=_storage_pass,
        derived_capacity_reservation=reserve,
        submit=submit,
        now=clock,
        lease_owner="worker-1",
    )

    blocked = dispatcher.dispatch_one()
    assert blocked.status == "pending"
    assert blocked.error_code == "derived_capacity_reservation_blocked"
    assert submit_calls == 0
    clock.current = datetime.fromisoformat(blocked.next_attempt_at)

    assert dispatcher.dispatch_one().status == "completed"
    assert reservation_calls == 2
    assert submit_calls == 1
    assert dispatcher.stats.derived_capacity_reservation_blocked == 1
    assert dispatcher.stats.derived_capacity_reservation_admitted == 1


def test_released_derived_reservation_submits_reconcile_only_receipt(tmp_path):
    store = _store(tmp_path)
    captured = {}

    def reserve(request):
        return validate_derived_capacity_reservation_receipt(
            _derived_reservation_receipt(request, status="released"), request
        )

    def submit(admission, request):
        captured["request"] = request
        return _success(admission, request)

    dispatcher = OutboxDispatcher(
        store=store,
        config=_config(tmp_path),
        enrich=_context,
        storage_admission=_storage_pass,
        derived_capacity_reservation=reserve,
        submit=submit,
        now=_clock_for(store),
        lease_owner="worker-1",
    )

    assert dispatcher.dispatch_one().status == "completed"
    assert (
        captured["request"].toolchain["derived_capacity_reservation"]["status"]
        == "released"
    )
    assert dispatcher.stats.derived_capacity_reservation_admitted == 0


def test_invalid_derived_reservation_decision_quarantines_one_case(tmp_path):
    store = _store(tmp_path)
    dispatcher = OutboxDispatcher(
        store=store,
        config=_config(tmp_path),
        enrich=_context,
        storage_admission=_storage_pass,
        derived_capacity_reservation=lambda request: {"not": "a decision"},
        submit=lambda admission, request: pytest.fail("must not submit"),
        now=_clock_for(store),
        lease_owner="worker-1",
    )

    outcome = dispatcher.dispatch_one()
    assert outcome.status == "quarantined"
    assert outcome.error_code == "derived_capacity_reservation_schema_invalid"
    assert dispatcher.stats.derived_capacity_reservation_errors == 1
    assert store.dispatcher_circuit().is_open is False


def test_single_reservation_timeout_retries_without_opening_global_circuit(tmp_path):
    store = _store(tmp_path)

    def timeout(_request):
        raise dispatcher_module.DerivedCapacityReservationError(
            "derived_capacity_reservation_timeout",
            "boundary timed out",
        )

    dispatcher = OutboxDispatcher(
        store=store,
        config=_config(tmp_path),
        enrich=_context,
        storage_admission=_storage_pass,
        derived_capacity_reservation=timeout,
        submit=lambda admission, request: pytest.fail("must not submit"),
        now=_clock_for(store),
        lease_owner="worker-1",
    )

    outcome = dispatcher.dispatch_one()
    assert outcome.status == "pending"
    assert outcome.error_code == "derived_capacity_reservation_timeout"
    assert store.dispatcher_circuit().is_open is False


@pytest.mark.parametrize(
    ("storage", "expected_code"),
    [
        (
            lambda request: _storage_payload(
                request, extra={"schema_version": "wrong-v1"}
            ),
            "storage_admission_schema_invalid",
        ),
        (
            lambda request: (_ for _ in ()).throw(OSError("unreadable")),
            "storage_admission_call_failed",
        ),
    ],
)
def test_storage_schema_drift_circuits_but_call_failure_only_retries(
    tmp_path, storage, expected_code
):
    store = _store(tmp_path)
    submit_calls = []
    dispatcher = OutboxDispatcher(
        store=store,
        config=_config(tmp_path),
        enrich=_context,
        storage_admission=storage,
        derived_capacity_reservation=_reservation_pass,
        submit=lambda admission, request: (
            submit_calls.append(request) or _success(admission, request)
        ),
        now=_clock_for(store),
        lease_owner="worker-1",
    )

    outcome = dispatcher.dispatch_one()
    expected_status = (
        "circuit_open"
        if expected_code == "storage_admission_schema_invalid"
        else "pending"
    )
    assert outcome.status == expected_status
    assert outcome.error_code == expected_code
    assert submit_calls == []
    assert dispatcher.stats.storage_admission_errors == 1
    assert store.dispatcher_circuit().is_open is (
        expected_code == "storage_admission_schema_invalid"
    )


def test_dispatch_config_cannot_bypass_required_admission_gates(tmp_path):
    required = {
        "HERMES_RCA_OUTBOX_DISPATCH_ENABLED": "true",
        "HERMES_RCA_OUTBOX_STORAGE_ADMISSION_ENABLED": "true",
        "HERMES_RCA_OUTBOX_STORAGE_RESERVATION_ENABLED": "false",
        "HERMES_RCA_OUTBOX_DERIVED_CAPACITY_RESERVATION_ENABLED": "true",
        "HERMES_RCA_OUTBOX_DELIVERY_BACKPRESSURE_ENABLED": "true",
    }
    with pytest.raises(ValueError, match="must all be true"):
        DispatcherConfig.from_env(
            {
                "HERMES_RCA_OUTBOX_DISPATCH_ENABLED": "true",
                "HERMES_RCA_OUTBOX_DELIVERY_BACKPRESSURE_ENABLED": "true",
            },
            hermes_home=tmp_path,
        )
    with pytest.raises(ValueError, match="must all be true"):
        DispatcherConfig.from_env(
            {
                "HERMES_RCA_OUTBOX_DISPATCH_ENABLED": "true",
                "HERMES_RCA_OUTBOX_STORAGE_ADMISSION_ENABLED": "true",
            },
            hermes_home=tmp_path,
        )
    with pytest.raises(ValueError, match="must be false"):
        DispatcherConfig.from_env(
            {
                "HERMES_RCA_OUTBOX_DISPATCH_ENABLED": "true",
                "HERMES_RCA_OUTBOX_STORAGE_ADMISSION_ENABLED": "true",
                "HERMES_RCA_OUTBOX_STORAGE_RESERVATION_ENABLED": "true",
            },
            hermes_home=tmp_path,
        )

    config = _config(tmp_path)
    assert config.data_access_mode == "remote_read"
    assert config.storage_admission_enabled is True
    assert config.storage_reservation_enabled is False
    assert config.derived_capacity_reservation_enabled is True
    assert config.derived_capacity_atomic_reservation is True
    assert config.delivery_backpressure_enabled is True
    assert config.delivery_db_path == config.control_db_path
    assert config.delivery_high_watermark == 100
    assert config.delivery_resume_watermark == 50
    assert config.storage_concurrency_reserve_cases == 4
    assert config.storage_cases_per_day == 200
    assert config.storage_expected_artifact_cache_bytes == 1_000_000_000
    assert config.storage_reserve_percent == 30
    assert config.storage_timeout_seconds == 45
    assert config.derived_capacity_reservation_timeout_seconds == 120
    assert config.input_wait_max_age_seconds == 900
    assert config.public_dict()["storage_admission_enabled"] is True
    assert config.public_dict()["storage_reservation_enabled"] is False
    assert config.public_dict()["derived_capacity_reservation_enabled"] is True
    assert config.public_dict()["delivery_backpressure_enabled"] is True
    assert config.public_dict()["data_access_mode"] == "remote_read"
    assert config.public_dict()["allow_download"] is False
    assert config.public_dict()["storage_capacity_scope"] == (
        "derived_artifact_and_cache"
    )
    assert config.public_dict()["derived_capacity_atomic_reservation"] is True
    assert config.public_dict()["storage_expected_artifact_cache_bytes"] == (
        1_000_000_000
    )
    assert config.public_dict()["input_wait_max_age_seconds"] == 900
    assert config.capacity_mode == "steady"
    assert config.public_dict()["capacity_mode"] == "steady"

    bootstrap_values = required | {
        dispatcher_module.PROD_CAPACITY_MODE_ENV: "bootstrap",
        dispatcher_module.PROD_RELEASE_ID_ENV: "rca-prod-20260713-001",
        dispatcher_module.PROD_BOOTSTRAP_EPOCH_ID_ENV: (
            "rca-bootstrap-release-20260713"
        ),
    }
    bootstrap_config = DispatcherConfig.from_env(
        bootstrap_values, hermes_home=tmp_path
    )
    assert bootstrap_config.capacity_mode == "bootstrap"
    assert bootstrap_config.release_id == "rca-prod-20260713-001"
    assert bootstrap_config.bootstrap_epoch_id == (
        "rca-bootstrap-release-20260713"
    )
    assert bootstrap_config.active_release_binding_path == (
        tmp_path
        / "runtime"
        / "pnc_agent"
        / "feishu_issue_kafka_rca"
        / "active-release-binding.json"
    )
    assert bootstrap_config.live_env_path == tmp_path / ".env"
    with pytest.raises(ValueError, match="exactly steady or bootstrap"):
        DispatcherConfig.from_env(
            required | {dispatcher_module.PROD_CAPACITY_MODE_ENV: "other"},
            hermes_home=tmp_path,
        )
    with pytest.raises(ValueError, match="requires release and epoch ids"):
        DispatcherConfig.from_env(
            required | {dispatcher_module.PROD_CAPACITY_MODE_ENV: "bootstrap"},
            hermes_home=tmp_path,
        )

    bypassed = replace(config, storage_admission_enabled=False)
    with pytest.raises(ValueError, match="derived-capacity admission"):
        OutboxDispatcher(
            store=_store(tmp_path),
            config=bypassed,
            enrich=_context,
            storage_admission=_storage_pass,
            derived_capacity_reservation=_reservation_pass,
            submit=_success,
        )

    bypassed = replace(config, storage_reservation_enabled=True)
    with pytest.raises(ValueError, match="legacy MDI-bound storage reservation"):
        OutboxDispatcher(
            store=_store(tmp_path),
            config=bypassed,
            enrich=_context,
            storage_admission=_storage_pass,
            derived_capacity_reservation=_reservation_pass,
            submit=_success,
        )

    bypassed = replace(config, derived_capacity_reservation_enabled=False)
    with pytest.raises(ValueError, match="atomic reservation"):
        OutboxDispatcher(
            store=_store(tmp_path),
            config=bypassed,
            enrich=_context,
            storage_admission=_storage_pass,
            derived_capacity_reservation=_reservation_pass,
            submit=_success,
        )

    bypassed = replace(config, delivery_backpressure_enabled=False)
    with pytest.raises(ValueError, match="delivery backpressure"):
        OutboxDispatcher(
            store=_store(tmp_path),
            config=bypassed,
            enrich=_context,
            storage_admission=_storage_pass,
            derived_capacity_reservation=_reservation_pass,
            submit=_success,
        )

    with pytest.raises(ValueError, match="must be greater than"):
        DispatcherConfig.from_env(
            required
            | {
                "HERMES_RCA_OUTBOX_DELIVERY_HIGH_WATERMARK": "10",
                "HERMES_RCA_OUTBOX_DELIVERY_RESUME_WATERMARK": "10",
            },
            hermes_home=tmp_path,
        )
    with pytest.raises(ValueError, match="must equal"):
        DispatcherConfig.from_env(
            required
            | {
                "HERMES_RCA_OUTBOX_CONTROL_DB_PATH": str(tmp_path / "control.sqlite3"),
                "HERMES_RCA_OUTBOX_DELIVERY_DB_PATH": str(tmp_path / "other.sqlite3"),
            },
            hermes_home=tmp_path,
        )
    for invalid_horizon in (59, 3601):
        with pytest.raises(ValueError, match="INPUT_WAIT_MAX_AGE_SECONDS"):
            DispatcherConfig.from_env(
                required
                | {
                    "HERMES_RCA_OUTBOX_INPUT_WAIT_MAX_AGE_SECONDS": str(
                        invalid_horizon
                    )
                },
                hermes_home=tmp_path,
            )
    assert _config(tmp_path, input_wait_max_age_seconds=60).input_wait_max_age_seconds == 60
    assert (
        _config(tmp_path, input_wait_max_age_seconds=3600).input_wait_max_age_seconds
        == 3600
    )
    with pytest.raises(ValueError, match="must not exceed"):
        DispatcherConfig.from_env(
            required
            | {
                "HERMES_RCA_OUTBOX_MAX_AGE_SECONDS": "899",
                "HERMES_RCA_OUTBOX_INPUT_WAIT_MAX_AGE_SECONDS": "900",
            },
            hermes_home=tmp_path,
        )

    with pytest.raises(ValueError, match="callable derived-capacity"):
        OutboxDispatcher(
            store=_store(tmp_path),
            config=config,
            enrich=_context,
            storage_admission=_storage_pass,
            derived_capacity_reservation=None,
            submit=_success,
        )


def test_remote_read_config_rejects_download_and_legacy_input_budget(tmp_path):
    base = {
        "HERMES_RCA_OUTBOX_DISPATCH_ENABLED": "false",
        "HERMES_RCA_OUTBOX_STORAGE_RESERVATION_ENABLED": "false",
    }
    for forbidden_value in ("true", "1", "0", "no", ""):
        with pytest.raises(ValueError, match="absent or exactly false"):
            DispatcherConfig.from_env(
                base | {"HERMES_RCA_OUTBOX_ALLOW_DOWNLOAD": forbidden_value},
                hermes_home=tmp_path,
            )

    explicit_false = DispatcherConfig.from_env(
        base
        | {
            "HERMES_RCA_OUTBOX_ALLOW_DOWNLOAD": "false",
            "HERMES_RCA_OUTBOX_STORAGE_EXPECTED_ARTIFACT_CACHE_BYTES": ("2000000000"),
        },
        hermes_home=tmp_path,
    )
    assert explicit_false.data_access_mode == "remote_read"
    assert explicit_false.storage_expected_artifact_cache_bytes == 2_000_000_000

    with pytest.raises(ValueError, match="must be exactly remote_read"):
        DispatcherConfig.from_env(
            base | {"HERMES_RCA_OUTBOX_DATA_ACCESS_MODE": "download"},
            hermes_home=tmp_path,
        )
    with pytest.raises(ValueError, match="unsupported for remote-read RCA"):
        DispatcherConfig.from_env(
            base | {"HERMES_RCA_OUTBOX_STORAGE_EXPECTED_INPUT_BYTES": "1"},
            hermes_home=tmp_path,
        )


def test_default_storage_gate_uses_absolute_governed_wrapper_and_timeout(
    monkeypatch,
):
    request = StorageAdmissionRequest(
        requested_cases=4,
        assumed_cases_per_day=200,
        expected_artifact_cache_bytes=1_000_000_000,
        reserve_percent=30,
        timeout_seconds=17,
    )
    captured = {}

    def fake_run(command, **kwargs):
        captured["command"] = command
        captured.update(kwargs)
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=json.dumps(_storage_payload(request)),
            stderr="",
        )

    monkeypatch.setattr(dispatcher_module.subprocess, "run", fake_run)
    result = default_storage_admission(request)

    assert result["status"] == "pass"
    assert captured["command"] == [
        str(Path.home() / ".local" / "bin" / "ssh-mini-agent"),
        "run_py_json",
    ]
    assert Path(captured["command"][0]).is_absolute()
    assert "ssh-mini-run" not in " ".join(captured["command"])
    assert captured["timeout"] == 17
    assert captured["env"]["SSH_MINI_AGENT_TIMEOUT"] == "17"
    assert captured["check"] is False
    assert dispatcher_module.REMOTE_STORAGE_ADMISSION_MODULE in captured["input"]
    assert "evaluate_storage_admission(" in captured["input"]
    assert "expected_derived_artifact_bytes=" in captured["input"]
    assert "expected_input_bytes=" not in captured["input"]
    assert "sys.modules[spec.name] = module" in captured["input"]
    assert "subprocess" not in captured["input"]


def test_default_storage_gate_timeout_is_circuit_error(monkeypatch):
    request = StorageAdmissionRequest(4, 200, 1_000_000_000, 30, 9)

    def timeout(*args, **kwargs):
        raise subprocess.TimeoutExpired(args[0], kwargs["timeout"])

    monkeypatch.setattr(dispatcher_module.subprocess, "run", timeout)

    with pytest.raises(DispatchCircuitError) as caught:
        default_storage_admission(request)
    assert caught.value.code == "storage_admission_timeout"


def test_default_storage_gate_strictly_rejects_wrong_schema(monkeypatch):
    request = StorageAdmissionRequest(4, 200, 1_000_000_000, 30, 9)
    wrong = _storage_payload(request, extra={"schema_version": "wrong-v1"})
    monkeypatch.setattr(
        dispatcher_module.subprocess,
        "run",
        lambda command, **kwargs: subprocess.CompletedProcess(
            command, 0, stdout=json.dumps(wrong), stderr=""
        ),
    )

    with pytest.raises(DispatchCircuitError) as caught:
        default_storage_admission(request)
    assert caught.value.code == "storage_admission_schema_invalid"


def test_eventual_consistency_retry_uses_declared_schedule(tmp_path):
    store = _store(tmp_path)
    clock = _clock_for(store)
    dispatcher = OutboxDispatcher(
        store=store,
        config=_config(tmp_path),
        enrich=lambda event: (_ for _ in ()).throw(
            EnrichmentNotReady("issue_not_visible", "not visible yet")
        ),
        storage_admission=lambda request: pytest.fail(
            "not ready must not check storage"
        ),
        derived_capacity_reservation=lambda request: pytest.fail(
            "not ready must not reserve"
        ),
        submit=lambda admission, request: pytest.fail("not ready must not submit"),
        now=clock,
        lease_owner="worker-1",
    )

    first = dispatcher.dispatch_one()
    assert first.status == "pending"
    assert datetime.fromisoformat(first.next_attempt_at) == clock.current + timedelta(
        seconds=2
    )
    assert dispatcher.dispatch_one().status == "idle"

    clock.current += timedelta(seconds=2)
    second = dispatcher.dispatch_one()
    assert second.attempt == 2
    assert datetime.fromisoformat(second.next_attempt_at) == clock.current + timedelta(
        seconds=5
    )


def test_issue_preread_timeout_is_persisted_as_retryable_input_wait(
    tmp_path, monkeypatch
):
    from gateway.pnc_issue_context import G1Q3IssueReadResult

    store = _store(tmp_path)
    clock = _clock_for(store)
    monkeypatch.setattr(
        "gateway.pnc_issue_context.fetch_g1q3_issue_context_result",
        lambda **_kwargs: G1Q3IssueReadResult(
            status="read_failed",
            blocker={
                "kind": "host_issue_preread_timeout",
                "message": "bounded issue preread timed out",
                "retryable": True,
            },
            errors=[{"tool": "meegle", "error_class": "TimeoutError"}],
        ),
    )
    dispatcher = OutboxDispatcher(
        store=store,
        config=_config(tmp_path),
        enrich=dispatcher_module.default_enrich_event,
        storage_admission=lambda request: pytest.fail(
            "timeout retry must not check storage"
        ),
        derived_capacity_reservation=lambda request: pytest.fail(
            "timeout retry must not reserve storage"
        ),
        submit=lambda admission, request: pytest.fail(
            "timeout retry must not submit"
        ),
        now=clock,
        lease_owner="timeout-retry-worker",
    )

    outcome = dispatcher.dispatch_one()

    assert outcome.status == "pending"
    assert outcome.error_code == "host_issue_preread_timeout"
    [row] = store.list_rows("rca_outbox")
    assert row["status"] == "pending"
    assert row["last_error_code"] == "host_issue_preread_timeout"


def test_retry_crossing_configured_age_horizon_with_live_lease_is_quarantined(tmp_path):
    store = _store(tmp_path)
    clock = _clock_for(store)
    created = datetime.fromisoformat(store.list_rows("rca_outbox")[0]["created_at"])

    def enrichment(_event):
        clock.current = created + timedelta(seconds=61)
        raise EnrichmentNotReady("issue_not_visible", "still not visible")

    dispatcher = OutboxDispatcher(
        store=store,
        config=replace(
            _config(
                tmp_path,
                max_age_seconds=86_400,
                input_wait_max_age_seconds=60,
            ),
            lease_seconds=180,
        ),
        enrich=enrichment,
        storage_admission=lambda request: pytest.fail("must not check storage"),
        derived_capacity_reservation=lambda request: pytest.fail(
            "must not reserve storage"
        ),
        submit=lambda admission, request: pytest.fail("must not submit"),
        now=clock,
        lease_owner="worker-1",
    )

    outcome = dispatcher.dispatch_one()
    assert outcome.status == "quarantined"
    row = store.list_rows("rca_outbox")[0]
    assert row["status"] == "quarantined"
    assert row["last_error_code"] == "issue_not_visible"


def test_input_wait_retry_schedule_is_capped_at_horizon_and_quarantines_on_deadline(
    tmp_path,
):
    store = _store(tmp_path)
    [row] = store.list_rows("rca_outbox")
    window_started = datetime.fromisoformat(row["retry_window_started_at"])
    deadline = window_started + timedelta(seconds=900)
    clock = Clock(window_started + timedelta(seconds=1))
    enrichment_calls = 0

    def not_ready(_event):
        nonlocal enrichment_calls
        enrichment_calls += 1
        raise EnrichmentNotReady(
            "issue_field_missing_remote_data_reference",
            "field not visible yet",
        )

    dispatcher = OutboxDispatcher(
        store=store,
        config=_config(tmp_path),
        enrich=not_ready,
        storage_admission=lambda request: pytest.fail("must not check storage"),
        derived_capacity_reservation=lambda request: pytest.fail(
            "must not reserve storage"
        ),
        submit=lambda admission, request: pytest.fail("must not submit"),
        now=clock,
        lease_owner="input-horizon-worker",
    )

    outcomes = []
    for _ in range(20):
        outcome = dispatcher.dispatch_one()
        outcomes.append(outcome)
        if outcome.status == "quarantined":
            break
        assert outcome.status == "pending"
        next_attempt = datetime.fromisoformat(outcome.next_attempt_at)
        assert next_attempt <= deadline
        clock.current = next_attempt

    assert outcomes[-1].status == "quarantined"
    assert clock.current == deadline
    assert enrichment_calls == len(outcomes)
    [quarantined] = store.list_rows("rca_outbox")
    assert quarantined["status"] == "quarantined"
    assert quarantined["last_error_code"] == (
        "issue_field_missing_remote_data_reference"
    )


def test_non_enrichment_transient_keeps_general_retry_horizon(tmp_path):
    store = _store(tmp_path)
    [row] = store.list_rows("rca_outbox")
    created = datetime.fromisoformat(row["created_at"])
    clock = Clock(created + timedelta(seconds=901))
    dispatcher = OutboxDispatcher(
        store=store,
        config=_config(tmp_path, input_wait_max_age_seconds=60),
        enrich=lambda _event: (_ for _ in ()).throw(RuntimeError("transient")),
        storage_admission=lambda request: pytest.fail("must not check storage"),
        derived_capacity_reservation=lambda request: pytest.fail(
            "must not reserve storage"
        ),
        submit=lambda admission, request: pytest.fail("must not submit"),
        now=clock,
        lease_owner="general-horizon-worker",
    )

    outcome = dispatcher.dispatch_one()

    assert outcome.status == "pending"
    assert outcome.error_code == "enrichment_or_submit_exception"
    assert store.list_rows("rca_outbox")[0]["last_error_code"] == (
        "enrichment_or_submit_exception"
    )


def test_missing_input_quarantine_then_kafka_update_rearms_and_submits_once(
    tmp_path,
):
    store = _store(tmp_path)
    [initial] = store.list_rows("rca_outbox")
    created = datetime.fromisoformat(initial["created_at"])
    wait_clock = Clock(created + timedelta(seconds=61))
    waiting_dispatcher = OutboxDispatcher(
        store=store,
        config=_config(tmp_path, input_wait_max_age_seconds=60),
        enrich=lambda _event: (_ for _ in ()).throw(
            EnrichmentNotReady(
                "issue_field_missing_remote_data_reference",
                "data reference not published yet",
            )
        ),
        storage_admission=lambda request: pytest.fail("must not check storage"),
        derived_capacity_reservation=lambda request: pytest.fail(
            "must not reserve storage"
        ),
        submit=lambda admission, request: pytest.fail("must not submit"),
        now=wait_clock,
        lease_owner="input-wait-worker",
    )
    waiting = waiting_dispatcher.dispatch_one()
    assert waiting.status == "quarantined"

    update = store.ingest_record(
        _record(offset=11),
        policy=_policy(),
        submit_enabled=True,
    )
    assert update.outbox_rearmed is True
    assert update.reason == "input_wait_quarantine_rearmed"
    [rearmed] = store.list_rows("rca_outbox")
    assert rearmed["status"] == "pending"
    assert rearmed["source_offset"] == 11
    assert rearmed["submission_key"] == initial["submission_key"]
    assert len(store.list_rows("rca_outbox")) == 1

    submit_calls = []
    submit_clock = Clock(
        datetime.fromisoformat(rearmed["retry_window_started_at"])
        + timedelta(seconds=1)
    )
    ready_dispatcher = OutboxDispatcher(
        store=store,
        config=_config(tmp_path, input_wait_max_age_seconds=60),
        enrich=_context,
        storage_admission=_storage_pass,
        derived_capacity_reservation=_reservation_pass,
        submit=lambda admission, request: (
            submit_calls.append((admission, request))
            or _success(admission, request)
        ),
        now=submit_clock,
        lease_owner="post-update-worker",
    )

    completed = ready_dispatcher.dispatch_one()

    assert completed.status == "completed"
    assert len(submit_calls) == 1
    admission, request = submit_calls[0]
    assert admission.submission_key == initial["submission_key"]
    assert admission.source_refs.offset == 11
    assert request.source_refs["offset"] == 11
    [final] = store.list_rows("rca_outbox")
    assert final["status"] == "completed"
    assert final["outbox_id"] == initial["outbox_id"]
    assert len(store.list_rows("rca_outbox_rearm_audit")) == 1


def test_dispatcher_renews_lease_before_each_external_boundary(tmp_path):
    store = _store(tmp_path)
    clock = _clock_for(store)

    def enrichment(event):
        clock.current += timedelta(seconds=100)
        return _context(event)

    def storage(request):
        clock.current += timedelta(seconds=100)
        return _storage_pass(request)

    def reserve(request):
        clock.current += timedelta(seconds=100)
        return _reservation_pass(request)

    def submit(admission, request):
        clock.current += timedelta(seconds=100)
        return _success(admission, request)

    dispatcher = OutboxDispatcher(
        store=store,
        config=_config(tmp_path),
        enrich=enrichment,
        storage_admission=storage,
        derived_capacity_reservation=reserve,
        submit=submit,
        now=clock,
        lease_owner="worker-1",
    )

    outcome = dispatcher.dispatch_one()

    assert outcome.status == "completed"
    assert dispatcher.stats.lease_lost == 0


def test_worker_that_loses_lease_during_submit_stops_without_old_token_mutation(
    tmp_path,
):
    store = _store(tmp_path)
    clock = _clock_for(store)
    reclaimed = []

    def submit(admission, request):
        clock.current += timedelta(seconds=181)
        reclaimed.append(
            store.claim_outbox(
                lease_owner="worker-2",
                lease_seconds=180,
                now=clock.current,
            )
        )
        return _success(admission, request)

    dispatcher = OutboxDispatcher(
        store=store,
        config=_config(tmp_path),
        enrich=_context,
        storage_admission=_storage_pass,
        derived_capacity_reservation=_reservation_pass,
        submit=submit,
        now=clock,
        lease_owner="worker-1",
    )

    outcome = dispatcher.dispatch_one()

    assert reclaimed[0] is not None
    assert outcome.status == "lease_lost"
    assert outcome.error_code == "stale_outbox_lease"
    assert dispatcher.stats.lease_lost == 1
    row = store.list_rows("rca_outbox")[0]
    assert row["status"] == "claimed"
    assert row["lease_owner"] == "worker-2"
    assert row["result_json"] is None
    assert store.dispatcher_circuit().is_open is False


def test_enrichment_identity_mismatch_quarantines_only_case(tmp_path):
    store = _store(tmp_path)

    def wrong_context(event):
        return RcaIssueContext(
            project_key=str(event["project_key"]),
            work_item_type=str(event["work_item_type_key"]),
            work_item_id="999999",
            source_quality="full",
        )

    dispatcher = OutboxDispatcher(
        store=store,
        config=_config(tmp_path),
        enrich=wrong_context,
        storage_admission=lambda request: pytest.fail(
            "bad identity must not check storage"
        ),
        derived_capacity_reservation=lambda request: pytest.fail(
            "bad identity must not reserve"
        ),
        submit=lambda admission, request: pytest.fail("must fail before submit"),
        now=_clock_for(store),
        lease_owner="worker-1",
    )

    outcome = dispatcher.dispatch_one()
    assert outcome.status == "quarantined"
    assert outcome.error_code == "dispatcher_enrichment_identity_mismatch"
    assert store.dispatcher_circuit().is_open is False


@pytest.mark.parametrize("corruption", ["malformed_json", "forged_admission_hash"])
def test_durable_poison_row_is_quarantined_and_next_row_continues(tmp_path, corruption):
    store = _store(tmp_path)
    store.ingest_record(
        _record(offset=11, issue_id=7041712813),
        policy=_policy(),
        submit_enabled=True,
    )
    row = store.list_rows("rca_outbox")[0]
    if corruption == "malformed_json":
        payload_json = "not-json"
    else:
        payload = json.loads(row["payload_json"])
        payload["admission"]["submission_key"] = "g1q3-rca-s1-" + "0" * 64
        payload_json = json.dumps(payload, sort_keys=True)
    conn = store._connect()
    try:
        conn.execute(
            "UPDATE rca_outbox SET payload_json = ? WHERE outbox_id = ?",
            (payload_json, row["outbox_id"]),
        )
    finally:
        conn.close()
    boundary_calls = []
    dispatcher = OutboxDispatcher(
        store=store,
        config=_config(tmp_path),
        enrich=lambda event: boundary_calls.append("enrich") or _context(event),
        storage_admission=lambda request: (
            boundary_calls.append("storage") or _storage_pass(request)
        ),
        derived_capacity_reservation=lambda request: (
            boundary_calls.append("reservation") or _reservation_pass(request)
        ),
        submit=lambda admission, request: (
            boundary_calls.append("submit") or _success(admission, request)
        ),
        now=_clock_for(store),
        lease_owner="worker-1",
    )

    outcome = dispatcher.dispatch_one()
    assert outcome.status == "quarantined"
    assert outcome.error_code == "dispatcher_outbox_contract_invalid"
    assert boundary_calls == []
    updated = next(
        item
        for item in store.list_rows("rca_outbox")
        if item["outbox_id"] == row["outbox_id"]
    )
    assert updated["status"] == "quarantined"
    assert updated["last_error_code"] == "dispatcher_outbox_contract_invalid"
    assert store.dispatcher_circuit().is_open is False

    assert dispatcher.dispatch_one().status == "completed"
    assert boundary_calls == ["enrich", "storage", "reservation", "submit"]


def test_service_auth_error_opens_circuit_and_fails_closed(tmp_path, monkeypatch):
    store = _store(tmp_path)
    monkeypatch.setattr(
        store,
        "open_dispatcher_circuit",
        lambda **_kwargs: pytest.fail("dispatcher must use the atomic store API"),
    )
    dispatcher = OutboxDispatcher(
        store=store,
        config=_config(tmp_path),
        enrich=_context,
        storage_admission=_storage_pass,
        derived_capacity_reservation=_reservation_pass,
        derived_capacity_abort_precreate=_precreate_abort_pass,
        submit=lambda admission, request: {
            "success": False,
            "error_code": "vm_task_service_permission_denied",
            "error": "service capability missing",
            "retryable": False,
        },
        now=_clock_for(store),
        lease_owner="worker-1",
    )

    outcome = dispatcher.dispatch_one()
    assert outcome.status == "circuit_open"
    assert store.dispatcher_circuit().reason_code == (
        "vm_task_service_permission_denied"
    )
    assert store.list_rows("rca_outbox")[0]["status"] == "pending"
    detail = store.list_rows("rca_outbox")[0]["last_error_detail"]
    assert "derived_capacity_precreate_abort=" in detail
    assert '"operation":"abort_precreate"' in detail
    assert '"state":"expired"' in detail
    assert '"task_id":' in detail
    assert '"receipt_sha256":' in detail
    assert detail.endswith("}")
    assert len(detail) < 1000
    assert dispatcher.stats.derived_capacity_precreate_aborted == 1


@pytest.mark.parametrize(
    "error_code",
    [
        "vm_task_service_reservation_invalid",
        "vm_task_service_reservation_reconcile_mismatch",
        "vm_task_service_reservation_not_admitted",
    ],
)
def test_service_case_contract_rejection_aborts_and_quarantines_only_case(
    tmp_path, error_code
):
    store = _store(tmp_path)
    dispatcher = OutboxDispatcher(
        store=store,
        config=_config(tmp_path),
        enrich=_context,
        storage_admission=_storage_pass,
        derived_capacity_reservation=_reservation_pass,
        derived_capacity_abort_precreate=_precreate_abort_pass,
        submit=lambda admission, request: {
            "success": False,
            "error_code": error_code,
            "error": "case contract rejected",
            "retryable": False,
            "created": False,
        },
        now=_clock_for(store),
        lease_owner="worker-1",
    )

    outcome = dispatcher.dispatch_one()
    assert outcome.status == "quarantined"
    assert outcome.error_code == error_code
    assert store.dispatcher_circuit().is_open is False
    row = store.list_rows("rca_outbox")[0]
    assert "derived_capacity_precreate_abort=" in row["last_error_detail"]
    assert dispatcher.stats.derived_capacity_precreate_aborted == 1


def test_precreate_abort_timeout_preserves_hold_and_retries_without_global_circuit(
    tmp_path,
):
    store = _store(tmp_path)

    def abort_timeout(_request, _receipt):
        raise dispatcher_module.DerivedCapacityReservationError(
            "derived_capacity_reservation_abort_precreate_timeout",
            "pre-create abort timed out",
        )

    dispatcher = OutboxDispatcher(
        store=store,
        config=_config(tmp_path),
        enrich=_context,
        storage_admission=_storage_pass,
        derived_capacity_reservation=_reservation_pass,
        derived_capacity_abort_precreate=abort_timeout,
        submit=lambda admission, request: {
            "success": False,
            "error_code": "vm_task_service_request_invalid",
            "error": "request rejected before create",
            "retryable": False,
            "created": False,
        },
        now=_clock_for(store),
        lease_owner="worker-1",
    )

    outcome = dispatcher.dispatch_one()
    assert outcome.status == "pending"
    assert outcome.error_code == (
        "derived_capacity_reservation_abort_precreate_timeout"
    )
    assert store.dispatcher_circuit().is_open is False
    assert dispatcher.stats.derived_capacity_precreate_abort_errors == 1


@pytest.mark.parametrize(
    ("retryable", "expected_status"), [(True, "pending"), (None, "quarantined")]
)
def test_nondefinitive_created_false_never_aborts_precreate_reservation(
    tmp_path, retryable, expected_status
):
    store = _store(tmp_path)
    abort_calls = 0

    def abort(_request, _receipt):
        nonlocal abort_calls
        abort_calls += 1
        return _precreate_abort_pass(_request, _receipt)

    dispatcher = OutboxDispatcher(
        store=store,
        config=_config(tmp_path),
        enrich=_context,
        storage_admission=_storage_pass,
        derived_capacity_reservation=_reservation_pass,
        derived_capacity_abort_precreate=abort,
        submit=lambda admission, request: {
            "success": False,
            "error_code": "vm_task_service_temporarily_unavailable",
            "error": "retry after transient service pressure",
            "retryable": retryable,
            "created": False,
        },
        now=_clock_for(store),
        lease_owner="worker-1",
    )

    outcome = dispatcher.dispatch_one()
    assert outcome.status == expected_status
    assert outcome.error_code == "vm_task_service_temporarily_unavailable"
    assert abort_calls == 0
    assert dispatcher.stats.derived_capacity_precreate_aborted == 0
    assert store.dispatcher_circuit().is_open is False


def test_uncertain_submit_exception_preserves_reservation_for_reconcile(tmp_path):
    store = _store(tmp_path)
    abort_calls = 0

    def abort(_request, _receipt):
        nonlocal abort_calls
        abort_calls += 1
        return _precreate_abort_pass(_request, _receipt)

    def uncertain_submit(_admission, _request):
        raise TimeoutError("submit outcome unknown")

    dispatcher = OutboxDispatcher(
        store=store,
        config=_config(tmp_path),
        enrich=_context,
        storage_admission=_storage_pass,
        derived_capacity_reservation=_reservation_pass,
        derived_capacity_abort_precreate=abort,
        submit=uncertain_submit,
        now=_clock_for(store),
        lease_owner="worker-1",
    )

    outcome = dispatcher.dispatch_one()
    assert outcome.status == "pending"
    assert outcome.error_code == "enrichment_or_submit_exception"
    assert abort_calls == 0
    assert store.dispatcher_circuit().is_open is False


def test_success_with_wrong_task_identity_quarantines_only_case(tmp_path):
    store = _store(tmp_path)
    dispatcher = OutboxDispatcher(
        store=store,
        config=_config(tmp_path),
        enrich=_context,
        storage_admission=_storage_pass,
        derived_capacity_reservation=_reservation_pass,
        submit=lambda admission, request: {
            "success": True,
            "task": {"task_id": "different-task", "state": "submitted"},
        },
        now=_clock_for(store),
        lease_owner="worker-1",
    )

    outcome = dispatcher.dispatch_one()
    assert outcome.status == "quarantined"
    assert outcome.error_code == "dispatcher_submit_identity_mismatch"
    assert store.list_rows("rca_outbox")[0]["status"] == "quarantined"
    assert store.dispatcher_circuit().is_open is False


def test_resident_loop_waits_on_circuit_and_recovers_after_external_clear(tmp_path):
    store = _store(tmp_path)
    config = _config(tmp_path)
    clock = _clock_for(store)
    submit_calls = 0

    def submit(admission, request):
        nonlocal submit_calls
        submit_calls += 1
        if submit_calls == 1:
            return {
                "success": False,
                "error_code": "vm_task_service_permission_denied",
                "error": "test circuit",
                "retryable": False,
            }
        return _success(admission, request)

    dispatcher = OutboxDispatcher(
        store=store,
        config=config,
        enrich=_context,
        storage_admission=_storage_pass,
        derived_capacity_reservation=_reservation_pass,
        derived_capacity_abort_precreate=_precreate_abort_pass,
        submit=submit,
        now=clock,
        lease_owner="worker-1",
    )
    health = HealthReporter(config, store)
    sleeps = []
    stopping = False

    def sleep(seconds):
        nonlocal stopping
        sleeps.append(seconds)
        if len(sleeps) == 1:
            assert store.dispatcher_circuit().is_open is True
            store.close_dispatcher_circuit(now=clock.current)
            clock.current += timedelta(seconds=config.circuit_poll_interval_seconds)
        else:
            stopping = True

    run_dispatch_loop(
        dispatcher,
        health,
        stop_requested=lambda: stopping,
        sleep=sleep,
    )

    assert sleeps == [
        config.circuit_poll_interval_seconds,
        config.poll_interval_seconds,
    ]
    assert submit_calls == 2
    assert store.list_rows("rca_outbox")[0]["status"] == "completed"
    health_payload = json.loads(config.health_path.read_text(encoding="utf-8"))
    assert health_payload["state"] == "idle"


def test_resident_loop_heartbeats_while_dispatch_batch_is_blocked(tmp_path):
    store = _store(tmp_path)
    config = _config(tmp_path)
    observed = []
    stopping = False

    class BlockingDispatcher:
        def __init__(self):
            self.config = config
            self.stats = dispatcher_module.DispatchStats()

        def dispatch_batch(self):
            initial = json.loads(config.health_path.read_text(encoding="utf-8"))
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                payload = json.loads(
                    config.health_path.read_text(encoding="utf-8")
                )
                if (
                    payload["heartbeat_at"] != initial["heartbeat_at"]
                    and payload["liveness"]["state"] == "processing"
                ):
                    observed.append((initial, payload))
                    break
                time.sleep(0.005)
            assert observed, "heartbeat did not advance during a blocked batch"
            return [dispatcher_module.DispatchOutcome(status="idle")]

    def sleep(_seconds):
        nonlocal stopping
        stopping = True

    health = HealthReporter(config, store)
    run_dispatch_loop(
        BlockingDispatcher(),
        health,
        stop_requested=lambda: stopping,
        sleep=sleep,
        heartbeat_interval_seconds=0.01,
    )

    initial, during = observed[0]
    assert initial["state"] == "starting"
    assert during["state"] == "starting"
    assert during["healthy"] is True
    assert during["readiness_observed_at"] == initial["readiness_observed_at"]
    assert during["readiness"]["state"] == "starting"
    assert during["liveness"]["readiness_observed_at"] == (
        initial["readiness_observed_at"]
    )
    assert not any(
        thread.name == "pnc-rca-outbox-heartbeat" and thread.is_alive()
        for thread in threading.enumerate()
    )


def test_health_v2_binds_config_and_immutable_runtime_identity(tmp_path):
    store = _store(tmp_path)
    config = _config(tmp_path, activation_required=True)
    reporter = HealthReporter(config, store)

    reporter.write(state="idle", stats=dispatcher_module.DispatchStats())
    first = json.loads(config.health_path.read_text(encoding="utf-8"))
    reporter.write(state="idle", stats=dispatcher_module.DispatchStats())
    second = json.loads(config.health_path.read_text(encoding="utf-8"))

    expected_config = config.public_dict()
    assert first["schema_version"] == "pnc_rca_outbox_dispatcher_health_v2"
    assert first["enabled"] is True
    assert first["healthy"] is True
    assert first["ok"] is True
    assert first["activation_required"] is True
    assert first["config"]["activation_required"] is True
    assert first["config"] == expected_config
    assert first["runtime_identity"]["service_label"] == (
        "local.pnc.rca-outbox-dispatcher"
    )
    assert first["runtime_identity"]["public_config_sha256"] == (
        dispatcher_module.canonical_json_sha256(expected_config)
    )
    assert len(first["runtime_identity"]["loaded_runtime_sha256"]) == 64
    assert first["runtime_identity"] == second["runtime_identity"]
    assert first["workspace_runtime"] == {
        "required": True,
        "bound": True,
        "ready": True,
        "state": "ready",
        "error_code": "",
        "startup_error_code": "",
        "identity": WORKSPACE_RUNTIME.to_dict(),
    }
    assert first["readiness"]["ready_for_dispatch"] is True
    serialized = json.dumps(first, ensure_ascii=False, sort_keys=True)
    assert "ACC braking issue" not in serialized
    assert "top-secret-password" not in serialized


def test_health_status_rejects_identity_without_loaded_runtime_digest(tmp_path):
    store = _store(tmp_path)
    config = _config(tmp_path)
    reporter = HealthReporter(config, store)
    reporter.write(state="idle", stats=dispatcher_module.DispatchStats())
    payload = json.loads(config.health_path.read_text(encoding="utf-8"))
    payload["runtime_identity"].pop("loaded_runtime_sha256")
    config.health_path.write_text(json.dumps(payload), encoding="utf-8")

    status = dispatcher_module.read_health_status(config, max_age_seconds=60)

    assert status["ok"] is False
    assert status["health_check"]["reason"] == "dispatcher_reported_unhealthy"


def test_disabled_health_is_healthy_but_not_enabled(tmp_path):
    store = _store(tmp_path)
    config = _config(tmp_path, enabled=False)
    reporter = HealthReporter(config, store)

    reporter.write(
        state="disabled",
        stats=dispatcher_module.DispatchStats(),
        last_outcome=dispatcher_module.DispatchOutcome(status="disabled"),
    )
    payload = json.loads(config.health_path.read_text(encoding="utf-8"))
    status = dispatcher_module.read_health_status(config, max_age_seconds=60)

    assert payload["state"] == "disabled"
    assert payload["enabled"] is False
    assert payload["healthy"] is True
    assert payload["ok"] is True
    assert payload["readiness"]["ready_for_dispatch"] is False
    assert status["ok"] is True


def test_enabled_health_fails_closed_when_workspace_runtime_is_missing(tmp_path):
    store = _store(tmp_path)
    config = _config(tmp_path)

    def unavailable():
        raise WorkspaceRuntimeError("rca_workspace_runtime_directory_unavailable")

    reporter = HealthReporter(
        config,
        store,
        workspace_runtime_observer=unavailable,
    )
    reporter.write(state="idle", stats=dispatcher_module.DispatchStats())
    payload = json.loads(config.health_path.read_text(encoding="utf-8"))

    assert payload["healthy"] is False
    assert payload["ok"] is False
    assert payload["readiness"]["ready_for_dispatch"] is False
    assert payload["workspace_runtime"]["required"] is True
    assert payload["workspace_runtime"]["ready"] is False
    assert payload["workspace_runtime"]["error_code"] == (
        "rca_workspace_runtime_directory_unavailable"
    )
    guard = reporter.dispatch_guard_outcome()
    assert guard is not None
    assert guard.status == "workspace_runtime_unavailable"


def test_activation_required_health_fails_closed_even_when_dispatch_disabled(
    tmp_path,
):
    store = _store(tmp_path)
    config = _config(tmp_path, enabled=False, activation_required=True)

    def unavailable():
        raise WorkspaceRuntimeError("rca_workspace_runtime_directory_unavailable")

    reporter = HealthReporter(
        config,
        store,
        workspace_runtime_observer=unavailable,
    )
    reporter.write(state="disabled", stats=dispatcher_module.DispatchStats())
    payload = json.loads(config.health_path.read_text(encoding="utf-8"))

    assert payload["enabled"] is False
    assert payload["activation_required"] is True
    assert payload["healthy"] is False
    assert payload["workspace_runtime"]["required"] is True


def test_disabled_dev_health_observes_missing_runtime_without_claiming_ready(tmp_path):
    store = _store(tmp_path)
    config = _config(tmp_path, enabled=False)

    def unavailable():
        raise WorkspaceRuntimeError("rca_workspace_runtime_directory_unavailable")

    reporter = HealthReporter(
        config,
        store,
        workspace_runtime_observer=unavailable,
    )
    reporter.write(state="disabled", stats=dispatcher_module.DispatchStats())
    payload = json.loads(config.health_path.read_text(encoding="utf-8"))

    assert payload["healthy"] is True
    assert payload["liveness"]["state"] == "reporting"
    assert payload["readiness"]["ready_for_dispatch"] is False
    assert payload["workspace_runtime"]["required"] is False
    assert payload["workspace_runtime"]["ready"] is False
    assert reporter.dispatch_guard_outcome() is None


def test_health_detects_bundle_identity_drift_after_startup(tmp_path):
    store = _store(tmp_path)
    config = _config(tmp_path)
    current = [WORKSPACE_RUNTIME]
    reporter = HealthReporter(
        config,
        store,
        workspace_runtime_observer=lambda: current[0],
    )
    reporter.write(state="idle", stats=dispatcher_module.DispatchStats())
    current[0] = replace(WORKSPACE_RUNTIME, closure_sha256="e" * 64)

    reporter.write(state="idle", stats=dispatcher_module.DispatchStats())
    payload = json.loads(config.health_path.read_text(encoding="utf-8"))

    assert payload["healthy"] is False
    assert payload["workspace_runtime"]["state"] == "drifted"
    assert payload["workspace_runtime"]["identity"] == WORKSPACE_RUNTIME.to_dict()
    assert payload["workspace_runtime"]["observed_identity"] == current[0].to_dict()


def _valid_bootstrap_authorization_projection() -> dict:
    return {
        "authorization_ready": True,
        "capacity_mode": "bootstrap",
        "bootstrap_epoch_id": "rca-bootstrap-release-20260713",
        "started_at": "2026-07-13T00:00:00+00:00",
        "deadline": "2026-07-20T00:00:00+00:00",
        "receipt_fingerprint": "a" * 64,
        "authorization_receipt_sha256": "b" * 64,
        "active_release_binding_sha256": "e" * 64,
        "candidate_env_sha256": "f" * 64,
        "release_bom_sha256": "c" * 64,
        "release_approval_id": "rca-prod-20260713-001",
        "approval_evidence_sha256": "d" * 64,
    }


def test_bootstrap_health_and_dispatch_guard_fail_closed_when_authority_disappears(
    tmp_path,
):
    store = _store(tmp_path)
    config = replace(
        _config(tmp_path),
        capacity_mode="bootstrap",
        release_id="rca-prod-20260713-001",
        bootstrap_epoch_id="rca-bootstrap-release-20260713",
    )
    current = [_valid_bootstrap_authorization_projection()]

    def observe():
        value = current[0]
        if isinstance(value, Exception):
            raise value
        return value

    reporter = HealthReporter(
        config,
        store,
        bootstrap_authorization_observer=observe,
    )
    reporter.write(state="idle", stats=dispatcher_module.DispatchStats())
    ready = json.loads(config.health_path.read_text(encoding="utf-8"))
    assert ready["healthy"] is True
    assert ready["capacity_admission"]["required"] is True
    assert ready["capacity_admission"]["ready"] is True

    current[0] = dispatcher_module.RcaBootstrapAuthorizationError(
        "rca_bootstrap_authorization_file_unavailable"
    )
    reporter.write(state="idle", stats=dispatcher_module.DispatchStats())
    blocked = json.loads(config.health_path.read_text(encoding="utf-8"))
    assert blocked["healthy"] is False
    assert blocked["readiness"]["ready_for_dispatch"] is False
    assert blocked["capacity_admission"] == {
        "required": True,
        "ready": False,
        "state": "unavailable",
        "error_code": "rca_bootstrap_authorization_file_unavailable",
        "capacity_mode": "bootstrap",
        "authorization": None,
    }
    guard = reporter.dispatch_guard_outcome()
    assert guard is not None
    assert guard.status == "capacity_authorization_unavailable"
    assert guard.error_code == "rca_bootstrap_authorization_file_unavailable"


def test_dynamic_steady_health_exposes_ratchet_and_passes_reader(tmp_path):
    store = _store(tmp_path)
    config = replace(
        _config(tmp_path),
        capacity_mode="bootstrap",
        release_id="rca-prod-20260713-001",
        bootstrap_epoch_id="rca-bootstrap-release-20260713",
    )
    capacity_runtime = FakeCapacityRuntime([_runtime_decision()])
    reporter = HealthReporter(
        config,
        store,
        capacity_runtime=capacity_runtime,
    )
    reporter.write(state="idle", stats=dispatcher_module.DispatchStats())
    payload = json.loads(config.health_path.read_text(encoding="utf-8"))
    status = dispatcher_module.read_health_status(config, max_age_seconds=60)

    capacity = payload["capacity_admission"]
    assert payload["healthy"] is True
    assert status["ok"] is True
    assert capacity["capacity_mode"] == "steady"
    assert capacity["runtime"]["generation"] == 2
    assert capacity["runtime"]["current_release_id"] == config.release_id
    assert capacity["runtime"]["ratchet_origin_release_id"] == (
        "rca-prod-origin-20260713"
    )
    assert "gateway/pnc_rca_capacity_runtime.py" in RCA_RUNTIME_RELATIVE_FILES
    assert "gateway/pnc_rca_capacity_transition.py" in RCA_RUNTIME_RELATIVE_FILES


def test_dynamic_bootstrap_health_rejects_active_release_binding_drift(tmp_path):
    store = _store(tmp_path)
    config = replace(
        _config(tmp_path),
        capacity_mode="bootstrap",
        release_id="rca-prod-20260713-001",
        bootstrap_epoch_id="rca-bootstrap-release-20260713",
    )
    decision = _runtime_decision(
        state="STEADY_READY",
        mode="bootstrap",
        generation=1,
        irreversible=False,
    )
    authorization = _valid_bootstrap_authorization_projection()
    assert decision["active_release_binding_sha256"] != authorization[
        "active_release_binding_sha256"
    ]
    reporter = HealthReporter(
        config,
        store,
        capacity_runtime=FakeCapacityRuntime([decision]),
        bootstrap_authorization_observer=lambda: authorization,
    )

    reporter.write(state="idle", stats=dispatcher_module.DispatchStats())
    payload = json.loads(config.health_path.read_text(encoding="utf-8"))
    guard = reporter.dispatch_guard_outcome()

    assert payload["healthy"] is False
    assert payload["readiness"]["ready_for_dispatch"] is False
    assert payload["capacity_admission"]["ready"] is False
    assert payload["capacity_admission"]["error_code"] == (
        "rca_bootstrap_authorization_projection_invalid"
    )
    assert guard is not None
    assert guard.status == "capacity_authorization_unavailable"
    assert store.dispatcher_circuit().is_open is True


def test_dynamic_capacity_block_opens_circuit_before_claim(tmp_path):
    store = _store(tmp_path)
    config = replace(
        _config(tmp_path),
        capacity_mode="bootstrap",
        release_id="rca-prod-20260713-001",
        bootstrap_epoch_id="rca-bootstrap-release-20260713",
    )
    capacity_runtime = FakeCapacityRuntime(
        [_runtime_decision(state="STEADY_BLOCKED", mode="blocked", ready=False)]
    )
    reporter = HealthReporter(
        config,
        store,
        capacity_runtime=capacity_runtime,
    )
    dispatcher = OutboxDispatcher(
        store=store,
        config=config,
        delivery_store=RcaDeliveryStore(config.delivery_db_path),
        enrich=lambda _event: pytest.fail("capacity guard must run before enrich"),
        storage_admission=lambda _request: pytest.fail(
            "capacity guard must run before storage admission"
        ),
        derived_capacity_reservation=lambda _request: pytest.fail(
            "capacity guard must run before reservation"
        ),
        submit=lambda _admission, _request: pytest.fail(
            "capacity guard must run before submit"
        ),
    )
    dispatcher.workspace_runtime_guard = reporter.dispatch_guard_outcome

    outcome = dispatcher.dispatch_one()

    assert outcome.status == "capacity_authorization_unavailable"
    assert outcome.error_code == "rca_capacity_steady_evidence_missing"
    assert store.dispatcher_circuit().is_open is True
    [row] = store.list_rows("rca_outbox")
    assert row["status"] == "pending"
    assert row["attempt"] == 0


def test_resident_loop_never_claims_when_required_workspace_runtime_is_missing(
    tmp_path,
):
    store = _store(tmp_path)
    config = _config(tmp_path)
    stopping = False

    class MustNotDispatch:
        def __init__(self):
            self.config = config
            self.stats = dispatcher_module.DispatchStats()
            self.calls = 0

        def dispatch_batch(self):
            self.calls += 1
            pytest.fail("missing fixed workspace runtime must block before claim")

    def unavailable():
        raise WorkspaceRuntimeError("rca_workspace_runtime_directory_unavailable")

    dispatcher = MustNotDispatch()
    reporter = HealthReporter(
        config,
        store,
        workspace_runtime_observer=unavailable,
    )

    def sleep(_seconds):
        nonlocal stopping
        stopping = True

    run_dispatch_loop(
        dispatcher,
        reporter,
        stop_requested=lambda: stopping,
        sleep=sleep,
    )

    payload = json.loads(config.health_path.read_text(encoding="utf-8"))
    assert dispatcher.calls == 0
    assert payload["state"] == "workspace_runtime_unavailable"
    assert payload["healthy"] is False


def test_dispatch_one_rechecks_workspace_runtime_before_each_claim(tmp_path):
    store = _store(tmp_path)
    config = _config(tmp_path)
    dispatcher = OutboxDispatcher(
        store=store,
        config=config,
        delivery_store=RcaDeliveryStore(config.delivery_db_path),
        enrich=lambda event: pytest.fail("workspace guard must run before enrich"),
        storage_admission=lambda request: pytest.fail(
            "workspace guard must run before storage admission"
        ),
        derived_capacity_reservation=lambda request: pytest.fail(
            "workspace guard must run before capacity reservation"
        ),
        submit=lambda admission, request: pytest.fail(
            "workspace guard must run before submit"
        ),
    )

    def unavailable():
        raise WorkspaceRuntimeError("rca_workspace_runtime_directory_unavailable")

    reporter = HealthReporter(
        config,
        store,
        workspace_runtime_observer=unavailable,
    )
    dispatcher.workspace_runtime_guard = reporter.dispatch_guard_outcome

    outcome = dispatcher.dispatch_one()

    assert outcome.status == "workspace_runtime_unavailable"
    [row] = store.list_rows("rca_outbox")
    assert row["status"] == "pending"
    assert row["attempt"] == 0


@pytest.mark.parametrize(
    ("future_seconds", "expected_ok", "expected_reason"),
    [
        (30, True, None),
        (31, False, "heartbeat_from_future"),
    ],
)
def test_health_status_bounds_future_heartbeat_clock_skew(
    tmp_path, future_seconds, expected_ok, expected_reason
):
    store = _store(tmp_path)
    config = _config(tmp_path, enabled=False)
    reporter = HealthReporter(config, store)
    reporter.write(
        state="disabled",
        stats=dispatcher_module.DispatchStats(),
        last_outcome=dispatcher_module.DispatchOutcome(status="disabled"),
    )
    payload = json.loads(config.health_path.read_text(encoding="utf-8"))
    now = datetime(2026, 7, 10, 0, 0, tzinfo=timezone.utc)
    payload["heartbeat_at"] = (now + timedelta(seconds=future_seconds)).isoformat()
    config.health_path.write_text(json.dumps(payload), encoding="utf-8")

    status = dispatcher_module.read_health_status(config, max_age_seconds=60, now=now)

    assert status["ok"] is expected_ok
    assert status["health_check"]["fresh"] is expected_ok
    assert status["health_check"]["heartbeat_age_seconds"] == -future_seconds
    assert status["health_check"].get("reason") == expected_reason


def test_health_status_rejects_timezone_naive_heartbeat(tmp_path):
    store = _store(tmp_path)
    config = _config(tmp_path, enabled=False)
    reporter = HealthReporter(config, store)
    reporter.write(
        state="disabled",
        stats=dispatcher_module.DispatchStats(),
        last_outcome=dispatcher_module.DispatchOutcome(status="disabled"),
    )
    payload = json.loads(config.health_path.read_text(encoding="utf-8"))
    payload["heartbeat_at"] = "2026-07-10T00:00:00"
    config.health_path.write_text(json.dumps(payload), encoding="utf-8")

    status = dispatcher_module.read_health_status(
        config,
        max_age_seconds=60,
        now=datetime(2026, 7, 10, 0, 0, tzinfo=timezone.utc),
    )

    assert status["ok"] is False
    assert status["error"] == "health_timestamp_invalid"


def test_health_status_rejects_legacy_v1_schema(tmp_path):
    store = _store(tmp_path)
    config = _config(tmp_path)
    reporter = HealthReporter(config, store)
    reporter.write(state="idle", stats=dispatcher_module.DispatchStats())
    payload = json.loads(config.health_path.read_text(encoding="utf-8"))
    payload["schema_version"] = "pnc_rca_outbox_dispatcher_health_v1"
    config.health_path.write_text(json.dumps(payload), encoding="utf-8")

    status = dispatcher_module.read_health_status(config, max_age_seconds=60)

    assert status["ok"] is False
    assert status["health_check"]["reason"] == "dispatcher_reported_unhealthy"


def test_resident_loop_reports_and_polls_delivery_backpressure(tmp_path):
    store = _store(tmp_path)
    config = replace(
        _config(tmp_path),
        delivery_high_watermark=2,
        delivery_resume_watermark=1,
    )
    dispatcher = OutboxDispatcher(
        store=store,
        config=config,
        delivery_store=DeliverySnapshotSource(_delivery_snapshot(config, pending=2)),
        enrich=lambda event: pytest.fail("must not enrich"),
        storage_admission=lambda request: pytest.fail("must not check storage"),
        derived_capacity_reservation=lambda request: pytest.fail(
            "must not reserve storage"
        ),
        submit=lambda admission, request: pytest.fail("must not submit"),
    )
    health = HealthReporter(
        config,
        store,
        delivery_backpressure_status=dispatcher.delivery_backpressure_health,
    )
    sleeps = []
    stopping = False

    def sleep(seconds):
        nonlocal stopping
        sleeps.append(seconds)
        stopping = True

    run_dispatch_loop(
        dispatcher,
        health,
        stop_requested=lambda: stopping,
        sleep=sleep,
    )

    assert sleeps == [config.circuit_poll_interval_seconds]
    payload = json.loads(config.health_path.read_text(encoding="utf-8"))
    assert payload["ok"] is False
    assert payload["state"] == "downstream_backpressure"
    assert payload["delivery_backpressure"]["active"] is True
    assert payload["delivery_backpressure"]["last_snapshot"]["unresolved_effects"] == 2


def test_dry_run_cli_does_not_claim_enrich_or_submit(tmp_path, monkeypatch, capsys):
    store = _store(tmp_path)
    monkeypatch.setenv("HERMES_RCA_OUTBOX_DISPATCH_ENABLED", "true")
    monkeypatch.setenv("HERMES_RCA_OUTBOX_STORAGE_ADMISSION_ENABLED", "true")
    monkeypatch.setenv("HERMES_RCA_OUTBOX_STORAGE_RESERVATION_ENABLED", "false")
    monkeypatch.setenv("HERMES_RCA_OUTBOX_DERIVED_CAPACITY_RESERVATION_ENABLED", "true")
    monkeypatch.setenv("HERMES_RCA_OUTBOX_DELIVERY_BACKPRESSURE_ENABLED", "true")
    monkeypatch.setenv(
        "HERMES_RCA_OUTBOX_CONTROL_DB_PATH", str(tmp_path / "control.sqlite3")
    )
    monkeypatch.setenv("HERMES_RCA_OUTBOX_HEALTH_PATH", str(tmp_path / "health.json"))
    monkeypatch.setattr(
        dispatcher_module,
        "default_enrich_event",
        lambda event: pytest.fail("dry-run must not enrich"),
    )
    monkeypatch.setattr(
        dispatcher_module,
        "default_storage_admission",
        lambda request: pytest.fail("dry-run must not check storage"),
    )
    monkeypatch.setattr(
        dispatcher_module,
        "default_submit",
        lambda admission, request: pytest.fail("dry-run must not submit"),
    )

    assert dispatcher_module.main(["--dry-run"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dry_run"] is True
    assert payload["activation_required"] is False
    assert payload["due_count_in_sample"] == 1
    assert store.list_rows("rca_outbox")[0]["status"] == "pending"


def test_dry_run_activation_required_holds_unconfigured_pending_row(
    tmp_path, monkeypatch, capsys
):
    store = _store(tmp_path)
    monkeypatch.setenv("HERMES_RCA_OUTBOX_DISPATCH_ENABLED", "true")
    monkeypatch.setenv("HERMES_RCA_OUTBOX_ACTIVATION_REQUIRED", "true")
    monkeypatch.setenv("HERMES_RCA_OUTBOX_STORAGE_ADMISSION_ENABLED", "true")
    monkeypatch.setenv("HERMES_RCA_OUTBOX_STORAGE_RESERVATION_ENABLED", "false")
    monkeypatch.setenv(
        "HERMES_RCA_OUTBOX_DERIVED_CAPACITY_RESERVATION_ENABLED", "true"
    )
    monkeypatch.setenv(
        "HERMES_RCA_OUTBOX_DELIVERY_BACKPRESSURE_ENABLED", "true"
    )
    monkeypatch.setenv(
        "HERMES_RCA_OUTBOX_CONTROL_DB_PATH",
        str(tmp_path / "control.sqlite3"),
    )
    monkeypatch.setenv(
        "HERMES_RCA_OUTBOX_HEALTH_PATH",
        str(tmp_path / "health.json"),
    )

    assert dispatcher_module.main(["--env-file", "/dev/null", "--dry-run"]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["dry_run"] is True
    assert payload["activation_required"] is True
    assert payload["due_count_in_sample"] == 0
    assert payload["rows"] == []
    assert store.list_rows("rca_outbox")[0]["status"] == "pending"


def test_default_enrich_event_preserves_read_source_provenance(monkeypatch):
    from gateway import pnc_issue_context

    monkeypatch.setattr(
        pnc_issue_context,
        "fetch_g1q3_issue_context_result",
        lambda **_kwargs: pnc_issue_context.G1Q3IssueReadResult(
            context_text=(
                "- title: G1Q3 RCA case\n"
                "- 数据地址: mdi download event -u event-123 -s ./"
            ),
            status="fields_extracted",
            source="mcp_auto_degraded",
            errors=[
                {"tool": "meegle", "error_class": "Unauthenticated"},
                {"tool": "meegle", "error_class": "Unauthenticated"},
            ],
        ),
    )

    context = dispatcher_module.default_enrich_event({
        "project_key": "68ef617fb371dc80a10641f7",
        "project_simple_name": "t03o4q",
        "work_item_type_key": "issue",
        "work_item_id": "7041712812",
        "issue_url": "",
    })

    assert context.source_quality == "partial"
    assert context.url == "https://project.feishu.cn/t03o4q/issue/detail/7041712812"
    assert context.media_refs == [{
        "type": "host_issue_read_status",
        "status": "fields_extracted",
        "source": "mcp_auto_degraded",
        "degraded": True,
        "error_classes": ["Unauthenticated"],
    }]
    assert "token" not in repr(context.media_refs).lower()


def test_cli_promotes_one_exact_shadow_event_with_audit(tmp_path, monkeypatch, capsys):
    store = _store(tmp_path, shadow=True)
    event_uid = store.list_rows("kafka_inbox")[0]["event_uid"]
    monkeypatch.setenv("HERMES_RCA_OUTBOX_DISPATCH_ENABLED", "false")
    monkeypatch.setenv(
        "HERMES_RCA_OUTBOX_CONTROL_DB_PATH", str(tmp_path / "control.sqlite3")
    )
    monkeypatch.setenv("HERMES_RCA_OUTBOX_HEALTH_PATH", str(tmp_path / "health.json"))

    assert (
        dispatcher_module.main([
            "--promote-shadow-event",
            event_uid,
            "--operator",
            "release-owner",
            "--reason",
            "approved canary",
        ])
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["promotion"]["event_uid"] == event_uid
    assert payload["promotion"]["promoted"] is True
    assert store.list_rows("rca_outbox")[0]["status"] == "pending"


def _configure_activation_cli(monkeypatch, tmp_path):
    for name in tuple(os.environ):
        if name.startswith("HERMES_RCA_OUTBOX_"):
            monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("HERMES_RCA_OUTBOX_DISPATCH_ENABLED", "false")
    monkeypatch.setenv("HERMES_RCA_OUTBOX_ACTIVATION_REQUIRED", "true")
    monkeypatch.setenv(
        "HERMES_RCA_OUTBOX_CONTROL_DB_PATH",
        str(tmp_path / "control.sqlite3"),
    )
    monkeypatch.setenv(
        "HERMES_RCA_OUTBOX_HEALTH_PATH",
        str(tmp_path / "health.json"),
    )


def test_cli_activation_promotes_exact_authorized_shadow(tmp_path, monkeypatch, capsys):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    epoch_id, _identities = _prepare_activation_epoch(store)
    store.transition_activation_epoch(
        epoch_id=epoch_id,
        expected_state="preauthorized",
        target_state="bounded_active",
        operator="dispatcher-test",
        reason="open exact promotion",
    )
    accepted = store.ingest_record(
        _record(offset=10),
        policy=_policy(),
        submit_enabled=True,
        activation_required=True,
    )
    _configure_activation_cli(monkeypatch, tmp_path)

    result = dispatcher_module.main(
        [
            "--env-file",
            "/dev/null",
            "--promote-shadow-event",
            accepted.event_uid,
            "--activation-epoch-id",
            epoch_id,
            "--operator",
            "release-owner",
            "--reason",
            "approved exact activation canary",
        ]
    )

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["promotion"]["promoted"] is True
    [outbox] = store.list_rows("rca_outbox")
    assert outbox["status"] == "pending"
    slot = next(
        row
        for row in store.list_rows("rca_activation_budget_slots")
        if row["slot_kind"] == "kafka_success"
    )
    assert slot["consumed_ledger_id"] == outbox["activation_ledger_id"]


def test_cli_activation_rejects_unauthorized_shadow(tmp_path, monkeypatch, capsys):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    epoch_id, _identities = _prepare_activation_epoch(store, kafka_offset=10)
    store.transition_activation_epoch(
        epoch_id=epoch_id,
        expected_state="preauthorized",
        target_state="bounded_active",
        operator="dispatcher-test",
        reason="open bounded promotion guard",
    )
    accepted = store.ingest_record(
        _record(offset=11),
        policy=_policy(),
        submit_enabled=True,
        activation_required=True,
        activation_slot_kind="kafka_success",
    )
    _configure_activation_cli(monkeypatch, tmp_path)

    result = dispatcher_module.main(
        [
            "--env-file",
            "/dev/null",
            "--promote-shadow-event",
            accepted.event_uid,
            "--activation-epoch-id",
            epoch_id,
            "--operator",
            "release-owner",
            "--reason",
            "must reject unauthorized source",
        ]
    )

    assert result == 2
    error = json.loads(capsys.readouterr().err)
    assert error["error"] == "ShadowPromotionError"
    [outbox] = store.list_rows("rca_outbox")
    assert outbox["status"] == "shadow"
    [audit] = store.list_rows("rca_shadow_promotion_audit")
    assert audit["outcome"] == "denied"
    assert audit["detail"] == "activation_bounded_identity_not_authorized"


def test_cli_activation_confirmed_reconciles_without_consuming_canary_slot(
    tmp_path, monkeypatch, capsys
):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    epoch_id, identities = _prepare_activation_epoch(store, kafka_offset=10)
    store.transition_activation_epoch(
        epoch_id=epoch_id,
        expected_state="preauthorized",
        target_state="bounded_active",
        operator="dispatcher-test",
        reason="open bounded canaries while catchup remains shadow",
    )
    catchup = store.ingest_record(
        _record(offset=11, issue_id=7041712816),
        policy=_policy(),
        submit_enabled=True,
        activation_required=True,
    )
    _materialize_activation_slots(store, identities)
    RcaDeliveryStore(tmp_path / "control.sqlite3")
    dispatcher = _activation_dispatcher(
        store,
        _config(tmp_path, activation_required=True),
    )
    assert all(
        dispatcher.dispatch_one().status == "completed" for _ in range(3)
    )
    end_fence = {TOPIC: {"2": 12}}
    epoch = store.activation_epoch()
    assert epoch is not None
    store.transition_activation_epoch(
        epoch_id=epoch_id,
        expected_state="bounded_active",
        target_state="confirmed",
        partition_end_fence=end_fence,
        production_fingerprint="3" * 64,
        production_gate_receipt_sha256="4" * 64,
        expected_config_sha256=epoch["config_sha256"],
        expected_db_logical_identity_sha256=epoch[
            "db_logical_identity_sha256"
        ],
        expected_partition_start_fence_sha256=epoch[
            "partition_start_fence_sha256"
        ],
        expected_release_binding_sha256=(
            store.activation_release_binding_sha256(
                epoch_id=epoch_id,
                partition_end_fence=end_fence,
            )
        ),
        operator="dispatcher-test",
        reason="freeze catchup reconciliation interval",
    )
    _configure_activation_cli(monkeypatch, tmp_path)

    result = dispatcher_module.main(
        [
            "--env-file",
            "/dev/null",
            "--promote-shadow-event",
            catchup.event_uid,
            "--activation-epoch-id",
            epoch_id,
            "--operator",
            "release-owner",
            "--reason",
            "reconcile exact confirmed catchup item",
        ]
    )

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["promotion"]["promoted"] is True
    catchup_outbox = next(
        row
        for row in store.list_rows("rca_outbox")
        if row["submission_key"] == catchup.submission_key
    )
    assert catchup_outbox["status"] == "pending"
    assert store.activation_epoch()["state"] == "confirmed"


def test_check_config_defaults_dispatch_disabled(tmp_path, monkeypatch, capsys):
    for name in list(os.environ):
        if name.startswith("HERMES_RCA_OUTBOX_"):
            monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv(
        "HERMES_RCA_OUTBOX_CONTROL_DB_PATH", str(tmp_path / "control.sqlite3")
    )
    monkeypatch.setenv("HERMES_RCA_OUTBOX_HEALTH_PATH", str(tmp_path / "health.json"))

    assert dispatcher_module.main(["--check-config"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["config"]["dispatch_enabled"] is False
    assert payload["config"]["activation_required"] is False
    assert not (tmp_path / "control.sqlite3").exists()
