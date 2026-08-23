from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3

import pytest

from gateway import pnc_rca_control_store as control_store_module
from gateway.pnc_rca_admission import build_rca_admission
from gateway.pnc_rca_control_store import (
    ACTIVATION_TRANSITION_BINDING_SCHEMA_V14,
    ACTIVATION_TRANSITION_BINDING_SCHEMA_V15,
    ActivationEpochError,
    CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION,
    CONTROL_STORE_SCHEMA_VERSION,
    ControlStoreCapacityError,
    INPUT_WAIT_QUARANTINE_REARMED_REASON,
    INPUT_WAIT_EXECUTION_WATCH_PRESENT_REASON,
    INPUT_WAIT_TERMINAL_NEW_GENERATION_REASON,
    KafkaRecord,
    LEGACY_KAFKA_GENERATION_REASON,
    MANUAL_TRIGGER_SCHEMA_VERSION,
    ManualRcaAdmissionError,
    ManualRcaTriggerRequest,
    RcaControlStore,
    RecordConflictError,
    RecordProcessingBlockedError,
    StaleOutboxLeaseError,
    build_historical_epoch_rerun_authority,
    build_silent_terminal_rerun_authority,
    build_batch_terminal_rerun_authority,
)
from gateway.pnc_rca_kafka_contract import WorkflowEventPolicy, WorkflowTransition
from gateway.pnc_rca_delivery_contract import DeliveryContractError
from gateway.pnc_rca_delivery_store import RcaDeliveryStore
from gateway.pnc_rca_runtime_identity import canonical_json_sha256


TOPIC = "feishu-project-workflow-event"
PREAUTHORIZATION_FINGERPRINT = "1" * 64
PREAUTHORIZATION_RECEIPT_SHA256 = "a" * 64
PREAUTHORIZATION_CAPSULE_SHA256 = "b" * 64
PREPRODUCTION_FINGERPRINT = "c" * 64
PREPRODUCTION_RECEIPT_SHA256 = "d" * 64
PREPRODUCTION_CAPSULE_SHA256 = "e" * 64


def _steady_control_store(path) -> RcaControlStore:
    store = RcaControlStore(path)
    if store.activation_epoch() is None:
        store.activate_direct_steady_epoch(
            epoch_id="rca-test-steady",
            release_fingerprint_sha256="1" * 64,
            release_note_sha256="a" * 64,
            config_sha256="2" * 64,
            db_logical_identity={"database": "control-test"},
            partition_start_fence={TOPIC: {"2": 0}},
            operator="control-test",
            reason="activate steady control-store test runtime",
        )
    return store


def _resident_identity(
    service_label: str = "local.pnc.rca-kafka-consumer",
    *,
    pid: int = 42000,
    script: str = "/candidate/scripts/pnc_rca_kafka_consumer.py",
):
    return {
        "service_label": service_label,
        "pid": pid,
        "process_create_time": 1_783_650_000.0,
        "boot_time": 1_783_000_000.0,
        "executable": "/candidate/.venv/bin/python",
        "script": script,
        "cwd": "/candidate",
        "script_sha256": "a" * 64,
        "runtime_files_sha256": "b" * 64,
        "public_config_sha256": "c" * 64,
        "loaded_runtime_sha256": "d" * 64,
    }


def _policy(*, policy_version="issue-created-v1"):
    return WorkflowEventPolicy(
        topic=TOPIC,
        policy_version=policy_version,
        project_keys=frozenset({"project-key"}),
        project_simple_names=frozenset({"g1q3"}),
        work_item_type_keys=frozenset({"problem-type"}),
        status_change_types=frozenset({"Reached"}),
        transitions=(
            WorkflowTransition(
                state_key="new-problem-state",
                pre_status=1,
                cur_status=2,
            ),
        ),
    )


def _value(*, updated_at=1783650000000, work_item_id=7041712812):
    return json.dumps(
        {
            "id": work_item_id,
            "name": "ACC braking issue",
            "nodes": [
                {
                    "state_key": "new-problem-state",
                    "node_name": "New problem",
                    "pre_status": 1,
                    "cur_status": 2,
                }
            ],
            "project_key": "project-key",
            "project_simple_name": "g1q3",
            "status_change_type": "Reached",
            "updated_at": updated_at,
            "work_item_type_key": "problem-type",
        },
        sort_keys=True,
    ).encode()


def _record(offset=10, *, value=None, partition=2):
    return KafkaRecord(
        topic=TOPIC,
        partition=partition,
        offset=offset,
        value=value if value is not None else _value(),
        key=b"issue-key",
        timestamp_ms=1783650000000,
        headers=(("trace", b"trace-1"),),
    )


def _profile_snapshot_policy():
    return WorkflowEventPolicy(
        topic=TOPIC,
        policy_version="issue-profile-snapshot-v1",
        project_keys=frozenset({"t03o4q"}),
        project_simple_names=frozenset({"t03o4q"}),
        work_item_type_keys=frozenset({"issue"}),
        snapshot_patterns=frozenset({"State"}),
        snapshot_sub_stages=frozenset({"OPEN"}),
    )


def _profile_snapshot_record(offset: int, option_id: str) -> KafkaRecord:
    value = {
        "created_at": 1783650001000,
        "fields": [
            {"field_key": "field_052f23", "field_value": [option_id]}
        ],
        "id": 7041712812,
        "name": "ACC braking issue",
        "pattern": "State",
        "project_key": "t03o4q",
        "project_simple_name": "t03o4q",
        "sub_stage": "OPEN",
        "updated_at": 1783650000000 + offset,
        "work_item_status": {"state_key": "open"},
        "work_item_type_key": "issue",
    }
    return _record(offset=offset, value=json.dumps(value, sort_keys=True).encode())


def _manual_request(
    message_id: str,
    *,
    mode: str = "run_or_join",
    thread_id: str = "topic:om_root",
    issue_url: str = "https://project.feishu.cn/g1q3/issue/detail/7041712812",
    requester_id: str = "ou_operator",
):
    return ManualRcaTriggerRequest(
        schema_version=MANUAL_TRIGGER_SCHEMA_VERSION,
        issue_url=issue_url,
        mode=mode,
        reason="manual_explicit_issue_action",
        platform="feishu",
        chat_id="oc_allowed",
        thread_id=thread_id,
        message_id=message_id,
        requester_id=requester_id,
    )


def _group_user_rerun_authority(work_item_id: str = "7041712812"):
    return {
        "schema_version": "pnc_rca_group_user_rerun_v1",
        "command_text": f"重新分析 {work_item_id}",
        "work_item_id": work_item_id,
    }


def _operator_request(
    message_id: str,
    *,
    issue_url: str = "https://project.feishu.cn/g1q3/issue/detail/7041712812",
    mode: str = "rerun",
    requester_id: str = "automation:test-operator",
):
    return ManualRcaTriggerRequest(
        schema_version=MANUAL_TRIGGER_SCHEMA_VERSION,
        issue_url=issue_url,
        mode=mode,
        reason="production_batch_rerun",
        platform="operator",
        chat_id="",
        thread_id="",
        message_id=message_id,
        requester_id=requester_id,
    )


def _silent_deadline_terminal_store(tmp_path):
    from tests.scripts.test_pnc_rca_delivery_collector import (
        NOW as delivery_now,
        _real_terminal_collector,
    )
    clock = [delivery_now]
    collector = _real_terminal_collector(
        tmp_path,
        clock=clock,
        blocker={"kind": "service_provenance_unavailable", "retryable": False},
    )
    collector.config = replace(collector.config, activation_required=True)
    assert collector.collect_one().status == "failure_hold"
    clock[0] = delivery_now + timedelta(seconds=1800)
    terminal = collector.collect_one()
    assert terminal.status == "terminal_failed"
    assert terminal.error_code == "service_provenance_unavailable"
    return (
        RcaControlStore(
            collector.store.db_path,
            require_current=True,
            allow_successor_write=True,
        ),
        clock[0],
    )


def _rewrite_silent_terminal_error_code(
    store: RcaControlStore, error_code: str
) -> None:
    delivery = RcaDeliveryStore(
        store.db_path,
        require_current=True,
        allow_successor_write=True,
    )
    [watch] = delivery.list_rows("rca_execution_watch")
    [route] = delivery.list_rows("rca_failure_routes")
    status = json.loads(watch["last_status_json"])
    taxonomy = status["failure_taxonomy"]
    is_taxonomy_gap = error_code.startswith("taxonomy_gap:")
    raw_code = error_code.removeprefix("taxonomy_gap:")
    taxonomy["raw_code"] = raw_code
    taxonomy["terminal_error_code"] = error_code
    taxonomy["known"] = not is_taxonomy_gap
    taxonomy["contract_errors"] = ["unknown_blocker_kind"] if is_taxonomy_gap else []
    route_payload = json.loads(route["route_payload_json"])
    route_payload["blocker"]["kind"] = raw_code
    route_payload["decision"]["raw_code"] = raw_code
    route_payload["decision"]["terminal_error_code"] = error_code
    route_payload["decision"]["known"] = not is_taxonomy_gap
    route_payload["decision"]["contract_errors"] = (
        ["unknown_blocker_kind"] if is_taxonomy_gap else []
    )
    conn = store._connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "UPDATE rca_execution_watch SET last_error_code=?, "
            "last_status_json=? WHERE submission_key=?",
            (
                error_code,
                control_store_module._canonical_json(status),
                watch["submission_key"],
            ),
        )
        conn.execute(
            "UPDATE rca_failure_routes SET terminal_error_code=?, "
            "route_payload_json=? WHERE route_key=?",
            (
                error_code,
                control_store_module._canonical_json(route_payload),
                route["route_key"],
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _convert_silent_terminal_to_issue_only_operator(
    store: RcaControlStore,
) -> None:
    [trigger] = store.list_rows("business_triggers")
    conn = store._connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "UPDATE rca_trigger_sources SET platform='operator', chat_id='', "
            "thread_id='', requester_id='automation:rca-batch-rerun' "
            "WHERE source_id=?",
            (trigger["origin_source_id"],),
        )
        conn.execute(
            "DELETE FROM rca_delivery_subscription_events "
            "WHERE subscription_key IN ("
            "SELECT subscription_key FROM rca_delivery_subscriptions "
            "WHERE business_key=? AND generation=? "
            "AND effect_kind='feishu_thread_reply')",
            (trigger["business_key"], trigger["generation"]),
        )
        conn.execute(
            "DELETE FROM rca_trigger_delivery_bindings "
            "WHERE subscription_key IN ("
            "SELECT subscription_key FROM rca_delivery_subscriptions "
            "WHERE business_key=? AND generation=? "
            "AND effect_kind='feishu_thread_reply')",
            (trigger["business_key"], trigger["generation"]),
        )
        conn.execute(
            "DELETE FROM rca_delivery_subscriptions "
            "WHERE business_key=? AND generation=? "
            "AND effect_kind='feishu_thread_reply'",
            (trigger["business_key"], trigger["generation"]),
        )
        conn.commit()
    finally:
        conn.close()


def _convert_silent_terminal_to_immediate_viz_gap(
    store: RcaControlStore,
) -> None:
    """Build the exact immediate service-result gap contract for the predicate."""
    delivery = RcaDeliveryStore(
        store.db_path,
        require_current=True,
        allow_successor_write=True,
    )
    [watch] = delivery.list_rows("rca_execution_watch")
    [route] = delivery.list_rows("rca_failure_routes")
    status = json.loads(watch["last_status_json"])
    taxonomy = status["failure_taxonomy"]
    error_code = "taxonomy_gap:viz_evidence_unavailable"
    receipt = {
        "schema_version": "g1q3_rca_service_result_v2",
        "pipeline_stage": "s6_report",
        "pipeline_status": "blocked",
        "status": "pipeline_not_successful",
        "task_id": watch["task_id"],
    }
    taxonomy.update(
        {
            "known": False,
            "retryable": False,
            "raw_code": "viz_evidence_unavailable",
            "terminal_error_code": error_code,
            "source": "rca_service_result",
            "observed_state": "failed",
            "source_conflict": False,
            "external_comment_policy": "honest_non_attribution_only",
            "receipt": receipt,
        }
    )
    route_payload = json.loads(route["route_payload_json"])
    route_payload["decision"].update(
        {
            "raw_code": "viz_evidence_unavailable",
            "terminal_error_code": error_code,
            "known": False,
            "retryable": False,
            "internal_route": "internal_alert",
            "lane": "hard_defect",
        }
    )
    route_payload["blocker"].update(
        {
            "kind": "viz_evidence_unavailable",
            "blocks_attribution": True,
        }
    )
    route_audit = json.loads(route["audit_json"])
    route_audit.update({"source": "rca_service_result", "receipt": receipt})
    conn = store._connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "UPDATE rca_execution_watch SET last_error_code=?, last_status_json=? "
            "WHERE submission_key=?",
            (
                error_code,
                control_store_module._canonical_json(status),
                watch["submission_key"],
            ),
        )
        conn.execute(
            "UPDATE rca_failure_routes SET terminal_error_code=?, audit_json=?, "
            "route_payload_json=? WHERE route_key=?",
            (
                error_code,
                control_store_module._canonical_json(route_audit),
                control_store_module._canonical_json(route_payload),
                route["route_key"],
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _convert_silent_terminal_to_legacy_gate_a_gap(
    store: RcaControlStore,
) -> None:
    """Build the exact pre-r15bx Gate-A terminal route shape."""
    delivery = RcaDeliveryStore(
        store.db_path,
        require_current=True,
        allow_successor_write=True,
    )
    [watch] = delivery.list_rows("rca_execution_watch")
    [route] = delivery.list_rows("rca_failure_routes")
    status = json.loads(watch["last_status_json"])
    taxonomy = status["failure_taxonomy"]
    error_code = "taxonomy_gap:gate_a_projection_invalid"
    taxonomy.update(
        {
            "known": False,
            "retryable": False,
            "raw_code": "gate_a_projection_invalid",
            "terminal_error_code": error_code,
            "source": "delivery_contract_verifier",
            "observed_state": "completed",
            "source_conflict": False,
            "external_comment_policy": "honest_non_attribution_only",
            "contract_errors": ["unknown_blocker_kind"],
        }
    )
    # The legacy producer omitted the receipt field entirely; an explicit
    # empty receipt is accepted only for compatibility with older snapshots.
    taxonomy.pop("receipt", None)
    route_payload = json.loads(route["route_payload_json"])
    route_payload["decision"].update(
        {
            "raw_code": "gate_a_projection_invalid",
            "terminal_error_code": error_code,
            "known": False,
            "retryable": False,
            "internal_route": "internal_alert",
            "lane": "hard_defect",
            "contract_errors": ["unknown_blocker_kind"],
        }
    )
    route_payload["blocker"].update(
        {
            "kind": "gate_a_projection_invalid",
            "message": "gate_a_projection_invalid: unmaterialized_failure_class_invalid",
        }
    )
    route_audit = json.loads(route["audit_json"])
    route_audit.update(
        {
            "source": "delivery_contract_verifier",
            "receipt": {},
            "contract_errors": ["unknown_blocker_kind"],
            "taxonomy_audit": {},
        }
    )
    conn = store._connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "UPDATE rca_execution_watch SET last_error_code=?, last_status_json=? "
            "WHERE submission_key=?",
            (
                error_code,
                control_store_module._canonical_json(status),
                watch["submission_key"],
            ),
        )
        conn.execute(
            "UPDATE rca_failure_routes SET terminal_error_code=?, audit_json=?, "
            "route_payload_json=? WHERE route_key=?",
            (
                error_code,
                control_store_module._canonical_json(route_audit),
                control_store_module._canonical_json(route_payload),
                route["route_key"],
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _silent_batch_request(batch_id: str = "batch-684") -> ManualRcaTriggerRequest:
    request = _operator_request(
        f"{batch_id}-7041712812-try-1",
        requester_id="automation:rca-batch-rerun",
    )
    return ManualRcaTriggerRequest(
        **{**request.to_dict(), "reason": f"production_gray_batch:{batch_id}"}
    )


def _silent_batch_authority(
    store: RcaControlStore,
    request: ManualRcaTriggerRequest,
    *,
    batch_id: str = "batch-684",
) -> dict:
    [prior] = store.list_rows("business_triggers")
    return build_silent_terminal_rerun_authority(
        batch_id=batch_id,
        queue_sha256="1" * 64,
        issue_id=str(prior["work_item_id"]),
        prior_submission_key=str(prior["submission_key"]),
        prior_generation=int(prior["generation"]),
        owner_receipt_path=str(store.db_path.parent / "owner-receipt.json"),
        owner_receipt_sha256="2" * 64,
        requester_id=request.requester_id,
        reason=request.reason,
    )


@pytest.mark.parametrize(
    "error_code",
    [
        "delivery_lineage_unavailable",
        "failure_receipt_missing",
        "rca_work_deadline_exceeded",
        "report_public_origin_invalid",
        "service_provenance_unavailable",
        "taxonomy_gap:derived_capacity_hfs_target_identity_mismatch",
        "taxonomy_gap:derived_capacity_reservation_activate_failed",
        "taxonomy_gap:gate_a_projection_invalid",
        "taxonomy_gap:viz_evidence_unavailable",
        "remote_evidence_domain_unsupported",
        "taxonomy_gap:remote_evidence_domain_unsupported",
    ],
)
def test_operator_silent_terminal_rerun_creates_new_generation_without_old_mutation(
    tmp_path, error_code,
):
    store, terminal_at = _silent_deadline_terminal_store(tmp_path)
    if error_code != "service_provenance_unavailable":
        _rewrite_silent_terminal_error_code(store, error_code)
    request = _silent_batch_request()
    authority = _silent_batch_authority(store, request)
    delivery = RcaDeliveryStore(
        store.db_path,
        require_current=True,
        allow_successor_write=True,
    )
    old_watch = dict(delivery.list_rows("rca_execution_watch")[0])
    old_route = dict(delivery.list_rows("rca_failure_routes")[0])

    rerun = store.admit_manual_trigger(
        request,
        allowed_chat_ids=set(),
        submit_enabled=True,
        operator_authorized=True,
        silent_terminal_rerun_authority=authority,
        outbox_high_watermark=10_000,
        activation_required=True,
        now=terminal_at + timedelta(seconds=1),
    )

    assert rerun.outcome == "created"
    assert rerun.generation == 2
    assert delivery.list_rows("rca_execution_watch") == [old_watch]
    assert delivery.list_rows("rca_failure_routes") == [old_route]
    generations = sorted(
        store.list_rows("business_triggers"), key=lambda item: item["generation"]
    )
    assert [(item["generation"], item["submission_key"]) for item in generations] == [
        (1, old_watch["submission_key"]),
        (2, rerun.submission_key),
    ]
    [new_outbox] = [
        item for item in store.list_rows("rca_outbox") if item["generation"] == 2
    ]
    assert new_outbox["status"] == "pending"
    subscriptions = [
        item
        for item in store.list_rows("rca_delivery_subscriptions")
        if item["generation"] == 2
    ]
    assert [item["effect_kind"] for item in subscriptions] == [
        "feishu_issue_comment"
    ]
    [audit] = [
        item
        for item in store.list_rows("rca_shadow_promotion_audit")
        if item["outcome"] == "silent_terminal_new_generation_created"
    ]
    assert json.loads(audit["detail"]) == authority
    assert audit["from_status"] == "terminal_failed:g1"
    assert audit["to_status"] == "pending:g2"


def test_owner_authorized_silent_terminal_rerun_can_publish_success_conclusion(
    tmp_path,
):
    store, terminal_at = _silent_deadline_terminal_store(tmp_path)
    request = _silent_batch_request()
    authority = _silent_batch_authority(store, request)
    rerun = store.admit_manual_trigger(
        request,
        allowed_chat_ids=set(),
        submit_enabled=True,
        operator_authorized=True,
        silent_terminal_rerun_authority=authority,
        outbox_high_watermark=10_000,
        activation_required=True,
        now=terminal_at + timedelta(seconds=1),
    )
    delivery = RcaDeliveryStore(
        store.db_path,
        require_current=True,
        allow_successor_write=True,
    )
    target_key = "feishu_project:g1q3:issue:7041712812"
    delivery_id = "silent-terminal-success-delivery"
    current = (terminal_at + timedelta(seconds=2)).isoformat()
    conn = delivery._connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        outbox_id = conn.execute(
            "SELECT outbox_id FROM rca_outbox "
            "WHERE business_key=? AND generation=2",
            (rerun.business_key,),
        ).fetchone()[0]
        conn.execute(
            """
            INSERT INTO rca_execution_watch(
                submission_key, submission_outbox_id, business_key, generation,
                project_key, work_item_type_key, work_item_id, state,
                next_poll_at, delivery_id, created_at, updated_at
            ) VALUES (?, ?, ?, 2, 'g1q3', 'issue', '7041712812',
                      'delivery_created', ?, ?, ?, ?)
            """,
            (
                rerun.submission_key,
                outbox_id,
                rerun.business_key,
                current,
                delivery_id,
                current,
                current,
            ),
        )
        conn.execute(
            """
            INSERT INTO rca_delivery_jobs(
                delivery_id, submission_key, business_key, generation,
                artifact_set_id, project_key, work_item_type_key, work_item_id,
                target_key, issue_url, report_url, status, manifest_json,
                contract_json, artifacts_json, created_at, updated_at
            ) VALUES (?, ?, ?, 2, 'silent-terminal-success-artifact', 'g1q3',
                      'issue', '7041712812', ?, ?, '', 'ready', '{}', '{}',
                      '[]', ?, ?)
            """,
            (
                delivery_id,
                rerun.submission_key,
                rerun.business_key,
                target_key,
                request.issue_url,
                current,
                current,
            ),
        )
        assert (
            delivery._learning_lane_guard_state_tx(
                conn,
                business_key=rerun.business_key,
                generation=rerun.generation,
                work_item_id="7041712812",
            )
            == "not_learning"
        )
        slot = delivery.enforce_issue_comment_budget_tx(
            conn,
            delivery_id=delivery_id,
            business_key=rerun.business_key,
            generation=rerun.generation,
            target_key=target_key,
            payload={"schema_version": "pnc_rca_delivery_effect_v4"},
        )
        conn.rollback()
    finally:
        conn.close()

    assert slot["comment_slot_kind"] == "conclusion"
    assert slot["comment_slot_generation"] == 2


def test_issue_only_operator_silent_terminal_drains_activation(tmp_path):
    store, _terminal_at = _silent_deadline_terminal_store(tmp_path)
    _convert_silent_terminal_to_issue_only_operator(store)
    conn = store._connect()
    try:
        [epoch] = conn.execute(
            "SELECT epoch_id FROM rca_activation_epochs WHERE is_current=1"
        ).fetchall()
        inflight = store._direct_steady_current_inflight_tx(
            conn, epoch_id=str(epoch["epoch_id"])
        )
    finally:
        conn.close()

    assert inflight["execution_delivery"] == 0


def test_issue_only_kafka_silent_terminal_drains_activation(tmp_path):
    store, _terminal_at = _silent_deadline_terminal_store(tmp_path)
    [source] = store.list_rows("rca_trigger_sources")
    [subscription] = store.list_rows("rca_delivery_subscriptions")
    assert source["source_kind"] == "kafka_workflow_event"
    assert subscription["effect_kind"] == "feishu_issue_comment"

    conn = store._connect()
    try:
        [epoch] = conn.execute(
            "SELECT epoch_id FROM rca_activation_epochs WHERE is_current=1"
        ).fetchall()
        inflight = store._direct_steady_current_inflight_tx(
            conn, epoch_id=str(epoch["epoch_id"])
        )
    finally:
        conn.close()

    assert inflight["execution_delivery"] == 0


def test_kafka_issue_only_shape_rejects_non_kafka_source(tmp_path):
    store, _terminal_at = _silent_deadline_terminal_store(tmp_path)
    [source] = store.list_rows("rca_trigger_sources")
    conn = store._connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "UPDATE rca_trigger_sources SET source_kind='feishu_group_manual' "
            "WHERE source_id=?",
            (source["source_id"],),
        )
        conn.commit()
        [epoch] = conn.execute(
            "SELECT epoch_id FROM rca_activation_epochs WHERE is_current=1"
        ).fetchall()
        inflight = store._direct_steady_current_inflight_tx(
            conn, epoch_id=str(epoch["epoch_id"])
        )
    finally:
        conn.close()

    assert inflight["execution_delivery"] == 1


def test_immediate_viz_gap_terminal_drains_activation_with_exact_receipt(tmp_path):
    store, _terminal_at = _silent_deadline_terminal_store(tmp_path)
    _convert_silent_terminal_to_issue_only_operator(store)
    _convert_silent_terminal_to_immediate_viz_gap(store)
    conn = store._connect()
    try:
        [epoch] = conn.execute(
            "SELECT epoch_id FROM rca_activation_epochs WHERE is_current=1"
        ).fetchall()
        inflight = store._direct_steady_current_inflight_tx(
            conn, epoch_id=str(epoch["epoch_id"])
        )
    finally:
        conn.close()

    assert inflight["execution_delivery"] == 0


def test_immediate_viz_gap_terminal_rejects_tampered_receipt(tmp_path):
    store, _terminal_at = _silent_deadline_terminal_store(tmp_path)
    _convert_silent_terminal_to_issue_only_operator(store)
    _convert_silent_terminal_to_immediate_viz_gap(store)
    delivery = RcaDeliveryStore(
        store.db_path,
        require_current=True,
        allow_successor_write=True,
    )
    [watch] = delivery.list_rows("rca_execution_watch")
    status = json.loads(watch["last_status_json"])
    status["failure_taxonomy"]["receipt"]["status"] = "success"
    conn = store._connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "UPDATE rca_execution_watch SET last_status_json=? "
            "WHERE submission_key=?",
            (
                control_store_module._canonical_json(status),
                watch["submission_key"],
            ),
        )
        conn.commit()
        [epoch] = conn.execute(
            "SELECT epoch_id FROM rca_activation_epochs WHERE is_current=1"
        ).fetchall()
        inflight = store._direct_steady_current_inflight_tx(
            conn, epoch_id=str(epoch["epoch_id"])
        )
    finally:
        conn.close()

    assert inflight["execution_delivery"] == 1


def test_legacy_gate_a_terminal_drains_activation_with_exact_route(tmp_path):
    store, _terminal_at = _silent_deadline_terminal_store(tmp_path)
    _convert_silent_terminal_to_issue_only_operator(store)
    _convert_silent_terminal_to_legacy_gate_a_gap(store)
    conn = store._connect()
    try:
        [epoch] = conn.execute(
            "SELECT epoch_id FROM rca_activation_epochs WHERE is_current=1"
        ).fetchall()
        inflight = store._direct_steady_current_inflight_tx(
            conn, epoch_id=str(epoch["epoch_id"])
        )
    finally:
        conn.close()

    assert inflight["execution_delivery"] == 0


def test_legacy_gate_a_terminal_rejects_route_source_tamper(tmp_path):
    store, _terminal_at = _silent_deadline_terminal_store(tmp_path)
    _convert_silent_terminal_to_issue_only_operator(store)
    _convert_silent_terminal_to_legacy_gate_a_gap(store)
    [watch] = RcaDeliveryStore(
        store.db_path,
        require_current=True,
        allow_successor_write=True,
    ).list_rows("rca_execution_watch")
    status = json.loads(watch["last_status_json"])
    status["failure_taxonomy"]["source"] = "untrusted_source"
    conn = store._connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "UPDATE rca_execution_watch SET last_status_json=? "
            "WHERE submission_key=?",
            (
                control_store_module._canonical_json(status),
                watch["submission_key"],
            ),
        )
        conn.commit()
        [epoch] = conn.execute(
            "SELECT epoch_id FROM rca_activation_epochs WHERE is_current=1"
        ).fetchall()
        inflight = store._direct_steady_current_inflight_tx(
            conn, epoch_id=str(epoch["epoch_id"])
        )
    finally:
        conn.close()

    assert inflight["execution_delivery"] == 1


def test_operator_silent_terminal_rerun_rejects_tampered_authority_without_mutation(
    tmp_path,
):
    store, terminal_at = _silent_deadline_terminal_store(tmp_path)
    request = _silent_batch_request()
    authority = {
        **_silent_batch_authority(store, request),
        "selection_sha256": "f" * 64,
    }
    before = {
        table: store.list_rows(table)
        for table in (
            "business_triggers",
            "rca_outbox",
            "rca_trigger_sources",
            "rca_shadow_promotion_audit",
        )
    }

    with pytest.raises(
        ManualRcaAdmissionError, match="silent_terminal_rerun_authority_invalid"
    ):
        store.admit_manual_trigger(
            request,
            allowed_chat_ids=set(),
            submit_enabled=True,
            operator_authorized=True,
            silent_terminal_rerun_authority=authority,
            activation_required=True,
            now=terminal_at + timedelta(seconds=1),
        )

    assert {table: store.list_rows(table) for table in before} == before


def test_evidence_quality_terminal_is_not_a_silent_technical_rerun(
    tmp_path,
):
    store, terminal_at = _silent_deadline_terminal_store(tmp_path)
    _rewrite_silent_terminal_error_code(store, "evidence_not_ready")
    request = _silent_batch_request(batch_id="batch-evidence-quality")
    authority = _silent_batch_authority(
        store,
        request,
        batch_id="batch-evidence-quality",
    )
    before = {
        table: store.list_rows(table)
        for table in (
            "business_triggers",
            "rca_outbox",
            "rca_trigger_sources",
            "rca_shadow_promotion_audit",
        )
    }

    with pytest.raises(
        ManualRcaAdmissionError,
        match="silent_terminal_rerun_terminal_generation_required",
    ):
        store.admit_manual_trigger(
            request,
            allowed_chat_ids=set(),
            submit_enabled=True,
            operator_authorized=True,
            silent_terminal_rerun_authority=authority,
            outbox_high_watermark=10_000,
            activation_required=True,
            now=terminal_at + timedelta(seconds=1),
        )

    assert {table: store.list_rows(table) for table in before} == before


def test_operator_silent_terminal_rerun_requires_callsite_activation_gate(
    tmp_path,
):
    store, terminal_at = _silent_deadline_terminal_store(tmp_path)
    request = _silent_batch_request()
    authority = _silent_batch_authority(store, request)
    before = {
        table: store.list_rows(table)
        for table in (
            "business_triggers",
            "rca_outbox",
            "rca_trigger_sources",
            "rca_shadow_promotion_audit",
        )
    }

    with pytest.raises(
        ManualRcaAdmissionError, match="silent_terminal_rerun_authority_invalid"
    ):
        store.admit_manual_trigger(
            request,
            allowed_chat_ids=set(),
            submit_enabled=True,
            operator_authorized=True,
            silent_terminal_rerun_authority=authority,
            activation_required=False,
            now=terminal_at + timedelta(seconds=1),
        )

    assert {table: store.list_rows(table) for table in before} == before


@pytest.mark.parametrize("invalid_state", ["retry_wait", "quarantined"])
def test_operator_silent_terminal_rerun_requires_settled_internal_outlet(
    tmp_path, invalid_state
):
    store, terminal_at = _silent_deadline_terminal_store(tmp_path)
    request = _silent_batch_request()
    authority = _silent_batch_authority(store, request)
    with sqlite3.connect(store.db_path) as conn:
        [status_raw] = conn.execute(
            "SELECT last_status_json FROM rca_execution_watch"
        ).fetchone()
        status = json.loads(status_raw)
        status["failure_taxonomy"]["durable_route"]["internal_outlet"][
            "status"
        ] = invalid_state
        conn.execute(
            "UPDATE rca_execution_watch SET last_status_json = ?",
            (json.dumps(status, sort_keys=True, separators=(",", ":")),),
        )

    with pytest.raises(
        ManualRcaAdmissionError,
        match="silent_terminal_rerun_terminal_generation_required",
    ):
        store.admit_manual_trigger(
            request,
            allowed_chat_ids=set(),
            submit_enabled=True,
            operator_authorized=True,
            silent_terminal_rerun_authority=authority,
            activation_required=True,
            now=terminal_at + timedelta(seconds=1),
        )
    assert len(store.list_rows("business_triggers")) == 1
    assert len(store.list_rows("rca_outbox")) == 1


@pytest.mark.parametrize(
    "error_code",
    [
        "service_provenance_unavailable",
        "taxonomy_gap:gate_a_projection_invalid",
        "taxonomy_gap:viz_evidence_unavailable",
        "remote_evidence_domain_unsupported",
        "taxonomy_gap:remote_evidence_domain_unsupported",
    ],
)
def test_operator_silent_terminal_rerun_rejects_materialized_old_effect(
    tmp_path, error_code
):
    store, terminal_at = _silent_deadline_terminal_store(tmp_path)
    _rewrite_silent_terminal_error_code(store, error_code)
    request = _silent_batch_request()
    authority = _silent_batch_authority(store, request)
    [watch] = RcaDeliveryStore(
        store.db_path,
        require_current=True,
        allow_successor_write=True,
    ).list_rows("rca_execution_watch")
    with sqlite3.connect(store.db_path) as conn:
        conn.execute(
            """
            INSERT INTO rca_delivery_jobs(
                delivery_id, submission_key, business_key, generation,
                artifact_set_id, project_key, work_item_type_key, work_item_id,
                target_key, issue_url, report_url, status, manifest_json,
                contract_json, artifacts_json, created_at, updated_at
            ) VALUES (
                'stale-delivery', ?, ?, ?, 'stale-artifact', ?, ?, ?,
                'stale-target', ?, '', 'ready', '{}', '{}', '[]', ?, ?
            )
            """,
            (
                watch["submission_key"],
                watch["business_key"],
                watch["generation"],
                watch["project_key"],
                watch["work_item_type_key"],
                watch["work_item_id"],
                request.issue_url,
                terminal_at.isoformat(),
                terminal_at.isoformat(),
            ),
        )

    with pytest.raises(
        ManualRcaAdmissionError,
        match="silent_terminal_rerun_terminal_generation_required",
    ):
        store.admit_manual_trigger(
            request,
            allowed_chat_ids=set(),
            submit_enabled=True,
            operator_authorized=True,
            silent_terminal_rerun_authority=authority,
            activation_required=True,
            now=terminal_at + timedelta(seconds=1),
        )
    assert len(store.list_rows("business_triggers")) == 1


def test_feishu_user_rerun_does_not_inherit_silent_terminal_exception(tmp_path):
    store, terminal_at = _silent_deadline_terminal_store(tmp_path)
    request = _manual_request(
        "om_user_silent_terminal",
        mode="rerun",
        requester_id="ou_" + "1" * 32,
    )

    with pytest.raises(
        ManualRcaAdmissionError,
        match="group_user_rerun_terminal_generation_required",
    ):
        store.admit_manual_trigger(
            request,
            allowed_chat_ids={"oc_allowed"},
            submit_enabled=True,
            user_rerun_authority=_group_user_rerun_authority(),
            activation_required=True,
            now=terminal_at + timedelta(seconds=1),
        )
    assert len(store.list_rows("business_triggers")) == 1


def _manual_activation_identity(
    message_id: str,
    *,
    mode: str = "run_or_join",
    issue_url: str = "https://project.feishu.cn/g1q3/issue/detail/7041712812",
    thread_id: str = "topic:om_root",
):
    request = _manual_request(
        message_id, mode=mode, issue_url=issue_url, thread_id=thread_id
    )
    return {
        "chat_id": request.chat_id,
        "thread_id": request.thread_id,
        "requester_id": request.requester_id,
        "message_id": request.message_id,
        "issue_url": request.issue_url,
        "mode": request.mode,
    }


def test_operator_terminal_rerun_is_rejected_without_generation(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    _register_policy_without_classifying(store)
    first = store.admit_manual_trigger(
        _manual_request("om_operator_seed"),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
    )
    _terminalize_permanent(store, first.submission_key)

    before = {
        table: len(store.list_rows(table))
        for table in (
            "business_triggers",
            "rca_outbox",
            "rca_trigger_sources",
            "rca_delivery_subscriptions",
        )
    }
    for index, mode in enumerate(("rerun", "debug"), start=1):
        with pytest.raises(
            ManualRcaAdmissionError,
            match="manual_generation_requires_explicit_user_rerun",
        ):
            store.admit_manual_trigger(
                _operator_request(
                    f"batch-20260724-7041712812-attempt-{index}",
                    mode=mode,
                ),
                allowed_chat_ids=set(),
                submit_enabled=True,
                operator_authorized=True,
            )
    assert {
        table: len(store.list_rows(table))
        for table in before
    } == before


def test_operator_rerun_requires_authorization_and_can_create_issue_only(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    _register_policy_without_classifying(store)
    request = _operator_request("batch-20260724-missing-attempt-1")

    with pytest.raises(ManualRcaAdmissionError, match="operator_not_authorized"):
        store.admit_manual_trigger(
            request,
            allowed_chat_ids=set(),
            submit_enabled=True,
        )
    admitted = store.admit_manual_trigger(
        request,
        allowed_chat_ids=set(),
        submit_enabled=True,
        operator_authorized=True,
    )

    assert admitted.outcome == "created"
    assert admitted.generation == 1
    subscriptions = [
        row
        for row in store.list_rows("rca_delivery_subscriptions")
        if row["business_key"] == admitted.business_key
    ]
    assert [row["effect_kind"] for row in subscriptions] == [
        "feishu_issue_comment"
    ]


def test_group_user_rerun_creates_next_generation_and_dedupes_for_ten_minutes(
    tmp_path,
):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    _register_policy_without_classifying(store)
    started = datetime(2026, 7, 28, 10, 0, tzinfo=timezone.utc)
    first = store.admit_manual_trigger(
        _manual_request("om_user_rerun_seed"),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
        now=started - timedelta(seconds=1),
    )
    _terminalize_permanent(store, first.submission_key)

    rerun = store.admit_manual_trigger(
        _manual_request(
            "om_user_rerun_1",
            mode="rerun",
            thread_id="topic:om_user_rerun_1",
            requester_id="ou_" + "1" * 32,
        ),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
        user_rerun_authority=_group_user_rerun_authority(),
        now=started,
    )
    counts_after_first = {
        table: len(store.list_rows(table))
        for table in (
            "business_triggers",
            "rca_outbox",
            "rca_trigger_sources",
            "rca_delivery_subscriptions",
        )
    }
    duplicate = store.admit_manual_trigger(
        _manual_request(
            "om_user_rerun_duplicate",
            mode="rerun",
            thread_id="topic:om_user_rerun_duplicate",
            requester_id="ou_" + "1" * 32,
        ),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
        user_rerun_authority=_group_user_rerun_authority(),
        now=started + timedelta(seconds=599),
    )

    assert rerun.outcome == "created"
    assert rerun.generation == 2
    assert duplicate.outcome == "deduped"
    assert duplicate.generation == rerun.generation
    assert duplicate.source_id == rerun.source_id
    assert {
        table: len(store.list_rows(table))
        for table in counts_after_first
    } == counts_after_first
    assert not [
        row
        for row in store.list_rows("rca_learning_lane_admissions")
        if row["business_key"] == rerun.business_key
        and row["generation"] == rerun.generation
    ]

    _terminalize_permanent(store, rerun.submission_key)
    after_window = store.admit_manual_trigger(
        _manual_request(
            "om_user_rerun_after_window",
            mode="rerun",
            thread_id="topic:om_user_rerun_after_window",
            requester_id="ou_" + "1" * 32,
        ),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
        user_rerun_authority=_group_user_rerun_authority(),
        now=started + timedelta(seconds=601),
    )

    assert after_window.outcome == "created"
    assert after_window.generation == 3


@pytest.mark.parametrize(
    "authority",
    [
        _group_user_rerun_authority("7041712813"),
        {
            **_group_user_rerun_authority(),
            "command_text": "重新分析  7041712812",
        },
        {
            **_group_user_rerun_authority(),
            "schema_version": "pnc_rca_group_user_rerun_v0",
        },
    ],
)
def test_group_user_rerun_invalid_authority_is_non_mutating(tmp_path, authority):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    before = len(store.list_rows("rca_trigger_sources"))

    with pytest.raises(
        ManualRcaAdmissionError,
        match="group_user_rerun_authority_(invalid|mismatch)",
    ):
        store.admit_manual_trigger(
            _manual_request(
                "om_invalid_user_rerun",
                mode="rerun",
                requester_id="ou_" + "1" * 32,
            ),
            allowed_chat_ids={"oc_allowed"},
            submit_enabled=True,
            user_rerun_authority=authority,
        )

    assert len(store.list_rows("rca_trigger_sources")) == before


def test_group_user_rerun_requires_human_feishu_identity(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")

    with pytest.raises(
        ManualRcaAdmissionError,
        match="manual_feishu_requester_identity_invalid",
    ):
        store.admit_manual_trigger(
            _manual_request(
                "om_machine_user_rerun",
                mode="rerun",
                requester_id="automation:batch",
            ),
            allowed_chat_ids={"oc_allowed"},
            submit_enabled=True,
            user_rerun_authority=_group_user_rerun_authority(),
        )

    assert store.list_rows("rca_trigger_sources") == []


def _activate_direct_steady_epoch(
    store: RcaControlStore,
    *,
    epoch_id: str = "rca-direct-20260712",
    release_fingerprint: str = "1" * 64,
    receipt_sha256: str = "a" * 64,
    config_sha256: str = "2" * 64,
    start_offset: int = 20,
    expected_predecessor: dict | None = None,
):
    predecessor = expected_predecessor or {}
    return store.activate_direct_steady_epoch(
        epoch_id=epoch_id,
        release_fingerprint_sha256=release_fingerprint,
        release_note_sha256=receipt_sha256,
        config_sha256=config_sha256,
        db_logical_identity={
            "device": 7,
            "inode": 11,
            "logical_store_id": "rca-control-primary",
        },
        partition_start_fence={TOPIC: {"2": start_offset}},
        operator="release-test",
        reason="activate direct steady test release",
        expected_predecessor_epoch_id=str(predecessor.get("epoch_id") or ""),
        expected_predecessor_state=str(predecessor.get("state") or ""),
        expected_predecessor_binding_fingerprint=str(
            predecessor.get("binding_fingerprint") or ""
        ),
    )


def _tamper_hidden_v14_binding(store: RcaControlStore) -> None:
    with sqlite3.connect(store.db_path) as conn:
        conn.execute(
            "UPDATE rca_activation_epochs "
            "SET preauthorization_fingerprint = ? WHERE is_current = 1",
            ("f" * 64,),
        )


def _sqlite_storage_identity(path: Path) -> dict[str, dict[str, int | str] | None]:
    identity: dict[str, dict[str, int | str] | None] = {}
    for suffix in ("", "-wal", "-shm"):
        candidate = Path(f"{path}{suffix}")
        try:
            observed = candidate.stat()
        except FileNotFoundError:
            identity[suffix or "db"] = None
            continue
        identity[suffix or "db"] = {
            "device": int(observed.st_dev),
            "inode": int(observed.st_ino),
            "size": int(observed.st_size),
            "mtime_ns": int(observed.st_mtime_ns),
            "sha256": hashlib.sha256(candidate.read_bytes()).hexdigest(),
        }
    return identity


def _assert_sqlite_payload_unchanged(
    before: dict[str, dict[str, int | str] | None],
    after: dict[str, dict[str, int | str] | None],
) -> None:
    """Existing WAL SHM coordination bytes may change during a read-only probe."""

    assert after["db"] == before["db"]
    assert after["-wal"] == before["-wal"]
    assert (after["-shm"] is None) is (before["-shm"] is None)


def _migrate_v14_fixture_to_v15(
    db_path,
    *,
    successor_epoch_id: str | None = None,
    successor_release_fingerprint_sha256: str = "3" * 64,
    successor_release_note_sha256: str = "4" * 64,
    successor_config_sha256: str = "5" * 64,
) -> dict[str, str]:
    """Build the reviewed physical v15 layout for N-1 compatibility tests."""

    path = Path(db_path)
    cross_trigger_names = (
        "trg_rca_admission_snapshot_execution_guard",
        "trg_terminal_rerun_delivery_authority_binding_guard",
        "trg_historical_epoch_rerun_delivery_authority_binding_guard",
    )

    def project_release_pairs(rows):
        projected = []
        for row in rows:
            release_fingerprint = row["production_fingerprint"]
            release_note = row["production_gate_receipt_sha256"]
            if release_fingerprint is None and release_note is None:
                projected.append((None, None))
                continue
            assert isinstance(release_fingerprint, str)
            assert isinstance(release_note, str)
            assert control_store_module._ACTIVATION_SHA256_RE.fullmatch(
                release_fingerprint
            )
            assert control_store_module._ACTIVATION_SHA256_RE.fullmatch(release_note)
            assert release_fingerprint != "0" * 64
            assert release_note != "0" * 64
            projected.append((release_fingerprint, release_note))
        return projected

    for successor_hash in (
        successor_release_fingerprint_sha256,
        successor_release_note_sha256,
        successor_config_sha256,
    ):
        assert control_store_module._ACTIVATION_SHA256_RE.fullmatch(successor_hash)
        assert successor_hash != "0" * 64
    with RcaControlStore.create_schema_probe_snapshot(path) as snapshot:
        uri = f"{snapshot.db_path.resolve().as_uri()}?mode=ro"
        preflight = sqlite3.connect(uri, uri=True)
        preflight.row_factory = sqlite3.Row
        try:
            project_release_pairs(
                preflight.execute(
                    "SELECT * FROM rca_activation_epochs ORDER BY epoch_id"
                ).fetchall()
            )
        finally:
            preflight.close()

    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA foreign_keys=OFF")
        assert int(conn.execute("PRAGMA foreign_keys").fetchone()[0]) == 0
        trigger_sql = {
            str(row["name"]): str(row["sql"])
            for row in conn.execute(
                "SELECT name, sql FROM sqlite_master "
                "WHERE type='trigger' AND name IN (?, ?, ?)",
                cross_trigger_names,
            ).fetchall()
        }
        assert set(trigger_sql) == set(cross_trigger_names)
        old_audits = [
            tuple(row)
            for row in conn.execute(
                "SELECT audit_id, epoch_id, from_state, to_state, operator, "
                "reason, binding_fingerprint, transitioned_at "
                "FROM rca_activation_transition_audit ORDER BY audit_id"
            ).fetchall()
        ]
        old_epochs = conn.execute(
            "SELECT * FROM rca_activation_epochs ORDER BY epoch_id"
        ).fetchall()
        old_current = next(
            (row for row in old_epochs if int(row["is_current"]) == 1),
            None,
        )
        assert old_current is not None
        old_current_audit = conn.execute(
            "SELECT binding_fingerprint FROM rca_activation_transition_audit "
            "WHERE epoch_id = ? ORDER BY audit_id DESC LIMIT 1",
            (old_current["epoch_id"],),
        ).fetchone()
        assert old_current_audit is not None
        successor_id = successor_epoch_id or f"{old_current['epoch_id']}-v15"
        migration_at = str(old_current["updated_at"])
        projected_release_pairs = project_release_pairs(old_epochs)
        conn.execute("BEGIN IMMEDIATE")
        for name in cross_trigger_names:
            conn.execute(f"DROP TRIGGER {name}")
        conn.execute(
            "ALTER TABLE rca_activation_transition_audit ADD COLUMN "
            "binding_schema_version TEXT NOT NULL "
            f"DEFAULT '{ACTIVATION_TRANSITION_BINDING_SCHEMA_V14}' "
            "CHECK(binding_schema_version IN ("
            f"'{ACTIVATION_TRANSITION_BINDING_SCHEMA_V14}', "
            f"'{ACTIVATION_TRANSITION_BINDING_SCHEMA_V15}'))"
        )
        conn.execute(control_store_module._V15_ACTIVATION_EPOCH_NEW_TABLE_SQL)
        for old, (release_fingerprint, release_note) in zip(
            old_epochs,
            projected_release_pairs,
            strict=True,
        ):
            conn.execute(
                """
                INSERT INTO rca_activation_epochs_v15_new(
                    epoch_id, state, is_current,
                    release_fingerprint_sha256, release_note_sha256,
                    config_sha256, db_logical_identity_json,
                    db_logical_identity_sha256, partition_start_fence_json,
                    partition_start_fence_sha256, created_at, updated_at,
                    activated_at, retired_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    old["epoch_id"],
                    "retired",
                    0,
                    release_fingerprint,
                    release_note,
                    old["config_sha256"],
                    old["db_logical_identity_json"],
                    old["db_logical_identity_sha256"],
                    old["partition_start_fence_json"],
                    old["partition_start_fence_sha256"],
                    old["created_at"],
                    migration_at,
                    old["steady_activated_at"],
                    migration_at,
                ),
            )
        conn.execute(
            """
            INSERT INTO rca_activation_epochs_v15_new(
                epoch_id, state, is_current,
                release_fingerprint_sha256, release_note_sha256,
                config_sha256, db_logical_identity_json,
                db_logical_identity_sha256, partition_start_fence_json,
                partition_start_fence_sha256, created_at, updated_at,
                activated_at, retired_at
            ) VALUES (?, 'steady_active', 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
            """,
            (
                successor_id,
                successor_release_fingerprint_sha256,
                successor_release_note_sha256,
                successor_config_sha256,
                old_current["db_logical_identity_json"],
                old_current["db_logical_identity_sha256"],
                old_current["partition_start_fence_json"],
                old_current["partition_start_fence_sha256"],
                migration_at,
                migration_at,
                migration_at,
            ),
        )
        conn.execute("DROP TABLE rca_activation_epochs")
        conn.execute(
            "ALTER TABLE rca_activation_epochs_v15_new RENAME TO rca_activation_epochs"
        )
        conn.execute(control_store_module._V15_CURRENT_ACTIVATION_INDEX_SQL)

        current_epoch = conn.execute(
            "SELECT * FROM rca_activation_epochs WHERE epoch_id = ?",
            (successor_id,),
        ).fetchone()
        assert current_epoch is not None
        material = RcaControlStore._v15_activation_transition_binding_material_tx(
            conn,
            epoch=current_epoch,
            from_state="v15_migration",
            to_state="steady_active",
        )
        successor_binding_fingerprint = control_store_module._canonical_sha256(material)
        conn.execute(
            """
            INSERT INTO rca_activation_transition_audit(
                epoch_id, from_state, to_state, operator, reason,
                binding_fingerprint, transitioned_at, binding_schema_version
            ) VALUES (?, 'v15_migration', 'steady_active', ?, ?, ?, ?, ?)
            """,
            (
                successor_id,
                "control-v15-fixture",
                "activate successor after v15 rebuild",
                successor_binding_fingerprint,
                migration_at,
                ACTIVATION_TRANSITION_BINDING_SCHEMA_V15,
            ),
        )
        for name in cross_trigger_names:
            conn.execute(trigger_sql[name])
        conn.execute(
            "UPDATE control_meta SET value = ? "
            "WHERE key = 'schema_version' AND value = ?",
            (
                CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION,
                CONTROL_STORE_SCHEMA_VERSION,
            ),
        )
        assert int(conn.execute("SELECT changes()").fetchone()[0]) == 1

        preserved_audits = [
            tuple(row)
            for row in conn.execute(
                "SELECT audit_id, epoch_id, from_state, to_state, operator, "
                "reason, binding_fingerprint, transitioned_at "
                "FROM rca_activation_transition_audit "
                "WHERE binding_schema_version = ? ORDER BY audit_id",
                (ACTIVATION_TRANSITION_BINDING_SCHEMA_V14,),
            ).fetchall()
        ]
        assert preserved_audits == old_audits

        expected_epoch_references = {
            "rca_activation_admission_ledger": "epoch_id",
            "rca_activation_transition_audit": "epoch_id",
            "rca_terminal_rerun_delivery_authorities": "activation_epoch_id",
            "rca_historical_epoch_rerun_delivery_authorities": ("activation_epoch_id"),
        }
        for table, source_column in expected_epoch_references.items():
            references = {
                (str(row["table"]), str(row["from"]), str(row["to"]))
                for row in conn.execute(f"PRAGMA foreign_key_list({table})")
            }
            assert (
                "rca_activation_epochs",
                source_column,
                "epoch_id",
            ) in references
        conn.commit()
        conn.execute("PRAGMA foreign_keys=ON")
        assert conn.execute("PRAGMA foreign_key_check").fetchone() is None
    except Exception:
        if conn.in_transaction:
            conn.rollback()
        raise
    finally:
        conn.close()
    return {
        "predecessor_epoch_id": str(old_current["epoch_id"]),
        "predecessor_binding_fingerprint": str(
            old_current_audit["binding_fingerprint"]
        ),
        "successor_epoch_id": successor_id,
        "successor_binding_fingerprint": successor_binding_fingerprint,
        "successor_release_fingerprint_sha256": (successor_release_fingerprint_sha256),
        "successor_release_note_sha256": successor_release_note_sha256,
        "successor_config_sha256": successor_config_sha256,
    }


def _direct_steady_contract(
    *,
    predecessor,
    epoch_id="rca-v15-successor",
    expected_schema=CONTROL_STORE_SCHEMA_VERSION,
    target_schema=CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION,
    release_fingerprint_sha256="3" * 64,
    release_note_sha256="4" * 64,
    config_sha256="5" * 64,
    db_logical_identity=None,
    partition_start_fence=None,
):
    db_identity = (
        {"database": "v15-control-test"}
        if db_logical_identity is None
        else db_logical_identity
    )
    partition_fence = {} if partition_start_fence is None else partition_start_fence
    predecessor = predecessor or {
        "epoch_id": "",
        "state": "",
        "binding_fingerprint": "",
    }
    activation = {
        "expected_control_schema_version": expected_schema,
        "target_control_schema_version": target_schema,
        "expected_predecessor_epoch_id": predecessor["epoch_id"],
        "expected_predecessor_state": predecessor["state"],
        "expected_predecessor_binding_fingerprint": predecessor[
            "binding_fingerprint"
        ],
        "db_logical_identity": db_identity,
        "db_logical_identity_sha256": control_store_module._canonical_sha256(
            db_identity
        ),
        "partition_start_fence": partition_fence,
        "partition_start_fence_sha256": control_store_module._canonical_sha256(
            partition_fence
        ),
    }
    material = {
        "schema_version": (
            control_store_module._MINIMAL_RELEASE_EPOCH_CONTRACT_SCHEMA_VERSION
        ),
        **activation,
    }
    return {
        "epoch_id": epoch_id,
        "release_fingerprint_sha256": release_fingerprint_sha256,
        "release_note_sha256": release_note_sha256,
        "config_sha256": config_sha256,
        "db_logical_identity": db_identity,
        "partition_start_fence": partition_fence,
        "expected_predecessor_epoch_id": predecessor["epoch_id"],
        "expected_predecessor_state": predecessor["state"],
        "expected_predecessor_binding_fingerprint": predecessor[
            "binding_fingerprint"
        ],
        "expected_control_schema_version": expected_schema,
        "target_control_schema_version": target_schema,
        "epoch_contract_sha256": control_store_module._canonical_sha256(material),
    }


def _migration_apply_kwargs(contract):
    return {
        **contract,
        "operator": "control-v15-migration-test",
        "reason": "atomically migrate exact v14 and activate v15",
        "now": datetime(2026, 8, 18, tzinfo=timezone.utc),
    }


def _install_known_legacy_v14_terminal_binding_guard(path):
    with sqlite3.connect(path) as conn:
        observed = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'trigger' "
            "AND name = 'trg_terminal_rerun_delivery_authority_binding_guard'"
        ).fetchone()
        assert observed is not None
        strict_raw = str(observed[0])
        strict_state = "AND epoch.state = 'steady_active'"
        legacy_state = "AND epoch.state IN ('bounded_active', 'steady_active')"
        assert strict_raw.count(strict_state) == 1
        legacy_raw = strict_raw.replace(strict_state, legacy_state, 1)
        strict = RcaControlStore._normalized_schema_sql(strict_raw)
        legacy = RcaControlStore._normalized_schema_sql(legacy_raw)
        conn.execute(
            "DROP TRIGGER trg_terminal_rerun_delivery_authority_binding_guard"
        )
        conn.execute(legacy_raw)
    return strict, legacy


def _install_slot_bound_v14_history(path, *, epoch_id):
    slot_rows = [
        (epoch_id, "kafka_success", None, None, None, None, None),
        (
            epoch_id,
            "manual_success",
            "manual",
            "d" * 64,
            "release-test",
            "authorize historical manual success",
            1,
        ),
        (
            epoch_id,
            "manual_terminal_failure",
            "manual",
            "e" * 64,
            "release-test",
            "authorize historical terminal failure",
            2,
        ),
    ]
    with sqlite3.connect(path) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute(
            """
            CREATE TABLE rca_activation_budget_slots(
                epoch_id TEXT NOT NULL,
                slot_kind TEXT NOT NULL,
                authorized_source_kind TEXT,
                authorized_identity_sha256 TEXT,
                authorized_at TEXT,
                authorized_operator TEXT,
                authorized_reason TEXT,
                consumed_ledger_id INTEGER,
                consumed_at TEXT,
                PRIMARY KEY(epoch_id, slot_kind)
            )
            """
        )
        conn.executemany(
            "INSERT INTO rca_activation_budget_slots("
            "epoch_id, slot_kind, authorized_source_kind, "
            "authorized_identity_sha256, authorized_operator, "
            "authorized_reason, consumed_ledger_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            slot_rows,
        )
        epoch = conn.execute(
            "SELECT * FROM rca_activation_epochs WHERE epoch_id = ?",
            (epoch_id,),
        ).fetchone()
        audit = conn.execute(
            "SELECT audit_id, from_state, to_state "
            "FROM rca_activation_transition_audit WHERE epoch_id = ? "
            "ORDER BY audit_id DESC LIMIT 1",
            (epoch_id,),
        ).fetchone()
        assert epoch is not None and audit is not None
        slots = [
            {
                "authorized_identity_sha256": str(
                    row["authorized_identity_sha256"] or ""
                ),
                "authorized_operator": str(row["authorized_operator"] or ""),
                "authorized_reason": str(row["authorized_reason"] or ""),
                "authorized_source_kind": str(
                    row["authorized_source_kind"] or ""
                ),
                "consumed_ledger_id": int(row["consumed_ledger_id"] or 0),
                "slot_kind": str(row["slot_kind"]),
            }
            for row in conn.execute(
                "SELECT slot_kind, authorized_source_kind, "
                "authorized_identity_sha256, authorized_operator, "
                "authorized_reason, consumed_ledger_id "
                "FROM rca_activation_budget_slots "
                "WHERE epoch_id = ? ORDER BY slot_kind",
                (epoch_id,),
            ).fetchall()
        ]
        material = {
            "config_sha256": str(epoch["config_sha256"]),
            "db_logical_identity_sha256": str(epoch["db_logical_identity_sha256"]),
            "epoch_id": str(epoch["epoch_id"]),
            "from_state": str(audit["from_state"]),
            "partition_end_fence_sha256": str(
                epoch["partition_end_fence_sha256"] or ""
            ),
            "partition_start_fence_sha256": str(
                epoch["partition_start_fence_sha256"]
            ),
            "preauthorization_capsule_sha256": str(
                epoch["preauthorization_capsule_sha256"]
            ),
            "preauthorization_fingerprint": str(
                epoch["preauthorization_fingerprint"]
            ),
            "preauthorization_gate_receipt_sha256": str(
                epoch["preauthorization_gate_receipt_sha256"]
            ),
            "preproduction_capsule_sha256": str(
                epoch["preproduction_capsule_sha256"] or ""
            ),
            "preproduction_fingerprint": str(
                epoch["preproduction_fingerprint"] or ""
            ),
            "preproduction_gate_receipt_sha256": str(
                epoch["preproduction_gate_receipt_sha256"] or ""
            ),
            "production_fingerprint": str(epoch["production_fingerprint"] or ""),
            "production_gate_receipt_sha256": str(
                epoch["production_gate_receipt_sha256"] or ""
            ),
            "slot_bindings_sha256": control_store_module._canonical_sha256(slots),
            "to_state": str(audit["to_state"]),
        }
        fingerprint = control_store_module._canonical_sha256(material)
        conn.execute(
            "UPDATE rca_activation_transition_audit "
            "SET binding_fingerprint = ? WHERE audit_id = ?",
            (fingerprint, int(audit["audit_id"])),
        )
    return fingerprint


def test_nminus1_successor_flag_keeps_exact_v14_writable(tmp_path):
    path = tmp_path / "control.sqlite3"
    RcaControlStore(path)
    before = _sqlite_storage_identity(path)

    store = RcaControlStore(
        path,
        require_current=True,
        allow_successor_read_only=True,
    )

    assert store.schema_runtime_capability() == {
        "observed_control_schema_version": CONTROL_STORE_SCHEMA_VERSION,
        "binary_write_schema_version": CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION,
        "mode": "current_write",
        "read_supported": True,
        "write_enabled": True,
        "work_admission_enabled": True,
        "lease_acquisition_enabled": True,
        "external_effect_enabled": True,
    }
    _assert_sqlite_payload_unchanged(before, _sqlite_storage_identity(path))
    assert _activate_direct_steady_epoch(store)["state"] == "steady_active"


def test_nminus1_exact_v15_opens_only_as_snapshot_query_only(tmp_path):
    path = tmp_path / "control.sqlite3"
    control = RcaControlStore(path)
    predecessor = _activate_direct_steady_epoch(control)
    migration = _migrate_v14_fixture_to_v15(
        path,
        successor_epoch_id="rca-v15-successor",
    )
    before = _sqlite_storage_identity(path)

    store = RcaControlStore(
        path,
        require_current=True,
        allow_successor_read_only=True,
    )

    assert migration["predecessor_epoch_id"] == predecessor["epoch_id"]
    assert store.schema_runtime_capability() == {
        "observed_control_schema_version": CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION,
        "binary_write_schema_version": CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION,
        "mode": "successor_read_only",
        "read_supported": True,
        "write_enabled": False,
        "work_admission_enabled": False,
        "lease_acquisition_enabled": False,
        "external_effect_enabled": False,
    }
    epoch = store.activation_epoch()
    assert epoch is not None
    assert epoch["epoch_id"] == migration["successor_epoch_id"]
    assert (
        epoch["release_fingerprint_sha256"]
        == migration["successor_release_fingerprint_sha256"]
    )
    assert epoch["release_note_sha256"] == migration["successor_release_note_sha256"]
    assert epoch["config_sha256"] == migration["successor_config_sha256"]
    assert "partition_end_fence_sha256" not in epoch
    health = store.health()
    assert health["schema_version"] == CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION
    assert health["ok"] is True
    assert health["process_healthy"] is True
    assert health["business_ready"] is False
    assert health["activation"]["production_active"] is False
    assert health["schema_runtime_capability"] == store.schema_runtime_capability()
    conn = store._connect()
    try:
        assert int(conn.execute("PRAGMA query_only").fetchone()[0]) == 1
        [(database_path,)] = conn.execute(
            "SELECT file FROM pragma_database_list WHERE name = 'main'"
        ).fetchall()
        assert Path(str(database_path)) != path
    finally:
        conn.close()
    _assert_sqlite_payload_unchanged(before, _sqlite_storage_identity(path))


def test_v15_neutral_binding_requires_nonzero_release_pairs_but_allows_retired_null(
    tmp_path,
):
    path = tmp_path / "control.sqlite3"
    control = RcaControlStore(path)
    _activate_direct_steady_epoch(control)
    migration = _migrate_v14_fixture_to_v15(path)

    with sqlite3.connect(path) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute(
            "UPDATE rca_activation_epochs "
            "SET release_fingerprint_sha256 = NULL, release_note_sha256 = NULL "
            "WHERE epoch_id = ? AND state = 'retired' AND is_current = 0",
            (migration["predecessor_epoch_id"],),
        )
        assert int(conn.execute("SELECT changes()").fetchone()[0]) == 1
        predecessor = conn.execute(
            "SELECT * FROM rca_activation_epochs WHERE epoch_id = ?",
            (migration["predecessor_epoch_id"],),
        ).fetchone()
        assert predecessor is not None
        predecessor_material = (
            RcaControlStore._v15_activation_transition_binding_material_tx(
                conn,
                epoch=predecessor,
                from_state="steady_active",
                to_state="retired",
            )
        )
        assert predecessor_material["release_fingerprint_sha256"] is None
        assert predecessor_material["release_note_sha256"] is None
        predecessor_binding = control_store_module._canonical_sha256(
            predecessor_material
        )
        conn.execute(
            """
            INSERT INTO rca_activation_transition_audit(
                epoch_id, from_state, to_state, operator, reason,
                binding_fingerprint, transitioned_at, binding_schema_version
            ) VALUES (?, 'steady_active', 'retired', ?, ?, ?, ?, ?)
            """,
            (
                migration["predecessor_epoch_id"],
                "control-v15-pair-test",
                "validate neutral retired release pair",
                predecessor_binding,
                str(predecessor["updated_at"]),
                ACTIVATION_TRANSITION_BINDING_SCHEMA_V15,
            ),
        )
        retired_binding = RcaControlStore._v15_activation_transition_binding_tx(
            conn,
            epoch=predecessor,
        )
        assert retired_binding["state"] == "retired"
        assert retired_binding["binding_fingerprint"] == predecessor_binding

        current = conn.execute(
            "SELECT * FROM rca_activation_epochs WHERE is_current = 1"
        ).fetchone()
        assert current is not None
        invalid_pairs = (
            (None, None),
            ("0" * 64, "0" * 64),
            (None, "6" * 64),
            ("6" * 64, None),
        )
        for release_fingerprint, release_note in invalid_pairs:
            invalid_current = dict(current)
            invalid_current["release_fingerprint_sha256"] = release_fingerprint
            invalid_current["release_note_sha256"] = release_note
            with pytest.raises(
                ActivationEpochError,
                match="activation_predecessor_binding_invalid",
            ):
                RcaControlStore._v15_activation_transition_binding_tx(
                    conn,
                    epoch=invalid_current,
                )

        for release_fingerprint, release_note in invalid_pairs[1:]:
            invalid_retired = dict(predecessor)
            invalid_retired["release_fingerprint_sha256"] = release_fingerprint
            invalid_retired["release_note_sha256"] = release_note
            with pytest.raises(
                ActivationEpochError,
                match="activation_predecessor_binding_invalid",
            ):
                RcaControlStore._v15_activation_transition_binding_tx(
                    conn,
                    epoch=invalid_retired,
                )


def test_nminus1_v15_default_constructor_rejects_without_source_mutation(tmp_path):
    path = tmp_path / "control.sqlite3"
    control = RcaControlStore(path)
    _activate_direct_steady_epoch(control)
    _migrate_v14_fixture_to_v15(path)
    before = _sqlite_storage_identity(path)

    with pytest.raises(RuntimeError, match="incompatible_control_store_schema:version"):
        RcaControlStore(path, require_current=True)

    _assert_sqlite_payload_unchanged(before, _sqlite_storage_identity(path))


def test_nminus1_v15_writer_is_denied_without_source_mutation(tmp_path):
    path = tmp_path / "control.sqlite3"
    control = RcaControlStore(path)
    _activate_direct_steady_epoch(control)
    _migrate_v14_fixture_to_v15(path)
    store = RcaControlStore(
        path,
        require_current=True,
        allow_successor_read_only=True,
    )
    before = _sqlite_storage_identity(path)

    with pytest.raises(sqlite3.OperationalError, match="readonly"):
        _activate_direct_steady_epoch(
            store,
            epoch_id="rca-v15-forbidden-writer",
            expected_predecessor=store.direct_steady_predecessor(),
        )

    _assert_sqlite_payload_unchanged(before, _sqlite_storage_identity(path))


@pytest.mark.parametrize("variant", ["unknown", "partial_v15", "mixed_v15"])
def test_nminus1_unknown_or_partial_layout_rejects_without_mutation(
    tmp_path,
    variant,
):
    path = tmp_path / f"{variant}.sqlite3"
    control = RcaControlStore(path)
    _activate_direct_steady_epoch(control)
    if variant == "mixed_v15":
        _migrate_v14_fixture_to_v15(path)
        with sqlite3.connect(path) as conn:
            conn.execute(
                "ALTER TABLE rca_activation_epochs "
                "ADD COLUMN production_fingerprint TEXT"
            )
    else:
        marker = (
            "pnc_rca_control_store_v99"
            if variant == "unknown"
            else CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION
        )
        with sqlite3.connect(path) as conn:
            conn.execute(
                "UPDATE control_meta SET value = ? WHERE key = 'schema_version'",
                (marker,),
            )
    before = _sqlite_storage_identity(path)

    with pytest.raises(RuntimeError, match="incompatible_control_store_schema"):
        RcaControlStore(
            path,
            require_current=True,
            allow_successor_read_only=True,
        )

    _assert_sqlite_payload_unchanged(before, _sqlite_storage_identity(path))


def test_nminus1_v15_rejects_weakened_activation_audit_ddl(tmp_path):
    path = tmp_path / "control.sqlite3"
    control = RcaControlStore(path)
    _activate_direct_steady_epoch(control)
    _migrate_v14_fixture_to_v15(path)
    with sqlite3.connect(path) as conn:
        conn.execute("PRAGMA foreign_keys=OFF")
        strong_sql = str(
            conn.execute(
                "SELECT sql FROM sqlite_master WHERE type = 'table' "
                "AND name = 'rca_activation_transition_audit'"
            ).fetchone()[0]
        )
        constraint = (
            "CHECK(binding_schema_version IN ("
            f"'{ACTIVATION_TRANSITION_BINDING_SCHEMA_V14}', "
            f"'{ACTIVATION_TRANSITION_BINDING_SCHEMA_V15}'))"
        )
        weak_sql = strong_sql.replace(
            constraint,
            f"CHECK ('{ACTIVATION_TRANSITION_BINDING_SCHEMA_V15}' <> '')",
        )
        assert weak_sql != strong_sql
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "ALTER TABLE rca_activation_transition_audit "
            "RENAME TO rca_activation_transition_audit_strong"
        )
        conn.execute(weak_sql)
        conn.execute(
            "INSERT INTO rca_activation_transition_audit "
            "SELECT * FROM rca_activation_transition_audit_strong"
        )
        conn.execute("DROP TABLE rca_activation_transition_audit_strong")
        conn.execute(
            "CREATE INDEX idx_rca_activation_transition_epoch "
            "ON rca_activation_transition_audit(epoch_id, audit_id)"
        )
        conn.commit()
    before = _sqlite_storage_identity(path)

    with pytest.raises(
        RuntimeError,
        match="incompatible_control_store_schema:v15_activation_sql",
    ):
        RcaControlStore(
            path,
            require_current=True,
            allow_successor_read_only=True,
        )

    _assert_sqlite_payload_unchanged(before, _sqlite_storage_identity(path))


@pytest.mark.parametrize(
    "statement, parameters",
    [
        (
            "UPDATE rca_activation_epochs SET release_fingerprint_sha256 = ? "
            "WHERE is_current = 1",
            ("f" * 64,),
        ),
        (
            "UPDATE rca_activation_transition_audit "
            "SET binding_fingerprint = ? WHERE audit_id = ("
            "SELECT MAX(audit_id) FROM rca_activation_transition_audit)",
            ("e" * 64,),
        ),
        (
            "UPDATE rca_activation_transition_audit "
            "SET binding_schema_version = ? WHERE audit_id = ("
            "SELECT MAX(audit_id) FROM rca_activation_transition_audit)",
            (ACTIVATION_TRANSITION_BINDING_SCHEMA_V14,),
        ),
    ],
)
def test_nminus1_v15_current_binding_tamper_rejects_without_mutation(
    tmp_path,
    statement,
    parameters,
):
    path = tmp_path / "control.sqlite3"
    control = RcaControlStore(path)
    _activate_direct_steady_epoch(control)
    _migrate_v14_fixture_to_v15(path)
    with sqlite3.connect(path) as conn:
        conn.execute(statement, parameters)
    before = _sqlite_storage_identity(path)

    with pytest.raises(
        ActivationEpochError,
        match="activation_predecessor_binding_invalid",
    ):
        RcaControlStore(
            path,
            require_current=True,
            allow_successor_read_only=True,
        )

    _assert_sqlite_payload_unchanged(before, _sqlite_storage_identity(path))


def test_v15_fixture_freezes_post_rename_sql_and_preserves_v14_audit(tmp_path):
    path = tmp_path / "control.sqlite3"
    control = RcaControlStore(path)
    _activate_direct_steady_epoch(control)
    migration = _migrate_v14_fixture_to_v15(path)
    expected_hashes = {
        "idx_rca_single_current_activation_epoch": (
            "420643b44c4c0930a565204caa9c13b64d082d00ad7bb90154ce7864f1493828"
        ),
            "rca_activation_epochs": (
                "37524df29261dcada26541544e1b911c47b5ae59285f3c55ece6ce66275a3b21"
            ),
        "rca_activation_transition_audit": (
            "bd44ef62a5acca229421259d71364456847f2cc988b9bc14ec1666eb7511473b"
        ),
    }
    with sqlite3.connect(path) as conn:
        rows = conn.execute(
            "SELECT name, sql FROM sqlite_master WHERE name IN (?, ?, ?) ORDER BY name",
            tuple(sorted(expected_hashes)),
        ).fetchall()
        audits = conn.execute(
            "SELECT epoch_id, binding_schema_version, binding_fingerprint "
            "FROM rca_activation_transition_audit ORDER BY audit_id"
        ).fetchall()

    assert {
        name: hashlib.sha256(sql.encode()).hexdigest() for name, sql in rows
    } == expected_hashes
    assert audits == [
        (
            migration["predecessor_epoch_id"],
            ACTIVATION_TRANSITION_BINDING_SCHEMA_V14,
            migration["predecessor_binding_fingerprint"],
        ),
        (
            migration["successor_epoch_id"],
            ACTIVATION_TRANSITION_BINDING_SCHEMA_V15,
            migration["successor_binding_fingerprint"],
        ),
    ]


@pytest.mark.parametrize("pair_variant", ["half", "zero"])
def test_v15_fixture_rejects_invalid_parent_release_pair_before_ddl(
    tmp_path,
    pair_variant,
):
    path = tmp_path / "control.sqlite3"
    control = RcaControlStore(path)
    _activate_direct_steady_epoch(control)
    with sqlite3.connect(path) as conn:
        if pair_variant == "half":
            conn.execute(
                "UPDATE rca_activation_epochs "
                "SET production_gate_receipt_sha256 = NULL WHERE is_current = 1"
            )
        else:
            conn.execute(
                "UPDATE rca_activation_epochs SET production_fingerprint = ?, "
                "production_gate_receipt_sha256 = ? WHERE is_current = 1",
                ("0" * 64, "0" * 64),
            )
    before = _sqlite_storage_identity(path)

    with pytest.raises(AssertionError):
        _migrate_v14_fixture_to_v15(path)

    assert _sqlite_storage_identity(path) == before
    with sqlite3.connect(path) as conn:
        assert (
            conn.execute(
                "SELECT value FROM control_meta WHERE key = 'schema_version'"
            ).fetchone()[0]
            == CONTROL_STORE_SCHEMA_VERSION
        )
        assert (
            conn.execute(
                "SELECT 1 FROM sqlite_master "
                "WHERE name = 'rca_activation_epochs_v15_new'"
            ).fetchone()
            is None
        )


def test_successor_write_requires_exact_v15_and_reports_current_write(tmp_path):
    path = tmp_path / "control.sqlite3"
    control = _steady_control_store(path)
    with pytest.raises(
        RuntimeError,
        match="incompatible_control_store_schema:write_marker",
    ):
        RcaControlStore(path, require_current=True, allow_successor_write=True)
    _migrate_v14_fixture_to_v15(path)

    store = RcaControlStore(
        path,
        require_current=True,
        allow_successor_write=True,
    )

    assert store.schema_runtime_capability() == {
        "observed_control_schema_version": CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION,
        "binary_write_schema_version": CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION,
        "mode": "current_write",
        "read_supported": True,
        "write_enabled": True,
        "work_admission_enabled": True,
        "lease_acquisition_enabled": True,
        "external_effect_enabled": True,
    }
    store.open_dispatcher_circuit(reason_code="v15_writer_test")
    assert store.dispatcher_circuit().state == "open"
    assert control.schema_runtime_capability()["binary_write_schema_version"] == (
        CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION
    )


@pytest.mark.parametrize(
    "arguments",
    [
        {"allow_successor_write": True},
        {"require_current": True, "read_only": True, "allow_successor_write": True},
        {
            "require_current": True,
            "allow_successor_read_only": True,
            "allow_successor_write": True,
        },
    ],
)
def test_successor_write_constructor_flags_fail_closed(tmp_path, arguments):
    path = tmp_path / "control.sqlite3"
    RcaControlStore(path)

    with pytest.raises(ValueError):
        RcaControlStore(path, **arguments)


def test_atomic_v14_to_v15_migration_preserves_audit_index_and_sequence(tmp_path):
    path = tmp_path / "control.sqlite3"
    control = _steady_control_store(path)
    predecessor = control.direct_steady_predecessor()
    assert predecessor is not None
    contract = _direct_steady_contract(predecessor=predecessor)
    probe_kwargs = dict(contract)
    with sqlite3.connect(path) as conn:
        old_audits = conn.execute(
            "SELECT audit_id, epoch_id, from_state, to_state, operator, reason, "
            "binding_fingerprint, transitioned_at "
            "FROM rca_activation_transition_audit ORDER BY audit_id"
        ).fetchall()
        old_index_sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'index' "
            "AND name = 'idx_rca_activation_transition_epoch'"
        ).fetchone()[0]
        old_sequence = conn.execute(
            "SELECT seq FROM sqlite_sequence "
            "WHERE name = 'rca_activation_transition_audit'"
        ).fetchone()[0]
    assert RcaControlStore.probe_v14_to_v15_migration_outcome(
        path,
        **probe_kwargs,
    ) == "not_committed"

    migrated = RcaControlStore.migrate_v14_to_v15_and_activate(
        path,
        **_migration_apply_kwargs(contract),
    )

    assert migrated["epoch_id"] == contract["epoch_id"]
    assert "partition_end_fence_sha256" not in migrated
    assert RcaControlStore.probe_v14_to_v15_migration_outcome(
        path,
        **probe_kwargs,
    ) == "committed"
    with sqlite3.connect(path) as conn:
        assert conn.execute("PRAGMA quick_check").fetchone()[0] == "ok"
        assert conn.execute("PRAGMA foreign_key_check").fetchone() is None
        assert conn.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'index' "
            "AND name = 'idx_rca_activation_transition_epoch'"
        ).fetchone()[0] == old_index_sql
        assert conn.execute(
            "SELECT seq FROM sqlite_sequence "
            "WHERE name = 'rca_activation_transition_audit'"
        ).fetchone()[0] == old_sequence + 1
        preserved = conn.execute(
            "SELECT audit_id, epoch_id, from_state, to_state, operator, reason, "
            "binding_fingerprint, transitioned_at "
            "FROM rca_activation_transition_audit "
            "WHERE binding_schema_version = ? ORDER BY audit_id",
            (ACTIVATION_TRANSITION_BINDING_SCHEMA_V14,),
        ).fetchall()
        assert preserved == old_audits
        assert conn.execute(
            "SELECT state, is_current FROM rca_activation_epochs WHERE epoch_id = ?",
            (predecessor["epoch_id"],),
        ).fetchone() == ("retired", 0)
        audit_count = conn.execute(
            "SELECT COUNT(*) FROM rca_activation_transition_audit"
        ).fetchone()[0]

    retried = RcaControlStore.migrate_v14_to_v15_and_activate(
        path,
        **_migration_apply_kwargs(contract),
    )
    assert retried == migrated
    with sqlite3.connect(path) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM rca_activation_transition_audit"
        ).fetchone()[0] == audit_count


def test_v14_to_v15_migration_preserves_slot_bound_historical_audits(tmp_path):
    path = tmp_path / "control.sqlite3"
    control = RcaControlStore(path)
    historical = _activate_direct_steady_epoch(
        control,
        epoch_id="rca-a-slot-bound-history",
    )
    predecessor = control.direct_steady_predecessor()
    assert predecessor is not None
    _activate_direct_steady_epoch(
        control,
        epoch_id="rca-z-current-direct",
        start_offset=30,
        expected_predecessor=predecessor,
    )
    _install_slot_bound_v14_history(path, epoch_id=historical["epoch_id"])
    current = control.direct_steady_predecessor()
    assert current is not None
    contract = _direct_steady_contract(predecessor=current)
    with sqlite3.connect(path) as conn:
        old_audits = conn.execute(
            "SELECT audit_id, epoch_id, from_state, to_state, operator, reason, "
            "binding_fingerprint, transitioned_at "
            "FROM rca_activation_transition_audit ORDER BY audit_id"
        ).fetchall()

    assert RcaControlStore.probe_v14_to_v15_migration_outcome(
        path,
        **contract,
    ) == "not_committed"
    migrated = RcaControlStore.migrate_v14_to_v15_and_activate(
        path,
        **_migration_apply_kwargs(contract),
    )

    assert migrated["epoch_id"] == contract["epoch_id"]
    with sqlite3.connect(path) as conn:
        preserved = conn.execute(
            "SELECT audit_id, epoch_id, from_state, to_state, operator, reason, "
            "binding_fingerprint, transitioned_at "
            "FROM rca_activation_transition_audit "
            "WHERE binding_schema_version = ? ORDER BY audit_id",
            (ACTIVATION_TRANSITION_BINDING_SCHEMA_V14,),
        ).fetchall()
    assert preserved == old_audits


def test_v14_to_v15_migration_rejects_slot_bound_history_drift_prewrite(tmp_path):
    path = tmp_path / "control.sqlite3"
    control = RcaControlStore(path)
    historical = _activate_direct_steady_epoch(
        control,
        epoch_id="rca-a-slot-bound-history",
    )
    predecessor = control.direct_steady_predecessor()
    assert predecessor is not None
    _activate_direct_steady_epoch(
        control,
        epoch_id="rca-z-current-direct",
        start_offset=30,
        expected_predecessor=predecessor,
    )
    _install_slot_bound_v14_history(path, epoch_id=historical["epoch_id"])
    current = control.direct_steady_predecessor()
    assert current is not None
    contract = _direct_steady_contract(predecessor=current)
    with sqlite3.connect(path) as conn:
        conn.execute(
            "UPDATE rca_activation_budget_slots SET authorized_reason = ? "
            "WHERE epoch_id = ? AND slot_kind = 'manual_success'",
            ("drifted historical authorization", historical["epoch_id"]),
        )
    before = _sqlite_storage_identity(path)

    assert RcaControlStore.probe_v14_to_v15_migration_outcome(
        path,
        **contract,
    ) == "unknown"
    with pytest.raises(
        ActivationEpochError,
        match="activation_predecessor_binding_invalid",
    ):
        RcaControlStore.migrate_v14_to_v15_and_activate(
            path,
            **_migration_apply_kwargs(contract),
        )

    _assert_sqlite_payload_unchanged(before, _sqlite_storage_identity(path))


def test_v14_to_v15_issue_only_empty_fence_ignores_unrelated_progress(tmp_path):
    path = tmp_path / "control.sqlite3"
    control = _steady_control_store(path)
    record = _record(offset=25)
    control.persist_raw(record, policy=_policy())
    control.process_event(record.event_uid)
    assert control.partition_progress(topic=TOPIC, partitions=[2]) == {2: 26}
    predecessor = control.direct_steady_predecessor()
    assert predecessor is not None
    contract = _direct_steady_contract(
        predecessor=predecessor,
        partition_start_fence={},
    )

    assert RcaControlStore.probe_v14_to_v15_migration_outcome(
        path,
        **contract,
    ) == "not_committed"
    migrated = RcaControlStore.migrate_v14_to_v15_and_activate(
        path,
        **_migration_apply_kwargs(contract),
    )
    assert migrated["epoch_id"] == contract["epoch_id"]


def test_v14_to_v15_declared_partition_fence_drift_is_prewrite_rejected(tmp_path):
    path = tmp_path / "control.sqlite3"
    control = _steady_control_store(path)
    record = _record(offset=25)
    control.persist_raw(record, policy=_policy())
    control.process_event(record.event_uid)
    assert control.partition_progress(topic=TOPIC, partitions=[2]) == {2: 26}
    predecessor = control.direct_steady_predecessor()
    assert predecessor is not None
    contract = _direct_steady_contract(
        predecessor=predecessor,
        partition_start_fence={TOPIC: {"2": 27}},
    )
    before = _sqlite_storage_identity(path)

    assert RcaControlStore.probe_v14_to_v15_migration_outcome(
        path,
        **contract,
    ) == "unknown"
    with pytest.raises(
        ActivationEpochError,
        match="activation_partition_fence_changed",
    ):
        RcaControlStore.migrate_v14_to_v15_and_activate(
            path,
            **_migration_apply_kwargs(contract),
        )

    _assert_sqlite_payload_unchanged(before, _sqlite_storage_identity(path))


def test_known_legacy_v14_terminal_guard_is_read_only_predecessor_only(tmp_path):
    path = tmp_path / "control.sqlite3"
    _steady_control_store(path)
    _install_known_legacy_v14_terminal_binding_guard(path)
    before = _sqlite_storage_identity(path)

    reader = RcaControlStore(
        path,
        require_current=True,
        read_only=True,
        allow_successor_read_only=True,
    )

    assert reader.schema_runtime_capability()["mode"] == "explicit_read_only"
    _assert_sqlite_payload_unchanged(before, _sqlite_storage_identity(path))
    with pytest.raises(
        RuntimeError,
        match="incompatible_control_store_schema:"
        "terminal_rerun_authority_trigger_sql",
    ):
        RcaControlStore(
            path,
            require_current=True,
            allow_successor_read_only=True,
        )
    _assert_sqlite_payload_unchanged(before, _sqlite_storage_identity(path))


def test_known_legacy_v14_terminal_guard_migrates_to_strict_v15(tmp_path):
    path = tmp_path / "control.sqlite3"
    control = _steady_control_store(path)
    predecessor = control.direct_steady_predecessor()
    assert predecessor is not None
    contract = _direct_steady_contract(predecessor=predecessor)
    strict, legacy = _install_known_legacy_v14_terminal_binding_guard(path)
    assert RcaControlStore.probe_v14_to_v15_migration_outcome(
        path,
        **contract,
    ) == "not_committed"

    migrated = RcaControlStore.migrate_v14_to_v15_and_activate(
        path,
        **_migration_apply_kwargs(contract),
    )

    assert migrated["epoch_id"] == contract["epoch_id"]
    with sqlite3.connect(path) as conn:
        marker = conn.execute(
            "SELECT value FROM control_meta WHERE key = 'schema_version'"
        ).fetchone()[0]
        observed = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'trigger' "
            "AND name = 'trg_terminal_rerun_delivery_authority_binding_guard'"
        ).fetchone()[0]
    normalized = RcaControlStore._normalized_schema_sql(observed)
    assert marker == CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION
    assert normalized == strict
    assert normalized != legacy


def test_known_legacy_v14_terminal_guard_extra_difference_is_prewrite_rejected(
    tmp_path,
):
    path = tmp_path / "control.sqlite3"
    control = _steady_control_store(path)
    predecessor = control.direct_steady_predecessor()
    assert predecessor is not None
    contract = _direct_steady_contract(predecessor=predecessor)
    _, legacy = _install_known_legacy_v14_terminal_binding_guard(path)
    changed = legacy.replace("; END", "; SELECT 1; END", 1)
    assert changed != legacy
    with sqlite3.connect(path) as conn:
        conn.execute(
            "DROP TRIGGER trg_terminal_rerun_delivery_authority_binding_guard"
        )
        conn.execute(changed)
    before = _sqlite_storage_identity(path)

    with pytest.raises(
        RuntimeError,
        match="incompatible_control_store_schema:"
        "terminal_rerun_authority_trigger_sql",
    ):
        RcaControlStore.migrate_v14_to_v15_and_activate(
            path,
            **_migration_apply_kwargs(contract),
        )

    _assert_sqlite_payload_unchanged(before, _sqlite_storage_identity(path))


def test_known_legacy_v14_terminal_guard_rollback_restores_legacy(
    tmp_path,
    monkeypatch,
):
    path = tmp_path / "control.sqlite3"
    control = _steady_control_store(path)
    predecessor = control.direct_steady_predecessor()
    assert predecessor is not None
    contract = _direct_steady_contract(predecessor=predecessor)
    _, legacy = _install_known_legacy_v14_terminal_binding_guard(path)

    def fail_after_epoch_swap(stage):
        if stage == "after_epoch_swap":
            raise RuntimeError("fault:after_epoch_swap")

    monkeypatch.setattr(
        RcaControlStore,
        "_v15_migration_fault",
        staticmethod(fail_after_epoch_swap),
    )
    with pytest.raises(RuntimeError, match="fault:after_epoch_swap"):
        RcaControlStore.migrate_v14_to_v15_and_activate(
            path,
            **_migration_apply_kwargs(contract),
        )

    with sqlite3.connect(path) as conn:
        marker = conn.execute(
            "SELECT value FROM control_meta WHERE key = 'schema_version'"
        ).fetchone()[0]
        observed = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'trigger' "
            "AND name = 'trg_terminal_rerun_delivery_authority_binding_guard'"
        ).fetchone()[0]
    assert marker == CONTROL_STORE_SCHEMA_VERSION
    assert RcaControlStore._normalized_schema_sql(observed) == legacy


def test_v15_rejects_known_legacy_terminal_binding_guard(tmp_path):
    path = tmp_path / "control.sqlite3"
    control = _steady_control_store(path)
    predecessor = control.direct_steady_predecessor()
    assert predecessor is not None
    contract = _direct_steady_contract(predecessor=predecessor)
    RcaControlStore.migrate_v14_to_v15_and_activate(
        path,
        **_migration_apply_kwargs(contract),
    )
    _install_known_legacy_v14_terminal_binding_guard(path)

    with pytest.raises(
        RuntimeError,
        match="incompatible_control_store_schema:"
        "terminal_rerun_authority_trigger_sql",
    ):
        RcaControlStore(
            path,
            require_current=True,
            read_only=True,
            allow_successor_read_only=True,
        )


@pytest.mark.parametrize(
    "stage",
    [
        "after_preflight",
        "after_trigger_drop",
        "after_audit_upgrade",
        "after_epoch_copy",
        "after_child_preflight",
        "after_epoch_swap",
        "after_successor_audit",
        "after_marker_cas",
        "before_commit",
    ],
)
def test_atomic_v14_to_v15_migration_rolls_back_every_fault_stage(
    tmp_path,
    monkeypatch,
    stage,
):
    path = tmp_path / f"control-{stage}.sqlite3"
    control = _steady_control_store(path)
    predecessor = control.direct_steady_predecessor()
    assert predecessor is not None
    contract = _direct_steady_contract(predecessor=predecessor)

    def fail_at(observed):
        if observed == stage:
            raise RuntimeError(f"fault:{stage}")

    monkeypatch.setattr(
        RcaControlStore,
        "_v15_migration_fault",
        staticmethod(fail_at),
    )

    with pytest.raises(RuntimeError, match=f"fault:{stage}"):
        RcaControlStore.migrate_v14_to_v15_and_activate(
            path,
            **_migration_apply_kwargs(contract),
        )

    assert RcaControlStore.probe_v14_to_v15_migration_outcome(
        path,
        **contract,
    ) == "not_committed"
    with sqlite3.connect(path) as conn:
        assert conn.execute(
            "SELECT value FROM control_meta WHERE key = 'schema_version'"
        ).fetchone()[0] == CONTROL_STORE_SCHEMA_VERSION
        assert conn.execute(
            "SELECT 1 FROM sqlite_master "
            "WHERE name = 'rca_activation_epochs_v15_new'"
        ).fetchone() is None
        assert [
            row[1]
            for row in conn.execute(
                "PRAGMA table_info(rca_activation_transition_audit)"
            ).fetchall()
        ] == list(control_store_module._V14_ACTIVATION_AUDIT_COLUMNS)


def test_atomic_v14_to_v15_commit_loss_converges_and_retry_is_idempotent(
    tmp_path,
    monkeypatch,
):
    path = tmp_path / "control.sqlite3"
    control = _steady_control_store(path)
    predecessor = control.direct_steady_predecessor()
    assert predecessor is not None
    contract = _direct_steady_contract(predecessor=predecessor)

    def commit_then_lose_ack(conn):
        conn.commit()
        raise sqlite3.OperationalError("commit acknowledgement lost")

    monkeypatch.setattr(
        RcaControlStore,
        "_commit_v15_migration_tx",
        staticmethod(commit_then_lose_ack),
    )

    migrated = RcaControlStore.migrate_v14_to_v15_and_activate(
        path,
        **_migration_apply_kwargs(contract),
    )

    assert migrated["epoch_id"] == contract["epoch_id"]
    assert RcaControlStore.probe_v14_to_v15_migration_outcome(
        path,
        **contract,
    ) == "committed"
    assert RcaControlStore.migrate_v14_to_v15_and_activate(
        path,
        **_migration_apply_kwargs(contract),
    ) == migrated


@pytest.mark.parametrize("partial", ["audit_column", "temporary_epoch_table"])
def test_v15_migration_outcome_probe_rejects_partial_layout(tmp_path, partial):
    path = tmp_path / "control.sqlite3"
    control = _steady_control_store(path)
    predecessor = control.direct_steady_predecessor()
    assert predecessor is not None
    contract = _direct_steady_contract(predecessor=predecessor)
    with sqlite3.connect(path) as conn:
        if partial == "audit_column":
            conn.execute(
                "ALTER TABLE rca_activation_transition_audit ADD COLUMN "
                "binding_schema_version TEXT"
            )
        else:
            conn.execute(control_store_module._V15_ACTIVATION_EPOCH_NEW_TABLE_SQL)

    assert RcaControlStore.probe_v14_to_v15_migration_outcome(
        path,
        **contract,
    ) == "unknown"


def test_atomic_v15_migration_does_not_invalidate_active_v14_reader(tmp_path):
    path = tmp_path / "control.sqlite3"
    control = _steady_control_store(path)
    predecessor = control.direct_steady_predecessor()
    assert predecessor is not None
    contract = _direct_steady_contract(predecessor=predecessor)
    reader = sqlite3.connect(path, isolation_level=None)
    try:
        assert reader.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
        reader.execute("BEGIN")
        assert reader.execute(
            "SELECT value FROM control_meta WHERE key = 'schema_version'"
        ).fetchone()[0] == CONTROL_STORE_SCHEMA_VERSION

        RcaControlStore.migrate_v14_to_v15_and_activate(
            path,
            **_migration_apply_kwargs(contract),
        )

        assert reader.execute(
            "SELECT value FROM control_meta WHERE key = 'schema_version'"
        ).fetchone()[0] == CONTROL_STORE_SCHEMA_VERSION
        assert RcaControlStore.probe_v14_to_v15_migration_outcome(
            path,
            **contract,
        ) == "committed"
    finally:
        if reader.in_transaction:
            reader.rollback()
        reader.close()


def test_atomic_v15_migration_validates_every_historical_parent_json(tmp_path):
    path = tmp_path / "control.sqlite3"
    control = _steady_control_store(path)
    predecessor = control.direct_steady_predecessor()
    assert predecessor is not None
    second = _direct_steady_contract(
        predecessor=predecessor,
        epoch_id="rca-v14-second",
        expected_schema=CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION,
        target_schema=CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION,
    )
    # Use the v14 API to create one historical parent before migration.
    control.activate_direct_steady_epoch(
        epoch_id=second["epoch_id"],
        release_fingerprint_sha256=second["release_fingerprint_sha256"],
        release_note_sha256=second["release_note_sha256"],
        config_sha256=second["config_sha256"],
        db_logical_identity=second["db_logical_identity"],
        partition_start_fence=second["partition_start_fence"],
        operator="control-v15-migration-test",
        reason="create historical v14 parent",
        expected_predecessor_epoch_id=predecessor["epoch_id"],
        expected_predecessor_state=predecessor["state"],
        expected_predecessor_binding_fingerprint=predecessor["binding_fingerprint"],
    )
    current = control.direct_steady_predecessor()
    assert current is not None
    contract = _direct_steady_contract(predecessor=current)
    with sqlite3.connect(path) as conn:
        conn.execute(
            "UPDATE rca_activation_epochs SET db_logical_identity_json = ? "
            "WHERE epoch_id = ?",
            ('{ "database": "control-test" }', predecessor["epoch_id"]),
        )

    with pytest.raises(
        ActivationEpochError,
        match="activation_v15_parent_projection_invalid",
    ):
        RcaControlStore.migrate_v14_to_v15_and_activate(
            path,
            **_migration_apply_kwargs(contract),
        )

    with sqlite3.connect(path) as conn:
        assert conn.execute(
            "SELECT value FROM control_meta WHERE key = 'schema_version'"
        ).fetchone()[0] == CONTROL_STORE_SCHEMA_VERSION


def test_v15_writer_direct_successor_and_outcome_are_idempotent(tmp_path):
    path = tmp_path / "control.sqlite3"
    v14 = _steady_control_store(path)
    v14_predecessor = v14.direct_steady_predecessor()
    assert v14_predecessor is not None
    migration = _direct_steady_contract(predecessor=v14_predecessor)
    RcaControlStore.migrate_v14_to_v15_and_activate(
        path,
        **_migration_apply_kwargs(migration),
    )
    writer = RcaControlStore(
        path,
        require_current=True,
        allow_successor_write=True,
    )
    predecessor = writer.direct_steady_predecessor()
    assert predecessor is not None
    successor = _direct_steady_contract(
        predecessor=predecessor,
        epoch_id="rca-v15-next",
        expected_schema=CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION,
        target_schema=CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION,
        release_fingerprint_sha256="6" * 64,
        release_note_sha256="7" * 64,
        config_sha256="8" * 64,
    )
    assert RcaControlStore.probe_direct_steady_activation_outcome(
        path,
        **successor,
    ) == "not_committed"

    activated = writer.activate_direct_steady_epoch(
        **successor,
        operator="control-v15-successor-test",
        reason="activate next exact v15 successor",
        now=datetime(2026, 8, 18, 1, tzinfo=timezone.utc),
    )

    assert activated["epoch_id"] == successor["epoch_id"]
    assert RcaControlStore.probe_direct_steady_activation_outcome(
        path,
        **successor,
    ) == "committed"
    assert writer.activate_direct_steady_epoch(
        **successor,
        operator="control-v15-successor-test",
        reason="activate next exact v15 successor",
    ) == activated
    with sqlite3.connect(path) as conn:
        conn.row_factory = sqlite3.Row
        retired = conn.execute(
            "SELECT * FROM rca_activation_epochs WHERE epoch_id = ?",
            (predecessor["epoch_id"],),
        ).fetchone()
        assert retired is not None
        assert RcaControlStore._v15_activation_transition_binding_tx(
            conn,
            epoch=retired,
        )["state"] == "retired"


def test_v15_connection_guard_rejects_schema_cookie_drift(tmp_path):
    path = tmp_path / "control.sqlite3"
    v14 = _steady_control_store(path)
    predecessor = v14.direct_steady_predecessor()
    assert predecessor is not None
    contract = _direct_steady_contract(predecessor=predecessor)
    RcaControlStore.migrate_v14_to_v15_and_activate(
        path,
        **_migration_apply_kwargs(contract),
    )
    writer = RcaControlStore(
        path,
        require_current=True,
        allow_successor_write=True,
    )
    writer.open_dispatcher_circuit(reason_code="v15_schema_cookie_test")
    guarded = writer._connect()
    try:
        with sqlite3.connect(path) as external:
            external.execute("CREATE TABLE unrelated_v15_schema_drift(id INTEGER)")
        with pytest.raises(
            sqlite3.IntegrityError,
            match="incompatible_control_store_schema:write_marker",
        ):
            guarded.execute(
                "UPDATE rca_dispatcher_circuit SET state = 'closed' "
                "WHERE circuit_name = 'submission'"
            )
    finally:
        guarded.close()


def test_v15_migration_contract_hash_mismatch_is_prewrite(tmp_path):
    path = tmp_path / "control.sqlite3"
    control = _steady_control_store(path)
    predecessor = control.direct_steady_predecessor()
    assert predecessor is not None
    contract = _direct_steady_contract(predecessor=predecessor)
    contract["epoch_contract_sha256"] = "f" * 64
    before = _sqlite_storage_identity(path)

    with pytest.raises(ActivationEpochError, match="activation_epoch_contract_invalid"):
        RcaControlStore.migrate_v14_to_v15_and_activate(
            path,
            **_migration_apply_kwargs(contract),
        )

    _assert_sqlite_payload_unchanged(before, _sqlite_storage_identity(path))


def test_v15_migration_partition_fence_exactly_cas_matches_progress(tmp_path):
    path = tmp_path / "control.sqlite3"
    control = _steady_control_store(path)
    record = _record(offset=25)
    control.persist_raw(record, policy=_policy())
    control.process_event(record.event_uid)
    assert control.partition_progress(topic=TOPIC, partitions=[2]) == {2: 26}
    predecessor = control.direct_steady_predecessor()
    assert predecessor is not None
    contract = _direct_steady_contract(
        predecessor=predecessor,
        partition_start_fence={TOPIC: {"2": 26}},
    )

    migrated = RcaControlStore.migrate_v14_to_v15_and_activate(
        path,
        **_migration_apply_kwargs(contract),
    )

    assert migrated["partition_start_fence_sha256"] == (
        control_store_module._canonical_sha256({TOPIC: {"2": 26}})
    )


def test_v15_migration_rejects_partition_progress_drift_before_ddl(tmp_path):
    path = tmp_path / "control.sqlite3"
    control = _steady_control_store(path)
    first = _record(offset=25)
    control.persist_raw(first, policy=_policy())
    control.process_event(first.event_uid)
    predecessor = control.direct_steady_predecessor()
    assert predecessor is not None
    contract = _direct_steady_contract(
        predecessor=predecessor,
        partition_start_fence={TOPIC: {"2": 26}},
    )
    assert RcaControlStore.probe_v14_to_v15_migration_outcome(
        path,
        **contract,
    ) == "not_committed"
    second = _record(offset=26, value=_value(updated_at=1783650000001))
    control.persist_raw(second, policy=_policy())
    control.process_event(second.event_uid)
    assert control.partition_progress(topic=TOPIC, partitions=[2]) == {2: 27}

    with pytest.raises(
        ActivationEpochError,
        match="activation_partition_fence_changed",
    ):
        RcaControlStore.migrate_v14_to_v15_and_activate(
            path,
            **_migration_apply_kwargs(contract),
        )

    assert RcaControlStore.probe_v14_to_v15_migration_outcome(
        path,
        **contract,
    ) == "unknown"
    with sqlite3.connect(path) as conn:
        assert conn.execute(
            "SELECT value FROM control_meta WHERE key = 'schema_version'"
        ).fetchone()[0] == CONTROL_STORE_SCHEMA_VERSION
        assert conn.execute(
            "SELECT 1 FROM sqlite_master "
            "WHERE name = 'rca_activation_epochs_v15_new'"
        ).fetchone() is None


def test_v15_migration_outcome_rejects_v14_preimage_with_inflight_work(tmp_path):
    path = tmp_path / "control.sqlite3"
    control = _steady_control_store(path)
    predecessor = control.direct_steady_predecessor()
    assert predecessor is not None
    contract = _direct_steady_contract(predecessor=predecessor)
    control.admit_manual_trigger(
        _manual_request("om-v15-outcome-inflight"),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
        active_policy=_policy(),
        activation_required=True,
    )
    observed = control.direct_steady_predecessor()
    assert observed is not None
    assert observed["inflight"]["total"] == 1

    assert RcaControlStore.probe_v14_to_v15_migration_outcome(
        path,
        **contract,
    ) == "unknown"


@pytest.mark.parametrize("weakened", ["cascade", "extra"])
def test_v15_migration_rejects_weakened_activation_child_fk_prewrite(
    tmp_path,
    weakened,
):
    path = tmp_path / "control.sqlite3"
    control = _steady_control_store(path)
    predecessor = control.direct_steady_predecessor()
    assert predecessor is not None
    contract = _direct_steady_contract(predecessor=predecessor)
    with sqlite3.connect(path) as conn:
        original = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' "
            "AND name = 'rca_activation_admission_ledger'"
        ).fetchone()[0]
        needle = (
            "FOREIGN KEY(epoch_id) REFERENCES rca_activation_epochs(epoch_id)"
        )
        replacement = (
            f"{needle} ON DELETE CASCADE"
            if weakened == "cascade"
            else f"{needle}, {needle}"
        )
        changed = original.replace(needle, replacement)
        assert changed != original
        conn.execute("PRAGMA writable_schema=ON")
        conn.execute(
            "UPDATE sqlite_master SET sql = ? WHERE type = 'table' "
            "AND name = 'rca_activation_admission_ledger'",
            (changed,),
        )
        schema_cookie = conn.execute("PRAGMA schema_version").fetchone()[0]
        conn.execute(f"PRAGMA schema_version={schema_cookie + 1}")
        conn.execute("PRAGMA writable_schema=OFF")

    with pytest.raises(
        RuntimeError,
        match="incompatible_control_store_schema:activation_foreign_keys",
    ):
        RcaControlStore.migrate_v14_to_v15_and_activate(
            path,
            **_migration_apply_kwargs(contract),
        )

    with sqlite3.connect(path) as conn:
        assert conn.execute(
            "SELECT value FROM control_meta WHERE key = 'schema_version'"
        ).fetchone()[0] == CONTROL_STORE_SCHEMA_VERSION
        assert conn.execute(
            "SELECT 1 FROM sqlite_master "
            "WHERE name = 'rca_activation_epochs_v15_new'"
        ).fetchone() is None


def test_committed_outcome_rejects_residual_v15_new_table(tmp_path):
    path = tmp_path / "control.sqlite3"
    control = _steady_control_store(path)
    predecessor = control.direct_steady_predecessor()
    assert predecessor is not None
    contract = _direct_steady_contract(predecessor=predecessor)
    RcaControlStore.migrate_v14_to_v15_and_activate(
        path,
        **_migration_apply_kwargs(contract),
    )
    with sqlite3.connect(path) as conn:
        conn.execute(control_store_module._V15_ACTIVATION_EPOCH_NEW_TABLE_SQL)

    assert RcaControlStore.probe_v14_to_v15_migration_outcome(
        path,
        **contract,
    ) == "unknown"


@pytest.mark.parametrize(
    ("digest_column", "constraint"),
    [
        (
            "release_fingerprint_sha256",
            "AND release_fingerprint_sha256 != printf('%064d', 0)",
        ),
        (
            "release_note_sha256",
            "AND release_note_sha256 != printf('%064d', 0)",
        ),
        (
            "config_sha256",
            "AND config_sha256 != printf('%064d', 0)",
        ),
        (
            "db_logical_identity_sha256",
            "AND db_logical_identity_sha256 != printf('%064d', 0)",
        ),
        (
            "partition_start_fence_sha256",
            "AND partition_start_fence_sha256 != printf('%064d', 0)",
        ),
    ],
)
def test_v15_required_digests_reject_zero_and_weakened_ddl(
    tmp_path,
    digest_column,
    constraint,
):
    path = tmp_path / "control.sqlite3"
    control = _steady_control_store(path)
    predecessor = control.direct_steady_predecessor()
    assert predecessor is not None
    contract = _direct_steady_contract(predecessor=predecessor)
    RcaControlStore.migrate_v14_to_v15_and_activate(
        path,
        **_migration_apply_kwargs(contract),
    )
    with sqlite3.connect(path) as conn:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                f"UPDATE rca_activation_epochs SET {digest_column} = ? "
                "WHERE is_current = 1",
                ("0" * 64,),
            )
        original = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' "
            "AND name = 'rca_activation_epochs'"
        ).fetchone()[0]
        weakened = original.replace(constraint, "")
        assert weakened != original
        conn.execute("PRAGMA writable_schema=ON")
        conn.execute(
            "UPDATE sqlite_master SET sql = ? WHERE type = 'table' "
            "AND name = 'rca_activation_epochs'",
            (weakened,),
        )
        schema_cookie = conn.execute("PRAGMA schema_version").fetchone()[0]
        conn.execute(f"PRAGMA schema_version={schema_cookie + 1}")
        conn.execute("PRAGMA writable_schema=OFF")

    with pytest.raises(
        RuntimeError,
        match="incompatible_control_store_schema:v15_activation_sql",
    ):
        RcaControlStore(
            path,
            require_current=True,
            allow_successor_write=True,
        )


@pytest.mark.parametrize(
    "digest_column",
    [
        "release_fingerprint_sha256",
        "release_note_sha256",
        "config_sha256",
        "db_logical_identity_sha256",
        "partition_start_fence_sha256",
    ],
)
def test_v15_application_validator_rejects_zero_historical_digest(
    tmp_path,
    digest_column,
):
    path = tmp_path / "control.sqlite3"
    control = _steady_control_store(path)
    predecessor = control.direct_steady_predecessor()
    assert predecessor is not None
    contract = _direct_steady_contract(predecessor=predecessor)
    RcaControlStore.migrate_v14_to_v15_and_activate(
        path,
        **_migration_apply_kwargs(contract),
    )
    with sqlite3.connect(path) as conn:
        conn.execute("PRAGMA ignore_check_constraints=ON")
        conn.execute(
            f"UPDATE rca_activation_epochs SET {digest_column} = ? "
            "WHERE epoch_id = ?",
            ("0" * 64, predecessor["epoch_id"]),
        )
        conn.execute("PRAGMA ignore_check_constraints=OFF")

    with pytest.raises(
        RuntimeError,
        match="incompatible_control_store_schema:v15_activation_state",
    ):
        RcaControlStore(
            path,
            require_current=True,
            allow_successor_write=True,
        )


def test_schema_snapshot_does_not_touch_live_source_wal_or_shm(tmp_path):
    path = tmp_path / "control.sqlite3"
    RcaControlStore(path)
    writer = sqlite3.connect(path)
    try:
        assert writer.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
        writer.execute("PRAGMA wal_autocheckpoint=0")
        writer.execute(
            "INSERT INTO control_meta(key, value) VALUES('snapshot_test', 'present')"
        )
        writer.commit()
        assert Path(f"{path}-wal").is_file()
        assert Path(f"{path}-shm").is_file()
        before = _sqlite_storage_identity(path)

        store = RcaControlStore(
            path,
            require_current=True,
            read_only=True,
            allow_successor_read_only=True,
        )

        assert store.schema_runtime_capability()["mode"] == "explicit_read_only"
        assert _sqlite_storage_identity(path) == before
    finally:
        writer.close()


def test_writable_v14_active_wal_does_not_use_raw_schema_snapshot(
    tmp_path,
    monkeypatch,
):
    path = tmp_path / "control.sqlite3"
    RcaControlStore(path)
    writer = sqlite3.connect(path)
    try:
        assert writer.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
        writer.execute("PRAGMA wal_autocheckpoint=0")
        writer.execute(
            "INSERT INTO control_meta(key, value) VALUES('active_wal', 'v14')"
        )
        writer.commit()
        before = _sqlite_storage_identity(path)

        def unexpected_snapshot(*_args, **_kwargs):
            raise AssertionError("v14 writable probe copied the control database")

        monkeypatch.setattr(
            RcaControlStore,
            "create_schema_probe_snapshot",
            unexpected_snapshot,
        )
        reopened = RcaControlStore(
            path,
            require_current=True,
            allow_successor_read_only=True,
        )

        assert reopened.schema_runtime_capability()["mode"] == "current_write"
        _assert_sqlite_payload_unchanged(before, _sqlite_storage_identity(path))
    finally:
        writer.close()


def test_writable_probe_with_wal_and_no_shm_uses_live_sqlite_coordination(
    tmp_path,
):
    source = tmp_path / "source.sqlite3"
    target = tmp_path / "target.sqlite3"
    RcaControlStore(source)
    writer = sqlite3.connect(source)
    snapshot = None
    try:
        assert writer.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
        writer.execute("PRAGMA wal_autocheckpoint=0")
        writer.execute(
            "INSERT INTO control_meta(key, value) VALUES('wal_only', 'present')"
        )
        writer.commit()
        shutil.copyfile(source, target)
        shutil.copyfile(Path(f"{source}-wal"), Path(f"{target}-wal"))
        assert not Path(f"{target}-shm").exists()
        before = _sqlite_storage_identity(target)

        version, snapshot = RcaControlStore.probe_writable_schema_source(target)

        assert version == CONTROL_STORE_SCHEMA_VERSION
        assert snapshot is None
        after = _sqlite_storage_identity(target)
        assert after["db"] == before["db"]
        assert after["-wal"] == before["-wal"]
        assert after["-shm"] is not None
    finally:
        if snapshot is not None:
            snapshot.close()
        writer.close()


def test_writable_probe_observes_v15_committed_only_in_active_wal(tmp_path):
    path = tmp_path / "control.sqlite3"
    control = RcaControlStore(path)
    _activate_direct_steady_epoch(control)
    reader = sqlite3.connect(path)
    snapshot = None
    try:
        assert reader.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
        reader.execute("BEGIN")
        assert reader.execute("SELECT COUNT(*) FROM control_meta").fetchone()
        _migrate_v14_fixture_to_v15(path)
        assert Path(f"{path}-wal").is_file()
        assert Path(f"{path}-shm").is_file()
        before = _sqlite_storage_identity(path)

        version, snapshot = RcaControlStore.probe_writable_schema_source(path)

        assert version == CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION
        assert snapshot is None
        _assert_sqlite_payload_unchanged(before, _sqlite_storage_identity(path))
    finally:
        if snapshot is not None:
            snapshot.close()
        if reader.in_transaction:
            reader.rollback()
        reader.close()


def test_writable_ctor_rejects_source_change_after_direct_validation(
    tmp_path,
    monkeypatch,
):
    path = tmp_path / "control.sqlite3"
    RcaControlStore(path)
    validate = RcaControlStore._validate_current_schema_read_only
    connect_calls = 0
    connect = RcaControlStore._connect

    def validate_then_change_source(self):
        validate(self)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "UPDATE control_meta SET value = ? WHERE key = 'schema_version'",
                (CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION,),
            )

    def counted_connect(self):
        nonlocal connect_calls
        connect_calls += 1
        return connect(self)

    monkeypatch.setattr(
        RcaControlStore,
        "_validate_current_schema_read_only",
        validate_then_change_source,
    )
    monkeypatch.setattr(RcaControlStore, "_connect", counted_connect)

    with pytest.raises(
        RuntimeError, match="incompatible_control_store_schema:write_marker"
    ):
        RcaControlStore(
            path,
            require_current=True,
            allow_successor_read_only=True,
        )

    assert connect_calls == 0


def test_writable_store_rejects_post_ctor_schema_drift_before_business_sql(
    tmp_path,
    monkeypatch,
):
    path = tmp_path / "control.sqlite3"
    store = RcaControlStore(path)
    with sqlite3.connect(path) as conn:
        conn.execute(
            "UPDATE control_meta SET value = ? WHERE key = 'schema_version'",
            (CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION,),
        )
        before_epochs = int(
            conn.execute("SELECT COUNT(*) FROM rca_activation_epochs").fetchone()[0]
        )

    statements: list[str] = []
    real_connect = sqlite3.connect

    def recording_connect(*args, **kwargs):
        conn = real_connect(*args, **kwargs)
        conn.set_trace_callback(statements.append)
        return conn

    monkeypatch.setattr(control_store_module.sqlite3, "connect", recording_connect)

    with pytest.raises(
        RuntimeError,
        match="incompatible_control_store_schema:write_marker",
    ):
        _activate_direct_steady_epoch(store, epoch_id="drifted-writer")

    with real_connect(path) as conn:
        assert (
            int(
                conn.execute("SELECT COUNT(*) FROM rca_activation_epochs").fetchone()[0]
            )
            == before_epochs
        )
    forbidden_prefixes = (
        "begin",
        "insert",
        "update",
        "delete",
        "pragma journal_mode",
        "pragma synchronous",
    )
    assert not any(
        statement.strip().lower().startswith(forbidden_prefixes)
        for statement in statements
    )


def test_v14_connection_guards_block_marker_and_midflight_v15_dml(
    tmp_path,
):
    path = tmp_path / "control.sqlite3"
    store = _steady_control_store(path)
    store.open_dispatcher_circuit(reason_code="test_midflight_cutover")
    guarded = store._connect()
    try:
        with pytest.raises(
            sqlite3.IntegrityError,
            match="incompatible_control_store_schema:write_marker",
        ):
            guarded.execute(
                "UPDATE control_meta SET value = ? WHERE key = 'schema_version'",
                (CONTROL_STORE_SCHEMA_SUCCESSOR_VERSION,),
            )
        with sqlite3.connect(path) as conn:
            assert conn.execute(
                "SELECT value FROM control_meta WHERE key = 'schema_version'"
            ).fetchone()[0] == CONTROL_STORE_SCHEMA_VERSION

        migration = _migrate_v14_fixture_to_v15(path)
        for statement, parameters in (
            (
                "UPDATE rca_dispatcher_circuit SET state = 'closed' "
                "WHERE circuit_name = 'submission'",
                (),
            ),
            (
                "UPDATE rca_activation_epochs SET updated_at = updated_at "
                "WHERE epoch_id = ?",
                (migration["successor_epoch_id"],),
            ),
        ):
            with pytest.raises(
                sqlite3.IntegrityError,
                match="incompatible_control_store_schema:write_marker",
            ):
                guarded.execute(statement, parameters)
    finally:
        guarded.close()

    with sqlite3.connect(path) as conn:
        assert conn.execute(
            "SELECT state FROM rca_dispatcher_circuit "
            "WHERE circuit_name = 'submission'"
        ).fetchone()[0] == "open"


def test_v14_require_current_connection_guard_rejects_schema_cookie_drift(
    tmp_path,
):
    path = tmp_path / "control.sqlite3"
    seed = _steady_control_store(path)
    seed.open_dispatcher_circuit(reason_code="test_schema_cookie_drift")
    guarded_store = RcaControlStore(path, require_current=True)
    guarded = guarded_store._connect()
    try:
        with sqlite3.connect(path) as external:
            external.execute("CREATE TABLE unrelated_schema_drift(id INTEGER)")

        with pytest.raises(
            sqlite3.IntegrityError,
            match="incompatible_control_store_schema:write_marker",
        ):
            guarded.execute(
                "UPDATE rca_dispatcher_circuit SET state = 'closed' "
                "WHERE circuit_name = 'submission'"
            )
    finally:
        guarded.close()

    with sqlite3.connect(path) as conn:
        assert conn.execute(
            "SELECT value FROM control_meta WHERE key = 'schema_version'"
        ).fetchone()[0] == CONTROL_STORE_SCHEMA_VERSION
        assert conn.execute(
            "SELECT state FROM rca_dispatcher_circuit "
            "WHERE circuit_name = 'submission'"
        ).fetchone()[0] == "open"


def test_v14_public_writer_is_fenced_when_v15_commits_after_connect(
    tmp_path,
    monkeypatch,
):
    path = tmp_path / "control.sqlite3"
    store = _steady_control_store(path)
    store.open_dispatcher_circuit(reason_code="test_midflight_public_writer")
    original_connect = store._connect
    migrated = False

    def connect_then_migrate():
        nonlocal migrated
        conn = original_connect()
        if not migrated:
            _migrate_v14_fixture_to_v15(path)
            migrated = True
        return conn

    monkeypatch.setattr(store, "_connect", connect_then_migrate)

    with pytest.raises(
        sqlite3.IntegrityError,
        match="incompatible_control_store_schema:write_marker",
    ):
        store.close_dispatcher_circuit()

    with sqlite3.connect(path) as conn:
        assert conn.execute(
            "SELECT state FROM rca_dispatcher_circuit "
            "WHERE circuit_name = 'submission'"
        ).fetchone()[0] == "open"


def test_kafka_runtime_transition_is_atomic_and_duplicate_preserves_first_identity(
    tmp_path,
):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    first_identity = _resident_identity(script="/candidate/scripts/消费者.py")

    accepted = store.ingest_record(
        _record(),
        policy=_policy(),
        submit_enabled=True,
        runtime_identity=first_identity,
    )
    duplicate = store.ingest_record(
        _record(),
        policy=_policy(),
        submit_enabled=True,
        runtime_identity=_resident_identity(pid=43000),
    )

    assert accepted.decision == "accepted"
    assert duplicate.transport_duplicate is True
    [transition] = store.list_rows("rca_host_runtime_transitions")
    assert transition["entity_key"] == accepted.event_uid
    assert transition["runtime_identity_sha256"] == canonical_json_sha256(
        first_identity
    )
    assert json.loads(transition["runtime_identity_json"])["pid"] == 42000
    [inbox] = store.list_rows("kafka_inbox")
    assert transition["transitioned_at"] == inbox["processed_at"]


def test_host_runtime_transition_rejects_wrong_service_kind_pair(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    accepted = store.ingest_record(_record(), policy=_policy(), submit_enabled=True)

    with pytest.raises(ValueError, match="transition identity is invalid"):
        store.record_host_runtime_transition(
            submission_key=accepted.submission_key,
            business_key=accepted.business_key,
            generation=accepted.generation,
            service_label="local.pnc.rca-kafka-consumer",
            transition_kind="effect_succeeded",
            entity_key=accepted.event_uid,
            runtime_identity=_resident_identity(),
        )

    assert store.list_rows("rca_host_runtime_transitions") == []


def test_v7_store_without_runtime_transition_table_migrates_to_v9(tmp_path):
    path = tmp_path / "control.sqlite3"
    store = RcaControlStore(path)
    conn = sqlite3.connect(path, isolation_level=None)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("DROP TABLE rca_host_runtime_transitions")
        conn.execute(
            "UPDATE control_meta SET value='pnc_rca_control_store_v7' "
            "WHERE key='schema_version'"
        )
    finally:
        conn.close()

    upgraded = RcaControlStore(path)

    assert upgraded.health()["schema_version"] == CONTROL_STORE_SCHEMA_VERSION
    conn = upgraded._connect()
    try:
        assert conn.execute(
            "SELECT 1 FROM sqlite_master "
            "WHERE type='table' AND name='rca_host_runtime_transitions'"
        ).fetchone() is not None
    finally:
        conn.close()


def test_v8_store_migrates_forward_to_durable_activation_schema(tmp_path):
    path = tmp_path / "control.sqlite3"
    store = RcaControlStore(path)
    conn = sqlite3.connect(path, isolation_level=None)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("DROP TABLE rca_activation_transition_audit")
        conn.execute("DROP TABLE rca_activation_admission_ledger")
        conn.execute("DROP TABLE rca_activation_epochs")
        conn.execute(
            "UPDATE control_meta SET value='pnc_rca_control_store_v8' "
            "WHERE key='schema_version'"
        )
    finally:
        conn.close()

    upgraded = RcaControlStore(path)

    assert upgraded.health()["schema_version"] == CONTROL_STORE_SCHEMA_VERSION
    assert upgraded.activation_epoch() is None
    assert upgraded.list_rows("rca_activation_admission_ledger") == []
    conn = upgraded._connect()
    try:
        inbox_columns = {
            row["name"] for row in conn.execute("PRAGMA table_info(kafka_inbox)")
        }
        outbox_columns = {
            row["name"] for row in conn.execute("PRAGMA table_info(rca_outbox)")
        }
    finally:
        conn.close()
    assert {
        "activation_epoch_id",
        "activation_ingress_state",
        "activation_required",
        "activation_source_identity_sha256",
    }.issubset(inbox_columns)
    assert "activation_slot_kind" not in inbox_columns
    assert {"activation_epoch_id", "activation_ledger_id"}.issubset(
        outbox_columns
    )


def test_direct_steady_is_idempotent_and_rejects_binding_conflicts(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    first = _activate_direct_steady_epoch(store)
    second = _activate_direct_steady_epoch(store)
    assert second == first
    assert first["release_fingerprint_sha256"] == "1" * 64
    assert first["release_note_sha256"] == "a" * 64
    assert first["partition_start_fence_sha256"] == first[
        "partition_end_fence_sha256"
    ]
    assert not {
        "preauthorization_fingerprint",
        "preproduction_fingerprint",
        "production_fingerprint",
        "production_gate_receipt_sha256",
    } & first.keys()
    assert len(store.list_rows("rca_activation_transition_audit")) == 1

    with pytest.raises(
        ActivationEpochError, match="activation_direct_steady_binding_conflict"
    ):
        _activate_direct_steady_epoch(store, config_sha256="9" * 64)


def test_activation_epoch_rejects_hidden_v14_binding_tamper(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    _activate_direct_steady_epoch(store)
    _tamper_hidden_v14_binding(store)

    with pytest.raises(
        ActivationEpochError,
        match="activation_predecessor_binding_invalid",
    ):
        store.activation_epoch()


def test_control_health_rejects_hidden_v14_binding_tamper(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    _activate_direct_steady_epoch(store)
    _tamper_hidden_v14_binding(store)

    health = store.health()
    assert health["ok"] is False
    assert health["activation"]["configured"] is True
    assert health["activation"]["binding_valid"] is False
    assert health["activation"]["production_active"] is False
    assert health["activation"]["current_epoch"] is None


def test_admission_rejects_hidden_v14_binding_tamper(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    _tamper_hidden_v14_binding(store)

    with pytest.raises(
        ActivationEpochError,
        match="activation_predecessor_binding_invalid",
    ):
        store.ingest_record(
            _record(),
            policy=_policy(),
            submit_enabled=True,
            activation_required=True,
        )
    assert store.list_rows("kafka_inbox") == []
    assert store.list_rows("rca_activation_admission_ledger") == []


def test_claim_preview_and_fence_reject_hidden_v14_binding_tamper(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    result = store.ingest_record(
        _record(),
        policy=_policy(),
        submit_enabled=True,
        activation_required=True,
    )
    assert result.decision == "accepted"
    _tamper_hidden_v14_binding(store)

    for operation in (
        lambda: store.preview_dispatchable(now=datetime.now(timezone.utc)),
        lambda: store.claim_outbox(
            lease_owner="audit-tamper-test",
            now=datetime.now(timezone.utc),
        ),
        lambda: store.activation_partition_start_fence(
            topic=TOPIC,
            partitions=[2],
        ),
    ):
        with pytest.raises(
            ActivationEpochError,
            match="activation_predecessor_binding_invalid",
        ):
            operation()
    [outbox] = store.list_rows("rca_outbox")
    assert outbox["status"] == "pending"


def test_fresh_v14_activation_sqlite_master_hashes_remain_stable(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    expected = {
        "idx_rca_single_current_activation_epoch": (
            "0a1f9aa726d2b8320cbd91abc8edb827d257967df6f685e38721548afdde26c7"
        ),
        "rca_activation_epochs": (
            "a1ced1ccf76bce5c3f635151776a7dc51ffda0bc543d446808414461b64262ca"
        ),
    }
    with sqlite3.connect(store.db_path) as conn:
        rows = conn.execute(
            "SELECT name, sql FROM sqlite_master "
            "WHERE name IN (?, ?) ORDER BY name",
            tuple(sorted(expected)),
        ).fetchall()

    assert {
        name: hashlib.sha256(sql.encode()).hexdigest() for name, sql in rows
    } == expected


def test_direct_steady_allows_empty_fence_only_for_kafka_disabled_release(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")

    epoch = store.activate_direct_steady_epoch(
        epoch_id="rca-direct-kafka-disabled-20260712",
        release_fingerprint_sha256="1" * 64,
        release_note_sha256="a" * 64,
        config_sha256="2" * 64,
        db_logical_identity={"database": "control-test"},
        partition_start_fence={},
        operator="release-test",
        reason="activate direct steady release with Kafka disabled",
    )

    assert epoch["partition_start_fence_sha256"] == canonical_json_sha256({})
    with pytest.raises(
        ActivationEpochError,
        match="activation_partition_start_fence_missing",
    ):
        store.activation_partition_start_fence(topic=TOPIC, partitions=[2])


def test_direct_steady_atomically_replaces_exact_drained_steady_predecessor(
    tmp_path,
):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    predecessor = _activate_direct_steady_epoch(
        store,
        epoch_id="rca-direct-first-20260712",
    )
    predecessor_binding = store.direct_steady_predecessor()
    assert predecessor_binding is not None
    assert predecessor_binding["epoch_id"] == predecessor["epoch_id"]
    assert predecessor_binding["inflight"]["total"] == 0

    successor = _activate_direct_steady_epoch(
        store,
        epoch_id="rca-direct-second-20260712",
        start_offset=30,
        expected_predecessor=predecessor_binding,
    )

    assert successor["state"] == "steady_active"
    assert store.activation_epoch() == successor
    [retired] = [
        row
        for row in store.list_rows("rca_activation_epochs")
        if row["epoch_id"] == predecessor["epoch_id"]
    ]
    assert retired["is_current"] == 0
    assert retired["state"] == "steady_active"
    assert retired["superseded_at"]


@pytest.mark.parametrize("outbox_status", ["pending", "claimed", "completed"])
def test_direct_steady_rejects_current_predecessor_inflight(
    tmp_path,
    outbox_status,
):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    predecessor = _activate_direct_steady_epoch(
        store,
        epoch_id="rca-direct-first-20260712",
    )
    admitted = store.admit_manual_trigger(
        _manual_request("om-direct-current-inflight"),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
        active_policy=_policy(),
        activation_required=True,
    )
    if outbox_status != "pending":
        with sqlite3.connect(store.db_path) as connection:
            connection.execute(
                "UPDATE rca_outbox SET status = ? WHERE submission_key = ?",
                (outbox_status, admitted.submission_key),
            )
    predecessor_binding = store.direct_steady_predecessor()
    assert predecessor_binding is not None
    assert predecessor_binding["inflight"]["total"] == 1

    with pytest.raises(
        ActivationEpochError,
        match="activation_predecessor_inflight_not_drained",
    ):
        _activate_direct_steady_epoch(
            store,
            epoch_id="rca-direct-second-20260712",
            start_offset=30,
            expected_predecessor=predecessor_binding,
        )
    assert store.activation_epoch() == predecessor


def test_direct_steady_counts_current_pending_inbox_and_ignores_old_rows(
    tmp_path,
):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    predecessor = _activate_direct_steady_epoch(
        store,
        epoch_id="rca-direct-first-20260712",
    )
    store.persist_raw(
        _record(offset=99),
        policy=_policy(),
        submit_enabled=True,
        activation_required=True,
    )
    current = store.direct_steady_predecessor()
    assert current is not None
    assert current["inflight"] == {
        "dispatchable_outbox": 0,
        "execution_delivery": 0,
        "pending_inbox": 1,
        "total": 1,
    }

    # A pending row carrying an old epoch must not become a successor gate.
    conn = store._connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "UPDATE kafka_inbox SET activation_epoch_id = ?, decision = 'pending'",
            ("rca-direct-old-20260711",),
        )
        conn.commit()
    finally:
        conn.close()
    observed = store.direct_steady_predecessor()
    assert observed is not None
    assert observed["inflight"] == {
        "dispatchable_outbox": 0,
        "execution_delivery": 0,
        "pending_inbox": 0,
        "total": 0,
    }
    successor = _activate_direct_steady_epoch(
        store,
        epoch_id="rca-direct-second-20260712",
        start_offset=30,
        expected_predecessor=observed,
    )
    assert successor["state"] == "steady_active"
    assert store.activation_epoch() == successor
    assert predecessor["epoch_id"] != successor["epoch_id"]


def test_direct_steady_successor_rejects_binding_drift_without_mutation(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    predecessor = _activate_direct_steady_epoch(
        store,
        epoch_id="rca-direct-first-20260712",
    )
    predecessor_binding = store.direct_steady_predecessor()
    assert predecessor_binding is not None
    drifted = {**predecessor_binding, "binding_fingerprint": "f" * 64}

    with pytest.raises(
        ActivationEpochError,
        match="activation_predecessor_binding_changed",
    ):
        _activate_direct_steady_epoch(
            store,
            epoch_id="rca-direct-second-20260712",
            start_offset=30,
            expected_predecessor=drifted,
        )
    assert store.activation_epoch() == predecessor


def test_activation_required_without_epoch_blocks_before_persist_or_commit(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")

    with pytest.raises(
        ActivationEpochError, match="activation_steady_epoch_required"
    ):
        store.ingest_record(
            _record(offset=20),
            policy=_policy(),
            submit_enabled=True,
            activation_required=True,
        )

    assert store.list_rows("kafka_inbox") == []
    assert store.list_rows("rca_outbox") == []
    assert store.claim_outbox(lease_owner="required-dispatcher") is None


def test_outbox_claim_and_preview_can_be_bound_to_one_submission_key(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    first = store.ingest_record(
        _record(20, value=_value(work_item_id=7041712820)),
        policy=_policy(),
        submit_enabled=True,
    )
    second = store.ingest_record(
        _record(21, value=_value(work_item_id=7041712821)),
        policy=_policy(),
        submit_enabled=True,
    )
    assert first.decision == "accepted"
    assert second.decision == "accepted"

    preview = store.preview_dispatchable(submission_key=second.submission_key)
    assert [row["submission_key"] for row in preview] == [second.submission_key]

    claim = store.claim_outbox(
        lease_owner="targeted-canary",
        submission_key=second.submission_key,
    )
    assert claim is not None
    assert claim.submission_key == second.submission_key
    rows = {row["submission_key"]: row for row in store.list_rows("rca_outbox")}
    assert rows[first.submission_key]["status"] == "pending"
    assert rows[second.submission_key]["status"] == "claimed"


def _register_policy_without_classifying(store: RcaControlStore) -> None:
    store.persist_raw(_record(), policy=_policy(), submit_enabled=True)


def _quarantine_for_input_wait(
    store: RcaControlStore,
    *,
    error_code: str = "issue_field_missing_remote_data_reference",
    submission_key: str = "",
) -> tuple[int, datetime]:
    rows = store.list_rows("rca_outbox")
    row = (
        next(item for item in rows if item["submission_key"] == submission_key)
        if submission_key
        else rows[0]
    )
    window_started = datetime.fromisoformat(row["retry_window_started_at"])
    claim = store.claim_outbox(
        lease_owner="input-wait-test",
        lease_seconds=180,
        now=window_started + timedelta(seconds=1),
    )
    assert claim is not None
    if submission_key:
        assert claim.submission_key == submission_key
    mutation = store.retry_outbox(
        outbox_id=claim.outbox_id,
        lease_token=claim.lease_token,
        error_code=error_code,
        error_detail="redacted test detail",
        delay_seconds=2,
        max_age_seconds=60,
        now=window_started + timedelta(seconds=61),
    )
    assert mutation.status == "pending"
    first_failure_at = window_started + timedelta(seconds=61)
    claim = store.claim_outbox(
        lease_owner="input-wait-test",
        lease_seconds=180,
        now=first_failure_at + timedelta(seconds=59),
    )
    assert claim is not None
    mutation = store.retry_outbox(
        outbox_id=claim.outbox_id,
        lease_token=claim.lease_token,
        error_code=error_code,
        error_detail="redacted test detail",
        delay_seconds=2,
        max_age_seconds=60,
        now=first_failure_at + timedelta(seconds=61),
    )
    assert mutation.status == "quarantined"
    return claim.outbox_id, window_started


def test_input_wait_retry_window_starts_at_first_failure_not_queue_admission(
    tmp_path,
):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    _register_policy_without_classifying(store)
    store.admit_manual_trigger(
        _manual_request("om_retry_window"),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
    )
    [before] = store.list_rows("rca_outbox")
    admitted_at = datetime.fromisoformat(before["created_at"])
    first_failure_at = admitted_at + timedelta(hours=6)
    claim = store.claim_outbox(
        lease_owner="late-first-failure",
        lease_seconds=180,
        now=first_failure_at,
    )
    assert claim is not None

    first = store.retry_outbox(
        outbox_id=claim.outbox_id,
        lease_token=claim.lease_token,
        error_code="host_issue_preread_failed",
        error_detail="transient preread failure",
        delay_seconds=2,
        max_age_seconds=900,
        now=first_failure_at,
    )

    assert first.status == "pending"
    [after_first] = store.list_rows("rca_outbox")
    assert after_first["retry_window_started_at"] == first_failure_at.isoformat()
    assert after_first["next_attempt_at"] == (
        first_failure_at + timedelta(seconds=2)
    ).isoformat()

    claim = store.claim_outbox(
        lease_owner="expired-failure-window",
        lease_seconds=180,
        now=first_failure_at + timedelta(seconds=899),
    )
    assert claim is not None
    expired = store.retry_outbox(
        outbox_id=claim.outbox_id,
        lease_token=claim.lease_token,
        error_code="host_issue_preread_failed",
        error_detail="persistent preread failure",
        delay_seconds=2,
        max_age_seconds=900,
        now=first_failure_at + timedelta(seconds=901),
    )

    assert expired.status == "quarantined"
    [after_expiry] = store.list_rows("rca_outbox")
    assert after_expiry["retry_window_started_at"] == first_failure_at.isoformat()


def test_input_wait_retry_window_restarts_when_entering_from_general_failure(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    _register_policy_without_classifying(store)
    store.admit_manual_trigger(
        _manual_request("om_retry_window_transition"),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
    )
    [before] = store.list_rows("rca_outbox")
    first_failure_at = datetime.fromisoformat(before["created_at"]) + timedelta(hours=1)
    claim = store.claim_outbox(
        lease_owner="general-failure",
        lease_seconds=180,
        now=first_failure_at,
    )
    assert claim is not None
    general = store.retry_outbox(
        outbox_id=claim.outbox_id,
        lease_token=claim.lease_token,
        error_code="storage_admission_blocked",
        delay_seconds=1,
        max_age_seconds=86_400,
        now=first_failure_at,
    )
    assert general.status == "pending"
    [after_general] = store.list_rows("rca_outbox")
    assert after_general["retry_window_started_at"] == before["created_at"]

    first_preread_failure_at = first_failure_at + timedelta(seconds=901)
    claim = store.claim_outbox(
        lease_owner="first-preread-failure",
        lease_seconds=180,
        now=first_preread_failure_at,
    )
    assert claim is not None
    preread = store.retry_outbox(
        outbox_id=claim.outbox_id,
        lease_token=claim.lease_token,
        error_code="host_issue_preread_failed",
        delay_seconds=1,
        max_age_seconds=900,
        now=first_preread_failure_at,
    )

    assert preread.status == "pending"
    [after] = store.list_rows("rca_outbox")
    assert after["retry_window_started_at"] == first_preread_failure_at.isoformat()


def _settle_delivery(store: RcaControlStore, submission_key: str) -> None:
    delivery = RcaDeliveryStore(store.db_path)
    delivery.materialize_pending_subscriptions()
    settled_at = datetime.now(timezone.utc).isoformat()
    conn = delivery._connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "UPDATE rca_delivery_effects SET status='succeeded', "
            "completed_at=COALESCE(completed_at, ?), updated_at=?",
            (settled_at, settled_at),
        )
        job = conn.execute(
            "SELECT delivery_id FROM rca_delivery_jobs WHERE submission_key=?",
            (submission_key,),
        ).fetchone()
        assert job is not None
        delivery._aggregate_job_status(
            conn,
            str(job["delivery_id"]),
            datetime.now(timezone.utc).isoformat(),
        )
        conn.commit()
    finally:
        conn.close()


def _terminalize_permanent(store: RcaControlStore, submission_key: str) -> None:
    current = datetime.now(timezone.utc).isoformat()
    conn = store._connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            """
            UPDATE rca_outbox SET status='quarantined', quarantined_at=?,
                   last_error_code='permanent_failure'
             WHERE submission_key=?
            """,
            (current, submission_key),
        )
        conn.execute(
            """
            UPDATE business_triggers SET state='quarantined'
             WHERE submission_key=?
            """,
            (submission_key,),
        )
        conn.commit()
    finally:
        conn.close()
    delivery = RcaDeliveryStore(store.db_path)
    assert delivery.backfill_completed_submissions() >= 1
    conn = delivery._connect()
    try:
        watch = conn.execute(
            "SELECT state, delivery_id FROM rca_execution_watch "
            "WHERE submission_key=?",
            (submission_key,),
        ).fetchone()
        assert watch is not None
        if watch["delivery_id"] is None:
            assert watch["state"] == "quarantined"
            assert conn.execute(
                "SELECT 1 FROM rca_delivery_jobs WHERE submission_key=?",
                (submission_key,),
            ).fetchone() is None
            return
    finally:
        conn.close()
    _settle_delivery(store, submission_key)


def _convert_silent_terminal_to_taxonomy_gap(
    store: RcaControlStore,
    terminal,
    status: dict,
) -> dict:
    converted = json.loads(json.dumps(status))
    error_code = "taxonomy_gap:submission_issue_title_missing"
    taxonomy = converted["failure_taxonomy"]
    taxonomy.update(
        {
            "external_comment_policy": "honest_non_attribution_only",
            "known": False,
            "observed_state": "pending",
            "raw_code": "submission_issue_title_missing",
            "resumed_route_key": taxonomy["durable_route"]["route_key"],
            "source": "durable_failure_route_deadline",
            "source_conflict": False,
            "terminal_error_code": error_code,
        }
    )
    route_key = taxonomy["durable_route"]["route_key"]
    conn = store._connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            """
            UPDATE rca_execution_watch
               SET last_error_code = ?, last_status_json = ?
             WHERE submission_key = ?
            """,
            (
                error_code,
                control_store_module._canonical_json(converted),
                terminal.submission_key,
            ),
        )
        conn.execute(
            "UPDATE rca_failure_routes SET terminal_error_code = ? "
            "WHERE route_key = ?",
            (error_code, route_key),
        )
        conn.execute(
            "DELETE FROM rca_delivery_subscription_events "
            "WHERE subscription_key IN ("
            "SELECT subscription_key FROM rca_delivery_subscriptions "
            "WHERE business_key = ? AND generation = ? "
            "AND effect_kind = 'feishu_thread_reply'"
            ")",
            (terminal.business_key, terminal.generation),
        )
        conn.execute(
            "DELETE FROM rca_trigger_delivery_bindings "
            "WHERE subscription_key IN ("
            "SELECT subscription_key FROM rca_delivery_subscriptions "
            "WHERE business_key = ? AND generation = ? "
            "AND effect_kind = 'feishu_thread_reply'"
            ")",
            (terminal.business_key, terminal.generation),
        )
        conn.execute(
            "DELETE FROM rca_delivery_subscriptions "
            "WHERE business_key = ? AND generation = ? "
            "AND effect_kind = 'feishu_thread_reply'",
            (terminal.business_key, terminal.generation),
        )
        conn.commit()
    finally:
        conn.close()
    return converted


def _terminalize_input_wait(
    store: RcaControlStore, submission_key: str, *, settle: bool = True
) -> None:
    _quarantine_for_input_wait(store, submission_key=submission_key)
    delivery = RcaDeliveryStore(store.db_path)
    assert delivery.backfill_completed_submissions() == 1
    if settle:
        _settle_delivery(store, submission_key)


def _inject_conflicting_issue_scope_chain(
    store: RcaControlStore, *, policy_version: str = "issue-created-v2"
) -> None:
    admission = build_rca_admission(
        project_key="project-key",
        project_simple_name="g1q3",
        work_item_type_key="problem-type",
        work_item_id="7041712812",
        rule_version=policy_version,
    )
    current = datetime.now(timezone.utc).isoformat()
    conn = store._connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            """
            INSERT INTO business_triggers(
                business_key, generation, submission_key, creation_rule_version,
                work_item_id, project_key, work_item_type_key, normalized_json,
                state, created_at
            ) VALUES (?, 1, ?, ?, '7041712812', 'project-key',
                      'problem-type', '{}', 'pending', ?)
            """,
            (
                admission.business_key,
                admission.submission_key,
                policy_version,
                current,
            ),
        )
        conn.execute(
            """
            INSERT INTO rca_outbox(
                action, business_key, submission_key, creation_rule_version,
                generation, payload_json, status, retry_window_started_at,
                created_at, updated_at
            ) VALUES ('submit_rca_issue_intake', ?, ?, ?, 1, '{}', 'pending',
                      ?, ?, ?)
            """,
            (
                admission.business_key,
                admission.submission_key,
                policy_version,
                current,
                current,
                current,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def test_store_uses_wal_full_and_foreign_keys(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    assert store.journal_settings() == {
        "journal_mode": "wal",
        "synchronous": 2,
        "foreign_keys": 1,
    }
    health = store.health()
    assert health["db_size_bytes"] >= health["db_file_sizes"]["main"] > 0
    assert set(health["db_file_sizes"]) == {"main", "wal", "shm"}
    assert health["db_size_error"] == ""
    assert isinstance(health["filesystem"]["available_bytes"], int)
    assert health["filesystem"]["error"] == ""


def test_legacy_outbox_without_foreign_keys_is_not_labeled_current(tmp_path):
    path = tmp_path / "control.sqlite3"
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE rca_outbox (
            outbox_id INTEGER PRIMARY KEY AUTOINCREMENT,
            action TEXT NOT NULL,
            business_key TEXT NOT NULL,
            submission_key TEXT NOT NULL UNIQUE,
            creation_rule_version TEXT NOT NULL,
            generation INTEGER NOT NULL,
            source_event_id TEXT NOT NULL,
            source_topic TEXT NOT NULL,
            source_partition INTEGER NOT NULL,
            source_offset INTEGER NOT NULL,
            payload_json TEXT NOT NULL,
            status TEXT NOT NULL,
            attempt INTEGER NOT NULL DEFAULT 0,
            next_attempt_at TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.close()

    with pytest.raises(RuntimeError, match="incompatible_control_store_schema"):
        RcaControlStore(path)

    conn = sqlite3.connect(path)
    try:
        control_meta = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'control_meta'"
        ).fetchone()
    finally:
        conn.close()
    assert control_meta is None


def test_future_control_store_schema_version_is_rejected(tmp_path):
    path = tmp_path / "control.sqlite3"
    conn = sqlite3.connect(path)
    try:
        conn.execute("CREATE TABLE control_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        conn.execute(
            "INSERT INTO control_meta VALUES ('schema_version', 'pnc_rca_control_store_v999')"
        )
        conn.execute("CREATE TABLE future_only (id INTEGER PRIMARY KEY, payload TEXT)")
        conn.commit()
        before = conn.execute(
            "SELECT name, sql FROM sqlite_master ORDER BY name"
        ).fetchall()
    finally:
        conn.close()

    with pytest.raises(RuntimeError, match="incompatible_control_store_schema:version"):
        RcaControlStore(path)

    conn = sqlite3.connect(path)
    try:
        after = conn.execute(
            "SELECT name, sql FROM sqlite_master ORDER BY name"
        ).fetchall()
    finally:
        conn.close()
    assert after == before
    assert {row[0] for row in after} == {"control_meta", "future_only", "sqlite_autoindex_control_meta_1"}


def test_require_current_control_store_never_creates_missing_path(tmp_path):
    path = tmp_path / "missing-parent" / "control.sqlite3"

    with pytest.raises(RuntimeError, match="rca_control_store_existing_path_missing"):
        RcaControlStore(path, require_current=True)

    assert path.parent.exists() is False


def test_require_current_control_store_never_migrates_predecessor(tmp_path):
    path = tmp_path / "control.sqlite3"
    RcaControlStore(path)
    with sqlite3.connect(path) as conn:
        conn.execute(
            "UPDATE control_meta SET value='pnc_rca_control_store_v8' "
            "WHERE key='schema_version'"
        )
        before = conn.execute(
            "SELECT name, sql FROM sqlite_master ORDER BY name"
        ).fetchall()

    with pytest.raises(RuntimeError, match="rca_control_store_schema_not_current"):
        RcaControlStore(path, require_current=True)

    with sqlite3.connect(path) as conn:
        after = conn.execute(
            "SELECT name, sql FROM sqlite_master ORDER BY name"
        ).fetchall()
        marker = conn.execute(
            "SELECT value FROM control_meta WHERE key='schema_version'"
        ).fetchone()[0]
    assert after == before
    assert marker == "pnc_rca_control_store_v8"


def test_require_current_control_store_opens_current_regular_file(tmp_path):
    path = tmp_path / "control.sqlite3"
    RcaControlStore(path)

    reopened = RcaControlStore(path, require_current=True)

    assert reopened.initialization_observation() == {
        "mode": "steady",
        "backfill_runs": 0,
    }


def test_require_current_control_store_rejects_symlink(tmp_path):
    path = tmp_path / "control.sqlite3"
    RcaControlStore(path)
    alias = tmp_path / "control-alias.sqlite3"
    alias.symlink_to(path)

    with pytest.raises(RuntimeError, match="rca_control_store_existing_path_invalid"):
        RcaControlStore(alias, require_current=True)


@pytest.mark.parametrize(
    ("suffix", "marker_kind"),
    [
        (".pnc-rca-maintenance", "maintenance"),
        (".pnc-rca-tombstone", "rollback_tombstone"),
    ],
)
@pytest.mark.parametrize("marker_form", ["regular", "directory", "dangling_symlink"])
def test_require_current_control_store_rejects_installation_markers_before_init(
    tmp_path, suffix, marker_kind, marker_form
):
    path = tmp_path / "control.sqlite3"
    RcaControlStore(path)
    marker = Path(f"{path}{suffix}")
    if marker_form == "regular":
        marker.write_text("installation in progress", encoding="utf-8")
    elif marker_form == "directory":
        marker.mkdir()
    else:
        marker.symlink_to(tmp_path / "missing-marker-target")

    with pytest.raises(
        RuntimeError,
        match=f"rca_control_store_installation_marker_present:{marker_kind}",
    ):
        RcaControlStore(path, require_current=True)


@pytest.mark.parametrize(
    ("suffix", "marker_kind"),
    [
        (".pnc-rca-maintenance", "maintenance"),
        (".pnc-rca-tombstone", "rollback_tombstone"),
    ],
)
def test_require_current_control_store_rechecks_marker_after_schema_validation(
    tmp_path, monkeypatch, suffix, marker_kind
):
    path = tmp_path / "control.sqlite3"
    RcaControlStore(path)
    marker = Path(f"{path}{suffix}")
    original = RcaControlStore._validate_current_schema_read_only

    def validate_and_fence(store):
        original(store)
        marker.write_text("installation started", encoding="utf-8")

    monkeypatch.setattr(
        RcaControlStore,
        "_validate_current_schema_read_only",
        validate_and_fence,
    )

    with pytest.raises(
        RuntimeError,
        match=f"rca_control_store_installation_marker_present:{marker_kind}",
    ):
        RcaControlStore(path, require_current=True)


@pytest.mark.parametrize(
    ("suffix", "marker_kind"),
    [
        (".pnc-rca-maintenance", "maintenance"),
        (".pnc-rca-tombstone", "rollback_tombstone"),
    ],
)
def test_require_current_control_store_rechecks_marker_before_each_connection(
    tmp_path, suffix, marker_kind
):
    path = tmp_path / "control.sqlite3"
    RcaControlStore(path)
    reopened = RcaControlStore(path, require_current=True)
    Path(f"{path}{suffix}").write_text("installation started", encoding="utf-8")

    with pytest.raises(
        RuntimeError,
        match=f"rca_control_store_installation_marker_present:{marker_kind}",
    ):
        reopened.list_rows("rca_activation_epochs")


@pytest.mark.parametrize(
    "suffix", [".pnc-rca-maintenance", ".pnc-rca-tombstone"]
)
def test_default_control_store_ignores_installation_markers(tmp_path, suffix):
    path = tmp_path / "control.sqlite3"
    marker = Path(f"{path}{suffix}")
    marker.write_text("controlled materialization", encoding="utf-8")

    store = RcaControlStore(path)

    assert store.health()["schema_version"] == CONTROL_STORE_SCHEMA_VERSION


def test_v4_store_is_upgraded_with_retry_window_and_rearm_audit(tmp_path):
    path = tmp_path / "control.sqlite3"
    store = _steady_control_store(path)
    store.ingest_record(_record(), policy=_policy(), submit_enabled=True)
    conn = sqlite3.connect(path, isolation_level=None)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute(
            "UPDATE control_meta SET value = 'pnc_rca_control_store_v4' "
            "WHERE key = 'schema_version'"
        )
        conn.execute("UPDATE rca_outbox SET retry_window_started_at = NULL")
        conn.execute("DROP TABLE rca_outbox_rearm_audit")
    finally:
        conn.close()

    upgraded = RcaControlStore(path)

    [outbox] = upgraded.list_rows("rca_outbox")
    assert outbox["retry_window_started_at"] == outbox["created_at"]
    assert upgraded.list_rows("rca_outbox_rearm_audit") == []
    assert upgraded.health()["schema_version"] == CONTROL_STORE_SCHEMA_VERSION
    assert upgraded.initialization_observation() == {
        "mode": "migration",
        "backfill_runs": 1,
    }


def test_v6_store_migrates_issue_scope_and_operator_rate_indexes(tmp_path):
    path = tmp_path / "control.sqlite3"
    store = _steady_control_store(path)
    store.ingest_record(_record(), policy=_policy(), submit_enabled=True)
    conn = sqlite3.connect(path, isolation_level=None)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("DROP INDEX idx_business_triggers_issue_scope")
        conn.execute("DROP INDEX idx_rca_manual_operator_rate")
        conn.execute(
            "UPDATE control_meta SET value='pnc_rca_control_store_v6' "
            "WHERE key='schema_version'"
        )
    finally:
        conn.close()

    upgraded = RcaControlStore(path)
    conn = upgraded._connect()
    try:
        indexes = {
            row["name"]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index'"
            ).fetchall()
        }
        plan = conn.execute(
            """
            EXPLAIN QUERY PLAN
            SELECT * FROM business_triggers
             WHERE project_key=? AND work_item_type_key=? AND work_item_id=?
             ORDER BY generation DESC, created_at DESC
            """,
            ("project-key", "problem-type", "7041712812"),
        ).fetchall()
    finally:
        conn.close()

    assert upgraded.health()["schema_version"] == CONTROL_STORE_SCHEMA_VERSION
    assert upgraded.initialization_observation()["mode"] == "migration"
    assert {
        "idx_business_triggers_issue_scope",
        "idx_rca_manual_operator_rate",
    }.issubset(indexes)
    assert any(
        "idx_business_triggers_issue_scope" in str(row["detail"])
        for row in plan
    )


def test_current_store_has_source_fairness_indexes(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    conn = store._connect()
    try:
        indexes = {
            str(row["name"])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index'"
            ).fetchall()
        }
    finally:
        conn.close()

    assert {
        "idx_outbox_source_status",
        "idx_trigger_bindings_generation",
        "idx_trigger_sources_kind_outcome",
    }.issubset(indexes)


def test_current_marker_missing_required_index_is_rejected_read_only(tmp_path):
    path = tmp_path / "control.sqlite3"
    store = RcaControlStore(path)
    conn = store._connect()
    try:
        conn.execute("DROP INDEX idx_business_triggers_issue_scope")
    finally:
        conn.close()

    with pytest.raises(
        RuntimeError, match="incompatible_control_store_schema:required_indexes"
    ):
        RcaControlStore(path)


def test_v5_kafka_bound_parent_rows_are_atomically_migrated_and_backfilled(tmp_path):
    path = tmp_path / "control.sqlite3"
    store = _steady_control_store(path)
    accepted = store.ingest_record(_record(), policy=_policy(), submit_enabled=True)
    conn = sqlite3.connect(path, isolation_level=None)
    try:
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.executescript(
            """
            BEGIN IMMEDIATE;
            CREATE TABLE trigger_backup AS
                SELECT business_key,generation,submission_key,creation_rule_version,
                       work_item_id,project_key,work_item_type_key,source_event_id,
                       source_topic,source_partition,source_offset,normalized_json,
                       state,created_at FROM business_triggers;
            CREATE TABLE outbox_backup AS
                SELECT outbox_id,action,business_key,submission_key,
                       creation_rule_version,generation,source_event_id,source_topic,
                       source_partition,source_offset,payload_json,status,attempt,
                       next_attempt_at,fence,lease_token,lease_owner,lease_expires_at,
                       claimed_at,completed_at,quarantined_at,last_error_code,
                       last_error_detail,result_json,retry_window_started_at,
                       created_at,updated_at FROM rca_outbox;
            DROP TABLE rca_snapshot_source_envelopes;
            DROP TABLE rca_admission_snapshots;
            DROP TABLE rca_source_authority_receipts;
            DROP TABLE rca_canonical_requests;
            DROP TABLE rca_terminal_rerun_delivery_authorities;
            DROP TABLE rca_trigger_delivery_bindings;
            DROP TABLE rca_delivery_subscriptions;
            DROP TABLE rca_trigger_bindings;
            DROP TABLE rca_trigger_sources;
            DROP TABLE rca_policy_snapshots;
            DROP TABLE rca_outbox;
            DROP TABLE business_triggers;
            CREATE TABLE business_triggers (
                business_key TEXT NOT NULL,
                generation INTEGER NOT NULL,
                submission_key TEXT NOT NULL UNIQUE,
                creation_rule_version TEXT NOT NULL,
                work_item_id TEXT NOT NULL,
                project_key TEXT NOT NULL,
                work_item_type_key TEXT NOT NULL,
                source_event_id TEXT NOT NULL,
                source_topic TEXT NOT NULL,
                source_partition INTEGER NOT NULL,
                source_offset INTEGER NOT NULL,
                normalized_json TEXT NOT NULL,
                state TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY(business_key,generation),
                FOREIGN KEY(source_event_id) REFERENCES kafka_inbox(event_uid)
            );
            INSERT INTO business_triggers SELECT * FROM trigger_backup;
            CREATE TABLE rca_outbox (
                outbox_id INTEGER PRIMARY KEY AUTOINCREMENT,
                action TEXT NOT NULL,business_key TEXT NOT NULL,
                submission_key TEXT NOT NULL UNIQUE,
                creation_rule_version TEXT NOT NULL,generation INTEGER NOT NULL,
                source_event_id TEXT NOT NULL,source_topic TEXT NOT NULL,
                source_partition INTEGER NOT NULL,source_offset INTEGER NOT NULL,
                payload_json TEXT NOT NULL,status TEXT NOT NULL,
                attempt INTEGER NOT NULL DEFAULT 0,next_attempt_at TEXT,
                fence INTEGER NOT NULL DEFAULT 0,lease_token TEXT,lease_owner TEXT,
                lease_expires_at TEXT,claimed_at TEXT,completed_at TEXT,
                quarantined_at TEXT,last_error_code TEXT NOT NULL DEFAULT '',
                last_error_detail TEXT NOT NULL DEFAULT '',result_json TEXT,
                retry_window_started_at TEXT,created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(business_key,generation)
                    REFERENCES business_triggers(business_key,generation),
                FOREIGN KEY(source_event_id) REFERENCES kafka_inbox(event_uid)
            );
            INSERT INTO rca_outbox SELECT * FROM outbox_backup;
            DROP TABLE trigger_backup;
            DROP TABLE outbox_backup;
            UPDATE control_meta SET value='pnc_rca_control_store_v5'
             WHERE key='schema_version';
            COMMIT;
            """
        )
    finally:
        conn.close()

    upgraded = RcaControlStore(path)

    [trigger] = upgraded.list_rows("business_triggers")
    [outbox] = upgraded.list_rows("rca_outbox")
    [source] = upgraded.list_rows("rca_trigger_sources")
    [binding] = upgraded.list_rows("rca_trigger_bindings")
    assert trigger["submission_key"] == accepted.submission_key
    assert outbox["submission_key"] == accepted.submission_key
    assert trigger["origin_source_id"] == source["source_id"]
    assert outbox["origin_source_id"] == source["source_id"]
    assert binding["role"] == "origin"
    assert len(upgraded.list_rows("rca_delivery_subscriptions")) == 1
    assert upgraded.health()["schema_version"] == CONTROL_STORE_SCHEMA_VERSION


def test_current_marker_with_missing_progress_foreign_key_is_rejected(tmp_path):
    path = tmp_path / "control.sqlite3"
    store = RcaControlStore(path)
    conn = store._connect()
    try:
        conn.execute("DROP TABLE kafka_partition_progress")
        conn.execute(
            """
            CREATE TABLE kafka_partition_progress (
                topic TEXT NOT NULL,
                partition_id INTEGER NOT NULL,
                first_offset INTEGER NOT NULL,
                durable_next_offset INTEGER NOT NULL,
                last_event_uid TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (topic, partition_id)
            )
            """
        )
    finally:
        conn.close()

    with pytest.raises(
        RuntimeError,
        match="incompatible_control_store_schema:kafka_partition_progress_foreign_keys",
    ):
        RcaControlStore(path)


def test_current_store_reopen_is_read_only_and_skips_backfill(tmp_path):
    path = tmp_path / "control.sqlite3"
    created = RcaControlStore(path)
    assert created.initialization_observation() == {
        "mode": "migration",
        "backfill_runs": 1,
    }

    writer = sqlite3.connect(path, isolation_level=None)
    try:
        writer.execute("BEGIN IMMEDIATE")
        reopened = RcaControlStore(path, busy_timeout_ms=25)
        assert reopened.initialization_observation() == {
            "mode": "steady",
            "backfill_runs": 0,
        }
    finally:
        writer.rollback()
        writer.close()


def test_concurrent_current_store_initialization_is_bounded_read_only(tmp_path):
    path = tmp_path / "control.sqlite3"
    RcaControlStore(path)

    with ThreadPoolExecutor(max_workers=8) as pool:
        observations = list(
            pool.map(
                lambda _index: RcaControlStore(path).initialization_observation(),
                range(16),
            )
        )

    assert observations == [{"mode": "steady", "backfill_runs": 0}] * 16


def test_health_surfaces_filesystem_probe_failure(tmp_path, monkeypatch):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    monkeypatch.setattr(
        control_store_module.os,
        "statvfs",
        lambda path: (_ for _ in ()).throw(OSError("probe failed")),
    )

    health = store.health()

    assert health["ok"] is False
    assert health["filesystem"]["available_bytes"] is None
    assert "probe failed" in health["filesystem"]["error"]


def test_raw_persist_fails_closed_below_control_store_disk_reserve(
    tmp_path, monkeypatch
):
    store = RcaControlStore(tmp_path / "control.sqlite3")

    class LowDisk:
        f_bavail = 1
        f_frsize = 4096

    monkeypatch.setattr(control_store_module.os, "statvfs", lambda _path: LowDisk())

    with pytest.raises(ControlStoreCapacityError, match="below_reserve"):
        store.persist_raw(_record(), policy=_policy())
    assert store.list_rows("kafka_inbox") == []


def test_low_disk_allows_hash_verified_durable_duplicate_replay(
    tmp_path, monkeypatch
):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    first = store.ingest_record(_record(), policy=_policy())

    class LowDisk:
        f_bavail = 1
        f_frsize = 4096

    monkeypatch.setattr(control_store_module.os, "statvfs", lambda _path: LowDisk())
    replay = store.ingest_record(_record(), policy=_policy())

    assert replay.decision == first.decision == "accepted"
    assert replay.transport_duplicate is True
    assert replay.raw_inserted is False


def test_accepted_event_persists_raw_admission_trigger_and_shadow_outbox(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    result = store.ingest_record(_record(), policy=_policy())

    inbox = store.get_inbox(result.event_uid)
    triggers = store.list_rows("business_triggers")
    outbox = store.list_rows("rca_outbox")
    canonical = build_rca_admission(
        project_key="project-key",
        project_simple_name="g1q3",
        work_item_type_key="problem-type",
        work_item_id="7041712812",
        rule_version="issue-created-v1",
        topic=TOPIC,
        partition=2,
        offset=10,
    )

    assert result.ack_safe is True
    assert result.raw_inserted is True
    assert result.decision == "accepted"
    assert result.trigger_created is True
    assert result.outbox_created is True
    assert inbox["raw_value"] == _value()
    assert inbox["processed_at"]
    assert result.business_key == canonical.business_key
    assert result.submission_key == canonical.submission_key
    assert triggers[0]["creation_rule_version"] == "issue-created-v1"
    assert triggers[0]["generation"] == 1
    assert triggers[0]["source_topic"] == TOPIC
    assert triggers[0]["source_partition"] == 2
    assert triggers[0]["source_offset"] == 10
    assert triggers[0]["state"] == "shadow"
    assert outbox[0]["status"] == "shadow"
    payload = json.loads(outbox[0]["payload_json"])
    assert payload["admission"] == canonical.to_dict()
    assert store.partition_progress(topic=TOPIC, partitions=[2]) == {2: 11}


def test_partition_progress_advances_only_after_record_processing_is_durable(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    record = _record(offset=25)

    store.persist_raw(record, policy=_policy())
    assert store.partition_progress(topic=TOPIC, partitions=[2]) == {}

    store.process_event(record.event_uid)
    assert store.partition_progress(topic=TOPIC, partitions=[2]) == {2: 26}


def test_same_transport_record_is_idempotent(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    first = store.ingest_record(_record(), policy=_policy())
    replay = store.ingest_record(_record(), policy=_policy())

    assert first.decision == "accepted"
    assert replay.decision == "accepted"
    assert replay.raw_inserted is False
    assert replay.transport_duplicate is True
    assert len(store.list_rows("kafka_inbox")) == 1
    assert len(store.list_rows("business_triggers")) == 1
    assert len(store.list_rows("rca_outbox")) == 1


def test_filtered_shared_topic_payload_drops_raw_blob_but_keeps_transport_hash(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    result = store.ingest_record(
        _record(value=json.dumps({"ci": "unrelated", "status": "ok"}).encode()),
        policy=_policy(),
    )

    inbox = store.get_inbox(result.event_uid)
    assert result.decision == "filtered"
    assert inbox["raw_value"] == b""
    assert len(inbox["raw_sha256"]) == 64


def test_policy_filtered_payload_is_retained_for_bounded_replay(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    payload = json.loads(_value())
    payload["project_key"] = "other-project"
    result = store.ingest_record(
        _record(value=json.dumps(payload).encode()),
        policy=_policy(),
    )

    inbox = store.get_inbox(result.event_uid)
    assert result.decision == "filtered"
    assert result.reason == "project_key_not_allowed"
    assert inbox["raw_value"]
    assert inbox["raw_pruned_at"] is None
    assert store.health()["replay_raw_retained_count"] == 1


def test_policy_filtered_raw_expires_after_seven_days(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    payload = json.loads(_value())
    payload["project_key"] = "other-project"
    result = store.ingest_record(
        _record(value=json.dumps(payload).encode()),
        policy=_policy(),
    )
    old = datetime(2026, 7, 1, tzinfo=timezone.utc)
    conn = store._connect()
    try:
        conn.execute(
            "UPDATE kafka_inbox SET processed_at = ? WHERE event_uid = ?",
            (old.isoformat(), result.event_uid),
        )
    finally:
        conn.close()

    assert store.prune_replay_raw(now=old + timedelta(days=8)) == 1
    inbox = store.get_inbox(result.event_uid)
    assert inbox["raw_value"] == b""
    assert inbox["raw_pruned_at"] is not None


def test_accepted_raw_expires_after_thirty_days_but_identity_remains(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    result = store.ingest_record(_record(), policy=_policy())
    old = datetime(2026, 6, 1, tzinfo=timezone.utc)
    conn = store._connect()
    try:
        conn.execute(
            "UPDATE kafka_inbox SET processed_at = ? WHERE event_uid = ?",
            (old.isoformat(), result.event_uid),
        )
    finally:
        conn.close()

    assert store.prune_replay_raw(now=old + timedelta(days=31)) == 1
    inbox = store.get_inbox(result.event_uid)
    assert inbox["raw_value"] == b""
    assert inbox["decision"] == "accepted"
    assert len(inbox["raw_sha256"]) == 64
    assert inbox["business_key"]


def test_unexpected_record_failure_never_advances_or_acknowledges_offset(tmp_path, monkeypatch):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    record = _record()
    store.persist_raw(record, policy=_policy())
    monkeypatch.setattr(
        control_store_module,
        "classify_workflow_event",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("payload-secret")),
    )

    for _attempt in range(3):
        with pytest.raises(RecordProcessingBlockedError) as error:
            store.process_event_resilient(record.event_uid)
        assert "payload-secret" not in str(error.value)
        assert isinstance(error.value.__cause__, RuntimeError)

    inbox = store.get_inbox(record.event_uid)
    assert inbox["decision"] == "pending"
    assert inbox["processing_attempts"] == 3
    assert inbox["raw_value"] == _value()
    assert "payload-secret" not in inbox["last_processing_error_detail"]
    assert store.list_rows("kafka_dead_letters") == []
    assert store.partition_progress(topic=TOPIC, partitions=[record.partition]) == {}


def test_new_offset_for_same_issue_is_business_deduped(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    first = store.ingest_record(_record(10), policy=_policy())
    replay = store.ingest_record(
        _record(11, value=_value(updated_at=1783659999999)),
        policy=_policy(),
    )

    assert replay.decision == "deduped"
    assert replay.reason == "business_trigger_exists"
    assert replay.business_key == first.business_key
    assert replay.submission_key == first.submission_key
    assert len(store.list_rows("kafka_inbox")) == 2
    assert len(store.list_rows("business_triggers")) == 1
    assert len(store.list_rows("rca_outbox")) == 1


def test_input_wait_quarantine_is_atomically_rearmed_by_new_offset(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    first = store.ingest_record(
        _record(10), policy=_policy(), submit_enabled=True
    )
    original_outbox_id, original_window = _quarantine_for_input_wait(store)
    quarantined = store.list_rows("rca_outbox")[0]
    prior_fence = quarantined["fence"]
    assert quarantined["attempt"] == 2

    replacement_value = _value(updated_at=1783659999999)
    replacement = store.ingest_record(
        _record(11, value=replacement_value),
        policy=_policy(),
        submit_enabled=True,
    )

    assert replacement.decision == "deduped"
    assert replacement.reason == INPUT_WAIT_QUARANTINE_REARMED_REASON
    assert replacement.rearm_reason == INPUT_WAIT_QUARANTINE_REARMED_REASON
    assert replacement.outbox_rearmed is True
    assert replacement.business_key == first.business_key
    assert replacement.submission_key == first.submission_key

    [outbox] = store.list_rows("rca_outbox")
    [trigger] = store.list_rows("business_triggers")
    assert outbox["outbox_id"] == original_outbox_id
    assert outbox["submission_key"] == first.submission_key
    assert outbox["status"] == "pending"
    assert outbox["source_event_id"] == replacement.event_uid
    assert outbox["source_offset"] == 11
    assert outbox["attempt"] == 0
    assert outbox["fence"] == prior_fence + 1
    assert outbox["retry_window_started_at"] > original_window.isoformat()
    assert outbox["next_attempt_at"] == outbox["retry_window_started_at"]
    assert outbox["quarantined_at"] is None
    assert outbox["last_error_code"] == ""
    assert outbox["last_error_detail"] == ""
    assert outbox["result_json"] is None
    assert trigger["state"] == "pending"
    assert trigger["source_event_id"] == replacement.event_uid
    assert trigger["source_offset"] == 11

    payload = json.loads(outbox["payload_json"])
    replacement_inbox = store.get_inbox(replacement.event_uid)
    assert payload["business_key"] == first.business_key
    assert payload["submission_key"] == first.submission_key
    assert payload["source_event_id"] == replacement.event_uid
    assert payload["offset"] == 11
    assert payload["admission"]["source_refs"]["offset"] == 11
    assert payload["normalized_event"] == json.loads(
        replacement_inbox["normalized_json"]
    )
    assert trigger["normalized_json"] == replacement_inbox["normalized_json"]

    [audit] = store.list_rows("rca_outbox_rearm_audit")
    assert audit == {
        "audit_id": audit["audit_id"],
        "outbox_id": original_outbox_id,
        "submission_key": first.submission_key,
        "prior_source_event_id": first.event_uid,
        "replacement_source_event_id": replacement.event_uid,
            "prior_attempt": 2,
        "prior_fence": prior_fence,
        "prior_error_code": "issue_field_missing_remote_data_reference",
        "reason": INPUT_WAIT_QUARANTINE_REARMED_REASON,
        "created_at": audit["created_at"],
    }
    audit_json = json.dumps(audit, sort_keys=True)
    assert "raw_value" not in audit_json
    assert "payload_json" not in audit_json
    assert "error_detail" not in audit_json
    assert "7041712812" not in audit_json

    claim = store.claim_outbox(
        lease_owner="post-rearm-worker",
        now=datetime.fromisoformat(outbox["retry_window_started_at"])
        + timedelta(seconds=1),
    )
    assert claim is not None
    assert claim.submission_key == first.submission_key
    completed = store.complete_outbox(
        outbox_id=claim.outbox_id,
        lease_token=claim.lease_token,
        result={"success": True},
        now=datetime.fromisoformat(claim.lease_expires_at) - timedelta(seconds=1),
    )
    assert completed.status == "completed"
    assert len(store.list_rows("rca_outbox")) == 1


@pytest.mark.parametrize(
    ("replacement", "expected_reason"),
    [
        (_record(9, value=_value(updated_at=1783659999999)), "replacement_offset_not_newer"),
        (
            _record(11, value=_value(updated_at=1783659999999), partition=3),
            "replacement_source_lineage_mismatch",
        ),
    ],
)
def test_input_wait_rearm_requires_monotonic_same_partition_lineage(
    tmp_path, replacement, expected_reason
):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    first = store.ingest_record(
        _record(10), policy=_policy(), submit_enabled=True
    )
    _quarantine_for_input_wait(store)

    result = store.ingest_record(
        replacement,
        policy=_policy(),
        submit_enabled=True,
    )

    assert result.outbox_rearmed is False
    assert result.rearm_reason == expected_reason
    [outbox] = store.list_rows("rca_outbox")
    assert outbox["status"] == "quarantined"
    assert outbox["source_event_id"] == first.event_uid
    assert outbox["source_partition"] == 2
    assert outbox["source_offset"] == 10
    assert store.list_rows("rca_outbox_rearm_audit") == []


@pytest.mark.parametrize(
    ("state", "expected_rearm_reason"),
    [
        ("pending", "outbox_not_quarantined"),
        ("claimed", "outbox_claimed"),
        ("completed", "completed_or_result_present"),
        ("poison_quarantine", "error_not_input_wait_allowlisted"),
        ("circuit_quarantine", "error_not_input_wait_allowlisted"),
        ("permanent_quarantine", "error_not_input_wait_allowlisted"),
        ("result_present", "completed_or_result_present"),
        ("source_mismatch", "identity_mismatch"),
    ],
)
def test_new_offset_never_rearms_non_input_wait_states(
    tmp_path, state, expected_rearm_reason
):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    first = store.ingest_record(
        _record(10), policy=_policy(), submit_enabled=True
    )
    created = datetime.fromisoformat(
        store.list_rows("rca_outbox")[0]["retry_window_started_at"]
    )
    if state != "pending":
        claim = store.claim_outbox(
            lease_owner="state-test",
            lease_seconds=180,
            now=created + timedelta(seconds=1),
        )
        assert claim is not None
        if state == "completed":
            store.complete_outbox(
                outbox_id=claim.outbox_id,
                lease_token=claim.lease_token,
                result={"success": True},
                now=created + timedelta(seconds=2),
            )
        elif state != "claimed":
            error_codes = {
                "poison_quarantine": "dispatcher_outbox_contract_invalid",
                "circuit_quarantine": "vm_task_service_permission_denied",
                "permanent_quarantine": "dispatcher_execution_request_build_failed",
                "result_present": "issue_field_missing_remote_data_reference",
                "source_mismatch": "issue_field_missing_remote_data_reference",
            }
            store.quarantine_outbox(
                outbox_id=claim.outbox_id,
                lease_token=claim.lease_token,
                error_code=error_codes[state],
                now=created + timedelta(seconds=2),
            )
            if state == "result_present":
                conn = store._connect()
                try:
                    conn.execute(
                        "UPDATE rca_outbox SET result_json = ? WHERE outbox_id = ?",
                        ('{"success":true}', claim.outbox_id),
                    )
                finally:
                    conn.close()
            if state == "source_mismatch":
                conn = store._connect()
                try:
                    conn.execute(
                        "UPDATE business_triggers SET source_offset = 999 "
                        "WHERE submission_key = ?",
                        (first.submission_key,),
                    )
                finally:
                    conn.close()

    before = dict(store.list_rows("rca_outbox")[0])
    replacement = store.ingest_record(
        _record(11, value=_value(updated_at=1783659999999)),
        policy=_policy(),
        submit_enabled=True,
    )
    after = store.list_rows("rca_outbox")[0]

    assert replacement.decision == "deduped"
    assert replacement.reason == "business_trigger_exists"
    assert replacement.outbox_rearmed is False
    assert replacement.rearm_reason == expected_rearm_reason
    assert store.get_inbox(replacement.event_uid)["rearm_reason"] == (
        expected_rearm_reason
    )
    assert after["outbox_id"] == before["outbox_id"]
    assert after["submission_key"] == first.submission_key
    assert after["source_event_id"] == first.event_uid
    assert after["source_offset"] == 10
    assert after["status"] == before["status"]
    assert after["payload_json"] == before["payload_json"]
    assert store.list_rows("rca_outbox_rearm_audit") == []


def test_concurrent_update_offsets_have_one_rearm_winner_and_replay_is_idempotent(
    tmp_path,
):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    first = store.ingest_record(
        _record(10), policy=_policy(), submit_enabled=True
    )
    _quarantine_for_input_wait(store)
    payloads = {
        offset: _value(updated_at=1783650000000 + offset)
        for offset in range(20, 28)
    }

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(
            pool.map(
                lambda offset: store.ingest_record(
                    _record(offset, value=payloads[offset]),
                    policy=_policy(),
                    submit_enabled=True,
                ),
                range(20, 28),
            )
        )

    winners = [result for result in results if result.outbox_rearmed]
    assert len(winners) == 1
    winner = winners[0]
    assert winner.rearm_reason == INPUT_WAIT_QUARANTINE_REARMED_REASON
    assert {result.submission_key for result in results} == {first.submission_key}
    assert len(store.list_rows("business_triggers")) == 1
    assert len(store.list_rows("rca_outbox")) == 1
    assert len(store.list_rows("rca_outbox_rearm_audit")) == 1
    [outbox] = store.list_rows("rca_outbox")
    assert outbox["source_event_id"] == winner.event_uid
    assert outbox["source_offset"] == int(winner.event_uid.rsplit(":", 1)[1])

    winner_offset = int(winner.event_uid.rsplit(":", 1)[1])
    replay = store.ingest_record(
        _record(winner_offset, value=payloads[winner_offset]),
        policy=_policy(),
        submit_enabled=True,
    )
    assert replay.transport_duplicate is True
    assert replay.outbox_rearmed is True
    assert replay.rearm_reason == INPUT_WAIT_QUARANTINE_REARMED_REASON
    assert len(store.list_rows("rca_outbox_rearm_audit")) == 1


def test_shadow_update_event_cannot_rearm_live_input_wait_quarantine(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    first = store.ingest_record(
        _record(10), policy=_policy(), submit_enabled=True
    )
    _quarantine_for_input_wait(store)

    replacement = store.ingest_record(
        _record(11, value=_value(updated_at=1783659999999)),
        policy=_policy(),
        submit_enabled=False,
    )

    assert replacement.outbox_rearmed is False
    assert replacement.rearm_reason == "replacement_event_not_pending"
    [outbox] = store.list_rows("rca_outbox")
    assert outbox["status"] == "quarantined"
    assert outbox["source_event_id"] == first.event_uid
    assert store.list_rows("rca_outbox_rearm_audit") == []


def test_concurrent_offsets_create_one_submission_key(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(
            pool.map(
                lambda offset: store.ingest_record(_record(offset), policy=_policy()),
                range(20, 28),
            )
        )

    assert (
        sorted(result.decision for result in results) == ["accepted"] + ["deduped"] * 7
    )
    assert len({result.business_key for result in results}) == 1
    assert len({result.submission_key for result in results}) == 1
    assert len(store.list_rows("business_triggers")) == 1
    assert len(store.list_rows("rca_outbox")) == 1


def test_shadow_outbox_is_never_claimed(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    store.ingest_record(_record(), policy=_policy(), submit_enabled=False)

    assert store.claim_outbox(lease_owner="worker-1") is None
    assert store.list_rows("rca_outbox")[0]["status"] == "shadow"


def test_concurrent_outbox_claim_has_one_winner(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    store.ingest_record(_record(), policy=_policy(), submit_enabled=True)
    current = datetime.now(timezone.utc) + timedelta(seconds=1)

    with ThreadPoolExecutor(max_workers=8) as pool:
        claims = list(
            pool.map(
                lambda index: store.claim_outbox(
                    lease_owner=f"worker-{index}", now=current
                ),
                range(8),
            )
        )

    winners = [claim for claim in claims if claim is not None]
    assert len(winners) == 1
    assert winners[0].attempt == 1
    assert winners[0].fence == 1


def test_expired_lease_is_reclaimed_and_old_token_is_fenced(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    store.ingest_record(_record(), policy=_policy(), submit_enabled=True)
    current = datetime.now(timezone.utc) + timedelta(seconds=1)
    first = store.claim_outbox(
        lease_owner="worker-1", lease_seconds=30, now=current
    )
    assert first is not None
    assert (
        store.claim_outbox(
            lease_owner="worker-2",
            lease_seconds=30,
            now=current + timedelta(seconds=29),
        )
        is None
    )

    recovered = store.claim_outbox(
        lease_owner="worker-2",
        lease_seconds=30,
        now=current + timedelta(seconds=31),
    )
    assert recovered is not None
    assert recovered.lease_token != first.lease_token
    assert recovered.attempt == 2
    assert recovered.fence == 2
    with pytest.raises(StaleOutboxLeaseError):
        store.complete_outbox(
            outbox_id=first.outbox_id,
            lease_token=first.lease_token,
            result={"success": True},
            now=current + timedelta(seconds=32),
        )
    with pytest.raises(StaleOutboxLeaseError):
        store.retry_outbox(
            outbox_id=first.outbox_id,
            lease_token=first.lease_token,
            error_code="late_worker",
            delay_seconds=2,
            now=current + timedelta(seconds=32),
        )

    completed = store.complete_outbox(
        outbox_id=recovered.outbox_id,
        lease_token=recovered.lease_token,
        result={"success": True},
        now=current + timedelta(seconds=32),
    )
    assert completed.status == "completed"


def test_live_lease_can_be_extended_but_expired_lease_cannot_be_revived(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    store.ingest_record(_record(), policy=_policy(), submit_enabled=True)
    current = datetime.now(timezone.utc) + timedelta(seconds=1)
    claim = store.claim_outbox(
        lease_owner="worker-1", lease_seconds=30, now=current
    )
    assert claim is not None

    extended = store.extend_outbox_lease(
        outbox_id=claim.outbox_id,
        lease_token=claim.lease_token,
        lease_owner="worker-1",
        lease_seconds=30,
        now=current + timedelta(seconds=20),
    )
    assert datetime.fromisoformat(extended) == current + timedelta(seconds=50)
    assert (
        store.claim_outbox(
            lease_owner="worker-2",
            lease_seconds=30,
            now=current + timedelta(seconds=40),
        )
        is None
    )
    with pytest.raises(StaleOutboxLeaseError):
        store.extend_outbox_lease(
            outbox_id=claim.outbox_id,
            lease_token=claim.lease_token,
            lease_owner="worker-1",
            lease_seconds=30,
            now=current + timedelta(seconds=51),
        )


def test_expired_owner_cannot_complete_or_retry_before_reclaim(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    store.ingest_record(_record(), policy=_policy(), submit_enabled=True)
    current = datetime.now(timezone.utc) + timedelta(seconds=1)
    claim = store.claim_outbox(
        lease_owner="worker-1", lease_seconds=30, now=current
    )
    assert claim is not None
    expired_at = current + timedelta(seconds=31)

    with pytest.raises(StaleOutboxLeaseError):
        store.complete_outbox(
            outbox_id=claim.outbox_id,
            lease_token=claim.lease_token,
            result={"success": True},
            now=expired_at,
        )
    with pytest.raises(StaleOutboxLeaseError):
        store.retry_outbox(
            outbox_id=claim.outbox_id,
            lease_token=claim.lease_token,
            error_code="late",
            delay_seconds=2,
            now=expired_at,
        )
    assert store.list_rows("rca_outbox")[0]["status"] == "claimed"


def test_retry_outbox_and_open_circuit_commit_together(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    store.ingest_record(_record(), policy=_policy(), submit_enabled=True)
    current = datetime.now(timezone.utc) + timedelta(seconds=1)
    claim = store.claim_outbox(lease_owner="worker-1", now=current)
    assert claim is not None

    mutation = store.retry_outbox_and_open_circuit(
        outbox_id=claim.outbox_id,
        lease_token=claim.lease_token,
        error_code="vm_task_service_permission_denied",
        error_detail="service capability missing",
        delay_seconds=5,
        now=current + timedelta(seconds=1),
    )

    assert mutation.status == "pending"
    assert store.list_rows("rca_outbox")[0]["status"] == "pending"
    circuit = store.dispatcher_circuit()
    assert circuit.is_open is True
    assert circuit.reason_code == "vm_task_service_permission_denied"


def test_retry_outbox_and_open_circuit_rolls_back_together(
    tmp_path, monkeypatch
):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    store.ingest_record(_record(), policy=_policy(), submit_enabled=True)
    current = datetime.now(timezone.utc) + timedelta(seconds=1)
    claim = store.claim_outbox(lease_owner="worker-1", now=current)
    assert claim is not None

    def fail_circuit_write(*_args, **_kwargs):
        raise RuntimeError("simulated circuit write failure")

    monkeypatch.setattr(
        store,
        "_open_dispatcher_circuit_in_transaction",
        fail_circuit_write,
    )
    with pytest.raises(RuntimeError, match="simulated circuit write failure"):
        store.retry_outbox_and_open_circuit(
            outbox_id=claim.outbox_id,
            lease_token=claim.lease_token,
            error_code="vm_task_service_permission_denied",
            delay_seconds=5,
            now=current + timedelta(seconds=1),
        )

    row = store.list_rows("rca_outbox")[0]
    assert row["status"] == "claimed"
    assert row["lease_token"] == claim.lease_token
    assert store.dispatcher_circuit().is_open is False


def _circuit_dict(circuit):
    return {
        "state": circuit.state,
        "reason_code": circuit.reason_code,
        "reason_detail": circuit.reason_detail,
        "opened_at": circuit.opened_at,
        "updated_at": circuit.updated_at,
    }


def _audited_reset(*, db_path, reset_id, before, after, operator, reason, recorded_at):
    info = db_path.stat()
    audit = {
        "schema_version": "pnc_rca_outbox_circuit_reset_v1",
        "command": "clear-circuit",
        "reset_id": reset_id,
        "recorded_at": recorded_at,
        "operator": operator,
        "reason": reason,
        "control_db_identity": {
            "path": str(db_path.absolute()),
            "device": int(info.st_dev),
            "inode": int(info.st_ino),
            "size": int(info.st_size),
            "mtime_ns": int(info.st_mtime_ns),
        },
        "config_binding_sha256": "1" * 64,
        "before": _circuit_dict(before),
        "after": after,
        "pre_state": _circuit_dict(before),
        "post_state": after,
        "effect_delta": {
            "external_writes": 0,
            "scope": "submission_circuit_reset_command",
        },
    }
    audit["receipt_fingerprint"] = canonical_json_sha256(audit)
    return audit


def test_close_dispatcher_circuit_with_audit_commits_receipt_atomically(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    opened_at = datetime(2026, 7, 29, 3, 0, tzinfo=timezone.utc)
    reset_at = datetime(2026, 7, 29, 3, 1, tzinfo=timezone.utc)
    store.open_dispatcher_circuit(
        reason_code="snapshot_stale",
        reason_detail="snapshot exceeded ttl",
        now=opened_at,
    )
    before = store.dispatcher_circuit()
    after = {
        "state": "closed",
        "reason_code": "",
        "reason_detail": "",
        "opened_at": None,
        "updated_at": reset_at.isoformat(),
    }
    audit = _audited_reset(
        db_path=tmp_path / "control.sqlite3",
        reset_id="reset-test-1",
        before=before,
        after=after,
        operator="owner@example.com",
        reason="verified snapshot freshness offline",
        recorded_at=reset_at.isoformat(),
    )

    observed_before, observed_after = store.close_dispatcher_circuit_with_audit(
        audit=audit, now=reset_at
    )

    assert observed_before == before
    assert _circuit_dict(observed_after) == after
    assert store.dispatcher_circuit_reset_audit("reset-test-1") == audit


def test_close_dispatcher_circuit_with_audit_rejects_duplicate_without_mutation(
    tmp_path,
):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    opened_at = datetime(2026, 7, 29, 3, 0, tzinfo=timezone.utc)
    reset_at = datetime(2026, 7, 29, 3, 1, tzinfo=timezone.utc)
    store.open_dispatcher_circuit(reason_code="stale", now=opened_at)
    before = store.dispatcher_circuit()
    audit = _audited_reset(
        db_path=tmp_path / "control.sqlite3",
        reset_id="reset-test-duplicate",
        before=before,
        after={
            "state": "closed",
            "reason_code": "",
            "reason_detail": "",
            "opened_at": None,
            "updated_at": reset_at.isoformat(),
        },
        operator="operator",
        reason="reason",
        recorded_at=reset_at.isoformat(),
    )
    store.close_dispatcher_circuit_with_audit(audit=audit, now=reset_at)
    with pytest.raises(RuntimeError, match="requires_open_circuit|state_changed"):
        store.close_dispatcher_circuit_with_audit(audit=audit, now=reset_at)
    assert store.dispatcher_circuit().state == "closed"


def test_close_dispatcher_circuit_with_audit_rejects_extra_state_field(
    tmp_path,
):
    db_path = tmp_path / "control.sqlite3"
    store = RcaControlStore(db_path)
    reset_at = datetime(2026, 7, 29, 3, 1, tzinfo=timezone.utc)
    store.open_dispatcher_circuit(reason_code="stale")
    before = store.dispatcher_circuit()
    after = {
        "state": "closed",
        "reason_code": "",
        "reason_detail": "",
        "opened_at": None,
        "updated_at": reset_at.isoformat(),
    }
    audit = _audited_reset(
        db_path=db_path,
        reset_id="reset-extra-field",
        before=before,
        after=after,
        operator="operator",
        reason="reason",
        recorded_at=reset_at.isoformat(),
    )
    audit["before"]["unexpected"] = True
    audit["pre_state"]["unexpected"] = True
    audit["receipt_fingerprint"] = canonical_json_sha256(
        {key: value for key, value in audit.items() if key != "receipt_fingerprint"}
    )

    with pytest.raises(ValueError, match="audit_state_invalid"):
        store.close_dispatcher_circuit_with_audit(audit=audit, now=reset_at)
    assert store.dispatcher_circuit().state == "open"


def test_dispatcher_circuit_reset_audit_detects_direct_sql_tampering(tmp_path):
    db_path = tmp_path / "control.sqlite3"
    store = RcaControlStore(db_path)
    reset_at = datetime(2026, 7, 29, 3, 1, tzinfo=timezone.utc)
    store.open_dispatcher_circuit(reason_code="stale")
    before = store.dispatcher_circuit()
    audit = _audited_reset(
        db_path=db_path,
        reset_id="reset-tamper-test",
        before=before,
        after={
            "state": "closed",
            "reason_code": "",
            "reason_detail": "",
            "opened_at": None,
            "updated_at": reset_at.isoformat(),
        },
        operator="operator",
        reason="reason",
        recorded_at=reset_at.isoformat(),
    )
    store.close_dispatcher_circuit_with_audit(audit=audit, now=reset_at)
    tampered = dict(audit)
    tampered["reason"] = "changed after commit"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE control_meta SET value = ? WHERE key = ?",
            (
                json.dumps(tampered, sort_keys=True, separators=(",", ":")),
                "rca_dispatcher_circuit_reset:reset-tamper-test",
            ),
        )

    with pytest.raises(RuntimeError, match="audit_invalid"):
        store.dispatcher_circuit_reset_audit("reset-tamper-test")


def test_crash_after_raw_commit_is_recovered_from_pending_on_restart(tmp_path):
    path = tmp_path / "control.sqlite3"
    store = RcaControlStore(path)

    def crash(_receipt):
        raise RuntimeError("simulated crash")

    with pytest.raises(RuntimeError, match="simulated crash"):
        store.ingest_record(
            _record(),
            policy=_policy(),
            after_raw_persisted=crash,
        )
    assert store.get_inbox(_record().event_uid)["decision"] == "pending"

    restarted = RcaControlStore(path)
    recovered = restarted.process_pending()

    assert [result.decision for result in recovered] == ["accepted"]
    assert restarted.pending_event_uids() == []
    assert restarted.list_rows("rca_outbox")[0]["status"] == "shadow"
    replay = restarted.ingest_record(_record(), policy=_policy())
    assert replay.transport_duplicate is True
    assert len(restarted.list_rows("business_triggers")) == 1
    assert len(restarted.list_rows("rca_outbox")) == 1


def test_invalid_message_is_durable_and_dead_lettered_before_ack(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    result = store.ingest_record(_record(value=b"not-json"), policy=_policy())

    assert result.decision == "invalid"
    assert result.ack_safe is True
    assert store.get_inbox(result.event_uid)["raw_value"] == b"not-json"
    dead_letters = store.list_rows("kafka_dead_letters")
    assert dead_letters[0]["source_event_id"] == result.event_uid
    assert dead_letters[0]["error_code"] == "invalid_json"


def test_same_kafka_coordinate_with_changed_payload_fails_closed(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    store.persist_raw(_record(value=b"first"), policy=_policy())

    with pytest.raises(RecordConflictError, match="changed raw payload"):
        store.persist_raw(_record(value=b"second"), policy=_policy())
    assert store.get_inbox(_record().event_uid)["raw_value"] == b"first"


def test_health_contains_only_counts_and_no_raw_payload(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    store.ingest_record(_record(value=b"raw-secret-payload"), policy=_policy())

    health_json = json.dumps(store.health(), sort_keys=True)

    assert "raw-secret-payload" not in health_json
    assert store.health()["inbox"] == {"invalid": 1}


def test_manual_first_then_kafka_joins_one_generation_without_rewriting_origin(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    _register_policy_without_classifying(store)

    manual = store.admit_manual_trigger(
        _manual_request("om_manual_first"),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
    )
    [before_outbox] = store.list_rows("rca_outbox")
    before_payload = before_outbox["payload_json"]
    kafka = store.process_event(_record().event_uid)

    assert manual.outcome == "created"
    assert kafka.decision == "deduped"
    assert kafka.submission_key == manual.submission_key
    assert len(store.list_rows("business_triggers")) == 1
    [outbox] = store.list_rows("rca_outbox")
    [trigger] = store.list_rows("business_triggers")
    assert outbox["payload_json"] == before_payload
    assert outbox["origin_source_id"] == manual.source_id
    assert trigger["origin_source_id"] == manual.source_id
    bindings = store.list_rows("rca_trigger_bindings")
    assert {row["role"] for row in bindings} == {"origin", "observer"}
    assert len(store.list_rows("rca_delivery_subscriptions")) == 2


def test_late_kafka_creation_event_binds_generation_one_after_manual_rerun(
    tmp_path,
):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    _register_policy_without_classifying(store)
    first = store.admit_manual_trigger(
        _manual_request("om_manual_before_kafka"),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
    )
    _terminalize_permanent(store, first.submission_key)
    rerun = store.admit_manual_trigger(
        _manual_request(
            "om_manual_rerun_before_kafka",
            mode="rerun",
            thread_id="topic:om_manual_rerun_before_kafka_root",
        ),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
        operator_authorized=True,
    )

    kafka = store.process_event(_record().event_uid)

    assert first.generation == 1
    assert rerun.generation == 2
    assert kafka.decision == "deduped"
    assert kafka.generation == 1
    assert kafka.submission_key == first.submission_key
    kafka_source = next(
        row
        for row in store.list_rows("rca_trigger_sources")
        if row["kafka_event_uid"] == kafka.event_uid
    )
    assert kafka_source["mode"] == "issue_created"
    kafka_binding = next(
        row
        for row in store.list_rows("rca_trigger_bindings")
        if row["source_id"] == kafka_source["source_id"]
    )
    assert kafka_binding["generation"] == 1
    bound_subscriptions = {
        row["subscription_key"]
        for row in store.list_rows("rca_trigger_delivery_bindings")
        if row["source_id"] == kafka_source["source_id"]
    }
    subscriptions = {
        row["subscription_key"]: row
        for row in store.list_rows("rca_delivery_subscriptions")
    }
    assert {
        subscriptions[key]["generation"] for key in bound_subscriptions
    } == {1}


def test_kafka_first_then_manual_joins_without_rewriting_kafka_origin(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    kafka = store.ingest_record(_record(), policy=_policy(), submit_enabled=True)
    [before_outbox] = store.list_rows("rca_outbox")
    before_payload = before_outbox["payload_json"]
    kafka_origin = before_outbox["origin_source_id"]

    manual = store.admit_manual_trigger(
        _manual_request("om_manual_observer"),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
    )

    assert manual.outcome == "joined"
    assert manual.submission_key == kafka.submission_key
    [outbox] = store.list_rows("rca_outbox")
    [trigger] = store.list_rows("business_triggers")
    assert outbox["payload_json"] == before_payload
    assert outbox["origin_source_id"] == kafka_origin
    assert trigger["origin_source_id"] == kafka_origin
    manual_binding = next(
        row
        for row in store.list_rows("rca_trigger_bindings")
        if row["source_id"] == manual.source_id
    )
    assert manual_binding["role"] == "observer"
    subscriptions = store.list_rows("rca_delivery_subscriptions")
    assert {row["effect_kind"] for row in subscriptions} == {
        "feishu_issue_comment",
        "feishu_thread_reply",
    }


def test_manual_active_policy_snapshot_bootstraps_empty_store(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")

    manual = store.admit_manual_trigger(
        _manual_request("om_empty_store_policy"),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
        active_policy=_policy(),
    )

    assert manual.outcome == "created"
    [active] = store.list_rows("rca_policy_snapshots")
    assert active["active"] == 1
    assert active["policy_version"] == "issue-created-v1"


def test_different_allowed_groups_converge_on_one_issue_generation(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    _register_policy_without_classifying(store)
    first_request = ManualRcaTriggerRequest(
        **{
            **_manual_request(
                "om_group_a", thread_id="topic:om_group_a_root"
            ).to_dict(),
            "chat_id": "oc_group_a",
        }
    )
    second_request = ManualRcaTriggerRequest(
        **{
            **_manual_request(
                "om_group_b", thread_id="topic:om_group_b_root"
            ).to_dict(),
            "chat_id": "oc_group_b",
        }
    )
    first = store.admit_manual_trigger(
        first_request,
        allowed_chat_ids={"oc_group_a", "oc_group_b"},
        submit_enabled=True,
    )
    second = store.admit_manual_trigger(
        second_request,
        allowed_chat_ids={"oc_group_a", "oc_group_b"},
        submit_enabled=True,
    )

    assert second.business_key == first.business_key
    assert second.submission_key == first.submission_key
    assert second.generation == first.generation == 1
    subscriptions = store.list_rows("rca_delivery_subscriptions")
    assert sum(row["effect_kind"] == "feishu_issue_comment" for row in subscriptions) == 1
    assert sum(row["effect_kind"] == "feishu_thread_reply" for row in subscriptions) == 2


def test_manual_active_policy_snapshot_overrides_stale_policy(
    tmp_path,
):
    path = tmp_path / "control.sqlite3"
    store = _steady_control_store(path)
    store.persist_raw(
        _record(offset=9, value=_value(work_item_id=7041712800)),
        policy=_policy(policy_version="issue-created-v1"),
        submit_enabled=False,
    )
    current_policy = _policy(policy_version="issue-created-v2")

    manual = store.admit_manual_trigger(
        _manual_request("om_policy_v2"),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
        active_policy=current_policy,
    )
    kafka = store.ingest_record(
        _record(),
        policy=current_policy,
        submit_enabled=True,
    )

    assert manual.outcome == "created"
    assert kafka.decision == "deduped"
    assert kafka.submission_key == manual.submission_key
    [trigger] = store.list_rows("business_triggers")
    [outbox] = store.list_rows("rca_outbox")
    assert trigger["creation_rule_version"] == "issue-created-v2"
    assert outbox["creation_rule_version"] == "issue-created-v2"
    active = [
        row for row in store.list_rows("rca_policy_snapshots") if row["active"] == 1
    ]
    assert len(active) == 1
    assert active[0]["policy_version"] == "issue-created-v2"


@pytest.mark.parametrize(
    ("initial_state", "mode", "expected_outcome", "expected_generation", "outboxes"),
    (
        ("pending", "run_or_join", "joined", 1, 1),
        ("pending", "rerun", "joined", 1, 1),
        ("pending", "debug", "joined", 1, 1),
    ),
)
def test_manual_cross_rule_uses_first_issue_chain_and_global_generation(
    tmp_path,
    initial_state,
    mode,
    expected_outcome,
    expected_generation,
    outboxes,
):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    first = store.ingest_record(
        _record(),
        policy=_policy(policy_version="issue-created-v1"),
        submit_enabled=initial_state != "shadow",
    )
    if initial_state == "terminal":
        _terminalize_permanent(store, first.submission_key)

    manual = store.admit_manual_trigger(
        _manual_request(f"om_cross_rule_{initial_state}_{mode}", mode=mode),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
        operator_authorized=mode in {"rerun", "debug"},
        active_policy=_policy(policy_version="issue-created-v2"),
    )

    assert manual.outcome == expected_outcome
    assert manual.generation == expected_generation
    assert manual.business_key == first.business_key
    rows = store.list_rows("rca_outbox")
    assert len(rows) == outboxes
    assert {row["creation_rule_version"] for row in rows} == {"issue-created-v1"}
    assert [row["generation"] for row in rows] == list(range(1, outboxes + 1))
    policy_audits = [
        row
        for row in store.list_rows("rca_shadow_promotion_audit")
        if row["outcome"] == "manual_active_policy_observed"
    ]
    assert len(policy_audits) == 1
    assert policy_audits[0]["event_uid"] == manual.source_id
    assert policy_audits[0]["submission_key"] == manual.submission_key
    assert policy_audits[0]["from_status"] == "issue-created-v1"
    assert policy_audits[0]["to_status"] == "issue-created-v2"


def test_terminal_debug_requires_explicit_user_rerun(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    _register_policy_without_classifying(store)
    first = store.admit_manual_trigger(
        _manual_request("om_terminal_debug_initial"),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
    )
    _terminalize_permanent(store, first.submission_key)
    before = len(store.list_rows("business_triggers"))

    with pytest.raises(
        ManualRcaAdmissionError,
        match="manual_generation_requires_explicit_user_rerun",
    ):
        store.admit_manual_trigger(
            _manual_request("om_terminal_debug", mode="debug"),
            allowed_chat_ids={"oc_allowed"},
            submit_enabled=True,
            operator_authorized=True,
        )

    assert len(store.list_rows("business_triggers")) == before


@pytest.mark.parametrize("initial_state", ["pending", "shadow", "terminal"])
def test_later_kafka_rule_is_observer_of_existing_issue_chain(
    tmp_path, initial_state
):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    first = store.ingest_record(
        _record(10),
        policy=_policy(policy_version="issue-created-v1"),
        submit_enabled=initial_state != "shadow",
    )
    if initial_state == "terminal":
        _terminalize_permanent(store, first.submission_key)
    before = store.list_rows("rca_outbox")[0]

    later = store.ingest_record(
        _record(11, value=_value(updated_at=1783659999999)),
        policy=_policy(policy_version="issue-created-v2"),
        submit_enabled=True,
    )

    assert later.decision == "deduped"
    assert later.business_key == first.business_key
    assert later.submission_key == first.submission_key
    assert later.generation == 1
    [after] = store.list_rows("rca_outbox")
    assert after["origin_source_id"] == before["origin_source_id"]
    assert after["payload_json"] == before["payload_json"]
    assert after["status"] == before["status"]
    later_source = next(
        row
        for row in store.list_rows("rca_trigger_sources")
        if row["kafka_event_uid"] == later.event_uid
    )
    later_binding = next(
        row
        for row in store.list_rows("rca_trigger_bindings")
        if row["source_id"] == later_source["source_id"]
    )
    assert later_binding["role"] == "observer"


@pytest.mark.parametrize("first_source", ["kafka", "manual"])
def test_cross_rule_kafka_first_or_manual_first_has_one_generation(
    tmp_path, first_source
):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    if first_source == "kafka":
        first = store.ingest_record(
            _record(),
            policy=_policy(policy_version="issue-created-v1"),
            submit_enabled=True,
        )
        second = store.admit_manual_trigger(
            _manual_request("om_kafka_first_v2"),
            allowed_chat_ids={"oc_allowed"},
            submit_enabled=True,
            active_policy=_policy(policy_version="issue-created-v2"),
        )
    else:
        first = store.admit_manual_trigger(
            _manual_request("om_manual_first_v1"),
            allowed_chat_ids={"oc_allowed"},
            submit_enabled=True,
            active_policy=_policy(policy_version="issue-created-v1"),
        )
        second = store.ingest_record(
            _record(),
            policy=_policy(policy_version="issue-created-v2"),
            submit_enabled=True,
        )

    assert first.business_key == second.business_key
    assert first.submission_key == second.submission_key
    assert len(store.list_rows("business_triggers")) == 1
    assert len(store.list_rows("rca_outbox")) == 1
    assert {row["role"] for row in store.list_rows("rca_trigger_bindings")} == {
        "origin",
        "observer",
    }


def test_concurrent_cross_rule_manual_admission_creates_one_issue_chain(tmp_path):
    path = tmp_path / "control.sqlite3"
    _steady_control_store(path)

    def admit(index):
        version = f"issue-created-v{index + 1}"
        return _steady_control_store(path).admit_manual_trigger(
            _manual_request(
                f"om_concurrent_rule_{index}",
                thread_id=f"topic:om_concurrent_rule_root_{index}",
            ),
            allowed_chat_ids={"oc_allowed"},
            submit_enabled=True,
            active_policy=_policy(policy_version=version),
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(admit, range(2)))

    assert [result.outcome for result in results].count("created") == 1
    assert [result.outcome for result in results].count("joined") == 1
    assert len({result.business_key for result in results}) == 1
    store = _steady_control_store(path)
    assert len(store.list_rows("business_triggers")) == 1
    assert len(store.list_rows("rca_outbox")) == 1


def test_concurrent_cross_rule_kafka_and_manual_create_one_issue_chain(tmp_path):
    path = tmp_path / "control.sqlite3"
    store = _steady_control_store(path)

    def kafka():
        return _steady_control_store(path).ingest_record(
            _record(),
            policy=_policy(policy_version="issue-created-v1"),
            submit_enabled=True,
        )

    def manual():
        return _steady_control_store(path).admit_manual_trigger(
            _manual_request("om_concurrent_kafka_manual"),
            allowed_chat_ids={"oc_allowed"},
            submit_enabled=True,
            active_policy=_policy(policy_version="issue-created-v2"),
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        kafka_result = pool.submit(kafka)
        manual_result = pool.submit(manual)
        results = [kafka_result.result(), manual_result.result()]

    assert len({result.business_key for result in results}) == 1
    assert len({result.submission_key for result in results}) == 1
    assert len(store.list_rows("business_triggers")) == 1
    assert len(store.list_rows("rca_outbox")) == 1
    assert {row["role"] for row in store.list_rows("rca_trigger_bindings")} == {
        "origin",
        "observer",
    }


def test_manual_issue_scope_history_conflict_is_audited_and_fails_closed(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    store.ingest_record(
        _record(), policy=_policy(policy_version="issue-created-v1"), submit_enabled=True
    )
    _inject_conflicting_issue_scope_chain(store)
    before_sources = len(store.list_rows("rca_trigger_sources"))

    with pytest.raises(
        ManualRcaAdmissionError,
        match="manual_issue_scope_business_key_conflict",
    ):
        store.admit_manual_trigger(
            _manual_request("om_scope_conflict"),
            allowed_chat_ids={"oc_allowed"},
            submit_enabled=True,
            active_policy=_policy(policy_version="issue-created-v3"),
        )

    assert len(store.list_rows("rca_trigger_sources")) == before_sources
    assert len(store.list_rows("business_triggers")) == 2
    assert len(store.list_rows("rca_outbox")) == 2
    [audit] = [
        row
        for row in store.list_rows("rca_shadow_promotion_audit")
        if row["outcome"] == "issue_scope_business_key_conflict"
    ]
    assert audit["to_status"] == "blocked"
    assert "7041712812" not in audit["detail"]
    assert audit["outcome"] not in {"promoted", "already_promoted"}


def test_kafka_issue_scope_history_conflict_is_invalid_dead_lettered_and_audited(
    tmp_path,
):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    store.ingest_record(
        _record(10),
        policy=_policy(policy_version="issue-created-v1"),
        submit_enabled=True,
    )
    _inject_conflicting_issue_scope_chain(store)

    result = store.ingest_record(
        _record(11, value=_value(updated_at=1783659999999)),
        policy=_policy(policy_version="issue-created-v3"),
        submit_enabled=True,
    )

    assert result.decision == "invalid"
    assert result.reason == "issue_scope_business_key_conflict"
    assert result.business_key == ""
    assert len(store.list_rows("business_triggers")) == 2
    assert len(store.list_rows("rca_outbox")) == 2
    [dead_letter] = store.list_rows("kafka_dead_letters")
    assert dead_letter["source_event_id"] == result.event_uid
    assert dead_letter["error_code"] == "issue_scope_business_key_conflict"
    [audit] = [
        row
        for row in store.list_rows("rca_shadow_promotion_audit")
        if row["event_uid"] == result.event_uid
    ]
    assert audit["outcome"] == "issue_scope_business_key_conflict"
    assert audit["outcome"] not in {"promoted", "already_promoted"}


def test_manual_high_water_blocks_new_execution_but_allows_existing_join(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    kafka = store.ingest_record(_record(), policy=_policy(), submit_enabled=True)

    joined = store.admit_manual_trigger(
        _manual_request("om_join_at_high_water"),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
        active_policy=_policy(),
        outbox_high_watermark=1,
    )
    before_sources = len(store.list_rows("rca_trigger_sources"))
    with pytest.raises(
        ManualRcaAdmissionError, match="manual_outbox_high_watermark_reached"
    ):
        store.admit_manual_trigger(
            _manual_request(
                "om_new_at_high_water",
                issue_url="https://project.feishu.cn/g1q3/issue/detail/7041712813",
            ),
            allowed_chat_ids={"oc_allowed"},
            submit_enabled=True,
            active_policy=_policy(),
            outbox_high_watermark=1,
        )

    assert joined.outcome == "joined"
    assert joined.submission_key == kafka.submission_key
    assert len(store.list_rows("business_triggers")) == 1
    assert len(store.list_rows("rca_outbox")) == 1
    assert len(store.list_rows("rca_trigger_sources")) == before_sources


def test_manual_source_quota_reserves_shared_outbox_capacity_for_kafka(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    for index in range(4):
        result = store.admit_manual_trigger(
            _manual_request(
                f"om_manual_quota_{index}",
                thread_id=f"topic:om_manual_quota_root_{index}",
                issue_url=(
                    "https://project.feishu.cn/g1q3/issue/detail/"
                    f"{7041713000 + index}"
                ),
            ),
            allowed_chat_ids={"oc_allowed"},
            submit_enabled=True,
            active_policy=_policy(),
            outbox_high_watermark=5,
        )
        assert result.outcome == "created"

    with pytest.raises(
        ManualRcaAdmissionError, match="manual_outbox_source_quota_reached"
    ):
        store.admit_manual_trigger(
            _manual_request(
                "om_manual_quota_rejected",
                thread_id="topic:om_manual_quota_rejected_root",
                issue_url=(
                    "https://project.feishu.cn/g1q3/issue/detail/7041713004"
                ),
            ),
            allowed_chat_ids={"oc_allowed"},
            submit_enabled=True,
            active_policy=_policy(),
            outbox_high_watermark=5,
        )

    kafka = store.ingest_record(
        _record(
            offset=30,
            value=_value(work_item_id=7041714000),
        ),
        policy=_policy(),
        submit_enabled=True,
    )
    assert kafka.decision == "accepted"
    assert len(store.list_rows("rca_outbox")) == 5


def test_outbox_claims_are_weighted_fair_between_kafka_and_manual(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    for index in range(5):
        result = store.ingest_record(
            _record(
                offset=40 + index,
                value=_value(work_item_id=7041715000 + index),
            ),
            policy=_policy(),
            submit_enabled=True,
        )
        assert result.decision == "accepted"
    for index in range(2):
        result = store.admit_manual_trigger(
            _manual_request(
                f"om_fair_manual_{index}",
                thread_id=f"topic:om_fair_manual_root_{index}",
                issue_url=(
                    "https://project.feishu.cn/g1q3/issue/detail/"
                    f"{7041716000 + index}"
                ),
            ),
            allowed_chat_ids={"oc_allowed"},
            submit_enabled=True,
            active_policy=_policy(),
        )
        assert result.outcome == "created"

    claims = [
        store.claim_outbox(lease_owner=f"fair-worker-{index}")
        for index in range(5)
    ]
    assert all(claim is not None for claim in claims)
    assert [bool(claim.source_topic) for claim in claims if claim is not None] == [
        True,
        True,
        True,
        False,
        True,
    ]


def test_concurrent_manual_creates_cannot_overshoot_high_watermark(tmp_path):
    path = tmp_path / "control.sqlite3"
    _steady_control_store(path)

    def admit(index):
        try:
            return _steady_control_store(path).admit_manual_trigger(
                _manual_request(
                    f"om_high_water_{index}",
                    thread_id=f"topic:om_high_water_root_{index}",
                    issue_url=(
                        "https://project.feishu.cn/g1q3/issue/detail/"
                        f"{7041712900 + index}"
                    ),
                ),
                allowed_chat_ids={"oc_allowed"},
                submit_enabled=True,
                active_policy=_policy(),
                outbox_high_watermark=1,
            )
        except ManualRcaAdmissionError as exc:
            return str(exc)

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(admit, range(8)))

    admitted = [item for item in results if not isinstance(item, str)]
    rejected = [item for item in results if isinstance(item, str)]
    assert len(admitted) == 1
    assert rejected == ["manual_outbox_high_watermark_reached"] * 7
    store = _steady_control_store(path)
    assert len(store.list_rows("business_triggers")) == 1
    assert len(store.list_rows("rca_outbox")) == 1
    assert len(store.list_rows("rca_trigger_sources")) == 1


def test_manual_storage_reserve_blocks_join_without_partial_source_or_subscription(
    tmp_path, monkeypatch
):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    kafka = store.ingest_record(_record(), policy=_policy(), submit_enabled=True)
    before = {
        table: len(store.list_rows(table))
        for table in (
            "rca_trigger_sources",
            "rca_trigger_bindings",
            "rca_delivery_subscriptions",
            "rca_trigger_delivery_bindings",
        )
    }

    class LowDisk:
        f_bavail = 1
        f_frsize = 4096

    monkeypatch.setattr(control_store_module.os, "statvfs", lambda _path: LowDisk())
    with pytest.raises(
        ManualRcaAdmissionError, match="manual_control_store_capacity_below_reserve"
    ):
        store.admit_manual_trigger(
            _manual_request("om_join_low_disk"),
            allowed_chat_ids={"oc_allowed"},
            submit_enabled=True,
            active_policy=_policy(),
        )

    assert kafka.submission_key == store.list_rows("rca_outbox")[0]["submission_key"]
    assert {
        table: len(store.list_rows(table))
        for table in before
    } == before


def test_manual_idempotent_replay_remains_read_only_below_disk_reserve(
    tmp_path, monkeypatch
):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    request = _manual_request("om_low_disk_replay")
    first = store.admit_manual_trigger(
        request,
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
        active_policy=_policy(),
    )

    class LowDisk:
        f_bavail = 1
        f_frsize = 4096

    monkeypatch.setattr(control_store_module.os, "statvfs", lambda _path: LowDisk())
    replay = store.admit_manual_trigger(
        request,
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
        active_policy=_policy(),
    )

    assert replay.outcome == first.outcome == "created"
    assert replay.reason == "idempotent_source_replay"
    assert len(store.list_rows("rca_trigger_sources")) == 1
    assert len(store.list_rows("rca_trigger_delivery_bindings")) == 2


def test_concurrent_manual_run_or_join_creates_one_outbox_and_one_topic_subscription(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    _register_policy_without_classifying(store)

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(
            pool.map(
                lambda index: store.admit_manual_trigger(
                    _manual_request(f"om_manual_{index}"),
                    allowed_chat_ids={"oc_allowed"},
                    submit_enabled=True,
                ),
                range(8),
            )
        )

    assert [row.outcome for row in results].count("created") == 1
    assert {row.submission_key for row in results} == {results[0].submission_key}
    assert len(store.list_rows("business_triggers")) == 1
    assert len(store.list_rows("rca_outbox")) == 1
    subscriptions = store.list_rows("rca_delivery_subscriptions")
    assert len(subscriptions) == 2
    assert len(store.list_rows("rca_trigger_sources")) == 8


def test_same_topic_two_manual_sources_have_one_subscription_and_two_bindings(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    _register_policy_without_classifying(store)
    first = store.admit_manual_trigger(
        _manual_request("om_topic_first", requester_id="ou_first"),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
    )
    second = store.admit_manual_trigger(
        _manual_request("om_topic_second", requester_id="ou_second"),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
    )

    assert first.subscription_key == second.subscription_key
    thread_subscriptions = [
        row
        for row in store.list_rows("rca_delivery_subscriptions")
        if row["effect_kind"] == "feishu_thread_reply"
    ]
    assert len(thread_subscriptions) == 1
    bindings = [
        row
        for row in store.list_rows("rca_trigger_delivery_bindings")
        if row["subscription_key"] == first.subscription_key
    ]
    assert {row["source_id"] for row in bindings} == {
        first.source_id,
        second.source_id,
    }
    assert len(bindings) == 2


def test_two_concurrent_terminal_reruns_create_only_generation_two(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    _register_policy_without_classifying(store)
    first = store.admit_manual_trigger(
        _manual_request("om_initial"),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
    )
    _terminalize_permanent(store, first.submission_key)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(
            pool.map(
                lambda index: store.admit_manual_trigger(
                    _manual_request(
                        f"om_rerun_{index}",
                        mode="rerun",
                        thread_id=f"topic:om_rerun_root_{index}",
                    ),
                    allowed_chat_ids={"oc_allowed"},
                    submit_enabled=True,
                    operator_authorized=True,
                ),
                range(2),
            )
        )

    assert {row.generation for row in results} == {2}
    assert [row.outcome for row in results].count("created") == 1
    assert [row.outcome for row in results].count("joined") == 1
    assert [row["generation"] for row in store.list_rows("business_triggers")] == [1, 2]
    assert len(store.list_rows("rca_outbox")) == 2


def test_manual_input_wait_rearms_same_generation_without_changing_origin(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    _register_policy_without_classifying(store)
    first = store.admit_manual_trigger(
        _manual_request("om_waiting"),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
    )
    _quarantine_for_input_wait(store)
    [before] = store.list_rows("rca_outbox")

    resumed = store.admit_manual_trigger(
        _manual_request("om_resume", thread_id="topic:om_resume_root"),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
    )

    assert resumed.outcome == "rearmed"
    assert resumed.generation == 1
    assert resumed.submission_key == first.submission_key
    [after] = store.list_rows("rca_outbox")
    assert after["status"] == "pending"
    assert after["origin_source_id"] == first.source_id
    assert after["payload_json"] == before["payload_json"]


def test_delivery_created_with_required_thread_pending_cannot_start_new_generation(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    _register_policy_without_classifying(store)
    first = store.admit_manual_trigger(
        _manual_request("om_delivery_origin"),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
    )
    subscriptions = store.list_rows("rca_delivery_subscriptions")
    conn = store._connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.executescript(
            """
            CREATE TABLE rca_execution_watch(
                submission_key TEXT PRIMARY KEY,
                state TEXT NOT NULL
            );
            CREATE TABLE rca_delivery_jobs(
                delivery_id TEXT PRIMARY KEY,
                business_key TEXT NOT NULL,
                generation INTEGER NOT NULL,
                status TEXT NOT NULL
            );
            CREATE TABLE rca_delivery_effects(
                effect_key TEXT PRIMARY KEY,
                delivery_id TEXT NOT NULL,
                required INTEGER NOT NULL,
                status TEXT NOT NULL
            );
            """
        )
        conn.execute(
            "INSERT INTO rca_execution_watch VALUES(?, 'delivery_created')",
            (first.submission_key,),
        )
        conn.execute(
            "INSERT INTO rca_delivery_jobs VALUES('delivery-1', ?, 1, 'delivered')",
            (first.business_key,),
        )
        for index, subscription in enumerate(subscriptions):
            effect_key = f"effect-{index}"
            conn.execute(
                """
                UPDATE rca_delivery_subscriptions
                   SET status='materialized', delivery_id='delivery-1', effect_key=?
                 WHERE subscription_key=?
                """,
                (effect_key, subscription["subscription_key"]),
            )
            conn.execute(
                "INSERT INTO rca_delivery_effects VALUES(?, 'delivery-1', 1, ?)",
                (
                    effect_key,
                    "pending"
                    if subscription["effect_kind"] == "feishu_thread_reply"
                    else "succeeded",
                ),
            )
        conn.commit()
    finally:
        conn.close()

    rerun = store.admit_manual_trigger(
        _manual_request(
            "om_delivery_rerun",
            mode="rerun",
            thread_id="topic:om_delivery_rerun_root",
        ),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
        operator_authorized=True,
    )
    debug = store.admit_manual_trigger(
        _manual_request(
            "om_delivery_debug",
            mode="debug",
            thread_id="topic:om_delivery_debug_root",
        ),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
        operator_authorized=True,
    )

    assert rerun.generation == debug.generation == 1
    assert rerun.outcome == debug.outcome == "catchup_attached"
    assert len(store.list_rows("business_triggers")) == 1
    assert len(store.list_rows("rca_outbox")) == 1


def test_manual_source_replay_is_idempotent_and_payload_change_conflicts(tmp_path):
    path = tmp_path / "control.sqlite3"
    store = _steady_control_store(path)
    _register_policy_without_classifying(store)
    request = _manual_request("om_idempotent")
    first = store.admit_manual_trigger(
        request, allowed_chat_ids={"oc_allowed"}, submit_enabled=True
    )
    issue_subscription_key = next(
        row["subscription_key"]
        for row in store.list_rows("rca_delivery_subscriptions")
        if row["effect_kind"] == "feishu_issue_comment"
    )
    conn = store._connect()
    try:
        conn.execute(
            "DELETE FROM rca_trigger_delivery_bindings "
            "WHERE source_id = ? AND subscription_key = ?",
            (first.source_id, issue_subscription_key),
        )
    finally:
        conn.close()

    replay = _steady_control_store(path).admit_manual_trigger(
        request, allowed_chat_ids={"oc_allowed"}, submit_enabled=True
    )
    assert replay.outcome == first.outcome
    assert replay.submission_key == first.submission_key
    assert replay.reason == "idempotent_source_replay"
    subscriptions = {
        row["subscription_key"]: row["effect_kind"]
        for row in store.list_rows("rca_delivery_subscriptions")
    }
    source_bindings = [
        row
        for row in store.list_rows("rca_trigger_delivery_bindings")
        if row["source_id"] == first.source_id
    ]
    assert {subscriptions[row["subscription_key"]] for row in source_bindings} == {
        "feishu_issue_comment",
        "feishu_thread_reply",
    }
    assert len(source_bindings) == 2

    changed = ManualRcaTriggerRequest(**{**request.to_dict(), "reason": "changed"})
    with pytest.raises(ManualRcaAdmissionError, match="payload_conflict"):
        store.admit_manual_trigger(
            changed, allowed_chat_ids={"oc_allowed"}, submit_enabled=True
        )


def test_late_thread_subscription_is_marked_pending_for_delivery_catchup(tmp_path):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    _register_policy_without_classifying(store)
    first = store.admit_manual_trigger(
        _manual_request("om_first_topic"),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
    )
    conn = store._connect()
    try:
        conn.execute(
            """
            CREATE TABLE rca_delivery_jobs(
                delivery_id TEXT PRIMARY KEY,
                business_key TEXT NOT NULL,
                generation INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            "INSERT INTO rca_delivery_jobs VALUES('delivery-1', ?, ?)",
            (first.business_key, first.generation),
        )
    finally:
        conn.close()

    late = store.admit_manual_trigger(
        _manual_request(
            "om_late_topic",
            thread_id="topic:om_late_root",
        ),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
    )

    assert late.outcome == "catchup_attached"
    subscription = next(
        row
        for row in store.list_rows("rca_delivery_subscriptions")
        if row["subscription_key"] == late.subscription_key
    )
    assert subscription["status"] == "pending"
    assert subscription["delivery_id"] == "delivery-1"
    assert subscription["catchup_requested_at"]
    assert subscription["effect_key"] is None


def test_quarantined_late_thread_subscription_fails_instead_of_claiming_catchup(
    tmp_path,
):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    _register_policy_without_classifying(store)
    first = store.admit_manual_trigger(
        _manual_request("om_first_quarantined_topic"),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
    )
    conn = store._connect()
    try:
        conn.execute(
            """
            CREATE TABLE rca_delivery_jobs(
                delivery_id TEXT PRIMARY KEY,
                business_key TEXT NOT NULL,
                generation INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            "INSERT INTO rca_delivery_jobs VALUES('delivery-q', ?, ?)",
            (first.business_key, first.generation),
        )
        conn.execute(
            """
            UPDATE rca_delivery_subscriptions
               SET status = 'quarantined'
             WHERE subscription_key = ?
            """,
            (first.subscription_key,),
        )
    finally:
        conn.close()

    with pytest.raises(
        ManualRcaAdmissionError, match="manual_late_catchup_unavailable"
    ):
        store.admit_manual_trigger(
            _manual_request("om_quarantined_catchup_retry"),
            allowed_chat_ids={"oc_allowed"},
            submit_enabled=True,
        )

    assert len(store.list_rows("rca_trigger_sources")) == 1
    subscription = next(
        row
        for row in store.list_rows("rca_delivery_subscriptions")
        if row["subscription_key"] == first.subscription_key
    )
    assert subscription["status"] == "quarantined"
    assert subscription["delivery_id"] is None
    assert subscription["catchup_requested_at"] is None


def test_operator_rate_limit_is_durable_but_run_or_join_and_replay_remain_open(
    tmp_path,
):
    store = _steady_control_store(tmp_path / "control.sqlite3")
    _register_policy_without_classifying(store)
    started = datetime(2026, 7, 11, 1, 0, tzinfo=timezone.utc)
    first_request = _manual_request(
        "om_debug_rate_1",
        mode="debug",
        issue_url="https://project.feishu.cn/g1q3/issue/detail/7041712813",
        thread_id="topic:om_debug_rate_root_1",
    )
    first = store.admit_manual_trigger(
        first_request,
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
        operator_authorized=True,
        operator_rate_limit=2,
        operator_rate_window_seconds=60,
        now=started,
    )
    store.admit_manual_trigger(
        _manual_request(
            "om_debug_rate_2",
            mode="debug",
            issue_url="https://project.feishu.cn/g1q3/issue/detail/7041712814",
            thread_id="topic:om_debug_rate_root_2",
        ),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
        operator_authorized=True,
        operator_rate_limit=2,
        operator_rate_window_seconds=60,
        now=started + timedelta(seconds=1),
    )

    replay = store.admit_manual_trigger(
        first_request,
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
        operator_authorized=True,
        operator_rate_limit=2,
        operator_rate_window_seconds=60,
        now=started + timedelta(seconds=2),
    )
    with pytest.raises(ManualRcaAdmissionError, match="manual_operator_rate_limited"):
        store.admit_manual_trigger(
            _manual_request(
                "om_debug_rate_3",
                mode="debug",
                issue_url="https://project.feishu.cn/g1q3/issue/detail/7041712815",
                thread_id="topic:om_debug_rate_root_3",
            ),
            allowed_chat_ids={"oc_allowed"},
            submit_enabled=True,
            operator_authorized=True,
            operator_rate_limit=2,
            operator_rate_window_seconds=60,
            now=started + timedelta(seconds=2),
        )

    ordinary = store.admit_manual_trigger(
        _manual_request(
            "om_ordinary_after_rate",
            issue_url="https://project.feishu.cn/g1q3/issue/detail/7041712816",
            thread_id="topic:om_ordinary_after_rate_root",
        ),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
        now=started + timedelta(seconds=3),
    )
    after_window = store.admit_manual_trigger(
        _manual_request(
            "om_debug_rate_after_window",
            mode="debug",
            issue_url="https://project.feishu.cn/g1q3/issue/detail/7041712815",
            thread_id="topic:om_debug_rate_after_window_root",
        ),
        allowed_chat_ids={"oc_allowed"},
        submit_enabled=True,
        operator_authorized=True,
        operator_rate_limit=2,
        operator_rate_window_seconds=60,
        now=started + timedelta(seconds=62),
    )

    assert replay.source_id == first.source_id
    assert ordinary.outcome == "created"
    assert after_window.outcome == "created"
    operator_sources = [
        row
        for row in store.list_rows("rca_trigger_sources")
        if row["mode"] in {"rerun", "debug"}
    ]
    assert len(operator_sources) == 3


def test_control_store_connections_require_recursive_triggers(tmp_path):
    path = tmp_path / "control.sqlite3"
    store = RcaControlStore(path)
    conn = store._connect()
    try:
        assert conn.execute("PRAGMA recursive_triggers").fetchone()[0] == 1
        conn.execute("PRAGMA recursive_triggers=OFF")
        with pytest.raises(
            RuntimeError,
            match="incompatible_control_store_schema:recursive_triggers",
        ):
            store._validate_structural_contract(conn, integrity_check=False)
    finally:
        conn.close()


V11_SNAPSHOT_TABLES = {
    "rca_canonical_requests",
    "rca_admission_snapshots",
    "rca_source_authority_receipts",
    "rca_snapshot_source_envelopes",
}
V12_LEARNING_TABLES = {
    "rca_learning_lane_admissions",
    "rca_learning_lane_stock_items",
    "rca_learning_lane_cohorts",
}
V13_HISTORICAL_HOLD_TABLES = {
    "rca_activation_historical_outbox_disposition_items",
    "rca_activation_historical_outbox_dispositions",
    "rca_activation_historical_outbox_hold_items",
    "rca_activation_historical_outbox_holds",
}
RETIRED_CONTROL_TABLES = V13_HISTORICAL_HOLD_TABLES | {
    "rca_activation_budget_slots",
    "rca_capacity_transition_state",
    "rca_capacity_transition_audit",
}
RETIRED_CONTROL_INDEXES = {
    "idx_rca_activation_slot_identity",
    "idx_rca_capacity_transition_audit_time",
}
V14_TERMINAL_RERUN_TABLES = {"rca_terminal_rerun_delivery_authorities"}
V14_HISTORICAL_EPOCH_RERUN_TABLES = {
    "rca_historical_epoch_rerun_delivery_authorities"
}
V14_RERUN_TABLES = (
    V14_TERMINAL_RERUN_TABLES | V14_HISTORICAL_EPOCH_RERUN_TABLES
)


def _drop_schema_objects(conn, *, tables):
    tables = tuple(tables)
    drop_learning = bool(set(tables) & V12_LEARNING_TABLES)
    drop_historical = bool(set(tables) & V13_HISTORICAL_HOLD_TABLES)
    drop_rerun = bool(set(tables) & V14_RERUN_TABLES)
    conn.execute("PRAGMA foreign_keys=OFF")
    if drop_rerun:
        conn.execute(
            "DROP VIEW IF EXISTS "
            "rca_owner_authorized_rerun_delivery_authorities"
        )
    for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'trigger'"
    ).fetchall():
        name = str(row["name"])
        if (
            drop_learning and name.startswith("trg_learning_lane_")
        ) or (
            drop_historical and name.startswith("trg_activation_historical_")
        ) or (
            drop_rerun
            and name.startswith(
                (
                    "trg_terminal_rerun_delivery_authority_",
                    "trg_historical_epoch_rerun_delivery_authority_",
                )
            )
        ):
            conn.execute(f"DROP TRIGGER {name}")
    for table in tables:
        conn.execute(f"DROP TABLE IF EXISTS {table}")


def _downgrade_current_store_to_v10(store):
    conn = sqlite3.connect(store.db_path, isolation_level=None)
    conn.row_factory = sqlite3.Row
    try:
        _drop_schema_objects(
            conn,
            tables=(
                *V13_HISTORICAL_HOLD_TABLES,
                *V12_LEARNING_TABLES,
                *V14_RERUN_TABLES,
            ),
        )
        for table in (
            "rca_snapshot_source_envelopes",
            "rca_admission_snapshots",
            "rca_source_authority_receipts",
            "rca_canonical_requests",
        ):
            conn.execute(f"DROP TABLE {table}")
        conn.execute(
            "UPDATE control_meta SET value='pnc_rca_control_store_v10' "
            "WHERE key='schema_version'"
        )
    finally:
        conn.close()


def _downgrade_current_store_to_v12(store):
    conn = sqlite3.connect(store.db_path, isolation_level=None)
    conn.row_factory = sqlite3.Row
    try:
        _drop_schema_objects(
            conn,
            tables=(*V13_HISTORICAL_HOLD_TABLES, *V14_RERUN_TABLES),
        )
        conn.execute(
            "UPDATE control_meta SET value='pnc_rca_control_store_v12' "
            "WHERE key='schema_version'"
        )
    finally:
        conn.close()


def _downgrade_current_store_to_v11(store):
    conn = sqlite3.connect(store.db_path, isolation_level=None)
    conn.row_factory = sqlite3.Row
    try:
        _drop_schema_objects(
            conn,
            tables=(
                *V13_HISTORICAL_HOLD_TABLES,
                *V12_LEARNING_TABLES,
                *V14_RERUN_TABLES,
            ),
        )
        conn.execute(
            "UPDATE control_meta SET value='pnc_rca_control_store_v11' "
            "WHERE key='schema_version'"
        )
    finally:
        conn.close()


def _downgrade_current_store_to_v13(store):
    conn = sqlite3.connect(store.db_path, isolation_level=None)
    conn.row_factory = sqlite3.Row
    try:
        _drop_schema_objects(conn, tables=V14_RERUN_TABLES)
        conn.execute(
            "UPDATE control_meta SET value='pnc_rca_control_store_v13' "
            "WHERE key='schema_version'"
        )
    finally:
        conn.close()


def test_v11_store_migrates_through_v12_to_v13(tmp_path):
    path = tmp_path / "control.sqlite3"
    _downgrade_current_store_to_v11(RcaControlStore(path))

    upgraded = RcaControlStore(path)

    assert upgraded.health()["schema_version"] == CONTROL_STORE_SCHEMA_VERSION
    assert upgraded.initialization_observation() == {
        "mode": "migration",
        "backfill_runs": 0,
    }
    assert all(upgraded.list_rows(table) == [] for table in V12_LEARNING_TABLES)
    with sqlite3.connect(path) as conn:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    assert tables.isdisjoint(V13_HISTORICAL_HOLD_TABLES)


def test_v12_store_migrates_without_recreating_retired_hold_schema(tmp_path):
    path = tmp_path / "control.sqlite3"
    original = _steady_control_store(path)
    accepted = original.ingest_record(
        _record(offset=10), policy=_policy(), submit_enabled=True
    )
    _downgrade_current_store_to_v12(original)

    upgraded = RcaControlStore(path)

    assert upgraded.health()["schema_version"] == CONTROL_STORE_SCHEMA_VERSION
    assert upgraded.initialization_observation() == {
        "mode": "migration",
        "backfill_runs": 0,
    }
    assert upgraded.list_rows("rca_outbox")[0]["submission_key"] == (
        accepted.submission_key
    )
    with sqlite3.connect(path) as conn:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    assert tables.isdisjoint(V13_HISTORICAL_HOLD_TABLES)
    for table in V13_HISTORICAL_HOLD_TABLES:
        with pytest.raises(ValueError, match="unsupported table"):
            upgraded.list_rows(table)


def test_v13_store_migrates_to_v14_terminal_rerun_authority_schema(tmp_path):
    path = tmp_path / "control.sqlite3"
    _downgrade_current_store_to_v13(RcaControlStore(path))
    with sqlite3.connect(path) as conn:
        assert conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' "
            "AND name='rca_historical_epoch_rerun_delivery_authorities'"
        ).fetchone() is None
        assert conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='view' "
            "AND name='rca_owner_authorized_rerun_delivery_authorities'"
        ).fetchone() is None

    with pytest.raises(RuntimeError, match="rca_control_store_schema_not_current"):
        RcaControlStore(path, require_current=True)
    upgraded = RcaControlStore(path)

    assert upgraded.health()["schema_version"] == CONTROL_STORE_SCHEMA_VERSION
    assert upgraded.initialization_observation()["mode"] == "migration"
    assert upgraded.list_rows("rca_terminal_rerun_delivery_authorities") == []
    assert upgraded.list_rows(
        "rca_historical_epoch_rerun_delivery_authorities"
    ) == []
    with sqlite3.connect(path) as conn:
        assert conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='view' "
            "AND name='rca_owner_authorized_rerun_delivery_authorities'"
        ).fetchone() is not None


def test_v13_to_v14_migration_rolls_back_ddl_and_marker(tmp_path, monkeypatch):
    path = tmp_path / "control.sqlite3"
    _downgrade_current_store_to_v13(RcaControlStore(path))
    create_schema = RcaControlStore._create_v14_terminal_rerun_delivery_authority_schema

    def fail_after_ddl(cls, conn):
        create_schema(conn)
        raise RuntimeError("injected_v14_schema_validation_failure")

    monkeypatch.setattr(
        RcaControlStore,
        "_create_v14_terminal_rerun_delivery_authority_schema",
        classmethod(fail_after_ddl),
    )
    with pytest.raises(RuntimeError, match="injected_v14_schema_validation_failure"):
        RcaControlStore(path)

    with sqlite3.connect(path) as conn:
        marker = conn.execute(
            "SELECT value FROM control_meta WHERE key='schema_version'"
        ).fetchone()[0]
        table = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' "
            "AND name='rca_terminal_rerun_delivery_authorities'"
        ).fetchone()
    assert marker == "pnc_rca_control_store_v13"
    assert table is None


def test_current_v14_redefined_terminal_authority_trigger_is_rejected(tmp_path):
    path = tmp_path / "control.sqlite3"
    store = RcaControlStore(path)
    conn = sqlite3.connect(path, isolation_level=None)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute(
            "DROP TRIGGER trg_terminal_rerun_delivery_authority_binding_guard"
        )
        conn.execute(
            """
            CREATE TRIGGER trg_terminal_rerun_delivery_authority_binding_guard
            BEFORE INSERT ON rca_terminal_rerun_delivery_authorities
            WHEN 0
            BEGIN
                SELECT RAISE(
                    ABORT, 'terminal_rerun_delivery_authority_binding_mismatch'
                );
            END
            """
        )
    finally:
        conn.close()

    with pytest.raises(
        RuntimeError,
        match="incompatible_control_store_schema:"
        "terminal_rerun_authority_trigger_sql",
    ):
        RcaControlStore(path, require_current=True)


def test_v12_to_v13_migration_rolls_back_ddl_and_marker_on_validation_failure(
    tmp_path,
    monkeypatch,
):
    path = tmp_path / "control.sqlite3"
    _downgrade_current_store_to_v12(RcaControlStore(path))

    def reject_migrated_schema(_conn, *, integrity_check):
        assert integrity_check is True
        raise RuntimeError("injected_v13_schema_validation_failure")

    monkeypatch.setattr(
        RcaControlStore,
        "_validate_structural_contract",
        staticmethod(reject_migrated_schema),
    )
    with pytest.raises(
        RuntimeError, match="injected_v13_schema_validation_failure"
    ):
        RcaControlStore(path)

    conn = sqlite3.connect(path, isolation_level=None)
    conn.row_factory = sqlite3.Row
    try:
        marker = conn.execute(
            "SELECT value FROM control_meta WHERE key='schema_version'"
        ).fetchone()[0]
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    finally:
        conn.close()
    assert marker == "pnc_rca_control_store_v12"
    assert tables.isdisjoint(V13_HISTORICAL_HOLD_TABLES)


def test_v12_to_v13_migration_rejects_incomplete_v12_learning_schema(tmp_path):
    path = tmp_path / "control.sqlite3"
    store = RcaControlStore(path)
    conn = sqlite3.connect(path, isolation_level=None)
    conn.row_factory = sqlite3.Row
    try:
        _drop_schema_objects(
            conn,
            tables=(*V13_HISTORICAL_HOLD_TABLES, *V12_LEARNING_TABLES),
        )
        conn.execute(
            "UPDATE control_meta SET value='pnc_rca_control_store_v12' "
            "WHERE key='schema_version'"
        )
    finally:
        conn.close()

    with pytest.raises(
        RuntimeError,
        match="incompatible_control_store_schema:rca_learning_lane_cohorts_columns",
    ):
        RcaControlStore(path)

    conn = sqlite3.connect(path)
    try:
        marker = conn.execute(
            "SELECT value FROM control_meta WHERE key='schema_version'"
        ).fetchone()[0]
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    finally:
        conn.close()
    assert marker == "pnc_rca_control_store_v12"
    assert tables.isdisjoint(V13_HISTORICAL_HOLD_TABLES)


@pytest.mark.parametrize(
    ("tables", "error"),
    [
        (
            V12_LEARNING_TABLES,
            "rca_learning_lane_cohorts_columns",
        ),
    ],
)
def test_v13_marker_requires_complete_predecessor_and_current_schema(
    tmp_path,
    tables,
    error,
):
    path = tmp_path / "control.sqlite3"
    store = RcaControlStore(path)
    conn = store._connect()
    try:
        _drop_schema_objects(conn, tables=tables)
        with pytest.raises(
            RuntimeError,
            match=f"incompatible_control_store_schema:{error}",
        ):
            RcaControlStore._validate_structural_contract(
                conn,
                integrity_check=False,
            )
    finally:
        conn.close()

    with pytest.raises(
        RuntimeError,
        match=f"incompatible_control_store_schema:{error}",
    ):
        RcaControlStore(path, require_current=True)


def test_v10_store_migrates_to_empty_inert_v11_snapshot_schema(tmp_path):
    path = tmp_path / "control.sqlite3"
    _downgrade_current_store_to_v10(RcaControlStore(path))

    upgraded = RcaControlStore(path)

    assert upgraded.health()["schema_version"] == CONTROL_STORE_SCHEMA_VERSION
    assert upgraded.initialization_observation() == {
        "mode": "migration",
        "backfill_runs": 0,
    }
    assert all(upgraded.list_rows(table) == [] for table in V11_SNAPSHOT_TABLES)
    conn = upgraded._connect()
    try:
        snapshot_sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' "
            "AND name='rca_admission_snapshots'"
        ).fetchone()["sql"]
    finally:
        conn.close()
    assert "DEFERRABLE INITIALLY DEFERRED" in snapshot_sql


def test_v10_to_v11_migration_rolls_back_all_ddl_and_marker_on_validation_failure(
    tmp_path, monkeypatch
):
    path = tmp_path / "control.sqlite3"
    _downgrade_current_store_to_v10(RcaControlStore(path))

    def reject_migrated_schema(_conn, *, integrity_check):
        assert integrity_check is True
        raise RuntimeError("injected_v11_schema_validation_failure")

    monkeypatch.setattr(
        RcaControlStore,
        "_validate_structural_contract",
        staticmethod(reject_migrated_schema),
    )
    with pytest.raises(
        RuntimeError, match="injected_v11_schema_validation_failure"
    ):
        RcaControlStore(path)

    conn = sqlite3.connect(path)
    try:
        marker = conn.execute(
            "SELECT value FROM control_meta WHERE key='schema_version'"
        ).fetchone()[0]
        v11_tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        } & V11_SNAPSHOT_TABLES
        v11_objects = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master "
                "WHERE name LIKE 'trg_rca_snapshot%' "
                "OR name LIKE 'idx_rca_snapshot%' "
                "OR name LIKE 'trg_rca_source_authority%' "
                "OR name LIKE 'idx_rca_source_authority%'"
            ).fetchall()
        }
    finally:
        conn.close()
    assert marker == "pnc_rca_control_store_v10"
    assert v11_tables == set()
    assert v11_objects == set()


def test_v10_to_v11_marker_last_compare_and_swap_rejects_marker_drift(
    tmp_path, monkeypatch
):
    path = tmp_path / "control.sqlite3"
    _downgrade_current_store_to_v10(RcaControlStore(path))
    create_schema = RcaControlStore._create_v11_snapshot_schema

    def create_after_marker_drift(cls, conn):
        create_schema(conn)
        conn.execute(
            "UPDATE control_meta SET value='pnc_rca_control_store_v9' "
            "WHERE key='schema_version'"
        )

    monkeypatch.setattr(
        RcaControlStore,
        "_create_v11_snapshot_schema",
        classmethod(create_after_marker_drift),
    )
    with pytest.raises(
        RuntimeError, match="incompatible_control_store_schema:version_marker"
    ):
        RcaControlStore(path)

    conn = sqlite3.connect(path)
    try:
        marker = conn.execute(
            "SELECT value FROM control_meta WHERE key='schema_version'"
        ).fetchone()[0]
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    finally:
        conn.close()
    assert marker == "pnc_rca_control_store_v10"
    assert tables.isdisjoint(V11_SNAPSHOT_TABLES)


def test_v10_stale_migration_starter_converges_on_committed_v11(tmp_path):
    path = tmp_path / "control.sqlite3"
    store = RcaControlStore(path)

    assert store._migrate_v10_to_v11() is False
    assert RcaControlStore(path, require_current=True).initialization_observation() == {
        "mode": "steady",
        "backfill_runs": 0,
    }


def test_current_v11_missing_source_authority_guard_is_rejected(tmp_path):
    path = tmp_path / "control.sqlite3"
    store = RcaControlStore(path)
    conn = store._connect()
    try:
        conn.execute("DROP TRIGGER trg_rca_source_authority_source_guard")
    finally:
        conn.close()

    with pytest.raises(
        RuntimeError, match="incompatible_control_store_schema:v11_triggers"
    ):
        RcaControlStore(path, require_current=True)


def test_current_v11_redefined_source_authority_guard_is_rejected(tmp_path):
    path = tmp_path / "control.sqlite3"
    store = RcaControlStore(path)
    conn = store._connect()
    try:
        conn.execute("DROP TRIGGER trg_rca_source_authority_source_guard")
        conn.execute(
            """
            CREATE TRIGGER trg_rca_source_authority_source_guard
            BEFORE INSERT ON rca_source_authority_receipts
            WHEN 0
            BEGIN
                SELECT RAISE(ABORT, 'rca_source_authority_source_mismatch');
            END
            """
        )
    finally:
        conn.close()

    with pytest.raises(
        RuntimeError, match="incompatible_control_store_schema:v11_trigger_sql"
    ):
        RcaControlStore(path, require_current=True)


@pytest.mark.parametrize("redefine", [False, True])
def test_current_v11_missing_or_redefined_source_authority_index_is_rejected(
    tmp_path, redefine
):
    path = tmp_path / "control.sqlite3"
    store = RcaControlStore(path)
    conn = store._connect()
    try:
        conn.execute("DROP INDEX idx_rca_source_authority_source")
        if redefine:
            conn.execute(
                "CREATE INDEX idx_rca_source_authority_source "
                "ON rca_source_authority_receipts(source_kind)"
            )
    finally:
        conn.close()

    expected = "v11_index_contract" if redefine else "v11_indexes"
    with pytest.raises(RuntimeError, match=expected):
        RcaControlStore(path, require_current=True)


def test_current_v11_redefined_table_constraint_is_rejected(tmp_path):
    path = tmp_path / "control.sqlite3"
    store = RcaControlStore(path)
    conn = store._connect()
    try:
        row = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' "
            "AND name='rca_canonical_requests'"
        ).fetchone()
        original = str(row["sql"])
        weakened = original.replace(
            "CHECK(schema_version = 'pnc_rca_canonical_request_v1')",
            "CHECK(length(schema_version) > 0)",
        )
        assert weakened != original
        schema_version = int(conn.execute("PRAGMA schema_version").fetchone()[0])
        conn.execute("PRAGMA writable_schema=ON")
        conn.execute(
            "UPDATE sqlite_master SET sql=? WHERE type='table' "
            "AND name='rca_canonical_requests'",
            (weakened,),
        )
        conn.execute("PRAGMA writable_schema=OFF")
        conn.execute(f"PRAGMA schema_version={schema_version + 1}")
    finally:
        conn.close()

    with pytest.raises(
        RuntimeError, match="incompatible_control_store_schema:v11_table_sql"
    ):
        RcaControlStore(path, require_current=True)


def test_v11_canonical_request_projection_guard_rejects_mismatched_json(tmp_path):
    store = RcaControlStore(tmp_path / "control.sqlite3")
    conn = store._connect()
    try:
        with pytest.raises(
            sqlite3.IntegrityError,
            match="rca_canonical_request_projection_mismatch",
        ):
            conn.execute(
                """
                INSERT INTO rca_canonical_requests(
                    request_sha256, schema_version, ticket_title_sha256,
                    creation_policy_sha256, business_profile_sha256,
                    execution_policy_sha256, publication_policy_sha256,
                    correction_lineage_policy_sha256, generation_reason,
                    generation_authorization_evidence_sha256,
                    canonical_request_json, persisted_at
                ) VALUES(?, 'pnc_rca_canonical_request_v1', ?, ?, ?, ?, ?, ?,
                         'initial', NULL, '{}', ?)
                """,
                tuple(f"{value:x}" * 64 for value in range(1, 8))
                + ("2026-07-25T10:00:00+00:00",),
            )
    finally:
        conn.close()
    assert store.list_rows("rca_canonical_requests") == []


def test_fresh_store_omits_retired_activation_schema(tmp_path):
    path = tmp_path / "control.sqlite3"
    store = RcaControlStore(path)

    with sqlite3.connect(path) as conn:
        objects = conn.execute(
            "SELECT type, name FROM sqlite_master "
            "WHERE type IN ('table', 'index', 'trigger')"
        ).fetchall()
        inbox_columns = {
            row[1] for row in conn.execute("PRAGMA table_info(kafka_inbox)")
        }
        ledger_columns = {
            row[1]
            for row in conn.execute(
                "PRAGMA table_info(rca_activation_admission_ledger)"
            )
        }

    names_by_type = {
        object_type: {name for kind, name in objects if kind == object_type}
        for object_type in ("table", "index", "trigger")
    }
    assert names_by_type["table"].isdisjoint(RETIRED_CONTROL_TABLES)
    assert names_by_type["index"].isdisjoint(RETIRED_CONTROL_INDEXES)
    assert not any(
        name.startswith(("trg_rca_capacity_", "trg_activation_historical_"))
        for name in names_by_type["trigger"]
    )
    assert "activation_slot_kind" not in inbox_columns
    assert "slot_kind" not in ledger_columns
    for table in RETIRED_CONTROL_TABLES:
        with pytest.raises(ValueError, match="unsupported table"):
            store.list_rows(table)


def test_retired_activation_schema_is_ignored_and_left_untouched(tmp_path):
    path = tmp_path / "control.sqlite3"
    RcaControlStore(path)
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE rca_activation_budget_slots(
                sentinel TEXT PRIMARY KEY
            );
            CREATE TABLE rca_capacity_transition_state(
                sentinel TEXT PRIMARY KEY
            );
            CREATE TABLE rca_capacity_transition_audit(
                sentinel TEXT PRIMARY KEY
            );
            CREATE TABLE rca_activation_historical_outbox_holds(
                sentinel TEXT PRIMARY KEY
            );
            CREATE TABLE rca_activation_historical_outbox_hold_items(
                sentinel TEXT PRIMARY KEY
            );
            CREATE TABLE rca_activation_historical_outbox_dispositions(
                sentinel TEXT PRIMARY KEY
            );
            CREATE TABLE rca_activation_historical_outbox_disposition_items(
                sentinel TEXT PRIMARY KEY
            );
            CREATE UNIQUE INDEX idx_rca_activation_slot_identity
                ON rca_activation_budget_slots(sentinel);
            CREATE INDEX idx_rca_capacity_transition_audit_time
                ON rca_capacity_transition_audit(sentinel);
            CREATE TRIGGER trg_rca_capacity_state_no_delete
            BEFORE DELETE ON rca_capacity_transition_state
            BEGIN
                SELECT RAISE(ABORT, 'retired_capacity_row_immutable');
            END;
            CREATE TRIGGER trg_activation_historical_hold_no_delete
            BEFORE DELETE ON rca_activation_historical_outbox_holds
            BEGIN
                SELECT RAISE(ABORT, 'retired_hold_row_immutable');
            END;
            """
        )
        for table in sorted(RETIRED_CONTROL_TABLES):
            conn.execute(f"INSERT INTO {table}(sentinel) VALUES(?)", (table,))
        before_objects = conn.execute(
            "SELECT type, name, tbl_name, sql FROM sqlite_master "
            "WHERE name LIKE 'rca_capacity_transition_%' "
            "OR name LIKE 'rca_activation_budget_slots' "
            "OR name LIKE 'rca_activation_historical_outbox_%' "
            "OR name LIKE 'idx_rca_activation_slot_%' "
            "OR name LIKE 'idx_rca_capacity_transition_%' "
            "OR name LIKE 'trg_rca_capacity_%' "
            "OR name LIKE 'trg_activation_historical_%' "
            "ORDER BY type, name"
        ).fetchall()
        before_rows = {
            table: conn.execute(f"SELECT * FROM {table}").fetchall()
            for table in RETIRED_CONTROL_TABLES
        }

    reopened = RcaControlStore(path, require_current=True)
    assert reopened.health()["ok"] is True
    assert "capacity_transition" not in reopened.health()

    with sqlite3.connect(path) as conn:
        after_objects = conn.execute(
            "SELECT type, name, tbl_name, sql FROM sqlite_master "
            "WHERE name LIKE 'rca_capacity_transition_%' "
            "OR name LIKE 'rca_activation_budget_slots' "
            "OR name LIKE 'rca_activation_historical_outbox_%' "
            "OR name LIKE 'idx_rca_activation_slot_%' "
            "OR name LIKE 'idx_rca_capacity_transition_%' "
            "OR name LIKE 'trg_rca_capacity_%' "
            "OR name LIKE 'trg_activation_historical_%' "
            "ORDER BY type, name"
        ).fetchall()
        after_rows = {
            table: conn.execute(f"SELECT * FROM {table}").fetchall()
            for table in RETIRED_CONTROL_TABLES
        }

    assert after_objects == before_objects
    assert after_rows == before_rows
    for table in RETIRED_CONTROL_TABLES:
        with pytest.raises(ValueError, match="unsupported table"):
            reopened.list_rows(table)


def test_v9_store_migrates_without_recreating_retired_activation_schema(tmp_path):
    path = tmp_path / "control.sqlite3"
    store = RcaControlStore(path)
    conn = sqlite3.connect(path, isolation_level=None)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute(
            "UPDATE control_meta SET value='pnc_rca_control_store_v9' "
            "WHERE key='schema_version'"
        )
    finally:
        conn.close()

    upgraded = RcaControlStore(path)
    assert upgraded.health()["schema_version"] == CONTROL_STORE_SCHEMA_VERSION
    assert upgraded.initialization_observation() == {
        "mode": "migration",
        "backfill_runs": 1,
    }
    with sqlite3.connect(path) as conn:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    assert tables.isdisjoint(RETIRED_CONTROL_TABLES)
